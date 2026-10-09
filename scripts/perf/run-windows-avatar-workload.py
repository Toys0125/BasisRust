#!/usr/bin/env python3
"""Windows-friendly Unity-policy avatar load test for a Rust or C# server."""

import argparse
import csv
import hashlib
import json
import os
import pathlib
import platform
import re
import shutil
import shlex
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request
import xml.etree.ElementTree as ET


ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "docs/performance/fixtures"
WINDOWS = os.name == "nt"
CREATE_NEW_PROCESS_GROUP = 0x00000200
CTRL_BREAK_EVENT = 1
CLOCK_TICKS = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100
# spawn kwargs that mirror CREATE_NEW_PROCESS_GROUP on Windows.
SPAWN_KWARGS = {"creationflags": CREATE_NEW_PROCESS_GROUP} if WINDOWS else {"start_new_session": True}
# Signals a workload process group can be interrupted with.
GROUP_BREAK_SIGNAL = signal.CTRL_BREAK_EVENT if WINDOWS else signal.SIGINT
GROUP_BREAK_SIGNALS = (signal.SIGBREAK, signal.SIGINT) if WINDOWS else (signal.SIGINT,)


if WINDOWS:
    import ctypes

    class FILETIME(ctypes.Structure):
        _fields_ = [("low", ctypes.c_uint32), ("high", ctypes.c_uint32)]

    class PROCESS_MEMORY_COUNTERS(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.c_uint32), ("PageFaultCount", ctypes.c_uint32),
            ("PeakWorkingSetSize", ctypes.c_size_t), ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t), ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t), ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t), ("PeakPagefileUsage", ctypes.c_size_t),
        ]

    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    kernel32.OpenProcess.restype = ctypes.c_void_p
    kernel32.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
    kernel32.GetProcessTimes.argtypes = [ctypes.c_void_p, ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME), ctypes.POINTER(FILETIME)]
    kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
    psapi.GetProcessMemoryInfo.argtypes = [ctypes.c_void_p, ctypes.POINTER(PROCESS_MEMORY_COUNTERS), ctypes.c_uint32]


def sha256(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def filetime_seconds(value):
    return ((value.high << 32) | value.low) / 10_000_000


def process_metrics(pid):
    """CPU and memory counters for a child process.

    Windows uses GetProcessTimes/GetProcessMemoryInfo. Linux reads /proc, where
    utime/stime come from field 14/15 of /proc/<pid>/stat (in clock ticks) and
    resident memory from VmRSS in /proc/<pid>/status. Linux has no direct
    equivalent of the Windows commit charge, so resident plus swapped pages is
    reported instead; that makes *_commit_charge_bytes a different quantity
    here and not comparable to a Windows run.
    """
    if WINDOWS:
        handle = kernel32.OpenProcess(0x1000, False, pid)
        if not handle:
            return None
        try:
            created, exited, kernel, user = FILETIME(), FILETIME(), FILETIME(), FILETIME()
            counters = PROCESS_MEMORY_COUNTERS()
            counters.cb = ctypes.sizeof(counters)
            if not kernel32.GetProcessTimes(handle, ctypes.byref(created), ctypes.byref(exited), ctypes.byref(kernel), ctypes.byref(user)):
                return None
            if not psapi.GetProcessMemoryInfo(handle, ctypes.byref(counters), counters.cb):
                return None
            return {
                "kernel_cpu_seconds": filetime_seconds(kernel),
                "user_cpu_seconds": filetime_seconds(user),
                "cpu_seconds": filetime_seconds(kernel) + filetime_seconds(user),
                "working_set_bytes": counters.WorkingSetSize,
                "commit_charge_bytes": counters.PagefileUsage,
            }
        finally:
            kernel32.CloseHandle(handle)

    try:
        with open(f"/proc/{pid}/stat", "rb") as stream:
            # comm can contain spaces and parentheses, so split after the last ')'.
            fields = stream.read().rpartition(b")")[2].split()
        user_ticks = int(fields[11])
        kernel_ticks = int(fields[12])
        with open(f"/proc/{pid}/status", "rb") as stream:
            status = stream.read().decode("ascii", "replace")
    except (OSError, ValueError, IndexError):
        return None
    pages = {}
    for line in status.splitlines():
        key, _, value = line.partition(":")
        if key in ("VmRSS", "VmSwap"):
            pages[key] = int(value.split()[0]) * 1024
    working_set = pages.get("VmRSS", 0)
    return {
        "kernel_cpu_seconds": kernel_ticks / CLOCK_TICKS,
        "user_cpu_seconds": user_ticks / CLOCK_TICKS,
        "cpu_seconds": (user_ticks + kernel_ticks) / CLOCK_TICKS,
        "working_set_bytes": working_set,
        "commit_charge_bytes": working_set + pages.get("VmSwap", 0),
    }


def stop_process(process):
    if process is None or process.poll() is not None:
        return
    try:
        process.send_signal(GROUP_BREAK_SIGNAL)
        process.wait(timeout=20)
    except (OSError, subprocess.TimeoutExpired):
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def interrupt_workload(signum, frame):
    # Let the run's finally block stop its owned server/client processes when
    # a crossover driver cancels this process group.
    raise KeyboardInterrupt


def health(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2) as response:
        return json.loads(response.read())


def available_port(family, socktype, host):
    sock = socket.socket(family, socktype)
    try:
        if family == socket.AF_INET6 and socktype == socket.SOCK_DGRAM:
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        sock.bind((host, 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def git_revision():
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    except Exception:
        return "unavailable"


def population(status, server_log, client_log, server_kind):
    if server_kind == "csharp":
        # C# health exposes transport visitors, not authenticated avatar states.
        # Independently require successful client joins, then validate all remote
        # avatar senders with the observer CSV after the run.
        joined = set(re.findall(r"client (\d+) connected as remote peer", client_log.read_text(errors="replace")))
        return int(status.get("visitors", 0)), None, len(joined)
    active_lines = [line for line in server_log.read_text(errors="replace").splitlines() if "active_states=" in line]
    match = re.search(r"active_states=(\d+)", active_lines[-1]) if active_lines else None
    return int(status.get("players_online", 0)), int(match.group(1)) if match else -1, None


def client_receive_overrides(voice_enabled, all_clients, no_shared, platform_name):
    overrides = {}
    if no_shared or (voice_enabled and platform_name != "nt"):
        overrides["BASIS_CLIENT_SHARED_RECEIVE"] = "0"
    if all_clients or voice_enabled:
        overrides["BASIS_CLIENT_LOAD_SINK_FILTER"] = "0"
    return overrides


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=pathlib.Path, default=ROOT / ("BasisRustServer/target/release/basis-server-console.exe" if WINDOWS else "BasisRustServer/target/release/basis-server-console"))
    parser.add_argument("--client", type=pathlib.Path, default=ROOT / ("BasisRustClient/target/release/basis-rust-client.exe" if WINDOWS else "BasisRustClient/target/release/basis-rust-client"))
    parser.add_argument("--server-kind", choices=("rust", "csharp"), default="rust")
    parser.add_argument("--server-config", type=pathlib.Path, default=FIXTURES / "avatar-1500-server.xml")
    parser.add_argument("--no-server-avatar-diagnostics", action="store_true", help="disable Rust per-pair timing diagnostics for process-counter comparisons")
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--clients", type=int, default=1000)
    parser.add_argument("--warmup-seconds", type=int, default=15)
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--workers", type=int, default=4, help="Tokio worker count passed through BASIS_CLIENT_TOKIO_WORKERS")
    parser.add_argument("--rayon-threads", type=int, help="optional server RAYON_NUM_THREADS override; 0 selects automatic workers")
    parser.add_argument("--port", type=int, default=0, help="UDP port; 0 selects an available ephemeral port")
    parser.add_argument("--health-port", type=int, default=0, help="health port; 0 selects an available ephemeral port")
    parser.add_argument("--remote-ssh", help="SSH destination for a Linux load generator")
    parser.add_argument("--remote-root", help="isolated matching-revision worktree on the remote host")
    parser.add_argument("--remote-output", help="new remote artifact directory")
    parser.add_argument("--server-ip", default="127.0.0.1")
    parser.add_argument("--voice-audio-folder", type=pathlib.Path, help="enable measured voice traffic from this Ogg Opus folder (local clients only)")
    parser.add_argument("--voice-speaker-percent", type=int, default=10)
    parser.add_argument("--no-voice-reencode", action="store_true", help="use pre-encoded Unity-compatible 20 ms Opus packets")
    parser.add_argument("--no-client-shared-receive", action="store_true",
                        help="force per-client receive tasks; on Linux the shared epoll receiver defaults on and "
                             "discards unreliable payloads for non-observer clients, which silently starves voice")
    parser.add_argument("--client-voice-all-clients", action="store_true",
                        help="disable the Linux bulk-unreliable receive filter for every client; automatic when "
                             "--voice-audio-folder is supplied, optional for the avatar baseline")
    parser.add_argument("--max-server-working-set-mib", type=int, help="stop measurement at this server working set; voice default=24576 MiB, otherwise unlimited; 0 disables")
    parser.add_argument("--post-client-seconds", type=int, default=0, help="observe server memory/health after clients exit before stopping the server")
    args = parser.parse_args()
    if args.max_server_working_set_mib is None:
        args.max_server_working_set_mib = 24576 if args.voice_audio_folder else 0
    if args.max_server_working_set_mib < 0:
        parser.error("server working set limit must be >=0")
    if args.post_client_seconds < 0:
        parser.error("post-client seconds must be >=0")
    if args.server_kind == "csharp" and args.remote_ssh:
        parser.error("C# comparison currently supports local clients only")
    if args.voice_audio_folder and args.remote_ssh:
        parser.error("voice measurements currently support local Windows clients only")
    if not 1 <= args.voice_speaker_percent <= 100:
        parser.error("voice speaker percent must be between 1 and 100")
    if args.voice_audio_folder and not args.voice_audio_folder.is_dir():
        parser.error(f"voice audio folder missing: {args.voice_audio_folder}")
    if args.remote_ssh and not (args.remote_root and args.remote_output and args.server_ip != "127.0.0.1"):
        parser.error("remote mode requires --remote-root, --remote-output, and the server LAN IP")
    rtk = shutil.which("rtk")
    if args.remote_ssh and not rtk:
        parser.error("remote mode requires rtk on PATH")

    def ssh(arguments):
        return [rtk, "proxy", "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", args.remote_ssh, shlex.join(arguments)]

    server_bin, client_bin = args.server.resolve(), args.client.resolve()
    output = args.output.resolve()
    if args.port == 0:
        args.port = available_port(socket.AF_INET6, socket.SOCK_DGRAM, "::")
    if args.health_port == 0:
        args.health_port = available_port(socket.AF_INET, socket.SOCK_STREAM, "127.0.0.1")
    if args.clients < 2 or args.warmup_seconds < 0 or args.window_seconds < 1 or args.workers < 1 or (args.rayon_threads is not None and args.rayon_threads < 0):
        parser.error("clients >=2, warmup >=0, window >=1, client workers >=1, and Rayon threads >=0 are required")
    if not server_bin.is_file() or not client_bin.is_file():
        parser.error("build the release server and client first or pass --server/--client")
    if not args.server_config.is_file():
        parser.error(f"server config fixture missing: {args.server_config}")
    if output.exists():
        parser.error(f"output directory already exists: {output}")

    output.mkdir(parents=True)
    base_dir = pathlib.Path(tempfile.mkdtemp(prefix="basis-avatar-server-" + ("win" if WINDOWS else "linux") + "-"))
    (base_dir / "config").mkdir()
    server_config = base_dir / "config/config.xml"
    shutil.copyfile(args.server_config, server_config)
    launch_server = server_bin
    if args.server_kind == "csharp":
        # The C# console resolves config beside its assembly, ignoring Rust CLI
        # options. Copy the published app so each run owns its config and logs.
        shutil.copytree(server_bin.parent, base_dir, dirs_exist_ok=True)
        launch_server = base_dir / server_bin.name
        tree = ET.parse(server_config)
        restriction = tree.getroot().find("BasisUserRestrictionMode")
        if restriction is not None and restriction.text == "None":
            restriction.text = "Normal"  # C# enum spelling for unrestricted joins.
        for name, value in {"SetPort": str(args.port), "HealthCheckHost": "127.0.0.1",
                            "HealthCheckPort": str(args.health_port)}.items():
            node = tree.getroot().find(name)
            if node is None:
                node = ET.SubElement(tree.getroot(), name)
            node.text = value
        tree.write(server_config, encoding="utf-8", xml_declaration=True)
        # Recent C# versions moved LiteNetLib settings into a sidecar. Translate
        # the shared fixture's transport fields rather than silently using defaults.
        transport = ET.Element("LNLTransportConfig")
        for name in ("UseNativeSockets", "NatPunchEnabled", "PingInterval", "DisconnectTimeout",
                     "SimulatePacketLoss", "SimulateLatency", "SimulationPacketLossChance",
                     "SimulationMinLatency", "SimulationMaxLatency", "ReconnectDelay",
                     "MaxConnectAttempts", "ReuseAddresss", "DontRoute", "Ipv6Enabled",
                     "MtuOverride", "MtuDiscovery", "DisconnectOnUnreachable", "AllowPeerAddressChange"):
            node = tree.getroot().find(name)
            if node is not None:
                ET.SubElement(transport, "IPv6Enabled" if name == "Ipv6Enabled" else name).text = node.text
        (base_dir / "config/transports").mkdir(exist_ok=True)
        ET.ElementTree(transport).write(base_dir / "config/transports/litenetlib.xml", encoding="utf-8", xml_declaration=True)
    marker = output / "observe-start.marker"
    server_log = (output / "server.log").open("w", buffering=1)
    client_log = None
    server = client = None
    started = time.time()
    for break_signal in GROUP_BREAK_SIGNALS:
        signal.signal(break_signal, interrupt_workload)
    try:
        server_env = dict(os.environ)
        server_env.update({
            "EnableBSRProfiling": "false", "HealthIncludeBSRProfiling": "false", "EnableConsole": "false",
            "BASIS_STATUS_INTERVAL_SECS": "5", "BASIS_AVATAR_DIAGNOSTIC_OBSERVER_ID": "0",
            "BASIS_AVATAR_DIAGNOSTIC_START_FILE": str(marker),
            "BASIS_AVATAR_DIAGNOSTIC_CSV": str(output / "server-pairs.csv"),
            "BASIS_AVATAR_DIAGNOSTIC_WINDOW_SECS": str(args.window_seconds),
        })
        if args.no_server_avatar_diagnostics or args.server_kind == "csharp":
            for key in list(server_env):
                if key.startswith("BASIS_AVATAR_DIAGNOSTIC_"):
                    server_env.pop(key)
        if args.rayon_threads is not None:
            server_env["RAYON_NUM_THREADS"] = str(args.rayon_threads)
        server_cmd = [str(server_bin), "--base-dir", str(base_dir), "--port", str(args.port),
                      "--no-console", "--health-host", "127.0.0.1", "--health-port", str(args.health_port)]
        if args.server_kind == "csharp":
            server_cmd = [str(launch_server)]
        (output / "commands.txt").write_text(" ".join(server_cmd) + "\n", encoding="utf-8")
        server = subprocess.Popen(server_cmd, cwd=ROOT, env=server_env, stdout=server_log,
                                  stderr=subprocess.STDOUT, **SPAWN_KWARGS)

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError(f"server exited early with status {server.returncode}")
            try:
                status = health(args.health_port)
                if status.get("status") == "healthy" or (args.server_kind == "csharp" and status.get("ready")):
                    break
            except Exception:
                pass
            time.sleep(0.25)
        else:
            raise RuntimeError("server health endpoint did not become ready")

        time.sleep(2)
        client_env = dict(os.environ)
        client_env.update({"BASIS_CLIENT_TOKIO_WORKERS": str(args.workers), "BASIS_AVATAR_DIAGNOSTICS": "true"})
        client_env.pop("BASIS_VOICE_DIAGNOSTIC_CSV", None)
        client_env.update(client_receive_overrides(
            bool(args.voice_audio_folder), args.client_voice_all_clients, args.no_client_shared_receive, os.name))
        if args.voice_audio_folder:
            client_env["BASIS_VOICE_DIAGNOSTIC_CSV"] = str(output / "voice.csv")
        remote_root = pathlib.PurePosixPath(args.remote_root) if args.remote_ssh else None
        remote_output = pathlib.PurePosixPath(args.remote_output) if args.remote_ssh else None
        client_executable = str(remote_root / "client-target/release/basis-rust-client") if args.remote_ssh else str(client_bin)
        client_fixture = str(remote_root / "docs/performance/fixtures/avatar-1500-client.xml") if args.remote_ssh else str(FIXTURES / "avatar-1500-client.xml")
        client_marker = str(remote_output / "observe-start.marker") if args.remote_ssh else str(marker)
        observer_csv = str(remote_output / "observer.csv") if args.remote_ssh else str(output / "observer.csv")
        client_cmd = [client_executable, "--config", client_fixture,
                      "--ip", args.server_ip, "--port", str(args.port), "--clients", str(args.clients),
                      "--no-reconnect", "--connect-timeout-ms", "60000", "--connect-batch-size", "25",
                      "--connect-batch-delay-ms", "250", "--movement-interval-ms", "20",
                      "--movement-jitter-percent", "0", "--no-spread", "--unity-avatar-policy",
                      "--unity-frame-rate", "60", "--unity-pose-amplitude-degrees", "20",
                      "--observe-avatar-csv", observer_csv, "--avatar-observe-radius", "40",
                      "--avatar-observe-expected-peers", str(args.clients - 1),
                      "--observe-avatar-start-file", client_marker, "--observe-avatar-window-secs", str(args.window_seconds)]
        if args.voice_audio_folder:
            client_cmd.extend(["--voice", "--voice-audio-folder", str(args.voice_audio_folder.resolve()),
                               "--voice-speaker-percent", str(args.voice_speaker_percent),
                               "--voice-jitter-percent", "0", "--voice-frame-duration-ms", "20"])
            if args.no_voice_reencode:
                client_cmd.append("--no-voice-reencode")
        if args.remote_ssh:
            remote_helper = str(remote_root / "remote-avatar-client.py")
            subprocess.run([rtk, "proxy", "scp", str(ROOT / "scripts/perf/remote-avatar-client.py"),
                            args.remote_ssh + ":" + remote_helper], check=True)
            client_cmd = ssh(["env", "BASIS_CLIENT_TOKIO_WORKERS=" + str(args.workers), "BASIS_AVATAR_DIAGNOSTICS=true",
                              "python3", remote_helper, "--output", str(remote_output), "--", *client_cmd])
        with (output / "commands.txt").open("a", encoding="utf-8") as command_file:
            command_file.write(" ".join(client_cmd) + "\n")
        client_log = (output / "client.log").open("w", buffering=1)
        client = subprocess.Popen(client_cmd, cwd=ROOT, env=client_env, stdin=subprocess.PIPE, stdout=client_log,
                                  stderr=subprocess.STDOUT, **SPAWN_KWARGS)
        process_ids = {"server_pid": server.pid, "client_pid": None if args.remote_ssh else client.pid}
        if args.remote_ssh:
            process_ids["ssh_process_pid"] = client.pid
        (output / "processes.json").write_text(json.dumps(process_ids, indent=2) + "\n", encoding="utf-8")

        ready_deadline = time.monotonic() + 360
        active_count = -1
        while time.monotonic() < ready_deadline:
            if server.poll() is not None:
                raise RuntimeError(f"server exited before readiness with status {server.returncode}")
            if client.poll() is not None:
                raise RuntimeError(f"client exited before readiness with status {client.returncode}")
            try:
                status = health(args.health_port)
            except Exception:
                status = {}
            active_peers, active_count, joined_count = population(status, output / "server.log", output / "client.log", args.server_kind)
            if active_peers == args.clients and (joined_count == args.clients if args.server_kind == "csharp" else active_count == args.clients):
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"{args.clients} clients did not become ready; peers={active_peers}, active_states={active_count}, client_joins={joined_count}")

        (output / "ready-health.json").write_text(json.dumps(status, indent=2) + "\n", encoding="utf-8")
        warmup_started = time.monotonic()
        warmup_ended_early_reason = None
        with (output / "warmup-metrics.csv").open("w", newline="", encoding="utf-8") as stream:
            warmup_writer = csv.DictWriter(stream, fieldnames=["elapsed_seconds", "server_working_set_bytes", "server_commit_charge_bytes", "server_cpu_seconds"])
            warmup_writer.writeheader()
            while time.monotonic() - warmup_started < args.warmup_seconds:
                sm = process_metrics(server.pid)
                if sm is None or server.poll() is not None or client.poll() is not None:
                    raise RuntimeError("server/client exited during warmup")
                warmup_writer.writerow({"elapsed_seconds": time.monotonic() - warmup_started,
                                        "server_working_set_bytes": sm["working_set_bytes"],
                                        "server_commit_charge_bytes": sm["commit_charge_bytes"],
                                        "server_cpu_seconds": sm["cpu_seconds"]})
                stream.flush()
                if args.max_server_working_set_mib and sm["working_set_bytes"] >= args.max_server_working_set_mib * 2**19:
                    warmup_ended_early_reason = "server working set reached half the memory limit; starting measurement early"
                    print(warmup_ended_early_reason, flush=True)
                    break
                time.sleep(min(1, max(0, args.warmup_seconds - (time.monotonic() - warmup_started))))
        actual_warmup_seconds = time.monotonic() - warmup_started
        if args.remote_ssh:
            subprocess.run(ssh(["touch", client_marker]), check=True)
        marker.write_text(f"{time.time():.6f}\n", encoding="utf-8")
        samples_path = output / "process-metrics.csv"
        fields = ["unix_seconds", "phase", "server_cpu_seconds", "server_kernel_cpu_seconds",
                  "server_user_cpu_seconds", "server_cpu_percent_of_machine",
                  "server_working_set_bytes", "server_commit_charge_bytes", "client_cpu_seconds",
                  "client_kernel_cpu_seconds", "client_user_cpu_seconds",
                  "client_cpu_percent_of_machine", "client_working_set_bytes", "client_commit_charge_bytes",
                  "players_online", "active_states", "client_join_logs"]
        cpus = os.cpu_count() or 1
        with samples_path.open("w", newline="", encoding="utf-8") as samples_file, (output / "health.jsonl").open("w", encoding="utf-8") as health_file:
            writer = csv.DictWriter(samples_file, fieldnames=fields)
            writer.writeheader()
            readiness_file = (output / "readiness.csv").open("w", newline="", encoding="utf-8")
            readiness = csv.DictWriter(readiness_file, fieldnames=["unix_seconds", "players_online", "active_states", "client_join_logs"])
            readiness.writeheader()
            previous, previous_time = {}, time.monotonic()
            end = previous_time + args.window_seconds
            measurement_stop_reason = None
            try:
                while time.monotonic() < end:
                    now = time.monotonic()
                    sm = process_metrics(server.pid)
                    cm = None if args.remote_ssh else process_metrics(client.pid)
                    status = health(args.health_port)
                    health_file.write(json.dumps({"unix_seconds": time.time(), **status}, sort_keys=True) + "\n")
                    health_file.flush()
                    active_peers, active_count, joined_count = population(status, output / "server.log", output / "client.log", args.server_kind)
                    readiness.writerow({"unix_seconds": time.time(), "players_online": active_peers, "active_states": active_count, "client_join_logs": joined_count})
                    readiness_file.flush()
                    row = {"unix_seconds": time.time(), "phase": "measure",
                           "players_online": active_peers, "active_states": active_count, "client_join_logs": joined_count}
                    for label, metrics in (("server", sm), ("client", cm)):
                        cpu_seconds = metrics["cpu_seconds"] if metrics else ""
                        old_cpu = previous.get(label)
                        pct = ((cpu_seconds - old_cpu) / max(now - previous_time, 0.001) / cpus * 100) if metrics and old_cpu is not None else ""
                        row.update({f"{label}_cpu_seconds": cpu_seconds,
                                    f"{label}_kernel_cpu_seconds": metrics["kernel_cpu_seconds"] if metrics else "",
                                    f"{label}_user_cpu_seconds": metrics["user_cpu_seconds"] if metrics else "",
                                    f"{label}_cpu_percent_of_machine": pct,
                                    f"{label}_working_set_bytes": metrics["working_set_bytes"] if metrics else "",
                                    f"{label}_commit_charge_bytes": metrics["commit_charge_bytes"] if metrics else ""})
                        if metrics:
                            previous[label] = metrics["cpu_seconds"]
                    writer.writerow(row)
                    samples_file.flush()
                    if active_peers != args.clients or (joined_count != args.clients if args.server_kind == "csharp" else active_count != args.clients):
                        raise RuntimeError(f"readiness dropped during measurement: peers={active_peers}, active_states={active_count}")
                    if client.poll() is not None or server.poll() is not None:
                        raise RuntimeError(f"process exited during measurement: client={client.poll()} server={server.poll()}")
                    if args.max_server_working_set_mib and sm and sm["working_set_bytes"] >= args.max_server_working_set_mib * 2**20:
                        measurement_stop_reason = f"server working set reached {sm['working_set_bytes'] / 2**20:.1f} MiB (limit {args.max_server_working_set_mib} MiB)"
                        print(f"Measurement stopped: {measurement_stop_reason}", flush=True)
                        break
                    previous_time = now
                    time.sleep(min(2, max(0, end - time.monotonic())))
            finally:
                readiness_file.close()

        if not measurement_stop_reason:
            time.sleep(2)
        if client.poll() is None and client.stdin:
            try:
                client.stdin.write(b"quit 100 0\n")
                client.stdin.flush()
                client.wait(timeout=45)
            except (BrokenPipeError, subprocess.TimeoutExpired):
                stop_process(client)
        else:
            stop_process(client)
        if args.post_client_seconds:
            post_started = time.monotonic()
            with (output / "post-client-metrics.csv").open("w", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=["unix_seconds", "elapsed_seconds", "players_online", "server_working_set_bytes", "server_commit_charge_bytes", "server_cpu_seconds"])
                writer.writeheader()
                while time.monotonic() - post_started < args.post_client_seconds:
                    metrics = process_metrics(server.pid)
                    if metrics is None or server.poll() is not None:
                        raise RuntimeError("server exited during post-client observation")
                    status = health(args.health_port)
                    writer.writerow({"unix_seconds": time.time(), "elapsed_seconds": time.monotonic() - post_started,
                                     "players_online": status.get("visitors" if args.server_kind == "csharp" else "players_online"),
                                     "server_working_set_bytes": metrics["working_set_bytes"],
                                     "server_commit_charge_bytes": metrics["commit_charge_bytes"],
                                     "server_cpu_seconds": metrics["cpu_seconds"]})
                    stream.flush()
                    time.sleep(min(1, max(0, args.post_client_seconds - (time.monotonic() - post_started))))
        stop_process(server)
        shutil.copytree(base_dir / "config", output / "effective-server-config")
        remote_metadata = None
        if args.remote_ssh:
            subprocess.run([rtk, "proxy", "scp", "-r", args.remote_ssh + ":" + str(remote_output),
                            str(output / "remote")], check=True)
            for source in (output / "remote").glob("observer*.csv"):
                shutil.copy2(source, output / source.name)
            remote_metadata = json.loads((output / "remote/client-process.json").read_text())
            process_ids["remote_client_pid"] = remote_metadata["client_pid"]
            (output / "processes.json").write_text(json.dumps(process_ids, indent=2) + "\n", encoding="utf-8")
            if client.returncode != 0 or remote_metadata["exit_code"] != 0:
                raise RuntimeError(f"Remote client/helper shutdown failed: client={remote_metadata['exit_code']}, ssh={client.returncode}")
        workload = {
            "platform": platform.platform(), "os_name": os.name, "python": platform.python_version(),
            "git_revision": git_revision(), "logical_processors": cpus,
            "server_pid": server.pid, "client_pid": client.pid,
            "clients": args.clients, "tokio_workers": args.workers,
            "server_rayon_threads_override": args.rayon_threads,
            "server_kind": args.server_kind,
            "server_avatar_diagnostics": args.server_kind == "rust" and not args.no_server_avatar_diagnostics,
            "readiness_basis": "transport visitors plus client join logs; final observer validates avatar coverage" if args.server_kind == "csharp" else "authenticated players and active avatar states",
            "warmup_seconds": args.warmup_seconds, "measurement_window_seconds": args.window_seconds,
            "actual_warmup_seconds": actual_warmup_seconds,
            "warmup_ended_early_reason": warmup_ended_early_reason,
            "measurement_complete": measurement_stop_reason is None,
            "measurement_stop_reason": measurement_stop_reason,
            "max_server_working_set_mib": args.max_server_working_set_mib,
            "post_client_observation_seconds": args.post_client_seconds,
            "server_port": args.port, "health_port": args.health_port,
            "server_ip": args.server_ip,
            "movement_interval_ms": 20, "jitter_percent": 0, "unity_frame_accumulator_fps": 60,
            "layout": "remote Linux client, zero positional drift" if args.remote_ssh else "colocated, zero positional drift",
            "pose": "valid synthetic deterministic root/body rotation channels; not captured Unity pose",
            "voice": bool(args.voice_audio_folder), "p2p": False, "server_profiling": False,
            "client_shared_receive_disabled": args.no_client_shared_receive,
            "voice_audio_folder": str(args.voice_audio_folder.resolve()) if args.voice_audio_folder else None,
            "voice_speaker_percent": args.voice_speaker_percent if args.voice_audio_folder else 0,
            "voice_frame_duration_ms": 20 if args.voice_audio_folder else None,
            "voice_reencode": not args.no_voice_reencode if args.voice_audio_folder else None,
            "expected_observer_peers": args.clients - 1,
            "server_binary_sha256": sha256(server_bin), "client_binary_sha256": remote_metadata["client_binary_sha256"] if remote_metadata else sha256(client_bin),
            "server_config_fixture_sha256": sha256(args.server_config),
            "client_config_fixture_sha256": remote_metadata["client_config_fixture_sha256"] if remote_metadata else sha256(FIXTURES / "avatar-1500-client.xml"),
            "server_config_local_copy_sha256": sha256(server_config),
            "server_config_files_sha256": {str(path.relative_to(base_dir / "config")): sha256(path) for path in sorted((base_dir / "config").rglob("*.xml"))},
            "server_runtime_files_sha256": {path.name: sha256(path) for path in sorted(server_bin.parent.iterdir()) if path.suffix in (".exe", ".dll", ".json")} if args.server_kind == "csharp" else None,
            "relevant_environment": {key: {"server": server_env.get(key), "client": client_env.get(key)}
                                     for key in ("RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS",
                                                 "BASIS_UDP_RECEIVE_WORKERS", "BASIS_CLIENT_TOKIO_WORKERS",
                                                 "BASIS_VOICE_SEND_WORKERS", "BASIS_VOICE_SERVER_DIAGNOSTIC_CSV", "BASIS_EVENT_DIAGNOSTIC_CSV",
                                                 "BASIS_AVATAR_MIN_RECEIVER_SLICES", "BASIS_AVATAR_MAX_RECEIVER_SLICES",
                                                 "BASIS_AVATAR_TICK_BUDGET_MS", "BASIS_AVATAR_RECEIVER_CYCLE_BUDGET_MS",
                                                 "BASIS_AVATAR_FLUSH_LANES", "BASIS_CLIENT_SHARED_RECEIVE", "BASIS_CLIENT_LOAD_SINK_FILTER",
                                                 "EnableBSRProfiling", "HealthIncludeBSRProfiling", "EnableComputeOffload",
                                                 "BASIS_AVATAR_DIAGNOSTICS")},
            "server_exit_code": server.returncode, "client_exit_code": client.returncode,
            "started_unix_seconds": started, "finished_unix_seconds": time.time(),
        }
        if remote_metadata:
            workload.update(remote_ssh=args.remote_ssh, remote_root=args.remote_root, remote_output=args.remote_output,
                            server_ip=args.server_ip, client_pid=remote_metadata["client_pid"],
                            client_exit_code=remote_metadata["exit_code"], ssh_process_pid=client.pid,
                            ssh_exit_code=client.returncode,
                            remote_client=remote_metadata)
        (output / "workload.json").write_text(json.dumps(workload, indent=2) + "\n", encoding="utf-8")
        print(f"Run complete: {output}")
        print("Check readiness.csv, process-metrics.csv, observer.csv, server-pairs.csv, and logs.")
    finally:
        stop_process(client)
        stop_process(server)
        if client_log:
            client_log.close()
        server_log.close()
        shutil.rmtree(base_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
