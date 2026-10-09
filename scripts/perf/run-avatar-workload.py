#!/usr/bin/env python3
"""Run the colocated Unity-policy avatar workload against a local Rust or C# server."""

import argparse
import csv
import hashlib
import json
import os
import pathlib
import re
import shutil
import signal
import subprocess
import tempfile
import time
import urllib.request
import socket
from avatar_benchmark import native_sampler, select_port
from script_server import prepare_server, ready, population


ROOT = pathlib.Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "docs/performance/fixtures"


def sha256(path):
    return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()


def stop_process(process):
    if process is None or process.poll() is not None:
        return
    process.send_signal(signal.SIGINT)
    try:
        process.wait(timeout=15)
    except subprocess.TimeoutExpired:
        process.terminate()
        process.wait(timeout=5)


def health(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
        return json.loads(response.read())


def summarize_capture(output, clients, window_seconds, samples, client_exit, server_exit):
    observer = {}
    with (output / 'observer.csv').open(newline='') as file:
        for row in csv.reader(file):
            if len(row) != 2 or row[0] == 'observed_channel':
                break
            if row[0] != 'metric':
                observer[row[0]] = row[1]
    with (output / 'observer.sender.csv').open(newline='') as file:
        senders = list(csv.DictReader(file))
    checks = {'client_shutdown': client_exit == 0, 'server_shutdown': server_exit == 0,
              'observer_started': observer.get('window_started') == 'true',
              'full_window': int(observer.get('window_ms', 0)) == window_seconds * 1000,
              'observer_coverage': int(observer.get('near_peers', 0)) == clients - 1,
              'sender_records': len(senders) == clients,
              'sender_cadence': bool(senders) and all(int(s['socket_sent_full']) + int(s['socket_sent_delta']) >= max(1, int(window_seconds * 45)) for s in senders),
              'sender_errors': all(int(s['send_errors']) == 0 for s in senders),
              'senders_connected': all(s['connected_at_end'] == 'true' for s in senders)}
    for key in ('missing_expected_peers', 'stale_peers_500ms', 'decode_errors', 'unapplied_deltas', 'malformed_items'):
        checks['zero_' + key] = int(observer.get(key, -1)) == 0
    seconds = int(observer.get('window_ms', 0)) / 1000
    applied = int(observer.get('applied_full_items', 0)) + int(observer.get('applied_delta_items', 0))
    checks['positive_application'] = applied > 0
    last = samples[-1]
    return {'valid': all(checks.values()), 'failed_checks': [k for k, v in checks.items() if not v],
            'observer': observer, 'observer_items_per_second': applied / seconds if seconds else 0,
            'minimum_sender_sends': min((int(s['socket_sent_full']) + int(s['socket_sent_delta']) for s in senders), default=0),
            'processes': {label: {'cpu_core_equivalents': last[label + '_cpu_seconds'] / last['elapsed_seconds'],
                'peak_rss_bytes': max(row[label + '_rss_bytes'] for row in samples)} for label in ('server', 'client')}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=pathlib.Path, default=ROOT / "BasisRustServer/target/release/basis-server-console")
    parser.add_argument("--server-kind", choices=("rust", "csharp"), default="rust")
    parser.add_argument("--ready-timeout", type=int, default=120)
    parser.add_argument("--client", type=pathlib.Path, default=ROOT / "BasisRustClient/target/release/basis-rust-client")
    parser.add_argument("--output", type=pathlib.Path, required=True, help="new directory for logs and CSV output")
    parser.add_argument("--clients", type=int, default=1500)
    parser.add_argument("--additional-avatar-bytes", type=int, default=0,
                        help="synthetic AdditionalAvatarData bytes per update (0..255)")
    parser.add_argument("--warmup-seconds", type=int, default=15)
    parser.add_argument("--window-seconds", type=int, default=60)
    parser.add_argument("--port", type=int, default=4296)
    parser.add_argument("--health-port", type=int, default=10666)
    parser.add_argument("--server-cpus", help="optional taskset CPU list, for example 0-7")
    parser.add_argument("--client-cpus", help="optional taskset CPU list, for example 8-15")
    args = parser.parse_args()

    server_bin = args.server.resolve()
    client_bin = args.client.resolve()
    output = args.output.resolve()
    if not 0 <= args.additional_avatar_bytes <= 255:
        parser.error("additional-avatar-bytes must be 0..255")
    if args.clients < 2 or args.warmup_seconds < 0 or args.window_seconds < 1 or args.ready_timeout < 1:
        parser.error("clients must be >=2, warmup >=0, window >=1, and ready-timeout >=1")
    if not server_bin.is_file() or not client_bin.is_file():
        parser.error("build the release server and client first or pass --server/--client")
    if output.exists():
        parser.error(f"output directory already exists: {output}")
    if (args.server_cpus or args.client_cpus) and not shutil.which("taskset"):
        parser.error("taskset is required when CPU affinity options are supplied")

    args.port = select_port(socket.SOCK_DGRAM, args.port)
    args.health_port = select_port(socket.SOCK_STREAM, args.health_port)
    output.mkdir(parents=True)
    base_dir = pathlib.Path(tempfile.mkdtemp(prefix="basis-avatar-server-"))
    raw_server_cmd, server_config, server_metadata = prepare_server(args.server_kind, server_bin, base_dir,
        FIXTURES / 'avatar-1500-server.xml', args.port, args.health_port)
    sampler = native_sampler()
    marker = output / "observe-start.marker"
    server_log = (output / "server.log").open("w")
    client_log = None
    server = client = None

    def affinity(cpus, command):
        return ["taskset", "-c", cpus, *command] if cpus else command

    try:
        server_env = dict(os.environ)
        server_env.update({
            "EnableBSRProfiling": "false",
            "EnableConsole": "false",
            "BASIS_STATUS_INTERVAL_SECS": "5",
            "BASIS_AVATAR_DIAGNOSTIC_OBSERVER_ID": "0",
            "BASIS_AVATAR_DIAGNOSTIC_START_FILE": str(marker),
            "BASIS_AVATAR_DIAGNOSTIC_CSV": str(output / "server-pairs.csv"),
            "BASIS_AVATAR_DIAGNOSTIC_WINDOW_SECS": str(args.window_seconds),
        })
        if args.server_kind == "csharp":
            server_env = {k: v for k, v in server_env.items() if not k.startswith("BASIS_AVATAR_DIAGNOSTIC_")}
        server_cmd = affinity(args.server_cpus, raw_server_cmd)
        (output / "commands.txt").write_text(" ".join(server_cmd) + "\n")
        server = subprocess.Popen(server_cmd, cwd=ROOT, env=server_env, stdout=server_log, stderr=subprocess.STDOUT)

        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if server.poll() is not None:
                raise RuntimeError(f"server exited early with status {server.returncode}")
            try:
                if ready(health(args.health_port), args.server_kind):
                    break
            except Exception:
                pass
            time.sleep(0.25)
        else:
            raise RuntimeError("server health endpoint did not become ready")

        time.sleep(2)
        client_env = dict(os.environ)
        client_env.update({"BASIS_CLIENT_TOKIO_WORKERS": "4", "BASIS_AVATAR_DIAGNOSTICS": "true"})
        client_cmd = affinity(args.client_cpus, [
            str(client_bin), "--config", str(FIXTURES / "avatar-1500-client.xml"),
            "--ip", "127.0.0.1", "--port", str(args.port), "--clients", str(args.clients),
            "--no-reconnect", "--connect-timeout-ms", "60000", "--connect-batch-size", "25",
            "--connect-batch-delay-ms", "250", "--movement-interval-ms", "20",
            "--movement-jitter-percent", "0", "--no-spread", "--unity-avatar-policy",
            "--additional-avatar-bytes", str(args.additional_avatar_bytes),
            "--unity-frame-rate", "60", "--unity-pose-amplitude-degrees", "20",
            "--observe-avatar-csv", str(output / "observer.csv"), "--avatar-observe-radius", "40",
            "--avatar-observe-expected-peers", str(args.clients - 1),
            "--observe-avatar-start-file", str(marker), "--observe-avatar-window-secs", str(args.window_seconds),
        ])
        with (output / "commands.txt").open("a") as command_file:
            command_file.write(" ".join(client_cmd) + "\n")
        client_log = (output / "client.log").open("w")
        client = subprocess.Popen(client_cmd, cwd=ROOT, env=client_env, stdout=client_log, stderr=subprocess.STDOUT)

        ready_deadline = time.monotonic() + args.ready_timeout
        active = False
        while time.monotonic() < ready_deadline:
            if client.poll() is not None:
                raise RuntimeError(f"client exited before readiness with status {client.returncode}")
            with (output / "server.log").open(errors="replace") as log_file:
                text = log_file.read()
            active_counts = re.findall(r"active_states=(\d+)", text)
            try:
                status = health(args.health_port)
            except Exception:
                status = {}
            player_count = population(status, args.server_kind, (output / 'client.log').read_text(errors='replace'))
            avatar_ready = args.server_kind == 'csharp' or (active_counts and int(active_counts[-1]) == args.clients)
            if player_count == args.clients and avatar_ready:
                active = True
                break
            time.sleep(1)
        if not active:
            raise RuntimeError(f"{args.clients} authenticated active avatar states did not become ready")

        (output / "ready-health.json").write_text(json.dumps(status, indent=2) + "\n")
        time.sleep(args.warmup_seconds)
        process_baseline = {'server': sampler(server.pid), 'client': sampler(client.pid)}
        measurement_start = time.monotonic()
        process_samples = []
        marker.write_text(f"{time.time():.6f}\n")
        readiness_path = output / "readiness.csv"
        with readiness_path.open("w", newline="") as readiness_file:
            writer = csv.DictWriter(readiness_file, fieldnames=["unix_seconds", "players_online", "active_states"])
            writer.writeheader()
            deadline = time.monotonic() + args.window_seconds
            while time.monotonic() < deadline:
                status = health(args.health_port)
                with (output / "server.log").open(errors="replace") as log_file:
                    text = log_file.read()
                active_counts = re.findall(r"active_states=(\d+)", text)
                active_count = int(active_counts[-1]) if active_counts else -1
                player_count = population(status, args.server_kind, (output / 'client.log').read_text(errors='replace'))
                sample = {'elapsed_seconds': time.monotonic() - measurement_start}
                for label, process in [('server', server), ('client', client)]:
                    counters = sampler(process.pid)
                    sample[label + '_cpu_seconds'] = counters['cpu_seconds'] - process_baseline[label]['cpu_seconds']
                    sample[label + '_rss_bytes'] = counters['rss_bytes']
                process_samples.append(sample)
                writer.writerow({"unix_seconds": time.time(), "players_online": player_count, "active_states": active_count})
                readiness_file.flush()
                if player_count != args.clients or (args.server_kind == 'rust' and active_count != args.clients):
                    raise RuntimeError(f"peer readiness dropped during measurement: {status}, active_states={active_count}")
                if client.poll() is not None:
                    raise RuntimeError(f"client exited during measurement with status {client.returncode}")
                time.sleep(min(1, max(0, deadline - time.monotonic())))

        sample = {'elapsed_seconds': time.monotonic() - measurement_start}
        for label, process in [('server', server), ('client', client)]:
            counters = sampler(process.pid)
            sample[label + '_cpu_seconds'] = counters['cpu_seconds'] - process_baseline[label]['cpu_seconds']
            sample[label + '_rss_bytes'] = counters['rss_bytes']
        process_samples.append(sample)
        with (output / 'samples.csv').open('w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=list(sample)); writer.writeheader(); writer.writerows(process_samples)
        # Let the client's applied-gap observer finish its full interval before shutdown.
        time.sleep(2)
        client.send_signal(signal.SIGINT)
        client.wait(timeout=30)
        server.send_signal(signal.SIGINT)
        server.wait(timeout=30)

        workload = {
            "server_kind": args.server_kind, "server_metadata": server_metadata,
            "client_exit_code": client.returncode, "server_exit_code": server.returncode,
            "window_seconds": args.window_seconds,
            "additional_avatar_bytes": args.additional_avatar_bytes,
            "clients": args.clients, "movement_interval_ms": 20, "jitter_percent": 0,
            "unity_frame_accumulator_fps": 60, "layout": "colocated, zero positional drift",
            "pose": "valid synthetic deterministic root/body rotation channels; not captured Unity pose",
            "voice": False, "p2p": False, "server_profiling": False,
            "expected_observer_peers": args.clients - 1,
            "server_binary_sha256": sha256(server_bin), "client_binary_sha256": sha256(client_bin),
            "server_config_fixture_sha256": sha256(FIXTURES / "avatar-1500-server.xml"),
            "client_config_fixture_sha256": sha256(FIXTURES / "avatar-1500-client.xml"),
            "server_config_local_copy_sha256": sha256(server_config),
            "cpu_affinity": {"server": args.server_cpus, "client": args.client_cpus},
        }
        (output / "workload.json").write_text(json.dumps(workload, indent=2) + "\n")
        summary = summarize_capture(output, args.clients, args.window_seconds, process_samples, client.returncode, server.returncode)
        (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary, indent=2))
        return 0 if summary['valid'] else 1
    except Exception as error:
        summary = {'valid': False, 'errors': [str(error)]}
        (output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
        print(json.dumps(summary, indent=2))
        return 1
    finally:
        stop_process(client)
        stop_process(server)
        if client_log:
            client_log.close()
        server_log.close()
        shutil.rmtree(base_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
