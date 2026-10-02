#!/usr/bin/env python3
"""Capture Windows WPR CPU stacks during a warmed-up avatar workload.

Windows displays a UAC prompt for the scoped capture helper. The helper owns a
unique WPR instance, waits for workload readiness, and stops after 30 seconds.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import ipaddress
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]


def ps_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def read_status(path):
    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, ValueError):
        return {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--clients", type=int, default=1000)
    parser.add_argument("--capture-seconds", type=int, default=30)
    parser.add_argument("--server", type=Path, default=ROOT / "BasisRustServer/target/release/basis-server-console.exe")
    parser.add_argument("--client", type=Path, default=ROOT / "BasisRustClient/target/release/basis-rust-client.exe")
    parser.add_argument("--server-kind", choices=("rust", "csharp"), default="rust")
    parser.add_argument("--server-config", type=Path, default=ROOT / "docs/performance/fixtures/avatar-1500-server.xml")
    parser.add_argument("--warmup-seconds", type=int, default=15)
    parser.add_argument("--no-server-avatar-diagnostics", action="store_true")
    parser.add_argument("--include-clr", action="store_true", help="capture CLR loader/JIT metadata and rundown for managed frame names")
    parser.add_argument("--remote-ssh")
    parser.add_argument("--remote-root")
    parser.add_argument("--remote-output")
    parser.add_argument("--server-ip")
    parser.add_argument("--port", type=int, default=0)
    args = parser.parse_args()
    if args.server_kind == "csharp" and args.remote_ssh:
        parser.error("C# native comparison supports local clients only")
    if os.name != "nt":
        parser.error("native WPR capture requires Windows")
    if args.clients < 2 or not 5 <= args.capture_seconds <= 45:
        parser.error("clients must be >=2 and capture seconds between 5 and 45")
    if args.remote_ssh:
        if not all((args.remote_root, args.remote_output, args.server_ip)):
            parser.error("remote mode requires remote root/output and server IP")
        remote_address = args.remote_ssh.rsplit("@", 1)[-1]
        ipaddress.IPv4Address(remote_address)
        ipaddress.IPv4Address(args.server_ip)
        if not args.port:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
                probe.bind((args.server_ip, 0))
                args.port = probe.getsockname()[1]
    output = args.output.resolve()
    if output.exists():
        parser.error("output directory already exists")
    output.mkdir(parents=True)
    rtk = shutil.which("rtk")
    if not rtk:
        parser.error("rtk is required on PATH")
    symbols = output / "symbols"
    symbols.mkdir()
    hashes = {}
    if args.server_kind == "csharp":
        shutil.copytree(args.server.resolve().parent, symbols, dirs_exist_ok=True)
        for source in symbols.rglob("*"):
            if source.is_file():
                hashes[str(source.relative_to(symbols))] = hashlib.sha256(source.read_bytes()).hexdigest()
    executables = [(args.client, "basis-rust-client")]
    if args.server_kind == "rust":
        executables.append((args.server, "basis-server-console"))
    for executable, stem in executables:
        for name in [stem + ".exe", stem.replace("-", "_") + ".pdb"]:
            source = executable.resolve() if name.endswith(".exe") else executable.resolve().with_name(name)
            if not source.is_file():
                parser.error(f"release binary/symbol file missing: {source}")
            target = symbols / name
            shutil.copy2(source, target)
            hashes[name] = hashlib.sha256(target.read_bytes()).hexdigest()
    (output / "symbol-manifest.json").write_text(json.dumps(hashes, indent=2) + "\n")
    workload = output / "workload"
    helper = ROOT / "scripts/perf/capture-windows-native.ps1"
    helper_args = ["-NoProfile", "-File", str(helper), "-OutputDirectory", str(output),
                   "-TriggerFile", str(workload / "observe-start.marker"), "-RtkPath", rtk,
                   "-CaptureSeconds", str(args.capture_seconds)]
    if args.include_clr:
        helper_args += ["-IncludeClr"]
    if args.remote_ssh:
        helper_args += ["-FirewallRemoteAddress", remote_address,
                        "-FirewallLocalAddress", args.server_ip, "-FirewallUdpPort", str(args.port)]
    # Each argument is quoted for Windows' native command-line parser. The
    # resulting string is separately quoted as a PowerShell literal.
    argline = subprocess.list2cmdline(helper_args)
    command = ("Start-Process -FilePath 'powershell.exe' -Verb RunAs -WindowStyle Hidden "
               "-ErrorAction Stop -ArgumentList " + ps_literal(argline))
    (output / "elevated-helper-command.txt").write_text(command + "\n")
    print("Requesting Windows UAC consent for the native CPU capture helper.", flush=True)
    launch = subprocess.Popen([rtk, "proxy", "powershell", "-NoProfile", "-Command", command],
                              stdout=(output / "helper-launch.log").open("w"), stderr=subprocess.STDOUT)
    state_path = output / "native-status.json"
    deadline = time.monotonic() + 180
    try:
        while time.monotonic() < deadline:
            state = read_status(state_path)
            if state.get("phase") == "armed":
                break
            if state.get("phase") == "failed":
                raise RuntimeError(state.get("error", "native capture helper failed"))
            if launch.poll() not in (None, 0):
                raise RuntimeError("Windows did not launch the elevated helper; see helper-launch.log")
            time.sleep(0.25)
        else:
            raise RuntimeError("Native helper did not become ready; no load processes were started.")
    except BaseException:
        (output / "native-abort.marker").touch()
        raise
    print(f"Native capture helper is armed; starting the {args.clients}-client workload.", flush=True)
    workload_command = [rtk, "proxy", sys.executable, str(ROOT / "scripts/perf/run-windows-avatar-workload.py"),
                        "--output", str(workload), "--clients", str(args.clients),
                        "--workers", "4", "--warmup-seconds", str(args.warmup_seconds), "--window-seconds", "60",
                        "--server-kind", args.server_kind, "--server-config", str(args.server_config.resolve()),
                        "--server", str(symbols / (args.server.name if args.server_kind == "csharp" else "basis-server-console.exe")),
                        "--client", str(symbols / "basis-rust-client.exe")]
    if args.no_server_avatar_diagnostics:
        workload_command += ["--no-server-avatar-diagnostics"]
    if args.remote_ssh:
        workload_command += ["--remote-ssh", args.remote_ssh, "--remote-root", args.remote_root,
                             "--remote-output", args.remote_output, "--server-ip", args.server_ip,
                             "--port", str(args.port)]
    elif args.port:
        workload_command += ["--port", str(args.port)]
    if args.server_ip and not args.remote_ssh:
        workload_command += ["--server-ip", args.server_ip]
    try:
        result = subprocess.run(workload_command, cwd=ROOT)
    except BaseException:
        (output / "native-abort.marker").touch()
        raise
    finally:
        (output / "workload-done.marker").touch()
    if result.returncode != 0:
        (output / "native-abort.marker").touch()
        raise RuntimeError(f"Workload exited with status {result.returncode}")
    deadline = time.monotonic() + 300
    while time.monotonic() < deadline:
        state = read_status(state_path)
        if state.get("phase") == "failed" or (state.get("phase") == "complete" and
                (not args.remote_ssh or state.get("firewallRemoved"))):
            break
        time.sleep(0.5)
    if state.get("phase") != "complete" or (args.remote_ssh and not state.get("firewallRemoved")):
        raise RuntimeError(f"Native capture did not finish successfully: {state}")
    print(f"Native trace complete: {output / 'cpu-stacks.etl'}", flush=True)


if __name__ == "__main__":
    main()
