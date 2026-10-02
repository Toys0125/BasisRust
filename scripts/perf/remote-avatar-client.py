#!/usr/bin/env python3
"""Run one Linux load-generator process over SSH and retain CPU/NIC counters."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import threading
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if not command or args.output.exists():
        parser.error("command required and output must not exist")
    args.output.mkdir(parents=True)
    metadata = {"platform": platform.platform(), "hostname": platform.node(),
                "logical_processors": os.cpu_count(), "command": command,
                "client_binary_sha256": hashlib.sha256(Path(command[0]).read_bytes()).hexdigest(),
                "started_unix_seconds": time.time()}
    if "--config" in command:
        fixture = Path(command[command.index("--config") + 1])
        metadata["client_config_fixture_sha256"] = hashlib.sha256(fixture.read_bytes()).hexdigest()
    process = subprocess.Popen(command, stdin=subprocess.PIPE)
    metadata["client_pid"] = process.pid
    (args.output / "client-process.json").write_text(json.dumps(metadata, indent=2) + "\n")

    def relay():
        try:
            while True:
                data = os.read(sys.stdin.fileno(), 4096)
                if not data:
                    break
                process.stdin.write(data)
                process.stdin.flush()
        except (BrokenPipeError, OSError):
            pass
        finally:
            # SSH EOF must not leave the load process behind.
            if process.poll() is None:
                try:
                    process.stdin.write(b"quit 100 0\n")
                    process.stdin.flush()
                except (BrokenPipeError, OSError):
                    pass
                try:
                    process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    process.terminate()

    threading.Thread(target=relay, daemon=True).start()
    deadline = time.monotonic() + 600
    try:
        with (args.output / "linux-metrics.jsonl").open("w") as metrics:
            while process.poll() is None:
                if time.monotonic() > deadline:
                    raise RuntimeError("remote client exceeded its ten-minute lifetime")
                row = {"unix_seconds": time.time(), "interfaces": {}}
                marker = args.output / "observe-start.marker"
                if marker.exists():
                    row["measurement_elapsed_seconds"] = time.time() - marker.stat().st_mtime
                try:
                    fields = Path(f"/proc/{process.pid}/stat").read_text().rsplit(")", 1)[1].split()
                    ticks = os.sysconf("SC_CLK_TCK")
                    row.update(user_cpu_seconds=int(fields[11]) / ticks,
                               kernel_cpu_seconds=int(fields[12]) / ticks,
                               rss_bytes=int(fields[21]) * os.sysconf("SC_PAGE_SIZE"))
                    for interface in Path("/sys/class/net").iterdir():
                        row["interfaces"][interface.name] = {
                            key: int((interface / "statistics" / key).read_text())
                            for key in ("rx_bytes", "tx_bytes", "rx_packets", "tx_packets", "rx_dropped", "tx_dropped", "rx_errors", "tx_errors", "rx_missed_errors")}
                except (FileNotFoundError, ProcessLookupError):
                    pass
                metrics.write(json.dumps(row) + "\n")
                metrics.flush()
                time.sleep(2)
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        metadata.update(exit_code=process.returncode, finished_unix_seconds=time.time())
        (args.output / "client-process.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return process.returncode


if __name__ == "__main__":
    sys.exit(main())
