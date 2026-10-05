#!/usr/bin/env python3
"""Prepare reproducible Opus clips and run a local Windows voice/avatar suite."""

import argparse
import concurrent.futures
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import time

ROOT = pathlib.Path(__file__).resolve().parents[2]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ogg_packets(path):
    data = path.read_bytes()
    offset, pending, packets = 0, bytearray(), []
    while offset < len(data):
        if data[offset:offset + 4] != b"OggS" or offset + 27 > len(data):
            raise ValueError(f"malformed Ogg page in {path}")
        segments = data[offset + 26]
        lacing = data[offset + 27:offset + 27 + segments]
        if len(lacing) != segments:
            raise ValueError(f"truncated Ogg lacing in {path}")
        position = offset + 27 + segments
        for length in lacing:
            chunk = data[position:position + length]
            if len(chunk) != length:
                raise ValueError(f"truncated Ogg packet in {path}")
            pending.extend(chunk)
            position += length
            if length < 255:
                packet = bytes(pending)
                pending.clear()
                if not packet.startswith((b"OpusHead", b"OpusTags")):
                    packets.append(packet)
        offset = position
    if pending:
        raise ValueError(f"incomplete Ogg packet in {path}")
    return packets


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audio-folder", type=pathlib.Path, required=True)
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--clients", type=int, default=1000)
    parser.add_argument("--warmup-seconds", type=int, default=45)
    parser.add_argument("--baseline-seconds", type=int, default=60)
    parser.add_argument("--normal-seconds", type=int, default=120)
    parser.add_argument("--stress-seconds", type=int, default=180)
    args = parser.parse_args()
    output = args.output.resolve()
    source = args.audio_folder.resolve()
    if output.exists():
        parser.error(f"output directory already exists: {output}")
    files = sorted(path for path in source.iterdir() if path.suffix.lower() in (".opus", ".ogg") and path.is_file())
    if not files:
        parser.error(f"no Ogg Opus audio in {source}")
    if args.clients < 2 or args.warmup_seconds < 0 or min(args.baseline_seconds, args.normal_seconds, args.stress_seconds) < 1:
        parser.error("clients >=2, warmup >=0, and all measurement windows >=1 are required")
    ffmpeg, ffprobe, rtk = (shutil.which(name) for name in ("ffmpeg", "ffprobe", "rtk"))
    if not all((ffmpeg, ffprobe, rtk)):
        parser.error("ffmpeg, ffprobe and rtk must be on PATH")
    output.mkdir(parents=True)
    encoded = output / "audio-20ms-mono"
    encoded.mkdir()
    binaries = output / "binaries"
    binaries.mkdir()
    for name, path in (("server.exe", ROOT / "BasisRustServer/target/release/basis-server-console.exe"),
                       ("client.exe", ROOT / "BasisRustClient/target/release/basis-rust-client.exe")):
        shutil.copy2(path, binaries / name)

    def encode(item):
        index, path = item
        destination = encoded / f"{index:02d}.opus"
        command = [rtk, "proxy", ffmpeg, "-hide_banner", "-loglevel", "error", "-nostdin", "-y",
                   "-i", str(path), "-vn", "-ac", "1", "-ar", "48000", "-c:a", "libopus",
                   "-threads", "1", "-b:a", "32000", "-application", "audio", "-frame_duration", "20", str(destination)]
        started = time.monotonic()
        subprocess.run(command, check=True, capture_output=True)
        probe = json.loads(subprocess.check_output([rtk, "proxy", ffprobe, "-v", "error", "-show_streams", "-show_format", "-of", "json", str(destination)], text=True))
        packets = ogg_packets(destination)
        result = {"source": str(path), "source_sha256": sha256(path), "encoded": str(destination),
                  "encoded_sha256": sha256(destination), "packets": len(packets),
                  "max_packet_bytes": max(map(len, packets)), "probe": probe,
                  "encoding_seconds": time.monotonic() - started, "command": command}
        print(f"Encoded {index + 1}/{len(files)}: {len(packets)} packets", flush=True)
        return result

    with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, os.cpu_count() or 1)) as pool:
        manifest = list(pool.map(encode, enumerate(files)))
    (output / "audio-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    (output / "source-changes.patch").write_bytes(subprocess.check_output([rtk, "proxy", "git", "diff", "--", "BasisRustClient/src/main.rs", "scripts/perf/run-windows-avatar-workload.py"], cwd=ROOT))
    for path in (ROOT / "BasisRustClient/src/voice_diagnostics.rs", pathlib.Path(__file__), ROOT / "scripts/perf/analyze-windows-voice.py"):
        shutil.copy2(path, output / path.name)
    (output / "source-revision.txt").write_text(subprocess.check_output([rtk, "proxy", "git", "rev-parse", "HEAD"], cwd=ROOT, text=True), encoding="utf-8")
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(("BASIS_", "EnableBSR", "HealthIncludeBSR", "EnableCompute")) or key in ("RAYON_NUM_THREADS", "TOKIO_WORKER_THREADS"):
            env.pop(key)
    runs = [("preflight", min(4, args.clients), 100, 2, 5),
            ("baseline-1000", args.clients, 0, args.warmup_seconds, args.baseline_seconds),
            ("voice-10pct", args.clients, 10, args.warmup_seconds, args.normal_seconds),
            ("voice-100pct", args.clients, 100, args.warmup_seconds, args.stress_seconds)]
    results = []
    for name, clients, percent, warmup, window in runs:
        command = [rtk, "proxy", "python", str(ROOT / "scripts/perf/run-windows-avatar-workload.py"),
                   "--server", str(binaries / "server.exe"), "--client", str(binaries / "client.exe"),
                   "--server-config", str(ROOT / "docs/performance/fixtures/avatar-cpu-only-server.xml"),
                   "--clients", str(clients), "--workers", "4", "--warmup-seconds", str(warmup),
                   "--window-seconds", str(window), "--no-server-avatar-diagnostics", "--output", str(output / name)]
        if percent:
            command.extend(["--voice-audio-folder", str(encoded), "--voice-speaker-percent", str(percent), "--no-voice-reencode"])
        print(f"Starting {name}: {clients} clients, {percent}% speaking, {window}s measurement", flush=True)
        with (output / f"{name}-harness.log").open("w", encoding="utf-8") as log:
            process = subprocess.run(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
        if process.returncode:
            raise RuntimeError(f"{name} failed ({process.returncode}); see {log.name}")
        analyze = [rtk, "proxy", "python", str(ROOT / "scripts/perf/analyze-windows-voice.py"), str(output / name), "--audio-folder", str(encoded)]
        subprocess.run(analyze, cwd=ROOT, check=True)
        result = json.loads((output / name / "validated-summary.json").read_text())
        results.append(result)
        (output / "suite-summary.json").write_text(json.dumps(results, indent=2) + "\n", encoding="utf-8")
        if name == "preflight" and not result["checks"]["voice_active"]:
            raise RuntimeError("voice preflight did not deliver audio")
    print(f"Suite complete: {output}", flush=True)


if __name__ == "__main__":
    main()
