#!/usr/bin/env python3
"""Build or run the frozen 1,000-player week-sync UDP integration test."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import shutil
import subprocess
import sys
from datetime import datetime, timezone


ROOT = pathlib.Path(__file__).resolve().parents[2]
SERVER = ROOT / "BasisRustServer"
TEST_NAME = "admin_runtime::wire_tests::week_sim::week_sync_1000_players"


def sha256(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def audio_inputs(path: pathlib.Path) -> list[pathlib.Path]:
    path = path.resolve(strict=True)
    if path.is_file():
        files = [path]
    elif path.is_dir():
        files = sorted(
            child.resolve() for child in path.iterdir()
            if child.is_file() and child.suffix.lower() in {".opus", ".ogg"}
        )
    else:
        raise ValueError(f"audio path is neither a file nor directory: {path}")
    if not files:
        raise ValueError(f"no .opus or .ogg audio files found at {path}")
    return files


def write_manifest(path: pathlib.Path, manifest: dict) -> None:
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audio", required=True, type=pathlib.Path, help="preencoded Ogg Opus file or directory")
    parser.add_argument("--peak", type=int, default=1000)
    parser.add_argument("--hours", type=int, default=168)
    parser.add_argument("--output", type=pathlib.Path, default=pathlib.Path("captures/week-simulation/full"))
    parser.add_argument("--binary", type=pathlib.Path, help="use an existing frozen test executable; skip the build")
    args = parser.parse_args()

    if not 4 <= args.peak <= 1000:
        parser.error("--peak must be between 4 and 1000")
    if not 1 <= args.hours <= 168:
        parser.error("--hours must be between 1 and 168")
    try:
        audio = audio_inputs(args.audio)
    except (OSError, ValueError) as error:
        parser.error(str(error))

    output = args.output if args.output.is_absolute() else ROOT / args.output
    if output.exists():
        parser.error(f"output directory already exists; refusing to overwrite evidence: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.mkdir()

    source_files = [
        SERVER / "Cargo.lock",
        SERVER / "crates/basis-server-core/Cargo.toml",
        SERVER / "crates/basis-server-core/src/lib.rs",
        SERVER / "crates/basis-server-core/src/realtime.rs",
        SERVER / "crates/basis-server-core/src/avatar_sync.rs",
        SERVER / "crates/basis-server-core/src/admin_runtime/wire_tests.rs",
        SERVER / "crates/basis-server-core/src/admin_runtime/wire_tests/week_sim.rs",
        SERVER / "crates/basis-server-core/src/admin_runtime/wire_tests/week_sim/array_sync_script.rs",
        SERVER / "crates/basis-server-core/src/admin_runtime/wire_tests/week_sim/lifecycle.rs",
        SERVER / "crates/basis-server-core/src/admin_runtime/wire_tests/week_sim/transport.rs",
        SERVER / "crates/basis-server-core/src/admin_runtime/wire_tests/week_sim/traffic.rs",
        SERVER / "crates/basis-protocol/src/avatar_delta.rs",
        SERVER / "crates/basis-transport/src/lib.rs",
        pathlib.Path(__file__).resolve(),
    ]
    git_head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
    )
    manifest = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "git_head": git_head.stdout.strip() if git_head.returncode == 0 else None,
        "git_head_error": git_head.stderr.strip() if git_head.returncode else None,
        "peak": args.peak,
        "hours": args.hours,
        "audio": [{"path": str(path), "sha256": sha256(path)} for path in audio],
        "source": [
            {"path": str(path.relative_to(ROOT)), "sha256": sha256(path)}
            for path in source_files
        ],
        "build": None,
        "run": None,
        "result_validation": None,
    }
    manifest_path = output / "manifest.json"
    write_manifest(manifest_path, manifest)

    if args.binary:
        binary_input = args.binary.resolve(strict=True)
        if not binary_input.is_file():
            parser.error(f"--binary is not a file: {binary_input}")
        build_command = None
        executable = output / "basis-server-core-test"
        shutil.copy2(binary_input, executable)
        manifest["build"] = {
            "command": None,
            "binary_input": str(binary_input),
            "frozen_binary": executable.name,
            "binary_sha256": sha256(executable),
            "build_skipped": True,
            "source_provenance_note": (
                "Current workspace source hashes are recorded for context only; they do not prove "
                "that these sources produced the externally supplied binary."
            ),
        }
        write_manifest(manifest_path, manifest)
    else:
        build_command = [
            "cargo", "test", "--locked", "--release", "-p", "basis-server-core",
            "--lib", "--no-run", "--message-format=json",
        ]
        build_env = os.environ.copy()
        build_env["CARGO_PROFILE_RELEASE_DEBUG"] = "1"
        build_proc = subprocess.run(
            build_command, cwd=SERVER, env=build_env, text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False,
        )
        (output / "build.jsonl").write_text(build_proc.stdout, encoding="utf-8")
        (output / "build.stderr.log").write_text(build_proc.stderr, encoding="utf-8")
        manifest["build"] = {
            "command": build_command,
            "cwd": str(SERVER),
            "CARGO_PROFILE_RELEASE_DEBUG": "1",
            "exit_code": build_proc.returncode,
        }
        write_manifest(manifest_path, manifest)
        if build_proc.returncode:
            print(f"build failed; evidence saved in {output}", file=sys.stderr)
            return build_proc.returncode

        artifacts = []
        for line in build_proc.stdout.splitlines():
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            target = event.get("target", {})
            profile = event.get("profile", {})
            if (event.get("reason") == "compiler-artifact"
                    and target.get("name") == "basis_server_core"
                    and profile.get("test") is True
                    and event.get("executable")):
                artifacts.append(pathlib.Path(event["executable"]))
        if len(artifacts) != 1:
            manifest["build"]["artifact_candidates"] = [str(path) for path in artifacts]
            manifest["build"]["error"] = "expected exactly one basis_server_core test executable"
            write_manifest(manifest_path, manifest)
            print(f"could not identify one test executable; evidence saved in {output}", file=sys.stderr)
            return 2
        executable = output / "basis-server-core-test"
        shutil.copy2(artifacts[0], executable)
        manifest["build"]["artifact"] = str(artifacts[0])
        manifest["build"]["frozen_binary"] = executable.name
        manifest["build"]["binary_sha256"] = sha256(executable)
        write_manifest(manifest_path, manifest)

    result_path = output / "result.json"
    run_command = [
        str(executable), "--exact", TEST_NAME, "--ignored", "--nocapture",
        "--test-threads=1",
    ]
    run_env = os.environ.copy()
    run_env.update({
        "BASIS_WEEK_PEAK_PLAYERS": str(args.peak),
        "BASIS_WEEK_HOURS": str(args.hours),
        "BASIS_WEEK_OUTPUT": str(result_path),
        "BASIS_WEEK_OPUS_FILE": str(audio[0].parent if args.audio.resolve().is_dir() else audio[0]),
        "RAYON_NUM_THREADS": "16",
        "RUST_BACKTRACE": "1",
    })
    manifest["run"] = {
        "command": run_command,
        "cwd": str(ROOT),
        "environment": {key: run_env[key] for key in (
            "BASIS_WEEK_PEAK_PLAYERS", "BASIS_WEEK_HOURS", "BASIS_WEEK_OUTPUT",
            "BASIS_WEEK_OPUS_FILE", "RAYON_NUM_THREADS", "RUST_BACKTRACE",
        )},
        "exit_code": None,
        "status": "running",
        "log": "run.log",
    }
    write_manifest(manifest_path, manifest)
    with (output / "run.log").open("w", encoding="utf-8", buffering=1) as run_log:
        run_proc = subprocess.run(
            run_command, cwd=ROOT, env=run_env, text=True,
            stdout=run_log, stderr=subprocess.STDOUT, check=False,
        )
    manifest["run"] = {
        "command": run_command,
        "cwd": str(ROOT),
        "environment": {key: run_env[key] for key in (
            "BASIS_WEEK_PEAK_PLAYERS", "BASIS_WEEK_HOURS", "BASIS_WEEK_OUTPUT",
            "BASIS_WEEK_OPUS_FILE", "RAYON_NUM_THREADS", "RUST_BACKTRACE",
        )},
        "exit_code": run_proc.returncode,
        "status": "finished",
        "log": "run.log",
    }
    write_manifest(manifest_path, manifest)

    if run_proc.returncode:
        manifest["result_validation"] = {"valid": False, "error": "test process exited nonzero"}
        write_manifest(manifest_path, manifest)
        print(f"week simulation failed; evidence saved in {output}", file=sys.stderr)
        return run_proc.returncode
    try:
        result = json.loads(result_path.read_text(encoding="utf-8"))
        checks = {
            "valid": result.get("valid") is True,
            "requested_peak": result.get("requested_peak") == args.peak,
            "logical_hours": result.get("logical_hours") == args.hours,
            "logical_seconds": result.get("logical_seconds") == args.hours * 3600,
            "observed_peak": args.hours < 19 or result.get("observed_peak") == args.peak,
            "audio_files": len(result.get("audio_files", [])) == len(audio),
            "protocol_errors": result.get("protocol_errors") == 0,
        }
        if not all(checks.values()):
            raise ValueError(f"result did not meet expected values: {checks}")
        manifest["result_validation"] = {"valid": True, "checks": checks}
    except (OSError, json.JSONDecodeError, ValueError) as error:
        manifest["result_validation"] = {"valid": False, "error": str(error)}
        write_manifest(manifest_path, manifest)
        print(f"result validation failed: {error}; evidence saved in {output}", file=sys.stderr)
        return 2
    write_manifest(manifest_path, manifest)
    print(f"week simulation passed; evidence saved in {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
