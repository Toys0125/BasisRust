#!/usr/bin/env python3
"""Freeze two transport binaries, then run sequential, CPU-pinned ABBA blocks.

The current benchmark files are overlaid identically on both source revisions.
Linux: BASIS_BRANCH_PMC=1 collects per-thread user-mode perf groups in each pass.
No working-tree production source is changed by the runner.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import tarfile
import time


def command(args, **kwargs):
    prefix = ["rtk", "proxy"] if shutil.which("rtk") else []
    return subprocess.run(prefix + list(map(str, args)), check=True, **kwargs)


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--git-dir", type=Path, help="translated common Git directory when reading a Windows worktree from WSL")
    parser.add_argument("--reuse", type=Path, help="rerun already frozen binaries from a prior output directory")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--baseline", default="1e45b3b")
    parser.add_argument("--candidate", default="3606a9b")
    parser.add_argument("--blocks", type=int, default=4)
    parser.add_argument("--cpu", type=int, default=2)
    parser.add_argument("--pmc", action="store_true")
    args = parser.parse_args()
    if args.blocks < 1 or args.cpu < 0 or (args.pmc and os.name == "nt"):
        parser.error("positive blocks/nonnegative CPU required; PMC mode requires Linux")
    repo = args.repository.resolve()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    environment = os.environ.copy()
    environment["CARGO_INCREMENTAL"] = "0"
    environment["CARGO_BUILD_JOBS"] = "2"
    environment.pop("BASIS_BRANCH_PMC", None)
    if args.pmc:
        environment["BASIS_BRANCH_PMC"] = "1"
    if args.reuse:
        prior = args.reuse.resolve()
        metadata = json.loads((prior / "manifest.json").read_text())
        metadata.update(cpu=args.cpu, pmc=args.pmc, blocks=args.blocks, reused_build=prior.name)
        binaries = {}
        for variant in metadata["revisions"]:
            binary = prior / variant / ("bench.exe" if os.name == "nt" else "bench")
            if digest(binary) != metadata["binary_sha256"][variant]:
                raise RuntimeError(f"frozen binary hash mismatch: {binary}")
            shutil.copytree(prior / variant, output / variant)
            binaries[variant] = output / variant / binary.name
        run_series(args, output, environment, metadata, binaries, metadata["revisions"])
        return
    git = ["git", "-c", f"safe.directory={repo}", "-C", repo]
    if args.git_dir:
        git += [f"--git-dir={args.git_dir.resolve()}"]
    revisions = {name: command(git + ["rev-parse", ref], capture_output=True, text=True).stdout.strip()
                 for name, ref in (("baseline", args.baseline), ("candidate", args.candidate))}
    archive = output / "source.tar"
    command(git + ["archive", revisions["candidate"], "BasisRustServer", "-o", archive])
    source = output / "source"
    source.mkdir()
    with tarfile.open(archive) as contents:
        # The archive is produced from this repository; data filtering also
        # prevents symlink/path traversal when running on Python 3.12+.
        contents.extractall(source, filter="data")
    workspace = source / "BasisRustServer"
    crate = Path("BasisRustServer/crates/basis-transport/src")
    for name in ("bench_branches.rs", "bench_pmc.rs"):
        shutil.copy2(repo / crate / name, source / crate / name)
    bench_hashes = {name: digest(source / crate / name)
                   for name in ("bench_branches.rs", "bench_pmc.rs")}
    metadata = {"revisions": revisions, "benchmark_sha256": bench_hashes,
                "cpu": args.cpu, "pmc": args.pmc, "blocks": args.blocks,
                "rustc": command(["rustc", "-Vv"], capture_output=True, text=True).stdout,
                "kernel": command(["uname", "-a"], capture_output=True, text=True).stdout if os.name != "nt" else "Windows",
                "inherited_build_environment": {k: v for k, v in environment.items()
                    if k in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_BUILD_TARGET", "CARGO_INCREMENTAL", "CARGO_BUILD_JOBS")}}
    binaries = {}
    for variant, revision in revisions.items():
        library = command(git + ["show", f"{revision}:{crate.as_posix()}/lib.rs"], capture_output=True).stdout
        if b"mod bench_branches;" not in library:
            library = b"#[cfg(test)]\nmod bench_branches;\n" + library
        (source / crate / "lib.rs").write_bytes(library)
        variant_dir = output / variant
        variant_dir.mkdir()
        with (variant_dir / "build.log").open("w") as log:
            command(["cargo", "test", "--locked", "--release", "-p", "basis-transport", "--lib", "--no-run", "--message-format=json"],
                    cwd=workspace, env=environment, stdout=log, stderr=subprocess.STDOUT)
        artifacts = [json.loads(line) for line in (variant_dir / "build.log").read_text().splitlines() if line.startswith("{")]
        executable = next((item["executable"] for item in artifacts
                          if item.get("reason") == "compiler-artifact" and item.get("executable")
                          and item.get("target", {}).get("name") == "basis_transport"), None)
        if executable is None:
            raise RuntimeError(f"basis_transport test executable missing: {variant_dir / 'build.log'}")
        binary = variant_dir / ("bench.exe" if os.name == "nt" else "bench")
        shutil.copy2(executable, binary)
        binaries[variant] = binary
        metadata.setdefault("binary_sha256", {})[variant] = digest(binary)
        # Compile the exact enum/decoder separately to inspect the claimed jump
        # table change; this probe is not included in measured binaries.
        text = library.decode()
        enum = text[text.index("#[derive", text.index("pub enum PacketProperty") - 150):text.index("impl PacketProperty")]
        start = text.index("impl PacketProperty")
        end = text.index("#[derive", start)
        probe = enum + text[start:end] + '\n#[no_mangle]\npub extern "C" fn decode(x: u8) -> u8 { PacketProperty::from_byte(x).map_or(255, |v| v as u8) }\n'
        probe_path = variant_dir / "property_probe.rs"
        probe_path.write_text(probe)
        command(["rustc", "--crate-type=lib", "-O", "--emit=asm", probe_path, "-o", variant_dir / "property_probe.s"],
                stdout=subprocess.DEVNULL)
        print(f"built {variant}: {metadata['binary_sha256'][variant]}", flush=True)
    run_series(args, output, environment, metadata, binaries, revisions)


def run_series(args, output, environment, metadata, binaries, revisions):
    (output / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")
    rows = []
    for block in range(args.blocks):
        order = ("baseline", "candidate", "candidate", "baseline") if block % 2 == 0 else ("candidate", "baseline", "baseline", "candidate")
        for variant in order:
            number = len(rows) + 1
            log_path = output / f"run-{number:02d}-{variant}.log"
            argv = [str(binaries[variant]), "--ignored", "bench_branches::branch_miss_bench", "--exact", "--nocapture", "--test-threads=1"]
            if os.name != "nt":
                argv = ["taskset", "-c", str(args.cpu)] + argv
            start = time.time()
            with log_path.open("w") as log:
                process = subprocess.Popen(argv, env=environment, stdout=log, stderr=subprocess.STDOUT)
                if os.name == "nt":
                    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
                    kernel.SetProcessAffinityMask.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
                    if not kernel.SetProcessAffinityMask(int(process._handle), 1 << args.cpu):
                        process.terminate()
                        raise ctypes.WinError(ctypes.get_last_error())
                code = process.wait()
            text = log_path.read_text()
            if code or "1 passed" not in text:
                raise RuntimeError(f"benchmark failed: {log_path}, code={code}")
            timings = {name: float(value) for name, value in re.findall(r"^  (\w+): ([0-9.]+) ns/op", text, re.M)}
            if len(timings) != 6:
                raise RuntimeError(f"incomplete benchmark: {log_path}")
            outcomes = re.findall(r"^    (?:workload outcomes|built bytes total|datagrams merged total).*", text, re.M)
            if len(outcomes) != 6 or (rows and outcomes != rows[0]["workload_outcomes"]):
                raise RuntimeError(f"workload outcomes changed: {log_path}")
            samples = [{"phase": m[0], **{k: int(v) for k, v in zip(
                ("pass", "ns", "ops", "branches", "misses", "enabled_ns", "running_ns"), m[1:])}}
                for m in re.findall(r"sample (\w+) pass=(\d+) ns=(\d+) ops=(\d+) branches=(\d+) misses=(\d+) enabled_ns=(\d+) running_ns=(\d+)", text)]
            if args.pmc and (len(samples) != 54 or any(s["running_ns"] / s["enabled_ns"] < .999 for s in samples)):
                raise RuntimeError(f"missing/multiplexed counters: {log_path}")
            rows.append({"run": number, "block": block + 1, "variant": variant,
                         "start_unix": start, "duration_s": time.time() - start,
                         "timing_ns_per_op": timings, "samples": samples,
                         "workload_outcomes": outcomes})
            (output / "runs.json").write_text(json.dumps(rows, indent=2) + "\n")
            print(f"run {number}/{args.blocks * 4}: {variant} complete", flush=True)
    summary = {}
    for phase in rows[0]["timing_ns_per_op"]:
        entry = {}
        for variant in revisions:
            runs = [r for r in rows if r["variant"] == variant]
            values = [r["timing_ns_per_op"][phase] for r in runs]
            result = {"median_ns_per_op": statistics.median(values), "range_ns_per_op": [min(values), max(values)]}
            samples = [s for r in runs for s in r["samples"] if s["phase"] == phase]
            if samples:
                totals = {k: sum(s[k] for s in samples) for k in ("ops", "branches", "misses", "enabled_ns", "running_ns")}
                result.update(totals)
                result.update(branches_per_op=totals["branches"] / totals["ops"],
                              misses_per_op=totals["misses"] / totals["ops"],
                              miss_rate_percent=100 * totals["misses"] / totals["branches"])
            entry[variant] = result
        entry["timing_change_percent"] = 100 * (entry["candidate"]["median_ns_per_op"] / entry["baseline"]["median_ns_per_op"] - 1)
        entry["block_timing_change_percent"] = [100 * (statistics.mean(r["timing_ns_per_op"][phase] for r in rows if r["block"] == b and r["variant"] == "candidate") / statistics.mean(r["timing_ns_per_op"][phase] for r in rows if r["block"] == b and r["variant"] == "baseline") - 1) for b in range(1, args.blocks + 1)]
        summary[phase] = entry
    (output / "summary.json").write_text(json.dumps({"manifest": metadata, "summary": summary}, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
