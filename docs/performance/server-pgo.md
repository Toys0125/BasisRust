# Qualified server PGO builds

PGO is an opt-in build for native `x86_64-unknown-linux-gnu`. Ordinary Cargo,
Docker and cross-platform release builds retain their existing defaults.
Profiles from diagnostic runs are never used by this supported workflow.

The pipeline follows the [rustc PGO workflow](https://doc.rust-lang.org/rustc/profile-guided-optimization.html):
build an instrumented server, exercise it, merge fresh profiles with the matching
LLVM tool, then rebuild with profile use and evaluate against an ordinary release.
It pins Rust 1.99.0, uses locked dependencies, an explicit target, absolute profile
paths and isolated output trees. Source, tools, fixtures, flags and executable
hashes are recorded and checked. Generation and use share release settings.

## Local qualification

Requires Python 3.12+, Linux x64 GNU, Rust 1.99.0 and `taskset`. Commit source,
benchmark and fixture changes first. Use a fresh output path without whitespace.
Choose disjoint CPU sets available on the host and keep the host idle/on external
power throughout the experiment.

```bash
rustup component add llvm-tools-preview --toolchain 1.99.0
python3 -B scripts/perf/server_pgo.py \
  --output captures/server-pgo-final \
  --clients 750 --minimum-clients 250 \
  --server-cpus 2-9 --client-cpus 10-15
```

Defaults: two fresh instrumented training processes, followed by eight fresh
ABBA/BAAB evaluation processes (four per arm). Both arms use the same Rust client,
45-second warmup and 60-second measured window: dense avatar input every 20 ms,
Unity-policy 60 FPS poses, 128 bytes of extra avatar data, 128-byte Scene payloads
at 5 Hz, voice off and CPU-only server processing. Server/client workers are fixed
at four. Scene batching is 3 ms, the peer queue quota is 1024 logical messages,
and generic incoming upload is capped at 128 KiB/s per peer before fanout, with
two seconds of burst credit. Images use their separate existing governor/cache.
This workload sends no images, so it does not qualify photo sharing capacity.

All existing quality/provenance gates remain in force, including cumulative
zero retransmits, full population/sender coverage, sender progress, observer
continuity and Scene receipt >=95%. Native Scene quota/errors/stale recipients,
upload rejections and image drop/cache work must also remain zero. Missing
telemetry, resets or changed provenance abort qualification. Setup retransmits
remain failures even if the measured-window delta is zero.

If training or either evaluation arm fails quality, retain that attempt and
retry at a lower fixed population with new, separate training profiles. The
750-client schedule is 750 -> 375 -> 250. Failed training is never merged.
A lower-count pass qualifies only that count; it cannot certify a failed higher
count. Every attempted run and failure appears in `PGO-VALIDATION.md`.
If no count passes, exit nonzero and produce no distributable.

A fully passing attempt produces `dist/BasisRust-server-linux-x64-pgo.tar.gz`,
containing the PGO server, tracked default `Config.xml`, Markdown validation
summary and checksums. Raw JSON and profiles stay in the local capture directory
and are excluded from the package and PR diff. Run the extracted server with
`./basis-server-console --base-dir . --config Config.xml` after configuring it
for your deployment. Qualification verifies behavior;
read the A/B measurements before choosing PGO for deployment. It does not require
or imply that PGO improved every metric.

## CI and artifact workflow

All actions in `.github/workflows/server-pgo.yml` are pinned to full commit
hashes. The workflow runs a four-client end-to-end smoke check on
relevant PRs. It tests instrumentation, training, merge, use and strict A/B.
Smoke mode never packages or establishes workload capacity.

For a full build, manually dispatch **Server PGO** with an initial and minimum
client count. Register a dedicated Linux x64 runner with an up-to-date GitHub Actions runner
that supports Node 24, labelled `basis-pgo`, with at
least 16 logical CPUs (CPU IDs 2-15 available), Python 3.12+ and `taskset`.
Do not share that runner with competing jobs during qualification. Dispatch
uploads the qualified package and Markdown summary; failures retain the summary
without publishing a server artifact. Each dispatch uses a fresh output directory.

Profiles are tied to the frozen source, compiler and target. Do not reuse them
for ARM, musl, Windows, changed code, changed compiler or a different workload.
The historical 750-client diagnostic profile is intentionally ineligible.
Loopback, one observer and a small number of processes limit capacity and
regression conclusions. See [all Rust results](rust-performance-validation.md).
