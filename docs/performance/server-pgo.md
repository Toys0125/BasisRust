# Automatic qualified server PGO builds

Automatic server releases use PGO for every shipping target. The main-branch
build workflow and release workflow run fresh, target-native training and strict
ordinary/PGO A/B validation on GitHub-hosted runners. There is no self-hosted
runner requirement and no ordinary-server fallback. If a target cannot qualify,
its artifact is not uploaded and the release attachment/Docker publication chain
cannot proceed. Profiles from diagnostic runs are never used.

Supported server targets are Windows x64 MSVC, macOS ARM64, Linux GNU x64/ARM64
and Linux musl x64/ARM64. Each target trains its own profiles on a matching native
runner; GNU and musl profiles stay separate. Clients remain ordinary releases.
Plain local Cargo commands also remain ordinary builds.

The pipeline follows the [rustc PGO workflow](https://doc.rust-lang.org/rustc/profile-guided-optimization.html):
build an instrumented server, exercise it, merge fresh profiles with the matching
LLVM tool, then rebuild with profile use and evaluate against an ordinary release.
It pins Rust 1.99.0, uses locked dependencies, an explicit target, absolute profile
paths and isolated output trees. Source, tools, fixtures, flags and executable
hashes are recorded and checked. Generation and use share release settings.
The server locks `cc` 1.8.0, which prevents Rust PGO flags being forwarded to
foreign C/C++ compilers. This avoids mixed profiling runtimes when Apple Clang's
LLVM differs from rustc's. Rust server code is trained; native C dependencies
retain ordinary compilation consistently across all three server builds.

## Benchmark validation feature

Scene relay and upload-budget counters, snapshot APIs and their detailed status
reporting are compiled only with `benchmark-validation` (or in core unit tests).
Normal server builds omit `sceneBatch` and `uploadBudget` from extended health
JSON instead of reporting fabricated zero counters. Per-peer upload admission,
Scene quotas, bounded queues and connection checks remain active in every build.
Other existing server health metrics remain available.

```bash
cargo build --release --manifest-path BasisRustServer/Cargo.toml \
  -p basis-server-console --features benchmark-validation
```

The PGO driver enables this feature identically for ordinary, instrumented and
PGO-use servers and verifies their Cargo feature fingerprints. Qualified PGO
packages therefore retain validation telemetry. Their results do not measure
the counter-free default build; remeasure after changing features or source.

## Local qualification

Requires Python 3.12+, a matching native Linux/Windows/macOS runner, Rust 1.99.0
and matching LLVM tools. Linux musl also requires `musl-tools` and the musl Rust
standard-library target. Linux CPU affinity requires `taskset`; Windows uses
native process-group/Job Object controls. macOS uses native BSD `ps` CPU/RSS
counters and rejects explicit CPU sets. Commit source,
benchmark and fixture changes first. Use a fresh output path without whitespace.
Choose disjoint CPU sets available on the host and keep the host idle/on external
power throughout the experiment.

```bash
rustup component add llvm-tools-preview --toolchain 1.99.0
python3 -B scripts/perf/server_pgo.py \
  --output captures/server-pgo-final \
  --target x86_64-unknown-linux-gnu \
  --clients 300 --minimum-clients 100 \
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
default schedule is 300 -> 150 -> 100; counts below 100 are rejected. Failed training is never merged.
A lower-count pass qualifies only that count; it cannot certify a failed higher
count. Every attempted run and failure appears in `PGO-VALIDATION.md`.
If no count passes, exit nonzero and produce no distributable.

A fully passing attempt produces `dist/BasisRust-server-<target>-pgo.tar.gz`,
containing the PGO server (`.exe` on Windows), tracked default `Config.xml`, Markdown validation
summary and checksums. Raw JSON and profiles stay in the local capture directory
and are excluded from the package and PR diff. Run the extracted server with
`./basis-server-console --base-dir . --config Config.xml` after configuring it
for your deployment. Qualification verifies behavior;
read the A/B measurements before choosing PGO for deployment. It does not require
or imply that PGO improved every metric.

The driver also exports only the validated server, shared client and Markdown
report under `release/`, with checksums, for the standard release packager.
`build.yml` keeps the existing `BasisRust-<platform>` archive names and
`server/` / `client/` layout, adds the PGO report, and hashes final package
contents after pretty-printing configuration files. musl remains server-only.
The distributed PGO server retains `benchmark-validation` telemetry; these
results do not qualify a counter-free binary. Platform-native server shutdown
must exit zero; forced signal termination cannot qualify or train a release.

## CI and artifact workflow

PGO workflow/build actions are pinned to full commit hashes. PR changes to the
PGO pipeline/build workflows/fixtures run the standard six-target build matrix
as a release preview. It uses the full qualification and package path; artifacts
are retained in the Actions run, without attaching them to a published release
or publishing Docker images. Source-only PR checks retain the existing Rust
correctness workflows. A local `--smoke` still uses short windows/four evaluations
and never packages; it is not used to qualify automatic release artifacts.

`build.yml` runs automatically on pushes to main, by manual dispatch, or when
called by `release.yml` for a published release tag. Every server matrix leg
runs full qualification: two training processes and eight evaluations with
45-second warmups and 60-second windows. It stages the validated PGO server and
exact shared client, then packages/uploads the existing archive format.
Release attachment waits for the entire matrix to succeed; Docker consumes the
qualified static musl archives and retains its native image smoke tests.

For a standalone qualified server package, manually dispatch **Server PGO**,
selecting a native server target and initial/minimum client count. The workflow
chooses `windows-latest`, `macos-15`, `ubuntu-24.04` or `ubuntu-24.04-arm` to match.
It uploads the qualified package and Markdown summary. Failed attempts retain
reports without publishing a server artifact. Automatic release jobs use the
same 300/100 defaults and a new output directory per target/run/attempt.
No fixed 16-CPU affinity assumption is made on hosted runners. Observable Linux
power/governor controls are checked for changes; equivalent sysfs controls are
not available on Windows/macOS. Hosted observations are not dedicated-hardware
capacity measurements. GitHub manual dispatch requires this workflow to exist
on the repository's default branch.

Profiles are tied to the frozen source, compiler and target. Do not reuse them
for ARM, musl, Windows, changed code, changed compiler or a different workload.
The historical 750-client diagnostic profile is intentionally ineligible.
Loopback, one observer and a small number of processes limit capacity and
regression conclusions. See [all Rust results](rust-performance-validation.md).
