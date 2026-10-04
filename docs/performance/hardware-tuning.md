# Tune avatar server settings on your hardware

[`tune-avatar-settings.py`](../../scripts/perf/tune-avatar-settings.py) runs a
local Linux or Windows **settings comparison**: one frozen server executable,
one frozen client executable, fixed XML fixtures and fixed offered workload.
It compares flush lanes `0,6` by default. It never changes your live config and
never runs two experiments concurrently. A preference applies to this host and
synthetic workload; six lanes is not a universal recommendation.

## Requirements and quick start

Use Python **3.9+** (standard library only), Rust/Cargo **1.95+**, and a native
compiler/linker for the existing server/client builds. The server must include
PR #27's `BASIS_AVATAR_FLUSH_LANES` capability. Linux needs `/proc`; `taskset`
from util-linux is needed only for affinity. Windows uses native process
counters and private Job Objects; no administrator privileges, pip packages,
PowerShell modules or host-wide scheduler changes are needed. Run from the
repository root. Leave other benchmarks and heavy applications stopped.

Linux:

```sh
cargo build --locked --release --manifest-path BasisRustServer/Cargo.toml -p basis-server-console
cargo build --locked --release --manifest-path BasisRustClient/Cargo.toml
python3 scripts/perf/tune-avatar-settings.py --mode quick --clients 250 --output captures/tune-quick
```

Windows PowerShell (requires the Rust MSVC build prerequisites):

```powershell
cargo build --locked --release --manifest-path BasisRustServer/Cargo.toml -p basis-server-console
cargo build --locked --release --manifest-path BasisRustClient/Cargo.toml
python scripts/perf/tune-avatar-settings.py --mode quick --clients 250 --output captures/tune-quick
```

The script selects `.exe` binaries on Windows. Supply `--server` and `--client`
for builds elsewhere. Output must be a new directory. Binaries and fixtures are
copied into `frozen/` before starting; every run uses those copies. The script
checks the server for the flush-lane capability string, but this cannot attest
binary source or prove that a modified binary implements the setting. Use a
known build; record its origin with `--binary-build-note "revision/build command"`.
The installed `rustc`/Cargo versions are recorded separately and do **not** prove
which toolchain produced an external executable.

Read `captures/tune-quick/report.md` and `summary.json`. Then confirm the same
workload, without rebuilding the frozen binaries:

```sh
python3 scripts/perf/tune-avatar-settings.py --mode confirm --clients 250 --server captures/tune-quick/frozen/server --client captures/tune-quick/frozen/client --output captures/tune-confirm
```

On Windows, use `python` and `frozen/server.exe` / `frozen/client.exe`. If the quick
run used custom fixtures, worker counts, ports or affinity, pass the **same**
choices to confirmation; use its frozen XML files. Do not increase the client
count when comparing a screen to its confirmation. To explore a higher load,
start a separate experiment, keep its load fixed, and confirm that experiment.

| Mode | Warmup / window | Repeats per setting | Default two-setting time, excluding builds | Establishes |
|---|---|---:|---|---|
| `quick` | 5 / 15 seconds | 2 | about 2 minutes at 250 clients | Plumbing/readiness check and short shortlist; sensitive to startup and tail noise |
| `screen` (default) | 15 / 60 seconds | 2 | about 5–6 minutes | Longer forward/reverse screening on this workload |
| `confirm` | 30 / 60 seconds | 4 | about 13 minutes | More process-run evidence and longer warmup; still not statistical proof or production prediction |

These estimates include fresh processes for every run; connection ramp and
shutdown add time. At 1,500 clients allow roughly another 15 seconds per run for
joins. A failed readiness wait is bounded by `--ready-timeout` (360 seconds),
server startup by `--startup-timeout` (30 seconds), and shutdown/diagnostic waits
are bounded. `--warmup-seconds`, `--window-seconds` (>=2) and `--repeats` (even,
>=2) override the presets; the report retains the actual values. A four-second
window is useful for a smoke check, not a tuning conclusion.

## Controls, defaults and workload

Use `--help` for all controls. `--lanes 0,6` includes the existing Rayon flush
scheduling (`0`) and bounded flush jobs (`1..8`). Values outside `0..8`, duplicate
values and single-setting comparisons are rejected. Keep the shortlist small;
the script does not sweep combinations of worker counts. With more settings it
uses forward/reverse pairs and rotates the starting setting between pairs;
with two settings the first two rounds are `0,6,6,0`.

Optional worker settings stay **fixed** for the entire sweep:

```sh
python3 scripts/perf/tune-avatar-settings.py --clients 750 --lanes 0,6 --rayon-threads 8 --tokio-workers 8 --client-workers 4 --mode screen --output captures/tune-workers
```

Omitting Rayon uses the server platform default: Linux Rayon available
parallelism; Windows `min(available parallelism,8)`. `--rayon-threads 0`
explicitly selects Rayon's automatic worker policy, including on Windows.
Omitting server Tokio uses available parallelism; explicit values must be >=1.
Client Tokio is explicitly fixed to 4 by default. Flush defaults without an
override are Linux `0`, Windows `6`; sweeps **always** set their requested lanes
explicitly. An affinity mask changes available parallelism, so retain it when
applying/reproducing a measured worker policy.

`--server-cpus 0-7 --client-cpus 8-15` pins Linux processes with `taskset`; choose
IDs allowed on your machine. Windows uses an affinity mask **before resuming**
each owned process; optional affinity supports only one processor group on hosts
with <=64 logical CPUs. No NUMA/processor-group sweep is implemented. Avoid
assuming split CPU IDs give separate physical cores: shared SMT, thermal limits
and generator contention can affect the outcome. Without flags both processes
use normal scheduling. Combined and separate server/client CPU and RSS are
reported, so load-generator cost stays visible.

UDP `--port 0` and TCP `--health-port 0` select ports once and reuse them across
all settings. Explicit ports must be in `1..65535`. Every run probes both ports
before launch and rejects occupied ports; a fresh server must report zero
players before the client starts. IPv6 dual-stack loopback support is required.
A competing application can still race the bind between probe and launch;
early process exits and unexpected population invalidate the run. TCP
`TIME_WAIT` from closed Linux runs is allowed by the probe, while live listeners
are rejected. Windows also checks exact loopback bindings/live health listeners
to avoid wildcard binding ambiguity; see [Microsoft's socket binding rules](https://learn.microsoft.com/en-us/windows/win32/winsock/using-so-reuseaddr-and-so-exclusiveaddruse). Processes are owned by POSIX sessions on Linux and a kill-on-close
Job Object on Windows. Ctrl-C, SIGTERM/SIGBREAK, timeout and exceptions unwind
ownership, including descendants. Cleanup failure is not a successful run.
The host lock prevents concurrent invocations of **this tool**; it cannot
prevent other benchmark programs or unrelated background work.

The default [CPU-only server fixture](fixtures/avatar-cpu-only-server.xml) and
[client fixture](fixtures/avatar-1500-client.xml) use dedicated loopback
credentials. This differs from earlier experiments that enabled compute
offload. The workload retains the portable runner's dense colocated layout,
20ms movement policy, zero jitter/drift, 60FPS Unity-policy accumulator,
deterministic synthetic root/body rotations, no voice/P2P and one all-near
observer. These are not captured Unity poses or end-to-end movement timings.
The server advertises its fixture's update interval; changing a fixture starts
a different workload, not a lane comparison against earlier captures.

Optional `--server-config` / `--client-config` files must match credentials and
application identity, disable voice/profiling/compute offload/simulated loss,
enable extended health metrics, and retain the supported version/loopback
settings. Missing/incompatible fields fail before launch. Server CLI overrides
port, health host/port and console only; original and copied XML hashes are
retained. Existing `BASIS_*`, Rayon/Tokio and XML field environment overrides
(including fields absent from the fixture) are cleared for child processes;
cleared **names**, never their secret values, are recorded. The script installs
only documented worker/diagnostic/flush overrides. It does not modify your shell
environment. Loader/library environment still applies to native binaries.

## Interpreting evidence

Each run waits for **all requested authenticated players and active avatar
states**, then checks readiness during warmup and the complete measured window.
CPU counter samples bracket that fixed marker window and are interpolated to
its endpoints. RSS is sampled every roughly half second. Observer windows can
start up to 50ms after the marker due to existing client polling. Server
cumulative diagnostic windows use their actual first/last elapsed times;
logical sends and input rates are **last minus first**, never a sum of rows.
Diagnostic window duration must match within max(0.5 seconds, 5%); exact durations
and phase tick means are reported. Health samples gate protocol, transport and
retransmit errors. Instrumentation overhead is present and matched in all runs.

The report and JSON distinguish:

- Pooled **applied observer p50/p95 gaps**: cadence at one receiver, not
  end-to-end latency. Per-peer data remain in the sectioned observer CSV.
- **Built logical avatar work/s**: recipient avatar items built before
  submission, not a count of successfully delivered work. Applied items/s at
  the observer is reported separately.
- Input updates/s, generated and socket-sent sender items/s, per-sender progress,
  coverage, stale/missing peers, decode/sequence/send/protocol/transport errors,
  process exits, separate/combined CPU cores, mean/peak sampled RSS, and
  tick/build/flush means. One CPU core means 100% of one logical CPU, not 100%
  of the machine.

Every sender must remain connected, preserve its peer identity, have zero send
errors and send at least 90% of the fixed policy's nominal 50 items/s over the
window. Missing peers, end-snapshot stale peers (500ms), discontinuous coverage,
decode/sequence errors, dropped datagrams, retransmits, tick failures, unexpected
exits or changed artifacts/configs invalidate the run. End-snapshot stale peers
do not assert that every earlier gap was <500ms. An overloaded generator causes
a visible failure; lower `--clients` in a **new** experiment, rather than treating
lost work as a faster setting. Any failed run stops the series and blocks its
recommendation. No failed run or outlier is silently discarded.

Ranking compares individual settings within each forward/reverse round. A
unique measured preference must beat **every other requested setting in every
round**, keep input and sender socket rates within 5%, and show no lower
built work or applied delivery. It must either reduce **both** p50/p95 by >=3%
(cadence preference, potentially more CPU) or keep both gaps within 3% while
reducing server CPU by >=5% (CPU preference). These are explicit practical
screens, not statistical significance thresholds. Results include paired
percentage changes and min/median/max across process runs. Variable directions,
ties, a single standout run, incomplete series and competing tradeoffs are
**inconclusive**; the tool does not force a winner. Check CPU and cadence together
before choosing, and confirm a screen's preference. The retained per-run values
matter more than a headline median.

For a measured preference, the report emits Linux and PowerShell environment
snippets. Apply manually to your server launch, retaining any fixed worker
settings and affinity used in the experiment. For example, if **your** confirmed
run prefers six lanes:

```sh
export BASIS_AVATAR_FLUSH_LANES=6
# Optional fixed settings, only if these matched your experiment:
export RAYON_NUM_THREADS=8
export TOKIO_WORKER_THREADS=8
```

```powershell
$env:BASIS_AVATAR_FLUSH_LANES="6"
$env:RAYON_NUM_THREADS="8"   # only if tested
$env:TOKIO_WORKER_THREADS="8" # only if tested
```

Unset the lane variable to restore platform defaults (`unset` on Linux,
`Remove-Item Env:BASIS_AVATAR_FLUSH_LANES` in PowerShell). No snippet is executed
by the tool.

## Reproduce and report

Keep the entire capture directory: `experiment.json` records exact invocation,
ordered settings, frozen paths/hashes, original fixture/binary paths, local
source/tool hashes, Python/platform/CPU/toolchain metadata, and cleared override
names. Each run has exact command arrays and child environment overrides in
`commands.json`, process IDs/exits/error in `run.json`, retained private server
base/config, stdout logs, readiness/health/process samples in `samples.jsonl`,
observer/sender CSVs, cumulative server diagnostics and validated summary.
`report.md` is readable; `summary.json` contains every run, gate, metric and
comparison. Paths and credentials in custom fixtures should be reviewed before
sharing. Binary build provenance is a user-supplied note, distinguished from
observed binary hashes and installed-tool metadata.

Reanalyze without launching another benchmark:

```sh
python3 scripts/perf/tune-avatar-settings.py --analyze captures/tune-confirm
```

This regenerates summaries from raw evidence, including failed series; it needs
the frozen binaries/fixtures still present for provenance checks. For relocation,
the recorded absolute paths also need to resolve. Exit 0 means all requested
runs passed gates, **not** that a winner exists. Exit 1 means failed/incomplete
validation; interrupted experiments return 130. Share per-run results, full
JSON/report, exact binary source/build command, CPU/OS, affinity/SMT details,
client count, worker settings and workload fixtures. Include poor runs and any
background-load/thermal observations. Repeat on the intended deployment load;
this loopback test does not characterize remote network latency, other avatar
quality mixes, voice, real Unity scheduling or all Windows hardware.

The existing Windows [revision comparator](../../scripts/perf/compare-windows-avatar-crossover.py)
continues to require **different binary hashes** for revision A/B tests. This
CLI intentionally uses identical server hashes with validated distinct lane
configs; it is a separate comparison path, not a weakened revision check.
