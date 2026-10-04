# Hardware tuning CLI validation — 2026-10-04

The Linux quick smoke completed all four fresh-process runs and reported
**inconclusive**. This validates the CLI's normal capture/delivery/ranking path;
it does not establish a preferred setting. The complete measured per-run
metrics, gates, hashes and paired comparisons are in the
[machine-readable summary](results/hardware-tuning-smoke-20261004.json).

Host: AMD Ryzen 9 5900HX, 16 logical CPUs, Linux 7.0.0-31, no affinity. One frozen
PR27 `1962927` server and one frozen current-main client from the prior capture
were reused throughout. Their complete SHA256 hashes are in the JSON; installed
Rust was 1.95.0, which is local-tool metadata rather than binary build attestation.
The CPU-only server fixture has SHA256 `21f6aaa3…`, client fixture `608c21fa…`.
Rayon and server Tokio used platform defaults, client Tokio explicitly used four
workers. Ports were selected once (UDP 59025, health TCP 37731) and reused.

Exact measured invocation (from the worktree root):

```sh
python3 -B scripts/perf/tune-avatar-settings.py --server /home/mgstange/Documents/BasisRust/captures/pr27-vs-58c107ef-2026-10-04/bin/candidate-basis-server-console --client /home/mgstange/Documents/BasisRust/captures/pr27-vs-58c107ef-2026-10-04/bin/basis-rust-client --mode quick --clients 250 --output captures/tuning-quick-final --binary-build-note 'PR27 1962927 frozen server; frozen current-main client from prior captures'
```

The binaries/raw CSVs/logs remain local and ignored; this command documents the
actual paths used, not downloadable binary artifacts. Build your own known
revision and select those binaries to reproduce the workload, following the
[quick start](hardware-tuning.md). Do not interpret the external binary note as
proof of toolchain/source provenance beyond the recorded hashes.

Each run waited for all 250 authenticated active senders, warmed for five
seconds and measured a fixed 15-second window. All runs retained 249 observer
peers, all senders progressed (minimum socket sends 750/750/751/750), all error
and delivery gates passed, and both processes exited zero. CPU cores are
process CPU seconds/wall seconds; built work is pre-transport recipient work,
and applied gaps are one observer's update intervals, not end-to-end latency.

| Order | lanes | Applied p50 / p95 ms | Built logical work/s | Input updates/s | Server CPU cores |
|---|---:|---|---:|---:|---:|
| 1 | 0 | 20.01 / 21.26 | 3,091,700 | 12,500 | 2.99 |
| 2 | 6 | 20.03 / 21.36 | 3,078,644 | 12,500 | 2.84 |
| 3 | 6 | 20.05 / 21.37 | 3,068,696 | 12,513 | 2.81 |
| 4 | 0 | 20.02 / 21.25 | 3,091,338 | 12,498 | 2.95 |

Six lanes used slightly less server CPU but built slightly less work, with gaps
within the practical tie threshold; no setting met every paired gate. No runs
were removed. Two short repeats, one small synthetic dense loopback workload,
shared generator CPU and no confirmation do not support a universal setting or
a performance significance claim. No Windows performance experiment was run.

Focused verification on the final implementation:

```sh
python3 -B scripts/perf/test_avatar_tuning.py
python3 -B scripts/perf/test_windows_avatar_tools.py
python3 -B scripts/perf/tune-avatar-settings.py --analyze captures/tuning-quick-final
```

The new suite ran 19 tests: 18 passed, one native Windows affinity test skipped.
The existing Windows-tool suite passed its two comparator tests (including
identical-binary rejection); three native Windows ownership tests skipped.
Coverage includes bounds/config/environment validation, sectioned observer CSV
parsing, cumulative counter deltas, fixed-window CPU interpolation,
counterbalanced order, ties/variable directions/input and work regressions,
config/worker/order provenance mismatch, host locking, occupied ports, direct
parent exit with surviving descendants, early server exit and startup timeout.

A separate real-server 12-client cancellation exercise waited for readiness,
then sent SIGTERM twice during a 30-second warmup. It returned 130, retained
`run.json`/config/logs and an inconclusive failure report, left both owned PIDs
gone, and both UDP/TCP ports could be rebound. Reanalysis of that interrupted
capture returned 1. An initial development smoke exposed Linux TCP TIME_WAIT
probe rejection on the second run; that failed capture remains retained. After
the probe fix, a full 12-client forward/reverse smoke passed. It is separate
from the final 250-client measurement, not pooled into it.

CodeRabbit completed a scoped review, found one minor repeated-signal cleanup
issue, and a second completed review reported zero findings after the fix.
Subsequent changes tightened the no-lower-work ranking gate and added a native
Windows affinity test; focused tests and the final quick smoke cover the final
implementation. Native Windows counters, binding/Job Object behavior and
pre-resume affinity still need execution on Windows; the checked-in Windows
tests make that limitation explicit.
