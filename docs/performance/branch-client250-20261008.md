# 250-client Rust avatar A/B — laptop, 2026-10-08

**No demonstrated speedup.** All eight runs passed the delivery/error gates. Median
candidate p95 applied gaps rose 0.39%, built work fell 0.50%, and server CPU changed
−0.08%. CPU per million built sends rose 0.87%. Paired directions varied; these small
differences do not establish significance or a general improvement/regression.

Baseline `1e45b3b` versus candidate `d2ac839` on the native Linux Ryzen 9 5900HX laptop.
The candidate retains the original `3606a9b` production changes; its earlier experimental
decoder revert was rejected. Both servers and one shared Rust client were built from
isolated Git snapshots with locked dependencies, Rust 1.99.0 GNU release and two build
jobs. Client source trees are identical; the shared client uses the candidate transport
dependency and remains unchanged across both server variants. Complete build commands,
revisions, compiler/OS/CPU metadata and binary/config/tool hashes are in
[the retained evidence](results/branch-client250-20261008-summary.json).

## Fixed workflow

One Rust-client process supplies 250 authenticated logical clients, dense colocated
synthetic Unity-policy poses, 20 ms movement, zero jitter/drift, 60 FPS accumulator,
voice/P2P off and CPU-only server processing. The CPU-only server fixture and shared
client fixture are frozen. BSR profiling is off; the existing diagnostics are on.

Eight fresh-process runs execute sequentially as `A B B A / B A A B`: four per variant.
Each waits for all 250 active states, warms for 45 seconds, then measures for 60 seconds.
Server CPUs 2–9 cover physical cores 1–4; client CPUs 10–15 cover cores 5–7. Coordinator
CPUs 0–1 cover core 0. Server Rayon/Tokio and client Tokio each use four workers; Linux
flush lanes stay at zero for both revisions. The same unused UDP/health ports are reused.
AC power and the existing performance power profile were observed before and after.

These fixed worker/affinity choices are part of this comparison; it does not measure
every platform-default worker configuration. Other user processes were not stopped.

## Results

Values are medians of four independent process results per variant, rather than pooled
updates. Ranges and all four paired comparisons are retained in JSON. CPU cores are
process user+system CPU seconds divided by the fixed wall-clock window, across threads.

| Metric | Baseline | Candidate | Candidate change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 20.650 | 20.760 | +0.53% |
| Applied gap p95 (ms) | 33.095 | 33.225 | +0.39% |
| Built logical avatar sends/s | 2,850,035 | 2,835,866 | -0.50% |
| Observer applied items/s | 11,404.64 | 11,339.08 | -0.57% |
| Server inbound updates/s | 12,500.27 | 12,499.79 | -0.00% |
| Sender socket items/s | 12,500.00 | 12,500.00 | +0.00% |
| Server CPU cores | 1.8071 | 1.8056 | -0.08% |
| Client CPU cores | 0.3772 | 0.3832 | +1.57% |
| Server CPU seconds/million built sends | 0.635118 | 0.640660 | +0.87% |
| Server peak RSS (MiB) | 46.69 | 46.17 | -1.11% |
| Client peak RSS (MiB) | 18.73 | 18.77 | +0.24% |

| Order | Variant | Applied p50 / p95 ms | Built logical sends/s | Server / client CPU cores | All gates |
|---|---|---:|---:|---:|---|
| 1 | baseline | 20.63 / 32.96 | 2,870,300 | 1.798 / 0.365 | pass |
| 2 | candidate | 20.81 / 33.41 | 2,833,265 | 1.814 / 0.373 | pass |
| 3 | candidate | 20.71 / 32.93 | 2,838,466 | 1.828 / 0.385 | pass |
| 4 | baseline | 20.68 / 33.23 | 2,865,707 | 1.771 / 0.364 | pass |
| 5 | candidate | 20.80 / 34.15 | 2,785,716 | 1.786 / 0.381 | pass |
| 6 | baseline | 20.57 / 33.41 | 2,819,187 | 1.817 / 0.400 | pass |
| 7 | baseline | 20.67 / 32.87 | 2,834,364 | 1.825 / 0.389 | pass |
| 8 | candidate | 20.72 / 33.04 | 2,842,769 | 1.798 / 0.387 | pass |

The candidate built less work in three of four paired comparisons. Its normalized CPU
cost was higher in the first two pairs and lower in the last two; this is inconclusive.
The tiny raw-CPU reduction cannot establish efficiency while delivered/built rates
also change. No production code was changed for this runtime measurement.

## Delivery and validation

All runs retained 250 authenticated active states and uninterrupted 249/249 observer
coverage. Every sender stayed connected with the same peer ID, generated/sent at least
3,000 items in 60 seconds, and reported zero send errors. The offered socket rate was
exactly 12,500 items/s in every run. All 944 half-second process/readiness samples
retained the requested population.

All missing/stale-peer, decode/malformed/unapplied-delta, sequence/discontinuity, tick-error,
protocol-error, retransmit, WouldBlock and transport-drop gates passed. Both owned
processes shut down successfully in every run. No failed run or outlier was omitted.

All eight summaries were independently rederived from raw captures. Real-capture tests
rejected the wrong server binary, changed worker settings and mismatched client counts.
The existing capture suite passed 18 tests with one Windows-only skip; three new tests
cover failed/incomplete/duplicate/overlapping series, zero baselines and reduced work.
CodeRabbit found a major zero-baseline division issue in the new aggregator; it was
fixed with regression coverage, and the follow-up review completed with zero findings.

## Limits and reproduction

Applied gaps describe one receiver’s update cadence, not network RTT or end-to-end
latency for all receivers. Built work counts logical recipient sends before transport,
not UDP packets. The workload is synthetic and dense; voice, churn, malformed traffic,
backpressure, loss and remote networking were not exercised. No runtime hardware branch
counters were collected. One session with four processes per variant gives descriptive
evidence, with no statistical significance or universal result claimed.

The capture tool hashes are recorded, and the retained JSON includes all observer,
sender and global-counter CSVs plus the process/readiness fields used for metrics and
gates. Full logs/health/per-pair CSVs and frozen binaries remain locally under
`captures/client250-branches-20261008/`; their hashes are retained. The new coordinator
reuses the established workload/process ownership and strict capture parser.

After building the binaries described in the evidence, the exact invocation was:

```sh
python3 -B scripts/perf/compare-avatar-revisions.py \
  --baseline captures/client250-branches-20261008/source-baseline/BasisRustServer/target/release/basis-server-console \
  --candidate captures/client250-branches-20261008/source-candidate/BasisRustServer/target/release/basis-server-console \
  --client captures/client250-branches-20261008/source-candidate/BasisRustServer/target/release/basis-rust-client \
  --build-manifest captures/client250-branches-20261008/build-manifest.json \
  --output captures/client250-branches-20261008/series \
  --clients 250 --blocks 2 --warmup-seconds 45 --window-seconds 60 \
  --server-cpus 2-9 --client-cpus 10-15
```

Choose a new output directory for another series; existing captures are never overwritten.
