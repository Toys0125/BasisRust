# Optimization and week-simulation results

Six optimizations are retained. Five experiments were rejected and their source changes reverted because component speedups did not preserve live delivery. Each comparison below uses its own matched control; percentages must not be added into a cumulative speedup.

## Retained optimizations

| Change | Measured result | Practical limit |
| --- | --- | --- |
| [Borrow avatar bundle payloads](optimization-20261013.md) | At 1,500 clients, p95 applied-update gap 344.28 → 331.85 ms (−3.6%), delivery +1.0%, server CPU −1.2%. Native clone/drop share 6.58% → 1.21%. | Both crossover orders improved; two observations per variant. The 1,000-client result was less consistent. Cache keys retain owning references. |
| [Share the tick membership snapshot](optimization-20261013-membership.md) | At 1,500 clients, p95 331.41 → 311.72 ms (−5.9%), delivery +5.0%, approximately flat combined CPU. Server CPU in the 25/100-speaker workloads fell 12.8%/6.9% while voice delivery stayed valid. | Generation and snapshot-identity checks preserve join/leave safety. Two observations per variant; not a capacity guarantee. |
| [Pack quaternion bits by byte chunks](optimization-20261013-packing.md) | Nine-field writer 415.5 → 48.3 ns/pose (−88.4%); client user CPU fell roughly 6–9% in the large live workloads. | Rust client/load generator only. At 1,500 clients, p95 was 1.35% worse and delivery 1.16% lower; no server or Unity C# speedup claimed. |
| [Bulk residual bit writing](optimization-20261013-delta.md) | Complete dense delta encoding cost fell 28.29%, 33.50%, 63.95%, and 76.82% across very-low/low/medium/high quality. Single-axis cost fell 8.55–10.34%. | Exact reconstruction and checksums passed. Live 1,500-client p95 +1.3% and delivery −0.9% do not support a process-wide gain. |
| [Skip exact-clean delta work](optimization-20261013-clean.md) | Byte-identical encoder cost fell 87.28–89.88% across all qualities. Sparse/dense cases stayed within 1%. | Four live observations per variant at 1,500 clients were near flat overall: delivery +0.48%, p95 314.50 → 313.27 ms, slight CPU increase. |
| [Read residual single bits directly](optimization-20261014-read-bit.md) | Complete dense decoding improved 5.45–8.87%; single-axis 1.62–4.23%. At 1,500 clients, p95 −3.90%, delivery +3.36%, server CPU +1.96%. | Byte/reference/truncation checks passed. Two live observations per variant; decoder leaf profile share 2.11% → 1.38% is a relative sampling share. |

Targeted clean/decoder screens used 16 observations per variant per case across two process invocations. Packing used eight alternating samples; the dense encoder screen used eight rounds over 32 deterministic pairs per quality. Detailed reports preserve each workload's methodology and limitations.

## Rejected experiments

| Experiment | Reason for rejection |
| --- | --- |
| [Receiver roster indices](optimization-20261013-index.md) | At 1,500 clients, p95 +3.45% and delivered updates −2.33%, worse in both crossover orders. |
| [Reusable voice send buffer](optimization-20261013-voice-send.md) | Small client CPU savings accompanied 21.72% fewer avatar updates in the 250-speaker overload case. |
| [Zstd destination buffer](optimization-20261013-zstd.md) | Component throughput generally improved 0.4–8.4%, but at 1,500 clients delivery fell 3.77% and p95 rose 3.47%. |
| [Channel discovery bitset](optimization-20261013-bundle.md), [confirmation](optimization-20261014-channel-confirm.md) | Encoder screens improved, but confirmation lost 2.38% of delivered updates at 1,000 clients with p95 +3.32%; both orders lost delivery. At 1,500 clients, delivery −0.93%, p95 +3.01%. |
| [Client payload borrowing](optimization-20261014-client-borrow.md) | Serializer cost fell 2.79%, but 1,500-client delivery fell 2.08% and p95 rose 3.50%. |

No rejected implementation remains in the committed source. Their reports and heatmaps remain useful evidence of why lower CPU or a faster component was insufficient.

## Benchmark coverage, profiling, and tooling

The common suite covers **12 commands / 37 subcases**: registry dispatch, reliable transport under loss/reordering, admission, peer snapshots, protocol extraction, client framing/soak, and hardware GPU distance-result consumption. Typical live cohorts contain 16 A/B/B/A captures over four avatar/voice workloads, with 20-second warmup and 60-second measurement. Per-run delivery, coverage, errors, sender progress, and voice cadence/fanout/hash/decode gates are retained. Microbenchmark drift in unchanged binaries is not credited to optimizations.

Linux `perf stat` collected CPU/cache/context-switch counters, and native `perf record` captures produced exported PNG/SVG heatmaps. Profiles retain unknown-symbol shares and validate zero lost samples. Most matched profiles span 30 seconds at 199 Hz; shares are not absolute CPU savings. These experiments ran on one shared Linux host without pinned affinity, so their small live cohorts do not establish statistical significance or physical-network capacity.

The [voice overload diagnosis](optimization-20261013-voice.md) found nominal source cadence around 83%, unique fanout receipt around 88%, and estimated bounded server-inbox replacement around 11.5% of received frames at 250 speakers. That overload did not pass capacity/freshness gates; no queue setting was changed or capacity improvement claimed.

The [tooling follow-up](optimization-20261013-round2-tooling.md) verifies exact sample counts, frozen binaries, corpus names/hashes, execution status/timeouts, and matched profile settings. Twenty-four focused profile-guard checks passed. Failed and incomplete trials remain preserved locally rather than being backfilled into eligible cohorts.

## Full week and lifecycle fixes

The [week simulation report](../testing/week-simulation.md) records a passed **168-hour logical schedule** with **1,000 players at peak on every day**, completed in 63.6 wall-clock minutes:

- 6,074 joins and leaves, 5,074 returns, six owner transfers, 6,070 exact 100-byte owner-script join snapshots, and 168 broadcast-convergence windows.
- 109,354 exact avatar/face checks; full/delta updates, sequence wrap/reconnect reset, linked-avatar changes, and 52-float face data enabled/disabled. Bounded recovery sent 29,139 additional source frames; this is eventual UDP state convergence, not lossless delivery.
- Join-sync latency: 49.7 ms p50, 59.0 ms p95, 313.9 ms maximum over 6,070 samples.
- All 5,040 stable frames overlapping churn and all 588 paced/three-frame-burst voice frames passed byte checks. Intentional overload delivered 126 of 1,344 burst frames, counted 1,218 drops, and recovered in all 42 windows. Voice routing was disabled and checked in 168 windows.
- All 40 normalized real Opus clips were loaded and sampled. Protocol errors and avatar admission rejections were zero. Session, voice, ownership, and retirement cleanup checks passed.

The run exposed and fixed stale voice subscriptions targeting recycled IDs and cross-channel leave/join ordering. Voice configuration captures exact peer sessions and invalidates queued audio when configurations change. IDs remain retired until reliable cleanup/leave delivery completes; join-history producers are serialized with retirement. Actor keepalives, merged control handling, and advancing-frame recovery make the long integration fixture faithful to real transport timing.

A short peak snapshot used about 9.4 CPU cores across the combined server-and-simulated-client process. A separate 49 Hz profile attributed 70.86% of sampled CPU to avatar fanout in `build_sends_for_receiver`, identifying remaining cost without claiming another optimization.

Final verification passed **360 server-workspace tests and 116 client-workspace tests**, server/core transport Clippy with warnings denied, both workspace formatting checks, and diff checks. The full ignored week test was run separately; all 14 recorded source hashes and the frozen binary hash matched after verification. Earlier targeted hardware GPU parity checks passed; the week run does not exercise Unity, scene assets, rendering, or audio playback decoding. Production timers remain wall-clock, so this is not a seven-day timer/memory soak or a universal capacity result.

## Reproduction and artifacts

Use the [Linux suite runner](../../scripts/perf/run-linux-optimization-suite.py), [comparison tool](../../scripts/perf/compare-optimization-suite.py), [week runner](../../scripts/sim/run-week-sync.py), and [heatmap exporter](../../scripts/sim/plot-week-sync.py). Individual reports contain workload commands. Corpus audio must be supplied locally.

Readable reports and PNG/SVG heatmaps are versioned. New JSON exports, frozen binaries, raw CSVs, logs, and perf recordings are preserved under ignored `captures/`, including `captures/optimization-evidence-20261014/` and `captures/week-simulation/full-1000-keepalive/`. These local artifacts are not included in the commit or available in a fresh checkout; no raw JSON is added.
