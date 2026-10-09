# Main integration: 1,500-client Rust avatar A/B — laptop, 2026-10-09

**No clear regression in avatar cadence or throughput; memory increased.** After pulling main into `performance/branch-optimization`, median applied p95 gap was 411.755 → 407.455 ms (−1.04%), built logical sends/s rose 1.59%, and server CPU rose 0.43%. CPU per million built sends fell 1.22%. Server results varied across pairs: two favored each variant on work and normalized CPU, while three favored the integrated source on p95. These small server changes are descriptive, not evidence of a statistically established improvement.

Client CPU fell **30.32%**, with all four matched pairs improving by 29–31%. The observed memory cost is consistent across server pairs: median peak server RSS rose **6.00 MiB (+2.56%)**, with paired increases of 0.92–7.75%. Median client peak RSS rose 1.36 MiB (+2.99%). This is an avatar-only result; it does not establish voice performance or absence of other regressions.

**The original strict series remains invalid:** all eight corrected runs fail only the cumulative absolute-zero retransmit gate. Those counts were already present at population readiness and stayed constant through all measurement samples. All other 33 gates pass. The separate steady-window checks pass 8/8 but do not change original validity.

[Retained evidence](results/main-regression-client1500-20261009-summary.json) includes every corrected and preliminary attempt, raw inputs used for metrics/gates, strict flags, supplemental checks, provenance and hashes.

## Corrected results

Medians of four independent processes per variant. CPU cores are process user+system CPU seconds divided by the fixed measured wall seconds across all threads. Normalized CPU is calculated per run before taking its median. Ranges and all pairwise metrics are retained in JSON.

| Metric | Before pull | Integrated main | Change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 356.020 | 353.730 | -0.64% |
| Applied gap p95 (ms) | 411.755 | 407.455 | -1.04% |
| Built logical avatar sends/s | 6,218,032 | 6,316,847 | +1.59% |
| Observer applied items/s | 4,159.73 | 4,234.67 | +1.80% |
| Server inbound sync updates/s | 74,994.93 | 75,003.90 | +0.01% |
| Sender socket items/s | 75,000.05 | 75,000.00 | -0.00% |
| Server CPU cores | 3.7898 | 3.8061 | +0.43% |
| Client CPU cores | 1.2116 | 0.8443 | -30.32% |
| Server CPU seconds/million built sends | 0.610744 | 0.603288 | -1.22% |
| Server tick time (ms/tick) | 11.0861 | 10.9163 | -1.53% |
| Server build time (ms/tick) | 5.0298 | 5.0023 | -0.55% |
| Server flush time (ms/tick) | 2.5982 | 2.4202 | -6.85% |
| Server peak RSS (MiB) | 234.59 | 240.60 | +2.56% |
| Server mean RSS (MiB) | 226.51 | 233.73 | +3.19% |
| Client peak RSS (MiB) | 45.37 | 46.72 | +2.99% |
| Server transmit (Mbps, loopback) | 1,008.22 | 1,000.75 | -0.74% |

| Order | Variant | Applied p50 / p95 ms | Built sends/s | Server / client CPU cores | Peak server RSS MiB | Retransmits first → last |
|---|---|---:|---:|---:|---:|---:|
| 1 | before pull | 351.50 / 400.10 | 6,363,606 | 3.787 / 1.221 | 230.29 | 44 → 44 |
| 2 | integrated main | 356.60 / 409.59 | 6,255,143 | 3.820 / 0.863 | 248.13 | 161 → 161 |
| 3 | integrated main | 350.86 / 387.73 | 6,378,551 | 3.801 / 0.832 | 239.43 | 143 → 143 |
| 4 | before pull | 353.11 / 426.29 | 6,230,380 | 3.793 / 1.208 | 232.74 | 105 → 105 |
| 5 | integrated main | 350.34 / 406.24 | 6,399,146 | 3.812 / 0.841 | 239.14 | 99 → 99 |
| 6 | before pull | 365.88 / 413.13 | 6,141,303 | 3.784 / 1.204 | 236.96 | 133 → 133 |
| 7 | before pull | 358.93 / 410.38 | 6,205,685 | 3.803 / 1.215 | 236.45 | 87 → 87 |
| 8 | integrated main | 366.18 / 408.67 | 6,145,900 | 3.779 / 0.848 | 241.77 | 139 → 139 |

| Pair | CPU/million built sends | Built sends/s | Applied p95 gap | Client CPU | Server peak RSS |
|---|---:|---:|---:|---:|---:|
| 1 | +2.63% | -1.70% | +2.37% | -29.28% | +7.75% |
| 2 | -2.12% | +2.38% | -9.05% | -31.14% | +2.87% |
| 3 | -3.32% | +4.20% | -1.67% | -30.14% | +0.92% |
| 4 | +0.33% | -0.96% | -0.42% | -30.23% | +2.25% |

The first pair had 1.70% less built work, 2.63% higher normalized server CPU and 2.37% higher p95 gap after integration. The fourth pair had 0.96% less work and 0.33% higher normalized CPU, with a slightly lower p95. Both are included. No performance outliers were removed.

## Population, input and retained work

All 944 corrected half-second samples retain 1,500 authenticated active states. Every observer covers 1,499/1,499 peers in one uninterrupted 60,000 ms segment. Every run has 1,500 connected sender records, unchanged peer IDs, zero send errors and matching generated/sent totals. Per-sender totals are 3,000–3,001, and socket rates differ from the planned 75,000 items/s by at most 0.000267%. Server inbound-sync rates stay around 75,000/s, and observer applied work rises 1.80% at the median.

The new queue counters show **no measured-window increase in avatar coalescing or rejection in any candidate run**. In the last candidate, seven coalesced inputs were already present before measurement and remained constant at seven. All other candidate coalescing samples and all rejection samples are zero. Candidate received and processed counter deltas are about 4.5 million inputs per sampled window. Their small differences reflect pending work at independently sampled endpoints, not an exact conservation equation. Baseline lacks these new counters; common UDP ingress, sync-upsert input, generated/socket sends and delivered observer cadence are retained for comparison. The steady-window results therefore do not indicate a gain from reducing offered input or queue processing.

All missing/stale-peer, decode/malformed/unapplied-delta, sequence/discontinuity, tick, protocol, WouldBlock and transport-drop gates pass. Client/server exits are zero. Retransmit counts are 44–161 and exactly constant from population readiness through the measured samples; the data does not identify which connection/ramp events caused them. No entire-run retransmission-free claim is made.

## Integration and fixed comparison

The control is pre-pull branch revision `c1d74bd2e48828dd31001e967ab79da1c0529897`. Main was fetched at `63748efc4fa8a562d27b88b7350b6131b92cd6a4` and integrated in merge revision `1e4dc73b9fbb2c4034225ca20ca4642c1df4ea7a`. The measured server trees are respectively `5960e7b2efe37b16ef5098dfa1a7b704635b61fe` and `b7130e68156ca47747f0832c7eac195923ed02f5`. Later report/tool-metadata changes leave those production trees unchanged.

Both servers were freshly built as ordinary release binaries, **without PGO**, from the same absolute source directory using Rust 1.99.0 / LLVM 23.1.1, the GNU x86-64 target, locked dependencies, disabled incremental compilation and two build jobs. Effective Rust flags were exactly `-Cllvm-args=-pgo-warn-missing-function` for both. That warning flag does not enable PGO. This isolates source integration from the previous PGO experiment and its historical timings. Exact commands, effective Cargo fingerprints and hashes are retained in JSON.

One updated ordinary Rust client is shared across both servers. Main enables CompactMerged sends by default; the old client cannot receive that format, while the updated client supports both legacy Merged and CompactMerged. The unchanged XML fixtures do not override that default. This compares the integrated behavior against the old server using a compatible common client, rather than comparing two different clients.

The accepted workload remains 1,500 colocated dense avatars, 20 ms movement, Unity-policy 60 FPS synthetic poses, zero jitter/drift, voice/P2P off and CPU-only server processing. Each fresh process pair waits for all clients/active states, warms for 45 seconds and measures a fixed 60-second window. Observer reception continues for the existing two-second shutdown grace so its independently starting 60-second window finishes. Server Rayon/Tokio and client Tokio each use four workers; flush lanes remain zero. Server CPUs 2–9, client CPUs 10–15, coordinator CPUs 0–1. The Ryzen 9 5900HX native Linux laptop remains on AC and the performance power profile. No compilation or other benchmark runs overlap evaluation.

Eight processes execute sequentially in balanced order `A B B A / B A A B`, four per variant. The 1,000 Mbps metadata value is a reference for reporting utilization, **not a network shaper or quality threshold**. Linux loopback can exceed it.

The merge retains main's CompactMerged, pooling, session/lifecycle and reliable-admission changes, and adapts the branch's transport lookup/cold helpers. The property lookup now recognizes CompactMerged, and its test exhaustively checks all 256 header bytes. Cold receive error handling preserves main's nonblocking enqueue path. New `PeerState` fields were added to the benchmark fixture. An obsolete blocking queue-full helper was removed. Main's client/server-core implementations are retained, including deduplicated voice diagnostics.

Dedicated avatar/voice threads and avatar coalescing already existed on the branch before this pull. The actual avatar queue change retains separate high-pose, low-pose and delta slots, drains retained inputs in arrival order and adds session checks. Main also exposes received/coalesced/rejected/processed counters. The performance comparison measures the combined integrated changes; it does not isolate any one commit or attribute all behavior to a new thread architecture.

## Measurement correction and validity

The preliminary baseline failed workload provenance because main added the existing `network_capacity_mbps` value to capture metadata but the branch's comparison coordinator omitted it from the expected manifest. Both comparison and PGO-training coordinators now retain this field. A real-capture guard check accepts the matching value 1,000 and rejects a changed value 975; no workload gate was relaxed. The whole preliminary capture is preserved separately and excluded from corrected medians because its manifest schema differed, not because of its performance.

The corrected standard coordinator stopped at its first cumulative retransmit gate failure. A supplemental local controller completed the remaining scheduled runs with identical binaries, settings and strict gates. Original invalid flags are retained alongside separate measured-window checks. Controller source/hash/invocation, preliminary evidence, corrected raw observer/sender/global CSVs, readiness and process/health samples, commands, and all artifact hashes are retained in the JSON.

## Verification and limits

After conflict fixes, server-core tests passed 113/113 and transport tests passed 87 with three ignored. Client-core tests passed 106 with two ignored. Both workspaces passed formatting checks. Avatar tool tests passed 35 with one Windows-only skip; voice tool tests passed 14. Avatar tests were rerun after the metadata correction. Initial compile failures for the generic cold-helper return type and new benchmark fields were fixed; failed attempt logs are retained. CodeRabbit's scoped follow-up against main completed with zero findings. Its initial major fixture finding was fixed; an initial minor finding concerned a preexisting, unchanged reliable-loss example that is not used by this workload.

Every corrected summary was independently rederived from raw captures, and binary/tool hashes were checked after evaluation. Four processes per variant on one laptop give descriptive evidence, not statistical significance. One observer measures update cadence, not end-to-end latency or delivery to all receivers. Built work counts logical recipient sends before transport. Common inbound-sync counters count upsert calls, not tick-consumed poses; the downstream pending map can overwrite intermediate updates. New queue counters are unavailable on the old server and are retained as null rather than assumed zero.

This dense loopback test does not cover voice, churn, malformed traffic, remote networking or deliberate loss. Fixed worker counts/affinity do not establish maximum client capacity or platform-default performance. No hardware branch counters were collected. Existing PGO profiles were not reused across changed source; this is an ordinary-release source-regression test.

The [subsequent fresh PGO A/B](pgo-main-client1500-20261009.md) trains on this
merged source and compares ordinary/PGO at 1,500 clients with a new matched
cohort. Its results are independent of these source-regression timings.

## Reproduction

Exact build commands and runtime invocations are in the evidence. The corrected initial comparison was:

```sh
python3 -B scripts/perf/compare-avatar-revisions.py \
  --baseline captures/main-regression-client1500-20261009/build-baseline/x86_64-unknown-linux-gnu/release/basis-server-console \
  --candidate captures/main-regression-client1500-20261009/build-candidate/x86_64-unknown-linux-gnu/release/basis-server-console \
  --client captures/main-regression-client1500-20261009/build-client/x86_64-unknown-linux-gnu/release/basis-rust-client \
  --build-manifest captures/main-regression-client1500-20261009/build-manifest.json \
  --output captures/main-regression-client1500-20261009/evaluation-corrected \
  --clients 1500 --blocks 2 --warmup-seconds 45 --window-seconds 60 \
  --server-cpus 2-9 --client-cpus 10-15
```

The standard coordinator stops at a strict gate failure; the retained supplemental controller then completed the remaining diagnostic trials. Use a new output directory for a repeat. These binaries/logs remain in ignored local captures; their relevant raw evidence, provenance and hashes are committed in JSON.
