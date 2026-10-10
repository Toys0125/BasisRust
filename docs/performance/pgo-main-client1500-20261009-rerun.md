# Latest-main PGO rerun: 1,500-client Rust avatar A/B — laptop, 2026-10-09

**Observed efficiency benefit in this workload.** PGO changed median server CPU by **+0.51%**, CPU seconds per million built sends by **-5.17%**, and built logical avatar sends/s by **+6.05%**. Applied p95 gap was **410.690 → 397.085 ms (-3.31%)**. Normalized CPU favored PGO in 4/4 pairs; p95 favored it in 3/4. Four processes per variant on one laptop provide descriptive evidence, not statistical significance or a universal speedup.

This is a **fresh profile on the merged source**: two separate 250-client training processes followed by eight 1,500-client evaluation processes. Both training runs pass all 34 gates. The same ordinary Rust client is used by both server variants. Older-source profiles and training timings do not enter this comparison. The PGO experiment adds no production changes beyond the main merge and preserves default build settings.

**Original strict evaluation validity: 0/8.** Separate steady-window checks pass 8/8. Cumulative retransmit counters and every original gate are retained; the supplemental checks do not overwrite strict invalid flags. Details follow below.

[Retained evidence](results/pgo-main-client1500-20261009-rerun-summary.json) includes all training/evaluation attempts, raw metrics/gate inputs, original flags, compiler/profile/build provenance, hashes and supplemental controller source.

## Evaluation results

Medians of four independent process results per variant. CPU cores are process user+system CPU seconds divided by the fixed measured wall interval, across all threads. CPU per million built sends is calculated per run before its median. Ranges and every pairwise metric are retained in JSON.

| Metric | Ordinary release | Fresh PGO | Change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 366.405 | 344.735 | -5.91% |
| Applied gap p95 (ms) | 410.690 | 397.085 | -3.31% |
| Built logical avatar sends/s | 6,120,423 | 6,490,742 | +6.05% |
| Observer applied items/s | 4,097.27 | 4,347.10 | +6.10% |
| Server inbound sync updates/s | 75,001.70 | 75,002.76 | +0.00% |
| Sender socket items/s | 75,000.00 | 75,000.00 | +0.00% |
| Server CPU cores | 3.7931 | 3.8125 | +0.51% |
| Client CPU cores | 0.9770 | 0.9883 | +1.16% |
| Server CPU seconds/million built sends | 0.620441 | 0.588382 | -5.17% |
| Server tick time (ms/tick) | 11.2681 | 10.6196 | -5.75% |
| Server build time (ms/tick) | 5.0442 | 4.9413 | -2.04% |
| Server flush time (ms/tick) | 2.4987 | 2.4755 | -0.93% |
| Server peak RSS (MiB) | 240.14 | 237.40 | -1.14% |
| Client peak RSS (MiB) | 45.93 | 47.01 | +2.34% |
| Server transmit (Mbps, loopback) | 990.50 | 1,032.79 | +4.27% |

| Order | Variant | Applied p50 / p95 ms | Built sends/s | Server / client CPU cores | Retransmits first → last |
|---|---|---:|---:|---:|---:|
| 1 | ordinary | 361.53 / 415.48 | 6,162,284 | 3.822 / 0.993 | 127 → 127 |
| 2 | PGO | 353.61 / 398.83 | 6,358,584 | 3.777 / 0.987 | 94 → 94 |
| 3 | PGO | 344.92 / 402.69 | 6,485,617 | 3.821 / 1.016 | 127 → 127 |
| 4 | ordinary | 358.30 / 402.28 | 6,267,540 | 3.814 / 0.979 | 86 → 86 |
| 5 | PGO | 344.55 / 395.34 | 6,495,866 | 3.817 / 0.988 | 94 → 94 |
| 6 | ordinary | 371.28 / 405.90 | 6,078,563 | 3.773 / 0.975 | 118 → 118 |
| 7 | ordinary | 382.68 / 420.88 | 5,932,297 | 3.716 / 0.955 | 195 → 195 |
| 8 | PGO | 334.89 / 382.78 | 6,631,127 | 3.808 / 0.988 | 233 → 233 |

| Pair | CPU/million built sends | Built sends/s | Applied p95 gap | Server CPU |
|---|---:|---:|---:|---:|
| 1 | -4.23% | +3.19% | -4.01% | -1.18% |
| 2 | -3.19% | +3.48% | +0.10% | +0.18% |
| 3 | -5.31% | +6.87% | -2.60% | +1.19% |
| 4 | -8.33% | +11.78% | -9.05% | +2.47% |

PGO builds more work in every pair (+3.19–11.78%) and lowers CPU cost per built send in every pair (−3.19–8.33%). Server CPU rises 0.51% at the median while built work rises 6.05%; observer applied work rises 6.10%. The second pair's p95 gap is 0.10% worse, and it remains included. The fourth pair has the largest work/cost improvement, partly reflecting its slower ordinary control. Both that control and the fast PGO observation remain in all statistics. Four pairs do not establish statistical significance or the cause of individual run variation.

Median transmitted bytes/s rises 4.27%, alongside increased output work. Shared-client CPU rises 1.16%, and peak client RSS rises 1.08 MiB (+2.34%). Median peak server RSS changes −1.14%, with mixed paired directions. This is evidence about work per CPU second under fixed offered input, not a maximum-capacity or network-efficiency test.

## Population, input and errors

All 944 half-second evaluation samples retain 1,500 authenticated active states and CPU-only processing. Every observer covers 1,499/1,499 expected peers in one uninterrupted 60,000 ms segment. Every run has 1,500 connected sender records with unchanged IDs and zero send errors. Sender totals range from 2,999–3,001 items over independently sampled diagnostic windows; generated and socket-sent counts match in every row. Socket rates remain within 0.03325% of the planned 75,000 items/s. Offered input is matched within that variation, not exactly identical.

Common received/processed/sync-upsert rates remain around 75,000/s. Every sampled avatarCoalesced and avatarRejected counter is zero in both variants. More built and observer-applied work therefore does not come with less offered input or extra realtime queue coalescing/rejection. The downstream sync pending map can still overwrite intermediate poses; upsert counts do not establish that every input was tick-consumed. Pending gauges and raw counts remain in the evidence.

All eight evaluation runs fail only `zero_retransmits`; the other 33 original gates pass. Sampled retransmit counts are 86–233 and exactly constant during measurement. Six runs already have those counts at population readiness. Ordinary run six rises from 50 at readiness to 118 before measurement; ordinary run seven rises from 77 to 195 during warmup. These are premeasurement counts, without attributing every one to connection startup. No whole-run retransmission-free claim is made.

Every sampled reliable pending/queued count is zero in all eight runs, so the slower controls do not show a measured reliable backlog. All window/coverage/readiness/input/decoding/malformed/delta/sequence/protocol/tick/WouldBlock/transport-drop/provenance gates pass. Client/server exits are zero. Separate steady-window checks pass 8/8 but do not overwrite original invalid flags; the strict series remains invalid.

The standard coordinator stops after the first original gate failure. A supplemental controller completes the seven remaining scheduled trials with the same frozen inputs and original gates. The first baseline stays in the same cohort. Its source/hash/invocation and the sequential build/evaluation driver's source are retained in JSON. No run was excluded and no capture procedure changed.

## Fresh profile and training

Raw profiles are 4,997,072 and 4,997,008 bytes; the merged profile is 9,873,560 bytes. The matching LLVM tool reports IR instrumentation, 29,331 function records, 279,210 blocks and 57,961,885,726 total counts. These are software instrumentation records/counts, not hardware branch measurements or positively exercised function counts. Top counts include avatar encoding, byte writing, hashing, delta decoding and bundle emission. The effective PGO build emitted no missing-profile or hash-mismatch diagnostics. This does not prove every function or production behavior was trained.

Every raw profile and the merged profile retains its recorded hash after evaluation. Exact merge/show commands and output are included in JSON. Profiles are tied to source tree/compiler/build settings; the prior pre-main profile was not reused.

## Method and fixed controls

This follows the [Rust compiler PGO workflow](https://doc.rust-lang.org/rustc/profile-guided-optimization.html): build an instrumented server, collect profiles, merge them with the toolchain's LLVM tool and rebuild with profile use. Ordinary and PGO variants share merged branch revision `98151c3976651da6d7fec2e3f5c36b636d426616`, server Git tree `b7130e68156ca47747f0832c7eac195923ed02f5`. This isolates PGO on the integrated source; it is not the earlier before/after-main comparison and does not reuse its historical timings as a control.

Both ordinary and PGO servers were freshly built from the same absolute source directory. Rust 1.99.0 / LLVM 23.1.1, GNU x86-64 target, locked release dependencies, two build jobs, disabled incremental compilation and default features are fixed. `CARGO_ENCODED_RUSTFLAGS` is explicitly unset. The common flag is `-Cllvm-args=-pgo-warn-missing-function`; only `-Cprofile-generate=<absolute profiles directory>` or `-Cprofile-use=<absolute merged profile>` is added for the appropriate stage. Each stage uses an isolated target directory. No LTO or target-CPU tuning was added. Cargo fingerprints verify effective flags and matching release profiles. All build commands, environments, logs/hashes and exact local build helpers are retained in the evidence.

The existing workflow trains with **two fresh 250-client runs**, each with 45 seconds of warmup and a 60-second capture after full readiness. Both pass all 34 original workload, delivery/error and provenance gates, exit successfully and produce one nonempty raw profile each. Only these new raw files enter the merge. Training timings are excluded from evaluation. Training at 250 clients does not establish an optimal profile for 1,500 clients.

Latest main at `4ce5f8ca5862254b5e61805c856e9b0ed5e7aba9` merged cleanly in this branch. It adds client control-loss fixes and scene/avatar-data workloads; the server source tree is unchanged from the previous PGO experiment. New scene and additional-avatar data defaults remain off (zero bytes), retaining the accepted dense avatar workload. A current-source ordinary Rust client is freshly rebuilt and shared across both variants: client Git tree `aed0f1a0c84cb8f5632157798786e46bc485545f`. Its binary/build provenance is checked and frozen for all evaluation runs. Both servers use the integrated CompactMerged default and the same XML fixtures. The client is neither instrumented nor optimized using PGO.

Evaluation uses the accepted **1,500-client** workload: dense colocated avatars, 20 ms movement, Unity-policy 60 FPS synthetic poses, zero jitter/drift, voice/P2P off and CPU-only server processing. Eight fresh-process trials execute sequentially in balanced order `A B B A / B A A B`, four per variant. Each waits for all authenticated clients/active states, warms for 45 seconds and measures a fixed 60-second CPU/server/sender window. The observer independently measures 60 seconds after its first inbound avatar following the start marker; the existing two-second shutdown grace preserves reception until that completes.

Server Rayon/Tokio and client Tokio each use four workers, with Linux flush lanes zero. Server CPUs 2–9, client CPUs 10–15, coordinator CPUs 0–1 remain fixed and disjoint. This native Linux Ryzen 9 5900HX laptop stays on AC and the performance power profile; other user processes are not stopped. No builds, training or other benchmark runs overlap evaluation. The 1,000 Mbps network-capacity value is an informational utilization reference, not a limiter; loopback traffic can exceed it.

## Validation and limits

Every evaluation and training summary is independently rederived from raw captures. Binary, configuration, tool and raw/merged-profile hashes are checked after evaluation. Observer/sender/global CSVs, readiness/process/health samples and commands used for metrics/gates are committed in JSON. Complete logs, per-pair CSVs, frozen binaries and profiles remain in ignored local captures with hashes. All attempts are retained; no performance outlier is removed.

The experiment makes no production or capture-core changes beyond main integration and preserves default release settings. The updated client passed 113 core tests (two ignored), ten real-client/server live loopback tests and its workspace formatting check. Python checks passed 37 avatar-tool tests (one Windows-only skip), three scene tests and three script-server tests. The unchanged server tree retains the preceding verified 113 server-core and 87 transport test results (three transport tests ignored). A PGO run does not replace correctness checks; the real training/evaluation additionally checks decoding, coverage, input, errors and successful shutdown.

Four evaluation processes per variant, one merged profile and one laptop yield descriptive observations, not statistical significance. One observer measures update cadence, not network RTT or end-to-end latency for all receivers. Built work counts logical recipient sends before transport. Sync inbound counters count upsert calls, not tick-consumed poses; a downstream pending map can overwrite intermediate updates. New realtime queue counters are checked separately to avoid accepting a speedup obtained by coalescing/rejecting more input.

Only target Rust crates are instrumented. The precompiled standard library and native C dependencies are not rebuilt with instrumentation, and the shared client remains ordinary release. LLVM profile records/counts are software instrumentation data, not hardware branch measurements or proof every recorded function executed. Dense loopback avatars omit voice, churn, malformed traffic, backpressure/loss and remote networking. Fixed workers/affinity do not establish maximum client capacity or platform-default performance. The fresh profile is tied to this source/compiler/build configuration; older profiles were not reused.

The previous [merged-source PGO cohort](pgo-main-client1500-20261009.md) is retained for context. This rerun has a rebuilt updated client, a fresh profile and a different session; comparing absolute times between reports does not isolate a main regression. The controlled comparison within this report is ordinary versus PGO on current source.

## Reproduction

Use a fresh capture root and the same source snapshot/toolchain. The recorded ordinary, generate and use builds share common flags and differ only by their profile flag/isolated target directory. Exact build commands/environments and local build helpers are included in JSON. The accepted training command is:

```sh
python3 -B scripts/perf/train-avatar-pgo.py \
  --root captures/pgo-main-client1500-20261009-rerun \
  --client captures/pgo-main-client1500-20261009-rerun/build-client/x86_64-unknown-linux-gnu/release/basis-rust-client
```

After merging only the two new validated raw files and rebuilding the PGO server, evaluation starts with:

```sh
python3 -B scripts/perf/compare-avatar-revisions.py \
  --baseline captures/pgo-main-client1500-20261009-rerun/build-release/x86_64-unknown-linux-gnu/release/basis-server-console \
  --candidate captures/pgo-main-client1500-20261009-rerun/build-use/x86_64-unknown-linux-gnu/release/basis-server-console \
  --client captures/pgo-main-client1500-20261009-rerun/build-client/x86_64-unknown-linux-gnu/release/basis-rust-client \
  --build-manifest captures/pgo-main-client1500-20261009-rerun/build-manifest.json \
  --output captures/pgo-main-client1500-20261009-rerun/evaluation \
  --clients 1500 --blocks 2 --warmup-seconds 45 --window-seconds 60 \
  --server-cpus 2-9 --client-cpus 10-15
```

The standard coordinator stops at an original strict gate failure. A supplemental local controller, retained with source/hash/invocation, completes the remaining diagnostic trials without changing gates or controls. A repeat requires new output directories. Complete local binaries/profiles/logs remain under the ignored capture root; their hashes and relevant evidence are committed.
