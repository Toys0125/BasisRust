# Fresh merged-source PGO: 1,500-client Rust avatar A/B — laptop, 2026-10-09

**Observed efficiency benefit in this workload.** PGO changed median server CPU by **-0.16%**, CPU seconds per million built sends by **-3.68%**, and built logical avatar sends/s by **+3.98%**. Applied p95 gap was **403.910 → 394.090 ms (-2.43%)**. Normalized CPU favored PGO in 4/4 pairs; p95 favored it in 3/4. Four processes per variant on one laptop provide descriptive evidence, not statistical significance or a universal speedup.

This is a **fresh profile on the merged source**: two separate 250-client training processes followed by eight 1,500-client evaluation processes. Both training runs pass all 34 gates. The same ordinary Rust client is used by both server variants. Older-source profiles and training timings do not enter this comparison. Production source/default build settings remain unchanged.

**Original strict evaluation validity: 0/8.** Separate steady-window checks pass 8/8. Cumulative retransmit counters and every original gate are retained; the supplemental checks do not overwrite strict invalid flags. Details follow below.

[Retained evidence](results/pgo-main-client1500-20261009-summary.json) includes all training/evaluation attempts, raw metrics/gate inputs, original flags, compiler/profile/build provenance, hashes and supplemental controller source.

## Evaluation results

Medians of four independent process results per variant. CPU cores are process user+system CPU seconds divided by the fixed measured wall interval, across all threads. CPU per million built sends is calculated per run before its median. Ranges and every pairwise metric are retained in JSON.

| Metric | Ordinary release | Fresh PGO | Change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 356.470 | 342.480 | -3.92% |
| Applied gap p95 (ms) | 403.910 | 394.090 | -2.43% |
| Built logical avatar sends/s | 6,258,783 | 6,508,103 | +3.98% |
| Observer applied items/s | 4,184.71 | 4,347.10 | +3.88% |
| Server inbound sync updates/s | 75,000.85 | 74,997.94 | -0.00% |
| Sender socket items/s | 75,001.62 | 75,000.00 | -0.00% |
| Server CPU cores | 3.8121 | 3.8060 | -0.16% |
| Client CPU cores | 0.8387 | 0.8520 | +1.59% |
| Server CPU seconds/million built sends | 0.608887 | 0.586478 | -3.68% |
| Server tick time (ms/tick) | 11.0169 | 10.5929 | -3.85% |
| Server build time (ms/tick) | 5.0376 | 4.9385 | -1.97% |
| Server flush time (ms/tick) | 2.4717 | 2.4567 | -0.61% |
| Server peak RSS (MiB) | 241.29 | 240.02 | -0.53% |
| Client peak RSS (MiB) | 45.05 | 47.01 | +4.35% |
| Server transmit (Mbps, loopback) | 1,011.67 | 1,071.94 | +5.96% |

| Order | Variant | Applied p50 / p95 ms | Built sends/s | Server / client CPU cores | Retransmits first → last |
|---|---|---:|---:|---:|---:|
| 1 | ordinary | 354.85 / 398.64 | 6,327,629 | 3.816 / 0.838 | 90 → 90 |
| 2 | PGO | 342.44 / 385.39 | 6,544,336 | 3.802 / 0.829 | 101 → 101 |
| 3 | PGO | 350.90 / 402.79 | 6,367,871 | 3.802 / 0.853 | 165 → 165 |
| 4 | ordinary | 357.07 / 400.37 | 6,289,950 | 3.808 / 0.848 | 122 → 122 |
| 5 | PGO | 342.52 / 402.92 | 6,474,955 | 3.810 / 0.851 | 107 → 107 |
| 6 | ordinary | 361.58 / 407.45 | 6,204,861 | 3.800 / 0.835 | 145 → 145 |
| 7 | ordinary | 355.87 / 430.56 | 6,227,616 | 3.824 / 0.840 | 143 → 143 |
| 8 | PGO | 342.24 / 385.13 | 6,541,250 | 3.824 / 0.860 | 130 → 130 |

| Pair | CPU/million built sends | Built sends/s | Applied p95 gap | Server CPU |
|---|---:|---:|---:|---:|
| 1 | -3.68% | +3.42% | -3.32% | -0.38% |
| 2 | -1.36% | +1.24% | +0.60% | -0.14% |
| 3 | -3.93% | +4.35% | -1.11% | +0.26% |
| 4 | -4.80% | +5.04% | -10.55% | -0.01% |

PGO builds more work in every pair (+1.24–5.04%) and lowers CPU cost per built send in every pair (−1.36–4.80%). Raw server CPU stays nearly unchanged, so the primary observed benefit is more work for similar CPU time. Observer applied work rises 3.88% at the median. In the second pair, PGO's p95 gap is 0.60% worse despite more built work; it remains included. These results support a workload-specific efficiency benefit, with cadence variation and strict validity limits retained.

The additional work also uses more network traffic: median transmitted bytes/s rises 5.96%, versus 3.98% more built logical sends. Median shared-client CPU rises 1.59%, and peak client RSS rises 1.96 MiB (+4.35%). Peak server RSS changes −0.53%; its paired direction varies. This does not establish better network efficiency or maximum client capacity.

## Population, input and errors

All 944 half-second evaluation samples retain 1,500 authenticated active states. Every observer covers 1,499/1,499 expected peers in one uninterrupted 60,000 ms segment. All 1,500 sender records stay connected with unchanged IDs and zero send errors. Per-sender totals are 3,000–3,001; socket rates remain within 0.0324% of the planned 75,000 items/s. Offered input is matched within that variation, not exactly identical. Twenty-seven sender rows differ by one between generated and socket-sent counters (18 generated-ahead, nine sent-ahead). Independent diagnostic snapshots are a plausible explanation, not a demonstrated cause; exact counts are retained.

Common received/processed/sync-upsert rates remain around 75,000/s. **Every sampled avatarCoalesced and avatarRejected counter is zero in both variants.** More built and observer-applied work therefore does not come with reduced offered input or extra queue coalescing/rejection. The downstream sync pending map can still overwrite intermediate poses; upsert counts do not prove every input was tick-consumed. That distinction and pending gauges are retained.

Every evaluation fails only `zero_retransmits`: counts of 90–165 are already present at population readiness and stay exactly constant in every measured sample. All other 33 original gates pass, including window duration, coverage, continuous readiness, input progress, decoding/malformed/delta/sequence checks, protocol/tick errors, WouldBlock, transport drops and control provenance. Client/server exits are zero. Separate steady-window checks pass all eight, but the original strict series remains invalid. The entire run is not described as retransmission-free, and the captured data does not identify which ramp/connection events caused those counts.

The standard coordinator stops after the first original gate failure. Its whole first baseline capture is retained within the same cohort. A supplemental controller completes the seven remaining scheduled trials using the same binaries, profile, controls and original gates; it preserves invalid flags and its exact source/hash/invocation is retained. No capture procedure changed and no run was excluded.

## Fresh profile and training

Raw profiles are 4,997,104 and 4,997,024 bytes; the merged profile is 9,873,640 bytes. The matching LLVM tool reports IR instrumentation, 29,331 function records, 279,210 blocks and 56,112,387,300 total counts. These are software instrumentation records/counts, not hardware branch measurements or positively exercised function counts. Top counts include avatar encoding, byte writing, hashing, delta decoding and bundle emission. The effective PGO build emitted no missing-profile or hash-mismatch diagnostics. This does not prove every function or production behavior was trained.

Every raw profile and the merged profile retains its recorded hash after evaluation. Exact merge/show commands and output are included in JSON. Profiles are tied to source tree/compiler/build settings; the prior pre-main profile was not reused.

## Method and fixed controls

This follows the [Rust compiler PGO workflow](https://doc.rust-lang.org/rustc/profile-guided-optimization.html): build an instrumented server, collect profiles, merge them with the toolchain's LLVM tool and rebuild with profile use. Ordinary and PGO variants share merged branch revision `d9ba84107b60484f0acf50d1bbeea17860fdb35b`, server Git tree `b7130e68156ca47747f0832c7eac195923ed02f5`. This isolates PGO on the integrated source; it is not the earlier before/after-main comparison and does not reuse its historical timings as a control.

Both ordinary and PGO servers were freshly built from the same absolute source directory. Rust 1.99.0 / LLVM 23.1.1, GNU x86-64 target, locked release dependencies, two build jobs, disabled incremental compilation and default features are fixed. `CARGO_ENCODED_RUSTFLAGS` is explicitly unset. The common flag is `-Cllvm-args=-pgo-warn-missing-function`; only `-Cprofile-generate=<absolute profiles directory>` or `-Cprofile-use=<absolute merged profile>` is added for the appropriate stage. Each stage uses an isolated target directory. No LTO or target-CPU tuning was added. Cargo fingerprints verify effective flags and matching release profiles. All build commands, environments, logs/hashes and exact local build helpers are retained in the evidence.

The existing workflow trains with **two fresh 250-client runs**, each with 45 seconds of warmup and a 60-second capture after full readiness. Both pass all 34 original workload, delivery/error and provenance gates, exit successfully and produce one nonempty raw profile each. Only these new raw files enter the merge. Training timings are excluded from evaluation. Training at 250 clients does not establish an optimal profile for 1,500 clients.

The shared ordinary Rust client is unchanged from the main-integration comparison: client Git tree `2b65b6f45b10228ffe264e4292b8bac99df09745`. Its binary/build provenance is checked and frozen for all evaluation runs. Both servers use the integrated CompactMerged default and the same XML fixtures. The client is neither instrumented nor optimized using PGO.

Evaluation uses the accepted **1,500-client** workload: dense colocated avatars, 20 ms movement, Unity-policy 60 FPS synthetic poses, zero jitter/drift, voice/P2P off and CPU-only server processing. Eight fresh-process trials execute sequentially in balanced order `A B B A / B A A B`, four per variant. Each waits for all authenticated clients/active states, warms for 45 seconds and measures a fixed 60-second CPU/server/sender window. The observer independently measures 60 seconds after its first inbound avatar following the start marker; the existing two-second shutdown grace preserves reception until that completes.

Server Rayon/Tokio and client Tokio each use four workers, with Linux flush lanes zero. Server CPUs 2–9, client CPUs 10–15, coordinator CPUs 0–1 remain fixed and disjoint. This native Linux Ryzen 9 5900HX laptop stays on AC and the performance power profile; other user processes are not stopped. No builds, training or other benchmark runs overlap evaluation. The 1,000 Mbps network-capacity value is an informational utilization reference, not a limiter; loopback traffic can exceed it.

## Validation and limits

Every evaluation and training summary is independently rederived from raw captures. Binary, configuration, tool and raw/merged-profile hashes are checked after evaluation. Observer/sender/global CSVs, readiness/process/health samples and commands used for metrics/gates are committed in JSON. Complete logs, per-pair CSVs, frozen binaries and profiles remain in ignored local captures with hashes. All attempts are retained; no performance outlier is removed.

Production source, capture tools and default release settings are unchanged. The same merged source passed focused checks in the preceding integration task: 113 server-core tests, 87 transport tests (three ignored), 106 client-core tests (two ignored), both workspace formatting checks, 35 avatar-tool tests (one Windows-only skip) and 14 voice-tool tests. A PGO run does not replace correctness checks; the real training/evaluation additionally checks decoding, coverage, input, errors and successful shutdown.

Four evaluation processes per variant, one merged profile and one laptop yield descriptive observations, not statistical significance. One observer measures update cadence, not network RTT or end-to-end latency for all receivers. Built work counts logical recipient sends before transport. Sync inbound counters count upsert calls, not tick-consumed poses; a downstream pending map can overwrite intermediate updates. New realtime queue counters are checked separately to avoid accepting a speedup obtained by coalescing/rejecting more input.

Only target Rust crates are instrumented. The precompiled standard library and native C dependencies are not rebuilt with instrumentation, and the shared client remains ordinary release. LLVM profile records/counts are software instrumentation data, not hardware branch measurements or proof every recorded function executed. Dense loopback avatars omit voice, churn, malformed traffic, backpressure/loss and remote networking. Fixed workers/affinity do not establish maximum client capacity or platform-default performance. The fresh profile is tied to this source/compiler/build configuration; older profiles were not reused.

## Reproduction

Use a fresh capture root and the same source snapshot/toolchain. The recorded ordinary, generate and use builds share common flags and differ only by their profile flag/isolated target directory. Exact build commands/environments and local build helpers are included in JSON. The accepted training command is:

```sh
python3 -B scripts/perf/train-avatar-pgo.py \
  --root captures/pgo-main-client1500-20261009 \
  --client captures/main-regression-client1500-20261009/build-client/x86_64-unknown-linux-gnu/release/basis-rust-client
```

After merging only the two new validated raw files and rebuilding the PGO server, evaluation starts with:

```sh
python3 -B scripts/perf/compare-avatar-revisions.py \
  --baseline captures/pgo-main-client1500-20261009/build-release/x86_64-unknown-linux-gnu/release/basis-server-console \
  --candidate captures/pgo-main-client1500-20261009/build-use/x86_64-unknown-linux-gnu/release/basis-server-console \
  --client captures/main-regression-client1500-20261009/build-client/x86_64-unknown-linux-gnu/release/basis-rust-client \
  --build-manifest captures/pgo-main-client1500-20261009/build-manifest.json \
  --output captures/pgo-main-client1500-20261009/evaluation \
  --clients 1500 --blocks 2 --warmup-seconds 45 --window-seconds 60 \
  --server-cpus 2-9 --client-cpus 10-15
```

The standard coordinator stops at an original strict gate failure. A supplemental local controller, retained with source/hash/invocation, completes the remaining diagnostic trials without changing gates or controls. A repeat requires new output directories. Complete local binaries/profiles/logs remain under the ignored capture root; their hashes and relevant evidence are committed.
