# Scene receipt investigation — 750 clients

**The receipt target is met with batching at 5 Hz:** candidate median receipt 99.976%, scene p95 histogram upper bound 131.071ms. At 20 Hz, batching improves median receipt from 5.681% to 46.025%, but remains overloaded. These are separate matched cohorts; the 5 Hz result trades scene update freshness for capacity. The default-off prototype is retained for reproducibility, with an explicit app-counter limitation below.

Native Linux Ryzen 9 5900HX laptop. One controlled change at a time; all failed attempts retained. This investigates scene relay capacity using ordinary Rust builds. PGO is off; older profiles are not applied to changed source.

## Why receipt collapses

The existing workload offers 750 × 749 × 20 = **11,235,000 recipient scene messages/s**. The 128-byte scene payload alone requires **1.438 GB/s (11.5046 Gbit/s)** of server egress, before scene/UDP/IP framing and avatars. The 1,000-Mbit/s fixture value is a reporting reference, not a loopback traffic shaper.

`relay_scene_generic` serializes one server scene envelope, then `ServerState::broadcast` awaits a separate transport send for every authenticated recipient except the source. SCENE uses bounded ordinary event handlers; avatar input has a separate real-time path. The fixture disables the per-source scene egress limiter (`MaxSceneRelayMegabitsPerSecondPerPlayer=0`).

A separate 30-second diagnostic baseline (not an A/B trial) had 5.67% receipt. The regular event queue reached 262,143 entries within 18.0s of its first active sample. Near saturation, ordinary handlers completed about 854/s versus 15,000 offered scene inputs/s. Perf attributed 79.3% of sampled server cycle periods to kernel addresses (12,056 samples, no lost samples). UDP/loopback processing appears across the hot stacks; `read_hpet` alone accounts for 8.49%. This supports packet/fanout cost and queue overload as the immediate bottleneck; it does not prove a particular shutdown deadlock.

## Opt-in prototype

`BASIS_SCENE_BATCH_MS=2` retains every serialized unreliable SCENE broadcast and packs recipient-local messages into existing MTU-bounded Merged/CompactMerged datagrams. It uses a bounded 1,024-input queue, batches of at most 128 messages/64 KiB, and four bounded recipient lanes. There is no message replacement or intentional capacity shedding. Full queues wait; transport socket refusals remain visible in existing would-block/drop counters. Source exclusion and captured recipient sessions are retained; read leases prevent recycled IDs receiving old batches. Reliable, directed and explicit-recipient traffic retain their existing path. Unset/0 retains the existing broadcast implementation.

The 2-ms setting limits collection waiting, **not total queue or end-to-end delay**. Shutdown finishes handlers, drains accepted input and joins workers before retiring remaining sessions. Pending recipient work can become stale when a recipient disconnects.

Both comparison arms use the **same ordinary binary**; only `BASIS_SCENE_BATCH_MS=0/2` changes. The client binary is unchanged. Each cohort uses four fresh server/client process pairs in ABBA order (two per arm), 45-second warmup and a fixed 60-second CPU/avatar window. Scene counters span the shared start marker through shutdown, including the two-second receive grace, and rates use their actual duration. All runs retain the original ≥95% scene receipt and full-population/error gates, including absolute-zero cumulative retransmits. Completed full-population overload trials continue diagnostically without changing their invalid flags.

## 5-Hz matched comparison

750 clients, 128 additional avatar bytes, 128 scene bytes; scene interval 200 ms. Avatar updates 20 ms, Unity-policy 60 FPS poses, amplitude 20, dense layout, voice/P2P off, CPU-only server. Server CPUs 2–9 / client 10–15 / coordinator 0–1; Rayon 4 / server Tokio 4 / client Tokio 4; avatar flush lanes 0. AC power and performance mode before/after; no overlapping native benchmark or build.

| Process-run median | Batching off | Batching on |
|---|---:|---:|
| Scene receipt | 22.770% | 99.976% |
| Scene p50 histogram upper bound | 33,554.431 ms | 65.535 ms |
| Scene p95 histogram upper bound | 67,108.863 ms | 131.071 ms |
| Observed unique scene receipts | 853.524 /s | 3,748.328 /s |
| Offered source scene messages | 3,753.509 /s | 3,754.223 /s |
| Avatar p95 applied-update gap | 522.340 ms | 483.430 ms |
| Observed applied avatars | 1,928.675 /s | 2,103.442 /s |
| Built logical avatar work | 1,442,207.246 /s | 1,574,988.125 /s |
| Server CPU | 6.057 cores | 5.333 cores |
| Client CPU | 1.509 cores | 1.970 cores |
| Server UDP transmit | 2,249.503 Mbit/s | 4,567.206 Mbit/s |

| Run | Receipt | Scene p95 upper bound | Avatar p95 gap | Failed original gates |
|---|---:|---:|---:|---|
| 01-baseline | 22.775% | 67,108.863ms | 520.750ms | server_exit, zero_retransmits, scene_delivery, scene_quality |
| 02-candidate | 100.000% | 131.071ms | 486.070ms | none |
| 03-candidate | 99.953% | 131.071ms | 480.790ms | none |
| 04-baseline | 22.765% | 67,108.863ms | 523.930ms | server_exit, scene_delivery, scene_quality |

Strict valid processes: 2/4. Histograms are latency bounds, not exact percentile measurements. All process runs and checks remain in the evidence.

## 20-Hz matched comparison

750 clients, 128 additional avatar bytes, 128 scene bytes; scene interval 50 ms. Avatar updates 20 ms, Unity-policy 60 FPS poses, amplitude 20, dense layout, voice/P2P off, CPU-only server. Server CPUs 2–9 / client 10–15 / coordinator 0–1; Rayon 4 / server Tokio 4 / client Tokio 4; avatar flush lanes 0. AC power and performance mode before/after; no overlapping native benchmark or build.

| Process-run median | Batching off | Batching on |
|---|---:|---:|
| Scene receipt | 5.681% | 46.025% |
| Scene p50 histogram upper bound | 33,554.431 ms | 16,777.215 ms |
| Scene p95 histogram upper bound | 67,108.863 ms | 33,554.431 ms |
| Observed unique scene receipts | 851.236 /s | 6,895.943 /s |
| Offered source scene messages | 15,003.485 /s | 15,002.999 /s |
| Avatar p95 applied-update gap | 521.125 ms | 511.940 ms |
| Observed applied avatars | 1,997.333 /s | 1,959.883 /s |
| Built logical avatar work | 1,497,181.619 /s | 1,470,872.927 /s |
| Server CPU | 6.066 cores | 7.236 cores |
| Client CPU | 1.569 cores | 2.454 cores |
| Server UDP transmit | 2,208.753 Mbit/s | 6,724.376 Mbit/s |

| Run | Receipt | Scene p95 upper bound | Avatar p95 gap | Failed original gates |
|---|---:|---:|---:|---|
| 01-baseline | 5.691% | 67,108.863ms | 496.080ms | server_exit, zero_stale_peers_500ms, scene_delivery, scene_quality |
| 02-candidate | 46.193% | 33,554.431ms | 542.470ms | scene_delivery, scene_quality |
| 03-candidate | 45.858% | 33,554.431ms | 481.410ms | scene_delivery, scene_quality |
| 04-baseline | 5.672% | 67,108.863ms | 546.170ms | server_exit, zero_retransmits, scene_delivery, scene_quality |

Strict valid processes: 0/4. Histograms are latency bounds, not exact percentile measurements. All process runs and checks remain in the evidence.

Across all eight comparison processes, 940 sampled observations retained 750 authenticated players and active avatar states. Both 5-Hz batching processes passed every original gate. All four baseline processes exceeded the server shutdown deadline; all four batching processes exited cleanly. Scene cadence stayed within 0.18% of its requested rate. Every run had zero scene send errors, backpressure skips, malformed messages, duplicates or out-of-order observations, and zero sampled transport would-block/dropped-datagram counters. Cumulative retransmit failures remain flagged; those counters did not increase during measurement.

## How to reach a useful range

For this synthetic all-peer 750-client case, the measured starting point is **2-ms batching with a 200-ms scene interval (5 Hz)**. It keeps avatar input unchanged and targets ≥95% scene receipt. Treat the two process runs as a screening result, not a universal configuration; validate actual scene content, bursts and churn before use. Publishing at 5 Hz permits up to 200 ms between updates before relay latency is added.

If 20-Hz scene freshness is required, reduce recipients through scene/interest membership using the existing recipient list. For example, 32 recipients per source reduces this offered scene fanout from 11.235 million to 480,000 messages/s (491.52 Mbit/s of payload). This is a budget calculation, **not a measured 32-recipient result**, and excludes avatars and framing. The current batching prototype applies only to empty-recipient broadcasts; explicit-recipient traffic retains the original path. Benchmark the real interest distribution before adopting a recipient budget.

Retaining all 749 recipients at 20 Hz requires substantially more capacity or another message-preserving transport/fanout optimization. The observed batching result still needs roughly 2.1 times its receipt throughput to approach 95%. Candidate follow-up work is a measured recipient-snapshot/grouping optimization or platform-native multi-datagram submission; neither is implemented here. Do not generically replace scene messages with newer ones: arbitrary script events may require every message.

## Verification and accounting limitation

117 server-core tests passed, including four new tests with real UDP captures for both merge formats, exact envelope conservation, source exclusion, session-incarnation rejection, bounded-queue waiting, drain and fallback eligibility. Two Rust-client live scene tests passed with batching enabled: exact unreliable/reliable relay and long-interval shutdown. The new module formatting and `git diff --check` passed. The ordinary release build succeeded with Rust 1.99.0 / LLVM 23.1.1, `--locked --release --target x86_64-unknown-linux-gnu`, incremental 0, jobs 2 and common flags `-Cllvm-args=-pgo-warn-missing-function`.

CodeRabbit completed a review of both changed server files with **one minor finding**: batched SCENE is excluded from legacy `appMessages.outbound`. This remains an explicit experimental limitation. The transport API reports successful datagrams and may return zero/partial success; incrementing every logical message on `Ok(datagrams)` would overstate work. Periodic `SceneBatch` status reports attempted recipient messages, submitted datagrams, stale messages, backpressure and errors separately. The A/B uses observed client receipts and raw transport counters, not legacy outbound totals. Before promoting this path, extend transport outcomes to count logical messages actually submitted, including partial errors, and restore consistent app accounting.

## Evidence and limits

Ordinary server SHA256: `2454de0ee8de5b3e7005487a7d5ffa421fb27f4154a1b493d9e76cde784b03c0`. Server source tree: `547f557d1a0f68b367a2b5f00fadc24ca83fce63`. Parent revision: `473e108c3f10f711b18abb6c4bdfd87465d1af76`. Main checkpoint remains `4ce5f8ca5862254b5e61805c856e9b0ed5e7aba9`.

[Portable evidence](results/scene-receipt-investigation-20261010-summary.json) retains raw observer/sender/server CSVs, process/health samples, commands, all strict flags and failures, source/binary/config/tool hashes, the diagnostic event CSV/perf summary, build metadata, review output and controller source. Local raw artifacts, including perf.data and frozen binaries, are under `captures/scene-receipt-investigation-20261010`.

One laptop, two independent processes per arm, loopback and one observed receiver. This synthetic scene relay benchmark does not include Unity script execution, asset loading, physics or ownership. The large-run avatar observer checks application/errors; exact additional avatar-byte preservation is covered by existing client tests. Receipt counts cannot separate all UDP loss from messages left in a backlog at shutdown. No universal capacity or PGO benefit is established.
