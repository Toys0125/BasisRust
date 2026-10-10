# Per-recipient Scene pending-work limit: 750-client A/B — 2026-10-10

Rust now supports `BASIS_SCENE_PEER_QUEUE_LIMIT` (any positive integer) as a hard bound on pending logical Scene messages per authenticated recipient connection. It applies to the existing experimental batched unreliable empty-recipient Scene broadcast path. Unset/0 leaves this new quota disabled; `BASIS_SCENE_BATCH_MS` remains disabled by default and must be enabled separately. Both tested arms use 3-ms batching. Direct UDP sends, reliable/directed Scene traffic and the existing per-source byte-rate limiter retain their existing behavior.

The quota reserves each recipient independently before admission to the shared inbox. A full recipient is refused while recipients with available capacity continue. A fully refused input does not wait for the inbox. Each reservation belongs to its connection incarnation and stays pending through inbox, collector, fanout lane and socket submission. Ownership releases it on send, stale discard, error, cancellation or queue teardown. It cannot consume or release capacity on a later connection that reuses the same numeric ID. Shared collector/lane backpressure remains; this bounds pending work but does not guarantee independent peer scheduling or end-to-end latency.

## Interpretation and configuration

A 70-message logical quota is too small for this synchronized 750-client workload: it rejects most Scene work and loses source coverage at both rates. Lower CPU and shorter received-message latency are therefore load shedding, not a speedup.

At 5 Hz, the 1,024 limit keeps all 749 senders, peaks at 749 pending messages and records zero quota rejections in both repeats. It passes Scene receipt/quality checks in both; one process passes every original gate and the other fails only absolute-zero retransmits (2 already present at first sample, unchanged during measurement). Keep that strict failure visible: this is promising bounded-work screening evidence, not complete validation. The quota remains opt-in with default 0.

The measured experimental combination is `BASIS_SCENE_BATCH_MS=3` and `BASIS_SCENE_PEER_QUEUE_LIMIT=1024` at a 200-ms Scene interval (5 Hz). The 1,024 limit has not been measured at 20 Hz; neither this result nor the 70-limit comparison establishes success at the original 20-Hz capacity requirement. No further tuning iteration is claimed here.

## Matched measurements

The 20-Hz cohort uses four fresh process pairs in **0/70/70/0 ABBA order**. After rejecting 70 as a capacity choice, the 5-Hz cohort uses six fresh pairs in **0/70/1024/1024/70/0 order**, two per arm. Each 0/70 and 0/1024 subset is ABBA. 1024 is predeclared as one 749-message burst plus 275 headroom, a heuristic rather than an optimum. One new ordinary server binary is shared between arms; the sole A/B dimension is `BASIS_SCENE_PEER_QUEUE_LIMIT`. Every original gate is retained, with additional mandatory native admission telemetry, hard-bound, zero-rejection, zero-relay-error and zero-stale gates. Intentional rejections remain losses even if receipt happens to clear 95%.

### Original 20-Hz Scene workload

| Process median | Unlimited | Hard 70 |
| --- | ---: | ---: |
| Scene receipt | 45.725% | 11.809% |
| Scene p95 histogram bound, ms | 33,554.431 | 32.767 |
| Avatar p95 applied gap, ms | 562.550 | 462.800 |
| Applied avatar items/s | 1,760.150 | 2,190.825 |
| Server CPU cores | 7.310 | 4.573 |
| Server peak RSS, MiB | 244.0 | 140.8 |
| Raw UDP transmit, Mbit/s | 6,684.8 | 2,946.2 |
| Admitted recipient Scene messages/s | 5,158,222 | 1,289,417 |
| Rejected recipient Scene messages/s | 0 | 9,942,740 |
| Maximum pending per recipient | 1,567 | 70 |
| All strict gates passed | 0/2 | 0/2 |

### Separate 5-Hz Scene workload

| Process median | Unlimited | Hard 70 | Hard 1024 |
| --- | ---: | ---: | ---: |
| Scene receipt | 99.866% | 11.365% | 99.949% |
| Scene p95 histogram bound, ms | 131.071 | 32.767 | 131.071 |
| Avatar p95 applied gap, ms | 472.555 | 443.725 | 486.650 |
| Applied avatar items/s | 2,103.442 | 2,190.825 | 2,034.783 |
| Server CPU cores | 5.417 | 3.390 | 5.397 |
| Server peak RSS, MiB | 161.0 | 135.7 | 159.0 |
| Raw UDP transmit, Mbit/s | 4,523.0 | 2,222.5 | 4,557.3 |
| Admitted recipient Scene messages/s | 2,807,646 | 312,669 | 2,807,919 |
| Rejected recipient Scene messages/s | 0 | 2,495,432 | 0 |
| Maximum pending per recipient | 749 | 70 | 749 |
| All strict gates passed | 2/2 | 0/2 | 1/2 |

Receipt uses all offered observer messages, including messages refused by the server. Scene latency covers only received messages; lower latency alongside lower receipt is an overload tradeoff, not a capacity improvement. CPU is likewise not accepted as a speedup if work is discarded. Tables contain process medians, not pooled percentiles. Two trials per arm/rate are descriptive screening evidence, not a universal capacity guarantee.

## Every process and original failure

| Rate | Run | Receipt | Scene p95 bound, ms | Avatar p95, ms | Rejected recipient messages/s | Peak peer pending | Failed gates |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 20 Hz | 01-baseline | 45.847% | 33,554.431 | 576.720 | 0 | 1,567 | zero_retransmits, scene_delivery, scene_quality |
| 20 Hz | 02-candidate | 11.889% | 32.767 | 453.140 | 9,932,406 | 70 | scene_delivery, scene_quality, zero_scene_peer_rejections |
| 20 Hz | 03-candidate | 11.728% | 32.767 | 472.460 | 9,953,075 | 70 | scene_delivery, scene_quality, zero_scene_peer_rejections |
| 20 Hz | 04-baseline | 45.604% | 33,554.431 | 548.380 | 0 | 1,567 | zero_retransmits, scene_delivery, scene_quality |
| 5 Hz | 01-baseline | 100.000% | 131.071 | 484.970 | 0 | 749 | none |
| 5 Hz | 02-small_limit | 11.235% | 32.767 | 458.760 | 2,495,539 | 70 | scene_delivery, scene_quality, zero_scene_peer_rejections |
| 5 Hz | 03-candidate | 100.000% | 131.071 | 456.360 | 0 | 749 | none |
| 5 Hz | 04-candidate | 99.897% | 131.071 | 516.940 | 0 | 749 | zero_retransmits |
| 5 Hz | 05-small_limit | 11.494% | 32.767 | 428.690 | 2,495,324 | 70 | zero_retransmits, scene_delivery, scene_quality, zero_scene_peer_rejections |
| 5 Hz | 06-baseline | 99.733% | 131.071 | 460.140 | 0 | 749 | none |

| Rate | Run | Scene senders observed / 749 | Offered source Scene messages/s | Scene out of order | Stale avatar peers, 500 ms | Retransmits first → last |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 20 Hz | 01-baseline | 749 | 15,010.026 | 0 | 0 | 16 → 16 |
| 20 Hz | 02-candidate | 512 | 15,003.054 | 0 | 0 | 0 → 0 |
| 20 Hz | 03-candidate | 573 | 15,003.399 | 0 | 0 | 0 → 0 |
| 20 Hz | 04-baseline | 749 | 15,008.154 | 0 | 0 | 22 → 22 |
| 5 Hz | 01-baseline | 749 | 3,750.357 | 0 | 0 | 0 → 0 |
| 5 Hz | 02-small_limit | 505 | 3,751.008 | 0 | 0 | 0 → 0 |
| 5 Hz | 03-candidate | 749 | 3,750.262 | 0 | 0 | 0 → 0 |
| 5 Hz | 04-candidate | 749 | 3,756.737 | 0 | 0 | 2 → 2 |
| 5 Hz | 05-small_limit | 494 | 3,750.763 | 0 | 0 | 12 → 12 |
| 5 Hz | 06-baseline | 749 | 3,761.313 | 0 | 0 | 0 → 0 |

The portable evidence retains `original_checks` and `original_valid` separately from the stricter combined result. Cumulative retransmit failures remain failures even when the count was already nonzero at measurement start. Rejection/admission rates use collection-time interpolation of native cumulative health counters over the same fixed 60-second CPU window; Scene receipt uses its actual CSV duration, including two seconds of receive grace.

## C# relationship

The compatible C# reference is frozen at `b28f78f1d2885fd21c4e5ad53307fedf3662fba2`. My earlier description of its 70-message Scene queue gate was incorrect. `NetworkServer.TrySendNoRecord` does contain a nominal 70-message check, but `NetPeer.GetPacketsCountInQueue` returns zero for Unreliable. That check therefore does not reject this workload’s unreliable Scene sends; it can affect Sequenced.

The actual C# bound is in `NetPeer.EnqueueUnreliable`: it counts queued pre-merge logical NetPacket sends across bulk unreliable channels per recipient, and drops oldest packets above `EffectiveUnreliableQueuePerPeer`, incrementing the native drop counter. Scene sends contribute one packet per recipient. Priority/voice has a separate queue. With configuration 0, the native resolver divides a 10% memory budget by 1432 bytes and population, clamping 512–8192. All four recorded 750-client processes expose `queuePerPeer=3118` throughout the sampled windows. Exact frozen source excerpts and hashes are retained in the new evidence.

Rust’s new bound instead counts pending recipient Scene messages across its entire batched pipeline and rejects newly offered work for a full recipient. Its 1,024 headroom trial is a declared workload heuristic, not a literal C# default. These are different stages/scopes and eviction policies, though both protect per-recipient pending capacity. Shared lanes and source unfairness under rejection remain limitations.

Rust explicitly exposes quota rejections in `extended.sceneBatch.rejectedRecipientMessages`, separate from raw UDP drops. The [earlier C# capacity check](csharp-mixed-client750-20261010.md) now corrects the 70 gate explanation; its measured results are unchanged and portable historical raw data remains untouched. C# was not rerun in this quota A/B.

## Fixed controls and exact source

- 750 colocated clients; 20-ms avatar updates, jitter 0/no spread; Unity policy 60-FPS poses, amplitude 20; 128 additional avatar bytes and 128 unreliable broadcast Scene bytes. Voice/P2P off, CPU-only server. Scene intervals 50 ms (20 Hz) and 200 ms (5 Hz) are separate cohorts.
- Frozen Rust client SHA-256 `dffb4157a1259489b4555d38938b154f791c7e06a82275dca4fb97d14a5e62ca`; original CPU-only fixtures. Server CPUs 2–9, client 10–15, coordinator 0–1; Rayon/server Tokio/client Tokio workers 4; avatar flush lanes 0. AC/performance profile verified each run and after each cohort. No overlapping builds or benchmarks.
- Full authenticated/active population, then 45-second avatar warmup; shared start marker for Scene/observer/diagnostics; 60-second fixed CPU/avatar window plus two-second receive grace and clean shutdown. 1000 Mbit/s is a reporting reference, not a shaper.
- New server SHA-256 `efa6da9d45e47d319a553115b66744e5c752c3766192a6267b897ed01502d0fb`, exact production server tree `92d7842659890dbf00ca7f5efcfef6af9a588709`, parent `a1bc7f2589246db61a6d2d0091977cb80e20e2c7`. Rust 1.99.0 / LLVM 23.1.1, locked ordinary release, x86_64-unknown-linux-gnu, incremental off, two build jobs, `-Cllvm-args=-pgo-warn-missing-function`, encoded flags unset. PGO is **off** for this changed source; old profiles are not reused.

## Implementation, verification and limits

Changes are in `scene_relay.rs` (reservation/ownership/configuration/counters), `ConnectedPeer` admission/test fixtures (fresh connection budget), `basis-server-health` (optional additive SceneBatch schema) and server console mapping. The new crate-private ConnectedPeer field means downstream code constructing this public struct with literals would need an API adaptation. The previous batched application outbound accounting limitation remains; receipts, explicit admission counters and raw transport bytes are measured here.

Eight focused relay tests, one health-schema test, console compilation, workspace formatting, diff checks and four analyzer-integrity tests pass. CodeRabbit completed five-file review with zero reported findings, but its outcome is completed_with_warnings with unverified findings; this is not an unqualified clean-review claim. Exact logs are retained in the evidence. Source/binary/config/tool hashes, all raw observer/sender/Scene CSV, native health/CPU/RSS samples, original failed gates, shutdown counters and exact local controller sources are retained. Native counters label logical attempts and submitted datagrams separately; neither claims client delivery.

[Portable evidence](results/scene-peer-limit-client750-20261010-summary.json). Local captures: `captures/scene-peer-limit-client750-20261010`. This is one Linux laptop on loopback with one receiver observer. Histograms give received-message latency upper bounds; avatar gaps are applied-update intervals, not end-to-end pose latency. No general performance claim or production promotion follows from this limited experiment.
