# Per-client application upload budget: 750-client A/B — 2026-10-10

`MaxUploadBytesPerSecondPerPlayer` now defaults to **131072 B/s (128 KiB/s)** for each authenticated connection, before fanout. The XML and environment override have the same exact name; 0 disables admission. The existing Scene after-fanout Mbps limit remains separate and defaults to 0. The optional pending-message quota and 3-ms batching are also separate, default-off controls.

Upload charging includes each application payload and its envelope once, including avatar additions, Scene, voice and other bulk application messages. It occurs before realtime coalescing or ordinary relay. Transport headers/retransmissions and protocol/auth/control messages are excluded. Buckets belong to connection incarnations, so recycled peer IDs and concurrent ingress do not share or overspend another connection’s budget.

The burst is two seconds (256 KiB at the default). Excess best-effort data is rejected and counted. Excess reliable data explicitly disconnects that connection rather than silently dropping an acknowledged application message. One reliable frame larger than the burst can be admitted from a full bucket and its full bytes charged as debt. Subsequent smaller data can resume as the debt is repaid; another oversized frame needs a full bucket. This preserves protocol-valid large resource frames without letting repeated oversized frames bypass the sustained rate. It is admission control, not a pacing queue; reliable clients must pace uploads to avoid disconnects.

`BasisImagePickupManager` Scene traffic remains exempt from the generic upload budget. Rust advertises the separate image settings for client pacing but currently has no C#-style server image governor. This exemption is therefore not a server-enforced image rate guarantee. The measurement uses extra avatar and ordinary Scene data, not photo chunks; focused ingress tests cover the image exemption.

## C# defaults and image fanout

At the frozen compatible C# reference `b28f78f1d2885fd21c4e5ad53307fedf3662fba2`, ordinary Scene `MaxSceneRelayMegabitsPerSecondPerPlayer` defaults to **0 (disabled)**. This existing limiter charges source bytes after multiplying by relay recipient count. It is not the requested each-client upload budget.

Photos/images have a separate default of **200 Mb/s per sharer**, charged against aggregate server relay egress. The server’s default enforcement factor is 150%, giving 300 Mb/s with a two-second burst; cached replay has a separate 200 Mb/s per joining peer allowance. Client image pacing accounts for one relay cohort plus direct P2P peers, while server relay accounting multiplies bytes by relayed recipients. Exact frozen configuration, relay and governor excerpts and Git blob hashes are retained in the evidence. C# was not rerun for this configuration A/B.

## Matched results

Four fresh process pairs run **disabled/enabled/enabled/disabled (ABBA)**. Both arms share one newly built ordinary binary, 3-ms Scene batching and a 1,024 pending-message recipient quota. Only the upload-rate override changes. Each process authenticates and retains all 750 active clients. The 5-Hz Scene rate is explicit; this does not establish success at the original 20-Hz capacity requirement.

| Process median | Disabled (0) | 128 KiB/s (131072) |
| --- | ---: | ---: |
| Scene receipt | 100.000% | 99.868% |
| Scene p95 histogram upper bound, ms | 131.071 | 131.071 |
| Avatar p95 applied gap, ms | 476.100 | 494.425 |
| Applied avatar items/s | 2,109.683 | 2,041.025 |
| Server CPU cores | 5.440 | 5.448 |
| Server peak RSS, MiB | 157.3 | 159.6 |
| Raw UDP transmit, Mbit/s | 4,515.6 | 4,531.2 |
| Accepted upload bytes/client/s | 9,130.8 | 9,130.6 |
| Rejected upload messages/s | 0 | 0 |
| Rejected Scene recipient messages/s | 0 | 0 |
| Maximum pending Scene messages/recipient | 749 | 749 |
| Every strict gate passed | 2/2 | 2/2 |

Median CPU changes +0.15% and avatar p95 applied gap +3.85%. Baseline avatar p95 spans 431.680–520.520 ms, bracketing both candidate values. This is descriptive screening evidence; two processes per arm do not establish a regression or a speedup.

Tables are medians of two processes per arm, not pooled percentiles. Scene receipt includes all offered observer messages and receive grace; Scene latency covers received messages only. Upload/rejection and CPU metrics use the same fixed 60-second window. Every original receipt/quality/error gate remains, including the absolute-zero cumulative retransmit gate. Added native gates require correct upload settings, positive admitted work, zero upload/Scene admission rejection, the recipient queue bound, zero relay errors and no image/oversized-frame exemption in this workload.

| Process | Scene receipt | Scene p95 bound, ms | Avatar p95, ms | CPU cores | Upload bytes/client/s | Scene senders / 749 | Retransmits first → last | Failed gates |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 01-baseline | 100.000% | 131.071 | 431.680 | 5.414 | 9,130.8 | 749 | 0 → 0 | none |
| 02-candidate | 99.897% | 131.071 | 483.530 | 5.441 | 9,129.8 | 749 | 0 → 0 | none |
| 03-candidate | 99.839% | 131.071 | 505.320 | 5.454 | 9,131.4 | 749 | 0 → 0 | none |
| 04-baseline | 100.000% | 131.071 | 520.520 | 5.465 | 9,130.8 | 749 | 0 → 0 | none |

## Controls, verification and limits

- 750 colocated Rust clients; avatar updates every 20 ms, Unity-policy 60-FPS poses, amplitude 20; no jitter/spread; 128 extra avatar bytes and 128 unreliable Scene bytes every 200 ms. Voice/P2P off, CPU-only server. Frozen original CPU-only server/client fixtures. Server CPUs 2–9, client 10–15, coordinator 0–1; Rayon/server Tokio/client Tokio workers 4; avatar flush lanes 0. AC/performance profile checked throughout. No overlapping builds or benchmarks.
- Full population, 45-second avatar warmup, shared Scene/observer/diagnostic start marker, 60-second CPU/avatar window and two-second Scene receive grace, then clean shutdown. 1000 Mb/s is a reporting reference, not a shaper.
- Server SHA-256 `51b269579793691be0f29632fc6f0f899142474464ae4127292139c06a2fdb20`; exact server source tree `8b69b573f7d756c102765f54ffc421d7e48ef274`, parent `66c8909c1994691144983ae61404024227d7719c`. Rust 1.99.0 / LLVM 23.1.1, locked ordinary release for x86_64-unknown-linux-gnu, incremental off, two jobs, `-Cllvm-args=-pgo-warn-missing-function`, encoded flags unset. **PGO is off** for this changed source; older profiles are not reused.
- Frozen client SHA-256 `dffb4157a1259489b4555d38938b154f791c7e06a82275dca4fb97d14a5e62ca`. All tool/config/binary hashes, raw observer/sender/Scene CSV, native health/CPU/RSS samples, original checks and failures, commands and local controller sources are retained.
- Eight focused core ingress/concurrency/debt/session tests, configuration XML/environment roundtrip, health-schema test, console compilation, workspace formatting and three analyzer-integrity tests pass. Review found one minor status-output issue; it was fixed and covered by the ingress test. Fresh final CodeRabbit review completed with zero findings; the intervening reused review is retained separately and is not claimed as fresh validation.
- `extended.uploadBudget` exposes accepted/rejected messages and bytes, reliable rejection, oversized reliable admission and image exemption. Counters remain active even when extended health collection is disabled. Upload status text respects verbose/extended metrics settings.
- The nonbinding workload tests the cost and behavior of enabled admission below the limit. Deterministic tests verify actual burst/refill/overflow enforcement; this does not measure photo fanout, rate-abusing peers, internet packet loss or a general capacity guarantee. One host on loopback and one receiver observer limit inference. Existing batched Scene app-output accounting limitation remains.

[Portable evidence](results/client-upload-limit-client750-20261010-summary.json). Local captures: `captures/client-upload-limit-client750-20261010`.
