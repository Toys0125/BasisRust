# C# capacity check: 750 mixed scene/avatar clients — 2026-10-10

The frozen compatible C# reference **does not handle the requested 750-client, 20-Hz mixed workload** within the existing quality gates. Both fresh processes authenticate all 750 clients, maintain offered input, and shut down cleanly, but fail scene receipt, avatar quality and native transport-drop checks. These are capacity observations, not a validated speedup or a compiler PGO comparison.

## 20-Hz scene workload

| Server | Scene receipt | Scene p95 upper bound | Avatar p95 gap | Applied avatar items/s | Server CPU cores |
| --- | ---: | ---: | ---: | ---: | ---: |
| Rust ordinary | 5.681% | 67,108.863 ms | 521.125 ms | 1,997.333 | 6.066 |
| Rust batch 2 ms | 46.025% | 33,554.431 ms | 511.940 ms | 1,959.883 | 7.236 |
| C# reference | 15.108% | 1,048.575 ms | 12,470.935 ms | 55.450 | 5.533 |

Medians of two independent processes per row. Rust results are from the preceding [Scene receipt experiment](scene-receipt-investigation-20261010.md), using the same frozen Rust client and workload. They were not interleaved with the C# runs; this is a sequential cross-server follow-up, not a randomized head-to-head A/B.

## 5-Hz follow-up

Only the scene interval changes from 50 to 200 ms. This is a separate capacity tradeoff; it does not satisfy the original 20-Hz requirement.

| Server | Scene receipt | Scene p95 upper bound | Avatar p95 gap | Applied avatar items/s | Server CPU cores |
| --- | ---: | ---: | ---: | ---: | ---: |
| Rust ordinary | 22.770% | 67,108.863 ms | 522.340 ms | 1,928.675 | 6.057 |
| Rust batch 2 ms | 99.976% | 131.071 ms | 483.430 ms | 2,103.442 | 5.333 |
| C# reference | 71.188% | 1,048.575 ms | 2,631.810 ms | 608.475 | 4.833 |

C# passes all available common/native gates in 0/2 runs at 5 Hz. Unavailable Rust-specific checks are excluded from that count and remain explicitly unknown. The earlier Rust batching prototype passes every original gate in both 5-Hz runs.

## Every C# process

| Scene rate | Run | Receipt | Scene p95 bound | Avatar p95 gap | CPU cores | Peak RSS MiB | Native unreliable counter/s | Strict failed gates |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 20 Hz | 01-csharp | 15.019% | 1,048.575 ms | 18,889.310 ms | 5.509 | 3,216.0 | 5,733,585 | zero_stale_peers_500ms, zero_unapplied_deltas, zero_discontinuities, zero_sequence_resyncs, zero_sequence_order_unknown_gaps, zero_transport_drops, scene_delivery, scene_quality |
| 20 Hz | 02-csharp | 15.196% | 1,048.575 ms | 6,052.560 ms | 5.557 | 3,257.2 | 5,796,719 | zero_stale_peers_500ms, zero_unapplied_deltas, zero_discontinuities, zero_sequence_resyncs, zero_sequence_order_unknown_gaps, zero_transport_drops, scene_delivery, scene_quality |
| 5 Hz | 01-csharp | 70.748% | 1,048.575 ms | 2,645.640 ms | 4.849 | 2,907.7 | 1,029,611 | zero_stale_peers_500ms, zero_unapplied_deltas, zero_discontinuities, zero_sequence_resyncs, zero_sequence_order_unknown_gaps, zero_transport_drops, scene_delivery, scene_quality |
| 5 Hz | 02-csharp | 71.628% | 1,048.575 ms | 2,617.980 ms | 4.817 | 2,748.9 | 990,738 | zero_stale_peers_500ms, zero_unapplied_deltas, zero_discontinuities, zero_sequence_resyncs, zero_sequence_order_unknown_gaps, zero_transport_drops, scene_delivery, scene_quality |

All four measured runs retain their original flags and full raw observations. Client/server exit codes are recorded individually; coverage, authenticated population, sender cadence, errors, duplicates and out-of-order messages are evaluated independently of receipt.

| Scene rate | Run | Avatar socket items/s | Scene input messages/s | Observed scene senders | Scene out-of-order | Scene max received latency | Client/server exits |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| 20 Hz | 01-csharp | 37,500.000 | 15,010.010 | 749 | 423 | 1,169.347 ms | 0/0 |
| 20 Hz | 02-csharp | 37,501.633 | 15,008.715 | 749 | 602 | 1,181.214 ms | 0/0 |
| 5 Hz | 01-csharp | 37,500.000 | 3,754.159 | 749 | 0 | 807.588 ms | 0/0 |
| 5 Hz | 02-csharp | 37,500.000 | 3,751.700 | 749 | 0 | 809.920 ms | 0/0 |

## Controls and provenance

- C# source `b28f78f1d2885fd21c4e5ad53307fedf3662fba2`, compatible protocol v55. The prior build manifest ties this revision to apphost SHA-256 `53e0c34af88b3aeba3b7f3948b4247b2671c33c8a3e8028733a8220ee9f415ce`. All 42 published runtime files are frozen and each run’s copied files are verified. This is the established C# reference, not a rebuild of the latest C# checkout.
- Actual loaded runtime is .NET **10.0.12**, verified from each process’s mapped assemblies/native runtime. The runtimeconfig requests net10.0/10.0.0 with concurrent server GC, dynamic adaptation, tiered compilation and tiered PGO enabled. Inherited `DOTNET_`/`COMPlus_` overrides are cleared. C# receives no Rust worker, batching or diagnostic environment variables.
- Rust client SHA-256 `dffb4157a1259489b4555d38938b154f791c7e06a82275dca4fb97d14a5e62ca`. 750 colocated clients, 20-ms movement updates, Unity-policy 60-FPS poses, 128 additional avatar bytes, 128-byte unreliable scene broadcasts to every other client, voice/P2P off, CPU-only server processing. Client options and observer gates are unchanged.
- Server CPUs 2–9, client CPUs 10–15, coordinator CPUs 0–1; client Tokio workers 4. C# retains its normal adaptive worker counts; the Rust reference uses Rayon/Tokio workers 4. Affinity is matched; worker semantics are not claimed identical. AC power and performance profile are checked before/after each experiment.
- Fresh isolated server/config per run. After complete authenticated population, warm avatar processing for 45 seconds; start scenes and observations on the shared marker. CPU/avatar windows are 60 seconds, with the same two-second receive grace. Scene rates normalize against the actual approximately 62-second Scene CSV window. The 1000-Mbps reference does not shape loopback traffic.
- Offered 20-Hz broadcast fanout is 11.235 million recipient messages/s and 11.505 Gbit/s of scene payload before framing/avatars. At 5 Hz it is 2.809 million recipient messages/s and 2.876 Gbit/s.

## Interpretation and instrumentation limits

C#’s default LiteNetLib configuration merges eligible unreliable packets with a 3-ms hold. It also changes avatar broadcast rate/tier/slicing under pressure. The raw server diagnostics retain queue drops, slicing, worker/socket adjustments, GC observations and host-wide UDP warnings. Lower CPU or shorter latency among surviving scene messages does not establish capacity when receipt and applied-avatar work collapse.

The native `droppedUnreliable` counter covers transport behavior, not all application-level rejection. `TrySendNoRecord` can reject unreliable/sequenced work when the channel queue exceeds its normal 70-message gate; that loss is not necessarily reflected in this counter. Native drop rates are reported counter units, not an attribution to scene packets. Host-wide UDP warning counts are not process-specific. Actual client receipt remains the capacity test.

C# does not emit Rust’s built-recipient avatar counter, per-tick/build/flush costs, protocolErrors, wouldBlock, retransmits or continuous avatar-state diagnostic. Those metrics/checks remain null with reasons. This is not full Rust diagnostic equivalence. Received Scene p95/p99 values are histogram upper bounds and omit messages that never arrive; avatar gaps are intervals between applied updates at one receiver, not end-to-end pose latency.

## Configuration audit and verification

The original four-client preflight delivered 100% of scenes but failed the initial saved-config predicate. C# removes the 18 shared LiteNetLib fields from `config.xml` and writes them to its transport sidecar. Source inspection confirmed all supplied sidecar controls were preserved. Exactly three fixture fields are unsupported by this C# schema: `HealthIncludeExtendedMetrics`, `DisableReadUnlessAdminPersistentFlag` and `DisableWriteUnlessAdminPersistentFlag`. Native health is captured directly and synthetic avatar/scene traffic exercises no persistent-data operations. The analyzer now attests the sidecar and allows only those exact unsupported field/value omissions; every other changed/missing supplied control fails. All schema additions/omissions are retained.

The original failed preflight is preserved alongside a final-analyzer reanalysis, followed by a fresh verified preflight that passed every available gate. `BSRSIncreaseRate=0` stays in saved configuration; C# advertises 0.005 in server metadata and logs its existing normalization warning. The colocated workload minimizes distance-policy effects; the schema/runtime distinction remains a comparison limit.

Only benchmark tooling changes: `avatar_benchmark.py` adds an opt-in C# runner, `cross_server_avatar.py` analyzes common checks/native C# health, and `test_cross_server_avatar.py` covers exact readiness, schema/sidecar controls and unavailable telemetry. No production server/client code or default configuration changes. Focused Python suite: 30 tests run, 29 passed, one existing Windows-only skip. CodeRabbit reviewed all three scripts after the preflight fix and completed with zero findings. Summary rederivation, all copied runtime hashes and frozen artifacts are verified.

[Portable evidence](results/csharp-mixed-client750-20261010-summary.json) contains every attempt, strict flag, raw observer/sender/scene CSV, native health/CPU/RSS samples, commands, prepared/saved configuration, actual loaded-runtime hashes, source/build provenance, controller sources and review output. Full local captures are in `captures/csharp-mixed-client750-20261010`.

These are two fresh processes per rate on one loopback laptop with one receiver observer. They establish a failure for this C# reference/configuration on this host, not a universal C# limit. If 20-Hz freshness is required, recipient interest management remains the next workload change to measure. Keep every receipt/quality/error gate when evaluating C# tuning.
