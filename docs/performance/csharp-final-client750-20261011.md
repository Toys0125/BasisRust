# C# gate check with final Rust client: 750 clients — 2026-10-11

**C# passes 0/2 fresh 750-client runs at 5 Hz.** Both runs fail the common Scene delivery/quality, avatar quality and native unreliable-drop gates. Median Scene receipt is **71.584%**, below the unchanged 95% requirement. Scene p95 received-message latency is ≤524.287 ms; avatar p95 applied-update gap is 1729.560 ms. All 750 clients remain connected and both processes exit cleanly.

This uses the final ordinary Rust client from the preceding PGO experiment, with the exact same synthetic avatar/Scene input. C# has .NET tiered PGO enabled. The established protocol-v55 C# binary is reused; its entire tracked server source tree is identical to the newer local checkout. This is a fresh gate check, not a current C# rebuild or pull.

## Every measured run

| C# process | Scene receipt (≥95%) | Scene p95 upper bound | Avatar p95 applied gap | CPU cores | Peak RSS MiB | Native unreliable drops/s | All available gates |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 01-csharp | 73.054% | 524.287 ms | 1732.800 ms | 4.760 | 2,159.0 | 985,905 | fail |
| 02-csharp | 70.113% | 524.287 ms | 1726.320 ms | 4.830 | 2,243.2 | 1,034,046 | fail |

Medians of two independent fresh processes: applied avatar items/s 469.742, CPU 4.795 cores. Lower CPU does not establish better performance when receipt and applied work fall. Native drop rates are transport counter units, not a Scene-only packet attribution. Received-message histogram bounds exclude messages that never arrive. Avatar gaps measure applied-update intervals at one observer, not end-to-end pose latency.

| Quality counter / gate | Required | Run 1 | Run 2 |
| --- | ---: | ---: | ---: |
| Stale avatar peers (>500 ms) | 0 | 157 | 749 |
| Unapplied avatar deltas | 0 | 23066 | 16672 |
| Avatar discontinuities | 0 | 4725 | 4601 |
| Avatar sequence resyncs | 0 | 4568 | 4601 |
| Unknown sequence-order gaps | 0 | 586 | 1318 |
| Native unreliable drops (first → last) | 0 | 3642006 → 62804837 | 9993472 → 72042045 |

Both runs retain these exact failed flags: `zero_stale_peers_500ms`, `zero_unapplied_deltas`, `zero_discontinuities`, `zero_sequence_resyncs`, `zero_sequence_order_unknown_gaps`, `zero_transport_drops`, `scene_delivery`, `scene_quality`. The two Scene flags reflect the same insufficient receipt, rather than two independent capacity defects.

| Integrity / offered input | Run 1 | Run 2 |
| --- | ---: | ---: |
| Continuous authenticated clients | 750 | 750 |
| Observed Scene senders | 749 / 749 | 749 / 749 |
| Avatar socket items/s | 37499.983 | 37500.000 |
| Scene offered messages/s | 3754.596 | 3755.067 |
| Client / server exit codes | 0 / 0 | 0 / 0 |

The four-client preflight passes all available gates with 100% Scene receipt. It verifies protocol compatibility, the updated client, translated/saved controls, runtime provenance and observer behavior; it is excluded from the capacity medians. No quality or receipt gate was relaxed for the full runs.

## Matched input and comparison limits

- 750 colocated clients; 20-ms avatar movement updates, zero jitter/spread, Unity-policy 60-FPS poses, amplitude 20. Both avatar updates and readiness/full/keyframe/delta paths carry 128 additional bytes. Each client broadcasts 128 ordinary Scene bytes every 200 ms (5 Hz), unreliable to the other clients. Voice/P2P off; CPU-only server fixture.
- Same final client and exact server/client fixtures as the Rust PGO experiment. Full population precedes 45-second avatar warmup; a shared marker starts Scene traffic and observation. Fixed 60-second CPU/avatar window plus two-second Scene receive grace. Scene rates use actual observer duration.
- Server CPUs 2–9, client CPUs 10–15, coordinator CPUs 0–1; client Tokio workers 4. AC power/performance profile checked before/after and for each run. Native runs and checks execute sequentially.
- C# retains normal adaptive workers, GC and transport controls, including default 3-ms eligible unreliable merge hold. Both native captures report per-peer bulk unreliable queue 2038. Rust’s experimental 1024 logical Scene quota and 131072-B/s generic before-fanout upload budget are not applied to C#. No equivalent control semantics are claimed.
- C# drops the oldest pre-merge bulk-unreliable work over its adaptive native per-peer cap. This is distinct from Rust’s opt-in logical Scene admission quota. The supplied controls and schema/sidecar translations are attested; exactly the established three unsupported fixture omissions remain documented.
- For context, the preceding [Rust PGO test](pgo-final-client750-20261011.md) used this same input/client and measured 99.924% Scene receipt, ≤131.071-ms Scene p95 bound and 494.775-ms avatar p95 gaps. All four PGO evaluations passed, while its training profile remained diagnostic due to setup retransmits. C# runs are sequential afterward, not interleaved cross-server A/B. Worker, queue and admission semantics differ.
- The gate verdict covers available common/native checks. **17 Rust diagnostic/admission/image checks remain unavailable, stored as null with reasons.** C# does not expose Rust’s retransmit/protocol/would-block/tick diagnostics, continuous avatar-state counter, upload-budget or logical Scene-admission telemetry, or matching image counters. Missing telemetry is not a pass or a zero value. Observed common failures already establish that C# does not pass the requested gates.

## Exact build, runtime and checks

- C# frozen build source: `b28f78f1d2885fd21c4e5ad53307fedf3662fba2`, protocol 55; apphost SHA-256 `53e0c34af88b3aeba3b7f3948b4247b2671c33c8a3e8028733a8220ee9f415ce`. Every one of 42 frozen/copied runtime files is hashed and verified per attempt.
- Local C# checkout `3555d5a04ac15430ccaea2f722211b55fa539a48` has identical tracked `Basis Server` tree `7f942bf708745c1d6af70c2a5b3f0a5dba974d07`. This verifies unchanged server source relative to the frozen reference; it does not claim a new build or latest upstream C# revision.
- Final shared Rust client SHA-256 `a394105596e06783b18860311a7af3c6837ba1776dc3e55cb9661a5a8515983b`, exact source tree `6462935e8ea266778748065bad43ea0079c838ad`. Its Rust 1.99.0 release build provenance is retained.
- Actual loaded runtime .NET 10.0.12 is verified from each process’s mapped native runtime/assemblies and hashes. Concurrent server GC, dynamic adaptation, tiered compilation and tiered PGO are enabled in the frozen runtimeconfig. Inherited DOTNET/COMPlus and LLVM profile overrides are cleared. Rust server worker/batch/upload settings are not sent to C#.
- Three focused adapter tests pass: receipt failure preserved, native-drop failure preserved, and unavailable diagnostics stay null without claiming full Rust-gate equivalence. Benchmark engine and common C# analyzer are unchanged from their prior verified tests/review. No new production unit-test or CodeRabbit run is claimed for this report-only follow-up.
- Evidence retention independently rederives preflight and both full observations, validates exact runtime/client/config/tool hashes, confirms source audit, and retains every original failure and unavailable check. No production source or default configuration changes.

## Reproduce

```sh
python3 -B captures/csharp-final-client750-20261011/run-csharp.py \
  --output captures/csharp-final-client750-20261011/evaluation-fresh \
  --clients 750 --runs 2 --scene-interval-ms 200
```

Local capture root: `captures/csharp-final-client750-20261011`. [Portable evidence](results/csharp-final-client750-20261011-summary.json) retains every attempt, strict flag, unavailable check, raw observer/sender/Scene CSV, native health/CPU/RSS samples, commands, prepared/saved configuration, actual loaded-runtime and binary hashes, source/build provenance and local controller sources.

Two fresh processes on one loopback laptop and one observer establish failure for this tested C# reference/configuration. They do not establish a universal C# capacity limit, 20-Hz or photo-transfer capacity. The 1000-Mbit/s reference is informational, not a traffic shaper.
