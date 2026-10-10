# Rust 3-ms scene batching: 750-client interval A/B — 2026-10-10

`BASIS_SCENE_BATCH_MS=3` is now supported, matching the numeric default of the C# reference’s `MergeHoldMs=3`. Unset/0 still disables the experimental relay; 1 and 2 ms remain supported. This change only extends accepted configuration and tests. The collector, queue bounds, recipient lanes, transport format and eligible traffic are unchanged.

At 20 Hz, median scene receipt is 46.266% with 2 ms and 46.249% with 3 ms. Both remain below the original 95% target; this does not establish a capacity improvement. At 5 Hz, 3-ms receipt is 100.000%, with 2/2 processes passing every original gate. The reduced scene rate is a freshness tradeoff, not success at the original 20-Hz rate.

## Matched measurements

Each rate uses four fresh server/client process pairs in **2/3/3/2-ms ABBA order**, two per interval, with one newly built ordinary Rust server binary. The single A/B dimension is `BASIS_SCENE_BATCH_MS=2/3`. All original receipt, cadence, population, error, retransmit and shutdown checks remain enforced. Failed overload runs retain their invalid flags.

### Original 20-Hz scene workload

| Server / interval | Scene receipt | Scene p95 bound | Avatar p95 gap | Applied avatar items/s | Server CPU cores | Gates passed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rust 2 ms | 46.266% | 33,554.431 ms | 547.990 ms | 1,810.083 | 7.247 | 0/2 |
| Rust 3 ms | 46.249% | 33,554.431 ms | 562.640 ms | 1,735.183 | 7.245 | 0/2 |
| C# 3-ms hold | 15.108% | 1,048.575 ms | 12,470.935 ms | 55.450 | 5.533 | 0/2 |

### Separate 5-Hz scene workload

| Server / interval | Scene receipt | Scene p95 bound | Avatar p95 gap | Applied avatar items/s | Server CPU cores | Gates passed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rust 2 ms | 99.951% | 131.071 ms | 467.140 ms | 2,115.925 | 5.324 | 2/2 |
| Rust 3 ms | 100.000% | 131.071 ms | 460.875 ms | 2,165.858 | 5.316 | 2/2 |
| C# 3-ms hold | 71.188% | 1,048.575 ms | 2,631.810 ms | 608.475 | 4.833 | 0/2 |

Tables contain process medians, not pooled-message percentiles. The C# rows come from the earlier [C# capacity experiment](csharp-mixed-client750-20261010.md); they are a separate reference, not an interleaved cross-server A/B. C# pass counts cover available gates only; its missing Rust diagnostics remain unknown.

### Interval effect

| Scene rate | Receipt, 2 → 3 ms | Paired receipt changes | Avatar p95, 2 → 3 ms | Server CPU, 2 → 3 ms |
| --- | ---: | --- | ---: | ---: |
| 20 Hz | 46.266% → 46.249% | -0.191 percentage points, +0.157 percentage points | 547.990 → 562.640 ms | 7.247 → 7.245 cores |
| 5 Hz | 99.951% → 100.000% | +0.051 percentage points, +0.047 percentage points | 467.140 → 460.875 ms | 5.324 → 5.316 cores |

Two processes per interval/rate limit confidence; paired changes and original failure flags are retained rather than treating a single trial as proof. Lower CPU is not a capacity success if receipt or applied-avatar work declines. At 5 Hz both intervals pass every original gate; the near-ceiling receipt difference is small, and paired avatar-gap changes have opposite directions. These observations support using 3 ms to match the configured C# interval, not a general claim that 3 ms is faster.

## Every Rust process

| Scene rate | Run / interval | Receipt | Scene p95 bound | Avatar p95 gap | CPU cores | Strict failed gates |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 20 Hz | 01-baseline / 2 ms | 46.288% | 33,554.431 ms | 551.260 ms | 7.246 | scene_delivery, scene_quality |
| 20 Hz | 02-candidate / 3 ms | 46.097% | 33,554.431 ms | 523.280 ms | 7.242 | scene_delivery, scene_quality |
| 20 Hz | 03-candidate / 3 ms | 46.401% | 33,554.431 ms | 602.000 ms | 7.247 | zero_stale_peers_500ms, scene_delivery, scene_quality |
| 20 Hz | 04-baseline / 2 ms | 46.244% | 33,554.431 ms | 544.720 ms | 7.249 | zero_retransmits, scene_delivery, scene_quality |
| 5 Hz | 01-baseline / 2 ms | 99.949% | 131.071 ms | 500.700 ms | 5.333 | none |
| 5 Hz | 02-candidate / 3 ms | 100.000% | 131.071 ms | 450.500 ms | 5.316 | none |
| 5 Hz | 03-candidate / 3 ms | 100.000% | 131.071 ms | 471.250 ms | 5.315 | none |
| 5 Hz | 04-baseline / 2 ms | 99.953% | 131.071 ms | 433.580 ms | 5.314 | none |

| Scene rate | Run | Stale avatar peers at 500 ms | Scene out-of-order | Retx first → last | Scene input messages/s |
| --- | --- | ---: | ---: | --- | ---: |
| 20 Hz | 01-baseline | 0 | 0 | 0 → 0 | 15,002.941 |
| 20 Hz | 02-candidate | 0 | 1 | 0 → 0 | 15,005.094 |
| 20 Hz | 03-candidate | 749 | 1 | 0 → 0 | 15,001.590 |
| 20 Hz | 04-baseline | 0 | 0 | 2 → 2 | 15,002.810 |
| 5 Hz | 01-baseline | 0 | 0 | 0 → 0 | 3,756.456 |
| 5 Hz | 02-candidate | 0 | 0 | 0 → 0 | 3,751.273 |
| 5 Hz | 03-candidate | 0 | 0 | 0 → 0 | 3,751.277 |
| 5 Hz | 04-baseline | 0 | 0 | 0 → 0 | 3,756.804 |

These counts keep the original quality failures visible even when receipt dominates the result. A nonzero cumulative retransmit count is still a failed absolute-zero gate when it is constant during measurement. Scene source cadence, sender coverage and per-run error details are retained alongside observed delivery.

## Controls and provenance

- Same existing dense avatar workload: 750 colocated clients, 20-ms updates, Unity-policy 60-FPS poses, 128 additional avatar bytes, 128-byte unreliable scene broadcasts to every other client, voice/P2P off, CPU-only server processing. Only the separate 5-Hz cohort changes the scene interval from 50 to 200 ms.
- Same frozen Rust client SHA-256 `dffb4157a1259489b4555d38938b154f791c7e06a82275dca4fb97d14a5e62ca` and original CPU-only server/client fixture bytes used by both preceding Rust and C# experiments.
- Server CPUs 2–9, client CPUs 10–15, coordinator CPUs 0–1; Rayon/server Tokio/client Tokio workers 4; avatar flush lanes 0. AC/performance profile is checked before every run and at cohort completion. Each cohort freezes its selected UDP/health ports.
- Complete authenticated and active-avatar population, then 45-second avatar warmup. Shared marker starts scenes/observers/diagnostics; CPU/avatar measurement is fixed at 60 seconds. Scene normalization uses actual Scene CSV duration, including two-second observer receive grace. The 1000-Mbps capacity reference is not a link shaper.
- New Rust server SHA-256 `7d716085d02c6dd15669a40fb14fe5e0e1afddf27d6c1a78ef64558e43ed8a74`, production server source tree `9f1c67af77c49461a5ad486a0e2fd94943fad7e0`, parent revision `1414df5bf8946d2aa223f9d235c54db31b058e57`. An exact staged-tree source snapshot is built with Rust 1.99.0 / LLVM 23.1.1, locked ordinary release, explicit x86_64-unknown-linux-gnu target, incremental off, two build jobs, `-Cllvm-args=-pgo-warn-missing-function`, encoded flags unset. PGO is **off**; older profiles are not applied to changed source. Source, tool and frozen binary/config hashes are retained.

## What matching 3 ms means

At frozen C# revision `b28f78f`, `LNLTransportConfig.cs:125` defaults `MergeHoldMs` to 3. `NetPeer.cs:1976–1991` accumulates elapsed flush-pass time for a partial per-peer merged transport buffer, flushing at the hold or immediately on MTU overflow. The verified C# sidecars also record 3 ms.

Rust uses 3 ms as the collection deadline after the first queued eligible scene input. The existing 128-message/64-KiB limits can finish collection earlier; recipient grouping, bounded fanout queues and socket processing follow. This is not a 3-ms end-to-end latency guarantee or identical cross-server scheduling. Reliable, directed and explicit-recipient scene traffic retains the existing path.

The original default-off prototype’s app-output accounting limitation remains: batched scenes are not included in the legacy application outbound counter. Actual client receipts and raw transport statistics are used here. Collector/recipient counters and their shutdown samples are retained, including work that becomes stale after clients disconnect.

## Verification and limits

The four existing scene relay tests pass. Configuration coverage now checks 1, 2 and 3 ms, rejects 4 ms and garbage, and retains disabled/ineligible checks. The real UDP conservation test runs at 3 ms for both Merged and CompactMerged, preserving payloads and source exclusion. The other tests cover bounded-queue backpressure/drain and recycled-session exclusion. Module rustfmt and staged diff checks pass; CodeRabbit completed the changed module review with zero findings.

[Portable evidence](results/scene-batch3-client750-20261010-summary.json) includes every trial, summary rederivation, raw observer/sender/scene CSV and native health/CPU/RSS samples, commands, source/build/tool hashes, original strict flags, relay counters, retained controller sources and review/test output. Local captures: `captures/scene-batch3-client750-20261010`.

This is one laptop on loopback with one receiver observer. Received scene latency percentiles are histogram upper bounds and omit messages that never arrive; avatar gaps are applied-update intervals, not end-to-end pose latency. C# uses .NET 10.0.12 server GC/tiered PGO and normal adaptive workers; this reference comparison is not a compiler PGO or equal-worker experiment.
