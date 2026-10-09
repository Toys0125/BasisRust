# Voice memory growth investigation — October 5, 2026

This is a historical investigation of revision `09cf74f`, before the admission fixes now on `main`. The dispatcher on current `main` acquires capacity before spawning. The [dedicated voice processing report](voice-isolation.md) measures the remaining shared-pipeline contention against current `main` (`f78dfa4`).

**The growth is an unbounded backlog of allocated dispatcher tasks.** The server limits concurrently executing handlers to 64 on this host, but it creates a Tokio task for every queued event before the task acquires a worker permit. Slow voice fanout occupies those workers while more tasks accumulate. Avatar uplink deltas share this path and accounted for 89.5% of the waiting tasks in the 1,000-user reproduction.

The cause is supported by three independent observations: measured task counts explain the process memory, both return to small values after traffic stops, and reserving worker capacity before task creation removes the large growth in the same workload. The temporary admission experiment also fills the existing ingress queue, so it establishes the allocation cause without establishing a complete voice-capacity fix.

## Measurements

| Metric | 250 users, all speaking | 1,000 users, 100 speakers | Same 1,000-user workload, admission probe |
|---|---:|---:|---:|
| Requested load measurement | 30 s | 15 s | 15 s |
| Sampled maximum waiting event tasks | 434,040 | 565,198 | 22 |
| Sampled maximum allocated event tasks, waiting plus running | 434,104 | 565,261 | 64 |
| Sampled maximum Tokio live tasks | 434,121 | 565,278 | 81 |
| Maximum concurrently executing handlers | 64 | 64 | 64 |
| Voice waiters at maximum total queue | 221,948 | 59,126 | 14 |
| Avatar delta waiters at maximum total queue | Not measured | 506,072 | 8 |
| Instrumented task future size | 5,552 bytes | 5,568 bytes | 5,568 bytes |
| Waiting future frames alone at maximum queue | 2,298 MiB | 3,001 MiB | 0.117 MiB |
| Peak server working set in marked load window | 2,495 MiB | 3,222 MiB | 198 MiB |
| Peak working set across load and post-client samples | 2,718 MiB | 3,675 MiB | 202 MiB |
| Working-set growth during marked load window | 64.6 MiB/s | 152.2 MiB/s | 1.3 MiB/s |
| Memory/task-count regression, R² | 0.999992 | 0.999947 | Not applicable |
| Measured working-set bytes per additional waiting task | 6,381 | 6,297 | Not applicable |
| Maximum sampled ingress event queue depth | Not measured | 333 | 262,143 |
| Working set after clients exit and idle observation | 34.8 MiB | 42.3 MiB | 28.9 MiB |
| Remaining waiting/running handlers at observation end | 0 / 0 | 0 / 0 | 0 / 0 |

All requested clients stayed authenticated with active avatar states in every marked-window readiness sample, and every client process exited with code 0. All servers ended with zero online players. The runs used a 4 GiB working-set safety limit and completed their requested load windows without hitting it. Post-client observation lasted 45 seconds in the first two runs and 60 seconds in the admission probe.

Queue maxima include the short interval after the marked measurement while the client shuts down; memory and queue maxima therefore need not occur at the same instant. The regression interpolates the one-second task counters at two-second process-sample timestamps inside the marked load window. Its slope includes task headers, retained payloads/state, allocator size-class overhead, and other associated allocations. It is empirical accounting rather than a native allocation-stack trace. The future-frame calculation is an independently measured lower bound and excludes the Tokio task header and referenced allocations.

## Mechanism

[`event_loop`](../../BasisRustServer/crates/basis-server-core/src/lib.rs) computes a worker limit and creates its semaphore. For every event outside the special inline avatar list, it clones `ServerState` and spawns an async task. **The semaphore wait is inside that already allocated task.** The limit controls running handlers; it does not limit the number of waiting tasks or their memory.

`handle_event` has a 4,848-byte async future in the measured release build. The complete dispatcher task future is about 5.4 KiB even for a voice packet with an approximately 80-byte Opus payload. Each waiter retains that future allocation, the event payload, cloned state handles, and Tokio bookkeeping. At roughly 6.15 KiB of additional working set per waiter, approximately 25,000 extra waiting tasks per second explain the reproduced 152 MiB/s growth. This closely matches the earlier 1,000-user test's 153 MiB/s growth with 100 speakers.

Voice relay holds a worker while it loops over the recipient list and calls `transport.send(...).await` once per recipient. With all participants in hearing range, one voice packet targets 999 other users. The measured server was using about 13 logical CPU cores, with most CPU in the kernel. The previous voice test observed about 1.8 million outgoing datagrams per second, while 100 speakers at 50 packets/s require nearly 5 million voice datagrams/s before avatar traffic. The unbounded dispatcher converts the overload into pending tasks and retained memory.

The inline high-frequency list handles full avatar channels but omits **`DELTA_AVATAR`**. Those deltas are consequently spawned into the same worker queue. At the 1,000-user queue maximum, 506,072 delta tasks and 59,126 voice tasks account for all 565,198 waiters. Memory attributed only to queued audio would miss most of the problem in this workload. The small ingress queue depth in the original-dispatch run also shows that the backlog was moved out of the bounded transport channel into the unbounded set of spawned tasks.

After clients disconnect, queued handlers finish, the waiting count reaches zero, and the server's existing population-drop reclamation releases idle allocator/cache memory. The large retained allocation is live pending work during overload; a permanently retained 24 GiB allocation is not needed to explain the observed behavior. This does not rule out unrelated leaks elsewhere in the server.

## Causal probe and fix requirements

The matched 1,000-user pair used the **same frozen diagnostic server binary, client binary, fixture, prepared audio, five-second warmup, and fifteen-second marked measurement**. In the second run only, the probe acquired a worker permit before cloning state and spawning the task. Allocated event tasks stayed at 64, and marked-window working set fell from 3,222 to 198 MiB. The remaining increase was small and finite compared with the original task-backed growth.

The transport channel reached its existing 262,144-event capacity in that probe. Its lossy admission path discards new unreliable events when full. CPU remained approximately 13 cores. This moves overload into a finite buffer and dropping policy; it is not an equal-work speedup or proof of usable voice quality. The probe was preserved in the frozen binary/source artifacts and removed from the normal source after the experiment.

A complete fix should address the following together:

1. **Bound admission before task allocation.** Keep the number and retained size of pending handler futures bounded, with separate capacity for reliable/control operations so overloaded media cannot delay joins, disconnects, or moderation behind stale traffic.
2. **Handle avatar deltas promptly.** Route the uplink delta application through the lightweight avatar path, or an ordered bounded mechanism that applies the delta chain before coalescing decoded poses. Arbitrarily dropping raw dependent deltas can break that chain and trigger keyframe recovery.
3. **Bound and expire voice work.** Use a small bounded per-sender queue and a deliberate freshness/drop policy. Prevent seconds of obsolete audio from accumulating while waiting for a worker or UDP emission.
4. **Reduce voice send overhead and expose overload.** Investigate batching recipient traffic and reducing unnecessary fanout; publish waiting work, frame age, and voice admission drops. The older `droppedVoice` field is unsupported and cannot show this dispatcher backlog.

## Artifacts and reproduction

The server revision was `09cf74f6168a02d3a8355dcfbed389fcd530ddb6` with opt-in event diagnostics. The 250-user run used diagnostic version 1; the matched 1,000-user pair used version 2, which added delta/channel counts and the temporary admission probe. No native heap/stack profiler was used. Counts were cross-checked against Tokio's independent live-task metric. This investigation is one run per condition and uses local Windows loopback; it identifies the retained work rather than defining production throughput.

Raw CSVs, source snapshots, frozen binaries, exact commands, effective configs, hashes, and per-run memory summaries are retained in **`captures/voice-memory-investigation-20261005/`**. This document preserves the measurements and limitations; detailed results are archived locally under `captures/pr31-results-history-cleanup-20261008/results/voice-memory-growth-20261005-summary.json`. The original voice load capture and its audio corpus remain in `captures/voice-1000-20261005/`.

`BASIS_EVENT_DIAGNOSTIC_CSV=<path>` enables the new task/queue counters. The [Windows workload runner](../../scripts/perf/run-windows-avatar-workload.py) accepts `--post-client-seconds` to keep the server running after client exit and measure queue drain and idle reclamation. The [memory analyzer](../../scripts/perf/analyze-event-memory.py) joins task counters and process samples. The experimental bounded mode exists only in the retained probe binary; the normal source retains diagnostics.

Three focused diagnostic tests passed, along with the final locked release build, formatting, Python parsing, and whitespace checks. The probe and final-source snapshots make the investigation reviewable without rerunning a large-memory test.
