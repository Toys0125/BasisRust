# GPU distance buckets at 2,000 clients

Tested September 30, 2026 on `fix/receiver-distance-cache-refresh`, using the
server changes above parent revision `1638c97304e1caff73f01fe86f58bbb7b106b704`.
Artifacts, exact source hashes, patches, configs, commands, binaries, health
samples, observer diagnostics, and native profiles are under
`captures/gpu-two-buckets-20260930/`.

The asynchronous GPU implementation works, but these repeated runs favor CPU
distance processing for this workload. Keep `EnableComputeOffload=false` when
optimizing this particular 2,000-client workload. Two runs per mode establish an
observation, not a general result for other GPUs, workloads, or client counts.

The [follow-up investigation](gpu-update-gap-investigation.md) isolates CPU-side
GPU-result processing as a contributor and examines the original outlier stall.

## Implementation

Two retained GPU buffer sets and two immutable CPU result buckets alternate.
The tick reads the active bucket while `BSR-GPU-Distance` uploads a captured
roster, computes its squared-distance matrix, and reads back the inactive bucket.
Only the worker waits for GPU submission and mapping. At every 32-tick boundary,
the controller publishes a completed bucket and submits the next snapshot.
Late work keeps the previous bucket active without blocking the tick; snapshots
older than 64 ticks fall back to CPU processing.

Peer incarnation IDs protect against roster reordering, joins, and reused IDs.
Missing pairs use current CPU distances. A new GPU epoch refreshes receiver
distance caches; CPU fallback refreshes them immediately. Float results near
quality thresholds or integer interval boundaries are recomputed using CPU
arithmetic on the captured positions. This preserves snapshot timing and CPU
decision precision. GPU errors disable offload until settings change, with the
reason exposed in `extended.avatarSync.gpuDistance`. Software GPU adapters are
rejected. Shutdown joins active and retiring workers before normal process exit.

Distance decisions deliberately lag current positions. At the measured tick
duration, a publication period was approximately 0.82 seconds; snapshot age can
approach two periods. This is not identical to the CPU cache's 500 ms refresh
timing. Packet building, compression, and UDP transport remain on the CPU.

## Controlled comparison

One experiment ran CPU, GPU, GPU, CPU sequentially. Each run used the same release
server and Rust client binary, 30 seconds of warmup, and a 120-second measurement.
Normalized XML configs differed only in `EnableComputeOffload`; the GPU period
remained 32 ticks. Binary hashes, patch hashes, and final Rust/Cargo source hashes
were checked. The GPU was an NVIDIA GeForce RTX 3080 Laptop GPU using Vulkan;
server affinity was CPUs 0–7 and generator affinity 8–15 on a Ryzen 5900HX.
Those ranges share physical cores through SMT.

Four groups of 500 clients started at x=0/15/30/45 m. Nonzero groups moved by
7*sin(2*pi*t/60) m. Quality thresholds were 10/20/40 m. Clients sent animated
poses at a fixed 20 ms uplink interval, producing approximately 100,000 inbound
updates/s. Native `cpu-clock` profiles used 99 Hz and DWARF stacks: 20 seconds
for the server followed by 20 seconds for the generator, without overlap.
CPU figures below use the final approximately 60 seconds without native perf
attached. Internal BSR profiling remained enabled throughout.

| Run | Observer p95 update gap | Server CPU | Average build | Logical fanout/s |
| --- | ---: | ---: | ---: | ---: |
| CPU 1 | 874.64 ms | 460.22% | 12.65 ms | 4.892 million |
| GPU 1 | 929.80 ms | 464.51% | 13.36 ms | 4.718 million |
| GPU 2 | 888.31 ms | 467.45% | 13.03 ms | 4.827 million |
| CPU 2 | 868.22 ms | 461.14% | 12.78 ms | 4.908 million |

Means, CPU to GPU:

- p95 update gap: 871.43 to 909.06 ms, **4.3% larger**.
- Server CPU: 460.68% to 465.98%, **1.1% more**; 100% is one logical CPU.
- Average build: 12.72 to 13.20 ms, **3.8% longer**.
- Average tick: 24.99 to 25.67 ms, **2.7% longer**.
- Logical fanout: 4.900 to 4.773 million items/s, **2.6% lower**.
- Server peak RSS: 558–631 MiB in CPU runs and 830–885 MiB in GPU runs.

Both GPU runs published every available 32-tick result: 141 and 144 swaps during
their measurement windows, zero missed swaps, zero stale fallbacks, and no GPU
errors. This confirms actual GPU results were consumed rather than silently
falling back to CPU. Removing tick-side GPU waits did not produce an overall
speedup; this measurement does not separately attribute the remaining overhead
to transfer, driver work, memory access, or precision checks.

The server native profiles contained 8,899–9,043 samples each with zero lost
samples. Receiver building remained the largest named application leaf at
12.54–13.65%, followed by delta construction, hashing, and memory copies. The
named GPU compute function accounted for 0.10–0.14% of server CPU samples in GPU
runs; that excludes GPU execution and does not isolate inlined distance work.

## Correctness and limits

Every health sample retained all 2,000 clients and avatar states. Each observer
covered all 1,999 other peers, with zero decode errors, malformed items, unapplied
deltas, or non-newer sequences. All senders stayed near 50 Hz with zero send
errors. Server UDP sockets and the unfiltered observer socket showed zero drops
in sampled counters. Every load process and native profiler exited successfully.

| Observed group | Peers | Quality behavior | Transitions in every run |
| --- | ---: | --- | ---: |
| x=0 | 499 | High | 0 |
| x=15 | 500 | High / Medium / Low | 4,000 |
| x=30 | 500 | Low | 0 |
| x=45 | 500 | Low / VeryLow | 2,000 |

All four tiers received updates. Each run recorded all 6,000 expected transitions
and zero final position/tier mismatches. These checks establish coverage and
transition counts, not identical tier-change timing between CPU and GPU snapshots.

This is a headless loopback test with one fully decoding observer. The remaining
1,999 sockets filter top-level avatar datagrams with BPF while retaining reliable
and control traffic. Aggregate generator socket drop counters increased by
3,306–5,773 packets; the mapped observer socket did not. Results do not establish
rendered BasisVR client performance or decoding capacity for 2,000 full clients.

Validation: 152 workspace tests passed, with two intentionally ignored tests;
the final console change also passed its focused test. The ignored hardware GPU
test was explicitly run and passed on the NVIDIA adapter, including the
2,000-by-2,000 matrix, buffer reuse/resizing, and boundary inputs. Two 40-client
GPU smoke tests after the shutdown fix covered every peer, recorded all 120
expected transitions, and exited cleanly. Formatting and whitespace checks pass.
