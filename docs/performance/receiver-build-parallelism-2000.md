# Receiver-build parallelism at 2,000 clients

Reduced `RECEIVER_BUILD_MIN_BATCH` from 16 to 4 in [avatar_sync.rs](../../BasisRustServer/crates/basis-server-core/src/avatar_sync.rs) on `fix/receiver-distance-cache-refresh`, based on the corrected server at `f45eb2f`.

At 2,000 peers and 32 receiver slices, a full slice has about 63 receivers. Rayon's previous minimum batch size permits only three leaves in the binary split tree (31/16/16). A minimum of four permits more subdivisions, allowing idle workers to take receiver builds. Actual splitting depends on the worker pool and stealing; a particular task count is not guaranteed. Bundle and pose encoding, quality thresholds, distance-cache refresh, receiver selection and interval-gating rules are unchanged.

## Controlled comparison

Four sequential moving-group runs in baseline/candidate/candidate/baseline order, each with 30 seconds of warmup and 120 seconds of measurement, separated by 15 seconds. All runs used the same client binary, configuration hash, four groups of 500, 7 m motion amplitude, 60-second motion period, fixed 20 ms uplink, LZ4/delta settings and CPU affinity. The server was pinned to CPUs 0–7 and the generator to 8–15 on a Ryzen 9 5900HX; they share physical cores through SMT. Internal BSR profiling was enabled. Native software `cpu-clock` sampling at 99 Hz used 20 seconds per process, sequentially without overlap.

Values below are arithmetic means of the two runs per variant. Gap p95 is pooled applied-frame interarrival time at the observer, not end-to-end movement latency. CPU comes from each run's final approximately 60 seconds after native sampling detached; internal BSR profiling remains enabled. 100% CPU means one logical CPU.

| Measurement | Baseline (16) | Candidate (4) | Change |
|---|---:|---:|---:|
| Observer update-gap p95 | 1102.12 ms | 851.07 ms | -22.8% |
| Median receiver cycle | 1029.75 ms | 786.50 ms | -23.6% |
| Receiver-build wall time/tick | 20.02 ms | 12.40 ms | -38.1% |
| Tick wall time | 32.02 ms | 24.59 ms | -23.2% |
| Logical fanout items/s | 3,837,099 | 4,980,608 | +29.8% |
| Input updates/s | 100,000 | 100,000 | unchanged |
| Server CPU | 325.4% | 458.0% | +40.7% |
| Generator CPU | 115.9% | 125.7% | +8.4% |

This is a latency/throughput improvement using more CPU capacity. Server CPU per logical fanout item, using matching native-perf-detached counter windows, increased by 9.0%; it is not a CPU-efficiency improvement. The change is kept because both candidate runs improved update gaps and build time while preserving work and tier correctness. Smaller workloads and CPU-constrained deployments were not benchmarked. No profiler-off control was collected.

| Run | Update-gap p95 | Build ms/tick | Server CPU | Fanout items/s |
|---|---:|---:|---:|---:|
| 01-baseline | 1103.96 ms | 20.06 | 325.0% | 3,830,561 |
| 02-candidate | 846.06 ms | 12.38 | 458.0% | 5,018,214 |
| 03-candidate | 856.07 ms | 12.41 | 458.0% | 4,943,001 |
| 04-baseline | 1100.28 ms | 19.97 | 325.8% | 3,843,637 |

## Correctness and checks

- Every health sample held 2,000 connected clients and 2,000 active avatar states. Input rate remained approximately 50 sends/s per client, with zero send errors.
- Each run had full 1,999-peer observer coverage and zero decode errors, unapplied deltas, malformed items, non-newer sequences or server protocol errors.
- Every run applied exactly 6,000 quality transitions: all 500 group-1 peers had eight transitions, all 500 group-3 peers had four, and the two fixed-tier groups had none. Final position/tier disagreements were zero.
- Server and separately mapped observer sockets had zero sampled UDP drop increases. The other 1,999 clients retain socket BPF filtering, so this does not measure full decoding or loss at every client.
- All 37 core tests passed, including receiver-refresh, byte-preservation, scratch reuse and concurrent quality materialization tests. Release build, formatting and whitespace checks passed.
- Native CPU captures reported zero lost samples. All load processes and native samplers exited successfully; test processes are stopped. Recorded source patches match the tested candidate.

The roughly 0.8-second receiver cycle remains a substantial limit. This experiment establishes one improvement for synthetic headless loopback traffic on one host; two repeats per variant do not establish production, WAN, voice or Unity-rendering capacity, nor the best batch size for every workload.

[Comparison chart](../../captures/perf-receiver-parallelism-20260930/comparison.png) · [Matched results and assertions](../../captures/perf-receiver-parallelism-20260930/matched-comparison.json) · [Experiment runner](../../captures/perf-receiver-parallelism-20260930/run-experiment.py) · [Comparison script](../../captures/perf-receiver-parallelism-20260930/compare.py) · [Candidate patch](../../captures/perf-receiver-parallelism-20260930/candidate-server-source.patch) · [Core test output](../../captures/perf-receiver-parallelism-20260930/core-tests.log)

Each run retains its command/manifest, health and CPU/RSS samples, applied quality metrics, per-pair diagnostics, and server/client native captures with leaf, inclusive and source-line reports.

Capture links reference ignored local experiment artifacts and are unavailable in a fresh checkout.
