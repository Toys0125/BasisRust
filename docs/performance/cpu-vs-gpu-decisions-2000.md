# CPU versus the expanded GPU path at 2,000 clients

September 30, 2026. Tested server revision: `291d044` on
`fix/receiver-distance-cache-refresh`.

**The GPU did not demonstrate a speed advantage.** Delivered latency and
throughput were effectively tied. CPU is the better default for this workload:
GPU used 1.4% more server CPU and about 50% more peak process resident memory.
The mean p95 difference was only 1.3 ms, far smaller than variation between
repeats, so this does not establish that CPU delivers faster updates either.

## Matched measurements

Both modes used the exact same release server binary, Rust client binary, pose
fixture, workload, and configuration. Only `EnableComputeOffload` changed.
GPU mode computed distance, quality tier, and encoded interval, using two
asynchronous buckets published every 32 ticks. CPU mode used the current CPU
reduction path. Run order was CPU / GPU / GPU / CPU, with a 30-second warmup,
120-second measurement, and 15-second separation between runs.

| Run | Observer p95 update gap | Server CPU | Receiver build | Logical fanout/s |
| --- | ---: | ---: | ---: | ---: |
| CPU 1 | 846.36 ms | 460.15% | 12.419 ms | 4.972 million |
| GPU 1 | 854.02 ms | 465.22% | 12.722 ms | 4.943 million |
| GPU 2 | 870.42 ms | 464.57% | 12.723 ms | 4.899 million |
| CPU 2 | 875.51 ms | 456.82% | 12.896 ms | 4.876 million |

| Mean metric | CPU | GPU | GPU versus CPU |
| --- | ---: | ---: | ---: |
| Observer p95 gap | 860.94 ms | 862.22 ms | +0.15% |
| Receiver build | 12.657 ms | 12.723 ms | +0.52% |
| Tick duration | 24.880 ms | 24.882 ms | +0.01% |
| Server CPU | 458.49% | 464.89% | +1.40% |
| Logical fanout/s | 4.924 million | 4.921 million | −0.06% |
| Peak server RSS | 485.35 MiB | 728.85 MiB | +50.17% |

CPU percentages are summed across cores; 100% represents one fully occupied
core. CPU measurements use the final approximately 60 seconds without native
profiling attached. Internal BSR profiling stayed enabled throughout. RSS is
host process resident memory, not GPU VRAM usage.

The hardware was a Ryzen 9 5900HX and NVIDIA GeForce RTX 3080 Laptop GPU through
Vulkan. Server affinity was CPUs 0–7 and client affinity was CPUs 8–15; these
ranges share SMT resources. Four groups of 500 started at x=0,15,30,45. The
last three moved with seven-unit amplitude and a 60-second period. Quality
thresholds were 10,20,40. All 2,000 clients sent animated pose updates at
approximately 50 Hz. LZ4 bundling and internal BSR profiling were enabled;
Zstd, voice, P2P, and reconnect were disabled.

## Coverage and GPU behavior

Every health sample retained 2,000 clients and avatar states. All four runs
covered all 1,999 expected observer peers and recorded the full 6,000 expected
tier transitions: 4,000 in group 1, 2,000 in group 3, and zero in groups 0 and 2.
Final position/tier mismatches, decode errors, unapplied deltas, malformed
items, non-newer sequences, and sender errors were zero. All clients remained
connected at the end, and sender rates remained within 49.98–50.02 Hz.

GPU mode was active on hardware in both GPU runs, with no GPU errors. During
measurement the runs completed 576 million and 584 million pair decisions;
8.58% and 9.29% required CPU boundary repair on the worker. They recorded
four/one missed swaps and eight/two stale-fallback counter increments. CPU
fallback preserved delivery. These results compare the operational GPU mode,
including its fallback behavior.

Median sampled last-worker duration was 19.28 ms and 19.13 ms. Maximum recorded
worker duration was 942.76 ms and 898.29 ms, including waits, scheduling,
readback, and correction. These are worker durations, not isolated shader
execution times, and do not establish the cause of late results.

Native CPU profiles sampled each process for separate 20-second windows at
99 Hz. All eight profiles reported zero lost samples. Receiver building
remained a substantial cost in both modes; neither mode sampled the previous
GPU consumer's `roundf` check.

Server and separately mapped observer UDP sockets had zero sampled drops.
Other clients' aggregate sampled socket-drop counter increases were
4,139/6,667/7,353/3,341 in run order. This is a headless loopback workload with
one fully decoding observer; the other 1,999 clients filter avatar datagrams
with socket BPF while retaining control and reliable traffic. It does not
measure 2,000 rendering Unity clients. Two repeats per mode limit confidence,
and results may differ at other populations, workloads, or hardware.

## Reproduction and provenance

Artifacts are in `captures/cpu-vs-gpu-decisions-2000-20260930/`: retained
binaries, manifests, configurations, commands, observer data, health samples,
native profiles, source hashes, `comparison.json`, `gpu-metrics.json`, and
`validation.json`. All binary and patch hashes matched their manifests. The
28 captured server source hashes remained unchanged, and normalized configs
matched after replacing the GPU-enable flag. All load processes and profilers
exited successfully.

```sh
python3 captures/cpu-vs-gpu-decisions-2000-20260930/run-experiment.py
python3 captures/cpu-vs-gpu-decisions-2000-20260930/analyze.py --perf
```

The runner expects retained binaries and earlier fixture/helper artifacts.
Move existing numbered run directories before rerunning; the runner creates
fresh directories and refuses to overwrite them.

The preceding [GPU implementation comparison](gpu-reduction-decisions-2000.md)
showed an improvement over the older GPU path. This direct comparison shows
that the improvement brings delivery approximately level with CPU at 2,000
clients, with higher resource use rather than an established speed gain.
