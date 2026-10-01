# GPU quality and interval decisions with 2,000 clients

September 30, 2026, branch `fix/receiver-distance-cache-refresh`.

The Rust GPU kernel now computes squared distance, quality tier, and the encoded
update interval, matching the work assigned to the C# GPU implementation. The
two asynchronous buckets still publish every 32 ticks. Receiver ticks consume
prepared decisions without floating-point policy or precision checks.

Two matched 120-second runs per implementation showed a modest improvement over
the previous distance-only GPU path: mean observer p95 gaps decreased 1.9%,
receiver build time decreased 3.7%, and logical fanout increased 2.4%. Run
variation is substantial; this experiment does not establish that GPU mode is
faster than CPU-only mode or C#.

## Implementation

- Each pair stores a two-byte tier/interval decision. Two decisions share one
  GPU storage word, reducing a 2,000-by-2,000 readback from 16 MB to 8 MB.
- The GPU flags ambiguous float boundaries. The worker repairs flagged pairs
  using the captured positions and the exact CPU policy before publishing.
  Precision correction never waits on or runs inside the receiver tick.
- A 256-entry interval lookup table supplies decoded intervals. Peer
  incarnation mapping and fallback for absent peers remain intact.
- Policy changes invalidate buckets, pending work, and receiver policy caches.
  Unsupported policies, unsafe coordinate ranges, and GPU failures fall back
  to CPU processing. Tests cover unordered quality thresholds and interval
  saturation as well as ordinary settings.
- Extended health metrics expose computed/corrected pair counts and last/max
  worker duration. Duration includes submission, waiting, readback, unpacking,
  and correction; it is not a GPU execution timer. Counts describe completed
  work, including results that could subsequently expire before publication.

## Controlled comparison

The baseline binary has the runtime behavior of `9cd2bfc`; `0ae5273` added only
the preceding investigation's tests and documentation. The candidate was built
from `0ae5273` plus the captured `variants/new-gpu.patch`. All server source
hashes remained unchanged throughout the four runs.

Both variants enabled the NVIDIA GeForce RTX 3080 Laptop GPU through Vulkan,
used the same configuration and client binary, and retained 32-tick publication.
Order was old / new / new / old. Each run had a 30-second warmup, followed by a
120-second measurement; runs were separated by 15 seconds.

The Ryzen 9 5900HX server used CPUs 0–7 and Rust load clients used CPUs 8–15
(these ranges share SMT resources). Four groups of 500 started at x=0,15,30,45.
The last three moved by a seven-unit sinusoid with a 60-second period. Quality
thresholds were 10,20,40. Every client sent animated pose data at 50 Hz. LZ4
bundling and internal BSR profiling were enabled; Zstd, voice, P2P, and reconnect
were disabled. Native CPU profiles sampled the server and client in separate
20-second windows. Reported CPU uses the final approximately 60 seconds without
native profiling; internal BSR profiling remained enabled.

| Run | Observer p95 gap | Average receiver build | Server CPU | Logical fanout/s |
| --- | ---: | ---: | ---: | ---: |
| Old GPU 1 | 879.62 ms | 13.04 ms | 465.87% | 4.899 million |
| New GPU 1 | 849.01 ms | 12.37 ms | 461.65% | 5.016 million |
| New GPU 2 | 894.21 ms | 12.95 ms | 466.32% | 4.852 million |
| Old GPU 2 | 898.11 ms | 13.27 ms | 471.20% | 4.740 million |

| Mean metric | Old GPU | New GPU | Change |
| --- | ---: | ---: | ---: |
| Observer p95 gap | 888.87 ms | 871.61 ms | −1.9% |
| Receiver build | 13.15 ms | 12.66 ms | −3.7% |
| Tick duration | 25.42 ms | 24.83 ms | −2.3% |
| Server CPU | 468.54% | 463.99% | −1.0% |
| Logical fanout/s | 4.820 million | 4.934 million | +2.4% |
| Peak server RSS | 839.17 MiB | 795.40 MiB | −5.2% |

Native server profiles sampled `roundf` at 0.20% and 0.21% in the old version
and did not sample it in either new run. Receiver building accounted for
13.31%/13.51% of leaf samples before and 12.01%/12.12% after. These sampling
observations are consistent with removing the old per-pair consumer checks;
they do not isolate the entire cost of any one function. All eight native
profiles reported zero lost samples.

The new runs completed 600 million and 584 million pair decisions during
measurement. CPU boundary repair covered 9.47% and 9.51%, respectively. Median
sampled last-worker duration was about 19.2 ms in both; maximum recorded duration
was 784.7 ms and 846.3 ms, including worker scheduling and GPU waits. Neither new
run recorded missed swaps, stale fallbacks, or GPU errors. The second old run
recorded two missed swaps and four stale-fallback counter increments; its CPU
fallback preserved delivery. The first old run recorded none.

## Correctness and limitations

All four runs retained 2,000 connected clients and avatar states in every health
sample. Each covered all 1,999 expected observer peers, recorded exactly 6,000
expected tier transitions (4,000 in group 1 and 2,000 in group 3), and ended
with zero position/tier mismatches. Decode, malformed-item, sequence, delta
application, and sender errors were zero. Sender rates stayed approximately
50 Hz. Server and observer sockets had zero sampled UDP drops, and all load
processes and profilers exited successfully.

This is a headless loopback experiment with one fully decoding observer.
The other 1,999 clients filter avatar datagrams with socket BPF while retaining
control and reliable traffic. Their aggregate sampled socket-drop counter
increases were 5,600/6,504/8,949/6,375 in run order; the separately mapped
observer socket remained at zero. These results do not measure 2,000 rendering
Unity clients or visual correctness. Two repeats per variant are limited
evidence, and the observed mean latency gain is smaller than run variation.

Verification passed 157 workspace tests and an explicitly enabled hardware
parity test covering all four million decisions at 2,000 peers, odd/even matrix
sizes, buffer reuse, quality and interval boundaries, extreme policies, and
invalid input handling. A separate 40-client smoke test passed all 120 expected
tier transitions and shut down cleanly. Format and diff checks passed.

```sh
cargo test --manifest-path BasisRustServer/Cargo.toml --offline --workspace
cargo test --manifest-path BasisRustServer/Cargo.toml --offline \
  -p basis-server-core hardware_gpu_distance_parity -- --ignored --nocapture
```

Artifacts: `captures/gpu-reduction-decisions-20260930/`, including fixed binaries,
variant patches, manifests, source hashes, health samples, observer data,
native profiles, test logs, `comparison.json`, and `validation.json`.
Reproduction uses `run-experiment.py` and `analyze.py --perf` in that directory.
The runner expects the retained binaries and earlier fixture/runner artifacts.

The preceding [update-gap investigation](gpu-update-gap-investigation.md)
explains why moving only distance arithmetic had increased CPU consumption.

A subsequent [matched CPU versus GPU comparison](cpu-vs-gpu-decisions-2000.md)
on `291d044` found effectively tied update latency and throughput, with higher
CPU and resident-memory use in GPU mode. CPU remains the recommended default
for this 2,000-client workload.
