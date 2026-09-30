# Why GPU mode increased update gaps

Investigation of the September 30, 2026 two-bucket implementation at `9cd2bfc`.
The evidence identifies CPU-side GPU-result consumption as a contributor. The
original 4.3% p95 increase also includes run variation and an unprofiled stall;
this investigation does not assign every millisecond of that increase.

## The work moved to the GPU was inexpensive

The GPU replaces three squared coordinate differences and their sum. Receiver
building still computes quality and the encoded send interval on the CPU.
`checked_gpu_distance` additionally maps the sender index, reads the matrix,
checks three quality boundaries, computes an interval error tolerance, and
rounds an interval to detect ambiguous decisions. Ambiguous pairs then recompute
the distance from captured positions. These checks preserve protocol decisions
near floating-point boundaries; removing them without replacement is unsafe.

A release-mode benchmark called the production distance and policy functions on
the same four groups of 500 captured peers and actual NVIDIA GPU matrices. It
used four motion phases, eight samples per mode per phase, and reversed mode
order on alternating rounds. Every pass evaluated 3,998,000 directed pairs.
Timing excluded GPU computation, readback, allocation, and correctness checks.

| Distance source, with identical quality/interval calculations | Mean of phase median times |
| --- | ---: |
| CPU position arithmetic | 17.71 ms |
| GPU matrix lookup, without precision checks | 15.24 ms |
| GPU lookup with current precision checks | 48.84 ms |
| CPU arithmetic from the mapped bucket roster | 19.25 ms |

Raw GPU lookup saved about 2.5 ms per pass, while adding the precision checks
cost about 33.6 ms. This is an isolated consumer benchmark, not a whole-server
speed ratio. The checked GPU and CPU modes produced matching decision checksums
at every phase. The original native profiles also sampled `roundf` only in GPU
mode, at 0.16–0.20% of total server CPU samples.

Reproduce the manual benchmark with:

```sh
cargo test --manifest-path BasisRustServer/Cargo.toml --release \
  -p basis-server-core profile_gpu_distance_consumption -- --ignored --nocapture
```

## Controlled 2,000-client check

Four new 120-second runs alternated production / diagnostic / diagnostic /
production, with the same 30-second warmup, workload, affinity, config, and client
binary as the original comparison. Both variants enabled the hardware GPU,
computed and read back the full matrix, and used the same bucket controller and
32-tick publication cadence. The diagnostic variant changed only distance
consumption to CPU arithmetic on the bucket's captured positions. This preserves
the intended snapshot delay and peer-incarnation mapping. It is an experimental
ablation, not a new production setting or a proposed GPU optimization.

| Run | Observer p95 gap | Average receiver build | Server CPU |
| --- | ---: | ---: | ---: |
| Production 1 | 886.62 ms | 12.97 ms | 465.75% |
| Diagnostic 1 | 868.26 ms | 12.76 ms | 465.17% |
| Diagnostic 2 | 878.84 ms | 12.72 ms | 462.19% |
| Production 2 | 905.12 ms | 13.35 ms | 465.67% |

Production to diagnostic means:

- p95 gap: **895.87 to 873.55 ms**, 2.5% smaller.
- Receiver build: **13.16 to 12.74 ms**, 3.2% shorter.
- Tick duration: **25.63 to 25.06 ms**, 2.2% shorter.
- Server CPU: **465.71% to 463.68%**, 0.4% lower.
- Logical fanout: **4.782 to 4.887 million items/s**, 2.2% higher.

All samples retained 2,000 clients and avatar states. Every run covered 1,999
observed peers, recorded all 6,000 expected tier transitions, had zero decode and
send errors, and had zero final position/tier mismatches. The server and observer
UDP sockets had zero sampled drops; all load processes and profilers exited
successfully. Config/client hashes matched across runs, and variant binary and
runtime patch hashes matched their manifests. CPU figures use the final roughly
60 seconds without native perf attached; internal BSR profiling stayed enabled.

In these new native profiles, `roundf` accounted for 0.10% and 0.22% of server
samples in production and was absent from both diagnostic variants, consistent
with removing the consumer's interval-boundary rounding check.

One production run and one diagnostic run each recorded one missed swap and two
stale-fallback counter increments during measurement. Delivery continued using
CPU distances, with no GPU errors. The other two runs had none. Thus occasional
late GPU results occurred in both variants and do not uniquely explain the
production consumer's higher cost. Two runs per variant still limit confidence
and do not establish an exact decomposition of the original regression.

## Why this affects update gaps

The server is already limited by receiver building and fanout. A slightly longer
tick lengthens the receiver sweep, so all groups wait longer between updates.
The stationary High-quality group also had larger gaps in the original GPU runs;
its quality never changed. The deliberate 32-tick snapshot delay alone therefore
does not explain those update gaps. That delay separately affects when moving
peers change quality tiers.

The original first GPU run had a large stall around 69 seconds: a smoothed
receiver-cycle estimate of 2,305 ms and a maximum tick of 118.8 ms. Its surrounding
five-second window averaged 32.38 ms/tick and 17.68 ms/build. Native profiles
ended before this event, and no GPU swaps were missed in that original run. Its
cause remains unknown; it contributed variation beyond the steady consumer cost.
The original second GPU run had no comparable stall.

The next optimization to test is preparing validated quality and interval data
on the bucket worker, then publishing it with the bucket. The tick could read
ready decisions instead of repeating precision checks and policy arithmetic.
Policy changes and CPU fallback would need to invalidate those decisions.
Simply removing precision protection would trade correctness for speed.

Artifacts: `captures/gpu-distance-investigation-20260930/`, including variant
patches and binaries, manifests, health samples, native profiles, the consumer
benchmark, and diagnostic JSON. The benchmark is the only Rust change from this
investigation; production GPU behavior remains unchanged. This remains a
headless loopback workload with one fully decoding observer; the other clients
filter avatar datagrams with BPF and retain control/reliable traffic.
