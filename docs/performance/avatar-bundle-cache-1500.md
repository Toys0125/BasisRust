# Exact avatar bundle cache at 1,500 clients

The server now shares an encoded avatar bundle chunk across receivers within one tick when the ordered source packets, channel, interval patches, and codec settings are identical. Each receiver still makes its own quality, interval, baseline, bundling, and send decisions. The cache retains source `Bytes` owners until the tick ends, stores only successful encodes, and keeps the complete encoded bytes and raw/compressed lengths so overshoot retries and bundle-ratio adaptation see the same values. Admission is limited to 4,096 keys and a conservative 16 MiB accounting budget per tick; denied chunks use the existing encoder. This is an accounting bound for cached entries, not an exact process RSS cap. The bundle `deflate_micros` profiler field now counts actual encode work on cache misses rather than attributing a full encode to every emitted receiver bundle.

## Matched rich-pose workload

The strict comparison used 1,500 C# v55 clients (one pose observer, 1,499 normal), 90 ms pose cadence, positions pinned to all-High distance, no voice, 15 s warmup and 60 s measurement. The same quality-probe client assembly, client configs, CPU pinning (server cores 0–7, clients 8–15), and explicit LZ4 setting were used for the Rust baseline and cache candidate. The C# server used four `SO_REUSEPORT` receive sockets; Rust used its existing one socket. BSR timing instrumentation was off in the Rust runs.

| Measurement | Rust baseline | Rust cache, 16 MiB cap | C# LZ4 reference |
|---|---:|---:|---:|
| Observer pose arrival gap p50 / p95 (10 ms bucket upper bounds) | ≤1,370 / ≤1,390 ms | **≤730 / ≤740 ms** | ≤760 / ≤770 ms |
| Logical avatar sends/s | 1.643 M (60 s) | **3.115 M (60 s)** | ~2.95 M (nominal final 5 s BSR window) |
| Logical sends/tick | 70,915 | 70,292 | 70,253 (final BSR window) |
| Rust tick / build / flush wall time | 43.14 / 24.66 / 14.55 ms | 22.46 / 6.32 / 13.92 ms | 23.85 ms total, 21.82 ms update |
| Server CPU over 60 s | 184.02 s | 224.21 s | 232.60 s |
| UDP egress | 198.07 MB/s | 375.45 MB/s | 353.06 MB/s |
| UDP packets/s | 207,977 | 392,598 | 279,354 |
| Sampled server peak RSS | 290.0 MB | 307.4 MB | — |

All three runs held 1,500 active clients and the observer decoded all 1,499 senders. The Rust baseline and cache run had q3 High quality on every recorded frame; the C# observer's 4,993 q0 frames occurred during initial convergence, then all 1,499 senders were at q3. The final measured observer windows had no frozen streams, unbaselined deltas, pose/parse errors, or T-pose detections. Rust reported zero unreliable/voice drops and UDP would-block events; the C# server reported zero unreliable/voice drops. The cache raised UDP egress by about 6% and packet rate by about 41% versus the C# reference while delivering more logical updates. Its sampled peak RSS was about 17 MB above the Rust baseline.

The Rust tick times are endpoint-derived from rounded cumulative `/health` averages and tick counts. The bounded Rust run recorded 186,907,312 logical sends and 2,659 ticks over its 60 s window; the C# final BSR window recorded 14,753,158 sends and 210 ticks. Rust send rates use the 60 s health-counter delta; the C# rate uses a nominal five-second BSR window. These rate windows are not directly equivalent. The shared observer gap and full quality/coverage checks show the user-facing result. An alternating Rust baseline/cache A/B before the memory cap found baseline p95 ≤1,390 ms and cache p95 ≤730 and ≤750 ms, with 1.641–1.643 M versus 3.115–3.131 M logical sends/s. The final bounded candidate received the separate 60 s validation shown above; this is a controlled workload result, not a guarantee for other traffic mixes.

## Reproduction and checks

- Rust baseline artifact: `/tmp/basisrust-receiver-latency-baseline-qprobe-run2`, source `4501c8347b690d9cc50a08b243bf41462815c2b9`, binary SHA-256 `6559521a57d5b60324e1304fc4a6b20e335b3a62682150fae39810c288e05a08`.
- Final bounded Rust artifact: `/tmp/basisrust-avatar-bundle-cache-final-qprobe`, binary SHA-256 `5bd157f93578d73ffc50b2b03d7c050d23e75c06e5d3627653f23dc3e81ff22c`.
- Strict C# artifact: `/tmp/basisrust-csharp-lz4-qprobe-reference`, server source `b28f78f1d2885fd21c4e5ad53307fedf3662fba2`, apphost SHA-256 `53e0c34af88b3aeba3b7f3948b4247b2671c33c8a3e8028733a8220ee9f415ce`.
- Shared quality-probe client DLL SHA-256: `bd428523a99d0c776335b9c3c15bfd2d3e39bcc4749688401a479b706fb4ed84`. Rust LZ4 server fixture SHA-256: `adee9ad397a9127f562cee96363c333ed8177495d6304b6accced50551d057cf`. Rust observer/load config SHA-256: `0265ee3bb76aece80f0e63096c0ae5eb9d8c9c20bdd300a13ac2286a02898586` / `97f5217af6e582cafe84fdd9109838227834281eed2c9c424d3602936aea6295`. C# server/transport initial config SHA-256: `7b4273fc850549532bf3fd52c6868967ba36808c25ce2cbdd027fcc74e47b709` / `fe485d005c546d3d964724688d52e0e3a8b89a887e5b62532f8f49ed17808c14`.
- Focused checks: `cargo test -p basis-server-core bundle_cache_ --offline` (two tests: exact encoded bytes/patch key and budget fallback/overshoot), `cargo test -p basis-server-core failed_bundle_encoding_is_not_cached --offline` (one test), `cargo build --release -p basis-server-console --offline`, and `git diff --check` passed in the isolated worktree.
