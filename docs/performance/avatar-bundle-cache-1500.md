# Exact avatar bundle cache at 1,500 clients

The server now shares an encoded avatar bundle chunk across receivers within one tick when the ordered source packets, channel, interval patches, and codec settings are identical. Each receiver still makes its own quality, interval, baseline, bundling, and send decisions. The cache retains source `Bytes` owners until the tick ends, stores only successful encodes, and keeps the complete encoded bytes and raw/compressed lengths so overshoot retries and bundle-ratio adaptation see the same values. Admission is limited to 4,096 keys and a conservative 16 MiB accounting budget per tick; denied chunks use the existing encoder. This is an accounting bound for cached entries, not an exact process RSS cap. The bundle `deflate_micros` profiler field now counts actual encode work on cache misses rather than attributing a full encode to every emitted receiver bundle.

## Production default and confirmed MTU packing

The production default is dictionary Zstd level −5 for bundles containing full/keyframe messages, with LZ4 for delta-only bundles. Fresh config files write `EnableAvatarBundleZstd=true` and `AvatarBundleZstdLevel=-5`; older files missing those fields select the same values. Explicit XML, environment, and dynamic overrides still take precedence.

The server now sends LiteNetLib-compatible outbound MTU probes to each connected peer. Only an exact pending response from that peer, including the connection number, probe size, and random token, can raise its outbound datagram cap. Confirmed rungs are 1,164, 1,392, 1,404, 1,424, and 1,432 bytes. Before a rung above 1,200 is confirmed, avatar bundles retain the previous 1,100-byte payload limit and unreliable merging retains the previous 1,200-byte datagram limit. After confirmation, the avatar bundle payload limit is the peer MTU minus 35 bytes of headroom; unreliable merging uses that peer MTU. Per-receiver quality choices, logical sends, and the bundle wire format remain the same. The per-tick cache may share identical encoded chunks across receivers with different MTU limits; each receiver still checks its own limit and retry path.

Two profiler-off runs of this final candidate used the missing-field Zstd defaults with the same 1,500-client, all-High, 90 ms pose workload, 15 s warmup, 60 s measurement, and CPU pinning as the prior runs. The second strict C# LZ4 reference and a one-socket Rust Zstd-default control give the nearest comparisons. Observer gaps below are upper bounds of 10 ms buckets in the **final observer-summary interval**, not a whole-60 s histogram.

| Measurement | Rust Zstd + confirmed MTU A | Rust Zstd + confirmed MTU B | Rust Zstd fixed cap | C# LZ4 refs 1 / 2 |
|---|---:|---:|---:|---:|
| Observer p50 / p95 | ≤630 / ≤640 ms | ≤640 / ≤670 ms | ≤740 / ≤740 ms | ≤760 / ≤770; ≤740 / ≤750 ms |
| Current q3 senders | 1,499 / 1,499 | 1,499 / 1,499 | 1,499 / 1,499 | 1,499 / 1,499 each |
| Logical sends/s, 60 s | 3.564 M | 3.532 M | 3.084 M | ~2.95 / ~3.01 M, nominal final 5 s BSR windows |
| Logical sends/tick | 70,244 | 70,241 | 70,257 | 70,253 / 70,257, final BSR windows |
| UDP packets/s | 353,699 | 350,440 | 388,818 | 279,354 / 282,814 |
| UDP egress | 432.95 MB/s | 428.96 MB/s | 376.94 MB/s | 353.06 / 357.41 MB/s |
| Server CPU over 60 s | 212.92 s | 213.50 s | 223.13 s | 232.60 / 232.59 s |
| Rust tick / build / flush, endpoint estimate | 19.59 / 6.66 / 11.01 ms | 19.77 / 6.77 / 11.03 ms | 22.68 / 6.55 / 13.89 ms | — |

Both final Rust runs kept 1,500 active clients. The observer accepted 182,069 and 182,536 q3 frames, saw all 1,499 remote senders at q3, and reported zero frozen streams, unbaselined deltas, pose/parse errors, or T-pose detections. Server health reported zero unreliable/voice drops, UDP would-block events, protocol errors, reliable window stalls, and retransmits. Logical sends per tick stayed near the control and C# references, so the shorter gap did not come from reducing per-recipient work. Compared with the fixed-cap Rust control, the final candidate delivered about 15% more logical sends/s while using about 9–10% fewer UDP packets/s; total byte rate rose with delivery rate, while bytes per logical send changed from about 122.2 to 121.4. These two local runs do not quantify performance for other traffic mixes or network paths.

Rust sends/tick divides the paired `/health` logical-send counter delta by the paired `/health` tick counter delta; the values differ slightly from the separate runner summary counters because their capture boundaries differ. C# sends/tick and its nominal send rate come from its final approximately five-second BSR window, not its 60-second UDP counter window. Rust tick/build/flush values are endpoint estimates from rounded cumulative averages weighted by tick counts.

Both candidate artifacts, `/tmp/basisrust-rust-pmtu-zstd-qprobe-run2` and `/tmp/basisrust-rust-pmtu-zstd-qprobe-run3`, used binary SHA-256 `55aadf951ef36c5658fb43f50d33d5410990eb57297969ed5b682661646a0b1f`, generated server XML SHA-256 `be51dd00959178faee8f446fd671d525cace0adbab6be230940ac874b4804767`, and the qprobe client DLL/role config hashes below. The fixed-cap control is `/tmp/basisrust-zstd-default-sockets-single`; the C# reference repeat is `/tmp/basisrust-csharp-lz4-qprobe-reference-run2`. The detailed paired counters and quality checks are recorded in `/tmp/basisrust-pmtu-zstd-validation.md`. No per-peer confirmed-MTU histogram or packet capture was collected, so the exact rung distribution is unknown. This change only raises the prior 1,100/1,200-byte avatar/merged limits after a larger probe succeeds. If a path supports only the 1,164-byte rung, the old 1,200-byte merged limit persists; the reliable merger also retains its fixed 1,200-byte limit. A full low-MTU downshift needs both paths updated together and was outside this measured change. A path that shrinks after startup is not re-probed or downshifted. Generic oversized one-message unreliable sends retain their preexisting standalone-send behavior. The observed improvement is for the loopback workload with successfully delivered packets.

## Zstd at the production default

Fresh or missing-field server configs now enable dictionary Zstd at level −5 for bundles containing full/keyframe avatar messages. Delta-only bundles continue to use LZ4 (`AvatarBundleZstdDeltaBundles=false`). An existing config that explicitly sets `EnableAvatarBundleZstd=false` or a different level keeps those values. At startup, a new config file is written with the new defaults; an existing file with these fields is loaded as written. Environment and dynamic config overrides remain available.

Two matched 1,500-client runs of the bounded-cache binary with explicit Zstd level −5 exercised this codec selection before MTU packing was added. The final observer-summary interval in each run had p50/p95 ≤730/≤740 ms, versus ≤760/≤770 ms for the first strict C# LZ4 reference. Both runs held 1,500 active clients, decoded all 1,499 senders at q3 High, sustained about 70.3k logical sends/tick, and had zero observer decode/pose errors, unreliable/voice drops, or UDP would-block events. The repeated C# reference later reached ≤740/≤750 ms; its 20 ms shift shows why the earlier one-bin margin did not establish a robust lead.

| Measurement | Rust cached Zstd −5, run 1 | Rust cached Zstd −5, run 2 | C# LZ4 reference |
|---|---:|---:|---:|
| Final observer-summary p50 / p95 | ≤730 / ≤740 ms | ≤730 / ≤740 ms | ≤760 / ≤770 ms |
| Logical sends/s | 3.114 M (60 s) | 3.075 M (60 s) | ~2.95 M (nominal final 5 s BSR window) |
| Logical sends/tick | 70,262 | 70,270 | 70,253 (final BSR window) |
| Server CPU over 60 s | 223.12 s | 222.35 s | 232.60 s |
| UDP egress | 380.85 MB/s | 375.96 MB/s | 353.06 MB/s |
| UDP packets/s | 392,480 | 387,696 | 279,354 |

The Zstd runs used server config SHA-256 `f4ca567846e3aa11368cfa8582d5df401cc1cb99d367633cdf52b0c067579759`, binary SHA-256 `5bd157f93578d73ffc50b2b03d7c050d23e75c06e5d3627653f23dc3e81ff22c`, and the same quality-probe client and role config hashes listed below. The policy selects Zstd for chunks with any full/keyframe channel and LZ4 for delta-only chunks; BSR profiling was off, so exact counts by codec were not captured. Artifacts: `/tmp/basisrust-avatar-bundle-cache-zstd-m5-qprobe` and `/tmp/basisrust-avatar-bundle-cache-zstd-m5-qprobe-run2`. These runs used explicit fields to pin the measured settings. The final MTU runs above validated the generated missing-field defaults.

## Matched rich-pose LZ4 workload

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
- Final MTU checks: `cargo test -p basis-transport mtu_probe --offline`, `cargo test -p basis-transport negotiated_mtu_allows_larger_merged_unreliable_datagram --offline`, `cargo test -p basis-transport multiple_small_packets_are_sent_as_litenetlib_merged_datagram --offline`, `cargo test -p basis-transport idle_peer_reliable_receive_flushes_ack_without_server_reliable_send --offline`, and `cargo test -p basis-server-core confirmed_mtu_expands_bundle_without_changing_encoded_bytes --offline` each passed. The default and explicit-override config tests passed with `cargo test -p basis-protocol config_ --offline` (four tests).
