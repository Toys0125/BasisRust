# Windows performance summary

Windows builds now use mimalloc in the client, reply to validated MTU probes,
and give avatar delivery a 6 ms work budget with at most eight default Rayon
workers in the server console. The 750-client comparison measured lower
applied-update gaps than C#, with higher CPU use and more delivered updates.

## Retained changes

- **Allocator:** At 1,000 clients, Windows mimalloc reduced sampled peak working
  set from 85-86 MiB to 29-32 MiB. No overall speedup was established. Opt out
  with `--no-default-features`; Linux retains its allocator.
- **MTU discovery:** Validated Windows replies allow confirmed 1432-byte
  datagrams instead of the unconfirmed 1200-byte limit. A fixed-32-slice,
  four-run test measured 16.6% fewer datagrams, 12.0% less flush work,
  9.4% less server kernel CPU, and 6.4% less client CPU. Total server CPU was
  essentially flat; adaptive scheduling can spend the capacity on more updates.
- **Cadence:** Windows uses a 6 ms avatar work budget instead of 3 ms; the
  nominal tick interval remains 4 ms. Console Rayon workers default to
  `min(available_parallelism, 8)`. Budget/worker overrides remain available,
  including `RAYON_NUM_THREADS=0`. Linux retains its 3 ms budget and automatic pool.
- **Atomics:** `try_update` replaces deprecated `fetch_update` without changing
  admission checks, memory ordering, or rollback. The server requires Rust 1.95
  or later; the latest Windows build used Rust 1.99.
- **Tooling:** Windows workload/native-profile runners support frozen Rust/C#
  binaries, remote Linux clients, CPU counters, and CLR-aware exports.

Scratch-buffer pooling, IPv6/explicit loopback binding, a single flush worker,
fixed slicing, and a smoothed slicing controller did not provide sufficient
overall benefit to retain. The accepted controller remains adaptive.

## Latest unprofiled 750-client comparison

October 1, 2026: Windows 11 build 26100, Ryzen 9800X3D, 16 logical processors.
750 local IPv4 Rust clients, four client Tokio workers, 45-second warmup after
readiness, and 60-second measurement. Order: C#, Rust, Rust, C#; means of two
runs per server. Client and protocol-v55 C# binaries were frozen; C# used
.NET 10.0.12. Native/application profilers and server per-pair diagnostics were off.

| Metric | C# server | Rust 1.99 server |
|---|---:|---:|
| Applied-update gap p50 | 60.935 ms | 47.270 ms |
| Applied-update gap p95 | 164.360 ms | 79.925 ms |
| Combined server/client CPU | 4.197 cores | 6.005 cores |
| Observer applied avatar items/sec | 10,354 | 14,568 |

Rust had **22.4% lower median gaps** and **51.4% lower p95 gaps**, using
**43.1% more combined CPU** and delivering **40.7% more observed items**.
Both Rust runs beat both C# runs on p50/p95. This is not an equal-work CPU
efficiency result. A core means one CPU-second per elapsed second; update gaps
are intervals between successful avatar applications, not transit latency or RTT.

The first tuned cohort measured Rust 35.07/46.25 ms versus C# 37.36/100.34 ms.
Both implementations slowed in the repeat. A subsequent same-session
previous/current Rust pair measured 54.23/84.92 ms versus 43.86/66.46 ms, with
combined CPU 5.452 versus 6.222 cores and observed items/sec 12,521 versus 16,135.
That single ordered pair does not isolate the API or compiler effect.

All six repeat/control runs retained 750 clients, observed all 749 peers, and
reported zero sender errors, observer decode/state errors, missing/stale peers,
or unreliable/voice drops. Rust reported zero UDP WouldBlock events.
Windows/Linux core tests passed 56 each; strict workspace Clippy, release build,
formatting, and default/GPU CI checks passed. Client tests passed 65 on Windows
and 68 on Linux. Linux performance was not rebenchmarked after the final server
changes; earlier allocator comparisons showed no regression.

## Profiling and network findings

Native C#/Rust captures had zero lost events. Client kernel time was about
69-70% of process CPU. Inclusive sample shares were 30-36% for Winsock sends,
13-16% for receives, and 5-8% for Mio readiness re-registration. Server
`sendto` shares were 47% for C# and 37% for Rust. These overlapping shares
cannot be added and do not measure blocked time or establish the gap's cause.

Remaining candidates are lower packet rates, reduced Windows reactor contention,
and less receiver/bundle construction and payload reference-count churn.
Linux's synthetic non-observer load-sink/shared epoll policy differs from the
Windows client, so these tests do not establish equal-work platform parity.

With 750 laptop clients, the laptop's 1 Gbit/s link averaged 977.4 Mbit/s RX and
24.8 Mbit/s TX and showed severe backpressure and observer delivery failures.
RX and TX are separate full-duplex capacities; that run is not a clean platform
baseline. Local tests stayed on loopback and did not consume the router link.

## Reproduction and local artifacts

Build with `--locked --release`, keep executable/config copies fixed, clear
Rayon and avatar budget/slice overrides, and use fresh output directories:

```powershell
rtk proxy python scripts/perf/run-windows-avatar-workload.py --server BasisRustServer/target/release/basis-server-console.exe --client BasisRustClient/target/release/basis-rust-client.exe --server-config docs/performance/fixtures/avatar-cpu-only-server.xml --output captures/windows-repeat --clients 750 --server-ip 127.0.0.1 --workers 4 --warmup-seconds 45 --window-seconds 60 --no-server-avatar-diagnostics
```

For C#, add `--server-kind csharp` and supply a published protocol-v55 apphost.
For native stacks, use `scripts/perf/run-windows-native-profile.py`, with
`--include-clr` for managed attribution, and export using
`scripts/perf/export-windows-managed-cpu.py`. Compare unprofiled performance separately.

Detailed reports/result JSONs are retained locally under ignored
`captures/perf-pr-cleanup-20261001/original-files/`. Raw CSVs, logs, hashes,
binaries/PDBs, and traces remain under `captures/` in these ignored archives:
`perf-windows-improvements-20261001`, `perf-windows-750-20261001`,
`perf-loopback-cost-20261001`, `perf-csharp-rust-750-20261001`,
`perf-windows-latency-750-20261001`, and `perf-windows-rust199-750-20261001`.
These archives are not included in a fresh checkout.
