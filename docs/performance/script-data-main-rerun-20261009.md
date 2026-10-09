# Script-data benchmarks after pulling main, 2026-10-09

All 16 trials passed after updating `benchmark/non-avatar-and-additional-data`
to latest fetched `origin/main`, `63748efc4fa8a562d27b88b7350b6131b92cd6a4`.
Existing uncommitted harness/receive fixes were preserved. Two client conflicts
were resolved by retaining both upstream voice diagnostics and scene observation
and shutdown handling.

Main adds dedicated, bounded avatar processing and updated reliable transport
handling. Both Rust server and client were rebuilt; the C# server binary and its
assemblies are unchanged from the [previous comparison](script-data-receive-fix-20261008.md).
This measures the updated Rust server/client pair, rather than isolating a
server-only change.

## Matched workload

Each workload ran twice in fresh processes, with all Rust trials before C#:
100 clients, 3 seconds warmup, 10 seconds measurement, IPv4 loopback, inherited
CPU affinity on the same 16-logical-CPU Linux host, four client Tokio workers.
Scene scripts were 128 bytes every 50 ms, broadcast to all other peers, with
unreliable and ReliableOrdered delivery. Avatar workloads used Unity-policy
60 FPS scheduling, 20 ms advertised interval, colocated positions, and either
zero or 128 additional bytes. No voice ran. Fixture/configuration translations
and workload parameters match the prior runs.

- Rust server SHA256: `c72aff833569d1006c48086eeecf283dbc7aad09a4c015ab83f29bd6f035397e`.
- Rust client SHA256: `b303e000b20f4d2985cae4612bfe06dcd4744bc20024dce180a1bb073e7ed37b`.
- C# source: `b28f78f1d2885fd21c4e5ad53307fedf3662fba2`, protocol v55,
  same published Linux .NET 10 apphost as before.

## Results

Ranges cover the two individual trials. Scene p95 values are same-host log2
latency histogram upper bounds; avatar p95 values are applied-update gaps.
Avatar rates measure one observer's applied items, not total server fanout.
CPU values are process CPU core equivalents; RSS is peak sampled server memory.

| Server / workload | Scene delivery or avatar items/s | p95 (ms) | Server CPU | Client CPU | Server RSS (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Rust / scene unreliable | 100% | 8.191 | 1.98 | 0.05–0.06 | 40.1–40.4 |
| Rust / scene reliable | 100% | 8.191–16.383 | 1.34–1.37 | 0.38 | 60.2–63.0 |
| Rust / avatar pose only | 4,940.1 | 20.45–20.52 | 1.13–1.15 | 0.08 | 42.5–43.0 |
| Rust / avatar +128 bytes | 4,938.8–4,940.1 | 20.71–20.72 | 1.49–1.53 | 0.10–0.11 | 42.4–43.6 |
| C# / scene unreliable | 100% | 8.191 | 0.41–0.45 | 0.13 | 240.0–240.2 |
| C# / scene reliable | 100% | 8.191 | 0.50–0.51 | 0.19 | 240.7–242.9 |
| C# / avatar pose only | 4,290.9–4,425.8 | 32.97–36.02 | 0.41–0.45 | 0.12 | 248.8–254.6 |
| C# / avatar +128 bytes | 4,294.7–4,447.9 | 33.42–36.00 | 0.79–0.82 | 0.33–0.36 | 267.7–272.5 |

Every scene run sustained approximately 2,000 total sends/s and 1,980 observer
receipts/s, reached all 99 observer-side senders, and reported zero duplicate or
malformed messages, send errors, and backpressure skips. Every avatar sender
emitted 500 updates per window on both servers; all 99 observer peers remained
current, with zero missing/stale peers, decode errors, malformed items, or
unapplied deltas.

Compared with the previous valid runs, Rust avatar rates and update gaps were
similar. Reliable-scene p95 bounds were 8.191–16.383 ms rather than
16.383–32.767 ms, while peak server RSS rose from 40.7–41.0 to 60.2–63.0 MiB.
Its server CPU remained about 1.35 cores. These two short observations do not
establish a repeatable speedup or long-run memory behavior. C# retained lower
scene CPU but fewer applied avatar items and wider avatar gaps than Rust.
Passing avatar coverage/cadence gates does not prove every sent pose arrived.

## Validation and evidence

124 Rust client-workspace tests passed, including eight real executable loopback
tests; two opt-in tests were ignored. Client workspace Clippy with warnings
denied, formatting, 12 focused Python tests, both release builds, and
`git diff --check` passed. The server source is clean at the fetched main commit;
the client retains the uncommitted harness and receive fixes.

The ignored local artifact
`docs/performance/results/script-data-server-comparison-main-20261009.json`
contains all 16 summaries, paired prior summaries, commands, binary/assembly and
configuration hashes, host/environment details, source-patch hash, and source
provenance. Raw logs/CSVs/configuration, the client patch snapshot and build/test
logs are preserved in ignored local archive
`captures/script-data-server-comparison-main-20261009.tar.gz` and original run
directory `/tmp/basis-script-comparison-main-20261009`. These local artifacts are
not included in a fresh checkout. Reproduce with
[the harness commands](script-data-harness.md), using the population and timings
above and the same C# apphost.

This synthetic, one-observer, single-host test does not establish production
capacity, Unity script execution, every receiver's application, or exact C#
additional-byte preservation. C# still translates the shared fixture and
serializes zero `IncreaseRate` as 0.005; colocated clients minimize that policy
difference.
