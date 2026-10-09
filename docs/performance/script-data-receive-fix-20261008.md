# Script-data benchmark receive fix, 2026-10-08

All 16 matched 100-client reruns passed after fixing the Linux Rust load client.
Eight further scene trials passed on the final build after the review fix below.
The Rust and C# server binaries are unchanged. The previous reliable-scene queue
growth and missing C# avatar senders were reproduced with a client that dropped
required transport/control traffic; those failures cannot rank server throughput.

## Cause and change

The non-observer Linux socket filter dropped every `CompactMerged` datagram.
Both servers can place raw reliable messages and ACKs inside that format,
alongside unreliable avatar entries. Dropping the entire datagram lost metadata
needed by the Unity-policy sender and prevented reliable receive/ACK progress.

The shared epoll `Merged` fast path also discarded valid ACKs and marked reliable
authentication, metadata, and keyframe-control packets without applying them.
Marking them before normal dispatch made the application handler treat them as
duplicates.

- `net.rs` now filters only standalone unreliable packets.
- `receiver.rs` validates both merged formats, ACKs discardable reliable payloads
  using the persistent receive window, and forwards ACK/auth/metadata/keyframe
  controls to normal dispatch without prematurely marking them received.
- Bulk unreliable avatar entries remain discarded for non-observer load sinks.
  Observer client zero continues exercising full payload decoding/application.

The expanded real-socket filter regression timed out with the old filter and
passes with the fix. Shared-receiver tests check persistent ACK windows, ACK
release of pending sends, metadata application, and keyframe-control application.

Code review found a second harness issue: scene replay/coverage tracking used
server peer IDs, which can change or be reused after reconnects. It now uses the
stable synthetic sender index that owns the sequence counter. A regression
checks both reconnect and peer-ID reuse. A follow-up review's proposed missing
Linux import guard was rejected after verifying the guard already exists.

## Matched reruns

Each case ran twice in fresh processes: 100 clients, 3 seconds warmup, 10 seconds
measurement, same host/configuration/worker count as the baseline. All Rust runs
completed before C# began. Scene payloads were 128 bytes every 50 ms; avatar
workloads used Unity-policy 60 FPS scheduling and either zero or 128 additional
bytes. Every run used fixed client SHA256
`6239b9f93038c9a3854af4e339134f506cd5facfb6d6cd8ac4ff14b54264f51c`.
Server hashes match their baseline runs.

After the scene-observer review fix, both scene delivery modes ran twice again
against Rust, then C#, with the final release client SHA256
`8337f8089cc5e42abb6ff934d39e760c488b5ed7d0712aff9d89b661965d05d6`.
All eight passed with 100% delivery and zero duplicates/errors. The table uses
these final scene measurements and the earlier eight avatar measurements;
avatar code is unchanged by the observer fix. Both phases are preserved in JSON.

| Workload | Rust, two trials | C#, two trials |
| --- | --- | --- |
| Scene, unreliable | 100% delivery; p95 upper bound 8.191 ms | 100% delivery; p95 upper bound 8.191 ms |
| Scene, ReliableOrdered | 100% delivery; p95 upper bound 16.383–32.767 ms | 100% delivery; p95 upper bound 8.191 ms |
| Avatar, pose only | 4,927–4,940 applied items/s; p95 gap 20.48–20.53 ms | 4,333–4,407 applied items/s; p95 gap 33.49–35.84 ms |
| Avatar, 128 additional bytes | 4,940 applied items/s; p95 gap 20.45–20.80 ms | 4,346–4,398 applied items/s; p95 gap 34.46–35.51 ms |

Every scene trial reached all 99 observer-side senders, sustained about 2,000
total sends/s and 1,980 observer receipts/s, and reported zero duplicates,
malformed messages, send errors, or backpressure skips. Rust reliable-scene peak
sampled server RSS fell from 3,809–4,256 MiB to 40.7–41.0 MiB on the final build
(41.6–50.0 MiB in the initial receive-fix phase). Its measured server CPU was
about 1.35 cores, with client CPU about 0.37 cores. C# reliable-scene RSS was
237.5–240.0 MiB, server CPU 0.47–0.50 cores, and client CPU about 0.18 cores.
The client now does the ACK/control work that the invalid baseline omitted.

Every avatar sender emitted at least 500 updates per window on both servers
(500–501 in the second C# additional-data trial). All 99
observer peers remained current, with zero missing/stale peers, pose decode
errors, malformed items, or unapplied deltas. Applied-item rates differ between
servers; passing coverage/cadence checks does not mean every sent pose was
delivered, or establish exact additional-byte preservation on C# sockets.

## Evidence and checks

The ignored local artifact
`docs/performance/results/script-data-server-comparison-fixed-20261008.json`
contains all summaries, paired baseline summaries, commands/configuration and
binary/assembly hashes. The original [baseline report](script-data-server-comparison-20261008.md)
and its captures are retained. Raw fixed-run logs/CSVs/configuration and Rust
check logs are archived in ignored local artifact
`captures/script-data-server-comparison-fixed-20261008.tar.gz`; the runs remain
at `/tmp/basis-script-comparison-fixed-20261008` and
`/tmp/basis-script-comparison-final-scene-20261008`.

Validation: 113 Rust workspace tests passed on the final revision, including eight live loopback tests
(two opt-in tests ignored); five focused shared-receiver tests passed; workspace
Clippy with warnings denied and formatting passed; eight focused Python tests
and three focused scene-observer tests passed; `git diff --check` passed.
Reproduce with the commands in
[script-data-harness.md](script-data-harness.md), using the timing/population above.

These are two short observations per case on one host, not production capacity
claims. Scene percentile values are same-host log2 histogram upper bounds;
avatar percentiles measure applied-update gaps. C# configuration still translates
the enum/transport schema and serializes zero `IncreaseRate` as 0.005; Rust
advertises zero. Colocated positions minimize this difference.
