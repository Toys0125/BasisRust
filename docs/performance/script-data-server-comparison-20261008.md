# Script-data benchmarks: Rust and C# servers, 2026-10-08

**Historical baseline:** the failures below were traced to dropped control
traffic in the Linux Rust load client. The client receive fix passed all 16
matched reruns with unchanged server binaries. See the
[fix and corrected results](script-data-receive-fix-20261008.md).

The 100-client suite passed the unreliable scene and both avatar workloads on
Rust. Rust reliable scene delivery failed twice. C# passed both scene workloads,
but its Unity-policy avatar runs failed because most clients emitted no ongoing
updates. A separate three-client AdditionalAvatarData check passed on both.

## Controlled workload

Each server/workload combination ran twice, in fresh processes, with 100 clients,
3 seconds of warmup and 10 seconds of measurement. All Rust runs completed before
C# runs started. Servers ran individually on the same Linux host with 16 logical
CPUs and inherited affinity; the identical release Rust client used four Tokio
workers throughout. Scene messages contained 128-byte synthetic scripts at
50 ms intervals with broadcast fanout. Avatar workloads used the Unity-policy
60 FPS accumulator, advertised 20 ms interval, colocated clients, and either zero
or 128 AdditionalAvatarData bytes. No voice ran.

- Rust server source: `ccedc4280dbe67cb5701ccdde3520df948502dc7`.
- C# server source: `b28f78f1d2885fd21c4e5ad53307fedf3662fba2`, protocol v55,
  published Linux .NET 10 apphost; server source unchanged. The source checkout
  has prior C# **client-only** edits that are not used by these Rust-client runs.
- Both runners used `avatar-1500-server.xml`, with C#'s enum spelling and transport
  sidecar translation. C# serializes zero `IncreaseRate` as 0.005; Rust advertises
  zero. The test keeps positions colocated to minimize distance-policy effects.

Binary, assembly, configuration hashes, commands, environment, per-run results,
and build provenance are in the ignored local artifact
`docs/performance/results/script-data-server-comparison-20261008.json`.
Raw logs and CSVs are preserved in
`captures/script-data-server-comparison-20261008.tar.gz` (ignored local artifact),
and the original runs remain at `/tmp/basis-script-comparison-20261008`.

## Results at 100 clients

| Workload | Rust | C# |
| --- | --- | --- |
| Scene, unreliable | 2/2 valid; 100% observer delivery | 2/2 valid; 100% observer delivery |
| Scene, ReliableOrdered | 0/2 valid; 26.9–27.5% delivery | 2/2 valid; 100% observer delivery |
| Avatar, pose only | 2/2 valid; all senders progressed | 0/2 valid; 84–85 senders emitted no updates |
| Avatar, 128 additional bytes | 2/2 valid; all senders progressed | 0/2 valid; 82–84 senders emitted no updates |

All scene runs maintained sender cadence and reached all 99 observer-side senders
with no malformed payloads or send errors. Rust's reliable runs still failed the
95% delivery gate; server peak sampled RSS was 3,808.8 and 4,256.2 MiB, compared
with 700.8 and 700.7 MiB for C#. The Rust observer recorded 32,909 and 32,702
replayed/duplicate messages. Its p95 latency histogram upper bound was about
8.39 seconds; both valid scene modes had a p95 upper bound of 8.191 ms on both
servers. The failed Rust runs cannot establish a reliable-throughput ranking.

Rust avatar runs applied approximately 4,933–4,940 observer updates/second, with
p95 applied gaps around 20.4–20.6 ms, all 99 peers present, no stale peers,
and no decode, malformed-item, or unapplied-delta errors. Every sender emitted
500 updates during the window.

C# avatar runs admitted all 100 clients, and the server logged metadata
serialization for all of them. However, 82–85 client sender records had 601
movement-frame visits and zero generated/sent full or delta updates. Those same
peers were stale at the observer. The Unity-policy sender waits for server
metadata, so these runs do **not** measure C# avatar throughput. No metadata
parse warning or pose decode error explains the failure. The C# configuration
uses the expected BSR field names; the zero-rate sentinel difference does not
explain why clients generated no packets. Missing usable metadata is established
by the sender behavior, but its delivery failure is not localized to the client,
server, or transport. C# also logged system UDP input-error increases; those
counters alone cannot determine which direction lost metadata.

## Three-client AdditionalAvatarData check

The same 128-byte additional-data workload ran once on Rust, then C#, with three
clients and the same warmup/window. Both runs passed: every sender emitted 500
updates, both observer peers remained present and current, and full/delta pose
application reported zero decode, malformed-item, or unapplied-delta errors.
This is functional coverage at small population, not a production capacity
measurement or an exact-byte C# AdditionalAvatarData wire assertion.

## Runner changes and checks

The portable scene and avatar runners now support `--server-kind csharp` with an
isolated published app and translated configuration. C# readiness combines health
visitors with client authenticated-join logs. The avatar runner now writes CPU/RSS
samples and a summary, validates sender cadence and actual application, and exits
nonzero on invalid results. Copied C# moderation/config state and logs are excluded
so prior tests cannot seed a run.

Eight focused Python tests passed, covering runtime preparation, config translation,
readiness, dropped scene work, corrupt avatar application, and stalled avatar
senders. Python compilation and `git diff --check` passed. Rust code and the tested
release binaries were unchanged during this test turn.

Reproduce using the commands in
[script-data-harness.md](script-data-harness.md), adding
`--clients 100 --warmup-seconds 3 --window-seconds 10` and, for C#,
`--server-kind csharp --server /path/to/published/BasisNetworkConsole`.
For scene runs use `--payload-bytes 128 --interval-ms 50`; test both delivery modes.
For avatars test `--additional-avatar-bytes 0` and `128`.

These are two short observations per large case on one host. Preserve the invalid
runs when investigating the Rust reliable queue growth and C# metadata delivery;
do not count reduced work as a server speedup.
