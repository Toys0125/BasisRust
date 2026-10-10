# Prop/scene scripts and additional avatar data

The [latest-main rerun](script-data-main-rerun-20261009.md) records the 2026-10-09
Rust/C# comparison after pulling `main`.

The [100-client receive-fix report](script-data-receive-fix-20261008.md) records
matched Rust/C# runs and explains why Linux load clients must retain reliable
control entries in both merged packet formats.

Build the current server and client from the repository root:

```sh
cargo build --locked --release --manifest-path BasisRustServer/Cargo.toml -p basis-server-console
cargo build --locked --release --manifest-path BasisRustClient/Cargo.toml
```

## Prop/scene script relay

```sh
python3 scripts/perf/run-scene-workload.py --output /tmp/scene-unreliable --clients 100 --payload-bytes 128 --interval-ms 50 --window-seconds 30
python3 scripts/perf/run-scene-workload.py --output /tmp/scene-reliable --clients 100 --payload-bytes 128 --interval-ms 50 --window-seconds 30 --delivery reliable
```

Each client broadcasts a `SceneDataMessage` on `SCENE` (24), with an empty recipient
list and synthetic message index 60000. This exercises the server's prop/scene
script relay, not asset loading, ownership, physics, or Unity script execution.
The payload contains a sender index, 64-bit sequence, same-host timestamp, and
changing deterministic bytes. Payload sizes are 24–1024 bytes, keeping these
messages below the transport MTU. No voice or ongoing avatar movement runs;
the normal connection Ready message still initializes avatar state.

The runner owns its processes, uses available local ports, waits for authenticated
population, warms up, starts script sends with a marker, and checks population
throughout the measurement. Each sender emits once per interval; missed ticks
are skipped instead of caught up in a burst. Reliable sends report backpressure
when the pending queue reaches 256. Shutdown interrupts the scene timer so long
send intervals do not delay CSV output or client disconnection.
Scene worker and observer CSV failures return an error after disconnecting clients.
This is one scheduled sender worker, so high
loads can become client-limited; inspect client CPU alongside server CPU.

Outputs:

- `scene.csv`: observer client zero's unique messages, payload bytes, sender coverage,
  integrity errors, duplicate/older replay counts, reordered unique messages, and
  latency. Deduplication retains a bounded 256-sequence window per sender; older
  arrivals are conservatively excluded. p50/p95/p99 are log2 histogram upper bounds
  in microseconds; max is measured directly. Same-host timestamps do not establish
  latency between unsynchronized machines.
- `samples.csv`: server/client CPU seconds and RSS, plus authenticated population.
- `summary.json`: validity, observer delivery ratio, throughput, CPU core equivalents,
  and peak sampled RSS. Throughput is for **one receiving client**, not total fanout.
- `commands.json`, `workload.json`, logs, and retained server configuration:
  reproducible arguments, relevant environment, and binary/configuration hashes.

A run exits nonzero on missing senders, corruption, send errors, backpressure,
population loss, a missed send cadence (allowing one partial shutdown tick), or delivery below `--minimum-delivery` (default 0.95). The
ratio compares unique observer receipts with successful sends from other clients;
reliable sends count enqueue success, so an undrained queue reduces delivery.
Packets still in flight at shutdown can reduce the ratio. Do not interpret an
invalid run as a performance improvement. Use fixed binaries, population,
payload, cadence, delivery, configuration, and worker/CPU settings when comparing
revisions; repeat measured runs. This harness does not prove Unity interoperability.

The same workload is available directly through the client:
`--scene-data-bytes`, `--scene-data-interval-ms`, `--scene-data-reliable`,
`--observe-scene-csv`, and `--scene-start-file`. All are opt-in.

## Additional avatar data

```sh
python3 scripts/perf/run-avatar-workload.py --output /tmp/avatar-additional --clients 100 --additional-avatar-bytes 128 --warmup-seconds 5 --window-seconds 30
```

Use `--additional-avatar-bytes 0` for the matching pose-only control. A value of
1–255 adds one script-like `AdditionalAvatarData` item (message index 0, linked
avatar 0) to Ready, ordinary full avatar updates, Unity-policy keyframes, and
Unity-policy deltas. Bytes change deterministically with each sent sequence.
Full updates use the additional-data channel; deltas carry its flag and tail.
Defaults preserve the existing pose-only workload. The existing avatar observer,
sender diagnostics, and server pair CSVs measure application cadence, coverage,
and drops. Their observer verifies pose application; exact additional-byte
preservation is checked by the live regression below. Existing low-quality
stripping and additional-data lock policies still apply.

## Rust and C# server runs

Both portable runners accept `--server-kind csharp --server /absolute/path/to/BasisNetworkConsole`.
Use a published Linux C# apphost with its assemblies and runtime files beside it.
Each run copies the app into its own directory, removes prior config/moderation/log
state, writes the shared configuration and LiteNetLib sidecar, and records assembly
hashes. C# readiness requires both health `visitors` and authenticated client join
logs; final observer metrics verify actual forwarding/application.

The avatar runner also writes `samples.csv` and `summary.json`, and exits nonzero
for incomplete sender cadence, missing/stale peers, decode/application failures, or
unclean shutdown. Its Unity-policy client requires server avatar metadata; a
connected population alone does not establish that the avatar workload ran.
The receive-cadence, scene payload latency, and CPU measurements are different
quantities and should be compared only within the same workload.

## Regression tests

```sh
cargo test --locked --manifest-path BasisRustClient/Cargo.toml --workspace
python3 -m unittest discover -s scripts/perf -p 'test_*workload.py'
python3 -m unittest discover -s scripts/perf -p test_script_server.py
```

Live loopback tests run real executables and verify exact scene payload fanout
for both delivery modes, client receive metrics, and exact additional avatar
bytes. Unit tests cover Ready serialization, additional full/delta envelopes,
observer corruption/replay handling, and benchmark rejection of dropped work.

## Combined avatar and scene A/B

`compare-avatar-revisions.py`, `train-avatar-pgo.py`, and `tune-avatar-settings.py`
accept the same optional `--additional-avatar-bytes`, `--scene-data-bytes`,
`--scene-data-interval-ms`, and `--scene-data-reliable` flags. Defaults keep the
pose-only workload. For the existing 128-byte data cases, use:

```sh
--additional-avatar-bytes 128 --scene-data-bytes 128 --scene-data-interval-ms 50
```

One marker starts avatar observation and scene sends. CPU and avatar diagnostics
keep their fixed window; scene rates use the actual duration through shutdown,
including the observer receive grace. Scene delivery, coverage, integrity,
cadence, and payload byte accounting are additional validity gates. The
[1,500-client mixed PGO report](pgo-mixed-client1500-20261010.md) retains overloaded
trials and explains why failed delivery prevents a clean speedup claim.

The [750-client rerun](pgo-mixed-client750-20261010.md) reuses those same binaries
and profile, changing only the offered client population.
