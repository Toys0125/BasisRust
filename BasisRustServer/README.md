# Basis Rust Server

Rust workspace for porting the Basis C# server console and server runtime.

The current implementation establishes the reusable workspace, protocol crate,
server config compatibility, LiteNetLib-shaped UDP transport, server core
accept/auth/spawn routing, health endpoint, persistent storage,
moderation and admin permissions, console commands, and source drift detection.

## Run

Requires Rust 1.95 or later; use `rustup update stable` to update.

```powershell
cargo run -p basis-server-console -- --base-dir . --config config/config.xml
```

By default, data paths are relative to the executable directory, matching BasisVR.
`--base-dir .` selects the current directory. `--config` chooses the configuration
file; permission and moderation files remain under `<base-dir>/config/`.

Useful flags:

```text
--base-dir <path>
--config <path>
--no-console
--port <u16>
--health-host <host>
--health-port <u16>
--log-level <filter>
```

## Test

```powershell
cargo test
```

## GPU distance processing

GPU offload is excluded from default builds, including its backend dependencies.
CPU processing is used even if a saved config sets `EnableComputeOffload=true`.
To compile GPU support, build from this workspace with the explicit feature flag:

```powershell
cargo build --release -p basis-server-console --features gpu
```

Then set `EnableComputeOffload=true` in the server config to use GPU computation
of avatar distances, quality tiers, and encoded send intervals. The runtime
setting defaults to false even in GPU-capable builds. `ComputeDevice` accepts an
adapter name substring or index; an empty value selects an available hardware
adapter. Software adapters are rejected. An unavailable or failed GPU falls
back to CPU distance processing.

In the [matched 2,000-client tests](../docs/performance/cpu-vs-gpu-decisions-2000.md),
GPU offload was slightly slower on average: p95 update gaps were 862 ms versus
861 ms on CPU. That difference was within run variation, and throughput was
effectively tied. GPU also used 1.4% more server CPU and 50% more peak resident
memory. CPU is the recommended option for this tested workload; other hardware
and workloads may differ.

Two buckets hold captured peer positions and packed tier/interval decisions. The server reads
one immutable bucket while a dedicated worker uploads, computes, and reads back
the other. Completed buckets are published every
`ComputeDistanceUpdateIntervalTicks` ticks (32 by default). The server tick never
waits for GPU completion. A late result keeps the previous bucket active; data
older than two publication periods falls back to CPU processing. New peers and
reused peer IDs also use CPU processing until their incarnation appears in a
completed bucket. The GPU flags decisions near floating-point boundaries, and
the worker corrects them using CPU arithmetic on the captured positions before
publication. Receiver builds read ready decisions without floating-point checks.
Each pair uses two result bytes. Reduction-policy changes invalidate old buckets
and cached decisions; unsupported GPU policies use CPU processing.

This deliberately delays distance decisions until bucket publication; packet
building and transport remain on the CPU. With `HealthIncludeExtendedMetrics`
enabled, `extended.avatarSync.gpuDistance` reports the adapter, active epoch,
submissions, swaps, missed swaps, stale fallbacks, and errors. `computedPairs`
and `correctedPairs` count worker-produced decisions and boundary corrections;
`lastWorkerMicros` and `maxWorkerMicros` measure complete worker jobs, including
GPU waits, readback, and correction. A non-null active
epoch confirms that GPU results are in use. Set `EnableComputeOffload=false`
to use CPU distance processing exclusively.

## Drift Check

Checks C# source from the Basis git repo against local Rust source:

```powershell
cargo run -p basis-source-sync
```

Options:
- `--repo <url>` - Git repo URL (default: `https://github.com/BasisVR/Basis/`)
- `--branch <branch>` - Branch to check (auto-detected as 'developer')
- `--rust-repo <url>` - Alternative Rust repo URL
- `--rust-source <path>` - Path to local Rust source

```powershell
# Check a specific branch
cargo run -p basis-source-sync -- --branch main

# Use a different C# repo
cargo run -p basis-source-sync -- --repo https://github.com/example/CsharpSource
```

## Current Scope

Implemented now:

- workspace and crate layout
- `basis-protocol` constants, readers/writers, config, message structs, DID payloads, server-info, avatar bundle helpers
- `basis-transport` UDP server event loop and LiteNetLib-shaped packet handling
- `basis-server-core` startup, auth parsing, accept/finalize, metadata/spawn fanout, basic movement/chat/database routing
- `/health`
  - C#-parity transport fields follow `EnableStatistics`. BasisRust has no separate C#-style unreliable/voice queue, so `queuePerPeer`, `voiceQueuePerPeer`, and `droppedVoice` are `0`; `droppedUnreliable` maps to UDP `WouldBlock` drops.
  - `HealthIncludeExtendedMetrics=false` by default; enabling it collects and exposes the detailed `/status live` / `/status verbose` counters under `extended`. With it disabled, the app/avatar extended counters are not incremented.
  - `HealthIncludeBSRProfiling` controls the optional `bsr` block and enables BSR profiling collection. Rust has no managed GC, so the C#-parity `gc` object reports zeroed counters with `supported=false`.
- interactive console commands: `/players`, `/status`, `/shutdown`, `/help`, `/clear`, `/config`, core `/perm`
  - Tab completion is context-aware for commands, subcommands, config fields/values, permission users/groups/nodes, and permission file paths.
  - `/config set <field> <value>` applies supported settings live without writing disk; `/config save` persists the current in-memory config. Legacy `/config <field> [value]` syntax remains available.
  - Listener/bootstrap settings such as server/health bind addresses and `EnableConsole` are accepted into the in-memory config but are reported as requiring restart to fully take effect.
- signed Ed25519 `did:key` challenge verification before UUID admission gates; rejoin-only mode requires `UseAuthIdentity=true`
- moderation/admin permission parity with the pinned BasisVR reference: persistent bans and independent voice/text mutes, protected targets, announce/shout, rename, restriction modes, locomotion policy, GIF lock, permission queries and live metadata refresh
- BasisVR XML/text file compatibility, case-insensitive permission resolution, seeded default upgrades, and default-library mutation/broadcast; `HasFileSupport=false` keeps these stores in memory
- [Moderation and permission parity report](../docs/basisvr-moderation-permission-comparison.md)
- read-only source drift checker

Remaining work is the deeper subsystem parity: full LiteNetLib fragmentation/merge behavior, resource
preload semantics, PIP/camera/content-share state, full voice optimization, and
high-scale avatar reduction tuning.
