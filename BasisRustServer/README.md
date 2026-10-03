# Basis Rust Server

Rust workspace for porting the Basis C# server console and server runtime.

The current implementation establishes the reusable workspace, protocol crate,
server config compatibility, LiteNetLib-shaped UDP transport, server core
accept/auth/spawn routing, health endpoint, persistent storage,
moderation and admin permissions, console commands, and source drift detection.

## Run

Requires Rust 1.95 or later. The Docker builder uses the exact 1.95.0 release;
see [prerequisites](../README.md#prerequisites).

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

## Configuration

Server startup precedence (highest wins): explicit `--port`, `--health-host`,
`--health-port` and `--no-console`; environment overrides; XML supplied fields;
`ServerConfig` defaults. `--log-level` controls tracing separately. Environment
names use exact PascalCase config field names (for example `SetPort`); invalid
non-secret scalar environment values are ignored. `BASIS_SERVER_PASSWORD` takes
precedence over legacy `Password`. A supplied empty or non-Unicode password
environment override stops startup without printing its value; it never becomes
an empty-password bypass. No secret is generated automatically.

The default config path is `<executable-directory>/config/config.xml`, not the
tracked top-level `Config.xml`. A relative `--config` is resolved under
`--base-dir`; an absolute path is used directly. A missing file is created from
defaults before environment/CLI overrides. Malformed XML or invalid supplied
server scalar fields fail startup. Old config versions or missing extended
metrics fields are upgraded/saved before runtime overrides are applied.

`BasisRustServer/Config.xml` is a tracked minimal server sample with valid flat
PascalCase fields. Its former `Ip`, `Port`, `ClientCount`, `AvatarPassword`,
`AvatarUrl` and `AvatarLoadMode` tags belonged to the client template and were
ignored by the server. Client Avatar fields remain supported in the client's
tracked sample; the two schemas are distinct. Copy the server sample to an
ignored local config if desired:

```sh
mkdir -p config
cp Config.xml config/config.xml
cargo run --locked -p basis-server-console -- --base-dir . --config config/config.xml
```

From this workspace, these commands use `./config/config.xml`. Keep the
compatibility default password for local parity tests only; configure matching
non-empty deployment passwords on clients. `ApiEnabled=false` and
`MaxSceneRelayMegabitsPerSecondPerPlayer=0` remain upstream defaults. Operators
can explicitly enable/configure the API or set relay limits.

Runtime overrides are not automatically saved. The explicit `/config save`
command writes all current in-memory values, including environment secrets, to
the selected config file. Config inspection redacts secret fields, but the saved
XML is private operator data. Ignore rules do not protect tracked sample edits.

## Docker deployment

From `BasisRustServer/`, Docker builds use the pinned official
[Rust 1.95.0 release](https://blog.rust-lang.org/2026/04/16/Rust-1.95.0/) and
[Bookworm image index](https://hub.docker.com/layers/library/rust/1.95-bookworm/images/sha256-3a9f3b7a4dbe011e3f6ebf0dd363685af52c85af958a29276b47a460d6323575).
The multi-platform index digest is
`sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1`;
`--locked` preserves the dependency lockfile. CPU-only builds are the default.
Docker daemon access and Compose are prerequisites for these commands.

Inject the password at runtime, rather than baking it into an image layer.
For Bash, read it without putting the value in command history:

```sh
read -rsp 'Server password: ' BASIS_SERVER_PASSWORD; printf '\n'
export BASIS_SERVER_PASSWORD
docker compose -f docker-compose-server.yml up --build -d
unset BASIS_SERVER_PASSWORD
```

Compose requires a non-empty value. The image alone also accepts a mounted XML
password or either supported password environment name; it preserves the default
when none is provided. Never pass an empty `Password` value.

The container uses `--base-dir /app`, so the Compose mounts `./config:/app/config`
and `./logs:/app/logs` match the runtime paths. Without that flag the executable
in `/usr/local/bin` would look under `/usr/local/bin/config`. The build context
excludes local config, logs, captures and target outputs. Container health still
binds to loopback by default; publishing port 10666 does not itself expose that
listener. To expose it intentionally, set `HealthCheckHost=0.0.0.0` and restrict
host/network access. UDP port 4296 is published. See [security guidance](../SECURITY.md).

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
