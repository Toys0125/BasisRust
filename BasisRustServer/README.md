# Basis Rust Server

Rust workspace for porting the Basis C# server console and server runtime.

The current implementation establishes the reusable workspace, protocol crate,
server config compatibility, LiteNetLib-shaped UDP transport, server core
accept/auth/spawn routing, health endpoint, persistent storage,
moderation and admin permissions, console commands, and source drift detection.

Voice uses dedicated batching and send threads, with bounded admission and fair
recipient turns. Avatar pose/delta input uses a separate thread; avatar downlinks
retain their tick and flush workers. See the
[voice processing and load-test report](../docs/performance/voice-isolation.md).

## Run

Requires Rust 1.95 or later. The Docker builder uses the exact 1.95.0 release;
see [prerequisites](../README.md#prerequisites).

Optional native Linux x64 [qualified PGO builds](../docs/performance/server-pgo.md)
train with the Rust client and require passing matched ordinary/PGO gates.
Ordinary builds retain their current defaults.

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

### Player storage compatibility and administration

The storage network protocol is BasisVR compatible. C# file saves use checksummed
binary `.bsr` records; Rust file storage uses JSON. When persistent **SQLite** is
selected, startup automatically imports valid C# saves from
`storage/players/<first-two-hash-characters>/<owner-hash>.bsr` next to the SQLite
database. Existing accounts are preserved, source files are unchanged, and
migration markers prevent restart from resurrecting subsequently wiped accounts.
An invalid new save rolls back the import and disables storage with an error.
File/JSON, memory, in-memory SQLite and PostgreSQL do not import `.bsr` files.

For backups, conversion checks, and the explicit SQLite-to-remote-PostgreSQL
migration script, follow the [player storage migration guide](../docs/operations/player-storage-migration.md).
Rust backend changes still do not synchronize or migrate existing JSON/SQL data.

Local console commands work with memory, file, SQLite and PostgreSQL backends:

- `/storage`: active backend, account count and serialized record bytes.
- `/storage show <uuid>`: a player's scopes, keys and value sizes, never values.
- `/storage wipe <uuid>`: permanently remove **all scopes** for that identity and
  reclaim its quota. This also removes corrupt records without decoding them.

Commands use the verified player UUID/DID as the identity, as stored in the
permissions data. They are local operator commands, not remote player requests.
Wipes are serialized with normal writes; a subsequent client write can recreate
an account. A console timeout does not cancel an operation already in progress.

Set requests consume both operation and byte tokens for every backend: ten writes
burst, two writes per second, plus a byte burst of twice the per-player quota and
refill of half that quota per second. At the default 16 KiB quota, this means a
32 KiB byte burst and 8 KiB/s refill. Delete and Clear consume write-operation
tokens but no byte tokens, so byte throttling does not prevent cleanup. After a
quota reduction, a shrinking payload larger than the new byte burst can spend
from a full bucket once; its complete byte cost remains as debt until refilled.

### Cilbox SQL player storage (optional)

`StorageEnabled`, `StorageBackend`, `StorageConnectionString`, and `StorageSqlTable`
are startup settings. Edit them in the server XML configuration and restart the
server; `/config set` rejects live changes to these fields without changing the
running configuration. `BASIS_STORAGE_URL` is also read only at startup.
Storage quota limits can be changed live.

Persistent storage reconciles aggregate usage at startup, then updates accounting
with each mutation instead of rescanning every account. File storage uses an OS
directory lock and a recovery ledger; SQL storage maintains totals with database
triggers. Deleting the last key or clearing the last scope removes the account's
file or SQL row. Legacy empty accounts are removed during startup reconciliation.
Stop the server before manually editing player files.

SQL table names are ASCII identifiers of at most 63 bytes. The PostgreSQL user
must own the storage table and its accounting objects, and have permission to
create tables, functions, and triggers in the configured schema.

Build or run with the `sql-storage` feature to add PostgreSQL and embedded
SQLite support. The feature is **off by default**; the existing `file` and
`memory` storage backends continue working without it.

```sh
cargo run -p basis-server-console --features sql-storage -- --base-dir .
# Or: cargo build --release -p basis-server-console --features sql-storage
```

Set these fields in `config/config.xml` (inside the `<Configuration>` element):

```xml
<StorageEnabled>true</StorageEnabled>
<StorageBackend>sql</StorageBackend>
<StorageSqlTable>basis_storage</StorageSqlTable>
<StorageMaxBytesPerPlayer>16384</StorageMaxBytesPerPlayer>
<StorageMaxTotalMegabytes>256</StorageMaxTotalMegabytes>
```

For **remote PostgreSQL**, supply its connection URL as
`BASIS_STORAGE_URL` (recommended, to keep credentials out of XML),
for example `postgresql://user:password@db.example.net:5432/basis?sslmode=require`.
Alternatively, set `<StorageConnectionString>postgresql://...</StorageConnectionString>`
in the server XML. `BASIS_STORAGE_URL` overrides the XML value. TLS is
available via SQLx/rustls; use `sslmode=require` for remote connections.

**SQLite fallback:** With no remote URL, or if PostgreSQL cannot be connected
to at startup, the server automatically opens
`<base-dir>/storage/players.sqlite3` (creates it if missing). SQLite is
embedded; there is no local database service to install. If
`HasFileSupport=false`, the fallback SQLite database is in-memory instead.

The chosen backend is logged on startup without printing connection
credentials. Remote and fallback SQLite databases are **separate datasets**:
falling back does not migrate existing PostgreSQL data, and when PostgreSQL
becomes reachable on the next server restart, it is used again. A live
PostgreSQL outage returns a storage-unavailable response rather than silently
switching stores in the middle of a session (avoids split-brain writes).
If PostgreSQL connects but its schema/accounting initialization fails, storage
is disabled with an explicit error instead of selecting a different database.
Back up the SQLite file and remote database independently. Changes to
`StorageBackend` likewise do not migrate old `file` storage records.

The server checks verified DID identity and the `basis.storage.use`
permission before allowing Get/Set/Delete/Keys/Usage/Clear. SQL writes use
transactions; PostgreSQL updates are serialized for cross-instance quota
correctness. Storage byte limits and per-player operation throttles apply to
both SQL variants. Non-SQL builds do not link SQLite or PostgreSQL libraries.
The SQL table name must be a safe SQL identifier (letters, digits,
underscores; cannot start with a digit). Only PostgreSQL remote URLs are
supported; a MySQL URL is not a PostgreSQL connection.

Image sharing uses the C# server's bandwidth governor and bounded image cache.
`ImageShareEgressMegabitsPerSecond` (default 200) sets each sharer's relayed
payload budget, counting recipient fan-out. Enforcement applies
`ImageShareEgressEnforcementPercent` (default 150, minimum 100) and permits two
seconds of burst credit. A chunk can spend beyond that credit; subsequent
messages are dropped until the debt refills. This can end an over-budget image
transfer. Setting the egress rate to 0 disables enforcement.

With `ImageCacheEnabled=true` (default), completed images are offered to arriving
players and peers who missed the original targeted share. Offers are small;
clients request nearby images, and the server sends the cached spawn, latest
transform, chunks, and eligible animation data in order. Each recipient's replay
is paced at `ImageShareDownloadMegabitsPerSecond` (default 200), with the same
two-second burst allowance and a 25 ms pump. A download rate of 0 sends new
replays inline; switching to 0 discards already queued replay work, matching C#.
Credit and debt persist between requests even when the replay queue drains.
GIF locks and their moderation bypass apply to animation uploads and cache requests.

`ImageCacheMaxMegabytes` (default 512) caps retained payload and chunk-slot bytes;
`ImageCacheMinimumPerOwnerMegabytes` (default 32) sets the floor for each owner's
fair share. Rust charges its actual chunk-slot size. Cached chunks are shared
with replay queues rather than copied per recipient. Despawn, eviction, and owner
departure invalidate replay handles and release their buffers immediately,
including payloads already taken by the pump. Cancellation is ordered against
transport admission so a removed card cannot be spawned by a later queued replay.
Departure clears the peer's
budget, queued downloads, and owned cache entries; server shutdown resets all
image state. `ServerState::image_egress_dropped()` exposes refused message and
fan-out byte counts, and `image_cache_stats()` exposes cache occupancy.

The reference implementation is BasisVR revision
`978a4b7202f299d20e79f4893bed11c879419f0a`, specifically
`Basis Server/BasisNetworkServer/Networking/BasisImageBandwidthGovernor.cs` and
`BasisNetworkImageCache.cs`. Image scene payloads remain wire-compatible and
ordinary scene traffic retains its separate governor.
Integration corrections are intentional: data rejected by upload enforcement
is excluded from the cache, so rejected transfers cannot bypass the limit through
cached downloads. Cache requests and despawns still update cache state when their
live relay is refused.
Download credit survives idle queues, and replay buffers cannot outlive their
cached image. Cached poses follow the latest relayed pickup transform: any player
can control a card in the C# protocol, so the original sharer is not a movement
authority. Server arbitration of pickup claims is outside this port.

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

`MaxUploadBytesPerSecondPerPlayer` defaults to `131072` (128 KiB/s) for each
authenticated connection before fanout. Set the same XML/environment field to
`0` to disable admission. The budget counts application payload bytes, including
avatar additions and Scene envelopes, once before coalescing or relay; transport
headers, retries and protocol/control traffic are excluded. It allows a two-second
burst. Excess unreliable/sequenced data is dropped and counted; excess reliable
data disconnects that connection with an explicit reason. A reliable frame larger
than the burst can spend from a full bucket and must repay its complete byte cost
before another such frame is admitted. Other data resumes only after that debt is fully repaid and new credit has accrued.

`BasisImagePickupManager` Scene traffic remains exempt and uses the separately
advertised image allowance. Rust enforces the separate image fanout governor and cached replay limits
ported from C#, described above. Image chunks bypass the generic upload cap
and are charged by that separate governor.
`extended.uploadBudget` reports accepted/rejected bytes/messages, reliable
rejections, oversized reliable admissions and image exemptions. This is separate
from the optional after-fanout Scene Mbps limit and pending-message queue quota.
`extended.images` reports image egress refusals/bytes and cache counts/bytes;
older binaries without that optional field have unavailable image telemetry.

Runtime overrides are not automatically saved. The explicit `/config save`
command writes all current in-memory values, including environment secrets, to
the selected config file. Config inspection redacts secret fields, but the saved
XML is private operator data. Ignore rules do not protect tracked sample edits.

## Docker deployment

From `BasisRustServer/`, Docker builds use the pinned official
[Rust 1.95.0 release](https://blog.rust-lang.org/2026/04/16/Rust-1.95.0/) and
[Alpine image](https://hub.docker.com/_/rust) (`rust:1.95.0-alpine`).
The multi-platform index digest is
`sha256:606fd313a0f49743ee2a7bd49a0914bab7deedb12791f3a846a34a4711db7ed2`;
`--locked` preserves the dependency lockfile. CPU-only builds are the default.
Docker daemon access and Compose are prerequisites for these commands.

Inject the password at runtime, rather than baking it into an image layer.
For Bash, read it without putting the value in command history:

```sh
read -rsp 'Server password: ' BASIS_SERVER_PASSWORD; printf '\n'
export BASIS_SERVER_PASSWORD
# On Linux, match the account that owns the bind-mounted files (use a non-root account).
export BASIS_UID="$(id -u)" BASIS_GID="$(id -g)"
mkdir -p config logs data/initialresources data/defaultlibrary data/CrashReports
docker compose -f docker-compose-server.yml up --build -d
unset BASIS_SERVER_PASSWORD BASIS_UID BASIS_GID
```

Compose requires a non-empty value. The image alone also accepts a mounted XML
password or either supported password environment name; it preserves the default
when none is provided. Never pass an empty `Password` value.

The container uses `--base-dir /app`, so the Compose mounts `./config:/app/config`
and `./logs:/app/logs` match the runtime paths. Bind mounts under `./data/` provide
writable storage for `initialresources`, `defaultlibrary`, and `CrashReports`;
config/log mounts retain their existing host paths. The server defaults to UID/GID 10001; set
`BASIS_UID` and `BASIS_GID` to the non-root owner of existing bind-mounted files.
A one-shot permissions service initializes only empty root-owned mount directories.
By default it never recursively changes ownership or alters existing files; operators must
provide mounts writable by the configured account. Docker
stops the server with SIGINT so its bounded shutdown saves persistent state.
Without the base-directory flag the executable
in `/usr/local/bin` would look under `/usr/local/bin/config`. The build context
excludes local config, logs, captures and target outputs. Container health still
binds to loopback by default; publishing port 10666 does not itself expose that
listener. To expose it intentionally, set `HealthCheckHost=0.0.0.0` and restrict
host/network access. UDP port 4296 is published. See [security guidance](../SECURITY.md).

When upgrading an older root-running Compose deployment, stop it and back up
`config/` and `logs/` before starting the new server. Export `BASIS_UID` and
`BASIS_GID` for the non-root account that should own the deployment files, as
above. If existing mounts or files are not writable by that account, use this
explicit, one-time migration:

```sh
BASIS_MIGRATE_ROOT_OWNERSHIP=true docker compose -f docker-compose-server.yml up --build -d
```

This opt-in transfers only root-owned entries in the five persistent mounts to
the configured account. It preserves other owners and file modes, does not follow
symlinks, and does not traverse nested filesystems. Subsequent starts should omit
the migration flag. Writable root-owned mounts (including group/ACL access) can
run without migration. Setup leaves populated mounts unchanged and lets the
server's actual file access determine whether permission changes are needed.

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
  - Legacy transport fields follow `EnableStatistics`, retaining their numeric types and names. BasisRust has no separate C#-style unreliable/voice queue, so `queuePerPeer`, `voiceQueuePerPeer`, and `droppedVoice` remain compatibility placeholders (`0`). In pinned BasisVR `81f190b`, the queue fields are per-peer **capacities**, and the drop fields count items shed on queue overflow. Rust's existing `droppedUnreliable` mapping remains the count of nonblocking UDP `WouldBlock` attempts, including reliable retries; it is not a C# queue-overflow count or an actual datagram-loss count.
  - The always-present `statisticsCapabilities` object reports `enabled` and availability for `sent`, `recv`, `packetsSent`, `packetsRecv`, `droppedUnreliable`, `droppedVoice`, `queuePerPeer`, `voiceQueuePerPeer`, and `transport`. `measured` means an actual Rust measurement, `partial` means one or both transport queue depths could not be sampled due to contention, `mapped` identifies the legacy `droppedUnreliable` mapping, `unsupported` identifies the three queue/voice placeholders, and `disabled` means a supported/mapped metric is omitted by `EnableStatistics=false`. An unsupported zero must not be interpreted as a measured empty queue or loss-free voice stream.
  - With `EnableStatistics=true`, the additive `transport` object exposes actual Rust transport metrics without requiring extended metrics. `peers` counts transport connections, including those awaiting application authentication. `reliablePending` counts in-flight reliable payloads/fragments awaiting ACKs; `reliableQueued` counts reliable payloads/fragments awaiting dispatch; `pendingDatagrams` counts reliable/ACK datagrams retained for socket retry. These are server-wide instantaneous totals, not per-peer capacities; sampling copies peer references before inspecting queues, never waits for queue locks, and is not atomic across queues. If any peer queue is contended, its entire aggregate (`reliableQueued` or `pendingDatagrams`) is `null` and `statisticsCapabilities.transport` is `partial`; other measurements remain available. Retained datagrams may overlap in-flight payloads, so do not sum the depths. `udpSendWouldBlock` counts refused nonblocking UDP send attempts (including retries/control traffic). `nonReliableDroppedDatagrams` counts only datagrams discarded on `WouldBlock` in nonblocking unreliable/sequenced sends; merged datagrams count once regardless of contained messages. It excludes reliable retries, control traffic, other send errors, and sends to disconnected peers. UDP/drop counters reset when transport statistics collection is re-enabled; depths reflect current connections. The entire `transport` block and legacy statistics are omitted, and their health callbacks are not sampled, when `EnableStatistics=false` (even if extended metrics are enabled).
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
