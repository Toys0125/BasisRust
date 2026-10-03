# Findings verification — 2026-10-03

Reviewed Rust main commit 58c107ef3308a2c5973f5936205545330e051480. BasisVR reference: developer commit 81f190b217c11c2b39231e0bc9db330fd4a2803c, locally available in /home/mgstange/Documents/Basis. Comparisons use git show at the pinned SHA, not mutable checkout HEAD. This is source verification; no new performance experiment, exploit reproduction, or whole-repository test run was performed.

## Server

| Claim | Verdict and evidence | Disposition |
|---|---|---|
| Default password; ordinary equality | Confirmed placeholders/defaults in basis-protocol/src/config.rs:242, server Config.xml:2, Dockerfile:11; equality in basis-server-core/src/lib.rs:1285. BasisVR has the same default. No inspected evidence of real leaked credentials. | Preserve compatibility defaults; constant-time equality and safer documented deployment overrides. Never replace password with empty string: Rust treats empty as permissive. |
| u16 writer truncation | Confirmed basis-protocol/src/io.rs:89-107. String prefix includes terminator convention and overflows above 65534 UTF-8 bytes; other u16 byte lengths above 65535. | Checked error before writing any malformed prefix; preserve valid bytes and C# boundary convention. |
| join-count panic | Conversion expect at core/lib.rs:885 exists, but batches are capped at 32 KiB (core/lib.rs:210-234; protocol/messages.rs MAX_PAYLOAD_BYTES). Positive-length records cannot exceed u16 count in a valid batch. | Reject claimed reachable high severity. Defensive cleanup only alongside framing changes. |
| unauthenticated resource work | No per-IP limiter at core/lib.rs:1003-1045. Challenge RNG/map/timer happen after app/version/password checks and transport accept (1205-1248), not every raw packet. TTL adds population-dependent delay up to 45 seconds (84-92). Server-info replies stateless; NAT config gated, default disabled. | Bound admission and pending work with limiter cleanup, preserving legitimate handshake bytes and reconnects. Security extension to upstream, not a protocol default change. |
| database crash corruption | Confirmed bare fs::write at basis-server-storage/src/lib.rs:56-65. | Atomic same-directory replacement + durability; retain JSON schema and HasFileSupport=false behavior. |
| peer-ID allocator livelock | Confirmed full-u16 scan loop at basis-transport/src/lib.rs:804-816. Pending identity transports are not authenticated player_count, so peer_limit=65535 alone does not rule out occupying all 65536 IDs. | Return exhaustion cleanly, bound pending capacity, test wrap/full/reuse. |
| unbounded reusable/retired sets | Rejected mathematically: subsets of 65536-value ID universe. IDs recycle on disconnect (core/lib.rs:1648). | Preserve retirement guarantees; do not reuse IDs still referenced by queued traffic. |
| network-ID permanent exhaustion | Qualified: basis-server-resources/src/lib.rs:274-307 has finite 65536-name namespace; names remain stable during populated session. Resets when last authenticated peer leaves (core/lib.rs:1621-1642). | Test/document graceful exhaustion; do not recycle stable named IDs arbitrarily or widen wire IDs. |
| health auth/API defaults/zero metrics | Health unauthenticated (basis-server-health/src/lib.rs:285-312), defaults to localhost/loopback; external exposure requires configured bind. ApiEnabled=false matches BasisVR. Console metrics around 790-815 report zero for C# queue/voice counters because Rust has a different queue architecture; README:118 already documents this. | Truthful additive availability/actual Rust counters; do not invent C# queues or change legacy meanings. Preserve API default. |
| process::exit shutdown | Confirmed console/main.rs:964-970 intentionally avoids runtime teardown hanging on blocking GPU work after timeout. Destructors/logs can be lost. | Investigate bounded graceful shutdown and flush; retain necessary bounded escape path for uninterruptible backend work. |
| tick println | Confirmed avatar_sync.rs:1247ff, invoked from tick around 2012, profiling gated. | Route through tracing/serialized output without tick blocking. |
| Docker mutable tag | Confirmed Dockerfile:1 rust:1-bookworm. Requested 1.95 is not established as correct. | Verify supported toolchain, pin a tested release/image; no arbitrary downgrade. |
| spatial culling false | Conservative default; environment override exists at avatar_sync.rs:2667-68. No verified matching C# config switch. | No unconditional enabling without parity/quality and matched performance evidence. |
| relay limit=0 | Matches pinned BasisVR default (BasisServerConfiguration.cs MaxSceneRelayMegabitsPerSecondPerPlayer). | Preserve upstream default, document optional operator limits. |

## Client

| Claim | Verdict and evidence | Disposition |
|---|---|---|
| 8000-line main | Confirmed 8048 lines. Already depends on shared protocol/transport crates (Cargo.toml:13-14). | Narrow module extraction where fixes benefit; three new crates is a design proposal, not required bug fix. |
| duplicated reliable receive state | Qualified: client ReliableReceiveState at main.rs:1179ff and server AckState/process_ack have different roles, not near-identical copies. | Preserve LiteNetLib ACK vectors; share only demonstrably equivalent primitives. |
| poisoned StdMutex / convoy | Three mutex fields at 1788-92; poison expects around 2517-2647,3744,4412. Guards are short and dropped before await. Poison cascades are plausible; whole-batch crash and convoy claims unmeasured. | Define narrow non-panicking recovery/failure policy; no blanket async mutex replacement. |
| epoll fd-reuse vulnerability | Unsafe receiver around 4475ff; manual close at 4646/4683 owns epoll fd. Weak upgrade, registered index/fd, in_use, current socket fd and Arc lifetime checks mitigate reuse. Vulnerability not established. | Optional RAII cleanup while preserving fast path; mio migration is architectural. |
| silent malformed packet drops | Confirmed parse_packet 1104-20, handle_packet 2358-61, merged 2450ff. | Bounded counters/debug without changing intentional malformed-packet discard semantics. |
| XML fallback | Confirmed load_or_create 274-91; scalar fallback 419-25; no strict flag. | Add opt-in --strict-config for malformed XML and invalid supplied scalar values; preserve legacy default behavior. |
| local_peer_id/index, connection_number 0 collisions | Rejected as stated: distinct indices within batch and valid initial LiteNetLib connection generation (2016-25,2111ff); pinned InternalPackets/ReliableChannel agree. | No randomization or wire generation changes without reconnect evidence. |
| u8 observer sequence | Half-range comparison 1744-49 cannot disambiguate gaps >=128, including complete wrap. This is information loss inherent in byte sequence, not a reason to widen wire field. | Explicit diagnostic resync/baseline handling and tests; never claim ordering across ambiguous gaps. |
| observer only index 0 | Confirmed 2045ff; losing observer stops measurement. | Safe failover/ownership and mark measurement discontinuities. |
| mixed std/tokio UdpSocket | Rejected: uses tokio UDP; socket2 handles setup. | No blanket socket rewrite. |

## Repository, tests and recommendations

- Root MIT LICENSE already tracked and restored by 58c107e; server copy matches. No root license fix needed.
- Both client and server Config.xml templates are tracked despite ignore rules. Root README:45 wording is misleading. Server sample AvatarPassword/AvatarUrl are stale extra tags not server runtime fields; this is template documentation, not proven runtime schema divergence.
- XML defaults are overridden by supplied file values; malformed client XML falls back wholesale, invalid scalar falls back individually. Document CLI/default-file/explicit-path and environment behavior precisely for both executables.
- Captures/config/build outputs are local ignored data; generated history was removed. scripts/interop/bin and obj already ignored. .claude/settings.local.json is absent/untracked, so committed Windows-path claim is stale. Preserve user's untracked audio/ and mixed-quality-2000-profile.md.
- Historical performance archives have no hosted download, documented in docs/performance/artifact-availability.md. A tracked portable runner scripts/perf/run-avatar-workload.py and fixtures already exist. Fix broken results README runner link; publish current fresh-checkout commands and repeat/matched-measurement limits without claiming reproduction of missing historical inputs.
- No root Cargo workspace; relative client path dependencies work. Workspace merge is optional ergonomics and can affect feature resolution/locks, not a parity defect.
- No CONTRIBUTING/CODEOWNERS/SECURITY files found. Add factual contribution/security documentation; do not invent ownership or vulnerability reporting contacts.
- Root README lacks prerequisite/architecture/protocol/license links. Appropriate docs correction.
- “Only avatar unit tests / no integration tests” is false: about 296 test attributes across projects, 73 in client, UDP loopback tests in transport, admin wire tests, and real C# ReliableChannel vector harness. Missing coverage is a live client↔server scenario suite.
- Rayon is current executor. Existing repeated receiver-build report shows lower min-batch improved latency while increasing CPU; no evidence that it is the majority of overhead or a custom executor improves behavior. Existing receiver-cycle scheduler changes scheduling policy, not the worker executor.
- Large ServerState already has domain crates/modules. Full service extraction, nested config, centralized constants, platform layer and universal error-type rewrite are suggestions, not verified defects. Preserve flat PascalCase C# XML and defaults if later undertaken.
- Plain UDP matches upstream transport; adding TLS/DTLS would require a separate protocol agreement. Retain Ed25519 identity-auth and existing secret-log redaction; this review does not constitute an independent cryptographic audit.

## Authorized implementation threads

1. Checked protocol writers and framing boundaries.
2. Server admission/authentication hardening and peer-ID exhaustion.
3. Atomic persistent database durability.
4. Truthful health statistics and parity capability reporting.
5. Client config, packet diagnostics, mutex/observer resilience.
6. Bounded shutdown and profiling-output cleanup.
7. Deployment/config/repository/performance documentation and image pin.
8. Live client-server parity regression scenarios.

Each thread uses its own new T3 worktree, implements focused fixes, runs relevant checks and review, opens/links a PR to main, then uses T3 watch_pull_request. Broad speculative rewrites are excluded. No automatic merge.


## Deployment/documentation disposition update

The original source-verification text above remains a baseline record at
`58c107ef3308a2c5973f5936205545330e051480`; its line numbers describe that commit.
This update records only this deployment/documentation branch's dispositions.
Other implementation threads' findings remain unchanged here.

- Password deployment: retained the upstream `default_password`, disabled API
  and zero relay-limit defaults. Removed the Docker image/Compose password
  placeholder; Compose now requires explicit runtime injection. The existing
  legacy `Password` override is retained, with preferred `BASIS_SERVER_PASSWORD`
  precedence. Supplied empty/non-Unicode password environment values fail
  startup with value-free errors. Constant-time authentication work belongs to
  the admission/auth thread and is not marked fixed here.
- Docker: pinned the official Rust 1.95.0 Bookworm multi-platform index to
  `sha256:6258907abe69656e41cd992e0b705cdcfabcbbe3db374f92ed2d47121282d4a1`,
  verified against Docker Hub's registry and image `RUST_VERSION=1.95.0`.
  The core manifest requires Rust 1.95. Builds use the lockfile and two jobs.
  Added `/app` base-dir so mounted config/log paths are actually used; excluded
  local operator data and build outputs from the Docker context.
- Configuration: corrected the tracked server sample to real server fields,
  removing stale client-only fields. Documented tracked samples versus ignored
  overrides, server/client path and XML behavior, environment/CLI precedence,
  and explicit `/config save` persistence of secrets. No nested XML/schema or
  protocol-default change was made.
- Repository: added prerequisite, architecture, protocol, existing license and
  audit links, factual contribution/security guidance without invented contacts
  or CODEOWNERS, and narrow root/client runtime-log directory ignore rules. Audio input assets remain
  visible; generated captures should live under already ignored `captures/`.
- Performance docs: fixed the runner link and added exact fresh-checkout
  build/run/output steps and repeated matched comparisons. Qualified the Rayon
  statement using the existing four-run 2,000-client batch experiment, including
  its CPU tradeoff and two-repeat/one-host limits. No new performance result or
  historic reproduction is claimed; missing archives remain unavailable.

Validation for these scoped changes: eight configuration tests (including
password precedence/empty/Unicode rejection and existing XML round trips),
locked native release server build on Rust 1.95.0, native startup/health/clean
SIGINT and no secret logging/automatic persistence, Compose validation and
missing-password rejection, formatting/whitespace and changed-doc local links.
CodeRabbit's authenticated scoped review completed on all 15 changed files with
zero findings. No unrelated protocol/default or performance changes were made.

The pinned builder's amd64 manifest
`sha256:4c2fd73ef19c5ef9d54bee03b06b2839a392604fbfcd578ed948b71b37c1d7fb`
and every downloaded layer were verified against the registry. A locked release
build passed inside that extracted image with PRoot, then the binary ran in the
extracted Debian Bookworm slim runtime: CLI help, health readiness, `/app` config
paths, no secret logging/automatic persistence, and process-group SIGINT passed.
Docker daemon access was denied on this host, so an actual `docker build` or
Compose launch was not run; this validates image/toolchain/runtime compatibility
without claiming Docker daemon execution.

The locked native client release build also passed. A four-client, zero-warmup,
two-second portable-runner smoke completed with the documented binary/CLI paths
and all documented artifacts present; observer and server diagnostic CSVs had
rows. This is a command/output smoke check, not a performance measurement or
historical reproduction. Raw smoke outputs remain ignored under `captures/`.
