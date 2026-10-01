# BasisVR moderation and permission parity

Implemented September 30, 2026 against BasisVR/Basis `developer` commit
[`81f190b217c1`](https://github.com/BasisVR/Basis/tree/81f190b217c11c2b39231e0bc9db330fd4a2803c),
committed September 28, 2026. Rust baseline was `28fb2a1e607a21fc34c14f0269cdade83e5f88d0`.

The gaps identified in the original audit are implemented. This covers server
moderation/admin permissions, their packets, live state, and existing files;
it is not a claim of complete BasisVR subsystem parity.

## Permissions and actions

| Area | Implemented behavior |
| --- | --- |
| Permission contract | All 44 canonical nodes, 12 default grants, 25 moderator grants, 96 action IDs, and 71 gated action mappings match the pinned C# source |
| Resolution | Case-insensitive identifiers/rules, inheritance with cycle protection, wildcard rules, negative rules, seeded default upgrade history, and inherited group queries |
| Migration | Old `basis.moderation.shout` grants, denies, and seed history migrate to `basis.moderation.announce` |
| Snapshot and queries | Full snapshot requires `basis.permissions.view`; connected-player node/group queries use the separate bounded, rate-limited request |
| Protection | Ban, kick, IP-ban, mute, rename, avatar, and locomotion actions enforce target protection, including the upstream self-action exceptions |
| Mutes | Persistent independent voice/text flags, state queries, live target notifications, reconnect replay, and voice/chat/typing enforcement |
| Announce and shout | Distinct current action IDs and state, permission bit 24 for announce, self-disable after permission loss, disconnect cleanup, reconnect replay, and channel-4 audio framing |
| Rename | Display-name sanitation, live metadata/join-record updates, and broadcast notification |
| Global settings | Persisted locomotion policy with upstream bounds/defaults, GIF lock and complete lock-state packets, live console updates |
| Restrictions | `Normal=0`, `BanList=1`, `AllowList=2`, `RejoinOnly=3`; rejoin population capture and reset-on-start behavior; UUID admission gates run after signed identity verification |
| Library | XML load/add/remove, fragment password extraction, raw/LZ4 client payloads, live broadcast and join replay; oversized additions rejected before persistence; oversized imported collections remain loaded and editable without blocking startup |
| Permission edits | Affected clients receive refreshed permission metadata; group changes refresh everyone; failed sends are retried |

Implementation: [dispatcher and authorization table](../BasisRustServer/crates/basis-server-core/src/lib.rs),
[moderation runtime](../BasisRustServer/crates/basis-server-core/src/admin_runtime.rs),
[permission store](../BasisRustServer/crates/basis-server-permissions/src/lib.rs).

## Existing files and paths

The console defaults to the executable directory, matching BasisVR. Use
`--base-dir .` to select the working directory. `--config` selects the config
file and subsequent admin saves write to that same file. Permission and
moderation data remain in `<base-dir>/config/`; library XML remains in
`<base-dir>/defaultlibrary/`.

| File | Compatibility |
| --- | --- |
| `config/config.xml` | Current restriction names/values, GIF lock, and global locomotion fields; legacy `None`, `BlackList`, and `WhiteList` names remain accepted |
| `config/permissions.xml` | Groups, users, parents, grants/denies, and `SeededDefaults`; real XML parsing supports compact formatting, single quotes, entities, and BOM |
| `config/banned_players.xml` | C# `ArrayOfBannedPlayer` schema, UUID/IP bans and flags, reasons, and `yyyy-MM-dd HH:mm:ss` timestamps |
| `config/muted_players.xml` | C# `ArrayOfMutedPlayer` schema, independent mute flags, and timestamps |
| `config/BasisAllowList.txt` | Current filename, one UUID per line; UTF-8 BOM supported |
| `config/BasisBanList.txt` | Current restriction ban-list filename, separate from persistent player bans; UTF-8 BOM supported |
| `defaultlibrary/*.xml` | C# `BasisDefaultLibraryConfiguration` fields `Mode`, `Url`, `Password` |

Legacy Rust `BasisWhiteList.txt` / `BasisBlackList.txt` are read when the
corresponding canonical file is absent. Mutations write canonical names and
retain the legacy files. Numeric restriction values follow the current C#
contract; legacy XML names retain their intended policies.

Malformed permission/moderation reloads return errors and retain live state.
Startup fails before overwriting invalid stores. Permission saves preserve
seed history and use atomic replacement; automatic writes are debounced and
flushed during shutdown. Backup exports retain pending primary saves. Both
permission and moderation files are synced before atomic replacement, and Unix
parent directories are synced after rename. Moderation writes complete before
replacing live state. Malformed server-name/MOTD requests preserve live and saved
settings. `HasFileSupport=false` initializes these stores in memory and suppresses
their disk reads/writes and automatic admin config writes. The console still
reads its selected configuration; explicit `/config save` remains available.
Changing `HasFileSupport` requires restart.

Legacy files without seed history record the former Rust default/moderator
baseline as already seeded. Missing old grants remain removed; genuinely new
moderator grants are added once. Imported history remains authoritative.

Library removal stages matching files and restores them if staging fails.
Interrupted, uncommitted removals recover during load; committed cleanup remnants
remain inactive. Oversized existing libraries retain every XML entry and can
be reduced through admin removals. Their broadcast resumes once the collection
fits the client's 65,535-byte raw packet limit; startup logs the unavailable
broadcast rather than failing the server.

Identity-enabled admission verifies an Ed25519 `did:key` signature against a
fresh, expiring peer challenge before applying UUID bans, allow/ban-list modes,
and rejoin/configuration-editor exceptions. This matches the pinned C# resolver's
supported DID method. `RejoinOnly` requires identity verification; it rejects
connections when `UseAuthIdentity=false` rather than trusting claimed UUIDs.

## Verification and limits

A checked-in [contract fixture](../BasisRustServer/crates/basis-server-core/tests/fixtures/basisvr-admin-contract.json)
was extracted from the pinned C# files. Rust tests compare the complete node,
default-grant, action-ID, and gate tables against it. Focused tests cover XML
compatibility and failure retention, seed migration, permission/protection
rules, mutes, rename, restriction transitions, policy persistence, disk-disabled
startup, legacy permission migration, staged-removal rollback/recovery, and
library persistence/reload including oversized imported collections.

The [loopback UDP test](../BasisRustServer/crates/basis-server-core/src/admin_runtime/wire_tests.rs)
uses actual transport packets to verify snapshot denial/allowance, inherited
group queries, mute notifications, muted announce audio suppression, ushort
sender framing, and live permission metadata refresh. Identity regressions also
verify spoofed captured/configuration-editor DIDs, signature replay on a fresh
nonce, valid captured-player rejoin, malformed responses, identity-disabled
rejoin rejection, and handshake timeout.

The final Rust workspace suite passes: **185 tests passed, 1 ignored**. Both
server-console and client compilation checks pass. Client tests pass (67), and
server/client Clippy checks pass with warnings denied. Unicode comparison uses
Rust's Unicode tables with .NET ordinal casing rules; older .NET/ICU versions
can differ for newly introduced characters. The C# runtime and a real BasisVR client
were not executed; .NET was unavailable. No deployed operator files were
present in the worktree, so production data has not been tested directly.

Reference sources: [permission manager][permissions], [moderation handlers][moderation],
[persistent mutes][mutes], [admin modes][modes], [permission bit indices][bits].

[permissions]: https://github.com/BasisVR/Basis/blob/81f190b217c11c2b39231e0bc9db330fd4a2803c/Basis%20Server/BasisNetworkServer/Security/PermissionManager.cs
[moderation]: https://github.com/BasisVR/Basis/blob/81f190b217c11c2b39231e0bc9db330fd4a2803c/Basis%20Server/BasisNetworkServer/Security/BasisPlayerModeration.cs
[mutes]: https://github.com/BasisVR/Basis/blob/81f190b217c11c2b39231e0bc9db330fd4a2803c/Basis%20Server/BasisNetworkServer/Security/BasisPlayerMuteManager.cs
[modes]: https://github.com/BasisVR/Basis/blob/81f190b217c11c2b39231e0bc9db330fd4a2803c/Basis%20Server/BasisNetworkCore/Serializable/Permissions/AdminRequest.cs
[bits]: https://github.com/BasisVR/Basis/blob/81f190b217c11c2b39231e0bc9db330fd4a2803c/Basis%20Server/BasisNetworkCore/Serializable/Permissions/PermissionBitsetMap.cs
