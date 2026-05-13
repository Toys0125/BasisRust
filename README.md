# Basis Rust Server

Rust workspace for porting the Basis C# server console and server runtime.

The current implementation establishes the reusable workspace, protocol crate,
server config compatibility, LiteNetLib-shaped UDP transport, server core
accept/auth/spawn routing, health endpoint, persistent storage scaffold,
permissions scaffold, console commands, and source drift detection.

## Run

```powershell
cargo run -p basis-server-console -- --config config/config.xml
```

Useful flags:

```text
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

## Drift Check

```powershell
cargo run -p basis-source-sync -- `
  --csharp-source "C:\Users\mgsta\Documents\Unity Projects\Basis\Basis Server" `
  --rust-source "C:\Users\mgsta\Documents\BasisRustServer"
```

## Current Scope

Implemented now:

- workspace and crate layout
- `basis-protocol` constants, readers/writers, config, message structs, DID payloads, server-info, avatar bundle helpers
- `basis-transport` UDP server event loop and LiteNetLib-shaped packet handling
- `basis-server-core` startup, auth parsing, accept/finalize, metadata/spawn fanout, basic movement/chat/database routing
- `/health`
- console commands: `/players`, `/status`, `/shutdown`, `/help`, `/clear`, `/config`, core `/perm`
- read-only source drift checker

Remaining work is the deeper subsystem parity: full DID identity resolution,
full LiteNetLib fragmentation/merge behavior, admin payload coverage, resource
preload semantics, PIP/camera/content-share state, full voice optimization, and
high-scale avatar reduction tuning.

