# Architecture

The repository contains two Cargo workspaces with separate lockfiles/build
outputs. `BasisRustClient` uses path dependencies on the server workspace's
`basis-protocol` and `basis-transport`; keep the directory layout when building.

| Component | Responsibility |
|---|---|
| `BasisRustClient` | Headless UDP clients, synthetic movement/voice workloads and observer measurements |
| `basis-server-console` | CLI, config startup, console commands and runtime orchestration |
| `basis-server-core` | Admission/auth routing, peer state, avatar synchronization and optional GPU distance processing |
| `basis-protocol` | Shared wire structures, encoding, constants and server XML config |
| `basis-transport` | LiteNetLib-shaped UDP transport, reliability, ACKs, fragmentation and peer IDs |
| `basis-server-resources`, `basis-server-storage` | Resource/network-ID state and persistence |
| `basis-server-permissions`, `basis-server-admin` | Permissions, moderation and admin routing |
| `basis-server-health` | HTTP health/status reporting |
| `basis-source-sync` | Read-only upstream source drift tool |

The port follows the BasisVR wire layout and flat PascalCase server XML. See the
[ACK interop vectors](../scripts/interop/README.md),
[moderation comparison](basisvr-moderation-permission-comparison.md), and
[pinned audit record](reviews/findings-verification-2026-10-03.md) for reference
scope and remaining parity limits. Source drift alone does not establish runtime
compatibility.

Avatar synchronization uses Rayon workers; the receiver-cycle scheduling policy
is separate from that executor. GPU support requires an explicit build feature
and runtime setting. See the [server README](../BasisRustServer/README.md) for
current scope and [performance evidence](performance/results/README.md) for
workload/measurement limits. Workspace merging, a custom executor and nested XML
configuration are design proposals, not implemented prerequisites.
