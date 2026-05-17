# BasisRust

Consolidated repository for the Rust Basis client and Rust Basis server workspaces.

## Layout

- `BasisRustClient/` - Headless Rust client for the Basis protocol
- `BasisRustServer/` - Rust server workspace, shared protocol crates, console app, and tooling

## BasisRustClient

`BasisRustClient` is a headless client that connects to a Basis-compatible server and can simulate client behavior such as movement, chat, and resource loading.

Common commands:

```powershell
cd BasisRustClient
cargo build --release
cargo run --release
```

Configuration is read from `Config.xml` by default.

## BasisRustServer

`BasisRustServer` is the Rust server workspace. It includes the server console, shared protocol and transport crates, support tooling, and Docker compose files for server-related workflows.

Common commands:

```powershell
cd BasisRustServer
cargo test
cargo run -p basis-server-console -- --config config/config.xml
```

Drift check command:

```powershell
cd BasisRustServer
cargo run -p basis-source-sync
```

## Notes

- Local config files such as `Config.xml` are ignored in the root repo.
- Generated captures, logs, and ZIP bundles have been removed from tracked history and are not part of this repo anymore.
- Each subproject keeps its own `Cargo.toml` and build output.

# Important Disclaimer
Majority of the code written was a direct translation from C# implementation of Basis Server using GPT 5.5 while I changed some code where needed. The main overhead I can see is using Rayon for the parallelization, causing majority of the overhead. The better implementation would to make my own threading scehduler.

The motiviation in the project was to see if converting the C# code base into rust will improve performance, in some places it did while others its about the same. Shows how well dooly has optimized the code base.
