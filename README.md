# BasisRustClient

A headless Rust client for the Basis VR platform that implements the Basis protocol using `basis-protocol` and `basis-transport`.

## Overview

This is a headless client implementation that mimics the behavior of the Basis VR client. It connects to the Basis server and can simulate multiple clients with configurable behavior including movement, chat, and resource loading.

## Features

- UDP-based client using the same protocol as the Basis server
- Configurable via XML config file
- Multiple client simulation support
- Movement and camera simulation
- Chat support
- Resource loading simulation
- Automatic reconnection

## Running

```powershell
cargo run --release
```

Configuration is read from `Config.xml` by default. You can specify a different config and server address via command-line arguments:

```powershell
cargo run --release -- --help
```

## Config

The client reads a `Config.xml` file for configuration including avatar data, server settings, and behavior parameters.

## Building

```powershell
cargo build --release
```

## Dependencies

This crate depends on:
- `basis-protocol` — Protocol definitions and message types (shared with the server)
- `basis-transport` — UDP transport layer (shared with the server)
