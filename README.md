# BasisRust

Rust port of the Basis server and a headless Basis-compatible workload client.
Much of the initial port was translated from C# with GPT assistance and then
adapted and tested. Protocol parity and measured behavior remain the criteria
for changes.

## Prerequisites

- Rust/Cargo 1.95.0 or newer, installed with [rustup](https://rustup.rs/).
  The server core declares Rust 1.95; the Docker builder pins 1.95.0.
- A native C/C++ compiler and linker for Cargo's native dependencies (on Debian/
  Ubuntu, `build-essential` and `pkg-config`). GPU support is opt-in.
- Python 3 for the portable workload runner; Linux `taskset` only for CPU affinity.
- `ffmpeg` on PATH for the client's default voice re-encoding path. Avatar-only
  workloads do not require it. The C# interop harness separately requires .NET 8.

## Build and run

From a fresh checkout, build the two workspaces independently:

```sh
CARGO_BUILD_JOBS=2 cargo build --locked --release --manifest-path BasisRustServer/Cargo.toml -p basis-server-console
CARGO_BUILD_JOBS=2 cargo build --locked --release --manifest-path BasisRustClient/Cargo.toml
```

Start the server from the repository root:

```sh
BasisRustServer/target/release/basis-server-console --base-dir BasisRustServer --config config/config.xml
```

In another terminal, run the client from its directory so its tracked sample
`Config.xml` is found:

```sh
cd BasisRustClient
cargo run --locked --release
```

These commands use the compatibility password default for local testing.
For deployment, configure a non-empty password on both ends; see
[configuration and Docker deployment](BasisRustServer/README.md#configuration).

## Layout and reference

- [Architecture](docs/architecture.md): server workspace/domain crates and shared client dependencies.
- [Server](BasisRustServer/README.md) and [client](BasisRustClient/README.md): CLI, configuration and optional features.
- [Protocol and ACK vectors](scripts/interop/README.md), [moderation parity](docs/basisvr-moderation-permission-comparison.md), and [verified audit](docs/reviews/findings-verification-2026-10-03.md).
- [Fresh workload runs and published results](docs/performance/results/README.md), [historical artifact availability](docs/performance/artifact-availability.md).
- [Contributing](CONTRIBUTING.md), [security guidance](SECURITY.md), [MIT license](LICENSE).

Both `BasisRustClient/Config.xml` and `BasisRustServer/Config.xml` are tracked
samples. Ignore rules protect untracked local `Config.xml`, `config/`, captures,
logs and build directories; they do not hide edits to tracked samples. Keep local
credentials in ignored files or runtime environment settings. Each workspace has
its own `Cargo.toml`, lockfile and `target/` output; no root Cargo workspace exists.

## Performance evidence

Rayon currently executes parallel receiver builds. In a four-run ABBA
[2,000-client comparison](docs/performance/receiver-build-parallelism-2000.md),
reducing the minimum batch size from 16 to 4 lowered mean observer p95 update gaps
from 1,102 to 851 ms while server CPU rose from 325% to 458%. Those two repeats
per variant on one host show a cadence/CPU tradeoff in a synthetic loopback
workload. They do not establish that Rayon causes most overhead or that replacing
it with a custom executor would improve performance.
