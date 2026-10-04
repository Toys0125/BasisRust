# Contributing

Open a pull request with a concrete problem, resulting behavior, affected scope,
and the focused checks you ran. Include limitations and reproducible commands.
There is no CODEOWNERS assignment in this repository; do not infer ownership.

Build requirements and workspace commands are in the [root README](README.md).
Run checks from the affected workspace, for example:

```sh
cargo fmt --manifest-path BasisRustServer/Cargo.toml --all -- --check
CARGO_BUILD_JOBS=2 cargo test --locked --manifest-path BasisRustServer/Cargo.toml -p basis-protocol
CARGO_BUILD_JOBS=2 cargo test --locked --manifest-path BasisRustClient/Cargo.toml
```

Select the affected crates/tests instead of compiling unrelated optional GPU
features. Keep existing wire vectors and valid encoding intact. For parity
changes, cite a specific BasisVR commit and matching source layout rather than
mutable upstream HEAD. Preserve flat PascalCase XML, field names and documented
compatibility defaults. The [interop harness](scripts/interop/README.md) can
regenerate authoritative C# ACK vectors with .NET 8.

Keep credentials, local configs, logs, audio captures, binaries and raw benchmark
captures out of commits. The tracked Config.xml files are samples; the client's
audio directory can contain source inputs and is not ignored wholesale. Put
generated captures under `captures/` and logs under `logs/`. Review staged files
with `git diff --cached` and `git status --short` before committing.

For performance changes, use [matched repeated runs](docs/performance/results/README.md#fresh-checkout-workload)
and report update gaps, work/coverage/errors, CPU and throughput over matching
windows. Record source, binary/config hashes and host/affinity; one run is an
observation. Historical summaries cannot replace unavailable raw captures.

See the [MIT license](LICENSE) and [security guidance](SECURITY.md).
