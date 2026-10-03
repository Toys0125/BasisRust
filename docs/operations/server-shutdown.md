# Console shutdown

The console starts shutdown on Ctrl+C or `/shutdown`. A watchdog on a dedicated
OS thread gives shutdown five seconds, followed by at most 250 ms for a final
output flush attempt. It stays armed through Tokio runtime destruction. A
Tokio future timeout alone cannot bound synchronous file I/O, a thread join, or
started `spawn_blocking` work.

Shutdown stops transport admission and the avatar tick loop, attempts both
permission and database saves before waiting for workers, drains accepted event
handlers, then saves again to capture their final updates. The console retains
and joins the server event, leave-broadcast, permission, and avatar tick workers;
it stops and joins its serialized profiling/status output thread and aborts and
awaits its memory-reclaim task. A stop channel wakes the output thread promptly
even when the configured status interval is long.
GPU submission channels are closed and GPU worker handles are joined after
persistence. Tokio runtime destruction cancels remaining async transport/health
and authentication-timer tasks and releases their resources.

Started blocking memory reclamation, GPU readback, or native driver destruction
cannot be cancelled. If cleanup or runtime destruction stalls, the watchdog
attempts to flush stdout/stderr and exits with status 1. This explicit fail-stop
fallback intentionally skips remaining destructors. A stuck disk cannot be
made durable by a timeout; a stuck output sink cannot be guaranteed to flush.
Both are attempted before exit, with persistence before native joins and a
bounded final output attempt so a broken pipe/terminal cannot defeat shutdown.

File-backed deployments depend on the atomic database replacement in
[PR #18](https://github.com/Toys0125/BasisRust/pull/18). A watchdog can interrupt
any save, including the final save; the baseline truncating write cannot
preserve the earlier snapshot. Integrate atomic persistence before enabling
this lifecycle in a file-backed deployment. Database implementation changes
remain in the storage PR.

Rustyline's blocking stdin read has no portable cancellation API. The console
joins its input thread when finished and detaches an input thread still waiting
for a line. It checks the stop flag before executing any newly returned command.
Normal process termination reclaims that thread. No indefinite stdin join is
introduced.

Idle allocator reclaim is a bounded, best-effort owner sweep. Its existing wake
waves and five-second deadline cannot guarantee that every Tokio owner leaves
its park loop. It reports actual partial coverage; a deferred owner collects
once on its next park/unpark or explicit poll. Shutdown can abort the reclaim
task, and the runtime teardown watchdog covers any started blocking work.

Profiling retains its five-second snapshot window and counters. The tick thread
only captures the snapshot; a separate console output thread emits each new snapshot as
a single `BSR Profile` tracing event. Explicitly enabled profiling and periodic
status output use a dedicated diagnostic target enabled even when `--log-level`
is `warn` or `off`, preserving their previous unconditional output behavior. Slow output no
longer runs on the avatar tick or Tokio runtime worker threads. A stalled reporter can skip intermediate
windows because it reads the latest snapshot; health statistics retain the latest
window as before.

Focused checks (from the repository root):

```sh
CARGO_BUILD_JOBS=2 cargo test --manifest-path BasisRustServer/Cargo.toml -p basis-server-console shutdown::tests -- --test-threads=1
CARGO_BUILD_JOBS=2 cargo test --manifest-path BasisRustServer/Cargo.toml -p basis-server-core shutdown_saves -- --test-threads=1
CARGO_BUILD_JOBS=2 cargo test --manifest-path BasisRustServer/Cargo.toml -p basis-server-core profile_window_capture
CARGO_BUILD_JOBS=2 cargo test --manifest-path BasisRustServer/Cargo.toml -p basis-server-core --features gpu shutdown_drops_channels
```

The subprocess tests use mock uninterruptible blocking work to exercise runtime
teardown and forced exit, including a stalled output lock. They do not simulate
or prove cancellation of a GPU driver. Hardware validation must be reported
separately.

The pinned BasisVR reference (`81f190b217c11c2b39231e0bc9db330fd4a2803c`,
`Basis Server/BasisServerConsole/Program.cs`, `Shutdown`/`FinishLogging`) likewise
stops reduction/network workers, saves permissions, and bounds logging before
process exit. These Rust changes affect lifecycle and output only; protocol
bytes, auth/ACK/fragment behavior, flat PascalCase XML, spatial settings, and
upstream configuration defaults are unchanged.
