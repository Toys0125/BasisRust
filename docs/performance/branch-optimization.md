# Transport cold-path layout and branch-miss testing

Branch: `performance/branch-optimization`, cut from `fix/dedicated-voice-processing` at
`1e45b3b`. Benchmark harness and checks are in
`BasisRustServer/crates/basis-transport/src/bench_branches.rs` and
`captures/branchmiss/` (local, not shipped with this checkout).

## Motivation

The transport hot paths — per-datagram property dispatch, message parsing, outbound packet
building, datagram merging, and ACK window processing — carried their rarely-taken error
handling inline. Inline error handling bloats the per-datagram code path, hurts instruction
cache locality, and gives the compiler no weight information for branch placement. Each
rarely-taken rejection is now extracted into a `#[cold]` helper so the hot path stays inline
and linear, and the per-datagram property dispatch is a constant lookup table instead of a
jump-table switch.

## What changed

All changes are in `BasisRustServer/crates/basis-transport/src/lib.rs`. Behavior is
identical; there are no public API changes.

- `PacketProperty::from_byte`: a 32-entry `const` lookup table (`FROM_BYTE_TABLE`) replaces
  the 19-arm match, so every received datagram's property decode is one masked load instead
  of a bounds check plus indirect jump.
- `process_ack`: malformed-length, forged/stale-window, and wrong-channel rejections now
  return through `#[cold]` `cold_rejected_ack`/`cold_ack_unknown_channel` helpers; the
  unknown-channel counter block (two atomic RMW ops) leaves the per-ACK path.
- `parse_message_packet`: truncated-datagram rejections return through the generic
  `#[cold]` `cold_rejected_packet`.
- `try_send_raw_to`: the WouldBlock counter block moved into `#[cold]`
  `TransportHandle::cold_send_would_block`.
- `record_pending_reliable`: the per-peer overflow shedding loop moved into `#[cold]`
  `shed_oldest_pending`; the per-message path keeps only the cap check.
- `allocate_peer_id`: the fresh-ID scan and exhaustion check moved into `#[cold]`
  `allocate_fresh_peer_id`; the accept path keeps the recycle-queue drain.
- `enqueue_reliable_payload`: the oversized-fragment drop log moved into `#[cold]`
  `cold_drop_oversized_reliable`.
- `enqueue_event`/`enqueue_lossy_event`: the queue-full blocking send and channel-closed
  error moved into `#[cold]` `cold_event_queue_full`/`cold_event_channel_closed`.
- `read_loop`: socket receive errors moved into `#[cold]` `cold_recv_error`.
- `process_packet`: the unknown-property trace and the no-arm logging arm moved into
  `#[cold]` `cold_unknown_property`/`cold_ignored_property`.

`MtuProbeState::accept_response` was left alone: only MtuOk datagrams reach it, so it is
already cold by call frequency, and its validation condition must stay inline regardless.

## Branch-miss benchmark

`bench_branches.rs` is a `#[cfg(test)]` module with one `#[ignore]`d test that exercises the
synchronous hot path over synthetic traffic where valid packets dominate (>=99%) and
malformed/error branches stay rare, using `std::hint::black_box` and a fixed-seed PRNG so
runs are comparable:

- `from_byte` property dispatch: 2,000,000 ops/pass, 1% invalid properties
- `parse_message_packet`: 1,000,000 ops/pass, 1% truncated packets
- `build_outbound_packet`: 100,000 ops/pass across all four delivery methods
- `build_merged_datagrams`: 2,000 batches of 32 packets/pass
- `process_ack`: 200,000 ops/pass at steady state (each ACK releases 16 in-flight sequences
  and the window is refilled behind them), 1% malformed ACKs
- `process_ack` no-op scan: 200,000 ops/pass with zero-bit ACKs, isolating the per-ACK parse,
  bounds checks, lock, and deque walk without refill churn

Baseline and optimized numbers come from the same harness, so only library code differs
between the two builds. Run from `BasisRustServer/`:

```sh
cargo test --release -p basis-transport -- --ignored bench_branches --nocapture
```

## Timing A/B: three runs per build

AMD Ryzen 7 9800X3D (8 cores), Windows, rustc 1.99.0, default release profile (opt-level 3,
no LTO, 16 codegen units). Each run reports the median of 9 passes; the table shows the
median across the 3 runs per build.

| Phase | baseline median | optimized median | delta | ranges overlap |
|---|---:|---:|---:|---|
| `from_byte` | 0.73 ns/op | 0.81 ns/op | +11% | yes (0.65–0.86 vs 0.77–0.84) |
| `parse_message_packet` | 11.89 ns/op | 12.00 ns/op | +0.9% | yes (11.42–13.76 vs 10.97–12.58) |
| `build_outbound_packet` | 55.10 ns/op | 52.17 ns/op | −5.3% | yes (51.70–56.52 vs 51.84–55.54) |
| `build_merged_datagrams` | 1611.55 ns/op | 1380.20 ns/op | −14.4% | **no** (1469.95–1651.15 vs 1317.10–1383.65) |
| `process_ack` (steady + refill) | 1260.02 ns/op | 1244.24 ns/op | −1.3% | yes (1256.55–1273.68 vs 1242.81–1323.14) |
| `process_ack` no-op scan | 267.42 ns/op | 266.11 ns/op | −0.5% | yes (266.42–270.08 vs 259.92–270.19) |

Reading of the table:

- `build_merged_datagrams` is the only phase whose baseline and optimized ranges do not
  overlap. That function was not modified by this branch; the delta is a code-layout effect
  of the extractions (function placement and instruction cache), which is exactly the class
  of effect `#[cold]` targets. A 3-run sample on a desktop machine does not pin the
  mechanism down, and the phase also includes its input-batch construction, so treat it as
  indicative rather than proven.
- `from_byte` is loop-overhead-dominated at sub-nanosecond scale; the table-versus-match
  difference is not resolvable by this harness.
- Every other phase is flat within run-to-run noise. No phase regressed beyond noise.

## Hardware branch-miss counters: not measured

Branch-miss counts need hardware performance counters, which were unavailable in this
environment:

- WSL2 is present (kernel 6.18.40.1-microsoft-standard-WSL2) but has no `perf` binary and no
  passwordless sudo to install one.
- `xperf` is installed (`Windows Performance Toolkit` 10.0.26100) and supports hardware
  counter sampling (`-PmcProfile`), but starting a kernel trace requires an elevated shell.
  The UAC approval for the elevated trace was canceled twice, so the counters were not
  sampled.

To rerun with hardware counters on a host that provides them:

```sh
# Linux (native or a container with perf):
perf stat -e branches,branch-misses \
    cargo test --release -p basis-transport -- --ignored bench_branches --nocapture
```

```bat
rem Windows, from an elevated shell; candidate counter names in first-to-last order:
xperf -on PROC_THREAD+LOADER -PmcProfile BranchMispredicts -SetProfInt 10000
target\release\deps\basis_transport-*.exe --ignored bench_branches --nocapture
xperf -d branchmiss.etl
xperf -i branchmiss.etl -o pmc-summary.csv -a profile -detail
```

The capture script that automates the elevated Windows path is
`captures/branchmiss/run-pmc-trace.bat`; it tries `BranchMispredicts,BranchInstructions`,
`BranchMispredicts`, `BranchInstructions`, then `TotalIssues`, and logs which source started.

## What was not measured

- Hardware branch-miss and branch-count deltas (see above).
- Profile-guided optimization (PGO): instrumented-build + representative-workload + rebuild
  was out of scope for this branch; `#[cold]` extraction is the static-layout half of what
  PGO's profile data drives.
- End-to-end multi-client server throughput: the bench isolates the synchronous hot-path
  helpers; socket I/O, tokio scheduling, and application-level work are out of its scope.

## Checks

- All 54 `basis-transport` tests pass with the changes; the bench harness stays `#[ignore]`d
  so ordinary `cargo test` runs skip it.
- `cargo fmt -p basis-transport -- --check` passes.
