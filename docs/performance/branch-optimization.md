# Transport cold-path layout and branch-miss testing

**Historical report — follow-up review:** see
[the laptop handoff](branch-optimization-handoff.md) and
[measured counters](results/branch-review-20261007-summary.json). WSL counters
were subsequently measured. The original decoder already compiled to a
branchless compare/conditional move in the inspected probe; the jump-table
rationale below is incorrect for Rust 1.99.0. The timing/layout interpretation
below is unproven, and subsequent counter/timing results are mixed.

The [completed laptop review](branch-optimization-laptop.md) retains both the
four-block original comparison and a two-block decoder-only alternative. The
match-restoration candidate was rejected after it failed to improve local
decoder timing and worsened parse timing. The original production code is
retained with corrected comments; no general application speedup is established.

Branch: `performance/branch-optimization`, cut from `fix/dedicated-voice-processing` at
`1e45b3b`. Benchmark harness and checks are in
`BasisRustServer/crates/basis-transport/src/bench_branches.rs` and
`captures/branchmiss/` (local, not shipped with this checkout).

## Motivation

The transport hot paths — per-datagram property dispatch, message parsing, outbound packet
building, datagram merging, and ACK window processing — carried their rarely-taken error
handling inline. The candidate extracts error handling into `#[cold]` helpers and replaces property decoding
with a lookup table. These are optimization hypotheses: source extraction alone does not
establish smaller optimized callers or better instruction-cache locality. Empty helpers
can disappear during optimization. The Rust 1.99.0 decoder probe compiles the original
match to a compare and conditional move, without an indirect jump.

## What changed

All changes are in `BasisRustServer/crates/basis-transport/src/lib.rs`. Behavior is
identical; there are no public API changes.

- `PacketProperty::from_byte`: a 32-entry `const` lookup table (`FROM_BYTE_TABLE`) replaces
  the 19-arm match in candidate `3606a9b`. The isolated optimized probe uses a table load;
  the original match uses a compare and conditional move. This is not disassembly of every
  production call site.
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
- `build_outbound_packet`: 100,000 ops/pass across Unreliable, Sequenced, ReliableOrdered
  and ReliableSequenced; ReliableUnordered is omitted
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
  overlap. That function was not modified by this branch. The variants ran in separate
  blocks, and these timings do not establish an instruction-cache or code-layout mechanism.
  The phase also includes input-batch construction. Treat the delta as an observation
  from this small synthetic experiment.
- These three-run timings do not establish statistical significance or absence of a
  regression. The subsequent balanced WSL study found the decoder slower in every block
  with more misses/op; the remaining phase results were mixed.

## Follow-up hardware counters

The later WSL study used direct `perf_event_open`, without needing the `perf` executable.
Eight processes per variant ran sequentially in four ABBA/BAAB blocks, pinned to CPU 2,
with nine measured passes per phase after one warmup. All 864 counter samples had 100%
scheduling coverage and matched workload outcomes. Full per-run timings, samples, totals,
hashes and assembly probes are retained in
[the desktop evidence](results/branch-review-20261007-summary.json).

The decoder had 36.3% more misses/op and median timing increased from 0.580 to 0.660 ns/op.
ACK/refill misses/op fell 47.6%, while median timing increased 2.1%. These mixed observations
do not establish a general speedup. Do not combine rates across phases with different
units: merge operations are 32-packet batches, while other phases use their own operations.

The later approved Windows xperf trace contained zero PMC sample rows. Successful trace
startup and ETL existence do not establish a counter measurement. The earlier unvalidated
`BranchMispredicts` commands have been removed. Windows measurements need supported
sources, actual nonzero events and matched traces, with sampled interrupts distinguished
from exact event totals.

Use the frozen runner on Linux x86-64 for per-thread user-mode counters:

```sh
RUSTUP_TOOLCHAIN=1.99.0 python3 scripts/perf/review-branches.py \
  --output captures/laptop-branches --baseline 1e45b3b --candidate 3606a9b \
  --blocks 4 --cpu 2 --pmc
```

The runner rejects existing output directories, mismatched workload outcomes, missing
counters and scheduling coverage below 99.9%. Choose a CPU available on the host. Windows
mode provides timings only. See [the handoff](branch-optimization-handoff.md) for setup.

## Measurement limits

The traffic mix and malformed rates are synthetic, not captured production distributions.
Workload outcome counts are not hardware branch counts; the parse payload total is a
checksum. Warmup and fixture construction are excluded; timed allocation/refill and small
clock/control overhead are included. Counters exclude kernel/hypervisor execution.

The harness does not exercise socket backpressure, event enqueue, receive dispatch,
shedding or ID allocation. `allocate_peer_id` first tries reusable IDs, then calls
`allocate_fresh_peer_id` when none is available, including during initial connections
and churn. Cold extractions have not been isolated for
attributing ACK changes to a helper. Representative runtime checks, application latency,
throughput and coverage would be needed for an application-level win claim. PGO and
end-to-end multi-client throughput remain unmeasured. Desktop findings do not establish
laptop or Windows release performance.

## Checks

- All 54 `basis-transport` tests pass with the changes; the bench harness stays `#[ignore]`d
  so ordinary `cargo test` runs skip it.
- `cargo fmt -p basis-transport -- --check` passes.
