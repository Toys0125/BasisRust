# Reliable receive loss and reordering comparison

A compact machine-readable audit summary, including all 36 sanitized per-sample observations, the peer-snapshot enumeration microbenchmark, and historical voice study aggregates, is available in [benchmark-summary.json](benchmark-summary.json). This controlled transport experiment compares `6009c2bc556cd483e02e07d4b07030147645b548` with the bounded reorder-window implementation committed with this report. The table below summarizes all 36 observations. Exact samples, source and binary hashes, and per-run ranges are archived locally under `captures/pr31-results-history-cleanup-20261008/results/reliable-receive-20261008.json`. Historical commit IDs identify the captured builds; the baseline's equivalent runtime source after the artifact-only history rewrite is `8079fa33fa47`.

## Method

Both endpoints are actual Rust `TransportHandle`s. The sender queues 512 ReliableOrdered messages of 16 bytes on channel 0. A two-socket UDP proxy forwards real receiver ACKs unchanged; application delivery is measured from the receiver's `ServerEvent::Message` events. Each transport uses two receive workers. The message payloads, harness, worker counts, and fault schedules are identical in both revisions.

The no-fault scenario injects no faults. The loss scenario drops the first transmission of sequence 10 and every sequence congruent to 7 modulo 20: 27 initial packets. The reorder scenario forwards sequence 11 before sequence 10, delaying sequence 10 by 10 ms. Concurrent receive workers can also process datagrams out of order, including in the no-fault scenario.

Each scenario runs sequentially in baseline/candidate/candidate/baseline order, repeated three times: six samples per revision. Builds use release mode, Rust 1.95.0, and separate Cargo target directories. Frozen binary symbols were checked to confirm that only the candidate contains the new reorder budget implementation. An earlier comparison reused the candidate library through a shared Cargo target; those measurements were discarded.

Host: Linux, AMD Ryzen 9 5900HX, 16 logical processors, 33,492,484,096 bytes RAM, NVIDIA GeForce RTX 3080 Mobile. Both transports and the proxy share this host and process. Builds and other benchmark runs had finished before measurement. CPU includes the entire benchmark child process and its measurement wrapper; peak RSS covers the benchmark process. This is a small control-message experiment, not a voice capacity test.

## Results

Every sample delivered all 512 unique messages in order, with zero application duplicates or ordering/payload-index errors. The table gives medians across six samples; latency p95 values are calculated within each sample, then summarized by their median.

| Scenario | Revision | Retransmitted packets | Completion, ms | First transmission to application delivery p95, ms | CPU, ms | Peak RSS, KiB |
|---|---|---:|---:|---:|---:|---:|
| No injected fault | Baseline | 571 | 1,576.2 | 596.0 | 95.8 | 3,976 |
| No injected fault | Candidate | 0 | 13.3 | 0.3 | 14.2 | 4,014 |
| Initial packet loss | Baseline | 709.5 | 1,801.1 | 596.5 | 113.8 | 4,038 |
| Initial packet loss | Candidate | 27 | 1,200.8 | 299.2 | 81.8 | 3,962 |
| Delayed sequence | Baseline | 709.5 | 1,801.1 | 596.5 | 113.8 | 4,038 |
| Delayed sequence | Candidate | 0 | 25.5 | 11.7 | 15.5 | 3,928 |

The candidate retransmits only the 27 deliberately lost packets in the loss scenario. Retaining later packets avoids the additional retransmissions caused by rejecting them before the missing predecessor arrives. Actual packet loss still incurs retransmission delay. The baseline also shows substantial retries without proxy faults, consistent with concurrent receive workers processing later datagrams first.

These observations support the bounded reorder window for this configuration. They do not establish throughput or memory use for large messages, many peers, or the 1,000-speaker workload. The byte budget accounts for retained payloads plus a fixed entry charge; it is not a total process memory limit. Regression tests separately cover queue saturation, sequence wraparound, duplicate suppression, budget limits, and release during disconnect.

## Peer snapshot follow-up

The review of `dc122ac` identified a fresh allocation and an `Arc` clone for every peer on every 2 ms dispatch pass, including idle peers. The follow-up filters for reliable send or ordered receive work before cloning and reuses the snapshot vector. Map guards still end before dispatch or callbacks.

A release-profile microbenchmark compares the main-style guarded scan, the previous all-peer snapshot, and the filtered reusable snapshot on the same 1,000-entry peer map. Each case uses six samples per strategy, 5,000 passes per sample, and alternating ABC/CBA order. Every strategy asserts identical active-peer coverage. Median elapsed time per pass:

| Active peers | Main-style scan, µs | Previous snapshot, µs | Filtered reusable snapshot, µs |
|---|---:|---:|---:|
| 0 | 12.440 | 20.037 | 12.823 |
| 100 | 12.562 | 20.528 | 13.849 |
| 1,000 | 12.369 | 20.735 | 21.785 |

The candidate avoids all idle-peer reference clones and creates no fresh vectors after warmup. Its measured enumeration cost is lower with zero or 100 active peers; with all peers active, it is about 5.1% higher than the previous snapshot. These are six samples from one experiment, not independent repeats. The main-style scan omits callbacks, and none of these timings include network sends, application latency, or whole-server dispatch. They do not establish a server throughput gain or regression. Configuration, source hash, exact samples, operation counts, and validation are retained in [benchmark-summary.json](benchmark-summary.json).

Run the focused measurement with:

```bash
cargo test --release --manifest-path BasisRustServer/Cargo.toml \
  -p basis-transport peer_snapshot_microbench -- --ignored --nocapture
```

Application handler ordering is covered by `reliable_ordered_handlers_wait_for_the_previous_handler`: a blocked first handler prevents the next handler on the same session/channel from starting, while independent lanes can proceed. Only runnable lane heads take control worker permits.

Ordered backlog has limits of 128 waiting events per session/channel, 256 per peer, and 4,096 globally. A session exceeding its local limit is explicitly disconnected and its waiting events are pruned. Accepted control messages are not silently dropped while that session continues. At aggregate saturation, only the separate ordered ingress receiver pauses; the regular control/lifecycle receiver remains enabled. The ordered ingress holds at most 128 events, and a full ingress queue leaves new ordered sequence admission and ACK state unchanged until retries succeed. Already acknowledged reorder entries remain protected by the existing transport budgets. Global pressure does not disconnect an unrelated arriving peer.

`saturated_ordered_lane_does_not_block_other_control_or_disconnects` injects events through the production receivers/scheduler with real connected sessions. One handler holds a blocked read lease while 4,096 arrivals flood its lane, and another peer's control handler and disconnect request still finish. Separate tests fill the global pending budget and verify regular event admission remains open, and fill transport ordered ingress to verify independent control admission plus cursor/ACK preservation on retry. These are dispatcher and transport regressions, not a new 1,000-user performance measurement.

## Reproduce reliable receive measurements

Overlay the same [benchmark source](../../BasisRustServer/crates/basis-transport/examples/reliable-loss-benchmark.rs) onto a clean baseline checkout and the candidate checkout. Build each using a separate target directory, and freeze both binaries before running them. For each revision and scenario:

```bash
cargo build --release --manifest-path BasisRustServer/Cargo.toml \
  -p basis-transport --example reliable-loss-benchmark
BASIS_UDP_RECEIVE_WORKERS=2 \
  BasisRustServer/target/release/examples/reliable-loss-benchmark \
  --scenario loss --messages 512
```

Use `nofault`, `loss`, and `reorder` for the three scenarios. The binary emits one JSON record and remains available to observe duplicates for 50 ms after the sender's pending and queued reliable counts reach zero. CPU and peak RSS were collected externally using Python `resource.getrusage` and `/usr/bin/time`; they are combined process measurements, not server-only costs. Local raw captures and frozen binaries are retained under `captures/control-reorder-review-20261008`, which is ignored by Git.
