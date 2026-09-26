# Avatar sync performance evidence

This note records the checked-in workload fixture and the measurements relevant to the receiver-cycle correction. Compact, machine-generated per-run evidence for the prior I10/I12 optimization results is published under [`results/`](results/README.md). The historical I10/I12 results used a positional receiver cursor and are not measurements of the identity-cycle change below.

## Reproduce the workload

Build release binaries, then run:

```sh
python3 scripts/perf/run-avatar-workload.py \
  --server BasisRustServer/target/release/basis-server-console \
  --client BasisRustClient/target/release/basis-rust-client \
  --output /tmp/avatar-1500-run
```

The runner starts a local server from [`fixtures/avatar-1500-server.xml`](fixtures/avatar-1500-server.xml), starts the configured number of clients using [`fixtures/avatar-1500-client.xml`](fixtures/avatar-1500-client.xml), and writes readiness, observer, sender, server-pair and socket-drop CSVs plus the binary/config hashes. The default workload is 1,500 colocated peers, advertised 20 ms avatar interval, zero positional drift, no voice or P2P, and the Rust client's Unity-policy mode: a 60 FPS frame accumulator targets 20 ms updates (50 Hz average) and emits valid deterministic synthetic pose data. It is protocol/send-policy compatible for the exercised fields, not a capture of Unity runtime poses or a claim of full Unity runtime equivalence. Server BSR profiling is disabled for timing. The runner's fixture hashes intentionally differ from historical capture hashes because the checked-in templates use a dedicated `avatar-bench-only` credential and normalized XML; [`results/README.md`](results/README.md) records exact hashes and settings differences.

The script verifies 1,500 authenticated active states throughout its sampling window and configures the observer to expect all 1,499 other peers. Treat a run as invalid if readiness drops or observer coverage/decode/rejection counters fail. Observer gaps are applied-update intervals at one receiver; they are not end-to-end movement latency. `outbound_logical_avatar_sends` counts avatar items built for recipients, not successful network delivery; encoded send entries may bundle multiple avatars.

## Receiver-cycle correction check

At PR head `3f4bee3f41ef0de948039e0333af44f7d15138e7`, the receiver slice was selected by numeric offsets into an authenticated-peer vector that can change order and membership. The candidate changes selection to a bounded cycle of `(peer ID, incarnation)` identities. Each selected identity is resolved against the current authenticated snapshot, so current pose/state is used; departures and reused IDs are skipped, and new peers join at the next cycle boundary. Slice width is held for the active cycle, and adaptation takes effect at its boundary. Effective cycle length is `ceil(N / ceil(N / S))` and is used for interval advertisement and adaptive estimation.

The regression screen used the same client, configuration, binaries' release settings and 60-second profiler-off capture for control and candidate. Control source was the exact PR head above. Candidate source was the working tree with the identity-cycle correction and CI lint fixes; its binary hash and all input hashes were saved in the ignored run data. Both runs had all 1,500 peers ready and progressing at the intended input rate; the observer covered 1,499/1,499 with no missing peers, stale peers over 500 ms, decode errors, unapplied deltas, non-newer sequences or socket drops.

| Measurement | Control | Identity-cycle candidate |
|---|---:|---:|
| Server CPU, mean of two 30 s windows | 283.24% | 283.68% |
| Client CPU, mean of two 30 s windows | 102.71% | 105.86% |
| Built logical avatar items/s, mean | 4.091 M | 4.031 M |
| Observer applied-gap p50 / p95 | 550.82 / 587.91 ms | 557.00 / 598.68 ms |
| Observer peers covered | 1,499 / 1,499 | 1,499 / 1,499 |

This was one unreplicated forward-order pair. It observed roughly 1–2% slower cadence/output with essentially flat server CPU; the pair cannot distinguish run noise from a small cost. It is a correctness regression screen, not evidence of a performance win or statistically established regression. The scheduler correction is motivated by identity-stability/fairness under membership churn; the screen verifies stable-roster behavior and no loss of peer coverage in this workload.

The CI-equivalent Rust workspace/client commands and targeted identity-cycle tests are run before submission. Machine-specific raw counters, per-peer CSVs and binary outputs are intentionally excluded; rerunning the portable script records hashes and fresh CSV evidence for the local machine.
