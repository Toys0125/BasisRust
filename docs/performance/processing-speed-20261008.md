# PR #31 processing-speed comparison — 2026-10-08

## Results

The `94b42f9` admission fix showed no material realtime regression in these matched runs. Relative to its parent, the combined one-peer control probe rate was 2.2% lower, with a 0.9 microsecond increase in median per-trial p95 completion time. The 64-peer control medians were effectively unchanged. These are observations on one host, not statistical significance or server capacity claims.

The tested queue repartition did not improve the medians meaningfully and was **not adopted**. The runtime for those measurements was `94b42f9`; the newer ordered-admission follow-up is measured below.

## Revisions and setup

| Label | Revision | Description |
| --- | --- | --- |
| main | `ccedc4280dbe67cb5701ccdde3520df948502dc7` | Current target branch reference |
| parent | `da0aafc6862660c526da05f6668060ce2a33bf51` | Ordered application handlers and reusable peer snapshots |
| current | `94b42f97c28d203cb7983f5399bc3df27b608639` | Independent ordered ingress and bounded per-lane admission |
| budget variant | Local modification of current | 2,048 ingress + 2,176 waiting slots; same combined 4,224-slot bound |

Host: AMD Ryzen 9 5900HX, 16 logical processors, 33,492,484,096 bytes of OS-reported RAM, NVIDIA GeForce RTX 3080 Laptop GPU. Linux, performance CPU governor, Rust 1.95.0. GPU processing was disabled. Normal desktop activity was not isolated.

Every revision was exported from committed source and built in release mode with a separate Cargo target directory. Existing workspace edits were excluded. All realtime trials used one frozen client binary from committed current source, the same harness and fixtures, and the same audio corpus. Binary, fixture, harness, probe and manifest hashes are recorded in [benchmark-summary.json](benchmark-summary.json).

## Realtime workloads

Each workload used the sequential order main / parent / current / current / parent / main: **two samples per revision**, each with 20 seconds of warmup after authenticated/active readiness and a 60-second measurement. Server Tokio and Rayon workers were both 16; client Tokio workers were four. All clients were colocated, P2P disabled, and server diagnostic CSV/profiling disabled.

### Avatar-only: 1,000 clients

Default Linux shared receive and socket load-sink filtering were retained. Avatar coverage and cadence are measured at one observer. All six samples passed the readiness, sender progress, coverage, freshness, decode, sequence and transport/protocol checks. Every sender produced at least the required 2,900 updates in the window; measured server avatar ingress was approximately 50,000 updates/s.

Medians of the two trial values:

| Revision | Avatar gap p95 | Observer applied items/s | Server CPU cores | Client CPU cores | Server peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| main | 172.81 ms | 6,327 | 4.676 | 0.614 | 193.8 |
| parent | 159.56 ms | 6,710 | 4.717 | 0.591 | 243.0 |
| current | 160.78 ms | 6,718 | 4.694 | 0.587 | 234.8 |

Current versus parent: p95 +0.8%, applied throughput +0.1%, server CPU −0.5%. The measured difference is small. Current versus main improved observer cadence at similar CPU cost; it used more memory in this workload.

### Voice plus avatars: 250 clients, 25 speakers

Linux shared receive and the socket load-sink filter were disabled. Every client received voice. The same 40 real source clips were encoded once as mono 48 kHz, 32 kbps Opus with 20 ms frames, using the harness encoder settings. No synthetic or single-clip replacement was used.

All six trials passed capture validation, sampled payload/hash and Opus decode validation, and the voice capacity checks **for this configuration**. Unique receipt counters were available; duplicate, reordered and ambiguous voice counts were zero. Offered voice cadence was approximately 1,250 frames/s, or 100% of nominal.

| Revision | Voice gap p95 histogram floor | Unique fanout received | Avatar gap p95 | Server CPU cores | Client CPU cores | Server peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| main | 20.5 ms | ~100% | 28.99 ms | 5.585 | 2.037 | 71.4 |
| parent | 21 ms | ~100% | 21.64 ms | 3.453 | 0.952 | 86.9 |
| current | 21 ms | ~100% | 21.79 ms | 3.466 | 0.963 | 87.6 |

Current versus parent: server CPU +0.4%, avatar gap p95 +0.7%, with the same median voice gap. Current versus main used 37.9% less server CPU while sustaining comparable voice delivery; server RSS was higher at this population. Slight receipt ratios above one in individual samples reflect in-flight frames crossing the measurement boundaries; exact ratios are retained in the JSON. The voice histogram values describe arrival cadence, not send-to-receive latency.

### Ineligible 1,000-client voice attempt

Before selecting the smaller voice population, main was tested with 1,000 clients and 100 speakers. It sustained only **13.6% of nominal voice send cadence**. Avatar freshness and delta application checks failed, and unique voice receipt tracking became unavailable. Server/client CPU consumption was approximately 11.14/3.55 cores. The next trial was interrupted and excluded.

These shared-host runs could not provide comparable offered voice load at that population. They were excluded from the speed comparison. No latest-revision 1,000-speaker capacity conclusion follows from this work.

## Live control processing probe

[registry-processing-benchmark.rs](../../BasisRustServer/crates/basis-server-core/examples/registry-processing-benchmark.rs) creates real UDP peers, authenticates their Ready metadata without password/identity authentication, and sends reliable ordered registry subscriptions through the transport and ordinary application dispatcher. Each successful trial sends 32,768 requests with globally unique subscription IDs.

The identical probe source is overlaid on each revision. It uses four Tokio workers, 16 Rayon threads and the default allocator, rather than the console's mimalloc allocator. Server and UDP driver share this process. Completion includes request submission, UDP reception, dispatch and visibility of committed final state. Six samples per revision/configuration use main / parent / current / variant / variant / current / parent / main, repeated three times.

The driver implements the 128-sequence sender window, correct 21-byte bitmap ACKs and unchanged-packet retries after 100 ms. Before timing, the initial server reliable queues must drain. Every successful trial requires the exact application inbound-count delta, each peer's final singleton subscription ID, zero reported protocol/ACK errors and empty client pending ACK windows. This checks the processing total and final state; it does not observe every intermediate application state. Whole-process CPU observations include startup/shutdown and the driver, and are available in the JSON; they do not isolate server processing CPU. The observer actively polls with cooperative `yield_now` on the same four-worker runtime and contributes CPU and map-read contention. This load is matched across revisions but limits attribution of small differences to server code alone. CodeRabbit reported this as a minor measurement-method concern; its suggested 1 ms polling would mask the approximately 30 microsecond single-request behavior, so active polling was retained and the limitation is explicit.

| Population / requests in flight per peer | main requests/s | parent requests/s | current requests/s | Current versus parent |
| --- | ---: | ---: | ---: | ---: |
| 1 peer / 1 | 47,469 | 44,711 | 43,722 | −2.2% |
| 64 peers / 1 | 128,511 | 127,485 | 128,543 | +0.8% |
| 64 peers / 64 | Failed final-state check | 135,225 | 135,231 | ~0% |

Median per-trial p95 batch completion: one peer 27.24 / 28.22 / 29.12 microseconds for main / parent / current; 64 peers with one request each 0.583 / 0.578 / 0.567 ms. With 64 requests per peer, parent/current medians were 31.43/31.53 ms.

Current had **one retry in six pipelined samples**. That trial reached 107.61 ms p95 and 101,143 requests/s; its other five samples reached 134,391–135,995 requests/s with zero retries. Parent's six samples had zero retries and 132,574–136,248 requests/s. This outlier is retained. The probe does not distinguish admission rejection from UDP/ACK loss, so it does not establish the cause or production frequency.

Main failed both pipelined attempts: all 4,096 first-batch requests were counted, but multiple peers retained older subscription IDs. Thus its pipelined throughput is not reported as comparable successful work. The full receive/application path is tested; the failure alone does not isolate the layer that reordered messages. Parent and current passed all 18 configured trials each, with exact totals and final states.

### Queue repartition experiment

The local variant preserved 4,224 combined queued slots by changing ingress/waiting capacities from 128/4,096 to 2,048/2,176. Per-lane 128 and per-peer 256 limits were retained. Its median throughput was 42,913 requests/s for one peer, 130,663 for 64 peers with one request each, and 135,595 for the pipelined case. All 18 trials passed with zero client retries.

The improvement was inconsistent across configurations and the burst median was only 0.3% higher than current. The smaller application waiting budget and larger FIFO ingress also change where overload accumulates. The variant was not adopted.

## Reproduction and evidence limits

For each revision, use an isolated export/checkout with its own Cargo target directory. Copy the committed control example into older revisions before building it:

```sh
cargo build --release --locked --manifest-path BasisRustServer/Cargo.toml \
  -p basis-server-core --example registry-processing-benchmark
RAYON_NUM_THREADS=16 BasisRustServer/target/release/examples/registry-processing-benchmark \
  --config docs/performance/fixtures/avatar-cpu-only-server.xml \
  --peers 1 --ops 32768 --batch 1
```

Repeat with `--peers 64 --ops 512 --batch 1` and `--batch 64`. Alternate revision order, require successful count/state/ACK validation, and keep builds separate from measurements.

The realtime harness is `scripts/perf/run-windows-avatar-workload.py` despite its name. Use a common frozen current client, fixture and harness; 1,000 clients for avatars, 250 clients/25 speakers for voice, 20-second warmup, 60-second windows, four client workers, 16 server Tokio/Rayon workers and no server diagnostic CSV. Voice additionally requires `--no-client-shared-receive`, `--client-voice-all-clients`, `--voice-speaker-percent 10`, `--no-voice-reencode`, and the common prepared corpus. Generated captures and binaries remain ignored under `captures/pr31-processing-speed-20261008`.

Preliminary control runs before correcting ACK size, request retries and sender window bounds were discarded. They contribute no published measurements. The committed [compact JSON](benchmark-summary.json) retains individual final observations, validation definitions, revision/build hashes and the rejected experiment. These limited samples do not prove the absence of smaller regressions, performance with larger control payloads, production packet loss behavior, or capacity on separate client/server machines.

## Ordered admission follow-up

Runtime `a2da362aa1047464b3b789b29928b8f60e112ea9` was compared with the previous runtime `94b42f97c28d203cb7983f5399bc3df27b608639`, using the identical committed UDP control probe and fixture. The same host remained on the performance CPU governor. Each case used previous / new / new / previous twice: **four samples per revision**, 32,768 operations per trial, four Tokio workers, 16 Rayon threads, system allocator, and GPU disabled. Ordinary handler capacity was unchanged; the new runtime reserves two additional slots for identity handlers.

| Peers / outstanding requests per peer | Previous requests/s | New requests/s | Change | Previous / new p95 batch completion |
| --- | ---: | ---: | ---: | ---: |
| 1 / 1 | 43,588 | 38,520 | −11.6% | 29.44 / 31.95 µs |
| 64 / 1 | 128,150 | 123,736 | −3.4% | 0.573 / 0.605 ms |
| 64 / 64 | 136,714 | 134,099 | −1.9% | 31.44 / 31.91 ms |

All **24 trials** passed the exact application-count and final-subscription checks, with empty client ACK windows, zero protocol/ACK errors, and zero client retransmissions. No gain was obtained by dropping work. Median whole-process CPU seconds (including driver and setup/shutdown) rose from 2.260 to 2.584, 1.515 to 1.653, and 1.403 to 1.503 respectively. These are not separate server CPU measurements.

The one-peer probe shows a consistent processing-cost increase: preliminary implementations also measured approximately 11–13% lower throughput. Avoiding empty scheduling permit probes, cloning one shared queue handle, and removing duplicate application quota bookkeeping did not eliminate it. The 64-peer differences are smaller observations on a shared host, without statistical significance established. Active cooperative polling adds CPU and map contention on the same runtime, so this probe cannot attribute every difference solely to server code. It does establish that the new control path should not be described as regression-free.

The runtime changes reserve an independent 128-event identity budget and two identity workers. Ordinary ordered quotas (4,096 global, 128 per lane, 256 per connection) cover ingress, queued, and running handlers. In-order admission reserves its permit before sequence commit/ACK; lack of capacity leaves it for retry rather than disconnecting the client. Out-of-order packets already ACKed under the separate bounded reorder budget remain retained until handler admission succeeds. The application FIFO retains accepted events and uses the transport permit as its quota owner.

Regression tests verify reserved identity admission at a full ordinary transport budget, a real signed identity response with all ordinary workers blocked, and a real 400-message UDP burst whose delayed handler eventually receives every message in FIFO order after unchanged-packet retries, with its session still connected. Server workspace validation passed **326 tests, two ignored**, targeted transport/core Clippy denied warnings, and formatting passed. CodeRabbit's first review emitted one minor queue-rejection panic concern, addressed by removing the second quota/rejection path. Its follow-up emitted zero findings but completed with an unverified-findings warning; this is not a clean-review guarantee. Luna audits found no additional concrete issue.

The JSON summary retains all 24 individual measurements, binary/source/fixture hashes, revisions, CPU observations, and validation results. Raw captures remain ignored. **No new voice/avatar workload or 1,000-user capacity test was run at this runtime.** Earlier realtime results above apply to `94b42f9`, not this follow-up.
