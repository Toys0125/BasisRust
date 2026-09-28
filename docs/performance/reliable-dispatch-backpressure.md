# Reliable UDP dispatch under backpressure

## Finding

The reliable dispatcher and retransmission loop sent each peer's batch by awaiting
`UdpSocket::send_to` before visiting the next peer. A send future that stayed pending
therefore stopped both loops from advancing later peers. The old flush also discarded
send errors. The previous instrumented stall was parked at this flush stage; its exact
binary was overwritten, so that particular stage attribution is not hash-verified.

This code path is identical in `origin/main` (`bf73f7f`) and PR #11
(`a528bc2`), so the liveness defect predates PR #11.

## Fix

Reliable batches now use nonblocking `try_send_to`. Each peer keeps its unsent datagrams
in FIFO order; a `WouldBlock` leaves the suffix queued and the loop advances to other
peers. The dispatcher and maintenance loop share a nonblocking per-peer turn lock. A
blocked retry skips new batch construction, and promotion into the reliable window uses
the remaining aggregate 4,096-packet budget. This preserves the exact encoded ACK bytes
after their dirty bits are cleared and keeps reliable payloads tracked until ACKed.

Permanent socket errors still drop a datagram, matching the previous behavior. Ordinary
backpressure now retries on a later loop turn without awaiting socket readiness.

## Checks

`cargo test -p basis-transport` passed all 32 tests. The injected-WouldBlock integration
test runs the actual transport loops with two UDP peers: peer A is blocked, peer B receives
its reliable packet, A's outgoing ACK remains retained, then unblocking A delivers its
reliable packet and ACK; subsequent ACKs clear pending state. A unit test also checks that
the retained datagram bytes match exactly.

Matched workload inputs used the same client binary
(`cc8dccfd44b9123aadf61fe3c401dfa3ff1a2731ce747646bde8f5dab66baa3b`), server fixture
(`7a2ec8afe44e515878e4840247fef4dfbc4326b2758de8a6919d61caae542b22`), client fixture
(`608c21fa2b13eaacda79e96776f65791d204287b33b5517f2aad661e5436e5af`), 15-second warmup,
60-second window, and CPU affinity 0–7 for the server and 8–15 for the client. The server
fixture enables extended health metrics on PR #11; main has always collected these counters.

| Build and run | Startup | Window evidence |
| --- | --- | --- |
| Fixed PR #11, `/tmp/basisrust1500-fixed-pr-run2` | 1,500/1,500; 60 readiness samples | 1,499/1,499 observed; p50/p95 gaps 544/584 ms; zero stale peers or decode, unapplied, malformed, or non-newer errors. 75,004 inbound updates/s; 4.140M logical sends/s; 58.92 ticks/s. |
| Fixed PR #11, `/tmp/basisrust1500-fixed-pr-run3` | 1,500/1,500; 60 readiness samples | 1,499/1,499 observed; p50/p95 gaps 538/576 ms; same zero-error counts. 74,995 inbound updates/s; 4.170M logical sends/s; 59.34 ticks/s. Server used 169.69 CPU-seconds and client 59.03 CPU-seconds over 59.985 seconds. |
| `origin/main` bf73f7f, `/tmp/basisrust1500-main-run1` | Failed at the 360-second readiness deadline; last active count 972 while client transport accepts reached 1,274 | No measurement window. Protocol errors and reported UDP `would_block` remained zero. |
| `origin/main` bf73f7f, `/tmp/basisrust500-main-final` | 500/500; 60 readiness samples | 499/499 observed; p50/p95 gaps 117.7/127.8 ms; 2.118M logical sends/s; server used 93.24 CPU-seconds over 59.983 seconds. |
| Fixed PR #11, `/tmp/basisrust500-fixed-pr-final` | 500/500; 60 readiness samples | 499/499 observed; p50/p95 gaps 116.0/125.9 ms; 2.144M logical sends/s; server used 91.85 CPU-seconds over 59.982 seconds. |

The 500-client comparison is one pair. Logical send rate, update cadence, observer
coverage, and CPU use were similar; outbound message rate differed by about 7.6%, so
that single pair does not establish a precise throughput change. The 1,500-client main
failure versus two fixed-PR completions supports the liveness fix, but the failure was
intermittent in earlier runs. These measurements do not prove that every possible
`send_to` readiness stall has the same kernel cause.
