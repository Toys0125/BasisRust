# Bundle-cache fingerprint optimization with 2,000 clients

Capture artifacts are retained locally and are not shipped with this checkout.
See [artifact availability and how to request the original materials](artifact-availability.md).

September 30, 2026, branch `fix/receiver-distance-cache-refresh`.
Baseline source: `f5cc329` (server runtime `291d044`). The candidate was built
from that revision plus `variants/fingerprint.patch` in the capture directory.

Profiling identified repeated hashing of long avatar-bundle cache keys as a
useful optimization target. Two matched repeats per implementation showed
11.3% shorter receiver builds, 6.9% more logical fanout, and 2.1% lower server
CPU. Mean observer p95 update gaps decreased 4.6%. Gap variation overlaps
between repeats, so the latency percentage is an observation rather than a
precise prediction; both candidate runs improved build time and CPU relative
to both baseline runs.

## Profile and implementation

The preceding CPU profiles sampled `DefaultHasher::write` at 5.65% and 6.13%
of server CPU samples. Source inspection found that every bundle-cache lookup
constructed an item-identity vector and hashed the full vector; a miss hashed
it again for insertion. The identities describe backing addresses, payload
lengths, channels, interval-patch offsets, and interval bytes, together with
codec settings and raw length.

`BundleCacheLookup::new` now computes an FxHasher fingerprint while assembling
that vector. Map operations hash the compact fingerprint through the existing
randomized map hasher. Full structural equality still checks the vector and
codec fields, and immutable Bytes owners still retain backing allocations for
the cache lifetime. Fingerprint collisions cannot substitute a different
bundle. Encoding, cache admission limits, quality tiers, intervals, and the
wire format retain their existing behavior.

The change adds a direct dependency on `rustc-hash` 1.1, which was already
present in the lockfile through existing dependencies. A new regression test
forces the same fingerprint across eight distinct keys covering interval
bytes, patch offsets, channels, item order, backing allocation, codec, and
codec level, and verifies each lookup returns its own entry. Existing tests
also verify exact encoded bytes, cache reuse, budgets, and MTU fallback.
All 52 regular core tests passed; two manual hardware/profiling tests remained
ignored. The release build, format check, and diff check passed.

In this experiment's fresh native profiles, the baseline sampled the default
hasher at 6.45% and 5.79%; neither candidate sampled that named function or
`Hash::hash_slice`. This supports the hashing hypothesis. Hashing still occurs
in the constructor and compact map lookup, including inlined code; an absent
symbol is not evidence of zero hashing cost. Clock reads, receiver building,
delta encoding, copying, and transport remain substantial costs. Clock-source
settings were retained throughout the comparison.

## Matched results

| Run | Observer p95 gap | Receiver build | Server CPU | Logical fanout/s |
| --- | ---: | ---: | ---: | ---: |
| Baseline 1 | 829.06 ms | 11.771 ms | 456.09% | 5.203 million |
| Candidate 1 | 795.83 ms | 10.673 ms | 447.08% | 5.488 million |
| Candidate 2 | 834.34 ms | 10.954 ms | 448.29% | 5.270 million |
| Baseline 2 | 880.55 ms | 12.622 ms | 458.75% | 4.862 million |

| Mean metric | Baseline | Candidate | Change |
| --- | ---: | ---: | ---: |
| Observer p95 gap | 854.81 ms | 815.09 ms | −4.65% |
| Receiver build | 12.196 ms | 10.814 ms | −11.34% |
| Tick duration | 24.367 ms | 22.745 ms | −6.66% |
| Server CPU | 457.42% | 447.69% | −2.13% |
| Logical fanout/s | 5.032 million | 5.379 million | +6.88% |
| Peak server RSS | 626.42 MiB | 629.57 MiB | +0.50% |

All four 120-second runs used CPU reduction, identical configs, client binary,
and animated pose fixture, with a 30-second warmup and 15-second separation.
Order was baseline / candidate / candidate / baseline. Hardware was a Ryzen
9 5900HX; server affinity was CPUs 0–7 and client affinity CPUs 8–15, sharing
SMT resources. Four groups of 500 started at x=0,15,30,45; the last three moved
with seven-unit amplitude and a 60-second period. Quality thresholds were
10,20,40. Each client sent pose updates at approximately 50 Hz. LZ4 and
internal BSR profiling were enabled; GPU, Zstd, voice, P2P, and reconnect were
disabled. Native profiles sampled server and client in separate 20-second
windows at 99 Hz. CPU figures use the final approximately 60 seconds without
native profiling attached; 100% represents one fully occupied core.

## Coverage and limits

Every health sample retained 2,000 clients and avatar states. Each run covered
all 1,999 observer peers, recorded exactly 6,000 tier transitions (4,000 in
group 1 and 2,000 in group 3), and ended with zero position/tier mismatches.
Decode, malformed-item, sequence, delta-application, and sender errors were
zero. All clients remained connected, with sender rates within
49.98–50.02 Hz. Server and observer sockets had zero sampled UDP drops. All
load processes and profilers exited successfully, and all eight profiles
reported zero lost samples.

This is a headless loopback workload with one fully decoding observer. Other
clients filter avatar datagrams with BPF while retaining control and reliable
traffic. Their aggregate sampled socket-drop counter increases were
1,878/4,820/8,770/5,383 in run order; the separately mapped observer remained
at zero. It does not measure 2,000 rendering Unity clients. Two repeats per
implementation limit confidence, particularly for latency, and these results
do not establish that every performance opportunity has been exhausted.

Artifacts: `captures/bundle-cache-fingerprint-2000-20260930/`, including retained
binaries, patches, configs, manifests, source hashes, test/build logs, native
profiles, observer data, health samples, `comparison.json`,
`profile-evidence.json`, and `validation.json`. Config, client, fixture, binary,
and patch hashes were verified. Candidate sources remained frozen throughout
measurement. Reproduction uses `run-experiment.py` and `analyze.py --perf` in
that directory, plus the retained fixture/helper artifacts; move existing
numbered run directories before rerunning.

The [CPU/GPU comparison](cpu-vs-gpu-decisions-2000.md) motivated optimizing the
CPU path first. This experiment compares the current CPU implementation with
the fingerprint change, independently of the earlier GPU comparisons.
