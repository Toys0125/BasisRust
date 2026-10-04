# Windows avatar delivery: bounded UDP flush, 2026-10-04

This records the earlier four-job experiment. The subsequent
[direct pre-merge validation](windows-regression-resolution-20261004.md) retained
a six-job Windows default after independent crossover confirmation. The
four-job measurements below remain the original results.

The first candidate divided each tick's receiver groups into at most four
jobs on the existing Rayon pool. Sixteen new sequential loopback runs passed
delivery validation. Four jobs improved p50, p95, observer applied items and
combined process CPU in both crossover orders at 750 and 1,000 clients in this
session. A two-job bound reduced delivery and was rejected. More delivered
updates increased UDP traffic; this change is a cadence improvement for this
workload, not a traffic-reduction or physical-network capacity claim.

The apparent pre-merge median-cadence regression remains **unisolated**. The
unchanged repeat reproduced a higher merged p50 while p95 improved slightly,
and the controls varied substantially. The retained setting addresses a
measured delivery bottleneck; it does not identify a particular PR as its cause.

## Change and rationale

Each flush job sends its contiguous receiver chunk serially through the existing
`try_send_many_unreliable_packets` API. Receiver packet construction uses the
existing build pool. Tests check that every receiver is visited once, active
flush callbacks stay within the configured bound, empty/short/uneven batches
work, and transport failure propagates.

`BASIS_AVATAR_FLUSH_LANES` is read at system initialization. Windows defaults
to **4** for this experiment (the follow-up defaults to **6**); other platforms
default to **0**, retaining the previous Rayon flush
scheduling. Positive values cap avatar flush jobs; **1** is serial, **0** restores
the previous scheduling, values above **8** clamp to 8, and invalid strings use
the platform default. This bounds avatar emission independently of build
parallelism. Other transport paths can send independently. Existing Windows
6 ms tick-work / 180 ms receiver-cycle budgets and adaptive controller apply.
Linux performance was not rebenchmarked.

The original review identified flush as 54-56% of tick work at 1,000 clients.
Older Windows native evidence attributed large inclusive CPU shares to Winsock
sends and a loopback Flow Steering Engine path with spin contention. That
Rust 1.95 capture is evidence for investigating emission, not fresh attribution
for these Rust 1.99 runs. No new native tracing or system configuration changes
were performed. Reduced contention is a hypothesis; grouping/scheduling and
adaptive fanout may also contribute.

## Matched final 750-client validation

Order: merged, four, four, merged. These runs use the final default-four binary,
with no flush override and per-pair diagnostics off. Means of two runs:

| Metric | Merged control | Four jobs | Change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 78.69 | 70.54 | -10.4% |
| Applied gap p95 (ms) | 116.01 | 97.82 | -15.7% |
| Observer applied items/s | 9,450 | 10,442 | +10.5% |
| Built logical avatar items/s | 7,040,328 | 7,789,527 | +10.6% |
| Server CPU (cores) | 4.190 | 4.027 | -3.9% |
| Client CPU (cores) | 1.398 | 1.378 | -1.4% |
| Combined process CPU (cores) | 5.588 | 5.405 | -3.3% |
| Host busy CPU (cores) | 13.120 | 12.095 | -7.8% |
| UDP payload (decimal MB/s) | 185.7 | 211.3 | +13.8% |
| UDP datagrams/s | 154,230 | 174,903 | +13.4% |

Both candidates beat their order-paired controls on p50/p95 and applied items,
with lower combined process CPU. Payload rose 13.8% and datagrams 13.4% as
delivery increased. This does not demonstrate fewer packets per application
update. Sampled server peak working sets were 138.8/140.9 MiB for four jobs
versus 146.2/135.6 MiB for controls; no memory reduction is established.

## Matched 1,000-client diagnostic comparison

Order: four, merged, merged, four. The first candidate binary was used with an
explicit flush override of 4. The final binary changes its startup default
from 2 to 4; the bounded flush implementation is identical. All four runs used
matching per-pair diagnostics. Means of two runs:

| Metric | Merged control | Four jobs | Change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 146.27 | 130.19 | -11.0% |
| Applied gap p95 (ms) | 214.07 | 175.51 | -18.0% |
| Observer applied items/s | 6,968 | 7,609 | +9.2% |
| Built logical avatar items/s | 6,921,551 | 7,638,355 | +10.4% |
| Server CPU (cores) | 4.152 | 4.067 | -2.0% |
| Client CPU (cores) | 1.745 | 1.401 | -19.7% |
| Combined process CPU (cores) | 5.896 | 5.468 | -7.3% |
| Host busy CPU (cores) | 12.859 | 12.774 | -0.7% |
| UDP payload (decimal MB/s) | 157.6 | 168.4 | +6.8% |
| UDP datagrams/s | 129,910 | 138,772 | +6.8% |
| Ticks/s | 149.3 | 152.7 | +2.3% |
| Tick work (ms/tick) | 6.474 | 6.378 | -1.5% |
| Build work (ms/tick) | 1.906 | 1.965 | +3.1% |
| Flush work (ms/tick) | 3.750 | 3.597 | -4.1% |

Both four-job runs beat their order-paired controls on p50/p95, applied items
and combined process CPU. Flush work improved only 4.1% on average; tick/build
work and adaptive receiver slicing varied. Client kernel CPU averaged 1.065
versus 1.455 cores, while server kernel CPU averaged 2.203 versus 2.374 cores.
These process counters do not establish new native stack attribution.

Host busy CPU was almost flat (-0.7%); the 7.3% CPU reduction is for the two
processes, not the whole machine. Host CPU minus process CPU includes other
applications and workload-related system/DPC CPU, with sample endpoints within
about a second of the process endpoints. It cannot isolate background load.
VRChat, SteamVR and other applications stayed active; background work varied.

UDP payload increased 6.9% to about 168 MB/s (1.35 Gbit/s before IP/link headers),
entirely on loopback. Sampled server working-set peaks increased to 177.3/182.7
MiB from 172.7/175.9 MiB. These are sampled maxima, not allocation counts or
whole-run high-water marks. The observer applied about 7.6 updates per peer
per second despite about 50 input updates per client per second. Cadence
remains a capacity limit; physical-network, voice and realistic spatial/quality
workloads remain unmeasured here.

## Regression repeat and rejected variant

The unchanged 750-client order was before, merged, merged, before, reusing the
original frozen servers and client. Before means were 64.83/116.30 ms p50/p95,
10,611 applied items/s and 5.482 process CPU cores. Merged means were
76.36/113.15 ms, 9,487 items/s and 5.330 cores: p50 +17.8%, p95 -2.7%, delivered
items -10.6% and CPU -2.8%. The two before p50 values were 70.89 and 58.76 ms;
merged values were 71.62 and 81.10 ms. This supports investigating the median
slowdown but does not prove a stable regression across quantiles or identify
its cause. Source comparison did not show a merged change to receiver slicing
or the avatar build/flush strategy with profiling disabled. Health/transport
changes, host/session variation and client differences from historical cohorts
were not independently isolated.

At 1,000 clients, the two-job ABBA screen worsened mean p50/p95 by 24.1%/28.8%,
reduced observed items by 21.1%, and increased flush work from 3.243 to 4.405
ms/tick. Its 18.1% lower combined CPU accompanied less delivery and was rejected.
All delivery/error gates still passed. Controls from separate series must not
be pooled as if host conditions were constant.

## Individual runs

| Series / run | Gap p50 / p95 (ms) | Applied items/s | Process CPU cores | Payload MB/s |
|---|---:|---:|---:|---:|
| unchanged-750 / 750-before-repeat-1 | 70.89 / 123.99 | 9,837 | 5.228 | 187.6 |
| unchanged-750 / 750-merged-repeat-1 | 71.62 / 109.53 | 9,999 | 5.231 | 196.6 |
| unchanged-750 / 750-merged-repeat-2 | 81.10 / 116.77 | 8,976 | 5.428 | 175.9 |
| unchanged-750 / 750-before-repeat-2 | 58.76 / 108.61 | 11,385 | 5.736 | 240.4 |
| rejected-two-1000 / 1000-merged-1 | 130.85 / 192.16 | 7,459 | 5.292 | 154.2 |
| rejected-two-1000 / 1000-bounded-1 | 149.35 / 233.22 | 6,327 | 4.617 | 126.7 |
| rejected-two-1000 / 1000-bounded-2 | 159.86 / 224.48 | 6,077 | 4.403 | 135.9 |
| rejected-two-1000 / 1000-merged-2 | 118.23 / 163.29 | 8,258 | 5.719 | 185.6 |
| four-1000 / 1000-four-1 | 127.24 / 158.59 | 7,875 | 5.487 | 171.7 |
| four-1000 / 1000-merged-1 | 140.07 / 214.57 | 6,943 | 5.807 | 158.3 |
| four-1000 / 1000-merged-2 | 152.47 / 213.57 | 6,993 | 5.986 | 156.9 |
| four-1000 / 1000-four-2 | 133.13 / 192.43 | 7,343 | 5.449 | 165.1 |
| final-four-750 / 750-merged-1 | 78.72 / 114.45 | 9,687 | 5.697 | 197.8 |
| final-four-750 / 750-four-1 | 72.70 / 97.95 | 10,286 | 5.577 | 199.0 |
| final-four-750 / 750-four-2 | 68.38 / 97.69 | 10,598 | 5.233 | 223.6 |
| final-four-750 / 750-merged-2 | 78.66 / 117.57 | 9,213 | 5.479 | 173.6 |

Full counters, window endpoints, scheduler-status ranges, observer distributions,
sender progress, validation flags, commands and binary/config hashes are in the
[machine-readable summary](results/windows-bounded-flush-20261004-summary.json).
The [original review](review-20261004.md) and its
[results](results/review-20261004-summary.json) were copied for supporting
evidence; the original checkout/captures remain untouched.

## Validation and limits

- All 16 runs kept every requested authenticated client active across all 30
  measurement samples. Every observer covered all 749 or 999 expected peers.
- Every sender stayed connected and progressed, with at least 2,995 successful
  sends during its 60-second window and zero send errors.
- Decode errors, malformed items, unapplied deltas, non-newer sequences,
  discontinuities, sequence ambiguities/resyncs, protocol errors, retransmits,
  avatar tick failures, UDP WouldBlock and reported non-reliable drops were zero. The old pre-merge
  binary lacks the explicit drop counter; its WouldBlock counter was zero.
  One observer-ownership warning occurred during controlled teardown, 62.02
  seconds after the 60-second window marker; its stored measured segment had
  zero discontinuities and the full 60-second duration.
- End-of-window missing/stale peer counts were zero. This does **not** prove
  no earlier 500 ms gaps. Coverage is at one observer, not every receiver pair.
- Release tests passed: 81 core, 51 transport, 8 console. The final default-four
  core was rechecked (81 passed). Formatting and Python syntax/validator checks
  passed. A separate two-client cancellation smoke verifies cleanup of the
  harness's owned server/client processes.
- Two repeats per variant per series are screening evidence, not statistical
  significance or a general production capacity guarantee. Observer gaps are
  application-update intervals, not end-to-end latency or RTT. Logical work
  counters count built fanout; they do not prove application at every receiver.

## Reproduction and artifacts

Host: Windows 11 build 26100, Ryzen 7 9800X3D, 8 physical/16 logical cores,
high-performance power plan. Rust 1.99.0 MSVC, locked default release features,
CPU-only fixture, IPv4 loopback, no affinity. Client workers=4; dense synthetic
poses, 20 ms input interval, 60 FPS Unity policy, zero jitter/spread, voice/P2P/
reconnect off. Native/application profiling off throughout; per-pair diagnostics
are on only in the 1,000-client series. Warmup is 45 seconds after all clients
are ready; measurement is 60 seconds. CPU/UDP deltas use recorded endpoints
about 58 seconds apart within the marked observer window.

Build in the separate workspaces with `rtk proxy cargo build --locked --release --jobs 2`
(server: add `-p basis-server-console`). Retain one client and config
for every variant. Freeze server/client executables before running. From the
repository root, substituting the recorded frozen binaries:

```powershell
rtk proxy python -B scripts/perf/run-windows-avatar-crossover.py --output captures/new-750-repeat --client captures/binaries/client.exe --clients 750 --variants control-1=captures/binaries/control.exe four-1=captures/binaries/four.exe four-2=captures/binaries/four.exe control-2=captures/binaries/control.exe
```

For the diagnostic comparison, use a new output directory, `--clients 1000`
and `--diagnostics`. Runs are sequential. The wrapper clears workload/budget/
slice/Rayon/password overrides, records frozen hashes and host CPU, and stops
on a delivery gate failure. `--flush-lanes 0..8` explicitly sets the override
for a tuning series; these final 750-client runs used no override. The validator
is read-only and returns a failure status for invalid captures:

```powershell
rtk proxy python -B scripts/perf/summarize_windows_avatar.py captures/new-750-repeat/control-1 captures/new-750-repeat/four-1
```

New raw captures, frozen candidate binaries, source patches, build manifests,
host samples, exact commands and analysis helpers stay under ignored
`captures/regression-improvements-20261004/` in this worktree. The original
frozen controls/client remain under `captures/performance-review-20261004/`
in the original checkout. Baseline binary SHA-256 values were verified before
use. The first candidate hash is `e8d9d118924fec3ac68c76e54cd14c0615d68fedef16454ee9ef6f8c759ebf46`;
the final default-four server hash is
`a4a8b007b4223d75d5a12310e96f0a98302019be3b94efb84c4ca7288c07b2a4`. The build manifests
record base revision `624ae0c`, the source patch/file hashes and exact toolchain.
Raw captures and executables are not committed. This experiment does not
attribute historical cohort differences to merged PRs or quantify Linux parity.
