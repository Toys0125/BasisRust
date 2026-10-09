# Windows avatar cadence: direct pre-merge comparisons, 2026-10-04

This follow-up compares the branch directly with frozen pre-merge server
`58c107e` to check recovery of the reviewed cadence.
The original [review](review-20261004.md) and
[four-job investigation](windows-bounded-flush-20261004.md) remain supporting
evidence. Their raw artifacts and frozen controls are preserved.

The retained Windows default is now **six flush jobs on the existing pool**,
with encoder-input allocation deferred until a bundle actually needs encoding.
Six jobs passed two independent eight-run 750-client series and an eight-run
1,000-client diagnostic series against frozen pre-merge, including both orders
in each series. This restores the measured workload's cadence on balanced block
means. It delivers more updates with higher process CPU and UDP traffic; the
specific cause of the apparent original merge regression remains unisolated.

## First direct crossover: four-job flush

Eight sequential 750-client runs used order before/four/four/before, then
four/before/before/four. The same frozen merged client, fixtures, Rust 1.99 MSVC
release binaries, four client Tokio workers, 45-second warmup after readiness
and 60-second measurement were used throughout. Per-pair diagnostics and
application/native profiling were off; the server retained its existing worker
pool, adaptive slicing and budgets. No affinity or system configuration changed.

The earlier wrapper selected a fresh server UDP port and health port for each
run. Runtime ports are retained in the metadata, but they were not matched
across variants. The updated wrapper selects available ports once per crossover
and validates that every variant reuses them. Existing native evidence includes
Windows port-reservation lookup costs; varying ports is therefore a variable
worth removing, not an established explanation for these timing differences.
The frozen client continues to use OS-selected ephemeral source ports.

| Metric | Pre-merge mean | Four-job mean | Change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 79.025 | 78.860 | -0.2% |
| Applied gap p95 (ms) | 126.493 | 117.285 | -7.3% |
| Observer applied items/s | 9,181.49 | 9,175.25 | -0.07% |
| Combined process CPU (cores) | 4.963 | 4.920 | -0.9% |
| UDP payload (decimal MB/s) | 171.830 | 170.344 | -0.9% |
| UDP datagrams/s | 142,743 | 141,547 | -0.8% |
| Host busy CPU (cores) | 13.945 | 13.922 | -0.2% |

The branch essentially matched pre-merge median cadence and applied updates in
this series and improved p95. It did not demonstrate consistent additional
median/observed-item headroom in both blocks: the first block was +0.10% p50 and
-0.20% applied items; the second was -0.50% p50 and +0.07% applied items. These
sub-percent differences must not be described as a stable remaining regression
or a statistically established speedup. Individual pre-merge p50 values ranged
from 64.19 to 89.44 ms. All eight delivery/error/progress gates passed.

## Candidate build-cost change

`try_emit_bundle_range` previously constructed a vector of borrowed encoder
inputs before checking whether the tick's bundle cache already had encoded
bytes. The candidate constructs that vector inside the existing encoding
closure. Cache hits avoid the allocation and walk; cache misses, uncached
fallbacks, compression selection, interval patches, MTU checks and encoded bytes
keep the existing behavior. No packets, receivers or input updates are omitted
to obtain lower CPU.

All 81 core release tests, including exact cached bytes, patch identity,
overshoot/fallback, incarnation reuse and receiver flush bounds, passed. Rust
formatting and whitespace checks passed. The candidate was built with
`cargo build --locked --release --jobs 2 -p basis-server-console` and
frozen as SHA-256
`a7334854bef5f60800a3abbae7cff55083e50c82ac709f3cc77bb0f7ce63b669`.

The eight-run cache screen used the earlier port selection, in reversed block
order (cache/before/before/cache, then before/cache/cache/before). Means were
79.49/116.49 ms p50/p95 versus 80.13/119.89 ms pre-merge, with 9,094 versus
9,272 observer items/s and 4.875 versus 5.087 process CPU cores. The -4.2% CPU
change accompanied -1.9% delivery and is not an equal-work speedup. The first
block was +0.86% p50 and -3.94% applied items; the second was -2.58% p50 and equal
applied items. All eight correctness/progress gates passed. This screen does
not establish a cadence improvement attributable to the cache change.

The next eight-run validation held UDP port 64142 and health port 64353 fixed
throughout. It still failed recovery: means were 89.51/126.20 ms p50/p95 versus
85.00/131.85 ms pre-merge, with 8,220 versus 8,639 observer items/s and 5.074
versus 5.089 process CPU cores. P50 was +5.3% and observed items -4.8%; the first
block was +13.3% p50/-9.3% items, and the reversed block was -2.5% p50/-0.4%
items. All eight delivery/error/progress gates and matching-port checks passed.
Holding ports constant removed a comparison variable but did not explain or
resolve the cadence difference. This candidate was not accepted as full recovery.

## Six-job emission candidate

The next candidate uses up to six avatar flush jobs on the same existing
eight-worker Windows Rayon pool. Packet construction retains its existing pool
and policy; thread counts, tick/cycle budgets, compression settings and quality
remain unchanged. Four jobs had insufficient delivery margin in the direct
screens, while eight is the existing pool ceiling. Six is an intermediate
bounded scheduling experiment, not an increase in worker threads.

The candidate retains the deferred cache-input allocation. The existing
receiver coverage/concurrency/error properties now also exercise six jobs.
All 81 core release tests and formatting/whitespace checks passed. The default
six-job binary is frozen as SHA-256
`1b3ce7242b6cc46b00c79e4b914f01f68f23bce95e855935a54f93e215d3bd64`.
The first eight-run screen used six/before/before/six, then
before/six/six/before, with the same fixed ports. Both balanced blocks passed
the recovery screen. Means of four runs per variant:

| Metric | Pre-merge mean | Six-job mean | Change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 86.210 | 82.238 | -4.6% |
| Applied gap p95 (ms) | 126.635 | 115.893 | -8.5% |
| Observer applied items/s | 8,682.16 | 9,025.45 | +4.0% |
| Combined process CPU (cores) | 5.256 | 5.599 | +6.5% |
| UDP payload (decimal MB/s) | 165.176 | 174.029 | +5.4% |
| UDP datagrams/s | 137,470 | 144,599 | +5.2% |
| Host busy CPU (cores) | 13.881 | 14.263 | +2.8% |

The blocks improved p50 by 7.9% and 1.0%, p95 by 11.9% and 4.9%, and observed
items by 8.1% and 0.2%, respectively. All eight delivery/error/progress gates
passed. One adjacent pair lost on p50 and observed items, and the second block's
delivery margin was small. This is a screen, not yet independent confirmation.
Higher process CPU accompanied more delivery; this is not a CPU-efficiency
claim.

## Independent 750-client confirmation

The same frozen six-job binary was tested again with opposite block order:
before/six/six/before, then six/before/before/six. Ports, client, fixtures,
instrumentation, warmup and measurement stayed matched. Both blocks passed the
recovery screen, with p50 changes of -7.3%/-18.3%, p95 -7.8%/-9.2%, and observed
items +5.4%/+14.4%. All eight delivery/error/progress gates passed. Means of four
runs per variant:

| Metric | Pre-merge mean | Six-job mean | Change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 82.240 | 71.713 | -12.8% |
| Applied gap p95 (ms) | 122.530 | 112.160 | -8.5% |
| Observer applied items/s | 9,041.05 | 9,936.73 | +9.9% |
| Built logical avatar items/s | 6,773,073 | 7,459,657 | +10.1% |
| Server CPU (cores) | 3.860 | 4.191 | +8.6% |
| Client CPU (cores) | 1.333 | 1.332 | -0.07% |
| Combined process CPU (cores) | 5.193 | 5.524 | +6.4% |
| UDP payload (decimal MB/s) | 174.244 | 193.119 | +10.8% |
| UDP datagrams/s | 144,967 | 160,720 | +10.9% |
| Host busy CPU (cores) | 13.839 | 14.244 | +2.9% |

The first screen and independent confirmation each satisfy the block criteria;
their controls are kept separate. Not every adjacent pair improved every
metric: the last confirmation candidate had p95 112.34 ms versus 109.46 ms for
its adjacent control. These results demonstrate recovery of the reviewed
750-client workload's measured cadence on balanced block means, not a promise
of faster delivery in every run or every receiver pair. They do not establish
an isolated cadence benefit from the cache change alone.

## 1,000-client diagnostic validation

Eight sequential runs used order before/six/six/before, then
six/before/before/six. They use the same frozen binaries, client, CPU-only
fixtures and fixed ports. Per-pair diagnostics are on for both variants, with
native/application profiling off. Both blocks passed the recovery screen:
p50 -28.2%/-27.4%, p95 -22.0%/-14.3%, and observed items +37.5%/+29.3%.
All four adjacent pairs improved these three delivery metrics. All eight
coverage/error/progress gates passed. Means of four runs per variant:

| Metric | Pre-merge mean | Six-job mean | Change |
|---|---:|---:|---:|
| Applied gap p50 (ms) | 189.563 | 136.895 | -27.8% |
| Applied gap p95 (ms) | 272.363 | 222.705 | -18.2% |
| Observer applied items/s | 5,257.24 | 7,009.65 | +33.3% |
| Built logical avatar items/s | 5,228,645 | 7,053,972 | +34.9% |
| Server CPU (cores) | 3.728 | 4.287 | +15.0% |
| Client CPU (cores) | 1.861 | 1.559 | -16.2% |
| Combined process CPU (cores) | 5.589 | 5.846 | +4.6% |
| UDP payload (decimal MB/s) | 109.731 | 159.171 | +45.1% |
| UDP datagrams/s | 90,917 | 131,395 | +44.5% |
| Host busy CPU (cores) | 14.075 | 14.325 | +1.8% |
| Ticks/s | 143.908 | 143.401 | -0.4% |
| Tick work (ms/tick) | 6.754 | 6.760 | +0.1% |
| Build work (ms/tick) | 1.879 | 2.212 | +17.7% |
| Flush work (ms/tick) | 3.859 | 3.554 | -7.9% |

Delivery increased despite essentially flat tick rate and tick work. Flush
work per tick fell while build work increased with more fanout. Adaptive
receiver slicing remains active: control run medians were 27/30/31/29.5 slices,
candidate medians 24/20/22/20. The joint scheduling/cache candidate gives the
controller a different emission balance; these measurements do not isolate
the cache change's contribution or establish new native stack attribution.

Server kernel CPU increased from 2.108 to 2.311 cores while client kernel CPU
fell from 1.579 to 1.259 cores. Sampled server working-set peaks averaged
167.0 versus 174.0 MiB; client peaks 34.1 versus 34.7 MiB. These sampled maxima
do not measure allocation counts or whole-run high-water marks. UDP payload
rose to 159.2 MB/s, about 1.27 Gbit/s before IP/link headers, entirely on
loopback. This is a cadence recovery with additional traffic, not a reduction
in network demand or a physical-network capacity result.

## Correctness and retained setting

- All **48** follow-up runs passed delivery/error/progress validation. All
  requested clients remained authenticated and active in all 30 measurement
  samples; each observer covered all 749 or 999 expected peers.
- Every sender stayed connected, made at least **2,993** successful sends
  during its measured window, and reported zero send errors.
- Decode errors, malformed items, unapplied deltas, non-newer sequences,
  discontinuities, sequence ambiguities/resyncs, protocol errors, retransmits,
  avatar tick failures and UDP WouldBlock were zero. The candidate's explicit
  non-reliable drop counter was zero. Pre-merge lacks that explicit counter;
  its WouldBlock counter was zero.
- All observer windows retained one uninterrupted 60-second segment. Missing
  and end-of-window stale peer counts were zero. That final stale snapshot does
  **not** prove no earlier 500 ms gaps or application at every receiver pair.
- The current six-job candidate passed all **81 core release tests**, including
  cached-byte/patch identity, MTU fallback, incarnation reuse, complete receiver
  coverage, maximum concurrent flush callbacks and error propagation. Earlier
  unchanged transport/console modules passed **51/8** release tests; repeated
  core checks are not additional distinct tests. Formatting, whitespace and
  Python syntax checks passed. Port probing rejected occupied UDP/TCP sockets.
- Analysis smoke checks rejected a mismatched client, varying destination port,
  overlapping run windows, failed delivery, fewer observed items and an
  incomplete balanced block. Lower delivery cannot satisfy the recovery screen.

`BASIS_AVATAR_FLUSH_LANES` is cached at system initialization. Windows defaults
to **6**, other platforms to **0** (existing scheduling); positive values cap
flush jobs, 1 is serial, 0 restores the old strategy, values above 8 clamp to 8,
and invalid strings use the platform default. The existing build pool, tick/
cycle budgets, compression, quality and adaptive fanout policy apply. No new
worker threads are created. Linux was not rebenchmarked.

## Scope and artifacts

The apparent original regression's specific merge cause is still unisolated.
The avatar build/flush strategy and successful-send framing did not change in
the reviewed merge with profiling disabled. The pre-existing native profile
supports investigating Windows Winsock/loopback contention but uses a different
Rust version/session and cannot attribute these fresh results. The new cache
change addresses directly observed unnecessary application work.

These are dense synthetic local-loopback measurements on a busy machine with
VRChat, SteamVR and other applications active. Host CPU minus process CPU also
includes system/DPC work related to the load test. It is not an isolated measure
of unrelated background load. Results do not establish physical-network
capacity, realistic spatial/voice behavior, every receiver's delivery, or
absence of earlier 500 ms gaps from zero end-of-window stale counts.

New raw captures, frozen candidate, build/test logs, source patches, manifests
and analysis helpers are retained under ignored
`captures/regression-resolution-20261004/` in this worktree. The read-only
[`compare-windows-avatar-crossover.py`](../../scripts/perf/compare-windows-avatar-crossover.py)
helper retains all runs and compares complete balanced blocks; its
strict screening flag requires non-worse p50/p95 and non-lower observer applied
items in each block, alongside matching inputs, matching ports, sequential
execution and delivery gates. The flag is
a workload recovery screen, not a significance test or a production guarantee.
The [consolidated results](results/README.md#windows-cadence-investigation-2026-10-04)
summarize all six separate series, including rejected screens, and list the
preserved artifact locations. Full individual runs, commands, hashes, endpoints,
observer counts, scheduler metrics, sender-progress minima and validation flags
remain in the local captures and archived JSON exports.
The original control/client hashes were reverified; the original captures and
other worktrees were preserved. No branch or PR was merged.

## Reproduction

PR review strengthened the benchmark tools after the performance captures.
The comparator now defaults to the retained `six` label and rejects identical
control/candidate executable hashes. Each crossover run uses a private Windows
kill-on-close Job Object: its RTK proxy starts suspended, joins the job before
spawning descendants, then resumes. Graceful cancellation still applies, while
forced termination or launcher exit also closes ownership of the complete run.
No job priority, affinity, CPU, or memory limits are imposed.

Five targeted tests cover current default labels, identical-binary rejection,
job closure, forced proxy exit, and a failed launch before resume. The cleanup
tests verify worker/grandchild exit and socket release while an independently
owned process stays alive. A real two-client crossover also passed all delivery
gates. Run these checks with
`python -B scripts/perf/test_windows_avatar_tools.py`.
Review-fix smoke artifacts are retained separately under
`captures/pr27-review-fixes-20261004/` and generated `captures/avatar-tools-tests-*`
directories. The Rust avatar source, frozen performance binaries and all 48
reported performance measurements are preserved.

Build each server revision in its separate Server workspace with
`cargo build --locked --release --jobs 2 -p basis-server-console`.
The candidate build starts from `5915bca` plus the recorded avatar source patch.
Client builds use their separate Client workspace and
`cargo build --locked --release --jobs 2`; freeze one client and reuse
it for every variant. These experiments reused the review's frozen merged
client without rebuilding it between series. Default features, Rust 1.99.0
MSVC and the CPU-only server fixture were used. Build environment/manifests
record source, toolchain and absence of optimization/worker/budget overrides.

From the repository root, substitute frozen binary paths and use a new capture
directory. This reproduces the confirmation's two opposite-order blocks:

```powershell
python -B scripts/perf/run-windows-avatar-crossover.py --output captures/new-confirm-750 --client captures/binaries/client.exe --clients 750 --port 64142 --health-port 64353 --variants before-1=captures/binaries/before-server.exe six-1=captures/binaries/six-cache-server.exe six-2=captures/binaries/six-cache-server.exe before-2=captures/binaries/before-server.exe six-3=captures/binaries/six-cache-server.exe before-3=captures/binaries/before-server.exe before-4=captures/binaries/before-server.exe six-4=captures/binaries/six-cache-server.exe
python -B scripts/perf/compare-windows-avatar-crossover.py captures/new-confirm-750 --candidate six --summary-only
```

For the heavier series, use a new directory, `--clients 1000 --diagnostics`.
All other options and frozen binaries remain matched. Use unused fixed ports;
omitting the port options selects free ports once for the entire series. The
wrapper runs sequentially and stops if delivery gates fail. The comparison
helper requires at least eight balanced runs, checks frozen inputs and matching
instrumentation, and retains each run when `--summary-only` is omitted.

Frozen review SHA-256 values, verified read-only:

| Binary | SHA-256 |
|---|---|
| Pre-merge server | `974208a415644f15e3b309d3713986abcaf94cef220b5cd9e9dc4418f066a5f1` |
| Merged server | `a89200336a9709e2aa7e9ecd7da87cc3c9844f5b650d73ff7fc528a1406a606e` |
| Common frozen client | `57ab9ecf56536cba84222db3e4100442ced8dc52ece824bae6699daccede3123` |

The six-job source file hash is
`8504569d884276475537ad2a683c154437e58f42f036a60ce681f4c5e596aa32`;
its build manifest also records the full source-patch hash. Raw captures and
executables are retained locally and are not committed.
