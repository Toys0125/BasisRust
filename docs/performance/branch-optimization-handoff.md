# Laptop agent handoff — branch optimization

## 3-ms scene batching interval — 2026-10-10

`BASIS_SCENE_BATCH_MS=3` is now supported to match C#’s numeric 3-ms merge hold.
Unset/0 remains disabled; 1/2 ms still work. The
[750-client 2/3-ms A/B](scene-batch3-client750-20261010.md) uses one new ordinary
binary in two four-process ABBA cohorts. At 20 Hz, receipt is essentially
unchanged: 46.27% versus 46.25%, with all processes invalid. At 5 Hz, 3 ms
reaches 100% receipt with a 131-ms p95 histogram bound; both intervals pass
every original gate in both repeats. This validates the lower-rate option on
this host, not a general speedup or success at the original 20-Hz rate.

Rust’s collector interval and C#’s per-peer transport merge hold govern different
stages. Retain that distinction and the existing app-output accounting limitation
before promoting the prototype. Exact source/binary/tool hashes, every original
failure, raw observations and focused test/review output are retained. PGO is off
for the changed source.

## C# reference capacity check — 2026-10-10

The [C# 750-client mixed workload check](csharp-mixed-client750-20261010.md)
uses the same frozen Rust client, payloads, affinities and measurement windows.
Two fresh processes at each scene rate retain all failures. At 20 Hz, C# scene
receipt is 15.0–15.2% and avatar p95 gaps are 6–19 seconds. At 5 Hz, receipt is
70.7–71.6% and avatar p95 gaps are 2.62–2.65 seconds. All four runs authenticate
all 750 clients and exit cleanly, but fail scene, avatar-quality and native-drop
gates. The compatible protocol-v55 reference uses normal adaptive workers and
.NET 10.0.12 tiered PGO; it is not a rebuild of the latest C# checkout.

The comparison uses the preceding Rust measurements, not interleaved cross-server
A/B. Unsupported C# telemetry remains null. Shared configuration translation,
all runtime hashes, raw measurements and the original failed schema preflight
are retained. No production server/client code changes in this follow-up.

## Scene receipt investigation — 2026-10-10

The [750-client scene investigation](scene-receipt-investigation-20261010.md)
profiles a saturated ordinary event queue and tests one default-off,
message-preserving batching prototype. Two matched ABBA cohorts retain all
eight process runs. At 20 Hz, median scene receipt rises from 5.68% to 46.03%,
still below the original 95% target. At 5 Hz, batching reaches 99.98% median
receipt with a 131-ms p95 histogram bound; both candidate processes pass every
original gate. Avatar input is unchanged; the lower scene rate is an explicit
freshness tradeoff. PGO is off for this changed source.

`BASIS_SCENE_BATCH_MS=2` enables the experimental path; unset/0 keeps the
existing relay. Before promotion, address its documented app-output accounting
limitation. If 20-Hz freshness is required, measure scene/interest recipient
filtering or further transport capacity work. Exact evidence, controls, review
findings and all failures are retained in the linked report and JSON.

Continue reviewing and measuring `performance/branch-optimization` in
`https://github.com/Toys0125/BasisRust.git` (remote `origin`). The user explicitly
requested stopping desktop benchmark runs, pushing the branch, and moving the
remaining work to their laptop. Do not resume desktop benchmarks.

## Completed laptop follow-up

The native Linux Ryzen 9 5900HX laptop review is recorded in
[the laptop report](branch-optimization-laptop.md), including all 16 original
A/B runs and eight decoder-alternative runs. All 1,296 samples had 100% counter
scheduling coverage and matched workload outcomes within each series. The
match-restoration candidate `83a96e9` was measured and rejected; the original
table/cold-extraction production code is retained with corrected comments.
Report claims and the runner's missing-executable diagnostic were corrected.
No desktop benchmark was resumed. The instructions below preserve the original
handoff context and reproducible baseline/candidate references.

## Completed 250-client runtime and PGO follow-up

The [250-client branch A/B](branch-client250-20261008.md) retained eight valid runs
and found no demonstrated speedup from the branch changes. The subsequent
[same-source PGO A/B](pgo-client250-20261008.md) used two separate training runs
and eight valid evaluation runs. PGO reduced median server CPU by 4.53% and
CPU per million built sends by 4.96% in this workload; cadence varied. Production
source and default build settings are unchanged. Both reports retain all runs,
controls, commands, hashes and delivery/error checks.

The [1,500-client PGO follow-up](pgo-client1500-20261008.md) retained eight
corrected process runs plus all preliminary attempts. Median observed p95 gaps
were 426 ms ordinary / 416 ms PGO, with PGO CPU per built send 2.32% lower.
Paired directions varied. All observer/coverage/error gates passed except the
original absolute-zero retransmit gate: counts accumulated before measurement
and stayed constant during it. A post-measurement shutdown grace now prevents
late-starting observer windows from being truncated.

## Source and state

- Production baseline: `1e45b3b8b74c76ec21324724e5de920032189b13`.
- Production candidate: `3606a9b` (transport cold extractions, property lookup,
  ignored benchmark, original report). Review this delta against its parent;
  comparison against `main` also includes earlier voice work.
- The follow-up commit contains test-only Linux counter collection, the portable
  `scripts/perf/review-branches.py` runner, this handoff, and desktop evidence.
  Production transport code has not been changed during this review.
- Earlier T3 thread: `a0cf0055-50f6-45f1-b1bf-94b70a252fca`, interrupted with
  no active run. Its final work was an approved Windows xperf trace.
- That trace exists locally under `captures/branchmiss/branchmiss.etl` but has
  **zero PMC interrupt/sample rows**. ETL existence and successful start are not
  counter measurements. The original report's unavailable-counter explanation
  predates that trace. None of these ignored desktop captures travel via Git.

## Review findings and completed measurements

No transport behavior regression was found in the extraction diff; all 54
transport tests passed before the test-only measurement additions. Both baseline
and candidate compiled on Windows MSVC and Linux with Rust 1.99.0.

The jump-table rationale is incorrect for the tested compiler. A probe compiled
from the exact property enum/decoder gives baseline `and/cmp/cmov/ret`, with no
indirect jump. The candidate uses a table load. This is an isolated assembly
probe, not a disassembly of every production call site.

The original timing study used separate variant blocks and does not establish
that the unchanged merge function improved because of instruction-cache layout.
Empty `#[cold]` helpers can be optimized away; inspect optimized callers before
claiming they actually outline code. The current benchmark does not exercise
socket backpressure, event enqueue, receive dispatch, shedding or ID allocation;
fresh-ID allocation also occurs on every initial connection, not just churn.
The outbound phase omits ReliableUnordered, despite the old report saying all
delivery methods. Its inputs and malformed rates are synthetic, not captured
production distributions. Printed workload outcomes are not hardware branch
counts; the parse payload total was a checksum, now labeled accordingly.

WSL hardware counters were accessible without installing `perf`: direct
`perf_event_open` for grouped hardware branch-instruction and branch-miss events.
The benchmark counts only its current thread's user-mode work and excludes
kernel/hypervisor execution. Warmup and fixture construction are excluded;
allocation/refill in timed phases and small clock/control overhead are included.

The retained desktop series ran **sequentially**, pinned to logical CPU 2:
four ABBA/BAAB blocks, eight independent processes per variant, nine measured
passes per phase after one warmup. AMD Ryzen 7 9800X3D, WSL2 kernel
6.18.40.1, Rust 1.99.0 GNU release. All 864 counter samples ran for 100% of
enabled time. Workload totals match across all runs.

| Phase | Branches/op baseline → candidate | Misses/op baseline → candidate | Miss rate baseline → candidate | Timing ns/op baseline → candidate |
|---|---:|---:|---:|---:|
| Property decode | 2.990 → 2.010 | 0.010409 → 0.014185 | 0.3481% → 0.7057% | 0.580 → 0.660 |
| Message parse | 2.000 → 2.000 | 0.010901 → 0.010927 | 0.5450% → 0.5463% | 1.715 → 1.720 |
| Outbound build | 72.626 → 72.626 | 0.005093 → 0.005108 | 0.00701% → 0.00703% | 22.180 → 21.245 |
| Merge (per 32-packet batch) | 3571.354 → 3571.349 | 47.862 → 45.322 | 1.3402% → 1.2690% | 1112.530 → 1055.275 |
| ACK with refill | 1854.579 → 1853.577 | 2.439793 → 1.279464 | 0.13156% → 0.06903% | 767.540 → 783.690 |
| No-op ACK | 595.591 → 595.591 | 0.034300 → 0.026834 | 0.00576% → 0.00451% | 201.160 → 201.995 |

The decoder was slower in every balanced block and had about 36.3% more
misses/op. ACK/refill misses/op fell about 47.6%, but median timing was 2.1%
slower. Results are mixed; do not call this a general speedup or combine phase
rates with different units/workloads. No statistical significance or application
throughput claim is established. The first overlapping Windows/WSL runs were
excluded. The later Windows-only series was stopped by user instruction and is
not a complete published comparison.

Full totals, per-pass counters, individual run timings, source/harness/binary
hashes and assembly probes are checked in at
`docs/performance/results/branch-review-20261007-summary.json`.

## Laptop work

1. Fetch `origin` and switch to `performance/branch-optimization`, preserving
   existing laptop edits. Read local AGENTS.md/RTK.md. Install/use Rust 1.99.0,
   Python 3.12+ and the platform's native compiler. Record laptop CPU/OS and
   power mode; use AC power and keep other workloads quiet.
2. Run ordinary transport tests and formatting checks. Review the original
   production diff and the findings above. Do not treat the desktop result as
   transferable to the laptop or Windows release code.
3. Run the identical frozen A/B harness sequentially with a valid logical CPU.
   The runner creates isolated snapshots and refuses an existing output folder:

   ```sh
   python scripts/perf/review-branches.py --output captures/laptop-branches --baseline 1e45b3b --candidate 3606a9b --blocks 4 --cpu 2
   ```

   On Linux/WSL x86-64 add `--pmc`. This fails loudly when grouped counters are
   unavailable or scheduled for less than 99.9% of enabled time. Running as root
   may be needed for the host's perf permissions; there is no need to install
   `perf` for this collector. If using WSL against a Windows Git worktree,
   supply `--repository /mnt/c/.../checkout --git-dir /mnt/c/.../common/.git`
   and put `--output` on the Linux filesystem. Use native Linux paths when
   available. Ensure Linux Rust/Cargo 1.99.0 is first on PATH.
4. Windows mode collects timings only. For Windows hardware counters, validate
   supported xperf sources and actual nonzero events, collect matched baseline
   and candidate traces, and distinguish sampled PMC interrupts from exact
   event totals. A successful trace command is insufficient. Do not repeat the
   old unvalidated `BranchMispredicts` name or infer miss rates from workload
   outcome counters. Native Linux counters are preferable if available.
5. Replicate the decoder result. Consider reverting only the table change in a
   separate candidate and compare to the original match; preserve existing
   baseline/candidate results. Isolate cold extractions before attributing ACK
   changes to a particular helper. Add representative runtime workload checks
   if claiming an application-level win.
6. Report every run, branches/op, misses/op, miss rate, timing, hashes, matched
   workload outcomes, counter scheduling coverage and limitations. Correct the
   original report's unsupported claims. Commit retained fixes/evidence and push
   to `origin/performance/branch-optimization`. No PR has been requested.

The desktop remains unchanged at the production level. A failed attempt to
install Kali `linux-perf` encountered stale-package 404s before installation;
Rust 1.99.0 was installed in WSL alongside the existing toolchain. Neither is a
laptop prerequisite beyond having the appropriate Rust compiler.

## Latest integration checkpoint — 2026-10-09

Main at `63748ef` is merged into this branch in `1e4dc73`. The
[1,500-client regression report](main-regression-client1500-20261009.md) compares
pre-pull `c1d74bd` with the integrated production source using fresh ordinary
release builds and one compatible updated Rust client. Cadence/throughput show
no clear regression, client CPU falls about 30%, and server peak RSS rises about
6 MiB. Four runs per variant are descriptive; strict cumulative retransmit gates
remain invalid while every other gate passes and counters stay constant during
measurement. Evidence and all attempts are retained. PGO was deliberately off
for this source comparison; older profiles have not been retrained for main.

The [subsequent fresh PGO experiment](pgo-main-client1500-20261009.md) now trains
a new merged-source profile on two valid 250-client runs and evaluates eight
matched 1,500-client runs. CPU per built send improves 3.68% and work rises 3.98%,
with all four pairs agreeing on those directions. Strict retransmit flags remain
invalid; all other gates pass. The older pre-main profile is not used. Production
source and default build settings remain unchanged.

## Latest main rerun — 2026-10-09

Main `4ce5f8c` is merged in `98151c3`. The server production tree remains
`b7130e68156ca47747f0832c7eac195923ed02f5`; the updated client is rebuilt from
`aed0f1a0c84cb8f5632157798786e46bc485545f`. The
[fresh PGO rerun](pgo-main-client1500-20261009-rerun.md) retains two valid
250-client training processes and eight matched 1,500-client evaluation trials.
CPU per built send falls 5.17% and built work rises 6.05%, with all pairs agreeing
on those directions. Original retransmit gates remain invalid on premeasurement
counts, including two warmup increases; measured counts are constant and all
other gates pass. Source/profile/tool hashes and all attempts are retained.

## Mixed scene/avatar workload checkpoint — 2026-10-10

The [mixed 1,500-client PGO A/B](pgo-mixed-client1500-20261010.md) adds 128-byte
additional avatar data and 128-byte scene broadcasts every 50 ms to the same
dense workload. It uses unchanged production source and a fresh mixed profile.
The offered scene fanout exceeds this host's delivery capacity. Original
training and evaluation failures remain explicit in the retained evidence;
these measurements are overload diagnostics, not a validated PGO speedup.

## 750-client mixed workload checkpoint — 2026-10-10

The [750-client mixed PGO A/B](pgo-mixed-client750-20261010.md) changes only the
evaluation population, reusing the exact ordinary/PGO/client binaries and the
250-client diagnostic mixed profile from the 1,500-client cohort. Payloads,
cadence, workers, affinity, windows, and strict gates are unchanged. All attempts
and original failed flags are retained; the report distinguishes workload
capacity observations from validated PGO speedup evidence.
