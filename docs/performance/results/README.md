# Performance results

This directory summarizes the measured outcomes and their limits. The Windows
investigation is consolidated below; the three existing historical JSON files
remain unchanged. Raw CSVs, binaries, recordings and full Windows exports are
preserved locally under ignored capture directories.

## Windows cadence investigation, 2026-10-04

The retained change uses **six bounded flush jobs on the existing Windows pool**
and defers bundle encoder-input allocation until encoding is needed. Two
independent eight-run 750-client series and one eight-run 1,000-client diagnostic
series passed the delivery recovery screen in both crossover orders. More
updates required more process CPU and UDP traffic.

Final means, **pre-merge -> retained candidate**, four runs per variant:

| Workload | Applied gap p50 (ms) | Applied gap p95 (ms) | Observer items/s | Combined CPU (cores) | UDP payload (MB/s) | UDP datagrams/s |
|---|---:|---:|---:|---:|---:|---:|
| 750 confirmation | 82.240 -> 71.713 | 122.530 -> 112.160 | 9,041 -> 9,937 | 5.193 -> 5.524 | 174.244 -> 193.119 | 144,967 -> 160,720 |
| 1,000 diagnostics | 189.563 -> 136.895 | 272.363 -> 222.705 | 5,257 -> 7,010 | 5.589 -> 5.846 | 109.731 -> 159.171 | 90,917 -> 131,395 |

All comparison series are summarized below; each uses its own matched control.
Changes are candidate relative to control. Lower gaps and higher applied items
are preferable. CPU is combined server/client process CPU; MB is decimal.

| Series | Total runs | p50 change | p95 change | Applied items change | CPU change | UDP payload change | Finding |
|---|---:|---:|---:|---:|---:|---:|---|
| 750 original review: merged vs pre-merge | 4 | +9.5% | +4.7% | -7.8% | -4.0% | -8.7% | Apparent slowdown; noisy two repeats/variant |
| 750 unchanged repeat: merged vs pre-merge | 4 | +17.8% | -2.7% | -10.6% | -2.8% | -13.0% | Original cause still unisolated |
| 1,000 two jobs vs merged | 4 | +24.1% | +28.8% | -21.1% | -18.1% | -22.7% | Rejected: less delivery despite lower CPU |
| 1,000 four jobs vs merged | 4 | -11.0% | -18.0% | +9.2% | -7.3% | +6.8% | Improved this screen; superseded |
| 750 four jobs vs merged | 4 | -10.4% | -15.7% | +10.5% | -3.3% | +13.8% | Improved this screen; needed direct pre-merge validation |
| 750 four jobs vs pre-merge | 8 | -0.2% | -7.3% | -0.1% | -0.9% | -0.9% | No consistent recovery margin |
| 750 four jobs + deferred cache vs pre-merge | 8 | -0.8% | -2.8% | -1.9% | -4.2% | -3.1% | Lower CPU accompanied less delivery |
| 750 same candidate, fixed ports vs pre-merge | 8 | +5.3% | -4.3% | -4.8% | -0.3% | -6.8% | Rejected: worse p50 and fewer applied items |
| 750 six jobs + deferred cache vs pre-merge | 8 | -4.6% | -8.5% | +4.0% | +6.5% | +5.4% | Both balanced blocks passed |
| 750 independent six-job confirmation vs pre-merge | 8 | -12.8% | -8.5% | +9.9% | +6.4% | +10.8% | Both balanced blocks passed |
| 1,000 six-job diagnostics vs pre-merge | 8 | -27.8% | -18.2% | +33.3% | +4.6% | +45.1% | Both balanced blocks passed |

The original review also included two merged-only 1,000-client diagnostic runs:
p50 117.18/147.52 ms, p95 178.98/200.22 ms, and payload 147.6/182.6 MB/s.
These were diagnostic observations, not a before/after comparison.

**Method and gates.** All Windows series used one frozen merged client, Rust
1.99.0 MSVC locked default release builds, CPU-only fixtures, IPv4 loopback,
four client Tokio workers, 45 s warmup after readiness and 60 s measurement.
Variants ran sequentially with repeated crossover order; per-pair diagnostics
were matched and enabled only at 1,000 clients. Native/application profiling and
affinity were off. The fixed-port series used UDP 64142 and health 64353; earlier
series varied ports. Client source ports remained OS-selected.

All 48 direct follow-up runs passed delivery/error/progress gates: every client
active in all 30 samples, all 749/999 observer peers covered, every sender
connected and progressing with at least 2,993 successful sends/window, and zero
measured sender/decode/state/protocol/UDP WouldBlock errors. Candidate drops were
zero; pre-merge lacks that explicit drop counter. The earlier 16-run bounded
investigation and six original review runs also passed their delivery gates.

**Interpretation.** Means of run quantiles are not pooled quantiles. The recovery
screen requires non-worse p50/p95 and non-lower applied items in each balanced
block, matching inputs/ports, sequential runs and delivery gates. It is not a
significance test. Some adjacent 750-client pairs lost; the first six-job
screen's second block improved applied items by only 0.2%. The original merge
cause remains unisolated, and the cache change alone did not establish a cadence
benefit. This busy-host, synthetic, one-observer loopback workload does not
establish physical-network capacity, every receiver's application, Linux/voice
performance, or absence of earlier 500 ms gaps from zero final stale counts.
Historical cohorts used different clients/toolchains/settings and are kept separate.

**Reports and evidence.** The [original review](../review-20261004.md),
[bounded-flush investigation](../windows-bounded-flush-20261004.md), and
[direct pre-merge report](../windows-regression-resolution-20261004.md) retain
the methodology, individual-run discussion, frozen hashes and reproduction
commands. Large JSON exports are local artifacts, rather than PR documentation:

| Artifact | Preserved location | Scope |
|---|---|---|
| Original review | Root checkout: `captures/performance-review-20261004/` and `docs/performance/results/review-20261004-summary.json` | Six fresh runs, historical context, frozen controls/client |
| Bounded-flush investigation | This worktree: `captures/regression-improvements-20261004/` | 16 runs, including rejected two-job candidate |
| Direct pre-merge validation | This worktree: `captures/regression-resolution-20261004/` | 48 runs, all six separate series, frozen candidate, manifests, analysis and build/test logs |
| Exact removed JSON exports | This worktree: `captures/pr27-results-cleanup-20261004/original-exports/` | Three byte-identical exports with SHA-256 index |

These ignored local artifacts are preserved but are not downloadable from a
fresh checkout. Use the checked-in crossover scripts and report commands for
fresh measurements; access to the archived captures is required to replay the
original analysis. The comparison tests use small synthetic fixtures, not
published benchmark measurements.

## Historical 1,500-peer results

The historical machine-generated JSON summaries are copied from the local experiment outputs and contain the measured per-run counters, hashes and peer distributions. Raw CSVs, binaries and perf recordings remain local and ignored. The JSON files were checked to contain no machine-local absolute paths; no measured values were changed.

| Summary | Comparison/source | Workload and role |
|---|---|---|
| [`i10-dense3-summary.json`](i10-dense3-summary.json) | I8 control source `1dfb900` vs I10 lazy-quality candidate `ba38df8`; the paired binary hashes are recorded in every run row | Four-run forward/reverse 1,500-colocated comparison plus a supplemental fixed-band 600-peer mixed-quality screen. The matched client binary hash is `b8818d7a…` and config hashes are included in the JSON. |
| [`i12-summary.json`](i12-summary.json) | I10 control source `ba38df8` vs I12 read-channel candidate `3f4bee3`; paired binary hashes are recorded in every run row | Four-run forward/reverse 1,500-colocated comparison. Client binary `896734c3…`; server/client config hashes are included in the JSON. |
| [`receiver-cycle-regression.json`](receiver-cycle-regression.json) | Control source exact PR head `3f4bee3f41ef0de948039e0333af44f7d15138e7` vs scheduler/CI-fix source now committed as `7c43786` | One unreplicated forward-order correctness regression screen. Full control and candidate binary/config hashes are recorded in each row. |

Historical matched settings were 1,500 colocated clients, 20 ms advertised server interval, zero movement jitter/drift, 60 FPS Unity-policy frame accumulator, deterministic synthetic valid pose with root yaw and nine body-rotation channels, voice/P2P disabled, server BSR profiling disabled, 15 s warmup, 60 s observer window, server affinity `0-7`, client affinity `8-15`, all 1,500 active/progressing senders, and 1,499 expected observer peers. The JSON retains exact sample endpoints, CPU windows, input/output counters, observer/error counts and hashes. Built logical sends count recipient avatar items before transport submission; encoded post-bundle entries are separate. Observer gaps are applied-update intervals at one receiver, not end-to-end movement latency. End age/stale is phase-dependent at the final snapshot.

The receiver-cycle correctness screen covered all 1,499 observer peers and all
1,500 progressing senders with zero decode/send errors. Its single forward
pair measured p50/p95 550.82/587.91 -> 557.00/598.68 ms and server CPU
283.24% -> 283.68%. It validates coverage, not a stable cadence improvement.

The summarized order-paired optimization outcomes were:

| Candidate vs accepted control | Server CPU mean | Built logical avatar work/s | Applied observer p50 / p95 | Additional tradeoff |
|---|---:|---:|---:|---|
| I10 lazy lower-quality materialization (`ba38df8` vs `1dfb900`) | 253.33% → 279.54% (+10.4%) | 2.884M → 3.873M (+34.3%) | 25.8% / 23.8% lower gaps | 600-peer supplemental fixed-band mixed-quality screen measured 1–2% less built work and 1–3% longer gaps with flat CPU; not a full-scale mixture. |
| I12 bounded 8-byte `read_channel` path (`3f4bee3` vs `ba38df8`) | 278.61% → 282.44% (+1.4%) | 3.897M → 4.129M (+5.9%) | 6.0% / 5.5% lower gaps | Higher total CPU, lower gaps in both run orders. |

Both candidates are higher-total-CPU cadence/throughput wins for this synthetic dense workload, not CPU reductions. The complete per-run rows, distributions, endpoints and hashes are in the linked JSON files; these headline values do not replace those rows.

The portable runner in [`run-avatar-workload.py`](../../../scripts/perf/run-avatar-workload.py) uses the same core workload policy, but its checked-in XML fixtures intentionally are not byte-identical to the historical configs. Historical server config hash: `35c48042…`; historical client config hash: `043b8b7f…`. Current fixture hashes: server `c27d64a1…`, client `608c21fa…`. The fixtures use a dedicated `avatar-bench-only` loopback credential instead of historical `default_password`, descriptive server/loopback host settings, a 1,500 client-count default, and normalized XML serialization. Server performance settings (20 ms default interval, base multiplier 1, increase rate 0, 2.5 slowest-send rate, uplink delta, compression/keyframe settings) match the historical harness overrides; remaining complete template values are retained. The portable runner defaults to no CPU pinning and does not itself sample `/proc` CPU windows; pass `--server-cpus 0-7 --client-cpus 8-15` to match historical affinity. Use the original published JSON for the exact historical A/B settings and outcomes; the portable runner is a safe fresh workload reproduction, not a byte-identical recreation of those paired captures.

Historical pose generation was synthetic and deterministic, not captured from a Unity runtime. “Unity-policy” means the source-backed send cadence/packing policy exercised by this Rust workload. No result here claims identical Unity physics, scheduling or pose entropy.

## Fresh-checkout workload

From the repository root, install Rust/Cargo 1.95 or newer, a native build
compiler/linker, and Python 3. The script uses Python's standard library; no pip
packages or `ffmpeg` are needed for this avatar-only workload. Linux `taskset`
is required only when affinity flags are supplied. Keep ports 4296/UDP and
10666/TCP free, or select unused `--port` / `--health-port` values.

```sh
CARGO_BUILD_JOBS=2 cargo build --locked --release --manifest-path BasisRustServer/Cargo.toml -p basis-server-console
CARGO_BUILD_JOBS=2 cargo build --locked --release --manifest-path BasisRustClient/Cargo.toml
python3 scripts/perf/run-avatar-workload.py --output captures/avatar-workload-A
```

The runner selects these built binaries by default, uses the checked-in
[server](../fixtures/avatar-1500-server.xml) and
[client](../fixtures/avatar-1500-client.xml) fixtures, waits for all 1,500 clients
to authenticate/become active, warms up for 15 seconds and measures for 60
seconds. It runs loopback only. Output directories must not already exist.
For a host with the corresponding allowed CPUs, add
`--server-cpus 0-7 --client-cpus 8-15`; otherwise choose valid affinity sets and
record them. Use `--help` to select binaries (`--server` / `--client`), client count and durations.
The fixtures are fixed by the runner; it has no config-selection flag. Clear
server configuration environment overrides (including `Password` and
`BASIS_SERVER_PASSWORD`) before running so they do not override the fixture or
prevent the matching client credential from connecting.

Outputs under the ignored capture directory include `workload.json` (settings and
binary/config hashes), `commands.txt` (commands), `server.log`, `client.log`,
`ready-health.json`, `readiness.csv`, `observer.csv`, `observer.sender.csv`,
`server-pairs.csv` and `server-pairs.global.csv`. Check that the files exist and
inspect coverage, decode/send errors, applied-update gaps and pair diagnostics;
the final “completed” message alone does not validate CSV contents or process
exit codes. Server/client CPU samples must be collected separately over matched
observer windows; this runner does not sample them.

For A/B comparisons, supply `--server` for each candidate, retain the
same client binary/configs, and use unique output directories. Repeat in ABBA
or forward/reverse pairs on the same host with the same clients, durations,
affinity and runtime settings. Record source/binary/config hashes, compare
matching measurement windows, and inspect individual runs before aggregating.
Report CPU together with update gaps, throughput, coverage and errors; a
speedup obtained by dropping work is not an improvement. State the number of
repeats and host/workload limits. Current fixture-based runs do not reproduce
unavailable historical captures; see [artifact availability](../artifact-availability.md).

## Hardware settings tuning

For a frozen-binary flush-lane sweep with native CPU/RSS sampling, delivery
validation and a readable recommendation/inconclusive report on Linux or
Windows, see [hardware tuning](../hardware-tuning.md). This settings path is
separate from binary revision comparisons and retains every failed/outlier run.

The [2026-10-04 tuning CLI smoke](../hardware-tuning-validation-20261004.md)
retains four Linux quick runs and reports an inconclusive result; its
[JSON](hardware-tuning-smoke-20261004.json) includes all measured runs and gates.
This is tool validation, not a new universal performance recommendation.
