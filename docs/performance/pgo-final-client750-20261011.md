# Fresh final-source PGO A/B: 750 clients — 2026-10-11

Compared freshly built ordinary and PGO servers from the same final merged revision `c75113fcd402bd31d92a24789212f9b9eaa8c844`. One ordinary current-source Rust client is shared across both arms. No main pull, production code change or earlier profile reuse is part of this experiment.

**Strict evaluation passes: ordinary 3/4; PGO 4/4.** Median server CPU changes -0.63%; avatar p95 applied gaps change -11.34%. Failed original gates remain visible; these observations do not establish a validated PGO performance win.

**Training profile is diagnostic only.** Instrumented training is excluded from A/B timings. Its original gates and failures are retained below. PGO remains an experimental build and has not been enabled by default.

## Matched workload and medians

Eight fresh evaluation processes run **ordinary/PGO/PGO/ordinary/PGO/ordinary/ordinary/PGO (ABBA/BAAB)**, four per arm. The fixed workload has 750 colocated Rust clients, 20-ms avatar updates, Unity-policy 60-FPS poses, amplitude 20, no jitter/spread, 128 extra avatar bytes and 128-byte unreliable Scene broadcasts every 200 ms (5 Hz). Voice/P2P off, CPU-only server. Both arms use upload cap 131072 B/s per connection before fanout, 3-ms Scene batching, 1024 pending-message recipient quota, avatar flush lanes 0 and four Rayon/server Tokio/client Tokio workers. Only the server build and PGO flag/profile differ.

| Process median | Ordinary | Fresh PGO |
| --- | ---: | ---: |
| Scene receipt | 99.952% | 99.924% |
| Scene p95 histogram bound, ms | 131.071 | 131.071 |
| Avatar p95 applied gap, ms | 558.090 | 494.775 |
| Applied avatar items/s | 1,847.533 | 2,041.025 |
| Built logical avatar work/s | 1,382,936.6 | 1,526,167.4 |
| Server CPU cores | 5.444 | 5.410 |
| Peak server RSS, MiB | 162.4 | 161.5 |
| Raw UDP transmit, Mbit/s | 4,392.8 | 4,491.6 |
| Accepted upload bytes/client/s | 9,130.6 | 9,130.5 |
| Rejected upload messages/s | 0 | 0 |
| Rejected Scene recipient messages/s | 0 | 0 |
| Maximum pending Scene messages/recipient | 749 | 749 |
| Every strict gate passed | 3/4 | 4/4 |

Tables contain medians of four processes per arm, not pooled percentiles. Scene receipt counts all offered observer messages; its latency is a received-message histogram upper bound. Avatar gaps measure applied-update intervals, not end-to-end pose latency. CPU/upload use the fixed 60-second window. Scene rates use actual observer duration including two-second receive grace. This 5-Hz experiment does not establish 20-Hz or photo-transfer capacity.

| Process | Scene receipt | Scene p95 bound, ms | Avatar p95, ms | CPU cores | Scene senders / 749 | Retransmits first → last | Client/server exits | Failed gates |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |
| 01-baseline | 99.904% | 131.071 | 555.690 | 5.513 | 749 | 0 → 0 | 0/0 | none |
| 02-candidate | 99.862% | 131.071 | 490.110 | 5.409 | 749 | 0 → 0 | 0/0 | none |
| 03-candidate | 100.000% | 131.071 | 460.670 | 5.411 | 749 | 0 → 0 | 0/0 | none |
| 04-baseline | 100.000% | 131.071 | 520.440 | 5.433 | 749 | 8 → 8 | 0/0 | zero_retransmits |
| 05-candidate | 99.953% | 131.071 | 504.540 | 5.371 | 749 | 0 → 0 | 0/0 | none |
| 06-baseline | 99.726% | 131.071 | 598.780 | 5.439 | 749 | 0 → 0 | 0/0 | none |
| 07-baseline | 100.000% | 131.071 | 560.490 | 5.449 | 749 | 0 → 0 | 0/0 | none |
| 08-candidate | 99.896% | 131.071 | 499.440 | 5.423 | 749 | 0 → 0 | 0/0 | none |

| Matched round | CPU change | Avatar p95 gap change |
| --- | ---: | ---: |
| 1 | -1.89% | -11.80% |
| 2 | -0.41% | -11.48% |
| 3 | -1.25% | -15.74% |
| 4 | -0.47% | -10.89% |

## Fresh profile and exact build

Two new instrumented 750-client training processes use the same workload, controls, client and affinities as evaluation. Their profiles are isolated by run, module and PID; only these two files enter the matching Rust-toolchain LLVM merge. No previous `.profraw` or `.profdata` is reused.

| Instrumented training process | Scene receipt | Scene p95 bound, ms | Retransmits first → last | Client/server exits | Failed gates |
| --- | ---: | ---: | --- | --- | --- |
| 01-baseline | 100.000% | 131.071 | 42 → 42 | 0/0 | zero_retransmits |
| 02-baseline | 100.000% | 131.071 | 11 → 11 | 0/0 | zero_retransmits |

Absolute-zero cumulative retransmits remain required, including connection setup; unchanged counters during a measurement window do not erase a failed gate. Profile readability alone does not validate training. All offered work, receipt, avatar cadence, CPU, errors and shutdown outcomes must be interpreted together.

- Server source tree: `693858532d09d00e67803caa9d9b8222e9605cf5`; client tree: `6462935e8ea266778748065bad43ea0079c838ad`. Source archived from the clean committed revision, with hashes of every source file retained.
- Ordinary server SHA-256: `f6d6f445112e0e651b23a9fa4ad3ce10f224d2eba01fff9cb438e328e23cf455`.
- PGO server SHA-256: `b6c1526b510ca2928226d9a5a1d675f49cd86de0fe0f43e53353f25c28fd9f65`.
- Shared client SHA-256: `a394105596e06783b18860311a7af3c6837ba1776dc3e55cb9661a5a8515983b`. Reuses the final-main client whose exact source/compiler/build provenance is verified.
- Fresh merged profile SHA-256: `f2abb77ca4013ac0ac2266c76f3910d8987a7044298e329bb73b131a50f5efc4`.
- Rust 1.99.0 / LLVM 23.1.1, locked release target `x86_64-unknown-linux-gnu`, isolated fresh target directories, `CARGO_INCREMENTAL=0`, build jobs 2. Common `-Cllvm-args=-pgo-warn-missing-function`; instrumented adds absolute `-Cprofile-generate=...`, PGO adds absolute `-Cprofile-use=...`. `CARGO_ENCODED_RUSTFLAGS` unset. Cargo fingerprints verify exact flags and identical release settings. No missing/stale/mismatched profile diagnostics.
- The same source directory is used for all server builds. The ordinary binary is freshly rebuilt for this A/B; yesterday’s timing medians are not used as the baseline. Instrumented timings are excluded.
- Original CPU-only server/client fixtures are frozen. Server CPUs 2–9, client 10–15, coordinator 0–1. AC/performance mode checked before, after and for each run. Full 750-client readiness, 45-second avatar warmup, shared start marker, 60-second window and two-second receive grace. Builds and native runs execute sequentially.
- Every original gate is preserved, including continuous population, all 749 senders, pose application, offered cadence, payload bytes, errors, clean exits, exact binaries and configuration. Both arms additionally require native zero upload/Scene rejections, queue bounds and all-zero image work/drop counters. Invalid completed runs are retained without relaxing checks.
- Focused analysis checks: five matched-artifact/image-telemetry tests and three upload-accounting tests passed. Controllers parse successfully. Retention independently recomputes every observation and verifies source, binaries, profiles, tool hashes, population, controls, power and order. Production source is unchanged; this experiment does not claim new production unit-test or CodeRabbit runs. Prior focused source checks are in the final-main report.

## Reproduce

Local capture root: `captures/pgo-final-client750-20261011`. Retained `build.py`, `compare-pgo.py`, `drive.py`, `merge-profile.py`, `finalize-build.py` and analysis/retention controllers contain exact commands. Training and evaluation outputs must be fresh paths. Example evaluation (same eight-run order):

```sh
python3 -B /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-final-client750-20261011/compare-pgo.py \
  --baseline /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-final-client750-20261011/build-release/x86_64-unknown-linux-gnu/release/basis-server-console \
  --server /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-final-client750-20261011/build-use/x86_64-unknown-linux-gnu/release/basis-server-console \
  --client /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/final-main-client750-20261010/build-client/x86_64-unknown-linux-gnu/release/basis-rust-client \
  --output /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-final-client750-20261011/evaluation-fresh --repeats 4 --clients 750 --scene-interval-ms 200
```

[Rust PGO workflow](https://doc.rust-lang.org/rustc/profile-guided-optimization.html) provides the generate/run/merge/use steps followed here.

[Portable evidence](results/pgo-final-client750-20261011-summary.json) retains both training attempts, all eight evaluations, original flags, raw observer/sender/Scene CSV, health/process samples, commands, profiles/build hashes, source provenance, controller sources and logs. One Linux laptop, loopback and one observer limit generalization. Four processes per arm provide descriptive evidence rather than significance or a universal speedup. The 1000-Mbit/s reference is not a shaper; batched Scene app-output accounting limitations remain.
