# 1,500-client PGO A/B with scene and additional avatar data — 2026-10-10 (UTC)

This cohort adds **128 additional avatar bytes** and **128-byte unreliable scene broadcasts every 50 ms** to the dense 20 ms, Unity-policy 60 FPS workload. **0/8 evaluation runs passed the original strict gates.** Treat these measurements as an overload diagnostic: failed scene delivery or avatar quality prevents a clean PGO speedup claim. Every failed attempt and slow trial is retained.

[Portable evidence](results/pgo-mixed-client1500-20261010-summary.json) contains strict flags, raw scene/avatar/sender/global-counter CSV data, process and relevant health samples, readiness, commands, training outcomes, profile/build/tool hashes, and supplemental controller source. Binaries, profiles, and full logs remain locally in `captures/pgo-mixed-client1500-20261010`.

## Matched observations

Four fresh processes per variant ran in ABBA/BAAB order. Medians include invalid trials. Built avatar sends count planned recipient work; they do not establish received poses or scene delivery. CPU includes both workloads. Scene delivery and throughput describe receiving client zero.

| Metric | Ordinary | Fresh PGO | PGO change |
| --- | ---: | ---: | ---: |
| Server CPU | 6.765 cores | 6.771 cores | +0.10% |
| Client CPU | 1.695 cores | 1.740 cores | +2.66% |
| Built avatar recipient sends | 2,747,209.038 /s | 2,818,257.293 /s | +2.59% |
| Applied avatar gap p95 | 966.515 ms | 972.435 ms | +0.61% |
| Observed applied avatar items | 1,836.275 /s | 1,873.750 /s | +2.04% |
| Scene offered sends | 30,015.766 /s | 30,007.212 /s | -0.03% |
| Scene send cadence / requested | 100.053 % | 100.024 % | -0.03% |
| Scene delivery at observer | 1.198 % | 1.239 % | +3.48% |
| Observed scene messages | 359.280 /s | 371.664 /s | +3.45% |
| Scene latency p95 upper bound | 67.109 s | 67.109 s | +0.00% |
| Scene malformed messages | 0.000 count | 0.000 count | — |
| Server UDP transmit | 2,124.701 Mbps | 2,257.820 Mbps | +6.27% |
| Server peak RSS | 333.170 MiB | 327.404 MiB | -1.73% |
| Client peak RSS | 46.730 MiB | 48.441 MiB | +3.66% |

Scene p50/p95/p99 are log2 histogram upper bounds from same-host timestamps; measured maximum latency across trials was 61.22–61.40 s. Avatar gaps measure intervals between applied poses. One observer does not establish delivery to all clients.

## Every evaluation process

| Run | Server CPU cores | Built avatar sends/s | Avatar p95 ms | Scene delivery | Scene p95 upper seconds | Server exit | Failed original gates |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 01-baseline | 6.763 | 2,684,355 | 1003.94 | 1.21% | 67.11 | 1 | server_exit, zero_retransmits, scene_delivery, scene_quality |
| 02-candidate | 6.774 | 2,807,815 | 948.17 | 1.23% | 67.11 | 1 | server_exit, zero_stale_peers_500ms, zero_retransmits, scene_delivery, scene_quality |
| 03-candidate | 6.769 | 2,795,649 | 996.70 | 1.24% | 67.11 | 1 | server_exit, zero_retransmits, scene_delivery, scene_quality |
| 04-baseline | 6.792 | 2,631,470 | 965.33 | 1.20% | 67.11 | 1 | server_exit, zero_stale_peers_500ms, zero_retransmits, scene_delivery, scene_quality |
| 05-candidate | 6.774 | 2,828,700 | 997.26 | 1.25% | 67.11 | 1 | server_exit, zero_stale_peers_500ms, zero_retransmits, scene_delivery, scene_quality |
| 06-baseline | 6.747 | 2,912,048 | 967.70 | 1.20% | 67.11 | 1 | server_exit, zero_retransmits, scene_delivery, scene_quality |
| 07-baseline | 6.766 | 2,810,063 | 933.76 | 1.20% | 67.11 | 1 | server_exit, zero_stale_peers_500ms, zero_retransmits, scene_delivery, scene_quality |
| 08-candidate | 6.744 | 2,838,191 | 918.73 | 1.24% | 67.11 | 1 | server_exit, zero_stale_peers_500ms, zero_retransmits, scene_delivery, scene_quality |

All eight servers exceeded their shutdown deadline and exited with code 1; all clients exited cleanly. Every sampled population remained at 1,500 clients. Scene send, backpressure, and corruption counters were zero, and all 1,499 senders were observed. Avatar input coalescing rose by 49–741 during sampled windows; rejected inputs remained zero. Cumulative retransmit counters were nonzero but constant across each sampled window. These observations do not replace the original failed gates.

## Fresh training and PGO provenance

| Instrumented 250-client run | Scene delivery | Server exit | Failed gates |
| --- | ---: | ---: | --- |
| 01-training | 54.27% | 0 | scene_delivery, scene_quality |
| 02-training | 53.97% | 1 | server_exit, scene_delivery, scene_quality |

The standard trainer stopped after its first delivery failure. A supplemental controller ran the second process with the same offered input and retained its failed flags. Both runs delivered about 54% of scene messages, below the 95% gate. These are **diagnostic training inputs**, not validated delivery training; the profile manifest records `diagnostic_only=true`. The second server exceeded its shutdown deadline and exited with code 1. Its nonempty raw profile remained readable by the matching LLVM tool. Only the two new raw profiles were merged; previous pose-only profiles were excluded.

Source revision `31fa6392aec6442c42c831feeb21cbae8fc03c9f` contains pulled main `4ce5f8ca5862254b5e61805c856e9b0ed5e7aba9`. Server tree `b7130e68156ca47747f0832c7eac195923ed02f5` and client tree `aed0f1a0c84cb8f5632157798786e46bc485545f` match the preceding pose-only A/B. This benchmark changes no production code. Both arms share the existing ordinary Rust client, SHA256 `dffb4157a1259489b4555d38938b154f791c7e06a82275dca4fb97d14a5e62ca`; its original build commands, fingerprints, and checks are retained.

Ordinary, instrumented, and PGO servers use one frozen absolute source directory; Rust 1.99.0 / LLVM 23.1.1; locked release builds; explicit `x86_64-unknown-linux-gnu` target; incremental compilation off; two build jobs. Common `-Cllvm-args=-pgo-warn-missing-function` is identical. Profile generate/use flags and isolated target directories are the differing build controls. `CARGO_ENCODED_RUSTFLAGS` is unset. Profile hashes, compiler fingerprints, logs, and checks for missing-function/profile-mismatch diagnostics are retained. The stages follow the [rustc PGO workflow](https://doc.rust-lang.org/rustc/profile-guided-optimization.html), with the training-quality limitations above.

## Frozen workload, windows, and validity

- 1,500 colocated authenticated clients; 20 ms movement, zero jitter, Unity-policy 60 FPS, and 20-degree amplitude. Voice and P2P are off; server distance processing uses CPU only.
- 128 additional bytes accompany Ready/full updates and Unity-policy keyframes/deltas. Existing live-loopback tests verify exact full/delta payload preservation. The large-run observer validates pose application, without checking exact additional-byte content; low-quality and lock policies still apply.
- Every client offers a 128-byte scene message every 50 ms to every other client: 30,000 source sends/s, 44,970,000 recipient deliveries/s, and about 5.76 GB/s of payload before headers. This measures synthetic script relay; asset loading, physics, ownership, and Unity script execution are outside its scope.
- Rayon, server Tokio, and client Tokio each use four workers; server flush lanes are zero. Server CPUs: 2–9; client: 10–15; coordinator: 0–1. AC power and the performance power profile were checked before and after measurement. Training, builds, and evaluations are sequential. The 1,000 Mbps setting is an informational reference; it does not shape loopback traffic.
- Readiness precedes 45 s of avatar warmup. A shared marker starts avatar observation and scene sends. CPU, server, and sender windows last exactly 60 s; the avatar observer starts on its first post-marker pose. Scene runs through the 2 s receive grace to client shutdown, using its recorded duration for rates. Scene input begins after warmup, so scene queues are not initially steady.
- Original pose, population, sender, configuration, binary, control, and error gates remain intact. Scene gates require at least 95% unique delivery, every sender, requested cadence with one partial shutdown tick allowed, zero send/backpressure/corruption errors, correct byte accounting, and a complete duration. Commands attest payload sizes, cadence, delivery mode, and the shared marker.
- The standard A/B coordinator stops on a strict failure. The retained diagnostic continuation completes the scheduled processes with the same binaries, tools, and workload while preserving every failed flag. Cumulative retransmit gates remain distinct from window deltas; startup or warmup failures do not become passing results.

## Reproduce and checks

From the repository root, this command compares the retained binaries using a **new output directory**. Choose another output name if `evaluation-fresh` already exists. The strict coordinator will stop on failed gates, as it did here.

```sh
TASK_CAPTURE_ROOT=/home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-mixed-client1500-20261010
TASK_CLIENT=/home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-main-client1500-20261009-rerun/build-client/x86_64-unknown-linux-gnu/release/basis-rust-client
python3 -B scripts/perf/compare-avatar-revisions.py \
  --baseline "$TASK_CAPTURE_ROOT/build-release/x86_64-unknown-linux-gnu/release/basis-server-console" \
  --candidate "$TASK_CAPTURE_ROOT/build-use/x86_64-unknown-linux-gnu/release/basis-server-console" \
  --client "$TASK_CLIENT" --build-manifest "$TASK_CAPTURE_ROOT/build-manifest.json" \
  --output "$TASK_CAPTURE_ROOT/evaluation-fresh" \
  --clients 1500 --blocks 2 \
  --warmup-seconds 45 --window-seconds 60 \
  --server-cpus 2-9 --client-cpus 10-15 \
  --additional-avatar-bytes 128 \
  --scene-data-bytes 128 --scene-data-interval-ms 50
```

For fresh training/builds, use a new capture root, empty raw-profile directory, and the same frozen absolute source directory across ordinary/generate/use stages. Portable evidence retains the exact stage commands, environment, compiler/profile hashes, `training_invocation`, and local build/controller source. Training accepts the same three mixed-workload flags shown above and trains 250 clients. The stock trainer and coordinator reject invalid delivery; reproducing all diagnostic processes additionally requires the retained training/evaluation continuations. Those continuations preserve validity and do not certify the profile or cohort as clean.

Focused Python checks: 43 avatar tests passed with one Windows skip; three standalone scene tests passed; diff whitespace check passed. CodeRabbit reviewed the eight changed benchmark/test files and completed with zero findings. For unchanged production trees, prior checks are reused: 113 client unit tests and 10 live-loopback tests, plus 113 server and 87 transport tests passed; two client and three transport tests were ignored. Source/hash provenance is retained. Rust checks were not rerun during measurement.

One laptop, one profile trained at 250 clients, and four evaluation processes per arm provide descriptive evidence. This cohort does not establish remote-network behavior, maximum capacity, production Unity behavior, or a PGO win with validated delivery.
