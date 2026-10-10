# 750-client PGO A/B with scene and additional avatar data — 2026-10-10 (UTC)

**0/8 runs passed the original strict gates.** This cohort changes evaluation population from 1,500 to 750 while reusing the exact ordinary server, PGO server, ordinary Rust client, and diagnostic 250-client training profile. All eight runs failed scene delivery and exceeded the server shutdown deadline. This remains an overload diagnostic, with every attempt retained.

[Portable evidence](results/pgo-mixed-client750-20261010-summary.json) retains strict flags, raw observer/sender/counter CSV data, process and health samples, readiness, commands, binary/tool/profile hashes, and continuation source. Full local artifacts are in `captures/pgo-mixed-client750-20261010`; reused builds/profiles remain in `captures/pgo-mixed-client1500-20261010`.

## Matched observations

Four fresh processes per variant ran in ABBA/BAAB order. Medians include invalid runs. CPU covers both workloads; built avatar sends count planned recipient work rather than received poses. Scene throughput and delivery describe client zero. Scene latency uses same-host log2 histogram upper bounds (measured maxima were 58.38–58.57 s); avatar gaps measure intervals between applied poses. One observer does not establish delivery to every client.

| Metric | Ordinary | Reused PGO | PGO change |
| --- | ---: | ---: | ---: |
| Server CPU | 6.056 cores | 6.046 cores | -0.17% |
| Client CPU | 1.579 cores | 1.576 cores | -0.21% |
| Built avatar recipient sends | 1,444,989.750 /s | 1,545,671.352 /s | +6.97% |
| Applied avatar gap p95 | 519.935 ms | 495.275 ms | -4.74% |
| Observed applied avatar items | 1,928.675 /s | 2,059.750 /s | +6.80% |
| Scene offered sends | 15,002.355 /s | 15,002.985 /s | +0.00% |
| Scene send cadence / requested | 100.016 % | 100.020 % | +0.00% |
| Scene delivery at observer | 5.666 % | 5.836 % | +2.99% |
| Observed scene messages | 848.876 /s | 874.359 /s | +3.00% |
| Scene latency p95 upper bound | 67.109 s | 67.109 s | +0.00% |
| Server UDP transmit | 2,205.710 Mbps | 2,255.711 Mbps | +2.27% |
| Server / client peak RSS | 178.28 / 24.79 MiB | 178.01 / 26.67 MiB | — |

| Run | Server CPU cores | Avatar p95 ms | Scene delivery | Scene p95 upper seconds | Client/server exit | Failed original gates |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 01-baseline | 6.056 | 510.51 | 5.69% | 67.11 | 0/1 | server_exit, scene_delivery, scene_quality |
| 02-candidate | 6.049 | 497.28 | 5.84% | 67.11 | 0/1 | server_exit, scene_delivery, scene_quality |
| 03-candidate | 6.046 | 482.63 | 5.83% | 67.11 | 0/1 | server_exit, zero_retransmits, scene_delivery, scene_quality |
| 04-baseline | 6.081 | 482.73 | 5.67% | 67.11 | 0/1 | server_exit, zero_retransmits, scene_delivery, scene_quality |
| 05-candidate | 6.046 | 511.67 | 5.83% | 67.11 | 0/1 | server_exit, scene_delivery, scene_quality |
| 06-baseline | 6.050 | 532.75 | 5.64% | 67.11 | 0/1 | server_exit, scene_delivery, scene_quality |
| 07-baseline | 6.057 | 529.36 | 5.66% | 67.11 | 0/1 | server_exit, scene_delivery, scene_quality |
| 08-candidate | 6.040 | 493.27 | 5.86% | 67.11 | 0/1 | server_exit, scene_delivery, scene_quality |

Client exits: 8 with code 0; server exits: 8 with code 1. Observer stale peers per run: 0–0; sampled avatar coalescing deltas: 0–2; sampled retransmit deltas: 0–0. These sampled deltas remain separate from strict cumulative error gates; the evidence retains readiness and first/last counters.

## Reused training and source

| Previous instrumented 250-client run | Scene delivery | Server exit | Failed gates |
| --- | ---: | ---: | --- |
| 01-training | 54.27% | 0 | scene_delivery, scene_quality |
| 02-training | 53.97% | 1 | server_exit, scene_delivery, scene_quality |

The profile remains `diagnostic_only=true`: both previous training runs failed scene delivery, and the second server exceeded its shutdown deadline and exited with code 1. Its raw profile was readable by the matching LLVM tool. The same two merged raw profiles are reused; this cohort performs no training or builds. The prior [1,500-client report](pgo-mixed-client1500-20261010.md) records their full provenance and limitations.

Evaluation checkout: `b301d9feb9045f1ad290a7f7df529f7db29f533e`. Reused server/client builds: `31fa6392aec6442c42c831feeb21cbae8fc03c9f`, containing main `4ce5f8ca5862254b5e61805c856e9b0ed5e7aba9`. Production source trees remain server `b7130e68156ca47747f0832c7eac195923ed02f5` and client `aed0f1a0c84cb8f5632157798786e46bc485545f`. Rust 1.99.0 / LLVM 23.1.1, locked release target `x86_64-unknown-linux-gnu`, common `-Cllvm-args=-pgo-warn-missing-function`, isolated target directories, and generate/use profile flags are unchanged. The copied build manifest and original client fingerprint attest the reused binaries; profile/build hashes are checked again. This is a population comparison with the previous cohort, not a new main regression test. Cross-session population effects are descriptive.

## Workload and validity

- 750 colocated authenticated clients override the fixture's 1,500-client default. Movement is every 20 ms with zero jitter, Unity-policy 60 FPS and 20-degree amplitude; voice/P2P are off and server distance processing uses CPU only. Ready/full updates and keyframes/deltas carry 128 additional bytes. Existing live-loopback tests validate exact payload bytes; the large-run observer validates pose application.
- Each client offers 128-byte unreliable scene broadcasts every 50 ms: **15,000 source sends/s**, **11,235,000 recipient deliveries/s**, about **1.438 GB/s** of payload before headers, and **14,980 expected messages/s** at client zero. This exercises synthetic script relay, without asset loading, physics, ownership, or Unity script execution.
- All worker counts remain four; server flush lanes are zero. Server CPUs: 2–9; client: 10–15; coordinator: 0–1. AC power/performance profile were checked before and after measurement. The unchanged 1,000 Mbps reference is informational, not a loopback shaper. Native experiments run sequentially.
- Readiness precedes 45 s of avatar warmup. One marker starts scene sends and avatar diagnostics. CPU/server/sender windows last 60 s; avatar observation starts on the first inbound pose. Scene continues through the 2 s receive grace to shutdown and uses its actual duration for rates. Scene queues are initially cold.
- Original pose, population, sender, configuration, binary, control, and error gates remain intact. Scene requires 95% unique delivery, every sender, offered cadence with one partial shutdown tick allowed, zero send/backpressure/corruption errors, correct byte accounting, and complete duration. Payload sizes, delivery mode, cadence, and marker are attested against commands.
- The stock coordinator stops on failed gates. A retained diagnostic continuation completes the scheduled runs with the same binaries/tools/workload and preserves every failed flag. It does not turn failed runs into validated results.

## Reproduce and checks

From the repository root, compare the retained binaries with a new output directory; change `evaluation-fresh` if it already exists. This strict command stops on invalid delivery. Completing an invalid cohort additionally requires the retained continuation, which preserves validity.

```sh
python3 -B scripts/perf/compare-avatar-revisions.py \
  --baseline /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-mixed-client1500-20261010/build-release/x86_64-unknown-linux-gnu/release/basis-server-console \
  --candidate /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-mixed-client1500-20261010/build-use/x86_64-unknown-linux-gnu/release/basis-server-console \
  --client /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-main-client1500-20261009-rerun/build-client/x86_64-unknown-linux-gnu/release/basis-rust-client \
  --build-manifest /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-mixed-client750-20261010/build-manifest.json \
  --output /home/mgstange/.t3/worktrees/BasisRust/t3code-961cb963/captures/pgo-mixed-client750-20261010/evaluation-fresh \
  --clients 750 --blocks 2 \
  --warmup-seconds 45 --window-seconds 60 \
  --server-cpus 2-9 --client-cpus 10-15 \
  --additional-avatar-bytes 128 \
  --scene-data-bytes 128 --scene-data-interval-ms 50
```

No builds, retraining, or tests were run for this population change. Unchanged benchmark code reuses the prior 43 passing Python avatar tests (one Windows skip), three passing scene tests, whitespace check, and eight-file CodeRabbit review with zero findings. Unchanged production trees reuse 113 client unit and 10 live-loopback passes, plus 113 server and 87 transport passes; two client and three transport tests were ignored. Their source/hash provenance remains retained.

One laptop, four processes per arm, and a reused diagnostic profile provide descriptive evidence. This cohort does not establish remote-network behavior, production Unity behavior, maximum capacity, or a PGO win with validated delivery.
