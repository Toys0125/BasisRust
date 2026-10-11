# Qualified mixed-workload server PGO — 375 clients, 2026-10-11

**The supported PGO pipeline qualifies at 375 clients.** Both fresh instrumented
training processes and all eight fresh evaluations pass every original/native
gate: 51 checks per process, plus fresh nonempty profiles for training. Ordinary
and PGO each pass 4/4 evaluations. The failed historical 750-client training is
preserved; it was not reused. The configured 250-client fallback was unnecessary.

Compared with ordinary release, PGO's median avatar p95 gap improves **12.24%**,
applied observer items increase **3.38%**, and built logical work increases
**3.34%**. Median CPU rises **0.45%**; CPU per million built logical sends falls
**3.57%**. Scene receipt is exactly **100%** for both arms, with the same received
p95 histogram upper bound of **32.767 ms**. This supports a qualified optional
build at this population, not a universal speedup or 750-client capacity claim.

## Frozen build and workload

- Source: `59c708a75070d0efe20ca5d207e8a22f42c1b691`, after merging latest main
  `f30b248329b213278153bee09995987a66b1a16d`. Subsequent report-only changes do not
  alter measured server/client or benchmark inputs.
- Rust 1.99.0 (`b940084d7`, 2026-09-28), matching LLVM 23.1.1 tools,
  native `x86_64-unknown-linux-gnu`, locked dependencies and fresh isolated
  ordinary/client/instrumented/profile-use builds. Every build exits zero with
  zero profile diagnostics; Cargo flags/release fingerprints verified.
- One shared freshly built Rust client; two excluded training processes, eight
  ABBA/BAAB evaluations (four per arm), 45-second warmup/60-second measurement.
- 375 clients, 20-ms avatar updates, Unity-policy 60-FPS poses, 128 additional
  avatar bytes and 128-byte Scene payloads every 200 ms; voice off, CPU-only.
- Server CPUs 2-9, client CPUs 10-15; fixed four server Rayon/Tokio and client
  workers, 3-ms Scene batching, 1024 logical pending messages per peer,
  131072-B/s generic upload cap before fanout and two-second burst credit.
- Every process retains all 375 clients and all 374 Scene senders, clean exits,
  zero retransmits, upload/Scene admission rejects and image work/drop counters.
  Input stays matched: about 18,749 avatar updates/s and 9,130 upload B/client/s.

## Descriptive results

| Metric, median of four processes per arm | Ordinary | PGO | Change |
| --- | ---: | ---: | ---: |
| Server CPU cores | 3.160 | 3.174 | +0.45% |
| Avatar p95 gap, ms | 127.150 | 111.590 | -12.24% |
| Observer applied avatar items/s | 3599.750 | 3721.300 | +3.38% |
| Built logical avatar work/s | 1,350,009 | 1,395,108 | +3.34% |
| CPU seconds/million built sends, per-run normalized median | 2.360318 | 2.276085 | -3.57% |
| Scene receipt | 100.000% | 100.000% | unchanged |
| Received Scene p95 upper bound, ms | 32.767 | 32.767 | unchanged |
| Accepted upload B/client/s | 9130.413 | 9129.702 | -0.008% |

All four matched pairs improve avatar p95 and built work, but CPU changes have
mixed direction. Pair 2's gap improvement is only 0.60%; pair 3 has 17.28% more
built work and 6.86% more CPU. Every process remains included. Four processes per
arm on one loopback host/one observer are descriptive, not statistical proof.
Received Scene latency excludes losses; receipt is separately required.
No qualification of 20-Hz Scene, voice or photo/image traffic is implied.

## Provenance and artifact verification

| Artifact | SHA-256 |
| --- | --- |
| Ordinary server | `ce09dbe1007cb19fe48d81b450efd497f88442f133d5f6660ede074fc72e8b05` |
| Shared Rust client | `2e12d153a90b7890c77b95704ab9a2c5f0cec4b40b8dbea4e1ea3c4493f4e32b` |
| Instrumented server | `1ca3f75c4d134e330a973ae83eb598a8c751bb3960a323fef6b7d5729629fd5c` |
| PGO server | `bc18c7363076db99a73de1bd9bbb7c15bb0e19d4bcbd4a3ed93161995d67f24f` |
| Fresh merged profile | `1514caccaa27aa2653d927671d3195526ab1b3a0fd59e97f40e63ec451d0fb6a` |

The pipeline produced the qualified package locally:
`captures/pr-pgo-qualified-20261011/dist/BasisRust-server-linux-x64-pgo.tar.gz`.
Archive SHA-256:
`949eae40ac5f6d0bf24f2087c8a5e9e7eeeaf26fa80b2424ef33e14e62bde249`.
Extraction verifies every packaged checksum. Contents are the server,
`Config.xml`, `PGO-VALIDATION.md` and `SHA256SUMS`; no JSON or profiles.
Full source/control hashes, profiles, logs and every raw observation stay local
under `captures/pr-pgo-qualified-20261011/` and are excluded from the PR diff.
The manual CI artifact workflow reproduces this process on a dedicated runner;
this local qualification is not a claim that that workflow has already run.

## Every fresh process

| Clients | Phase / process | Scene receipt | Scene p95 bound, ms | Avatar p95 gap, ms | CPU cores | Failed gates |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 375 | training/01-baseline | 100.000% | 32.767 | 191.100 | 3.177 | none |
| 375 | training/02-baseline | 100.000% | 32.767 | 190.220 | 2.871 | none |
| 375 | evaluation/01-baseline | 100.000% | 32.767 | 118.230 | 3.144 | none |
| 375 | evaluation/02-candidate | 100.000% | 32.767 | 111.590 | 3.183 | none |
| 375 | evaluation/03-candidate | 100.000% | 32.767 | 112.080 | 3.168 | none |
| 375 | evaluation/04-baseline | 100.000% | 32.767 | 112.760 | 3.216 | none |
| 375 | evaluation/05-candidate | 100.000% | 32.767 | 109.590 | 3.150 | none |
| 375 | evaluation/06-baseline | 100.000% | 32.767 | 156.860 | 2.948 | none |
| 375 | evaluation/07-baseline | 100.000% | 32.767 | 136.070 | 3.177 | none |
| 375 | evaluation/08-candidate | 100.000% | 32.767 | 111.590 | 3.181 | none |

