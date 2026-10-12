# Hosted PGO release validation — 2026-10-12 UTC

All six native release-preview jobs in [run 38196274858](https://github.com/Toys0125/BasisRust/actions/runs/38196274858)
pass qualification, standard release staging, configuration formatting, final
checksums and artifact upload. Tested PR head is `f39ed491666deebdc89f155f6118fc28bc3cb3c2`.
The reports record GitHub's synthetic merge source
`c6625b9db1fadfdf719e130f0b0bc3e95edeb806`, whose parents are main
`f30b248329b213278153bee09995987a66b1a16d` and that PR head.
This validates the automatic build/package path, not actual release attachment
or Docker registry publication; those were not invoked.

Every selected population passes both training processes and all eight strict
evaluations (four ordinary, four PGO). Compiler is Rust 1.99.0, commit
`b940084d7eb6a299eb4bfeb8e34901bc051e7ac4`, with matching LLVM 23.1.1 tools.
The workload is dense 20-ms avatars, Unity-policy 60 FPS, 128 extra avatar bytes,
128-byte Scene at 5 Hz, voice off, CPU-only, 3-ms Scene batching, queue quota 1024
and 128 KiB/s upload/peer with two-second burst. Full processes use 45-second
warmups and 60-second measured windows. Server/client workers are fixed at four.

| Native server target | Selected clients | Retained higher-count failures | Selected evaluation Scene receipt | Received Scene p95 bound |
| --- | ---: | --- | --- | --- |
| Linux x64 GNU | 150 | At 300, one training run fails cumulative zero retransmits; profile is not merged. | 100% | 16.383 ms |
| Linux x64 musl | 150 | At 300, training passes; ordinary 4/4, PGO 2/4. Two PGO evaluations fail zero retransmits. Lower count retrains separately. | 99.953–100% | 16.383 ms |
| Linux ARM64 GNU | 300 | None | 99.678–100% | 32.767 ms |
| Linux ARM64 musl | 300 | None | 99.862–100% | 32.767 ms |
| macOS ARM64 | 300 | None | 99.762–100% | 65.535–131.071 ms |
| Windows x64 MSVC | 300 | None | 100% | 65.535 ms |

The 100-client floor was not needed. A 150-client pass does not certify the failed
300-client attempt. Retransmit gates remain absolute, including setup traffic.
All selected runs retain full population/sender coverage, clean native server
exit and the Scene/upload/image/provenance gates. macOS now completes fresh
training, merge, profile use and packaging without the earlier crash or missing
profile diagnostics. Its three server builds share record-retention flags;
other targets retain their normal flags. The common report sentence describes
macOS policy, not a claim that other targets use that flag.

## Descriptive A/B results

Medians below are four processes per arm on each target's hosted runner.
Percentages use the rounded medians in the Markdown reports.
CPU is process CPU seconds divided by elapsed seconds. Avatar gap is the
observer's interval between applied updates. Built work counts logical avatar
recipient work and is not a count of network datagrams or guaranteed receipts.

| Target / clients | CPU cores ordinary → PGO | Avatar p95 gap, ms ordinary → PGO | Built logical work/s ordinary → PGO | CPU / gap / work change |
| --- | --- | --- | --- | --- |
| Linux x64 GNU / 150 | 1.629 → 1.636 | 47.715 → 47.940 | 705,628 → 745,577 | +0.43% / +0.47% / +5.66% |
| Linux x64 musl / 150 | 1.594 → 1.676 | 48.500 → 41.160 | 669,396 → 823,264 | +5.14% / −15.13% / +22.99% |
| Linux ARM64 GNU / 300 | 1.618 → 1.563 | 140.490 → 133.430 | 919,271 → 955,780 | −3.40% / −5.03% / +3.97% |
| Linux ARM64 musl / 300 | 1.589 → 1.587 | 132.775 → 113.870 | 859,908 → 1,003,450 | −0.13% / −14.24% / +16.69% |
| macOS ARM64 / 300 | 1.018 → 1.146 | 273.835 → 355.920 | 470,701 → 397,970 | +12.57% / +29.98% / −15.45% |
| Windows x64 MSVC / 300 | 1.864 → 1.859 | 152.360 → 138.625 | 787,827 → 910,313 | −0.27% / −9.01% / +15.55% |

PGO is not a universal speedup: macOS PGO is slower in this sample, and Linux x64
GNU has nearly unchanged gap/CPU. Qualification checks behavior, not performance
dominance. The requested always-PGO release policy is retained, with these
results visible. One host per target, four observations per arm, hosted-machine
variation, loopback and one observer limit regression/performance conclusions.
Received Scene latency excludes lost messages. These runs do not certify 20-Hz
Scene, voice, images/photos, larger populations or counter-free builds.

macOS ordinary/instrumented/PGO server sizes are 24,177,696 / 64,798,272 /
21,779,328 bytes. Both ordinary and PGO carry the same retention setting; these
sizes do not measure overhead relative to a normally stripped macOS build.
Distributed PGO servers retain `benchmark-validation` counters.

## Package verification

All six downloaded archives pass every final `SHA256SUMS` entry. Each server's
hash matches the PGO build for the report's specifically selected passing client
count. Every non-musl client hash matches the exact shared benchmark client.
Standard `server/` / `client/` layout and archive names remain intact; musl stays
server-only for Docker. Each archive includes Markdown validation and has no
JSON, JSONL, raw profile or merged-profile files. Local evidence is under
`captures/pr35-qualified-f39ed49/` and `captures/pr35-release-artifacts-f39ed49/`.

A later CI-only follow-up restores full Python benchmark discovery, including
all 23 PGO rejection guards and macOS sampler tests, in the existing lightweight
PR job. The exact command passes 103 tests locally with four platform skips.
This change does not modify server/client source, PGO driver, workload, fixtures
or release packaging; the native results above remain tied to their recorded
source. Earlier failed attempts remain in the consolidated validation history.
