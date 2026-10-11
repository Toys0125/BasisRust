# Final main pull: 750-client regression benchmark — 2026-10-10

Merged latest fetched `main` `f30b248329b213278153bee09995987a66b1a16d` into `performance/branch-optimization`. The candidate production source is merge revision `48163a20b05fc9eeb0cad982b1905aab4ed88e35`. All final source/binary hashes are verified. This compares **pre-pull server `97fd88e16b9197733846db15216464952a9e86da` against the merged server**, using one freshly built final Rust client for both arms. It is a server regression A/B; client rebuild effects are held constant.

**Every original and added strict gate passed in 2/2 pre-pull and 2/2 merged processes.** Median server CPU changes +0.28% and avatar p95 applied gap +3.81%. CPU and avatar-gap paired directions reverse between rounds. These are two-process descriptive medians, not significance or a general no-regression guarantee. Keep delivery, admitted work, avatar cadence and errors together when interpreting the changes.

## Matched measurements

Four fresh process pairs run **pre-pull/merged/merged/pre-pull (ABBA)**. The fixed workload has 750 colocated Rust clients, 20-ms avatar updates, Unity-policy 60-FPS poses, amplitude 20, no jitter/spread, 128 extra avatar bytes and 128 unreliable Scene bytes every 200 ms (5 Hz). Voice/P2P off, CPU-only server. Both arms use upload limit 131072 B/s per connection, 3-ms Scene batching, 1024 pending-message recipient quota, avatar flush lanes 0 and four Rayon/server Tokio/client Tokio workers. Only the server binary changes.

| Process median | Pre-pull | Merged main |
| --- | ---: | ---: |
| Scene receipt | 100.000% | 99.943% |
| Scene p95 histogram bound, ms | 131.071 | 131.071 |
| Avatar p95 applied gap, ms | 487.180 | 505.725 |
| Applied avatar items/s | 2,034.783 | 1,959.883 |
| Server CPU cores | 5.443 | 5.458 |
| Server peak RSS, MiB | 161.6 | 159.7 |
| Raw UDP transmit, Mbit/s | 4,542.8 | 4,546.0 |
| Accepted upload bytes/client/s | 9,130.6 | 9,131.1 |
| Rejected upload messages/s | 0 | 0 |
| Rejected Scene recipient messages/s | 0 | 0 |
| Maximum pending Scene messages/recipient | 749 | 749 |
| Every strict gate passed | 2/2 | 2/2 |

Tables contain process medians, not pooled percentiles. Scene receipt includes every offered observer message; latency is a received-message histogram upper bound. CPU/upload rates use the fixed 60-second window; Scene rates use actual observer duration including two-second receive grace. Avatar gaps are applied-update intervals, not end-to-end pose latency. The 5-Hz rate is explicit; this does not validate the original 20-Hz capacity requirement.

| Process | Scene receipt | Scene p95 bound, ms | Avatar p95, ms | CPU cores | Scene senders / 749 | Retransmits first → last | Failed gates |
| --- | ---: | ---: | ---: | ---: | ---: | --- | --- |
| 01-baseline | 100.000% | 131.071 | 479.860 | 5.445 | 749 | 0 → 0 | none |
| 02-candidate | 100.000% | 131.071 | 519.330 | 5.487 | 749 | 0 → 0 | none |
| 03-candidate | 99.886% | 131.071 | 492.120 | 5.430 | 749 | 0 → 0 | none |
| 04-baseline | 100.000% | 131.071 | 494.500 | 5.440 | 749 | 0 → 0 | none |

## Merge integration and image behavior

`main` adds the image fanout bandwidth governor, bounded cache/cached replay, and reliable fragmented-message receive support in both client and server. The merge retains this branch’s cold dispatch helpers, realtime upload budget and experimental Scene batching/quota. Conflicting state fields/initializers preserve both implementations; the branch-only transport benchmark fixture was updated for the new receive-fragment field and parser tuple. The new image-test connection fixture owns fresh upload and Scene budgets.

The previous report’s absence of a Rust server image governor describes the pre-pull revision. **The merged server now enforces the separate image governor and cached replay allowance.** Image Scene traffic still bypasses the 128 KiB/s generic upload budget and is charged after fanout by that separate governor. The measured workload contains ordinary Scene traffic and extra avatar data, not image chunks. Focused image/cache and real-UDP fragmented-upload tests validate the integration; this cohort does not claim photo-transfer capacity. The incoming [500-client image-load report](image-load-500-20261010.md) retains its own workload and limitations.

The added optional `extended.images` telemetry reports `egressDroppedMessages`, `egressDroppedBytes`, `cachedImages`, `completeImages` and `cacheBytes`. Both merged runs retain all-zero observations for these fields, alongside zero image-exempt uploads; ordinary Scene traffic therefore did not enter the image path. The older baseline lacks this telemetry and is explicitly marked unavailable, with null observations rather than invented zeros. This small observability addition is part of the measured merged candidate.

## Exact source and verification

- Latest fetched main `f30b248329b213278153bee09995987a66b1a16d` is an ancestor of merge `48163a20b05fc9eeb0cad982b1905aab4ed88e35`. Merge parents: `97fd88e16b9197733846db15216464952a9e86da`, `f30b248329b213278153bee09995987a66b1a16d`. No fetch or source mutation during measurement.
- Baseline server SHA-256 `51b269579793691be0f29632fc6f0f899142474464ae4127292139c06a2fdb20`, source tree `8b69b573f7d756c102765f54ffc421d7e48ef274`. Merged server SHA-256 `a74980477983c9c05d8bd4e2f886f68aa010bd8d10b0e4e714abd401faaca2d2`, source tree `693858532d09d00e67803caa9d9b8222e9605cf5`. Final client SHA-256 `a394105596e06783b18860311a7af3c6837ba1776dc3e55cb9661a5a8515983b`, source tree `6462935e8ea266778748065bad43ea0079c838ad`. Both binary-source matches are checked against the final branch.
- Rust 1.99.0 / LLVM 23.1.1, locked ordinary release for x86_64-unknown-linux-gnu, incremental off, two build jobs, common `-Cllvm-args=-pgo-warn-missing-function`, encoded flags unset. **PGO is off**; no old profile is applied to changed source.
- Fixed original CPU-only server/client fixtures. Server CPUs 2–9, client 10–15, coordinator 0–1; AC/performance profile checked for each run and after the cohort. Full authenticated/active population, 45-second warmup, shared observer/Scene/diagnostic marker, 60-second window plus two-second grace and clean shutdown. No overlapping builds or benchmarks. 1000 Mb/s is a reporting reference, not a shaper.
- Nine core upload tests including real-UDP fragment reassembly charged once and reliable oversize/debt/rejection behavior; 23 image-filtered core tests; six server transport fragment/lifecycle tests; six final-client fragment tests; ten health tests; console compilation; both workspace format checks; three upload-analysis and four final-analysis integrity checks pass. Overlapping filtered tests are not presented as unique totals. Initial fixture/signature compile failures are retained beside corrected passing checks.
- Initial scoped server CodeRabbit review completed with warnings and one major finding: InvalidFragment rejection could remove a transport peer without publishing disconnect lifecycle cleanup. The finding was verified and fixed using a shared lifecycle rejection helper and a regression test covering full-queue backpressure, cleanup ownership, retained-budget release and ID reuse. The first follow-up identified a blocking lifecycle reservation and silently ignored invalid fragment headers; both were fixed and covered by retry/backpressure and six invalid-header cases. Final fresh scoped transport review reports zero findings, with outcome `completed_with_warnings` and unverified findings; this is not an unqualified clean-review claim. Exact review rounds are retained. Final provenance checks rederive every run, preserve all original flags, verify source/binary/config/tool hashes, fix the entire server environment across arms, and require continuous 750-client population and zero upload/Scene admission loss. The absolute-zero cumulative retransmit gate is unchanged.

local evidence (`captures/pr-pgo-implementation-20261011/raw-results/final-main-client750-20261010-summary.json`) includes all raw observer/sender/Scene CSV, native health/CPU/RSS samples, commands, errors, shutdown logs, build/merge manifests and controller sources. Local captures: `captures/final-main-client750-20261010`. One Linux laptop on loopback and one receiver observer limit conclusions; existing batched Scene app-output accounting limitations remain.
