# Rust performance validation

This is the consolidated review record for PR #35: implementation decisions,
all Rust benchmark series, C# reference checks, native PGO release qualification,
and correctness/tooling verification. The operational guide is
[automatic qualified PGO builds](server-pgo.md). Earlier main-branch performance
reports remain available through [the results index](results/README.md).

## Current implementation and decisions

Automatic releases require freshly qualified PGO for all six native server
targets on GitHub-hosted runners. Clients and plain local Cargo builds remain
ordinary. No self-hosted runner or ordinary-server release fallback is used.
Qualification is a behavior gate, not a guarantee that PGO improves performance.

Scene batching (`BASIS_SCENE_BATCH_MS`) and recipient pending-message quotas
(`BASIS_SCENE_PEER_QUEUE_LIMIT`) are independent, default-off controls. The
benchmarks explicitly enable 3 ms and 1024 logical messages per recipient for
the batched unreliable empty-recipient broadcast path. Permits follow connection
incarnations and release when work leaves the queue; limits prevent unbounded
backlog without interpreting rejected work as a speedup. Directed/reliable Scene
traffic retains its existing path. Main integration also fixes fragment receive
and lifecycle behavior.

`MaxUploadBytesPerSecondPerPlayer` defaults to 131072 B/s (128 KiB/s) per
connection, before fanout; XML/environment overrides use that exact name and 0
disables admission. Application envelopes/payloads are charged once before
coalescing/relay; protocol/auth/control traffic and transport overhead are
excluded. Two seconds of burst credit are allowed. Best-effort excess is
rejected; reliable excess disconnects rather than silently dropping acknowledged
work. One oversized reliable frame can start from a full bucket and incur its
full byte debt; another oversized frame needs a refilled bucket. Clients must
pace reliable uploads. The existing after-fanout Scene Mbps limiter is separate.

`BasisImagePickupManager` Scene traffic is exempt from the generic budget.
Rust advertises separate image-pacing settings but does not implement the C#
server image governor. Image telemetry/exemption tests do not establish a
server-enforced photo rate or image-fanout capacity.

`benchmark-validation` compiles Scene/upload counters, snapshot APIs and detailed
status/health fields out of normal builds. Real admission/queue limits stay
active. Missing diagnostic fields are omitted, not fabricated as zero. All PGO
server stages enable the same feature and check Cargo fingerprints; distributed
PGO results therefore include counter overhead.

Transport cold-path/decoder changes have mixed synthetic results and no measured
application speedup. The decoder-only revert was rejected. Rust 1.99.0 already
compiled the original decoder probe to compare/conditional-move code; the early
jump-table rationale was incorrect. Corrected comments retain that limitation.

## Latest hosted native qualification — 2026-10-12 UTC

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

### Descriptive A/B results

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

### Package verification

All six downloaded archives pass every final `SHA256SUMS` entry. Each server's
hash matches the PGO build for the report's specifically selected passing client
count. Every non-musl client hash matches the exact shared benchmark client.
Standard `server/` / `client/` layout and archive names remain intact; musl stays
server-only for Docker. Each archive includes Markdown validation and has no
JSON, JSONL, raw profile or merged-profile files. Local evidence is under
`captures/pr35-qualified-f39ed49/` and `captures/pr35-release-artifacts-f39ed49/`.


## Earlier controlled Rust measurements

Each comparison freezes its own source, shared Rust client, configurations,
compiler and controls. Experiments changing client source, population or Scene
rate are separate cohorts. Percentage changes are candidate relative to the
matched baseline. Most eight-process series use ABBA/BAAB (four per arm);
four-process screens use ABBA (two per arm). Failed runs/outliers are retained.

| Experiment | Strict results and user-facing outcome | Interpretation |
| --- | --- | --- |
| Desktop branch microbench, laptop repeat | Desktop mixed. Laptop: 864 samples, matched outcomes and full coverage; decoder +1.20% time, parsing -4.09%, other phases mixed. Decoder-only revert: decoder +0.40%, parsing +4.61%; rejected. | Synthetic results do not establish an application speedup. |
| 250-client branch A/B | Eight fresh processes pass. Avatar p95 gap +0.39%, built logical work -0.50%, CPU approximately unchanged; CPU per million sends +0.87%. | No measured branch speedup. |
| Initial 250-client PGO | Two valid training processes and all eight evaluation processes pass. CPU -4.53%, CPU/work -4.96%, work +0.41%, avatar p95 -0.30%. | CPU-efficiency observation; cadence comparisons mixed. |
| 1500-client PGO | 0/8 strict passes due to cumulative setup retransmits; other 33 gates pass. Avatar p95 426.430 -> 416.005 ms, work +1.90%, CPU -0.58%, CPU/work -2.32%. | Diagnostic, not a validated win. |
| Main regression, 1500 clients | 0/8 strict, setup retransmits only. Avatar p95 411.755 -> 407.455 ms (-1.04%), work +1.59%, server CPU +0.43%, client CPU -30.32%; peak server RSS +6.00 MiB (+2.56%). | No failed run is promoted to evidence of a pass. |
| Fresh-main 1500 PGO, repeat | Each 0/8 strict due to setup retransmits; remaining gates pass. First: work +3.98%, CPU -0.16%, CPU/work -3.68%, avatar p95 -2.43%. Repeat: work +6.05%, CPU +0.51%, CPU/work -5.17%, avatar p95 -3.31%. | Descriptive, no validated series win. |
| Mixed 1500 PGO, 20-Hz Scene | 0/8. All server shutdown deadlines fail; Scene receipt about 1.20%, received Scene p95 bound 67.109 s, avatar gaps about 967-972 ms. The 250-client profile training also fails Scene receipt (about 54%) and one shutdown. | Overload; profile diagnostic only. |
| Mixed 750 PGO, 20-Hz Scene | 0/8 on Scene/quality and some shutdown gates. | Lower population alone did not resolve that heavy workload. |
| Scene relay investigation | At 750 clients, 5-Hz Scene receipt 22.770% -> 99.976%; candidate 2/2 strict, baseline fails shutdown/Scene. At 20 Hz: 5.681% -> 46.025%, 0/4. | Batching improves the supported 5-Hz case; 20 Hz remains overloaded. |
| 3-ms batching | At 20 Hz, 2/3-ms cases both about 46% receipt and all invalid. At 5 Hz all four pass, receipt 99.95-100%. | Small screening sample supports 3 ms for this workload. |
| Peer Scene quota | Quota 70 deliberately rejects work, about 11% receipt and no pass. Quota 1024 at 5 Hz: 1/2 strict (setup retransmit), 99.949% receipt, zero rejects. | Bounded queues prevent unbounded backlog; dropped work is never counted as a performance win. |
| 128-KiB/s upload cap | All four processes pass. Scene receipt 100% / 99.868%; avatar p95 476.100 / 494.425 ms (+3.85%), CPU +0.15%, zero upload rejects. | Two per arm; confirms admission compatibility, no speedup/image-capacity claim. |
| Final main pull, 750 clients | 2/2 per arm pass. Scene 100% / 99.943%, p95 bound 131.071 ms both. CPU 5.443 / 5.458 cores (+0.28%); avatar p95 487.180 / 505.725 ms (+3.81%). Paired CPU/gap direction reverses. | No consistent regression observed; small sample limits assurance. |
| Final-source PGO, 750 clients | Ordinary 3/4, PGO 4/4. Training fails on 42 and 11 cumulative setup retransmits; profile diagnostic only. CPU 5.444 -> 5.410 cores (-0.63%); avatar p95 558.090 -> 494.775 ms (-11.34%); Scene 99.952% -> 99.924%, p95 bound 131.071 ms both; applied avatar items +10.47%. | Not a qualified PGO win despite passing PGO evaluations. |


The final 750-client mixed workload uses 20-ms avatars, Unity-policy 60 FPS,
128 extra avatar bytes, 128-byte Scene at 5 Hz, voice off, CPU-only processing,
3-ms batching, quota 1024 and upload 131072 B/s per peer. It retains all clients
and 749 Scene senders, zero upload/Scene rejection, no image work and clean
exits. Earlier laptop runs use native Ryzen 9 5900HX Linux, server CPUs 2–9,
client 10–15, coordinator 0–1, four workers and avatar flush lanes 0, with AC/power
controls observed. Main's final measured server tree is
`693858532d09d00e67803caa9d9b8222e9605cf5`; merged main is `f30b248`.

At 750 clients, 20-Hz Scene offers 11.235 million logical recipient messages/s
and about 1.438 GB/s payload before framing/avatars. Moving qualification to
5 Hz changes offered load; it does not satisfy the failed 20-Hz requirement.

A separate fresh supported-pipeline run on source
`59c708a75070d0efe20ca5d207e8a22f42c1b691` qualifies **375 clients**: two valid
training processes, 4/4 evaluations per arm and all 51 gates. Scene receipt is
100%, received p95 bound 32.767 ms; avatar p95 is 127.150 → 111.590 ms
(−12.24%), CPU 3.160 → 3.174 cores (+0.45%), built work +3.34%, applied items
+3.38%, CPU/million built sends −3.57%. Every matched pair improves gap/work;
CPU direction varies. The fallback was unnecessary and package checksums pass.
This is historical qualification of that exact source; its profiles/package
cannot qualify later feature/source changes or failed 750-client training.

Earlier voice isolation already integrated in main remains a separate screen:
100-speaker receipt 97.9% misses its 99% gate; 1000 speakers reach 59.2% and
remain overloaded. Memory is bounded and the avatar-only control passes.
Historical packet counts do not pass the newer all-client unique-receipt screen
or establish voice/photo capacity for this PR.

## C# reference results and limit semantics

The frozen protocol-55 C# server source is
`b28f78f1d2885fd21c4e5ad53307fedf3662fba2`, running .NET 10.0.12 with native
adaptive workers, GC and tiered PGO. Every copied runtime file is hashed; each
comparison freezes its Rust client. C# receives no Rust worker/quota/upload
controls. These are sequential references, not interleaved cross-server A/B.

| Reference / 750 clients | Strict result | Scene receipt | Received Scene p95 bound | Avatar p95 gap |
| --- | --- | ---: | ---: | ---: |
| Initial mixed, 20-Hz Scene | 0/2; Scene/avatar quality and native drops fail | 15.108% | 1048.575 ms | 12470.935 ms |
| Initial mixed, 5-Hz Scene | 0/2; same failure classes | 71.188% | 1048.575 ms | 2631.810 ms |
| Final Rust client, 5-Hz Scene | Four-client preflight passes; 0/2 at 750 | 71.584% | 524.287 ms | 1729.560 ms |

Final reference runs retain population/sender coverage and clean exits, with
about 1.010M native unreliable-drop counter units/s. Seventeen Rust-specific
checks remain unavailable with reasons. Saved configuration is verified against
C#'s transport sidecar; exactly three unsupported health/persistent-admin
fixture fields are allowed to be omitted. No C# production changes are made.

C#'s eligible unreliable merge hold defaults to 3 ms. Its nominal 70-message
Scene check is ineffective for unreliable packets because that queue query
returns zero; it can affect Sequenced traffic. The real adaptive bulk-unreliable
queue evicts old packets (sampled per-peer caps 3118 initially, 2038 in the final
reference). Drop counters cover native transport, not exclusively Scene.

C# ordinary Scene after-fanout rate defaults to 0 (disabled). Its separate image
relay default is 200 Mb/s per sharer, with a 150% enforcement factor (300 Mb/s)
and two-second burst; cached replay has its own 200-Mb/s per joining-peer budget.
These egress controls differ from Rust's 128-KiB/s upload admission. Lower CPU
from load shedding and shorter latency among surviving messages are not capacity
wins. Two processes per rate, one host and unavailable telemetry limit inference.

## Correctness, provenance and review verification

| Scope | Verified result |
| --- | --- |
| Rust workspaces before feature split | Server 389 passed / 4 ignored; client 137 passed / 2 ignored; both formatting and all-targets Clippy with warnings denied pass; GPU console compiles/lints. |
| Feature split | Core/health 162 tests pass in each feature mode; core/console/health all-targets Clippy passes in both; live HTTP checks prove fields absent/present and clean shutdown. |
| macOS C-builder correction | Feature-enabled server workspace 389 passed / 4 ignored; locked console check, workspace Clippy and formatting pass. |
| Focused behavior | Packet/transport tests, 117 core tests and two live Scene tests during batching; eight upload/status tests; six fragment/lifecycle cases; queue retry, client receive, XML/environment and health checks pass in their recorded scopes. |
| Current Python suite | 103 tests, four platform skips, including 23 PGO provenance/admission/fallback/rejection guards and three macOS sampler tests. Exact full discovery passes with `PATH=/usr/bin:/bin`, excluding RTK. |
| Compiler/profile probes | Real GNU/musl generation → run → merge → use reproduce output. A C-constructor/Rust probe confirms cc 1.8.0 keeps C profiling flags off while Rust PGO works. Matched retention preserves a never-called function with zero counters and clean profile-use output. |
| Historical branch runner | Frozen PMC harness `6b8e7696aba8b6fa14da4971c8dbdebd996fabd1`; both default revisions build, harness hashes match Git, `--build-only` works. No new timings replace old data. |
| Release artifacts | All six archive checksums and selected-count PGO/shared-client hashes verify; no JSON or profiles ship. |

The Python PR job now discovers all `test_*.py`, restoring PGO rejection tests
lost when the expensive duplicate smoke job was removed. Its first hosted run
on `0353d09` exposed two tests requiring local RTK. Commit `ac79144` makes test
subprocesses invoke Python directly. The restored hosted Python job
[passes on ac79144](https://github.com/Toys0125/BasisRust/actions/runs/38203190158/job/114660858596).
The three Windows-only ownership tests remain skipped in that Linux verification.
Earlier smoke runs and earlier suite counts are historical checks,
not qualification of a later source revision.

CodeRabbit rounds fixed zero-baseline aggregation, major provenance/tooling and
minor status/missing-telemetry/debt-documentation findings. Some completed rounds
carry unverified-findings warnings. The all-target implementation and C-builder
reviews completed with zero findings; the matched-retention follow-up could not
run because account quota/seat policy blocked it. GitHub automatic CodeRabbit
review is skipped for this repository and is not approval. Greptile's historical
harness/action-pin and missing-PGO-tests findings are fixed. No unqualified
clean-review claim is made. Workflow actions now track upstream stable major
release tags (`@stable` for the Rust installer) instead of the earlier commit
hash pins. This changes the workflow action policy, not the recorded benchmark
compiler, source or results; PGO still uses matched Rust 1.99.0/LLVM tools.

### Retained failed native attempts

On `321c2c3`, the macOS instrumented server crashed with SIGSEGV before training
([run 38193234744](https://github.com/Toys0125/BasisRust/actions/runs/38193234744)).
No stack was captured or profile merged. Server `cc` 1.2.62 → 1.8.0 fixes
forwarding Rust profiling flags into Apple Clang's different profiling runtime
([upstream fix](https://github.com/rust-lang/cc-rs/pull/2003)).

On `d4511eb`, two 300-client training runs pass, but profile-use emits 2109
missing-function diagnostics and qualification aborts
([run 38194432106](https://github.com/Toys0125/BasisRust/actions/runs/38194432106)).
No mismatch/corruption diagnostics appear. Matching `-Clink-dead-code=yes` across
all three macOS server stages retains zero-count records; missing/mismatch guards
stay fatal. Diagnostic inspections are bounded to five symbols. Full native
`f39ed49` qualification above confirms successful training/use/evaluation after
both fixes. No failed artifact is promoted.

## Reproduction, evidence and limits

Use [the PGO guide](server-pgo.md) for current supported commands and release
flow; [the existing results index](results/README.md) and
[script-data harness](script-data-harness.md) document other portable runners.
Historical branch builds use `scripts/perf/review-branches.py --build-only`;
run its `--help` for matched PMC/timing controls. Old per-session controllers
under ignored `captures/` are evidence, not fresh-checkout reproduction scripts.

The 21 individual PR reports/handoff pages are consolidated here. Their original
full tables, exact source/client/config/binary hashes, commands and preliminary
attempts remain in [Git history at ac79144](https://github.com/Toys0125/BasisRust/tree/ac79144/docs/performance).
Raw benchmark JSON is excluded from the PR diff/packages; prior raw results stay
local under `captures/pr-pgo-implementation-20261011/raw-results/`. Historical
375-client evidence stays under `captures/pr-pgo-qualified-20261011/`; native
reports/packages under the `pr35-*-f39ed49` paths above. Git history is preserved.

A single loopback host, one observer and two/four processes per arm provide
observations rather than statistical proof. Avatar gaps are receiver update
intervals, not network RTT. Logical built work is not datagram throughput or
guaranteed receipt. Scene received-latency histograms exclude lost messages.
Always assess coverage, receipt, errors, CPU and work together. These results do
not certify internet networking, 20-Hz Scene, larger populations, voice, image
sharing or counter-free builds. New source/features/compiler/target/workloads
require fresh profiles and matched validation.
