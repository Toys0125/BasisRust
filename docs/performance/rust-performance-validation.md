# Rust performance validation

This branch adds bounded Scene relay admission and 3-ms batching, a configurable
128-KiB/s per-peer upload budget before fanout, fragment lifecycle fixes after
merging main, native admission/image telemetry, reproducible benchmarks and an
opt-in qualified PGO build. It also retains branch/decoder changes whose measured
benefit was mixed. No broad branch-speedup or universal capacity claim is made.

Latest fetched main is `f30b248329b213278153bee09995987a66b1a16d`; it is merged.
The previously measured final server tree was `693858532d09d00e67803caa9d9b8222e9605cf5`.
All A/B comparisons kept each experiment's workload and client fixed. Separate
experiments changed populations or Scene rates explicitly. Failed runs are
retained; setup retransmits still fail the absolute-zero gate. Percent changes
below are candidate relative to its matched baseline, not cross-experiment gains.

## Rust benchmark history

| Experiment | Strict results and user-facing outcome | Interpretation |
| --- | --- | --- |
| [Desktop branch microbench](branch-optimization.md), [laptop repeat](branch-optimization-laptop.md) | Desktop mixed. Laptop: 864 samples, matched outcomes and full coverage; decoder +1.20% time, parsing -4.09%, other phases mixed. Decoder-only revert: decoder +0.40%, parsing +4.61%; rejected. | Synthetic results do not establish an application speedup. |
| [250-client branch A/B](branch-client250-20261008.md) | Eight fresh processes pass. Avatar p95 gap +0.39%, built logical work -0.50%, CPU approximately unchanged; CPU per million sends +0.87%. | No measured branch speedup. |
| [Initial 250-client PGO](pgo-client250-20261008.md) | Two valid training processes and all eight evaluation processes pass. CPU -4.53%, CPU/work -4.96%, work +0.41%, avatar p95 -0.30%. | CPU-efficiency observation; cadence comparisons mixed. |
| [1500-client PGO](pgo-client1500-20261008.md) | 0/8 strict passes due to cumulative setup retransmits; other 33 gates pass. Avatar p95 426.430 -> 416.005 ms, work +1.90%, CPU -0.58%, CPU/work -2.32%. | Diagnostic, not a validated win. |
| [Main regression, 1500 clients](main-regression-client1500-20261009.md) | 0/8 strict, setup retransmits only. Avatar p95 411.755 -> 407.455 ms (-1.04%), work +1.59%, server CPU +0.43%, client CPU -30.32%; peak server RSS +6.00 MiB (+2.56%). | No failed run is promoted to evidence of a pass. |
| [Fresh-main 1500 PGO](pgo-main-client1500-20261009.md), [repeat](pgo-main-client1500-20261009-rerun.md) | Each 0/8 strict due to setup retransmits; remaining gates pass. First: work +3.98%, CPU -0.16%, CPU/work -3.68%, avatar p95 -2.43%. Repeat: work +6.05%, CPU +0.51%, CPU/work -5.17%, avatar p95 -3.31%. | Descriptive, no validated series win. |
| [Mixed 1500 PGO, 20-Hz Scene](pgo-mixed-client1500-20261010.md) | 0/8. All server shutdown deadlines fail; Scene receipt about 1.20%, received Scene p95 bound 67.109 s, avatar gaps about 967-972 ms. The 250-client profile training also fails Scene receipt (about 54%) and one shutdown. | Overload; profile diagnostic only. |
| [Mixed 750 PGO, 20-Hz Scene](pgo-mixed-client750-20261010.md) | 0/8 on Scene/quality and some shutdown gates. | Lower population alone did not resolve that heavy workload. |
| [Scene relay investigation](scene-receipt-investigation-20261010.md) | At 750 clients, 5-Hz Scene receipt 22.770% -> 99.976%; candidate 2/2 strict, baseline fails shutdown/Scene. At 20 Hz: 5.681% -> 46.025%, 0/4. | Batching improves the supported 5-Hz case; 20 Hz remains overloaded. |
| [3-ms batching](scene-batch3-client750-20261010.md) | At 20 Hz, 2/3-ms cases both about 46% receipt and all invalid. At 5 Hz all four pass, receipt 99.95-100%. | Small screening sample supports 3 ms for this workload. |
| [Peer Scene quota](scene-peer-limit-client750-20261010.md) | Quota 70 deliberately rejects work, about 11% receipt and no pass. Quota 1024 at 5 Hz: 1/2 strict (setup retransmit), 99.949% receipt, zero rejects. | Bounded queues prevent unbounded backlog; dropped work is never counted as a performance win. |
| [128-KiB/s upload cap](client-upload-limit-client750-20261010.md) | All four processes pass. Scene receipt 100% / 99.868%; avatar p95 476.100 / 494.425 ms (+3.85%), CPU +0.15%, zero upload rejects. | Two per arm; confirms admission compatibility, no speedup/image-capacity claim. |
| [Final main pull, 750 clients](final-main-client750-20261010.md) | 2/2 per arm pass. Scene 100% / 99.943%, p95 bound 131.071 ms both. CPU 5.443 / 5.458 cores (+0.28%); avatar p95 487.180 / 505.725 ms (+3.81%). Paired CPU/gap direction reverses. | No consistent regression observed; small sample limits assurance. |
| [Final-source PGO, 750 clients](pgo-final-client750-20261011.md) | Ordinary 3/4, PGO 4/4. Training fails on 42 and 11 cumulative setup retransmits; profile diagnostic only. CPU 5.444 -> 5.410 cores (-0.63%); avatar p95 558.090 -> 494.775 ms (-11.34%); Scene 99.952% -> 99.924%, p95 bound 131.071 ms both; applied avatar items +10.47%. | Not a qualified PGO win despite passing PGO evaluations. |

The mixed final workload is 750 clients, 20-ms avatar updates, Unity-policy 60-FPS
poses, 128 extra avatar bytes and 128-byte Scene payloads at 5 Hz, voice off,
CPU-only processing, 3-ms batching, quota 1024 and upload 131072 B/s per peer.
Every final 750-client process retained all clients/all 749 Scene senders,
zero upload/Scene rejection, zero image work/drop counters and clean exits.
Received Scene latency excludes lost messages; receipt must be considered with it.

Historical [voice isolation checks](voice-isolation.md), already integrated in
main, also remain available: original server 284 tests passed (one ignored),
client 98 passed, Clippy and release/format checks passed. Dense 100-speaker
receipt reached 97.9%, below the 99% capacity screen; 1000 speakers remained
overloaded (59.2%). Memory stayed bounded; the avatar-only control passed.
Those older packet-count observations cannot pass the newer all-client unique
receipt screen and do not qualify PGO, voice or photo capacity for this PR.

## C# reference gates

[C# with the final Rust client](csharp-final-client750-20261011.md) passes a
four-client preflight, then fails both fresh 750-client 5-Hz runs. Scene receipt
median 71.584% (gate >=95%), received p95 bound 524.287 ms, avatar p95 median
1729.560 ms and native unreliable drops about 1.010M counter units/s. Full
population/sender coverage and clean exits do not offset dropped work. C# keeps
its native worker/queue policy and .NET tiered PGO; 17 Rust-specific unavailable
checks are reported unavailable. This sequential reference is not interleaved
cross-server A/B and does not establish general C# capacity.

## Correctness and tooling verification

Historical focused checks covered branch packet/transport behavior, 117 server
core tests and two live client Scene tests during batching, upload/image
admission, six fragment/lifecycle cases, nonblocking queue retry, client fragment
receive and health telemetry. Reports retain their exact scopes. Later
measurement-only commits did not claim new Rust unit tests.

Pre-feature-split Rust 1.99.0 workspace verification: server **389 passed, four ignored**;
client **137 passed, two ignored**; zero failures. Both workspaces pass formatting.
Final Python benchmark suites: **93 tests, four platform skips**, including 16
PGO admission/provenance/fallback tests. Both workspaces pass all-targets Clippy
with warnings denied after fixing four new-Rust test/benchmark-only lints.
The optional GPU-feature console compiles and passes all-targets Clippy with
warnings denied. Eight focused upload/status tests
pass again after the reviewed status-formatting change. [Fresh supported-pipeline qualification](pgo-qualified-client375-20261011.md):
375 clients, 2/2 training and 4/4 evaluations per arm pass all 51 gates; fresh
nonempty profiles also pass. Scene receipt is 100% in both arms, p95 bound
32.767 ms. PGO avatar p95 improves 12.24%, applied items rise 3.38%, built work
rises 3.34%; CPU rises 0.45%, CPU/million built sends falls 3.57%. All four pairs
improve p95/work, CPU direction varies. The 250 fallback was unnecessary.
This validates optional PGO at 375 clients, not the failed 750-client training.
The generated package extracts with all checksums verified and no JSON/profiles.

Two CodeRabbit review rounds covered changed server and benchmark tooling.
Three major tooling findings and a minor status-formatting finding were fixed;
follow-up minor missing-telemetry reporting and upload-debt documentation issues
were also addressed. The first server review completed with an unverified-findings
warning; no unqualified clean-review claim is made. Ordinary releases remain the default; the supported
[PGO pipeline](server-pgo.md) rejects failed training before merging profiles,
retries lower populations independently and packages only a fully passing count.

Raw benchmark JSON is excluded from the final PR diff and packages. Prior raw
results are preserved locally under
`captures/pr-pgo-implementation-20261011/raw-results/`; reports provide the
reviewable summaries. Historical Git commits were not rewritten.

## PR review follow-up — 2026-10-11

GitHub CI on `8b087f6` passes formatting, Python benchmark validation, Linux
server/client/GPU tests and Clippy, Windows client checks and storage persistence.
The hosted PGO smoke completes both instrumented training processes and all four
ordinary/PGO evaluations with all gates passing at four clients; its artifact
attests smoke-only mode. This checks the full PGO path, not workload capacity.

Greptile identified a broken historical fresh-build path and mutable workflow
action references. The historical runner now overlays the exact frozen PMC
harness from `6b8e7696aba8b6fa14da4971c8dbdebd996fabd1`, logs that revision and
supports `--build-only`. Both default historical revisions build successfully;
both recorded harness hashes match Git. New benchmark timings were not collected
or substituted into historical measurements. All nine PGO workflow action uses
are pinned to full GitHub commit hashes; JavaScript actions declare Node 24.
The 93 Python tests still pass with four platform skips; workflow YAML and pins
validate. Server/client and PGO measurement inputs remain unchanged, so the
375-client qualification still applies to their exact measured code.

Focused CodeRabbit follow-up: the historical runner review completes with zero
findings; the pinned workflow review completes with zero findings and an
unverified-findings warning. No unqualified clean-review claim is made.

## Benchmark telemetry feature and smaller defaults — 2026-10-11

`benchmark-validation` is opt-in on the core/console crates. Normal builds have
no Scene diagnostic counter storage/reference-counted counter handles and no
upload diagnostic atomics; compile-time size assertions enforce that separation.
Actual per-peer Scene permits and upload token/debt admission remain active.
Snapshots, detailed status and extended health `sceneBatch` / `uploadBudget`
require the feature; unavailable fields are omitted rather than filled with zero.
Other existing health telemetry remains available. Core unit tests also enable
counters to retain their admission/frame-conservation assertions.

The supported PGO driver enables identical validation features in all server
stages, checks Cargo feature fingerprints and reports that requirement. Both
ordinary qualification and PR smoke default to 300 clients with 300 -> 150 -> 100
fallback; counts below 100 are rejected. Smoke still uses 3-second warmup,
10-second measurement windows, two training processes and four evaluations,
and cannot publish. Full qualification retains 45/60-second timings and eight
evaluations. Workload, receipt/provenance/error gates and fallback isolation
are unchanged.

Focused checks: Rust 1.99.0 core/health **162 tests pass in each feature mode**;
core/console/health all-targets Clippy with warnings denied passes in both modes;
default and feature-enabled console builds and server formatting pass. Live
HTTP checks with extended metrics and 3-ms batching confirm both fields are
omitted without the feature and present with it, with clean shutdowns. Python
**94 tests, four platform skips** pass, including 17 PGO guard/default tests;
workflow YAML/default wiring validates. A focused CodeRabbit review returns
zero findings with an unverified-findings warning.

CI on `be56000` passed all tests, including the former four-client PGO smoke,
before this change. New-head CI must validate the changed feature-enabled PGO
build and 300-client smoke. Earlier 375-client measurements and its package
remain evidence for their exact recorded source, not qualification of this
changed source; regenerate profiles. No new capacity or speedup claim is made.

## Automatic PGO release targets — implementation follow-up

Marcus requested PGO for every released server target using GitHub-hosted
runners. The standard build matrix now runs full per-target training and strict
A/B qualification before staging any server binary. Native support covers GNU
and musl Linux x64/ARM64, Windows x64 MSVC and macOS ARM64. Profiles cannot cross
architectures, operating systems or GNU/musl targets. Docker continues consuming
its static musl archives, which must now qualify independently. The distributed
servers retain the validation feature; local plain Cargo releases remain ordinary.
No self-hosted runner is required, and no ordinary-server fallback is published.

The driver discovers host LLVM tools/executable suffixes, checks target/host
compatibility, exports the validated server and exact shared client for the
existing package layout, and adds build-target/compiler/executable hashes to
Markdown. macOS process CPU/RSS sampling uses BSD `ps`; explicit macOS CPU sets
are rejected. Windows Ctrl+Break now follows normal server drain/persistence so
training profiles can flush. PGO adds a clean native server-exit gate; generic
historical benchmark exit allowances do not qualify a new release artifact.

Local checks: **100 Python tests, four platform skips**, including 20 PGO guards
and three macOS sampler tests, pass. Eight console tests, console all-targets
Clippy with warnings denied (validation feature), and server formatting pass.
Real Rust 1.99.0 GNU and musl probes both instrument, execute, merge fresh
profiles, build with profile use and reproduce output without diagnostics;
these probes are compiler/tool checks, not application capacity measurements.
Package tests verify exact validated binary/client export, checksums and absence
of JSON/profiles. All six shipping targets run the full hosted release preview matrix for
pipeline/build-workflow/fixture PR changes; workflow YAML/pins validate. Focused CodeRabbit review completes
with zero findings. Native ARM64/Windows/macOS application qualification and the
full automatic release packaging still require their new GitHub runs; previous
Linux hosted smoke and 375-client results are historical evidence only.

The configured GitHub token rejected a manual build dispatch with HTTP 403
(`Resource not accessible by personal access token`). The full six-target
release preview therefore runs automatically on PR pipeline/workflow changes,
using the same `build.yml` packaging path. Standalone `server-pgo.yml` is hosted
manual qualification; redundant short PR smoke jobs were removed. PR preview
uploads build artifacts only, without release attachment or Docker publication.

The final workflow-only review returns zero findings with an unverified-findings
warning; no unqualified clean-review claim is made for that round.

## macOS PGO mixed-runtime correction

The first full macOS ARM64 preview on `321c2c3` built ordinary/client/instrumented
binaries, then the instrumented server exited with SIGSEGV before the first
training workload. Its Markdown report retains the failure; no profile was
merged and no package uploaded. The stack was not captured, so the exact crashing
frame is not claimed. This matches the upstream C/Rust instrumentation ABI issue:
[cc-rs #2003](https://github.com/rust-lang/cc-rs/pull/2003) stops forwarding Rust
PGO generation/use flags to Clang, whose profiling format can differ from Rust's
bundled LLVM. mimalloc and zstd use this C builder.

The server lockfile updates `cc` 1.2.62 -> 1.8.0 with its required `shlex` and
`find-msvc-tools` updates. Rust profiling stays enabled; C dependencies keep
ordinary compilation across baseline, generation and use. Failure artifacts now
retain plain-text compiler/server/client logs, excluding JSON and raw profiles.
The macOS retry must still complete clean training, merge/use, strict A/B and
packaging; the upstream match is a diagnosis hypothesis until that native run.

Local correction verification: all **389 server workspace tests pass, four
manual benchmarks ignored**, with the validation feature. The locked console
check and workspace all-targets Clippy with warnings denied pass. A real Clang
C-constructor/Rust probe built with generation and use flags retains Rust PGO,
records actual C compiler commands without inherited profiling flags, merges
fresh Rust profiles and reproduces output with clean exit. This verifies the
C-builder fix locally; it does not replace native macOS application validation.

The focused correction review completes with zero findings.
