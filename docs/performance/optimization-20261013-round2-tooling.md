# Second-pass benchmark guard review — 2026-10-13

Independent OpenCode reviews using Muse Spark found two remaining comparison gaps: truncated successful micro logs could still provide too few samples, and an audio corpus changed at the same folder path could pass input checks. Both are fixed in the [comparison tool](../../scripts/perf/compare-optimization-suite.py).

Admission cases now require exactly five samples with identities 0–4. Snapshot strategies require six elapsed values. GPU phase/mode cases require exactly eight samples with round identities 0–7. Duplicate extra samples are rejected as well as missing/repeated identities. Benchmarks without internal rounds retain their existing work, completion, and error checks.

Every live capture must have a manifest with a nonempty audio hash map. All variants and tags must use identical clip names and hashes. Missing manifests, empty maps, mixed presence, and any corpus change raise before aggregation. Historical captures without this evidence are preserved but cannot be retrospectively certified by reading today's corpus; no manifests were fabricated or backfilled.

These checks supplement the existing timeout, frozen executable identity, measured delivery/voice eligibility, and zero-lost native profile guards. Failed captures remain untouched and are rejected rather than filtered into favorable comparisons.

Focused checks reject shortened admission/snapshot logs, missing or duplicated GPU rounds, extra duplicate samples, swapped clip hashes, missing/empty manifests, and frozen/live audio mismatch. Complete membership, client-packing, and bulk-encoder cohorts still parse all 37 micro subcases and four eligible live workloads. Raw files were verified unchanged during analysis.

Ignored focused checks and replay JSON remain in `captures/optimization-followups-20261013/round2-tooling/`. Muse Spark review text remains in `captures/optimization-followups-20261013/receiver-index/benchmark-review.md`; server and voice findings are stored beside it. These tooling fixes do not claim a runtime speedup.

## Paired native profile guards

The second review found that matching binaries and zero lost samples did not verify paired workload settings, corpus identity or profiler-driver validation. The comparison now requires both recordings and reports, checks legacy suite status, and verifies complete, identical custom voice profile inputs and corpus hashes against the live captures. Failed profiler, timeout, avatar/voice or capacity status rejects the pair; custom voice captures cannot fall back to legacy avatar-only handling.

All 24 focused profile injections pass. The membership, packing, residual-writer and receiver-index cohorts replay successfully, and the new voice cohort passes with all 37 micro subcases and 16 eligible live captures. Historical metadata was not invented or backfilled. Profile heatmap colorbars now identify their 35% saturation cap.


The custom profiler now sets and records 16 Tokio/Rayon workers explicitly. Comparison requires that declaration to match the recorded worker environment, and rejects automatic/missing settings. Original automatic-worker voice profiles are preserved; a fresh fixed-worker pair replaces them without backfilling metadata.
