# Published 1,500-peer result summaries

These compact machine-generated JSON summaries are copied from the local experiment outputs and contain the measured per-run counters, hashes and peer distributions. Raw CSVs, binaries and perf recordings remain local and ignored. The JSON files were checked to contain no machine-local absolute paths; no measured values were changed.

| Summary | Comparison/source | Workload and role |
|---|---|---|
| [`i10-dense3-summary.json`](i10-dense3-summary.json) | I8 control source `1dfb900` vs I10 lazy-quality candidate `ba38df8`; the paired binary hashes are recorded in every run row | Four-run forward/reverse 1,500-colocated comparison plus a supplemental fixed-band 600-peer mixed-quality screen. The matched client binary hash is `b8818d7a…` and config hashes are included in the JSON. |
| [`i12-summary.json`](i12-summary.json) | I10 control source `ba38df8` vs I12 read-channel candidate `3f4bee3`; paired binary hashes are recorded in every run row | Four-run forward/reverse 1,500-colocated comparison. Client binary `896734c3…`; server/client config hashes are included in the JSON. |
| [`receiver-cycle-regression.json`](receiver-cycle-regression.json) | Control source exact PR head `3f4bee3f41ef0de948039e0333af44f7d15138e7` vs scheduler/CI-fix source now committed as `7c43786` | One unreplicated forward-order correctness regression screen. Full control and candidate binary/config hashes are recorded in each row. |

Historical matched settings were 1,500 colocated clients, 20 ms advertised server interval, zero movement jitter/drift, 60 FPS Unity-policy frame accumulator, deterministic synthetic valid pose with root yaw and nine body-rotation channels, voice/P2P disabled, server BSR profiling disabled, 15 s warmup, 60 s observer window, server affinity `0-7`, client affinity `8-15`, all 1,500 active/progressing senders, and 1,499 expected observer peers. The JSON retains exact sample endpoints, CPU windows, input/output counters, observer/error counts and hashes. Built logical sends count recipient avatar items before transport submission; encoded post-bundle entries are separate. Observer gaps are applied-update intervals at one receiver, not end-to-end movement latency. End age/stale is phase-dependent at the final snapshot.

The summarized order-paired outcomes were:

| Candidate vs accepted control | Server CPU mean | Built logical avatar work/s | Applied observer p50 / p95 | Additional tradeoff |
|---|---:|---:|---:|---|
| I10 lazy lower-quality materialization (`ba38df8` vs `1dfb900`) | 253.33% → 279.54% (+10.4%) | 2.884M → 3.873M (+34.3%) | 25.8% / 23.8% lower gaps | 600-peer supplemental fixed-band mixed-quality screen measured 1–2% less built work and 1–3% longer gaps with flat CPU; not a full-scale mixture. |
| I12 bounded 8-byte `read_channel` path (`3f4bee3` vs `ba38df8`) | 278.61% → 282.44% (+1.4%) | 3.897M → 4.129M (+5.9%) | 6.0% / 5.5% lower gaps | Higher total CPU, lower gaps in both run orders. |

Both candidates are higher-total-CPU cadence/throughput wins for this synthetic dense workload, not CPU reductions. The complete per-run rows, distributions, endpoints and hashes are in the linked JSON files; these headline values do not replace those rows.

The portable runner in [`../run-avatar-workload.py`](../run-avatar-workload.py) uses the same core workload policy, but its checked-in XML fixtures intentionally are not byte-identical to the historical configs. Historical server config hash: `35c48042…`; historical client config hash: `043b8b7f…`. Current fixture hashes: server `c27d64a1…`, client `608c21fa…`. The fixtures use a dedicated `avatar-bench-only` loopback credential instead of historical `default_password`, descriptive server/loopback host settings, a 1,500 client-count default, and normalized XML serialization. Server performance settings (20 ms default interval, base multiplier 1, increase rate 0, 2.5 slowest-send rate, uplink delta, compression/keyframe settings) match the historical harness overrides; remaining complete template values are retained. The portable runner defaults to no CPU pinning and does not itself sample `/proc` CPU windows; pass `--server-cpus 0-7 --client-cpus 8-15` to match historical affinity. Use the original published JSON for the exact historical A/B settings and outcomes; the portable runner is a safe fresh workload reproduction, not a byte-identical recreation of those paired captures.

Historical pose generation was synthetic and deterministic, not captured from a Unity runtime. “Unity-policy” means the source-backed send cadence/packing policy exercised by this Rust workload. No result here claims identical Unity physics, scheduling or pose entropy.
