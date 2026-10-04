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

The portable runner in [`run-avatar-workload.py`](../../../scripts/perf/run-avatar-workload.py) uses the same core workload policy, but its checked-in XML fixtures intentionally are not byte-identical to the historical configs. Historical server config hash: `35c48042…`; historical client config hash: `043b8b7f…`. Current fixture hashes: server `c27d64a1…`, client `608c21fa…`. The fixtures use a dedicated `avatar-bench-only` loopback credential instead of historical `default_password`, descriptive server/loopback host settings, a 1,500 client-count default, and normalized XML serialization. Server performance settings (20 ms default interval, base multiplier 1, increase rate 0, 2.5 slowest-send rate, uplink delta, compression/keyframe settings) match the historical harness overrides; remaining complete template values are retained. The portable runner defaults to no CPU pinning and does not itself sample `/proc` CPU windows; pass `--server-cpus 0-7 --client-cpus 8-15` to match historical affinity. Use the original published JSON for the exact historical A/B settings and outcomes; the portable runner is a safe fresh workload reproduction, not a byte-identical recreation of those paired captures.

Historical pose generation was synthetic and deterministic, not captured from a Unity runtime. “Unity-policy” means the source-backed send cadence/packing policy exercised by this Rust workload. No result here claims identical Unity physics, scheduling or pose entropy.

## Fresh-checkout workload

From the repository root, install Rust/Cargo 1.95 or newer, a native build
compiler/linker, and Python 3. The script uses Python's standard library; no pip
packages or `ffmpeg` are needed for this avatar-only workload. Linux `taskset`
is required only when affinity flags are supplied. Keep ports 4296/UDP and
10666/TCP free, or select unused `--port` / `--health-port` values.

```sh
CARGO_BUILD_JOBS=2 cargo build --locked --release --manifest-path BasisRustServer/Cargo.toml -p basis-server-console
CARGO_BUILD_JOBS=2 cargo build --locked --release --manifest-path BasisRustClient/Cargo.toml
python3 scripts/perf/run-avatar-workload.py --output captures/avatar-workload-A
```

The runner selects these built binaries by default, uses the checked-in
[server](../fixtures/avatar-1500-server.xml) and
[client](../fixtures/avatar-1500-client.xml) fixtures, waits for all 1,500 clients
to authenticate/become active, warms up for 15 seconds and measures for 60
seconds. It runs loopback only. Output directories must not already exist.
For a host with the corresponding allowed CPUs, add
`--server-cpus 0-7 --client-cpus 8-15`; otherwise choose valid affinity sets and
record them. Use `--help` to select binaries (`--server` / `--client`), client count and durations.
The fixtures are fixed by the runner; it has no config-selection flag. Clear
server configuration environment overrides (including `Password` and
`BASIS_SERVER_PASSWORD`) before running so they do not override the fixture or
prevent the matching client credential from connecting.

Outputs under the ignored capture directory include `workload.json` (settings and
binary/config hashes), `commands.txt` (commands), `server.log`, `client.log`,
`ready-health.json`, `readiness.csv`, `observer.csv`, `observer.sender.csv`,
`server-pairs.csv` and `server-pairs.global.csv`. Check that the files exist and
inspect coverage, decode/send errors, applied-update gaps and pair diagnostics;
the final “completed” message alone does not validate CSV contents or process
exit codes. Server/client CPU samples must be collected separately over matched
observer windows; this runner does not sample them.

For A/B comparisons, supply `--server` for each candidate, retain the
same client binary/configs, and use unique output directories. Repeat in ABBA
or forward/reverse pairs on the same host with the same clients, durations,
affinity and runtime settings. Record source/binary/config hashes, compare
matching measurement windows, and inspect individual runs before aggregating.
Report CPU together with update gaps, throughput, coverage and errors; a
speedup obtained by dropping work is not an improvement. State the number of
repeats and host/workload limits. Current fixture-based runs do not reproduce
unavailable historical captures; see [artifact availability](../artifact-availability.md).
