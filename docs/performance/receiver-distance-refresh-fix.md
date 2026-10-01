# Per-receiver distance-cache refresh fix

Branch: `fix/receiver-distance-cache-refresh`.

The global 500 ms refresh flag only reached the receiver slice selected when it became due. At 32 slices and approximately one-second receiver cycles, some receivers retained stale distance-based quality tiers and intervals for long periods.

[Receiver tracking](../../BasisRustServer/crates/basis-server-core/src/avatar_sync.rs) now stores one refresh timestamp per receiver alongside its sender map. Each receiver checks the existing 500 ms interval using the tick's monotonic time when its build runs. An overdue receiver refreshes all eligible sender pairs on that visit. New pairs still initialize from their current positions; disconnecting a receiver removes its timestamp with its tracking state. There is no added timestamp per sender pair and no extra clock read in the loop.

This corrects quality selection. It preserves receiver slicing, distance thresholds, interval encoding, bundle/delta compression and the configured refresh period. Receivers still wait for their scheduled slice; it does not guarantee delivery every 500 ms.

## Regression checks

- The new per-receiver timing test failed on the old code: High remained cached when Medium was required. It passes with the fix.
- Tests cover 499/500 ms timing, independent receivers, all four tiers in both directions, recreated receiver state, and every receiver in a 32-slice cycle. The slice regression also checks transmitted quality and advertised intervals.
- All 37 `basis-server-core` tests pass. Release console build, formatting and whitespace checks pass. [Exact commands and outcomes](../../captures/receiver-distance-refresh-20260930/checks.json).

## 2,000-client moving validation

One 120-second window after a 30-second warmup, using the previous four groups of 500 at 0/15/30/45 m, a 7 m sinusoidal movement amplitude and a 60-second period. Uplink remains fixed at 20 ms; thresholds remain 10/20/40 m. Same client binary, LZ4/delta settings, CPU affinities, loopback transport and internal BSR instrumentation as the baseline. Native CPU sampling used 20 seconds per process instead of the baseline's 60 seconds. The manifest records binary/configuration hashes and the server patch hash; that patch was checked against the final tested source.

| Check | Result |
|---|---:|
| Health population | 2,000 connected and active in every sample |
| Observer coverage | 1,999 / 1,999 remote peers |
| Decode, unapplied delta, malformed and sequence errors | 0 |
| Client send errors / server protocol errors | 0 / 0 |
| Group 0: fixed High | 499 peers, each with 0 changes |
| Group 1: High/Medium/Low | 500 peers, each with 8 changes |
| Group 2: fixed Low | 500 peers, each with 0 changes |
| Group 3: Low/Very Low | 500 peers, each with 4 changes |
| Total quality changes | 6,000, matching two complete motion periods |
| Final position/tier disagreements | 0 |
| Incoming updates/s | 100,000 |
| Whole-window server / client CPU | 324.3% / 116.6% |
| Observer applied-update gap p95 | 1092.73 ms |
| Mean receiver-build / tick wall time | 20.01 / 31.80 ms |

100% CPU means one logical CPU. This single shorter validation establishes observed tier transitions and coverage; it is not a repeated performance comparison or evidence of a speedup. The roughly one-second receiver cycle and update gaps remain. One observer decodes all poses; the other 1,999 clients retain the baseline socket filtering. This is a synthetic headless test, not Unity rendering or WAN capacity testing.

The server and separately mapped observer socket had zero sampled UDP drop increases. Filtered-client aggregate drops are not treated as packet-loss measurements. Both load processes and both native CPU samplers exited successfully, and all test processes are stopped.

[Tier trace chart](../../captures/receiver-distance-refresh-20260930/tier-refresh.png) compares the first 120 seconds of both historical moving baselines with this validation. The chart uses five-second diagnostics of pair baseline quality; per-peer applied transition counts above independently verify the observer's received tiers.

[Structured results](../../captures/receiver-distance-refresh-20260930/comparison.json) · [Transition verification](../../captures/receiver-distance-refresh-20260930/verification.json) · [Run manifest](../../captures/receiver-distance-refresh-20260930/01-moving/manifest.json) · [Runner](../../captures/receiver-distance-refresh-20260930/run-validation.py) · [Server patch](../../captures/receiver-distance-refresh-20260930/server-source.patch) · Prior baseline report: local `docs/performance/mixed-quality-2000-profile.md` (not published)

Capture links reference ignored local experiment artifacts and are unavailable in a fresh checkout.
