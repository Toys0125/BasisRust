# Windows voice test: 1,000 users, October 5, 2026

The tested build cannot sustain this dense voice workload. Both voice runs kept all 1,000 clients authenticated and delivered some audio to every client, but delivery rate, packet cadence, and memory growth failed the capacity checks. The run with all 1,000 clients speaking stopped at the 24 GiB server memory limit after 98.5 seconds of measurement, before its requested 180-second window finished.

The [follow-up memory investigation](voice-memory-growth.md) confirms that the growth comes from unbounded dispatcher tasks, mostly avatar deltas queued behind slow voice relay. A bounded-admission causal probe removed the large growth, while exposing a separate ingress overload problem.

## Results

| Metric | Avatar-only baseline | 100 simultaneous speakers | All 1,000 speaking |
|---|---:|---:|---:|
| Voice measurement window | — | 120.015 s | 98.508 s; stopped early |
| Requested measurement window | 60 s | 120 s | 180 s |
| Voice source send rate | — | 4,341 packets/s | 9,760 packets/s |
| Nominal voice source rate at 20 ms | — | 5,000 packets/s | 50,000 packets/s |
| Observed / nominal source rate | — | 86.8% | 19.5% |
| Voice deliveries observed / expected fanout | — | 39.46% | 17.93% |
| Observer voice gap p50 / p95 | — | 47 / 126 ms | 102 / 418 ms |
| Observer maximum voice gap | — | 2.105 s | 32.184 s |
| Server CPU, logical cores | 6.13 | 12.82 | 12.95 |
| Client CPU, logical cores | 1.23 | 2.00 | 1.97 |
| Sampled peak server working set | 186.2 MiB | 24.320 GiB | 24.199 GiB |
| Server memory growth during measurement | — | 153.4 MiB/s | 168.9 MiB/s |
| Sampled peak client working set | 36.8 MiB | 86.8 MiB | 90.5 MiB |
| Total UDP payload egress | 522.5 MB/s | 229.1 MB/s | 240.2 MB/s |
| Total outgoing UDP datagrams | 423,779/s | 1,783,173/s | 1,815,745/s |
| Observer avatar gap p50 / p95 | 56.10 / 72.00 ms | 530.28 / 644.67 ms | 519.74 / 1,028.33 ms |
| Observer avatar peers stale over 500 ms at window end | 0 | 0 | 124 |

Voice gap percentiles are the lower edge of 1 ms histogram bins, measured between arrivals from the same source at client 0. These are arrival gaps, not end-to-end latency. The 1,000-speaker p99 lands in the overflow bin at **at least 10 seconds**; the maximum above retains its actual value.

Expected voice fanout is the count of successfully submitted source packets during the capture multiplied by 999 other clients. Missing in-window deliveries can reflect backlog, drops, or scheduling delay; the ratio is not a precise network loss rate. The source itself also missed its nominal cadence, especially with 1,000 speakers. This shared-host run does not isolate the server's maximum throughput from load-generator or loopback costs.

Server memory rose from 6.53 to 24.32 GiB during the 100-speaker measurement and from 7.95 to 24.20 GiB during the 1,000-speaker measurement. No steady memory plateau was observed. The guard samples every two seconds, so the measured stop slightly exceeds its 24 GiB limit. The guard was added after the 100-speaker run exposed the growth; that earlier run completed its full window without a cap.

## Delivery and audio validation

- The four-client preflight passed: 998 voice packets produced exactly 2,994 receiver deliveries, with no sequence gaps, duplicates, or reordered/ambiguous sequences at the observer. All captured samples matched the encoded source corpus and decoded.
- The baseline and both voice workloads kept 1,000 authenticated clients and 1,000 active avatar states in every collected readiness sample. The two voice runs collected 60 and 50 such samples respectively.
- Every client received voice in both large runs. All 1,000 clients sent voice in the full-speaking run, with at least 908 successfully submitted packets per client during its shortened measurement. The 10% speaker pool rotated among 133 clients; client 0 observed 100 distinct sources in that window.
- Voice send errors, skipped frames, malformed frame/header checks, and self-relayed packets were zero. Avatar send/decode errors and server protocol errors were also zero. Both small and large peer-ID formats were exercised.
- Four sampled streams per large voice run matched the encoded corpus byte-for-byte: 1,000 sampled packets in the 100-speaker run and 731 in the full-speaking run. All samples decoded with FFmpeg without errors, produced the expected number of 48 kHz PCM samples, and contained finite, non-silent audio. Sampled decode validation is not a decode of every packet at every receiver, and it does not assert uninterrupted audible playback.
- The full-speaking run failed completion checks and had 124 stale avatar peers at its shortened window end. Its partial measurements are retained as failure evidence, not a completed 180-second pass.
- All clients exited with code 0. Servers were stopped by the harness with the expected Windows console-control status 3221225786. Test server, client, and encoder processes were absent after cleanup.

The screening capacity checks require at least 95% of nominal voice source cadence, at least 99% of expected in-window voice fanout, and observer p95 voice gaps below 40 ms. Both large voice runs failed all three checks. These are stated investigation thresholds, not previously agreed product service-level requirements. The older server `droppedVoice` health field is unsupported, so its zero value is not evidence of loss-free voice delivery.

## Workload and evidence

Audio was found in **`C:\Users\mgsta\Desktop\Temp\Basis\AudioClips`**; the requested `BasisVR` directory did not exist. All 40 top-level Opus files were used. They are music clips, not recorded voice conversations. Preparation preserved the originals and encoded each full clip to 48 kHz mono, 32 kbit/s target Opus, with 20 ms frames. The prepared corpus contains 320,332 packets; the largest packet is 173 bytes. Encoding happened before any measurements, and identical prepared files were reused by the load runs.

The server source revision was **`09cf74f6168a02d3a8355dcfbed389fcd530ddb6`**, from the current checkout. This is not the later merged revision referenced by the separate October 4 performance report. Server code was not changed. The client was built from the same checkout with opt-in voice measurements added. Every run used the same frozen release binaries and hashes:

- Server SHA-256: `074ddd3e4aeb3139fd298edd72af930c8091f5a8fcad0e54e8fc5f367d2ef8fd`
- Client SHA-256: `96214d0ca8b7b6e7af6f633df5c2dda2f7289796a27debb3d567b403d9f79235`

Host: Windows 11 build 26100, Ryzen 7 9800X3D, 8 physical / 16 logical cores, about 64 GiB RAM. The server and one process simulating 1,000 headless clients ran on this machine over UDP loopback. Client Tokio workers=4; server Rayon defaults; CPU-only fixture; movement interval=20 ms; Unity avatar policy=60 fps; zero movement/voice jitter; zero positional spread; 25 m hearing distance; all participants within hearing range. P2P, reconnect, GPU, and native/application profiling were disabled. Server per-pair avatar diagnostics were disabled; client sender/observer diagnostics were enabled.

Each large run used 45 seconds of warmup after readiness. CPU and bandwidth rates use endpoint counter deltas across 58.3, 118.7, and 98.5 seconds within their respective observer windows. One CPU core means one process CPU-second per elapsed second. UDP rates include avatar and voice payload before IP/link overhead and do not measure physical NIC traffic. Memory peaks are maxima of sampled working sets, not process high-water marks. No 1,000-way audio playback/mixing, Unity client, headset, physical-network, or remote-host test was performed. This is one run per condition, without repeat-based confidence estimates.

Raw evidence is retained locally under **`captures/voice-1000-20261005/`**: frozen binaries, source/encoded audio hashes, encoded clips, exact launch commands, effective configs, health/CPU/memory CSVs, per-client voice counters, observer sequence/gap data, received Ogg samples, and per-run validation JSON. [Machine-readable results](results/voice-1000-20261005-summary.json) contain all checks and measurements.

## Reproduction and next investigation

Build the separate server and client workspaces with `cargo build --locked --release --jobs 2`. From the repository root, run:

```powershell
python scripts/perf/run-windows-voice-workload.py --audio-folder 'C:\Users\mgsta\Desktop\Temp\Basis\AudioClips' --output captures/voice-1000-repeat
```

The reusable suite now applies a 24 GiB server working-set limit to both voice conditions and monitors warmup. For an exact uncapped 100-speaker control, use the [lower-level runner](../../scripts/perf/run-windows-avatar-workload.py) with `--max-server-working-set-mib 0`; its other settings and frozen binary paths are in the captured `commands.txt`. The [analyzer](../../scripts/perf/analyze-windows-voice.py) verifies byte matching, sampled decoding, delivery counters, and capacity checks. Three focused Rust diagnostic tests, Python compile checks, formatting, and the live preflight passed. A separate four-client guard smoke test with a deliberately tiny limit verified early warmup, controlled stop, clean client exit, and analysis of a single-sample partial capture; it is not included in the capacity measurements.

The [follow-up investigation](voice-memory-growth.md) identified the retained memory in waiting dispatcher task futures and confirmed the cause with queue counts, drain measurements, and a bounded-admission experiment. The next implementation should bound media admission, keep avatar delta application prompt, expire stale voice frames, and reduce per-recipient UDP calls, then repeat with a separate load-generator host.
