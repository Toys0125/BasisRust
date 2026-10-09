# C# voice comparison on the Windows host

The frozen protocol-v55 C# server did not deliver 100% of expected voice fanout in either 1,000-user voice workload. With 100 simultaneous speakers it received 62.3%; with all 1,000 speaking it received 8.85%. Both runs completed their requested windows with all clients connected, all clients receiving voice, zero client voice send/parse errors, and sampled Opus matching and decoding from the source corpus. Both fail the voice capacity screen and avatar correctness/continuity checks.

The C# reference is `b28f78f1d2885fd21c4e5ad53307fedf3662fba2`, the same frozen protocol-v55 build used in earlier avatar comparisons, running on .NET 10.0.12 with server GC. Its published files were checked against the earlier binary manifest before being copied for these runs. This result applies to that build, rather than every C# version.

## Workload and results

The tests use the same Ryzen 7 9800X3D Windows 11 host with 16 logical processors, frozen Rust headless client, 40 preencoded local audio clips, CPU-only server fixture, four client workers, and 20 ms voice frames as the [Rust dedicated-thread tests](voice-isolation.md). All 1,000 clients send avatar poses/deltas, remain colocated in hearing range, and have P2P disabled, so every voice frame targets 999 listeners. C# gets its own temporary application/configuration copy for each run; the shared fixture's transport settings are translated into its LiteNetLib sidecar.

Each large run warms up for 30 seconds after readiness and observes the server for 15 seconds after the client exits. C# runs took place October 6 UTC (October 5 local time), following the earlier Rust measurements on this host. They are single runs, not randomized paired repetitions. A four-client, 20-second preflight received exactly 100% of expected voice packets, with no missing, duplicate, or reordered sequences and a 30 ms voice gap p95.

| Server and workload | Window | Source frames/s, nominal | Expected fanout received | Received voice frames/s | Voice gap p95 | Server peak working set |
|---|---:|---:|---:|---:|---:|---:|
| C#, 100 speakers | 120 s | 4,997 (99.93%) | 62.32% | 3.11 million | 53 ms | 12,922.7 MiB |
| Rust, 100 speakers | 120 s | 4,974 (99.47%) | 97.95% | 4.87 million | 39 ms | 227.7 MiB |
| C#, 1,000 speakers | 180 s | 49,964 (99.93%) | 8.85% | 4.42 million | 1,182 ms | 5,829.3 MiB |
| Rust, 1,000 speakers | 180 s | 16,797 (33.59%) | 59.18% | 9.93 million | 187 ms | 267.1 MiB |

The all-speaking C# generator meets its source cadence target. Its server delivers a small fraction of expected fanout despite that, records 45.3 million voice queue drops during the sampled health-counter window, and produces a 1,482 ms voice gap p99. The 100-speaker run records 44.7 million voice drops and a 600 ms voice gap p99. Queue-drop counters do not account for every missing receipt.

The normal workloads have nearly matching offered voice rates. In the all-speaking workloads, Rust's prioritized server threads and colocated generator achieve a much lower source rate than C#'s generator. The receipt percentages therefore have different denominators and cannot alone rank capacity at identical offered load. The absolute receipt rates, actual source rates, and avatar delivery measurements are recorded alongside them. Neither implementation passes the full voice capacity screen on this shared-host setup; these measurements do not establish the hardware's ultimate capacity.

## Validation and memory

The tables and validation notes in this document retain the summary. Detailed hashes, failed checks, audio validation, process counters, GC samples, memory slopes, and post-client observations are archived locally under `captures/pr31-results-history-cleanup-20261008/results/voice-csharp-20261005-summary.json`. Raw artifacts and frozen binaries remain under `captures/voice-csharp-20261005`.

Both C# voice runs retain all 1,000 clients and observe all 999 remote avatar peers. They also report unreliable/voice drops, stale avatars, and unapplied deltas. Avatar p95 gaps are 270.9 ms with 100 speakers and 2,315.6 ms with all speaking. The 100-speaker avatar observation spans 119,907 ms instead of exactly 120,000 ms, so the strict avatar-window check also fails; the independently timed voice window completes at 120,018 ms. The all-speaking voice window completes at 180,007 ms.

The C# server retains about 12.6 GiB and 5.7 GiB working set, respectively, at the end of the 15-second post-client observations, and still reports 1,000 transport visitors. These observations do not establish whether retained pools or delayed work would drain after a longer idle period, and do not prove a memory leak. Lower memory in the all-speaking run also accompanies much lower measured server UDP ingress and poor delivery, rather than equal accepted work.

C# health does not expose Rust's UDP WouldBlock or application protocol-error counters. The analyzer marks these checks unavailable instead of claiming zero errors. It uses C# visitor counts plus successful client join logs for readiness, then validates sender records and observer coverage. Frame receipt ratios include observation-window boundary timing and are capacity screens rather than exact packet-loss measurements. These tests measure packet arrivals, not 1,000 playback decoders/mixers or Internet/NIC capacity.

The avatar-only C# baseline passes all validation checks with no reported queue drops, a 120.4 ms avatar gap p95, 6.45 average server CPU cores, and 2,675.4 MiB peak working set. Its post-client observation ends with zero visitors and 2,687.2 MiB working set. The corresponding Rust baseline has a 108.2 ms avatar gap p95, 5.46 server CPU cores, and 228.7 MiB peak working set. The servers deliver different numbers of avatar updates, so this is not an equal-work CPU-efficiency comparison.

## Reproduction

Run `scripts/perf/run-windows-avatar-workload.py` with a published, protocol-compatible C# executable and the same client/audio fixtures:

```powershell
python scripts/perf/run-windows-avatar-workload.py --server <BasisNetworkConsole.exe> --server-kind csharp --client <frozen-client.exe> --server-config docs/performance/fixtures/avatar-cpu-only-server.xml --clients 1000 --workers 4 --warmup-seconds 30 --window-seconds 180 --post-client-seconds 15 --voice-audio-folder <encoded-audio-folder> --voice-speaker-percent 100 --no-voice-reencode --output <new-capture-folder>
python scripts/perf/analyze-windows-voice.py <capture-folder> --audio-folder <encoded-audio-folder>
```

Use `--voice-speaker-percent 10 --window-seconds 120` for the 100-speaker workload. Omit voice options and use a 60-second window for the avatar-only baseline.
