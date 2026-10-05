# Dedicated voice and avatar processing

Voice packets bypass the control-event queue at transport admission. A dedicated `BSR-VoiceBatch` thread collects them every 10 ms, and `BSR-VoiceSend-*` threads dispatch MTU-packed batches. The pool defaults to half the available logical processors, bounded to 1–16 threads; `BASIS_VOICE_SEND_WORKERS` overrides it within those limits, with zero selecting automatic sizing. This test host uses eight send threads. Each recipient belongs to one send thread, preserving voice sequence order. Voice threads use nonblocking duplicated socket handles with the original server port and shared connection/statistics state. Windows registers voice threads with the multimedia scheduler's Audio task and falls back to higher thread priority when that scheduler is unavailable.

Avatar full poses and uplink deltas bypass the control queue into `BSR-AvatarInput`. Each sender retains its latest full pose and latest dependent delta, processed in that order. Existing avatar tick and Rayon flush threads continue handling downlinks. Reliable keyframe requests continue through the control dispatcher, so requests for different senders are not coalesced away.

Voice admission retains at most three frames per authenticated sender. Each send thread retains at most five pending batches and combines up to two batches per flush. The batch queue covers 50 ms at the 10 ms batching cadence, leaving time for dispatch within the 100 ms expiry deadline. It sends turns of at most 32 frames per recipient and rotates the starting recipient, preventing a large fanout from consuming the entire deadline on the first listeners. Already-expired groups are discarded before walking recipient lists. Audio older than 100 ms is discarded before dispatch; slow receivers cannot create unbounded replay queues. Recipient lists retain indices into immutable batch payloads, avoiding per-recipient audio copies and shared reference-count updates. Those lists are reused and their storage is released when the server becomes empty. These bounds prevent memory growth with elapsed overload time; memory still scales with population and fanout.

Queued messages and sends retain a connection incarnation, so reusing a numeric peer ID does not replay a queued batch into a new connection. Authentication, voice mute, voice lock/bypass permissions, announcement permissions, recipient lists, and P2P offload filtering remain enforced. Shutdown closes realtime admission and joins the dedicated threads.

## Validation

The loopback regression deliberately holds the avatar delta-state lock while transmitting voice. Ten consecutive voice frames arrive in order within the test's 100 ms deadlines. Other checks cover bounded admission, expiry, connection changes, keyframe/delta coalescing, independent routing when the control queue is full, reused peer IDs, and duplicated socket source-port/wire/statistics compatibility.

Server workspace tests pass (284 passed, one ignored), client workspace tests pass (98 passed), and both workspaces pass Clippy for all targets with warnings denied. Release builds and formatting checks pass.

The Windows workload uses 1,000 authenticated headless clients, colocated inside hearing range, sending the Unity avatar policy at a simulated 60 FPS. Each voice frame therefore targets 999 recipients. Forty audio files from `Desktop/Temp/Basis/AudioClips` were preencoded to 48 kHz mono Opus at 32 kbit/s with 20 ms frames. Captured Opus samples are compared against that corpus and decoded with FFmpeg. Client and server run on the same Ryzen 7 9800X3D host with 16 logical processors and approximately 64 GiB RAM.

The current-main comparison uses `f78dfa4`; it already includes bounded control-handler admission. The [earlier memory investigation](voice-memory-growth.md) measured `09cf74f`, which predates that fix.

Measurements use frozen binaries from `ac17492`; the final branch has identical server and client runtime code. Raw captures, frozen binaries, and source snapshots are retained locally under `captures/voice-isolation-20261005`. The [machine-readable results](results/voice-isolation-20261005-summary.json) include binary/configuration hashes, audio verification, queue counters, memory observations, and capacity checks.

| Workload | Marked window | Voice send cadence, nominal | Expected voice fanout received | Observer voice gap p95 | Server peak working set | Avatar gap p95 |
|---|---:|---:|---:|---:|---:|---:|
| Current main, 100 speakers | 60 s | 71.0% | 23.9% | 429 ms | 2,862.5 MiB | 8,431.1 ms |
| Dedicated threads, 100 speakers | 120 s | 99.5% | 97.9% | 39 ms | 227.7 MiB | 286.4 ms |
| Dedicated threads, 1,000 speakers | 180 s | 33.6% | 59.2% | 187 ms | 267.1 MiB | 388.7 ms |
| Dedicated threads, avatar only | 60 s | — | — | — | 228.7 MiB | 108.2 ms |

The 100-speaker run completed with all clients active, zero measured application/protocol/send errors, and matching, decodable Opus samples. No control-event tasks waited for worker capacity, and sampled control-queue depth stayed zero. Server working-set growth was -0.033 MiB/s over the marked window, with 214.0 MiB remaining after clients exited and 15 seconds of idle observation. The main comparison failed the avatar stale-peer and unapplied-delta checks; these checks pass in the 100-speaker dedicated-thread run.

Voice continuity improves substantially, but the 100-speaker run still falls short of the 99% receipt capacity screen. Separate threads and bounded queues do not establish uninterrupted audio at arbitrary load. Voice gap percentiles use 1 ms histogram bins and are packet arrival measurements.

The all-speaking stress run completed its full 180-second window with all 1,000 clients sending and receiving voice. Samples matched the source corpus and decoded successfully, and no application/protocol/send errors were recorded. The headless generator sustained only 16,797 source frames/s against a nominal 50,000; 59.2% of expected fanout arrived, and voice gap p99 was 2,624 ms. This fails all three voice-capacity checks and the avatar stale-peer check. It cannot be described as uninterrupted voice.

Memory remained bounded under that overload: 267.1 MiB peak, -0.018 MiB/s marked-window growth, zero sampled waiting control tasks and queue depth, and 255.5 MiB after clients exited. All players disconnected and the harness stopped the server after the idle observation. Separate threads address shared dispatch contention and runaway backlog; they do not remove the CPU/socket cost of 1,000 speakers each targeting 999 listeners. At the requested cadence this would require 49,950,000 voice recipient frames/s in addition to avatar traffic. Hearing-range distribution, P2P offload, stronger hardware, and a separate load-generator machine need their own capacity measurements.

The avatar-only baseline passes all validation checks, uses 5.46 server CPU cores on average, and finishes the idle observation at 158.6 MiB with no online players or queued control work. Servers are stopped with the harness's controlled console interrupt after clients exit; dedicated-thread shutdown/join behavior is exercised directly by the Rust regression tests.

## Reproduction

Build both Rust workspaces in release mode, then run `scripts/perf/run-windows-voice-workload.py --audio-folder <folder> --output <new-capture-folder>`. It prepares the clips and runs a small audio check, an avatar baseline, 100 simultaneous speakers, and all 1,000 simultaneous speakers. Its defaults use 45-second warmups and 60/120/180-second marked windows. The comparison here uses 30-second warmups, 15-second post-client observations, and frozen server binaries passed to `run-windows-avatar-workload.py`.

Set `BASIS_VOICE_SERVER_DIAGNOSTIC_CSV` on the server to record source admission, expired frames, queued batch replacements, pending frames, sent datagrams, and expired recipient deliveries. `BASIS_EVENT_DIAGNOSTIC_CSV` records control tasks and queue depths; `BASIS_VOICE_DIAGNOSTIC_CSV` enables client voice capture aligned to the avatar observation marker. These diagnostics are opt-in.

This is a dense local server relay and packet-continuity test. It does not instantiate 1,000 playback decoders/mixers or establish physical-NIC, Internet, P2P, or GPU capacity. A fixed observation window's receipt-to-expected-fanout ratio also includes boundary timing, so it is a capacity screen rather than an exact network packet-loss measurement. Maximum source gaps can include intentional speaker rotation when fewer than all clients speak.
