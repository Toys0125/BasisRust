# Historical avatar bundle codec default

At the revision measured below, Rust defaulted `EnableAvatarBundleZstd` to `false` and used LZ4 for avatar bundles. This historical result predates the exact per-tick bundle cache. The current production default enables dictionary Zstd at level −5 for full/keyframe-containing bundles while delta-only bundles use LZ4; see [the cached 1,500-client measurements](avatar-bundle-cache-1500.md). Existing configs with explicit codec settings retain them.

The historical change followed a matched 1500-client C# load test against the fixed reliable-dispatch server. Both 60-second windows used the same rich-pose C# DLL, 90 ms movement cadence, all-High tier, 1499 normal clients plus one pose observer, and no random reconnects. The Zstandard control used level -2. The LZ4/default case used a config with the Zstandard field omitted, so the then-current Rust default selected LZ4.

| Metric | Zstandard enabled, level -2 | LZ4 default | Change |
|---|---:|---:|---:|
| Window mean tick time, estimated from endpoint counters | 49.32 ms | 42.62 ms | 13.6% lower |
| End receiver-cycle estimate | 1539 ms | 1344 ms | 12.7% lower |
| Logical recipient sends/s | 1.443 M | 1.657 M | 14.8% higher |
| Aggregate UDP egress | 169.85 MB/s | 199.66 MB/s | 17.6% higher |
| UDP bytes/logical send | 117.68 B | 120.53 B | 2.4% higher |
| Server CPU time / 60 s | 177.81 s | 184.43 s | 3.7% higher |

Tick time is estimated by differencing `avgTickUs × ticks` from the 60-second start/end `/health` snapshots. `avgTickUs` is cumulative since startup and rounded to the nearest microsecond, so this is an endpoint-derived window estimate rather than a separately sampled per-tick series. Receiver-cycle values are the end-snapshot estimates. Both windows stayed at 1500 active clients, decoded 1499 observer senders, had zero dropped unreliable packets and protocol/pose parse errors, and reported zero client send errors. The LZ4 case sent more recipient work per second, so its lower cycle was not caused by reduced coverage. This is one matched run per setting and records the pre-cache tradeoff on this rich-pose workload. LZ4 remains an available compatible bundle codec through an explicit config override.

Reproduction artifacts:

- Zstandard control: `/tmp/basisrust-csharp1500-v55-rich-allhigh-noreconnect-zstd-run2`
- New default with the XML field omitted: `/tmp/basisrust-csharp1500-v55-rich-allhigh-defaultlz4-finalrun`
- C# client DLL SHA-256: `d1b2d0070d2d7a112e4947c34227b9b5adcae5c2fe58ff2fc00630af45655294`
- Optimized Rust server binary SHA-256: `6559521a57d5b60324e1304fc4a6b20e335b3a62682150fae39810c288e05a08`
- Missing-field test config SHA-256: `ff95b50dd93dda3cc9d9a9c6196b1d7a33e493f0d488acadebd923ba2e687df5`
