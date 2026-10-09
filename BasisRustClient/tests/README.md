# Live loopback regressions

Run from the repository root:

```sh
CARGO_BUILD_JOBS=2 cargo test --manifest-path BasisRustClient/Cargo.toml --test live_loopback
```

These conventional integration tests start the real `basis-rust-client` executable
against `ServerState::start`, which runs the real server transport, authentication,
dispatch, and avatar tick loops. They supplement the existing in-module UDP/admin
tests and C# reliable-channel vectors. The existing client CI `cargo test` step
discovers them automatically.

- Wrong-password rejection before transport/player admission.
- Corrupted Ed25519 identity response rejection after transport acceptance, without
  admitting a player or sending post-auth metadata.
- Correct DID authentication after dropping the first client response; verify its
  signature against the server challenge and advertised public key.
- Reliable server-to-client traffic through 160 sequence numbers. Withhold sequence
  zero until an actual gap ACK arrives, then check retransmission, absolute bitmap
  coverage, window refill, and server pending/queued drain.
- Two clients send moving high-quality avatar state and Opus silence. Compare all
  159 pose bytes and sequence to captured uplinks, require forwarding to the other
  peer, and inspect the real client's observer CSV for applied state and decode
  errors. Verify voice sender ID, sequence, silence count, and unchanged Opus bytes
  in both forwarding directions.

The fixture owns a temporary directory, ephemeral loopback sockets, a supervised
client process, and a supervised forwarding task tree. Readiness is based on packet
events and bounded condition checks, not startup sleeps. The successful client
exits via its normal `quit` command. Each scenario runs in a child Tokio task while
its supervisor retains the fixture. Even on an assertion/deadline panic, the
supervisor awaits `ServerState::shutdown()`, kills/reaps the client, and joins the
proxy and its forwarders before propagating the original failure. Each cleanup
operation has a deadline and still runs if an earlier operation fails. An injected
assertion/deadline regression verifies this failure path. `Drop` remains a
best-effort fallback for cancellation. Voice input is a
locally generated mono Ogg Opus silence stream; no user audio, FFmpeg, GPU, audio
device, Unity installation, fixed ports, or external server is required.

## Pinned evidence and limits

Wire assertions use BasisVR commit
`81f190b217c11c2b39231e0bc9db330fd4a2803c`, specifically:

- `Basis Server/BasisNetworkClient/BasisDIDAuthIdentityClient.cs`: length-prefixed
  challenge, 64-byte Ed25519 signature, and `N/A` fragment convention.
- `Basis Server/BasisNetworkServer/Auth/Password.cs`: configured nonempty password
  rejection. Tests retain the upstream `default_password` and flat PascalCase XML.
- `Basis Server/BasisNetworkCore/Io/NetDataWriter.cs`: encoded string byte count
  includes one, without an extra wire terminator.
- `Basis Server/LiteNetLib/ReliableChannel.cs`: a 4-byte ACK header plus 17 bytes
  for a 128-slot bitmap (last byte padding), absolute sequence bits, and window
  sliding only on arrivals beyond the current window. Existing
  [C# interop vectors](../../scripts/interop/README.md), especially S1/S6/S7,
  remain the independent channel reference and are unchanged.
- `Basis Server/BasisNetworkCore/Serializable/Identity/PlayerIdMessage.cs` and
  `Serializable/Audio/{ServerAudioSegmentMessage,AudioSegmentDataMessage}.cs`:
  small sender ID followed by sequence, silence count, and remaining Opus bytes.
- `Basis Server/BasisNetworkCore/Serializable/Avatar/RemoteAvatarDataMessage.cs`
  and `LocalAvatarSyncMessage.cs`: channel-derived pose quality and fixed payload.
  The live fixture checks the current small-ID high-quality fanout envelope and
  exact pose preservation; existing protocol vectors cover packing/repacking.

This is Rust executable-to-Rust server coverage, **not proof of live Unity
interoperability**. The client is a synthetic load client: it ACKs reliable CHAT
payloads without displaying chat, and does not decode/play received voice. Voice
assertions establish server forwarding to the receiving client's socket; avatar
CSV establishes actual client application. Extension bundle/delta codecs are
disabled for this fixture; large IDs, fragmentation, all pose quality levels,
sequence wrap, extension negotiation, and live C# sockets are outside these
scenarios. Merged captures are unpacked independently; CompactMerged handling is
an extension-aware capture helper, not an upstream Unity parity assertion.

Prop/scene script relay and AdditionalAvatarData workloads are covered by the
live suite as well. See [script-data-harness.md](../../docs/performance/script-data-harness.md)
for benchmark commands, metrics, and limitations.
