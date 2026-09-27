# Reliable-ACK wire interop

`ReliableChannel.cs` here is a **verbatim copy** of the file Basis ships:

```
Packages/com.basis.server/LiteNetLib/ReliableChannel.cs
```

from a Basis Unity checkout. It is not edited. Everything else in this directory is a thin
stub so that one file can be compiled and driven on its own, which lets us generate the
authoritative ACK bytes instead of reasoning about what they should be.

## Why this exists

LiteNetLib's ACK encoding is not what it looks like at a glance, and getting it wrong is a
silent stall rather than a visible error:

- **ACK bits are absolute** -- bit `sequence % 128`. The window start in the header is *not*
  an offset base; the receiving peer uses it only to bound-check the packet
  (`ReliableChannel.ProcessAck`).
- **The window slides only when a packet arrives from beyond it**, never as contiguous
  packets arrive. A retransmit of anything still inside the window therefore re-sets its bit
  and is acknowledged again.

The second point is what makes an over-eager "improvement" so damaging. Re-anchoring the bit
set on the newest sequence looks equivalent and is not: once the anchor moves past a sequence,
no ACK the receiver can still send covers it, so the sender's oldest unacknowledged packet is
stranded for good. The Rust client did exactly this at one point and 117 of 5000 packets stuck
in flight.

## Regenerating the golden vectors

```
dotnet run --project scripts/interop
```

`Program.cs` drives the real channel through a set of scenarios and prints the ACK datagrams
it puts on the wire. `S1`-`S5` are encode vectors; `S6` is the decode direction, and it is
deliberately discriminating: the same coverage of sequences 64..=127 is offered twice, once
with absolute bits and once with window-relative bits. The real C# drains on the first and
releases **nothing** on the second.

The expected values live in
`crates/basis-transport/src/lib.rs::outgoing_acks_match_golden_vectors_from_the_csharp` and
`::decoding_matches_what_the_csharp_accepts`. If Basis changes its reliable layer, copy the
new `ReliableChannel.cs` over this one, re-run, and update those two tests -- they will tell
you exactly what moved.

## Refreshing the copy

```
copy /Y "<BasisUnity>\Basis\Packages\com.basis.server\LiteNetLib\ReliableChannel.cs" scripts\interop\
```

If the real file needs new types to compile, add them to `Stubs.cs` -- do not edit
`ReliableChannel.cs`, or the vectors stop being authoritative.
