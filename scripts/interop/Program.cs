// Golden-vector generator: runs the VERBATIM ReliableChannel.cs from the Basis package against
// the stub peer and prints the ACK datagrams, so the Rust transport can be pinned to them.
//
// Scenarios cover what matters, including the case a hand-written approximation got wrong:
//   S1  contiguous from 0            -> window never slides, bits absolute from 0
//   S2  201 contiguous, one dropped  -> window slid twice, off any 128 boundary
//   S3  out-of-order arrival         -> bits set across gaps, window start unchanged
//   S4  arrival that forces a slide  -> bits cleared as the window advances
//   S5  duplicate retransmits        -> re-acknowledged; window must not strand the sender
//   S6  decode direction             -> a Rust-built ACK fed into the real C# ProcessAck
//
// The output is pasted into the Rust test `csharp_reliable_channel_golden_vectors` as expected
// values, so the wire format is pinned to this C# and not to our reading of it.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace LiteNetLib
{
    public static class Program
    {
        private const int BitCount = (NetConstants.DefaultWindowSize - 1) / 8 + 2;
        // 74 is the channel id the Rust transport uses for CHAT / ReliableOrdered, so the golden
        // vectors can be compared byte for byte instead of with the channel byte masked off.
        private const byte ChannelId = 74;

        private static NetPacket MakeChanneled(ushort sequence, byte payload)
        {
            var p = new NetPacket(PacketProperty.Channeled, 1) { ChannelId = ChannelId };
            p.Sequence = sequence;
            p.RawData[NetConstants.ChanneledHeaderSize] = payload;
            return p;
        }

        private static string Hex(byte[] bytes) => string.Concat(bytes.Select(b => b.ToString("x2")));

        private static int WindowStart(ReliableChannel ch) =>
            (int)typeof(ReliableChannel)
                .GetField("_remoteWindowStart", BindingFlags.NonPublic | BindingFlags.Instance)!
                .GetValue(ch)!;

        /// The datagram as it goes on the wire.
        ///
        /// The C# keeps `Property` and `ChannelId` as fields and only writes `Sequence` into
        /// RawData; the property byte and channel id are stamped in by the send path. So
        /// reconstruct byte 0 and byte 3 here, otherwise the golden vector is not the wire form.
        private static byte[] WireForm(NetPacket ack, byte channelId)
        {
            var wire = new List<byte> { (byte)((byte)ack.Property | (0 << 5)) };
            wire.AddRange(BitConverter.GetBytes(ack.Sequence));
            wire.Add(channelId);
            wire.AddRange(ack.RawData.Skip(NetConstants.ChanneledHeaderSize).ToArray());
            return wire.ToArray();
        }

        /// Flush the ACK the C# would put on the wire and report it plus the state behind it.
        private static void Emit(string name, NetPeer peer, ReliableChannel ch)
        {
            int before = peer.Sent.Count;
            ch.SendNextForTest();
            var ack = peer.Sent.Skip(before).LastOrDefault(p => p.Property == PacketProperty.Ack);
            if (ack == null)
            {
                Console.WriteLine($"{name} ACK <none>");
                return;
            }
            Console.WriteLine($"{name} ACK {Hex(WireForm(ack, ChannelId))}");
            var local = new List<byte>();
            local.AddRange(BitConverter.GetBytes((ushort)WindowStart(ch)));
            local.AddRange(ack.RawData.Skip(NetConstants.ChanneledHeaderSize).ToArray());
            Console.WriteLine($"{name} LOCAL {Hex(local.ToArray())}");
        }

        public static void Main()
        {
            Console.WriteLine($"WINDOW {NetConstants.DefaultWindowSize} MAXSEQ {NetConstants.MaxSequence} BITS {BitCount}");

            // S1: contiguous from zero.
            {
                var peer = new NetPeer();
                var ch = new ReliableChannel(peer, true, ChannelId);
                for (ushort s = 0; s < 6; s++) ch.ProcessPacket(MakeChanneled(s, (byte)s));
                Emit("S1", peer, ch);
            }

            // S2: 201 contiguous with sequence 198 lost. The window slides twice and ends up off
            // any 128 boundary -- the case that separates absolute from window-relative bits.
            {
                var peer = new NetPeer();
                var ch = new ReliableChannel(peer, true, ChannelId);
                for (ushort s = 0; s <= 200; s++)
                {
                    if (s == 198) continue;
                    ch.ProcessPacket(MakeChanneled(s, (byte)(s % 251)));
                }
                Emit("S2", peer, ch);
            }

            // S3: out of order, with gaps that are never filled.
            {
                var peer = new NetPeer();
                var ch = new ReliableChannel(peer, true, ChannelId);
                foreach (ushort s in new ushort[] { 0, 1, 2, 5, 6, 9 })
                    ch.ProcessPacket(MakeChanneled(s, (byte)s));
                Emit("S3", peer, ch);
            }

            // S4: one arrival from beyond the window, which must slide it by the minimum and land
            // the newcomer on the top bit.
            {
                var peer = new NetPeer();
                var ch = new ReliableChannel(peer, true, ChannelId);
                for (ushort s = 0; s < 4; s++) ch.ProcessPacket(MakeChanneled(s, (byte)s));
                ch.ProcessPacket(MakeChanneled((ushort)(NetConstants.DefaultWindowSize + 3), 0xEE));
                Emit("S4", peer, ch);
            }

            // S5: retransmits of things still inside the window must be re-acknowledged. A window
            // that slid on contiguous arrivals would strand the sender's oldest packet forever.
            {
                var peer = new NetPeer();
                var ch = new ReliableChannel(peer, true, ChannelId);
                for (ushort s = 0; s < 8; s++) ch.ProcessPacket(MakeChanneled(s, (byte)s));
                ch.ProcessPacket(MakeChanneled(0, 0));
                ch.ProcessPacket(MakeChanneled(3, 3));
                Emit("S5", peer, ch);
            }

            // S6: the decode direction, against the real C# ProcessAck. Put 300 packets in
            // flight, clear the first 64 with one ACK so the sender's window start is no longer
            // 0, then offer the same coverage two ways. Absolute bits release the rest; the
            // window-relative reading of the same coverage aliases onto already-released
            // sequences and releases nothing. That difference is the whole bug this pins down.
            {
                foreach (var (name, windowStart, first, last) in new[]
                {
                    // Same coverage (64..=127), encoded the two competing ways.
                    ("ABSOLUTE", (ushort)64, (ushort)64, (ushort)127),
                    ("RELATIVE", (ushort)64, (ushort)0, (ushort)63),
                })
                {
                    var peer = new NetPeer();
                    var ch = new ReliableChannel(peer, true, ChannelId);
                    // Exactly one window's worth, so the outgoing queue drains and the return
                    // value of SendNextPackets reflects only whether anything is still in flight.
                    for (int i = 0; i < NetConstants.DefaultWindowSize; i++)
                    {
                        var p = new NetPacket(PacketProperty.Channeled, 1) { ChannelId = ChannelId };
                        p.RawData[NetConstants.ChanneledHeaderSize] = (byte)i;
                        ch.EnqueueForTest(p);
                    }
                    ch.SendNextForTest();
                    Console.WriteLine($"S6 {name} INFLIGHT {peer.Sent.Count} FIRST {peer.Sent[0].Sequence} LAST {peer.Sent[^1].Sequence}");

                    // Step 1: release 0..=63 so the sender's window start moves to 64.
                    ch.ProcessPacket(Decode(Ack(0, 0, 63, Array.Empty<ushort>())));
                    // Step 2: the same coverage 64..=127, encoded both ways.
                    var ack = Ack(windowStart, first, last, Array.Empty<ushort>());
                    ch.ProcessPacket(Decode(ack));
                    bool anythingLeft = ch.SendNextForTest();

                    Console.WriteLine($"S6 {name} ACK {Hex(ack)}");
                    Console.WriteLine($"S6 {name} DRAINED {(!anythingLeft).ToString().ToUpperInvariant()}");
                }
            }
        }

        /// An ACK over sequences `from..=to` with the given header window start, setting each
        /// bit at `from + (index passed) % 128` so the caller picks absolute or relative.
        private static byte[] Ack(ushort windowStart, ushort first, ushort to, ushort[] missing)
        {
            var bits = new byte[BitCount];
            for (ushort s = first; s <= to; s++)
            {
                if (Array.IndexOf(missing, s) >= 0) continue;
                int idx = s % NetConstants.DefaultWindowSize;
                bits[idx / 8] |= (byte)(1 << (idx % 8));
            }
            var packet = new List<byte> { (byte)PacketProperty.Ack };
            packet.AddRange(BitConverter.GetBytes(windowStart));
            packet.Add(ChannelId);
            packet.AddRange(bits);
            return packet.ToArray();
        }

        private static NetPacket Decode(byte[] raw)
        {
            var p = new NetPacket(raw.Length) { RawData = raw, Size = raw.Length };
            p.Property = (PacketProperty)(raw[0] & 0x1F);
            p.ChannelId = raw[3];
            return p;
        }
    }
}
