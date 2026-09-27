// Minimal stubs so the VERBATIM ReliableChannel.cs from the Basis package compiles and runs
// standalone. Only the plumbing is stubbed; the reliable-channel logic under test is the real
// file. Signatures mirror the real LiteNetLib types closely enough that the channel code is
// unchanged.
using System;
using System.Collections.Generic;

namespace LiteNetLib
{
    internal enum PacketProperty : byte
    {
        Unreliable = 0, Channeled = 1, Ack = 2, Ping = 3, Pong = 4, ConnectRequest = 5,
        ConnectAccept = 6, Disconnect = 7, UnconnectedMessage = 8, MtuCheck = 9, MtuOk = 10,
        Broadcast = 11, Merged = 12, ShutdownOk = 13, PeerNotFound = 14, InvalidProtocol = 15,
        NatMessage = 16, Empty = 17, CompactMerged = 18
    }

    internal enum DeliveryMethod : byte
    {
        ReliableUnordered = 0, Sequenced = 1, ReliableOrdered = 2, ReliableSequenced = 3,
        Unreliable = 4
    }

    internal static class NetConstants
    {
        public const int DefaultWindowSize = 128;
        public const int HeaderSize = 1;
        public const int UnreliableHeaderSize = 2;
        public const int ChanneledHeaderSize = 4;
        public const ushort MaxSequence = 32768;
        public const ushort HalfMaxSequence = MaxSequence / 2;
    }

    internal static class NetUtils
    {
        // Copied verbatim from the real NetUtils.RelativeSequenceNumber.
        internal static int RelativeSequenceNumber(int number, int expected)
        {
            return (number - expected + NetConstants.MaxSequence + NetConstants.HalfMaxSequence)
                   % NetConstants.MaxSequence - NetConstants.HalfMaxSequence;
        }
    }

    internal static class NetDebug
    {
        public static void Write(string s) { }
    }

    internal sealed class NetPacket
    {
        public byte[] RawData;
        public int Size;
        public PacketProperty Property;
        public byte ChannelId;

        public NetPacket(int size) { RawData = new byte[size]; Size = size; }

        public NetPacket(PacketProperty property, int size)
        {
            size += GetHeaderSize(property);
            RawData = new byte[size];
            Property = property;
            Size = size;
        }

        public static int GetHeaderSize(PacketProperty property)
        {
            switch (property)
            {
                case PacketProperty.Unreliable: return NetConstants.UnreliableHeaderSize;
                case PacketProperty.Channeled:
                case PacketProperty.Ack: return NetConstants.ChanneledHeaderSize;
                default: return NetConstants.HeaderSize;
            }
        }

        // RawData[1..3] little endian, exactly as the real NetPacket does.
        public ushort Sequence
        {
            get => BitConverter.ToUInt16(RawData, 1);
            set => BitConverter.GetBytes(value).CopyTo(RawData, 1);
        }
    }

    internal sealed class NetStatistics
    {
        public void IncrementPacketLoss() { }
    }

    internal sealed class NetManager
    {
        public bool EnableStatistics = false;
        public NetStatistics Statistics = new NetStatistics();
    }

    internal sealed class NetPeer
    {
        public readonly List<NetPacket> Sent = new List<NetPacket>();
        public readonly List<NetPacket> Delivered = new List<NetPacket>();
        public NetManager NetManager = new NetManager();
        public NetStatistics Statistics = new NetStatistics();
        // The real NetPeer exposes ResendDelay as a double of milliseconds, not a TimeSpan.
        public double ResendDelay = 27.0;

        public void SendUserData(NetPacket p) { Sent.Add(p); }
        public void RecycleAndDeliver(NetPacket p) { Delivered.Add(p); }
        public void AddReliablePacket(DeliveryMethod m, NetPacket p) { Delivered.Add(p); }
    }

    internal abstract class BaseChannel
    {
        protected readonly NetPeer Peer;
        // Public rather than protected: the verbatim ReliableChannel.cs only reads it, and a
        // public field still satisfies that. Visibility here is a stub detail, not behaviour.
        public readonly Queue<NetPacket> OutgoingQueue = new Queue<NetPacket>();
        private bool _sendQueueSet;

        protected BaseChannel(NetPeer peer) { Peer = peer; }

        protected void AddToPeerChannelSendQueue() { _sendQueueSet = true; }
        public bool SendQueueSet => _sendQueueSet;

        // Test hooks: the real members are protected, so expose narrow wrappers instead of
        // changing the visibility the derived class overrides.
        public bool SendNextForTest() => SendNextPackets();
        public void EnqueueForTest(NetPacket p) => OutgoingQueue.Enqueue(p);

        protected abstract bool SendNextPackets();
        public abstract bool ProcessPacket(NetPacket packet);
    }
}
