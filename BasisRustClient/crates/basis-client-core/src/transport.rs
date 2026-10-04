use basis_transport::{relative_sequence, PacketProperty, DEFAULT_WINDOW_SIZE, MAX_SEQUENCE};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Notify;

#[cfg(any(target_os = "linux", windows, test))]
pub(crate) const LITENETLIB_MAX_MTU: usize = 1432;
/// LiteNetLib's `NetManager.UpdateTime` default, and so the cadence at which accumulated ACKs
/// are flushed. This is the number that matters for throughput: it bounds how long a received
/// packet waits to be acknowledged, and the sender's 128-deep window cannot refill faster.
/// Both maintenance loops tick at this rate and derive their slower periods from the tick
/// count, so there is no separate "maintenance interval" any more.
pub(crate) const ACK_FLUSH_INTERVAL: Duration = Duration::from_millis(15);
/// Ticks between resend passes, so the period stays the old 100 ms.
pub(crate) const RESEND_INTERVAL_TICKS: usize = 7;
/// Ticks between pings: 100 ticks keeps the previous ~1.5 s period at a 15 ms tick.
pub(crate) const PING_INTERVAL_TICKS: usize = 100;
/// Ticks between client-list refreshes in the shared loop, preserving the previous ~1 s period
/// now that it ticks at 15 ms rather than 100 ms.
pub(crate) const SHARED_SNAPSHOT_REFRESH_TICKS: usize = 64;
pub(crate) const INITIAL_START_ATTEMPTS: usize = 3;
pub(crate) const SOCKET_BUFFER_SIZE: usize = 32 * 1024 * 1024;
pub(crate) const SOCKET_TTL: u32 = 255;
#[derive(Debug, Clone)]
pub(crate) struct ParsedPacket<'a> {
    pub(crate) property: PacketProperty,
    #[allow(dead_code)]
    pub(crate) connection_number: u8,
    pub(crate) sequence: Option<u16>,
    pub(crate) channel_id: Option<u8>,
    pub(crate) payload: &'a [u8],
}

pub(crate) fn parse_packet(bytes: &[u8]) -> Option<ParsedPacket<'_>> {
    if bytes.is_empty() {
        return None;
    }
    let property = PacketProperty::from_byte(bytes[0])?;
    let connection_number = (bytes[0] & 0x60) >> 5;
    let header = match property {
        PacketProperty::Unreliable => 2,
        PacketProperty::Channeled | PacketProperty::Ack => 4,
        PacketProperty::Ping => 3,
        PacketProperty::Pong => 11,
        PacketProperty::ConnectAccept => 15,
        PacketProperty::Disconnect => 9,
        _ => 1,
    };
    if bytes.len() < header {
        return None;
    }
    let sequence = match property {
        PacketProperty::Channeled
        | PacketProperty::Ack
        | PacketProperty::Ping
        | PacketProperty::Pong => Some(u16::from_le_bytes([bytes[1], bytes[2]])),
        _ => None,
    };
    let channel_id = match property {
        PacketProperty::Channeled | PacketProperty::Ack => Some(bytes[3]),
        _ => None,
    };
    Some(ParsedPacket {
        property,
        connection_number,
        sequence,
        channel_id,
        payload: &bytes[header..],
    })
}

#[derive(Clone)]
pub(crate) struct MaintenanceOptions {
    pub(crate) shared: bool,
    pub(crate) refresh: Arc<Notify>,
}

#[derive(Clone, Copy)]
pub(crate) struct ConnectOptions {
    pub(crate) batch_size: usize,
    pub(crate) batch_delay: Duration,
    pub(crate) timeout: Duration,
    pub(crate) shared_maintenance: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct ReliableSend {
    pub(crate) channel_id: u8,
    /// `None` means queued but not yet admitted to LiteNetLib's 128-sequence send window.
    pub(crate) sequence: Option<u16>,
    pub(crate) bytes: Vec<u8>,
    pub(crate) last_sent: Option<SystemTime>,
}

/// Mirror of the receive side of LiteNetLib's `ReliableChannel`.
///
/// Two details are load-bearing for wire compatibility, and both are easy to get wrong:
///
/// 1. ACK bits are **absolute** -- bit `sequence % DEFAULT_WINDOW_SIZE`. The window start is
///    carried in the ACK header only so the peer can bound-check the packet; the bits are not
///    offsets from it. Treating them as relative makes a real client's windowed ACK match
///    almost nothing once sequence numbers grow, and its send window never refills.
/// 2. The window slides forward only when a packet arrives from *beyond* it, never as
///    contiguous packets arrive. So a retransmit of anything still inside the window re-sets
///    its bit and gets acknowledged again. A window that slid on every contiguous packet would
///    strand the sender's oldest unacknowledged packet permanently, because no ACK the
///    receiver can still send would ever cover it again.
#[derive(Debug)]
pub(crate) struct ReliableReceiveState {
    /// Bits are absolute: bit `sequence % DEFAULT_WINDOW_SIZE` is set once that sequence has
    /// been received.
    pub(crate) received: [u128; 256],
    /// LiteNetLib's `_remoteWindowStart`: the oldest sequence still inside the ACK window.
    pub(crate) window_start: [u16; 256],
    pub(crate) started: [bool; 256],
    /// LiteNetLib's `_mustSendAcks`, per channel: the window has changed and an ACK is owed.
    /// The C# sets this on arrival and flushes it in `SendNextPackets`, which the network
    /// update tick calls; it never sends an ACK inline. We do the same, because sending one
    /// datagram per received packet made roughly a third of all ACK traffic redundant --
    /// re-sending a window that had not changed since the last ACK.
    pub(crate) ack_dirty: [bool; 256],
}

impl Default for ReliableReceiveState {
    fn default() -> Self {
        Self {
            received: [0; 256],
            window_start: [0; 256],
            started: [false; 256],
            ack_dirty: [false; 256],
        }
    }
}

impl ReliableReceiveState {
    /// Record a received sequence, returning whether it is newly seen.
    ///
    /// A `false` return for a *duplicate* still has to be acknowledged by the caller -- that is
    /// what lets a lost ACK recover. A `false` return for a too-old or nonsensical sequence is
    /// not acknowledged, matching LiteNetLib, which rejects those before its ACK bookkeeping.
    pub(crate) fn mark_new(&mut self, channel_id: u8, sequence: u16) -> bool {
        let index = channel_id as usize;
        if sequence >= MAX_SEQUENCE {
            return false; // bad sequence
        }
        let relate = relative_sequence(sequence, self.window_start[index]);
        if relate < 0 {
            // Too old to still be in the window. Believing it would set a bit that aliases an
            // in-window sequence, so drop it -- and, as LiteNetLib does, do not acknowledge.
            return false;
        }
        if relate >= (DEFAULT_WINDOW_SIZE * 2) as i32 {
            return false; // implausibly far ahead of the window
        }
        // Keep the C#'s initial window at zero even when packet zero is lost. A first arrival
        // at sequence 1 must produce an ACK with header window zero so the sender accepts it.
        self.started[index] = true;
        // `_mustSendAcks = true` in the C#, set after every rejection and *before* the duplicate
        // check. A retransmit therefore still schedules an ACK, which is what lets a lost ACK
        // recover -- the sender would otherwise never hear about that packet again.
        self.ack_dirty[index] = true;
        if relate >= DEFAULT_WINDOW_SIZE as i32 {
            // Slide just far enough to bring the newcomer back inside, clearing bits as we go.
            let shift = relate as usize - DEFAULT_WINDOW_SIZE + 1;
            for _ in 0..shift {
                let leaving = self.window_start[index] as usize % DEFAULT_WINDOW_SIZE;
                self.received[index] &= !(1u128 << leaving);
                self.window_start[index] = self.window_start[index].wrapping_add(1) % MAX_SEQUENCE;
            }
        }

        let bit = 1u128 << (sequence as usize % DEFAULT_WINDOW_SIZE);
        if self.received[index] & bit != 0 {
            return false; // duplicate
        }
        self.received[index] |= bit;
        true
    }

    /// Render the receive window as a LiteNetLib ACK: the window start for the header, and the
    /// absolute bit set for the payload. Returns `None` if the channel has received nothing.
    ///
    /// The whole window goes out every time, exactly as LiteNetLib sends its accumulated
    /// `_outgoingAcks`, so one datagram can acknowledge a whole run of packets.
    pub(crate) fn ack_window(&self, channel_id: u8) -> Option<(u16, Vec<u8>)> {
        let index = channel_id as usize;
        if !self.started[index] {
            return None;
        }
        let mut bits = vec![0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
        for offset in 0..DEFAULT_WINDOW_SIZE {
            if self.received[index] & (1u128 << offset) != 0 {
                bits[offset / 8] |= 1 << (offset % 8);
            }
        }
        Some((self.window_start[index], bits))
    }

    /// Take the channels that owe an ACK, clearing their flags.
    ///
    /// Flags are cleared *before* the datagrams go out, so a packet arriving during the send
    /// re-arms the channel for the next pass instead of being lost. A send that fails outright
    /// is recovered the same way LiteNetLib recovers it: the server retransmits, the
    /// retransmit re-arms the channel, and the next pass acknowledges it.
    pub(crate) fn take_dirty_channels(&mut self) -> Vec<u8> {
        let mut channels = Vec::new();
        for (channel_id, dirty) in self.ack_dirty.iter_mut().enumerate() {
            if *dirty {
                *dirty = false;
                channels.push(channel_id as u8);
            }
        }
        channels
    }
}

pub(crate) fn read_bytes_message(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 2 {
        return None;
    }
    let len = u16::from_le_bytes([data[0], data[1]]) as usize;
    data.get(2..2 + len)
}
