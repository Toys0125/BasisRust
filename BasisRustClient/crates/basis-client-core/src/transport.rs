use basis_transport::{relative_sequence, PacketProperty, DEFAULT_WINDOW_SIZE, MAX_SEQUENCE};
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant, SystemTime},
};
use tokio::sync::Notify;

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
const MAX_REASSEMBLIES: usize = DEFAULT_WINDOW_SIZE / 2;
const MAX_REASSEMBLY_BYTES: usize = DEFAULT_WINDOW_SIZE * (LITENETLIB_MAX_MTU - 10);
const MAX_REASSEMBLY_PARTS: usize = 128;
const FRAGMENT_EXPIRY: Duration = Duration::from_secs(10);
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

#[derive(Debug)]
struct FragmentAssembly {
    total: usize,
    fragment_size: Option<usize>,
    parts: Vec<Option<Vec<u8>>>,
    received: usize,
    bytes: usize,
    updated_at: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct ReliableFragmentReassembler {
    entries: HashMap<(u8, u16), FragmentAssembly>,
    bytes: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum FragmentResult {
    Pending,
    Complete(Vec<u8>),
    Duplicate,
    Invalid,
}

impl ReliableFragmentReassembler {
    pub(crate) fn push(&mut self, channel_id: u8, payload: &[u8], now: Instant) -> FragmentResult {
        let expired = self
            .entries
            .iter()
            .filter_map(|(key, entry)| {
                (now.saturating_duration_since(entry.updated_at) > FRAGMENT_EXPIRY).then_some(*key)
            })
            .collect::<Vec<_>>();
        if !expired.is_empty() {
            for key in expired {
                self.remove(key);
            }
            // These packet sequences have already been ACKed. Continuing would leave a
            // permanently incomplete message with no way to request its missing pieces.
            return FragmentResult::Invalid;
        }
        if payload.len() <= 6 {
            return FragmentResult::Invalid;
        }
        let id = u16::from_le_bytes([payload[0], payload[1]]);
        let part = u16::from_le_bytes([payload[2], payload[3]]) as usize;
        let total = u16::from_le_bytes([payload[4], payload[5]]) as usize;
        let data = &payload[6..];
        // LiteNetLib derives fragment data length from the negotiated MTU. A peer may
        // therefore send smaller pieces than our own sender's fixed 1014-byte pieces.
        let max_fragment_size = LITENETLIB_MAX_MTU - 10; // packet and fragment headers
        if !(2..=MAX_REASSEMBLY_PARTS).contains(&total)
            || part >= total
            || data.is_empty()
            || data.len() > max_fragment_size
        {
            return FragmentResult::Invalid;
        }
        let key = (channel_id, id);
        if let Some(entry) = self.entries.get(&key) {
            if entry.total != total {
                self.remove(key);
                return FragmentResult::Invalid;
            }
            if part + 1 < total {
                if entry.fragment_size.is_some_and(|size| size != data.len()) {
                    self.remove(key);
                    return FragmentResult::Invalid;
                }
                if entry.fragment_size.is_none()
                    && entry.parts[total - 1]
                        .as_ref()
                        .is_some_and(|last| last.len() > data.len())
                {
                    self.remove(key);
                    return FragmentResult::Invalid;
                }
            } else if entry.fragment_size.is_some_and(|size| data.len() > size) {
                self.remove(key);
                return FragmentResult::Invalid;
            }
            if let Some(existing) = &entry.parts[part] {
                if existing == data {
                    return FragmentResult::Duplicate;
                }
                self.remove(key);
                return FragmentResult::Invalid;
            }
        } else {
            if self.entries.len() >= MAX_REASSEMBLIES {
                return FragmentResult::Invalid;
            }
            self.entries.insert(
                key,
                FragmentAssembly {
                    total,
                    fragment_size: (part + 1 < total).then_some(data.len()),
                    parts: vec![None; total],
                    received: 0,
                    bytes: 0,
                    updated_at: now,
                },
            );
        }
        if self.bytes.saturating_add(data.len()) > MAX_REASSEMBLY_BYTES {
            self.remove(key);
            return FragmentResult::Invalid;
        }
        let entry = self
            .entries
            .get_mut(&key)
            .expect("fragment assembly exists");
        if part + 1 < total {
            entry.fragment_size = Some(data.len());
        }
        entry.parts[part] = Some(data.to_vec());
        entry.received += 1;
        entry.bytes += data.len();
        entry.updated_at = now;
        self.bytes += data.len();
        if entry.received != entry.total {
            return FragmentResult::Pending;
        }
        let mut assembled = Vec::with_capacity(entry.bytes);
        for bytes in &entry.parts {
            let Some(bytes) = bytes else {
                return FragmentResult::Invalid;
            };
            assembled.extend_from_slice(bytes);
        }
        self.remove(key);
        FragmentResult::Complete(assembled)
    }

    fn remove(&mut self, key: (u8, u16)) {
        if let Some(entry) = self.entries.remove(&key) {
            self.bytes = self.bytes.saturating_sub(entry.bytes);
        }
    }
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

#[cfg(test)]
mod fragment_tests {
    use super::*;

    fn packet(id: u16, part: usize, total: usize, data: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(6 + data.len());
        bytes.extend_from_slice(&id.to_le_bytes());
        bytes.extend_from_slice(&(part as u16).to_le_bytes());
        bytes.extend_from_slice(&(total as u16).to_le_bytes());
        bytes.extend_from_slice(data);
        bytes
    }

    #[test]
    fn reassembles_basis_16k_image_chunk_across_negotiated_litenetlib_fragment_sizes() {
        let source = (0..(16 * 1024 + 29))
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        for (id, fragment_size) in [500, 1014, LITENETLIB_MAX_MTU - 10].into_iter().enumerate() {
            let fragments = source.chunks(fragment_size).collect::<Vec<_>>();
            let total = fragments.len();
            let mut receiver = ReliableFragmentReassembler::default();
            let now = Instant::now();
            assert_eq!(
                receiver.push(
                    96,
                    &packet(id as u16, total - 1, total, fragments[total - 1]),
                    now
                ),
                FragmentResult::Pending
            );
            assert_eq!(
                receiver.push(
                    96,
                    &packet(id as u16, total - 1, total, fragments[total - 1]),
                    now
                ),
                FragmentResult::Duplicate
            );
            let mut completed = None;
            for part in (0..total - 1).rev() {
                match receiver.push(96, &packet(id as u16, part, total, fragments[part]), now) {
                    FragmentResult::Complete(bytes) => completed = Some(bytes),
                    FragmentResult::Pending => {}
                    other => panic!("unexpected fragment result: {other:?}"),
                }
            }
            assert_eq!(completed.unwrap(), source);
            assert_eq!(receiver.bytes, 0);
            assert!(receiver.entries.is_empty());
        }
    }

    #[test]
    fn rejects_conflicts_mismatched_totals_oversized_and_expired_state() {
        let now = Instant::now();
        let full = vec![7; 500];
        let mut receiver = ReliableFragmentReassembler::default();
        assert_eq!(
            receiver.push(96, &packet(1, 0, 2, &full), now),
            FragmentResult::Pending
        );
        assert_eq!(
            receiver.push(96, &packet(1, 1, 3, b"x"), now),
            FragmentResult::Invalid
        );
        assert!(receiver.entries.is_empty());
        assert_eq!(
            receiver.push(96, &packet(2, 0, 2, &full), now),
            FragmentResult::Pending
        );
        let mut conflict = full.clone();
        conflict[0] ^= 1;
        assert_eq!(
            receiver.push(96, &packet(2, 0, 2, &conflict), now),
            FragmentResult::Invalid
        );
        assert!(receiver.entries.is_empty());
        assert_eq!(
            receiver.push(96, &packet(3, 0, MAX_REASSEMBLY_PARTS + 1, &full), now),
            FragmentResult::Invalid
        );
        for id in 4..(4 + MAX_REASSEMBLIES) {
            assert_eq!(
                receiver.push(96, &packet(id as u16, 0, 2, &full), now),
                FragmentResult::Pending
            );
        }
        assert_eq!(
            receiver.push(96, &packet((4 + MAX_REASSEMBLIES) as u16, 0, 2, &full), now),
            FragmentResult::Invalid
        );
        let expired_at = now + FRAGMENT_EXPIRY + Duration::from_millis(1);
        assert_eq!(
            receiver.push(
                96,
                &packet((5 + MAX_REASSEMBLIES) as u16, 0, 2, &full),
                expired_at
            ),
            FragmentResult::Invalid
        );
        assert!(receiver.entries.is_empty());
        assert_eq!(
            receiver.push(
                96,
                &packet((6 + MAX_REASSEMBLIES) as u16, 0, 2, &full),
                expired_at
            ),
            FragmentResult::Pending
        );
        assert!(receiver.bytes <= MAX_REASSEMBLY_BYTES);
    }

    #[test]
    fn rejects_inconsistent_nonfinal_fragment_lengths() {
        let now = Instant::now();
        let mut receiver = ReliableFragmentReassembler::default();
        assert_eq!(
            receiver.push(96, &packet(12, 2, 3, b"last"), now),
            FragmentResult::Pending
        );
        assert_eq!(
            receiver.push(96, &packet(12, 0, 3, &[1; 500]), now),
            FragmentResult::Pending
        );
        assert_eq!(
            receiver.push(96, &packet(12, 1, 3, &[2; 499]), now),
            FragmentResult::Invalid
        );
        assert!(receiver.entries.is_empty());
    }

    #[test]
    fn reassembles_full_receive_window_of_interleaved_fragments() {
        let now = Instant::now();
        let first = vec![0x31; 500];
        let last = vec![0x72; 17];
        let mut receiver = ReliableFragmentReassembler::default();
        for id in 0..MAX_REASSEMBLIES {
            assert_eq!(
                receiver.push(96, &packet(id as u16, 0, 2, &first), now),
                FragmentResult::Pending
            );
        }
        for id in (0..MAX_REASSEMBLIES).rev() {
            assert_eq!(
                receiver.push(96, &packet(id as u16, 1, 2, &last), now),
                FragmentResult::Complete([first.clone(), last.clone()].concat())
            );
        }
        assert_eq!(receiver.bytes, 0);
        assert!(receiver.entries.is_empty());
    }
}
