use basis_protocol::{
    channels,
    io::NetWriter,
    server_info::{
        parse_server_info_query_nonce, ServerInfoResponse, SERVER_INFO_MIN_REQUEST_BYTES,
        SERVER_INFO_PROTOCOL_VERSION, SERVER_INFO_QUERY_MAGIC,
    },
    version::LITENETLIB_PROTOCOL_ID,
};
use bytes::Bytes;
use dashmap::DashMap;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::{net::UdpSocket, sync::mpsc, time};
use tracing::{debug, trace, warn};

pub type PeerId = u16;

const DEFAULT_WINDOW_SIZE: usize = 128;
const MAX_SEQUENCE: u16 = 32768;
const MAX_PENDING_RELIABLE_PER_PEER: usize = 4096;
const SOCKET_BUFFER_SIZE: usize = 32 * 1024 * 1024;
const SOCKET_TTL: u32 = 255;
const MAX_MERGED_PACKET_SIZE: usize = 1200;
const LITENETLIB_INITIAL_MTU: usize = 1024;
// The LiteNetLib client accepts this ladder. Grow only after it echoes our exact probe.
const LITENETLIB_MTU_STEPS: [usize; 6] = [1024, 1164, 1392, 1404, 1424, 1432];
const MTU_PROBE_INTERVAL: Duration = Duration::from_secs(1);
const MAX_MTU_PROBE_ATTEMPTS: u8 = 4;
const LITENETLIB_CHANNELED_HEADER_SIZE: usize = 4;
const LITENETLIB_FRAGMENT_HEADER_SIZE: usize = 6;
const LITENETLIB_FRAGMENTED_HEADER_SIZE: usize =
    LITENETLIB_CHANNELED_HEADER_SIZE + LITENETLIB_FRAGMENT_HEADER_SIZE;
const RELIABLE_FRAGMENT_PAYLOAD_SIZE: usize =
    LITENETLIB_INITIAL_MTU - LITENETLIB_FRAGMENTED_HEADER_SIZE;
const DEFAULT_MAX_RECEIVE_WORKERS: usize = 8;

/// Reliable window refill cadence. This is the throughput ceiling: a peer can move at most
/// `DEFAULT_WINDOW_SIZE` messages per channel per pass, so the aggregate reliable rate is
/// `DEFAULT_WINDOW_SIZE / RELIABLE_DISPATCH_INTERVAL` per peer per channel. The previous
/// 15 ms pass capped every peer at ~8.5k messages/s and, because the loop also had to walk
/// every peer's pending map twice, it could not even sustain that under load.
const RELIABLE_DISPATCH_INTERVAL: Duration = Duration::from_millis(2);

/// Retransmit and keepalive cadence. Nothing here needs millisecond resolution: a lost
/// reliable packet is only declared late after 150 ms anyway, and pings go out every 1500 ms.
const RELIABLE_MAINTENANCE_INTERVAL: Duration = Duration::from_millis(150);
const RELIABLE_RETRANSMIT_AFTER: Duration = Duration::from_millis(150);
const PEER_PING_INTERVAL: Duration = Duration::from_millis(1500);

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("event channel closed")]
    EventChannelClosed,
}

pub type Result<T> = std::result::Result<T, TransportError>;

pub trait UnreliablePacket {
    fn channel(&self) -> u8;
    fn payload(&self) -> &[u8];
    fn interval_patch(&self) -> Option<(usize, u8)>;
}

impl<P: AsRef<[u8]>> UnreliablePacket for (u8, P, Option<(usize, u8)>) {
    #[inline]
    fn channel(&self) -> u8 {
        self.0
    }

    #[inline]
    fn payload(&self) -> &[u8] {
        self.1.as_ref()
    }

    #[inline]
    fn interval_patch(&self) -> Option<(usize, u8)> {
        self.2
    }
}

impl UnreliablePacket for (u8, Bytes) {
    #[inline]
    fn channel(&self) -> u8 {
        self.0
    }

    #[inline]
    fn payload(&self) -> &[u8] {
        self.1.as_ref()
    }

    #[inline]
    fn interval_patch(&self) -> Option<(usize, u8)> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketProperty {
    Unreliable = 0,
    Channeled = 1,
    Ack = 2,
    Ping = 3,
    Pong = 4,
    ConnectRequest = 5,
    ConnectAccept = 6,
    Disconnect = 7,
    UnconnectedMessage = 8,
    MtuCheck = 9,
    MtuOk = 10,
    Broadcast = 11,
    Merged = 12,
    ShutdownOk = 13,
    PeerNotFound = 14,
    InvalidProtocol = 15,
    NatMessage = 16,
    Empty = 17,
    CompactMerged = 18,
}

impl PacketProperty {
    pub fn from_byte(value: u8) -> Option<Self> {
        Some(match value & 0x1f {
            0 => Self::Unreliable,
            1 => Self::Channeled,
            2 => Self::Ack,
            3 => Self::Ping,
            4 => Self::Pong,
            5 => Self::ConnectRequest,
            6 => Self::ConnectAccept,
            7 => Self::Disconnect,
            8 => Self::UnconnectedMessage,
            9 => Self::MtuCheck,
            10 => Self::MtuOk,
            11 => Self::Broadcast,
            12 => Self::Merged,
            13 => Self::ShutdownOk,
            14 => Self::PeerNotFound,
            15 => Self::InvalidProtocol,
            16 => Self::NatMessage,
            17 => Self::Empty,
            18 => Self::CompactMerged,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DeliveryMethod {
    ReliableUnordered = 0,
    Sequenced = 1,
    ReliableOrdered = 2,
    ReliableSequenced = 3,
    Unreliable = 4,
}

impl DeliveryMethod {
    pub fn from_channel_id(channel_id: u8) -> Self {
        match channel_id % 4 {
            0 => Self::ReliableUnordered,
            1 => Self::Sequenced,
            2 => Self::ReliableOrdered,
            _ => Self::ReliableSequenced,
        }
    }

    pub fn channel_id(channel: u8, delivery: Self) -> u8 {
        channel * 4
            + match delivery {
                Self::ReliableUnordered => 0,
                Self::Sequenced => 1,
                Self::ReliableOrdered => 2,
                Self::ReliableSequenced => 3,
                Self::Unreliable => 1,
            }
    }
}

#[derive(Debug, Clone)]
pub struct ConnectionRequest {
    pub remote_addr: SocketAddr,
    pub payload: Bytes,
    connection_number: u8,
    connect_time: i64,
    pub local_peer_id: i32,
}

#[derive(Debug, Clone)]
pub struct PeerSnapshot {
    pub id: PeerId,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone)]
pub enum DisconnectReason {
    Remote,
    Timeout,
    Rejected(String),
}

#[derive(Debug, Clone)]
pub enum ServerEvent {
    ConnectionRequest(ConnectionRequest),
    PeerConnected(PeerId),
    PeerDisconnected {
        peer: PeerId,
        reason: DisconnectReason,
    },
    Message {
        peer: PeerId,
        channel: u8,
        delivery: DeliveryMethod,
        payload: Bytes,
    },
    NetworkError(String),
    UnconnectedRequest {
        remote_addr: SocketAddr,
        nonce: u16,
        payload: Bytes,
    },
    NatIntroductionRequest {
        remote_addr: SocketAddr,
        local_addr: SocketAddr,
        token: String,
    },
}

#[derive(Debug, Clone, Default)]
pub struct TransportStatsSnapshot {
    pub raw_packets_received: u64,
    pub raw_packets_sent: u64,
    pub raw_bytes_received: u64,
    pub raw_bytes_sent: u64,
    pub raw_send_would_block: u64,
    /// Non-reliable datagrams discarded after a nonblocking send returned WouldBlock.
    pub non_reliable_dropped_datagrams: u64,
    pub reliable_window_fills: u64,
    pub reliable_retransmits: u64,
    pub reliable_dispatch_passes: u64,
    pub reliable_peers_visited: u64,
    pub reliable_acks_received: u64,
    pub reliable_acks_released: u64,
    pub reliable_acks_unknown_channel: u64,
    pub reliable_window_stalls: u64,
}

/// Instantaneous totals over currently connected transport peers. Payload/fragment counts
/// and datagram counts have different units and may overlap; do not sum them as a backlog.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportDepthSnapshot {
    pub peers: usize,
    pub reliable_pending: usize,
    pub reliable_queued: usize,
    pub pending_datagrams: usize,
}

#[derive(Debug)]
struct TransportStats {
    enabled: AtomicBool,
    extended_enabled: AtomicBool,
    raw_packets_received: AtomicU64,
    raw_packets_sent: AtomicU64,
    raw_bytes_received: AtomicU64,
    raw_bytes_sent: AtomicU64,
    raw_send_would_block: AtomicU64,
    non_reliable_dropped_datagrams: AtomicU64,
    /// Reliable messages promoted from the outgoing queue into the send window, and
    /// retransmissions emitted, so reliable throughput can be verified from the outside.
    reliable_window_fills: AtomicU64,
    reliable_retransmits: AtomicU64,
    reliable_dispatch_passes: AtomicU64,
    reliable_peers_visited: AtomicU64,
    /// ACK datagrams that named a channel this peer has in flight, and how many in-flight
    /// packets those ACKs actually released. A large received-but-zero-released count means
    /// the two sides disagree about the window encoding; equal counts mean the window is
    /// healthy and the queue is being fed faster than it drains.
    reliable_acks_received: AtomicU64,
    reliable_acks_released: AtomicU64,
    reliable_acks_unknown_channel: AtomicU64,
    /// Dispatch passes where a channel had queued payloads but no free window space.
    reliable_window_stalls: AtomicU64,
}

impl TransportStats {
    fn new(enabled: bool, extended_enabled: bool) -> Self {
        Self {
            enabled: AtomicBool::new(enabled),
            extended_enabled: AtomicBool::new(extended_enabled),
            raw_packets_received: AtomicU64::new(0),
            raw_packets_sent: AtomicU64::new(0),
            raw_bytes_received: AtomicU64::new(0),
            raw_bytes_sent: AtomicU64::new(0),
            raw_send_would_block: AtomicU64::new(0),
            non_reliable_dropped_datagrams: AtomicU64::new(0),
            reliable_window_fills: AtomicU64::new(0),
            reliable_retransmits: AtomicU64::new(0),
            reliable_dispatch_passes: AtomicU64::new(0),
            reliable_peers_visited: AtomicU64::new(0),
            reliable_acks_received: AtomicU64::new(0),
            reliable_acks_released: AtomicU64::new(0),
            reliable_acks_unknown_channel: AtomicU64::new(0),
            reliable_window_stalls: AtomicU64::new(0),
        }
    }

    fn reset(&self) {
        self.raw_packets_received.store(0, Ordering::Relaxed);
        self.raw_packets_sent.store(0, Ordering::Relaxed);
        self.raw_bytes_received.store(0, Ordering::Relaxed);
        self.raw_bytes_sent.store(0, Ordering::Relaxed);
        self.raw_send_would_block.store(0, Ordering::Relaxed);
        self.non_reliable_dropped_datagrams
            .store(0, Ordering::Relaxed);
    }

    fn reset_extended(&self) {
        self.reliable_window_fills.store(0, Ordering::Relaxed);
        self.reliable_retransmits.store(0, Ordering::Relaxed);
        self.reliable_dispatch_passes.store(0, Ordering::Relaxed);
        self.reliable_peers_visited.store(0, Ordering::Relaxed);
        self.reliable_acks_received.store(0, Ordering::Relaxed);
        self.reliable_acks_released.store(0, Ordering::Relaxed);
        self.reliable_acks_unknown_channel
            .store(0, Ordering::Relaxed);
        self.reliable_window_stalls.store(0, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct PeerState {
    id: PeerId,
    addr: SocketAddr,
    connection_number: u8,
    connect_time: i64,
    last_seen: parking_lot::Mutex<Instant>,
    last_ping_sent: parking_lot::Mutex<Instant>,
    next_ping_sequence: AtomicU16,
    next_reliable_sequence: parking_lot::Mutex<HashMap<u8, u16>>,
    next_sequenced_sequence: parking_lot::Mutex<HashMap<u8, u16>>,
    remote_sequenced_sequence: parking_lot::Mutex<HashMap<u8, u16>>,
    next_fragment_id: AtomicU16,
    /// In-flight reliable packets grouped by channel id. Each channel is an ordered deque so
    /// that (a) the number of unacknowledged packets is `len()` in O(1) instead of a
    /// per-pass HashMap build, and (b) an ACK only has to pop from the front, turning ACK
    /// processing from O(pending) into O(acked).
    pending_reliable: parking_lot::Mutex<HashMap<u8, VecDeque<PendingReliable>>>,
    /// Mirror of the total length of every `pending_reliable` queue. Kept as a counter so the
    /// overflow guard and the status line stay O(1) instead of walking every in-flight packet.
    pending_total: AtomicUsize,
    /// Datagrams not yet accepted by the kernel. Kept per peer so one full socket send queue
    /// cannot block dispatch to other peers or lose ACKs cleared while building the batch.
    /// Both producers share `reliable_send_turn`; a blocked retry skips batch construction, so
    /// this holds at most one batch, bounded by MAX_PENDING_RELIABLE_PER_PEER reliable packets
    /// plus dirty ACKs for at most 256 channel IDs.
    pending_datagrams: parking_lot::Mutex<VecDeque<Vec<u8>>>,
    /// The dispatcher and retransmit loop share one peer send queue. A nonblocking try-lock
    /// gives one loop exclusive ownership for a peer turn without making either loop wait.
    reliable_send_turn: parking_lot::Mutex<()>,
    outgoing_reliable: parking_lot::Mutex<HashMap<u8, VecDeque<OutgoingReliable>>>,
    outgoing_acks: parking_lot::Mutex<HashMap<u8, AckState>>,
    /// Set whenever this peer has queued payloads, in-flight packets, or dirty ACKs. The
    /// dispatch loop skips peers without it, so a settled server stops paying per-peer polling cost.
    reliable_active: AtomicBool,
    confirmed_mtu: AtomicUsize,
    mtu_probe: parking_lot::Mutex<MtuProbeState>,
}

#[derive(Debug)]
struct MtuProbeState {
    next_step: usize,
    attempts: u8,
    next_at: Instant,
    pending: Option<(usize, u64)>,
}

impl MtuProbeState {
    fn new(now: Instant) -> Self {
        Self {
            next_step: 1,
            attempts: 0,
            next_at: now + MTU_PROBE_INTERVAL,
            pending: None,
        }
    }

    fn next_packet(&mut self, now: Instant, connection_number: u8) -> Option<Vec<u8>> {
        if now < self.next_at || self.next_step >= LITENETLIB_MTU_STEPS.len() {
            return None;
        }
        if self.attempts >= MAX_MTU_PROBE_ATTEMPTS {
            self.next_step = LITENETLIB_MTU_STEPS.len();
            self.pending = None;
            return None;
        }
        let mtu = LITENETLIB_MTU_STEPS[self.next_step];
        let token = rand::random::<u64>();
        self.attempts += 1;
        self.next_at = now + MTU_PROBE_INTERVAL;
        self.pending = Some((mtu, token));
        let mut packet = vec![0; mtu];
        packet[0] = PacketProperty::MtuCheck as u8 | (connection_number << 5);
        packet[1..5].copy_from_slice(&(mtu as i32).to_le_bytes());
        packet[5..13].copy_from_slice(&token.to_le_bytes());
        packet[mtu - 4..].copy_from_slice(&(mtu as i32).to_le_bytes());
        Some(packet)
    }

    fn accept_response(&mut self, bytes: &[u8], connection_number: u8) -> Option<usize> {
        let (mtu, token) = self.pending?;
        if bytes.len() != mtu
            || bytes[0] != (PacketProperty::MtuOk as u8 | (connection_number << 5))
            || bytes[1..5] != (mtu as i32).to_le_bytes()
            || bytes[5..13] != token.to_le_bytes()
            || bytes[13..mtu - 4].iter().any(|&byte| byte != 0)
            || bytes[mtu - 4..] != (mtu as i32).to_le_bytes()
        {
            return None;
        }
        self.pending = None;
        self.next_step += 1;
        self.attempts = 0;
        self.next_at = Instant::now() + MTU_PROBE_INTERVAL;
        Some(mtu)
    }
}

fn try_send_mtu_probe(
    state: &mut MtuProbeState,
    now: Instant,
    connection_number: u8,
    mut send: impl FnMut(&[u8]) -> Result<bool>,
) -> Result<()> {
    let previous_pending = state.pending;
    let Some(packet) = state.next_packet(now, connection_number) else {
        return Ok(());
    };
    match send(&packet) {
        Ok(true) => Ok(()),
        result => {
            // No new token was sent. Keep any earlier in-flight probe valid, and retry this
            // step after the usual interval without spending an attempt.
            state.attempts -= 1;
            state.pending = previous_pending;
            result.map(|_| ())
        }
    }
}

impl PeerState {
    fn total_pending(&self) -> usize {
        self.pending_total.load(Ordering::Relaxed)
    }

    fn total_queued(&self) -> usize {
        self.outgoing_reliable
            .lock()
            .values()
            .map(VecDeque::len)
            .sum()
    }

    /// Clear `reliable_active` once nothing is queued, in flight, or waiting to be ACKed.
    /// Keep all producer locks held through the clear so a concurrent producer either becomes
    /// visible here or sets the flag again after publishing its work.
    fn refresh_reliable_active(&self) {
        let datagrams = self.pending_datagrams.lock();
        if !datagrams.is_empty() {
            return;
        }
        let pending = self.pending_reliable.lock();
        if pending.values().any(|queue| !queue.is_empty()) {
            return;
        }
        let outgoing = self.outgoing_reliable.lock();
        if outgoing.values().any(|queue| !queue.is_empty()) {
            return;
        }
        let acks = self.outgoing_acks.lock();
        if acks.values().any(|ack| ack.dirty) {
            return;
        }
        self.reliable_active.store(false, Ordering::Release);
    }
}

#[derive(Debug, Clone)]
struct PendingReliable {
    sequence: u16,
    bytes: Vec<u8>,
    last_sent: Instant,
}

#[derive(Debug, Clone)]
struct OutgoingReliable {
    payload: Vec<u8>,
    fragment: Option<ReliableFragment>,
}

#[derive(Debug, Clone, Copy)]
struct ReliableFragment {
    id: u16,
    part: u16,
    total: u16,
}

#[derive(Debug, Clone)]
struct AckState {
    window_start: u16,
    bits: Vec<u8>,
    /// Set by `queue_ack` whenever a new bit is set or the window advances, and cleared by
    /// the dispatch loop once the packet has been built. Previously every pass drained and
    /// reallocated the whole window for every channel of every peer, even when nothing had
    /// been received since the previous pass.
    dirty: bool,
}

#[derive(Debug, Clone, Copy)]
struct PendingRequestInfo {
    connect_time: i64,
    connection_number: u8,
}

#[derive(Clone)]
pub struct TransportHandle {
    socket: Arc<UdpSocket>,
    peers: Arc<DashMap<PeerId, Arc<PeerState>>>,
    by_addr: Arc<DashMap<SocketAddr, PeerId>>,
    pending_requests: Arc<DashMap<SocketAddr, PendingRequestInfo>>,
    next_peer_id: Arc<AtomicU16>,
    reusable_peer_ids: Arc<parking_lot::Mutex<VecDeque<PeerId>>>,
    retired_peer_ids: Arc<parking_lot::Mutex<HashSet<PeerId>>>,
    shutdown: Arc<AtomicBool>,
    stats: Arc<TransportStats>,
    #[cfg(test)]
    test_blocked_send_addrs: Arc<parking_lot::RwLock<HashSet<SocketAddr>>>,
}

impl TransportHandle {
    pub async fn bind(addr: SocketAddr) -> Result<(Self, mpsc::Receiver<ServerEvent>)> {
        Self::bind_with_statistics_options(addr, true, true).await
    }

    pub async fn bind_with_statistics(
        addr: SocketAddr,
        enable_statistics: bool,
    ) -> Result<(Self, mpsc::Receiver<ServerEvent>)> {
        Self::bind_with_statistics_options(addr, enable_statistics, enable_statistics).await
    }

    pub async fn bind_with_statistics_options(
        addr: SocketAddr,
        enable_statistics: bool,
        enable_extended_statistics: bool,
    ) -> Result<(Self, mpsc::Receiver<ServerEvent>)> {
        let socket = Arc::new(bind_udp_socket(addr)?);
        let (tx, rx) = mpsc::channel(262_144);
        let handle = Self {
            socket: socket.clone(),
            peers: Arc::new(DashMap::new()),
            by_addr: Arc::new(DashMap::new()),
            pending_requests: Arc::new(DashMap::new()),
            next_peer_id: Arc::new(AtomicU16::new(0)),
            reusable_peer_ids: Arc::new(parking_lot::Mutex::new(VecDeque::new())),
            retired_peer_ids: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            stats: Arc::new(TransportStats::new(
                enable_statistics,
                enable_extended_statistics,
            )),
            #[cfg(test)]
            test_blocked_send_addrs: Arc::new(parking_lot::RwLock::new(HashSet::new())),
        };
        for _ in 0..udp_receive_worker_count() {
            tokio::spawn(read_loop(handle.clone(), tx.clone()));
        }
        tokio::spawn(timeout_loop(handle.clone(), tx));
        tokio::spawn(reliable_dispatch_loop(handle.clone()));
        tokio::spawn(reliable_maintenance_loop(handle.clone()));
        Ok((handle, rx))
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn connected_peers_count(&self) -> usize {
        self.peers.len()
    }

    /// The outbound datagram size confirmed for this peer. Until an exact MTU probe reply,
    /// retain the transport's previous 1200-byte packing limit.
    pub fn peer_mtu(&self, peer: PeerId) -> usize {
        self.peers
            .get(&peer)
            .map(|state| state.confirmed_mtu.load(Ordering::Relaxed))
            .unwrap_or(MAX_MERGED_PACKET_SIZE)
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    pub fn pending_reliable_count(&self) -> usize {
        self.peers.iter().map(|peer| peer.total_pending()).sum()
    }

    pub fn queued_reliable_count(&self) -> usize {
        self.peers.iter().map(|peer| peer.total_queued()).sum()
    }

    /// Samples actual Rust queues, taking one queue lock at a time. Concurrent sends/ACKs
    /// can change them during sampling, so this is not an atomic cross-queue snapshot.
    pub fn depths_snapshot(&self) -> TransportDepthSnapshot {
        let mut snapshot = TransportDepthSnapshot::default();
        for peer in self.peers.iter() {
            snapshot.peers += 1;
            snapshot.reliable_pending += peer.total_pending();
            snapshot.reliable_queued += peer.total_queued();
            snapshot.pending_datagrams += peer.pending_datagrams.lock().len();
        }
        snapshot
    }

    pub fn set_statistics_enabled(&self, enabled: bool) {
        let was_enabled = self.stats.enabled.load(Ordering::Relaxed);
        if enabled && !was_enabled {
            self.stats.reset();
        }
        self.stats.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn statistics_enabled(&self) -> bool {
        self.stats.enabled.load(Ordering::Relaxed)
    }

    pub fn set_extended_statistics_enabled(&self, enabled: bool) {
        let was_enabled = self.stats.extended_enabled.load(Ordering::Relaxed);
        if enabled && !was_enabled {
            self.stats.reset_extended();
        }
        self.stats
            .extended_enabled
            .store(enabled, Ordering::Relaxed);
    }

    pub fn extended_statistics_enabled(&self) -> bool {
        self.stats.extended_enabled.load(Ordering::Relaxed)
    }

    pub fn stats_snapshot(&self) -> TransportStatsSnapshot {
        let statistics_enabled = self.statistics_enabled();
        let extended_enabled = self.extended_statistics_enabled();
        if !statistics_enabled && !extended_enabled {
            return TransportStatsSnapshot::default();
        }
        TransportStatsSnapshot {
            raw_packets_received: if statistics_enabled {
                self.stats.raw_packets_received.load(Ordering::Relaxed)
            } else {
                0
            },
            raw_packets_sent: if statistics_enabled {
                self.stats.raw_packets_sent.load(Ordering::Relaxed)
            } else {
                0
            },
            raw_bytes_received: if statistics_enabled {
                self.stats.raw_bytes_received.load(Ordering::Relaxed)
            } else {
                0
            },
            raw_bytes_sent: if statistics_enabled {
                self.stats.raw_bytes_sent.load(Ordering::Relaxed)
            } else {
                0
            },
            raw_send_would_block: if statistics_enabled {
                self.stats.raw_send_would_block.load(Ordering::Relaxed)
            } else {
                0
            },
            non_reliable_dropped_datagrams: if statistics_enabled {
                self.stats
                    .non_reliable_dropped_datagrams
                    .load(Ordering::Relaxed)
            } else {
                0
            },
            reliable_window_fills: if extended_enabled {
                self.stats.reliable_window_fills.load(Ordering::Relaxed)
            } else {
                0
            },
            reliable_retransmits: if extended_enabled {
                self.stats.reliable_retransmits.load(Ordering::Relaxed)
            } else {
                0
            },
            reliable_dispatch_passes: if extended_enabled {
                self.stats.reliable_dispatch_passes.load(Ordering::Relaxed)
            } else {
                0
            },
            reliable_peers_visited: if extended_enabled {
                self.stats.reliable_peers_visited.load(Ordering::Relaxed)
            } else {
                0
            },
            reliable_acks_received: if extended_enabled {
                self.stats.reliable_acks_received.load(Ordering::Relaxed)
            } else {
                0
            },
            reliable_acks_released: if extended_enabled {
                self.stats.reliable_acks_released.load(Ordering::Relaxed)
            } else {
                0
            },
            reliable_acks_unknown_channel: if extended_enabled {
                self.stats
                    .reliable_acks_unknown_channel
                    .load(Ordering::Relaxed)
            } else {
                0
            },
            reliable_window_stalls: if extended_enabled {
                self.stats.reliable_window_stalls.load(Ordering::Relaxed)
            } else {
                0
            },
        }
    }

    async fn send_raw_to(&self, bytes: &[u8], addr: SocketAddr) -> Result<usize> {
        let sent = self.socket.send_to(bytes, addr).await?;
        if self.statistics_enabled() {
            self.stats.raw_packets_sent.fetch_add(1, Ordering::Relaxed);
            self.stats
                .raw_bytes_sent
                .fetch_add(sent as u64, Ordering::Relaxed);
        }
        Ok(sent)
    }

    fn try_send_raw_to(&self, bytes: &[u8], addr: SocketAddr) -> Result<bool> {
        #[cfg(test)]
        if self.test_blocked_send_addrs.read().contains(&addr) {
            if self.statistics_enabled() {
                self.stats
                    .raw_send_would_block
                    .fetch_add(1, Ordering::Relaxed);
            }
            return Ok(false);
        }
        match self.socket.try_send_to(bytes, addr) {
            Ok(sent) => {
                if self.statistics_enabled() {
                    self.stats.raw_packets_sent.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .raw_bytes_sent
                        .fetch_add(sent as u64, Ordering::Relaxed);
                }
                Ok(true)
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                if self.statistics_enabled() {
                    self.stats
                        .raw_send_would_block
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(false)
            }
            Err(err) => Err(err.into()),
        }
    }

    /// These callers discard a refused datagram. Reliable/ACK/MTU callers use
    /// try_send_raw_to directly and must not be counted as non-reliable drops.
    fn try_send_non_reliable_raw_to(&self, bytes: &[u8], addr: SocketAddr) -> Result<bool> {
        let sent = self.try_send_raw_to(bytes, addr)?;
        if !sent && self.statistics_enabled() {
            self.stats
                .non_reliable_dropped_datagrams
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(sent)
    }

    pub fn peer_snapshots(&self) -> Vec<PeerSnapshot> {
        self.peers
            .iter()
            .map(|p| PeerSnapshot {
                id: *p.key(),
                addr: p.addr,
            })
            .collect()
    }

    pub async fn accept(&self, request: &ConnectionRequest) -> Result<PeerId> {
        self.pending_requests.remove(&request.remote_addr);
        let id = self.allocate_peer_id();
        let state = Arc::new(PeerState {
            id,
            addr: request.remote_addr,
            connection_number: request.connection_number,
            connect_time: request.connect_time,
            last_seen: parking_lot::Mutex::new(Instant::now()),
            last_ping_sent: parking_lot::Mutex::new(Instant::now()),
            next_ping_sequence: AtomicU16::new(0),
            next_reliable_sequence: parking_lot::Mutex::new(HashMap::new()),
            next_sequenced_sequence: parking_lot::Mutex::new(HashMap::new()),
            remote_sequenced_sequence: parking_lot::Mutex::new(HashMap::new()),
            next_fragment_id: AtomicU16::new(0),
            pending_reliable: parking_lot::Mutex::new(HashMap::new()),
            pending_total: AtomicUsize::new(0),
            pending_datagrams: parking_lot::Mutex::new(VecDeque::new()),
            reliable_send_turn: parking_lot::Mutex::new(()),
            outgoing_reliable: parking_lot::Mutex::new(HashMap::new()),
            outgoing_acks: parking_lot::Mutex::new(HashMap::new()),
            reliable_active: AtomicBool::new(false),
            confirmed_mtu: AtomicUsize::new(MAX_MERGED_PACKET_SIZE),
            mtu_probe: parking_lot::Mutex::new(MtuProbeState::new(Instant::now())),
        });
        self.by_addr.insert(request.remote_addr, id);
        self.peers.insert(id, state);

        send_connect_accept(
            self,
            request.remote_addr,
            request.connection_number,
            request.connect_time,
            id,
        )
        .await?;
        Ok(id)
    }

    fn allocate_peer_id(&self) -> PeerId {
        loop {
            if let Some(id) = self.reusable_peer_ids.lock().pop_front() {
                if !self.peers.contains_key(&id) {
                    return id;
                }
                continue;
            }
            let id = self.next_peer_id.fetch_add(1, Ordering::SeqCst);
            if !self.peers.contains_key(&id) {
                return id;
            }
        }
    }

    pub async fn reject(&self, request: &ConnectionRequest, reason: &str) -> Result<()> {
        let mut payload = NetWriter::new();
        payload.put_string(reason);
        self.reject_payload(request, payload.as_slice()).await
    }

    pub async fn reject_payload(&self, request: &ConnectionRequest, payload: &[u8]) -> Result<()> {
        self.pending_requests.remove(&request.remote_addr);
        let mut writer = NetWriter::with_capacity(payload.len() + 9);
        writer.put_u8(PacketProperty::Disconnect as u8 | (request.connection_number << 5));
        writer.put_i64(request.connect_time);
        writer.put_bytes(payload);
        self.send_raw_to(writer.as_slice(), request.remote_addr)
            .await?;
        Ok(())
    }

    pub fn recycle_peer_id(&self, peer: PeerId) {
        if self.peers.contains_key(&peer) {
            return;
        }
        if !self.retired_peer_ids.lock().remove(&peer) {
            return;
        }
        let mut reusable = self.reusable_peer_ids.lock();
        if !reusable.iter().any(|id| *id == peer) {
            reusable.push_back(peer);
        }
    }

    pub async fn send(
        &self,
        peer: PeerId,
        channel: u8,
        delivery: DeliveryMethod,
        payload: &[u8],
    ) -> Result<()> {
        let Some(state) = self.peers.get(&peer).map(|p| p.clone()) else {
            return Ok(());
        };
        if is_reliable_delivery(delivery) {
            enqueue_reliable_payload(&state, channel, delivery, payload);
            return Ok(());
        }
        let built = build_outbound_packet(&state, channel, delivery, payload);
        if let Some((channel_id, sequence)) = built.reliable_key {
            // A retained copy is required for retransmit, so this path still copies once.
            record_pending_reliable(
                &state,
                channel_id,
                sequence,
                built.bytes.clone(),
                Some(&self.stats),
            );
        }
        self.send_raw_to(&built.bytes, state.addr).await?;
        Ok(())
    }

    pub async fn send_many(
        &self,
        peer: PeerId,
        packets: &[(u8, DeliveryMethod, Vec<u8>)],
    ) -> Result<()> {
        let borrowed = packets
            .iter()
            .map(|(channel, delivery, payload)| (*channel, *delivery, payload.as_slice()))
            .collect::<Vec<_>>();
        self.send_many_slices(peer, &borrowed).await
    }

    pub async fn send_many_bytes(
        &self,
        peer: PeerId,
        packets: &[(u8, DeliveryMethod, Bytes)],
    ) -> Result<()> {
        let borrowed = packets
            .iter()
            .map(|(channel, delivery, payload)| (*channel, *delivery, payload.as_ref()))
            .collect::<Vec<_>>();
        self.send_many_slices(peer, &borrowed).await
    }

    pub fn try_send_many_bytes(
        &self,
        peer: PeerId,
        packets: &[(u8, DeliveryMethod, Bytes)],
    ) -> Result<usize> {
        if packets.is_empty() {
            return Ok(0);
        }
        let Some(state) = self.peers.get(&peer).map(|p| p.clone()) else {
            return Ok(0);
        };

        let mut outbound = Vec::with_capacity(packets.len());
        for (channel, delivery, payload) in packets {
            if is_reliable_delivery(*delivery) {
                enqueue_reliable_payload(&state, *channel, *delivery, payload);
                continue;
            }
            let built = build_outbound_packet(&state, *channel, *delivery, payload.as_ref());
            if let Some((channel_id, sequence)) = built.reliable_key {
                record_pending_reliable(
                    &state,
                    channel_id,
                    sequence,
                    built.bytes.clone(),
                    Some(&self.stats),
                );
            }
            outbound.push(built.bytes);
        }

        let mut sent = 0usize;
        for packet in build_merged_datagrams(state.connection_number, outbound) {
            if self.try_send_non_reliable_raw_to(&packet, state.addr)? {
                sent += 1;
            }
        }
        Ok(sent)
    }

    pub fn try_send_many_unreliable_bytes(
        &self,
        peer: PeerId,
        packets: &[(u8, Bytes)],
    ) -> Result<usize> {
        self.try_send_many_unreliable_packets(peer, packets)
    }

    pub fn try_send_many_unreliable_packets<T: UnreliablePacket>(
        &self,
        peer: PeerId,
        packets: &[T],
    ) -> Result<usize> {
        if packets.is_empty() {
            return Ok(0);
        }
        let Some(state) = self.peers.get(&peer).map(|p| p.clone()) else {
            return Ok(0);
        };

        if packets.len() == 1 {
            let payload = packets[0].payload();
            let mut packet = Vec::with_capacity(payload.len() + 2);
            packet.push(PacketProperty::Unreliable as u8 | (state.connection_number << 5));
            packet.push(packets[0].channel());
            extend_payload_with_patch(&mut packet, payload, packets[0].interval_patch());
            return self
                .try_send_non_reliable_raw_to(&packet, state.addr)
                .map(usize::from);
        }

        let mut sent = 0usize;
        let mtu = state.confirmed_mtu.load(Ordering::Relaxed);
        let mut current = Vec::with_capacity(mtu);
        current.push(PacketProperty::Merged as u8 | (state.connection_number << 5));
        let mut current_count = 0usize;

        for packet in packets {
            let payload = packet.payload();
            let packet_len = payload.len() + 2;
            let framed_len = packet_len + 2;
            if current_count > 0 && current.len() + framed_len > mtu {
                if self.try_send_non_reliable_raw_to(&current, state.addr)? {
                    sent += 1;
                }
                current.clear();
                current.push(PacketProperty::Merged as u8 | (state.connection_number << 5));
                current_count = 0;
            }

            if framed_len + 1 > mtu {
                let mut oversized = Vec::with_capacity(packet_len);
                oversized.push(PacketProperty::Unreliable as u8 | (state.connection_number << 5));
                oversized.push(packet.channel());
                extend_payload_with_patch(&mut oversized, payload, packet.interval_patch());
                if self.try_send_non_reliable_raw_to(&oversized, state.addr)? {
                    sent += 1;
                }
                continue;
            }

            current.extend_from_slice(&(packet_len as u16).to_le_bytes());
            current.push(PacketProperty::Unreliable as u8 | (state.connection_number << 5));
            current.push(packet.channel());
            extend_payload_with_patch(&mut current, payload, packet.interval_patch());
            current_count += 1;
        }

        if current_count > 0 && self.try_send_non_reliable_raw_to(&current, state.addr)? {
            sent += 1;
        }
        Ok(sent)
    }

    pub async fn send_many_slices(
        &self,
        peer: PeerId,
        packets: &[(u8, DeliveryMethod, &[u8])],
    ) -> Result<()> {
        if packets.is_empty() {
            return Ok(());
        }
        let Some(state) = self.peers.get(&peer).map(|p| p.clone()) else {
            return Ok(());
        };

        let mut outbound = Vec::with_capacity(packets.len());
        for (channel, delivery, payload) in packets {
            if is_reliable_delivery(*delivery) {
                enqueue_reliable_payload(&state, *channel, *delivery, payload);
                continue;
            }
            let built = build_outbound_packet(&state, *channel, *delivery, payload);
            if let Some((channel_id, sequence)) = built.reliable_key {
                record_pending_reliable(
                    &state,
                    channel_id,
                    sequence,
                    built.bytes.clone(),
                    Some(&self.stats),
                );
            }
            outbound.push(built.bytes);
        }

        for packet in build_merged_datagrams(state.connection_number, outbound) {
            self.send_raw_to(&packet, state.addr).await?;
        }
        Ok(())
    }

    pub async fn disconnect(&self, peer: PeerId, reason: &str) -> Result<()> {
        if let Some((_, state)) = self.peers.remove(&peer) {
            self.by_addr.remove(&state.addr);
            self.retire_peer_id(peer);
            let mut payload = NetWriter::new();
            payload.put_string(reason);
            let mut writer = NetWriter::with_capacity(payload.len() + 9);
            writer.put_u8(PacketProperty::Disconnect as u8 | (state.connection_number << 5));
            writer.put_i64(state.connect_time);
            writer.put_bytes(payload.as_slice());
            self.send_raw_to(writer.as_slice(), state.addr).await?;
        }
        Ok(())
    }

    fn retire_peer_id(&self, peer: PeerId) {
        self.retired_peer_ids.lock().insert(peer);
    }

    pub async fn send_server_info(
        &self,
        remote_addr: SocketAddr,
        response: &ServerInfoResponse,
    ) -> Result<()> {
        let payload = response.serialize();
        let mut packet = Vec::with_capacity(payload.len() + 1);
        packet.push(PacketProperty::UnconnectedMessage as u8);
        packet.extend_from_slice(&payload);
        self.send_raw_to(&packet, remote_addr).await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn send_nat_introduce(
        &self,
        host_internal: SocketAddr,
        host_external: SocketAddr,
        host_prediction_count: u8,
        client_internal: SocketAddr,
        client_external: SocketAddr,
        client_prediction_count: u8,
        token: &str,
    ) -> Result<()> {
        let to_client = build_nat_introduce_response(
            host_internal,
            host_external,
            token,
            host_prediction_count,
        );
        self.send_raw_to(&to_client, client_external).await?;
        let to_host = build_nat_introduce_response(
            client_internal,
            client_external,
            token,
            client_prediction_count,
        );
        self.send_raw_to(&to_host, host_external).await?;
        Ok(())
    }

    pub fn peer_addr(&self, peer: PeerId) -> Option<SocketAddr> {
        self.peers.get(&peer).map(|state| state.addr)
    }
}

/// Record a freshly built reliable packet as in-flight. The built bytes are moved, not cloned:
/// the merged datagram is assembled from this copy, so cloning here doubled both the
/// allocation count and the memcpy volume on the reliable path.
fn record_pending_reliable(
    state: &PeerState,
    channel_id: u8,
    sequence: u16,
    bytes: Vec<u8>,
    stats: Option<&TransportStats>,
) {
    let mut pending = state.pending_reliable.lock();
    while state.pending_total.load(Ordering::Relaxed) >= MAX_PENDING_RELIABLE_PER_PEER {
        // Shed exactly the single oldest in-flight packet rather than growing without bound.
        // Inspect each queue front first; popping while searching would discard one packet from
        // every non-empty channel and immediately desynchronize `pending_total`.
        let oldest_channel = pending
            .iter()
            .filter_map(|(channel_id, queue)| {
                queue.front().map(|item| (*channel_id, item.last_sent))
            })
            .min_by_key(|(_, last_sent)| *last_sent)
            .map(|(channel_id, _)| channel_id);
        let Some(oldest_channel) = oldest_channel else {
            break;
        };

        let remove_channel = {
            let queue = pending
                .get_mut(&oldest_channel)
                .expect("oldest pending channel disappeared while locked");
            queue.pop_front();
            queue.is_empty()
        };
        if remove_channel {
            pending.remove(&oldest_channel);
        }
        state.pending_total.fetch_sub(1, Ordering::Relaxed);
    }
    pending
        .entry(channel_id)
        .or_default()
        .push_back(PendingReliable {
            sequence,
            bytes,
            last_sent: Instant::now(),
        });
    state.pending_total.fetch_add(1, Ordering::Relaxed);
    drop(pending);
    if let Some(stats) = stats.filter(|stats| stats.extended_enabled.load(Ordering::Relaxed)) {
        stats.reliable_window_fills.fetch_add(1, Ordering::Relaxed);
    }
    state.reliable_active.store(true, Ordering::Release);
}

fn is_reliable_delivery(delivery: DeliveryMethod) -> bool {
    matches!(
        delivery,
        DeliveryMethod::ReliableUnordered
            | DeliveryMethod::ReliableOrdered
            | DeliveryMethod::ReliableSequenced
    )
}

/// Number of new sequence values that fit before the LiteNetLib send-window span reaches 128.
/// The oldest unacknowledged packet, not the number of outstanding packets, anchors the window:
/// an ACK for sequence 1 cannot make room for sequence 128 while sequence 0 is still missing.
fn reliable_window_capacity(next_sequence: u16, oldest_unacked: Option<u16>) -> usize {
    let window_start = oldest_unacked.unwrap_or(next_sequence);
    let span = relative_sequence(next_sequence, window_start);
    if !(0..=DEFAULT_WINDOW_SIZE as i32).contains(&span) {
        return 0;
    }
    DEFAULT_WINDOW_SIZE - span as usize
}

fn dequeue_reliable_window(
    next_sequence: u16,
    pending: &VecDeque<PendingReliable>,
    outgoing: &mut VecDeque<OutgoingReliable>,
    max_count: usize,
) -> Vec<OutgoingReliable> {
    let capacity =
        reliable_window_capacity(next_sequence, pending.front().map(|item| item.sequence));
    let count = capacity.min(outgoing.len()).min(max_count);
    (0..count).filter_map(|_| outgoing.pop_front()).collect()
}

fn enqueue_reliable_payload(
    state: &PeerState,
    channel: u8,
    delivery: DeliveryMethod,
    payload: &[u8],
) {
    let channel_id = DeliveryMethod::channel_id(channel, delivery);
    let mut outgoing = state.outgoing_reliable.lock();
    let queue = outgoing.entry(channel_id).or_default();

    if payload.len() + LITENETLIB_CHANNELED_HEADER_SIZE <= LITENETLIB_INITIAL_MTU
        || !matches!(
            delivery,
            DeliveryMethod::ReliableOrdered | DeliveryMethod::ReliableUnordered
        )
    {
        queue.push_back(OutgoingReliable {
            payload: payload.to_vec(),
            fragment: None,
        });
        state.reliable_active.store(true, Ordering::Release);
        return;
    }

    let total_fragments = payload.len().div_ceil(RELIABLE_FRAGMENT_PAYLOAD_SIZE);
    if total_fragments > u16::MAX as usize {
        warn!(
            "dropping reliable payload requiring {total_fragments} fragments; LiteNetLib limit is {}",
            u16::MAX
        );
        return;
    }

    let fragment_id = state
        .next_fragment_id
        .fetch_add(1, Ordering::SeqCst)
        .wrapping_add(1);
    for (part, chunk) in payload.chunks(RELIABLE_FRAGMENT_PAYLOAD_SIZE).enumerate() {
        queue.push_back(OutgoingReliable {
            payload: chunk.to_vec(),
            fragment: Some(ReliableFragment {
                id: fragment_id,
                part: part as u16,
                total: total_fragments as u16,
            }),
        });
    }
    state.reliable_active.store(true, Ordering::Release);
}

fn build_queued_reliable_packet(
    state: &PeerState,
    channel_id: u8,
    outgoing: OutgoingReliable,
) -> BuiltPacket {
    let sequence = if channel_id % 4 == DeliveryMethod::ReliableSequenced as u8 {
        next_sequenced_channel_sequence(&state.next_reliable_sequence, channel_id)
    } else {
        next_channel_sequence(&state.next_reliable_sequence, channel_id)
    };
    let header_size = if outgoing.fragment.is_some() {
        LITENETLIB_FRAGMENTED_HEADER_SIZE
    } else {
        LITENETLIB_CHANNELED_HEADER_SIZE
    };
    let mut writer = NetWriter::with_capacity(outgoing.payload.len() + header_size);
    let mut header = PacketProperty::Channeled as u8 | (state.connection_number << 5);
    if outgoing.fragment.is_some() {
        header |= 0x80;
    }
    writer.put_u8(header);
    writer.put_u16(sequence);
    writer.put_u8(channel_id);
    if let Some(fragment) = outgoing.fragment {
        writer.put_u16(fragment.id);
        writer.put_u16(fragment.part);
        writer.put_u16(fragment.total);
    }
    writer.put_bytes(&outgoing.payload);
    BuiltPacket {
        bytes: writer.into_vec(),
        sequence,
        reliable_key: Some((channel_id, sequence)),
    }
}

fn build_merged_datagrams(connection_number: u8, packets: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    if packets.len() <= 1 {
        return packets;
    }

    let mut datagrams = Vec::new();
    let mut current = Vec::with_capacity(MAX_MERGED_PACKET_SIZE);
    let mut current_count = 0usize;
    current.push(PacketProperty::Merged as u8 | (connection_number << 5));

    for packet in packets {
        let framed_len = 2 + packet.len();
        if current_count > 0 && current.len() + framed_len > MAX_MERGED_PACKET_SIZE {
            if current_count == 1 {
                datagrams.push(unpack_single_merged_packet(&current));
            } else {
                datagrams.push(current);
            }
            current = Vec::with_capacity(MAX_MERGED_PACKET_SIZE);
            current.push(PacketProperty::Merged as u8 | (connection_number << 5));
            current_count = 0;
        }

        if framed_len + 1 > MAX_MERGED_PACKET_SIZE {
            if current_count > 0 {
                if current_count == 1 {
                    datagrams.push(unpack_single_merged_packet(&current));
                } else {
                    datagrams.push(current);
                }
                current = Vec::with_capacity(MAX_MERGED_PACKET_SIZE);
                current.push(PacketProperty::Merged as u8 | (connection_number << 5));
                current_count = 0;
            }
            datagrams.push(packet);
            continue;
        }

        current.extend_from_slice(&(packet.len() as u16).to_le_bytes());
        current.extend_from_slice(&packet);
        current_count += 1;
    }

    if current_count == 1 {
        datagrams.push(unpack_single_merged_packet(&current));
    } else if current_count > 1 {
        datagrams.push(current);
    }
    datagrams
}

fn unpack_single_merged_packet(merged: &[u8]) -> Vec<u8> {
    if merged.len() < 3 {
        return merged.to_vec();
    }
    let size = u16::from_le_bytes([merged[1], merged[2]]) as usize;
    if merged.len() < 3 + size {
        return merged.to_vec();
    }
    merged[3..3 + size].to_vec()
}

fn bind_udp_socket(addr: SocketAddr) -> std::io::Result<UdpSocket> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    if matches!(addr, SocketAddr::V6(_)) {
        let _ = socket.set_only_v6(false);
    }
    let _ = socket.set_recv_buffer_size(SOCKET_BUFFER_SIZE);
    let _ = socket.set_send_buffer_size(SOCKET_BUFFER_SIZE);
    let _ = socket.set_ttl(SOCKET_TTL);
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    UdpSocket::from_std(socket.into())
}

fn udp_receive_worker_count() -> usize {
    if let Ok(value) = std::env::var("BASIS_UDP_RECEIVE_WORKERS") {
        if let Ok(parsed) = value.parse::<usize>() {
            return parsed.clamp(1, 64);
        }
    }
    std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1).max(1))
        .unwrap_or(1)
        .min(DEFAULT_MAX_RECEIVE_WORKERS)
}

async fn read_loop(handle: TransportHandle, tx: mpsc::Sender<ServerEvent>) {
    let mut buf = vec![0u8; 64 * 1024];
    while !handle.shutdown.load(Ordering::Relaxed) {
        match handle.socket.recv_from(&mut buf).await {
            Ok((len, remote_addr)) => {
                if handle.statistics_enabled() {
                    handle
                        .stats
                        .raw_packets_received
                        .fetch_add(1, Ordering::Relaxed);
                    handle
                        .stats
                        .raw_bytes_received
                        .fetch_add(len as u64, Ordering::Relaxed);
                }
                if let Err(err) = process_packet(&handle, &tx, remote_addr, &buf[..len]).await {
                    warn!("transport packet processing failed: {err}");
                }
            }
            Err(err) => {
                if err.kind() == std::io::ErrorKind::ConnectionReset {
                    continue;
                }
                let _ = tx.send(ServerEvent::NetworkError(err.to_string())).await;
            }
        }
    }
}

async fn process_packet(
    handle: &TransportHandle,
    tx: &mpsc::Sender<ServerEvent>,
    remote_addr: SocketAddr,
    bytes: &[u8],
) -> Result<()> {
    if let Some(server_info_payload) = server_info_payload(bytes) {
        if server_info_payload.len() >= SERVER_INFO_MIN_REQUEST_BYTES {
            enqueue_event(
                tx,
                ServerEvent::UnconnectedRequest {
                    remote_addr,
                    nonce: parse_server_info_query_nonce(server_info_payload).unwrap_or_default(),
                    payload: Bytes::copy_from_slice(server_info_payload),
                },
            )
            .await
            .map_err(|_| TransportError::EventChannelClosed)?;
            return Ok(());
        }
    }

    let Some(header) = bytes.first().copied() else {
        return Ok(());
    };
    let connection_number = (header & 0x60) >> 5;
    let Some(property) = PacketProperty::from_byte(header) else {
        trace!("unknown packet property: {header}");
        return Ok(());
    };

    match property {
        PacketProperty::ConnectRequest => match parse_connect_request(bytes) {
            ConnectRequestParse::Ok(parsed) => {
                if let Some(existing_peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                    if let Some(existing_peer) = handle.peers.get(&existing_peer_id) {
                        if parsed.connect_time == existing_peer.connect_time {
                            send_connect_accept(
                                handle,
                                remote_addr,
                                existing_peer.connection_number,
                                existing_peer.connect_time,
                                existing_peer.id,
                            )
                            .await?;
                            return Ok(());
                        }
                        if parsed.connect_time < existing_peer.connect_time {
                            return Ok(());
                        }
                    }
                    if let Some((_, old_peer)) = handle.peers.remove(&existing_peer_id) {
                        handle.by_addr.remove(&old_peer.addr);
                        handle.retire_peer_id(existing_peer_id);
                        enqueue_event(
                            tx,
                            ServerEvent::PeerDisconnected {
                                peer: existing_peer_id,
                                reason: DisconnectReason::Remote,
                            },
                        )
                        .await
                        .map_err(|_| TransportError::EventChannelClosed)?;
                    }
                }
                if let Some(existing) = handle.pending_requests.get(&remote_addr) {
                    if parsed.connect_time < existing.connect_time
                        || (parsed.connect_time == existing.connect_time
                            && connection_number == existing.connection_number)
                    {
                        return Ok(());
                    }
                }
                handle.pending_requests.insert(
                    remote_addr,
                    PendingRequestInfo {
                        connect_time: parsed.connect_time,
                        connection_number,
                    },
                );
                enqueue_event(
                    tx,
                    ServerEvent::ConnectionRequest(ConnectionRequest {
                        remote_addr,
                        payload: Bytes::copy_from_slice(parsed.payload),
                        connection_number,
                        connect_time: parsed.connect_time,
                        local_peer_id: parsed.local_peer_id,
                    }),
                )
                .await
                .map_err(|_| TransportError::EventChannelClosed)?;
            }
            ConnectRequestParse::InvalidProtocol => {
                send_simple_property(handle, remote_addr, PacketProperty::InvalidProtocol).await?;
            }
            ConnectRequestParse::Malformed => {}
        },
        PacketProperty::Disconnect => {
            if let Some(peer_id) = handle.by_addr.remove(&remote_addr).map(|p| p.1) {
                let Some((_, peer)) = handle.peers.remove(&peer_id) else {
                    return Ok(());
                };
                if !disconnect_matches(&peer, bytes, connection_number) {
                    handle.by_addr.insert(remote_addr, peer_id);
                    handle.peers.insert(peer_id, peer);
                    return Ok(());
                }
                handle.retire_peer_id(peer_id);
                send_simple_property(handle, remote_addr, PacketProperty::ShutdownOk).await?;
                enqueue_event(
                    tx,
                    ServerEvent::PeerDisconnected {
                        peer: peer_id,
                        reason: DisconnectReason::Remote,
                    },
                )
                .await
                .map_err(|_| TransportError::EventChannelClosed)?;
            }
        }
        PacketProperty::Ping => {
            if let Some(peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                if let Some(peer) = handle.peers.get(&peer_id) {
                    *peer.last_seen.lock() = Instant::now();
                }
            }
            if bytes.len() >= 3 {
                let sequence = u16::from_le_bytes([bytes[1], bytes[2]]);
                send_pong(handle, remote_addr, connection_number, sequence).await?;
            }
        }
        PacketProperty::Merged => {
            if let Some(peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                if let Some(peer) = handle.peers.get(&peer_id) {
                    *peer.last_seen.lock() = Instant::now();
                }
            }
            process_merged_packet(handle, tx, remote_addr, bytes).await?;
        }
        PacketProperty::CompactMerged => {
            if let Some(peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                if let Some(peer) = handle.peers.get(&peer_id) {
                    *peer.last_seen.lock() = Instant::now();
                }
            }
            process_compact_merged_packet(handle, tx, remote_addr, connection_number, bytes)
                .await?;
        }
        PacketProperty::MtuCheck => {
            if let Some(peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                if let Some(peer) = handle.peers.get(&peer_id) {
                    *peer.last_seen.lock() = Instant::now();
                }
            }
            send_mtu_ok(handle, remote_addr, bytes).await?;
        }
        PacketProperty::MtuOk => {
            if let Some(peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                if let Some(peer) = handle.peers.get(&peer_id) {
                    if let Some(mtu) = peer
                        .mtu_probe
                        .lock()
                        .accept_response(bytes, peer.connection_number)
                    {
                        peer.confirmed_mtu
                            .store(mtu.max(MAX_MERGED_PACKET_SIZE), Ordering::Relaxed);
                        *peer.last_seen.lock() = Instant::now();
                    }
                }
            }
        }
        PacketProperty::NatMessage => {
            if let Some((local_addr, token)) = parse_nat_introduce_request(&bytes[1..]) {
                enqueue_event(
                    tx,
                    ServerEvent::NatIntroductionRequest {
                        remote_addr,
                        local_addr,
                        token,
                    },
                )
                .await
                .map_err(|_| TransportError::EventChannelClosed)?;
            }
        }
        PacketProperty::Ack => {
            if let Some(peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                if let Some(peer) = handle.peers.get(&peer_id) {
                    *peer.last_seen.lock() = Instant::now();
                    process_ack(&peer, bytes, Some(&handle.stats));
                }
            }
        }
        PacketProperty::Channeled | PacketProperty::Unreliable => {
            if let Some(peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                if let Some(peer) = handle.peers.get(&peer_id) {
                    *peer.last_seen.lock() = Instant::now();
                }
                if let Some((channel, delivery, payload)) = parse_message_packet(property, bytes) {
                    let mut deliver_event = true;
                    if matches!(
                        delivery,
                        DeliveryMethod::ReliableOrdered
                            | DeliveryMethod::ReliableUnordered
                            | DeliveryMethod::ReliableSequenced
                    ) {
                        let sequence = u16::from_le_bytes([bytes[1], bytes[2]]);
                        if sequence >= MAX_SEQUENCE {
                            return Ok(());
                        }
                        let channel_id = bytes[3];
                        if delivery == DeliveryMethod::ReliableSequenced {
                            if let Some(peer) = handle.peers.get(&peer_id).map(|peer| peer.clone())
                            {
                                if let Some((ack_sequence, is_new)) =
                                    queue_reliable_sequenced_ack(&peer, channel_id, sequence)
                                {
                                    deliver_event = is_new;
                                    let ack = build_reliable_sequenced_ack(
                                        peer.connection_number,
                                        channel_id,
                                        ack_sequence,
                                    );
                                    handle.send_raw_to(&ack, peer.addr).await?;
                                }
                            }
                        } else if let Some(peer) = handle.peers.get(&peer_id) {
                            queue_ack(&peer, channel_id, sequence);
                        }
                    }
                    if !deliver_event {
                        return Ok(());
                    }
                    let event = ServerEvent::Message {
                        peer: peer_id,
                        channel,
                        delivery,
                        payload: Bytes::copy_from_slice(payload),
                    };
                    if matches!(
                        delivery,
                        DeliveryMethod::Unreliable | DeliveryMethod::Sequenced
                    ) {
                        enqueue_lossy_event(tx, event).await?;
                    } else {
                        enqueue_event(tx, event).await?;
                    }
                }
            }
        }
        _ => debug!("ignored packet property {property:?} from {remote_addr}"),
    }
    Ok(())
}

fn server_info_payload(bytes: &[u8]) -> Option<&[u8]> {
    if looks_like_server_info_payload(bytes) {
        return Some(bytes);
    }
    if bytes.first().map(|header| header & 0x1f) == Some(PacketProperty::UnconnectedMessage as u8) {
        let payload = &bytes[1..];
        if looks_like_server_info_payload(payload) {
            return Some(payload);
        }
    }
    None
}

fn looks_like_server_info_payload(bytes: &[u8]) -> bool {
    if bytes.len() < 6 {
        return false;
    }
    let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    let protocol = u16::from_le_bytes([bytes[4], bytes[5]]);
    magic == SERVER_INFO_QUERY_MAGIC && protocol == SERVER_INFO_PROTOCOL_VERSION
}

async fn enqueue_event(tx: &mpsc::Sender<ServerEvent>, event: ServerEvent) -> Result<()> {
    match tx.try_send(event) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(event)) => tx
            .send(event)
            .await
            .map_err(|_| TransportError::EventChannelClosed),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(TransportError::EventChannelClosed),
    }
}

async fn enqueue_lossy_event(tx: &mpsc::Sender<ServerEvent>, event: ServerEvent) -> Result<()> {
    match tx.try_send(event) {
        Ok(()) => Ok(()),
        Err(mpsc::error::TrySendError::Full(_)) => Ok(()),
        Err(mpsc::error::TrySendError::Closed(_)) => Err(TransportError::EventChannelClosed),
    }
}

#[derive(Debug, Clone, Copy)]
struct ParsedConnectRequest<'a> {
    payload: &'a [u8],
    connect_time: i64,
    local_peer_id: i32,
}

#[derive(Debug, Clone, Copy)]
enum ConnectRequestParse<'a> {
    Ok(ParsedConnectRequest<'a>),
    InvalidProtocol,
    Malformed,
}

fn parse_connect_request(bytes: &[u8]) -> ConnectRequestParse<'_> {
    if bytes.len() < 18 {
        return ConnectRequestParse::Malformed;
    }
    let protocol = i32::from_le_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
    if protocol != LITENETLIB_PROTOCOL_ID {
        return ConnectRequestParse::InvalidProtocol;
    }
    let connection_number = (bytes[0] & 0x60) >> 5;
    if connection_number >= 4 {
        return ConnectRequestParse::Malformed;
    }
    let Ok(connect_time_bytes) = bytes[5..13].try_into() else {
        return ConnectRequestParse::Malformed;
    };
    let Ok(local_peer_id_bytes) = bytes[13..17].try_into() else {
        return ConnectRequestParse::Malformed;
    };
    let connect_time = i64::from_le_bytes(connect_time_bytes);
    let local_peer_id = i32::from_le_bytes(local_peer_id_bytes);
    let addr_len = bytes[17] as usize;
    if addr_len != 16 && addr_len != 28 {
        return ConnectRequestParse::Malformed;
    }
    let payload_start = 18 + addr_len;
    if bytes.len() < payload_start {
        return ConnectRequestParse::Malformed;
    }
    ConnectRequestParse::Ok(ParsedConnectRequest {
        payload: &bytes[payload_start..],
        connect_time,
        local_peer_id,
    })
}

async fn send_simple_property(
    handle: &TransportHandle,
    remote_addr: SocketAddr,
    property: PacketProperty,
) -> Result<()> {
    handle.send_raw_to(&[property as u8], remote_addr).await?;
    Ok(())
}

async fn send_connect_accept(
    handle: &TransportHandle,
    remote_addr: SocketAddr,
    connection_number: u8,
    connect_time: i64,
    peer_id: PeerId,
) -> Result<()> {
    let mut writer = NetWriter::with_capacity(15);
    writer.put_u8(PacketProperty::ConnectAccept as u8 | (connection_number << 5));
    writer.put_i64(connect_time);
    writer.put_u8(connection_number);
    writer.put_u8(0);
    writer.put_i32(peer_id as i32);
    handle.send_raw_to(writer.as_slice(), remote_addr).await?;
    Ok(())
}

fn disconnect_matches(peer: &PeerState, bytes: &[u8], connection_number: u8) -> bool {
    if bytes.len() < 9 || connection_number != peer.connection_number {
        return false;
    }
    i64::from_le_bytes(
        bytes[1..9]
            .try_into()
            .expect("disconnect header length checked"),
    ) == peer.connect_time
}

fn parse_message_packet(
    property: PacketProperty,
    bytes: &[u8],
) -> Option<(u8, DeliveryMethod, &[u8])> {
    match property {
        PacketProperty::Unreliable => {
            if bytes.len() < 2 {
                return None;
            }
            Some((bytes[1], DeliveryMethod::Unreliable, &bytes[2..]))
        }
        PacketProperty::Channeled => {
            if bytes.len() < 4 {
                return None;
            }
            let channel_id = bytes[3];
            let channel = channel_id / 4;
            let delivery = DeliveryMethod::from_channel_id(channel_id);
            Some((channel, delivery, &bytes[4..]))
        }
        _ => None,
    }
}

struct BuiltPacket {
    bytes: Vec<u8>,
    /// Assigned for reliable deliveries so the caller can record the in-flight entry without
    /// re-deriving it from `reliable_key`.
    sequence: u16,
    reliable_key: Option<(u8, u16)>,
}

#[inline]
fn extend_payload_with_patch(output: &mut Vec<u8>, payload: &[u8], patch: Option<(usize, u8)>) {
    let payload_start = output.len();
    output.extend_from_slice(payload);
    if let Some((offset, value)) = patch {
        if offset < payload.len() {
            output[payload_start + offset] = value;
        }
    }
}

fn build_outbound_packet(
    state: &PeerState,
    channel: u8,
    delivery: DeliveryMethod,
    payload: &[u8],
) -> BuiltPacket {
    match delivery {
        DeliveryMethod::Unreliable => {
            let mut writer = NetWriter::with_capacity(payload.len() + 2);
            writer.put_u8(PacketProperty::Unreliable as u8 | (state.connection_number << 5));
            writer.put_u8(channel);
            writer.put_bytes(payload);
            BuiltPacket {
                bytes: writer.into_vec(),
                sequence: 0,
                reliable_key: None,
            }
        }
        DeliveryMethod::Sequenced => {
            let channel_id = DeliveryMethod::channel_id(channel, delivery);
            let sequence =
                next_sequenced_channel_sequence(&state.next_sequenced_sequence, channel_id);
            let mut writer = NetWriter::with_capacity(payload.len() + 4);
            writer.put_u8(PacketProperty::Channeled as u8 | (state.connection_number << 5));
            writer.put_u16(sequence);
            writer.put_u8(channel_id);
            writer.put_bytes(payload);
            BuiltPacket {
                bytes: writer.into_vec(),
                sequence: 0,
                reliable_key: None,
            }
        }
        _ => {
            let channel_id = DeliveryMethod::channel_id(channel, delivery);
            let sequence = next_channel_sequence(&state.next_reliable_sequence, channel_id);
            let mut writer = NetWriter::with_capacity(payload.len() + 4);
            writer.put_u8(PacketProperty::Channeled as u8 | (state.connection_number << 5));
            writer.put_u16(sequence);
            writer.put_u8(channel_id);
            writer.put_bytes(payload);
            BuiltPacket {
                bytes: writer.into_vec(),
                sequence,
                reliable_key: Some((channel_id, sequence)),
            }
        }
    }
}

fn next_channel_sequence(sequences: &parking_lot::Mutex<HashMap<u8, u16>>, channel_id: u8) -> u16 {
    let mut sequences = sequences.lock();
    let sequence = sequences.entry(channel_id).or_insert(0);
    let current = *sequence;
    *sequence = sequence.wrapping_add(1) % MAX_SEQUENCE;
    current
}

fn next_sequenced_channel_sequence(
    sequences: &parking_lot::Mutex<HashMap<u8, u16>>,
    channel_id: u8,
) -> u16 {
    let mut sequences = sequences.lock();
    let sequence = sequences.entry(channel_id).or_insert(0);
    *sequence = sequence.wrapping_add(1) % MAX_SEQUENCE;
    *sequence
}

/// Apply an incoming ACK window.
///
/// Any in-flight packet the ACK covers is released, wherever it sits in the channel's deque.
/// That leniency is load-bearing, not laziness: ACKs are individual UDP datagrams, so one can
/// be lost. If this only released a prefix, a single dropped ACK would leave the oldest packet
/// unacknowledged forever, every later ACK would compare negative against it, and the send
/// window would deadlock permanently. The window is at most `DEFAULT_WINDOW_SIZE` deep, so
/// scanning it is cheap -- and it is per channel, unlike the previous whole-peer scan.
fn process_ack(peer: &PeerState, bytes: &[u8], stats: Option<&TransportStats>) {
    if bytes.len() < LITENETLIB_CHANNELED_HEADER_SIZE {
        return;
    }
    let channel_id = bytes[3];
    let sequenced = channel_id % 4 == DeliveryMethod::ReliableSequenced as u8;
    let expected_size = if sequenced {
        LITENETLIB_CHANNELED_HEADER_SIZE
    } else {
        LITENETLIB_CHANNELED_HEADER_SIZE + (DEFAULT_WINDOW_SIZE - 1) / 8 + 2
    };
    if bytes.len() != expected_size {
        return;
    }
    let ack_window_start = u16::from_le_bytes([bytes[1], bytes[2]]);
    if sequenced {
        if ack_window_start >= MAX_SEQUENCE {
            return;
        }
        let mut pending = peer.pending_reliable.lock();
        let Some(queue) = pending.get_mut(&channel_id) else {
            return;
        };
        let Some(ack_index) = queue
            .iter()
            .position(|item| item.sequence == ack_window_start)
        else {
            return;
        };
        for _ in 0..=ack_index {
            queue.pop_front();
        }
        let released = ack_index + 1;
        if queue.is_empty() {
            pending.remove(&channel_id);
        }
        drop(pending);
        peer.pending_total.fetch_sub(released, Ordering::Relaxed);
        if let Some(stats) = stats.filter(|stats| stats.extended_enabled.load(Ordering::Relaxed)) {
            stats.reliable_acks_received.fetch_add(1, Ordering::Relaxed);
            stats
                .reliable_acks_released
                .fetch_add(released as u64, Ordering::Relaxed);
        }
        return;
    }
    let ack_bits = &bytes[4..];
    let mut pending = peer.pending_reliable.lock();
    let Some(queue) = pending.get_mut(&channel_id) else {
        drop(pending);
        if let Some(stats) = stats.filter(|stats| stats.extended_enabled.load(Ordering::Relaxed)) {
            stats.reliable_acks_received.fetch_add(1, Ordering::Relaxed);
            stats
                .reliable_acks_unknown_channel
                .fetch_add(1, Ordering::Relaxed);
        }
        return;
    };
    // LiteNetLib `ReliableChannel.ProcessAck` bounds check, reproduced exactly: the ACK's
    // window start has to be a legal sequence and has to sit within one window *ahead* of our
    // own oldest unacknowledged packet. Anything else is a stale or forged ACK and is dropped
    // whole, rather than partially believed.
    if ack_window_start >= MAX_SEQUENCE {
        return;
    }
    let local_window_start = queue.front().map(|item| item.sequence).unwrap_or(0);
    let window_rel = relative_sequence(local_window_start, ack_window_start);
    if window_rel < 0 || window_rel as usize >= DEFAULT_WINDOW_SIZE {
        return;
    }
    let mut released = 0usize;
    queue.retain(|item| {
        // C# stops walking pending sequences once they are beyond this ACK window. Without
        // this bound, an old ACK for bits 0..9 would also release sequences 128..137 because
        // the absolute 128-bit bitmap aliases them.
        let relative = relative_sequence(item.sequence, ack_window_start);
        // ACK bits are *absolute*: bit `sequence % DEFAULT_WINDOW_SIZE`, not an offset from
        // `window_start`. That is what LiteNetLib does on both sides, and the window start is
        // only the bounds check above. Modulo-128 is a bijection across any 128 consecutive
        // sequences, so within the window a set bit identifies exactly one sequence.
        let acknowledged = relative < DEFAULT_WINDOW_SIZE as i32
            && ack_bit(ack_bits, item.sequence as usize % DEFAULT_WINDOW_SIZE);
        if acknowledged {
            released += 1;
        }
        !acknowledged
    });
    if queue.is_empty() {
        pending.remove(&channel_id);
    }
    drop(pending);
    if let Some(stats) = stats.filter(|stats| stats.extended_enabled.load(Ordering::Relaxed)) {
        stats.reliable_acks_received.fetch_add(1, Ordering::Relaxed);
        stats
            .reliable_acks_released
            .fetch_add(released as u64, Ordering::Relaxed);
    }
    if released > 0 {
        peer.pending_total.fetch_sub(released, Ordering::Relaxed);
    }
}

#[inline]
fn ack_bit(ack_bits: &[u8], bit: usize) -> bool {
    ack_bits
        .get(bit / 8)
        .map(|byte| (byte & (1 << (bit % 8))) != 0)
        .unwrap_or(false)
}

fn queue_ack(peer: &PeerState, channel_id: u8, sequence: u16) {
    if sequence >= MAX_SEQUENCE {
        return;
    }
    let mut acks = peer.outgoing_acks.lock();
    let ack = acks.entry(channel_id).or_insert_with(|| AckState {
        // LiteNetLib starts `_remoteWindowStart` and the ACK's sequence field at 0 and only
        // ever moves them by sliding. Seeding from the first packet seen would differ on the
        // wire, and the peer rejects an ACK whose window start is not within one window of its
        // own oldest unacknowledged packet -- so this has to match the C# exactly.
        window_start: 0,
        bits: vec![0; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2],
        dirty: false,
    });

    let rel = relative_sequence(sequence, ack.window_start);
    if rel < 0 {
        // Older than the window. LiteNetLib drops such a packet before it ever reaches its ACK
        // bookkeeping (`if (relate < 0) return false;`), and believing it here would set a bit
        // that aliases an in-window sequence, so refuse it rather than corrupt the window.
        return;
    }
    if rel as usize >= DEFAULT_WINDOW_SIZE {
        // LiteNetLib `ReliableChannel.ProcessPacket`: a packet from beyond the window slides it
        // forward just far enough to bring the newcomer back inside, clearing the bits that fall
        // out on the way. The window never jumps to the newest sequence.
        let shift = rel as usize - DEFAULT_WINDOW_SIZE + 1;
        for _ in 0..shift {
            let index = ack.window_start as usize % DEFAULT_WINDOW_SIZE;
            if let Some(byte) = ack.bits.get_mut(index / 8) {
                *byte &= !(1 << (index % 8));
            }
            ack.window_start = ack.window_start.wrapping_add(1) % MAX_SEQUENCE;
        }
    }

    // Absolute bit index, matching LiteNetLib. `window_start` is written into the ACK header
    // purely so the peer can bound-check; the bits themselves are not relative to it.
    let index = sequence as usize % DEFAULT_WINDOW_SIZE;
    if let Some(byte) = ack.bits.get_mut(index / 8) {
        *byte |= 1 << (index % 8);
    }
    ack.dirty = true;
    peer.reliable_active.store(true, Ordering::Release);
}

/// Record the newest valid inbound ReliableSequenced packet and return the sequence the C#
/// SequencedChannel expects in its header-only ACK. Old/duplicate packets re-ACK the current
/// sequence; invalid values are rejected by the caller before application dispatch.
fn queue_reliable_sequenced_ack(
    peer: &PeerState,
    channel_id: u8,
    sequence: u16,
) -> Option<(u16, bool)> {
    if sequence >= MAX_SEQUENCE {
        return None;
    }
    let mut sequences = peer.remote_sequenced_sequence.lock();
    let current = sequences.entry(channel_id).or_insert(0);
    let is_new = relative_sequence(sequence, *current) > 0;
    if is_new {
        *current = sequence;
    }
    Some((*current, is_new))
}

fn build_reliable_sequenced_ack(connection_number: u8, channel_id: u8, sequence: u16) -> [u8; 4] {
    let mut packet = [0; LITENETLIB_CHANNELED_HEADER_SIZE];
    packet[0] = PacketProperty::Ack as u8 | (connection_number << 5);
    packet[1..3].copy_from_slice(&sequence.to_le_bytes());
    packet[3] = channel_id;
    packet
}

fn build_ack_packet(
    connection_number: u8,
    channel_id: u8,
    window_start: u16,
    bits: &[u8],
) -> Vec<u8> {
    let mut packet = Vec::with_capacity(4 + bits.len());
    packet.push(PacketProperty::Ack as u8 | (connection_number << 5));
    packet.extend_from_slice(&window_start.to_le_bytes());
    packet.push(channel_id);
    packet.extend_from_slice(bits);
    packet
}

const NAT_INTRODUCE_REQUEST_HASH: u64 =
    fnv1_64("LiteNetLib.NatPunchModule+NatIntroduceRequestPacket");
const NAT_INTRODUCE_RESPONSE_HASH: u64 =
    fnv1_64("LiteNetLib.NatPunchModule+NatIntroduceResponsePacket");

const fn fnv1_64(text: &str) -> u64 {
    let bytes = text.as_bytes();
    let mut hash = 14_695_981_039_346_656_037u64;
    let mut index = 0usize;
    while index < bytes.len() {
        hash ^= bytes[index] as u64;
        hash = hash.wrapping_mul(1_099_511_628_211u64);
        index += 1;
    }
    hash
}

fn parse_nat_introduce_request(bytes: &[u8]) -> Option<(SocketAddr, String)> {
    if bytes.len() < 8 {
        return None;
    }
    let hash = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
    if hash != NAT_INTRODUCE_REQUEST_HASH {
        return None;
    }
    let mut position = 8usize;
    let local_addr = read_litenet_endpoint(bytes, &mut position)?;
    let token = read_litenet_string(bytes, &mut position)?;
    Some((local_addr, token))
}

fn build_nat_introduce_response(
    internal: SocketAddr,
    external: SocketAddr,
    token: &str,
    prediction_count: u8,
) -> Vec<u8> {
    let mut packet = Vec::with_capacity(1 + 8 + 2 * 19 + token.len() + 2);
    packet.push(PacketProperty::NatMessage as u8);
    packet.extend_from_slice(&NAT_INTRODUCE_RESPONSE_HASH.to_le_bytes());
    write_litenet_endpoint(&mut packet, internal);
    write_litenet_endpoint(&mut packet, external);
    write_litenet_string(&mut packet, token);
    // Current LiteNetLib appends NatIntroduceResponsePacket.PredictionCount.
    packet.push(prediction_count);
    packet
}

fn read_litenet_endpoint(bytes: &[u8], position: &mut usize) -> Option<SocketAddr> {
    let family = *bytes.get(*position)?;
    *position += 1;
    match family {
        0 => {
            let octets: [u8; 4] = bytes.get(*position..*position + 4)?.try_into().ok()?;
            *position += 4;
            let port = u16::from_le_bytes(bytes.get(*position..*position + 2)?.try_into().ok()?);
            *position += 2;
            Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::from(octets)), port))
        }
        1 => {
            let octets: [u8; 16] = bytes.get(*position..*position + 16)?.try_into().ok()?;
            *position += 16;
            let port = u16::from_le_bytes(bytes.get(*position..*position + 2)?.try_into().ok()?);
            *position += 2;
            Some(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port))
        }
        _ => None,
    }
}

fn write_litenet_endpoint(packet: &mut Vec<u8>, endpoint: SocketAddr) {
    match endpoint.ip() {
        IpAddr::V4(addr) => {
            packet.push(0);
            packet.extend_from_slice(&addr.octets());
        }
        IpAddr::V6(addr) => {
            packet.push(1);
            packet.extend_from_slice(&addr.octets());
        }
    }
    packet.extend_from_slice(&endpoint.port().to_le_bytes());
}

fn read_litenet_string(bytes: &[u8], position: &mut usize) -> Option<String> {
    let size = u16::from_le_bytes(bytes.get(*position..*position + 2)?.try_into().ok()?) as usize;
    *position += 2;
    if size == 0 {
        return Some(String::new());
    }
    let len = size.checked_sub(1)?;
    let raw = bytes.get(*position..*position + len)?;
    *position += len;
    std::str::from_utf8(raw).ok().map(str::to_owned)
}

fn write_litenet_string(packet: &mut Vec<u8>, value: &str) {
    if value.is_empty() {
        packet.extend_from_slice(&0u16.to_le_bytes());
        return;
    }
    let bytes = value.as_bytes();
    let len_plus = (bytes.len() + 1).min(u16::MAX as usize) as u16;
    packet.extend_from_slice(&len_plus.to_le_bytes());
    packet.extend_from_slice(&bytes[..len_plus as usize - 1]);
}

async fn send_pong(
    handle: &TransportHandle,
    remote_addr: SocketAddr,
    connection_number: u8,
    sequence: u16,
) -> Result<()> {
    let mut writer = NetWriter::with_capacity(11);
    writer.put_u8(PacketProperty::Pong as u8 | (connection_number << 5));
    writer.put_u16(sequence);
    writer.put_i64(dotnet_utc_ticks());
    handle.send_raw_to(writer.as_slice(), remote_addr).await?;
    Ok(())
}

fn build_ping_packet(connection_number: u8, sequence: u16) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(3);
    writer.put_u8(PacketProperty::Ping as u8 | (connection_number << 5));
    writer.put_u16(sequence);
    writer.into_vec()
}

async fn send_mtu_ok(
    handle: &TransportHandle,
    remote_addr: SocketAddr,
    mtu_check_packet: &[u8],
) -> Result<()> {
    if mtu_check_packet.is_empty() {
        return Ok(());
    }
    let mut packet = mtu_check_packet.to_vec();
    packet[0] = (packet[0] & 0xe0) | PacketProperty::MtuOk as u8;
    handle.send_raw_to(&packet, remote_addr).await?;
    Ok(())
}

async fn process_merged_packet(
    handle: &TransportHandle,
    tx: &mpsc::Sender<ServerEvent>,
    remote_addr: SocketAddr,
    bytes: &[u8],
) -> Result<()> {
    let mut position = 1;
    while position < bytes.len() {
        if position + 2 > bytes.len() {
            break;
        }
        let size = u16::from_le_bytes([bytes[position], bytes[position + 1]]) as usize;
        if size == 0 {
            break;
        }
        position += 2;
        if bytes.len() - position < size {
            break;
        }
        let packet = &bytes[position..position + size];
        if is_valid_merged_packet(packet) {
            Box::pin(process_packet(handle, tx, remote_addr, packet)).await?;
        }
        position += size;
    }
    Ok(())
}

async fn process_compact_merged_packet(
    handle: &TransportHandle,
    tx: &mpsc::Sender<ServerEvent>,
    remote_addr: SocketAddr,
    connection_number: u8,
    bytes: &[u8],
) -> Result<()> {
    const LONG_LENGTH_FLAG: u8 = 0x80;
    const RAW_PACKET_FLAG: u8 = 0x40;
    const CHANNEL_MASK: u8 = 0x3f;

    let mut position = 1usize;
    while position < bytes.len() {
        if bytes.len() - position < 2 {
            break;
        }

        let tag = bytes[position];
        position += 1;
        let is_raw = tag & RAW_PACKET_FLAG != 0;
        let channel = tag & CHANNEL_MASK;
        if is_raw && channel != 0 {
            break;
        }

        let payload_len = if tag & LONG_LENGTH_FLAG != 0 {
            if bytes.len() - position < 2 {
                break;
            }
            let len = u16::from_le_bytes([bytes[position], bytes[position + 1]]) as usize;
            position += 2;
            if len <= u8::MAX as usize {
                break;
            }
            len
        } else {
            let len = bytes[position] as usize;
            position += 1;
            len
        };

        if payload_len > bytes.len() - position {
            break;
        }
        let payload = &bytes[position..position + payload_len];
        position += payload_len;

        if is_raw {
            if payload_len < 4 {
                break;
            }
            let Some(property) = payload
                .first()
                .and_then(|header| PacketProperty::from_byte(*header))
            else {
                break;
            };
            if !matches!(property, PacketProperty::Ack | PacketProperty::Channeled) {
                break;
            }
            Box::pin(process_packet(handle, tx, remote_addr, payload)).await?;
        } else {
            let mut packet = Vec::with_capacity(payload_len + 2);
            packet.push(PacketProperty::Unreliable as u8 | (connection_number << 5));
            packet.push(channel);
            packet.extend_from_slice(payload);
            Box::pin(process_packet(handle, tx, remote_addr, &packet)).await?;
        }
    }
    Ok(())
}

fn is_valid_merged_packet(bytes: &[u8]) -> bool {
    let Some(property) = bytes
        .first()
        .and_then(|header| PacketProperty::from_byte(*header))
    else {
        return false;
    };
    let header_size = match property {
        PacketProperty::Unreliable => 2,
        PacketProperty::Channeled | PacketProperty::Ack => 4,
        PacketProperty::Ping => 3,
        PacketProperty::Pong => 11,
        PacketProperty::ConnectRequest => 18,
        PacketProperty::ConnectAccept => 15,
        PacketProperty::Disconnect => 9,
        _ => 1,
    };
    bytes.len() >= header_size
}

async fn timeout_loop(handle: TransportHandle, tx: mpsc::Sender<ServerEvent>) {
    let mut tick = time::interval(Duration::from_secs(5));
    while !handle.shutdown.load(Ordering::Relaxed) {
        tick.tick().await;
        let now = Instant::now();
        let timed_out: Vec<_> = handle
            .peers
            .iter()
            .filter_map(|peer| {
                if now.duration_since(*peer.last_seen.lock()) > Duration::from_secs(30) {
                    Some(peer.id)
                } else {
                    None
                }
            })
            .collect();
        for peer_id in timed_out {
            if let Some((_, peer)) = handle.peers.remove(&peer_id) {
                handle.by_addr.remove(&peer.addr);
                handle.retire_peer_id(peer_id);
                let _ = tx
                    .send(ServerEvent::PeerDisconnected {
                        peer: peer_id,
                        reason: DisconnectReason::Timeout,
                    })
                    .await;
            }
        }
    }
}

/// Fill reliable send windows and flush pending ACKs.
///
/// This is the only reliable path that has to run fast: a peer can move at most
/// `DEFAULT_WINDOW_SIZE` messages per channel per pass, so the pass interval *is* the
/// reliable throughput ceiling. Everything that does not need millisecond resolution
/// (retransmits, keepalive pings) lives in `reliable_maintenance_loop` instead, so this loop
/// no longer has to touch peers that have nothing to send.
async fn reliable_dispatch_loop(handle: TransportHandle) {
    let mut tick = time::interval(RELIABLE_DISPATCH_INTERVAL);
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut builder = MergedDatagramBuilder::new();
    while !handle.shutdown.load(Ordering::Relaxed) {
        tick.tick().await;
        if handle.extended_statistics_enabled() {
            handle
                .stats
                .reliable_dispatch_passes
                .fetch_add(1, Ordering::Relaxed);
        }

        for peer in handle.peers.iter() {
            if !peer.reliable_active.load(Ordering::Acquire) {
                continue;
            }
            let Some(_send_turn) = try_peer_send_turn(&peer) else {
                continue;
            };
            if handle.extended_statistics_enabled() {
                handle
                    .stats
                    .reliable_peers_visited
                    .fetch_add(1, Ordering::Relaxed);
            }
            let addr = peer.addr;
            let connection_number = peer.connection_number;
            builder.reset(connection_number);
            if !retry_peer_datagrams(&peer, &handle, addr) {
                continue;
            }

            // 1. Release ACKs that have new bits. Unchanged windows are skipped entirely, so a
            //    quiet channel costs one bool check instead of a packet rebuild.
            {
                let mut acks = peer.outgoing_acks.lock();
                let mut built = Vec::new();
                acks.retain(|channel_id, ack| {
                    if !ack.dirty {
                        return true;
                    }
                    ack.dirty = false;
                    built.push(build_ack_packet(
                        connection_number,
                        *channel_id,
                        ack.window_start,
                        &ack.bits,
                    ));
                    true
                });
                drop(acks);
                for packet in built {
                    builder.push(&packet);
                }
            }

            // 2. Move queued payloads into the free part of each channel's send window. Window
            // occupancy is a deque length, so this is O(channels) rather than a per-pass
            // HashMap built by walking every in-flight packet of the peer.
            let newly_queued = {
                let pending = peer.pending_reliable.lock();
                let mut outgoing = peer.outgoing_reliable.lock();
                let mut newly_queued = Vec::new();
                // Keep the datagram batch in one-to-one correspondence with tracked in-flight
                // packets. Without an aggregate budget, 128 slots from every channel could be
                // moved here before record_pending_reliable evicts past its per-peer cap.
                let mut remaining = MAX_PENDING_RELIABLE_PER_PEER
                    .saturating_sub(peer.pending_total.load(Ordering::Relaxed));
                for (channel_id, queue) in outgoing.iter_mut() {
                    if remaining == 0 {
                        break;
                    }
                    let next_sequence = peer
                        .next_reliable_sequence
                        .lock()
                        .get(channel_id)
                        .copied()
                        .unwrap_or(0);
                    let empty = VecDeque::new();
                    let in_flight = pending.get(channel_id).unwrap_or(&empty);
                    let capacity = reliable_window_capacity(
                        next_sequence,
                        in_flight.front().map(|item| item.sequence),
                    );
                    if capacity == 0 && !queue.is_empty() && handle.extended_statistics_enabled() {
                        handle
                            .stats
                            .reliable_window_stalls
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    let payloads =
                        dequeue_reliable_window(next_sequence, in_flight, queue, remaining);
                    remaining = remaining.saturating_sub(payloads.len());
                    newly_queued.extend(payloads.into_iter().map(|payload| (*channel_id, payload)));
                }
                newly_queued
            };

            if !newly_queued.is_empty() {
                for (channel_id, payload) in newly_queued {
                    let built = build_queued_reliable_packet(&peer, channel_id, payload);
                    let bytes = built.bytes;
                    record_pending_reliable(
                        &peer,
                        channel_id,
                        built.sequence,
                        bytes.clone(),
                        Some(&handle.stats),
                    );
                    builder.push(&bytes);
                }
            }

            // 3. Nothing queued and nothing in flight: stop being visited.
            if peer
                .outgoing_reliable
                .lock()
                .values()
                .all(|queue| queue.is_empty())
                && peer.pending_total.load(Ordering::Relaxed) == 0
            {
                peer.refresh_reliable_active();
            }

            // A single UDP send must not park the global dispatcher. Preserve unsent datagrams
            // on this peer and let later peers progress; the next pass retries this peer first.
            builder.flush_for_peer(&peer, &handle, addr);
        }
    }
}

/// Retransmit overdue reliable packets and emit keepalive pings. Both are coarse-grained by
/// nature (150 ms and 1500 ms), so keeping them out of the dispatch loop is what allows that
/// loop to run at 2 ms without also re-walking every in-flight packet 500 times a second.
async fn reliable_maintenance_loop(handle: TransportHandle) {
    let mut tick = time::interval(RELIABLE_MAINTENANCE_INTERVAL);
    tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut builder = MergedDatagramBuilder::new();
    while !handle.shutdown.load(Ordering::Relaxed) {
        tick.tick().await;
        let now = Instant::now();

        for peer in handle.peers.iter() {
            let addr = peer.addr;
            let connection_number = peer.connection_number;
            // Hold the state lock through the nonblocking send so a response cannot replace the
            // pending token between preparation and send bookkeeping.
            let _ = try_send_mtu_probe(
                &mut peer.mtu_probe.lock(),
                now,
                connection_number,
                |probe| handle.try_send_raw_to(probe, addr),
            );
            builder.reset(connection_number);
            let Some(_send_turn) = try_peer_send_turn(&peer) else {
                continue;
            };
            if !retry_peer_datagrams(&peer, &handle, addr) {
                continue;
            }

            {
                let mut last_ping = peer.last_ping_sent.lock();
                if now.duration_since(*last_ping) >= PEER_PING_INTERVAL {
                    *last_ping = now;
                    let sequence = peer.next_ping_sequence.fetch_add(1, Ordering::Relaxed);
                    let packet = build_ping_packet(connection_number, sequence);
                    builder.push(&packet);
                }
            }

            if peer.reliable_active.load(Ordering::Acquire) {
                let mut pending = peer.pending_reliable.lock();
                let mut retransmits = Vec::new();
                for queue in pending.values_mut() {
                    for item in queue.iter_mut() {
                        if now.duration_since(item.last_sent) >= RELIABLE_RETRANSMIT_AFTER {
                            item.last_sent = now;
                            retransmits.push(item.bytes.clone());
                        }
                    }
                }
                drop(pending);
                if !retransmits.is_empty() {
                    if handle.extended_statistics_enabled() {
                        handle
                            .stats
                            .reliable_retransmits
                            .fetch_add(retransmits.len() as u64, Ordering::Relaxed);
                    }
                    for bytes in &retransmits {
                        builder.push(bytes);
                    }
                }
            }

            builder.flush_for_peer(&peer, &handle, addr);
        }
    }
}

/// Accumulates one peer's packets into 1200-byte merged datagrams, reusing its buffers across
/// peers and passes. The previous code allocated a `Vec` per packet, then copied every packet
/// a second time into a global regroup `HashMap`, then allocated a third time while framing.
struct MergedDatagramBuilder {
    connection_number: u8,
    current: Vec<u8>,
    datagrams: Vec<Vec<u8>>,
    count: usize,
}

impl MergedDatagramBuilder {
    fn new() -> Self {
        Self {
            connection_number: 0,
            current: Vec::with_capacity(MAX_MERGED_PACKET_SIZE),
            datagrams: Vec::new(),
            count: 0,
        }
    }

    fn reset(&mut self, connection_number: u8) {
        self.connection_number = connection_number;
        self.current.clear();
        for datagram in self.datagrams.iter_mut() {
            datagram.clear();
        }
        self.datagrams.clear();
        self.count = 0;
    }

    fn start_datagram(&mut self) {
        self.current.clear();
        self.current
            .push(PacketProperty::Merged as u8 | (self.connection_number << 5));
        self.count = 0;
    }

    fn push(&mut self, packet: &[u8]) {
        let framed_len = 2 + packet.len();
        if self.count > 0 && self.current.len() + framed_len > MAX_MERGED_PACKET_SIZE {
            self.seal_datagram();
        }
        if framed_len + 1 > MAX_MERGED_PACKET_SIZE {
            // A single packet that cannot fit alongside others travels on its own.
            if self.count > 0 {
                self.seal_datagram();
            }
            self.datagrams.push(packet.to_vec());
            return;
        }
        if self.count == 0 && self.current.is_empty() {
            self.start_datagram();
        }
        self.current
            .extend_from_slice(&(packet.len() as u16).to_le_bytes());
        self.current.extend_from_slice(packet);
        self.count += 1;
    }

    /// Move the in-progress datagram aside, unwrapping the single-packet case exactly as
    /// `unpack_single_merged_packet` did.
    fn seal_datagram(&mut self) {
        if self.count == 0 {
            return;
        }
        if self.count == 1 {
            let mut single = Vec::with_capacity(self.current.len());
            single.extend_from_slice(&unpack_single_merged_packet(&self.current));
            self.datagrams.push(single);
        } else {
            self.datagrams.push(std::mem::take(&mut self.current));
        }
        self.current = Vec::with_capacity(MAX_MERGED_PACKET_SIZE);
        self.count = 0;
    }

    fn flush_for_peer(
        &mut self,
        peer: &PeerState,
        handle: &TransportHandle,
        addr: SocketAddr,
    ) -> bool {
        self.seal_datagram();
        if !self.datagrams.is_empty() {
            peer.pending_datagrams
                .lock()
                .extend(self.datagrams.drain(..));
            peer.reliable_active.store(true, Ordering::Release);
        }
        retry_peer_datagrams(peer, handle, addr)
    }

    #[cfg(test)]
    fn flush_with(&mut self, peer: &PeerState, mut try_send: impl FnMut(&[u8]) -> bool) -> bool {
        self.seal_datagram();
        peer.pending_datagrams
            .lock()
            .extend(self.datagrams.drain(..));
        peer_send_turn(peer, |datagram| Ok(try_send(datagram)))
    }
}

/// Attempt a peer's queued UDP datagrams without waiting for socket writability. A WouldBlock
/// leaves the entire unsent suffix queued for its next fair turn; permanent socket errors retain
/// the historical drop behavior because UDP cannot be partially sent.
fn retry_peer_datagrams(peer: &PeerState, handle: &TransportHandle, addr: SocketAddr) -> bool {
    peer_send_turn(peer, |datagram| handle.try_send_raw_to(datagram, addr))
}

fn try_peer_send_turn(peer: &PeerState) -> Option<parking_lot::MutexGuard<'_, ()>> {
    peer.reliable_send_turn.try_lock()
}

fn peer_send_turn(peer: &PeerState, mut try_send: impl FnMut(&[u8]) -> Result<bool>) -> bool {
    let mut pending = peer.pending_datagrams.lock();
    while let Some(datagram) = pending.front() {
        match try_send(datagram) {
            Ok(true) | Err(_) => {
                pending.pop_front();
            }
            Ok(false) => return false,
        }
    }
    true
}

fn relative_sequence(seq: u16, expected: u16) -> i32 {
    let seq = seq as i32;
    let expected = expected as i32;
    let diff = seq - expected;
    if diff <= -((MAX_SEQUENCE / 2) as i32) {
        diff + MAX_SEQUENCE as i32
    } else if diff >= (MAX_SEQUENCE / 2) as i32 {
        diff - MAX_SEQUENCE as i32
    } else {
        diff
    }
}

fn dotnet_utc_ticks() -> i64 {
    const TICKS_AT_UNIX_EPOCH: i64 = 621_355_968_000_000_000;
    let unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    TICKS_AT_UNIX_EPOCH + unix.as_secs() as i64 * 10_000_000 + (unix.subsec_nanos() / 100) as i64
}

pub fn any_addr(port: u16) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), port)
}

pub fn channel_name(channel: u8) -> &'static str {
    match channel {
        channels::AUTH_IDENTITY => "AuthIdentity",
        channels::META_DATA => "MetaData",
        channels::DISCONNECTION => "Disconnection",
        channels::VOICE => "Voice",
        channels::PLAYER_AVATAR_HIGH => "PlayerAvatarHigh",
        channels::CHAT => "Chat",
        channels::ADMIN => "Admin",
        _ => "Unknown",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn blocked_peer_retains_reliable_and_ack_datagrams_without_stalling_next_peer() {
        let blocked = test_peer_state(1);
        let later = test_peer_state(2);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);
        let built = build_queued_reliable_packet(
            &blocked,
            channel_id,
            OutgoingReliable {
                payload: vec![0x5a],
                fragment: None,
            },
        );
        record_pending_reliable(
            &blocked,
            channel_id,
            built.sequence,
            built.bytes.clone(),
            None,
        );

        // Match dispatch's ACK collection: once encoded, the ACK is no longer dirty. The
        // datagram itself must remain queued until the kernel accepts it.
        queue_ack(&blocked, channel_id, 7);
        let ack_packet = {
            let mut acks = blocked.outgoing_acks.lock();
            let ack = acks.get_mut(&channel_id).unwrap();
            assert!(ack_bit(&ack.bits, 7));
            let packet = build_ack_packet(
                blocked.connection_number,
                channel_id,
                ack.window_start,
                &ack.bits,
            );
            ack.dirty = false;
            packet
        };

        let mut blocked_batch = MergedDatagramBuilder::new();
        blocked_batch.push(&built.bytes);
        blocked_batch.push(&ack_packet);
        let mut attempted = None;
        let _blocked_turn = try_peer_send_turn(&blocked).expect("first peer turn is free");
        assert!(!blocked_batch.flush_with(&blocked, |datagram| {
            attempted = Some(datagram.to_vec());
            false // deterministic kernel WouldBlock
        }));
        assert_eq!(blocked.pending_datagrams.lock().len(), 1);
        assert!(blocked.reliable_active.load(Ordering::Acquire));
        assert_eq!(
            blocked.pending_reliable.lock()[&channel_id][0].bytes,
            built.bytes
        );

        // The dispatcher can visit another peer immediately while the first peer remains
        // blocked. This is the head-of-line condition the old awaited flush could not pass.
        assert!(try_peer_send_turn(&blocked).is_none());
        let mut later_batch = MergedDatagramBuilder::new();
        later_batch.push(&[0x33]);
        let mut later_sent = false;
        let _later_turn = try_peer_send_turn(&later).expect("later peer has its own turn");
        assert!(later_batch.flush_with(&later, |_| {
            later_sent = true;
            true
        }));
        assert!(later_sent);

        // On the next fair turn, retry the exact retained datagram and then allow its reliable
        // packet to be retired by the matching ACK.
        drop(_blocked_turn);
        let _blocked_retry_turn = try_peer_send_turn(&blocked).expect("next peer turn is free");
        let mut retried = None;
        assert!(peer_send_turn(&blocked, |datagram| {
            retried = Some(datagram.to_vec());
            Ok(true)
        }));
        assert_eq!(retried, attempted);
        assert!(blocked.pending_datagrams.lock().is_empty());
        assert!(!blocked.outgoing_acks.lock()[&channel_id].dirty);

        let mut ack = build_ack_packet(
            blocked.connection_number,
            channel_id,
            built.sequence,
            &[1; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2],
        );
        // Keep only the bit for the sent sequence set.
        ack[4..].fill(0);
        ack[4 + (built.sequence as usize / 8)] |= 1 << (built.sequence % 8);
        process_ack(&blocked, &ack, None);
        assert_eq!(blocked.total_pending(), 0);
    }

    #[tokio::test]
    async fn blocked_udp_send_preserves_dispatch_fairness_and_ack_delivery() {
        let (handle, _events) = TransportHandle::bind(loopback_addr(0)).await.unwrap();
        let client_a = UdpSocket::bind(loopback_addr(0)).await.unwrap();
        let client_b = UdpSocket::bind(loopback_addr(0)).await.unwrap();
        let addr_a = client_a.local_addr().unwrap();
        let addr_b = client_b.local_addr().unwrap();
        let peer_a = handle
            .accept(&ConnectionRequest {
                remote_addr: addr_a,
                payload: Bytes::new(),
                connection_number: 0,
                connect_time: 1,
                local_peer_id: 0,
            })
            .await
            .unwrap();
        let peer_b = handle
            .accept(&ConnectionRequest {
                remote_addr: addr_b,
                payload: Bytes::new(),
                connection_number: 0,
                connect_time: 2,
                local_peer_id: 0,
            })
            .await
            .unwrap();
        let state_a = handle.peers.get(&peer_a).unwrap().clone();
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);

        // Clear the two ConnectAccept packets before the actual dispatcher starts.
        let mut buf = vec![0; 65_535];
        for client in [&client_a, &client_b] {
            tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
                .await
                .expect("ConnectAccept timed out")
                .unwrap();
        }

        // This test-only fault injection makes try_send_to report WouldBlock for exactly one
        // peer while the real reliable dispatcher, maintenance loop, and UDP receive loops run.
        handle.test_blocked_send_addrs.write().insert(addr_a);
        handle
            .send(
                peer_a,
                channels::CHAT,
                DeliveryMethod::ReliableOrdered,
                &[0x5a],
            )
            .await
            .unwrap();
        handle
            .send(
                peer_b,
                channels::CHAT,
                DeliveryMethod::ReliableOrdered,
                &[0x6b],
            )
            .await
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(3);
        let mut b_received = false;
        while Instant::now() < deadline && !b_received {
            if let Ok(Ok((len, _))) =
                tokio::time::timeout(Duration::from_millis(100), client_b.recv_from(&mut buf)).await
            {
                let mut seen = HashSet::new();
                let mut arrived = Vec::new();
                collect_reliable_sequences(&buf[..len], &mut seen, &mut arrived);
                b_received = !arrived.is_empty();
            }
        }
        assert!(
            b_received,
            "the later peer must receive despite peer A's WouldBlock"
        );
        assert_eq!(state_a.pending_datagrams.lock().len(), 1);
        assert_eq!(state_a.total_queued(), 0);

        // While A is blocked, its incoming reliable packet queues a server ACK. The dispatcher
        // must leave that ACK dirty and avoid growing A's retained batch on subsequent turns.
        let inbound = vec![PacketProperty::Channeled as u8, 0, 0, channel_id, 0x31];
        client_a
            .send_to(&inbound, handle.local_addr().unwrap())
            .await
            .unwrap();
        let ack_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < ack_deadline
            && !state_a
                .outgoing_acks
                .lock()
                .get(&channel_id)
                .is_some_and(|ack| ack.dirty)
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert!(state_a.outgoing_acks.lock()[&channel_id].dirty);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(state_a.pending_datagrams.lock().len(), 1);

        // Acknowledge B's first send, then allow A's send queue to recover.
        let mut ack_b = vec![PacketProperty::Ack as u8, 0, 0, channel_id];
        ack_b.extend_from_slice(&[1; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2]);
        client_b
            .send_to(&ack_b, handle.local_addr().unwrap())
            .await
            .unwrap();
        handle.test_blocked_send_addrs.write().remove(&addr_a);

        let mut a_received_reliable = false;
        let mut a_received_ack = false;
        let retry_deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < retry_deadline && !(a_received_reliable && a_received_ack) {
            if let Ok(Ok((len, _))) =
                tokio::time::timeout(Duration::from_millis(100), client_a.recv_from(&mut buf)).await
            {
                let packet = &buf[..len];
                let mut seen = HashSet::new();
                let mut arrived = Vec::new();
                collect_reliable_sequences(packet, &mut seen, &mut arrived);
                a_received_reliable |= !arrived.is_empty();
                a_received_ack |= packet_has_ack(packet, channel_id, 0);
            }
        }
        assert!(
            a_received_reliable,
            "peer A's reliable datagram must be retried"
        );
        assert!(
            a_received_ack,
            "peer A's pending ACK must be delivered after recovery"
        );

        let mut ack_a = vec![PacketProperty::Ack as u8, 0, 0, channel_id];
        ack_a.extend_from_slice(&[1; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2]);
        client_a
            .send_to(&ack_a, handle.local_addr().unwrap())
            .await
            .unwrap();
        let clear_deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < clear_deadline && handle.pending_reliable_count() != 0 {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        assert_eq!(handle.pending_reliable_count(), 0);
        assert!(state_a.pending_datagrams.lock().is_empty());
        handle.shutdown();
    }

    #[test]
    fn packet_property_masks_connection_number() {
        assert_eq!(
            PacketProperty::from_byte(PacketProperty::ConnectRequest as u8 | (2 << 5)),
            Some(PacketProperty::ConnectRequest)
        );
    }

    #[test]
    fn connection_number_ignores_fragment_bit() {
        let header = PacketProperty::Channeled as u8 | (2 << 5) | 0x80;
        assert_eq!((header & 0x60) >> 5, 2);
    }

    #[test]
    fn connect_request_rejects_invalid_protocol() {
        let mut bytes = vec![0u8; 18 + 16];
        bytes[0] = PacketProperty::ConnectRequest as u8;
        bytes[1..5].copy_from_slice(&999i32.to_le_bytes());
        bytes[17] = 16;
        assert!(matches!(
            parse_connect_request(&bytes),
            ConnectRequestParse::InvalidProtocol
        ));
    }

    #[test]
    fn connect_request_requires_litenetlib_address_size() {
        let mut bytes = vec![0u8; 18 + 8];
        bytes[0] = PacketProperty::ConnectRequest as u8;
        bytes[1..5].copy_from_slice(&LITENETLIB_PROTOCOL_ID.to_le_bytes());
        bytes[17] = 8;
        assert!(matches!(
            parse_connect_request(&bytes),
            ConnectRequestParse::Malformed
        ));
    }

    #[test]
    fn relative_sequence_wrap_shape_is_known() {
        assert_eq!(relative_sequence(0, MAX_SEQUENCE - 1), 1);
        assert_eq!(relative_sequence(MAX_SEQUENCE - 1, 0), -1);
    }

    #[test]
    fn payload_patch_is_applied_during_copy() {
        let payload = Bytes::from_static(&[1, 2, 3, 4]);
        let mut output = vec![0xaa, 0xbb];
        extend_payload_with_patch(&mut output, &payload, Some((2, 9)));
        assert_eq!(output, [0xaa, 0xbb, 1, 2, 9, 4]);

        let mut unchanged = Vec::new();
        extend_payload_with_patch(&mut unchanged, &payload, Some((99, 9)));
        assert_eq!(unchanged, payload);
    }

    #[test]
    fn multiple_small_packets_are_sent_as_litenetlib_merged_datagram() {
        let datagrams = build_merged_datagrams(
            0,
            vec![
                vec![PacketProperty::Channeled as u8, 0, 0, 0x8a, 1],
                vec![PacketProperty::Channeled as u8, 1, 0, 0x8a, 2],
            ],
        );

        assert_eq!(datagrams.len(), 1);
        assert_eq!(datagrams[0][0], PacketProperty::Merged as u8);
        assert_eq!(u16::from_le_bytes([datagrams[0][1], datagrams[0][2]]), 5);
        assert_eq!(
            &datagrams[0][3..8],
            &[PacketProperty::Channeled as u8, 0, 0, 0x8a, 1]
        );
        assert_eq!(u16::from_le_bytes([datagrams[0][8], datagrams[0][9]]), 5);
        assert_eq!(
            &datagrams[0][10..15],
            &[PacketProperty::Channeled as u8, 1, 0, 0x8a, 2]
        );
    }

    #[test]
    fn single_packet_batch_is_not_wrapped_as_merged() {
        let packet = vec![PacketProperty::Channeled as u8, 0, 0, 0x8a, 1];
        let datagrams = build_merged_datagrams(0, vec![packet.clone()]);
        assert_eq!(datagrams, vec![packet]);
    }

    /// The dispatch loop frames packets with `MergedDatagramBuilder` instead of
    /// `build_merged_datagrams`. The two must stay byte-identical or real clients break, so
    /// pin the equivalence across the size boundaries that exercise every branch: several
    /// packets per datagram, an exact fit, an oversized single packet, and the single-packet
    /// unwrap.
    #[test]
    fn merged_datagram_builder_matches_reference_framing() {
        let cases: Vec<Vec<Vec<u8>>> = vec![
            vec![],
            vec![vec![1, 2, 3]],
            vec![vec![1, 2, 3], vec![4, 5]],
            vec![vec![7u8; 600], vec![8u8; 600]],
            vec![vec![9u8; 1190]],
            vec![vec![9u8; 1190], vec![1]],
            vec![vec![1u8; 400]; 8],
            vec![vec![0xabu8; 1300], vec![0xcd, 0xef]],
        ];
        for (case_index, packets) in cases.iter().enumerate() {
            for connection_number in [0u8, 3] {
                let reference = build_merged_datagrams(connection_number, packets.clone());
                let mut builder = MergedDatagramBuilder::new();
                builder.reset(connection_number);
                for packet in packets.iter() {
                    builder.push(packet);
                }
                builder.seal_datagram();
                let built = std::mem::take(&mut builder.datagrams);
                assert_eq!(
                    built, reference,
                    "framing diverged for case {case_index} at connection number {connection_number}"
                );
            }
        }
    }

    /// Regression: an ACK must release exactly the packets it covers, from the oldest
    /// outwards, and must never release a packet that is still in flight. A single wrong bit
    /// here silently corrupts the reliable window for the life of the connection.
    #[test]
    fn ack_releases_only_acknowledged_prefix_of_the_window() {
        let state = test_peer_state(11);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);
        for sequence in 0..10u16 {
            state
                .pending_reliable
                .lock()
                .entry(channel_id)
                .or_default()
                .push_back(PendingReliable {
                    sequence,
                    bytes: vec![sequence as u8],
                    last_sent: Instant::now(),
                });
            state.pending_total.fetch_add(1, Ordering::Relaxed);
        }

        // Acknowledge sequences 0..=3 with the client's convention: `window_start` is the
        // oldest sequence the window covers and bits are set at `sequence % WINDOW_SIZE`.
        let mut ack = vec![PacketProperty::Ack as u8, 0, 0, channel_id];
        ack.extend_from_slice(&[0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2]);
        for sequence in 0..=3u16 {
            let bit = sequence as usize % DEFAULT_WINDOW_SIZE;
            ack[4 + bit / 8] |= 1 << (bit % 8);
        }
        ack[1..3].copy_from_slice(&0u16.to_le_bytes());
        process_ack(&state, &ack, None);

        let pending = state.pending_reliable.lock();
        let queue = &pending[&channel_id];
        assert_eq!(
            queue.len(),
            6,
            "only the acknowledged prefix may be released"
        );
        assert_eq!(
            queue.iter().map(|item| item.sequence).collect::<Vec<_>>(),
            vec![4, 5, 6, 7, 8, 9]
        );
    }

    #[test]
    fn ack_out_of_order_does_not_release_newer_packets() {
        let state = test_peer_state(12);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);
        for sequence in 0..5u16 {
            state
                .pending_reliable
                .lock()
                .entry(channel_id)
                .or_default()
                .push_back(PendingReliable {
                    sequence,
                    bytes: vec![sequence as u8],
                    last_sent: Instant::now(),
                });
            state.pending_total.fetch_add(1, Ordering::Relaxed);
        }

        // An ACK for a sequence the peer has not reached yet must not disturb the window.
        let mut ack = vec![PacketProperty::Ack as u8, 0, 0, channel_id];
        ack.extend_from_slice(&[0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2]);
        let bit = 9usize % DEFAULT_WINDOW_SIZE;
        ack[4 + bit / 8] |= 1 << (bit % 8);
        ack[1..3].copy_from_slice(&9u16.to_le_bytes());
        process_ack(&state, &ack, None);

        assert_eq!(state.pending_reliable.lock()[&channel_id].len(), 5);
    }

    #[test]
    fn reliable_sequenced_ack_bytes_match_csharp_header_only_semantics() {
        let peer = test_peer_state(19);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableSequenced);

        assert_eq!(
            queue_reliable_sequenced_ack(&peer, channel_id, 1),
            Some((1, true))
        );
        assert_eq!(
            build_reliable_sequenced_ack(2, channel_id, 1),
            [PacketProperty::Ack as u8 | (2 << 5), 1, 0, channel_id]
        );
        assert_eq!(
            queue_reliable_sequenced_ack(&peer, channel_id, 1),
            Some((1, false)),
            "a duplicate re-ACKs the current remote sequence"
        );
        assert_eq!(
            queue_reliable_sequenced_ack(&peer, channel_id, 0),
            Some((1, false)),
            "an older update does not move the sequenced ACK backward"
        );
        assert_eq!(
            queue_reliable_sequenced_ack(&peer, channel_id, MAX_SEQUENCE),
            None,
            "the C# rejects sequence values outside 0..32768"
        );

        peer.remote_sequenced_sequence
            .lock()
            .insert(channel_id, MAX_SEQUENCE - 1);
        assert_eq!(
            queue_reliable_sequenced_ack(&peer, channel_id, 0),
            Some((0, true)),
            "the ACK sequence advances correctly across 32767 to 0"
        );
    }

    #[test]
    fn reliable_sequenced_starts_at_one_and_ack_zero_cannot_retire_it() {
        let peer = test_peer_state(24);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableSequenced);
        let built = build_queued_reliable_packet(
            &peer,
            channel_id,
            OutgoingReliable {
                payload: vec![0x5a],
                fragment: None,
            },
        );

        assert_eq!(built.sequence, 1);
        assert_eq!(
            u16::from_le_bytes([built.bytes[1], built.bytes[2]]),
            1,
            "LiteNetLib SequencedChannel increments before its first send"
        );

        record_pending_reliable(&peer, channel_id, built.sequence, built.bytes, None);
        let ack_zero = [PacketProperty::Ack as u8, 0, 0, channel_id];
        process_ack(&peer, &ack_zero, None);
        assert_eq!(
            peer.pending_total.load(Ordering::Relaxed),
            1,
            "the receiver's initial ACK sequence 0 must not retire the first real update"
        );

        let ack_one = [PacketProperty::Ack as u8, 1, 0, channel_id];
        process_ack(&peer, &ack_one, None);
        assert_eq!(peer.pending_total.load(Ordering::Relaxed), 0);

        let sequenced =
            build_outbound_packet(&peer, channels::CHAT, DeliveryMethod::Sequenced, &[0x33]);
        assert_eq!(
            u16::from_le_bytes([sequenced.bytes[1], sequenced.bytes[2]]),
            1,
            "unreliable Sequenced uses the same C# SequencedChannel numbering"
        );
    }

    #[test]
    fn dirty_ack_keeps_peer_reliable_active_until_it_is_flushed() {
        let peer = test_peer_state(25);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);

        assert!(!peer.reliable_active.load(Ordering::Acquire));
        queue_ack(&peer, channel_id, 0);
        assert!(peer.reliable_active.load(Ordering::Acquire));

        peer.refresh_reliable_active();
        assert!(
            peer.reliable_active.load(Ordering::Acquire),
            "dirty ACK work must keep the dispatch loop visiting this peer"
        );

        peer.outgoing_acks
            .lock()
            .get_mut(&channel_id)
            .unwrap()
            .dirty = false;
        peer.refresh_reliable_active();
        assert!(!peer.reliable_active.load(Ordering::Acquire));
    }

    #[test]
    fn pending_overflow_evicts_exactly_one_oldest_packet() {
        let peer = test_peer_state(26);
        let base = Instant::now();

        {
            let mut pending = peer.pending_reliable.lock();
            for channel_id in 0..32u8 {
                let queue = pending.entry(channel_id).or_default();
                for sequence in 0..DEFAULT_WINDOW_SIZE as u16 {
                    queue.push_back(PendingReliable {
                        sequence,
                        bytes: vec![channel_id, sequence as u8],
                        last_sent: base
                            + Duration::from_millis(channel_id as u64)
                            + Duration::from_micros(sequence as u64),
                    });
                }
            }
        }
        peer.pending_total
            .store(MAX_PENDING_RELIABLE_PER_PEER, Ordering::Relaxed);

        record_pending_reliable(&peer, 250, 0, vec![0xaa], None);

        let pending = peer.pending_reliable.lock();
        let actual_total: usize = pending.values().map(VecDeque::len).sum();
        assert_eq!(actual_total, MAX_PENDING_RELIABLE_PER_PEER);
        assert_eq!(
            peer.pending_total.load(Ordering::Relaxed),
            actual_total,
            "the O(1) counter must stay equal to the actual queue population"
        );
        assert_eq!(pending[&0].len(), DEFAULT_WINDOW_SIZE - 1);
        for channel_id in 1..32u8 {
            assert_eq!(
                pending[&channel_id].len(),
                DEFAULT_WINDOW_SIZE,
                "searching for the oldest packet must not pop other channel fronts"
            );
        }
        assert_eq!(pending[&250].len(), 1);
    }

    #[test]
    fn invalid_inbound_reliable_sequence_does_not_dirty_an_ack_window() {
        let peer = test_peer_state(20);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);
        queue_ack(&peer, channel_id, MAX_SEQUENCE);
        assert!(peer.outgoing_acks.lock().is_empty());
        assert!(!peer.reliable_active.load(Ordering::Acquire));
    }

    /// Regression: ACKs are individual UDP datagrams, so any one of them can be dropped. If
    /// only a prefix were released, one lost ACK would pin the oldest packet forever, every
    /// later ACK would compare negative against it, and the send window would deadlock
    /// permanently -- the queue would grow without bound while retransmits ran flat out.
    ///
    /// This reproduces that: skip the ACK for sequence 0, then deliver the rest, and require
    /// the window to keep opening.
    #[test]
    fn a_single_dropped_ack_does_not_deadlock_the_window() {
        let state = test_peer_state(13);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);
        for sequence in 0..8u16 {
            state
                .pending_reliable
                .lock()
                .entry(channel_id)
                .or_default()
                .push_back(PendingReliable {
                    sequence,
                    bytes: vec![sequence as u8],
                    last_sent: Instant::now(),
                });
            state.pending_total.fetch_add(1, Ordering::Relaxed);
        }

        // The C# client never observes sequence 0, but observes 1..=7, and its window slides
        // past 0 on arrival -- so its ACK covers 1..=7 and nothing acknowledges sequence 0.
        let mut reference = CsReliableChannel::default();
        for sequence in 1..8u16 {
            assert!(reference.process(sequence));
        }
        let (window_start, bits) = reference.encode_ack().unwrap();
        let mut ack = vec![PacketProperty::Ack as u8, 0, 0, channel_id];
        ack.extend_from_slice(&bits);
        ack[1..3].copy_from_slice(&window_start.to_le_bytes());
        process_ack(&state, &ack, None);

        let remaining = state
            .pending_reliable
            .lock()
            .get(&channel_id)
            .map(VecDeque::len)
            .unwrap_or(0);
        assert_eq!(
            remaining, 1,
            "only the unacknowledged packet may remain; a dropped ACK must not stall the window"
        );
        assert_eq!(state.pending_total.load(Ordering::Relaxed), 1);
    }

    /// An ACK produced by the C# reference must release exactly the sequences it covers, once
    /// the window has slid well past the start of the sequence space.
    ///
    /// This is the interop proof in the decode direction: the Unity client's own ACK encoding,
    /// fed to the server's `process_ack`, has to drain the window regardless of where in the
    /// sequence space the channel has got to. The state here is the one a real connection is
    /// actually in -- the client has received 0..=200 (bar one lost packet), so earlier ACKs
    /// already released everything below 195, and this ACK is what releases the tail.
    #[test]
    fn csharp_reference_ack_releases_its_sequences_at_large_offsets() {
        let state = test_peer_state(21);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);
        // Only the tail is still in flight, because the earlier ACKs released the rest.
        for sequence in 195..=200u16 {
            state
                .pending_reliable
                .lock()
                .entry(channel_id)
                .or_default()
                .push_back(PendingReliable {
                    sequence,
                    bytes: vec![sequence as u8],
                    last_sent: Instant::now(),
                });
            state.pending_total.fetch_add(1, Ordering::Relaxed);
        }

        // The C# received everything except sequence 198.
        let mut reference = CsReliableChannel::default();
        for sequence in (0..=200u16).filter(|s| *s != 198) {
            assert!(reference.process(sequence));
        }
        let (window_start, bits) = reference.encode_ack().expect("channel has seen packets");
        assert_eq!(
            window_start, 73,
            "the reference window must have slid off a 128 boundary for this test to mean anything"
        );

        let mut ack = vec![PacketProperty::Ack as u8, 0, 0, channel_id];
        ack.extend_from_slice(&bits);
        ack[1..3].copy_from_slice(&window_start.to_le_bytes());
        process_ack(&state, &ack, None);

        let remaining = state
            .pending_reliable
            .lock()
            .get(&channel_id)
            .map(VecDeque::len)
            .unwrap_or(0);
        assert_eq!(
            remaining, 1,
            "a C#-shaped windowed ACK must release the sequences it covers regardless of the \
             absolute sequence values"
        );
    }

    /// The reverse direction: the ACKs the server emits must be byte-identical to what the C#
    /// would emit. A Unity client cannot drain its own send window otherwise, and this is the
    /// only thing that catches it -- decode-side tests pass either way.
    #[test]
    fn outgoing_acks_match_the_csharp_reference_byte_for_byte() {
        const COUNT: u16 = 201;
        let state = test_peer_state(22);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);

        let mut reference = CsReliableChannel::default();
        for sequence in 0..COUNT {
            queue_ack(&state, channel_id, sequence);
            reference.process(sequence);
        }
        let (expected_start, expected_bits) = reference.encode_ack().unwrap();
        let packet = {
            let acks = state.outgoing_acks.lock();
            let ack = &acks[&channel_id];
            build_ack_packet(0, channel_id, ack.window_start, &ack.bits)
        };
        assert_eq!(
            &packet[1..3],
            &expected_start.to_le_bytes(),
            "ACK header window start must match the C#"
        );
        assert_eq!(
            &packet[4..],
            &expected_bits[..],
            "ACK bit set must match the C# byte for byte"
        );
    }

    /// Golden vectors captured by running the **verbatim** `ReliableChannel.cs` from
    /// `Packages/com.basis.server/LiteNetLib` against a stub peer, and dumping the ACK datagrams
    /// it puts on the wire.
    ///
    /// These are the whole point of the exercise: the expected bytes come from the C# itself, not
    /// from this codebase's reading of it. Regenerate them by replaying the same sequences
    /// through that file. `build_ack_packet` must reproduce every one of them exactly, because a
    /// Unity client drops an ACK whose size or layout is off and cannot drain its send window.
    #[test]
    fn outgoing_acks_match_golden_vectors_from_the_csharp() {
        fn hex(bytes: &[u8]) -> String {
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }
        fn ack_for(state: &PeerState, channel_id: u8) -> String {
            let acks = state.outgoing_acks.lock();
            let ack = &acks[&channel_id];
            hex(&build_ack_packet(
                0,
                channel_id,
                ack.window_start,
                &ack.bits,
            ))
        }

        // Six contiguous from zero: the window never slides, so it stays at 0.
        let state = test_peer_state(31);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);
        for sequence in 0..6u16 {
            queue_ack(&state, channel_id, sequence);
        }
        assert_eq!(
            ack_for(&state, channel_id),
            "0200004a3f00000000000000000000000000000000"
        );

        // 201 contiguous with 198 lost. The window slides twice to 73, which is not a multiple
        // of 128, and bit 70 stays clear. This is the case that separates absolute bit indexing
        // from window-relative indexing, and it is why a lone client can look fine while the
        // window wedges under load.
        let state = test_peer_state(32);
        for sequence in (0..=200u16).filter(|s| *s != 198) {
            queue_ack(&state, channel_id, sequence);
        }
        assert_eq!(
            ack_for(&state, channel_id),
            "0249004affffffffffffffffbfffffffffffffff00"
        );

        // Out of order with unfilled gaps: the window start must not move.
        let state = test_peer_state(33);
        for sequence in [0u16, 1, 2, 5, 6, 9] {
            queue_ack(&state, channel_id, sequence);
        }
        assert_eq!(
            ack_for(&state, channel_id),
            "0200004a6702000000000000000000000000000000"
        );

        // An arrival from beyond the window slides it by the minimum, landing the newcomer on
        // the top bit rather than jumping the start to it.
        let state = test_peer_state(34);
        for sequence in 0..4u16 {
            queue_ack(&state, channel_id, sequence);
        }
        queue_ack(&state, channel_id, DEFAULT_WINDOW_SIZE as u16 + 3);
        assert_eq!(
            ack_for(&state, channel_id),
            "0204004a0800000000000000000000000000000000"
        );

        // Retransmits of sequences still inside the window must stay acknowledged. A window that
        // slid on contiguous arrivals would strand the sender's oldest packet for good.
        let state = test_peer_state(35);
        for sequence in 0..8u16 {
            queue_ack(&state, channel_id, sequence);
        }
        queue_ack(&state, channel_id, 0);
        queue_ack(&state, channel_id, 3);
        assert_eq!(
            ack_for(&state, channel_id),
            "0200004aff00000000000000000000000000000000"
        );
    }

    /// The decode direction, pinned to what the C# actually accepts.
    ///
    /// Captured the same way: with 128 packets in flight and the sender's window start moved to
    /// 64 by a first ACK, the C# drains completely on an ACK whose bits are absolute
    /// (`sequence % 128`) and drains **nothing** on the same coverage expressed relative to the
    /// window start. Reading those bits the second way is a silent stall, not a visible error,
    /// which is why it is worth asserting.
    #[test]
    fn decoding_matches_what_the_csharp_accepts() {
        fn ack(channel_id: u8, window_start: u16, first: u16, last: u16) -> Vec<u8> {
            let mut bits = vec![0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
            for sequence in first..=last {
                let index = sequence as usize % DEFAULT_WINDOW_SIZE;
                bits[index / 8] |= 1 << (index % 8);
            }
            let mut packet = vec![PacketProperty::Ack as u8, 0, 0, channel_id];
            packet.extend_from_slice(&bits);
            packet[1..3].copy_from_slice(&window_start.to_le_bytes());
            packet
        }
        fn queue(state: &PeerState, channel_id: u8, from: u16, to: u16) {
            for sequence in from..=to {
                state
                    .pending_reliable
                    .lock()
                    .entry(channel_id)
                    .or_default()
                    .push_back(PendingReliable {
                        sequence,
                        bytes: vec![sequence as u8],
                        last_sent: Instant::now(),
                    });
                state.pending_total.fetch_add(1, Ordering::Relaxed);
            }
        }
        fn remaining(state: &PeerState, channel_id: u8) -> usize {
            state
                .pending_reliable
                .lock()
                .get(&channel_id)
                .map(VecDeque::len)
                .unwrap_or(0)
        }

        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);
        let malformed = test_peer_state(38);
        queue(&malformed, channel_id, 0, 1);
        let valid_ack = ack(channel_id, 0, 0, 0);
        process_ack(&malformed, &valid_ack[..valid_ack.len() - 1], None);
        let mut overlong_ack = valid_ack.clone();
        overlong_ack.push(0);
        process_ack(&malformed, &overlong_ack, None);
        process_ack(&malformed, &ack(channel_id, MAX_SEQUENCE, 0, 0), None);
        assert_eq!(
            remaining(&malformed, channel_id),
            2,
            "the C# rejects malformed sizes and sequence values outside 0..32768"
        );

        // C# limits the send window by sequence span, not by the number of packets that still
        // need retransmission. ACKing 1..=127 while 0 is missing must not permit sequence 128.
        let hole = test_peer_state(39);
        queue(&hole, channel_id, 0, 127);
        process_ack(&hole, &ack(channel_id, 0, 1, 127), None);
        let mut outgoing = VecDeque::from([
            OutgoingReliable {
                payload: vec![1],
                fragment: None,
            },
            OutgoingReliable {
                payload: vec![2],
                fragment: None,
            },
        ]);
        {
            let pending = hole.pending_reliable.lock();
            let drained =
                dequeue_reliable_window(128, &pending[&channel_id], &mut outgoing, usize::MAX);
            assert!(drained.is_empty());
            assert_eq!(
                outgoing.len(),
                2,
                "the hole keeps the full sequence span occupied"
            );
        }
        process_ack(&hole, &ack(channel_id, 0, 0, 127), None);
        {
            let pending = hole.pending_reliable.lock();
            let drained = dequeue_reliable_window(128, &VecDeque::new(), &mut outgoing, usize::MAX);
            assert_eq!(
                drained.len(),
                2,
                "covering sequence 0 opens the queued window"
            );
            assert!(outgoing.is_empty());
            assert!(pending.get(&channel_id).is_none_or(VecDeque::is_empty));
        }
        assert_eq!(reliable_window_capacity(0, Some(MAX_SEQUENCE - 1)), 127);

        // A delayed duplicate ACK for the original 0..=9 window is still a valid ACK header
        // after the local start has advanced to 10, but its repeated low bits must not alias and
        // release the newly sent 128..=137 packets. C# stops scanning at relative sequence 128.
        let delayed = test_peer_state(40);
        queue(&delayed, channel_id, 0, 127);
        let old_ack = ack(channel_id, 0, 0, 9);
        process_ack(&delayed, &old_ack, None);
        assert_eq!(reliable_window_capacity(128, Some(10)), 10);
        queue(&delayed, channel_id, 128, 137);
        process_ack(&delayed, &old_ack, None);
        let pending = delayed.pending_reliable.lock();
        let delayed_queue = &pending[&channel_id];
        assert!(delayed_queue.iter().any(|item| item.sequence == 128));
        assert!(delayed_queue.iter().any(|item| item.sequence == 137));
        drop(pending);

        let absolute = test_peer_state(36);
        queue(&absolute, channel_id, 0, 127);
        process_ack(&absolute, &ack(channel_id, 0, 0, 63), None);
        process_ack(&absolute, &ack(channel_id, 64, 64, 127), None);
        assert_eq!(
            remaining(&absolute, channel_id),
            0,
            "absolute bits must drain the window"
        );

        // Same coverage, bits placed relative to the window start. The C# releases nothing here.
        let relative = test_peer_state(37);
        queue(&relative, channel_id, 0, 127);
        process_ack(&relative, &ack(channel_id, 0, 0, 63), None);
        process_ack(&relative, &ack(channel_id, 64, 0, 63), None);
        assert_eq!(remaining(&relative, channel_id), 64);
    }

    #[test]
    fn reliable_sequenced_ack_uses_header_only_and_retires_superseded_records() {
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableSequenced);
        let peer = test_peer_state(7);
        {
            let mut pending = peer.pending_reliable.lock();
            pending.insert(
                channel_id,
                (8..=10)
                    .map(|sequence| PendingReliable {
                        sequence,
                        bytes: vec![PacketProperty::Channeled as u8, 0, 0, channel_id],
                        last_sent: Instant::now(),
                    })
                    .collect(),
            );
        }
        peer.pending_total.store(3, Ordering::Relaxed);

        // LiteNetLib's reliable SequencedChannel ACK packet has only its 4-byte header. An ACK
        // for sequence 9 supersedes older updates but leaves already-sent newer sequence 10.
        let mut ack = vec![PacketProperty::Ack as u8, 9, 0, channel_id];
        process_ack(&peer, &ack, None);
        assert_eq!(peer.pending_total.load(Ordering::Relaxed), 1);
        assert_eq!(
            peer.pending_reliable.lock()[&channel_id]
                .iter()
                .map(|item| item.sequence)
                .collect::<Vec<_>>(),
            vec![10]
        );

        // Header-only shape is specific to ReliableSequenced. A future ACK cannot retire any
        // in-flight work, and an overlong ACK is rejected before window processing.
        ack[1..3].copy_from_slice(&11u16.to_le_bytes());
        process_ack(&peer, &ack, None);
        assert_eq!(peer.pending_total.load(Ordering::Relaxed), 1);
        ack[1..3].copy_from_slice(&MAX_SEQUENCE.to_le_bytes());
        process_ack(&peer, &ack, None);
        assert_eq!(peer.pending_total.load(Ordering::Relaxed), 1);
        ack[1..3].copy_from_slice(&10u16.to_le_bytes());
        ack.push(0);
        process_ack(&peer, &ack, None);
        assert_eq!(peer.pending_total.load(Ordering::Relaxed), 1);
    }

    /// When a packet arrives from beyond the window, LiteNetLib slides the window by the
    /// minimum needed to bring the newcomer back inside -- the new sequence lands on the top
    /// bit, and the window start does *not* jump to it -- clearing the bits left behind.
    #[test]
    fn outgoing_ack_window_slides_exactly_like_the_csharp() {
        let state = test_peer_state(23);
        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);

        let mut reference = CsReliableChannel::default();
        for sequence in 0..4u16 {
            queue_ack(&state, channel_id, sequence);
            reference.process(sequence);
        }
        // Far enough ahead to force a slide.
        let jumped = DEFAULT_WINDOW_SIZE as u16 + 3;
        queue_ack(&state, channel_id, jumped);
        reference.process(jumped);

        let (expected_start, expected_bits) = reference.encode_ack().unwrap();
        let acks = state.outgoing_acks.lock();
        let ack = &acks[&channel_id];
        assert_eq!(ack.window_start, expected_start);
        assert_eq!(
            ack.bits, expected_bits,
            "the slid window and its cleared bits must match the C#"
        );
        assert_eq!(
            expected_start, 4,
            "sliding brings the newcomer to the top of the window, it does not jump to it"
        );
        assert!(
            ack_bit(&ack.bits, jumped as usize % DEFAULT_WINDOW_SIZE),
            "the sequence that forced the slide is acknowledged at its absolute index"
        );
    }

    /// A literal transcription of the receive side of LiteNetLib's C# `ReliableChannel`
    /// (`Packages/com.basis.server/LiteNetLib/ReliableChannel.cs`), kept here as the reference
    /// implementation the Rust transport is held to.
    ///
    /// Having the C# semantics written out beside the Rust means the wire format is pinned to
    /// what the Unity client actually does, rather than to this codebase's assumptions about
    /// it. The two things that are easy to get wrong, and that this pins down, are:
    ///
    /// - ACK bits are **absolute** (`seq % 128`), not offsets from the window start. The window
    ///   start travels in the header only so the sender can bound-check the packet.
    /// - The window slides only when a packet arrives from beyond it, so a retransmit inside
    ///   the window re-sets its bit and is acknowledged again.
    #[derive(Default)]
    struct CsReliableChannel {
        /// `_remoteWindowStart`, and also `_outgoingAcks.Sequence`. Both start at 0 and move
        /// only by sliding.
        remote_window_start: u16,
        /// `_remoteSequence`: the next sequence expected in order.
        remote_sequence: u16,
        started: bool,
        bits: [u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2],
    }

    impl CsReliableChannel {
        /// C# `ReliableChannel.ProcessPacket`, non-ACK path. Returns whether the packet was
        /// newly received (a duplicate returns false but must still be acknowledged).
        fn process(&mut self, sequence: u16) -> bool {
            if sequence >= MAX_SEQUENCE {
                return false; // "Bad sequence"
            }
            // Note there is no first-packet special case: the C# initialises `_remoteWindowStart`
            // and `_remoteSequence` to 0 in the constructor and every packet, including the
            // first, goes through the window logic below. Per-channel sequences start at 0 in
            // practice, so a channel never sees a sequence far from its own window start.
            let relate = relative_sequence(sequence, self.remote_window_start);
            let relate_seq = relative_sequence(sequence, self.remote_sequence);
            if relate_seq > DEFAULT_WINDOW_SIZE as i32 {
                return false; // "Bad sequence"
            }
            if relate < 0 {
                return false; // "Too old packet doesn't ack"
            }
            if relate >= (DEFAULT_WINDOW_SIZE * 2) as i32 {
                return false; // "Some very new packet"
            }
            if relate >= DEFAULT_WINDOW_SIZE as i32 {
                // "If very new - move window"
                let new_window_start = (self.remote_window_start as i32 + relate
                    - DEFAULT_WINDOW_SIZE as i32
                    + 1) as u16
                    % MAX_SEQUENCE;
                while self.remote_window_start != new_window_start {
                    let leaving = self.remote_window_start as usize % DEFAULT_WINDOW_SIZE;
                    self.bits[leaving / 8] &= !(1 << (leaving % 8));
                    self.remote_window_start = (self.remote_window_start + 1) % MAX_SEQUENCE;
                }
            }
            if self.get_bit(sequence) {
                return false; // "ReliableInOrder duplicate"
            }
            // `_mustSendAcks = true` -- the ACK goes out for duplicates too, which is what lets
            // a lost ACK recover.
            self.started = true;
            self.set_bit(sequence);
            if sequence == self.remote_sequence {
                self.remote_sequence = (self.remote_sequence + 1) % MAX_SEQUENCE;
            }
            true
        }

        fn get_bit(&self, sequence: u16) -> bool {
            let index = sequence as usize % DEFAULT_WINDOW_SIZE;
            self.bits[index / 8] & (1 << (index % 8)) != 0
        }

        fn set_bit(&mut self, sequence: u16) {
            let index = sequence as usize % DEFAULT_WINDOW_SIZE;
            self.bits[index / 8] |= 1 << (index % 8);
        }

        /// The ACK datagram the C# would send: the window start for the header, then the whole
        /// accumulated bit set.
        fn encode_ack(&self) -> Option<(u16, Vec<u8>)> {
            if !self.started {
                return None;
            }
            Some((self.remote_window_start, self.bits.to_vec()))
        }
    }

    #[tokio::test]
    async fn idle_peer_reliable_receive_flushes_ack_without_server_reliable_send() {
        let (handle, _events) = TransportHandle::bind(loopback_addr(0)).await.unwrap();
        let server_addr = handle.local_addr().unwrap();
        let client = UdpSocket::bind(loopback_addr(0)).await.unwrap();
        let client_addr = client.local_addr().unwrap();

        let peer_id = handle
            .accept(&ConnectionRequest {
                remote_addr: client_addr,
                payload: Bytes::new(),
                connection_number: 0,
                connect_time: 0x1234_5678,
                local_peer_id: 0,
            })
            .await
            .unwrap();
        let peer = handle.peers.get(&peer_id).unwrap().clone();

        let mut buf = vec![0u8; 65_535];
        let _ = tokio::time::timeout(Duration::from_millis(200), client.recv_from(&mut buf)).await;
        assert!(
            !peer.reliable_active.load(Ordering::Acquire),
            "accepting an otherwise-idle peer must not require server reliable traffic"
        );

        let channel_id =
            DeliveryMethod::channel_id(channels::CHAT, DeliveryMethod::ReliableOrdered);
        let packet = [PacketProperty::Channeled as u8, 0, 0, channel_id, 0x5a];
        client.send_to(&packet, server_addr).await.unwrap();

        let (len, _) = tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .expect("idle peer ACK timed out")
            .unwrap();
        assert_eq!(buf[0] & 0x1f, PacketProperty::Ack as u8);
        assert_eq!(
            len,
            LITENETLIB_CHANNELED_HEADER_SIZE + (DEFAULT_WINDOW_SIZE - 1) / 8 + 2
        );
        assert_eq!(u16::from_le_bytes([buf[1], buf[2]]), 0);
        assert_eq!(buf[3], channel_id);
        assert_ne!(buf[4] & 1, 0, "sequence 0 must be acknowledged");

        handle.shutdown();
    }

    #[tokio::test]
    /// End-to-end proof that a large reliable burst actually drains.
    ///
    /// This is the failure the 15 ms single-loop design produced: a burst far larger than the
    /// 128-deep window never emptied because the window could only be refilled once per pass.
    /// A real UDP socket stands in for the client, acknowledges every reliable packet it
    /// receives, and the test asserts the server-side queue reaches zero.
    async fn large_reliable_burst_drains_through_the_window() {
        const MESSAGES: usize = 5_000;
        const PAYLOAD: usize = 64;
        const CHANNEL: u8 = channels::CHAT;

        // NOTE: bind to loopback, not `any_addr(0)`. `bind_udp_socket` enables dual stack, and
        // on Windows binding the wildcard address with an ephemeral port fails with
        // AddrNotAvailable (10049). `any_addr(0)` is still correct for real servers.
        let (handle, _events) = TransportHandle::bind(loopback_addr(0)).await.unwrap();
        let server_addr = handle.local_addr().unwrap();
        let client = UdpSocket::bind(loopback_addr(0)).await.unwrap();
        let client_addr = client.local_addr().unwrap();

        // Register the peer directly instead of hand-rolling a ConnectRequest. The reliable
        // window is driven entirely by the peer tables plus the ACKs that arrive on the read
        // path, so the handshake is not what this test is exercising.
        let peer_id = handle
            .accept(&ConnectionRequest {
                remote_addr: client_addr,
                payload: Bytes::new(),
                connection_number: 0,
                connect_time: 0x1234_5678_9abc_def0i64,
                local_peer_id: 0,
            })
            .await
            .unwrap();
        let peer = handle.peers.get(&peer_id).unwrap().clone();

        let mut buf = vec![0u8; 65_535];
        // Drain the ConnectAccept the server just sent so it cannot be mistaken for payload.
        let _ = tokio::time::timeout(Duration::from_millis(200), client.recv_from(&mut buf)).await;

        // Queue the burst, then let the client acknowledge everything it receives.
        for index in 0..MESSAGES {
            handle
                .send(
                    peer.id,
                    CHANNEL,
                    DeliveryMethod::ReliableOrdered,
                    &[(index % 251) as u8; PAYLOAD],
                )
                .await
                .unwrap();
        }
        assert_eq!(handle.queued_reliable_count(), MESSAGES);

        let mut received = 0usize;
        let mut seen: HashSet<u16> = HashSet::new();
        let mut arrived: Vec<u16> = Vec::new();
        let mut reference = CsReliableChannel::default();
        let channel_id = DeliveryMethod::channel_id(CHANNEL, DeliveryMethod::ReliableOrdered);
        let mut sent_acks = 0usize;
        let mut dropped_acks = 0usize;
        let drain_deadline = Instant::now() + Duration::from_secs(60);
        // Keep reading until the burst is fully delivered AND the window has closed. The loop
        // must not stop at "received everything": packets whose ACK was dropped are still
        // in flight server-side and only clear once the retransmit is received and ACKed.
        while Instant::now() < drain_deadline {
            if received >= MESSAGES && handle.pending_reliable_count() == 0 {
                break;
            }
            let Ok(Ok((len, _))) =
                tokio::time::timeout(Duration::from_millis(200), client.recv_from(&mut buf)).await
            else {
                continue;
            };
            arrived.clear();
            received += collect_reliable_sequences(&buf[..len], &mut seen, &mut arrived);
            for sequence in arrived.iter() {
                reference.process(*sequence);
            }
            // Acknowledge with the C# channel itself, so what the server has to understand here
            // is the real client's encoding and not a convenient approximation of it. Every
            // 17th ACK is deliberately dropped, because surviving that is the whole point --
            // ACKs are individual UDP datagrams and real ones get lost.
            if let Some((window_start, bits)) = reference.encode_ack() {
                sent_acks += 1;
                if !sent_acks.is_multiple_of(17) {
                    let mut ack = vec![PacketProperty::Ack as u8, 0, 0, channel_id];
                    ack.extend_from_slice(&bits);
                    ack[1..3].copy_from_slice(&window_start.to_le_bytes());
                    client.send_to(&ack, server_addr).await.unwrap();
                } else {
                    dropped_acks += 1;
                }
            }
        }
        assert!(
            dropped_acks > 0,
            "the test must actually drop ACKs to be meaningful"
        );
        assert_eq!(
            received, MESSAGES,
            "client never received the whole reliable burst"
        );

        // Give the final ACKs time to land, then require both the outgoing queue and the
        // in-flight window to be empty.
        let empty_deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < empty_deadline
            && (handle.queued_reliable_count() > 0 || handle.pending_reliable_count() > 0)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let queued = handle.queued_reliable_count();
        let in_flight = handle.pending_reliable_count();
        let stats = handle.stats_snapshot();
        handle.shutdown();
        assert_eq!(
            (queued, in_flight),
            (0, 0),
            "reliable state failed to drain: {queued} queued, {in_flight} in flight after receiving all {MESSAGES}"
        );
        assert!(
            stats.reliable_window_fills >= MESSAGES as u64,
            "expected at least {MESSAGES} window fills, saw {}",
            stats.reliable_window_fills
        );
    }

    /// Walk one datagram, appending every reliable sequence it carried to `arrived` (so the
    /// caller can acknowledge retransmits as well as first-time packets) and returning how
    /// many were seen for the first time. Handles both the bare and the `Merged` framing.
    fn collect_reliable_sequences(
        datagram: &[u8],
        seen: &mut HashSet<u16>,
        arrived: &mut Vec<u16>,
    ) -> usize {
        let Some(&header) = datagram.first() else {
            return 0;
        };
        let property = header & 0x1f;
        if property == PacketProperty::Merged as u8 {
            let mut pos = 1usize;
            let mut fresh = 0usize;
            while pos + 2 <= datagram.len() {
                let size = u16::from_le_bytes([datagram[pos], datagram[pos + 1]]) as usize;
                pos += 2;
                if size == 0 || pos + size > datagram.len() {
                    break;
                }
                fresh += collect_reliable_sequences(&datagram[pos..pos + size], seen, arrived);
                pos += size;
            }
            return fresh;
        }
        if property != PacketProperty::Channeled as u8 || datagram.len() < 4 {
            return 0;
        }
        let sequence = u16::from_le_bytes([datagram[1], datagram[2]]);
        arrived.push(sequence);
        usize::from(seen.insert(sequence))
    }

    fn packet_has_ack(datagram: &[u8], channel_id: u8, sequence: u16) -> bool {
        let Some(&header) = datagram.first() else {
            return false;
        };
        match header & 0x1f {
            value if value == PacketProperty::Merged as u8 => {
                let mut pos = 1usize;
                while pos + 2 <= datagram.len() {
                    let size = u16::from_le_bytes([datagram[pos], datagram[pos + 1]]) as usize;
                    pos += 2;
                    if size == 0 || pos + size > datagram.len() {
                        break;
                    }
                    if packet_has_ack(&datagram[pos..pos + size], channel_id, sequence) {
                        return true;
                    }
                    pos += size;
                }
                false
            }
            value if value == PacketProperty::Ack as u8 => {
                datagram.len() >= 5
                    && datagram[3] == channel_id
                    && ack_bit(&datagram[4..], sequence as usize % DEFAULT_WINDOW_SIZE)
            }
            _ => false,
        }
    }

    fn loopback_addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn test_peer_state(id: PeerId) -> Arc<PeerState> {
        Arc::new(PeerState {
            id,
            addr: "127.0.0.1:4296".parse().unwrap(),
            connection_number: 0,
            connect_time: 1,
            last_seen: parking_lot::Mutex::new(Instant::now()),
            last_ping_sent: parking_lot::Mutex::new(Instant::now()),
            next_ping_sequence: AtomicU16::new(0),
            next_reliable_sequence: parking_lot::Mutex::new(HashMap::new()),
            next_sequenced_sequence: parking_lot::Mutex::new(HashMap::new()),
            remote_sequenced_sequence: parking_lot::Mutex::new(HashMap::new()),
            next_fragment_id: AtomicU16::new(0),
            pending_reliable: parking_lot::Mutex::new(HashMap::new()),
            pending_total: AtomicUsize::new(0),
            pending_datagrams: parking_lot::Mutex::new(VecDeque::new()),
            reliable_send_turn: parking_lot::Mutex::new(()),
            outgoing_reliable: parking_lot::Mutex::new(HashMap::new()),
            outgoing_acks: parking_lot::Mutex::new(HashMap::new()),
            reliable_active: AtomicBool::new(false),
            confirmed_mtu: AtomicUsize::new(MAX_MERGED_PACKET_SIZE),
            mtu_probe: parking_lot::Mutex::new(MtuProbeState::new(Instant::now())),
        })
    }

    #[tokio::test]
    async fn transport_depths_track_actual_queues_and_disconnection() {
        let (handle, _events) = TransportHandle::bind(loopback_addr(0)).await.unwrap();
        // Keep this queue-assembly test independent of the background dispatch timing.
        handle.shutdown();
        assert_eq!(handle.depths_snapshot(), TransportDepthSnapshot::default());
        let peer = test_peer_state(0);
        handle.peers.insert(peer.id, peer.clone());
        enqueue_reliable_payload(&peer, channels::CHAT, DeliveryMethod::ReliableOrdered, &[1]);
        let built =
            build_outbound_packet(&peer, channels::CHAT, DeliveryMethod::ReliableOrdered, &[2]);
        let (channel_id, sequence) = built.reliable_key.unwrap();
        record_pending_reliable(&peer, channel_id, sequence, built.bytes.clone(), None);
        peer.pending_datagrams.lock().push_back(built.bytes);
        assert_eq!(
            handle.depths_snapshot(),
            TransportDepthSnapshot {
                peers: 1,
                reliable_pending: 1,
                reliable_queued: 1,
                pending_datagrams: 1,
            }
        );

        handle.disconnect(peer.id, "test").await.unwrap();
        // A retained Arc to old peer queues must not count as a live transport depth.
        assert_eq!(peer.total_pending(), 1);
        assert_eq!(handle.depths_snapshot(), TransportDepthSnapshot::default());
    }

    #[tokio::test]
    async fn non_reliable_drop_counter_excludes_retries_and_respects_statistics_toggle() {
        let (handle, _events) =
            TransportHandle::bind_with_statistics_options(loopback_addr(0), true, false)
                .await
                .unwrap();
        handle.shutdown();
        let peer = test_peer_state(0);
        handle.peers.insert(peer.id, peer.clone());
        handle.test_blocked_send_addrs.write().insert(peer.addr);

        // Retained reliable/ACK datagrams and MTU probes use this raw path.
        assert!(!handle.try_send_raw_to(&[1], peer.addr).unwrap());
        assert_eq!(handle.stats_snapshot().raw_send_would_block, 1);
        assert_eq!(handle.stats_snapshot().non_reliable_dropped_datagrams, 0);

        let single = [(channels::AVATAR, Bytes::from_static(&[1]))];
        assert_eq!(
            handle
                .try_send_many_unreliable_bytes(peer.id, &single)
                .unwrap(),
            0
        );
        assert_eq!(handle.stats_snapshot().non_reliable_dropped_datagrams, 1);

        let merged = [single[0].clone(), single[0].clone()];
        assert_eq!(
            handle
                .try_send_many_unreliable_bytes(peer.id, &merged)
                .unwrap(),
            0
        );
        assert_eq!(handle.stats_snapshot().non_reliable_dropped_datagrams, 2);

        // Exercise the flush, oversized, and trailing-merged branches (three datagrams).
        let split = [
            single[0].clone(),
            (channels::AVATAR, Bytes::from(vec![1; 1300])),
            single[0].clone(),
        ];
        assert_eq!(
            handle
                .try_send_many_unreliable_bytes(peer.id, &split)
                .unwrap(),
            0
        );
        assert_eq!(handle.stats_snapshot().non_reliable_dropped_datagrams, 5);

        let mixed = [
            (
                channels::AVATAR,
                DeliveryMethod::Unreliable,
                Bytes::from_static(&[1]),
            ),
            (
                channels::AVATAR,
                DeliveryMethod::Sequenced,
                Bytes::from_static(&[2]),
            ),
            (
                channels::CHAT,
                DeliveryMethod::ReliableOrdered,
                Bytes::from_static(&[3]),
            ),
        ];
        assert_eq!(handle.try_send_many_bytes(peer.id, &mixed).unwrap(), 0);
        assert_eq!(handle.stats_snapshot().non_reliable_dropped_datagrams, 6);
        assert_eq!(handle.queued_reliable_count(), 1);
        assert_eq!(handle.stats_snapshot().raw_send_would_block, 7);

        handle.set_statistics_enabled(false);
        handle
            .try_send_many_unreliable_bytes(peer.id, &single)
            .unwrap();
        assert_eq!(handle.stats_snapshot().non_reliable_dropped_datagrams, 0);
        assert_eq!(
            handle
                .stats
                .non_reliable_dropped_datagrams
                .load(Ordering::Relaxed),
            6
        );
        handle.set_statistics_enabled(true);
        assert_eq!(handle.stats_snapshot().non_reliable_dropped_datagrams, 0);
        handle
            .try_send_many_unreliable_bytes(peer.id, &single)
            .unwrap();
        assert_eq!(handle.stats_snapshot().non_reliable_dropped_datagrams, 1);

        handle.disconnect(peer.id, "test").await.unwrap();
        handle
            .try_send_many_unreliable_bytes(peer.id, &single)
            .unwrap();
        assert_eq!(handle.stats_snapshot().non_reliable_dropped_datagrams, 1);
    }

    #[test]
    fn mtu_probe_requires_exact_pending_echo_and_stops_after_timeout() {
        let now = Instant::now();
        let mut probe = MtuProbeState::new(now);
        assert!(probe.next_packet(now, 2).is_none());
        let first = probe.next_packet(now + MTU_PROBE_INTERVAL, 2).unwrap();
        assert_eq!(first.len(), 1164);
        assert_eq!(first[0], PacketProperty::MtuCheck as u8 | (2 << 5));
        let mut response = first.clone();
        response[0] = PacketProperty::MtuOk as u8 | (2 << 5);
        let mut forged = response.clone();
        forged[5] ^= 1;
        assert_eq!(probe.accept_response(&forged, 2), None);
        assert_eq!(probe.accept_response(&response, 1), None);
        assert_eq!(probe.accept_response(&response[..1163], 2), None);
        assert_eq!(probe.accept_response(&response, 2), Some(1164));
        assert_eq!(probe.accept_response(&response, 2), None);

        let mut failed = MtuProbeState::new(now);
        for attempt in 0..MAX_MTU_PROBE_ATTEMPTS {
            let packet = failed
                .next_packet(now + MTU_PROBE_INTERVAL * (u32::from(attempt) + 1), 0)
                .unwrap();
            assert_eq!(packet.len(), 1164);
        }
        assert!(failed
            .next_packet(
                now + MTU_PROBE_INTERVAL * (u32::from(MAX_MTU_PROBE_ATTEMPTS) + 1),
                0
            )
            .is_none());
        assert_eq!(failed.next_step, LITENETLIB_MTU_STEPS.len());
    }

    #[tokio::test]
    async fn blocked_mtu_probes_do_not_use_attempts_or_replace_sent_token() {
        let (server, _events) = TransportHandle::bind(loopback_addr(0)).await.unwrap();
        let client = UdpSocket::bind(loopback_addr(0)).await.unwrap();
        let addr = client.local_addr().unwrap();
        let now = Instant::now();
        let mut probe = MtuProbeState::new(now);
        server.test_blocked_send_addrs.write().insert(addr);
        for interval in 1..=MAX_MTU_PROBE_ATTEMPTS {
            try_send_mtu_probe(
                &mut probe,
                now + MTU_PROBE_INTERVAL * u32::from(interval),
                2,
                |packet| server.try_send_raw_to(packet, addr),
            )
            .unwrap();
            assert_eq!(probe.attempts, 0);
            assert_eq!(probe.pending, None);
            assert_eq!(probe.next_step, 1);
        }

        server.test_blocked_send_addrs.write().remove(&addr);
        server.socket.writable().await.unwrap();
        try_send_mtu_probe(
            &mut probe,
            now + MTU_PROBE_INTERVAL * u32::from(MAX_MTU_PROBE_ATTEMPTS + 1),
            2,
            |packet| server.try_send_raw_to(packet, addr),
        )
        .unwrap();
        let mut received = [0u8; 2048];
        let (len, _) = time::timeout(Duration::from_secs(1), client.recv_from(&mut received))
            .await
            .unwrap()
            .unwrap();
        let mut response = received[..len].to_vec();
        response[0] = PacketProperty::MtuOk as u8 | (2 << 5);
        assert_eq!(probe.attempts, 1);
        let sent_token = probe.pending;

        server.test_blocked_send_addrs.write().insert(addr);
        try_send_mtu_probe(
            &mut probe,
            now + MTU_PROBE_INTERVAL * u32::from(MAX_MTU_PROBE_ATTEMPTS + 2),
            2,
            |packet| server.try_send_raw_to(packet, addr),
        )
        .unwrap();
        assert_eq!(probe.pending, sent_token);
        assert_eq!(probe.attempts, 1);
        let mut forged = response.clone();
        forged[5] ^= 1;
        assert_eq!(probe.accept_response(&forged, 2), None);
        assert_eq!(probe.accept_response(&response, 1), None);
        assert_eq!(probe.accept_response(&response, 2), Some(len));
        assert_eq!(probe.accept_response(&response, 2), None);
        server.shutdown();
    }

    #[tokio::test]
    async fn negotiated_mtu_allows_larger_merged_unreliable_datagram() {
        let (server, _events) = TransportHandle::bind(loopback_addr(0)).await.unwrap();
        let client = UdpSocket::bind(loopback_addr(0)).await.unwrap();
        let client_addr = client.local_addr().unwrap();
        let peer = server
            .accept(&ConnectionRequest {
                remote_addr: client_addr,
                payload: Bytes::new(),
                connection_number: 1,
                connect_time: 1,
                local_peer_id: 0,
            })
            .await
            .unwrap();
        assert_eq!(server.peer_mtu(peer), MAX_MERGED_PACKET_SIZE);

        let mut recv = vec![0u8; 2048];
        time::timeout(Duration::from_secs(5), async {
            while server.peer_mtu(peer) < 1392 {
                let (len, from) = client.recv_from(&mut recv).await.unwrap();
                if recv[0] & 0x1f != PacketProperty::MtuCheck as u8 {
                    continue;
                }
                assert_eq!(from, server.local_addr().unwrap());
                assert!(LITENETLIB_MTU_STEPS.contains(&len));
                let mut response = recv[..len].to_vec();
                response[0] = (response[0] & 0xe0) | PacketProperty::MtuOk as u8;
                client.send_to(&response, from).await.unwrap();
            }
        })
        .await
        .unwrap();
        assert!(server.peer_mtu(peer) >= 1392);

        let packets = [
            (channels::CHAT, Bytes::from(vec![1u8; 650])),
            (channels::CHAT, Bytes::from(vec![2u8; 650])),
        ];
        assert_eq!(
            server
                .try_send_many_unreliable_bytes(peer, &packets)
                .unwrap(),
            1
        );
        let len = time::timeout(Duration::from_secs(1), async {
            loop {
                let (len, _) = client.recv_from(&mut recv).await.unwrap();
                if recv[0] & 0x1f == PacketProperty::Merged as u8 {
                    break len;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(recv[0] & 0x1f, PacketProperty::Merged as u8);
        assert_eq!(len, 1 + 2 * (2 + 2 + 650));
        assert!(len <= server.peer_mtu(peer));
        server.shutdown();
    }

    #[test]
    fn extended_reliable_counters_do_not_collect_until_enabled() {
        let peer = test_peer_state(1);
        let stats = TransportStats::new(true, false);

        record_pending_reliable(&peer, 1, 0, vec![1], Some(&stats));
        assert_eq!(stats.reliable_window_fills.load(Ordering::Relaxed), 0);

        stats.extended_enabled.store(true, Ordering::Relaxed);
        record_pending_reliable(&peer, 1, 1, vec![2], Some(&stats));
        assert_eq!(stats.reliable_window_fills.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn oversized_reliable_payload_uses_litenetlib_fragments() {
        let state = PeerState {
            id: 7,
            addr: "127.0.0.1:4296".parse().unwrap(),
            connection_number: 2,
            connect_time: 123,
            last_seen: parking_lot::Mutex::new(Instant::now()),
            last_ping_sent: parking_lot::Mutex::new(Instant::now()),
            next_ping_sequence: AtomicU16::new(0),
            next_reliable_sequence: parking_lot::Mutex::new(HashMap::new()),
            next_sequenced_sequence: parking_lot::Mutex::new(HashMap::new()),
            remote_sequenced_sequence: parking_lot::Mutex::new(HashMap::new()),
            next_fragment_id: AtomicU16::new(0),
            pending_reliable: parking_lot::Mutex::new(HashMap::new()),
            pending_total: AtomicUsize::new(0),
            pending_datagrams: parking_lot::Mutex::new(VecDeque::new()),
            reliable_send_turn: parking_lot::Mutex::new(()),
            outgoing_reliable: parking_lot::Mutex::new(HashMap::new()),
            outgoing_acks: parking_lot::Mutex::new(HashMap::new()),
            reliable_active: AtomicBool::new(false),
            confirmed_mtu: AtomicUsize::new(MAX_MERGED_PACKET_SIZE),
            mtu_probe: parking_lot::Mutex::new(MtuProbeState::new(Instant::now())),
        };
        let payload = (0..RELIABLE_FRAGMENT_PAYLOAD_SIZE * 2 + 1)
            .map(|index| (index & 0xff) as u8)
            .collect::<Vec<_>>();

        enqueue_reliable_payload(
            &state,
            channels::CREATE_REMOTE_PLAYERS_FOR_NEW_PEER,
            DeliveryMethod::ReliableOrdered,
            &payload,
        );

        let channel_id = DeliveryMethod::channel_id(
            channels::CREATE_REMOTE_PLAYERS_FOR_NEW_PEER,
            DeliveryMethod::ReliableOrdered,
        );
        let queued = state
            .outgoing_reliable
            .lock()
            .get_mut(&channel_id)
            .unwrap()
            .drain(..)
            .collect::<Vec<_>>();
        assert_eq!(queued.len(), 3);

        let mut reassembled = Vec::new();
        for (part, outgoing) in queued.into_iter().enumerate() {
            let packet = build_queued_reliable_packet(&state, channel_id, outgoing);
            assert!(packet.bytes.len() <= LITENETLIB_INITIAL_MTU);
            assert_eq!(
                packet.bytes[0],
                PacketProperty::Channeled as u8 | (2 << 5) | 0x80
            );
            assert_eq!(
                u16::from_le_bytes([packet.bytes[1], packet.bytes[2]]),
                part as u16
            );
            assert_eq!(packet.bytes[3], channel_id);
            assert_eq!(u16::from_le_bytes([packet.bytes[4], packet.bytes[5]]), 1);
            assert_eq!(
                u16::from_le_bytes([packet.bytes[6], packet.bytes[7]]),
                part as u16
            );
            assert_eq!(u16::from_le_bytes([packet.bytes[8], packet.bytes[9]]), 3);
            reassembled.extend_from_slice(&packet.bytes[LITENETLIB_FRAGMENTED_HEADER_SIZE..]);
        }

        assert_eq!(reassembled, payload);
    }

    #[test]
    fn server_info_payload_accepts_raw_and_litenetlib_unconnected() {
        let mut payload = vec![0u8; SERVER_INFO_MIN_REQUEST_BYTES];
        payload[0..4].copy_from_slice(&SERVER_INFO_QUERY_MAGIC.to_le_bytes());
        payload[4..6].copy_from_slice(&SERVER_INFO_PROTOCOL_VERSION.to_le_bytes());
        payload[6..8].copy_from_slice(&0xCAFEu16.to_le_bytes());

        assert_eq!(server_info_payload(&payload).unwrap()[6..8], [0xFE, 0xCA]);

        let mut wrapped = Vec::with_capacity(payload.len() + 1);
        wrapped.push(PacketProperty::UnconnectedMessage as u8);
        wrapped.extend_from_slice(&payload);
        assert_eq!(server_info_payload(&wrapped).unwrap()[6..8], [0xFE, 0xCA]);
    }

    #[tokio::test]
    async fn compact_merged_unreliable_entry_decodes_to_message_event() {
        // Loopback rather than `any_addr(0)`: dual-stack wildcard binds with an ephemeral port
        // fail on Windows with AddrNotAvailable (10049).
        let (handle, _events) = TransportHandle::bind(loopback_addr(0)).await.unwrap();
        let remote = UdpSocket::bind(loopback_addr(0)).await.unwrap();
        let remote_addr = remote.local_addr().unwrap();
        let request = ConnectionRequest {
            remote_addr,
            payload: Bytes::new(),
            connection_number: 0,
            connect_time: 123,
            local_peer_id: 0,
        };
        let peer = handle.accept(&request).await.unwrap();
        let (tx, mut rx) = mpsc::channel(4);

        let packet = [
            PacketProperty::CompactMerged as u8,
            channels::CHAT,
            3,
            1,
            2,
            3,
        ];
        process_compact_merged_packet(&handle, &tx, remote_addr, 0, &packet)
            .await
            .unwrap();

        match rx.recv().await.unwrap() {
            ServerEvent::Message {
                peer: event_peer,
                channel,
                delivery,
                payload,
            } => {
                assert_eq!(event_peer, peer);
                assert_eq!(channel, channels::CHAT);
                assert_eq!(delivery, DeliveryMethod::Unreliable);
                assert_eq!(payload.as_ref(), &[1, 2, 3]);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        handle.shutdown();
    }

    #[tokio::test]
    async fn peer_ids_reuse_only_after_cleanup_release() {
        let (handle, _events) = TransportHandle::bind(any_addr(0)).await.unwrap();
        let first = handle.allocate_peer_id();
        let second = handle.allocate_peer_id();
        assert_eq!(first, 0);
        assert_eq!(second, 1);

        handle.retire_peer_id(first);
        assert_eq!(handle.allocate_peer_id(), 2);

        handle.recycle_peer_id(first);
        assert_eq!(handle.allocate_peer_id(), first);
    }
}
