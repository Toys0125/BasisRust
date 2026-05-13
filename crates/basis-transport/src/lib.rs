use basis_protocol::{
    channels,
    io::NetWriter,
    server_info::{
        ServerInfoResponse, SERVER_INFO_MIN_REQUEST_BYTES, SERVER_INFO_PROTOCOL_VERSION,
        SERVER_INFO_QUERY_MAGIC,
    },
    version::LITENETLIB_PROTOCOL_ID,
};
use bytes::Bytes;
use dashmap::DashMap;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicU16, Ordering},
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

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("event channel closed")]
    EventChannelClosed,
}

pub type Result<T> = std::result::Result<T, TransportError>;

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
        payload: Bytes,
    },
}

#[derive(Debug)]
struct PeerState {
    id: PeerId,
    addr: SocketAddr,
    connection_number: u8,
    connect_time: i64,
    last_seen: parking_lot::Mutex<Instant>,
    next_reliable_sequence: parking_lot::Mutex<HashMap<u8, u16>>,
    next_sequenced_sequence: parking_lot::Mutex<HashMap<u8, u16>>,
    pending_reliable: parking_lot::Mutex<HashMap<PendingReliableKey, PendingReliable>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PendingReliableKey {
    channel_id: u8,
    sequence: u16,
}

#[derive(Debug, Clone)]
struct PendingReliable {
    bytes: Vec<u8>,
    last_sent: Instant,
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
}

impl TransportHandle {
    pub async fn bind(addr: SocketAddr) -> Result<(Self, mpsc::Receiver<ServerEvent>)> {
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
        };
        tokio::spawn(read_loop(handle.clone(), tx.clone()));
        tokio::spawn(timeout_loop(handle.clone(), tx));
        tokio::spawn(reliable_resend_loop(handle.clone()));
        Ok((handle, rx))
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn connected_peers_count(&self) -> usize {
        self.peers.len()
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
            next_reliable_sequence: parking_lot::Mutex::new(HashMap::new()),
            next_sequenced_sequence: parking_lot::Mutex::new(HashMap::new()),
            pending_reliable: parking_lot::Mutex::new(HashMap::new()),
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
        self.pending_requests.remove(&request.remote_addr);
        let mut payload = NetWriter::new();
        payload.put_string(reason);
        let mut writer = NetWriter::with_capacity(payload.len() + 9);
        writer.put_u8(PacketProperty::Disconnect as u8 | (request.connection_number << 5));
        writer.put_i64(request.connect_time);
        writer.put_bytes(payload.as_slice());
        self.socket
            .send_to(writer.as_slice(), request.remote_addr)
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
        let built = build_outbound_packet(&state, channel, delivery, payload);
        record_pending_reliable(&state, &built);
        self.socket.send_to(&built.bytes, state.addr).await?;
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
            let built = build_outbound_packet(&state, *channel, *delivery, payload);
            record_pending_reliable(&state, &built);
            outbound.push(built.bytes);
        }

        for packet in build_merged_datagrams(state.connection_number, outbound) {
            self.socket.send_to(&packet, state.addr).await?;
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
            self.socket.send_to(writer.as_slice(), state.addr).await?;
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
        self.socket
            .send_to(&response.serialize(), remote_addr)
            .await?;
        Ok(())
    }
}

fn record_pending_reliable(state: &PeerState, built: &BuiltPacket) {
    if let Some((channel_id, sequence)) = built.reliable_key {
        let mut pending = state.pending_reliable.lock();
        if pending.len() >= MAX_PENDING_RELIABLE_PER_PEER {
            let oldest = pending
                .iter()
                .min_by_key(|(_, item)| item.last_sent)
                .map(|(key, _)| *key);
            if let Some(oldest) = oldest {
                pending.remove(&oldest);
            }
        }
        pending.insert(
            PendingReliableKey {
                channel_id,
                sequence,
            },
            PendingReliable {
                bytes: built.bytes.clone(),
                last_sent: Instant::now(),
            },
        );
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

async fn read_loop(handle: TransportHandle, tx: mpsc::Sender<ServerEvent>) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match handle.socket.recv_from(&mut buf).await {
            Ok((len, remote_addr)) => {
                if let Err(err) = process_packet(&handle, &tx, remote_addr, &buf[..len]).await {
                    warn!("transport packet processing failed: {err}");
                }
            }
            Err(err) => {
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
    if bytes.len() >= 6 {
        let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let protocol = u16::from_le_bytes([bytes[4], bytes[5]]);
        if magic == SERVER_INFO_QUERY_MAGIC
            && protocol == SERVER_INFO_PROTOCOL_VERSION
            && bytes.len() >= SERVER_INFO_MIN_REQUEST_BYTES
        {
            enqueue_event(
                tx,
                ServerEvent::UnconnectedRequest {
                    remote_addr,
                    payload: Bytes::copy_from_slice(bytes),
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
                    *peer.last_seen.lock() = Instant::now();
                }
            }
        }
        PacketProperty::Ack => {
            if let Some(peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                if let Some(peer) = handle.peers.get(&peer_id) {
                    *peer.last_seen.lock() = Instant::now();
                    process_ack(&peer, bytes);
                }
            }
        }
        PacketProperty::Channeled | PacketProperty::Unreliable => {
            if let Some(peer_id) = handle.by_addr.get(&remote_addr).map(|p| *p) {
                if let Some(peer) = handle.peers.get(&peer_id) {
                    *peer.last_seen.lock() = Instant::now();
                }
                if let Some((channel, delivery, payload)) = parse_message_packet(property, bytes) {
                    if matches!(
                        delivery,
                        DeliveryMethod::ReliableOrdered | DeliveryMethod::ReliableUnordered
                    ) {
                        let sequence = u16::from_le_bytes([bytes[1], bytes[2]]);
                        let channel_id = bytes[3];
                        send_ack(handle, remote_addr, connection_number, channel_id, sequence)
                            .await?;
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
    handle
        .socket
        .send_to(&[property as u8], remote_addr)
        .await?;
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
    handle
        .socket
        .send_to(writer.as_slice(), remote_addr)
        .await?;
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
    reliable_key: Option<(u8, u16)>,
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
                reliable_key: None,
            }
        }
        DeliveryMethod::Sequenced => {
            let channel_id = DeliveryMethod::channel_id(channel, delivery);
            let sequence = next_channel_sequence(&state.next_sequenced_sequence, channel_id);
            let mut writer = NetWriter::with_capacity(payload.len() + 4);
            writer.put_u8(PacketProperty::Channeled as u8 | (state.connection_number << 5));
            writer.put_u16(sequence);
            writer.put_u8(channel_id);
            writer.put_bytes(payload);
            BuiltPacket {
                bytes: writer.into_vec(),
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

fn process_ack(peer: &PeerState, bytes: &[u8]) {
    if bytes.len() < 4 {
        return;
    }
    let ack_window_start = u16::from_le_bytes([bytes[1], bytes[2]]);
    let channel_id = bytes[3];
    let ack_bits = &bytes[4..];
    let mut pending = peer.pending_reliable.lock();
    pending.retain(|key, _| {
        if key.channel_id != channel_id {
            return true;
        }
        let rel = relative_sequence(key.sequence, ack_window_start);
        if rel < 0 || rel as usize >= DEFAULT_WINDOW_SIZE {
            return true;
        }
        let pos = key.sequence as usize % DEFAULT_WINDOW_SIZE;
        let acked = ack_bits
            .get(pos / 8)
            .map(|b| (b & (1 << (pos % 8))) != 0)
            .unwrap_or(false);
        !acked
    });
}

async fn send_ack(
    handle: &TransportHandle,
    remote_addr: SocketAddr,
    connection_number: u8,
    channel_id: u8,
    sequence: u16,
) -> Result<()> {
    let mut packet = vec![0u8; 4 + ((DEFAULT_WINDOW_SIZE - 1) / 8 + 2)];
    packet[0] = PacketProperty::Ack as u8 | (connection_number << 5);
    packet[1..3].copy_from_slice(&sequence.to_le_bytes());
    packet[3] = channel_id;
    let bit_index = sequence as usize % DEFAULT_WINDOW_SIZE;
    packet[4 + bit_index / 8] |= 1 << (bit_index % 8);
    handle.socket.send_to(&packet, remote_addr).await?;
    Ok(())
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
    handle
        .socket
        .send_to(writer.as_slice(), remote_addr)
        .await?;
    Ok(())
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
    handle.socket.send_to(&packet, remote_addr).await?;
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
    loop {
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

async fn reliable_resend_loop(handle: TransportHandle) {
    let mut tick = time::interval(Duration::from_millis(50));
    loop {
        tick.tick().await;
        let now = Instant::now();
        let mut sends = Vec::new();
        for peer in handle.peers.iter() {
            let addr = peer.addr;
            let connection_number = peer.connection_number;
            let mut pending = peer.pending_reliable.lock();
            for item in pending.values_mut() {
                if now.duration_since(item.last_sent) >= Duration::from_millis(150) {
                    item.last_sent = now;
                    sends.push((addr, connection_number, item.bytes.clone()));
                }
            }
        }
        let mut by_peer: HashMap<(SocketAddr, u8), Vec<Vec<u8>>> = HashMap::new();
        for (addr, connection_number, bytes) in sends {
            by_peer
                .entry((addr, connection_number))
                .or_default()
                .push(bytes);
        }
        for ((addr, connection_number), packets) in by_peer {
            for bytes in build_merged_datagrams(connection_number, packets) {
                let _ = handle.socket.send_to(&bytes, addr).await;
            }
        }
    }
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
