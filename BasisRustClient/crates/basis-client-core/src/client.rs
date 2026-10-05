use crate::observer_session::ObserverSession;
use crate::packet_diagnostics::{DropReason, PacketDiagnostics};
pub(crate) const OBSERVER_CANDIDATE_COUNT: usize = 3;
use crate::avatar::{
    parse_server_avatar_metadata, shared_receive_handoff_ready, PoseState, ServerAvatarMetadata,
};
use crate::config::Config;
use crate::diagnostics::ClientAvatarDiagnostics;
use crate::identity::Identity;
#[cfg(windows)]
use crate::net::windows_mtu_probe_reply;
use crate::net::{
    any_local_addr, bind_udp_socket, configure_load_sink_socket, resolve_addr, socket_address_bytes,
};
use crate::transport::{
    parse_packet, read_bytes_message, ReliableReceiveState, ReliableSend, ACK_FLUSH_INTERVAL,
    PING_INTERVAL_TICKS, RESEND_INTERVAL_TICKS,
};
use crate::wire::build_connection_payload;
use anyhow::{anyhow, Result};
use basis_protocol::channels;
use basis_protocol::io::NetWriter;
use basis_protocol::messages::ReadyMessage;
use basis_protocol::version::LITENETLIB_PROTOCOL_ID;
use basis_transport::{
    dotnet_utc_ticks, relative_sequence, DeliveryMethod, PacketProperty, DEFAULT_WINDOW_SIZE,
    LITENETLIB_CHANNELED_HEADER_SIZE, LITENETLIB_FRAGMENTED_HEADER_SIZE, LITENETLIB_INITIAL_MTU,
    MAX_SEQUENCE, RELIABLE_FRAGMENT_PAYLOAD_SIZE,
};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU8, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime};
use tokio::net::UdpSocket;
use tokio::sync::{Mutex, Notify};
use tokio::time;
use tracing::{debug, error, info, trace, warn};

#[derive(Debug)]
pub(crate) struct BasisClient {
    pub(crate) index: usize,
    pub(crate) socket: Arc<UdpSocket>,
    pub(crate) server_addr: SocketAddr,
    pub(crate) connect_time: i64,
    pub(crate) connection_number: u8,
    pub(crate) local_peer_id: i32,
    pub(crate) remote_peer_id: Mutex<Option<i32>>,
    pub(crate) connected: AtomicBool,
    pub(crate) in_use: AtomicBool,
    pub(crate) intentional_reconnect: AtomicBool,
    pub(crate) movement_sequence: AtomicU8,
    pub(crate) voice_sequence: AtomicU8,
    pub(crate) reliable_sequences: [AtomicU16; 256],
    pub(crate) fragment_id: AtomicU16,
    pub(crate) ping_sequence: AtomicU16,
    pub(crate) pending_reliable: Mutex<VecDeque<ReliableSend>>,
    pub(crate) pending_reliable_active: AtomicBool,
    /// Some channel's ACK window has changed and an ACK is owed. Lets the flush tick skip the
    /// receive-state mutex entirely when there is nothing to send, which is the common case.
    pub(crate) ack_pending: AtomicBool,
    pub(crate) shared_receive: AtomicBool,
    pub(crate) shared_receive_eligible: AtomicBool,
    pub(crate) receive_shutdown: Notify,
    pub(crate) received_reliable: StdMutex<ReliableReceiveState>,
    pub(crate) server_avatar_metadata: StdMutex<Option<ServerAvatarMetadata>>,
    pub(crate) force_avatar_keyframe: AtomicBool,
    pub(crate) pose: Mutex<PoseState>,
    pub(crate) avatar_observer: Option<Arc<ObserverSession>>,
    pub(crate) packet_diagnostics: PacketDiagnostics,
    pub(crate) avatar_diagnostics: Option<Arc<ClientAvatarDiagnostics>>,
    pub(crate) voice_diagnostics: Option<crate::voice_diagnostics::VoiceDiagnostics>,
    pub(crate) identity: Identity,
}

impl BasisClient {
    pub(crate) async fn start(
        index: usize,
        config: &Config,
        mut ready: ReadyMessage,
        spawn_base: [f32; 3],
        shared_maintenance_enabled: bool,
    ) -> Result<Arc<Self>> {
        let server_addr = resolve_addr(&config.ip, config.port)?;
        let socket = bind_udp_socket(any_local_addr(server_addr))?;
        socket.connect(server_addr).await?;
        let socket = Arc::new(socket);
        let connect_time = dotnet_utc_ticks();
        let local_peer_id = index as i32;
        let identity = Identity::random();
        ready.player_meta_data_message.player_uuid = identity.did_key();
        let client = Arc::new(Self {
            index,
            socket,
            server_addr,
            connect_time,
            connection_number: 0,
            local_peer_id,
            remote_peer_id: Mutex::new(None),
            connected: AtomicBool::new(false),
            in_use: AtomicBool::new(false),
            intentional_reconnect: AtomicBool::new(false),
            movement_sequence: AtomicU8::new(0),
            voice_sequence: AtomicU8::new(0),
            reliable_sequences: std::array::from_fn(|_| AtomicU16::new(0)),
            fragment_id: AtomicU16::new(0),
            ping_sequence: AtomicU16::new(0),
            pending_reliable: Mutex::new(VecDeque::new()),
            pending_reliable_active: AtomicBool::new(false),
            ack_pending: AtomicBool::new(false),
            shared_receive: AtomicBool::new(false),
            shared_receive_eligible: AtomicBool::new(false),
            receive_shutdown: Notify::new(),
            received_reliable: StdMutex::new(ReliableReceiveState::default()),
            server_avatar_metadata: StdMutex::new(None),
            force_avatar_keyframe: AtomicBool::new(false),
            pose: Mutex::new(PoseState::new_at(spawn_base)),
            avatar_observer: (index < OBSERVER_CANDIDATE_COUNT)
                .then(|| config.observer_session.clone())
                .flatten(),
            packet_diagnostics: PacketDiagnostics::default(),
            avatar_diagnostics: ClientAvatarDiagnostics::enabled_from_env()
                .then(|| Arc::new(ClientAvatarDiagnostics::default())),
            voice_diagnostics: crate::voice_diagnostics::VoiceDiagnostics::enabled()
                .then(|| crate::voice_diagnostics::VoiceDiagnostics::new(index)),
            identity,
        });

        client
            .start_client(config, &ready, shared_maintenance_enabled)
            .await?;
        Ok(client)
    }

    pub(crate) async fn start_client(
        self: &Arc<Self>,
        config: &Config,
        ready: &ReadyMessage,
        shared_maintenance_enabled: bool,
    ) -> Result<()> {
        let payload = build_connection_payload(config, ready)?;
        if self.in_use.swap(true, Ordering::SeqCst) {
            error!("Call Shutdown First!");
            return Err(anyhow!("Call Shutdown First!"));
        }
        let request = self.make_connect_request(&payload);
        self.send_connected(&request).await?;
        debug!(
            "client {} sent connect request to {}",
            self.index, self.server_addr
        );

        // Every freshly-created client starts with a dedicated receive task so reconnect/auth
        // always has a path for the DID challenge. On Linux, the shared receiver takes over only
        // after the client has observed post-auth server traffic and marks itself eligible.
        let client = self.clone();
        tokio::spawn(async move {
            let index = client.index;
            if let Err(err) = client.clone().receive_loop().await {
                debug!("client {index} receive loop ended: {err}");
                client.deactivate();
            }
        });

        if !shared_maintenance_enabled {
            let client = self.clone();
            tokio::spawn(async move {
                client.maintenance_loop().await;
            });
        }

        Ok(())
    }

    pub(crate) async fn send_connected(&self, bytes: &[u8]) -> Result<()> {
        self.socket.send(bytes).await?;
        Ok(())
    }

    pub(crate) fn make_connect_request(&self, payload: &[u8]) -> Vec<u8> {
        let addr_bytes = socket_address_bytes(self.server_addr);
        let mut writer = NetWriter::with_capacity(18 + addr_bytes.len() + payload.len());
        writer.put_u8(PacketProperty::ConnectRequest as u8 | (self.connection_number << 5));
        writer.put_i32(LITENETLIB_PROTOCOL_ID);
        writer.put_i64(self.connect_time);
        writer.put_i32(self.local_peer_id);
        writer.put_u8(addr_bytes.len() as u8);
        writer.put_bytes(&addr_bytes);
        writer.put_bytes(payload);
        writer.into_vec()
    }

    pub(crate) fn stop_receive_loop(&self) {
        // A UDP recv future otherwise keeps this Arc (and its socket FD) alive indefinitely when
        // a connection attempt is replaced without receiving a final server packet.
        self.receive_shutdown.notify_one();
    }

    pub(crate) fn deactivate(&self) {
        self.in_use.store(false, Ordering::SeqCst);
        self.connected.store(false, Ordering::SeqCst);
        if let Some(session) = &self.avatar_observer {
            session.release((self.index, self.connect_time));
        }
        let drops = self.packet_diagnostics.snapshot();
        if drops.iter().any(|count| *count != 0) {
            debug!(client = self.index, ?drops, "packet drop counts: empty, unknown_property, short_header, invalid_ack_size, invalid_merged");
        }
        self.stop_receive_loop();
    }

    pub(crate) async fn disconnect(&self) {
        self.deactivate();
        info!("client {} called disconnect", self.index);
        let mut packet = NetWriter::with_capacity(9);
        packet.put_u8(PacketProperty::Disconnect as u8 | (self.connection_number << 5));
        packet.put_i64(self.connect_time);
        let _ = self.send_connected(&packet.into_vec()).await;
        info!("client {} worker thread stopped", self.index);
    }

    pub(crate) async fn send_unreliable(&self, channel: u8, payload: &[u8]) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut packet = Vec::with_capacity(2 + payload.len());
        packet.push(PacketProperty::Unreliable as u8 | (self.connection_number << 5));
        packet.push(channel);
        packet.extend_from_slice(payload);
        trace!(
            "client {} sending unreliable channel={} bytes={} header={:02x} {:02x}",
            self.index,
            channel,
            payload.len(),
            packet[0],
            packet[1]
        );
        self.send_connected(&packet).await?;
        Ok(())
    }

    pub(crate) async fn current_position(&self) -> [f32; 3] {
        self.pose.lock().await.position()
    }

    pub(crate) async fn remote_peer_id(&self) -> Option<u16> {
        let id = *self.remote_peer_id.lock().await;
        id.and_then(|id| {
            if (0..=u16::MAX as i32).contains(&id) {
                Some(id as u16)
            } else {
                None
            }
        })
    }

    pub(crate) async fn mark_reliable_sent(&self, sent: &ReliableSend, sent_at: SystemTime) {
        let mut pending = self.pending_reliable.lock().await;
        if let Some(item) = pending.iter_mut().find(|item| {
            item.channel_id == sent.channel_id
                && item.sequence == sent.sequence
                && item.bytes == sent.bytes
        }) {
            item.last_sent = Some(sent_at);
        }
    }

    /// Promote unsent queue records into each channel's 128-sequence window. Sequence numbers
    /// are assigned only here, so a large queued payload cannot wrap and alias before it is
    /// admitted to the protocol window. This function is synchronous and is called under the
    /// pending queue mutex; callers perform socket writes only after it returns.
    pub(crate) fn promote_reliable_window(
        &self,
        pending: &mut VecDeque<ReliableSend>,
    ) -> Vec<ReliableSend> {
        let mut queued = [false; 256];
        let mut oldest = [None; 256];
        for item in pending.iter() {
            let index = item.channel_id as usize;
            if let Some(sequence) = item.sequence {
                oldest[index].get_or_insert(sequence);
            } else {
                queued[index] = true;
            }
        }

        let mut next = [0u16; 256];
        let mut capacity = [0usize; 256];
        for channel in 0..256 {
            if !queued[channel] {
                continue;
            }
            let sequence = self.reliable_sequences[channel].load(Ordering::Relaxed) % MAX_SEQUENCE;
            next[channel] = sequence;
            let start = oldest[channel].unwrap_or(sequence);
            let span = relative_sequence(sequence, start);
            if (0..=DEFAULT_WINDOW_SIZE as i32).contains(&span) {
                capacity[channel] = DEFAULT_WINDOW_SIZE - span as usize;
            }
        }

        let now = SystemTime::now();
        let mut promoted = Vec::new();
        let mut changed = [false; 256];
        for item in pending.iter_mut() {
            if item.sequence.is_some() {
                continue;
            }
            let index = item.channel_id as usize;
            if capacity[index] == 0 {
                continue;
            }
            let sequence = next[index];
            next[index] = sequence.wrapping_add(1) % MAX_SEQUENCE;
            capacity[index] -= 1;
            changed[index] = true;
            item.sequence = Some(sequence);
            item.bytes[1..3].copy_from_slice(&sequence.to_le_bytes());
            // Reserve the record before releasing the lock, preventing another maintenance
            // pass from selecting it while this batch awaits socket writes.
            item.last_sent = Some(now);
            promoted.push(item.clone());
        }
        for channel in 0..256 {
            if changed[channel] {
                self.reliable_sequences[channel].store(next[channel], Ordering::Relaxed);
            }
        }
        promoted
    }

    pub(crate) async fn pump_reliable_window(&self) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }
        let promoted = {
            let mut pending = self.pending_reliable.lock().await;
            let promoted = self.promote_reliable_window(&mut pending);
            self.pending_reliable_active
                .store(!pending.is_empty(), Ordering::Relaxed);
            promoted
        };
        for (index, packet) in promoted.iter().enumerate() {
            if let Err(err) = self.send_connected(&packet.bytes).await {
                let mut pending = self.pending_reliable.lock().await;
                for unsent in &promoted[index..] {
                    if let Some(current) = pending.iter_mut().find(|current| {
                        current.channel_id == unsent.channel_id
                            && current.sequence == unsent.sequence
                            && current.bytes == unsent.bytes
                    }) {
                        current.last_sent = None;
                    }
                }
                return Err(err);
            }
            self.mark_reliable_sent(packet, SystemTime::now()).await;
        }
        Ok(())
    }

    pub(crate) async fn send_reliable_ordered(&self, channel: u8, payload: &[u8]) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }

        let channel_id = DeliveryMethod::channel_id(channel, DeliveryMethod::ReliableOrdered);
        let mut packets = Vec::new();
        if payload.len() + LITENETLIB_CHANNELED_HEADER_SIZE <= LITENETLIB_INITIAL_MTU {
            let mut packet = Vec::with_capacity(LITENETLIB_CHANNELED_HEADER_SIZE + payload.len());
            packet.push(PacketProperty::Channeled as u8 | (self.connection_number << 5));
            packet.extend_from_slice(&0u16.to_le_bytes());
            packet.push(channel_id);
            packet.extend_from_slice(payload);
            packets.push(ReliableSend {
                channel_id,
                sequence: None,
                bytes: packet,
                last_sent: None,
            });
        } else {
            let total_fragments = payload.len().div_ceil(RELIABLE_FRAGMENT_PAYLOAD_SIZE);
            if total_fragments > u16::MAX as usize {
                return Err(anyhow!(
                    "reliable payload requires {} fragments, exceeding LiteNetLib limit",
                    total_fragments
                ));
            }

            let fragment_id = self
                .fragment_id
                .fetch_add(1, Ordering::SeqCst)
                .wrapping_add(1);
            packets.reserve(total_fragments);
            for (part, chunk) in payload.chunks(RELIABLE_FRAGMENT_PAYLOAD_SIZE).enumerate() {
                let mut packet =
                    Vec::with_capacity(LITENETLIB_FRAGMENTED_HEADER_SIZE + chunk.len());
                packet.push(PacketProperty::Channeled as u8 | (self.connection_number << 5) | 0x80);
                packet.extend_from_slice(&0u16.to_le_bytes());
                packet.push(channel_id);
                packet.extend_from_slice(&fragment_id.to_le_bytes());
                packet.extend_from_slice(&(part as u16).to_le_bytes());
                packet.extend_from_slice(&(total_fragments as u16).to_le_bytes());
                packet.extend_from_slice(chunk);
                packets.push(ReliableSend {
                    channel_id,
                    sequence: None,
                    bytes: packet,
                    last_sent: None,
                });
            }
        }

        // Record the complete message before its first datagram can elicit an ACK. If a send
        // fails, every packet remains queued (unsent entries have last_sent == None) for retry.
        {
            let mut pending = self.pending_reliable.lock().await;
            pending.extend(packets.iter().cloned());
            self.pending_reliable_active.store(true, Ordering::Relaxed);
        }
        self.pump_reliable_window().await
    }

    pub(crate) async fn receive_loop(self: Arc<Self>) -> Result<()> {
        let mut buffer = vec![0u8; 65535];
        loop {
            if !self.in_use.load(Ordering::Relaxed) || self.shared_receive.load(Ordering::Relaxed) {
                break;
            }
            let len = tokio::select! {
                result = self.socket.recv(&mut buffer) => result?,
                _ = self.receive_shutdown.notified() => break,
            };
            self.handle_packet(&buffer[..len]).await?;
        }
        Ok(())
    }

    pub(crate) fn record_unparsed_packet(&self, bytes: &[u8]) {
        let reason = match bytes.first().copied() {
            None => DropReason::Empty,
            Some(first) if PacketProperty::from_byte(first).is_none() => {
                DropReason::UnknownProperty
            }
            _ => DropReason::ShortHeader,
        };
        self.packet_diagnostics.record(self.index, reason);
    }

    pub(crate) async fn handle_packet(&self, bytes: &[u8]) -> Result<()> {
        let packet = match parse_packet(bytes) {
            Some(packet) => packet,
            None => {
                self.record_unparsed_packet(bytes);
                return Ok(());
            }
        };
        trace!("client {} received {:?}", self.index, packet.property);
        if self.connected.load(Ordering::Acquire) && self.in_use.load(Ordering::Acquire) {
            if let Some(session) = &self.avatar_observer {
                session.note_packet((self.index, self.connect_time), std::time::Instant::now());
            }
        }
        match packet.property {
            PacketProperty::ConnectAccept
                if bytes.len() == 15
                    && i64::from_le_bytes(bytes[1..9].try_into().unwrap()) == self.connect_time =>
            {
                let remote_peer = i32::from_le_bytes(bytes[11..15].try_into().unwrap());
                *self.remote_peer_id.lock().await = Some(remote_peer);
                if self.index != 0 && self.avatar_observer.is_none() {
                    if let Err(err) = configure_load_sink_socket(&self.socket) {
                        warn!(
                            "client {} failed to enable load-sink receive filter: {err}",
                            self.index
                        );
                    }
                }
                self.connected.store(true, Ordering::SeqCst);
                self.refresh_shared_receive_eligibility();
                info!(
                    "client {} connected as remote peer {}",
                    self.index, remote_peer
                );
            }
            PacketProperty::ConnectAccept => {}
            PacketProperty::Disconnect
            | PacketProperty::PeerNotFound
            | PacketProperty::InvalidProtocol => {
                let reason =
                    if packet.property == PacketProperty::Disconnect && packet.payload.len() >= 2 {
                        let length =
                            u16::from_le_bytes([packet.payload[0], packet.payload[1]]) as usize;
                        if packet.payload.len() >= 2 + length {
                            std::str::from_utf8(&packet.payload[2..2 + length]).ok()
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                warn!(
                    "client {} disconnected/rejected by server: {:?} reason={:?}",
                    self.index, packet.property, reason
                );
                self.deactivate();
            }
            PacketProperty::Ping => {
                if let Some(sequence) = packet.sequence {
                    self.send_pong(sequence).await?;
                }
            }
            PacketProperty::Pong => {}
            #[cfg(windows)]
            PacketProperty::MtuCheck => {
                if let Some(reply) = windows_mtu_probe_reply(bytes, self.connection_number) {
                    self.socket.send(&reply).await?;
                }
            }
            PacketProperty::Ack => {
                if let Some(sequence) = packet.sequence {
                    if let Some(channel_id) = packet.channel_id {
                        self.process_ack(channel_id, sequence, packet.payload)
                            .await?;
                    }
                }
            }
            PacketProperty::Channeled => {
                if let Some(channel_id) = packet.channel_id {
                    self.handle_channeled(channel_id, packet.sequence.unwrap_or(0), packet.payload)
                        .await?;
                }
            }
            PacketProperty::Unreliable => {
                if let (Some(channel), Some(payload)) = (bytes.get(1).copied(), bytes.get(2..)) {
                    if let Some(diagnostics) = &self.voice_diagnostics {
                        diagnostics.receive(channel, payload);
                    }
                    self.observe_avatar_channel(channel, payload).await;
                }
            }
            PacketProperty::Merged => {
                let mut pos = 1;
                while pos + 2 <= bytes.len() {
                    let size = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
                    pos += 2;
                    if size == 0 || pos + size > bytes.len() {
                        self.packet_diagnostics
                            .record(self.index, DropReason::InvalidMerged);
                        return Ok(());
                    }
                    Box::pin(self.handle_packet(&bytes[pos..pos + size])).await?;
                    pos += size;
                }
                if pos != bytes.len() {
                    self.packet_diagnostics
                        .record(self.index, DropReason::InvalidMerged);
                }
            }
            PacketProperty::CompactMerged => {
                const LONG_LENGTH_FLAG: u8 = 0x80;
                const RAW_PACKET_FLAG: u8 = 0x40;
                const CHANNEL_MASK: u8 = 0x3f;
                let connection_number = (bytes[0] & 0x60) >> 5;
                let mut pos = 1usize;
                while pos < bytes.len() {
                    if bytes.len() - pos < 2 {
                        self.packet_diagnostics
                            .record(self.index, DropReason::InvalidMerged);
                        return Ok(());
                    }
                    let tag = bytes[pos];
                    pos += 1;
                    let is_raw = tag & RAW_PACKET_FLAG != 0;
                    let channel = tag & CHANNEL_MASK;
                    if is_raw && channel != 0 {
                        self.packet_diagnostics
                            .record(self.index, DropReason::InvalidMerged);
                        return Ok(());
                    }
                    let payload_len = if tag & LONG_LENGTH_FLAG != 0 {
                        if bytes.len() - pos < 2 {
                            self.packet_diagnostics
                                .record(self.index, DropReason::InvalidMerged);
                            return Ok(());
                        }
                        let len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
                        pos += 2;
                        if len <= u8::MAX as usize {
                            self.packet_diagnostics
                                .record(self.index, DropReason::InvalidMerged);
                            return Ok(());
                        }
                        len
                    } else {
                        let len = bytes[pos] as usize;
                        pos += 1;
                        len
                    };
                    if payload_len > bytes.len() - pos {
                        self.packet_diagnostics
                            .record(self.index, DropReason::InvalidMerged);
                        return Ok(());
                    }
                    let payload = &bytes[pos..pos + payload_len];
                    pos += payload_len;
                    if is_raw {
                        if payload_len < 4 {
                            self.packet_diagnostics
                                .record(self.index, DropReason::InvalidMerged);
                            return Ok(());
                        }
                        let property = PacketProperty::from_byte(payload[0]);
                        if !matches!(
                            property,
                            Some(PacketProperty::Ack | PacketProperty::Channeled)
                        ) {
                            self.packet_diagnostics
                                .record(self.index, DropReason::InvalidMerged);
                            return Ok(());
                        }
                        Box::pin(self.handle_packet(payload)).await?;
                    } else {
                        let mut packet = Vec::with_capacity(payload_len + 2);
                        packet.push(PacketProperty::Unreliable as u8 | (connection_number << 5));
                        packet.push(channel);
                        packet.extend_from_slice(payload);
                        Box::pin(self.handle_packet(&packet)).await?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) async fn observe_avatar_channel(&self, channel: u8, payload: &[u8]) {
        let Some(session) = &self.avatar_observer else {
            return;
        };
        if !session.could_own(self.index) {
            return;
        }
        let observer_position = self.pose.lock().await.position();
        if let Some(mut state) = session.lock() {
            let now = std::time::Instant::now();
            // Check after locking: a deactivated/replaced client must not reclaim ownership.
            if self.connected.load(Ordering::Acquire)
                && self.in_use.load(Ordering::Acquire)
                && session.claim(&mut state, (self.index, self.connect_time), now)
            {
                state
                    .observer
                    .observe_channel(channel, payload, observer_position, now);
            }
        }
    }

    pub(crate) fn metadata_state(&self) -> std::sync::MutexGuard<'_, Option<ServerAvatarMetadata>> {
        match self.server_avatar_metadata.lock() {
            Ok(state) => state,
            Err(poisoned) => {
                let mut state = poisoned.into_inner();
                *state = None;
                self.server_avatar_metadata.clear_poison();
                self.shared_receive_eligible.store(false, Ordering::Release);
                self.force_avatar_keyframe.store(true, Ordering::Release);
                warn!(
                    client = self.index,
                    "server metadata mutex poisoned; discarded metadata, deactivating for reconnect"
                );
                // Metadata is sent during startup, not periodically. A fresh connection
                // must obtain it again rather than leaving Unity avatar generation stalled.
                self.deactivate();
                state
            }
        }
    }

    pub(crate) fn reliable_receive_state(
        &self,
    ) -> Option<std::sync::MutexGuard<'_, ReliableReceiveState>> {
        match self.received_reliable.lock() {
            Ok(state) => Some(state),
            Err(_) => {
                // Resetting an ACK/replay window could acknowledge lost data or replay auth.
                // Fail this connection closed; a replacement has fresh protocol state.
                if self.in_use.load(Ordering::Acquire) {
                    warn!(
                        client = self.index,
                        "reliable receive mutex poisoned; deactivating connection without ACK"
                    );
                    self.deactivate();
                }
                self.ack_pending.store(false, Ordering::Relaxed);
                None
            }
        }
    }

    pub(crate) fn refresh_shared_receive_eligibility(&self) {
        let metadata_ready = self.metadata_state().is_some();
        if shared_receive_handoff_ready(self.connected.load(Ordering::Acquire), metadata_ready) {
            self.shared_receive_eligible.store(true, Ordering::Release);
        }
    }

    pub(crate) async fn handle_channeled(
        &self,
        channel_id: u8,
        sequence: u16,
        payload: &[u8],
    ) -> Result<()> {
        let channel = channel_id / 4;
        let delivery = DeliveryMethod::from_channel_id(channel_id);
        if matches!(
            delivery,
            DeliveryMethod::ReliableOrdered | DeliveryMethod::ReliableUnordered
        ) {
            // Record first, then let the ACK flush pick the window up: `ack_window` reports what
            // has been received so far, so the sequence being handled has to be in the bitmap.
            // `mark_new` also arms the channel's dirty flag -- for duplicates too, which is
            // what lets a lost ACK recover -- and the flush sends one datagram per armed
            // channel rather than one per received packet.
            let Some(is_new) = self
                .reliable_receive_state()
                .map(|mut state| state.mark_new(channel_id, sequence))
            else {
                return Ok(());
            };
            self.ack_pending.store(true, Ordering::Relaxed);
            if !is_new {
                trace!(
                    "client {} suppressed duplicate reliable packet channel_id={} sequence={}",
                    self.index,
                    channel_id,
                    sequence
                );
                return Ok(());
            }
        }

        match channel {
            channels::AUTH_IDENTITY => {
                if let Some(challenge) = read_bytes_message(payload) {
                    let response = self.identity.response_payload(challenge)?;
                    self.send_reliable_ordered(channels::AUTH_IDENTITY, &response)
                        .await?;
                }
            }
            channels::META_DATA => match parse_server_avatar_metadata(payload) {
                Ok(metadata) => {
                    if self.avatar_observer.is_some() {
                        info!(
                            "observer client {} received avatar metadata: interval_ms={} base_multiplier={} distance_increase={} slowest_seconds={} uplink_delta={}",
                            self.index,
                            metadata.sync_interval_ms,
                            metadata.base_multiplier,
                            metadata.increase_rate,
                            metadata.slowest_send_rate_secs,
                            metadata.uplink_delta_enabled,
                        );
                    }
                    *self.metadata_state() = Some(metadata);
                    self.refresh_shared_receive_eligibility();
                }
                Err(err) => warn!(
                    "client {} could not parse server avatar metadata: {err}",
                    self.index
                ),
            },
            channels::DELTA_AVATAR
                if payload.first() == Some(&channels::DELTA_CONTROL_UPLINK_KEYFRAME_REQUEST) =>
            {
                self.force_avatar_keyframe.store(true, Ordering::Release);
            }
            channels::DELTA_AVATAR => {}
            channels::CREATE_REMOTE_PLAYER
            | channels::CREATE_REMOTE_PLAYERS_FOR_NEW_PEER
            | channels::DISCONNECTION
            | channels::PLAYER_AVATAR_VERY_LOW
            | channels::PLAYER_AVATAR_VERY_LOW_ADDITIONAL
            | channels::PLAYER_AVATAR_LOW
            | channels::PLAYER_AVATAR_LOW_ADDITIONAL
            | channels::PLAYER_AVATAR_MEDIUM
            | channels::PLAYER_AVATAR_MEDIUM_ADDITIONAL
            | channels::PLAYER_AVATAR_HIGH
            | channels::PLAYER_AVATAR_HIGH_ADDITIONAL
            | channels::PLAYER_AVATAR_VERY_LOW_LARGE
            | channels::PLAYER_AVATAR_VERY_LOW_ADDITIONAL_LARGE
            | channels::PLAYER_AVATAR_LOW_LARGE
            | channels::PLAYER_AVATAR_LOW_ADDITIONAL_LARGE
            | channels::PLAYER_AVATAR_MEDIUM_LARGE
            | channels::PLAYER_AVATAR_MEDIUM_ADDITIONAL_LARGE
            | channels::PLAYER_AVATAR_HIGH_LARGE
            | channels::PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE
            | channels::COMPRESSED_AVATAR_BUNDLE
            | channels::SERVER_LIBRARY => {}
            _ => {}
        }
        Ok(())
    }

    /// Acknowledge every channel whose receive window has changed since the last ACK.
    ///
    /// This is LiteNetLib's batching: the C# sets `_mustSendAcks` on arrival and flushes the
    /// accumulated window from `SendNextPackets` on the network update tick, so a burst of
    /// reliable packets costs one ACK rather than one per packet. The bytes are identical either
    /// way -- a full window with the same window start -- so this changes volume, not protocol.
    ///
    /// Windows are rendered under the lock and sent outside it, so a slow socket write never
    /// blocks packet reception.
    pub(crate) async fn flush_acks(&self) -> Result<()> {
        let pending = {
            let Some(mut received) = self.reliable_receive_state() else {
                return Ok(());
            };
            received
                .take_dirty_channels()
                .into_iter()
                .filter_map(|channel_id| {
                    received
                        .ack_window(channel_id)
                        .map(|window| (channel_id, window))
                })
                .collect::<Vec<_>>()
        };
        for (channel_id, (window_start, bits)) in pending {
            let mut packet = Vec::with_capacity(4 + bits.len());
            packet.push(PacketProperty::Ack as u8 | (self.connection_number << 5));
            packet.extend_from_slice(&window_start.to_le_bytes());
            packet.push(channel_id);
            packet.extend_from_slice(&bits);
            self.send_connected(&packet).await?;
        }
        Ok(())
    }

    pub(crate) async fn send_pong(&self, sequence: u16) -> Result<()> {
        let mut writer = NetWriter::with_capacity(11);
        writer.put_u8(PacketProperty::Pong as u8 | (self.connection_number << 5));
        writer.put_u16(sequence);
        writer.put_i64(dotnet_utc_ticks());
        self.send_connected(&writer.into_vec()).await?;
        Ok(())
    }

    pub(crate) async fn send_ping(&self) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }
        let sequence = self.ping_sequence.fetch_add(1, Ordering::SeqCst);
        let mut writer = NetWriter::with_capacity(3);
        writer.put_u8(PacketProperty::Ping as u8 | (self.connection_number << 5));
        writer.put_u16(sequence);
        self.send_connected(&writer.into_vec()).await?;
        Ok(())
    }

    pub(crate) async fn process_ack(
        &self,
        channel_id: u8,
        ack_window_start: u16,
        ack_bits: &[u8],
    ) -> Result<()> {
        if ack_bits.len() != (DEFAULT_WINDOW_SIZE - 1) / 8 + 2 {
            self.packet_diagnostics
                .record(self.index, DropReason::InvalidAckSize);
            return Ok(());
        }
        let mut pending = self.pending_reliable.lock().await;
        let Some(local_window_start) = pending
            .iter()
            .filter(|item| item.channel_id == channel_id)
            .find_map(|item| item.sequence)
        else {
            drop(pending);
            return self.pump_reliable_window().await;
        };
        // LiteNetLib `ReliableChannel.ProcessAck` bounds check: the window start must be a legal
        // sequence and within one window ahead of our own oldest unacknowledged packet.
        if ack_window_start >= MAX_SEQUENCE {
            return Ok(());
        }
        let window_rel = relative_sequence(local_window_start, ack_window_start);
        if window_rel < 0 || window_rel as usize >= DEFAULT_WINDOW_SIZE {
            return Ok(());
        }
        pending.retain(|item| {
            if item.channel_id != channel_id {
                return true;
            }
            let Some(sequence) = item.sequence else {
                return true;
            };
            // LiteNetLib stops scanning once a pending sequence is 128 or more ahead of this
            // ACK window. The bitmap repeats every 128 sequences, so omitting this bound lets
            // a delayed old ACK alias and release newer packets.
            let relative = relative_sequence(sequence, ack_window_start);
            // ACK bits are absolute -- `sequence % DEFAULT_WINDOW_SIZE` -- not offsets from the
            // window start. That is what the server sends, matching LiteNetLib.
            let pos = sequence as usize % DEFAULT_WINDOW_SIZE;
            let acked = relative < DEFAULT_WINDOW_SIZE as i32
                && ack_bits
                    .get(pos / 8)
                    .map(|b| (b & (1 << (pos % 8))) != 0)
                    .unwrap_or(false);
            !acked
        });
        self.pending_reliable_active
            .store(!pending.is_empty(), Ordering::Relaxed);
        drop(pending);
        self.pump_reliable_window().await
    }

    pub(crate) async fn maintenance_loop(self: Arc<Self>) {
        // Ticks at LiteNetLib's network update rate rather than the old 100 ms maintenance
        // period, because that is what bounds how long a received packet waits for its ACK.
        // ACKs have to go out promptly: the sender's window is only 128 deep and refills no
        // faster than we acknowledge, so a 100 ms flush cadence would throttle throughput
        // rather than batch it. The slower work is derived from the tick count so its period
        // is unchanged. Everything except the ACK flush is gated, so the extra ticks are a
        // timer wake and one relaxed atomic load.
        let mut tick = time::interval(ACK_FLUSH_INTERVAL);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut ticks = 0usize;
        loop {
            tick.tick().await;
            if !self.in_use.load(Ordering::Relaxed) {
                break;
            }
            ticks = ticks.wrapping_add(1);

            // One ACK per channel whose window changed, however many packets arrived since.
            if self.ack_pending.swap(false, Ordering::Relaxed) {
                let _ = self.flush_acks().await;
            }

            if ticks.is_multiple_of(RESEND_INTERVAL_TICKS)
                && self.pending_reliable_active.load(Ordering::Relaxed)
            {
                let _ = self.resend_reliable().await;
            }

            if ticks.is_multiple_of(PING_INTERVAL_TICKS) {
                let _ = self.send_ping().await;
            }
        }
    }

    pub(crate) async fn resend_reliable(&self) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Reserve due packets while holding the queue lock, but never await a socket operation
        // while holding it. This lets an ACK remove packets while a resend batch is in flight.
        let now = SystemTime::now();
        let due = {
            let mut pending = self.pending_reliable.lock().await;
            if pending.is_empty() {
                self.pending_reliable_active.store(false, Ordering::Relaxed);
                return Ok(());
            }

            let mut due = Vec::new();
            for item in pending.iter_mut() {
                let should_send = item.sequence.is_some()
                    && item
                        .last_sent
                        .and_then(|sent| now.duration_since(sent).ok())
                        .map(|elapsed| elapsed >= Duration::from_millis(150))
                        .unwrap_or(true);
                if should_send {
                    // Mark before sending so another maintenance pass cannot select the same
                    // packet while this pass is awaiting the UDP write.
                    item.last_sent = Some(now);
                    due.push(item.clone());
                }
            }
            due
        };

        for item in due {
            if let Err(err) = self.send_connected(&item.bytes).await {
                // Keep failed packets immediately eligible for the next retry. The full message
                // remains in the queue, including fragments that were not reached yet.
                let mut pending = self.pending_reliable.lock().await;
                if let Some(current) = pending.iter_mut().find(|current| {
                    current.channel_id == item.channel_id
                        && current.sequence == item.sequence
                        && current.bytes == item.bytes
                }) {
                    current.last_sent = None;
                }
                return Err(err);
            }
        }
        Ok(())
    }
}
