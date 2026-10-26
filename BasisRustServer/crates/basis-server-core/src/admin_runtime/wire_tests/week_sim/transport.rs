//! Continuously drained loopback peers for the accelerated lifecycle simulation.
use super::super::{unpack, Client, Frame};
use super::traffic::AvatarReceiver;
use crate::ServerState;
use basis_protocol::{
    channels,
    io::{NetReader, NetWriter},
    messages::{
        BasisDeserialize, LocalLoadResource, OwnershipTransferMessage, ReadyMessage,
        RemoteAvatarDataMessage, ServerNetIdMessage, ServerReadyBatchMessage, UnloadResource,
    },
};
use basis_transport::{
    dotnet_utc_ticks, relative_sequence, DeliveryMethod, PacketProperty, PeerSession, MAX_SEQUENCE,
};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::Duration,
};
use tokio::{net::UdpSocket, sync::oneshot, task::JoinHandle};

const WINDOW: usize = 128;
const MAX_FRAGMENTS: usize = 32;
const MAX_PARTS: usize = 1024;
const MAX_REASSEMBLED_BYTES: usize = 1024 * 1024;
const MAX_ENTRIES: usize = 4096;
const VOICE_HISTORY: usize = 256;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SceneSnapshot {
    pub sender: u16,
    pub payload: Vec<u8>,
}

#[derive(Clone, Default)]
pub struct View {
    pub latest: HashMap<u8, Vec<u8>>,
    pub channel_counts: HashMap<u8, u64>,
    pub scenes: HashMap<u16, SceneSnapshot>,
    pub roster: HashSet<u16>,
    pub departures: u64,
    pub ownership: HashMap<String, u16>,
    pub net_ids: HashMap<String, u16>,
    pub resources: HashMap<String, LocalLoadResource>,
    pub voice_counts: HashMap<u16, u64>,
    pub voice_latest: HashMap<u16, Vec<u8>>,
    /// A bounded exact-packet tail; counters remain valid after tail eviction.
    pub voice_packets: VecDeque<(u16, Vec<u8>)>,
    pub voice_history_evictions: u64,
    pub dynamic_avatar: HashMap<(u16, u8, u8), Vec<u8>>,
    pub avatars: AvatarReceiver,
    pub errors: Vec<String>,
    pub error_count: u64,
    pub datagrams: u64,
    pub reliable_duplicates: u64,
    pub fragmented_messages: u64,
    pub pings: u64,
    pub pong_replies: u64,
    pub pongs: u64,
    pub client_pings: u64,
    pub disconnect_reason: Option<String>,
    pub keyframe_requests: u64,
    pub convergence_keyframe_requests: u64,
    pub reliable_retries: u64,
    pub reliable_acks: u64,
    pub disconnected: bool,
}

impl View {
    fn error(&mut self, message: impl Into<String>) {
        self.error_count += 1;
        if self.errors.len() < 16 {
            self.errors.push(message.into());
        }
    }

    fn join(&mut self, id: u16) {
        if self.roster.insert(id) {
            self.avatars.remove_peer(id);
            self.voice_counts.remove(&id);
            self.voice_latest.remove(&id);
            self.voice_packets.retain(|(sender, _)| *sender != id);
        }
    }

    fn observe(&mut self, channel: u8, payload: Vec<u8>) -> anyhow::Result<()> {
        *self.channel_counts.entry(channel).or_default() += 1;
        let mut reader = NetReader::new(&payload);
        match channel {
            channels::CREATE_REMOTE_PLAYER => {
                let id = reader.get_u16()?;
                ReadyMessage::deserialize(&mut reader)?;
                self.join(id);
            }
            channels::CREATE_REMOTE_PLAYERS_FOR_NEW_PEER => {
                let batch = ServerReadyBatchMessage::deserialize(&mut reader)?;
                let mut records = NetReader::new(&batch.payload);
                for _ in 0..batch.count {
                    let id = records.get_u16()?;
                    ReadyMessage::deserialize(&mut records)?;
                    self.join(id);
                }
                anyhow::ensure!(records.remaining() == 0, "roster batch trailing bytes");
            }
            channels::DISCONNECTION => {
                while reader.remaining() > 0 {
                    let id = reader.get_u16()?;
                    self.roster.remove(&id);
                    self.avatars.remove_peer(id);
                    self.voice_counts.remove(&id);
                    self.voice_latest.remove(&id);
                    self.voice_packets.retain(|(sender, _)| *sender != id);
                    self.dynamic_avatar.retain(|(peer, _, _), _| *peer != id);
                    self.departures += 1;
                }
            }
            channels::SCENE | channels::DIRECT_SCENE_SERVER => {
                let sender = reader.get_u16()?;
                let index = reader.get_u16()?;
                anyhow::ensure!(
                    self.scenes.contains_key(&index) || self.scenes.len() < MAX_ENTRIES,
                    "scene observer capacity"
                );
                self.scenes.insert(
                    index,
                    SceneSnapshot {
                        sender,
                        payload: reader.remaining_slice().to_vec(),
                    },
                );
            }
            channels::GET_CURRENT_OWNER_REQUEST | channels::CHANGE_CURRENT_OWNER_REQUEST => {
                let ownership = OwnershipTransferMessage::deserialize(&mut reader)?;
                anyhow::ensure!(
                    self.ownership.contains_key(&ownership.ownership_id)
                        || self.ownership.len() < MAX_ENTRIES,
                    "ownership observer capacity"
                );
                self.ownership
                    .insert(ownership.ownership_id, ownership.player_id);
            }
            channels::REMOVE_CURRENT_OWNER_REQUEST => {
                let ownership = OwnershipTransferMessage::deserialize(&mut reader)?;
                self.ownership.remove(&ownership.ownership_id);
            }
            channels::NET_ID_ASSIGN | channels::NET_ID_ASSIGNS => {
                let count = if channel == channels::NET_ID_ASSIGNS {
                    reader.get_u16()?
                } else {
                    1
                };
                for _ in 0..count {
                    let net_id = ServerNetIdMessage::deserialize(&mut reader)?;
                    anyhow::ensure!(
                        self.net_ids.contains_key(&net_id.net_id_message.player_id)
                            || self.net_ids.len() < MAX_ENTRIES,
                        "net ID observer capacity"
                    );
                    self.net_ids.insert(
                        net_id.net_id_message.player_id,
                        net_id.ushort_unique_id_message.unique_id_ushort,
                    );
                }
            }
            channels::LOAD_RESOURCE => {
                let resource = LocalLoadResource::deserialize(&mut reader)?;
                anyhow::ensure!(
                    self.resources.contains_key(&resource.loaded_net_id)
                        || self.resources.len() < MAX_ENTRIES,
                    "resource observer capacity"
                );
                self.resources
                    .insert(resource.loaded_net_id.clone(), resource);
            }
            channels::UNLOAD_RESOURCE => {
                let resource = UnloadResource::deserialize(&mut reader)?;
                self.resources.remove(&resource.loaded_net_id);
            }
            channels::VOICE | channels::VOICE_LARGE => {
                let sender = if channel == channels::VOICE_LARGE {
                    reader.get_u16()?
                } else {
                    u16::from(reader.get_u8()?)
                };
                let audio = reader.remaining_slice().to_vec();
                *self.voice_counts.entry(sender).or_default() += 1;
                self.voice_latest.insert(sender, audio.clone());
                if self.voice_packets.len() == VOICE_HISTORY {
                    self.voice_packets.pop_front();
                    self.voice_history_evictions += 1;
                }
                self.voice_packets.push_back((sender, audio));
            }
            channels::AVATAR | channels::DIRECT_AVATAR_SERVER => {
                let sender = reader.get_u16()?;
                let avatar = RemoteAvatarDataMessage::deserialize(&mut reader)?;
                let key = (sender, avatar.avatar_link_index, avatar.message_index);
                anyhow::ensure!(
                    self.dynamic_avatar.contains_key(&key)
                        || self.dynamic_avatar.len() < MAX_ENTRIES,
                    "dynamic avatar observer capacity"
                );
                self.dynamic_avatar.insert(key, avatar.payload);
            }
            channels::COMPRESSED_AVATAR_BUNDLE | channels::DELTA_AVATAR => {
                self.avatars.ingest(channel, &payload)?
            }
            c if channels::PLAYER_AVATAR_QUALITY_CHANNELS.contains(&c) => {
                self.avatars.ingest(channel, &payload)?
            }
            _ => {}
        }
        self.latest.insert(channel, payload);
        Ok(())
    }
}

/// One real transport incarnation. Reconnection creates a fresh actor and view.
pub struct WirePeer {
    pub id: u16,
    pub uuid: String,
    pub session: PeerSession,
    pub view: Arc<Mutex<View>>,
    socket: Arc<UdpSocket>,
    sequences: HashMap<u8, u16>,
    outbound: Arc<Mutex<Outbound>>,
    stop: Option<oneshot::Sender<()>>,
    reader: Option<JoinHandle<()>>,
}

impl WirePeer {
    pub async fn connect(state: &ServerState, uuid: &str) -> Self {
        let client = Client::connect_pending(state, uuid).await;
        let socket = Arc::new(client.socket);
        let view = Arc::new(Mutex::new(View::default()));
        let outbound = Arc::new(Mutex::new(Outbound::default()));
        let (stop, stop_rx) = oneshot::channel();
        let reader = tokio::spawn(read_loop(
            socket.clone(),
            view.clone(),
            outbound.clone(),
            stop_rx,
        ));
        let (id, session) = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let admitted = state.authenticated_peers.iter().find_map(|peer| {
                    (peer.metadata.player_uuid == uuid)
                        .then(|| peer.session.clone().map(|session| (*peer.key(), session)))
                        .flatten()
                });
                if let Some(admitted) = admitted {
                    if view.lock().latest.contains_key(&channels::META_DATA) {
                        break admitted;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "wire admission timed out for {uuid}: {:?}",
                view.lock().errors
            )
        });
        Self {
            id,
            uuid: uuid.to_owned(),
            session,
            view,
            socket,
            sequences: HashMap::new(),
            outbound,
            stop: Some(stop),
            reader: Some(reader),
        }
    }

    pub fn snapshot(&self) -> View {
        self.view.lock().clone()
    }

    pub fn unreliable_socket(&self) -> Arc<UdpSocket> {
        self.socket.clone()
    }

    pub async fn wait_until(&self, predicate: impl Fn(&View) -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if predicate(&self.view.lock()) { return; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.unwrap_or_else(|_| {
            let view = self.view.lock();
            let mut avatars = view.avatars.avatars.iter().map(|(id, avatar)| {
                (*id, avatar.sequence, avatar.quality, avatar.additional.len())
            }).collect::<Vec<_>>();
            avatars.sort_unstable_by_key(|avatar| avatar.0);
            let avatar_count = avatars.len();
            if avatars.len() > 16 { avatars.truncate(16); }
            panic!("wire expectation timed out for {} (id {}, datagrams {}, channels {:?}, avatars {}/sample(id,seq,quality,additional) {:?}, full {}, delta {}, unapplied {}, stale {}, errors {:?})", self.uuid, self.id, view.datagrams, view.channel_counts, avatar_count, avatars, view.avatars.full_items, view.avatars.delta_items, view.avatars.unapplied_deltas, view.avatars.stale_items, view.errors);
        });
    }

    pub async fn send(&mut self, channel: u8, payload: &[u8], reliable: bool) {
        let mut writer = NetWriter::with_capacity(payload.len() + 4);
        let mut reliable_key = None;
        if reliable {
            writer.put_u8(PacketProperty::Channeled as u8);
            let channel_id = DeliveryMethod::channel_id(channel, DeliveryMethod::ReliableOrdered);
            let next = self.sequences.get(&channel).copied().unwrap_or(0);
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if self.outbound.lock().has_capacity(channel_id, next) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("wire reliable send window stalled");
            let sequence = self.sequences.entry(channel).or_default();
            reliable_key = Some((channel_id, *sequence));
            writer.put_u16(*sequence);
            *sequence = sequence.wrapping_add(1) % MAX_SEQUENCE;
            writer.put_u8(channel_id);
        } else {
            writer.put_u8(PacketProperty::Unreliable as u8);
            writer.put_u8(channel);
        }
        writer.put_bytes(payload);
        if let Some((channel_id, sequence)) = reliable_key {
            let now = tokio::time::Instant::now();
            self.outbound
                .lock()
                .pending
                .entry(channel_id)
                .or_default()
                .push_back(PendingSend {
                    sequence,
                    packet: writer.as_slice().to_vec(),
                    first_sent: now,
                    last_sent: now,
                });
        }
        self.socket
            .send(writer.as_slice())
            .await
            .expect("wire send");
    }

    /// Send the real LiteNetLib leave packet; wait for server removal separately.
    pub async fn disconnect(&mut self) {
        let mut writer = NetWriter::new();
        writer.put_u8(PacketProperty::Disconnect as u8);
        writer.put_i64(1);
        self.socket
            .send(writer.as_slice())
            .await
            .expect("wire disconnect");
        self.stop().await;
    }

    pub async fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(reader) = self.reader.take() {
            reader.await.expect("wire reader task");
        }
    }
}

impl Drop for WirePeer {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(reader) = self.reader.take() {
            reader.abort();
        }
    }
}

#[derive(Default)]
struct ReceiveChannel {
    start: u16,
    bits: [u8; 17],
    next: u16,
    pending: HashMap<u16, Frame>,
}

impl ReceiveChannel {
    fn mark(&mut self, sequence: u16) -> bool {
        let relative = relative_sequence(sequence, self.start);
        if relative < 0 {
            return false;
        }
        if relative >= WINDOW as i32 {
            for _ in 0..(relative as usize - WINDOW + 1) {
                let index = self.start as usize % WINDOW;
                self.bits[index / 8] &= !(1 << (index % 8));
                self.start = self.start.wrapping_add(1) % MAX_SEQUENCE;
            }
        }
        let index = sequence as usize % WINDOW;
        let new = self.bits[index / 8] & (1 << (index % 8)) == 0;
        self.bits[index / 8] |= 1 << (index % 8);
        new
    }

    fn ack(&self, channel_id: u8) -> Vec<u8> {
        let mut packet = Vec::with_capacity(21);
        packet.push(PacketProperty::Ack as u8);
        let sequenced =
            DeliveryMethod::from_channel_id(channel_id) == DeliveryMethod::ReliableSequenced;
        packet.extend_from_slice(&if sequenced { self.next } else { self.start }.to_le_bytes());
        packet.push(channel_id);
        if !sequenced {
            packet.extend_from_slice(&self.bits);
        }
        packet
    }
}

struct FragmentParts {
    parts: Vec<Option<Vec<u8>>>,
    bytes: usize,
    created: tokio::time::Instant,
}

struct PendingSend {
    sequence: u16,
    packet: Vec<u8>,
    first_sent: tokio::time::Instant,
    last_sent: tokio::time::Instant,
}

#[derive(Default)]
struct Outbound {
    pending: HashMap<u8, VecDeque<PendingSend>>,
}

impl Outbound {
    fn has_capacity(&self, channel_id: u8, next: u16) -> bool {
        self.pending.get(&channel_id).is_none_or(|queue| {
            queue.len() < WINDOW
                && queue.front().is_none_or(|oldest| {
                    let relative = relative_sequence(next, oldest.sequence);
                    (0..WINDOW as i32).contains(&relative)
                })
        })
    }

    fn ack(&mut self, packet: &[u8]) -> usize {
        if packet.len() < 21 {
            return 0;
        }
        let start = u16::from_le_bytes([packet[1], packet[2]]);
        if start >= MAX_SEQUENCE {
            return 0;
        }
        let Some(queue) = self.pending.get_mut(&packet[3]) else {
            return 0;
        };
        let Some(oldest) = queue.front() else {
            return 0;
        };
        if !(0..WINDOW as i32).contains(&relative_sequence(oldest.sequence, start)) {
            return 0;
        }
        let before = queue.len();
        queue.retain(|pending| {
            let relative = relative_sequence(pending.sequence, start);
            let index = pending.sequence as usize % WINDOW;
            !(relative < WINDOW as i32 && packet[4 + index / 8] & (1 << (index % 8)) != 0)
        });
        let released = before - queue.len();
        if queue.is_empty() {
            self.pending.remove(&packet[3]);
        }
        released
    }

    fn retries(&mut self, now: tokio::time::Instant) -> (Vec<Vec<u8>>, usize) {
        let mut packets = Vec::new();
        let mut expired = 0;
        self.pending.retain(|_, queue| {
            queue.retain_mut(|pending| {
                if now.duration_since(pending.first_sent) >= Duration::from_secs(10) {
                    expired += 1;
                    return false;
                }
                if now.duration_since(pending.last_sent) >= Duration::from_millis(100) {
                    pending.last_sent = now;
                    packets.push(pending.packet.clone());
                }
                true
            });
            !queue.is_empty()
        });
        (packets, expired)
    }
}

/// Control packets can also be raw children of either merged framing format.
fn collect_controls<'a>(packet: &'a [u8], controls: &mut Vec<&'a [u8]>) -> anyhow::Result<()> {
    let Some(first) = packet.first() else {
        return Ok(());
    };
    match PacketProperty::from_byte(*first) {
        Some(
            PacketProperty::Ack
            | PacketProperty::Ping
            | PacketProperty::Pong
            | PacketProperty::Disconnect,
        ) => controls.push(packet),
        Some(PacketProperty::Merged) => {
            let mut reader = NetReader::new(&packet[1..]);
            while reader.remaining() > 0 {
                let size = reader.get_u16()? as usize;
                collect_controls(reader.get_bytes(size)?, controls)?;
            }
        }
        Some(PacketProperty::CompactMerged) => {
            let mut reader = NetReader::new(&packet[1..]);
            while reader.remaining() > 0 {
                let tag = reader.get_u8()?;
                let size = if tag & 128 != 0 {
                    reader.get_u16()? as usize
                } else {
                    reader.get_u8()? as usize
                };
                let payload = reader.get_bytes(size)?;
                if tag & 64 != 0 {
                    collect_controls(payload, controls)?;
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn ping_reply(packet: &[u8]) -> anyhow::Result<Vec<u8>> {
    anyhow::ensure!(packet.len() >= 3, "short ping packet");
    let mut pong = NetWriter::new();
    pong.put_u8(PacketProperty::Pong as u8 | (packet[0] & 0x60));
    pong.put_bytes(&packet[1..3]);
    pong.put_i64(dotnet_utc_ticks());
    Ok(pong.into_vec())
}

async fn read_loop(
    socket: Arc<UdpSocket>,
    view: Arc<Mutex<View>>,
    outbound: Arc<Mutex<Outbound>>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut packet = vec![0u8; 65535];
    let mut channels: HashMap<u8, ReceiveChannel> = HashMap::new();
    let mut fragments: HashMap<(u8, u16), FragmentParts> = HashMap::new();
    let mut frames = Vec::new();
    let mut retry_timer = tokio::time::interval(Duration::from_millis(100));
    retry_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut last_client_ping = tokio::time::Instant::now();
    let mut client_ping_sequence = 0u16;
    loop {
        let length = tokio::select! {
            _ = &mut stop => return,
            _ = retry_timer.tick() => {
                let now = tokio::time::Instant::now();
                let (packets, expired) = outbound.lock().retries(now);
                if expired > 0 { view.lock().error(format!("{expired} reliable sends expired")); }
                for packet in packets {
                    if let Err(error) = socket.send(&packet).await { view.lock().error(format!("reliable retry: {error}")); }
                    view.lock().reliable_retries += 1;
                }
                // Real clients initiate Ping as well as answering server Ping. Pong alone
                // does not refresh this server's last_seen, so idle peers require this path.
                let ping_due = !view.lock().disconnected && now.duration_since(last_client_ping) >= Duration::from_secs(1);
                if ping_due {
                    let [low, high] = client_ping_sequence.to_le_bytes();
                    let ping = [PacketProperty::Ping as u8, low, high];
                    if let Err(error) = socket.send(&ping).await { view.lock().error(format!("client ping: {error}")); }
                    else { view.lock().client_pings += 1; }
                    client_ping_sequence = client_ping_sequence.wrapping_add(1);
                    last_client_ping = now;
                }
                continue;
            }
            result = socket.recv(&mut packet) => match result {
                Ok(length) => length,
                Err(error) => { view.lock().error(format!("recv: {error}")); return; }
            }
        };
        if length == 0 {
            view.lock().error("empty transport datagram");
            continue;
        }
        view.lock().datagrams += 1;
        let mut controls = Vec::new();
        if let Err(error) = collect_controls(&packet[..length], &mut controls) {
            view.lock().error(format!("control framing: {error:#}"));
            continue;
        }
        let released = {
            let mut outbound = outbound.lock();
            controls
                .iter()
                .filter(|packet| PacketProperty::from_byte(packet[0]) == Some(PacketProperty::Ack))
                .map(|ack| outbound.ack(ack))
                .sum::<usize>()
        };
        view.lock().reliable_acks += released as u64;
        for control in controls {
            match PacketProperty::from_byte(control[0]) {
                Some(PacketProperty::Ping) => {
                    view.lock().pings += 1;
                    match ping_reply(control) {
                        Ok(pong) => {
                            if let Err(error) = socket.send(&pong).await {
                                view.lock().error(format!("pong: {error}"));
                            } else {
                                view.lock().pong_replies += 1;
                            }
                        }
                        Err(error) => view.lock().error(format!("ping: {error:#}")),
                    }
                }
                Some(PacketProperty::Pong) => {
                    view.lock().pongs += 1;
                }
                Some(PacketProperty::Disconnect) => {
                    let reason = if control.len() >= 9 {
                        let mut reader = NetReader::new(&control[9..]);
                        if reader.remaining() == 0 {
                            "remote disconnect".to_owned()
                        } else {
                            reader
                                .get_string()
                                .map(|reason| reason.chars().take(512).collect())
                                .unwrap_or_else(|error| {
                                    format!("unreadable disconnect reason: {error}")
                                })
                        }
                    } else {
                        "short disconnect packet".to_owned()
                    };
                    {
                        let mut view = view.lock();
                        view.disconnected = true;
                        view.disconnect_reason = Some(reason);
                    }
                    if let Err(error) = socket
                        .send(&[PacketProperty::ShutdownOk as u8 | (control[0] & 0x60)])
                        .await
                    {
                        view.lock().error(format!("shutdown ACK: {error}"));
                    }
                }
                _ => {}
            }
        }
        frames.clear();
        unpack(&packet[..length], &mut frames);
        let mut ready = Vec::new();
        let mut ack_channels = HashSet::new();
        for frame in frames.drain(..) {
            if let Some((channel_id, sequence)) = frame.1 {
                if sequence >= MAX_SEQUENCE {
                    view.lock().error("invalid reliable sequence");
                    continue;
                }
                let state = channels.entry(channel_id).or_default();
                let delivery = DeliveryMethod::from_channel_id(channel_id);
                if matches!(
                    delivery,
                    DeliveryMethod::Sequenced | DeliveryMethod::ReliableSequenced
                ) {
                    if delivery == DeliveryMethod::ReliableSequenced {
                        ack_channels.insert(channel_id);
                    }
                    if relative_sequence(sequence, state.next) > 0 {
                        state.next = sequence;
                        ready.push(frame);
                    } else {
                        view.lock().reliable_duplicates += 1;
                    }
                    continue;
                }
                let new = state.mark(sequence);
                ack_channels.insert(channel_id);
                if !new {
                    view.lock().reliable_duplicates += 1;
                    continue;
                }
                if delivery == DeliveryMethod::ReliableOrdered {
                    let relative = relative_sequence(sequence, state.next);
                    if relative < 0 {
                        continue;
                    }
                    if relative as usize >= WINDOW || state.pending.len() >= WINDOW {
                        view.lock().error("ordered receive window exceeded");
                        continue;
                    }
                    state.pending.insert(sequence, frame);
                    while let Some(frame) = state.pending.remove(&state.next) {
                        ready.push(frame);
                        state.next = state.next.wrapping_add(1) % MAX_SEQUENCE;
                    }
                } else {
                    ready.push(frame);
                }
            } else {
                ready.push(frame);
            }
        }
        for channel_id in ack_channels {
            if let Err(error) = socket.send(&channels[&channel_id].ack(channel_id)).await {
                view.lock().error(format!("ack: {error}"));
            }
        }
        fragments.retain(|_, parts| {
            if parts.created.elapsed() <= Duration::from_secs(10) {
                true
            } else {
                view.lock().error("fragment assembly expired");
                false
            }
        });
        for (channel, _, mut payload, fragment) in ready {
            if let Some((id, part, total)) = fragment {
                if total == 0 || usize::from(total) > MAX_PARTS || part >= total {
                    view.lock().error("invalid fragment header");
                    continue;
                }
                if !fragments.contains_key(&(channel, id)) && fragments.len() >= MAX_FRAGMENTS {
                    view.lock().error("fragment assembly capacity");
                    continue;
                }
                let parts = fragments
                    .entry((channel, id))
                    .or_insert_with(|| FragmentParts {
                        parts: vec![None; usize::from(total)],
                        bytes: 0,
                        created: tokio::time::Instant::now(),
                    });
                if parts.parts.len() != usize::from(total) {
                    view.lock().error("fragment count changed");
                    continue;
                }
                if parts.parts[usize::from(part)].is_none() {
                    parts.bytes += payload.len();
                    parts.parts[usize::from(part)] = Some(payload);
                }
                if parts.bytes > MAX_REASSEMBLED_BYTES {
                    fragments.remove(&(channel, id));
                    view.lock().error("fragment byte capacity");
                    continue;
                }
                if parts.parts.iter().any(Option::is_none) {
                    continue;
                }
                payload = Vec::with_capacity(parts.bytes);
                for part in &parts.parts {
                    payload.extend_from_slice(part.as_ref().unwrap());
                }
                fragments.remove(&(channel, id));
                view.lock().fragmented_messages += 1;
            }
            let mut view = view.lock();
            if let Err(error) = view.observe(channel, payload) {
                view.error(format!("channel {channel}: {error:#}"));
            }
        }
        let requests = view.lock().avatars.take_keyframe_requests();
        for sender in requests {
            let [low, high] = sender.to_le_bytes();
            let request = [
                PacketProperty::Unreliable as u8,
                channels::DELTA_AVATAR,
                channels::DELTA_CONTROL_KEYFRAME_REQUEST,
                low,
                high,
            ];
            if let Err(error) = socket.send(&request).await {
                view.lock().error(format!("keyframe request: {error}"));
            }
            view.lock().keyframe_requests += 1;
        }
    }
}

#[test]
fn reliable_sender_window_and_ack_wrap() {
    let channel = DeliveryMethod::channel_id(channels::SCENE, DeliveryMethod::ReliableOrdered);
    let pending = |sequence| {
        let now = tokio::time::Instant::now();
        PendingSend {
            sequence,
            packet: vec![],
            first_sent: now,
            last_sent: now,
        }
    };
    let ack = |start: u16, sequences: &[u16]| {
        let mut packet = vec![
            PacketProperty::Ack as u8,
            start as u8,
            (start >> 8) as u8,
            channel,
        ];
        packet.extend_from_slice(&[0; 17]);
        for sequence in sequences {
            let bit = *sequence as usize % WINDOW;
            packet[4 + bit / 8] |= 1 << (bit % 8);
        }
        packet
    };
    let mut outbound = Outbound::default();
    outbound
        .pending
        .insert(channel, VecDeque::from([pending(0)]));
    assert!(outbound.has_capacity(channel, 127));
    assert!(!outbound.has_capacity(channel, 128));
    assert_eq!(
        outbound.ack(&ack(1, &[0])),
        0,
        "future window cannot ACK oldest"
    );
    assert_eq!(
        outbound.ack(&ack(0, &[128])),
        1,
        "absolute bitmap bit aliases only inside validated window"
    );
    assert!(outbound.has_capacity(channel, 128));
    outbound.pending.insert(
        channel,
        VecDeque::from([pending(MAX_SEQUENCE - 1), pending(0)]),
    );
    assert!(outbound.has_capacity(channel, 1));
    assert_eq!(
        outbound.ack(&ack(MAX_SEQUENCE - 200, &[MAX_SEQUENCE - 1, 0])),
        0
    );
    assert_eq!(outbound.ack(&ack(MAX_SEQUENCE - 8, &[0])), 1);
    assert_eq!(
        outbound.pending[&channel].front().unwrap().sequence,
        MAX_SEQUENCE - 1
    );
    assert_eq!(outbound.ack(&ack(MAX_SEQUENCE - 8, &[MAX_SEQUENCE - 1])), 1);
    assert!(outbound.pending.is_empty());
}

#[test]
fn reliable_receive_ack_wrap_and_merged_ack_extraction() {
    let mut state = ReceiveChannel::default();
    for sequence in 0..MAX_SEQUENCE {
        assert!(state.mark(sequence));
    }
    assert!(state.mark(0));
    assert!(!state.mark(0));
    assert!(!state.mark(MAX_SEQUENCE - WINDOW as u16));
    assert_eq!(state.start, MAX_SEQUENCE - WINDOW as u16 + 1);
    let ack = state.ack(98);
    let mut merged = vec![PacketProperty::Merged as u8];
    merged.extend_from_slice(&(ack.len() as u16).to_le_bytes());
    merged.extend_from_slice(&ack);
    let mut compact = vec![PacketProperty::CompactMerged as u8, 64, merged.len() as u8];
    compact.extend_from_slice(&merged);
    let mut extracted = vec![];
    collect_controls(&compact, &mut extracted).unwrap();
    assert_eq!(extracted, vec![ack.as_slice()]);
}

#[test]
fn nested_merged_ping_and_disconnect_controls_are_preserved() {
    let ping = vec![PacketProperty::Ping as u8 | 0x40, 0x34, 0x12];
    let mut disconnect = NetWriter::new();
    disconnect.put_u8(PacketProperty::Disconnect as u8 | 0x40);
    disconnect.put_i64(1);
    disconnect.put_string("fixture disconnect").unwrap();
    let disconnect = disconnect.into_vec();
    let mut merged = vec![PacketProperty::Merged as u8 | 0x40];
    for child in [&ping, &disconnect] {
        merged.extend_from_slice(&(child.len() as u16).to_le_bytes());
        merged.extend_from_slice(child);
    }
    let mut compact = vec![
        PacketProperty::CompactMerged as u8 | 0x40,
        64,
        merged.len() as u8,
    ];
    compact.extend_from_slice(&merged);
    for wrapped in [&merged, &compact] {
        let mut controls = Vec::new();
        collect_controls(wrapped, &mut controls).unwrap();
        assert_eq!(controls, vec![ping.as_slice(), disconnect.as_slice()]);
        let pong = ping_reply(controls[0]).unwrap();
        assert_eq!(&pong[..3], &[PacketProperty::Pong as u8 | 0x40, 0x34, 0x12]);
        assert_eq!(pong.len(), 11);
        assert!(i64::from_le_bytes(pong[3..11].try_into().unwrap()) > 621_355_968_000_000_000);
        assert_eq!(
            NetReader::new(&controls[1][9..]).get_string().unwrap(),
            "fixture disconnect"
        );
        let mut data_frames = Vec::new();
        unpack(wrapped, &mut data_frames);
        assert!(
            data_frames.is_empty(),
            "the generic data unpacker deliberately discards controls"
        );
    }
}
