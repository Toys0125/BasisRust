mod avatar_sync;
mod p2p;

use anyhow::{Context, Result};
use basis_protocol::{
    application::NetworkApplication,
    avatar::BitQuality,
    avatar_delta::apply_delta,
    channels,
    config::{BasisUserRestrictionMode, ServerConfig},
    io::{NetReader, NetWriter},
    messages::{
        core_message_supply, decompress_permission_extras, AdminRequest, AdminRequestMode,
        AvatarDataMessage, BasisDeserialize, BasisMessageSubscribe, BasisSerialize, BytesMessage,
        CameraCountdownMessage, CameraShutterSoundMessage, ChatMessage, ClientBodyFitMessage,
        ClientCameraCountdownMessage, ClientCameraPipPositionMessage, ClientCameraPipStateMessage,
        ClientMetaDataMessage, ContentShareCleanupMessage, ContentShareMessage, ContentShareType,
        LocalLoadResource, ModifyResource, NetIdMessage, OwnershipTransferMessage,
        PreloadReadyMessage, ReadyMessage, RemoteAvatarDataMessage, RemoteSceneDataMessage,
        SceneDataMessage, ServerAudioSegmentMessage, ServerAvatarChangeMessage,
        ServerAvatarDataMessage, ServerBodyFitMessage, ServerChatMessage, ServerMetaDataMessage,
        ServerNetIdMessage, ServerReadyBatchMessage, ServerReadyMessage, ServerSceneDataMessage,
        ServerStatisticMessage, ServerUniqueIdMessages, SpawnPreloadedMessage, UnloadResource,
        UshortUniqueIdMessage, VoiceReceiversMessage,
    },
    server_info::ServerInfoResponse,
    version::SERVER_VERSION,
};
use basis_server_admin::{GlobalState, ModerationLists};
use basis_server_permissions::PermissionManager;
use basis_server_resources::{
    ContentShareState, NetIdState, OwnershipState, PipState, ResourceState,
};
use basis_server_storage::PersistentDatabase;
use basis_transport::{DeliveryMethod, DisconnectReason, PeerId, ServerEvent, TransportHandle};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::Write,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, oneshot, Semaphore};
use tracing::{error, info, warn};

pub use avatar_sync::{AvatarSyncConfig, AvatarSyncSystem};

#[derive(Debug, Clone)]
pub struct ConnectedPeer {
    pub id: PeerId,
    pub metadata: ClientMetaDataMessage,
    pub ready: ReadyMessage,
}

const JOIN_BATCH_FLUSH_INTERVAL: Duration = Duration::from_millis(50);

#[derive(Debug)]
struct JoinBroadcastRecord {
    sequence: u64,
    peer_id: PeerId,
    revision: AtomicU64,
    payload: RwLock<Vec<u8>>,
}

#[derive(Debug)]
struct JoinPeerState {
    sequence: u64,
    peer: ConnectedPeer,
    spawn_record: Arc<JoinBroadcastRecord>,
    initial_history_queued: bool,
    pending: Vec<Arc<JoinBroadcastRecord>>,
}

#[derive(Debug, Default)]
struct JoinBroadcastState {
    next_sequence: u64,
    peers: HashMap<PeerId, JoinPeerState>,
}

impl JoinBroadcastState {
    fn register_peer(
        &mut self,
        peer: ConnectedPeer,
        record_payload: Vec<u8>,
    ) -> Vec<ConnectedPeer> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let mut existing = self
            .peers
            .iter()
            .filter(|(_, state)| state.sequence < sequence)
            .map(|(peer_id, state)| (state.sequence, *peer_id, state.peer.clone()))
            .collect::<Vec<_>>();
        existing.sort_by_key(|(peer_sequence, _, _)| *peer_sequence);

        let record = Arc::new(JoinBroadcastRecord {
            sequence,
            peer_id: peer.id,
            revision: AtomicU64::new(0),
            payload: RwLock::new(record_payload),
        });
        for state in self.peers.values_mut() {
            if state.sequence < sequence {
                state.pending.push(record.clone());
            }
        }
        self.peers.insert(
            peer.id,
            JoinPeerState {
                sequence,
                peer,
                spawn_record: record,
                initial_history_queued: false,
                pending: Vec::new(),
            },
        );
        existing.into_iter().map(|(_, _, peer)| peer).collect()
    }

    fn mark_initial_history_queued(&mut self, peer_id: PeerId) {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.initial_history_queued = true;
        }
    }

    fn update_peer_ready(&mut self, peer_id: PeerId, ready: ReadyMessage) {
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.peer.ready = ready.clone();
            *peer.spawn_record.payload.write() = serialize_server_ready(peer_id, &ready);
            peer.spawn_record.revision.fetch_add(1, Ordering::Release);
        }
    }

    fn remove_peer(&mut self, peer_id: PeerId) {
        self.peers.remove(&peer_id);
        for peer in self.peers.values_mut() {
            peer.pending.retain(|record| record.peer_id != peer_id);
        }
    }

    fn take_batches(&mut self, peer_id: PeerId) -> Vec<Vec<Arc<JoinBroadcastRecord>>> {
        let Some(peer) = self.peers.get_mut(&peer_id) else {
            return Vec::new();
        };
        if !peer.initial_history_queued {
            return Vec::new();
        }
        peer.pending.sort_by_key(|record| record.sequence);
        let mut batches = Vec::new();
        while !peer.pending.is_empty() {
            let mut payload_bytes = 0usize;
            let mut take = 0usize;
            for record in &peer.pending {
                let record_len = record.payload.read().len();
                if take > 0
                    && payload_bytes + record_len > ServerReadyBatchMessage::MAX_PAYLOAD_BYTES
                {
                    break;
                }
                payload_bytes += record_len;
                take += 1;
            }
            batches.push(peer.pending.drain(..take).collect());
        }
        batches
    }

    fn ready_targets(&self) -> Vec<PeerId> {
        self.peers
            .iter()
            .filter_map(|(peer_id, peer)| peer.initial_history_queued.then_some(*peer_id))
            .collect()
    }
}

#[derive(Debug, Clone)]
struct UplinkDeltaState {
    baseline: Vec<u8>,
    baseline_sequence: u8,
    last_nack: Instant,
}

impl UplinkDeltaState {
    fn empty() -> Self {
        let now = Instant::now();
        Self {
            baseline: Vec::new(),
            baseline_sequence: 0,
            last_nack: now.checked_sub(Duration::from_secs(2)).unwrap_or(now),
        }
    }
}

#[derive(Debug, Clone)]
struct SceneEgressBucket {
    tokens: f64,
    last_refill: Instant,
}

#[derive(Debug, Clone)]
struct JiggleTokenBucket {
    tokens: f32,
    last_refill: Instant,
}

#[derive(Debug, Clone, Default)]
pub struct Statistics {
    pub inbound_packets: Arc<AtomicU64>,
    pub outbound_packets: Arc<AtomicU64>,
    pub protocol_errors: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct ServerState {
    pub config: Arc<RwLock<ServerConfig>>,
    config_path: Arc<PathBuf>,
    pub transport: TransportHandle,
    pub authenticated_peers: Arc<DashMap<PeerId, ConnectedPeer>>,
    join_broadcast: Arc<Mutex<JoinBroadcastState>>,
    pub pending_identity: Arc<DashMap<PeerId, ReadyMessage>>,
    pub permissions: PermissionManager,
    pub database: PersistentDatabase,
    pub resources: ResourceState,
    pub net_ids: NetIdState,
    pub ownership: OwnershipState,
    pub content_share: ContentShareState,
    pub pip: PipState,
    pub voice_recipients: Arc<DashMap<PeerId, Vec<PeerId>>>,
    pub message_subscriptions: Arc<DashMap<PeerId, HashSet<u16>>>,
    uplink_delta_states: Arc<DashMap<PeerId, UplinkDeltaState>>,
    scene_egress: Arc<DashMap<PeerId, SceneEgressBucket>>,
    jiggle_buckets: Arc<DashMap<PeerId, JiggleTokenBucket>>,
    error_report_hashes: Arc<DashMap<String, HashSet<u64>>>,
    pub avatar_sync: AvatarSyncSystem,
    pub p2p_broker: p2p::P2pBroker,
    pub moderation: ModerationLists,
    pub global_state: Arc<RwLock<GlobalState>>,
    pub statistics: Statistics,
    pending_leaves: Arc<Mutex<Vec<PeerId>>>,
    shutdown: Arc<AtomicBool>,
}

impl ServerState {
    pub async fn start(
        config: ServerConfig,
        base_dir: &Path,
    ) -> Result<(Self, oneshot::Sender<()>)> {
        let bind_addr = if config.override_auto_discovery_of_ipv {
            SocketAddr::new(
                config
                    .ipv4_address
                    .parse()
                    .unwrap_or(IpAddr::V4(Ipv4Addr::UNSPECIFIED)),
                config.set_port,
            )
        } else if config.ipv6_enabled {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), config.set_port)
        } else {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), config.set_port)
        };
        let (transport, events) = TransportHandle::bind(bind_addr).await?;
        info!("server listening on {}", transport.local_addr()?);

        let config_path = base_dir
            .join(ServerConfig::CONFIG_FOLDER_NAME)
            .join("config.xml");
        let permissions_path = base_dir
            .join(ServerConfig::CONFIG_FOLDER_NAME)
            .join("permissions.xml");
        let permissions = PermissionManager::new(permissions_path);
        let _ = permissions.load_from_xml();
        permissions.ensure_defaults();
        let _ = permissions.save_to_xml();

        let database = PersistentDatabase::file_backed(
            base_dir
                .join(ServerConfig::CONFIG_FOLDER_NAME)
                .join("database.json"),
        );
        let _ = database.load();

        let p2p_broker = p2p::P2pBroker::default();
        let mut avatar_sync = AvatarSyncSystem::new(
            AvatarSyncConfig {
                default_interval_ms: config.bsrsmillisecond_default_interval.max(1) as u64,
                base_multiplier: config.bsrbase_multiplier as f32,
                increase_rate: config.bsrsincrease_rate,
                high_distance_sq: config.high_quality_distance * config.high_quality_distance,
                medium_distance_sq: config.medium_quality_distance * config.medium_quality_distance,
                low_distance_sq: config.low_quality_distance * config.low_quality_distance,
                enable_bundle_compression: config.enable_avatar_bundle_compression,
                enable_bundle_zstd: config.enable_avatar_bundle_zstd,
                bundle_zstd_delta_bundles: config.avatar_bundle_zstd_delta_bundles,
                bundle_zstd_level: config.avatar_bundle_zstd_level,
                enable_delta_compression: config.enable_avatar_delta_compression,
                delta_keyframe_interval_ms: config.avatar_delta_keyframe_interval_ms.max(1) as u64,
                delta_keyframe_max_interval_ms: config.avatar_delta_keyframe_max_interval_ms.max(0)
                    as u64,
                strip_additional_data_at_low_quality: config.strip_additional_data_at_low_quality,
                bundle_min_messages: config.avatar_bundle_min_messages.max(1) as usize,
                bundle_min_bytes: config.avatar_bundle_min_bytes.max(0) as usize,
                min_receiver_slices: 1,
                max_receiver_slices: 32,
                tick_budget_ms: avatar_sync::DEFAULT_AVATAR_TICK_BUDGET_MS,
                receiver_cycle_budget_ms: avatar_sync::DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS,
                spatial_cull_enabled: false,
                enable_bsr_profiling: config.enable_bsrprofiling,
            }
            .apply_env_tuning(),
        );
        avatar_sync.set_offloaded_pairs(p2p_broker.offloaded_pairs());

        let moderation =
            ModerationLists::file_backed(base_dir.join(ServerConfig::CONFIG_FOLDER_NAME))?;

        let state = Self {
            config: Arc::new(RwLock::new(config.clone())),
            config_path: Arc::new(config_path),
            transport,
            authenticated_peers: Arc::new(DashMap::new()),
            join_broadcast: Arc::new(Mutex::new(JoinBroadcastState::default())),
            pending_identity: Arc::new(DashMap::new()),
            permissions,
            database,
            resources: ResourceState::default(),
            net_ids: NetIdState::default(),
            ownership: OwnershipState::default(),
            content_share: ContentShareState::default(),
            pip: PipState::default(),
            voice_recipients: Arc::new(DashMap::new()),
            message_subscriptions: Arc::new(DashMap::new()),
            uplink_delta_states: Arc::new(DashMap::new()),
            scene_egress: Arc::new(DashMap::new()),
            jiggle_buckets: Arc::new(DashMap::new()),
            error_report_hashes: Arc::new(DashMap::new()),
            avatar_sync,
            p2p_broker,
            moderation,
            global_state: Arc::new(RwLock::new(GlobalState::from(&config))),
            statistics: Statistics::default(),
            pending_leaves: Arc::new(Mutex::new(Vec::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        };
        state
            .avatar_sync
            .spawn_tick_loop(state.transport.clone(), state.shutdown.clone(), {
                let peers = state.authenticated_peers.clone();
                move || peers.iter().map(|entry| *entry.key()).collect()
            });
        spawn_leave_broadcast_loop(state.clone());
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(event_loop(state.clone(), events, shutdown_rx));
        Ok((state, shutdown_tx))
    }

    pub fn player_count(&self) -> usize {
        self.authenticated_peers.len()
    }

    fn scene_egress_allowed(&self, peer_id: PeerId, bytes: u64) -> bool {
        let megabits = self
            .config
            .read()
            .max_scene_relay_megabits_per_second_per_player;
        if megabits <= 0 || bytes == 0 {
            return true;
        }
        const MEGABITS_TO_BYTES: f64 = 125_000.0;
        const BURST_SECONDS: f64 = 2.0;
        let rate = megabits as f64 * MEGABITS_TO_BYTES;
        let now = Instant::now();
        let mut bucket = self
            .scene_egress
            .entry(peer_id)
            .or_insert_with(|| SceneEgressBucket {
                tokens: rate * BURST_SECONDS,
                last_refill: now,
            });
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            bucket.last_refill = now;
            let ceiling = rate * BURST_SECONDS;
            bucket.tokens = (bucket.tokens + rate * elapsed).min(ceiling);
        }
        if bucket.tokens <= 0.0 {
            return false;
        }
        bucket.tokens -= bytes as f64;
        true
    }

    fn jiggle_token_allowed(&self, peer_id: PeerId) -> bool {
        const TOKENS_PER_SECOND: f32 = 8.0;
        const TOKEN_BURST: f32 = 16.0;
        const MAX_TRACKED_PEERS: usize = 4096;
        if self.jiggle_buckets.len() > MAX_TRACKED_PEERS {
            self.jiggle_buckets.clear();
        }
        let now = Instant::now();
        let mut bucket = self
            .jiggle_buckets
            .entry(peer_id)
            .or_insert_with(|| JiggleTokenBucket {
                tokens: TOKEN_BURST,
                last_refill: now,
            });
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f32();
        bucket.last_refill = now;
        bucket.tokens = (bucket.tokens + elapsed * TOKENS_PER_SECOND).min(TOKEN_BURST);
        if bucket.tokens < 1.0 {
            return false;
        }
        bucket.tokens -= 1.0;
        true
    }

    pub fn refresh_runtime_config(&self) {
        let config = self.config.read().clone();
        self.avatar_sync.update_config(
            AvatarSyncConfig {
                default_interval_ms: config.bsrsmillisecond_default_interval.max(1) as u64,
                base_multiplier: config.bsrbase_multiplier as f32,
                increase_rate: config.bsrsincrease_rate,
                high_distance_sq: config.high_quality_distance * config.high_quality_distance,
                medium_distance_sq: config.medium_quality_distance * config.medium_quality_distance,
                low_distance_sq: config.low_quality_distance * config.low_quality_distance,
                enable_bundle_compression: config.enable_avatar_bundle_compression,
                enable_bundle_zstd: config.enable_avatar_bundle_zstd,
                bundle_zstd_delta_bundles: config.avatar_bundle_zstd_delta_bundles,
                bundle_zstd_level: config.avatar_bundle_zstd_level,
                enable_delta_compression: config.enable_avatar_delta_compression,
                delta_keyframe_interval_ms: config.avatar_delta_keyframe_interval_ms.max(1) as u64,
                delta_keyframe_max_interval_ms: config.avatar_delta_keyframe_max_interval_ms.max(0)
                    as u64,
                strip_additional_data_at_low_quality: config.strip_additional_data_at_low_quality,
                bundle_min_messages: config.avatar_bundle_min_messages.max(1) as usize,
                bundle_min_bytes: config.avatar_bundle_min_bytes.max(0) as usize,
                min_receiver_slices: 1,
                max_receiver_slices: 32,
                tick_budget_ms: avatar_sync::DEFAULT_AVATAR_TICK_BUDGET_MS,
                receiver_cycle_budget_ms: avatar_sync::DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS,
                spatial_cull_enabled: false,
                enable_bsr_profiling: config.enable_bsrprofiling,
            }
            .apply_env_tuning(),
        );
    }

    pub fn players_text(&self) -> String {
        let mut text = format!("Connected Player count is {} ", self.player_count());
        for peer in self.authenticated_peers.iter() {
            text.push_str(&format!(
                "Player: {} UUID: {}, ",
                peer.metadata.player_display_name, peer.metadata.player_uuid
            ));
        }
        text
    }

    pub fn status_text(&self) -> String {
        self.status_text_with_detail(false)
    }

    pub fn status_text_with_detail(&self, verbose: bool) -> String {
        let transport = self.transport.stats_snapshot();
        let avatar = self.avatar_sync.stats();
        if !verbose {
            return format!(
                "Server is running and healthy. Players: {} PendingReliable: {} QueuedReliable: {} AppIn: {} AppOut: {} RawIn: {} RawOut: {} AvatarIn: {} AvatarOut: {} ProtocolErrors: {}",
                self.player_count(),
                self.transport.pending_reliable_count(),
                self.transport.queued_reliable_count(),
                self.statistics.inbound_packets.load(Ordering::Relaxed),
                self.statistics.outbound_packets.load(Ordering::Relaxed),
                transport.raw_packets_received,
                transport.raw_packets_sent,
                avatar.inbound_updates,
                avatar.outbound_messages,
                self.statistics.protocol_errors.load(Ordering::Relaxed),
            );
        }
        format!(
            "Server is running and healthy\nPlayers: {}\nReliable: pending={} queued={}\nApp messages: inbound={} outbound={} protocol_errors={}\nRaw UDP: packets_in={} packets_out={} bytes_in={} bytes_out={} would_block={}\nAvatar sync: inbound_updates={} outbound_messages={} outbound_logical_avatar_sends={} outbound_batches={} active_states={} pending_updates={} receiver_slices={}\nAvatar timing: ticks={} avg_tick_us={} smooth_tick_us={} avg_build_us={} avg_flush_us={} max_tick_us={} receiver_cycle_ms={} cycle_budget_ms={} tick_budget_ms={}",
            self.player_count(),
            self.transport.pending_reliable_count(),
            self.transport.queued_reliable_count(),
            self.statistics.inbound_packets.load(Ordering::Relaxed),
            self.statistics.outbound_packets.load(Ordering::Relaxed),
            self.statistics.protocol_errors.load(Ordering::Relaxed),
            transport.raw_packets_received,
            transport.raw_packets_sent,
            transport.raw_bytes_received,
            transport.raw_bytes_sent,
            transport.raw_send_would_block,
            avatar.inbound_updates,
            avatar.outbound_messages,
            avatar.outbound_logical_avatar_sends,
            avatar.outbound_batches,
            avatar.active_states,
            avatar.pending_updates,
            avatar.slice_count,
            avatar.tick_count,
            avatar.avg_tick_micros,
            avatar.smoothed_tick_micros,
            avatar.build_micros.checked_div(avatar.tick_count).unwrap_or(0),
            avatar.flush_micros.checked_div(avatar.tick_count).unwrap_or(0),
            avatar.max_tick_micros,
            avatar.receiver_cycle_micros / 1000,
            avatar.receiver_cycle_budget_micros / 1000,
            avatar.tick_budget_micros / 1000,
        )
    }

    pub async fn shutdown(&self) -> Result<()> {
        if self.shutdown.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.transport.shutdown();
        for peer in self.authenticated_peers.iter() {
            let _ = self
                .transport
                .disconnect(*peer.key(), "Server shutting down")
                .await;
        }
        self.database.shutdown()?;
        Ok(())
    }

    pub async fn broadcast(
        &self,
        channel: u8,
        delivery: DeliveryMethod,
        payload: &[u8],
        except: Option<PeerId>,
    ) {
        for peer in self.authenticated_peers.iter() {
            let target = *peer.key();
            if Some(target) == except {
                continue;
            }
            if let Some(sender) = except {
                if is_p2p_offload_channel(channel) && self.p2p_broker.is_offloaded(sender, target) {
                    continue;
                }
            }
            if self
                .transport
                .send(target, channel, delivery, payload)
                .await
                .is_ok()
            {
                self.statistics
                    .outbound_packets
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

fn is_p2p_offload_channel(channel: u8) -> bool {
    matches!(
        channel,
        channels::VOICE | channels::VOICE_LARGE | channels::SHOUT_VOICE | channels::AVATAR
    )
}

const LEAVE_BROADCAST_INTERVAL: Duration = Duration::from_millis(50);

fn spawn_leave_broadcast_loop(state: ServerState) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(LEAVE_BROADCAST_INTERVAL).await;
            if state.shutdown.load(Ordering::Relaxed) {
                break;
            }
            flush_pending_leaves(&state).await;
        }
    });
}

fn serialize_leave_batch(leaves: &[PeerId]) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(std::mem::size_of_val(leaves));
    for peer in leaves {
        writer.put_u16(*peer);
    }
    writer.into_vec()
}

async fn flush_pending_leaves(state: &ServerState) {
    let mut leaves = {
        let mut pending = state.pending_leaves.lock();
        if pending.is_empty() {
            return;
        }
        std::mem::take(&mut *pending)
    };
    leaves.sort_unstable();
    leaves.dedup();

    let payload = serialize_leave_batch(&leaves);
    let leavers = leaves.iter().copied().collect::<HashSet<_>>();
    let recipients = state
        .authenticated_peers
        .iter()
        .map(|entry| *entry.key())
        .filter(|peer| !leavers.contains(peer))
        .collect::<Vec<_>>();

    for peer in recipients {
        if state
            .transport
            .send(
                peer,
                channels::DISCONNECTION,
                DeliveryMethod::ReliableOrdered,
                &payload,
            )
            .await
            .is_ok()
        {
            state
                .statistics
                .outbound_packets
                .fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn serialize_server_ready(peer_id: PeerId, ready: &ReadyMessage) -> Vec<u8> {
    let message = ServerReadyMessage {
        local_ready_message: ready.clone(),
        player_id_message: basis_protocol::messages::PlayerIdMessage { player_id: peer_id },
    };
    let mut writer = NetWriter::new();
    message.serialize(&mut writer);
    writer.into_vec()
}

fn frame_join_records(records: &[Arc<JoinBroadcastRecord>]) -> Vec<u8> {
    let count = u16::try_from(records.len()).expect("join batch count fits u16");
    let mut payload = Vec::new();
    for record in records {
        payload.extend_from_slice(&record.payload.read());
    }
    let mut writer = NetWriter::with_capacity(payload.len() + 32);
    ServerReadyBatchMessage { count, payload }.serialize(&mut writer);
    writer.into_vec()
}

async fn flush_join_batches(state: &ServerState) {
    let targets = state.join_broadcast.lock().ready_targets();
    let mut framed_by_batch = HashMap::<Vec<(u64, u64)>, Vec<u8>>::new();
    for peer_id in targets {
        if !state.authenticated_peers.contains_key(&peer_id) {
            continue;
        }
        let batches = state.join_broadcast.lock().take_batches(peer_id);
        for records in batches {
            if records.is_empty() {
                continue;
            }
            let key = records
                .iter()
                .map(|record| (record.sequence, record.revision.load(Ordering::Acquire)))
                .collect::<Vec<_>>();
            let framed = framed_by_batch
                .entry(key)
                .or_insert_with(|| frame_join_records(&records));
            if state
                .transport
                .send(
                    peer_id,
                    channels::CREATE_REMOTE_PLAYERS_FOR_NEW_PEER,
                    DeliveryMethod::ReliableOrdered,
                    framed,
                )
                .await
                .is_ok()
            {
                state
                    .statistics
                    .outbound_packets
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

async fn event_loop(
    state: ServerState,
    mut events: mpsc::Receiver<ServerEvent>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let worker_limit = std::thread::available_parallelism()
        .map(|count| (count.get() * 4).clamp(8, 256))
        .unwrap_or(32);
    let workers = Arc::new(Semaphore::new(worker_limit));
    let mut join_flush = tokio::time::interval(JOIN_BATCH_FLUSH_INTERVAL);
    join_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    join_flush.tick().await;
    loop {
        tokio::select! {
            _ = join_flush.tick() => {
                flush_join_batches(&state).await;
            }
            _ = &mut shutdown => {
                break;
            }
            maybe_event = events.recv() => {
                let Some(event) = maybe_event else { break; };
                if is_high_frequency_inline_event(&event) {
                    if let Err(err) = handle_event(&state, event).await {
                        error!("server event failed: {err:#}");
                    }
                    continue;
                }
                let state = state.clone();
                let workers = workers.clone();
                tokio::spawn(async move {
                    let Ok(_permit) = workers.acquire_owned().await else {
                        return;
                    };
                    if let Err(err) = handle_event(&state, event).await {
                        error!("server event failed: {err:#}");
                    }
                });
            }
        }
    }
}

fn is_high_frequency_inline_event(event: &ServerEvent) -> bool {
    matches!(
        event,
        ServerEvent::Message {
            channel: channels::PLAYER_AVATAR_HIGH
                | channels::PLAYER_AVATAR_HIGH_ADDITIONAL
                | channels::PLAYER_AVATAR_VERY_LOW
                | channels::PLAYER_AVATAR_VERY_LOW_ADDITIONAL
                | channels::PLAYER_AVATAR_LOW
                | channels::PLAYER_AVATAR_LOW_ADDITIONAL
                | channels::PLAYER_AVATAR_MEDIUM
                | channels::PLAYER_AVATAR_MEDIUM_ADDITIONAL
                | channels::PLAYER_AVATAR_VERY_LOW_LARGE
                | channels::PLAYER_AVATAR_VERY_LOW_ADDITIONAL_LARGE
                | channels::PLAYER_AVATAR_LOW_LARGE
                | channels::PLAYER_AVATAR_LOW_ADDITIONAL_LARGE
                | channels::PLAYER_AVATAR_MEDIUM_LARGE
                | channels::PLAYER_AVATAR_MEDIUM_ADDITIONAL_LARGE
                | channels::PLAYER_AVATAR_HIGH_LARGE
                | channels::PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE,
            ..
        }
    )
}

async fn handle_event(state: &ServerState, event: ServerEvent) -> Result<()> {
    match event {
        ServerEvent::ConnectionRequest(request) => {
            let remote_addr = request.remote_addr;
            let payload = request.payload.clone();
            handle_connection_request(state, remote_addr, payload, request).await
        }
        ServerEvent::PeerDisconnected { peer, reason } => {
            handle_disconnect(state, peer, reason).await;
            Ok(())
        }
        ServerEvent::Message {
            peer,
            channel,
            delivery,
            payload,
        } => handle_message(state, peer, channel, delivery, payload).await,
        ServerEvent::UnconnectedRequest {
            remote_addr, nonce, ..
        } => {
            let config = state.config.read().clone();
            let response = ServerInfoResponse {
                name: config.server_name,
                motd: config.server_motd,
                online: state.player_count() as u16,
                max: config.peer_limit.clamp(0, u16::MAX as i32) as u16,
                nonce,
            };
            state
                .transport
                .send_server_info(remote_addr, &response)
                .await?;
            Ok(())
        }
        ServerEvent::NatIntroductionRequest {
            remote_addr,
            local_addr,
            token,
        } => {
            if state.config.read().nat_punch_enabled {
                state
                    .p2p_broker
                    .handle_nat_introduction_request(
                        &state.transport,
                        local_addr,
                        remote_addr,
                        token,
                    )
                    .await;
            }
            Ok(())
        }
        ServerEvent::NetworkError(err) => {
            warn!("network error: {err}");
            Ok(())
        }
        ServerEvent::PeerConnected(_) => Ok(()),
    }
}

fn structured_reject_payload(kind: u8, aux0: u16, aux1: u16, message: &str) -> Vec<u8> {
    let mut writer = NetWriter::new();
    writer.put_u32(channels::REJECT_MAGIC);
    writer.put_u8(kind);
    writer.put_u16(aux0);
    writer.put_u16(aux1);
    writer.put_string(message);
    writer.as_slice().to_vec()
}

async fn reject_structured(
    state: &ServerState,
    request: &basis_transport::ConnectionRequest,
    kind: u8,
    aux0: u16,
    aux1: u16,
    message: &str,
) -> Result<()> {
    let payload = structured_reject_payload(kind, aux0, aux1, message);
    state.transport.reject_payload(request, &payload).await?;
    Ok(())
}

async fn handle_connection_request(
    state: &ServerState,
    remote_addr: SocketAddr,
    payload: Bytes,
    request: basis_transport::ConnectionRequest,
) -> Result<()> {
    let config = state.config.read().clone();
    if state.moderation.is_ip_banned(&remote_addr.ip().to_string()) {
        state.transport.reject(&request, "Banned IP").await?;
        return Ok(());
    }
    if config.peer_limit > 0 && state.player_count() >= config.peer_limit as usize {
        reject_structured(
            state,
            &request,
            channels::REJECT_KIND_SERVER_FULL,
            0,
            0,
            &format!(
                "This server is full ({}/{}). Please try again later.",
                state.player_count(),
                config.peer_limit
            ),
        )
        .await?;
        return Ok(());
    }

    let mut reader = NetReader::new(&payload);
    let client_version = match reader.get_u16() {
        Ok(version) => version,
        Err(_) => {
            state
                .transport
                .reject(&request, "Invalid client data.")
                .await?;
            return Ok(());
        }
    };
    if client_version != SERVER_VERSION {
        let guidance = if client_version < SERVER_VERSION {
            "Update your Basis client to match the server."
        } else {
            "This server is running an older Basis build than your client."
        };
        reject_structured(
            state,
            &request,
            channels::REJECT_KIND_VERSION_MISMATCH,
            SERVER_VERSION,
            client_version,
            &format!(
                "This server needs client protocol v{SERVER_VERSION}; your client is v{client_version}. {guidance}"
            ),
        )
        .await?;
        return Ok(());
    }

    let application = match NetworkApplication::try_read(&mut reader) {
        Some(application) => application,
        None => {
            state
                .transport
                .reject(&request, "Invalid client data.")
                .await?;
            return Ok(());
        }
    };
    if !application.matches(&config.company_name, &config.product_name) {
        state
            .transport
            .reject(
                &request,
                &application.unsupported_reason(&config.company_name, &config.product_name),
            )
            .await?;
        return Ok(());
    }

    let auth = match BytesMessage::deserialize(&mut reader) {
        Ok(auth) => auth,
        Err(_) => {
            state
                .transport
                .reject(&request, "Malformed auth payload")
                .await?;
            return Ok(());
        }
    };
    if config.use_auth && !password_matches(&config.password, &auth.data) {
        state
            .transport
            .reject(&request, "Authentication failed, Auth rejected")
            .await?;
        return Ok(());
    }

    let ready = match ReadyMessage::deserialize(&mut reader) {
        Ok(ready) => ready,
        Err(_) => {
            state
                .transport
                .reject(&request, "Malformed ready payload")
                .await?;
            return Ok(());
        }
    };

    if state.global_state.read().disallow_headless
        && is_headless_platform(&ready.player_meta_data_message.player_platform)
    {
        state
            .transport
            .reject(&request, "Headless client disallowed by server.")
            .await?;
        return Ok(());
    }

    if state
        .moderation
        .is_uuid_banned(&ready.player_meta_data_message.player_uuid)
    {
        state.transport.reject(&request, "Banned").await?;
        return Ok(());
    }

    if config.basis_user_restriction_mode == BasisUserRestrictionMode::WhiteList
        && !state
            .moderation
            .is_whitelisted(&ready.player_meta_data_message.player_uuid)
    {
        state
            .transport
            .reject(&request, "You are not on the whitelist.")
            .await?;
        return Ok(());
    }
    if config.basis_user_restriction_mode == BasisUserRestrictionMode::BlackList
        && state
            .moderation
            .is_blacklisted(&ready.player_meta_data_message.player_uuid)
    {
        state
            .transport
            .reject(&request, "You are on the blacklist.")
            .await?;
        return Ok(());
    }

    let peer_id = state.transport.accept(&request).await?;
    if config.use_auth_identity {
        state.pending_identity.insert(peer_id, ready);
        let challenge = uuid::Uuid::new_v4().as_bytes().to_vec();
        let mut writer = NetWriter::new();
        BytesMessage { data: challenge }.serialize(&mut writer);
        state
            .transport
            .send(
                peer_id,
                channels::AUTH_IDENTITY,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
            )
            .await?;
    } else {
        finalize_accept(state, peer_id, ready).await?;
    }
    Ok(())
}

fn password_matches(server_password: &str, auth_bytes: &[u8]) -> bool {
    if server_password.is_empty() {
        return true;
    }
    if auth_bytes.is_empty() {
        return false;
    }
    auth_bytes == server_password.as_bytes()
}

async fn finalize_accept(state: &ServerState, peer_id: PeerId, ready: ReadyMessage) -> Result<()> {
    let uuid = ready.player_meta_data_message.player_uuid.clone();
    let config = state.config.read().clone();
    let metadata = ready.player_meta_data_message.clone();
    let connected = ConnectedPeer {
        id: peer_id,
        metadata: metadata.clone(),
        ready: ready.clone(),
    };
    let existing_players = state
        .join_broadcast
        .lock()
        .register_peer(connected.clone(), serialize_server_ready(peer_id, &ready));
    state.authenticated_peers.insert(peer_id, connected);
    info!("peer connected: {peer_id}");

    let server_meta = ServerMetaDataMessage {
        client_meta_data_message: metadata,
        sync_interval: config.bsrsmillisecond_default_interval,
        base_multiplier: config.bsrbase_multiplier,
        increase_rate: config.bsrsincrease_rate,
        slowest_send_rate: config.bsrslowest_send_rate,
        peer_limit: config.peer_limit,
        allowed_permissions: state.permissions.allowed_rules(&uuid),
        denied_permissions: state.permissions.denied_rules(&uuid),
        uplink_delta_enabled: config.enable_uplink_avatar_delta,
        image_share_egress_megabits_per_second: config.image_share_egress_megabits_per_second,
        image_pickup_range_meters: config.image_pickup_range_meters.max(0.0),
    };
    let mut writer = NetWriter::new();
    server_meta.serialize(&mut writer);
    state
        .transport
        .send(
            peer_id,
            channels::META_DATA,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
        )
        .await?;

    let mut registry_writer = NetWriter::new();
    registry_writer.put_u8(channels::REGISTRY_SUB_SUPPLY);
    core_message_supply().serialize(&mut registry_writer);
    state
        .transport
        .send(
            peer_id,
            channels::REGISTRY_CONTROL,
            DeliveryMethod::ReliableOrdered,
            registry_writer.as_slice(),
        )
        .await?;

    cache_initial_avatar_sync(state, peer_id, &ready);
    send_accept_fanout(state, peer_id, &existing_players).await?;
    Ok(())
}

fn cache_initial_avatar_sync(state: &ServerState, peer_id: PeerId, ready: &ReadyMessage) {
    let quality = ready.local_avatar_sync_message.data_quality_level;
    let has_additional = !ready
        .local_avatar_sync_message
        .additional_avatar_datas
        .is_empty();
    let channel = channels::player_avatar_channel_for_quality(quality, has_additional);
    let mut writer = NetWriter::with_capacity(1 + ready.local_avatar_sync_message.array.len());
    writer.put_u8(0);
    ready
        .local_avatar_sync_message
        .serialize_for_channel(&mut writer, has_additional);
    if let Err(err) =
        state
            .avatar_sync
            .upsert_from_channel_payload(peer_id, channel, writer.as_slice())
    {
        warn!("failed to cache initial avatar sync for peer {peer_id}: {err:#}");
    }
}

async fn send_accept_fanout(
    state: &ServerState,
    peer_id: PeerId,
    existing_players: &[ConnectedPeer],
) -> Result<()> {
    let mut existing_player_packets = Vec::new();
    let mut batch_payload = Vec::new();
    let mut batch_count = 0u16;
    for existing in existing_players {
        let ready = state
            .authenticated_peers
            .get(&existing.id)
            .map(|peer| peer.ready.clone())
            .unwrap_or_else(|| existing.ready.clone());
        let record = serialize_server_ready(existing.id, &ready);

        if batch_count > 0
            && batch_payload.len() + record.len() > ServerReadyBatchMessage::MAX_PAYLOAD_BYTES
        {
            let mut writer = NetWriter::new();
            ServerReadyBatchMessage {
                count: batch_count,
                payload: std::mem::take(&mut batch_payload),
            }
            .serialize(&mut writer);
            existing_player_packets.push((
                channels::CREATE_REMOTE_PLAYERS_FOR_NEW_PEER,
                DeliveryMethod::ReliableOrdered,
                writer.into_vec(),
            ));
            batch_count = 0;
        }

        batch_payload.extend_from_slice(&record);
        batch_count = batch_count.saturating_add(1);
    }
    if batch_count > 0 {
        let mut writer = NetWriter::new();
        ServerReadyBatchMessage {
            count: batch_count,
            payload: batch_payload,
        }
        .serialize(&mut writer);
        existing_player_packets.push((
            channels::CREATE_REMOTE_PLAYERS_FOR_NEW_PEER,
            DeliveryMethod::ReliableOrdered,
            writer.into_vec(),
        ));
    }
    state
        .transport
        .send_many(peer_id, &existing_player_packets)
        .await?;
    state
        .join_broadcast
        .lock()
        .mark_initial_history_queued(peer_id);
    replay_late_join_state(state, peer_id).await;
    Ok(())
}

async fn replay_late_join_state(state: &ServerState, peer_id: PeerId) {
    let net_ids = state
        .net_ids
        .all()
        .into_iter()
        .map(|(name, id)| ServerNetIdMessage {
            net_id_message: NetIdMessage { player_id: name },
            ushort_unique_id_message: UshortUniqueIdMessage {
                unique_id_ushort: id,
            },
        })
        .collect::<Vec<_>>();
    if !net_ids.is_empty() {
        let mut writer = NetWriter::new();
        ServerUniqueIdMessages { messages: net_ids }.serialize(&mut writer);
        state
            .transport
            .send(
                peer_id,
                channels::NET_ID_ASSIGNS,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
            )
            .await
            .unwrap_or_else(|err| warn!("failed to replay net ids to peer {peer_id}: {err:#}"));
    }
    for resource in state.resources.all_resources() {
        let mut resource = resource;
        if resource.load_strategy == 2 {
            resource.load_strategy = 0;
        }
        let mut writer = NetWriter::new();
        resource.serialize(&mut writer);
        state
            .transport
            .send(
                peer_id,
                channels::LOAD_RESOURCE,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
            )
            .await
            .unwrap_or_else(|err| warn!("failed to replay resource to peer {peer_id}: {err:#}"));
    }
    for ownership in state.ownership.all() {
        let mut writer = NetWriter::new();
        ownership.serialize(&mut writer);
        state
            .transport
            .send(
                peer_id,
                channels::GET_CURRENT_OWNER_REQUEST,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
            )
            .await
            .unwrap_or_else(|err| warn!("failed to replay ownership to peer {peer_id}: {err:#}"));
    }
    for sphere in state.content_share.all() {
        let mut writer = NetWriter::new();
        writer.put_u8(channels::CONTENT_SHARE_SUB_DROP);
        sphere.serialize(&mut writer);
        state
            .transport
            .send(
                peer_id,
                channels::CONTENT_SHARE,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
            )
            .await
            .unwrap_or_else(|err| {
                warn!("failed to replay content share sphere to peer {peer_id}: {err:#}")
            });
    }
    for pip in state.pip.all_active() {
        let mut writer = NetWriter::new();
        pip.serialize(&mut writer);
        state
            .transport
            .send(
                peer_id,
                channels::CAMERA_PIP_STATE,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
            )
            .await
            .unwrap_or_else(|err| warn!("failed to replay PIP state to peer {peer_id}: {err:#}"));
    }
    send_initial_admin_state_to_peer(state, peer_id).await;
}

async fn handle_disconnect(state: &ServerState, peer: PeerId, reason: DisconnectReason) {
    state.join_broadcast.lock().remove_peer(peer);
    state.p2p_broker.remove_peer(&state.transport, peer).await;
    state.net_ids.remove_peer(peer);
    let departed_uuid = state
        .authenticated_peers
        .get(&peer)
        .map(|peer_state| peer_state.metadata.player_uuid.clone())
        .unwrap_or_default();
    state.pending_identity.remove(&peer);
    state.voice_recipients.remove(&peer);
    state.message_subscriptions.remove(&peer);
    state.uplink_delta_states.remove(&peer);
    state.scene_egress.remove(&peer);
    state.jiggle_buckets.remove(&peer);
    if !departed_uuid.is_empty() {
        state.error_report_hashes.remove(&departed_uuid);
    }
    state.avatar_sync.remove_player(peer);
    for removed in state.ownership.remove_player(peer) {
        let mut writer = NetWriter::new();
        removed.serialize(&mut writer);
        state
            .broadcast(
                channels::REMOVE_CURRENT_OWNER_REQUEST,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
                Some(peer),
            )
            .await;
    }
    for removed in state.content_share.remove_player(peer) {
        let mut writer = NetWriter::new();
        writer.put_u8(channels::CONTENT_SHARE_SUB_CLEANUP);
        removed.serialize(&mut writer);
        state
            .broadcast(
                channels::CONTENT_SHARE,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
                Some(peer),
            )
            .await;
    }
    for unload in state
        .resources
        .remove_creator_non_persistent(&departed_uuid)
    {
        let mut writer = NetWriter::new();
        unload.serialize(&mut writer);
        state
            .broadcast(
                channels::UNLOAD_RESOURCE,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
                Some(peer),
            )
            .await;
    }
    if let Some(pip_destroy) = state.pip.remove_player(peer) {
        let mut writer = NetWriter::new();
        pip_destroy.serialize(&mut writer);
        state
            .broadcast(
                channels::CAMERA_PIP_STATE,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
                Some(peer),
            )
            .await;
    }
    if state.authenticated_peers.remove(&peer).is_some() {
        info!("peer removed: {peer} ({reason:?})");
        for spawn in state.resources.remove_preload_peer(peer) {
            broadcast_spawn_preloaded(state, spawn).await;
        }
        state.pending_leaves.lock().push(peer);
        if state.authenticated_peers.is_empty() {
            for unload in state.resources.reset_non_persistent() {
                let mut writer = NetWriter::new();
                unload.serialize(&mut writer);
                state
                    .broadcast(
                        channels::UNLOAD_RESOURCE,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                        None,
                    )
                    .await;
            }
            state.net_ids.reset();
            state.ownership.reset();
            state.content_share.reset();
            state.pip.reset();
        }
    }
    state.transport.recycle_peer_id(peer);
}

fn capture_uplink_delta_baseline(state: &ServerState, peer: PeerId, payload: &[u8]) {
    let required = 1 + BitQuality::High.payload_len();
    if payload.len() < required {
        return;
    }
    let sequence = payload[0];
    let baseline = payload[1..required].to_vec();
    state
        .uplink_delta_states
        .entry(peer)
        .and_modify(|entry| {
            entry.baseline = baseline.clone();
            entry.baseline_sequence = sequence;
        })
        .or_insert_with(|| UplinkDeltaState {
            baseline,
            baseline_sequence: sequence,
            ..UplinkDeltaState::empty()
        });
}

async fn send_uplink_keyframe_request(state: &ServerState, peer: PeerId) -> Result<()> {
    state
        .transport
        .send(
            peer,
            channels::DELTA_AVATAR,
            DeliveryMethod::ReliableOrdered,
            &[channels::DELTA_CONTROL_UPLINK_KEYFRAME_REQUEST],
        )
        .await?;
    Ok(())
}

async fn handle_uplink_avatar_delta(
    state: &ServerState,
    peer: PeerId,
    payload: &[u8],
) -> Result<()> {
    if payload.is_empty() {
        return Ok(());
    }
    let header = payload[0];
    if header & channels::DELTA_HEADER_CONTROL_BIT != 0 {
        if header == channels::DELTA_CONTROL_KEYFRAME_REQUEST && payload.len() >= 3 {
            let sender_id = u16::from_le_bytes([payload[1], payload[2]]);
            state.avatar_sync.request_keyframe(sender_id, peer);
        }
        return Ok(());
    }
    if header & channels::DELTA_HEADER_QUALITY_MASK != BitQuality::High as u8 {
        return Ok(());
    }
    if payload.len() < 3 {
        anyhow::bail!("uplink avatar delta missing sequence header");
    }

    let sequence = payload[1];
    let base_sequence = payload[2];
    let now = Instant::now();
    let mut should_nack = false;
    let baseline = {
        let mut entry = state
            .uplink_delta_states
            .entry(peer)
            .or_insert_with(UplinkDeltaState::empty);
        if entry.baseline.len() != BitQuality::High.payload_len()
            || entry.baseline_sequence != base_sequence
        {
            if now.duration_since(entry.last_nack) >= Duration::from_secs(1) {
                entry.last_nack = now;
                should_nack = true;
            }
            None
        } else {
            Some(entry.baseline.clone())
        }
    };

    let Some(baseline) = baseline else {
        if should_nack {
            send_uplink_keyframe_request(state, peer).await?;
        }
        return Ok(());
    };

    let (full_payload, delta_body_len) = apply_delta(&baseline, &payload[3..], BitQuality::High)?;
    let has_additional = header & channels::DELTA_HEADER_ADDITIONAL_DATA != 0
        && !state.global_state.read().additional_avatar_data_lock;
    let additional_start = 3 + delta_body_len;
    let additional_data = if has_additional {
        anyhow::ensure!(
            additional_start <= payload.len(),
            "uplink avatar delta body exceeds payload"
        );
        &payload[additional_start..]
    } else {
        &[]
    };
    let mut full_frame = Vec::with_capacity(1 + full_payload.len() + additional_data.len());
    full_frame.push(sequence);
    full_frame.extend_from_slice(&full_payload);
    full_frame.extend_from_slice(additional_data);
    let channel = if has_additional {
        channels::PLAYER_AVATAR_HIGH_ADDITIONAL
    } else {
        channels::PLAYER_AVATAR_HIGH
    };
    state
        .avatar_sync
        .upsert_from_channel_payload(peer, channel, &full_frame)?;
    Ok(())
}

async fn handle_message(
    state: &ServerState,
    peer: PeerId,
    channel: u8,
    delivery: DeliveryMethod,
    payload: Bytes,
) -> Result<()> {
    state
        .statistics
        .inbound_packets
        .fetch_add(1, Ordering::Relaxed);
    match channel {
        channels::AUTH_IDENTITY => {
            if let Some((_, ready)) = state.pending_identity.remove(&peer) {
                finalize_accept(state, peer, ready).await?;
            }
        }
        channels::PLAYER_AVATAR_HIGH | channels::PLAYER_AVATAR_HIGH_ADDITIONAL => {
            let strip_additional = state.global_state.read().additional_avatar_data_lock
                && channels::channel_has_additional_data(channel);
            let ingest_channel = if strip_additional {
                channel - 1
            } else {
                channel
            };
            let ingest_payload = if strip_additional {
                let end = (1 + BitQuality::High.payload_len()).min(payload.len());
                &payload[..end]
            } else {
                payload.as_ref()
            };
            match state.avatar_sync.upsert_from_channel_payload(
                peer,
                ingest_channel,
                ingest_payload,
            ) {
                Ok(()) => capture_uplink_delta_baseline(state, peer, ingest_payload),
                Err(err) => {
                    state
                        .statistics
                        .protocol_errors
                        .fetch_add(1, Ordering::Relaxed);
                    warn!("invalid avatar update from peer {peer}: {err}");
                }
            }
        }
        channels::PLAYER_AVATAR_VERY_LOW
        | channels::PLAYER_AVATAR_VERY_LOW_ADDITIONAL
        | channels::PLAYER_AVATAR_LOW
        | channels::PLAYER_AVATAR_LOW_ADDITIONAL
        | channels::PLAYER_AVATAR_MEDIUM
        | channels::PLAYER_AVATAR_MEDIUM_ADDITIONAL
        | channels::PLAYER_AVATAR_VERY_LOW_LARGE
        | channels::PLAYER_AVATAR_VERY_LOW_ADDITIONAL_LARGE
        | channels::PLAYER_AVATAR_LOW_LARGE
        | channels::PLAYER_AVATAR_LOW_ADDITIONAL_LARGE
        | channels::PLAYER_AVATAR_MEDIUM_LARGE
        | channels::PLAYER_AVATAR_MEDIUM_ADDITIONAL_LARGE
        | channels::PLAYER_AVATAR_HIGH_LARGE
        | channels::PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE => {
            let strip_additional = state.global_state.read().additional_avatar_data_lock
                && channels::channel_has_additional_data(channel);
            let ingest_channel = if strip_additional {
                channel - 1
            } else {
                channel
            };
            let quality = match channels::quality_from_channel(channel) {
                0 => BitQuality::VeryLow,
                1 => BitQuality::Low,
                2 => BitQuality::Medium,
                _ => BitQuality::High,
            };
            let ingest_payload = if strip_additional {
                let end = (1 + quality.payload_len()).min(payload.len());
                &payload[..end]
            } else {
                payload.as_ref()
            };
            match state.avatar_sync.upsert_from_channel_payload(
                peer,
                ingest_channel,
                ingest_payload,
            ) {
                Ok(()) => {
                    if matches!(
                        channel,
                        channels::PLAYER_AVATAR_HIGH_LARGE
                            | channels::PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE
                    ) {
                        capture_uplink_delta_baseline(state, peer, ingest_payload);
                    }
                }
                Err(err) => {
                    state
                        .statistics
                        .protocol_errors
                        .fetch_add(1, Ordering::Relaxed);
                    warn!("invalid avatar update from peer {peer}: {err}");
                }
            }
        }
        channels::DELTA_AVATAR => {
            if let Err(err) = handle_uplink_avatar_delta(state, peer, &payload).await {
                state
                    .statistics
                    .protocol_errors
                    .fetch_add(1, Ordering::Relaxed);
                warn!("invalid avatar delta from peer {peer}: {err}");
            }
        }
        channels::CHAT => {
            if state.global_state.read().text_chat_locked
                && !peer_has_permission(
                    state,
                    peer,
                    basis_server_permissions::nodes::CHAT_LOCK_BYPASS,
                )
            {
                return Ok(());
            }
            let mut reader = NetReader::new(&payload);
            let chat = ChatMessage::deserialize(&mut reader)?;
            let message = ServerChatMessage {
                player_id: peer,
                chat_message: chat,
            };
            let mut writer = NetWriter::new();
            message.serialize(&mut writer);
            state
                .broadcast(
                    channels::CHAT,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    None,
                )
                .await;
        }
        channels::AVATAR_CHANGE_MESSAGE => {
            let mut reader = NetReader::new(&payload);
            let kind = reader.get_u8()?;
            match kind {
                channels::AVATAR_CHANGE_KIND_FULL => {
                    let avatar = basis_protocol::messages::ClientAvatarChangeMessage::deserialize(
                        &mut reader,
                    )?;
                    if state.global_state.read().avatars_locked
                        && !peer_has_permission(
                            state,
                            peer,
                            basis_server_permissions::nodes::RESOURCE_LOCK_BYPASS_AVATAR,
                        )
                    {
                        return Ok(());
                    }
                    let updated_ready =
                        if let Some(mut peer_state) = state.authenticated_peers.get_mut(&peer) {
                            peer_state.ready.client_avatar_change_message = avatar.clone();
                            Some(peer_state.ready.clone())
                        } else {
                            None
                        };
                    if let Some(ready) = updated_ready {
                        state.join_broadcast.lock().update_peer_ready(peer, ready);
                    }
                    let message = ServerAvatarChangeMessage {
                        player_id: peer,
                        client_avatar_change_message: avatar,
                    };
                    let mut writer = NetWriter::new();
                    writer.put_u8(channels::AVATAR_CHANGE_KIND_FULL);
                    message.serialize(&mut writer);
                    state
                        .broadcast(
                            channels::AVATAR_CHANGE_MESSAGE,
                            DeliveryMethod::ReliableOrdered,
                            writer.as_slice(),
                            Some(peer),
                        )
                        .await;
                }
                channels::AVATAR_CHANGE_KIND_BODY_FIT => {
                    let body_fit = ClientBodyFitMessage::deserialize(&mut reader)?;
                    let updated_ready =
                        if let Some(mut peer_state) = state.authenticated_peers.get_mut(&peer) {
                            peer_state.ready.client_avatar_change_message.arm_scale =
                                body_fit.arm_scale;
                            peer_state.ready.client_avatar_change_message.leg_scale =
                                body_fit.leg_scale;
                            peer_state.ready.client_avatar_change_message.torso_scale =
                                body_fit.torso_scale;
                            Some(peer_state.ready.clone())
                        } else {
                            None
                        };
                    if let Some(ready) = updated_ready {
                        state.join_broadcast.lock().update_peer_ready(peer, ready);
                    }
                    let message = ServerBodyFitMessage {
                        player_id: peer,
                        body_fit,
                    };
                    let mut writer = NetWriter::new();
                    writer.put_u8(channels::AVATAR_CHANGE_KIND_BODY_FIT);
                    message.serialize(&mut writer);
                    state
                        .broadcast(
                            channels::AVATAR_CHANGE_MESSAGE,
                            DeliveryMethod::ReliableOrdered,
                            writer.as_slice(),
                            Some(peer),
                        )
                        .await;
                }
                _ => {
                    state
                        .statistics
                        .protocol_errors
                        .fetch_add(1, Ordering::Relaxed);
                    warn!("unknown avatar change kind {kind} from peer {peer}");
                }
            }
        }
        channels::NET_ID_ASSIGN => {
            let mut reader = NetReader::new(&payload);
            let request = NetIdMessage::deserialize(&mut reader)?;
            if request.player_id.is_empty() {
                return Ok(());
            }
            let max_ids = {
                let configured = state.config.read().max_network_ids_per_player;
                if configured > 0 {
                    configured as usize
                } else {
                    32_768
                }
            };
            let Some((id, existed)) =
                state
                    .net_ids
                    .add_or_find_for_peer(&request.player_id, peer, max_ids)
            else {
                return Ok(());
            };
            let message = ServerNetIdMessage {
                net_id_message: request,
                ushort_unique_id_message: UshortUniqueIdMessage {
                    unique_id_ushort: id,
                },
            };
            let mut writer = NetWriter::new();
            message.serialize(&mut writer);
            if existed {
                state
                    .transport
                    .send(
                        peer,
                        channels::NET_ID_ASSIGN,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                    )
                    .await?;
            } else {
                state
                    .broadcast(
                        channels::NET_ID_ASSIGN,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                        None,
                    )
                    .await;
            }
        }
        channels::LOAD_RESOURCE => {
            let mut reader = NetReader::new(&payload);
            let mut resource = LocalLoadResource::deserialize(&mut reader)?;
            let Some(peer_state) = state.authenticated_peers.get(&peer) else {
                return Ok(());
            };
            resource.uuid_of_creator = peer_state.metadata.player_uuid.clone();
            drop(peer_state);
            if resource_locked(state, &resource, peer) {
                return Ok(());
            }
            let max_loaded_resources = {
                let configured = state.config.read().max_loaded_resources_per_player;
                if configured > 0 {
                    configured as usize
                } else {
                    16_384
                }
            };
            let should_broadcast = if resource.load_strategy == 2 {
                let peers: Vec<u16> = state.authenticated_peers.iter().map(|p| *p.key()).collect();
                state
                    .resources
                    .start_preload(resource.clone(), &peers, max_loaded_resources)
            } else {
                state
                    .resources
                    .load_resource_with_limit(resource.clone(), max_loaded_resources)
            };
            if should_broadcast {
                let mut writer = NetWriter::new();
                resource.serialize(&mut writer);
                state
                    .broadcast(
                        channels::LOAD_RESOURCE,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                        None,
                    )
                    .await;
            }
        }
        channels::UNLOAD_RESOURCE => {
            let mut reader = NetReader::new(&payload);
            let request = UnloadResource::deserialize(&mut reader)?;
            if let Some(resource) = state.resources.unload_resource(&request.loaded_net_id) {
                if resource.is_admin_locked && !has_protection_permission(state, peer) {
                    state.resources.load_resource(resource);
                    return Ok(());
                }
                let mut writer = NetWriter::new();
                request.serialize(&mut writer);
                state
                    .broadcast(
                        channels::UNLOAD_RESOURCE,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                        None,
                    )
                    .await;
            }
        }
        channels::MODIFY_RESOURCE => {
            let mut reader = NetReader::new(&payload);
            let mut request = ModifyResource::deserialize(&mut reader)?;
            let Some(resource) = state
                .resources
                .all_resources()
                .into_iter()
                .find(|resource| resource.loaded_net_id == request.loaded_net_id)
            else {
                return Ok(());
            };
            let is_moderator = has_protection_permission(state, peer);
            let requester_uuid = state
                .authenticated_peers
                .get(&peer)
                .map(|p| p.metadata.player_uuid.clone())
                .unwrap_or_default();
            let target_admin_locked = request.static_admin_locked;
            let target_static = request.static_resource || target_admin_locked;
            let involves_admin_tier = resource.static_admin_locked || target_admin_locked;
            let is_creator =
                !resource.uuid_of_creator.is_empty() && requester_uuid == resource.uuid_of_creator;
            if (!is_creator || involves_admin_tier) && !is_moderator {
                return Ok(());
            }
            request.mode = resource.mode;
            request.static_resource = target_static;
            request.static_admin_locked = target_admin_locked;
            if state.resources.modify_resource(&request) {
                let mut writer = NetWriter::new();
                request.serialize(&mut writer);
                state
                    .broadcast(
                        channels::MODIFY_RESOURCE,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                        None,
                    )
                    .await;
            }
        }
        channels::PRELOAD_READY => {
            let mut reader = NetReader::new(&payload);
            let ready = PreloadReadyMessage::deserialize(&mut reader)?;
            if let Some(spawn) = state.resources.mark_preload_ready(peer, ready) {
                if let Some(resource) =
                    state
                        .resources
                        .all_resources()
                        .into_iter()
                        .find(|resource| {
                            resource.loaded_net_id == spawn.loaded_net_id && resource.mode == 1
                        })
                {
                    let _ = resource;
                    for unload in state.resources.all_scene_unloads() {
                        let mut writer = NetWriter::new();
                        unload.serialize(&mut writer);
                        state
                            .broadcast(
                                channels::UNLOAD_RESOURCE,
                                DeliveryMethod::ReliableOrdered,
                                writer.as_slice(),
                                None,
                            )
                            .await;
                    }
                }
                broadcast_spawn_preloaded(state, spawn).await;
            }
        }
        channels::GET_CURRENT_OWNER_REQUEST => {
            let mut reader = NetReader::new(&payload);
            let request = OwnershipTransferMessage::deserialize(&mut reader)?;
            let current_owner = state
                .ownership
                .request_new_or_existing(&request.ownership_id, request.player_id);
            let response = OwnershipTransferMessage {
                player_id: current_owner,
                ownership_id: request.ownership_id,
            };
            let mut writer = NetWriter::new();
            response.serialize(&mut writer);
            state
                .transport
                .send(
                    peer,
                    channels::GET_CURRENT_OWNER_REQUEST,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                )
                .await?;
        }
        channels::CHANGE_CURRENT_OWNER_REQUEST => {
            let mut reader = NetReader::new(&payload);
            let request = OwnershipTransferMessage::deserialize(&mut reader)?;
            let owner = state
                .ownership
                .switch_ownership(&request.ownership_id, peer);
            let response = OwnershipTransferMessage {
                player_id: owner,
                ownership_id: request.ownership_id,
            };
            let mut writer = NetWriter::new();
            response.serialize(&mut writer);
            state
                .broadcast(
                    channels::CHANGE_CURRENT_OWNER_REQUEST,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    None,
                )
                .await;
        }
        channels::REMOVE_CURRENT_OWNER_REQUEST => {
            let mut reader = NetReader::new(&payload);
            let request = OwnershipTransferMessage::deserialize(&mut reader)?;
            if state
                .ownership
                .remove_if_owner(&request.ownership_id, request.player_id)
            {
                let mut writer = NetWriter::new();
                request.serialize(&mut writer);
                state
                    .broadcast(
                        channels::REMOVE_CURRENT_OWNER_REQUEST,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                        None,
                    )
                    .await;
            }
        }
        channels::CONTENT_SHARE => {
            let mut reader = NetReader::new(&payload);
            match reader.get_u8()? {
                channels::CONTENT_SHARE_SUB_DROP => {
                    let request = ContentShareMessage::deserialize(&mut reader)?;
                    if content_locked(state, request.content_type, peer) {
                        return Ok(());
                    }
                    let Some(peer_state) = state.authenticated_peers.get(&peer) else {
                        return Ok(());
                    };
                    let max_spheres = {
                        let configured = state.config.read().max_content_spheres_per_player;
                        if configured < 1 {
                            32usize
                        } else {
                            configured.min(4096) as usize
                        }
                    };
                    let Some(server_message) = state.content_share.add_with_limit(
                        peer,
                        peer_state.metadata.player_uuid.clone(),
                        peer_state.metadata.player_display_name.clone(),
                        request,
                        max_spheres,
                    ) else {
                        return Ok(());
                    };
                    drop(peer_state);
                    let mut writer = NetWriter::new();
                    writer.put_u8(channels::CONTENT_SHARE_SUB_DROP);
                    server_message.serialize(&mut writer);
                    state
                        .broadcast(
                            channels::CONTENT_SHARE,
                            DeliveryMethod::ReliableOrdered,
                            writer.as_slice(),
                            None,
                        )
                        .await;
                }
                channels::CONTENT_SHARE_SUB_CLEANUP => {
                    let request = ContentShareCleanupMessage::deserialize(&mut reader)?;
                    if let Some(server_message) = state.content_share.remove(peer, request) {
                        let mut writer = NetWriter::new();
                        writer.put_u8(channels::CONTENT_SHARE_SUB_CLEANUP);
                        server_message.serialize(&mut writer);
                        state
                            .broadcast(
                                channels::CONTENT_SHARE,
                                DeliveryMethod::ReliableOrdered,
                                writer.as_slice(),
                                None,
                            )
                            .await;
                    }
                }
                _ => {}
            }
        }
        channels::CAMERA_PIP_STATE => {
            let mut reader = NetReader::new(&payload);
            let request = ClientCameraPipStateMessage::deserialize(&mut reader)?;
            let response = state.pip.state_change(peer, request);
            let mut writer = NetWriter::new();
            response.serialize(&mut writer);
            state
                .broadcast(
                    channels::CAMERA_PIP_STATE,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
        channels::CAMERA_PIP_POSITION => {
            let mut reader = NetReader::new(&payload);
            let request = ClientCameraPipPositionMessage::deserialize(&mut reader)?;
            if let Some(response) = state.pip.position_update(peer, request) {
                let mut writer = NetWriter::new();
                response.serialize(&mut writer);
                state
                    .broadcast(
                        channels::CAMERA_PIP_POSITION,
                        DeliveryMethod::Sequenced,
                        writer.as_slice(),
                        Some(peer),
                    )
                    .await;
            }
        }
        channels::ADMIN => {
            handle_admin_message(state, peer, &payload).await?;
        }
        channels::P2P => {
            let peers = state.authenticated_peers.clone();
            let direct_connect_allowed = !state.global_state.read().direct_connect_locked
                || peer_has_permission(
                    state,
                    peer,
                    basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
                );
            state
                .p2p_broker
                .handle_signal(
                    &state.transport,
                    peer,
                    &payload,
                    direct_connect_allowed,
                    move |id| peers.contains_key(&id),
                )
                .await;
        }
        channels::SERVER_STATISTICS => {
            handle_statistics_request(state, peer, &payload).await?;
        }
        channels::AUDIO_RECIPIENTS => {
            update_voice_recipients(state, peer, &payload, false, false).await?;
        }
        channels::AUDIO_RECIPIENTS_LARGE => {
            update_voice_recipients(state, peer, &payload, true, false).await?;
        }
        channels::AUDIO_RECIPIENTS_INVERTED => {
            update_voice_recipients(state, peer, &payload, false, true).await?;
        }
        channels::AUDIO_RECIPIENTS_INVERTED_LARGE => {
            update_voice_recipients(state, peer, &payload, true, true).await?;
        }
        channels::AUDIO_RECIPIENTS_BITFIELD => {
            update_voice_recipients_bitfield(state, peer, &payload);
        }
        channels::VOICE | channels::VOICE_LARGE => {
            if !state.global_state.read().voice_chat_locked
                || peer_has_permission(
                    state,
                    peer,
                    basis_server_permissions::nodes::VOICE_LOCK_BYPASS,
                )
            {
                relay_voice_message(state, peer, &payload).await;
            }
        }
        channels::SHOUT_VOICE => {
            if !state.global_state.read().voice_chat_locked
                || peer_has_permission(
                    state,
                    peer,
                    basis_server_permissions::nodes::VOICE_LOCK_BYPASS,
                )
            {
                relay_shout_voice_message(state, peer, &payload).await;
            }
        }
        channels::AVATAR => {
            relay_avatar_generic(state, peer, delivery, channels::AVATAR, &payload).await?;
        }
        channels::DIRECT_AVATAR_SERVER => {
            relay_avatar_generic(
                state,
                peer,
                delivery,
                channels::DIRECT_AVATAR_SERVER,
                &payload,
            )
            .await?;
        }
        channels::SCENE => {
            relay_scene_generic(state, peer, delivery, channels::SCENE, &payload).await?;
        }
        channels::DIRECT_SCENE_SERVER => {
            relay_scene_generic(
                state,
                peer,
                delivery,
                channels::DIRECT_SCENE_SERVER,
                &payload,
            )
            .await?;
        }
        channels::EVENTS => {
            relay_event(state, peer, &payload).await?;
        }
        channels::REGISTRY_CONTROL => {
            let mut reader = NetReader::new(&payload);
            if reader.get_u8()? == channels::REGISTRY_SUB_SUBSCRIBE {
                let subscription = BasisMessageSubscribe::deserialize(&mut reader)?;
                state
                    .message_subscriptions
                    .insert(peer, subscription.ids.into_iter().collect());
            }
        }
        channels::SERVER_BOUND => {
            state
                .broadcast(channel, delivery, &payload, Some(peer))
                .await;
        }
        _ => {
            state
                .statistics
                .protocol_errors
                .fetch_add(1, Ordering::Relaxed);
            warn!("unknown channel {channel} from peer {peer}");
        }
    }
    Ok(())
}

async fn relay_avatar_generic(
    state: &ServerState,
    peer: PeerId,
    delivery: DeliveryMethod,
    broadcast_channel: u8,
    payload: &[u8],
) -> Result<()> {
    let mut reader = NetReader::new(payload);
    let avatar = AvatarDataMessage::deserialize(&mut reader)?;
    let message = ServerAvatarDataMessage {
        player_id: peer,
        avatar_data_message: RemoteAvatarDataMessage {
            player_id: avatar.player_id,
            avatar_link_index: avatar.avatar_link_index,
            message_index: avatar.message_index,
            payload: avatar.payload,
        },
    };
    let mut writer = NetWriter::new();
    message.serialize(&mut writer);
    send_to_recipients_or_broadcast(
        state,
        peer,
        delivery,
        broadcast_channel,
        writer.as_slice(),
        &avatar.recipients,
    )
    .await
}

async fn relay_scene_generic(
    state: &ServerState,
    peer: PeerId,
    delivery: DeliveryMethod,
    broadcast_channel: u8,
    payload: &[u8],
) -> Result<()> {
    let mut reader = NetReader::new(payload);
    let scene = SceneDataMessage::deserialize(&mut reader)?;
    let is_image_traffic =
        state.net_ids.find("BasisImagePickupManager") == Some(scene.message_index);
    if !is_image_traffic {
        let fan_out = if scene.recipients.is_empty() {
            state.authenticated_peers.len().saturating_sub(1)
        } else {
            scene.recipients.len()
        };
        let egress_bytes = scene.payload.len().saturating_mul(fan_out.max(1)) as u64;
        if !state.scene_egress_allowed(peer, egress_bytes) {
            return Ok(());
        }
    }
    let message = ServerSceneDataMessage {
        player_id: peer,
        scene_data_message: RemoteSceneDataMessage {
            message_index: scene.message_index,
            payload: scene.payload,
        },
    };
    let mut writer = NetWriter::new();
    message.serialize(&mut writer);
    send_to_recipients_or_broadcast(
        state,
        peer,
        delivery,
        broadcast_channel,
        writer.as_slice(),
        &scene.recipients,
    )
    .await
}

async fn send_to_recipients_or_broadcast(
    state: &ServerState,
    peer: PeerId,
    delivery: DeliveryMethod,
    channel: u8,
    payload: &[u8],
    recipients: &[PeerId],
) -> Result<()> {
    if recipients.is_empty() {
        state
            .broadcast(channel, delivery, payload, Some(peer))
            .await;
        return Ok(());
    }
    for recipient in recipients {
        if *recipient == peer
            || !state.authenticated_peers.contains_key(recipient)
            || (is_p2p_offload_channel(channel) && state.p2p_broker.is_offloaded(peer, *recipient))
        {
            continue;
        }
        let _ = state
            .transport
            .send(*recipient, channel, delivery, payload)
            .await;
    }
    Ok(())
}

async fn relay_event(state: &ServerState, peer: PeerId, payload: &[u8]) -> Result<()> {
    let Some((&event_type, rest)) = payload.split_first() else {
        return Ok(());
    };
    let mut writer = NetWriter::new();
    writer.put_u8(event_type);
    match event_type {
        channels::EVENT_TYPE_CAMERA_SHUTTER_SOUND => {
            CameraShutterSoundMessage { player_id: peer }.serialize(&mut writer);
            state
                .broadcast(
                    channels::EVENTS,
                    DeliveryMethod::Sequenced,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
        channels::EVENT_TYPE_CAMERA_COUNTDOWN => {
            let mut reader = NetReader::new(rest);
            let countdown = ClientCameraCountdownMessage::deserialize(&mut reader)?;
            CameraCountdownMessage {
                player_id: peer,
                seconds: countdown.seconds,
            }
            .serialize(&mut writer);
            state
                .broadcast(
                    channels::EVENTS,
                    DeliveryMethod::Sequenced,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
        channels::EVENT_TYPE_PLAYER_TEMP_BLOCK => {
            if rest.len() < 3 {
                return Ok(());
            }
            let target = u16::from_le_bytes([rest[0], rest[1]]);
            if !state.authenticated_peers.contains_key(&target) {
                return Ok(());
            }
            writer.put_u16(peer);
            writer.put_bool(rest[2] != 0);
            state
                .transport
                .send(
                    target,
                    channels::EVENTS,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                )
                .await?;
        }
        channels::EVENT_TYPE_AVATAR_RATE_CHANGE => {
            if rest.len() < 2 {
                return Ok(());
            }
            writer.put_u16(peer);
            writer.put_u16(u16::from_le_bytes([rest[0], rest[1]]));
            state
                .broadcast(
                    channels::EVENTS,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
        channels::EVENT_TYPE_TALK_MODE_CHANGED | channels::EVENT_TYPE_MUTE_STATE_CHANGED => {
            let Some(&value) = rest.first() else {
                return Ok(());
            };
            writer.put_u16(peer);
            writer.put_u8(value);
            state
                .broadcast(
                    channels::EVENTS,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
        channels::EVENT_TYPE_PLAYER_CHAT_TYPING => {
            let Some(&typing) = rest.first() else {
                return Ok(());
            };
            if state.global_state.read().text_chat_locked
                && !peer_has_permission(
                    state,
                    peer,
                    basis_server_permissions::nodes::CHAT_LOCK_BYPASS,
                )
            {
                return Ok(());
            }
            writer.put_u16(peer);
            writer.put_bool(typing != 0);
            state
                .broadcast(
                    channels::EVENTS,
                    DeliveryMethod::Sequenced,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
        channels::EVENT_TYPE_ERROR_REPORT => {
            handle_error_report_event(state, peer, rest).await?;
        }
        channels::EVENT_TYPE_VOICE_RECORD_REQUEST | channels::EVENT_TYPE_VOICE_RECORD_CONSENT => {
            let has_state = event_type == channels::EVENT_TYPE_VOICE_RECORD_CONSENT;
            let needed = if has_state { 4 } else { 3 };
            if rest.len() < needed {
                return Ok(());
            }
            let target = u16::from_le_bytes([rest[0], rest[1]]);
            if !state.authenticated_peers.contains_key(&target) {
                return Ok(());
            }
            writer.put_u16(peer);
            let mut offset = 2usize;
            if has_state {
                writer.put_u8(rest[offset]);
                offset += 1;
            }
            writer.put_u8(rest[offset]);
            state
                .transport
                .send(
                    target,
                    channels::EVENTS,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                )
                .await?;
        }
        channels::EVENT_TYPE_JIGGLE_GRAB => {
            handle_jiggle_grab_event(state, peer, rest).await?;
        }
        _ => {
            state
                .statistics
                .protocol_errors
                .fetch_add(1, Ordering::Relaxed);
            warn!("unknown event type {event_type} from peer {peer}");
        }
    }
    Ok(())
}

async fn handle_jiggle_grab_event(state: &ServerState, peer: PeerId, payload: &[u8]) -> Result<()> {
    let mut reader = NetReader::new(payload);
    let op = reader.get_u8()?;
    if !state.jiggle_token_allowed(peer) {
        return Ok(());
    }
    let mut writer = NetWriter::new();
    writer.put_u8(channels::EVENT_TYPE_JIGGLE_GRAB);
    writer.put_u8(op);
    writer.put_u16(peer);
    match op {
        channels::JIGGLE_GRAB_OP_START => {
            let target_id = reader.get_u16()?;
            let rig_index = reader.get_u8()?;
            let point_index = reader.get_u16()?;
            let hand = reader.get_u8()?;
            let bone_name_hash = reader.get_u32()?;
            let offset_x = reader.get_u16()?;
            let offset_y = reader.get_u16()?;
            let offset_z = reader.get_u16()?;
            writer.put_u16(target_id);
            writer.put_u8(rig_index);
            writer.put_u16(point_index);
            writer.put_u8(hand);
            writer.put_u32(bone_name_hash);
            writer.put_u16(offset_x);
            writer.put_u16(offset_y);
            writer.put_u16(offset_z);

            let Some(target_position) = state.avatar_sync.player_position(target_id) else {
                state
                    .broadcast(
                        channels::EVENTS,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                        Some(peer),
                    )
                    .await;
                return Ok(());
            };
            const RELEVANCE_DISTANCE_SQ: f32 = 64.0 * 64.0;
            for recipient in state.authenticated_peers.iter().map(|entry| *entry.key()) {
                if recipient == peer {
                    continue;
                }
                if recipient != target_id {
                    if let Some(position) = state.avatar_sync.player_position(recipient) {
                        let dx = position[0] - target_position[0];
                        let dy = position[1] - target_position[1];
                        let dz = position[2] - target_position[2];
                        if dx * dx + dy * dy + dz * dz > RELEVANCE_DISTANCE_SQ {
                            continue;
                        }
                    }
                }
                let _ = state
                    .transport
                    .send(
                        recipient,
                        channels::EVENTS,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                    )
                    .await;
            }
        }
        channels::JIGGLE_GRAB_OP_STOP => {
            writer.put_u16(reader.get_u16()?);
            writer.put_u8(reader.get_u8()?);
            writer.put_u16(reader.get_u16()?);
            state
                .broadcast(
                    channels::EVENTS,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
        channels::JIGGLE_GRAB_OP_DENY => {
            writer.put_u16(reader.get_u16()?);
            state
                .broadcast(
                    channels::EVENTS,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
        _ => {}
    }
    Ok(())
}

async fn handle_error_report_event(
    state: &ServerState,
    peer: PeerId,
    payload: &[u8],
) -> Result<()> {
    let config = state.config.read().clone();
    if !config.crash_reporting_enabled || !config.has_file_support {
        return Ok(());
    }
    let mut reader = NetReader::new(payload);
    let severity = reader.get_u8()?;
    let compressed = reader.get_bytes_with_length()?;
    let parts = decompress_permission_extras(compressed, 3);
    if parts.len() < 3 {
        return Ok(());
    }
    let Some(peer_state) = state.authenticated_peers.get(&peer) else {
        return Ok(());
    };
    let uuid = if peer_state.metadata.player_uuid.is_empty() {
        "unknown".to_string()
    } else {
        peer_state.metadata.player_uuid.clone()
    };
    let display_name = peer_state.metadata.player_display_name.clone();
    let platform = peer_state.metadata.player_platform.clone();
    drop(peer_state);

    let system = parts[0].clone();
    let message = truncate_chars(&parts[1], 2000);
    let stack = truncate_chars(&parts[2], 12_000);
    if state.error_report_hashes.len() >= 4096 && !state.error_report_hashes.contains_key(&uuid) {
        state.error_report_hashes.clear();
    }
    let hash = error_report_hash(severity, &system, &message, &stack);
    {
        let mut seen = state.error_report_hashes.entry(uuid.clone()).or_default();
        if seen.len() >= 256 || !seen.insert(hash) {
            return Ok(());
        }
    }

    let base_dir = state
        .config_path
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let file_name = format!("{}.jsonl", sanitize_log_file_name(&uuid));
    let time_unix_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let severity_name = match severity {
        1 => "exception",
        2 => "crash",
        _ => "error",
    };
    let line = serde_json::json!({
        "timeUnixMs": time_unix_ms,
        "uuid": uuid,
        "displayName": display_name,
        "platform": platform,
        "severity": severity_name,
        "system": system,
        "message": message,
        "stack": stack,
    })
    .to_string();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let dir = base_dir.join("CrashReports");
        fs::create_dir_all(&dir)?;
        let path = dir.join(file_name);
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        Ok(())
    })
    .await
    .context("joining crash-report writer")??;
    Ok(())
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }
    value.chars().take(max_chars).collect()
}

fn error_report_hash(severity: u8, system: &str, message: &str, stack: &str) -> u64 {
    fn mix_byte(mut hash: u64, value: u8) -> u64 {
        hash ^= value as u64;
        hash.wrapping_mul(1_099_511_628_211)
    }
    fn mix_string(mut hash: u64, value: &str) -> u64 {
        hash = mix_byte(hash, 0x1f);
        for unit in value.encode_utf16() {
            hash = mix_byte(hash, unit as u8);
            hash = mix_byte(hash, (unit >> 8) as u8);
        }
        hash
    }
    let first_stack_line = stack.split('\n').next().unwrap_or_default();
    let mut hash = 14_695_981_039_346_656_037u64;
    hash = mix_byte(hash, severity);
    hash = mix_string(hash, system);
    hash = mix_string(hash, message);
    mix_string(hash, first_stack_line)
}

fn sanitize_log_file_name(value: &str) -> String {
    if value.is_empty() {
        return "unknown".to_string();
    }
    value
        .chars()
        .map(|ch| {
            if matches!(ch, '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*') || ch.is_control()
            {
                '_'
            } else {
                ch
            }
        })
        .collect()
}

async fn handle_statistics_request(
    state: &ServerState,
    peer: PeerId,
    payload: &[u8],
) -> Result<()> {
    let mut reader = NetReader::new(payload);
    let enabled = reader.get_bool().unwrap_or(false);
    if !enabled {
        return Ok(());
    }
    let text = state.status_text_with_detail(true).into_bytes();
    let message = ServerStatisticMessage { data: text };
    let mut writer = NetWriter::new();
    message.serialize(&mut writer);
    state
        .transport
        .send(
            peer,
            channels::SERVER_STATISTICS,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
        )
        .await?;
    Ok(())
}

async fn update_voice_recipients(
    state: &ServerState,
    peer: PeerId,
    payload: &[u8],
    large_count: bool,
    inverted: bool,
) -> Result<()> {
    let mut reader = NetReader::new(payload);
    let message = VoiceReceiversMessage::deserialize(&mut reader, large_count)?;
    if inverted {
        let excluded = message
            .users
            .into_iter()
            .collect::<std::collections::HashSet<_>>();
        let recipients = state
            .authenticated_peers
            .iter()
            .filter_map(|entry| {
                let id = *entry.key();
                (id != peer && !excluded.contains(&id)).then_some(id)
            })
            .collect::<Vec<_>>();
        state.voice_recipients.insert(peer, recipients);
    } else {
        let recipients = message
            .users
            .into_iter()
            .filter(|id| *id != peer && state.authenticated_peers.contains_key(id))
            .collect::<Vec<_>>();
        state.voice_recipients.insert(peer, recipients);
    }
    Ok(())
}

fn update_voice_recipients_bitfield(state: &ServerState, peer: PeerId, payload: &[u8]) {
    if payload.len() < 2 {
        return;
    }
    let byte_count = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    if payload.len() < 2 + byte_count {
        return;
    }
    let mut recipients = Vec::new();
    for (byte_index, byte) in payload[2..2 + byte_count].iter().enumerate() {
        if *byte == 0 {
            continue;
        }
        let base_id = byte_index * 8;
        for bit in 0..8 {
            if (byte & (1 << bit)) == 0 {
                continue;
            }
            let id = (base_id + bit) as PeerId;
            if id != peer && state.authenticated_peers.contains_key(&id) {
                recipients.push(id);
            }
        }
    }
    state.voice_recipients.insert(peer, recipients);
}

async fn relay_voice_message(state: &ServerState, peer: PeerId, payload: &[u8]) {
    let Some(recipients) = state.voice_recipients.get(&peer).map(|entry| entry.clone()) else {
        return;
    };
    let large_id = peer > u8::MAX as u16;
    let channel = if large_id {
        channels::VOICE_LARGE
    } else {
        channels::VOICE
    };
    let message = ServerAudioSegmentMessage {
        player_id: peer,
        audio_segment: payload.to_vec(),
    };
    let mut writer = NetWriter::new();
    message.serialize_with_id_size(&mut writer, large_id);
    for recipient in recipients {
        if state.p2p_broker.is_offloaded(peer, recipient) {
            continue;
        }
        let _ = state
            .transport
            .send(
                recipient,
                channel,
                DeliveryMethod::Unreliable,
                writer.as_slice(),
            )
            .await;
    }
}

async fn relay_shout_voice_message(state: &ServerState, peer: PeerId, payload: &[u8]) {
    let large_id = peer > u8::MAX as u16;
    let channel = if large_id {
        channels::VOICE_LARGE
    } else {
        channels::SHOUT_VOICE
    };
    let message = ServerAudioSegmentMessage {
        player_id: peer,
        audio_segment: payload.to_vec(),
    };
    let mut writer = NetWriter::new();
    message.serialize_with_id_size(&mut writer, large_id);
    state
        .broadcast(
            channel,
            DeliveryMethod::Unreliable,
            writer.as_slice(),
            Some(peer),
        )
        .await;
}

fn content_locked(state: &ServerState, content_type: ContentShareType, peer: PeerId) -> bool {
    let locks = state.global_state.read().clone();
    let Some(peer_state) = state.authenticated_peers.get(&peer) else {
        return true;
    };
    let uuid = &peer_state.metadata.player_uuid;
    match content_type {
        ContentShareType::Avatar => {
            locks.avatars_locked
                && !state.permissions.has(
                    uuid,
                    basis_server_permissions::nodes::RESOURCE_LOCK_BYPASS_AVATAR,
                )
        }
        ContentShareType::Prop => {
            locks.props_locked
                && !state.permissions.has(
                    uuid,
                    basis_server_permissions::nodes::RESOURCE_LOCK_BYPASS_PROP,
                )
        }
        ContentShareType::World => {
            locks.worlds_locked
                && !state.permissions.has(
                    uuid,
                    basis_server_permissions::nodes::RESOURCE_LOCK_BYPASS_WORLD,
                )
        }
        ContentShareType::Server => {
            locks.servers_locked
                && !state.permissions.has(
                    uuid,
                    basis_server_permissions::nodes::RESOURCE_LOCK_BYPASS_SERVER,
                )
        }
    }
}

fn resource_locked(state: &ServerState, resource: &LocalLoadResource, peer: PeerId) -> bool {
    let locks = state.global_state.read().clone();
    let Some(peer_state) = state.authenticated_peers.get(&peer) else {
        return true;
    };
    let uuid = &peer_state.metadata.player_uuid;
    match resource.mode {
        0 => {
            locks.props_locked
                && !state.permissions.has(
                    uuid,
                    basis_server_permissions::nodes::RESOURCE_LOCK_BYPASS_PROP,
                )
        }
        1 => {
            locks.worlds_locked
                && !state.permissions.has(
                    uuid,
                    basis_server_permissions::nodes::RESOURCE_LOCK_BYPASS_WORLD,
                )
        }
        _ => true,
    }
}

fn peer_has_permission(state: &ServerState, peer: PeerId, node: &str) -> bool {
    let Some(peer_state) = state.authenticated_peers.get(&peer) else {
        return false;
    };
    state
        .permissions
        .has(&peer_state.metadata.player_uuid, node)
}

fn has_protection_permission(state: &ServerState, peer: PeerId) -> bool {
    peer_has_permission(state, peer, basis_server_permissions::nodes::PROTECTION)
}

async fn broadcast_spawn_preloaded(state: &ServerState, spawn: SpawnPreloadedMessage) {
    let mut writer = NetWriter::new();
    spawn.serialize(&mut writer);
    state
        .broadcast(
            channels::SPAWN_PRELOADED,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
            None,
        )
        .await;
}

async fn handle_admin_message(state: &ServerState, peer: PeerId, payload: &[u8]) -> Result<()> {
    let mut reader = NetReader::new(payload);
    let request = AdminRequest::deserialize(&mut reader)?;
    if let Some(required_node) = admin_mode_required_permission(request.mode) {
        if !peer_has_permission(state, peer, required_node) {
            return Ok(());
        }
    }
    match request.mode {
        AdminRequestMode::GlobalToggleAvatars => {
            toggle_simple_lock(state, |s| &mut s.avatars_locked, |c| &mut c.avatars_locked).await;
        }
        AdminRequestMode::GlobalToggleProps => {
            toggle_simple_lock(state, |s| &mut s.props_locked, |c| &mut c.props_locked).await;
        }
        AdminRequestMode::GlobalToggleWorlds => {
            toggle_simple_lock(state, |s| &mut s.worlds_locked, |c| &mut c.worlds_locked).await;
        }
        AdminRequestMode::GlobalToggleServers => {
            toggle_simple_lock(state, |s| &mut s.servers_locked, |c| &mut c.servers_locked).await;
        }
        AdminRequestMode::GlobalToggleThirdPerson => {
            toggle_simple_lock(
                state,
                |s| &mut s.third_person_disabled,
                |c| &mut c.third_person_disabled,
            )
            .await;
        }
        AdminRequestMode::GlobalToggleAdditionalAvatarDataLock => {
            let value = {
                let mut locks = state.global_state.write();
                locks.additional_avatar_data_lock ^= true;
                locks.additional_avatar_data_lock
            };
            state.config.write().additional_avatar_data_lock = value;
            broadcast_lock_state(state).await;
        }
        AdminRequestMode::SetGlobalCameraPolicy => {
            let value = reader.get_u8().unwrap_or(0);
            state.global_state.write().camera_metadata_disallow_mask = value;
            state.config.write().camera_metadata_disallow_mask = value;
            broadcast_lock_state(state).await;
        }
        AdminRequestMode::GlobalGetCrashReportState => {
            let value = state.config.read().crash_reporting_enabled;
            send_admin_payload_to_peer(
                state,
                peer,
                encode_bool_admin_state_payload(AdminRequestMode::GlobalGetCrashReportState, value),
            )
            .await?;
        }
        AdminRequestMode::SetGlobalCrashReporting => {
            let value = reader.get_bool().unwrap_or(true);
            state.config.write().crash_reporting_enabled = value;
            broadcast_admin_payload(
                state,
                encode_bool_admin_state_payload(AdminRequestMode::GlobalGetCrashReportState, value),
            )
            .await;
        }
        AdminRequestMode::GlobalGetAudioRangeLimits => {
            let config = state.config.read().clone();
            send_admin_payload_to_peer(
                state,
                peer,
                encode_f32_pair_admin_state_payload(
                    AdminRequestMode::GlobalGetAudioRangeLimits,
                    config.max_microphone_range_meters,
                    config.max_hearing_range_meters,
                ),
            )
            .await?;
        }
        AdminRequestMode::SetGlobalAudioRangeLimits => {
            let microphone = sanitize_positive_range(reader.get_f32().unwrap_or(25.0), 25.0);
            let hearing = sanitize_positive_range(reader.get_f32().unwrap_or(25.0), 25.0);
            {
                let mut config = state.config.write();
                config.max_microphone_range_meters = microphone;
                config.max_hearing_range_meters = hearing;
            }
            broadcast_admin_payload(
                state,
                encode_f32_pair_admin_state_payload(
                    AdminRequestMode::GlobalGetAudioRangeLimits,
                    microphone,
                    hearing,
                ),
            )
            .await;
        }
        AdminRequestMode::GlobalGetAvatarScaleLimits => {
            let config = state.config.read().clone();
            send_admin_payload_to_peer(
                state,
                peer,
                encode_f32_pair_admin_state_payload(
                    AdminRequestMode::GlobalGetAvatarScaleLimits,
                    config.min_avatar_eye_height_meters,
                    config.max_avatar_eye_height_meters,
                ),
            )
            .await?;
        }
        AdminRequestMode::SetGlobalAvatarScaleLimits => {
            let (min_meters, max_meters) = sanitize_avatar_scale_limits(
                reader.get_f32().unwrap_or(0.1),
                reader.get_f32().unwrap_or(100.0),
            );
            {
                let mut config = state.config.write();
                config.min_avatar_eye_height_meters = min_meters;
                config.max_avatar_eye_height_meters = max_meters;
            }
            broadcast_admin_payload(
                state,
                encode_f32_pair_admin_state_payload(
                    AdminRequestMode::GlobalGetAvatarScaleLimits,
                    min_meters,
                    max_meters,
                ),
            )
            .await;
        }
        AdminRequestMode::GlobalGetResourceLimits => {
            let value = state.config.read().max_content_spheres_per_player;
            send_admin_payload_to_peer(
                state,
                peer,
                encode_i32_admin_state_payload(AdminRequestMode::GlobalGetResourceLimits, value),
            )
            .await?;
        }
        AdminRequestMode::SetGlobalResourceLimits => {
            let requested = reader.get_i32().unwrap_or(32);
            let value = if requested < 1 {
                32
            } else {
                requested.min(4096)
            };
            state.config.write().max_content_spheres_per_player = value;
            broadcast_admin_payload(
                state,
                encode_i32_admin_state_payload(AdminRequestMode::GlobalGetResourceLimits, value),
            )
            .await;
        }
        AdminRequestMode::GlobalGetReductionSettings => {
            let payload = encode_reduction_settings_payload(&state.config.read());
            send_admin_payload_to_peer(state, peer, payload).await?;
        }
        AdminRequestMode::SetGlobalReductionSettings => {
            {
                let mut config = state.config.write();
                config.bsrsmillisecond_default_interval = reader.get_i32().unwrap_or(50).max(1);
                config.bsrbase_multiplier = reader.get_i32().unwrap_or(1).max(1);
                config.bsrsincrease_rate = reader.get_f32().unwrap_or(0.005).max(0.0);
                config.bsrslowest_send_rate = reader.get_f32().unwrap_or(2.55).max(0.0);
                config.high_quality_distance = reader.get_f32().unwrap_or(10.0).clamp(0.0, 1000.0);
                config.medium_quality_distance =
                    reader.get_f32().unwrap_or(20.0).clamp(0.0, 1000.0);
                config.low_quality_distance = reader.get_f32().unwrap_or(40.0).clamp(0.0, 1000.0);
                config.enable_avatar_bundle_compression = reader.get_bool().unwrap_or(true);
                config.avatar_bundle_min_messages = reader.get_i32().unwrap_or(2).max(1);
                config.avatar_bundle_min_bytes = reader.get_i32().unwrap_or(0).max(0);
                config.enable_bsrprofiling = reader.get_bool().unwrap_or(false);
                config.enable_avatar_bundle_zstd = reader.get_bool().unwrap_or(false);
                config.avatar_bundle_zstd_delta_bundles = reader.get_bool().unwrap_or(false);
                config.avatar_bundle_zstd_level =
                    reader.get_i32().unwrap_or(-2).clamp(-131_072, 22);
                config.avatar_bundle_zstd_max_shed_tier = reader.get_i32().unwrap_or(0).clamp(0, 2);
            }
            state.refresh_runtime_config();
            let payload = encode_reduction_settings_payload(&state.config.read());
            broadcast_admin_payload(state, payload).await;
        }
        AdminRequestMode::GlobalGetImageBandwidth => {
            let payload = encode_image_bandwidth_payload(&state.config.read());
            send_admin_payload_to_peer(state, peer, payload).await?;
        }
        AdminRequestMode::SetGlobalImageBandwidth => {
            {
                let mut config = state.config.write();
                config.image_share_egress_megabits_per_second =
                    reader.get_i32().unwrap_or(200).max(0);
                config.image_share_download_megabits_per_second =
                    reader.get_i32().unwrap_or(200).max(0);
                config.image_share_egress_enforcement_percent =
                    reader.get_i32().unwrap_or(150).clamp(100, 1000);
            }
            let payload = encode_image_bandwidth_payload(&state.config.read());
            broadcast_admin_payload(state, payload).await;
        }
        AdminRequestMode::GlobalGetPeerLimit => {
            let value = state.config.read().peer_limit;
            send_admin_payload_to_peer(
                state,
                peer,
                encode_i32_admin_state_payload(AdminRequestMode::GlobalGetPeerLimit, value),
            )
            .await?;
        }
        AdminRequestMode::SetGlobalPeerLimit => {
            let value = reader.get_i32().unwrap_or(1).clamp(1, u16::MAX as i32);
            state.config.write().peer_limit = value;
            broadcast_admin_payload(
                state,
                encode_i32_admin_state_payload(AdminRequestMode::GlobalGetPeerLimit, value),
            )
            .await;
        }
        AdminRequestMode::GlobalTogglePlayspaceMover => {
            toggle_simple_lock(
                state,
                |s| &mut s.playspace_mover_locked,
                |c| &mut c.playspace_mover_locked,
            )
            .await;
        }
        AdminRequestMode::GlobalToggleDirectConnect => {
            toggle_simple_lock(
                state,
                |s| &mut s.direct_connect_locked,
                |c| &mut c.direct_connect_locked,
            )
            .await;
        }
        AdminRequestMode::GlobalToggleCilbox => {
            toggle_simple_lock(state, |s| &mut s.cilbox_locked, |c| &mut c.cilbox_locked).await;
        }
        AdminRequestMode::GlobalToggleImages => {
            toggle_simple_lock(state, |s| &mut s.images_locked, |c| &mut c.images_locked).await;
        }
        AdminRequestMode::GlobalToggleEndEffectorIK => {
            toggle_simple_lock(
                state,
                |s| &mut s.end_effector_ik_disabled,
                |c| &mut c.end_effector_ik_disabled,
            )
            .await;
        }
        AdminRequestMode::GlobalToggleTextChat => {
            toggle_simple_lock(
                state,
                |s| &mut s.text_chat_locked,
                |c| &mut c.text_chat_locked,
            )
            .await;
        }
        AdminRequestMode::GlobalToggleVoiceChat => {
            toggle_simple_lock(
                state,
                |s| &mut s.voice_chat_locked,
                |c| &mut c.voice_chat_locked,
            )
            .await;
        }
        AdminRequestMode::GlobalToggleMediaPlayer => {
            toggle_simple_lock(
                state,
                |s| &mut s.media_player_locked,
                |c| &mut c.media_player_locked,
            )
            .await;
        }
        AdminRequestMode::GlobalToggleCameraCapture => {
            toggle_simple_lock(
                state,
                |s| &mut s.camera_capture_locked,
                |c| &mut c.camera_capture_locked,
            )
            .await;
        }
        AdminRequestMode::GlobalTogglePropGrabbing => {
            toggle_simple_lock(
                state,
                |s| &mut s.prop_grabbing_locked,
                |c| &mut c.prop_grabbing_locked,
            )
            .await;
        }
        AdminRequestMode::GlobalToggleSafeDisplayNames => {
            toggle_simple_lock(
                state,
                |s| &mut s.safe_display_names_forced,
                |c| &mut c.safe_display_names_forced,
            )
            .await;
        }
        AdminRequestMode::GlobalGetLockState => {
            send_lock_state_to_peer(state, peer).await?;
        }
        AdminRequestMode::GlobalGetHeadlessAudioState => {
            let headless_audio_off = state.global_state.read().headless_audio_off;
            send_bool_admin_state(
                state,
                peer,
                AdminRequestMode::GlobalGetHeadlessAudioState,
                headless_audio_off,
            )
            .await?;
        }
        AdminRequestMode::SetGlobalHeadlessAudio => {
            let value = reader.get_bool().unwrap_or(false);
            state.global_state.write().headless_audio_off = value;
            broadcast_bool_admin_state(state, AdminRequestMode::GlobalGetHeadlessAudioState, value)
                .await;
        }
        AdminRequestMode::GlobalGetHeadlessDisallowState => {
            let disallow_headless = state.global_state.read().disallow_headless;
            send_bool_admin_state(
                state,
                peer,
                AdminRequestMode::GlobalGetHeadlessDisallowState,
                disallow_headless,
            )
            .await?;
        }
        AdminRequestMode::SetGlobalHeadlessDisallow => {
            let value = reader.get_bool().unwrap_or(false);
            state.global_state.write().disallow_headless = value;
            state.config.write().disallow_headless = value;
            broadcast_bool_admin_state(
                state,
                AdminRequestMode::GlobalGetHeadlessDisallowState,
                value,
            )
            .await;
            if value {
                disconnect_headless_peers(state).await;
            }
        }
        AdminRequestMode::GlobalGetOpusPacketLossState => {
            let packet_loss = state.global_state.read().opus_packet_loss_percent;
            send_u8_admin_state(
                state,
                peer,
                AdminRequestMode::GlobalGetOpusPacketLossState,
                packet_loss,
            )
            .await?;
        }
        AdminRequestMode::SetGlobalOpusPacketLoss => {
            let value = reader.get_u8().unwrap_or(10).min(100);
            state.global_state.write().opus_packet_loss_percent = value;
            broadcast_u8_admin_state(state, AdminRequestMode::GlobalGetOpusPacketLossState, value)
                .await;
        }
        AdminRequestMode::GlobalGetOpusFrameDurationState => {
            let frame_duration = state.global_state.read().opus_frame_duration_ms;
            send_u8_admin_state(
                state,
                peer,
                AdminRequestMode::GlobalGetOpusFrameDurationState,
                frame_duration,
            )
            .await?;
        }
        AdminRequestMode::SetGlobalOpusFrameDuration => {
            let requested = reader.get_u8().unwrap_or(20);
            let value = if requested == 40 { 40 } else { 20 };
            state.global_state.write().opus_frame_duration_ms = value;
            broadcast_u8_admin_state(
                state,
                AdminRequestMode::GlobalGetOpusFrameDurationState,
                value,
            )
            .await;
        }
        AdminRequestMode::GlobalGetOpusBitrateState => {
            let value = state.global_state.read().global_opus_bitrate;
            send_admin_payload_to_peer(
                state,
                peer,
                encode_i32_admin_state_payload(AdminRequestMode::GlobalGetOpusBitrateState, value),
            )
            .await?;
        }
        AdminRequestMode::SetGlobalOpusBitrate => {
            let requested = reader.get_i32().unwrap_or(0);
            let value = if requested <= 0 {
                0
            } else {
                requested.clamp(6_000, 510_000)
            };
            state.global_state.write().global_opus_bitrate = value;
            broadcast_admin_payload(
                state,
                encode_i32_admin_state_payload(AdminRequestMode::GlobalGetOpusBitrateState, value),
            )
            .await;
        }
        AdminRequestMode::SetUserOpusBitrate => {
            let target = reader.get_u16().unwrap_or(peer);
            let requested = reader.get_i32().unwrap_or(0).clamp(0, 510_000);
            let applied = if requested == 0 {
                0
            } else {
                requested.max(6_000)
            };
            let mut writer = NetWriter::new();
            AdminRequest {
                mode: AdminRequestMode::UserOpusBitrateOverride,
            }
            .serialize(&mut writer);
            writer.put_i32(applied);
            let _ = state
                .transport
                .send(
                    target,
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                )
                .await;
        }
        AdminRequestMode::GetPermissions => {
            send_permissions_snapshot(state, peer).await?;
        }
        AdminRequestMode::SetUserGroup => {
            if let (Ok(uuid), Ok(group), Ok(add)) =
                (reader.get_string(), reader.get_string(), reader.get_bool())
            {
                if add {
                    state.permissions.add_user_to_group(&uuid, &group);
                } else {
                    state.permissions.remove_user_from_group(&uuid, &group);
                }
                let _ = state.permissions.save_to_xml();
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::SetUserNode => {
            if let (Ok(uuid), Ok(node), Ok(add)) =
                (reader.get_string(), reader.get_string(), reader.get_bool())
            {
                if add {
                    state.permissions.add_user_node(&uuid, &node);
                } else {
                    state.permissions.remove_user_node(&uuid, &node);
                }
                let _ = state.permissions.save_to_xml();
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::SetGroupNode => {
            if let (Ok(group), Ok(node), Ok(add)) =
                (reader.get_string(), reader.get_string(), reader.get_bool())
            {
                if add {
                    state.permissions.add_group_node(&group, &node);
                } else {
                    state.permissions.remove_group_node(&group, &node);
                }
                let _ = state.permissions.save_to_xml();
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::CreateGroup => {
            if let Ok(group) = reader.get_string() {
                state.permissions.get_or_create_group(&group);
                let _ = state.permissions.save_to_xml();
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::DeleteGroup => {
            if let Ok(group) = reader.get_string() {
                state.permissions.delete_group(&group);
                let _ = state.permissions.save_to_xml();
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::SetGroupParent => {
            if let (Ok(group), Ok(parent), Ok(add)) =
                (reader.get_string(), reader.get_string(), reader.get_bool())
            {
                if add {
                    state.permissions.add_group_parent(&group, &parent);
                } else {
                    state.permissions.remove_group_parent(&group, &parent);
                }
                let _ = state.permissions.save_to_xml();
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::Message => {
            let target = reader.get_u16().unwrap_or(peer);
            let message = reader.get_string().unwrap_or_default();
            send_admin_text(state, target, &message).await?;
        }
        AdminRequestMode::MessageAll => {
            let message = reader.get_string().unwrap_or_default();
            let mut writer = NetWriter::new();
            AdminRequest {
                mode: AdminRequestMode::MessageAll,
            }
            .serialize(&mut writer);
            writer.put_string(&message);
            state
                .broadcast(
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    None,
                )
                .await;
        }
        AdminRequestMode::TeleportAll => {
            let target = reader.get_u16().unwrap_or(peer);
            let mut writer = NetWriter::new();
            request.serialize(&mut writer);
            writer.put_u16(target);
            state
                .broadcast(
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
        AdminRequestMode::TeleportPlayer => {
            let target = reader.get_u16().unwrap_or(peer);
            let mut writer = NetWriter::new();
            request.serialize(&mut writer);
            writer.put_u16(peer);
            let _ = state
                .transport
                .send(
                    target,
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                )
                .await;
        }
        AdminRequestMode::EnableShoutMode | AdminRequestMode::DisableShoutMode => {
            let target = reader.get_u16().unwrap_or(peer);
            let mut writer = NetWriter::new();
            request.serialize(&mut writer);
            writer.put_u16(target);
            state
                .broadcast(
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    None,
                )
                .await;
        }
        AdminRequestMode::SetFullQualityBroadcast => {
            let target = reader.get_u16().unwrap_or(peer);
            let enabled = reader.get_bool().unwrap_or(false);
            state.avatar_sync.set_bypass_reduction(target, enabled);
            send_admin_text(
                state,
                peer,
                if enabled {
                    "Full-quality broadcast enabled."
                } else {
                    "Full-quality broadcast disabled."
                },
            )
            .await?;
        }
        AdminRequestMode::ForceAvatar => {
            handle_force_avatar(state, peer, &mut reader, false).await?;
        }
        AdminRequestMode::ForceAvatarAll => {
            handle_force_avatar(state, peer, &mut reader, true).await?;
        }
        AdminRequestMode::SetLocomotionOverride => {
            handle_locomotion_override(state, peer, &mut reader, false).await?;
        }
        AdminRequestMode::SetLocomotionOverrideAll => {
            handle_locomotion_override(state, peer, &mut reader, true).await?;
        }
        AdminRequestMode::RequestAllLogs => {
            send_log_bundle(state, peer).await?;
        }
        AdminRequestMode::DeleteAllLogs => {
            delete_all_logs(state, peer).await?;
        }
        AdminRequestMode::Ban => {
            if let Ok(uuid) = reader.get_string() {
                let reason = reader.get_string().unwrap_or_else(|_| "Banned".to_string());
                state
                    .moderation
                    .add_ban_with_details(uuid.clone(), reason.clone(), None)?;
                if let Some(target) = peer_by_uuid(state, &uuid) {
                    let _ = state.transport.disconnect(target, &reason).await;
                }
            }
        }
        AdminRequestMode::Kick => {
            if let Ok(uuid) = reader.get_string() {
                if let Some(target) = peer_by_uuid(state, &uuid) {
                    let reason = reader.get_string().unwrap_or_else(|_| "Kicked".to_string());
                    let _ = state.transport.disconnect(target, &reason).await;
                }
            }
        }
        AdminRequestMode::IpAndBan => {
            if let Ok(uuid) = reader.get_string() {
                let reason = reader.get_string().unwrap_or_else(|_| "Banned".to_string());
                let ip = peer_by_uuid(state, &uuid).and_then(|target| {
                    state
                        .transport
                        .peer_snapshots()
                        .into_iter()
                        .find(|snapshot| snapshot.id == target)
                        .map(|snapshot| snapshot.addr.ip().to_string())
                });
                state
                    .moderation
                    .add_ban_with_details(uuid.clone(), reason.clone(), ip)?;
                if let Some(target) = peer_by_uuid(state, &uuid) {
                    let _ = state.transport.disconnect(target, &reason).await;
                }
            }
        }
        AdminRequestMode::UnBan => {
            if let Ok(uuid) = reader.get_string() {
                let _ = state.moderation.remove_ban(&uuid)?;
            }
        }
        AdminRequestMode::UnBanIP => {
            if let Ok(ip) = reader.get_string() {
                let _ = state.moderation.remove_ip_ban(&ip)?;
            }
        }
        AdminRequestMode::SetServerName => {
            state.config.write().server_name = reader.get_string().unwrap_or_default();
        }
        AdminRequestMode::SetServerMotd => {
            state.config.write().server_motd = reader.get_string().unwrap_or_default();
        }
        AdminRequestMode::SetAllowlistMode => {
            let mode = reader.get_u8().unwrap_or(0);
            let restriction = match mode {
                1 => basis_protocol::config::BasisUserRestrictionMode::WhiteList,
                2 => basis_protocol::config::BasisUserRestrictionMode::BlackList,
                _ => basis_protocol::config::BasisUserRestrictionMode::None,
            };
            state.config.write().basis_user_restriction_mode = restriction;
            state.global_state.write().restriction_mode = restriction as u8;
            broadcast_lock_state(state).await;
        }
        AdminRequestMode::AddAllowlist => {
            if let Ok(uuid) = reader.get_string() {
                state.moderation.add_whitelist(uuid)?;
            }
        }
        AdminRequestMode::RemoveAllowlist => {
            if let Ok(uuid) = reader.get_string() {
                let _ = state.moderation.remove_whitelist(&uuid)?;
            }
        }
        AdminRequestMode::AddDefaultLibraryItem | AdminRequestMode::RemoveDefaultLibraryItem => {
            send_admin_text(
                state,
                peer,
                "Default library mutation is accepted by the Rust admin API, but filesystem persistence is handled by server startup library loading.",
            )
            .await?;
        }
        _ => {
            warn!("admin mode {:?} is not accepted from clients", request.mode);
        }
    }
    if admin_mode_persists_config(request.mode) {
        state.config.read().save(&state.config_path)?;
    }
    Ok(())
}

async fn handle_force_avatar(
    state: &ServerState,
    moderator: PeerId,
    reader: &mut NetReader<'_>,
    all: bool,
) -> Result<()> {
    let target = if all { None } else { Some(reader.get_u16()?) };
    let url = reader.get_string()?;
    let password = reader.get_string()?;
    let embedded_source = reader.get_u8()?;
    if url.is_empty() {
        send_admin_text(state, moderator, "Avatar url invalid").await?;
        return Ok(());
    }

    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::ForceAvatarApply,
    }
    .serialize(&mut writer);
    writer.put_u16(moderator);
    writer.put_string(&url);
    writer.put_string(&password);
    writer.put_u8(embedded_source);
    let payload = writer.into_vec();

    if let Some(target) = target {
        if !state.authenticated_peers.contains_key(&target) {
            send_admin_text(state, moderator, "Player not found").await?;
            return Ok(());
        }
        if has_protection_permission(state, target) {
            send_admin_text(state, moderator, "Target is protected").await?;
            return Ok(());
        }
        send_admin_payload_to_peer(state, target, payload).await?;
        send_admin_text(state, moderator, "Avatar forced on player.").await?;
        return Ok(());
    }

    let targets = state
        .authenticated_peers
        .iter()
        .map(|entry| *entry.key())
        .filter(|target| *target != moderator && !has_protection_permission(state, *target))
        .collect::<Vec<_>>();
    for target in targets {
        send_admin_payload_to_peer(state, target, payload.clone()).await?;
    }
    send_admin_text(state, moderator, "Avatar forced on eligible players.").await?;
    Ok(())
}

async fn handle_locomotion_override(
    state: &ServerState,
    moderator: PeerId,
    reader: &mut NetReader<'_>,
    all: bool,
) -> Result<()> {
    let target = if all { None } else { Some(reader.get_u16()?) };
    let fields = reader.get_u8()?;
    let jump_height = reader.get_f32()?;
    let walk_speed = reader.get_f32()?;
    let run_speed = reader.get_f32()?;
    let gravity = reader.get_f32()?;
    let movement_mode = reader.get_u8()?;

    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::LocomotionOverrideApply,
    }
    .serialize(&mut writer);
    writer.put_u16(moderator);
    writer.put_u8(fields);
    writer.put_f32(jump_height);
    writer.put_f32(walk_speed);
    writer.put_f32(run_speed);
    writer.put_f32(gravity);
    writer.put_u8(movement_mode);
    let payload = writer.into_vec();

    if let Some(target) = target {
        if !state.authenticated_peers.contains_key(&target) {
            send_admin_text(state, moderator, "Player not found").await?;
            return Ok(());
        }
        if has_protection_permission(state, target) {
            send_admin_text(state, moderator, "Target is protected").await?;
            return Ok(());
        }
        send_admin_payload_to_peer(state, target, payload).await?;
        send_admin_text(state, moderator, "Locomotion override updated.").await?;
        return Ok(());
    }

    let targets = state
        .authenticated_peers
        .iter()
        .map(|entry| *entry.key())
        .filter(|target| *target != moderator && !has_protection_permission(state, *target))
        .collect::<Vec<_>>();
    for target in targets {
        send_admin_payload_to_peer(state, target, payload.clone()).await?;
    }
    send_admin_text(
        state,
        moderator,
        "Locomotion override updated for eligible players.",
    )
    .await?;
    Ok(())
}

const LOG_BUNDLE_CHUNK_SIZE: usize = 32 * 1024;
const LOG_BUNDLE_MAX_RAW_BYTES: usize = 256 * 1024 * 1024;

struct PreparedLogBundle {
    payload: Vec<u8>,
    raw_len: usize,
    file_count: usize,
    compressed: bool,
}

async fn send_log_bundle(state: &ServerState, peer: PeerId) -> Result<()> {
    if !state.config.read().has_file_support {
        send_admin_text(
            state,
            peer,
            "File support is disabled on this server; there are no logs to pull.",
        )
        .await?;
        return Ok(());
    }

    let base_dir = state
        .config_path
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let prepared = tokio::task::spawn_blocking(move || build_log_bundle(&base_dir))
        .await
        .context("joining log bundle worker")??;
    let Some(prepared) = prepared else {
        send_admin_text(state, peer, "No log files were found to send.").await?;
        return Ok(());
    };

    let server_name = sanitize_log_bundle_name(&state.config.read().server_name);
    let total_chunks = prepared.payload.len().div_ceil(LOG_BUNDLE_CHUNK_SIZE);
    let mut begin = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::LogBundleBegin,
    }
    .serialize(&mut begin);
    begin.put_string(&server_name);
    begin.put_string("logs");
    begin.put_bool(prepared.compressed);
    begin.put_i32(prepared.payload.len() as i32);
    begin.put_i32(prepared.raw_len as i32);
    begin.put_i32(total_chunks as i32);
    send_admin_payload_to_peer(state, peer, begin.into_vec()).await?;

    for (index, chunk) in prepared.payload.chunks(LOG_BUNDLE_CHUNK_SIZE).enumerate() {
        let mut writer = NetWriter::new();
        AdminRequest {
            mode: AdminRequestMode::LogBundleChunk,
        }
        .serialize(&mut writer);
        writer.put_i32(index as i32);
        writer.put_bytes_with_length(chunk);
        send_admin_payload_to_peer(state, peer, writer.into_vec()).await?;
    }

    let mut end = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::LogBundleEnd,
    }
    .serialize(&mut end);
    end.put_bool(true);
    end.put_string(&format!(
        "Sent {} log file(s), {} KB compressed.",
        prepared.file_count,
        prepared.payload.len() / 1024
    ));
    send_admin_payload_to_peer(state, peer, end.into_vec()).await?;
    Ok(())
}

fn build_log_bundle(base_dir: &Path) -> Result<Option<PreparedLogBundle>> {
    let mut raw = vec![0u8; 4];
    let mut file_count = 0usize;
    append_log_directory(
        &mut raw,
        &mut file_count,
        &base_dir.join(ServerConfig::LOGS_FOLDER_NAME),
        "logs",
    )?;
    append_log_directory(
        &mut raw,
        &mut file_count,
        &base_dir.join("CrashReports"),
        "CrashReports",
    )?;
    if file_count == 0 {
        return Ok(None);
    }
    anyhow::ensure!(
        raw.len() <= LOG_BUNDLE_MAX_RAW_BYTES,
        "log bundle exceeds {} bytes",
        LOG_BUNDLE_MAX_RAW_BYTES
    );
    raw[..4].copy_from_slice(&(file_count as i32).to_le_bytes());
    let raw_len = raw.len();
    let compressed = lz4_flex::block::compress(&raw);
    let (payload, is_compressed) = if compressed.len() < raw.len() {
        (compressed, true)
    } else {
        (raw, false)
    };
    Ok(Some(PreparedLogBundle {
        payload,
        raw_len,
        file_count,
        compressed: is_compressed,
    }))
}

fn append_log_directory(
    raw: &mut Vec<u8>,
    file_count: &mut usize,
    root: &Path,
    prefix: &str,
) -> Result<()> {
    if !root.is_dir() {
        return Ok(());
    }
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
                continue;
            }
            if !file_type.is_file() {
                continue;
            }
            let relative = path.strip_prefix(root).unwrap_or(&path);
            let relative = relative
                .components()
                .map(|part| part.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            let entry_name = format!("{prefix}/{relative}");
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(err) => {
                    warn!("skipping log file {}: {err}", path.display());
                    continue;
                }
            };
            let projected = raw
                .len()
                .saturating_add(entry_name.len())
                .saturating_add(bytes.len())
                .saturating_add(16);
            anyhow::ensure!(
                projected <= LOG_BUNDLE_MAX_RAW_BYTES,
                "log bundle exceeds {} bytes",
                LOG_BUNDLE_MAX_RAW_BYTES
            );
            write_binary_writer_string(raw, &entry_name);
            raw.extend_from_slice(&(bytes.len() as i32).to_le_bytes());
            raw.extend_from_slice(&bytes);
            *file_count += 1;
        }
    }
    Ok(())
}

fn write_binary_writer_string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    let mut length = bytes.len() as u32;
    while length >= 0x80 {
        out.push((length as u8) | 0x80);
        length >>= 7;
    }
    out.push(length as u8);
    out.extend_from_slice(bytes);
}

fn sanitize_log_bundle_name(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "server".to_string();
    }
    let mut safe = String::with_capacity(trimmed.len());
    for ch in trimmed.chars() {
        if matches!(
            ch,
            '<' | '>' | ':' | '"' | '/' | '\\' | '|' | '?' | '*' | ' '
        ) || ch.is_control()
        {
            safe.push('_');
        } else {
            safe.push(ch);
        }
    }
    if safe.is_empty() {
        "server".to_string()
    } else {
        safe
    }
}

async fn delete_all_logs(state: &ServerState, peer: PeerId) -> Result<()> {
    if !state.config.read().has_file_support {
        send_admin_text(
            state,
            peer,
            "File support is disabled on this server; there are no logs to delete.",
        )
        .await?;
        return Ok(());
    }
    let base_dir = state
        .config_path
        .parent()
        .and_then(Path::parent)
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    let deleted = tokio::task::spawn_blocking(move || -> Result<usize> {
        let mut deleted = 0usize;
        deleted += delete_directory_files(&base_dir.join(ServerConfig::LOGS_FOLDER_NAME))?;
        deleted += delete_directory_files(&base_dir.join("CrashReports"))?;
        Ok(deleted)
    })
    .await
    .context("joining log deletion worker")??;
    state.error_report_hashes.clear();
    send_admin_text(
        state,
        peer,
        &format!("Deleted {deleted} log/crash file(s) from logs/ and CrashReports/."),
    )
    .await?;
    Ok(())
}

fn delete_directory_files(root: &Path) -> Result<usize> {
    if !root.is_dir() {
        return Ok(0);
    }
    let mut deleted = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).with_context(|| format!("reading {}", dir.display()))? {
            let entry = entry?;
            let path = entry.path();
            let file_type = entry.file_type()?;
            if file_type.is_dir() {
                stack.push(path);
            } else if file_type.is_file() {
                match fs::remove_file(&path) {
                    Ok(()) => deleted += 1,
                    Err(err) => warn!("could not delete log file {}: {err}", path.display()),
                }
            }
        }
    }
    Ok(deleted)
}

async fn send_lock_state_to_peer(state: &ServerState, peer_id: PeerId) -> Result<()> {
    let locks = state.global_state.read().clone();
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GlobalGetLockState,
    }
    .serialize(&mut writer);
    write_lock_state_fields(&mut writer, &locks);
    state
        .transport
        .send(
            peer_id,
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
        )
        .await?;
    Ok(())
}

async fn send_initial_admin_state_to_peer(state: &ServerState, peer_id: PeerId) {
    let globals = state.global_state.read().clone();
    let config = state.config.read().clone();
    let payloads = [
        encode_lock_state_payload(&globals),
        encode_bool_admin_state_payload(
            AdminRequestMode::GlobalGetHeadlessAudioState,
            globals.headless_audio_off,
        ),
        encode_bool_admin_state_payload(
            AdminRequestMode::GlobalGetHeadlessDisallowState,
            globals.disallow_headless,
        ),
        encode_u8_admin_state_payload(
            AdminRequestMode::GlobalGetOpusPacketLossState,
            globals.opus_packet_loss_percent,
        ),
        encode_u8_admin_state_payload(
            AdminRequestMode::GlobalGetOpusFrameDurationState,
            globals.opus_frame_duration_ms,
        ),
        encode_user_opus_bitrate_override_payload(0),
        encode_i32_admin_state_payload(
            AdminRequestMode::GlobalGetOpusBitrateState,
            globals.global_opus_bitrate,
        ),
        encode_bool_admin_state_payload(
            AdminRequestMode::GlobalGetCrashReportState,
            config.crash_reporting_enabled,
        ),
        encode_f32_pair_admin_state_payload(
            AdminRequestMode::GlobalGetAudioRangeLimits,
            config.max_microphone_range_meters,
            config.max_hearing_range_meters,
        ),
        encode_f32_pair_admin_state_payload(
            AdminRequestMode::GlobalGetAvatarScaleLimits,
            config.min_avatar_eye_height_meters,
            config.max_avatar_eye_height_meters,
        ),
        encode_i32_admin_state_payload(
            AdminRequestMode::GlobalGetResourceLimits,
            config.max_content_spheres_per_player,
        ),
        encode_reduction_settings_payload(&config),
        encode_image_bandwidth_payload(&config),
        encode_i32_admin_state_payload(AdminRequestMode::GlobalGetPeerLimit, config.peer_limit),
    ];
    let messages =
        payloads.map(|payload| (channels::ADMIN, DeliveryMethod::ReliableOrdered, payload));
    if let Err(err) = state.transport.send_many(peer_id, &messages).await {
        warn!("failed to send initial admin state to peer {peer_id}: {err:#}");
    }
}

fn encode_lock_state_payload(locks: &GlobalState) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GlobalGetLockState,
    }
    .serialize(&mut writer);
    write_lock_state_fields(&mut writer, locks);
    writer.into_vec()
}

fn write_lock_state_fields(writer: &mut NetWriter, locks: &GlobalState) {
    writer.put_bool(locks.avatars_locked);
    writer.put_bool(locks.props_locked);
    writer.put_bool(locks.worlds_locked);
    writer.put_bool(locks.servers_locked);
    writer.put_bool(locks.third_person_disabled);
    writer.put_bool(locks.additional_avatar_data_lock);
    writer.put_u8(locks.camera_metadata_disallow_mask);
    writer.put_u8(locks.restriction_mode);
    writer.put_bool(locks.playspace_mover_locked);
    writer.put_bool(locks.direct_connect_locked);
    writer.put_bool(locks.cilbox_locked);
    writer.put_bool(locks.images_locked);
    writer.put_bool(locks.end_effector_ik_disabled);
    writer.put_bool(locks.text_chat_locked);
    writer.put_bool(locks.voice_chat_locked);
    writer.put_bool(locks.media_player_locked);
    writer.put_bool(locks.camera_capture_locked);
    writer.put_bool(locks.prop_grabbing_locked);
    writer.put_bool(locks.safe_display_names_forced);
}

fn encode_bool_admin_state_payload(mode: AdminRequestMode, value: bool) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest { mode }.serialize(&mut writer);
    writer.put_bool(value);
    writer.into_vec()
}

fn encode_u8_admin_state_payload(mode: AdminRequestMode, value: u8) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest { mode }.serialize(&mut writer);
    writer.put_u8(value);
    writer.into_vec()
}

fn encode_i32_admin_state_payload(mode: AdminRequestMode, value: i32) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest { mode }.serialize(&mut writer);
    writer.put_i32(value);
    writer.into_vec()
}

fn encode_f32_pair_admin_state_payload(mode: AdminRequestMode, first: f32, second: f32) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest { mode }.serialize(&mut writer);
    writer.put_f32(first);
    writer.put_f32(second);
    writer.into_vec()
}

fn encode_reduction_settings_payload(config: &ServerConfig) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GlobalGetReductionSettings,
    }
    .serialize(&mut writer);
    writer.put_i32(config.bsrsmillisecond_default_interval);
    writer.put_i32(config.bsrbase_multiplier);
    writer.put_f32(config.bsrsincrease_rate);
    writer.put_f32(config.bsrslowest_send_rate);
    writer.put_f32(config.high_quality_distance);
    writer.put_f32(config.medium_quality_distance);
    writer.put_f32(config.low_quality_distance);
    writer.put_bool(config.enable_avatar_bundle_compression);
    writer.put_i32(config.avatar_bundle_min_messages);
    writer.put_i32(config.avatar_bundle_min_bytes);
    writer.put_bool(config.enable_bsrprofiling);
    writer.put_bool(config.enable_avatar_bundle_zstd);
    writer.put_bool(config.avatar_bundle_zstd_delta_bundles);
    writer.put_i32(config.avatar_bundle_zstd_level);
    writer.put_i32(config.avatar_bundle_zstd_max_shed_tier);
    writer.into_vec()
}

fn encode_image_bandwidth_payload(config: &ServerConfig) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GlobalGetImageBandwidth,
    }
    .serialize(&mut writer);
    writer.put_i32(config.image_share_egress_megabits_per_second);
    writer.put_i32(config.image_share_download_megabits_per_second);
    writer.put_i32(config.image_share_egress_enforcement_percent);
    writer.into_vec()
}

fn encode_user_opus_bitrate_override_payload(value: i32) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::UserOpusBitrateOverride,
    }
    .serialize(&mut writer);
    writer.put_i32(value);
    writer.into_vec()
}

async fn send_admin_payload_to_peer(
    state: &ServerState,
    peer_id: PeerId,
    payload: Vec<u8>,
) -> Result<()> {
    state
        .transport
        .send(
            peer_id,
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            &payload,
        )
        .await?;
    Ok(())
}

async fn broadcast_admin_payload(state: &ServerState, payload: Vec<u8>) {
    state
        .broadcast(
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            &payload,
            None,
        )
        .await;
}

fn sanitize_positive_range(value: f32, fallback: f32) -> f32 {
    if value.is_finite() && value > 0.0 {
        value
    } else {
        fallback
    }
}

fn sanitize_avatar_scale_limits(min_meters: f32, max_meters: f32) -> (f32, f32) {
    let mut min_meters = sanitize_positive_range(min_meters, 0.1).max(0.01);
    let mut max_meters = sanitize_positive_range(max_meters, 100.0).min(1000.0);
    min_meters = min_meters.min(1000.0);
    if max_meters < min_meters {
        max_meters = min_meters;
    }
    (min_meters, max_meters)
}

async fn send_admin_text(state: &ServerState, peer_id: PeerId, message: &str) -> Result<()> {
    if message.is_empty() {
        return Ok(());
    }
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::Message,
    }
    .serialize(&mut writer);
    writer.put_string(message);
    state
        .transport
        .send(
            peer_id,
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
        )
        .await?;
    Ok(())
}

async fn send_permissions_snapshot(state: &ServerState, peer_id: PeerId) -> Result<()> {
    let snapshot = state.permissions.snapshot();
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GetPermissions,
    }
    .serialize(&mut writer);
    writer.put_i32(snapshot.groups.len() as i32);
    for group in snapshot.groups.values() {
        writer.put_string(&group.name);
        writer.put_i32(group.nodes.len() as i32);
        for node in &group.nodes {
            writer.put_string(node);
        }
        writer.put_i32(group.parents.len() as i32);
        for parent in &group.parents {
            writer.put_string(parent);
        }
    }
    writer.put_i32(snapshot.users.len() as i32);
    for user in snapshot.users.values() {
        writer.put_string(&user.uuid);
        writer.put_i32(user.groups.len() as i32);
        for group in &user.groups {
            writer.put_string(group);
        }
        writer.put_i32(user.nodes.len() as i32);
        for node in &user.nodes {
            writer.put_string(node);
        }
    }
    state
        .transport
        .send(
            peer_id,
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
        )
        .await?;
    Ok(())
}

async fn send_bool_admin_state(
    state: &ServerState,
    peer_id: PeerId,
    mode: AdminRequestMode,
    value: bool,
) -> Result<()> {
    let mut writer = NetWriter::new();
    AdminRequest { mode }.serialize(&mut writer);
    writer.put_bool(value);
    state
        .transport
        .send(
            peer_id,
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
        )
        .await?;
    Ok(())
}

async fn broadcast_bool_admin_state(state: &ServerState, mode: AdminRequestMode, value: bool) {
    let mut writer = NetWriter::new();
    AdminRequest { mode }.serialize(&mut writer);
    writer.put_bool(value);
    state
        .broadcast(
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
            None,
        )
        .await;
}

async fn send_u8_admin_state(
    state: &ServerState,
    peer_id: PeerId,
    mode: AdminRequestMode,
    value: u8,
) -> Result<()> {
    let mut writer = NetWriter::new();
    AdminRequest { mode }.serialize(&mut writer);
    writer.put_u8(value);
    state
        .transport
        .send(
            peer_id,
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
        )
        .await?;
    Ok(())
}

async fn broadcast_u8_admin_state(state: &ServerState, mode: AdminRequestMode, value: u8) {
    let mut writer = NetWriter::new();
    AdminRequest { mode }.serialize(&mut writer);
    writer.put_u8(value);
    state
        .broadcast(
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
            None,
        )
        .await;
}

fn peer_by_uuid(state: &ServerState, uuid: &str) -> Option<PeerId> {
    state
        .authenticated_peers
        .iter()
        .find_map(|peer| (peer.metadata.player_uuid == uuid).then_some(*peer.key()))
}

async fn disconnect_headless_peers(state: &ServerState) {
    let peers = state
        .authenticated_peers
        .iter()
        .filter_map(|peer| {
            let platform = &peer.metadata.player_platform;
            is_headless_platform(platform).then_some(*peer.key())
        })
        .collect::<Vec<_>>();
    for peer in peers {
        let _ = state
            .transport
            .disconnect(peer, "Headless client disallowed by server.")
            .await;
    }
}

fn is_headless_platform(platform: &str) -> bool {
    matches!(
        platform.to_ascii_lowercase().as_str(),
        "headless" | "windowsserver" | "linuxserver" | "osxserver"
    )
}

fn admin_mode_persists_config(mode: AdminRequestMode) -> bool {
    matches!(
        mode,
        AdminRequestMode::GlobalToggleAvatars
            | AdminRequestMode::GlobalToggleProps
            | AdminRequestMode::GlobalToggleWorlds
            | AdminRequestMode::GlobalToggleServers
            | AdminRequestMode::GlobalToggleThirdPerson
            | AdminRequestMode::GlobalToggleAdditionalAvatarDataLock
            | AdminRequestMode::SetGlobalCameraPolicy
            | AdminRequestMode::SetGlobalCrashReporting
            | AdminRequestMode::SetGlobalAudioRangeLimits
            | AdminRequestMode::GlobalTogglePlayspaceMover
            | AdminRequestMode::GlobalToggleDirectConnect
            | AdminRequestMode::SetGlobalHeadlessDisallow
            | AdminRequestMode::GlobalToggleCilbox
            | AdminRequestMode::GlobalToggleImages
            | AdminRequestMode::SetGlobalAvatarScaleLimits
            | AdminRequestMode::SetGlobalResourceLimits
            | AdminRequestMode::SetGlobalReductionSettings
            | AdminRequestMode::SetGlobalImageBandwidth
            | AdminRequestMode::GlobalToggleEndEffectorIK
            | AdminRequestMode::GlobalToggleTextChat
            | AdminRequestMode::GlobalToggleVoiceChat
            | AdminRequestMode::GlobalToggleMediaPlayer
            | AdminRequestMode::GlobalToggleCameraCapture
            | AdminRequestMode::GlobalTogglePropGrabbing
            | AdminRequestMode::GlobalToggleSafeDisplayNames
            | AdminRequestMode::SetServerName
            | AdminRequestMode::SetServerMotd
            | AdminRequestMode::SetAllowlistMode
            | AdminRequestMode::SetGlobalPeerLimit
    )
}

fn admin_mode_required_permission(mode: AdminRequestMode) -> Option<&'static str> {
    use basis_server_permissions::nodes;
    Some(match mode {
        AdminRequestMode::Ban => nodes::MODERATION_BAN,
        AdminRequestMode::Kick => nodes::MODERATION_KICK,
        AdminRequestMode::IpAndBan => nodes::MODERATION_IP_BAN,
        AdminRequestMode::UnBan => nodes::MODERATION_UNBAN,
        AdminRequestMode::UnBanIP => nodes::MODERATION_UNBAN_IP,
        AdminRequestMode::Message => nodes::MODERATION_MESSAGE,
        AdminRequestMode::MessageAll => nodes::MODERATION_MESSAGE_ALL,
        AdminRequestMode::TeleportAll | AdminRequestMode::TeleportPlayer => {
            nodes::MODERATION_TELEPORT
        }
        AdminRequestMode::EnableShoutMode | AdminRequestMode::DisableShoutMode => {
            nodes::MODERATION_SHOUT
        }
        AdminRequestMode::SetFullQualityBroadcast => nodes::MODERATION_FULL_QUALITY_BROADCAST,
        AdminRequestMode::ForceAvatar | AdminRequestMode::ForceAvatarAll => {
            nodes::MODERATION_FORCE_AVATAR
        }
        AdminRequestMode::SetLocomotionOverride | AdminRequestMode::SetLocomotionOverrideAll => {
            nodes::MODERATION_LOCOMOTION
        }
        AdminRequestMode::GlobalToggleAvatars
        | AdminRequestMode::GlobalToggleProps
        | AdminRequestMode::GlobalToggleWorlds
        | AdminRequestMode::GlobalToggleServers
        | AdminRequestMode::GlobalToggleThirdPerson
        | AdminRequestMode::GlobalToggleAdditionalAvatarDataLock
        | AdminRequestMode::SetGlobalCameraPolicy
        | AdminRequestMode::SetGlobalCrashReporting
        | AdminRequestMode::SetGlobalAudioRangeLimits
        | AdminRequestMode::GlobalTogglePlayspaceMover
        | AdminRequestMode::GlobalToggleDirectConnect
        | AdminRequestMode::SetGlobalHeadlessDisallow
        | AdminRequestMode::SetGlobalOpusPacketLoss
        | AdminRequestMode::GlobalToggleCilbox
        | AdminRequestMode::GlobalToggleImages
        | AdminRequestMode::SetGlobalAvatarScaleLimits
        | AdminRequestMode::SetGlobalResourceLimits
        | AdminRequestMode::SetGlobalReductionSettings
        | AdminRequestMode::SetGlobalImageBandwidth
        | AdminRequestMode::GlobalToggleEndEffectorIK
        | AdminRequestMode::GlobalToggleTextChat
        | AdminRequestMode::GlobalToggleVoiceChat
        | AdminRequestMode::GlobalToggleMediaPlayer
        | AdminRequestMode::GlobalToggleCameraCapture
        | AdminRequestMode::GlobalTogglePropGrabbing
        | AdminRequestMode::GlobalToggleSafeDisplayNames => nodes::MODERATION_GLOBAL_LOCK,
        AdminRequestMode::SetGlobalHeadlessAudio => nodes::MODERATION_HEADLESS_AUDIO,
        AdminRequestMode::SetUserOpusBitrate
        | AdminRequestMode::SetGlobalOpusFrameDuration
        | AdminRequestMode::SetGlobalOpusBitrate => nodes::MODERATION_OPUS_BITRATE,
        AdminRequestMode::SetUserGroup
        | AdminRequestMode::SetUserNode
        | AdminRequestMode::SetGroupNode
        | AdminRequestMode::CreateGroup
        | AdminRequestMode::DeleteGroup
        | AdminRequestMode::SetGroupParent => nodes::PERMISSIONS_EDIT,
        AdminRequestMode::SetServerName
        | AdminRequestMode::SetServerMotd
        | AdminRequestMode::SetAllowlistMode
        | AdminRequestMode::SetGlobalPeerLimit
        | AdminRequestMode::AddDefaultLibraryItem
        | AdminRequestMode::RemoveDefaultLibraryItem => nodes::CONFIGURATION_EDITOR,
        AdminRequestMode::AddAllowlist | AdminRequestMode::RemoveAllowlist => {
            nodes::MODERATION_WHITELIST
        }
        AdminRequestMode::RequestAllLogs | AdminRequestMode::DeleteAllLogs => nodes::ADMIN_LOGS,
        _ => return None,
    })
}

async fn toggle_simple_lock(
    state: &ServerState,
    state_field: for<'a> fn(&'a mut GlobalState) -> &'a mut bool,
    config_field: for<'a> fn(&'a mut ServerConfig) -> &'a mut bool,
) {
    let value = {
        let mut locks = state.global_state.write();
        let field = state_field(&mut locks);
        *field = !*field;
        *field
    };
    *config_field(&mut state.config.write()) = value;
    broadcast_lock_state(state).await;
}

async fn broadcast_lock_state(state: &ServerState) {
    let locks = state.global_state.read().clone();
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GlobalGetLockState,
    }
    .serialize(&mut writer);
    write_lock_state_fields(&mut writer, &locks);
    state
        .broadcast(
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
            None,
        )
        .await;
}

pub fn migrate_legacy_resource_dirs(base_dir: &Path) -> Result<()> {
    let correct = base_dir.join(ServerConfig::INITIAL_RESOURCES_FOLDER_NAME);
    if correct.exists() {
        return Ok(());
    }
    for legacy in ["initalresources", "initialressources", "intialresources"] {
        let path = base_dir.join(legacy);
        if path.exists() {
            std::fs::rename(&path, &correct).with_context(|| {
                format!(
                    "migrating legacy resource directory {} to {}",
                    path.display(),
                    correct.display()
                )
            })?;
            break;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_ready_message() -> ReadyMessage {
        ReadyMessage {
            player_meta_data_message: ClientMetaDataMessage {
                player_uuid: "00000000-0000-0000-0000-000000000000".to_string(),
                player_display_name: "test".to_string(),
                player_platform: "linux".to_string(),
            },
            client_avatar_change_message: basis_protocol::messages::ClientAvatarChangeMessage {
                load_mode: 0,
                byte_array: Vec::new(),
                local_avatar_index: 0,
                arm_scale: 1.0,
                leg_scale: 1.0,
                torso_scale: 1.0,
            },
            local_avatar_sync_message: basis_protocol::messages::LocalAvatarSyncMessage::empty_high(
            ),
        }
    }

    fn test_connected_peer(peer_id: PeerId) -> ConnectedPeer {
        ConnectedPeer {
            id: peer_id,
            metadata: test_ready_message().player_meta_data_message,
            ready: test_ready_message(),
        }
    }

    #[test]
    fn join_batches_wait_for_initial_history_and_preserve_order() {
        let mut state = JoinBroadcastState::default();
        assert!(state
            .register_peer(test_connected_peer(1), vec![1])
            .is_empty());
        let existing = state.register_peer(test_connected_peer(2), vec![2]);
        assert_eq!(
            existing.iter().map(|peer| peer.id).collect::<Vec<_>>(),
            vec![1]
        );
        assert!(state.ready_targets().is_empty());

        state.mark_initial_history_queued(1);
        state.mark_initial_history_queued(2);
        let first_batches = state.take_batches(1);
        assert_eq!(first_batches.len(), 1);
        assert_eq!(first_batches[0].len(), 1);
        assert_eq!(first_batches[0][0].sequence, 1);
        assert!(state.take_batches(2).is_empty());
    }

    #[test]
    fn join_ready_updates_refresh_pending_spawn_and_history() {
        let mut state = JoinBroadcastState::default();
        let peer_one = test_connected_peer(1);
        let peer_two = test_connected_peer(2);
        state.register_peer(
            peer_one.clone(),
            serialize_server_ready(peer_one.id, &peer_one.ready),
        );
        state.register_peer(
            peer_two.clone(),
            serialize_server_ready(peer_two.id, &peer_two.ready),
        );

        let mut updated_two = peer_two.ready.clone();
        updated_two.client_avatar_change_message.arm_scale = 1.75;
        state.update_peer_ready(peer_two.id, updated_two.clone());

        state.mark_initial_history_queued(peer_one.id);
        let pending = state.take_batches(peer_one.id);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].len(), 1);
        assert_eq!(pending[0][0].revision.load(Ordering::Acquire), 1);
        assert_eq!(
            *pending[0][0].payload.read(),
            serialize_server_ready(peer_two.id, &updated_two)
        );

        let mut updated_one = peer_one.ready.clone();
        updated_one.client_avatar_change_message.torso_scale = 1.25;
        state.update_peer_ready(peer_one.id, updated_one.clone());

        let peer_three = test_connected_peer(3);
        let existing = state.register_peer(
            peer_three.clone(),
            serialize_server_ready(peer_three.id, &peer_three.ready),
        );
        let current_one = existing.iter().find(|peer| peer.id == peer_one.id).unwrap();
        assert_eq!(
            current_one.ready.client_avatar_change_message.torso_scale,
            updated_one.client_avatar_change_message.torso_scale
        );
    }

    #[test]
    fn removing_a_join_removes_it_from_pending_targets() {
        let mut state = JoinBroadcastState::default();
        state.register_peer(test_connected_peer(1), vec![1]);
        state.register_peer(test_connected_peer(2), vec![2]);
        state.mark_initial_history_queued(1);
        state.remove_peer(2);
        assert!(state.take_batches(1).is_empty());
    }

    #[test]
    fn structured_reject_payload_matches_current_wire() {
        let payload = structured_reject_payload(
            channels::REJECT_KIND_VERSION_MISMATCH,
            SERVER_VERSION,
            SERVER_VERSION - 1,
            "Update required",
        );
        let mut reader = NetReader::new(&payload);
        assert_eq!(reader.get_u32().unwrap(), channels::REJECT_MAGIC);
        assert_eq!(
            reader.get_u8().unwrap(),
            channels::REJECT_KIND_VERSION_MISMATCH
        );
        assert_eq!(reader.get_u16().unwrap(), SERVER_VERSION);
        assert_eq!(reader.get_u16().unwrap(), SERVER_VERSION - 1);
        assert_eq!(reader.get_string().unwrap(), "Update required");
    }

    #[test]
    fn leave_batch_matches_current_basis_disconnect_wire() {
        let leaves = [1u16, 2, 513, u16::MAX];
        assert_eq!(
            serialize_leave_batch(&leaves),
            vec![1, 0, 2, 0, 1, 2, 255, 255]
        );

        let mass_leave = serialize_leave_batch(&(0..1500u16).collect::<Vec<_>>());
        assert_eq!(mass_leave.len(), 3000);
    }

    #[test]
    fn crash_report_hash_matches_current_utf16_fnv_shape() {
        let a = error_report_hash(1, "system", "message", "line one\nline two");
        let b = error_report_hash(1, "system", "message", "line one\ndifferent tail");
        let c = error_report_hash(2, "system", "message", "line one\nline two");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
