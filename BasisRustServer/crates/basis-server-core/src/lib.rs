mod admin_runtime;
mod avatar_sync;
mod event_diagnostics;
#[cfg(feature = "gpu")]
mod gpu_distance;
#[cfg(not(feature = "gpu"))]
#[path = "gpu_distance_disabled.rs"]
mod gpu_distance;
#[cfg(feature = "gpu")]
mod gpu_distance_backend;
mod gpu_distance_types;
mod gpu_policy;
mod image_cache;
mod image_governor;
#[cfg(test)]
mod image_tests;
pub mod memory_reclaim;
mod p2p;
mod realtime;

pub use avatar_sync::BsrProfilerSnapshot;

use anyhow::{Context, Result};
use basis_protocol::{
    application::NetworkApplication,
    avatar::BitQuality,
    avatar_delta::apply_delta,
    channels,
    config::{BasisUserRestrictionMode, ServerConfig},
    did::{did_key_verifying_key, DidResponse},
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
use basis_transport::{
    DeliveryMethod, DisconnectReason, OrderedAdmissionConfig, OrderedEvent, PeerId, PeerSession,
    ServerEvent, TransportHandle,
};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{HashMap, HashSet, VecDeque},
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
use subtle::ConstantTimeEq;
use tokio::sync::{mpsc, oneshot, OwnedSemaphorePermit, Semaphore};
use tracing::{error, info, warn};

pub use avatar_sync::{AvatarSyncConfig, AvatarSyncSystem};

#[derive(Debug, Clone)]
pub struct ConnectedPeer {
    pub id: PeerId,
    pub metadata: ClientMetaDataMessage,
    pub ready: ReadyMessage,
    // Present for live connections. Unit fixtures that exercise app state without a
    // transport session may leave this absent.
    pub session: Option<PeerSession>,
}

struct PendingIdentity {
    session: PeerSession,
    ready: ReadyMessage,
    challenge: Vec<u8>,
    expires_at: Instant,
    _timeout_cancel: oneshot::Sender<()>,
    _admission_slot: AdmissionSlot,
}

const MAX_PENDING_ADMISSIONS: usize = 4096;

// Reserve half the pending capacity for other source IPs, while retaining the
// established 2000-client same-host load-test batch.
const MAX_PENDING_PER_IP: usize = MAX_PENDING_ADMISSIONS / 2;

struct AdmissionSlot {
    _permit: OwnedSemaphorePermit,
    ip: IpAddr,
    counts: Arc<Mutex<HashMap<IpAddr, usize>>>,
}

impl Drop for AdmissionSlot {
    fn drop(&mut self) {
        let mut counts = self.counts.lock();
        if let Some(count) = counts.get_mut(&self.ip) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&self.ip);
            }
        }
    }
}

fn reserve_admission(state: &ServerState, ip: IpAddr) -> Option<AdmissionSlot> {
    let permit = state.admission_slots.clone().try_acquire_owned().ok()?;
    let ip = match ip {
        IpAddr::V6(ip) => ip
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(ip)),
        ip => ip,
    };
    let mut counts = state.admissions_per_ip.lock();
    let count = counts.entry(ip).or_default();
    if *count >= MAX_PENDING_PER_IP {
        return None;
    }
    *count += 1;
    Some(AdmissionSlot {
        _permit: permit,
        ip,
        counts: state.admissions_per_ip.clone(),
    })
}

fn identity_challenge_ttl(authenticated_population: usize, configured_ms: i32) -> Duration {
    // Keep the legitimate batch allowance, but unverified transports must not extend it.
    // A hard ceiling bounds how long a slot and its timeout task can be held.
    let population_extra_ms = (authenticated_population as u64)
        .saturating_mul(12)
        .min(45_000);
    Duration::from_millis(
        (configured_ms.max(0) as u64)
            .saturating_add(population_extra_ms)
            .min(60_000),
    )
}

fn admission_rejection(state: &ServerState, ready: &ReadyMessage) -> Option<&'static str> {
    let uuid = &ready.player_meta_data_message.player_uuid;
    let config = state.config.read();
    if state.moderation.is_uuid_banned(uuid) {
        return Some("Banned");
    }
    if config.basis_user_restriction_mode == BasisUserRestrictionMode::WhiteList
        && !state.moderation.is_whitelisted(uuid)
    {
        return Some("You are not on the whitelist.");
    }
    if config.basis_user_restriction_mode == BasisUserRestrictionMode::BlackList
        && state.moderation.is_blacklisted(uuid)
    {
        return Some("You are on the blacklist.");
    }
    if config.basis_user_restriction_mode == BasisUserRestrictionMode::RejoinOnly
        && !state.admin_runtime.can_rejoin(uuid)
        && !state
            .permissions
            .has(uuid, basis_server_permissions::nodes::CONFIGURATION_EDITOR)
    {
        return Some("The server is locked — only players already here may rejoin.");
    }
    None
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

    fn update_peer_ready(&mut self, peer_id: PeerId, ready: ReadyMessage) -> Result<()> {
        let payload = serialize_server_ready(peer_id, &ready)?;
        if let Some(peer) = self.peers.get_mut(&peer_id) {
            peer.peer.metadata = ready.player_meta_data_message.clone();
            peer.peer.ready = ready.clone();
            *peer.spawn_record.payload.write() = payload;
            peer.spawn_record.revision.fetch_add(1, Ordering::Release);
        }
        Ok(())
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

#[derive(Debug, Clone, Copy, Default)]
pub struct StatisticsSnapshot {
    pub inbound_packets: u64,
    pub outbound_packets: u64,
    pub protocol_errors: u64,
    /// Avatar input packets intercepted by the realtime handler.
    pub avatar_received: u64,
    /// Received avatar packets superseded by per-peer coalescing.
    pub avatar_coalesced: u64,
    /// Avatar packets discarded for authentication, session, registration, or protocol reasons.
    pub avatar_rejected: u64,
    /// Avatar packets accepted by avatar input processing.
    pub avatar_processed: u64,
}

#[derive(Debug)]
struct GatedCounter {
    enabled: Arc<AtomicBool>,
    value: AtomicU64,
}

impl GatedCounter {
    fn new(enabled: Arc<AtomicBool>) -> Self {
        Self {
            enabled,
            value: AtomicU64::new(0),
        }
    }

    fn fetch_add(&self, value: u64, ordering: Ordering) -> u64 {
        if self.enabled.load(Ordering::Relaxed) {
            self.value.fetch_add(value, ordering)
        } else {
            0
        }
    }

    fn load(&self, ordering: Ordering) -> u64 {
        if self.enabled.load(Ordering::Relaxed) {
            self.value.load(ordering)
        } else {
            0
        }
    }

    fn store(&self, value: u64, ordering: Ordering) {
        self.value.store(value, ordering);
    }
}

#[derive(Debug, Clone)]
pub struct Statistics {
    enabled: Arc<AtomicBool>,
    inbound_packets: Arc<GatedCounter>,
    outbound_packets: Arc<GatedCounter>,
    protocol_errors: Arc<GatedCounter>,
    avatar_received: Arc<GatedCounter>,
    avatar_coalesced: Arc<GatedCounter>,
    avatar_rejected: Arc<GatedCounter>,
    avatar_processed: Arc<GatedCounter>,
}

impl Statistics {
    fn new(enabled: bool) -> Self {
        let enabled = Arc::new(AtomicBool::new(enabled));
        Self {
            enabled: Arc::clone(&enabled),
            inbound_packets: Arc::new(GatedCounter::new(Arc::clone(&enabled))),
            outbound_packets: Arc::new(GatedCounter::new(Arc::clone(&enabled))),
            protocol_errors: Arc::new(GatedCounter::new(Arc::clone(&enabled))),
            avatar_received: Arc::new(GatedCounter::new(Arc::clone(&enabled))),
            avatar_coalesced: Arc::new(GatedCounter::new(Arc::clone(&enabled))),
            avatar_rejected: Arc::new(GatedCounter::new(Arc::clone(&enabled))),
            avatar_processed: Arc::new(GatedCounter::new(Arc::clone(&enabled))),
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn set_enabled(&self, enabled: bool) {
        let was_enabled = self.enabled.load(Ordering::Relaxed);
        if enabled && !was_enabled {
            self.inbound_packets.store(0, Ordering::Relaxed);
            self.outbound_packets.store(0, Ordering::Relaxed);
            self.protocol_errors.store(0, Ordering::Relaxed);
            self.avatar_received.store(0, Ordering::Relaxed);
            self.avatar_coalesced.store(0, Ordering::Relaxed);
            self.avatar_rejected.store(0, Ordering::Relaxed);
            self.avatar_processed.store(0, Ordering::Relaxed);
        }
        self.enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> StatisticsSnapshot {
        StatisticsSnapshot {
            inbound_packets: self.inbound_packets.load(Ordering::Relaxed),
            outbound_packets: self.outbound_packets.load(Ordering::Relaxed),
            protocol_errors: self.protocol_errors.load(Ordering::Relaxed),
            avatar_received: self.avatar_received.load(Ordering::Relaxed),
            avatar_coalesced: self.avatar_coalesced.load(Ordering::Relaxed),
            avatar_rejected: self.avatar_rejected.load(Ordering::Relaxed),
            avatar_processed: self.avatar_processed.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct ServerState {
    pub config: Arc<RwLock<ServerConfig>>,
    config_path: Arc<PathBuf>,
    pub transport: TransportHandle,
    pub authenticated_peers: Arc<DashMap<PeerId, ConnectedPeer>>,
    join_broadcast: Arc<Mutex<JoinBroadcastState>>,
    pending_identity: Arc<DashMap<PeerId, PendingIdentity>>,
    admission_slots: Arc<Semaphore>,
    admission_commit: Arc<Mutex<()>>,
    admissions_per_ip: Arc<Mutex<HashMap<IpAddr, usize>>>,
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
    image_governor: Arc<image_governor::ImageBandwidthGovernor<PeerSession>>,
    image_cache: Arc<Mutex<image_cache::ImageCache>>,
    jiggle_buckets: Arc<DashMap<PeerId, JiggleTokenBucket>>,
    error_report_hashes: Arc<DashMap<String, HashSet<u64>>>,
    pub avatar_sync: AvatarSyncSystem,
    pub p2p_broker: p2p::P2pBroker,
    pub moderation: ModerationLists,
    pub global_state: Arc<RwLock<GlobalState>>,
    admin_runtime: Arc<admin_runtime::AdminRuntime>,
    pub statistics: Statistics,
    pending_leaves: Arc<Mutex<Vec<PeerId>>>,
    shutdown: Arc<AtomicBool>,
    tick_thread: Arc<Mutex<Option<std::thread::JoinHandle<()>>>>,
    workers: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    disconnect_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    identity_timer_tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    realtime_threads: Arc<Mutex<Vec<std::thread::JoinHandle<()>>>>,
}

impl ServerState {
    pub async fn start(
        config: ServerConfig,
        base_dir: &Path,
    ) -> Result<(Self, oneshot::Sender<()>)> {
        Self::start_with_config_path(
            config,
            base_dir,
            &base_dir
                .join(ServerConfig::CONFIG_FOLDER_NAME)
                .join("config.xml"),
        )
        .await
    }

    pub async fn start_with_config_path(
        mut config: ServerConfig,
        base_dir: &Path,
        config_path: &Path,
    ) -> Result<(Self, oneshot::Sender<()>)> {
        // RejoinOnly's captured population is session-only; a restart unlocks it.
        if config.basis_user_restriction_mode == BasisUserRestrictionMode::RejoinOnly {
            config.basis_user_restriction_mode = BasisUserRestrictionMode::Normal;
        }
        let permissions = PermissionManager::new(
            base_dir
                .join(ServerConfig::CONFIG_FOLDER_NAME)
                .join("permissions.xml"),
        );
        permissions.set_file_support(config.has_file_support);
        permissions.load_from_xml()?;
        permissions.ensure_defaults();
        permissions.save_to_xml()?;
        let _ = permissions.take_changes();
        let moderation = if config.has_file_support {
            ModerationLists::file_backed(base_dir.join(ServerConfig::CONFIG_FOLDER_NAME))?
        } else {
            ModerationLists::default()
        };
        let admin_runtime = admin_runtime::AdminRuntime::load(base_dir, &config)?;
        let database = if config.has_file_support {
            let database = PersistentDatabase::file_backed(
                base_dir
                    .join(ServerConfig::CONFIG_FOLDER_NAME)
                    .join("database.json"),
            );
            database.load()?;
            database
        } else {
            PersistentDatabase::default()
        };

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
        let (transport, events) = TransportHandle::bind_with_statistics_options(
            bind_addr,
            config.enable_statistics || config.health_include_extended_metrics,
            config.health_include_extended_metrics,
        )
        .await?;
        let lifecycle_events =
            transport.enable_lifecycle_event_queues(MAX_LIFECYCLE_EVENTS, MAX_LIFECYCLE_EVENTS)?;
        let (ordered_events, critical_events) =
            transport.enable_ordered_admission(OrderedAdmissionConfig {
                regular_capacity: MAX_PENDING_ORDERED_EVENTS,
                critical_capacity: MAX_CRITICAL_ORDERED_EVENTS,
                per_lane: MAX_PENDING_ORDERED_PER_LANE,
                per_peer: MAX_PENDING_ORDERED_PER_PEER,
                critical_per_lane: 2,
                critical_per_peer: 2,
                critical_channel: channels::AUTH_IDENTITY,
            })?;
        transport.set_compact_merge_send(config.compact_merged);
        info!("server listening on {}", transport.local_addr()?);

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
                enable_compute_offload: config.enable_compute_offload,
                compute_device: config.compute_device.clone(),
                compute_distance_update_interval_ticks: config
                    .compute_distance_update_interval_ticks
                    .max(1) as u64,
                enable_bsr_profiling: config.enable_bsrprofiling
                    || config.health_include_bsr_profiling,
                collect_extended_metrics: config.health_include_extended_metrics,
            }
            .apply_env_tuning(),
        );
        avatar_sync.set_offloaded_pairs(p2p_broker.offloaded_pairs());

        let state = Self {
            config: Arc::new(RwLock::new(config.clone())),
            config_path: Arc::new(config_path.to_path_buf()),
            transport,
            authenticated_peers: Arc::new(DashMap::new()),
            join_broadcast: Arc::new(Mutex::new(JoinBroadcastState::default())),
            pending_identity: Arc::new(DashMap::new()),
            admission_slots: Arc::new(Semaphore::new(MAX_PENDING_ADMISSIONS)),
            admission_commit: Arc::new(Mutex::new(())),
            admissions_per_ip: Arc::new(Mutex::new(HashMap::new())),
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
            image_governor: Arc::new(image_governor::ImageBandwidthGovernor::default()),
            image_cache: Arc::new(Mutex::new(image_cache::ImageCache::default())),
            jiggle_buckets: Arc::new(DashMap::new()),
            error_report_hashes: Arc::new(DashMap::new()),
            avatar_sync,
            p2p_broker,
            moderation,
            global_state: Arc::new(RwLock::new(GlobalState::from(&config))),
            admin_runtime: Arc::new(admin_runtime),
            statistics: Statistics::new(config.health_include_extended_metrics),
            pending_leaves: Arc::new(Mutex::new(Vec::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            tick_thread: Arc::new(Mutex::new(None)),
            workers: Arc::new(Mutex::new(Vec::new())),
            disconnect_tasks: Arc::new(Mutex::new(Vec::new())),
            identity_timer_tasks: Arc::new(Mutex::new(Vec::new())),
            realtime_threads: Arc::new(Mutex::new(Vec::new())),
        };
        let tick_thread = state
            .avatar_sync
            .spawn_tick_loop(state.transport.clone(), state.shutdown.clone(), {
                let peers = state.authenticated_peers.clone();
                move || peers.iter().map(|entry| *entry.key()).collect()
            })
            .context("starting avatar tick thread")?;
        *state.tick_thread.lock() = Some(tick_thread);
        match realtime::start(&state) {
            Ok(threads) => *state.realtime_threads.lock() = threads,
            Err(error) => {
                state.shutdown().await?;
                return Err(error.context("starting realtime threads"));
            }
        }
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        state.workers.lock().extend([
            spawn_leave_broadcast_loop(state.clone()),
            spawn_image_replay_loop(state.clone()),
            admin_runtime::spawn_permission_updates(state.clone()),
            tokio::spawn(event_loop(
                state.clone(),
                events,
                ordered_events,
                critical_events,
                lifecycle_events,
                shutdown_rx,
            )),
        ]);
        Ok((state, shutdown_tx))
    }

    pub fn player_count(&self) -> usize {
        self.authenticated_peers.len()
    }

    /// Image relays refused and payload bytes avoided, including fan-out.
    pub fn image_egress_dropped(&self) -> (u64, u64) {
        self.image_governor.dropped()
    }

    /// Cached images, complete images, and retained payload/chunk-slot bytes.
    pub fn image_cache_stats(&self) -> (usize, usize, u64) {
        let cache = self.image_cache.lock();
        (cache.count(), cache.servable_count(), cache.total_bytes())
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
                enable_compute_offload: config.enable_compute_offload,
                compute_device: config.compute_device.clone(),
                compute_distance_update_interval_ticks: config
                    .compute_distance_update_interval_ticks
                    .max(1) as u64,
                enable_bsr_profiling: config.enable_bsrprofiling
                    || config.health_include_bsr_profiling,
                collect_extended_metrics: config.health_include_extended_metrics,
            }
            .apply_env_tuning(),
        );
        self.transport.set_statistics_enabled(
            config.enable_statistics || config.health_include_extended_metrics,
        );
        self.transport
            .set_extended_statistics_enabled(config.health_include_extended_metrics);
        self.transport.set_compact_merge_send(config.compact_merged);
        self.statistics
            .set_enabled(config.health_include_extended_metrics);

        let previous_globals = self.global_state.read().clone();
        admin_runtime::refresh_rejoin_population(
            self,
            previous_globals.restriction_mode,
            config.basis_user_restriction_mode,
        );
        let mut globals = GlobalState::from(&config);
        globals.headless_audio_off = previous_globals.headless_audio_off;
        globals.opus_packet_loss_percent = previous_globals.opus_packet_loss_percent;
        globals.opus_frame_duration_ms = previous_globals.opus_frame_duration_ms;
        globals.global_opus_bitrate = previous_globals.global_opus_bitrate;
        *self.global_state.write() = globals;
    }

    pub async fn refresh_runtime_config_live(&self) {
        self.refresh_runtime_config();
        broadcast_lock_state(self).await;
        admin_runtime::broadcast_locomotion_policy(self).await;
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
        let players = self.player_count();
        if !self.config.read().health_include_extended_metrics {
            return if verbose {
                format!(
                    "Server is running and healthy\nPlayers: {players}\nExtended metrics: disabled (set HealthIncludeExtendedMetrics=true to collect them)"
                )
            } else {
                format!("Server is running and healthy. Players: {players}")
            };
        }

        let transport = self.transport.stats_snapshot();
        let avatar = self.avatar_sync.stats();
        let app = self.statistics.snapshot();
        if !verbose {
            return format!(
                "Server is running and healthy. Players: {} PendingReliable: {} QueuedReliable: {} AppIn: {} AppOut: {} RawIn: {} RawOut: {} AvatarIn: {} AvatarOut: {} ProtocolErrors: {} AvatarReceived: {} AvatarCoalesced: {} AvatarRejected: {} AvatarProcessed: {}",
                players,
                self.transport.pending_reliable_count(),
                self.transport.queued_reliable_count(),
                app.inbound_packets,
                app.outbound_packets,
                transport.raw_packets_received,
                transport.raw_packets_sent,
                avatar.inbound_updates,
                avatar.outbound_messages,
                app.protocol_errors,
                app.avatar_received,
                app.avatar_coalesced,
                app.avatar_rejected,
                app.avatar_processed,
            );
        }
        format!(
            "Server is running and healthy\nPlayers: {}\nReliable: pending={} queued={} window_fills={} retransmits={} dispatch_passes={} peers_visited={} acks_in={} acks_released={} acks_unknown_chan={} window_stalls={}\nApp messages: inbound={} outbound={} protocol_errors={}\nAvatar input: received={} coalesced={} rejected={} processed={}\nRaw UDP: packets_in={} packets_out={} bytes_in={} bytes_out={} would_block={}\nAvatar sync: inbound_updates={} outbound_messages={} outbound_logical_avatar_sends={} outbound_batches={} active_states={} pending_updates={} receiver_slices={}\nAvatar timing: ticks={} avg_tick_us={} smooth_tick_us={} avg_build_us={} avg_flush_us={} max_tick_us={} receiver_cycle_ms={} cycle_budget_ms={} tick_budget_ms={}",
            players,
            self.transport.pending_reliable_count(),
            self.transport.queued_reliable_count(),
            transport.reliable_window_fills,
            transport.reliable_retransmits,
            transport.reliable_dispatch_passes,
            transport.reliable_peers_visited,
            transport.reliable_acks_received,
            transport.reliable_acks_released,
            transport.reliable_acks_unknown_channel,
            transport.reliable_window_stalls,
            app.inbound_packets,
            app.outbound_packets,
            app.protocol_errors,
            app.avatar_received,
            app.avatar_coalesced,
            app.avatar_rejected,
            app.avatar_processed,
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
            avatar
                .build_micros
                .checked_div(avatar.tick_count)
                .unwrap_or(0),
            avatar
                .flush_micros
                .checked_div(avatar.tick_count)
                .unwrap_or(0),
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
        // Save before waiting on any worker: native work or an in-flight handler
        // may never return. The console bounds this entire lifecycle, including
        // synchronous persistence and runtime teardown, with a watchdog.
        let initial_save = self.flush_shutdown_state();
        if let Err(err) = &initial_save {
            error!("initial shutdown persistence failed: {err:#}");
        }
        let workers = std::mem::take(&mut *self.workers.lock());
        let mut worker_result = Ok(());
        for worker in workers {
            if let Err(err) = worker.await {
                warn!("server worker failed to join: {err}");
                worker_result = Err(anyhow::anyhow!("server worker failed to join: {err}"));
            }
        }
        // Workers can no longer create identity timers. Dropping pending entries
        // cancels timers that have not started disconnecting; join every timer so
        // any cleanup it already claimed is visible before draining cleanup tasks.
        self.pending_identity.clear();
        let identity_timer_tasks = std::mem::take(&mut *self.identity_timer_tasks.lock());
        for task in identity_timer_tasks {
            if let Err(err) = task.await {
                warn!("identity timer failed to join: {err}");
                worker_result = Err(anyhow::anyhow!("identity timer failed to join: {err}"));
            }
        }
        // Retire every transport session, including accepted peers still waiting
        // for identity verification. No admission worker remains to add sessions.
        let sessions = self
            .transport
            .peer_snapshots()
            .into_iter()
            .filter_map(|peer| self.transport.peer_session(peer.id))
            .collect::<Vec<_>>();
        for session in sessions {
            let retired = match self
                .transport
                .disconnect_session(&session, "Server shutting down")
                .await
            {
                Ok(retired) => retired,
                Err(err) => {
                    // A disconnect error must not skip the remaining teardown or final save.
                    // Only Ok(true) grants ownership of the session cleanup.
                    error!("failed to retire session during shutdown: {err:#}");
                    if worker_result.is_ok() {
                        worker_result = Err(anyhow::anyhow!(
                            "disconnecting session during shutdown: {err:#}"
                        ));
                    }
                    false
                }
            };
            if retired {
                session.wait_for_read_leases().await;
                handle_disconnect(self, &session, DisconnectReason::Remote).await;
            }
        }
        let disconnect_tasks = std::mem::take(&mut *self.disconnect_tasks.lock());
        for task in disconnect_tasks {
            if let Err(err) = task.await {
                warn!("disconnect cleanup task failed to join: {err}");
                worker_result = Err(anyhow::anyhow!(
                    "disconnect cleanup task failed to join: {err}"
                ));
            }
        }
        // Accepted event handlers and all disconnect cleanup have now finished.
        self.image_governor.reset();
        self.image_cache.lock().reset();
        let final_save = self.flush_shutdown_state();
        if let Err(err) = &final_save {
            error!("final shutdown persistence failed: {err:#}");
        }
        let tick_thread = self.tick_thread.lock().take();
        let realtime_threads = std::mem::take(&mut *self.realtime_threads.lock());
        let realtime_result = tokio::task::spawn_blocking(move || {
            let mut failed = false;
            for thread in realtime_threads {
                failed |= thread.join().is_err();
            }
            if failed {
                Err(anyhow::anyhow!("realtime thread panicked"))
            } else {
                Ok(())
            }
        })
        .await
        .context("joining realtime threads")?;
        let tick_result = if let Some(thread) = tick_thread {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .context("joining avatar tick task")
                .and_then(|result| {
                    result.map_err(|_| anyhow::anyhow!("avatar tick thread panicked"))
                })
        } else {
            Ok(())
        };
        // Dropping the GPU channels stops new submissions; joining readback or
        // driver cleanup is deliberately still covered by the console watchdog.
        self.avatar_sync.stop_compute_offload().await;
        worker_result?;
        tick_result?;
        realtime_result?;
        final_save?;
        initial_save?;
        Ok(())
    }

    fn flush_shutdown_state(&self) -> Result<()> {
        let permissions = self.permissions.flush_pending_save();
        let database = self.database.shutdown();
        // Attempt both saves even if one fails.
        database?;
        permissions?;
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

fn image_animation_allowed(state: &ServerState, peer: PeerId) -> bool {
    !state.global_state.read().gifs_locked
        || peer_has_permission(
            state,
            peer,
            basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
        )
}

async fn send_image_payload(
    state: &ServerState,
    session: &PeerSession,
    pending: image_governor::PendingPayload,
) {
    let Some(message_index) = state.net_ids.find(image_cache::IMAGE_MANAGER_IDENTIFIER) else {
        return;
    };
    // An owner may leave while another peer's replay is queued. Clients already
    // removed that owner's cards; do not recreate them after the disconnect.
    if !state.authenticated_peers.contains_key(&pending.owner) {
        return;
    }
    let message = ServerSceneDataMessage {
        player_id: pending.owner,
        scene_data_message: RemoteSceneDataMessage {
            message_index,
            payload: pending.payload.to_vec(),
        },
    };
    let mut writer = NetWriter::new();
    if let Err(err) = message.serialize(&mut writer) {
        warn!("failed to serialize image cache replay: {err}");
        return;
    }
    if state
        .transport
        .send_session(
            session,
            channels::SCENE,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
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

async fn deliver_image_cache_sends(state: &ServerState, sends: Vec<image_cache::CacheSend>) {
    let mut replays = HashMap::<PeerId, (PeerSession, Vec<image_governor::PendingPayload>)>::new();
    for send in sends {
        let Some(session) = state
            .authenticated_peers
            .get(&send.recipient)
            .and_then(|peer| peer.session.clone())
        else {
            continue;
        };
        let pending = image_governor::PendingPayload {
            owner: send.owner,
            payload: send.payload,
        };
        if send.paced {
            replays
                .entry(send.recipient)
                .or_insert_with(|| (session, Vec::new()))
                .1
                .push(pending);
        } else {
            send_image_payload(state, &session, pending).await;
        }
    }
    for (peer, (session, payloads)) in replays {
        let inline = state.image_governor.enqueue_replay(
            peer,
            session.clone(),
            payloads,
            &state.config.read(),
            Instant::now(),
        );
        for pending in inline {
            send_image_payload(state, &session, pending).await;
        }
    }
}

fn spawn_image_replay_loop(state: ServerState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(25));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            if state.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let batches = state
                .image_governor
                .pump(&state.config.read(), Instant::now());
            for (session, payloads) in batches {
                for pending in payloads {
                    send_image_payload(&state, &session, pending).await;
                }
            }
        }
    })
}

fn spawn_leave_broadcast_loop(state: ServerState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(LEAVE_BROADCAST_INTERVAL).await;
            if state.shutdown.load(Ordering::Relaxed) {
                break;
            }
            flush_pending_leaves(&state).await;
        }
    })
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

fn serialize_server_ready(peer_id: PeerId, ready: &ReadyMessage) -> Result<Vec<u8>> {
    let message = ServerReadyMessage {
        local_ready_message: ready.clone(),
        player_id_message: basis_protocol::messages::PlayerIdMessage { player_id: peer_id },
    };
    let mut writer = NetWriter::new();
    message.serialize(&mut writer)?;
    Ok(writer.into_vec())
}

fn frame_join_records(records: &[Arc<JoinBroadcastRecord>]) -> Result<Vec<u8>> {
    let count = u16::try_from(records.len()).expect("join batch count fits u16");
    let mut payload = Vec::new();
    for record in records {
        payload.extend_from_slice(&record.payload.read());
    }
    let mut writer = NetWriter::with_capacity(payload.len() + 32);
    ServerReadyBatchMessage { count, payload }.serialize(&mut writer)?;
    Ok(writer.into_vec())
}

async fn flush_join_batches(state: &ServerState) -> Result<()> {
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
            let framed = match framed_by_batch.entry(key) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(frame_join_records(&records)?)
                }
            };
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
    Ok(())
}

async fn event_loop(
    state: ServerState,
    events: mpsc::Receiver<ServerEvent>,
    ordered_events: mpsc::Receiver<OrderedEvent>,
    critical_events: mpsc::Receiver<OrderedEvent>,
    lifecycle_events: basis_transport::LifecycleEventReceivers,
    shutdown: oneshot::Receiver<()>,
) {
    event_loop_with_handler(
        state,
        events,
        ordered_events,
        critical_events,
        lifecycle_events,
        shutdown,
        |state, event| async move { handle_event(&state, event).await },
    )
    .await;
}

async fn event_loop_with_handler<F, Fut>(
    state: ServerState,
    mut events: mpsc::Receiver<ServerEvent>,
    mut ordered_events: mpsc::Receiver<OrderedEvent>,
    mut critical_events: mpsc::Receiver<OrderedEvent>,
    lifecycle_events: basis_transport::LifecycleEventReceivers,
    mut shutdown: oneshot::Receiver<()>,
    handle: F,
) where
    F: Fn(Arc<ServerState>, ServerEvent) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    // Handlers share this single state allocation instead of cloning every field
    // of ServerState for each spawned event.
    let state = Arc::new(state);
    let worker_limit = std::thread::available_parallelism()
        .map(|count| (count.get() * 4).clamp(8, 256))
        .unwrap_or(32);
    let workers = Arc::new(Semaphore::new(worker_limit));
    // Authentication cannot wait for ordinary handlers to release all worker slots.
    let critical_workers = Arc::new(Semaphore::new(2));
    let diagnostics = event_diagnostics::EventDiagnostics::start_from_env(&state, worker_limit + 6);
    let (lifecycle_stop, lifecycle_shutdown) = oneshot::channel();
    let lifecycle = tokio::spawn(lifecycle_loop(
        state.clone(),
        lifecycle_events,
        lifecycle_shutdown,
        diagnostics.clone(),
        handle.clone(),
    ));
    let mut handlers = tokio::task::JoinSet::<()>::new();
    let mut ordered_task_keys = HashMap::<tokio::task::Id, OrderedLaneKey>::new();
    let mut ordered_sessions = HashMap::<(PeerId, u8), (PeerSession, u64)>::new();
    let mut next_session_generation = 1u64;
    let mut ordered_queue = OrderedHandlerQueue::default();
    let mut critical_queue = OrderedHandlerQueue::default();
    let mut ingress_open = [true, true, true];
    let mut join_flush = tokio::time::interval(JOIN_BATCH_FLUSH_INTERVAL);
    join_flush.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    join_flush.tick().await;
    while !state.shutdown.load(Ordering::Relaxed) {
        if !ingress_open.iter().any(|open| *open) {
            break;
        }
        let ordered_has_capacity = ordered_queue.pending() < MAX_PENDING_ORDERED_EVENTS;
        let envelope = tokio::select! {
            completed = handlers.join_next_with_id(), if !handlers.is_empty() => {
                if let Some(completed) = completed {
                    let id = match completed {
                        Ok((id, ())) => id,
                        Err(err) => { warn!("event handler failed to join: {err}"); err.id() },
                    };
                    if let Some(key) = ordered_task_keys.remove(&id) {
                        let queue = if key.1 == channels::AUTH_IDENTITY {
                            &mut critical_queue
                        } else {
                            &mut ordered_queue
                        };
                        queue.complete(key);
                        if !queue.contains(key)
                            && ordered_sessions
                                .get(&(key.0, key.1))
                                .is_some_and(|(_, generation)| *generation == key.2)
                        {
                            ordered_sessions.remove(&(key.0, key.1));
                        }
                    }
                }
                spawn_ready_ordered(
                    &mut ordered_queue,
                    &mut handlers,
                    &mut ordered_task_keys,
                    workers.clone(),
                    &state,
                    diagnostics.as_ref(),
                    handle.clone(),
                );
                spawn_ready_ordered(
                    &mut critical_queue,
                    &mut handlers,
                    &mut ordered_task_keys,
                    critical_workers.clone(),
                    &state,
                    diagnostics.as_ref(),
                    handle.clone(),
                );
                continue;
            }
            _ = join_flush.tick() => {
                prune_ordered_sessions(
                    &state.transport,
                    &mut ordered_queue,
                    &mut critical_queue,
                    &mut ordered_sessions,
                );
                if let Err(err) = flush_join_batches(&state).await {
                    error!("join batch serialization failed: {err:#}");
                }
                continue;
            }
            _ = &mut shutdown => {
                break;
            }
            event = events.recv(), if ingress_open[0] && workers.available_permits() > 0 => {
                match event {
                    Some(event) => event.into(),
                    None => {
                        ingress_open[0] = false;
                        continue;
                    }
                }
            }
            event = ordered_events.recv(), if ingress_open[1] && ordered_has_capacity => {
                match event {
                    Some(event) => event,
                    None => {
                        ingress_open[1] = false;
                        continue;
                    }
                }
            }
            event = critical_events.recv(), if ingress_open[2] && critical_queue.pending() < MAX_CRITICAL_ORDERED_EVENTS => {
                match event {
                    Some(event) => event,
                    None => {
                        ingress_open[2] = false;
                        continue;
                    }
                }
            }

        };
        if let Some(diagnostics) = &diagnostics {
            diagnostics.record_queue_depth(events.len());
        }
        let event = &envelope.event;
        if let ServerEvent::PeerDisconnected { session, .. } = event {
            discard_ordered_session(&mut ordered_queue, &mut ordered_sessions, session);
            discard_ordered_session(&mut critical_queue, &mut ordered_sessions, session);
        }
        if is_high_frequency_inline_event(event)
            && !matches!(
                event,
                ServerEvent::Message {
                    delivery: DeliveryMethod::ReliableOrdered,
                    ..
                }
            )
        {
            if let Err(err) = handle(state.clone(), envelope.event).await {
                error!("server event failed: {err:#}");
            }
            continue;
        }
        if let ServerEvent::Message {
            peer,
            session,
            channel,
            delivery: DeliveryMethod::ReliableOrdered,
            ..
        } = event
        {
            if !state.transport.is_current_session(session) {
                discard_ordered_session(&mut ordered_queue, &mut ordered_sessions, session);
                discard_ordered_session(&mut critical_queue, &mut ordered_sessions, session);
                continue;
            }
            let base = (*peer, *channel);
            let generation = ordered_lane_generation(
                base,
                session,
                &mut next_session_generation,
                &mut ordered_sessions,
                &mut ordered_queue,
                &mut critical_queue,
            );
            let key = (base.0, base.1, generation);
            if *channel == channels::AUTH_IDENTITY {
                // Admission is reserved before ACK and retained through handler
                // completion. Temporary overload leaves packets for retry.
                critical_queue.enqueue(key, envelope);
                spawn_ready_ordered(
                    &mut critical_queue,
                    &mut handlers,
                    &mut ordered_task_keys,
                    critical_workers.clone(),
                    &state,
                    diagnostics.as_ref(),
                    handle.clone(),
                );
            } else {
                // Admission is reserved before ACK and retained through handler
                // completion. Temporary overload leaves packets for retry.
                ordered_queue.enqueue(key, envelope);
                spawn_ready_ordered(
                    &mut ordered_queue,
                    &mut handlers,
                    &mut ordered_task_keys,
                    workers.clone(),
                    &state,
                    diagnostics.as_ref(),
                    handle.clone(),
                );
            }
            continue;
        }
        // Ordinary ingress is selected only when its worker class has capacity.
        let permit = workers
            .clone()
            .try_acquire_owned()
            .expect("ordinary ingress selected with worker capacity");
        spawn_event_handler(
            &mut handlers,
            &state,
            diagnostics.as_ref(),
            handle.clone(),
            envelope,
            permit,
        );
    }
    // Stop admission, then finish accepted handlers before final persistence.
    events.close();
    ordered_events.close();
    critical_events.close();
    let _ = lifecycle_stop.send(());
    while !handlers.is_empty() || ordered_queue.pending() > 0 || critical_queue.pending() > 0 {
        prune_ordered_sessions(
            &state.transport,
            &mut ordered_queue,
            &mut critical_queue,
            &mut ordered_sessions,
        );
        spawn_ready_ordered(
            &mut ordered_queue,
            &mut handlers,
            &mut ordered_task_keys,
            workers.clone(),
            &state,
            diagnostics.as_ref(),
            handle.clone(),
        );
        spawn_ready_ordered(
            &mut critical_queue,
            &mut handlers,
            &mut ordered_task_keys,
            critical_workers.clone(),
            &state,
            diagnostics.as_ref(),
            handle.clone(),
        );
        if let Some(result) = handlers.join_next_with_id().await {
            match result {
                Ok((id, ())) => {
                    if let Some(key) = ordered_task_keys.remove(&id) {
                        if key.1 == channels::AUTH_IDENTITY {
                            critical_queue.complete(key);
                        } else {
                            ordered_queue.complete(key);
                        }
                    }
                }
                Err(err) => {
                    if let Some(key) = ordered_task_keys.remove(&err.id()) {
                        if key.1 == channels::AUTH_IDENTITY {
                            critical_queue.complete(key);
                        } else {
                            ordered_queue.complete(key);
                        }
                    }
                    warn!("event handler failed to join: {err}");
                }
            }
        }
    }
    if let Err(err) = lifecycle.await {
        warn!("lifecycle dispatcher failed to join: {err}");
    }
}

async fn lifecycle_loop<F, Fut>(
    state: Arc<ServerState>,
    mut events: basis_transport::LifecycleEventReceivers,
    mut shutdown: oneshot::Receiver<()>,
    diagnostics: Option<Arc<event_diagnostics::EventDiagnostics>>,
    handle: F,
) where
    F: Fn(Arc<ServerState>, ServerEvent) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    // Lease waits occupy only the disconnect class. Both live tasks and queued
    // events are bounded; no task is spawned merely to wait for a worker permit.
    let connections = Arc::new(Semaphore::new(LIFECYCLE_WORKERS_PER_CLASS));
    let disconnects = Arc::new(Semaphore::new(LIFECYCLE_WORKERS_PER_CLASS));
    let mut handlers = tokio::task::JoinSet::new();
    let mut open = [true, true];
    while !state.shutdown.load(Ordering::Relaxed) && open.iter().any(|open| *open) {
        let (event, workers) = tokio::select! {
            _ = &mut shutdown => break,
            completed = handlers.join_next(), if !handlers.is_empty() => {
                if let Some(Err(err)) = completed {
                    warn!("lifecycle handler failed to join: {err}");
                }
                continue;
            }
            event = events.connections.recv(),
                if open[0] && connections.available_permits() > 0
                    && handlers.len() < LIFECYCLE_WORKERS_PER_CLASS * 2 => {
                match event {
                    Some(event) => (event, &connections),
                    None => { open[0] = false; continue; }
                }
            }
            event = events.disconnects.recv(),
                if open[1] && disconnects.available_permits() > 0
                    && handlers.len() < LIFECYCLE_WORKERS_PER_CLASS * 2 => {
                match event {
                    Some(event) => (event, &disconnects),
                    None => { open[1] = false; continue; }
                }
            }
        };
        let permit = workers
            .clone()
            .try_acquire_owned()
            .expect("lifecycle capacity selected");
        spawn_event_handler(
            &mut handlers,
            &state,
            diagnostics.as_ref(),
            handle.clone(),
            event.into(),
            permit,
        );
    }
    events.connections.close();
    events.disconnects.close();
    let mut drained = false;
    while !handlers.is_empty() || !drained {
        while !drained && handlers.len() < LIFECYCLE_WORKERS_PER_CLASS * 2 {
            let Ok(permit) = disconnects.clone().try_acquire_owned() else {
                break;
            };
            let Some(event) = events.disconnects.recv().await else {
                drained = true;
                break;
            };
            // Retirement already committed; finish cleanup before persistence.
            // Closed connection ingress admits no queued new requests.
            if matches!(event, ServerEvent::PeerDisconnected { .. }) {
                spawn_event_handler(
                    &mut handlers,
                    &state,
                    diagnostics.as_ref(),
                    handle.clone(),
                    event.into(),
                    permit,
                );
            }
        }
        if let Some(Err(err)) = handlers.join_next().await {
            warn!("lifecycle handler failed to join: {err}");
        }
    }
}

fn spawn_event_handler<F, Fut>(
    handlers: &mut tokio::task::JoinSet<()>,
    state: &Arc<ServerState>,
    diagnostics: Option<&Arc<event_diagnostics::EventDiagnostics>>,
    handle: F,
    envelope: OrderedEvent,
    permit: tokio::sync::OwnedSemaphorePermit,
) where
    F: Fn(Arc<ServerState>, ServerEvent) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    let OrderedEvent { event, admission } = envelope;
    let mut diagnostic_guard = diagnostics.map(|d| d.spawned(&event));
    let state = state.clone();
    let task = async move {
        let _permit = permit;
        let _admission = admission;
        if let Some(guard) = &mut diagnostic_guard {
            guard.started();
        }
        if let Err(err) = handle(state, event).await {
            error!("server event failed: {err:#}");
        }
    };
    if let Some(diagnostics) = diagnostics {
        diagnostics.record_task_size(std::mem::size_of_val(&task));
    }
    handlers.spawn(task);
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn receive_control_event(
    events: &mut mpsc::Receiver<ServerEvent>,
    ordered_events: &mut mpsc::Receiver<OrderedEvent>,
    critical_events: &mut mpsc::Receiver<OrderedEvent>,
    open: &mut [bool; 3],
    ordered_has_capacity: bool,
    critical_has_capacity: bool,
    regular_has_capacity: bool,
) -> Option<OrderedEvent> {
    loop {
        if !open.iter().any(|open| *open) {
            return None;
        }
        tokio::select! {
            event = events.recv(), if open[0] && regular_has_capacity => match event {
                Some(event) => return Some(event.into()),
                None => open[0] = false,
            },
            event = ordered_events.recv(), if open[1] && ordered_has_capacity => match event {
                Some(event) => return Some(event),
                None => open[1] = false,
            },
            event = critical_events.recv(), if open[2] && critical_has_capacity => match event {
                Some(event) => return Some(event),
                None => open[2] = false,
            },
            // Handler completion cancels this wait when admission is paused.
            else => std::future::pending::<()>().await,
        }
    }
}

const MAX_PENDING_ORDERED_EVENTS: usize = 4096;
const MAX_CRITICAL_ORDERED_EVENTS: usize = 128;
const MAX_LIFECYCLE_EVENTS: usize = 256;
const LIFECYCLE_WORKERS_PER_CLASS: usize = 2;
const MAX_PENDING_ORDERED_PER_LANE: usize = 128;
const MAX_PENDING_ORDERED_PER_PEER: usize = 256;
type OrderedLaneKey = (PeerId, u8, u64);

struct OrderedHandlerQueue<E = OrderedEvent> {
    lanes: HashMap<OrderedLaneKey, OrderedLane<E>>,
    ready: VecDeque<OrderedLaneKey>,
    pending: usize,
}

struct OrderedLane<E> {
    events: VecDeque<E>,
    running: bool,
}

impl<E> Default for OrderedHandlerQueue<E> {
    fn default() -> Self {
        Self {
            lanes: HashMap::new(),
            ready: VecDeque::new(),
            pending: 0,
        }
    }
}

impl<E> Default for OrderedLane<E> {
    fn default() -> Self {
        Self {
            events: VecDeque::new(),
            running: false,
        }
    }
}

impl<E> OrderedHandlerQueue<E> {
    // Every production envelope already owns its transport admission budget. Keep
    // accepted data until processing or session retirement; never drop an ACKed event
    // because a second layer disagrees about the session/lane key.
    fn enqueue(&mut self, key: OrderedLaneKey, event: E) {
        let lane = self.lanes.entry(key).or_default();
        if !lane.running && lane.events.is_empty() {
            self.ready.push_back(key);
        }
        lane.events.push_back(event);
        self.pending += 1;
    }

    fn start_next(&mut self) -> Option<(OrderedLaneKey, E)> {
        while let Some(key) = self.ready.pop_front() {
            let Some(lane) = self.lanes.get_mut(&key) else {
                continue;
            };
            if lane.running {
                continue;
            }
            let Some(event) = lane.events.pop_front() else {
                self.lanes.remove(&key);
                continue;
            };
            lane.running = true;
            self.pending -= 1;
            return Some((key, event));
        }
        None
    }

    fn complete(&mut self, key: OrderedLaneKey) {
        let Some(lane) = self.lanes.get_mut(&key) else {
            return;
        };
        lane.running = false;
        if lane.events.is_empty() {
            self.lanes.remove(&key);
        } else {
            self.ready.push_back(key);
        }
    }

    fn discard_matching(&mut self, mut matches: impl FnMut(&E) -> bool) {
        let keys: Vec<_> = self.lanes.keys().copied().collect();
        for key in keys {
            let lane = self.lanes.get_mut(&key).unwrap();
            let before = lane.events.len();
            lane.events.retain(|event| !matches(event));
            let removed = before - lane.events.len();
            let empty = lane.events.is_empty() && !lane.running;
            self.pending -= removed;
            if empty {
                self.lanes.remove(&key);
            }
        }
        self.compact_ready();
    }

    fn discard_lane(&mut self, key: OrderedLaneKey) -> bool {
        let Some(lane) = self.lanes.get_mut(&key) else {
            return false;
        };
        let changed = !lane.events.is_empty() || !lane.running;
        self.pending -= lane.events.len();
        lane.events.clear();
        if !lane.running {
            self.lanes.remove(&key);
        }
        changed
    }

    fn compact_ready(&mut self) {
        self.ready.retain(|key| self.lanes.contains_key(key));
    }

    fn pending(&self) -> usize {
        self.pending
    }

    fn contains(&self, key: OrderedLaneKey) -> bool {
        self.lanes.contains_key(&key)
    }
}

fn ordered_lane_generation(
    base: (PeerId, u8),
    session: &PeerSession,
    next_generation: &mut u64,
    sessions: &mut HashMap<(PeerId, u8), (PeerSession, u64)>,
    ordered: &mut OrderedHandlerQueue,
    critical: &mut OrderedHandlerQueue,
) -> u64 {
    if let Some((old_session, generation)) = sessions.get(&base) {
        if old_session.same_connection(session) {
            return *generation;
        }
        // Retire the old pending lane before its last registry entry is replaced.
        // A running head retains its key until completion and cannot erase the
        // new generation when its JoinSet result is collected.
        let old_key = (base.0, base.1, *generation);
        if ordered.discard_lane(old_key) {
            ordered.compact_ready();
        }
        if critical.discard_lane(old_key) {
            critical.compact_ready();
        }
    }
    let generation = *next_generation;
    *next_generation = next_generation.wrapping_add(1).max(1);
    sessions.insert(base, (session.clone(), generation));
    generation
}

fn prune_ordered_sessions(
    transport: &TransportHandle,
    ordered: &mut OrderedHandlerQueue,
    critical: &mut OrderedHandlerQueue,
    sessions: &mut HashMap<(PeerId, u8), (PeerSession, u64)>,
) {
    let mut changed = false;
    sessions.retain(|base, (session, generation)| {
        let key = (base.0, base.1, *generation);
        if !transport.is_current_session(session) {
            changed |= ordered.discard_lane(key);
            changed |= critical.discard_lane(key);
        }
        ordered.contains(key) || critical.contains(key)
    });
    if changed {
        // Compact once per pass, so repeated retirement cannot accumulate stale
        // ready keys while ordinary workers are blocked.
        ordered.compact_ready();
        critical.compact_ready();
    }
}

fn discard_ordered_session(
    queue: &mut OrderedHandlerQueue,
    sessions: &mut HashMap<(PeerId, u8), (PeerSession, u64)>,
    session: &PeerSession,
) {
    queue.discard_matching(|event| {
        matches!(&event.event,
            ServerEvent::Message { session: queued, .. } if queued.same_connection(session)
        )
    });
    sessions.retain(|_, (queued, _)| !queued.same_connection(session));
}

fn spawn_ready_ordered<F, Fut>(
    queue: &mut OrderedHandlerQueue<OrderedEvent>,
    handlers: &mut tokio::task::JoinSet<()>,
    task_keys: &mut HashMap<tokio::task::Id, OrderedLaneKey>,
    workers: Arc<Semaphore>,
    state: &Arc<ServerState>,
    diagnostics: Option<&Arc<event_diagnostics::EventDiagnostics>>,
    handle: F,
) where
    F: Fn(Arc<ServerState>, ServerEvent) -> Fut + Clone + Send + 'static,
    Fut: std::future::Future<Output = Result<()>> + Send + 'static,
{
    spawn_ready_ordered_with(queue, handlers, task_keys, workers, |envelope, permit| {
        let OrderedEvent { event, admission } = envelope;
        let state = state.clone();
        let handle = handle.clone();
        let mut diagnostic_guard = diagnostics.map(|d| d.spawned(&event));
        let task = async move {
            let _permit = permit;
            let _admission = admission;
            if let Some(guard) = &mut diagnostic_guard {
                guard.started();
            }
            if let Err(err) = handle(state, event).await {
                error!("server event failed: {err:#}");
            }
        };
        if let Some(diagnostics) = diagnostics {
            diagnostics.record_task_size(std::mem::size_of_val(&task));
        }
        task
    });
}

// Waiting lane followers stay as event data; only runnable heads become tasks.
fn spawn_ready_ordered_with<E, F, Fut>(
    queue: &mut OrderedHandlerQueue<E>,
    handlers: &mut tokio::task::JoinSet<()>,
    task_keys: &mut HashMap<tokio::task::Id, OrderedLaneKey>,
    workers: Arc<Semaphore>,
    mut handle: F,
) where
    E: Send + 'static,
    F: FnMut(E, OwnedSemaphorePermit) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    while queue.pending() > 0 {
        let Ok(permit) = workers.clone().try_acquire_owned() else {
            break;
        };
        let Some((key, event)) = queue.start_next() else {
            drop(permit);
            break;
        };
        let id = handlers.spawn(handle(event, permit)).id();
        task_keys.insert(id, key);
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
        ServerEvent::PeerDisconnected {
            peer,
            session,
            reason,
        } => {
            if peer != session.peer_id() || !state.transport.close_session_admission(&session) {
                return Ok(());
            }
            session.wait_for_read_leases().await;
            handle_disconnect(state, &session, reason).await;
            Ok(())
        }
        ServerEvent::Message {
            peer,
            session,
            channel,
            delivery,
            payload,
        } => {
            if peer != session.peer_id() {
                return Ok(());
            }
            let Some(_lease) = session.try_read_lease() else {
                return Ok(());
            };
            if !state.transport.is_current_session(&session) {
                return Ok(());
            }
            handle_message(
                state,
                peer,
                Some(&session),
                channel,
                delivery,
                payload,
                true,
                false,
            )
            .await
        }
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

fn structured_reject_payload(kind: u8, aux0: u16, aux1: u16, message: &str) -> Result<Vec<u8>> {
    let mut writer = NetWriter::new();
    writer.put_u32(channels::REJECT_MAGIC);
    writer.put_u8(kind);
    writer.put_u16(aux0);
    writer.put_u16(aux1);
    writer.put_string(message)?;
    Ok(writer.into_vec())
}

async fn reject_structured(
    state: &ServerState,
    request: &basis_transport::ConnectionRequest,
    kind: u8,
    aux0: u16,
    aux1: u16,
    message: &str,
) -> Result<()> {
    let payload = structured_reject_payload(kind, aux0, aux1, message)?;
    state.transport.reject_payload(request, &payload).await?;
    Ok(())
}

async fn handle_connection_request(
    state: &ServerState,
    remote_addr: SocketAddr,
    payload: Bytes,
    request: basis_transport::ConnectionRequest,
) -> Result<()> {
    if !state.transport.is_pending_request(&request) {
        return Ok(());
    }
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

    if config.use_auth_identity {
        if let Err(error) = did_key_verifying_key(&ready.player_meta_data_message.player_uuid) {
            state
                .transport
                .reject(&request, &format!("Unsupported identity: {error}"))
                .await?;
            return Ok(());
        }
    } else if config.basis_user_restriction_mode == BasisUserRestrictionMode::RejoinOnly {
        state
            .transport
            .reject(
                &request,
                "Rejoin-only mode requires authenticated identity.",
            )
            .await?;
        return Ok(());
    } else if let Some(reason) = admission_rejection(state, &ready) {
        state.transport.reject(&request, reason).await?;
        return Ok(());
    }

    let Some(admission_slot) = reserve_admission(state, remote_addr.ip()) else {
        reject_structured(
            state,
            &request,
            channels::REJECT_KIND_SERVER_FULL,
            0,
            0,
            "Server admission capacity reached. Please try again later.",
        )
        .await?;
        return Ok(());
    };
    // Validation can await storage and policy work. A newer ConnectRequest for
    // this address may have superseded this queued event in the meantime.
    if !state.transport.is_pending_request(&request) {
        return Ok(());
    }
    let session = match state.transport.accept_session(&request).await {
        Ok(session) => session,
        Err(basis_transport::TransportError::PeerIdExhausted) => {
            reject_structured(
                state,
                &request,
                channels::REJECT_KIND_SERVER_FULL,
                0,
                0,
                "Server connection capacity reached. Please try again later.",
            )
            .await?;
            return Ok(());
        }
        Err(basis_transport::TransportError::StaleAdmission) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    let peer_id = session.peer_id();
    let Some(_session_lease) = session.try_read_lease() else {
        return Ok(());
    };
    if !state.transport.is_current_session(&session) {
        return Ok(());
    }
    if config.use_auth_identity {
        let challenge_ttl = identity_challenge_ttl(
            state.player_count(),
            config.auth_validation_time_out_miliseconds,
        );
        let mut challenge = vec![0; 32];
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut challenge);
        let expires_at = Instant::now() + challenge_ttl;
        let (timeout_cancel, mut timeout_cancelled) = oneshot::channel();
        state.pending_identity.insert(
            peer_id,
            PendingIdentity {
                session: session.clone(),
                ready,
                challenge: challenge.clone(),
                expires_at,
                _timeout_cancel: timeout_cancel,
                _admission_slot: admission_slot,
            },
        );
        let timeout_challenge = challenge.clone();
        let pending_identity = state.pending_identity.clone();
        let timeout_state = state.clone();
        let timeout_session = session.clone();
        let timer_task = tokio::spawn(async move {
            tokio::select! {
                _ = tokio::time::sleep(challenge_ttl) => {
                    if pending_identity
                        .remove_if(&peer_id, |_, pending| {
                            pending.session.same_connection(&timeout_session)
                                && pending.challenge == timeout_challenge
                                && pending.expires_at <= Instant::now()
                        })
                        .is_some()
                    {
                        disconnect_admission(
                            &timeout_state,
                            &timeout_session,
                            "Authentication timeout",
                        )
                        .await;
                    }
                }
                _ = &mut timeout_cancelled => {}
            }
        });
        {
            let mut timer_tasks = state.identity_timer_tasks.lock();
            timer_tasks.retain(|task| !task.is_finished());
            timer_tasks.push(timer_task);
        }
        let mut writer = NetWriter::new();
        BytesMessage {
            data: challenge.clone(),
        }
        .serialize(&mut writer)?;
        state
            .transport
            .send_session(
                &session,
                channels::AUTH_IDENTITY,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
            )
            .await?;
    } else {
        finalize_accept(state, &session, ready, false).await?;
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
    bool::from(auth_bytes.ct_eq(server_password.as_bytes()))
}

async fn disconnect_admission(state: &ServerState, session: &PeerSession, reason: &str) {
    if let Err(error) = request_disconnect(state, session, reason).await {
        warn!(
            peer = session.peer_id(),
            "failed to request peer disconnect: {error:#}"
        );
    }
}

/// Remove the exact connection and defer keyed cleanup until its message leases drain.
/// This function may be called by a message handler that holds the session's own lease,
/// so it must never await `wait_for_read_leases` itself.
pub(crate) async fn request_disconnect(
    state: &ServerState,
    session: &PeerSession,
    reason: &str,
) -> Result<()> {
    if !state.transport.disconnect_session(session, reason).await? {
        return Ok(());
    }
    let disconnect_tasks = state.disconnect_tasks.clone();
    let cleanup_state = state.clone();
    let session = session.clone();
    let task = tokio::spawn(async move {
        session.wait_for_read_leases().await;
        handle_disconnect(&cleanup_state, &session, DisconnectReason::Remote).await;
    });
    let mut tasks = disconnect_tasks.lock();
    tasks.retain(|task| !task.is_finished());
    tasks.push(task);
    Ok(())
}

async fn finalize_accept(
    state: &ServerState,
    session: &PeerSession,
    ready: ReadyMessage,
    identity_verified: bool,
) -> Result<()> {
    let peer_id = session.peer_id();
    let uuid = ready.player_meta_data_message.player_uuid.clone();
    let config = state.config.read().clone();
    if config.basis_user_restriction_mode == BasisUserRestrictionMode::RejoinOnly
        && !identity_verified
    {
        disconnect_admission(
            state,
            session,
            "Rejoin-only mode requires authenticated identity.",
        )
        .await;
        return Ok(());
    }
    if let Some(reason) = admission_rejection(state, &ready) {
        disconnect_admission(state, session, reason).await;
        return Ok(());
    }
    let metadata = ready.player_meta_data_message.clone();
    let connected = ConnectedPeer {
        id: peer_id,
        metadata: metadata.clone(),
        ready: ready.clone(),
        session: Some(session.clone()),
    };
    let existing_players = {
        let _commit = state.admission_commit.lock();
        if config.peer_limit > 0 && state.player_count() >= config.peer_limit as usize {
            None
        } else {
            let existing = state
                .join_broadcast
                .lock()
                .register_peer(connected.clone(), serialize_server_ready(peer_id, &ready)?);
            state.avatar_sync.register_player(peer_id);
            state.authenticated_peers.insert(peer_id, connected);
            Some(existing)
        }
    };
    let Some(existing_players) = existing_players else {
        disconnect_admission(
            state,
            session,
            "This server is full. Please try again later.",
        )
        .await;
        return Ok(());
    };
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
    server_meta.serialize(&mut writer)?;
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
    core_message_supply().serialize(&mut registry_writer)?;
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
    if let Err(err) = ready
        .local_avatar_sync_message
        .serialize_for_channel(&mut writer, has_additional)
    {
        warn!("failed to serialize initial avatar sync for peer {peer_id}: {err}");
        return;
    }
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
        let record = serialize_server_ready(existing.id, &ready)?;

        if batch_count > 0
            && batch_payload.len() + record.len() > ServerReadyBatchMessage::MAX_PAYLOAD_BYTES
        {
            let mut writer = NetWriter::new();
            ServerReadyBatchMessage {
                count: batch_count,
                payload: std::mem::take(&mut batch_payload),
            }
            .serialize(&mut writer)?;
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
        .serialize(&mut writer)?;
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
        if let Err(err) = (ServerUniqueIdMessages { messages: net_ids }).serialize(&mut writer) {
            warn!("failed to serialize replay_late_join_state: {err}");
            return;
        }
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
        if let Err(err) = resource.serialize(&mut writer) {
            warn!("failed to serialize replay_late_join_state: {err}");
            return;
        }
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
        if let Err(err) = ownership.serialize(&mut writer) {
            warn!("failed to serialize replay_late_join_state: {err}");
            return;
        }
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
        if let Err(err) = sphere.serialize(&mut writer) {
            warn!("failed to serialize replay_late_join_state: {err}");
            return;
        }
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
        if let Err(err) = pip.serialize(&mut writer) {
            warn!("failed to serialize replay_late_join_state: {err}");
            return;
        }
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
    if let Err(err) = admin_runtime::send_join_state(state, peer_id).await {
        warn!("failed to replay moderation state to peer {peer_id}: {err:#}");
    }
    let offers = state
        .image_cache
        .lock()
        .offer_peer(peer_id, &state.config.read());
    deliver_image_cache_sends(state, offers).await;
}

async fn handle_disconnect(state: &ServerState, session: &PeerSession, reason: DisconnectReason) {
    let peer = session.peer_id();
    // Close avatar admission before any asynchronous disconnect cleanup.
    state.avatar_sync.remove_player(peer);
    state.admin_runtime.remove_peer(peer);
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
    state.image_governor.remove_peer(peer);
    state.image_cache.lock().remove_player(peer);
    state.jiggle_buckets.remove(&peer);
    if !departed_uuid.is_empty() {
        state.error_report_hashes.remove(&departed_uuid);
    }
    for removed in state.ownership.remove_player(peer) {
        let mut writer = NetWriter::new();
        if let Err(err) = removed.serialize(&mut writer) {
            warn!("failed to serialize disconnect cleanup: {err}");
            continue;
        }
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
        if let Err(err) = removed.serialize(&mut writer) {
            warn!("failed to serialize disconnect cleanup: {err}");
            continue;
        }
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
        if let Err(err) = unload.serialize(&mut writer) {
            warn!("failed to serialize disconnect cleanup: {err}");
            continue;
        }
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
        if let Err(err) = pip_destroy.serialize(&mut writer) {
            warn!("failed to serialize PIP cleanup: {err}");
        } else {
            state
                .broadcast(
                    channels::CAMERA_PIP_STATE,
                    DeliveryMethod::ReliableOrdered,
                    writer.as_slice(),
                    Some(peer),
                )
                .await;
        }
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
                if let Err(err) = unload.serialize(&mut writer) {
                    warn!("failed to serialize disconnect cleanup: {err}");
                    continue;
                }
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
) -> Result<bool> {
    if payload.is_empty() {
        return Ok(false);
    }
    let header = payload[0];
    if header & channels::DELTA_HEADER_CONTROL_BIT != 0 {
        if header == channels::DELTA_CONTROL_KEYFRAME_REQUEST && payload.len() >= 3 {
            let sender_id = u16::from_le_bytes([payload[1], payload[2]]);
            state.avatar_sync.request_keyframe(sender_id, peer);
        }
        return Ok(false);
    }
    if header & channels::DELTA_HEADER_QUALITY_MASK != BitQuality::High as u8 {
        return Ok(false);
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
        return Ok(false);
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
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn handle_message(
    state: &ServerState,
    peer: PeerId,
    session: Option<&PeerSession>,
    channel: u8,
    delivery: DeliveryMethod,
    payload: Bytes,
    count_inbound: bool,
    count_avatar: bool,
) -> Result<()> {
    if count_inbound {
        state
            .statistics
            .inbound_packets
            .fetch_add(1, Ordering::Relaxed);
    }
    if channel != channels::AUTH_IDENTITY && !state.authenticated_peers.contains_key(&peer) {
        if count_avatar {
            state
                .statistics
                .avatar_rejected
                .fetch_add(1, Ordering::Relaxed);
        }
        return Ok(());
    }
    if (channels::PLAYER_AVATAR_QUALITY_CHANNELS.contains(&channel)
        || channel == channels::DELTA_AVATAR)
        && !state.avatar_sync.is_player_registered(peer)
    {
        if count_avatar {
            state
                .statistics
                .avatar_rejected
                .fetch_add(1, Ordering::Relaxed);
        }
        return Ok(());
    }
    match channel {
        channels::AUTH_IDENTITY => {
            let Some(session) = session else {
                return Ok(());
            };
            if let Some((_, pending)) = state
                .pending_identity
                .remove_if(&peer, |_, pending| pending.session.same_connection(session))
            {
                if pending.expires_at <= Instant::now() {
                    disconnect_admission(state, session, "Authentication timeout").await;
                    return Ok(());
                }
                if !pending.session.same_connection(session) {
                    return Ok(());
                }
                let identity_check = (|| -> Result<()> {
                    let mut reader = NetReader::new(&payload);
                    let response = DidResponse::deserialize(&mut reader)
                        .context("malformed identity response")?;
                    anyhow::ensure!(
                        reader.remaining() == 0,
                        "trailing bytes in identity response"
                    );
                    let verifying_key =
                        did_key_verifying_key(&pending.ready.player_meta_data_message.player_uuid)?;
                    response.verify(&pending.challenge, &verifying_key)
                })();
                if let Err(error) = identity_check {
                    disconnect_admission(
                        state,
                        session,
                        &format!("Identity verification failed: {error}"),
                    )
                    .await;
                    return Ok(());
                }
                finalize_accept(state, session, pending.ready, true).await?;
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
            if ingest_payload.is_empty() {
                if count_avatar {
                    state
                        .statistics
                        .avatar_rejected
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
            match state.avatar_sync.upsert_from_channel_payload(
                peer,
                ingest_channel,
                ingest_payload,
            ) {
                Ok(()) => {
                    capture_uplink_delta_baseline(state, peer, ingest_payload);
                    if count_avatar {
                        state
                            .statistics
                            .avatar_processed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(err) => {
                    state
                        .statistics
                        .protocol_errors
                        .fetch_add(1, Ordering::Relaxed);
                    if count_avatar {
                        state
                            .statistics
                            .avatar_rejected
                            .fetch_add(1, Ordering::Relaxed);
                    }
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
            if ingest_payload.is_empty() {
                if count_avatar {
                    state
                        .statistics
                        .avatar_rejected
                        .fetch_add(1, Ordering::Relaxed);
                }
                return Ok(());
            }
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
                    if count_avatar {
                        state
                            .statistics
                            .avatar_processed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(err) => {
                    state
                        .statistics
                        .protocol_errors
                        .fetch_add(1, Ordering::Relaxed);
                    if count_avatar {
                        state
                            .statistics
                            .avatar_rejected
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    warn!("invalid avatar update from peer {peer}: {err}");
                }
            }
        }
        channels::DELTA_AVATAR => match handle_uplink_avatar_delta(state, peer, &payload).await {
            Ok(true) => {
                if count_avatar {
                    state
                        .statistics
                        .avatar_processed
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(false) => {
                if count_avatar {
                    state
                        .statistics
                        .avatar_rejected
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(err) => {
                state
                    .statistics
                    .protocol_errors
                    .fetch_add(1, Ordering::Relaxed);
                if count_avatar {
                    state
                        .statistics
                        .avatar_rejected
                        .fetch_add(1, Ordering::Relaxed);
                }
                warn!("invalid avatar delta from peer {peer}: {err}");
            }
        },
        channels::CHAT => {
            if admin_runtime::is_text_muted(state, peer) {
                return Ok(());
            }
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
            message.serialize(&mut writer)?;
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
                        state.join_broadcast.lock().update_peer_ready(peer, ready)?;
                    }
                    let message = ServerAvatarChangeMessage {
                        player_id: peer,
                        client_avatar_change_message: avatar,
                    };
                    let mut writer = NetWriter::new();
                    writer.put_u8(channels::AVATAR_CHANGE_KIND_FULL);
                    message.serialize(&mut writer)?;
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
                        state.join_broadcast.lock().update_peer_ready(peer, ready)?;
                    }
                    let message = ServerBodyFitMessage {
                        player_id: peer,
                        body_fit,
                    };
                    let mut writer = NetWriter::new();
                    writer.put_u8(channels::AVATAR_CHANGE_KIND_BODY_FIT);
                    message.serialize(&mut writer)?;
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
            message.serialize(&mut writer)?;
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
                resource.serialize(&mut writer)?;
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
                request.serialize(&mut writer)?;
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
                request.serialize(&mut writer)?;
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
                        unload.serialize(&mut writer)?;
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
            response.serialize(&mut writer)?;
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
            response.serialize(&mut writer)?;
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
                request.serialize(&mut writer)?;
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
                    server_message.serialize(&mut writer)?;
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
                        server_message.serialize(&mut writer)?;
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
            response.serialize(&mut writer)?;
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
                response.serialize(&mut writer)?;
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
            if admin_runtime::is_voice_muted(state, peer) {
                return Ok(());
            }
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
            if admin_runtime::is_voice_muted(state, peer)
                || !state.admin_runtime.is_announcing(peer)
            {
                return Ok(());
            }
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
    message.serialize(&mut writer)?;
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
        state.net_ids.find(image_cache::IMAGE_MANAGER_IDENTIFIER) == Some(scene.message_index);
    let allow_animation = !is_image_traffic || image_animation_allowed(state, peer);
    if is_image_traffic && matches!(scene.payload.first(), Some(6 | 7)) && !allow_animation {
        return Ok(());
    }
    let fan_out = if scene.recipients.is_empty() {
        state.authenticated_peers.len().saturating_sub(1)
    } else {
        scene.recipients.len()
    };
    let egress_bytes = scene.payload.len().saturating_mul(fan_out.max(1)) as u64;
    let allowed = if is_image_traffic {
        state.image_governor.try_consume_egress(
            peer,
            egress_bytes,
            &state.config.read(),
            Instant::now(),
        )
    } else {
        state.scene_egress_allowed(peer, egress_bytes)
    };
    // Only admitted data may populate the cache. A rejected spawn must not mark
    // recipients as delivered, and a refused chunk must not bypass upload limits
    // through a later cached replay. Requests and despawns still update the cache.
    if is_image_traffic && (allowed || matches!(scene.payload.first(), Some(4 | 10))) {
        let connected = state
            .authenticated_peers
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        let animation_allowed = connected
            .iter()
            .copied()
            .filter(|peer| image_animation_allowed(state, *peer))
            .collect::<Vec<_>>();
        let effects = state.image_cache.lock().observe(
            peer,
            &scene.payload,
            &scene.recipients,
            image_cache::CachePeers {
                connected: &connected,
                animation_allowed: &animation_allowed,
            },
            allow_animation,
            &state.config.read(),
        );
        deliver_image_cache_sends(state, effects.sends).await;
    }
    if !allowed {
        return Ok(());
    }
    let message = ServerSceneDataMessage {
        player_id: peer,
        scene_data_message: RemoteSceneDataMessage {
            message_index: scene.message_index,
            payload: scene.payload,
        },
    };
    let mut writer = NetWriter::new();
    message.serialize(&mut writer)?;
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
            CameraShutterSoundMessage { player_id: peer }.serialize(&mut writer)?;
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
            .serialize(&mut writer)?;
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
            if admin_runtime::is_text_muted(state, peer) {
                return Ok(());
            }
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
    message.serialize(&mut writer)?;
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
    let message = ServerAudioSegmentMessage {
        player_id: peer,
        audio_segment: payload.to_vec(),
    };
    let mut writer = NetWriter::new();
    if let Err(err) = message.serialize(&mut writer) {
        warn!("failed to serialize relay_shout_voice_message: {err}");
        return;
    }
    state
        .broadcast(
            channels::SHOUT_VOICE,
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
    if let Err(err) = spawn.serialize(&mut writer) {
        warn!("failed to serialize broadcast_spawn_preloaded: {err}");
        return;
    }
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
    if !state.authenticated_peers.contains_key(&peer) {
        return Ok(());
    }
    let mut reader = NetReader::new(payload);
    let request = AdminRequest::deserialize(&mut reader)?;
    if let Some(required_node) = admin_mode_required_permission(request.mode) {
        let releasing_self = matches!(
            request.mode,
            AdminRequestMode::DisableAnnounceMode | AdminRequestMode::DisableShoutMode
        ) && payload
            .get(1..3)
            .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]]))
            == Some(peer);
        if !releasing_self && !peer_has_permission(state, peer, required_node) {
            send_admin_text(state, peer, &format!("No permission: {required_node}")).await?;
            return Ok(());
        }
    }
    if admin_runtime::handle_request(state, peer, request.mode, &mut reader).await? {
        return Ok(());
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
            .serialize(&mut writer)?;
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
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::CreateGroup => {
            if let Ok(group) = reader.get_string() {
                state.permissions.get_or_create_group(&group);
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::DeleteGroup => {
            if let Ok(group) = reader.get_string() {
                state.permissions.delete_group(&group);
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
            .serialize(&mut writer)?;
            writer.put_string(&message)?;
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
            request.serialize(&mut writer)?;
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
            request.serialize(&mut writer)?;
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
        AdminRequestMode::SetServerName => {
            state.config.write().server_name = reader.get_string()?;
        }
        AdminRequestMode::SetServerMotd => {
            state.config.write().server_motd = reader.get_string()?;
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
        _ => {
            warn!("admin mode {:?} is not accepted from clients", request.mode);
        }
    }
    if admin_mode_persists_config(request.mode) && state.config.read().has_file_support {
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
    .serialize(&mut writer)?;
    writer.put_u16(moderator);
    writer.put_string(&url)?;
    writer.put_string(&password)?;
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
    .serialize(&mut writer)?;
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
        if target != moderator && has_protection_permission(state, target) {
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
    .serialize(&mut begin)?;
    begin.put_string(&server_name)?;
    begin.put_string("logs")?;
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
        .serialize(&mut writer)?;
        writer.put_i32(index as i32);
        writer.put_bytes_with_length(chunk)?;
        send_admin_payload_to_peer(state, peer, writer.into_vec()).await?;
    }

    let mut end = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::LogBundleEnd,
    }
    .serialize(&mut end)?;
    end.put_bool(true);
    end.put_string(&format!(
        "Sent {} log file(s), {} KB compressed.",
        prepared.file_count,
        prepared.payload.len() / 1024
    ))?;
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
    .serialize(&mut writer)?;
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
    writer.put_u8(AdminRequestMode::GlobalGetLockState as u8);
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
    writer.put_bool(locks.gifs_locked);
}

fn encode_bool_admin_state_payload(mode: AdminRequestMode, value: bool) -> Vec<u8> {
    let mut writer = NetWriter::new();
    writer.put_u8(mode as u8);
    writer.put_bool(value);
    writer.into_vec()
}

fn encode_u8_admin_state_payload(mode: AdminRequestMode, value: u8) -> Vec<u8> {
    let mut writer = NetWriter::new();
    writer.put_u8(mode as u8);
    writer.put_u8(value);
    writer.into_vec()
}

fn encode_i32_admin_state_payload(mode: AdminRequestMode, value: i32) -> Vec<u8> {
    let mut writer = NetWriter::new();
    writer.put_u8(mode as u8);
    writer.put_i32(value);
    writer.into_vec()
}

fn encode_f32_pair_admin_state_payload(mode: AdminRequestMode, first: f32, second: f32) -> Vec<u8> {
    let mut writer = NetWriter::new();
    writer.put_u8(mode as u8);
    writer.put_f32(first);
    writer.put_f32(second);
    writer.into_vec()
}

fn encode_reduction_settings_payload(config: &ServerConfig) -> Vec<u8> {
    let mut writer = NetWriter::new();
    writer.put_u8(AdminRequestMode::GlobalGetReductionSettings as u8);
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
    writer.put_u8(AdminRequestMode::GlobalGetImageBandwidth as u8);
    writer.put_i32(config.image_share_egress_megabits_per_second);
    writer.put_i32(config.image_share_download_megabits_per_second);
    writer.put_i32(config.image_share_egress_enforcement_percent);
    writer.into_vec()
}

fn encode_user_opus_bitrate_override_payload(value: i32) -> Vec<u8> {
    let mut writer = NetWriter::new();
    writer.put_u8(AdminRequestMode::UserOpusBitrateOverride as u8);
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
    .serialize(&mut writer)?;
    writer.put_string(message)?;
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
    .serialize(&mut writer)?;
    writer.put_i32(snapshot.groups.len() as i32);
    for group in snapshot.groups.values() {
        writer.put_string(&group.name)?;
        writer.put_i32(group.nodes.len() as i32);
        for node in &group.nodes {
            writer.put_string(node)?;
        }
        writer.put_i32(group.parents.len() as i32);
        for parent in &group.parents {
            writer.put_string(parent)?;
        }
    }
    writer.put_i32(snapshot.users.len() as i32);
    for user in snapshot.users.values() {
        writer.put_string(&user.uuid)?;
        writer.put_i32(user.groups.len() as i32);
        for group in &user.groups {
            writer.put_string(group)?;
        }
        writer.put_i32(user.nodes.len() as i32);
        for node in &user.nodes {
            writer.put_string(node)?;
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
    AdminRequest { mode }.serialize(&mut writer)?;
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
    writer.put_u8(mode as u8);
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
    AdminRequest { mode }.serialize(&mut writer)?;
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
    writer.put_u8(mode as u8);
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

fn peer_session_by_uuid(state: &ServerState, uuid: &str) -> Option<(PeerId, PeerSession)> {
    state.authenticated_peers.iter().find_map(|peer| {
        if peer.metadata.player_uuid == uuid {
            peer.session.clone().map(|session| (*peer.key(), session))
        } else {
            None
        }
    })
}

async fn disconnect_headless_peers(state: &ServerState) {
    let peers = state
        .authenticated_peers
        .iter()
        .filter_map(|peer| {
            let platform = &peer.metadata.player_platform;
            (is_headless_platform(platform))
                .then(|| peer.session.clone())
                .flatten()
        })
        .collect::<Vec<_>>();
    for session in peers {
        if let Err(error) =
            request_disconnect(state, &session, "Headless client disallowed by server.").await
        {
            warn!(
                peer = session.peer_id(),
                "failed to disconnect headless peer: {error:#}"
            );
        }
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
        AdminRequestMode::GetPermissions => nodes::PERMISSIONS_VIEW,
        AdminRequestMode::SetVoiceMute
        | AdminRequestMode::SetTextMute
        | AdminRequestMode::GetMuteState => nodes::MODERATION_MUTE,
        AdminRequestMode::RenamePlayer => nodes::MODERATION_RENAME,
        AdminRequestMode::Kick => nodes::MODERATION_KICK,
        AdminRequestMode::IpAndBan => nodes::MODERATION_IP_BAN,
        AdminRequestMode::UnBan => nodes::MODERATION_UNBAN,
        AdminRequestMode::UnBanIP => nodes::MODERATION_UNBAN_IP,
        AdminRequestMode::Message => nodes::MODERATION_MESSAGE,
        AdminRequestMode::MessageAll => nodes::MODERATION_MESSAGE_ALL,
        AdminRequestMode::TeleportAll | AdminRequestMode::TeleportPlayer => {
            nodes::MODERATION_TELEPORT
        }
        AdminRequestMode::EnableAnnounceMode
        | AdminRequestMode::DisableAnnounceMode
        | AdminRequestMode::EnableShoutMode
        | AdminRequestMode::DisableShoutMode => nodes::MODERATION_ANNOUNCE,
        AdminRequestMode::SetFullQualityBroadcast => nodes::MODERATION_FULL_QUALITY_BROADCAST,
        AdminRequestMode::ForceAvatar | AdminRequestMode::ForceAvatarAll => {
            nodes::MODERATION_FORCE_AVATAR
        }
        AdminRequestMode::SetLocomotionOverride | AdminRequestMode::SetLocomotionOverrideAll => {
            nodes::MODERATION_LOCOMOTION
        }
        AdminRequestMode::GlobalToggleAvatars
        | AdminRequestMode::GlobalToggleGifs
        | AdminRequestMode::SetGlobalLocomotionPolicy
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
    writer.put_u8(AdminRequestMode::GlobalGetLockState as u8);
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

    #[tokio::test]
    async fn reliable_ordered_handlers_wait_for_the_previous_handler() {
        let mut queue = OrderedHandlerQueue::<u8>::default();
        let lane = (17, 3, 1);
        for event in [1, 2, 3] {
            queue.enqueue(lane, event);
        }
        queue.enqueue((17, 4, 1), 4); // Independent channel.
        queue.enqueue((18, 3, 1), 5); // Independent peer.
        queue.enqueue((17, 3, 2), 6); // Replacement incarnation.
        let workers = Arc::new(Semaphore::new(2));
        let mut handlers = tokio::task::JoinSet::new();
        let mut keys = HashMap::new();
        let release = Arc::new(tokio::sync::Notify::new());
        let (started, mut starts) = mpsc::unbounded_channel();
        let handle = |event, permit| {
            let release = release.clone();
            let started = started.clone();
            async move {
                let _permit = permit;
                started.send(event).unwrap();
                if event == 1 {
                    release.notified().await;
                }
            }
        };
        spawn_ready_ordered_with(
            &mut queue,
            &mut handlers,
            &mut keys,
            workers.clone(),
            handle,
        );
        // Only the blocked head and an independent lane can occupy the two slots.
        assert_eq!(handlers.len(), 2);
        for expected in [4, 5, 6] {
            let (id, ()) =
                tokio::time::timeout(Duration::from_secs(2), handlers.join_next_with_id())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            let finished_lane = keys.remove(&id).unwrap();
            assert_ne!(finished_lane, lane);
            queue.complete(finished_lane);
            spawn_ready_ordered_with(
                &mut queue,
                &mut handlers,
                &mut keys,
                workers.clone(),
                handle,
            );
            assert!(queue.contains(lane));
            // Consume the starts deterministically, without assuming executor poll order.
            if expected == 4 {
                let mut initial = [starts.recv().await.unwrap(), starts.recv().await.unwrap()];
                initial.sort_unstable();
                assert_eq!(initial, [1, 4]);
            } else {
                assert_eq!(starts.recv().await.unwrap(), expected);
            }
        }
        assert_eq!(workers.available_permits(), 1);
        assert_eq!(queue.pending(), 2);
        assert!(starts.try_recv().is_err()); // Neither follower started while head blocked.
        release.notify_one();
        // Drain accepted lane data using the same scheduling primitive as shutdown.
        while !handlers.is_empty() || queue.pending() > 0 {
            let (id, ()) =
                tokio::time::timeout(Duration::from_secs(2), handlers.join_next_with_id())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
            queue.complete(keys.remove(&id).unwrap());
            spawn_ready_ordered_with(
                &mut queue,
                &mut handlers,
                &mut keys,
                workers.clone(),
                handle,
            );
        }
        assert_eq!(starts.recv().await.unwrap(), 2);
        assert_eq!(starts.recv().await.unwrap(), 3);
        assert!(queue.lanes.is_empty());
        assert!(keys.is_empty());
        assert_eq!(workers.available_permits(), 2);
    }

    #[test]
    fn retiring_ordered_lane_preserves_running_head_and_new_generation() {
        let mut queue = OrderedHandlerQueue::<u8>::default();
        let old = (17, 3, 1);
        let new = (17, 3, 2);
        queue.enqueue(old, 1);
        queue.enqueue(old, 2);
        queue.enqueue(new, 3);
        assert_eq!(queue.start_next(), Some((old, 1)));
        assert!(queue.discard_lane(old));
        assert_eq!(queue.pending(), 1);
        assert!(queue.contains(old));
        assert!(!queue.discard_lane(old));
        queue.complete(old);
        assert!(!queue.contains(old));
        assert_eq!(queue.start_next(), Some((new, 3)));
        queue.complete(new);
        assert!(queue.lanes.is_empty());
    }

    #[test]
    fn retired_ordered_ready_keys_are_compacted_once_per_pass() {
        let mut queue = OrderedHandlerQueue::<u8>::default();
        for generation in 1..=4096 {
            let key = (17, 3, generation);
            queue.enqueue(key, 1);
            assert!(queue.discard_lane(key));
        }
        let live = (18, 3, 4097);
        queue.enqueue(live, 2);
        queue.compact_ready();
        assert_eq!(queue.pending(), 1);
        assert_eq!(queue.ready.len(), 1);
        assert_eq!(queue.start_next(), Some((live, 2)));
    }

    #[tokio::test]
    async fn reliable_ordered_handler_panic_releases_its_lane() {
        let mut queue = OrderedHandlerQueue::<u8>::default();
        let lane = (17, 3, 1);
        queue.enqueue(lane, 1);
        queue.enqueue(lane, 2);
        let workers = Arc::new(Semaphore::new(1));
        let mut handlers = tokio::task::JoinSet::new();
        let mut keys = HashMap::new();
        let handle = |event, permit| async move {
            let _permit = permit;
            assert_ne!(event, 1, "deliberately failing first handler");
        };
        spawn_ready_ordered_with(
            &mut queue,
            &mut handlers,
            &mut keys,
            workers.clone(),
            handle,
        );
        let error = tokio::time::timeout(Duration::from_secs(2), handlers.join_next_with_id())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.is_panic());
        queue.complete(keys.remove(&error.id()).unwrap());
        spawn_ready_ordered_with(
            &mut queue,
            &mut handlers,
            &mut keys,
            workers.clone(),
            handle,
        );
        let (id, ()) = tokio::time::timeout(Duration::from_secs(2), handlers.join_next_with_id())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        queue.complete(keys.remove(&id).unwrap());
        assert!(queue.lanes.is_empty());
        assert_eq!(workers.available_permits(), 1);
    }

    #[test]
    fn pruning_ordered_followers_preserves_running_head_and_other_lanes() {
        let mut queue = OrderedHandlerQueue::<u8>::default();
        let old_lane = (17, 3, 1);
        let replacement_lane = (17, 3, 2);
        queue.enqueue(old_lane, 1);
        assert_eq!(queue.start_next(), Some((old_lane, 1)));
        queue.enqueue(old_lane, 2);
        queue.enqueue(replacement_lane, 3);
        queue.discard_matching(|event| *event == 2);
        assert_eq!(queue.pending(), 1);
        assert!(queue.contains(old_lane));
        queue.complete(old_lane);
        assert!(!queue.contains(old_lane));
        assert_eq!(queue.start_next(), Some((replacement_lane, 3)));
        queue.complete(replacement_lane);
        assert!(queue.lanes.is_empty());
    }

    #[tokio::test]
    async fn global_ordered_backpressure_keeps_regular_event_admission_open() {
        let mut queue = OrderedHandlerQueue::<u8>::default();
        for peer in 0..(MAX_PENDING_ORDERED_EVENTS / MAX_PENDING_ORDERED_PER_LANE) {
            for _ in 0..MAX_PENDING_ORDERED_PER_LANE {
                queue.enqueue((peer as PeerId, 3, 1), 1);
            }
        }
        let (control_tx, mut control) = mpsc::channel(1);
        let (ordered_tx, mut ordered) = mpsc::channel(1);
        let (_critical_tx, mut critical) = mpsc::channel(1);
        ordered_tx
            .send(ServerEvent::NetworkError("ordered waiting".into()).into())
            .await
            .unwrap();
        control_tx
            .send(ServerEvent::NetworkError("lifecycle admitted".into()))
            .await
            .unwrap();
        let event = tokio::time::timeout(
            Duration::from_secs(1),
            receive_control_event(
                &mut control,
                &mut ordered,
                &mut critical,
                &mut [true, true, true],
                queue.pending() < MAX_PENDING_ORDERED_EVENTS,
                true,
                true,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            matches!(event.event, ServerEvent::NetworkError(value) if value == "lifecycle admitted")
        );
        assert_eq!(ordered.len(), 1); // Already admitted ordered data is retained, not dropped.
        queue.start_next().unwrap();
        let event = tokio::time::timeout(
            Duration::from_secs(1),
            receive_control_event(
                &mut control,
                &mut ordered,
                &mut critical,
                &mut [true, true, true],
                queue.pending() < MAX_PENDING_ORDERED_EVENTS,
                true,
                true,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            matches!(event.event, ServerEvent::NetworkError(value) if value == "ordered waiting")
        );
    }

    #[tokio::test]
    async fn critical_ordered_ingress_survives_full_ordinary_queues_and_workers() {
        let (control_tx, mut control) = mpsc::channel(1);
        let (ordered_tx, mut ordered) = mpsc::channel(1);
        let (critical_tx, mut critical) = mpsc::channel(1);
        control_tx
            .send(ServerEvent::NetworkError("ordinary control".into()))
            .await
            .unwrap();
        ordered_tx
            .send(ServerEvent::NetworkError("ordinary ordered".into()).into())
            .await
            .unwrap();
        critical_tx
            .send(ServerEvent::NetworkError("identity".into()).into())
            .await
            .unwrap();
        let event = tokio::time::timeout(
            Duration::from_secs(1),
            receive_control_event(
                &mut control,
                &mut ordered,
                &mut critical,
                &mut [true, true, true],
                false,
                true,
                false,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(event.event, ServerEvent::NetworkError(value) if value == "identity"));
        assert_eq!(control.len(), 1);
        assert_eq!(ordered.len(), 1);
    }

    #[tokio::test]
    async fn closing_one_ingress_preserves_the_other_buffered_events() {
        let (control_tx, mut control) = mpsc::channel(1);
        let (ordered_tx, mut ordered) = mpsc::channel(1);
        let (critical_tx, mut critical) = mpsc::channel(1);
        let mut open = [true, true, true];
        drop(critical_tx);
        ordered_tx
            .send(ServerEvent::NetworkError("retained ordered".into()).into())
            .await
            .unwrap();
        drop(control_tx);
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            receive_control_event(
                &mut control,
                &mut ordered,
                &mut critical,
                &mut open,
                false,
                true,
                true,
            )
        )
        .await
        .is_err());
        assert!(!open[0]);
        assert_eq!(ordered.len(), 1);
        let event = tokio::time::timeout(
            Duration::from_secs(1),
            receive_control_event(
                &mut control,
                &mut ordered,
                &mut critical,
                &mut open,
                true,
                true,
                true,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            matches!(event.event, ServerEvent::NetworkError(value) if value == "retained ordered")
        );
        drop(ordered_tx);
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            receive_control_event(
                &mut control,
                &mut ordered,
                &mut critical,
                &mut open,
                true,
                true,
                true,
            )
        )
        .await
        .unwrap()
        .is_none());
        assert_eq!(open, [false, false, false]);
    }

    #[tokio::test]
    async fn shutdown_saves_before_worker_wait_and_again_after_final_updates() {
        let path =
            std::env::temp_dir().join(format!("basis-core-shutdown-{}.json", uuid::Uuid::new_v4()));
        let config = ServerConfig {
            has_file_support: false,
            set_port: 0,
            override_auto_discovery_of_ipv: true,
            ipv4_address: "127.0.0.1".into(),
            ..ServerConfig::default()
        };
        let (mut server, _shutdown_tx) = ServerState::start(config, &std::env::temp_dir())
            .await
            .unwrap();
        server.database = PersistentDatabase::file_backed(&path);
        server
            .database
            .add_or_update(basis_server_storage::BasisData {
                name: "initial".into(),
                json_payload: serde_json::json!(1),
            });
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let database = server.database.clone();
        server.workers.lock().push(tokio::spawn(async move {
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
            database.add_or_update(basis_server_storage::BasisData {
                name: "final".into(),
                json_payload: serde_json::json!(2),
            });
        }));
        ready_rx.await.unwrap();
        let mut shutdown = Box::pin(server.shutdown());
        // Poll shutdown until it is waiting on the deliberately blocked worker.
        assert!(
            tokio::time::timeout(Duration::from_millis(150), &mut shutdown)
                .await
                .is_err()
        );
        let saved = PersistentDatabase::file_backed(&path);
        saved.load().unwrap();
        assert!(saved.get("initial").is_some());
        assert!(saved.get("final").is_none());
        release_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), shutdown)
            .await
            .unwrap()
            .unwrap();
        saved.load().unwrap();
        assert!(saved.get("final").is_some());
        assert!(server.workers.lock().is_empty());
        assert!(server.tick_thread.lock().is_none());
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn password_comparison_preserves_open_empty_utf8_and_mismatch_semantics() {
        assert!(password_matches("", b""));
        assert!(password_matches("", b"arbitrary"));
        assert!(!password_matches("default_password", b""));
        assert!(password_matches("default_password", b"default_password"));
        for index in 0..16 {
            let mut wrong = b"default_password".to_vec();
            wrong[index] ^= 1;
            assert!(!password_matches("default_password", &wrong));
        }
        assert!(!password_matches("default_password", b"default_password\0"));
        assert!(password_matches("páss🔑", "páss🔑".as_bytes()));
        assert!(!password_matches("páss🔑", b"pass"));
    }

    #[test]
    fn identity_ttl_has_a_hard_bound_and_keeps_legitimate_batch_allowance() {
        assert_eq!(identity_challenge_ttl(0, -1), Duration::ZERO);
        assert_eq!(identity_challenge_ttl(0, 5000), Duration::from_secs(5));
        assert_eq!(identity_challenge_ttl(2000, 5000), Duration::from_secs(29));
        assert_eq!(
            identity_challenge_ttl(usize::MAX, i32::MAX),
            Duration::from_secs(60)
        );
    }

    #[test]
    fn extended_statistics_do_not_count_until_enabled() {
        let statistics = Statistics::new(false);
        statistics.inbound_packets.fetch_add(1, Ordering::Relaxed);
        statistics.outbound_packets.fetch_add(1, Ordering::Relaxed);
        statistics.protocol_errors.fetch_add(1, Ordering::Relaxed);
        statistics.avatar_received.fetch_add(1, Ordering::Relaxed);
        statistics.avatar_coalesced.fetch_add(1, Ordering::Relaxed);
        statistics.avatar_rejected.fetch_add(1, Ordering::Relaxed);
        statistics.avatar_processed.fetch_add(1, Ordering::Relaxed);
        assert_eq!(statistics.snapshot().inbound_packets, 0);
        assert_eq!(statistics.snapshot().outbound_packets, 0);
        assert_eq!(statistics.snapshot().protocol_errors, 0);
        assert_eq!(statistics.snapshot().avatar_received, 0);

        statistics.set_enabled(true);
        statistics.inbound_packets.fetch_add(2, Ordering::Relaxed);
        statistics.outbound_packets.fetch_add(3, Ordering::Relaxed);
        statistics.protocol_errors.fetch_add(4, Ordering::Relaxed);
        statistics.avatar_received.fetch_add(5, Ordering::Relaxed);
        statistics.avatar_coalesced.fetch_add(2, Ordering::Relaxed);
        statistics.avatar_rejected.fetch_add(1, Ordering::Relaxed);
        statistics.avatar_processed.fetch_add(2, Ordering::Relaxed);
        let snapshot = statistics.snapshot();
        assert_eq!(snapshot.inbound_packets, 2);
        assert_eq!(snapshot.outbound_packets, 3);
        assert_eq!(snapshot.protocol_errors, 4);
        assert_eq!(snapshot.avatar_received, 5);
        assert_eq!(snapshot.avatar_coalesced, 2);
        assert_eq!(snapshot.avatar_rejected, 1);
        assert_eq!(snapshot.avatar_processed, 2);

        statistics.set_enabled(false);
        statistics.inbound_packets.fetch_add(10, Ordering::Relaxed);
        statistics.set_enabled(true);
        let reset = statistics.snapshot();
        assert_eq!(reset.inbound_packets, 0);
        assert_eq!(reset.avatar_received, 0);
        assert_eq!(reset.avatar_coalesced, 0);
        assert_eq!(reset.avatar_rejected, 0);
        assert_eq!(reset.avatar_processed, 0);
    }

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
            session: None,
        }
    }

    #[test]
    fn join_records_use_server_ready_batch_framing() {
        for padding in [0, 1024] {
            let mut ready = test_ready_message();
            ready
                .player_meta_data_message
                .player_display_name
                .push_str(&"x".repeat(padding));
            let payload = serialize_server_ready(2, &ready).unwrap();
            let record = Arc::new(JoinBroadcastRecord {
                sequence: 1,
                peer_id: 2,
                revision: AtomicU64::new(0),
                payload: RwLock::new(payload.clone()),
            });
            let framed = frame_join_records(&[record]).unwrap();
            assert_eq!(&framed[..2], &[1, 0]);
            assert_eq!(framed[2], u8::from(padding != 0));
            if padding == 0 {
                assert_eq!(&framed[3..7], &(payload.len() as i32).to_le_bytes());
                assert_eq!(&framed[7..], payload);
            }
            let mut reader = NetReader::new(&framed);
            let batch = ServerReadyBatchMessage::deserialize(&mut reader).unwrap();
            assert_eq!(reader.remaining(), 0);
            assert_eq!(batch.count, 1);
            assert_eq!(batch.payload, payload);
            let mut records = NetReader::new(&batch.payload);
            assert_eq!(records.get_u16().unwrap(), 2);
            let decoded = ReadyMessage::deserialize(&mut records).unwrap();
            assert_eq!(
                decoded.player_meta_data_message,
                ready.player_meta_data_message
            );
            assert_eq!(
                decoded.local_avatar_sync_message,
                ready.local_avatar_sync_message
            );
            assert_eq!(records.remaining(), 0);
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
            serialize_server_ready(peer_one.id, &peer_one.ready).unwrap(),
        );
        state.register_peer(
            peer_two.clone(),
            serialize_server_ready(peer_two.id, &peer_two.ready).unwrap(),
        );

        let mut updated_two = peer_two.ready.clone();
        updated_two.client_avatar_change_message.arm_scale = 1.75;
        state
            .update_peer_ready(peer_two.id, updated_two.clone())
            .unwrap();

        state.mark_initial_history_queued(peer_one.id);
        let pending = state.take_batches(peer_one.id);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].len(), 1);
        assert_eq!(pending[0][0].revision.load(Ordering::Acquire), 1);
        assert_eq!(
            *pending[0][0].payload.read(),
            serialize_server_ready(peer_two.id, &updated_two).unwrap()
        );

        let mut updated_one = peer_one.ready.clone();
        updated_one.client_avatar_change_message.torso_scale = 1.25;
        state
            .update_peer_ready(peer_one.id, updated_one.clone())
            .unwrap();

        let peer_three = test_connected_peer(3);
        let existing = state.register_peer(
            peer_three.clone(),
            serialize_server_ready(peer_three.id, &peer_three.ready).unwrap(),
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
        )
        .unwrap();
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
