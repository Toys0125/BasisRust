mod avatar_sync;

use anyhow::{Context, Result};
use basis_protocol::{
    channels,
    config::{BasisUserRestrictionMode, ServerConfig},
    io::{NetReader, NetWriter},
    messages::{
        AdminRequest, AdminRequestMode, AvatarDataMessage, BasisDeserialize, BasisSerialize,
        BytesMessage, CameraCountdownMessage, CameraShutterSoundMessage, ChatMessage,
        ClientCameraCountdownMessage, ClientCameraPipPositionMessage, ClientCameraPipStateMessage,
        ClientMetaDataMessage, ContentShareCleanupMessage, ContentShareMessage, ContentShareType,
        DatabasePrimitiveMessage, LocalLoadResource, NetIdMessage, OwnershipTransferMessage,
        PreloadReadyMessage, ReadyMessage, RemoteAvatarDataMessage, RemoteSceneDataMessage,
        SceneDataMessage, ServerAudioSegmentMessage, ServerAvatarChangeMessage,
        ServerAvatarDataMessage, ServerChatMessage, ServerMetaDataMessage, ServerNetIdMessage,
        ServerReadyMessage, ServerSceneDataMessage, ServerStatisticMessage, ServerUniqueIdMessages,
        SpawnPreloadedMessage, UnloadResource, UshortUniqueIdMessage, VoiceReceiversMessage,
    },
    server_info::ServerInfoResponse,
    version::SERVER_VERSION,
};
use basis_server_admin::{GlobalState, ModerationLists};
use basis_server_permissions::PermissionManager;
use basis_server_resources::{
    ContentShareState, NetIdState, OwnershipState, PipState, ResourceState,
};
use basis_server_storage::{BasisData, PersistentDatabase};
use basis_transport::{DeliveryMethod, DisconnectReason, PeerId, ServerEvent, TransportHandle};
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde_json::Value;
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::Path,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
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

#[derive(Debug, Clone, Default)]
pub struct Statistics {
    pub inbound_packets: Arc<AtomicU64>,
    pub outbound_packets: Arc<AtomicU64>,
    pub protocol_errors: Arc<AtomicU64>,
}

#[derive(Clone)]
pub struct ServerState {
    pub config: Arc<RwLock<ServerConfig>>,
    pub transport: TransportHandle,
    pub authenticated_peers: Arc<DashMap<PeerId, ConnectedPeer>>,
    pub pending_identity: Arc<DashMap<PeerId, ReadyMessage>>,
    pub permissions: PermissionManager,
    pub database: PersistentDatabase,
    pub resources: ResourceState,
    pub net_ids: NetIdState,
    pub ownership: OwnershipState,
    pub content_share: ContentShareState,
    pub pip: PipState,
    pub voice_recipients: Arc<DashMap<PeerId, Vec<PeerId>>>,
    pub avatar_sync: AvatarSyncSystem,
    pub moderation: ModerationLists,
    pub global_state: Arc<RwLock<GlobalState>>,
    pub statistics: Statistics,
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

        let avatar_sync = AvatarSyncSystem::new(AvatarSyncConfig {
            default_interval_ms: config.bsrsmillisecond_default_interval.max(1) as u64,
            base_multiplier: config.bsrbase_multiplier as f32,
            increase_rate: config.bsrsincrease_rate,
            high_distance_sq: config.high_quality_distance * config.high_quality_distance,
            medium_distance_sq: config.medium_quality_distance * config.medium_quality_distance,
            low_distance_sq: config.low_quality_distance * config.low_quality_distance,
            enable_bundle_compression: config.enable_avatar_bundle_compression,
            bundle_min_messages: config.avatar_bundle_min_messages.max(1) as usize,
            bundle_min_bytes: config.avatar_bundle_min_bytes.max(0) as usize,
            min_receiver_slices: 1,
            max_receiver_slices: 32,
            tick_budget_ms: avatar_sync::DEFAULT_AVATAR_TICK_BUDGET_MS,
            receiver_cycle_budget_ms: avatar_sync::DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS,
        }
        .apply_env_tuning());

        let moderation = ModerationLists::file_backed(
            base_dir.join(ServerConfig::CONFIG_FOLDER_NAME),
        )?;

        let state = Self {
            config: Arc::new(RwLock::new(config.clone())),
            transport,
            authenticated_peers: Arc::new(DashMap::new()),
            pending_identity: Arc::new(DashMap::new()),
            permissions,
            database,
            resources: ResourceState::default(),
            net_ids: NetIdState::default(),
            ownership: OwnershipState::default(),
            content_share: ContentShareState::default(),
            pip: PipState::default(),
            voice_recipients: Arc::new(DashMap::new()),
            avatar_sync,
            moderation,
            global_state: Arc::new(RwLock::new(GlobalState::from(&config))),
            statistics: Statistics::default(),
            shutdown: Arc::new(AtomicBool::new(false)),
        };
        state
            .avatar_sync
            .spawn_tick_loop(state.transport.clone(), state.shutdown.clone(), {
                let peers = state.authenticated_peers.clone();
                move || peers.iter().map(|entry| *entry.key()).collect()
            });
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        tokio::spawn(event_loop(state.clone(), events, shutdown_rx));
        Ok((state, shutdown_tx))
    }

    pub fn player_count(&self) -> usize {
        self.authenticated_peers.len()
    }

    pub fn refresh_runtime_config(&self) {
        let config = self.config.read().clone();
        self.avatar_sync.update_config(AvatarSyncConfig {
            default_interval_ms: config.bsrsmillisecond_default_interval.max(1) as u64,
            base_multiplier: config.bsrbase_multiplier as f32,
            increase_rate: config.bsrsincrease_rate,
            high_distance_sq: config.high_quality_distance * config.high_quality_distance,
            medium_distance_sq: config.medium_quality_distance * config.medium_quality_distance,
            low_distance_sq: config.low_quality_distance * config.low_quality_distance,
            enable_bundle_compression: config.enable_avatar_bundle_compression,
            bundle_min_messages: config.avatar_bundle_min_messages.max(1) as usize,
            bundle_min_bytes: config.avatar_bundle_min_bytes.max(0) as usize,
            min_receiver_slices: 1,
            max_receiver_slices: 32,
            tick_budget_ms: avatar_sync::DEFAULT_AVATAR_TICK_BUDGET_MS,
            receiver_cycle_budget_ms: avatar_sync::DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS,
        }
        .apply_env_tuning());
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
            "Server is running and healthy\nPlayers: {}\nReliable: pending={} queued={}\nApp messages: inbound={} outbound={} protocol_errors={}\nRaw UDP: packets_in={} packets_out={} bytes_in={} bytes_out={} would_block={}\nAvatar sync: inbound_updates={} outbound_messages={} outbound_batches={} active_states={} pending_updates={} receiver_slices={}\nAvatar timing: ticks={} avg_tick_us={} smooth_tick_us={} avg_build_us={} avg_flush_us={} max_tick_us={} receiver_cycle_ms={} cycle_budget_ms={} tick_budget_ms={}",
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
            avatar.outbound_batches,
            avatar.active_states,
            avatar.pending_updates,
            avatar.slice_count,
            avatar.tick_count,
            avatar.avg_tick_micros,
            avatar.smoothed_tick_micros,
            if avatar.tick_count == 0 { 0 } else { avatar.build_micros / avatar.tick_count },
            if avatar.tick_count == 0 { 0 } else { avatar.flush_micros / avatar.tick_count },
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
            if Some(*peer.key()) == except {
                continue;
            }
            if self
                .transport
                .send(*peer.key(), channel, delivery, payload)
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

async fn event_loop(
    state: ServerState,
    mut events: mpsc::Receiver<ServerEvent>,
    mut shutdown: oneshot::Receiver<()>,
) {
    let worker_limit = std::thread::available_parallelism()
        .map(|count| (count.get() * 4).clamp(8, 256))
        .unwrap_or(32);
    let workers = Arc::new(Semaphore::new(worker_limit));
    loop {
        tokio::select! {
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
        ServerEvent::NetworkError(err) => {
            warn!("network error: {err}");
            Ok(())
        }
        ServerEvent::PeerConnected(_) => Ok(()),
    }
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
    if state.player_count() >= config.peer_limit as usize {
        state
            .transport
            .reject(&request, "Server is full! Rejected.")
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
    if client_version < SERVER_VERSION {
        state
            .transport
            .reject(&request, "Outdated client version.")
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
    state.permissions.get_or_create_user(&uuid);
    let config = state.config.read().clone();
    let metadata = ready.player_meta_data_message.clone();
    state.authenticated_peers.insert(
        peer_id,
        ConnectedPeer {
            id: peer_id,
            metadata: metadata.clone(),
            ready: ready.clone(),
        },
    );
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

    cache_initial_avatar_sync(state, peer_id, &ready);
    send_accept_fanout(state, peer_id, ready).await?;
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
    ready: ReadyMessage,
) -> Result<()> {
    let spawn = ServerReadyMessage {
        local_ready_message: ready.clone(),
        player_id_message: basis_protocol::messages::PlayerIdMessage { player_id: peer_id },
    };
    let mut spawn_writer = NetWriter::new();
    spawn.serialize(&mut spawn_writer);
    state
        .broadcast(
            channels::CREATE_REMOTE_PLAYER,
            DeliveryMethod::ReliableOrdered,
            spawn_writer.as_slice(),
            Some(peer_id),
        )
        .await;

    let mut existing_player_packets = Vec::new();
    for existing in state.authenticated_peers.iter() {
        if *existing.key() == peer_id {
            continue;
        }
        let message = ServerReadyMessage {
            local_ready_message: existing.ready.clone(),
            player_id_message: basis_protocol::messages::PlayerIdMessage {
                player_id: *existing.key(),
            },
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
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
    state.pending_identity.remove(&peer);
    state.voice_recipients.remove(&peer);
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
        removed.serialize(&mut writer);
        state
            .broadcast(
                channels::CONTENT_SHARE_CLEANUP,
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
        let mut writer = NetWriter::new();
        writer.put_u16(peer);
        state
            .broadcast(
                channels::DISCONNECTION,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
                Some(peer),
            )
            .await;
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
            if let Err(err) = state
                .avatar_sync
                .upsert_from_channel_payload(peer, channel, &payload)
            {
                state
                    .statistics
                    .protocol_errors
                    .fetch_add(1, Ordering::Relaxed);
                warn!("invalid avatar update from peer {peer}: {err}");
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
            if let Err(err) = state
                .avatar_sync
                .upsert_from_channel_payload(peer, channel, &payload)
            {
                state
                    .statistics
                    .protocol_errors
                    .fetch_add(1, Ordering::Relaxed);
                warn!("invalid avatar update from peer {peer}: {err}");
            }
        }
        channels::CHAT => {
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
            let avatar =
                basis_protocol::messages::ClientAvatarChangeMessage::deserialize(&mut reader)?;
            if state.global_state.read().avatars_locked && !has_protection_permission(state, peer) {
                return Ok(());
            }
            if let Some(mut peer_state) = state.authenticated_peers.get_mut(&peer) {
                peer_state.ready.client_avatar_change_message = avatar.clone();
            }
            let message = ServerAvatarChangeMessage {
                player_id: peer,
                client_avatar_change_message: avatar,
            };
            let mut writer = NetWriter::new();
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
        channels::NET_ID_ASSIGN => {
            let mut reader = NetReader::new(&payload);
            let request = NetIdMessage::deserialize(&mut reader)?;
            if request.player_id.is_empty() {
                return Ok(());
            }
            let existed = state
                .net_ids
                .all()
                .iter()
                .any(|(name, _)| name == &request.player_id);
            let id = state.net_ids.add_or_find(&request.player_id);
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
            let should_broadcast = if resource.load_strategy == 2 {
                let peers: Vec<u16> = state.authenticated_peers.iter().map(|p| *p.key()).collect();
                state.resources.start_preload(resource.clone(), &peers)
            } else {
                state.resources.load_resource(resource.clone())
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
        channels::STORE_DATABASE => {
            let mut reader = NetReader::new(&payload);
            let message = DatabasePrimitiveMessage::deserialize(&mut reader)?;
            let json_payload = serde_json::from_str(&message.json_payload)
                .unwrap_or(Value::String(message.json_payload));
            state.database.add_or_update(BasisData {
                name: message.name,
                json_payload,
            });
        }
        channels::REQUEST_STORE_DATABASE => {
            let mut reader = NetReader::new(&payload);
            let request = basis_protocol::messages::DataBaseRequest::deserialize(&mut reader)?;
            if let Some(data) = state.database.get(&request.database_id) {
                let message = DatabasePrimitiveMessage {
                    name: data.name,
                    json_payload: data.json_payload.to_string(),
                };
                let mut writer = NetWriter::new();
                message.serialize(&mut writer);
                state
                    .transport
                    .send(
                        peer,
                        channels::STORE_DATABASE,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                    )
                    .await?;
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
            let request = ContentShareMessage::deserialize(&mut reader)?;
            if content_locked(state, request.content_type, peer) {
                return Ok(());
            }
            let Some(peer_state) = state.authenticated_peers.get(&peer) else {
                return Ok(());
            };
            let Some(server_message) = state.content_share.add(
                peer,
                peer_state.metadata.player_uuid.clone(),
                peer_state.metadata.player_display_name.clone(),
                request,
            ) else {
                return Ok(());
            };
            drop(peer_state);
            let mut writer = NetWriter::new();
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
        channels::CONTENT_SHARE_CLEANUP => {
            let mut reader = NetReader::new(&payload);
            let request = ContentShareCleanupMessage::deserialize(&mut reader)?;
            if let Some(server_message) = state.content_share.remove(peer, request) {
                let mut writer = NetWriter::new();
                server_message.serialize(&mut writer);
                state
                    .broadcast(
                        channels::CONTENT_SHARE_CLEANUP,
                        DeliveryMethod::ReliableOrdered,
                        writer.as_slice(),
                        None,
                    )
                    .await;
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
            relay_voice_message(state, peer, &payload).await;
        }
        channels::SHOUT_VOICE => {
            relay_shout_voice_message(state, peer, &payload).await;
        }
        channels::AVATAR => {
            relay_avatar_generic(state, peer, delivery, &payload).await?;
        }
        channels::SCENE => {
            relay_scene_generic(state, peer, delivery, &payload).await?;
        }
        channels::EVENTS => {
            relay_event(state, peer, &payload).await?;
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
        channels::AVATAR,
        writer.as_slice(),
        &avatar.recipients,
    )
    .await
}

async fn relay_scene_generic(
    state: &ServerState,
    peer: PeerId,
    delivery: DeliveryMethod,
    payload: &[u8],
) -> Result<()> {
    let mut reader = NetReader::new(payload);
    let scene = SceneDataMessage::deserialize(&mut reader)?;
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
        channels::SCENE,
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
        if *recipient == peer || !state.authenticated_peers.contains_key(recipient) {
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

fn has_protection_permission(state: &ServerState, peer: PeerId) -> bool {
    let Some(peer_state) = state.authenticated_peers.get(&peer) else {
        return false;
    };
    state.permissions.has(
        &peer_state.metadata.player_uuid,
        basis_server_permissions::nodes::PROTECTION,
    )
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
    match request.mode {
        AdminRequestMode::GlobalToggleAvatars => {
            state.global_state.write().avatars_locked ^= true;
            broadcast_lock_state(state).await;
        }
        AdminRequestMode::GlobalToggleProps => {
            state.global_state.write().props_locked ^= true;
            broadcast_lock_state(state).await;
        }
        AdminRequestMode::GlobalToggleWorlds => {
            state.global_state.write().worlds_locked ^= true;
            broadcast_lock_state(state).await;
        }
        AdminRequestMode::GlobalToggleServers => {
            state.global_state.write().servers_locked ^= true;
            broadcast_lock_state(state).await;
        }
        AdminRequestMode::GlobalToggleThirdPerson => {
            state.global_state.write().third_person_disabled ^= true;
            broadcast_lock_state(state).await;
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
            if let (Ok(uuid), Ok(group)) = (reader.get_string(), reader.get_string()) {
                state.permissions.add_user_to_group(&uuid, &group);
                let _ = state.permissions.save_to_xml();
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::SetUserNode => {
            if let (Ok(uuid), Ok(node)) = (reader.get_string(), reader.get_string()) {
                state.permissions.add_user_node(&uuid, &node);
                let _ = state.permissions.save_to_xml();
                send_admin_text(state, peer, "Permission updated").await?;
            }
        }
        AdminRequestMode::SetGroupNode => {
            if let (Ok(group), Ok(node)) = (reader.get_string(), reader.get_string()) {
                state.permissions.add_group_node(&group, &node);
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
            if let (Ok(group), Ok(parent)) = (reader.get_string(), reader.get_string()) {
                state.permissions.add_group_parent(&group, &parent);
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
        AdminRequestMode::SetWhitelistMode => {
            let mode = reader.get_u8().unwrap_or(0);
            state.config.write().basis_user_restriction_mode = match mode {
                1 => basis_protocol::config::BasisUserRestrictionMode::WhiteList,
                2 => basis_protocol::config::BasisUserRestrictionMode::BlackList,
                _ => basis_protocol::config::BasisUserRestrictionMode::None,
            };
        }
        AdminRequestMode::AddWhitelist => {
            if let Ok(uuid) = reader.get_string() {
                state.moderation.add_whitelist(uuid)?;
            }
        }
        AdminRequestMode::RemoveWhitelist => {
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
    Ok(())
}

async fn send_lock_state_to_peer(state: &ServerState, peer_id: PeerId) -> Result<()> {
    let locks = state.global_state.read().clone();
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GlobalGetLockState,
    }
    .serialize(&mut writer);
    writer.put_bool(locks.avatars_locked);
    writer.put_bool(locks.props_locked);
    writer.put_bool(locks.worlds_locked);
    writer.put_bool(locks.servers_locked);
    writer.put_bool(locks.third_person_disabled);
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
    if let Err(err) = state
        .transport
        .send_many(
            peer_id,
            &[
                (
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    encode_lock_state_payload(&globals),
                ),
                (
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    encode_bool_admin_state_payload(
                        AdminRequestMode::GlobalGetHeadlessAudioState,
                        globals.headless_audio_off,
                    ),
                ),
                (
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    encode_bool_admin_state_payload(
                        AdminRequestMode::GlobalGetHeadlessDisallowState,
                        globals.disallow_headless,
                    ),
                ),
                (
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    encode_u8_admin_state_payload(
                        AdminRequestMode::GlobalGetOpusPacketLossState,
                        globals.opus_packet_loss_percent,
                    ),
                ),
                (
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    encode_u8_admin_state_payload(
                        AdminRequestMode::GlobalGetOpusFrameDurationState,
                        globals.opus_frame_duration_ms,
                    ),
                ),
                (
                    channels::ADMIN,
                    DeliveryMethod::ReliableOrdered,
                    encode_user_opus_bitrate_override_payload(0),
                ),
            ],
        )
        .await
    {
        warn!("failed to send initial admin state to peer {peer_id}: {err:#}");
    }
}

fn encode_lock_state_payload(locks: &GlobalState) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GlobalGetLockState,
    }
    .serialize(&mut writer);
    writer.put_bool(locks.avatars_locked);
    writer.put_bool(locks.props_locked);
    writer.put_bool(locks.worlds_locked);
    writer.put_bool(locks.servers_locked);
    writer.put_bool(locks.third_person_disabled);
    writer.into_vec()
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

fn encode_user_opus_bitrate_override_payload(value: i32) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::UserOpusBitrateOverride,
    }
    .serialize(&mut writer);
    writer.put_i32(value);
    writer.into_vec()
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

async fn broadcast_lock_state(state: &ServerState) {
    let locks = state.global_state.read().clone();
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GlobalGetLockState,
    }
    .serialize(&mut writer);
    writer.put_bool(locks.avatars_locked);
    writer.put_bool(locks.props_locked);
    writer.put_bool(locks.worlds_locked);
    writer.put_bool(locks.servers_locked);
    writer.put_bool(locks.third_person_disabled);
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
