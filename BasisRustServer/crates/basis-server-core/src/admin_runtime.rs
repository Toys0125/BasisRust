//! Moderation session state and the admin operations shared with BasisVR.
use super::*;
use basis_server_admin::LocomotionPolicy;
use basis_server_resources::DefaultLibrary;
use unicode_general_category::{get_general_category, GeneralCategory};

#[derive(Debug)]
pub(super) struct AdminRuntime {
    queries: Mutex<HashMap<PeerId, QueryBucket>>,
    permission_retries: Mutex<HashSet<PeerId>>,
    announcing: RwLock<HashSet<PeerId>>,
    shouting: RwLock<HashSet<PeerId>>,
    rejoin_population: RwLock<HashSet<String>>,
    library: RwLock<DefaultLibrary>,
    library_wire: RwLock<Option<Vec<u8>>>,
    library_path: PathBuf,
}

impl AdminRuntime {
    pub(super) fn load(base_dir: &Path, config: &ServerConfig) -> Result<Self> {
        let library_path = base_dir.join(ServerConfig::DEFAULT_LIBRARY_FOLDER_NAME);
        let library = if config.has_file_support {
            DefaultLibrary::load_xml_dir(&library_path)?
        } else {
            DefaultLibrary::default()
        };
        let wire = match library.encode_library() {
            Ok(wire) => Some(wire),
            Err(error) => {
                warn!("default library loaded but cannot be broadcast: {error:#}; remove entries to fit the client packet limit");
                None
            }
        };
        Ok(Self {
            queries: Mutex::new(HashMap::new()),
            permission_retries: Mutex::new(HashSet::new()),
            announcing: RwLock::new(HashSet::new()),
            shouting: RwLock::new(HashSet::new()),
            rejoin_population: RwLock::new(HashSet::new()),
            library: RwLock::new(library),
            library_wire: RwLock::new(wire),
            library_path,
        })
    }

    pub(super) fn remove_peer(&self, peer: PeerId) {
        self.queries.lock().remove(&peer);
        self.permission_retries.lock().remove(&peer);
        self.announcing.write().remove(&peer);
        self.shouting.write().remove(&peer);
        // The lockdown population deliberately survives disconnects.
    }

    pub(super) fn can_rejoin(&self, uuid: &str) -> bool {
        self.rejoin_population.read().contains(uuid)
    }

    pub(super) fn is_announcing(&self, peer: PeerId) -> bool {
        self.announcing.read().contains(&peer)
    }
}

#[derive(Debug)]
struct QueryBucket {
    tokens: f64,
    updated: Instant,
}

impl QueryBucket {
    fn consume(&mut self, now: Instant) -> bool {
        self.tokens =
            (self.tokens + now.duration_since(self.updated).as_secs_f64() * 10.0).min(60.0);
        self.updated = now;
        if self.tokens < 1.0 {
            return false;
        }
        self.tokens -= 1.0;
        true
    }
}

fn peer_uuid(state: &ServerState, peer: PeerId) -> Option<String> {
    state
        .authenticated_peers
        .get(&peer)
        .map(|p| p.metadata.player_uuid.clone())
}

pub(super) fn is_text_muted(state: &ServerState, peer: PeerId) -> bool {
    peer_uuid(state, peer).is_some_and(|uuid| state.moderation.mute_state(&uuid).1)
}

pub(super) fn is_voice_muted(state: &ServerState, peer: PeerId) -> bool {
    peer_uuid(state, peer).is_some_and(|uuid| state.moderation.mute_state(&uuid).0)
}

pub(super) fn spawn_permission_updates(state: ServerState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        while !state.shutdown.load(Ordering::Relaxed) {
            interval.tick().await;
            if let Err(err) = sync_permission_changes(&state).await {
                warn!("permission update failed: {err:#}");
            }
            if let Err(err) = state.permissions.save_if_due() {
                warn!("permission save failed: {err:#}");
            }
        }
        if let Err(err) = state.permissions.flush_pending_save() {
            warn!("permission shutdown save failed: {err:#}");
        }
    })
}

async fn sync_permission_changes(state: &ServerState) -> Result<()> {
    let changes = state.permissions.take_changes();
    if changes.is_empty() && state.admin_runtime.permission_retries.lock().is_empty() {
        return Ok(());
    }
    let all = changes.iter().any(Option::is_none);
    let mut targets = state
        .authenticated_peers
        .iter()
        .filter_map(|p| {
            (all || changes.iter().flatten().any(|uuid| {
                basis_protocol::permissions::ordinal_ignore_case_equal(
                    uuid,
                    &p.metadata.player_uuid,
                )
            }))
            .then_some(*p.key())
        })
        .collect::<HashSet<_>>();
    targets.extend(std::mem::take(
        &mut *state.admin_runtime.permission_retries.lock(),
    ));
    for target in targets {
        if let Err(err) = send_permission_metadata(state, target).await {
            warn!("permission update for peer {target} failed: {err:#}");
            if state.authenticated_peers.contains_key(&target) {
                state.admin_runtime.permission_retries.lock().insert(target);
            }
        }
    }
    Ok(())
}

async fn send_permission_metadata(state: &ServerState, peer: PeerId) -> Result<()> {
    let Some(metadata) = state
        .authenticated_peers
        .get(&peer)
        .map(|p| p.metadata.clone())
    else {
        return Ok(());
    };
    let config = state.config.read().clone();
    let uuid = &metadata.player_uuid;
    let message = ServerMetaDataMessage {
        allowed_permissions: state.permissions.allowed_rules(uuid),
        denied_permissions: state.permissions.denied_rules(uuid),
        client_meta_data_message: metadata,
        sync_interval: config.bsrsmillisecond_default_interval,
        base_multiplier: config.bsrbase_multiplier,
        increase_rate: config.bsrsincrease_rate,
        slowest_send_rate: config.bsrslowest_send_rate,
        peer_limit: config.peer_limit,
        uplink_delta_enabled: config.enable_uplink_avatar_delta,
        image_share_egress_megabits_per_second: config.image_share_egress_megabits_per_second,
        image_pickup_range_meters: config.image_pickup_range_meters.max(0.0),
    };
    let mut writer = NetWriter::new();
    message.serialize(&mut writer);
    state
        .transport
        .send(
            peer,
            channels::META_DATA,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
        )
        .await?;
    Ok(())
}

pub(super) async fn send_join_state(state: &ServerState, peer: PeerId) -> Result<()> {
    let policy = LocomotionPolicy::from(&*state.config.read());
    send_admin_payload_to_peer(state, peer, policy_payload(&policy)).await?;
    if let Some(uuid) = peer_uuid(state, peer) {
        if state.moderation.mute_state(&uuid) != (false, false) {
            send_mute_to_target(state, &uuid).await?;
        }
    }
    let modes = [
        (
            AdminRequestMode::EnableAnnounceMode,
            state
                .admin_runtime
                .announcing
                .read()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
        ),
        (
            AdminRequestMode::EnableShoutMode,
            state
                .admin_runtime
                .shouting
                .read()
                .iter()
                .copied()
                .collect::<Vec<_>>(),
        ),
    ];
    for (mode, players) in modes {
        for target in players {
            send_admin_payload_to_peer(state, peer, mode_payload(mode, target, target)).await?;
        }
    }
    let wire = state.admin_runtime.library_wire.read().clone();
    if let Some(wire) = wire {
        state
            .transport
            .send(
                peer,
                channels::SERVER_LIBRARY,
                DeliveryMethod::ReliableOrdered,
                &wire,
            )
            .await?;
    }
    Ok(())
}

fn policy_payload(policy: &LocomotionPolicy) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::GlobalGetLocomotionPolicy,
    }
    .serialize(&mut writer);
    policy.serialize(&mut writer);
    writer.into_vec()
}

fn mode_payload(mode: AdminRequestMode, target: PeerId, initiator: PeerId) -> Vec<u8> {
    let mut writer = NetWriter::new();
    AdminRequest { mode }.serialize(&mut writer);
    writer.put_u16(target);
    writer.put_u16(initiator);
    writer.into_vec()
}

fn save_config(state: &ServerState) -> Result<()> {
    let config = state.config.read();
    if config.has_file_support {
        config.save(&state.config_path)?;
    }
    Ok(())
}

pub(super) async fn handle_request(
    state: &ServerState,
    peer: PeerId,
    mode: AdminRequestMode,
    reader: &mut NetReader<'_>,
) -> Result<bool> {
    match mode {
        AdminRequestMode::Ban | AdminRequestMode::Kick | AdminRequestMode::IpAndBan => {
            let uuid = reader.get_string()?;
            let reason = reader.get_string()?;
            let target = if uuid.is_empty() {
                Err("UUID invalid")
            } else if reason.is_empty() {
                Err("Reason invalid")
            } else if let Some(target) = peer_by_uuid(state, &uuid) {
                if state
                    .permissions
                    .has(&uuid, basis_server_permissions::nodes::PROTECTION)
                {
                    Err("Target is protected")
                } else {
                    Ok(target)
                }
            } else {
                Err("Player not found")
            };
            match target {
                Err(error) => send_admin_text(state, peer, error).await?,
                Ok(target) => {
                    if mode != AdminRequestMode::Kick {
                        let ip = if mode == AdminRequestMode::IpAndBan {
                            state
                                .transport
                                .peer_snapshots()
                                .into_iter()
                                .find(|p| p.id == target)
                                .map(|p| p.addr.ip().to_string())
                        } else {
                            None
                        };
                        state.moderation.add_ban_with_details(&uuid, &reason, ip)?;
                    }
                    state.transport.disconnect(target, &reason).await?;
                    send_admin_text(
                        state,
                        peer,
                        &format!(
                            "Player {uuid} {}.",
                            if mode == AdminRequestMode::Kick {
                                "kicked"
                            } else {
                                "banned"
                            }
                        ),
                    )
                    .await?;
                }
            }
        }
        AdminRequestMode::SetVoiceMute | AdminRequestMode::SetTextMute => {
            let uuid = reader.get_string()?;
            let muted = reader.get_bool()?;
            if uuid.trim().is_empty() {
                send_admin_text(state, peer, "UUID invalid").await?;
            } else if state
                .permissions
                .has(&uuid, basis_server_permissions::nodes::PROTECTION)
            {
                send_admin_text(state, peer, "Target is protected").await?;
            } else {
                if mode == AdminRequestMode::SetVoiceMute {
                    state.moderation.set_voice_mute(&uuid, muted)?;
                } else {
                    state.moderation.set_text_mute(&uuid, muted)?;
                }
                send_mute_to_target(state, &uuid).await?;
                send_admin_text(
                    state,
                    peer,
                    if muted {
                        "Player muted."
                    } else {
                        "Player unmuted."
                    },
                )
                .await?;
            }
            send_mute_result(state, peer, &uuid).await?;
        }
        AdminRequestMode::GetMuteState => {
            let uuid = reader.get_string()?;
            send_mute_result(state, peer, &uuid).await?;
        }
        AdminRequestMode::QueryPermission => handle_query(state, peer, reader).await?,
        AdminRequestMode::RenamePlayer => handle_rename(state, peer, reader).await?,
        AdminRequestMode::EnableAnnounceMode
        | AdminRequestMode::DisableAnnounceMode
        | AdminRequestMode::EnableShoutMode
        | AdminRequestMode::DisableShoutMode => {
            let target = reader.get_u16()?;
            if state.authenticated_peers.contains_key(&target) {
                let enabled = matches!(
                    mode,
                    AdminRequestMode::EnableAnnounceMode | AdminRequestMode::EnableShoutMode
                );
                let set = if matches!(
                    mode,
                    AdminRequestMode::EnableAnnounceMode | AdminRequestMode::DisableAnnounceMode
                ) {
                    &state.admin_runtime.announcing
                } else {
                    &state.admin_runtime.shouting
                };
                if enabled {
                    set.write().insert(target);
                } else {
                    set.write().remove(&target);
                }
                broadcast_admin_payload(state, mode_payload(mode, target, peer)).await;
            }
        }
        AdminRequestMode::SetGlobalLocomotionPolicy => {
            let policy = LocomotionPolicy::deserialize(reader)?;
            policy.write_to_config(&mut state.config.write());
            save_config(state)?;
            broadcast_admin_payload(state, policy_payload(&policy)).await;
            send_admin_text(state, peer, "Instance locomotion policy updated.").await?;
        }
        AdminRequestMode::GlobalGetLocomotionPolicy => {
            let policy = LocomotionPolicy::from(&*state.config.read());
            send_admin_payload_to_peer(state, peer, policy_payload(&policy)).await?;
        }
        AdminRequestMode::GlobalToggleGifs => {
            toggle_simple_lock(state, |s| &mut s.gifs_locked, |c| &mut c.gifs_locked).await;
            save_config(state)?;
        }
        AdminRequestMode::SetAllowlistMode => {
            let value = reader.get_u8()?;
            let restriction = match value {
                0 => BasisUserRestrictionMode::Normal,
                1 => BasisUserRestrictionMode::BanList,
                2 => BasisUserRestrictionMode::AllowList,
                3 => BasisUserRestrictionMode::RejoinOnly,
                _ => {
                    send_admin_text(
                        state,
                        peer,
                        &format!("Unknown restriction mode value {value}."),
                    )
                    .await?;
                    return Ok(true);
                }
            };
            set_restriction_mode(state, restriction);
            save_config(state)?;
            broadcast_lock_state(state).await;
            send_admin_text(state, peer, "Restriction mode updated.").await?;
        }
        AdminRequestMode::AddDefaultLibraryItem | AdminRequestMode::RemoveDefaultLibraryItem => {
            handle_library(state, peer, mode, reader).await?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

fn set_restriction_mode(state: &ServerState, mode: BasisUserRestrictionMode) {
    let mut config = state.config.write();
    if mode == BasisUserRestrictionMode::RejoinOnly {
        *state.admin_runtime.rejoin_population.write() = state
            .authenticated_peers
            .iter()
            .map(|p| p.metadata.player_uuid.clone())
            .collect();
    } else {
        state.admin_runtime.rejoin_population.write().clear();
    }
    config.basis_user_restriction_mode = mode;
    state.global_state.write().restriction_mode = mode as u8;
}

pub(super) fn refresh_rejoin_population(
    state: &ServerState,
    previous: u8,
    mode: BasisUserRestrictionMode,
) {
    if mode == BasisUserRestrictionMode::RejoinOnly {
        if previous != mode as u8 {
            *state.admin_runtime.rejoin_population.write() = state
                .authenticated_peers
                .iter()
                .map(|p| p.metadata.player_uuid.clone())
                .collect();
        }
    } else {
        state.admin_runtime.rejoin_population.write().clear();
    }
}

pub(super) async fn broadcast_locomotion_policy(state: &ServerState) {
    let policy = LocomotionPolicy::from(&*state.config.read());
    policy.write_to_config(&mut state.config.write());
    broadcast_admin_payload(state, policy_payload(&policy)).await;
}

async fn send_mute_to_target(state: &ServerState, uuid: &str) -> Result<()> {
    if let Some(target) = peer_by_uuid(state, uuid) {
        let (voice, text) = state.moderation.mute_state(uuid);
        let mut writer = NetWriter::new();
        AdminRequest {
            mode: AdminRequestMode::MuteStateApply,
        }
        .serialize(&mut writer);
        writer.put_bool(voice);
        writer.put_bool(text);
        send_admin_payload_to_peer(state, target, writer.into_vec()).await?;
    }
    Ok(())
}

async fn send_mute_result(state: &ServerState, peer: PeerId, uuid: &str) -> Result<()> {
    let (voice, text) = state.moderation.mute_state(uuid);
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::MuteStateResult,
    }
    .serialize(&mut writer);
    writer.put_string(uuid);
    writer.put_bool(voice);
    writer.put_bool(text);
    send_admin_payload_to_peer(state, peer, writer.into_vec()).await
}

async fn handle_query(state: &ServerState, peer: PeerId, reader: &mut NetReader<'_>) -> Result<()> {
    let target = reader.get_u16()?;
    let kind = reader.get_u8()?;
    let value = reader.get_string()?;
    let now = Instant::now();
    if !state
        .admin_runtime
        .queries
        .lock()
        .entry(peer)
        .or_insert(QueryBucket {
            tokens: 60.0,
            updated: now,
        })
        .consume(now)
    {
        return Ok(());
    }
    let valid = !value.trim().is_empty() && value.encode_utf16().count() <= 128;
    let uuid = valid.then(|| peer_uuid(state, target)).flatten();
    let found = uuid.is_some();
    let held = uuid.is_some_and(|uuid| {
        if kind == 1 {
            state.permissions.is_in_group(&uuid, &value)
        } else {
            state.permissions.has(&uuid, &value)
        }
    });
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::QueryPermissionResult,
    }
    .serialize(&mut writer);
    writer.put_u16(target);
    writer.put_u8(kind);
    writer.put_string(if valid { &value } else { "" });
    writer.put_bool(held);
    writer.put_bool(found);
    send_admin_payload_to_peer(state, peer, writer.into_vec()).await
}

fn sanitize_display_name(name: &str) -> String {
    name.chars()
        .filter(|ch| {
            // BasisVR checks UTF-16 char categories; supplementary scalars are surrogates there.
            let is_format =
                ch.len_utf16() == 1 && get_general_category(*ch) == GeneralCategory::Format;
            !(ch.is_control()
                || is_format
                || matches!(
                    ch,
                    '\u{115f}' | '\u{1160}' | '\u{3164}' | '\u{ffa0}' | '\u{2800}' | '\u{180e}'
                ))
        })
        .map(|ch| if ch.is_whitespace() { ' ' } else { ch })
        .collect::<String>()
        .trim()
        .to_string()
}

async fn handle_rename(
    state: &ServerState,
    peer: PeerId,
    reader: &mut NetReader<'_>,
) -> Result<()> {
    let target = reader.get_u16()?;
    let name = sanitize_display_name(&reader.get_string()?);
    if name.is_empty() {
        send_admin_text(state, peer, "Name invalid").await?;
    } else if !state.authenticated_peers.contains_key(&target) {
        send_admin_text(state, peer, "Player not found").await?;
    } else if target != peer && has_protection_permission(state, target) {
        send_admin_text(state, peer, "Target is protected").await?;
    } else {
        if let Some(mut target_peer) = state.authenticated_peers.get_mut(&target) {
            target_peer.metadata.player_display_name = name.clone();
            target_peer
                .ready
                .player_meta_data_message
                .player_display_name = name.clone();
            state
                .join_broadcast
                .lock()
                .update_peer_ready(target, target_peer.ready.clone());
        }
        let mut writer = NetWriter::new();
        AdminRequest {
            mode: AdminRequestMode::RenamePlayer,
        }
        .serialize(&mut writer);
        writer.put_u16(target);
        writer.put_string(&name);
        writer.put_u16(peer);
        broadcast_admin_payload(state, writer.into_vec()).await;
        send_admin_text(
            state,
            peer,
            &format!("Player {target} renamed to '{name}'."),
        )
        .await?;
    }
    Ok(())
}

async fn handle_library(
    state: &ServerState,
    peer: PeerId,
    mode: AdminRequestMode,
    reader: &mut NetReader<'_>,
) -> Result<()> {
    let file_support = state.config.read().has_file_support;
    let result = (|| -> Result<Option<Vec<u8>>> {
        let mut library = state.admin_runtime.library.write();
        let mut candidate = library.clone();
        if mode == AdminRequestMode::AddDefaultLibraryItem {
            let item_mode = reader.get_u8()?;
            let url = reader.get_string()?;
            let password = reader.get_string()?;
            candidate.add_item_in_memory(item_mode, &url, &password)?;
            let wire = candidate.encode_library()?;
            if file_support {
                library.add_item(
                    &state.admin_runtime.library_path,
                    item_mode,
                    &url,
                    &password,
                )?;
            } else {
                *library = candidate;
            }
            Ok(Some(wire))
        } else {
            let url = reader.get_string()?;
            candidate.remove_item_in_memory(&url)?;
            // Existing oversized collections remain editable until enough entries are removed.
            let wire = candidate.encode_library().ok();
            if file_support {
                library.remove_item(&state.admin_runtime.library_path, &url)?;
            } else {
                *library = candidate;
            }
            Ok(wire)
        }
    })();
    match result {
        Ok(wire) => {
            *state.admin_runtime.library_wire.write() = wire.clone();
            if let Some(wire) = wire {
                state
                    .broadcast(
                        channels::SERVER_LIBRARY,
                        DeliveryMethod::ReliableOrdered,
                        &wire,
                        None,
                    )
                    .await;
                send_admin_text(state, peer, "Default library updated.").await?;
            } else {
                warn!("default library updated but still exceeds client packet limits");
                send_admin_text(
                    state,
                    peer,
                    "Default library updated; remove more entries to fit the client packet limit.",
                )
                .await?;
            }
        }
        Err(err) => {
            send_admin_text(
                state,
                peer,
                &format!("Default library update failed: {err:#}"),
            )
            .await?
        }
    }
    Ok(())
}

#[cfg(test)]
mod wire_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permission_queries_have_a_bounded_burst_and_refill() {
        let now = Instant::now();
        let mut bucket = QueryBucket {
            tokens: 60.0,
            updated: now,
        };
        for _ in 0..60 {
            assert!(bucket.consume(now));
        }
        assert!(!bucket.consume(now));
        assert!(bucket.consume(now + Duration::from_millis(100)));
        assert!(!bucket.consume(now + Duration::from_millis(100)));
    }

    #[test]
    fn rename_strips_invisible_names_and_folds_whitespace() {
        assert_eq!(sanitize_display_name("\u{200b}\u{3164}\0\u{202e}"), "");
        assert_eq!(
            sanitize_display_name("  Marcus\u{00a0}VR\u{200b}  "),
            "Marcus VR"
        );
        assert_eq!(sanitize_display_name("\tA\nB"), "AB");
        assert_eq!(sanitize_display_name("A\u{e0001}"), "A\u{e0001}");
    }

    #[test]
    fn announce_and_shout_use_distinct_ids_and_include_initiator() {
        assert_eq!(
            mode_payload(AdminRequestMode::EnableAnnounceMode, 7, 9),
            vec![16, 7, 0, 9, 0]
        );
        assert_eq!(
            mode_payload(AdminRequestMode::EnableShoutMode, 7, 9),
            vec![88, 7, 0, 9, 0]
        );
    }

    pub(super) async fn test_server(files: bool) -> (ServerState, oneshot::Sender<()>, PathBuf) {
        let dir =
            std::env::temp_dir().join(format!("basis-runtime-parity-{}", uuid::Uuid::new_v4()));
        let config = ServerConfig {
            has_file_support: files,
            use_auth: false,
            use_auth_identity: false,
            override_auto_discovery_of_ipv: true,
            ipv4_address: "127.0.0.1".to_string(),
            set_port: 0,
            ..ServerConfig::default()
        };
        let (state, shutdown) = ServerState::start(config, &dir).await.unwrap();
        (state, shutdown, dir)
    }

    pub(super) fn ready_message(uuid: &str) -> ReadyMessage {
        let metadata = ClientMetaDataMessage {
            player_uuid: uuid.to_string(),
            player_display_name: "Original".to_string(),
            player_platform: "linux".to_string(),
        };
        ReadyMessage {
            player_meta_data_message: metadata.clone(),
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

    fn add_peer(state: &ServerState, id: PeerId, uuid: &str) {
        let ready = ready_message(uuid);
        let metadata = ready.player_meta_data_message.clone();
        state.authenticated_peers.insert(
            id,
            ConnectedPeer {
                id,
                metadata,
                ready,
            },
        );
    }

    async fn request(
        state: &ServerState,
        peer: PeerId,
        mode: AdminRequestMode,
        payload: impl FnOnce(&mut NetWriter),
    ) {
        let mut writer = NetWriter::new();
        AdminRequest { mode }.serialize(&mut writer);
        payload(&mut writer);
        handle_admin_message(state, peer, writer.as_slice())
            .await
            .unwrap();
    }

    #[test]
    fn critical_gates_match_basisvr_contract() {
        use basis_server_permissions::nodes;
        for (mode, node) in [
            (AdminRequestMode::GetPermissions, nodes::PERMISSIONS_VIEW),
            (AdminRequestMode::SetVoiceMute, nodes::MODERATION_MUTE),
            (AdminRequestMode::SetTextMute, nodes::MODERATION_MUTE),
            (AdminRequestMode::GetMuteState, nodes::MODERATION_MUTE),
            (AdminRequestMode::RenamePlayer, nodes::MODERATION_RENAME),
            (
                AdminRequestMode::EnableAnnounceMode,
                nodes::MODERATION_ANNOUNCE,
            ),
            (
                AdminRequestMode::DisableAnnounceMode,
                nodes::MODERATION_ANNOUNCE,
            ),
            (
                AdminRequestMode::EnableShoutMode,
                nodes::MODERATION_ANNOUNCE,
            ),
            (
                AdminRequestMode::DisableShoutMode,
                nodes::MODERATION_ANNOUNCE,
            ),
            (
                AdminRequestMode::GlobalToggleGifs,
                nodes::MODERATION_GLOBAL_LOCK,
            ),
            (
                AdminRequestMode::SetGlobalLocomotionPolicy,
                nodes::MODERATION_GLOBAL_LOCK,
            ),
        ] {
            assert_eq!(admin_mode_required_permission(mode), Some(node), "{mode:?}");
        }
        assert_eq!(
            admin_mode_required_permission(AdminRequestMode::QueryPermission),
            None
        );
    }

    #[test]
    fn complete_pinned_basisvr_catalog_defaults_and_gates_match() {
        // This fixture was extracted from BasisVR source, independently of the Rust tables.
        let contract: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/basisvr-admin-contract.json"
        ))
        .unwrap();
        let strings = |name: &str| {
            contract[name]
                .as_array()
                .unwrap()
                .iter()
                .map(|v| v.as_str().unwrap().to_string())
                .collect::<HashSet<_>>()
        };
        assert_eq!(
            strings("permission_nodes"),
            basis_protocol::permissions::nodes::ALL_NODES
                .iter()
                .map(|n| n.to_string())
                .collect()
        );
        assert_eq!(
            strings("default_nodes"),
            basis_protocol::permissions::DEFAULT_GROUP_NODES
                .iter()
                .map(|n| n.to_string())
                .collect()
        );
        assert_eq!(
            strings("moderator_nodes"),
            basis_protocol::permissions::MODERATOR_GROUP_NODES
                .iter()
                .map(|n| n.to_string())
                .collect()
        );
        for (id, expected_name) in contract["admin_modes"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
        {
            let mode = AdminRequestMode::from(id as u8);
            assert_eq!(
                format!("{mode:?}"),
                expected_name.as_str().unwrap(),
                "mode ID {id}"
            );
            let node = contract["required_permissions"][expected_name.as_str().unwrap()].as_str();
            assert_eq!(admin_mode_required_permission(mode), node, "{mode:?}");
        }
    }

    #[tokio::test]
    async fn unauthorized_actions_and_protected_targets_do_not_mutate_state() {
        let (state, shutdown, dir) = test_server(false).await;
        add_peer(&state, 1, "ordinary");
        add_peer(&state, 2, "staff");
        add_peer(&state, 3, "protected");
        state.permissions.add_user_to_group("staff", "moderator");
        state
            .permissions
            .add_user_node("protected", basis_server_permissions::nodes::PROTECTION);
        request(&state, 1, AdminRequestMode::GlobalToggleGifs, |_| {}).await;
        assert!(!state.global_state.read().gifs_locked);
        request(&state, 1, AdminRequestMode::SetVoiceMute, |w| {
            w.put_string("staff");
            w.put_bool(true);
        })
        .await;
        assert_eq!(state.moderation.mute_state("staff"), (false, false));
        request(&state, 1, AdminRequestMode::RenamePlayer, |w| {
            w.put_u16(2);
            w.put_string("Unauthorized");
        })
        .await;
        assert_eq!(
            state
                .authenticated_peers
                .get(&2)
                .unwrap()
                .metadata
                .player_display_name,
            "Original"
        );
        for mode in [AdminRequestMode::Ban, AdminRequestMode::IpAndBan] {
            request(&state, 2, mode, |w| {
                w.put_string("protected");
                w.put_string("reason");
            })
            .await;
            assert!(!state.moderation.is_uuid_banned("protected"));
        }
        request(&state, 2, AdminRequestMode::Ban, |w| {
            w.put_string("absent");
            w.put_string("reason");
        })
        .await;
        assert!(!state.moderation.is_uuid_banned("absent"));
        request(&state, 2, AdminRequestMode::Ban, |w| {
            w.put_string("ordinary");
            w.put_string("");
        })
        .await;
        assert!(!state.moderation.is_uuid_banned("ordinary"));
        request(&state, 2, AdminRequestMode::SetTextMute, |w| {
            w.put_string("protected");
            w.put_bool(true);
        })
        .await;
        assert_eq!(state.moderation.mute_state("protected"), (false, false));
        state.shutdown().await.unwrap();
        let _ = shutdown.send(());
        assert!(
            !dir.exists(),
            "disk-disabled startup/actions/shutdown must not create files"
        );
    }

    #[tokio::test]
    async fn mutes_rename_and_self_release_follow_runtime_permissions() {
        let (state, shutdown, _) = test_server(false).await;
        add_peer(&state, 1, "staff");
        add_peer(&state, 2, "target");
        state.permissions.add_user_to_group("staff", "moderator");
        request(&state, 1, AdminRequestMode::SetVoiceMute, |w| {
            w.put_string("target");
            w.put_bool(true);
        })
        .await;
        request(&state, 1, AdminRequestMode::SetTextMute, |w| {
            w.put_string("target");
            w.put_bool(true);
        })
        .await;
        assert!(is_voice_muted(&state, 2) && is_text_muted(&state, 2));
        request(&state, 1, AdminRequestMode::SetVoiceMute, |w| {
            w.put_string("target");
            w.put_bool(false);
        })
        .await;
        assert!(!is_voice_muted(&state, 2) && is_text_muted(&state, 2));
        request(&state, 1, AdminRequestMode::RenamePlayer, |w| {
            w.put_u16(2);
            w.put_string("  Updated\u{200b}  ");
        })
        .await;
        let target = state.authenticated_peers.get(&2).unwrap().clone();
        assert_eq!(target.metadata.player_display_name, "Updated");
        assert_eq!(
            target.ready.player_meta_data_message.player_display_name,
            "Updated"
        );
        request(&state, 1, AdminRequestMode::EnableAnnounceMode, |w| {
            w.put_u16(2)
        })
        .await;
        request(&state, 1, AdminRequestMode::EnableShoutMode, |w| {
            w.put_u16(2)
        })
        .await;
        assert!(state.admin_runtime.is_announcing(2));
        assert!(state.admin_runtime.shouting.read().contains(&2));
        request(&state, 2, AdminRequestMode::DisableAnnounceMode, |w| {
            w.put_u16(2)
        })
        .await;
        request(&state, 2, AdminRequestMode::DisableShoutMode, |w| {
            w.put_u16(2)
        })
        .await;
        assert!(!state.admin_runtime.is_announcing(2));
        assert!(!state.admin_runtime.shouting.read().contains(&2));
        state.shutdown().await.unwrap();
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn restriction_values_capture_and_clear_population_for_wire_and_console_updates() {
        let (state, shutdown, _) = test_server(false).await;
        add_peer(&state, 1, "admin");
        add_peer(&state, 2, "captured");
        state.permissions.add_user_to_group("admin", "admin");
        request(&state, 1, AdminRequestMode::SetAllowlistMode, |w| {
            w.put_u8(3)
        })
        .await;
        assert!(state.admin_runtime.can_rejoin("captured"));
        state.authenticated_peers.remove(&2);
        state.admin_runtime.remove_peer(2);
        assert!(state.admin_runtime.can_rejoin("captured"));
        add_peer(&state, 3, "late");
        assert!(!state.admin_runtime.can_rejoin("late"));
        request(&state, 1, AdminRequestMode::SetAllowlistMode, |w| {
            w.put_u8(3)
        })
        .await;
        assert!(!state.admin_runtime.can_rejoin("captured"));
        assert!(state.admin_runtime.can_rejoin("late"));
        request(&state, 1, AdminRequestMode::SetAllowlistMode, |w| {
            w.put_u8(1)
        })
        .await;
        assert_eq!(
            state.config.read().basis_user_restriction_mode,
            BasisUserRestrictionMode::BanList
        );
        assert!(!state.admin_runtime.can_rejoin("late"));
        request(&state, 1, AdminRequestMode::SetAllowlistMode, |w| {
            w.put_u8(2)
        })
        .await;
        assert_eq!(
            state.config.read().basis_user_restriction_mode,
            BasisUserRestrictionMode::AllowList
        );
        state.config.write().basis_user_restriction_mode = BasisUserRestrictionMode::RejoinOnly;
        state.refresh_runtime_config_live().await;
        assert!(state.admin_runtime.can_rejoin("late"));
        request(&state, 1, AdminRequestMode::SetAllowlistMode, |w| {
            w.put_u8(255)
        })
        .await;
        assert_eq!(
            state.config.read().basis_user_restriction_mode,
            BasisUserRestrictionMode::RejoinOnly
        );
        state.shutdown().await.unwrap();
        let _ = shutdown.send(());
    }

    #[tokio::test]
    async fn immediate_shutdown_flushes_permission_changes_and_malformed_imports_survive() {
        let (state, shutdown, dir) = test_server(true).await;
        state.permissions.add_user_to_group("saved", "admin");
        state.shutdown().await.unwrap();
        let _ = shutdown.send(());
        let loaded = PermissionManager::new(dir.join("config/permissions.xml"));
        loaded.load_from_xml().unwrap();
        assert!(loaded.has("saved", basis_server_permissions::nodes::PERMISSIONS_EDIT));
        let malformed = "<Permissions><Groups>";
        fs::write(dir.join("config/permissions.xml"), malformed).unwrap();
        assert!(ServerState::start(ServerConfig::default(), &dir)
            .await
            .is_err());
        assert_eq!(
            fs::read_to_string(dir.join("config/permissions.xml")).unwrap(),
            malformed
        );
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn truncated_server_settings_preserve_live_and_persisted_values() {
        let (state, shutdown, dir) = test_server(true).await;
        add_peer(&state, 1, "admin");
        state.permissions.add_user_to_group("admin", "admin");
        request(&state, 1, AdminRequestMode::SetServerName, |w| {
            w.put_string("Existing server")
        })
        .await;
        request(&state, 1, AdminRequestMode::SetServerMotd, |w| {
            w.put_string("Existing MOTD")
        })
        .await;
        let config_path = dir.join("config/config.xml");
        let before = fs::read(&config_path).unwrap();
        for mode in [
            AdminRequestMode::SetServerName,
            AdminRequestMode::SetServerMotd,
        ] {
            let mut malformed = NetWriter::new();
            AdminRequest { mode }.serialize(&mut malformed);
            malformed.put_u16(50); // Advertised string bytes never arrive.
            assert!(handle_admin_message(&state, 1, malformed.as_slice())
                .await
                .is_err());
            assert_eq!(state.config.read().server_name, "Existing server");
            assert_eq!(state.config.read().server_motd, "Existing MOTD");
            assert_eq!(fs::read(&config_path).unwrap(), before);
        }
        // An explicitly supplied empty value remains a valid, deliberate edit.
        request(&state, 1, AdminRequestMode::SetServerMotd, |w| {
            w.put_string("")
        })
        .await;
        assert!(ServerConfig::load_or_create(&config_path)
            .unwrap()
            .server_motd
            .is_empty());
        state.shutdown().await.unwrap();
        let _ = shutdown.send(());
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn oversized_existing_library_starts_and_can_be_reduced_without_data_loss() {
        let dir =
            std::env::temp_dir().join(format!("basis-large-library-{}", uuid::Uuid::new_v4()));
        let path = dir.join(ServerConfig::DEFAULT_LIBRARY_FOLDER_NAME);
        let mut library = DefaultLibrary::default();
        for i in 0..3 {
            library
                .add_item(&path, 0, &format!("{i}{}", "x".repeat(35_000)), "")
                .unwrap();
        }
        assert!(library.encode_library().is_err());
        let config = ServerConfig {
            has_file_support: true,
            use_auth: false,
            use_auth_identity: false,
            override_auto_discovery_of_ipv: true,
            ipv4_address: "127.0.0.1".into(),
            set_port: 0,
            ..ServerConfig::default()
        };
        let (state, shutdown) = ServerState::start(config, &dir).await.unwrap();
        assert_eq!(state.admin_runtime.library.read().entries.len(), 3);
        assert!(state.admin_runtime.library_wire.read().is_none());
        add_peer(&state, 1, "admin");
        state.permissions.add_user_to_group("admin", "admin");
        for i in 0..2 {
            request(&state, 1, AdminRequestMode::RemoveDefaultLibraryItem, |w| {
                w.put_string(&library.entries[i].url);
            })
            .await;
            assert_eq!(
                DefaultLibrary::load_xml_dir(&path).unwrap().entries.len(),
                2 - i
            );
            assert_eq!(state.admin_runtime.library.read().entries.len(), 2 - i);
            assert_eq!(state.admin_runtime.library_wire.read().is_some(), i == 1);
        }
        state.shutdown().await.unwrap();
        let _ = shutdown.send(());
        fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn config_policy_and_library_actions_persist_and_reload() {
        let (state, shutdown, dir) = test_server(true).await;
        add_peer(&state, 1, "admin");
        state.permissions.add_user_to_group("admin", "admin");
        request(
            &state,
            1,
            AdminRequestMode::SetGlobalLocomotionPolicy,
            |w| {
                w.put_u8(255);
                w.put_f32(f32::NAN);
                w.put_f32(12.0);
                w.put_f32(20.0);
                w.put_f32(4.0);
                w.put_u8(9);
            },
        )
        .await;
        request(&state, 1, AdminRequestMode::GlobalToggleGifs, |_| {}).await;
        request(&state, 1, AdminRequestMode::AddDefaultLibraryItem, |w| {
            w.put_u8(1);
            w.put_string("https://example/world#c2VjcmV0");
            w.put_string("");
        })
        .await;
        request(&state, 1, AdminRequestMode::SetVoiceMute, |w| {
            w.put_string("offline");
            w.put_bool(true);
        })
        .await;
        let saved = ServerConfig::load_or_create(&dir.join("config/config.xml")).unwrap();
        assert!(saved.gifs_locked);
        assert_eq!(saved.locomotion_policy_fields, 31);
        assert_eq!(saved.locomotion_policy_jump_height, 1.0);
        assert_eq!(saved.locomotion_policy_gravity, 0.0);
        assert_eq!(saved.locomotion_policy_mode, 0);
        let library = DefaultLibrary::load_xml_dir(&dir.join("defaultlibrary")).unwrap();
        assert_eq!(library.entries.len(), 1);
        assert_eq!(library.entries[0].password, "secret");
        // Reject an oversized broadcast before changing the files or live library.
        request(&state, 1, AdminRequestMode::AddDefaultLibraryItem, |w| {
            w.put_u8(1);
            w.put_string(&"x".repeat(60_000));
            w.put_string(&"p".repeat(10_000));
        })
        .await;
        assert_eq!(state.admin_runtime.library.read().entries.len(), 1);
        assert_eq!(
            DefaultLibrary::load_xml_dir(&dir.join("defaultlibrary"))
                .unwrap()
                .entries
                .len(),
            1
        );
        assert_eq!(
            ModerationLists::file_backed(dir.join("config"))
                .unwrap()
                .mute_state("offline"),
            (true, false)
        );
        request(&state, 1, AdminRequestMode::RemoveDefaultLibraryItem, |w| {
            w.put_string("https://example/world")
        })
        .await;
        assert!(DefaultLibrary::load_xml_dir(&dir.join("defaultlibrary"))
            .unwrap()
            .entries
            .is_empty());
        state.shutdown().await.unwrap();
        let _ = shutdown.send(());
        fs::remove_dir_all(dir).unwrap();
    }
}
