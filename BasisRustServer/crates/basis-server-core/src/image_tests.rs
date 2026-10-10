use super::*;

async fn server() -> ServerState {
    let (state, _shutdown) = ServerState::start(
        ServerConfig {
            has_file_support: false,
            use_auth: false,
            use_auth_identity: false,
            set_port: 0,
            override_auto_discovery_of_ipv: true,
            ipv4_address: "127.0.0.1".into(),
            image_share_egress_megabits_per_second: 1,
            image_share_egress_enforcement_percent: 100,
            ..ServerConfig::default()
        },
        &std::env::temp_dir(),
    )
    .await
    .unwrap();
    state
}

fn add_peer(state: &ServerState, id: PeerId) {
    let metadata = ClientMetaDataMessage {
        player_uuid: format!("image-test-{id}"),
        player_display_name: format!("Peer {id}"),
        player_platform: "linux".into(),
    };
    state.authenticated_peers.insert(
        id,
        ConnectedPeer {
            id,
            metadata: metadata.clone(),
            ready: ReadyMessage {
                player_meta_data_message: metadata,
                client_avatar_change_message: basis_protocol::messages::ClientAvatarChangeMessage {
                    load_mode: 0,
                    byte_array: Vec::new(),
                    local_avatar_index: 0,
                    arm_scale: 1.0,
                    leg_scale: 1.0,
                    torso_scale: 1.0,
                },
                local_avatar_sync_message:
                    basis_protocol::messages::LocalAvatarSyncMessage::empty_high(),
            },
            session: None,
        },
    );
}

async fn relay(
    state: &ServerState,
    peer: PeerId,
    index: u16,
    recipients: Vec<PeerId>,
    payload: Vec<u8>,
    channel: u8,
) {
    let mut writer = NetWriter::new();
    SceneDataMessage {
        message_index: index,
        recipients,
        payload,
    }
    .serialize(&mut writer)
    .unwrap();
    relay_scene_generic(
        state,
        peer,
        DeliveryMethod::ReliableOrdered,
        channel,
        writer.as_slice(),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn image_relay_governor_covers_both_scene_channels_and_fanout_modes() {
    let state = server().await;
    state.config.write().image_cache_enabled = false;
    for id in 1..=40 {
        add_peer(&state, id);
    }
    let index = state
        .net_ids
        .add_or_find_for_peer(image_cache::IMAGE_MANAGER_IDENTIFIER, 1, 10)
        .unwrap()
        .0;
    // Broadcast costs 39 recipients. It admits one oversized chunk, then debt
    // blocks a direct-scene relay from the same sender rather than a fresh bucket.
    relay(&state, 1, index, vec![], vec![0; 32_768], channels::SCENE).await;
    relay(
        &state,
        1,
        index,
        (2..=40).collect(),
        vec![0; 32_768],
        channels::DIRECT_SCENE_SERVER,
    )
    .await;
    assert_eq!(state.image_egress_dropped(), (1, 32_768 * 39));
    relay(&state, 2, index, vec![3], vec![0; 32_768], channels::SCENE).await;
    assert_eq!(state.image_egress_dropped().0, 1);
    let ordinary = state
        .net_ids
        .add_or_find_for_peer("OtherScene", 1, 10)
        .unwrap()
        .0;
    relay(
        &state,
        1,
        ordinary,
        vec![],
        vec![0; 32_768],
        channels::SCENE,
    )
    .await;
    assert_eq!(state.image_egress_dropped().0, 1);
    state.config.write().image_share_egress_megabits_per_second = 0;
    relay(&state, 1, index, vec![], vec![0; 32_768], channels::SCENE).await;
    assert_eq!(state.image_egress_dropped().0, 1);
    state.shutdown().await.unwrap();
    assert_eq!(state.image_egress_dropped(), (0, 0));
}

#[tokio::test]
async fn image_relay_populates_cache_and_applies_gif_lock_with_permission_bypass() {
    let state = server().await;
    add_peer(&state, 1);
    let index = state
        .net_ids
        .add_or_find_for_peer(image_cache::IMAGE_MANAGER_IDENTIFIER, 1, 10)
        .unwrap()
        .0;
    // BinaryWriter spawn: opcode, GUID, owner, seven-bit name length, dimensions,
    // byte count, chunk count, position, quaternion. Cache treats pixels as opaque.
    let mut spawn = vec![1];
    spawn.extend([42; 16]);
    spawn.extend(1u16.to_le_bytes());
    spawn.push(0);
    for value in [8i32, 8, 1, 1] {
        spawn.extend(value.to_le_bytes());
    }
    spawn.extend([0; 28]);
    relay(&state, 1, index, vec![], spawn, channels::SCENE).await;
    let mut chunk = vec![2];
    chunk.extend([42; 16]);
    chunk.extend(0i32.to_le_bytes());
    chunk.extend(1i32.to_le_bytes());
    chunk.push(99);
    relay(
        &state,
        1,
        index,
        vec![],
        chunk,
        channels::DIRECT_SCENE_SERVER,
    )
    .await;
    assert_eq!(
        (state.image_cache_stats().0, state.image_cache_stats().1),
        (1, 1)
    );
    // Animation headers and chunks must not reach either cache or relay when locked.
    state.global_state.write().gifs_locked = true;
    let mut animation = vec![6];
    animation.extend([42; 16]);
    animation.push(1);
    animation.extend(1i32.to_le_bytes());
    animation.extend(1i32.to_le_bytes());
    animation.extend(0i64.to_le_bytes());
    let bytes = state.image_cache_stats().2;
    relay(&state, 1, index, vec![], animation.clone(), channels::SCENE).await;
    assert_eq!(state.image_cache_stats().2, bytes);
    state.permissions.add_user_node(
        "image-test-1",
        basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
    );
    relay(&state, 1, index, vec![], animation, channels::SCENE).await;
    assert!(state.image_cache_stats().2 > bytes);
    state.shutdown().await.unwrap();
    assert_eq!(state.image_cache_stats(), (0, 0, 0));
}

#[tokio::test]
async fn rejected_image_data_is_not_cached_but_cache_controls_still_work() {
    let state = server().await;
    add_peer(&state, 1);
    add_peer(&state, 2);
    let index = state
        .net_ids
        .add_or_find_for_peer(image_cache::IMAGE_MANAGER_IDENTIFIER, 1, 10)
        .unwrap()
        .0;
    let spawn = |id, owner: u16| {
        let mut payload = vec![1];
        payload.extend([id; 16]);
        payload.extend(owner.to_le_bytes());
        payload.push(0);
        for value in [8i32, 8, 1, 1] {
            payload.extend(value.to_le_bytes());
        }
        payload.extend([0; 28]);
        payload
    };
    let chunk = |id| {
        let mut payload = vec![2];
        payload.extend([id; 16]);
        payload.extend(0i32.to_le_bytes());
        payload.extend(1i32.to_le_bytes());
        payload.push(99);
        payload
    };
    relay(&state, 1, index, vec![2], spawn(1, 1), channels::SCENE).await;
    // Force substantial debt so this check cannot depend on test execution speed.
    assert!(state.image_governor.try_consume_egress(
        1,
        125_000_000,
        &state.config.read(),
        Instant::now()
    ));
    relay(&state, 1, index, vec![2], spawn(2, 1), channels::SCENE).await;
    relay(
        &state,
        1,
        index,
        vec![2],
        chunk(1),
        channels::DIRECT_SCENE_SERVER,
    )
    .await;
    assert_eq!(
        (state.image_cache_stats().0, state.image_cache_stats().1),
        (1, 0)
    );
    let mut despawn = vec![4];
    despawn.extend([1; 16]);
    relay(&state, 1, index, vec![2], despawn, channels::SCENE).await;
    assert_eq!(state.image_cache_stats(), (0, 0, 0));

    relay(&state, 2, index, vec![2], spawn(3, 2), channels::SCENE).await;
    relay(&state, 2, index, vec![2], chunk(3), channels::SCENE).await;
    let mut request = vec![10];
    request.extend([3; 16]);
    relay(&state, 1, index, vec![2], request.clone(), channels::SCENE).await;
    // A second request is empty only if the first was processed despite upload debt.
    assert!(state
        .image_cache
        .lock()
        .request(1, &request, true, &state.config.read())
        .is_empty());
    assert_eq!(state.image_egress_dropped().0, 4);
    state.shutdown().await.unwrap();
}
