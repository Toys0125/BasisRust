//! Regression coverage for v57 moderation and configuration synchronization.
use super::*;

#[tokio::test]
async fn locked_video_rejects_forged_live_uploads_but_allows_moderator_and_cleanup() {
    use basis_protocol::video_clip::{chunk_header, Opcode, MANAGER_IDENTIFIER};
    let (state, shutdown, dir) = super::super::tests::test_server(false).await;
    let _owner = Client::connect(&state, "locked-clip-owner").await;
    let mut viewer = Client::connect(&state, "locked-clip-viewer").await;
    let owner_id = peer_by_uuid(&state, "locked-clip-owner").unwrap();
    let viewer_id = peer_by_uuid(&state, "locked-clip-viewer").unwrap();
    let message_index = state
        .net_ids
        .add_or_find_for_peer(MANAGER_IDENTIFIER, owner_id, 100)
        .unwrap()
        .0;
    state.global_state.write().video_clips_locked = true;
    state.config.write().video_clips_locked = true;
    let id = [91; 16];
    let mut offer = vec![Opcode::Offer as u8];
    offer.extend(id);
    offer.extend(3i32.to_le_bytes());
    offer.extend([0, 1]); // A modified sender lies about bypassing the lock.
    offer.extend([0; 32]);
    offer.extend(0u16.to_le_bytes());
    async fn relay(state: &ServerState, owner: u16, viewer: u16, index: u16, bytes: Vec<u8>) {
        let mut writer = NetWriter::new();
        SceneDataMessage {
            message_index: index,
            recipients: vec![viewer],
            payload: bytes,
        }
        .serialize(&mut writer)
        .unwrap();
        crate::relay_scene_generic(
            state,
            owner,
            DeliveryMethod::ReliableOrdered,
            channels::SCENE,
            writer.as_slice(),
        )
        .await
        .unwrap();
    }
    let is_clip = |channel: u8, p: &[u8]| {
        channel == channels::SCENE && p.len() >= 5 && p[2..4] == message_index.to_le_bytes()
    };
    relay(&state, owner_id, viewer_id, message_index, offer.clone()).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), viewer.receive(is_clip))
            .await
            .is_err()
    );
    state
        .permissions
        .add_user_to_group("locked-clip-owner", "moderator");
    relay(&state, owner_id, viewer_id, message_index, offer).await;
    assert_eq!(viewer.receive(is_clip).await[4], Opcode::Offer as u8);
    state
        .permissions
        .remove_user_from_group("locked-clip-owner", "moderator");
    let mut chunk = chunk_header(id, 0);
    chunk.extend([1, 2, 3]);
    relay(&state, owner_id, viewer_id, message_index, chunk.clone()).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), viewer.receive(is_clip))
            .await
            .is_err()
    );
    state
        .permissions
        .add_user_to_group("locked-clip-owner", "moderator");
    relay(&state, owner_id, viewer_id, message_index, chunk).await;
    assert_eq!(viewer.receive(is_clip).await[4], Opcode::Chunk as u8);
    state
        .permissions
        .remove_user_from_group("locked-clip-owner", "moderator");
    let mut remove = vec![Opcode::Remove as u8];
    remove.extend(id);
    relay(&state, owner_id, viewer_id, message_index, remove).await;
    assert_eq!(viewer.receive(is_clip).await[4], Opcode::Remove as u8);
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn protection_editors_require_permission_and_receive_each_others_updates() {
    let (state, shutdown, dir) = super::super::tests::test_server(false).await;
    for uuid in ["editor-a", "editor-b", "editor-without-capability"] {
        state.permissions.add_user_to_group(uuid, "admin");
    }
    let mut a = Client::connect_pending_with_capabilities(
        &state,
        "editor-a",
        &[],
        SERVER_VERSION,
        None,
        Some(2),
    )
    .await;
    let mut b = Client::connect_pending_with_capabilities(
        &state,
        "editor-b",
        &[],
        SERVER_VERSION,
        None,
        Some(2),
    )
    .await;
    let mut ordinary = Client::connect_pending_with_capabilities(
        &state,
        "not-editor",
        &[],
        SERVER_VERSION,
        None,
        Some(2),
    )
    .await;
    let mut no_capability = Client::connect(&state, "editor-without-capability").await;
    let settings = |c: u8, p: &[u8]| {
        c == channels::ADMIN && admin_mode(p, AdminRequestMode::GlobalGetProtectionSettings)
    };
    for client in [&mut a, &mut b] {
        client.receive(settings).await;
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(100), ordinary.receive(settings))
            .await
            .is_err()
    );
    let id = peer_by_uuid(&state, "editor-a").unwrap();
    let mut change = NetWriter::new();
    change.put_u8(AdminRequestMode::SetProtectionSettings as u8);
    change.put_u16(1);
    change.put_string("ScreenShareMaxConcurrent").unwrap();
    change.put_f64(17.0);
    crate::handle_admin_message(&state, id, change.as_slice())
        .await
        .unwrap();
    for client in [&mut a, &mut b] {
        let p = client.receive(settings).await;
        let mut reader = NetReader::new(&p);
        reader.get_u8().unwrap();
        let count = reader.get_u16().unwrap();
        let mut found = false;
        for _ in 0..count {
            let key = reader.get_string().unwrap();
            for _ in 0..3 {
                reader.get_string().unwrap();
            }
            reader.get_bool().unwrap();
            reader.get_f64().unwrap();
            reader.get_f64().unwrap();
            let value = reader.get_f64().unwrap();
            if key == "ScreenShareMaxConcurrent" {
                assert_eq!(value, 17.0);
                found = true;
            }
        }
        assert!(found);
    }
    for client in [&mut ordinary, &mut no_capability] {
        assert!(
            tokio::time::timeout(Duration::from_millis(100), client.receive(settings))
                .await
                .is_err()
        );
    }
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
    let _ = std::fs::remove_dir_all(dir);
}
