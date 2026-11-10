//! Real connection and policy-failure regressions from the v57 review.
use super::*;
use basis_protocol::screen_share as protocol;

fn control_payload(sub: u8, write: impl FnOnce(&mut NetWriter)) -> Vec<u8> {
    let mut writer = NetWriter::new();
    writer.put_u8(sub);
    write(&mut writer);
    writer.into_vec()
}
fn is_control(payload: &[u8], sub: u8) -> bool {
    payload.len() >= 3 && payload[..2] == protocol::CONTROL_ID.to_le_bytes() && payload[2] == sub
}
fn is_media(payload: &[u8]) -> bool {
    payload.len() >= 4 && payload[..2] == protocol::MEDIA_ID.to_le_bytes()
}
async fn subscribed_client(state: &ServerState, uuid: &str) -> (Client, PeerId) {
    let client = Client::connect(state, uuid).await;
    let peer = peer_by_uuid(state, uuid).unwrap();
    state.message_subscriptions.insert(
        peer,
        HashSet::from([protocol::CONTROL_ID, protocol::MEDIA_ID]),
    );
    (client, peer)
}
async fn start_screen(state: &ServerState, sharer: PeerId) {
    let bytes = control_payload(protocol::START, |writer| {
        protocol::StreamInfo {
            width: 1280,
            height: 720,
            fps: 30,
            has_audio: false,
            title: "Review regression".into(),
        }
        .serialize(writer)
        .unwrap();
        protocol::ScreenPose {
            position: [0.; 3],
            rotation: [0., 0., 0., 1.],
            scale: 1.,
        }
        .serialize(writer)
        .unwrap();
    });
    crate::screen_share::handle(state, sharer, protocol::CONTROL_ID, &bytes)
        .await
        .unwrap();
}
async fn watch(state: &ServerState, viewer: PeerId, sharer: PeerId) {
    let bytes = control_payload(protocol::WATCH, |writer| {
        writer.put_u16(sharer);
        writer.put_bool(true);
    });
    crate::screen_share::handle(state, viewer, protocol::CONTROL_ID, &bytes)
        .await
        .unwrap();
}
fn frame_chunk(index: u16) -> Vec<u8> {
    let mut writer = NetWriter::new();
    protocol::ChunkHeader {
        sequence: 17,
        kind: protocol::KIND_VIDEO,
        flags: protocol::FLAG_KEY,
        index,
        count: 2,
        pts_us: 42,
        total_length: 1101,
    }
    .serialize(&mut writer)
    .unwrap();
    writer.put_bytes(&vec![7; if index == 0 { 1100 } else { 1 }]);
    writer.into_vec()
}

#[tokio::test]
async fn screen_review_multipart_frame_never_targets_recycled_viewer_connection() {
    let (state, shutdown, _) = super::super::tests::test_server(false).await;
    let (mut sharer_client, sharer) = subscribed_client(&state, "frame-sharer").await;
    let (mut first, first_id) = subscribed_client(&state, "frame-first-viewer").await;
    let (mut second, second_id) = subscribed_client(&state, "frame-retained-viewer").await;
    start_screen(&state, sharer).await;
    watch(&state, first_id, sharer).await;
    watch(&state, second_id, sharer).await;
    crate::screen_share::handle(&state, sharer, protocol::MEDIA_ID, &frame_chunk(0))
        .await
        .unwrap();
    first
        .receive(|c, p| c == channels::PLUGIN_RELIABLE && is_media(p))
        .await;
    second
        .receive(|c, p| c == channels::PLUGIN_RELIABLE && is_media(p))
        .await;

    let (_, old_session) = session_for_uuid(&state, "frame-first-viewer");
    request_disconnect(&state, &old_session, "frame viewer disconnect")
        .await
        .unwrap();
    first.receive_disconnect().await;
    wait_for_disconnect_cleanups(&state).await;
    tokio::join!(
        receive_leave_for(&mut sharer_client, first_id),
        receive_leave_for(&mut second, first_id)
    );
    wait_for_peer_id_recycle(&state, first_id).await;
    let (mut replacement, replacement_id) = subscribed_client(&state, "frame-replacement").await;
    let (_, replacement_session) = session_for_uuid(&state, "frame-replacement");
    assert_eq!(replacement_id, first_id);
    assert!(!replacement_session.same_connection(&old_session));

    // Replacement subscribes to the plugin but never watches this screen.
    crate::screen_share::handle(&state, sharer, protocol::MEDIA_ID, &frame_chunk(1))
        .await
        .unwrap();
    let continuation = second
        .receive(|c, p| c == channels::PLUGIN_RELIABLE && is_media(p))
        .await;
    let mut reader = NetReader::new(&continuation[4..]);
    assert_eq!(protocol::ChunkHeader::read(&mut reader).unwrap().index, 1);
    assert!(
        tokio::time::timeout(
            Duration::from_millis(150),
            replacement.receive_inner(|c, p| c == channels::PLUGIN_RELIABLE && is_media(p)),
        )
        .await
        .is_err(),
        "replacement connection received a frame it never watched"
    );
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}

#[tokio::test]
async fn screen_review_save_failure_still_ends_live_stream_and_broadcasts_policy() {
    let (mut state, shutdown, dir) = super::super::tests::test_server(false).await;
    let (mut admin, admin_id) = subscribed_client(&state, "screen-lock-admin").await;
    state.permissions.add_user_node(
        "screen-lock-admin",
        basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
    );
    let (_sharer, sharer_id) = subscribed_client(&state, "screen-lock-sharer").await;
    let (mut viewer, _) = subscribed_client(&state, "screen-lock-viewer").await;
    start_screen(&state, sharer_id).await;
    let invalid_path = dir.join("config-is-a-directory");
    std::fs::create_dir_all(&invalid_path).unwrap();
    state.config_path = Arc::new(invalid_path);
    state.config.write().has_file_support = true;
    crate::screen_share::handle(&state, admin_id, protocol::CONTROL_ID, &[protocol::LOCK, 1])
        .await
        .expect("persistence error must not abort the live moderation action");
    assert!(state.config.read().screen_share_locked);
    let policy = viewer
        .receive(|c, p| c == channels::PLUGIN_RELIABLE && is_control(p, protocol::POLICY))
        .await;
    assert_eq!(policy[3], 1);
    let ended = viewer
        .receive(|c, p| c == channels::PLUGIN_RELIABLE && is_control(p, protocol::ENDED))
        .await;
    assert_eq!(
        u16::from_le_bytes(ended[3..5].try_into().unwrap()),
        sharer_id
    );
    let reported = admin
        .receive(|c, p| {
            c == channels::ADMIN
                && p.first() == Some(&(AdminRequestMode::Message as u8))
                && NetReader::new(&p[1..])
                    .get_string()
                    .is_ok_and(|message| message.contains("saving the configuration failed"))
        })
        .await;
    assert!(NetReader::new(&reported[1..])
        .get_string()
        .unwrap()
        .contains("saving the configuration failed"));
    state.config.write().has_file_support = false;
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn screen_review_file_support_disabled_never_writes_configuration() {
    let (mut state, shutdown, dir) = super::super::tests::test_server(false).await;
    let (_admin, admin_id) = subscribed_client(&state, "screen-memory-admin").await;
    state.permissions.add_user_node(
        "screen-memory-admin",
        basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
    );
    let config_parent = dir.join("must-not-be-created");
    state.config_path = Arc::new(config_parent.join("config.xml"));
    crate::screen_share::handle(&state, admin_id, protocol::CONTROL_ID, &[protocol::LOCK, 1])
        .await
        .unwrap();
    assert!(state.config.read().screen_share_locked);
    assert!(!config_parent.exists());
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}
