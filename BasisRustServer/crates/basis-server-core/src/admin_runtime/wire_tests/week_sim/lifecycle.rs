//! Voice subscriptions must not survive their recipient's disconnect.
use super::super::*;
use super::traffic::{expected_voice_downlink, voice_payload, voice_recipients, RecipientMode};
use super::transport::WirePeer;
use std::time::Duration;

async fn wait_state(predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server state did not converge");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn departed_voice_recipient_is_removed_before_peer_id_reuse() {
    let (state, shutdown, dir) = super::super::super::tests::test_server(false).await;
    let mut sender = WirePeer::connect(&state, "lifecycle-voice-sender").await;
    let mut target = WirePeer::connect(&state, "lifecycle-voice-target").await;
    let mut observer = WirePeer::connect(&state, "lifecycle-voice-observer").await;

    let (channel, recipients) =
        voice_recipients(&[target.id, observer.id], RecipientMode::Large).unwrap();
    sender.send(channel, &recipients, true).await;
    wait_state(|| {
        state
            .voice_recipients
            .get(&sender.id)
            .is_some_and(|current| current.as_slice() == [target.id, observer.id])
    })
    .await;

    let opus = [0x13, 0x37, 0x00, 0xfe, 0x42];
    let packet = voice_payload(7, &opus);
    let (downlink_channel, expected) = expected_voice_downlink(sender.id, &packet);
    sender.send(channels::VOICE, &packet, false).await;
    observer
        .wait_until(|view| view.latest.get(&downlink_channel) == Some(&expected))
        .await;
    target
        .wait_until(|view| view.latest.get(&downlink_channel) == Some(&expected))
        .await;

    let departed_id = target.id;
    let old_session = target.session.clone();
    target.disconnect().await;
    wait_state(|| {
        !state.transport.is_current_session(&old_session)
            && !state.authenticated_peers.contains_key(&departed_id)
    })
    .await;

    observer
        .wait_until(|v| !v.roster.contains(&departed_id))
        .await;
    sender
        .wait_until(|v| !v.roster.contains(&departed_id))
        .await;
    wait_state(|| !state.retiring_peer_ids.contains_key(&departed_id)).await;
    assert!(state
        .voice_recipients
        .iter()
        .all(|entry| !entry.value().contains(&departed_id)));

    let mut replacement = WirePeer::connect(&state, "lifecycle-voice-replacement").await;
    assert_eq!(replacement.id, departed_id, "test requires peer ID reuse");
    let before = replacement
        .snapshot()
        .channel_counts
        .get(&downlink_channel)
        .copied()
        .unwrap_or(0);
    let next = voice_payload(8, &[0xa5, 0x5a, 0x01, 0x02]);
    let (downlink_channel, expected) = expected_voice_downlink(sender.id, &next);
    sender.send(channels::VOICE, &next, false).await;
    observer
        .wait_until(|view| view.latest.get(&downlink_channel) == Some(&expected))
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        replacement
            .snapshot()
            .channel_counts
            .get(&downlink_channel)
            .copied()
            .unwrap_or(0),
        before,
        "replacement must not inherit the departed peer's voice subscription"
    );

    let (channel, recipients) =
        voice_recipients(&[replacement.id, observer.id], RecipientMode::Large).unwrap();
    sender.send(channel, &recipients, true).await;
    wait_state(|| {
        state
            .voice_recipients
            .get(&sender.id)
            .is_some_and(|current| current.as_slice() == [replacement.id, observer.id])
    })
    .await;
    let explicit = voice_payload(9, &[0x10, 0x20, 0x30, 0x40]);
    let (downlink_channel, expected) = expected_voice_downlink(sender.id, &explicit);
    sender.send(channels::VOICE, &explicit, false).await;
    replacement
        .wait_until(|view| view.latest.get(&downlink_channel) == Some(&expected))
        .await;
    observer
        .wait_until(|view| view.latest.get(&downlink_channel) == Some(&expected))
        .await;

    replacement.disconnect().await;
    sender.disconnect().await;
    observer.disconnect().await;
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
    let _ = std::fs::remove_dir_all(dir);
}
