//! Executable-to-server regressions. Wire expectations are pinned to BasisVR
//! 81f190b217c11c2b39231e0bc9db330fd4a2803c; see README.md for source paths/limits.
mod support;

use std::collections::{HashMap, HashSet};

use basis_protocol::channels;
use basis_transport::{DeliveryMethod, PacketProperty};
use support::{Fault, Live, LIMIT, SILENCE};
use tokio::time::{timeout, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_password_rejects_actual_client_before_identity_or_player_admission() {
    Live::start("loopback-wrong-password", 1, false, Fault::None)
        .await
        .run(|live| {
            Box::pin(async move {
                let rejected = live
                    .until(|p| p.from_server && p.property() == PacketProperty::Disconnect)
                    .await;
                // LiteNetLib Disconnect: property + timestamp + string. The encoded string
                // length includes one; the wire carries no terminating NUL.
                let len = u16::from_le_bytes(rejected.bytes[9..11].try_into().unwrap()) as usize;
                assert_eq!(
                    &rejected.bytes[11..11 + len - 1],
                    b"Authentication failed, Auth rejected"
                );
                assert_eq!(live.server.player_count(), 0);
                assert_eq!(live.server.transport.connected_peers_count(), 0);
                live.await_client_rejection().await;
            })
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupted_identity_signature_rejects_transport_accepted_client() {
    Live::start("default_password", 1, false, Fault::InvalidSignature)
        .await
        .run(|live| {
            Box::pin(async move {
                let deadline = Instant::now() + LIMIT;
                let mut transport_accepted = false;
                let mut response_sent = false;
                loop {
                    let packet = live.next(deadline).await;
                    transport_accepted |=
                        packet.from_server && packet.property() == PacketProperty::ConnectAccept;
                    response_sent |=
                        !packet.from_server && packet.channeled(channels::AUTH_IDENTITY);
                    if packet.from_server && packet.property() == PacketProperty::Disconnect {
                        let len =
                            u16::from_le_bytes(packet.bytes[9..11].try_into().unwrap()) as usize;
                        let reason = std::str::from_utf8(&packet.bytes[11..11 + len - 1]).unwrap();
                        assert!(
                            reason.starts_with("Identity verification failed:"),
                            "{reason}"
                        );
                        break;
                    }
                    assert!(
                        !(packet.from_server && packet.channeled(channels::META_DATA)),
                        "invalid identity received post-auth metadata"
                    );
                }
                assert!(transport_accepted && response_sent);
                assert_eq!(live.server.player_count(), 0);
                live.await_client_rejection().await;
            })
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_auth_and_reliable_ack_recover_loss_across_a_full_window() {
    Live::start("default_password", 1, false, Fault::ReliableLoss)
        .await
        .run(|live| {
            Box::pin(async move {
                let deadline = Instant::now() + LIMIT;
                let mut challenge = None;
                let mut response = None;
                let mut dropped_response = None;
                let mut metadata = false;
                let mut challenge_ack = false;
                while !(metadata && response.is_some() && challenge_ack) {
                    let packet = live.next(deadline).await;
                    if packet.channeled(channels::AUTH_IDENTITY) {
                        if packet.from_server {
                            // BytesMessage = u16 length + challenge bytes.
                            let len =
                                u16::from_le_bytes(packet.bytes[4..6].try_into().unwrap()) as usize;
                            assert_eq!(len, 32);
                            challenge = Some(packet.bytes[6..6 + len].to_vec());
                        } else if packet.dropped {
                            dropped_response = Some(packet.bytes);
                        } else {
                            assert_eq!(
                                Some(&packet.bytes),
                                dropped_response.as_ref(),
                                "auth resend changed bytes"
                            );
                            response = Some(packet.bytes);
                        }
                    } else if !packet.from_server
                        && packet.property() == PacketProperty::Ack
                        && packet.bytes[3] == 2
                    {
                        // C# ReliableChannel S1/S7: absolute bit zero, window anchored at zero.
                        assert_eq!(packet.bytes.len(), 21);
                        assert_eq!(&packet.bytes[1..4], &[0, 0, 2]);
                        assert_eq!(
                            &packet.bytes[4..],
                            &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                        );
                        challenge_ack = true;
                    } else if packet.from_server && packet.channeled(channels::META_DATA) {
                        metadata = true;
                    }
                }
                assert_eq!(live.server.player_count(), 1);
                let peer = live.server.authenticated_peers.iter().next().unwrap();
                let peer_id = *peer.key();
                let did = &peer.value().metadata.player_uuid;
                let key = bs58::decode(did.strip_prefix("did:key:z").unwrap())
                    .into_vec()
                    .unwrap();
                assert_eq!(&key[..2], &[0xed, 1]);
                let key =
                    ed25519_dalek::VerifyingKey::from_bytes(key[2..].try_into().unwrap()).unwrap();
                let response = response.unwrap();
                assert_eq!(&response[4..6], &[64, 0]);
                assert_eq!(&response[70..], &[3, 0, b'N', b'/', b'A']);
                key.verify_strict(
                    &challenge.unwrap(),
                    &ed25519_dalek::Signature::from_slice(&response[6..70]).unwrap(),
                )
                .unwrap();
                drop(peer);

                // CHAT has no application side effects in the load client, but uses its real reliable
                // receive/ACK loop. 160 messages force window refill beyond the 128-slot ACK bitmap.
                for sequence in 0u16..160 {
                    live.server
                        .transport
                        .send(
                            peer_id,
                            channels::CHAT,
                            DeliveryMethod::ReliableOrdered,
                            &sequence.to_le_bytes(),
                        )
                        .await
                        .unwrap();
                }
                let deadline = Instant::now() + LIMIT;
                let mut delivered = HashSet::new();
                let mut dropped_zero = false;
                let mut anchored_after_loss = false;
                let mut acknowledged = HashSet::new();
                while delivered.len() < 160 || acknowledged.len() < 160 || !anchored_after_loss {
                    let packet = live.next(deadline).await;
                    if packet.from_server && packet.channeled(channels::CHAT) {
                        let sequence = packet.sequence();
                        assert_eq!(&packet.bytes[4..], &sequence.to_le_bytes());
                        if packet.dropped {
                            dropped_zero |= sequence == 0;
                        } else {
                            delivered.insert(sequence);
                        }
                    } else if !packet.from_server
                        && packet.property() == PacketProperty::Ack
                        && packet.bytes[3] == channels::CHAT * 4 + 2
                    {
                        assert_eq!(packet.bytes.len(), 21);
                        assert_eq!(packet.bytes[20], 0, "C# ACK padding byte");
                        let start = packet.sequence();
                        // Absolute bitmap positions, independently checked against captured delivery;
                        // a relative bitmap would falsely acknowledge unseen sequences after sliding.
                        for sequence in start..start + 128 {
                            let bit = sequence as usize % 128;
                            if packet.bytes[4 + bit / 8] & (1 << (bit % 8)) != 0 {
                                assert!(
                                    delivered.contains(&sequence),
                                    "ACK for undelivered {sequence}"
                                );
                                acknowledged.insert(sequence);
                            }
                        }
                        if dropped_zero && !delivered.contains(&0) {
                            assert_eq!(start, 0);
                            assert_eq!(packet.bytes[4] & 1, 0);
                            anchored_after_loss = true;
                        }
                    }
                }
                assert!(dropped_zero);
                // An ACK capture precedes its forwarding. Poll the exposed queue condition under a
                // deadline, rather than sleeping and assuming the transport has processed it.
                timeout(LIMIT, async {
                    let mut tick = tokio::time::interval(std::time::Duration::from_millis(5));
                    while live.server.transport.pending_reliable_count() != 0
                        || live.server.transport.queued_reliable_count() != 0
                    {
                        tick.tick().await;
                    }
                })
                .await
                .expect("server reliable queues did not drain");
                live.finish().await;
            })
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_clients_forward_avatar_state_and_exact_opus_payload_to_other_peer() {
    Live::start("default_password", 2, true, Fault::None)
        .await
        .run(|live| {
            Box::pin(async move {
                let deadline = Instant::now() + LIMIT;
                let mut peers = HashMap::new();
                let mut uplink_voice = HashSet::new();
                let mut forwarded_voice = HashSet::new();
                let mut forwarded_avatar = HashSet::new();
                let mut uplink_avatar = HashSet::new();
                while peers.len() < 2 || forwarded_voice.len() < 2 || forwarded_avatar.len() < 2 {
                    let packet = live.next(deadline).await;
                    match packet.property() {
                        PacketProperty::ConnectAccept if packet.from_server => {
                            assert_eq!(packet.bytes.len(), 15);
                            let id =
                                i32::from_le_bytes(packet.bytes[11..15].try_into().unwrap()) as u16;
                            peers.insert(packet.client, id);
                        }
                        PacketProperty::Unreliable
                            if !packet.from_server
                                && packet.bytes[1] == channels::PLAYER_AVATAR_HIGH =>
                        {
                            assert_eq!(packet.bytes.len(), 2 + 1 + 159);
                            uplink_avatar.insert((
                                peers[&packet.client],
                                packet.bytes[2],
                                packet.bytes[3..].to_vec(),
                            ));
                        }
                        PacketProperty::Unreliable if packet.bytes[1] == channels::VOICE => {
                            if packet.from_server {
                                // Pinned C# small-ID audio layout: id, sequence, silence count, Opus.
                                assert_eq!(packet.bytes.len(), 8);
                                let sender = packet.bytes[2] as u16;
                                assert_ne!(sender, peers[&packet.client], "voice echoed to sender");
                                assert!(uplink_voice.contains(&(sender, packet.bytes[3])));
                                assert_eq!(packet.bytes[4], 0);
                                assert_eq!(&packet.bytes[5..], SILENCE);
                                forwarded_voice.insert(sender);
                            } else {
                                assert_eq!(packet.bytes.len(), 7);
                                assert_eq!(packet.bytes[3], 0);
                                assert_eq!(&packet.bytes[4..], SILENCE);
                                uplink_voice.insert((peers[&packet.client], packet.bytes[2]));
                            }
                        }
                        PacketProperty::Unreliable
                            if packet.from_server
                                && packet.bytes[1] == channels::PLAYER_AVATAR_HIGH =>
                        {
                            // Small ID, interval byte, sequence byte, 159-byte high-quality pose.
                            assert_eq!(packet.bytes.len(), 2 + 3 + 159);
                            let sender = packet.bytes[2] as u16;
                            assert_ne!(sender, peers[&packet.client], "avatar echoed to sender");
                            // Ready state can arrive before a movement uplink. Require an actual
                            // movement's sequence and all 159 pose bytes to survive fanout.
                            if uplink_avatar.contains(&(
                                sender,
                                packet.bytes[4],
                                packet.bytes[5..].to_vec(),
                            )) {
                                forwarded_avatar.insert(sender);
                            }
                        }
                        _ => {}
                    }
                }
                assert_eq!(live.server.player_count(), 2);
                for peer in live.server.authenticated_peers.iter() {
                    assert_eq!(
                        live.server.voice_recipients.get(peer.key()).unwrap().len(),
                        1
                    );
                }
                // Let the real observer apply a later update before quitting; the wire capture itself
                // runs before proxy delivery, so it alone cannot establish client-side application.
                live.until(|p| {
                    p.from_server
                        && p.property() == PacketProperty::Unreliable
                        && p.bytes[1] == channels::PLAYER_AVATAR_HIGH
                })
                .await;
                live.finish().await;
                let csv = std::fs::read_to_string(live.dir.path().join("avatar.csv")).unwrap();
                let metric = |name: &str| -> u64 {
                    csv.lines()
                        .find_map(|line| line.strip_prefix(&format!("{name},")))
                        .unwrap()
                        .parse()
                        .unwrap()
                };
                assert!(
                    metric("applied_full_items") > 0,
                    "observer did not apply forwarded state: {csv}"
                );
                assert_eq!(metric("near_peers"), 1);
                assert_eq!(metric("decode_errors"), 0);
                assert_eq!(metric("malformed_items"), 0);
            })
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scene_script_payloads_survive_unreliable_and_reliable_relay() {
    for reliable in [false, true] {
        let mut args = vec![
            "--scene-data-bytes",
            "64",
            "--scene-data-interval-ms",
            "20",
            "--observe-scene-csv",
            "scene.csv",
        ];
        if reliable {
            args.push("--scene-data-reliable");
        }
        Live::start_with_args("default_password", 2, false, Fault::None, &args)
            .await
            .run(move |live| {
                Box::pin(async move {
                    let deadline = Instant::now() + LIMIT;
                    let mut uplinks = HashSet::new();
                    let mut senders = HashSet::new();
                    while senders.len() < 2 {
                        let packet = live.next(deadline).await;
                        let offset = if reliable && packet.channeled(channels::SCENE) {
                            4
                        } else if !reliable
                            && packet.property() == PacketProperty::Unreliable
                            && packet.bytes[1] == channels::SCENE
                        {
                            2
                        } else {
                            continue;
                        };
                        let body = &packet.bytes[offset..];
                        assert_eq!(body.len(), 68);
                        if packet.from_server {
                            assert_eq!(&body[2..4], &60000u16.to_le_bytes());
                            assert!(uplinks.contains(&body[4..]), "relay changed script bytes");
                            senders.insert(u16::from_le_bytes(body[..2].try_into().unwrap()));
                        } else {
                            assert_eq!(&body[..2], &60000u16.to_le_bytes());
                            assert_eq!(&body[2..4], &[0, 0], "expected broadcast recipient list");
                            uplinks.insert(body[4..].to_vec());
                        }
                    }
                    // The capture precedes socket delivery. Wait for further sends before shutdown.
                    for _ in 0..4 {
                        live.next(deadline).await;
                    }
                    live.finish().await;
                    let csv = std::fs::read_to_string(live.dir.path().join("scene.csv")).unwrap();
                    let metric = |name: &str| -> u64 {
                        csv.lines()
                            .find_map(|l| l.strip_prefix(&format!("{name},")))
                            .unwrap()
                            .parse()
                            .unwrap()
                    };
                    assert!(
                        metric("received_messages") > 0,
                        "client did not observe scene relay: {csv}"
                    );
                    assert_eq!(metric("observed_senders"), 1);
                    assert_eq!(metric("malformed_messages"), 0);
                    assert_eq!(metric("send_errors"), 0);
                })
            })
            .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn additional_avatar_data_survives_real_client_fanout() {
    Live::start_with_args(
        "default_password",
        2,
        true,
        Fault::None,
        &["--additional-avatar-bytes", "32"],
    )
    .await
    .run(|live| {
        Box::pin(async move {
            let deadline = Instant::now() + LIMIT;
            let mut uplinks = HashSet::new();
            let mut senders = HashSet::new();
            while senders.len() < 2 {
                let packet = live.next(deadline).await;
                if packet.property() != PacketProperty::Unreliable
                    || packet.bytes[1] != channels::PLAYER_AVATAR_HIGH_ADDITIONAL
                {
                    continue;
                }
                let (pose_start, seq_offset) = if packet.from_server { (5, 4) } else { (3, 2) };
                assert_eq!(packet.bytes.len(), pose_start + 159 + 4 + 32);
                let tail = &packet.bytes[pose_start + 159..];
                assert_eq!(&tail[..4], &[1, 0, 32, 0]);
                let update = (
                    packet.bytes[seq_offset],
                    packet.bytes[pose_start..].to_vec(),
                );
                if packet.from_server {
                    if uplinks.contains(&update) {
                        senders.insert(packet.bytes[2]);
                    }
                } else {
                    uplinks.insert(update);
                }
            }
            // Captures precede delivery; require later fanout before shutdown.
            live.until(|p| {
                p.from_server
                    && p.property() == PacketProperty::Unreliable
                    && p.bytes[1] == channels::PLAYER_AVATAR_HIGH_ADDITIONAL
            })
            .await;
            live.finish().await;
            let csv = std::fs::read_to_string(live.dir.path().join("avatar.csv")).unwrap();
            let applied: u64 = csv
                .lines()
                .find_map(|l| l.strip_prefix("applied_full_items,"))
                .unwrap()
                .parse()
                .unwrap();
            assert!(
                applied > 0,
                "observer did not apply additional avatar state: {csv}"
            );
            assert!(csv.contains("decode_errors,0"));
            assert!(csv.contains("malformed_items,0"));
        })
    })
    .await;
}
