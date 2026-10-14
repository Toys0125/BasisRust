//! A discrete-event week of real loopback sessions. Logical schedule time is
//! accelerated; production wall-clock timers are neither mocked nor aged a week.
mod array_sync_script;
mod lifecycle;
mod traffic;
mod transport;

use super::super::*;
use array_sync_script::ArraySyncScript;
use traffic::{avatar_frame, voice_payload, voice_recipients, RecipientMode};
use transport::WirePeer;

const SCRIPT: &str = "week-owner-array-100";
const RESOURCE: &str = "week-array-scene";
const POPULATION: [usize; 24] = [
    30, 25, 20, 20, 25, 30, 40, 50, 60, 70, 80, 80, 80, 85, 90, 95, 100, 100, 100, 95, 90, 80, 65,
    45,
];

#[derive(Default)]
struct Counts {
    joins: u64,
    leaves: u64,
    rejoins: u64,
    snapshots: u64,
    broadcasts: u64,
    avatar_checks: u64,
    avatar_packets_sent: u64,
    avatar_recovery_frames: u64,
    voice_packets: u64,
    voice_sent: u64,
    overload_voice_dropped: u64,
    concurrent_join_voice_frames: u64,
    muted_checks: u64,
    transfers: u64,
    peak: usize,
    join_sync_ms: Vec<f64>,
}

async fn wait_state(predicate: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server state did not converge");
}

async fn acquire(state: &ServerState, owner: &mut WirePeer) {
    let mut w = NetWriter::new();
    OwnershipTransferMessage {
        player_id: owner.id,
        ownership_id: SCRIPT.into(),
    }
    .serialize(&mut w)
    .unwrap();
    owner
        .send(channels::CHANGE_CURRENT_OWNER_REQUEST, w.as_slice(), true)
        .await;
    wait_state(|| {
        state
            .ownership
            .all()
            .iter()
            .any(|o| o.ownership_id == SCRIPT && o.player_id == owner.id)
    })
    .await;
}

async fn load_scene(owner: &mut WirePeer) {
    let resource = LocalLoadResource {
        mode: 0,
        loaded_net_id: RESOURCE.into(),
        unlock_password: String::new(),
        combined_url: "https://example.invalid/week-test-scene".into(),
        uuid_of_creator: owner.uuid.clone(),
        is_admin_locked: false,
        position_x: 0.0,
        position_y: 0.0,
        position_z: 0.0,
        quaternion_x: 0.0,
        quaternion_y: 0.0,
        quaternion_z: 0.0,
        quaternion_w: 1.0,
        scale_x: 1.0,
        scale_y: 1.0,
        scale_z: 1.0,
        persist: true,
        static_resource: false,
        static_admin_locked: false,
        modify_scale: false,
        load_strategy: 0,
    };
    let mut w = NetWriter::new();
    resource.serialize(&mut w).unwrap();
    owner
        .send(channels::LOAD_RESOURCE, w.as_slice(), true)
        .await;
}

async fn add_player(
    state: &ServerState,
    players: &mut [Option<WirePeer>],
    generation: &mut [u32],
    slot: usize,
    owner_slot: usize,
    script: &ArraySyncScript,
    counts: &mut Counts,
) {
    let started = std::time::Instant::now();
    generation[slot] += 1;
    let peer = WirePeer::connect(state, &format!("week-player-{slot}")).await;
    let id = peer.id;
    players[owner_slot]
        .as_ref()
        .unwrap()
        .wait_until(|v| v.roster.contains(&id))
        .await;
    let owner_id = players[owner_slot].as_ref().unwrap().id;
    script
        .on_player_joined(players[owner_slot].as_mut().unwrap(), id)
        .await;
    peer.wait_until(|v| {
        v.scenes
            .get(&script.index)
            .is_some_and(|s| s.sender == owner_id && s.payload == script.bytes)
            && v.ownership.get(SCRIPT) == Some(&owner_id)
            && v.net_ids.get(SCRIPT) == Some(&script.index)
            && v.resources.contains_key(RESOURCE)
    })
    .await;
    counts
        .join_sync_ms
        .push(started.elapsed().as_secs_f64() * 1000.0);
    counts.joins += 1;
    counts.snapshots += 1;
    if generation[slot] > 1 {
        counts.rejoins += 1;
    }
    players[slot] = Some(peer);
}

async fn remove_player(
    state: &ServerState,
    players: &mut [Option<WirePeer>],
    slot: usize,
    counts: &mut Counts,
) {
    let mut peer = players[slot].take().unwrap();
    let id = peer.id;
    let session = peer.session.clone();
    peer.disconnect().await;
    wait_state(|| {
        !state.transport.is_current_session(&session)
            && !state.authenticated_peers.contains_key(&id)
    })
    .await;
    assert!(!state.voice_recipients.contains_key(&id));
    assert!(!state.uplink_delta_states.contains_key(&id));
    assert!(!state.message_subscriptions.contains_key(&id));
    assert!(!state.avatar_sync.is_player_registered(id));
    assert!(!state.ownership.all().iter().any(|o| o.player_id == id));
    counts.leaves += 1;
}

async fn avatar_window(
    state: &ServerState,
    players: &mut [Option<WirePeer>],
    generation: &[u32],
    source_sequences: &mut [(u32, u8)],
    hour: usize,
    counts: &mut Counts,
) {
    let observer = 0;
    let active: Vec<_> = players
        .iter()
        .enumerate()
        .filter_map(|(i, p)| (i != observer && p.is_some()).then_some(i))
        .collect();
    let mut expected = Vec::new();
    for &slot in &active {
        if source_sequences[slot].0 != generation[slot] {
            source_sequences[slot] = (generation[slot], 0);
        }
        let peer = players[slot].as_ref().unwrap().id;
        expected.push((
            peer,
            avatar_frame(
                peer,
                generation[slot],
                (hour * 5) as u32,
                hour % 3 != 2,
                (hour % 3) as u8,
            ),
        ));
    }
    // Real avatars refresh over UDP. Use ten fixed advancing frames, never
    // outcome-dependent retries, so a lost first downlink can trigger NACK recovery.
    for round in 0..10 {
        let started = tokio::time::Instant::now();
        for offset in 0..active.len() {
            let index = (offset + round * 64) % active.len();
            let slot = active[index];
            source_sequences[slot].1 = source_sequences[slot].1.wrapping_add(1);
            let (channel, packet) = expected[index]
                .1
                .uplink_full(source_sequences[slot].1)
                .unwrap();
            players[slot]
                .as_mut()
                .unwrap()
                .send(channel, &packet, false)
                .await;
            counts.avatar_packets_sent += 1;
            if (offset + 1) % 64 == 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }
        tokio::time::sleep_until(started + Duration::from_millis(20)).await;
    }
    for (index, (peer, frame)) in expected.into_iter().enumerate() {
        let recovered = expect_avatar(
            state,
            players[observer].as_ref().unwrap(),
            players[active[index]].as_ref().unwrap(),
            &mut source_sequences[active[index]].1,
            peer,
            &frame,
        )
        .await;
        counts.avatar_packets_sent += recovered;
        counts.avatar_recovery_frames += recovered;
        counts.avatar_checks += 1;
    }
    let sender_slot = active[(hour * 53) % active.len()];
    let sender = players[sender_slot].as_ref().unwrap().id;
    let mut baseline = None;
    for step in 0..4 {
        let frame = avatar_frame(
            sender,
            generation[sender_slot],
            (hour * 5 + step + 1) as u32,
            step != 3,
            ((hour + step / 2) % 3) as u8,
        );
        for _ in 0..3 {
            source_sequences[sender_slot].1 = source_sequences[sender_slot].1.wrapping_add(1);
            let sequence = source_sequences[sender_slot].1;
            let (channel, packet) = if step % 2 == 0 {
                baseline = Some((sequence, frame.clone()));
                frame.uplink_full(sequence).unwrap()
            } else {
                let (base, reference) = baseline.as_ref().unwrap();
                frame.uplink_delta(sequence, *base, reference).unwrap()
            };
            players[sender_slot]
                .as_mut()
                .unwrap()
                .send(channel, &packet, false)
                .await;
            counts.avatar_packets_sent += 1;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let recovered = expect_avatar(
            state,
            players[observer].as_ref().unwrap(),
            players[sender_slot].as_ref().unwrap(),
            &mut source_sequences[sender_slot].1,
            sender,
            &frame,
        )
        .await;
        counts.avatar_packets_sent += recovered;
        counts.avatar_recovery_frames += recovered;
        if step % 2 == 0 {
            baseline = Some((source_sequences[sender_slot].1, frame.clone()));
        }
        counts.avatar_checks += 1;
    }
}

async fn expect_avatar(
    state: &ServerState,
    observer: &WirePeer,
    source: &WirePeer,
    source_sequence: &mut u8,
    sender: u16,
    frame: &traffic::AvatarFrame,
) -> u64 {
    let result = tokio::time::timeout(Duration::from_secs(15), async {
        let mut last_request = tokio::time::Instant::now();
        let mut recovery_frames = 0;
        loop {
            if observer
                .view
                .lock()
                .avatars
                .avatars
                .get(&sender)
                .is_some_and(|s| frame.assert_matches(s).is_ok())
            {
                return recovery_frames;
            }
            if last_request.elapsed() >= Duration::from_millis(100) {
                let [low, high] = sender.to_le_bytes();
                observer
                    .unreliable_socket()
                    .send(&[
                        basis_transport::PacketProperty::Unreliable as u8,
                        channels::DELTA_AVATAR,
                        channels::DELTA_CONTROL_KEYFRAME_REQUEST,
                        low,
                        high,
                    ])
                    .await
                    .unwrap();
                observer.view.lock().convergence_keyframe_requests += 1;
                // The real receiver requires advancing full frames after silence.
                // Keep this source alive while recovering a sparse logical sample.
                *source_sequence = source_sequence.wrapping_add(1);
                let (channel, payload) = frame.uplink_full(*source_sequence).unwrap();
                let mut wire = vec![basis_transport::PacketProperty::Unreliable as u8, channel];
                wire.extend_from_slice(&payload);
                source.unreliable_socket().send(&wire).await.unwrap();
                recovery_frames += 1;
                last_request = tokio::time::Instant::now();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    if result.is_err() {
        let source_meta = {
            let view = source.view.lock();
            (view.pings, view.disconnected)
        };
        let v = observer.view.lock();
        let actual = v.avatars.avatars.get(&sender);
        eprintln!(
            "avatar expected_prefix={:?} server_position={:?} stats={:?} registered={} authenticated={} source_current={} source_pings={} source_disconnected={}",
            &frame.body[..9],
            state.avatar_sync.player_position(sender),
            state.statistics.snapshot(),
            state.avatar_sync.is_player_registered(sender),
            state.authenticated_peers.contains_key(&sender),
            state.transport.is_current_session(&source.session),
            source_meta.0,
            source_meta.1,
        );
        panic!(
            "avatar convergence failed sender={sender}, mismatch={:?}, actual={:?}, full={} delta={} unapplied={} stale={} errors={:?}",
            actual.map(|s| frame.assert_matches(s).map_err(|e| e.to_string())),
            actual.map(|s| (
                s.sequence,
                s.quality,
                s.interval,
                &s.payload[..9],
                s.additional.len(),
                s.linked_avatar_index
            )),
            v.avatars.full_items,
            v.avatars.delta_items,
            v.avatars.unapplied_deltas,
            v.avatars.stale_items,
            v.errors
        );
    }
    result.unwrap()
}

async fn voice_window(
    state: &ServerState,
    players: &mut [Option<WirePeer>],
    hour: usize,
    opus: &[Vec<u8>],
    counts: &mut Counts,
) {
    let active: Vec<_> = players
        .iter()
        .enumerate()
        .filter_map(|(i, p)| (i > 1 && p.is_some()).then_some(i))
        .collect();
    // Rotate speakers, including >255 IDs at peak. Recipient changes are wire messages.
    let slot = active[(hour * 53) % active.len()];
    let sender = players[slot].as_ref().unwrap().id;
    let recipient = players[0].as_ref().unwrap().id;
    let mode = match hour % 5 {
        0 => RecipientMode::Small,
        1 => RecipientMode::Large,
        2 => RecipientMode::Bitfield,
        3 if active.len() < 255 => RecipientMode::InvertedSmall,
        _ => RecipientMode::InvertedLarge,
    };
    let recipients = if matches!(
        mode,
        RecipientMode::InvertedSmall | RecipientMode::InvertedLarge
    ) {
        players
            .iter()
            .flatten()
            .filter_map(|p| (p.id != recipient).then_some(p.id))
            .collect::<Vec<_>>()
    } else {
        vec![recipient]
    };
    let (channel, payload) = voice_recipients(&recipients, mode).unwrap();
    players[slot]
        .as_mut()
        .unwrap()
        .send(channel, &payload, true)
        .await;
    wait_state(|| {
        state
            .voice_recipients
            .get(&sender)
            .is_some_and(|r| r.as_slice() == [recipient])
    })
    .await;
    let observer = players[0].as_ref().unwrap();
    let before = *observer.view.lock().voice_counts.get(&sender).unwrap_or(&0);
    observer.view.lock().voice_packets.clear();
    let mut expected = HashSet::new();
    // Three-frame scheduling bursts fit the production inbox. A separate
    // 32-frame overload exceeds that bound: measure losses and require recovery.
    let overload = hour % 4 == 3;
    let paced = hour.is_multiple_of(4);
    let frames = if paced {
        8
    } else if overload {
        32
    } else {
        3
    };
    for frame in 0..frames {
        let packet = voice_payload(
            (hour * 37 + frame) as u8,
            &opus[(hour + frame) % opus.len()],
        );
        expected.insert((sender, packet.clone()));
        players[slot]
            .as_mut()
            .unwrap()
            .send(channels::VOICE, &packet, false)
            .await;
        if paced {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    if overload {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let recovery = voice_payload((hour * 37 + frames) as u8, &opus[0]);
        expected.insert((sender, recovery.clone()));
        players[slot]
            .as_mut()
            .unwrap()
            .send(channels::VOICE, &recovery, false)
            .await;
        players[0]
            .as_ref()
            .unwrap()
            .wait_until(|v| v.voice_latest.get(&sender) == Some(&recovery))
            .await;
    } else {
        players[0]
            .as_ref()
            .unwrap()
            .wait_until(|v| {
                v.voice_counts.get(&sender).copied().unwrap_or(0) == before + frames as u64
            })
            .await;
    }
    let received: HashSet<_> = players[0]
        .as_ref()
        .unwrap()
        .view
        .lock()
        .voice_packets
        .iter()
        .cloned()
        .collect();
    if overload {
        assert!(
            received.is_subset(&expected),
            "overload delivered a corrupt or unexpected packet"
        );
        counts.overload_voice_dropped += (expected.len() - received.len()) as u64;
    } else {
        assert_eq!(
            received, expected,
            "every normal/jitter-burst Opus packet must remain byte-exact"
        );
    }
    assert_eq!(
        players[0]
            .as_ref()
            .unwrap()
            .view
            .lock()
            .voice_counts
            .get(&sender)
            .copied()
            .unwrap_or(0)
            - before,
        received.len() as u64,
        "duplicate voice frames"
    );
    counts.voice_sent += expected.len() as u64;
    counts.voice_packets += received.len() as u64;
    let (channel, payload) = voice_recipients(&[], RecipientMode::Large).unwrap();
    players[slot]
        .as_mut()
        .unwrap()
        .send(channel, &payload, true)
        .await;
    wait_state(|| {
        state
            .voice_recipients
            .get(&sender)
            .is_some_and(|r| r.is_empty())
    })
    .await;
    let before = players[0]
        .as_ref()
        .unwrap()
        .view
        .lock()
        .voice_counts
        .clone();
    for frame in 0..4 {
        players[slot]
            .as_mut()
            .unwrap()
            .send(channels::VOICE, &voice_payload(frame, &opus[0]), false)
            .await;
    }
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert_eq!(
        players[0].as_ref().unwrap().view.lock().voice_counts,
        before,
        "disabled voice must not route"
    );
    counts.muted_checks += 1;
}

async fn run_week(peak: usize, hours: usize, output: Option<std::path::PathBuf>) {
    assert!((4..=1000).contains(&peak));
    assert!(hours > 0 && hours <= 168);
    let dir = std::env::temp_dir().join(format!("basis-week-{}", uuid::Uuid::new_v4()));
    let config = ServerConfig {
        peer_limit: peak as i32 + 8,
        has_file_support: false,
        use_auth: false,
        use_auth_identity: false,
        override_auto_discovery_of_ipv: true,
        ipv4_address: "127.0.0.1".into(),
        set_port: 0,
        enable_compute_offload: false,
        strip_additional_data_at_low_quality: false,
        enable_statistics: true,
        health_include_extended_metrics: true,
        ..ServerConfig::default()
    };
    let (state, shutdown) = ServerState::start(config, &dir).await.unwrap();
    let started = std::time::Instant::now();
    let (opus, audio_files) = if let Some(path) = std::env::var_os("BASIS_WEEK_OPUS_FILE") {
        let path = std::path::PathBuf::from(path);
        let mut files = if path.is_dir() {
            std::fs::read_dir(&path)
                .unwrap()
                .map(|e| e.unwrap().path())
                .filter(|p| p.extension().is_some_and(|e| e == "opus" || e == "ogg"))
                .collect::<Vec<_>>()
        } else {
            vec![path]
        };
        files.sort();
        assert!(!files.is_empty(), "real Opus corpus must not be empty");
        let clips: Vec<_> = files
            .iter()
            .map(|p| traffic::load_ogg_opus(p).unwrap())
            .collect();
        let mut packets = Vec::new();
        for index in 0..clips.iter().map(Vec::len).max().unwrap() {
            for clip in &clips {
                if let Some(packet) = clip.get(index) {
                    packets.push(packet.clone());
                }
            }
        }
        (
            packets,
            files
                .iter()
                .map(|p| p.file_name().unwrap().to_string_lossy().to_string())
                .collect::<Vec<_>>(),
        )
    } else {
        (vec![vec![0xf8, 0xff, 0xfe]], Vec::new())
    };
    let mut players: Vec<Option<WirePeer>> = (0..peak).map(|_| None).collect();
    let mut generation = vec![0; peak];
    let mut source_sequences = vec![(0, 0); peak];
    let mut counts = Counts::default();
    for i in 0..4 {
        players[i] = Some(WirePeer::connect(&state, &format!("week-player-{i}")).await);
        generation[i] = 1;
        counts.joins += 1;
    }
    let mut owner_slot = 1;
    acquire(&state, players[owner_slot].as_mut().unwrap()).await;
    load_scene(players[owner_slot].as_mut().unwrap()).await;
    let mut w = NetWriter::new();
    NetIdMessage {
        player_id: SCRIPT.into(),
    }
    .serialize(&mut w)
    .unwrap();
    players[owner_slot]
        .as_mut()
        .unwrap()
        .send(channels::NET_ID_ASSIGN, w.as_slice(), true)
        .await;
    wait_state(|| {
        state.net_ids.find(SCRIPT).is_some()
            && state
                .resources
                .all_resources()
                .iter()
                .any(|r| r.loaded_net_id == RESOURCE)
    })
    .await;
    let mut script = ArraySyncScript {
        index: state.net_ids.find(SCRIPT).unwrap(),
        bytes: [0; 100],
    };
    for peer in players.iter().flatten() {
        peer.wait_until(|v| v.roster.len() == 3).await;
    }
    let mut hourly = Vec::new();
    for hour in 0..hours {
        let hour_started = std::time::Instant::now();
        let before_hour = (
            counts.joins,
            counts.leaves,
            counts.rejoins,
            counts.avatar_checks,
            counts.voice_sent,
            counts.voice_packets,
            counts.overload_voice_dropped,
        );
        script.update(hour);
        // Speech overlaps actual admission, owner handoff and script snapshots.
        let voice_sender = players[3].as_ref().unwrap().id;
        let observer_id = players[0].as_ref().unwrap().id;
        let (channel, payload) = voice_recipients(&[observer_id], RecipientMode::Large).unwrap();
        players[3]
            .as_mut()
            .unwrap()
            .send(channel, &payload, true)
            .await;
        wait_state(|| {
            state
                .voice_recipients
                .get(&voice_sender)
                .is_some_and(|r| r.as_slice() == [observer_id])
        })
        .await;
        let voice_before = players[0]
            .as_ref()
            .unwrap()
            .view
            .lock()
            .voice_counts
            .get(&voice_sender)
            .copied()
            .unwrap_or(0);
        players[0]
            .as_ref()
            .unwrap()
            .view
            .lock()
            .voice_packets
            .clear();
        let concurrent_packets: Vec<_> = (0..30)
            .map(|frame| {
                voice_payload(
                    (hour * 73 + frame) as u8,
                    &opus[(hour + frame) % opus.len()],
                )
            })
            .collect();
        let socket = players[3].as_ref().unwrap().unreliable_socket();
        let send_packets = concurrent_packets.clone();
        let (ready, started_voice) = tokio::sync::oneshot::channel();
        let voice_job = tokio::spawn(async move {
            let mut ready = Some(ready);
            for packet in send_packets {
                let mut wire = vec![
                    basis_transport::PacketProperty::Unreliable as u8,
                    channels::VOICE,
                ];
                wire.extend_from_slice(&packet);
                socket.send(&wire).await.unwrap();
                if let Some(ready) = ready.take() {
                    let _ = ready.send(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        });
        started_voice.await.unwrap();
        // Owner hands off daily, then actually leaves and returns with the same UUID.
        if hour > 0 && hour % 24 == 0 {
            let next = if owner_slot == 1 { 2 } else { 1 };
            let mut w = NetWriter::new();
            OwnershipTransferMessage {
                player_id: players[next].as_ref().unwrap().id,
                ownership_id: SCRIPT.into(),
            }
            .serialize(&mut w)
            .unwrap();
            players[next]
                .as_mut()
                .unwrap()
                .send(channels::CHANGE_CURRENT_OWNER_REQUEST, w.as_slice(), true)
                .await;
            acquire(&state, players[next].as_mut().unwrap()).await;
            let old = owner_slot;
            owner_slot = next;
            counts.transfers += 1;
            remove_player(&state, &mut players, old, &mut counts).await;
            add_player(
                &state,
                &mut players,
                &mut generation,
                old,
                owner_slot,
                &script,
                &mut counts,
            )
            .await;
        }
        let target = (peak * POPULATION[hour % 24] / 100).max(4);
        let active = players.iter().filter(|p| p.is_some()).count();
        if active > target {
            for i in (4..peak)
                .rev()
                .filter(|&i| players[i].is_some())
                .take(active - target)
                .collect::<Vec<_>>()
            {
                remove_player(&state, &mut players, i, &mut counts).await;
            }
        }
        if players.iter().filter(|p| p.is_some()).count() < target {
            let add = target - players.iter().filter(|p| p.is_some()).count();
            for i in (4..peak)
                .filter(|&i| players[i].is_none())
                .take(add)
                .collect::<Vec<_>>()
            {
                add_player(
                    &state,
                    &mut players,
                    &mut generation,
                    i,
                    owner_slot,
                    &script,
                    &mut counts,
                )
                .await;
            }
        }
        // Repeated short dropouts even during stable population, with ID reuse.
        if peak > 4 {
            let candidates: Vec<_> = (4..peak).filter(|&i| players[i].is_some()).collect();
            if !candidates.is_empty() {
                let i = candidates[hour % candidates.len()];
                remove_player(&state, &mut players, i, &mut counts).await;
                add_player(
                    &state,
                    &mut players,
                    &mut generation,
                    i,
                    owner_slot,
                    &script,
                    &mut counts,
                )
                .await;
            }
        }
        let owner_id = players[owner_slot].as_ref().unwrap().id;
        script
            .broadcast(players[owner_slot].as_mut().unwrap())
            .await;
        counts.broadcasts += 1;
        for peer in players.iter().flatten().filter(|p| p.id != owner_id) {
            peer.wait_until(|v| {
                v.scenes
                    .get(&script.index)
                    .is_some_and(|s| s.sender == owner_id && s.payload == script.bytes)
            })
            .await;
        }
        let ids: HashSet<_> = players.iter().flatten().map(|p| p.id).collect();
        for peer in players.iter().flatten() {
            peer.wait_until(|v| {
                v.roster.len() == ids.len() - 1 && v.roster.iter().all(|id| ids.contains(id))
            })
            .await;
        }
        assert_eq!(state.authenticated_peers.len(), ids.len());
        counts.peak = counts.peak.max(ids.len());
        voice_job.await.unwrap();
        players[0]
            .as_ref()
            .unwrap()
            .wait_until(|v| {
                v.voice_counts.get(&voice_sender).copied().unwrap_or(0) == voice_before + 30
            })
            .await;
        let expected: HashSet<_> = concurrent_packets
            .into_iter()
            .map(|p| (voice_sender, p))
            .collect();
        let actual: HashSet<_> = players[0]
            .as_ref()
            .unwrap()
            .view
            .lock()
            .voice_packets
            .iter()
            .cloned()
            .collect();
        assert_eq!(
            actual, expected,
            "voice during joins/scene sync must stay byte-exact"
        );
        counts.concurrent_join_voice_frames += 30;
        let (channel, payload) = voice_recipients(&[], RecipientMode::Large).unwrap();
        players[3]
            .as_mut()
            .unwrap()
            .send(channel, &payload, true)
            .await;
        wait_state(|| {
            state
                .voice_recipients
                .get(&voice_sender)
                .is_some_and(|r| r.is_empty())
        })
        .await;
        avatar_window(
            &state,
            &mut players,
            &generation,
            &mut source_sequences,
            hour,
            &mut counts,
        )
        .await;
        voice_window(&state, &mut players, hour, &opus, &mut counts).await;
        for peer in players.iter().flatten() {
            let v = peer.view.lock();
            assert_eq!(v.error_count, 0, "{}: {:?}", peer.uuid, v.errors);
        }
        hourly.push(serde_json::json!({"hour":hour,"active":ids.len(),"joins":counts.joins-before_hour.0,"leaves":counts.leaves-before_hour.1,"rejoins":counts.rejoins-before_hour.2,"avatar_checks":counts.avatar_checks-before_hour.3,"voice_sent":counts.voice_sent-before_hour.4,"voice_checked":counts.voice_packets-before_hour.5,"overload_voice_dropped":counts.overload_voice_dropped-before_hour.6,"real_seconds":hour_started.elapsed().as_secs_f64()}));
        if hour % 6 == 5 || hour + 1 == hours {
            eprintln!(
                "week-sim hour={} active={} joins={} rejoins={} scene_snapshots={} avatar_checks={} voice_packets={} real_seconds={:.1}",
                hour + 1,
                ids.len(),
                counts.joins,
                counts.rejoins,
                counts.snapshots,
                counts.avatar_checks,
                counts.voice_packets,
                started.elapsed().as_secs_f64()
            );
        }
    }
    let statistics = state.statistics.snapshot();
    let protocol_errors = statistics.protocol_errors;
    assert_eq!(protocol_errors, 0);
    let recovery_requests = players[0]
        .as_ref()
        .unwrap()
        .view
        .lock()
        .convergence_keyframe_requests;
    let automatic_requests = players[0].as_ref().unwrap().view.lock().keyframe_requests;
    for i in 0..peak {
        if players[i].is_some() {
            remove_player(&state, &mut players, i, &mut counts).await;
        }
    }
    wait_state(|| state.authenticated_peers.is_empty()).await;
    wait_state(|| state.retiring_peer_ids.is_empty()).await;
    assert!(state.ownership.all().is_empty());
    assert!(state.voice_recipients.is_empty());
    assert!(state.uplink_delta_states.is_empty());
    assert!(state.voice_recipient_sessions.is_empty());
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
    if hours >= 19 {
        assert_eq!(counts.peak, peak, "requested peak must actually be reached");
    }
    counts.join_sync_ms.sort_by(f64::total_cmp);
    let join_percentile =
        |p: f64| counts.join_sync_ms[((counts.join_sync_ms.len() - 1) as f64 * p).ceil() as usize];
    let join_latency = if counts.join_sync_ms.is_empty() {
        serde_json::json!({"samples":0})
    } else {
        serde_json::json!({"samples":counts.join_sync_ms.len(),"p50":join_percentile(0.5),"p95":join_percentile(0.95),"max":join_percentile(1.0),"scope":"wire connect until owner-script snapshot plus ownership/net-ID/resource replay verified"})
    };
    let result = serde_json::json!({"valid":true,"hourly":hourly,"join_sync_ms":join_latency,"logical_seconds":hours*3600,"logical_hours":hours,"requested_peak":peak,"observed_peak":counts.peak,"joins":counts.joins,"leaves":counts.leaves,"rejoins":counts.rejoins,"owner_transfers":counts.transfers,"scene_bytes":100,"join_snapshots":counts.snapshots,"scene_broadcasts":counts.broadcasts,"avatar_checks":counts.avatar_checks,"avatar_packets_sent":counts.avatar_packets_sent,"avatar_recovery_frames":counts.avatar_recovery_frames,"avatar_convergence_keyframe_requests":recovery_requests,"avatar_automatic_keyframe_requests":automatic_requests,"voice_packets_sent":counts.voice_sent,"voice_packets_checked":counts.voice_packets,"overload_voice_dropped":counts.overload_voice_dropped,"concurrent_join_voice_frames":counts.concurrent_join_voice_frames,"voice_off_windows":counts.muted_checks,"protocol_errors":protocol_errors,"avatar_received":statistics.avatar_received,"avatar_processed":statistics.avatar_processed,"avatar_coalesced":statistics.avatar_coalesced,"avatar_rejected":statistics.avatar_rejected,"voice_payload_rejected":statistics.voice_rejected,"audio_files":audio_files,"audio_packets_loaded":opus.len(),"real_seconds":started.elapsed().as_secs_f64(),"clock":"hourly discrete-event logical schedule; production timers remain wall-clock","unity_script_execution":false,"opus_playback_decode":false});
    if let Some(path) = output {
        std::fs::write(path, serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    }
    let mut summary = result.clone();
    summary.as_object_mut().unwrap().remove("hourly");
    eprintln!("week-sim-summary {summary}");
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn week_sync_smoke() {
    run_week(12, 26, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 16)]
#[ignore = "manual 1000-player accelerated seven-day lifecycle integration"]
async fn week_sync_1000_players() {
    assert!(
        std::env::var_os("BASIS_WEEK_OPUS_FILE").is_some(),
        "full run requires a real preencoded Ogg Opus corpus"
    );
    let peak = std::env::var("BASIS_WEEK_PEAK_PLAYERS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(1000);
    let hours = std::env::var("BASIS_WEEK_HOURS")
        .map(|v| v.parse().unwrap())
        .unwrap_or(168);
    run_week(
        peak,
        hours,
        std::env::var_os("BASIS_WEEK_OUTPUT").map(std::path::PathBuf::from),
    )
    .await;
}
