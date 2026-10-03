use super::*;

#[test]
fn strict_config_reports_file_and_invalid_present_field() {
    let path = Path::new("test-config.xml");
    for (field, value) in [
        ("Port", "65536"),
        ("Port", ""),
        ("ClientCount", "-1"),
        ("AvatarLoadMode", "256"),
        ("VoiceEnabled", "yes"),
        ("VoiceSpeakerPercent", "101"),
        ("VoiceHearingDistance", "NaN"),
        ("VoiceHearingDistance", "inf"),
        ("VoiceHearingDistance", "-1"),
        ("VoiceFrameDurationMs", "0"),
    ] {
        let xml = format!("<Configuration><{field}>{value}</{field}></Configuration>");
        let error = Config::from_xml(&xml, path, true).unwrap_err().to_string();
        assert!(
            error.contains("test-config.xml") && error.contains(field),
            "{error}"
        );
        assert!(
            Config::from_xml(&xml, path, false).is_ok(),
            "legacy {field}"
        );
    }
}

#[test]
fn strict_config_rejects_malformed_documents_and_keeps_absent_defaults() {
    let path = Path::new("broken.xml");
    for xml in [
        "",
        "<Configuration>",
        "<Configuration><Port>5</Configuration>",
        "<Other/>",
        "<Configuration/><Configuration/>",
        "<Configuration/>trailing",
        "<Configuration invalid=unquoted/>",
        "<Configuration><Password>\0</Password></Configuration>",
        "<Configuration><Password>&#1;</Password></Configuration>",
        "<Configuration bad='&#1;'/>",
        "<Configuration><Unknown>&undefined;</Unknown></Configuration>",
    ] {
        assert!(Config::from_xml(xml, path, true)
            .unwrap_err()
            .to_string()
            .contains("broken.xml"));
        assert!(Config::from_xml(xml, path, false).is_ok());
    }
    let config = Config::from_xml("<Configuration/>", path, true).unwrap();
    assert_eq!(config.password, "default_password");
    assert_eq!(config.port, 4296);
    // Preserve the accepted byte domain and zero count/port; don't invent wire enum defaults.
    let config = Config::from_xml("<Configuration><Port>0</Port><ClientCount>0</ClientCount><AvatarLoadMode>255</AvatarLoadMode></Configuration>", path, true).unwrap();
    assert_eq!(
        (config.port, config.client_count, config.avatar_load_mode),
        (0, 0, 255)
    );
}

#[test]
fn strict_config_creates_same_flat_default_file() {
    let path = std::env::temp_dir().join(format!("basis-strict-{}.xml", Uuid::new_v4()));
    let config = Config::load_or_create(&path, true).unwrap();
    let xml = std::fs::read_to_string(&path).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(xml, config.to_pretty_xml());
    assert!(xml.contains("<Configuration>\n  <Password>default_password</Password>"));
    assert!(
        Args::try_parse_from(["client", "--strict-config"])
            .unwrap()
            .strict_config
    );
}

#[tokio::test]
async fn malformed_packet_diagnostics_preserve_prefix_ack_and_discard_suffix() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = tests::test_client(0, server.local_addr().unwrap()).await;
    for packet in [
        &[][..],
        &[31],
        &[PacketProperty::Ack as u8, 0, 0],
        &[PacketProperty::Ping as u8, 0],
    ] {
        client.handle_packet(packet).await.unwrap();
    }
    client
        .handle_packet(&[PacketProperty::Ack as u8, 0, 0, 0])
        .await
        .unwrap();
    // A valid reliable prefix must still be recorded/ACKed before a bad merged suffix.
    let child = [
        PacketProperty::Channeled as u8,
        0,
        0,
        DeliveryMethod::channel_id(channels::SERVER_LIBRARY, DeliveryMethod::ReliableOrdered),
    ];
    let mut merged = vec![PacketProperty::Merged as u8, child.len() as u8, 0];
    merged.extend_from_slice(&child);
    merged.push(1);
    client.handle_packet(&merged).await.unwrap();
    for merged in [
        vec![PacketProperty::Merged as u8, 0, 0],
        vec![PacketProperty::Merged as u8, 5, 0, 1],
        vec![PacketProperty::CompactMerged as u8, 0x40, 1, 0],
    ] {
        client.handle_packet(&merged).await.unwrap();
    }
    assert_eq!(client.packet_diagnostics.snapshot(), [1, 1, 2, 1, 4]);
    client.flush_acks().await.unwrap();
    let mut bytes = [0; 128];
    let len = time::timeout(Duration::from_secs(1), server.recv(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    let ack = parse_packet(&bytes[..len]).unwrap();
    assert_eq!(ack.property, PacketProperty::Ack);
    assert_eq!(ack.channel_id, Some(child[3]));
    assert_eq!(ack.payload[0] & 1, 1);
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn shared_receive_drop_diagnostics_preserve_reliable_prefix() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = tests::test_client(1, server.local_addr().unwrap()).await;
    shared_receiver_mark_reliable(&client, &[PacketProperty::Channeled as u8]);
    let child = [PacketProperty::Channeled as u8, 0, 0, 0];
    let mut merged = vec![PacketProperty::Merged as u8, 4, 0];
    merged.extend_from_slice(&child);
    merged.push(0);
    shared_receiver_process_merged(&client, client.socket.as_raw_fd(), &merged);
    assert_eq!(client.packet_diagnostics.snapshot(), [0, 0, 1, 0, 1]);
    assert!(client.ack_pending.load(Ordering::Relaxed));
    assert!(client
        .reliable_receive_state()
        .unwrap()
        .ack_window(0)
        .is_some());
}

fn poison<T>(mutex: &StdMutex<T>) {
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _guard = mutex.lock().unwrap();
        panic!("inject partial state failure");
    }));
    assert!(mutex.is_poisoned());
}

#[tokio::test]
async fn poisoned_reliable_state_deactivates_without_ack_or_repeat_panic() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = tests::test_client(1, server.local_addr().unwrap()).await;
    poison(&client.received_reliable);
    client.handle_channeled(0, 0, &[]).await.unwrap();
    client.flush_acks().await.unwrap();
    assert!(client.reliable_receive_state().is_none());
    assert!(!client.in_use.load(Ordering::Relaxed));
    assert!(!client.connected.load(Ordering::Relaxed));
    assert!(!client.ack_pending.load(Ordering::Relaxed));
    let mut bytes = [0; 128];
    assert!(
        time::timeout(Duration::from_millis(20), server.recv(&mut bytes))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn poisoned_metadata_is_discarded_and_can_be_refreshed() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let client = tests::test_client(1, server.local_addr().unwrap()).await;
    client
        .shared_receive_eligible
        .store(true, Ordering::Relaxed);
    poison(&client.server_avatar_metadata);
    assert!(client.metadata_state().is_none());
    assert!(!client.server_avatar_metadata.is_poisoned());
    assert!(!client.shared_receive_eligible.load(Ordering::Relaxed));
    assert!(client.force_avatar_keyframe.load(Ordering::Relaxed));
    *client.metadata_state() = Some(ServerAvatarMetadata {
        sync_interval_ms: 20,
        base_multiplier: 1.0,
        increase_rate: 1.0,
        slowest_send_rate_secs: 5.0,
        uplink_delta_enabled: true,
    });
    client.refresh_shared_receive_eligibility();
    assert!(client.metadata_state().is_some());
    assert!(client.shared_receive_eligible.load(Ordering::Acquire));
}

#[test]
fn poisoned_observer_disables_measurement_instead_of_exporting_corrupt_csv() {
    let session = ObserverSession::new(AvatarObserver::new(40.0, 1, None, Duration::from_secs(60)));
    // Panic while a collection guard is held, as a failed observer task could do.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _state = session.lock().unwrap();
        panic!("observer failure");
    }));
    assert!(session.lock().is_none());
    assert!(!session.could_own(0));
    assert!(session.lock().is_none());
}

#[tokio::test]
async fn observer_failover_survives_reconnect_without_mixing_cadence_gaps() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let session = Arc::new(ObserverSession::new(AvatarObserver::new(
        40.0,
        1,
        None,
        Duration::from_secs(60),
    )));
    let mut first = tests::test_client(0, server.local_addr().unwrap()).await;
    let mut second = tests::test_client(1, server.local_addr().unwrap()).await;
    Arc::get_mut(&mut first).unwrap().avatar_observer = Some(session.clone());
    Arc::get_mut(&mut second).unwrap().avatar_observer = Some(session.clone());
    let baseline = vec![0; ProtocolBitQuality::High.payload_len()];
    let (channel, full) = tests::observer_full_frame(7, 1, &baseline);
    first.observe_avatar_channel(channel, &full).await;
    second.observe_avatar_channel(channel, &full).await;
    assert_eq!(session.lock().unwrap().observer.accepted_avatar_items, 1);
    first.deactivate();
    // A fresh owner may establish the same byte baseline without dropping it as a duplicate.
    second.observe_avatar_channel(channel, &full).await;
    first.observe_avatar_channel(channel, &full).await;
    let mut replacement = tests::test_client(1, server.local_addr().unwrap()).await;
    Arc::get_mut(&mut replacement).unwrap().connect_time = 1;
    Arc::get_mut(&mut replacement).unwrap().avatar_observer = Some(session.clone());
    replacement.observe_avatar_channel(channel, &full).await;
    assert_eq!(session.lock().unwrap().observer.accepted_avatar_items, 2);
    second.deactivate();
    replacement.observe_avatar_channel(channel, &full).await;
    // A delayed deactivation from the old generation cannot release the replacement's lease.
    second.deactivate();
    let state = session.lock().unwrap();
    assert_eq!(state.owner, Some((1, 1)));
    assert_eq!(state.observer.accepted_avatar_items, 3);
    assert_eq!(state.observer.discontinuities, 2);
    assert!(state.observer.peers[&7].near_gaps_micros.is_empty());
    assert!(state
        .observer
        .summary_and_csv(std::time::Instant::now())
        .1
        .contains("discontinuities,2"));
}

#[test]
fn observer_ambiguous_half_range_and_complete_wrap_require_full_resync() {
    for next in [10u8, 11, 138, 150] {
        let mut observer = AvatarObserver::new(40.0, 1, None, Duration::from_secs(60));
        let start = std::time::Instant::now();
        let baseline = vec![0; ProtocolBitQuality::High.payload_len()];
        let (channel, full) = tests::observer_full_frame(7, 10, &baseline);
        observer.observe_channel(channel, &full, [0.0; 3], start);
        let (_, ambiguous) = tests::observer_full_frame(7, 138, &baseline);
        observer.observe_channel(
            channel,
            &ambiguous,
            [0.0; 3],
            start + Duration::from_millis(20),
        );
        assert_eq!(observer.sequence_ambiguities, 1);
        assert_eq!(observer.peers[&7].last_sequence, Some(10));
        let silent = start + Duration::from_secs(3);
        let (_, candidate) = tests::observer_full_frame(7, next, &baseline);
        observer.observe_channel(channel, &candidate, [0.0; 3], silent);
        assert_eq!(
            observer.applied_full_items, 1,
            "one stale/ambiguous full is not enough"
        );
        assert!(observer.peers[&7].baselines.iter().all(Option::is_none));
        let body = build_delta(&baseline, &baseline, ProtocolBitQuality::High).unwrap();
        observer.observe_channel(
            channels::DELTA_AVATAR,
            &tests::server_fanout_delta(7, next.wrapping_add(1), next, &body),
            [0.0; 3],
            silent + Duration::from_millis(20),
        );
        assert_eq!(observer.applied_delta_items, 0);
        let (_, confirmed) = tests::observer_full_frame(7, next.wrapping_add(1), &baseline);
        observer.observe_channel(
            channel,
            &confirmed,
            [0.0; 3],
            silent + Duration::from_millis(40),
        );
        assert_eq!(observer.sequence_resyncs, 1);
        assert_eq!(observer.discontinuities, 1);
        assert_eq!(observer.peers[&7].last_sequence, Some(next.wrapping_add(1)));
        assert!(observer.peers[&7].near_gaps_micros.is_empty());
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn nonzero_observer_candidate_receives_unreliable_after_connect_accept() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut client = tests::test_client(1, server.local_addr().unwrap()).await;
    Arc::get_mut(&mut client).unwrap().avatar_observer = Some(Arc::new(ObserverSession::new(
        AvatarObserver::new(40.0, 1, None, Duration::from_secs(60)),
    )));
    let mut accept = [0u8; 15];
    accept[0] = PacketProperty::ConnectAccept as u8;
    client.handle_packet(&accept).await.unwrap();
    let baseline = vec![0; ProtocolBitQuality::High.payload_len()];
    let (channel, full) = tests::observer_full_frame(7, 1, &baseline);
    let mut packet = vec![PacketProperty::Unreliable as u8, channel];
    packet.extend_from_slice(&full);
    server
        .send_to(&packet, client.socket.local_addr().unwrap())
        .await
        .unwrap();
    let mut bytes = [0; 1024];
    let len = time::timeout(Duration::from_secs(1), client.socket.recv(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    client.handle_packet(&bytes[..len]).await.unwrap();
    assert_eq!(
        client
            .avatar_observer
            .as_ref()
            .unwrap()
            .lock()
            .unwrap()
            .observer
            .accepted_avatar_items,
        1
    );
}

#[test]
fn observer_resync_accepts_idle_keyframe_confirmation_but_not_a_duplicate() {
    let mut sequence = ObserverSequence::default();
    let start = std::time::Instant::now();
    sequence.applied(start);
    assert_eq!(
        sequence.consider(5, Some(5), true, start + Duration::from_secs(5)),
        SequenceDecision::Pending { began: true }
    );
    assert_eq!(
        sequence.consider(5, Some(5), true, start + Duration::from_secs(6)),
        SequenceDecision::Pending { began: false }
    );
    assert_eq!(
        sequence.consider(6, Some(5), true, start + Duration::from_secs(10)),
        SequenceDecision::Resynced
    );
}

#[test]
fn observer_sequence_metrics_are_scoped_to_measurement_window() {
    let marker = std::env::temp_dir().join(format!("basis-missing-marker-{}", Uuid::new_v4()));
    let mut observer = AvatarObserver::new(40.0, 1, Some(marker), Duration::from_secs(1));
    let start = std::time::Instant::now();
    let baseline = vec![0; ProtocolBitQuality::High.payload_len()];
    for (seconds, sequence) in [(0, 1), (3, 130), (4, 131)] {
        let (channel, full) = tests::observer_full_frame(7, sequence, &baseline);
        observer.observe_channel(
            channel,
            &full,
            [0.0; 3],
            start + Duration::from_secs(seconds),
        );
    }
    assert_eq!(
        (observer.discontinuities, observer.sequence_resyncs),
        (0, 0)
    );
    observer.begin_window([0.0; 3], start + Duration::from_secs(5));
    observer.mark_discontinuity(start + Duration::from_millis(5500));
    assert_eq!(observer.discontinuities, 1);
    observer.mark_discontinuity(start + Duration::from_secs(7));
    assert_eq!(observer.discontinuities, 1);
    observer.begin_window([0.0; 3], start + Duration::from_secs(8));
    assert_eq!(
        (
            observer.discontinuities,
            observer.sequence_ambiguities,
            observer.sequence_resyncs
        ),
        (0, 0, 0)
    );
}

#[test]
fn observer_post_window_resync_and_handoff_preserve_finished_statistics() {
    let mut observer = AvatarObserver::new(40.0, 1, None, Duration::from_secs(1));
    let start = std::time::Instant::now();
    let baseline = vec![0; ProtocolBitQuality::High.payload_len()];
    for (millis, sequence) in [(0, 1), (900, 2)] {
        let (channel, full) = tests::observer_full_frame(7, sequence, &baseline);
        observer.observe_channel(
            channel,
            &full,
            [0.0; 3],
            start + Duration::from_millis(millis),
        );
    }
    let completed = observer.summary_and_csv(start + Duration::from_secs(1));
    let (channel, full) = tests::observer_full_frame(7, 140, &baseline);
    observer.observe_channel(channel, &full, [0.0; 3], start + Duration::from_secs(3));
    observer.mark_discontinuity(start + Duration::from_secs(4));
    assert_eq!(
        observer.summary_and_csv(start + Duration::from_secs(5)),
        completed
    );
}

#[tokio::test]
async fn observer_candidates_are_bounded_and_reconnect_uses_same_session() {
    let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let session = Arc::new(ObserverSession::new(AvatarObserver::new(
        40.0,
        1,
        None,
        Duration::from_secs(60),
    )));
    let config = Config {
        ip: "127.0.0.1".to_string(),
        port: server.local_addr().unwrap().port(),
        observer_session: Some(session.clone()),
        ..Config::default()
    };
    let ready = ReadyMessage::new(&config, [0.0; 3]).unwrap();
    for index in [0, 2, 3, 2] {
        let client = BasisClient::start(index, &config, ready.clone(), [0.0; 3], true)
            .await
            .unwrap();
        if index < OBSERVER_CANDIDATE_COUNT {
            assert!(Arc::ptr_eq(
                client.avatar_observer.as_ref().unwrap(),
                &session
            ));
        } else {
            assert!(client.avatar_observer.is_none());
        }
        assert_eq!(client.local_peer_id, index as i32);
        assert_eq!(client.connection_number, 0);
        client.deactivate();
    }
}
