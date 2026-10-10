//! Per-connection application upload admission, before coalescing or fanout.
use super::*;

const NANOS_PER_SECOND: u128 = 1_000_000_000;
pub const BURST_SECONDS: u64 = 2;

#[derive(Debug, Default)]
pub(crate) struct Bucket {
    rate: u64,
    tokens: u128,
    debt: u128,
    last_refill: Option<Instant>,
}

impl Bucket {
    fn admit_at(&mut self, rate: u64, bytes: usize, now: Instant, reliable: bool) -> bool {
        if rate == 0 {
            self.rate = 0;
            self.last_refill = None;
            self.debt = 0;
            return true;
        }
        let capacity = u128::from(rate) * u128::from(BURST_SECONDS) * NANOS_PER_SECOND;
        if let Some(previous) = self.last_refill {
            let refill = now
                .saturating_duration_since(previous)
                .as_nanos()
                .saturating_mul(u128::from(self.rate));
            let repayment = refill.min(self.debt);
            self.debt -= repayment;
            self.tokens = self.tokens.saturating_add(refill - repayment).min(capacity);
        } else {
            self.tokens = capacity;
        }
        self.rate = rate;
        self.last_refill = Some(now);
        let cost = (bytes as u128) * NANOS_PER_SECOND;
        if cost > self.tokens {
            // Preserve protocol-valid reliable frames larger than the ordinary
            // burst. Exactly one can spend from a full bucket; subsequent data
            // must wait until its complete cost has been replenished.
            if reliable && cost > capacity && self.tokens == capacity && self.debt == 0 {
                self.tokens = 0;
                self.debt = cost - capacity;
                return true;
            }
            return false;
        }
        self.tokens -= cost;
        true
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct UploadBudgetSnapshot {
    pub bytes_per_second_per_player: u64,
    pub burst_seconds: u64,
    pub accepted_messages: u64,
    pub accepted_bytes: u64,
    pub rejected_messages: u64,
    pub rejected_bytes: u64,
    pub reliable_rejections: u64,
    pub oversized_reliable_messages: u64,
    pub image_exempt_messages: u64,
    pub image_exempt_bytes: u64,
}

#[derive(Debug)]
pub(crate) struct UploadBudget {
    pub rate: AtomicU64,
    accepted_messages: AtomicU64,
    accepted_bytes: AtomicU64,
    rejected_messages: AtomicU64,
    rejected_bytes: AtomicU64,
    reliable_rejections: AtomicU64,
    oversized_reliable_messages: AtomicU64,
    image_exempt_messages: AtomicU64,
    image_exempt_bytes: AtomicU64,
}

pub(crate) fn best_effort(delivery: DeliveryMethod) -> bool {
    matches!(
        delivery,
        DeliveryMethod::Unreliable | DeliveryMethod::Sequenced
    )
}

/// Exempt control messages do not carry bulk application data. Images use their
/// separate advertised allowance; their Scene envelope is checked by the caller.
pub(crate) fn is_data(channel: u8, payload: &[u8]) -> bool {
    channels::PLAYER_AVATAR_QUALITY_CHANNELS.contains(&channel)
        || (channel == channels::DELTA_AVATAR
            && payload
                .first()
                .is_none_or(|header| header & channels::DELTA_HEADER_CONTROL_BIT == 0))
        || matches!(
            channel,
            channels::VOICE
                | channels::VOICE_LARGE
                | channels::SHOUT_VOICE
                | channels::AVATAR_CHANGE_MESSAGE
                | channels::AVATAR
                | channels::DIRECT_AVATAR_SERVER
                | channels::SCENE
                | channels::DIRECT_SCENE_SERVER
                | channels::CHAT
                | channels::NET_ID_ASSIGN
                | channels::LOAD_RESOURCE
                | channels::UNLOAD_RESOURCE
                | channels::MODIFY_RESOURCE
                | channels::CONTENT_SHARE
                | channels::CAMERA_PIP_STATE
                | channels::CAMERA_PIP_POSITION
                | channels::EVENTS
                | channels::SERVER_BOUND
        )
}

impl UploadBudget {
    pub fn new(rate: u64) -> Self {
        Self {
            rate: AtomicU64::new(rate),
            accepted_messages: AtomicU64::new(0),
            accepted_bytes: AtomicU64::new(0),
            rejected_messages: AtomicU64::new(0),
            rejected_bytes: AtomicU64::new(0),
            reliable_rejections: AtomicU64::new(0),
            oversized_reliable_messages: AtomicU64::new(0),
            image_exempt_messages: AtomicU64::new(0),
            image_exempt_bytes: AtomicU64::new(0),
        }
    }

    pub fn admit(&self, bucket: &Mutex<Bucket>, bytes: usize, delivery: DeliveryMethod) -> bool {
        let rate = self.rate.load(Ordering::Relaxed);
        let allowed = bucket
            .lock()
            .admit_at(rate, bytes, Instant::now(), !best_effort(delivery));
        if allowed {
            if rate != 0
                && !best_effort(delivery)
                && (bytes as u128) > u128::from(rate) * u128::from(BURST_SECONDS)
            {
                self.oversized_reliable_messages
                    .fetch_add(1, Ordering::Relaxed);
            }
            self.accepted_messages.fetch_add(1, Ordering::Relaxed);
            self.accepted_bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
        } else {
            self.rejected_messages.fetch_add(1, Ordering::Relaxed);
            self.rejected_bytes
                .fetch_add(bytes as u64, Ordering::Relaxed);
            if !best_effort(delivery) {
                self.reliable_rejections.fetch_add(1, Ordering::Relaxed);
            }
        }
        allowed
    }

    pub fn exempt_image(&self, bytes: usize) {
        self.image_exempt_messages.fetch_add(1, Ordering::Relaxed);
        self.image_exempt_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> UploadBudgetSnapshot {
        UploadBudgetSnapshot {
            bytes_per_second_per_player: self.rate.load(Ordering::Relaxed),
            burst_seconds: BURST_SECONDS,
            accepted_messages: self.accepted_messages.load(Ordering::Relaxed),
            accepted_bytes: self.accepted_bytes.load(Ordering::Relaxed),
            rejected_messages: self.rejected_messages.load(Ordering::Relaxed),
            rejected_bytes: self.rejected_bytes.load(Ordering::Relaxed),
            reliable_rejections: self.reliable_rejections.load(Ordering::Relaxed),
            oversized_reliable_messages: self.oversized_reliable_messages.load(Ordering::Relaxed),
            image_exempt_messages: self.image_exempt_messages.load(Ordering::Relaxed),
            image_exempt_bytes: self.image_exempt_bytes.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strict_burst_refill_and_oversized_packets() {
        let now = Instant::now();
        let mut bucket = Bucket::default();
        assert!(bucket.admit_at(131072, 262144, now, false));
        assert!(!bucket.admit_at(131072, 1, now, false));
        assert!(bucket.admit_at(131072, 65536, now + Duration::from_millis(500), false));
        assert!(!bucket.admit_at(131072, 1, now + Duration::from_millis(500), false));
        assert!(!bucket.admit_at(131072, 262145, now + Duration::from_secs(100), false));
        assert!(bucket.admit_at(131072, 262144, now + Duration::from_secs(100), false));
    }

    #[test]
    fn one_oversized_reliable_frame_from_full_bucket_is_charged_as_debt() {
        let now = Instant::now();
        let mut bucket = Bucket::default();
        assert!(bucket.admit_at(100, 300, now, true));
        assert!(!bucket.admit_at(100, 1, now, false));
        assert!(!bucket.admit_at(100, 300, now, true));
        assert!(!bucket.admit_at(100, 1, now + Duration::from_secs(1), false));
        assert!(bucket.admit_at(100, 50, now + Duration::from_millis(1500), false));
        assert!(!bucket.admit_at(100, 300, now + Duration::from_secs(2), true));
        assert!(bucket.admit_at(100, 300, now + Duration::from_millis(3500), true));
        let budget = UploadBudget::new(100);
        assert!(budget.admit(
            &Mutex::new(Bucket::default()),
            300,
            DeliveryMethod::ReliableOrdered
        ));
        assert_eq!(budget.snapshot().oversized_reliable_messages, 1);
        assert_eq!(budget.snapshot().accepted_bytes, 300);
    }

    #[test]
    fn independent_connections_and_reconfiguration() {
        let now = Instant::now();
        let mut old = Bucket::default();
        assert!(old.admit_at(100, 200, now, false));
        assert!(!old.admit_at(100, 1, now, false));
        let mut replacement = Bucket::default();
        assert!(replacement.admit_at(100, 200, now, false));
        assert!(old.admit_at(0, usize::MAX, now, false));
        assert!(old.admit_at(100, 200, now, false));
        assert!(!old.admit_at(50, 1, now, false));
    }

    #[test]
    fn concurrent_admission_cannot_overspend_and_counts_rejections() {
        let now = Instant::now();
        let bucket = Arc::new(Mutex::new(Bucket::default()));
        let accepted = std::thread::scope(|scope| {
            let joins = (0..8)
                .map(|_| {
                    let bucket = bucket.clone();
                    scope.spawn(move || {
                        (0..50)
                            .filter(|_| bucket.lock().admit_at(100, 1, now, false))
                            .count()
                    })
                })
                .collect::<Vec<_>>();
            joins
                .into_iter()
                .map(|join| join.join().unwrap())
                .sum::<usize>()
        });
        assert_eq!(accepted, 200);
        let budget = UploadBudget::new(100);
        assert!(budget.admit(
            &Mutex::new(Bucket::default()),
            100,
            DeliveryMethod::Unreliable
        ));
        assert!(!budget.admit(&bucket, 201, DeliveryMethod::ReliableOrdered));
        let stats = budget.snapshot();
        assert_eq!((stats.accepted_messages, stats.accepted_bytes), (1, 100));
        assert_eq!(
            (
                stats.rejected_messages,
                stats.rejected_bytes,
                stats.reliable_rejections
            ),
            (1, 201, 1)
        );
    }

    #[test]
    fn data_and_control_routes_are_distinct() {
        for channel in channels::PLAYER_AVATAR_QUALITY_CHANNELS {
            assert!(is_data(channel, &[]));
        }
        for channel in [
            channels::VOICE,
            channels::SCENE,
            channels::DIRECT_SCENE_SERVER,
            channels::AVATAR,
            channels::CHAT,
            channels::EVENTS,
            channels::LOAD_RESOURCE,
        ] {
            assert!(is_data(channel, &[]));
        }
        assert!(is_data(channels::DELTA_AVATAR, &[0]));
        assert!(!is_data(
            channels::DELTA_AVATAR,
            &[channels::DELTA_CONTROL_KEYFRAME_REQUEST]
        ));
        for channel in [
            channels::AUTH_IDENTITY,
            channels::ADMIN,
            channels::P2P,
            channels::REGISTRY_CONTROL,
            channels::SERVER_STATISTICS,
            channels::AUDIO_RECIPIENTS,
        ] {
            assert!(!is_data(channel, &[]));
        }
    }

    #[tokio::test]
    async fn ingress_charges_once_before_fanout_preserves_images_and_realtime_admission() {
        let config = ServerConfig {
            has_file_support: false,
            set_port: 0,
            override_auto_discovery_of_ipv: true,
            ipv4_address: "127.0.0.1".into(),
            max_upload_bytes_per_second_per_player: 100,
            ..ServerConfig::default()
        };
        let (server, _shutdown) = ServerState::start(config, &std::env::temp_dir())
            .await
            .unwrap();
        assert!(!server
            .status_text_with_detail(false)
            .contains("UploadBudget:"));
        server.config.write().health_include_extended_metrics = false;
        assert!(!server
            .status_text_with_detail(true)
            .contains("UploadBudget:"));
        server.config.write().health_include_extended_metrics = true;
        assert!(server
            .status_text_with_detail(true)
            .contains("UploadBudget: rate=100B/s/peer"));
        for id in 0..3 {
            server
                .authenticated_peers
                .insert(id, crate::tests::test_connected_peer(id));
        }
        let mut writer = NetWriter::new();
        SceneDataMessage {
            message_index: 1,
            recipients: Vec::new(),
            payload: vec![0; 96],
        }
        .serialize(&mut writer)
        .unwrap();
        let payload = Bytes::from(writer.into_vec());
        handle_message(
            &server,
            0,
            None,
            channels::SCENE,
            DeliveryMethod::Unreliable,
            payload.clone(),
            true,
            false,
            false,
        )
        .await
        .unwrap();
        assert_eq!(server.upload_budget_snapshot().accepted_bytes, 100);
        // Recipient count never multiplies source debit; the second 100-byte
        // upload fits the same two-second budget despite two recipient sends.
        handle_message(
            &server,
            0,
            None,
            channels::SCENE,
            DeliveryMethod::Unreliable,
            payload.clone(),
            true,
            false,
            false,
        )
        .await
        .unwrap();
        handle_message(
            &server,
            0,
            None,
            channels::SCENE,
            DeliveryMethod::Unreliable,
            payload,
            true,
            false,
            false,
        )
        .await
        .unwrap();
        assert_eq!(server.upload_budget_snapshot().accepted_bytes, 200);
        assert_eq!(server.upload_budget_snapshot().rejected_messages, 1);

        let (image, _) = server
            .net_ids
            .add_or_find_for_peer("BasisImagePickupManager", 0, 10)
            .unwrap();
        let mut writer = NetWriter::new();
        SceneDataMessage {
            message_index: image,
            recipients: Vec::new(),
            payload: vec![0; 300000],
        }
        .serialize(&mut writer)
        .unwrap();
        let image_payload = Bytes::from(writer.into_vec());
        assert!(is_image_scene_upload(&server, &image_payload));
        assert!(!is_image_scene_upload(&server, &[image as u8]));
        // Invalid recipient count cannot obtain the image exemption.
        let malformed = [image as u8, (image >> 8) as u8, 1, 0];
        assert!(!is_image_scene_upload(&server, &malformed));
        // Avoid a >MTU broadcast in this fixture while testing the same ingress
        // exception and its counters through the complete application handler.
        server.authenticated_peers.remove(&1);
        server.authenticated_peers.remove(&2);
        handle_message(
            &server,
            0,
            None,
            channels::SCENE,
            DeliveryMethod::ReliableOrdered,
            image_payload,
            true,
            false,
            false,
        )
        .await
        .unwrap();
        assert_eq!(server.upload_budget_snapshot().image_exempt_bytes, 300004);
        assert_eq!(server.upload_budget_snapshot().rejected_messages, 1);

        server
            .authenticated_peers
            .insert(0, crate::tests::test_connected_peer(0));
        server.avatar_sync.register_player(0);
        let mut pose = NetWriter::new();
        crate::tests::test_connected_peer(0)
            .ready
            .local_avatar_sync_message
            .serialize(&mut pose)
            .unwrap();
        let pose = Bytes::from(pose.into_vec());
        let bucket = server
            .authenticated_peers
            .get(&0)
            .unwrap()
            .upload_bucket
            .clone();
        // The realtime handler admits before queueing. Processing the admitted
        // pose must not debit a second time.
        assert!(server
            .upload_budget
            .admit(&bucket, pose.len(), DeliveryMethod::Unreliable));
        let before = server.upload_budget_snapshot().accepted_bytes;
        handle_message(
            &server,
            0,
            None,
            channels::PLAYER_AVATAR_HIGH,
            DeliveryMethod::Unreliable,
            pose,
            false,
            true,
            true,
        )
        .await
        .unwrap();
        assert_eq!(server.upload_budget_snapshot().accepted_bytes, before);
        server.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reliable_excess_disconnects_the_exact_connection_with_explicit_reason() {
        use basis_transport::PacketProperty;
        use tokio::{net::UdpSocket, time::timeout};
        let config = ServerConfig {
            has_file_support: false,
            set_port: 0,
            override_auto_discovery_of_ipv: true,
            ipv4_address: "127.0.0.1".into(),
            max_upload_bytes_per_second_per_player: 100,
            ..ServerConfig::default()
        };
        let (original, _shutdown) = ServerState::start(config, &std::env::temp_dir())
            .await
            .unwrap();
        let (transport, mut events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket
            .connect(transport.local_addr().unwrap())
            .await
            .unwrap();
        let mut writer = NetWriter::new();
        writer.put_u8(PacketProperty::ConnectRequest as u8);
        writer.put_i32(basis_protocol::version::LITENETLIB_PROTOCOL_ID);
        writer.put_i64(1);
        writer.put_i32(0);
        writer.put_u8(16);
        writer.put_bytes(&[0; 16]);
        socket.send(writer.as_slice()).await.unwrap();
        let ServerEvent::ConnectionRequest(request) =
            timeout(Duration::from_secs(2), events.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected connection request");
        };
        let session = transport.accept_session(&request).await.unwrap();
        let mut buffer = [0; 1024];
        socket.recv(&mut buffer).await.unwrap();
        let mut server = original.clone();
        server.transport = transport.clone();
        let mut peer = crate::tests::test_connected_peer(session.peer_id());
        peer.session = Some(session.clone());
        assert!(server
            .upload_budget
            .admit(&peer.upload_bucket, 200, DeliveryMethod::Unreliable));
        server.authenticated_peers.insert(peer.id, peer);
        handle_message(
            &server,
            session.peer_id(),
            Some(&session),
            channels::SERVER_BOUND,
            DeliveryMethod::ReliableOrdered,
            Bytes::from_static(b"excess"),
            true,
            false,
            false,
        )
        .await
        .unwrap();
        let len = timeout(Duration::from_secs(2), socket.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            PacketProperty::from_byte(buffer[0]),
            Some(PacketProperty::Disconnect)
        );
        assert!(String::from_utf8_lossy(&buffer[..len])
            .contains("Application upload rate limit exceeded"));
        assert_eq!(server.upload_budget_snapshot().reliable_rejections, 1);
        assert!(transport.peer_session(session.peer_id()).is_none());
        original.shutdown().await.unwrap();
        transport.shutdown();
    }
}
