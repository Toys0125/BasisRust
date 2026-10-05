//! Loopback checks exercise the dispatcher and actual outbound client packets.
use super::*;
use basis_transport::PacketProperty;
use ed25519_dalek::{Signer, SigningKey};
use tokio::net::UdpSocket;

struct Client {
    socket: UdpSocket,
    pending: Vec<(u8, Vec<u8>)>,
    received: HashSet<(u8, u16)>,
    sequences: HashMap<u8, u16>,
    fragments: HashMap<(u8, u16), Vec<Option<Vec<u8>>>>,
}

impl Client {
    async fn connect(state: &ServerState, uuid: &str) -> Self {
        let mut client = Self::connect_pending(state, uuid).await;
        client
            .receive(|channel, _| channel == channels::META_DATA)
            .await;
        client
    }

    async fn connect_pending(state: &ServerState, uuid: &str) -> Self {
        Self::connect_pending_with_auth(state, uuid, &[]).await
    }

    async fn connect_pending_with_auth(state: &ServerState, uuid: &str, auth: &[u8]) -> Self {
        Self::connect_pending_with_contract(state, uuid, auth, SERVER_VERSION, None).await
    }

    async fn connect_pending_with_contract(
        state: &ServerState,
        uuid: &str,
        auth: &[u8],
        version: u16,
        application: Option<(&str, &str)>,
    ) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket
            .connect(state.transport.local_addr().unwrap())
            .await
            .unwrap();
        let mut writer = NetWriter::new();
        writer.put_u8(PacketProperty::ConnectRequest as u8);
        writer.put_i32(basis_protocol::version::LITENETLIB_PROTOCOL_ID);
        writer.put_i64(1);
        writer.put_i32(0);
        writer.put_u8(16);
        writer.put_bytes(&[0; 16]);
        writer.put_u16(version);
        let config = state.config.read().clone();
        let (company, product) =
            application.unwrap_or((&config.company_name, &config.product_name));
        NetworkApplication::write(&mut writer, company, product).unwrap();
        BytesMessage {
            data: auth.to_vec(),
        }
        .serialize(&mut writer)
        .unwrap();
        super::tests::ready_message(uuid)
            .serialize(&mut writer)
            .unwrap();
        socket.send(writer.as_slice()).await.unwrap();
        Self {
            socket,
            pending: Vec::new(),
            received: HashSet::new(),
            sequences: HashMap::new(),
            fragments: HashMap::new(),
        }
    }

    async fn answer_identity(&mut self, key: &SigningKey) -> Vec<u8> {
        let challenge = self.receive_identity_challenge().await;
        let signature = key.sign(&challenge);
        self.send_identity_response(signature.to_bytes().to_vec())
            .await;
        challenge
    }

    async fn receive_identity_challenge(&mut self) -> Vec<u8> {
        let payload = self
            .receive(|channel, _| channel == channels::AUTH_IDENTITY)
            .await;
        BytesMessage::deserialize(&mut NetReader::new(&payload))
            .unwrap()
            .data
    }

    async fn send_identity_response(&mut self, signature: Vec<u8>) {
        let mut response = NetWriter::new();
        BytesMessage { data: signature }
            .serialize(&mut response)
            .unwrap();
        BytesMessage {
            data: b"N/A".to_vec(),
        }
        .serialize(&mut response)
        .unwrap();
        self.send(channels::AUTH_IDENTITY, response.as_slice(), true)
            .await;
    }

    async fn send_raw_identity(&mut self, payload: &[u8]) {
        self.send(channels::AUTH_IDENTITY, payload, true).await;
    }

    async fn receive_disconnect(&mut self) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let mut packet = vec![0; 65535];
                let length = self.socket.recv(&mut packet).await.unwrap();
                packet.truncate(length);
                if PacketProperty::from_byte(packet[0]) == Some(PacketProperty::Disconnect) {
                    return packet;
                }
            }
        })
        .await
        .expect("server did not disconnect rejected identity")
    }

    async fn send(&mut self, channel: u8, payload: &[u8], reliable: bool) {
        let mut writer = NetWriter::new();
        if reliable {
            writer.put_u8(PacketProperty::Channeled as u8);
            let sequence = self.sequences.entry(channel).or_default();
            writer.put_u16(*sequence);
            *sequence += 1;
            writer.put_u8(DeliveryMethod::channel_id(
                channel,
                DeliveryMethod::ReliableOrdered,
            ));
        } else {
            writer.put_u8(PacketProperty::Unreliable as u8);
            writer.put_u8(channel);
        }
        writer.put_bytes(payload);
        self.socket.send(writer.as_slice()).await.unwrap();
    }

    async fn admin(&mut self, mode: AdminRequestMode, payload: impl FnOnce(&mut NetWriter)) {
        let mut writer = NetWriter::new();
        AdminRequest { mode }.serialize(&mut writer).unwrap();
        payload(&mut writer);
        self.send(channels::ADMIN, writer.as_slice(), true).await;
    }

    async fn receive(&mut self, matches: impl Fn(u8, &[u8]) -> bool) -> Vec<u8> {
        tokio::time::timeout(Duration::from_secs(2), self.receive_inner(matches))
            .await
            .expect("expected client payload did not arrive")
    }

    async fn receive_inner(&mut self, matches: impl Fn(u8, &[u8]) -> bool) -> Vec<u8> {
        loop {
            if let Some(index) = self
                .pending
                .iter()
                .position(|(channel, payload)| matches(*channel, payload))
            {
                return self.pending.remove(index).1;
            }
            let mut packet = vec![0; 65535];
            let length = self.socket.recv(&mut packet).await.unwrap();
            let mut frames = Vec::new();
            unpack(&packet[..length], &mut frames);
            for (channel, sequence, mut payload, fragment) in frames {
                if let Some((channel_id, sequence)) = sequence {
                    let mut ack = vec![PacketProperty::Ack as u8, 0, 0, channel_id];
                    ack.extend_from_slice(&[0; 16]);
                    ack[4 + (sequence as usize % 128) / 8] |= 1 << (sequence % 8);
                    self.socket.send(&ack).await.unwrap();
                    if !self.received.insert((channel_id, sequence)) {
                        continue;
                    }
                }
                if let Some((id, part, total)) = fragment {
                    let parts = self
                        .fragments
                        .entry((channel, id))
                        .or_insert_with(|| vec![None; total as usize]);
                    parts[part as usize] = Some(payload);
                    if parts.iter().any(Option::is_none) {
                        continue;
                    }
                    payload = parts
                        .iter()
                        .flat_map(|part| part.as_ref().unwrap().iter().copied())
                        .collect();
                    self.fragments.remove(&(channel, id));
                }
                self.pending.push((channel, payload));
            }
        }
    }
}

type Frame = (u8, Option<(u8, u16)>, Vec<u8>, Option<(u16, u16, u16)>);

fn unpack(packet: &[u8], frames: &mut Vec<Frame>) {
    match PacketProperty::from_byte(packet[0]) {
        Some(PacketProperty::Channeled) => {
            let sequence = u16::from_le_bytes([packet[1], packet[2]]);
            let fragment = if packet[0] & 128 != 0 {
                Some((
                    u16::from_le_bytes([packet[4], packet[5]]),
                    u16::from_le_bytes([packet[6], packet[7]]),
                    u16::from_le_bytes([packet[8], packet[9]]),
                ))
            } else {
                None
            };
            let offset = if fragment.is_some() { 10 } else { 4 };
            frames.push((
                packet[3] / 4,
                Some((packet[3], sequence)),
                packet[offset..].to_vec(),
                fragment,
            ));
        }
        Some(PacketProperty::Unreliable) => {
            frames.push((packet[1], None, packet[2..].to_vec(), None))
        }
        Some(PacketProperty::Merged) => {
            let mut reader = NetReader::new(&packet[1..]);
            while reader.remaining() != 0 {
                let size = reader.get_u16().unwrap() as usize;
                unpack(reader.get_bytes(size).unwrap(), frames);
            }
        }
        Some(PacketProperty::CompactMerged) => {
            let mut reader = NetReader::new(&packet[1..]);
            while reader.remaining() != 0 {
                let tag = reader.get_u8().unwrap();
                let size = if tag & 128 != 0 {
                    reader.get_u16().unwrap() as usize
                } else {
                    reader.get_u8().unwrap() as usize
                };
                let payload = reader.get_bytes(size).unwrap();
                if tag & 64 != 0 {
                    unpack(payload, frames);
                } else {
                    frames.push((tag & 63, None, payload.to_vec(), None));
                }
            }
        }
        _ => {}
    }
}

fn admin_mode(payload: &[u8], mode: AdminRequestMode) -> bool {
    payload.first() == Some(&(mode as u8))
}

#[tokio::test]
async fn voice_continues_while_avatar_input_is_blocked() {
    let (state, shutdown, _) = super::tests::test_server(false).await;
    let mut sender = Client::connect(&state, "voice-sender").await;
    let mut recipient = Client::connect(&state, "voice-recipient").await;
    let sender_id = peer_by_uuid(&state, "voice-sender").unwrap();
    let recipient_id = peer_by_uuid(&state, "voice-recipient").unwrap();
    state.voice_recipients.insert(sender_id, vec![recipient_id]);
    state
        .uplink_delta_states
        .insert(sender_id, UplinkDeltaState::empty());
    let avatar_lock = state.uplink_delta_states.get_mut(&sender_id).unwrap();
    sender
        .send(
            channels::DELTA_AVATAR,
            &[BitQuality::High as u8, 0, 0],
            false,
        )
        .await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    for sequence in 0..10 {
        sender
            .send(channels::VOICE, &[sequence, 0, 0xf8, 0], false)
            .await;
        let payload = tokio::time::timeout(
            Duration::from_millis(100),
            recipient.receive_inner(|c, _| c == channels::VOICE),
        )
        .await
        .expect("avatar input blocked voice");
        assert_eq!(payload, [sender_id as u8, sequence, 0, 0xf8, 0]);
    }
    drop(avatar_lock);
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}

#[tokio::test]
async fn client_packets_verify_snapshot_gate_queries_mutes_and_live_permission_updates() {
    let (state, shutdown, _) = super::tests::test_server(false).await;
    state.permissions.add_user_to_group("staff", "moderator");
    let mut ordinary = Client::connect(&state, "ordinary").await;
    let mut staff = Client::connect(&state, "staff").await;
    let ordinary_id = peer_by_uuid(&state, "ordinary").unwrap();
    ordinary
        .admin(AdminRequestMode::GetPermissions, |_| {})
        .await;
    let denial = ordinary
        .receive(|c, p| c == channels::ADMIN && admin_mode(p, AdminRequestMode::Message))
        .await;
    assert_eq!(
        NetReader::new(&denial[1..]).get_string().unwrap(),
        "No permission: basis.permissions.view"
    );
    staff.admin(AdminRequestMode::GetPermissions, |_| {}).await;
    let snapshot = staff
        .receive(|c, p| c == channels::ADMIN && admin_mode(p, AdminRequestMode::GetPermissions))
        .await;
    assert_eq!(NetReader::new(&snapshot[1..]).get_i32().unwrap(), 3);
    ordinary
        .admin(AdminRequestMode::QueryPermission, |w| {
            w.put_u16(peer_by_uuid(&state, "staff").unwrap());
            w.put_u8(1);
            w.put_string("MODERATOR").unwrap();
        })
        .await;
    let query = ordinary
        .receive(|c, p| {
            c == channels::ADMIN && admin_mode(p, AdminRequestMode::QueryPermissionResult)
        })
        .await;
    let mut reader = NetReader::new(&query[1..]);
    reader.get_u16().unwrap();
    assert_eq!(reader.get_u8().unwrap(), 1);
    assert_eq!(reader.get_string().unwrap(), "MODERATOR");
    assert!(reader.get_bool().unwrap() && reader.get_bool().unwrap());
    staff
        .admin(AdminRequestMode::SetVoiceMute, |w| {
            w.put_string("ordinary").unwrap();
            w.put_bool(true);
        })
        .await;
    let mute = ordinary
        .receive(|c, p| c == channels::ADMIN && admin_mode(p, AdminRequestMode::MuteStateApply))
        .await;
    assert_eq!(mute, vec![85, 1, 0]);
    staff
        .admin(AdminRequestMode::EnableAnnounceMode, |w| {
            w.put_u16(ordinary_id)
        })
        .await;
    ordinary
        .receive(|c, p| c == channels::ADMIN && admin_mode(p, AdminRequestMode::EnableAnnounceMode))
        .await;
    ordinary
        .send(channels::SHOUT_VOICE, &[0xaa, 0xbb], false)
        .await;
    assert!(tokio::time::timeout(
        Duration::from_millis(150),
        staff.receive_inner(|c, _| c == channels::SHOUT_VOICE)
    )
    .await
    .is_err());
    staff
        .admin(AdminRequestMode::SetVoiceMute, |w| {
            w.put_string("ordinary").unwrap();
            w.put_bool(false);
        })
        .await;
    ordinary
        .receive(|c, p| c == channels::ADMIN && admin_mode(p, AdminRequestMode::MuteStateApply))
        .await;
    ordinary
        .send(channels::SHOUT_VOICE, &[0xaa, 0xbb], false)
        .await;
    let audio = staff.receive(|c, _| c == channels::SHOUT_VOICE).await;
    assert_eq!(
        audio,
        [ordinary_id.to_le_bytes().as_slice(), &[0xaa, 0xbb]].concat()
    );
    state.permissions.add_user_to_group("ordinary", "moderator");
    let metadata = ordinary.receive(|c, _| c == channels::META_DATA).await;
    let mut reader = NetReader::new(&metadata);
    ClientMetaDataMessage::deserialize(&mut reader).unwrap();
    for _ in 0..5 {
        reader.get_i32().unwrap();
    }
    let bits = reader.get_bytes_with_length().unwrap();
    assert_ne!(
        bits[3] & (1 << 1),
        0,
        "live refresh should grant the permissions.view bit"
    );
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}

fn client_did(key: &SigningKey) -> String {
    let mut multicodec = [0u8; 34];
    multicodec[0] = 0xed;
    multicodec[1] = 0x01;
    multicodec[2..].copy_from_slice(&key.verifying_key().to_bytes());
    format!("did:key:z{}", bs58::encode(multicodec).into_string())
}

#[tokio::test]
async fn admission_capacity_preserves_large_batches_and_releases_ip_and_global_slots() {
    let (state, shutdown, _) = super::tests::test_server(false).await;
    let first = "127.0.0.1".parse().unwrap();
    let second = "127.0.0.2".parse().unwrap();
    let mut slots = (0..2000)
        .map(|_| reserve_admission(&state, first).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        state.admission_slots.available_permits(),
        MAX_PENDING_ADMISSIONS - 2000
    );
    for _ in 2000..MAX_PENDING_PER_IP {
        slots.push(reserve_admission(&state, first).unwrap());
    }
    assert!(reserve_admission(&state, first).is_none());
    assert!(reserve_admission(&state, "::ffff:127.0.0.1".parse().unwrap()).is_none());
    for _ in 0..MAX_PENDING_PER_IP {
        slots.push(reserve_admission(&state, second).unwrap());
    }
    assert!(reserve_admission(&state, "127.0.0.3".parse().unwrap()).is_none());
    drop(slots);
    assert!(state.admissions_per_ip.lock().is_empty());
    assert_eq!(
        state.admission_slots.available_permits(),
        MAX_PENDING_ADMISSIONS
    );
    assert!(reserve_admission(&state, first).is_some());
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}

#[tokio::test]
async fn password_rejection_creates_no_challenge_and_valid_identity_keeps_wire_flow() {
    let (state, shutdown, _) = super::tests::test_server(false).await;
    let key = SigningKey::from_bytes(&[91; 32]);
    let did = client_did(&key);
    {
        let mut config = state.config.write();
        config.use_auth = true;
        config.use_auth_identity = true;
    }
    for auth in [&b""[..], &b"wrong_password"[..]] {
        let mut rejected = Client::connect_pending_with_auth(&state, &did, auth).await;
        rejected.receive_disconnect().await;
        assert!(state.pending_identity.is_empty());
        assert_eq!(state.transport.connected_peers_count(), 0);
        assert_eq!(
            state.admission_slots.available_permits(),
            MAX_PENDING_ADMISSIONS
        );
    }
    for (version, application) in [
        (SERVER_VERSION - 1, None),
        (SERVER_VERSION, Some(("wrong-company", "wrong-product"))),
    ] {
        let mut rejected = Client::connect_pending_with_contract(
            &state,
            &did,
            b"default_password",
            version,
            application,
        )
        .await;
        rejected.receive_disconnect().await;
        assert!(state.pending_identity.is_empty());
        assert_eq!(state.transport.connected_peers_count(), 0);
    }
    let mut valid = Client::connect_pending_with_auth(&state, &did, b"default_password").await;
    let challenge = valid.answer_identity(&key).await;
    assert_eq!(challenge.len(), 32);
    valid
        .receive(|channel, _| channel == channels::META_DATA)
        .await;
    assert!(peer_by_uuid(&state, &did).is_some());
    assert!(state.pending_identity.is_empty());
    assert_eq!(
        state.admission_slots.available_permits(),
        MAX_PENDING_ADMISSIONS
    );
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}

#[tokio::test]
async fn pending_capacity_rejection_is_clean_and_timeout_allows_reconnect() {
    let (state, shutdown, _) = super::tests::test_server(false).await;
    let key = SigningKey::from_bytes(&[92; 32]);
    let did = client_did(&key);
    {
        let mut config = state.config.write();
        config.use_auth_identity = true;
        config.auth_validation_time_out_miliseconds = 50;
    }
    let reserved = state
        .admission_slots
        .clone()
        .acquire_many_owned(MAX_PENDING_ADMISSIONS as u32)
        .await
        .unwrap();
    let mut rejected = Client::connect_pending(&state, &did).await;
    let rejection = rejected.receive_disconnect().await;
    let mut reader = NetReader::new(&rejection[9..]);
    assert_eq!(reader.get_u32().unwrap(), channels::REJECT_MAGIC);
    assert_eq!(reader.get_u8().unwrap(), channels::REJECT_KIND_SERVER_FULL);
    assert!(state.pending_identity.is_empty());
    assert_eq!(state.transport.connected_peers_count(), 0);
    drop(reserved);
    let mut silent = Client::connect_pending(&state, &did).await;
    silent.receive_identity_challenge().await;
    silent.receive_disconnect().await;
    // Timeout cancellation releases its capacity even without another raw packet.
    assert!(state.pending_identity.is_empty());
    assert_eq!(
        state.admission_slots.available_permits(),
        MAX_PENDING_ADMISSIONS
    );
    state.config.write().auth_validation_time_out_miliseconds = 5000;
    let mut reconnected = Client::connect_pending(&state, &did).await;
    reconnected.answer_identity(&key).await;
    reconnected
        .receive(|channel, _| channel == channels::META_DATA)
        .await;
    assert!(peer_by_uuid(&state, &did).is_some());
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}

#[tokio::test]
async fn legitimate_identity_batch_completes_and_releases_capacity() {
    let (state, shutdown, _) = super::tests::test_server(false).await;
    state.config.write().use_auth_identity = true;
    let key = SigningKey::from_bytes(&[93; 32]);
    let did = client_did(&key);
    let mut clients = Vec::new();
    for _ in 0..64 {
        let mut client = Client::connect_pending(&state, &did).await;
        let challenge = client.receive_identity_challenge().await;
        clients.push((client, challenge));
    }
    assert_eq!(state.pending_identity.len(), 64);
    for (client, challenge) in &mut clients {
        client
            .send_identity_response(key.sign(challenge).to_bytes().to_vec())
            .await;
    }
    for (client, _) in &mut clients {
        client
            .receive(|channel, _| channel == channels::META_DATA)
            .await;
    }
    assert_eq!(state.player_count(), 64);
    assert!(state.pending_identity.is_empty());
    assert_eq!(
        state.admission_slots.available_permits(),
        MAX_PENDING_ADMISSIONS
    );
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}

#[tokio::test]
async fn authenticated_peer_limit_is_rechecked_after_pending_identity_verification() {
    let (state, shutdown, _) = super::tests::test_server(false).await;
    {
        let mut config = state.config.write();
        config.use_auth_identity = true;
        config.peer_limit = 1;
    }
    let key = SigningKey::from_bytes(&[94; 32]);
    let did = client_did(&key);
    let mut first = Client::connect_pending(&state, &did).await;
    let first_challenge = first.receive_identity_challenge().await;
    let mut second = Client::connect_pending(&state, &did).await;
    let second_challenge = second.receive_identity_challenge().await;
    assert_eq!(state.pending_identity.len(), 2);
    first
        .send_identity_response(key.sign(&first_challenge).to_bytes().to_vec())
        .await;
    first
        .receive(|channel, _| channel == channels::META_DATA)
        .await;
    second
        .send_identity_response(key.sign(&second_challenge).to_bytes().to_vec())
        .await;
    second.receive_disconnect().await;
    assert_eq!(state.player_count(), 1);
    assert!(state.pending_identity.is_empty());
    assert_eq!(
        state.admission_slots.available_permits(),
        MAX_PENDING_ADMISSIONS
    );
    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}

#[tokio::test]
async fn identity_admission_rejects_spoofed_rejoin_and_editor_claims_and_replay() {
    let (state, shutdown, _) = super::tests::test_server(false).await;
    let captured_key = SigningKey::from_bytes(&[31; 32]);
    let captured_did = client_did(&captured_key);
    let editor_key = SigningKey::from_bytes(&[47; 32]);
    let editor_did = client_did(&editor_key);
    let attacker_key = SigningKey::from_bytes(&[63; 32]);
    {
        let mut config = state.config.write();
        config.use_auth_identity = true;
        config.basis_user_restriction_mode =
            basis_protocol::config::BasisUserRestrictionMode::RejoinOnly;
    }
    state
        .admin_runtime
        .rejoin_population
        .write()
        .insert(captured_did.clone());
    state.permissions.add_user_node(
        &editor_did,
        basis_protocol::permissions::nodes::CONFIGURATION_EDITOR,
    );

    let mut spoof = Client::connect_pending(&state, &captured_did).await;
    let first_challenge = spoof.receive_identity_challenge().await;
    let replayable_signature = captured_key.sign(&first_challenge).to_bytes().to_vec();
    spoof
        .send_identity_response(attacker_key.sign(&first_challenge).to_bytes().to_vec())
        .await;
    spoof.receive_disconnect().await;

    let mut replay = Client::connect_pending(&state, &captured_did).await;
    let second_challenge = replay.receive_identity_challenge().await;
    assert_ne!(first_challenge, second_challenge);
    replay.send_identity_response(replayable_signature).await;
    replay.receive_disconnect().await;

    let mut legitimate = Client::connect_pending(&state, &captured_did).await;
    legitimate.answer_identity(&captured_key).await;
    legitimate
        .receive(|channel, _| channel == channels::META_DATA)
        .await;
    assert!(peer_by_uuid(&state, &captured_did).is_some());

    let mut editor_spoof = Client::connect_pending(&state, &editor_did).await;
    let editor_challenge = editor_spoof.receive_identity_challenge().await;
    editor_spoof
        .send_identity_response(attacker_key.sign(&editor_challenge).to_bytes().to_vec())
        .await;
    editor_spoof.receive_disconnect().await;
    assert!(peer_by_uuid(&state, &editor_did).is_none());

    let mut malformed = Client::connect_pending(&state, &editor_did).await;
    malformed.receive_identity_challenge().await;
    malformed.send_raw_identity(&[0xff]).await;
    malformed.receive_disconnect().await;
    assert!(peer_by_uuid(&state, &editor_did).is_none());

    state.config.write().use_auth_identity = false;
    let mut unauthenticated_editor = Client::connect_pending(&state, &editor_did).await;
    unauthenticated_editor.receive_disconnect().await;
    assert!(peer_by_uuid(&state, &editor_did).is_none());

    {
        let mut config = state.config.write();
        config.use_auth_identity = true;
        config.auth_validation_time_out_miliseconds = 50;
    }
    let mut silent = Client::connect_pending(&state, &captured_did).await;
    silent.receive_disconnect().await;
    assert_eq!(
        state
            .authenticated_peers
            .iter()
            .filter(|peer| peer.metadata.player_uuid == captured_did)
            .count(),
        1
    );

    state.shutdown().await.unwrap();
    let _ = shutdown.send(());
}
