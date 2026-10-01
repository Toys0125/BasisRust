//! Loopback checks exercise the dispatcher and actual outbound client packets.
use super::*;
use basis_transport::PacketProperty;
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
        writer.put_u16(SERVER_VERSION);
        let config = state.config.read().clone();
        NetworkApplication::write(&mut writer, &config.company_name, &config.product_name);
        BytesMessage { data: Vec::new() }.serialize(&mut writer);
        super::tests::ready_message(uuid).serialize(&mut writer);
        socket.send(writer.as_slice()).await.unwrap();
        let mut client = Self {
            socket,
            pending: Vec::new(),
            received: HashSet::new(),
            sequences: HashMap::new(),
            fragments: HashMap::new(),
        };
        client
            .receive(|channel, _| channel == channels::META_DATA)
            .await;
        client
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
        AdminRequest { mode }.serialize(&mut writer);
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
            w.put_string("MODERATOR");
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
            w.put_string("ordinary");
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
            w.put_string("ordinary");
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
