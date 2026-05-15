use std::{
    collections::VecDeque,
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU8, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, Context, Result};
use basis_protocol::{channels, version::LITENETLIB_PROTOCOL_ID, version::SERVER_VERSION};
use basis_transport::{DeliveryMethod, PacketProperty};
use clap::Parser;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use flate2::{write::DeflateEncoder, Compression};
use rand::{rngs::OsRng, Rng, RngCore};
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{net::UdpSocket, sync::Mutex, time};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

const DEFAULT_WINDOW_SIZE: usize = 128;
const MAX_SEQUENCE: u16 = 32768;
const MOVEMENT_INTERVAL: Duration = Duration::from_millis(90);
const SOCKET_BUFFER_SIZE: usize = 32 * 1024 * 1024;
const SOCKET_TTL: u32 = 255;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Basis LiteNetLib-compatible Rust headless client"
)]
struct Args {
    #[arg(long, default_value = "Config.xml")]
    config: PathBuf,
    #[arg(long)]
    ip: Option<String>,
    #[arg(long)]
    port: Option<u16>,
    #[arg(long)]
    clients: Option<usize>,
    #[arg(long)]
    no_reconnect: bool,
    #[arg(long)]
    no_movement: bool,
    #[arg(long)]
    duration_secs: Option<u64>,
    #[arg(long, default_value_t = 100)]
    connect_batch_size: usize,
    #[arg(long, default_value_t = 250)]
    connect_batch_delay_ms: u64,
    #[arg(long, default_value_t = 5000)]
    connect_timeout_ms: u64,
    #[arg(long, default_value_t = 0)]
    spawn_group_size: usize,
    #[arg(long, default_value_t = 1000.0)]
    spawn_group_spacing: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "Configuration", rename_all = "PascalCase")]
struct Config {
    password: String,
    ip: String,
    port: u16,
    client_count: usize,
    avatar_password: String,
    avatar_url: String,
    avatar_load_mode: u8,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename = "Configuration", rename_all = "PascalCase")]
struct RawConfig {
    password: Option<String>,
    ip: Option<String>,
    port: Option<String>,
    client_count: Option<String>,
    avatar_password: Option<String>,
    avatar_url: Option<String>,
    avatar_load_mode: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            password: "default_password".to_string(),
            ip: "localhost".to_string(),
            port: 4296,
            client_count: 250,
            avatar_password: "default_avatar_password".to_string(),
            avatar_url: "http://localhost/avatar".to_string(),
            avatar_load_mode: 1,
        }
    }
}

impl Config {
    fn load_or_create(path: &Path) -> Result<Self> {
        if !path.exists() {
            let config = Self::default();
            let xml = config.to_pretty_xml();
            std::fs::write(path, xml)?;
            info!("created default config at {}", path.display());
            return Ok(config);
        }

        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        match quick_xml::de::from_str::<RawConfig>(&text) {
            Ok(raw) => Ok(Self::from_raw(raw)),
            Err(err) => {
                warn!("failed to parse config, using defaults: {err}");
                Ok(Self::default())
            }
        }
    }

    fn from_raw(raw: RawConfig) -> Self {
        let defaults = Self::default();
        Self {
            password: raw
                .password
                .filter(|s| !s.is_empty())
                .unwrap_or(defaults.password),
            ip: raw.ip.filter(|s| !s.is_empty()).unwrap_or(defaults.ip),
            port: parse_or_default(raw.port, defaults.port, "Port"),
            client_count: parse_or_default(raw.client_count, defaults.client_count, "ClientCount"),
            avatar_password: raw
                .avatar_password
                .filter(|s| !s.is_empty())
                .unwrap_or(defaults.avatar_password),
            avatar_url: raw
                .avatar_url
                .filter(|s| !s.is_empty())
                .unwrap_or(defaults.avatar_url),
            avatar_load_mode: parse_or_default(
                raw.avatar_load_mode,
                defaults.avatar_load_mode,
                "AvatarLoadMode",
            ),
        }
    }

    fn to_pretty_xml(&self) -> String {
        format!(
            "<Configuration>\n  <Password>{}</Password>\n  <Ip>{}</Ip>\n  <Port>{}</Port>\n  <ClientCount>{}</ClientCount>\n  <AvatarPassword>{}</AvatarPassword>\n  <AvatarUrl>{}</AvatarUrl>\n  <AvatarLoadMode>{}</AvatarLoadMode>\n</Configuration>\n",
            escape_xml(&self.password),
            escape_xml(&self.ip),
            self.port,
            self.client_count,
            escape_xml(&self.avatar_password),
            escape_xml(&self.avatar_url),
            self.avatar_load_mode
        )
    }
}

fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn parse_or_default<T>(value: Option<String>, default: T, field: &str) -> T
where
    T: std::str::FromStr + Copy,
{
    match value {
        Some(text) if !text.is_empty() => match text.parse::<T>() {
            Ok(value) => value,
            Err(_) => {
                warn!("invalid integer config field {field}={text:?}; using default");
                default
            }
        },
        _ => default,
    }
}

#[derive(Default, Debug, Clone)]
struct NetWriter {
    data: Vec<u8>,
}

impl NetWriter {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    fn put_u8(&mut self, value: u8) {
        self.data.push(value);
    }

    fn put_u16(&mut self, value: u16) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    fn put_i32(&mut self, value: i32) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    fn put_i64(&mut self, value: i64) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    fn put_f32(&mut self, value: f32) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    fn put_bytes(&mut self, value: &[u8]) {
        self.data.extend_from_slice(value);
    }

    fn put_string(&mut self, value: &str) {
        if value.is_empty() {
            self.put_u16(0);
            return;
        }
        let bytes = value.as_bytes();
        self.put_u16((bytes.len() + 1) as u16);
        self.put_bytes(bytes);
    }

    fn put_raw_len_string(&mut self, value: &str) {
        let bytes = value.as_bytes();
        self.put_u16(bytes.len() as u16);
        self.put_bytes(bytes);
    }

    fn into_vec(self) -> Vec<u8> {
        self.data
    }
}

fn put_bytes_message(writer: &mut NetWriter, data: &[u8]) {
    writer.put_u16(data.len() as u16);
    writer.put_bytes(data);
}

#[derive(Debug, Clone)]
struct ClientMetaDataMessage {
    player_uuid: String,
    player_display_name: String,
    player_platform: String,
}

impl ClientMetaDataMessage {
    fn random() -> Self {
        Self {
            player_uuid: Uuid::new_v4().to_string(),
            player_display_name: random_display_name(),
            player_platform: "Headless".to_string(),
        }
    }

    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_string(non_empty_or_failure(&self.player_uuid));
        writer.put_string(non_empty_or_failure(&self.player_display_name));
        writer.put_string(non_empty_or_failure(&self.player_platform));
    }
}

fn non_empty_or_failure(value: &str) -> &str {
    if value.is_empty() {
        "Failure"
    } else {
        value
    }
}

#[derive(Debug, Clone)]
struct ReadyMessage {
    metadata: ClientMetaDataMessage,
    avatar_change: ClientAvatarChangeMessage,
    local_avatar_sync: LocalAvatarSyncMessage,
}

impl ReadyMessage {
    fn new(config: &Config, spawn_base: [f32; 3]) -> Result<Self> {
        Ok(Self {
            metadata: ClientMetaDataMessage::random(),
            avatar_change: ClientAvatarChangeMessage::new(config)?,
            local_avatar_sync: LocalAvatarSyncMessage::standing_high(spawn_base),
        })
    }

    fn serialize(&self, writer: &mut NetWriter) {
        self.metadata.serialize(writer);
        self.avatar_change.serialize(writer);
        self.local_avatar_sync.serialize_initial(writer);
    }
}

#[derive(Debug, Clone)]
struct ClientAvatarChangeMessage {
    load_mode: u8,
    byte_array: Vec<u8>,
    local_avatar_index: u8,
}

impl ClientAvatarChangeMessage {
    fn new(config: &Config) -> Result<Self> {
        Ok(Self {
            load_mode: config.avatar_load_mode,
            byte_array: encode_avatar_network_load(&config.avatar_url, &config.avatar_password)?,
            local_avatar_index: 0,
        })
    }

    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.load_mode);
        writer.put_u16(self.byte_array.len() as u16);
        writer.put_bytes(&self.byte_array);
        writer.put_u8(self.local_avatar_index);
    }
}

fn encode_avatar_network_load(url: &str, unlock_password: &str) -> Result<Vec<u8>> {
    let mut raw = NetWriter::with_capacity(url.len() + unlock_password.len() + 4);
    raw.put_raw_len_string(url);
    raw.put_raw_len_string(unlock_password);

    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&raw.into_vec())?;
    Ok(encoder.finish()?)
}

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
#[allow(dead_code)]
enum BitQuality {
    VeryLow = 0,
    Low = 1,
    Medium = 2,
    High = 3,
}

impl BitQuality {
    fn payload_len(self) -> usize {
        match self {
            Self::VeryLow => 112,
            Self::Low => 131,
            Self::Medium => 156,
            Self::High => 182,
        }
    }

    fn rotation_len(self) -> usize {
        match self {
            Self::VeryLow => 78,
            Self::Low => 97,
            Self::Medium => 122,
            Self::High => 148,
        }
    }
}

#[derive(Debug, Clone)]
struct LocalAvatarSyncMessage {
    quality: BitQuality,
    payload: Vec<u8>,
}

impl LocalAvatarSyncMessage {
    fn standing_high(spawn_base: [f32; 3]) -> Self {
        let mut pose = PoseState::new_at(spawn_base);
        let payload = pose.high_quality_payload(0.0);
        Self {
            quality: BitQuality::High,
            payload,
        }
    }

    fn serialize_initial(&self, writer: &mut NetWriter) {
        writer.put_u8(self.quality as u8);
        writer.put_bytes(&self.payload);
        writer.put_u8(0);
    }
}

#[derive(Debug, Clone)]
struct PoseState {
    base: [f32; 3],
    packet: Vec<u8>,
}

impl PoseState {
    #[cfg(test)]
    fn new_random() -> Self {
        let mut rng = rand::thread_rng();
        Self::new_at([
            rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
        ])
    }

    fn new_at(base: [f32; 3]) -> Self {
        Self {
            base,
            packet: vec![0; 1 + BitQuality::High.payload_len()],
        }
    }

    fn drift(&mut self) {
        let mut rng = rand::thread_rng();
        self.base[0] += rng.gen_range(-0.25..=0.25);
        self.base[1] += rng.gen_range(-0.25..=0.25);
        self.base[2] += rng.gen_range(-0.25..=0.25);
    }

    fn high_quality_payload(&mut self, elapsed_secs: f32) -> Vec<u8> {
        self.drift();
        let mut writer = NetWriter::with_capacity(BitQuality::High.payload_len());
        writer.put_f32(self.base[0]);
        writer.put_f32(self.base[1] + elapsed_secs.sin() * 0.015);
        writer.put_f32(self.base[2]);

        let mut rotations = vec![0u8; BitQuality::High.rotation_len()];
        FakePoseGenerator::write_high_quality_rotations(&mut rotations, elapsed_secs);
        writer.put_bytes(&rotations);

        writer.put_u16(compress_scale(1.0));
        writer.put_bytes(&smallest_three_quaternion([0.0, 0.0, 0.0, 1.0]));
        writer.put_bytes(&[0; 6]);
        writer.put_bytes(&[3, 0, 0x80, 0, 0x80, 0, 0x80]);

        let mut payload = writer.into_vec();
        payload.resize(BitQuality::High.payload_len(), 0);
        payload
    }

    fn write_movement_packet(&mut self, sequence: u8, start: SystemTime) -> &[u8] {
        let elapsed = start.elapsed().unwrap_or_default().as_secs_f32();
        let payload = self.high_quality_payload(elapsed);
        self.packet[0] = sequence;
        self.packet[1..].copy_from_slice(&payload);
        &self.packet
    }
}

struct FakePoseGenerator;

impl FakePoseGenerator {
    fn write_high_quality_rotations(target: &mut [u8], elapsed_secs: f32) {
        const BPC_HIGH: [u8; 51] = [
            10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 9, 9, 5, 5, 6, 6,
            6, 6, 5, 6, 6, 6, 6, 5, 6, 6, 5, 5, 5, 6, 6, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
        ];
        const MAX_COMPONENT: [f32; 51] = [
            0.70710677, 0.70710677, 0.50, 0.70710677, 0.70710677, 0.70710677, 0.70710677,
            0.70710677, 0.70710677, 0.70710677, 0.70710677, 0.70710677, 0.70710677, 0.50, 0.50,
            0.70710677, 0.70710677, 0.60, 0.60, 0.50, 0.50, 0.68, 0.68, 0.68, 0.68, 0.68, 0.68,
            0.68, 0.68, 0.68, 0.68, 0.58, 0.58, 0.58, 0.58, 0.58, 0.58, 0.58, 0.58, 0.58, 0.58,
            0.65, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65, 0.65,
        ];

        target.fill(0);
        let mut bit_writer = BitWriter::new(target);
        for bone in 0..51 {
            let sway = (elapsed_secs * 0.7 + bone as f32 * 0.11).sin() * 0.006;
            let q = match bone {
                0 => [0.0, sway, 0.0, 1.0],
                1..=3 => [sway * 0.25, 0.0, 0.0, 1.0],
                10..=20 => [0.0, 0.0, sway * 0.35, 1.0],
                _ => [0.0, 0.0, 0.0, 1.0],
            };
            bit_writer.write_smallest_three(q, BPC_HIGH[bone] as usize, MAX_COMPONENT[bone]);
        }
    }
}

struct BitWriter<'a> {
    data: &'a mut [u8],
    bit: usize,
}

impl<'a> BitWriter<'a> {
    fn new(data: &'a mut [u8]) -> Self {
        Self { data, bit: 0 }
    }

    fn write_bits(&mut self, mut value: u32, count: usize) {
        for _ in 0..count {
            let byte = self.bit / 8;
            if byte >= self.data.len() {
                return;
            }
            let bit = self.bit % 8;
            if (value & 1) != 0 {
                self.data[byte] |= 1 << bit;
            }
            value >>= 1;
            self.bit += 1;
        }
    }

    fn write_smallest_three(&mut self, q: [f32; 4], bits_per_component: usize, max_range: f32) {
        let normalized = normalize_quat(q);
        let (largest, sign) = largest_component(normalized);
        self.write_bits(largest as u32, 2);
        let max_quantized = (1u32 << bits_per_component) - 1;
        for i in 0..4 {
            if i == largest {
                continue;
            }
            let normalized_component = ((normalized[i] * sign) / max_range).clamp(-1.0, 1.0);
            let quantized =
                ((normalized_component * 0.5 + 0.5) * max_quantized as f32).round() as u32;
            self.write_bits(quantized.min(max_quantized), bits_per_component);
        }
    }
}

fn normalize_quat(mut q: [f32; 4]) -> [f32; 4] {
    let len = (q[0] * q[0] + q[1] * q[1] + q[2] * q[2] + q[3] * q[3]).sqrt();
    if len > f32::EPSILON {
        for v in &mut q {
            *v /= len;
        }
    } else {
        q = [0.0, 0.0, 0.0, 1.0];
    }
    q
}

fn largest_component(q: [f32; 4]) -> (usize, f32) {
    let mut largest = 0;
    let mut largest_abs = q[0].abs();
    for (idx, value) in q.iter().enumerate().skip(1) {
        let abs = value.abs();
        if abs > largest_abs {
            largest = idx;
            largest_abs = abs;
        }
    }
    let sign = if q[largest] < 0.0 { -1.0 } else { 1.0 };
    (largest, sign)
}

fn smallest_three_quaternion(q: [f32; 4]) -> [u8; 7] {
    let q = normalize_quat(q);
    let (largest, sign) = largest_component(q);
    let mut out = [0u8; 7];
    out[0] = largest as u8;
    let mut offset = 1;
    for i in 0..4 {
        if i == largest {
            continue;
        }
        let value = (q[i] * sign).clamp(-0.70710677, 0.70710677);
        let quantized = (((value + 0.70710677) / 1.4142135) * 65535.0).round() as u16;
        out[offset..offset + 2].copy_from_slice(&quantized.to_le_bytes());
        offset += 2;
    }
    out
}

fn compress_scale(scale: f32) -> u16 {
    const MIN: f32 = 0.005;
    const MAX: f32 = 150.0;
    const RANGE: f32 = MAX - MIN;
    (((scale - MIN) / RANGE) * u16::MAX as f32).trunc() as u16
}

fn build_connection_payload(config: &Config, ready: &ReadyMessage) -> Vec<u8> {
    let auth = config.password.as_bytes();
    let mut writer = NetWriter::with_capacity(512);
    writer.put_u16(SERVER_VERSION);
    put_bytes_message(&mut writer, auth);
    ready.serialize(&mut writer);
    writer.into_vec()
}

fn build_movement_packet(sequence: u8, pose: &mut PoseState, start: SystemTime) -> Vec<u8> {
    pose.write_movement_packet(sequence, start).to_vec()
}

#[derive(Debug)]
struct Identity {
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
    fragment: String,
}

impl Identity {
    fn random() -> Self {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        let signing_key = SigningKey::from_bytes(&bytes);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
            fragment: String::new(),
        }
    }

    fn response_payload(&self, challenge: &[u8]) -> Result<Vec<u8>> {
        let signature = self.signing_key.sign(challenge);
        self.verifying_key
            .verify(challenge, &signature)
            .context("DID signature self-verification failed")?;
        let mut writer = NetWriter::with_capacity(96);
        put_bytes_message(&mut writer, &signature.to_bytes());
        let fragment = if self.fragment.is_empty() {
            "N/A".as_bytes()
        } else {
            self.fragment.as_bytes()
        };
        put_bytes_message(&mut writer, fragment);
        Ok(writer.into_vec())
    }
}

#[derive(Debug, Clone)]
struct ParsedPacket<'a> {
    property: PacketProperty,
    #[allow(dead_code)]
    connection_number: u8,
    sequence: Option<u16>,
    channel_id: Option<u8>,
    payload: &'a [u8],
}

fn parse_packet(bytes: &[u8]) -> Option<ParsedPacket<'_>> {
    if bytes.is_empty() {
        return None;
    }
    let property = PacketProperty::from_byte(bytes[0])?;
    let connection_number = (bytes[0] & 0x60) >> 5;
    let header = match property {
        PacketProperty::Unreliable => 2,
        PacketProperty::Channeled | PacketProperty::Ack => 4,
        PacketProperty::Ping => 3,
        PacketProperty::Pong => 11,
        PacketProperty::ConnectAccept => 15,
        PacketProperty::Disconnect => 9,
        _ => 1,
    };
    if bytes.len() < header {
        return None;
    }
    let sequence = match property {
        PacketProperty::Channeled
        | PacketProperty::Ack
        | PacketProperty::Ping
        | PacketProperty::Pong => Some(u16::from_le_bytes([bytes[1], bytes[2]])),
        _ => None,
    };
    let channel_id = match property {
        PacketProperty::Channeled | PacketProperty::Ack => Some(bytes[3]),
        _ => None,
    };
    Some(ParsedPacket {
        property,
        connection_number,
        sequence,
        channel_id,
        payload: &bytes[header..],
    })
}

#[derive(Debug)]
struct ReliableSend {
    channel_id: u8,
    sequence: u16,
    bytes: Vec<u8>,
    last_sent: Option<SystemTime>,
}

#[derive(Debug)]
struct BasisClient {
    index: usize,
    socket: Arc<UdpSocket>,
    server_addr: SocketAddr,
    connect_time: i64,
    connection_number: u8,
    local_peer_id: i32,
    remote_peer_id: Mutex<Option<i32>>,
    connected: AtomicBool,
    in_use: AtomicBool,
    movement_sequence: AtomicU8,
    reliable_sequence: AtomicU16,
    ping_sequence: AtomicU16,
    pending_reliable: Mutex<VecDeque<ReliableSend>>,
    pose: Mutex<PoseState>,
    identity: Identity,
}

impl BasisClient {
    async fn start(
        index: usize,
        config: &Config,
        ready: ReadyMessage,
        spawn_base: [f32; 3],
    ) -> Result<Arc<Self>> {
        let server_addr = resolve_addr(&config.ip, config.port)?;
        let socket = Arc::new(bind_udp_socket(any_local_addr(server_addr))?);
        let connect_time = dotnet_utc_ticks();
        let local_peer_id = index as i32;
        let client = Arc::new(Self {
            index,
            socket,
            server_addr,
            connect_time,
            connection_number: 0,
            local_peer_id,
            remote_peer_id: Mutex::new(None),
            connected: AtomicBool::new(false),
            in_use: AtomicBool::new(false),
            movement_sequence: AtomicU8::new(0),
            reliable_sequence: AtomicU16::new(0),
            ping_sequence: AtomicU16::new(0),
            pending_reliable: Mutex::new(VecDeque::new()),
            pose: Mutex::new(PoseState::new_at(spawn_base)),
            identity: Identity::random(),
        });

        client.start_client(config, &ready).await?;
        Ok(client)
    }

    async fn start_client(self: &Arc<Self>, config: &Config, ready: &ReadyMessage) -> Result<()> {
        if self.in_use.swap(true, Ordering::SeqCst) {
            error!("Call Shutdown First!");
            return Err(anyhow!("Call Shutdown First!"));
        }
        let payload = build_connection_payload(config, ready);
        let request = self.make_connect_request(&payload);
        self.socket.send_to(&request, self.server_addr).await?;
        debug!(
            "client {} sent connect request to {}",
            self.index, self.server_addr
        );

        let client = self.clone();
        tokio::spawn(async move {
            let index = client.index;
            if let Err(err) = client.receive_loop().await {
                debug!("client {index} receive loop ended: {err}");
            }
        });

        let client = self.clone();
        tokio::spawn(async move {
            client.maintenance_loop().await;
        });

        Ok(())
    }

    fn make_connect_request(&self, payload: &[u8]) -> Vec<u8> {
        let addr_bytes = socket_address_bytes(self.server_addr);
        let mut writer = NetWriter::with_capacity(18 + addr_bytes.len() + payload.len());
        writer.put_u8(PacketProperty::ConnectRequest as u8 | (self.connection_number << 5));
        writer.put_i32(LITENETLIB_PROTOCOL_ID);
        writer.put_i64(self.connect_time);
        writer.put_i32(self.local_peer_id);
        writer.put_u8(addr_bytes.len() as u8);
        writer.put_bytes(&addr_bytes);
        writer.put_bytes(payload);
        writer.into_vec()
    }

    async fn disconnect(&self) {
        self.in_use.store(false, Ordering::SeqCst);
        self.connected.store(false, Ordering::SeqCst);
        info!("client {} called disconnect", self.index);
        let mut packet = NetWriter::with_capacity(9);
        packet.put_u8(PacketProperty::Disconnect as u8 | (self.connection_number << 5));
        packet.put_i64(self.connect_time);
        let _ = self
            .socket
            .send_to(&packet.into_vec(), self.server_addr)
            .await;
        info!("client {} worker thread stopped", self.index);
    }

    async fn send_unreliable(&self, channel: u8, payload: &[u8]) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut packet = Vec::with_capacity(2 + payload.len());
        packet.push(PacketProperty::Unreliable as u8 | (self.connection_number << 5));
        packet.push(channel);
        packet.extend_from_slice(payload);
        trace!(
            "client {} sending unreliable channel={} bytes={} header={:02x} {:02x}",
            self.index,
            channel,
            payload.len(),
            packet[0],
            packet[1]
        );
        self.socket.send_to(&packet, self.server_addr).await?;
        Ok(())
    }

    async fn send_reliable_ordered(&self, channel: u8, payload: &[u8]) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }
        let sequence = self.reliable_sequence.fetch_add(1, Ordering::SeqCst) % MAX_SEQUENCE;
        let channel_id = DeliveryMethod::channel_id(channel, DeliveryMethod::ReliableOrdered);
        let mut packet = Vec::with_capacity(4 + payload.len());
        packet.push(PacketProperty::Channeled as u8 | (self.connection_number << 5));
        packet.extend_from_slice(&sequence.to_le_bytes());
        packet.push(channel_id);
        packet.extend_from_slice(payload);
        self.socket.send_to(&packet, self.server_addr).await?;
        self.pending_reliable.lock().await.push_back(ReliableSend {
            channel_id,
            sequence,
            bytes: packet,
            last_sent: Some(SystemTime::now()),
        });
        Ok(())
    }

    async fn receive_loop(self: Arc<Self>) -> Result<()> {
        let mut buffer = vec![0u8; 65535];
        loop {
            let (len, from) = self.socket.recv_from(&mut buffer).await?;
            if from != self.server_addr {
                continue;
            }
            self.handle_packet(&buffer[..len]).await?;
            if !self.in_use.load(Ordering::Relaxed) {
                break;
            }
        }
        Ok(())
    }

    async fn handle_packet(&self, bytes: &[u8]) -> Result<()> {
        let packet = match parse_packet(bytes) {
            Some(packet) => packet,
            None => return Ok(()),
        };
        trace!("client {} received {:?}", self.index, packet.property);
        match packet.property {
            PacketProperty::ConnectAccept => {
                if bytes.len() == 15
                    && i64::from_le_bytes(bytes[1..9].try_into().unwrap()) == self.connect_time
                {
                    let remote_peer = i32::from_le_bytes(bytes[11..15].try_into().unwrap());
                    *self.remote_peer_id.lock().await = Some(remote_peer);
                    self.connected.store(true, Ordering::SeqCst);
                    info!(
                        "client {} connected as remote peer {}",
                        self.index, remote_peer
                    );
                }
            }
            PacketProperty::Disconnect
            | PacketProperty::PeerNotFound
            | PacketProperty::InvalidProtocol => {
                warn!(
                    "client {} disconnected/rejected by server: {:?}",
                    self.index, packet.property
                );
                self.connected.store(false, Ordering::SeqCst);
                self.in_use.store(false, Ordering::SeqCst);
            }
            PacketProperty::Ping => {
                if let Some(sequence) = packet.sequence {
                    self.send_pong(sequence).await?;
                }
            }
            PacketProperty::Pong => {}
            PacketProperty::Ack => {
                if let Some(sequence) = packet.sequence {
                    if let Some(channel_id) = packet.channel_id {
                        self.process_ack(channel_id, sequence, packet.payload).await;
                    }
                }
            }
            PacketProperty::Channeled => {
                if let Some(channel_id) = packet.channel_id {
                    self.handle_channeled(channel_id, packet.sequence.unwrap_or(0), packet.payload)
                        .await?;
                }
            }
            PacketProperty::Unreliable => {
                let _channel = bytes.get(1).copied().unwrap_or_default();
            }
            PacketProperty::Merged => {
                let mut pos = 1;
                while pos + 2 <= bytes.len() {
                    let size = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
                    pos += 2;
                    if size == 0 || pos + size > bytes.len() {
                        break;
                    }
                    Box::pin(self.handle_packet(&bytes[pos..pos + size])).await?;
                    pos += size;
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_channeled(&self, channel_id: u8, sequence: u16, payload: &[u8]) -> Result<()> {
        let channel = channel_id / 4;
        let delivery = DeliveryMethod::from_channel_id(channel_id);
        if matches!(
            delivery,
            DeliveryMethod::ReliableOrdered | DeliveryMethod::ReliableUnordered
        ) {
            self.send_ack(channel_id, sequence).await?;
        }

        match channel {
            channels::AUTH_IDENTITY => {
                if let Some(challenge) = read_bytes_message(payload) {
                    let response = self.identity.response_payload(challenge)?;
                    self.send_reliable_ordered(channels::AUTH_IDENTITY, &response)
                        .await?;
                }
            }
            channels::META_DATA
            | channels::DISCONNECTION
            | channels::PLAYER_AVATAR_VERY_LOW
            | channels::PLAYER_AVATAR_VERY_LOW_ADDITIONAL
            | channels::PLAYER_AVATAR_LOW
            | channels::PLAYER_AVATAR_LOW_ADDITIONAL
            | channels::PLAYER_AVATAR_MEDIUM
            | channels::PLAYER_AVATAR_MEDIUM_ADDITIONAL
            | channels::PLAYER_AVATAR_HIGH
            | channels::PLAYER_AVATAR_HIGH_ADDITIONAL
            | channels::PLAYER_AVATAR_VERY_LOW_LARGE
            | channels::PLAYER_AVATAR_VERY_LOW_ADDITIONAL_LARGE
            | channels::PLAYER_AVATAR_LOW_LARGE
            | channels::PLAYER_AVATAR_LOW_ADDITIONAL_LARGE
            | channels::PLAYER_AVATAR_MEDIUM_LARGE
            | channels::PLAYER_AVATAR_MEDIUM_ADDITIONAL_LARGE
            | channels::PLAYER_AVATAR_HIGH_LARGE
            | channels::PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE
            | channels::COMPRESSED_AVATAR_BUNDLE
            | channels::SERVER_LIBRARY => {}
            _ => {}
        }
        Ok(())
    }

    async fn send_ack(&self, channel_id: u8, sequence: u16) -> Result<()> {
        let mut packet = vec![0u8; 4 + ((DEFAULT_WINDOW_SIZE - 1) / 8 + 2)];
        packet[0] = PacketProperty::Ack as u8 | (self.connection_number << 5);
        packet[1..3].copy_from_slice(&sequence.to_le_bytes());
        packet[3] = channel_id;
        let bit_index = (sequence as usize) % DEFAULT_WINDOW_SIZE;
        packet[4 + bit_index / 8] |= 1 << (bit_index % 8);
        self.socket.send_to(&packet, self.server_addr).await?;
        Ok(())
    }

    async fn send_pong(&self, sequence: u16) -> Result<()> {
        let mut writer = NetWriter::with_capacity(11);
        writer.put_u8(PacketProperty::Pong as u8 | (self.connection_number << 5));
        writer.put_u16(sequence);
        writer.put_i64(dotnet_utc_ticks());
        self.socket
            .send_to(&writer.into_vec(), self.server_addr)
            .await?;
        Ok(())
    }

    async fn send_ping(&self) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }
        let sequence = self.ping_sequence.fetch_add(1, Ordering::SeqCst);
        let mut writer = NetWriter::with_capacity(3);
        writer.put_u8(PacketProperty::Ping as u8 | (self.connection_number << 5));
        writer.put_u16(sequence);
        self.socket
            .send_to(&writer.into_vec(), self.server_addr)
            .await?;
        Ok(())
    }

    async fn process_ack(&self, channel_id: u8, ack_window_start: u16, ack_bits: &[u8]) {
        let mut pending = self.pending_reliable.lock().await;
        pending.retain(|item| {
            if item.channel_id != channel_id {
                return true;
            }
            let rel = relative_sequence(item.sequence, ack_window_start);
            if rel < 0 || rel as usize >= DEFAULT_WINDOW_SIZE {
                return true;
            }
            let pos = item.sequence as usize % DEFAULT_WINDOW_SIZE;
            let acked = ack_bits
                .get(pos / 8)
                .map(|b| (b & (1 << (pos % 8))) != 0)
                .unwrap_or(false);
            !acked
        });
    }

    async fn maintenance_loop(self: Arc<Self>) {
        let mut ping_tick = time::interval(Duration::from_millis(1500));
        let mut reliable_tick = time::interval(Duration::from_millis(15));
        loop {
            tokio::select! {
                _ = ping_tick.tick() => {
                    if !self.in_use.load(Ordering::Relaxed) {
                        break;
                    }
                    let _ = self.send_ping().await;
                }
                _ = reliable_tick.tick() => {
                    if !self.in_use.load(Ordering::Relaxed) {
                        break;
                    }
                    let _ = self.resend_reliable().await;
                }
            }
        }
    }

    async fn resend_reliable(&self) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }
        let mut pending = self.pending_reliable.lock().await;
        let now = SystemTime::now();
        for item in pending.iter_mut() {
            let should_send = item
                .last_sent
                .and_then(|sent| now.duration_since(sent).ok())
                .map(|elapsed| elapsed >= Duration::from_millis(150))
                .unwrap_or(true);
            if should_send {
                self.socket.send_to(&item.bytes, self.server_addr).await?;
                item.last_sent = Some(now);
            }
        }
        Ok(())
    }
}

fn relative_sequence(seq: u16, expected: u16) -> i32 {
    let seq = seq as i32;
    let expected = expected as i32;
    let diff = seq - expected;
    if diff <= -((MAX_SEQUENCE / 2) as i32) {
        diff + MAX_SEQUENCE as i32
    } else if diff >= (MAX_SEQUENCE / 2) as i32 {
        diff - MAX_SEQUENCE as i32
    } else {
        diff
    }
}

fn read_bytes_message(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 2 {
        return None;
    }
    let len = u16::from_le_bytes([data[0], data[1]]) as usize;
    data.get(2..2 + len)
}

fn resolve_addr(ip: &str, port: u16) -> Result<SocketAddr> {
    (ip, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("failed to resolve {ip}:{port}"))
}

fn any_local_addr(remote: SocketAddr) -> SocketAddr {
    match remote {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 0),
    }
}

fn bind_udp_socket(addr: SocketAddr) -> std::io::Result<UdpSocket> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    if matches!(addr, SocketAddr::V6(_)) {
        let _ = socket.set_only_v6(false);
    }
    let _ = socket.set_recv_buffer_size(SOCKET_BUFFER_SIZE);
    let _ = socket.set_send_buffer_size(SOCKET_BUFFER_SIZE);
    let _ = socket.set_ttl(SOCKET_TTL);
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    UdpSocket::from_std(socket.into())
}

fn socket_address_bytes(addr: SocketAddr) -> Vec<u8> {
    match addr {
        SocketAddr::V4(v4) => {
            let mut bytes = vec![0u8; 16];
            bytes[0] = 2;
            bytes[1] = 0;
            bytes[2..4].copy_from_slice(&v4.port().to_be_bytes());
            bytes[4..8].copy_from_slice(&v4.ip().octets());
            bytes
        }
        SocketAddr::V6(v6) => {
            let mut bytes = vec![0u8; 28];
            bytes[0] = 23;
            bytes[1] = 0;
            bytes[2..4].copy_from_slice(&v6.port().to_be_bytes());
            bytes[8..24].copy_from_slice(&v6.ip().octets());
            bytes
        }
    }
}

fn dotnet_utc_ticks() -> i64 {
    const TICKS_AT_UNIX_EPOCH: i64 = 621_355_968_000_000_000;
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    TICKS_AT_UNIX_EPOCH + unix.as_secs() as i64 * 10_000_000 + (unix.subsec_nanos() / 100) as i64
}

fn random_display_name() -> String {
    const ADJECTIVES: &[&str] = &[
        "Brisk", "Calm", "Clever", "Bright", "Swift", "Steady", "Quiet", "Lucky",
    ];
    const NOUNS: &[&str] = &[
        "Runner", "Pilot", "Mapper", "Drifter", "Builder", "Walker", "Scout", "Rider",
    ];
    const TITLES: &[&str] = &["Jr", "II", "III", "Prime", "Zero", "North", "West"];
    const COLORS: &[&str] = &["red", "green", "blue", "yellow", "cyan", "magenta", "white"];
    let mut rng = rand::thread_rng();
    format!(
        "<color={}>{} {} {}</color>",
        COLORS[rng.gen_range(0..COLORS.len())],
        ADJECTIVES[rng.gen_range(0..ADJECTIVES.len())],
        NOUNS[rng.gen_range(0..NOUNS.len())],
        TITLES[rng.gen_range(0..TITLES.len())]
    )
}

#[derive(Debug, Clone, Copy)]
struct SpawnLayout {
    group_size: usize,
    group_spacing: f32,
}

impl SpawnLayout {
    fn disabled() -> Self {
        Self {
            group_size: 0,
            group_spacing: 0.0,
        }
    }

    fn new(group_size: usize, group_spacing: f32) -> Self {
        if group_size == 0 {
            Self::disabled()
        } else {
            Self {
                group_size,
                group_spacing,
            }
        }
    }

    fn base_for_client(self, index: usize) -> [f32; 3] {
        let mut rng = rand::thread_rng();
        let group_offset = if self.group_size == 0 {
            0.0
        } else {
            (index / self.group_size) as f32 * self.group_spacing
        };
        [
            group_offset + rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
        ]
    }
}

async fn movement_workers(clients: Arc<Mutex<Vec<Arc<BasisClient>>>>, shutdown: Arc<AtomicBool>) {
    let initial_len = clients.lock().await.len();
    let worker_count = num_cpus::get().max(1).min(initial_len.max(1));
    let start = SystemTime::now();
    for worker in 0..worker_count {
        let clients = clients.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            time::sleep(Duration::from_millis((worker * 12) as u64)).await;
            let mut ticker = time::interval(MOVEMENT_INTERVAL);
            loop {
                ticker.tick().await;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let snapshot = clients.lock().await.clone();
                let mut idx = worker;
                while idx < snapshot.len() {
                    let client = &snapshot[idx];
                    if client.connected.load(Ordering::Relaxed) {
                        let sequence = client.movement_sequence.fetch_add(1, Ordering::SeqCst);
                        let mut pose = client.pose.lock().await;
                        let packet = build_movement_packet(sequence, &mut pose, start);
                        drop(pose);
                        if let Err(err) = client
                            .send_unreliable(channels::PLAYER_AVATAR_HIGH, &packet)
                            .await
                        {
                            trace!("movement send failed for {}: {err}", client.index);
                        }
                    }
                    idx += worker_count;
                }
            }
        });
    }
}

async fn random_reconnect_loop(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    config: Config,
    shutdown: Arc<AtomicBool>,
    spawn_layout: SpawnLayout,
) {
    loop {
        let minutes = rand::thread_rng().gen_range(1..=20);
        time::sleep(Duration::from_secs(minutes * 60)).await;
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let len = clients.lock().await.len();
        if len == 0 {
            continue;
        }
        let idx = rand::thread_rng().gen_range(0..len);
        let old = { clients.lock().await[idx].clone() };
        old.disconnect().await;
        time::sleep(Duration::from_secs(3)).await;
        let spawn_base = spawn_layout.base_for_client(idx);
        let result = match ReadyMessage::new(&config, spawn_base) {
            Ok(ready) => BasisClient::start(idx, &config, ready, spawn_base).await,
            Err(err) => Err(err),
        };
        match result {
            Ok(new_client) => {
                clients.lock().await[idx] = new_client;
                info!("reconnected client {idx}");
            }
            Err(err) => warn!("failed to reconnect client {idx}: {err}"),
        }
    }
}

async fn sleep_or_shutdown(duration: Duration, shutdown: &AtomicBool) {
    let deadline = time::Instant::now() + duration;
    loop {
        if shutdown.load(Ordering::Relaxed) || time::Instant::now() >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        time::sleep(remaining.min(Duration::from_millis(50))).await;
    }
}

async fn wait_for_batch_connected(
    clients: &[Arc<BasisClient>],
    timeout: Duration,
    shutdown: &AtomicBool,
) -> usize {
    let deadline = time::Instant::now() + timeout;
    loop {
        let connected = clients
            .iter()
            .filter(|client| client.connected.load(Ordering::Relaxed))
            .count();
        if connected == clients.len()
            || shutdown.load(Ordering::Relaxed)
            || time::Instant::now() >= deadline
        {
            return connected;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "basis_rust_client=info,info".to_string()),
        )
        .init();

    let args = Args::parse();
    let mut config = Config::load_or_create(&args.config)?;
    if let Some(ip) = args.ip {
        config.ip = ip;
    }
    if let Some(port) = args.port {
        config.port = port;
    }
    if let Some(clients) = args.clients {
        config.client_count = clients;
    }

    info!(
        "starting {} clients against {}:{}",
        config.client_count, config.ip, config.port
    );
    let spawn_layout = SpawnLayout::new(args.spawn_group_size, args.spawn_group_spacing);
    if args.spawn_group_size > 0 {
        info!(
            "spawning clients in groups of {} spaced {:.1} units apart",
            args.spawn_group_size, args.spawn_group_spacing
        );
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = shutdown.clone();
    tokio::spawn(async move {
        if let Err(err) = tokio::signal::ctrl_c().await {
            warn!("failed to listen for Ctrl+C: {err}");
            return;
        }
        info!("shutdown requested");
        signal_shutdown.store(true, Ordering::SeqCst);
    });

    let connect_batch_size = args.connect_batch_size.max(1);
    let connect_timeout = Duration::from_millis(args.connect_timeout_ms);
    let connect_batch_delay = Duration::from_millis(args.connect_batch_delay_ms);
    let mut started = Vec::with_capacity(config.client_count);
    let mut index = 0;
    while index < config.client_count {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let batch_end = (index + connect_batch_size).min(config.client_count);
        let batch_start = index;
        let mut batch_clients = Vec::with_capacity(batch_end - batch_start);
        for client_index in batch_start..batch_end {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            let spawn_base = spawn_layout.base_for_client(client_index);
            let ready = ReadyMessage::new(&config, spawn_base)?;
            match BasisClient::start(client_index, &config, ready, spawn_base).await {
                Ok(client) => {
                    batch_clients.push(client.clone());
                    started.push(client);
                }
                Err(err) => error!("failed to start client {client_index}: {err}"),
            }
        }

        let connected_in_batch =
            wait_for_batch_connected(&batch_clients, connect_timeout, &shutdown).await;
        if connected_in_batch < batch_clients.len() {
            for client in &batch_clients {
                if !client.connected.load(Ordering::Relaxed) {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    client.in_use.store(false, Ordering::SeqCst);
                    client.connected.store(false, Ordering::SeqCst);
                    warn!(
                        "client {} did not connect within {}ms",
                        client.index, args.connect_timeout_ms
                    );
                }
            }
        }

        info!(
            "connection batch {}-{} accepted {}/{} clients",
            batch_start,
            batch_end.saturating_sub(1),
            connected_in_batch,
            batch_clients.len()
        );

        index = batch_end;
        if index < config.client_count
            && !connect_batch_delay.is_zero()
            && !shutdown.load(Ordering::Relaxed)
        {
            sleep_or_shutdown(connect_batch_delay, &shutdown).await;
        }
    }

    let managed_clients = Arc::new(Mutex::new(started));
    if !args.no_movement && !shutdown.load(Ordering::Relaxed) {
        movement_workers(managed_clients.clone(), shutdown.clone()).await;
    }
    if !args.no_reconnect && !shutdown.load(Ordering::Relaxed) {
        tokio::spawn(random_reconnect_loop(
            managed_clients.clone(),
            config.clone(),
            shutdown.clone(),
            spawn_layout,
        ));
    }

    if !shutdown.load(Ordering::Relaxed) {
        if let Some(duration_secs) = args.duration_secs {
            sleep_or_shutdown(Duration::from_secs(duration_secs), &shutdown).await;
        } else {
            while !shutdown.load(Ordering::Relaxed) {
                time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
    shutdown.store(true, Ordering::SeqCst);
    info!("shutting down clients");
    for client in managed_clients.lock().await.iter() {
        client.disconnect().await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signature;
    use flate2::read::DeflateDecoder;
    use std::io::Read;

    #[test]
    fn connection_payload_starts_with_version_auth_and_ready() {
        let config = Config::default();
        let ready = ReadyMessage::new(&config, [0.0, 0.0, 0.0]).unwrap();
        let payload = build_connection_payload(&config, &ready);
        assert_eq!(&payload[0..2], &[0x20, 0x00]);
        let auth_len = u16::from_le_bytes([payload[2], payload[3]]) as usize;
        assert_eq!(&payload[4..4 + auth_len], b"default_password");
        assert!(payload.len() > 4 + auth_len);
    }

    #[test]
    fn metadata_empty_fields_serialize_as_failure() {
        let mut writer = NetWriter::default();
        ClientMetaDataMessage {
            player_uuid: String::new(),
            player_display_name: String::new(),
            player_platform: String::new(),
        }
        .serialize(&mut writer);
        let bytes = writer.into_vec();
        assert_eq!(read_lnl_string(&bytes, 0).0, "Failure");
        let (_, next) = read_lnl_string(&bytes, 0);
        assert_eq!(read_lnl_string(&bytes, next).0, "Failure");
    }

    #[test]
    fn avatar_network_load_deflates_raw_len_strings() {
        let encoded = encode_avatar_network_load("http://localhost/avatar", "pw").unwrap();
        let mut decoder = DeflateDecoder::new(encoded.as_slice());
        let mut raw = Vec::new();
        decoder.read_to_end(&mut raw).unwrap();
        let (url, next) = read_raw_len_string(&raw, 0);
        let (pw, _) = read_raw_len_string(&raw, next);
        assert_eq!(url, "http://localhost/avatar");
        assert_eq!(pw, "pw");
    }

    #[test]
    fn avatar_change_uses_avatar_password_not_login_password() {
        let config = Config {
            password: "server-login-password".to_string(),
            avatar_password: "avatar-unlock-password".to_string(),
            ..Config::default()
        };
        let message = ClientAvatarChangeMessage::new(&config).unwrap();
        let mut decoder = DeflateDecoder::new(message.byte_array.as_slice());
        let mut raw = Vec::new();
        decoder.read_to_end(&mut raw).unwrap();
        let (_, next) = read_raw_len_string(&raw, 0);
        let (unlock_password, _) = read_raw_len_string(&raw, next);
        assert_eq!(unlock_password, "avatar-unlock-password");
        assert_ne!(unlock_password, "server-login-password");
    }

    #[test]
    fn high_quality_payload_and_movement_packet_sizes_match() {
        let mut pose = PoseState::new_random();
        let payload = pose.high_quality_payload(0.0);
        assert_eq!(payload.len(), 182);
        assert_eq!(u16::from_le_bytes([payload[160], payload[161]]), 434);
        assert_eq!(&payload[169..175], &[0, 0, 0, 0, 0, 0]);
        let packet = build_movement_packet(7, &mut pose, SystemTime::now());
        assert_eq!(packet.len(), 183);
        assert_eq!(packet[0], 7);
    }

    #[test]
    fn spawn_layout_offsets_groups_by_spacing() {
        let layout = SpawnLayout::new(3, 1000.0);
        let first = layout.base_for_client(0);
        let same_group = layout.base_for_client(2);
        let next_group = layout.base_for_client(3);
        let later_group = layout.base_for_client(8);

        assert!(first[0].abs() <= 0.25);
        assert!(same_group[0].abs() <= 0.25);
        assert!((999.75..=1000.25).contains(&next_group[0]));
        assert!((1999.75..=2000.25).contains(&later_group[0]));
    }

    #[test]
    fn initial_ready_pose_uses_spawn_base() {
        let ready = ReadyMessage::new(&Config::default(), [1000.0, 2.0, -3.0]).unwrap();
        let payload = &ready.local_avatar_sync.payload;
        let x = f32::from_le_bytes(payload[0..4].try_into().unwrap());
        let y = f32::from_le_bytes(payload[4..8].try_into().unwrap());
        let z = f32::from_le_bytes(payload[8..12].try_into().unwrap());

        assert!((999.75..=1000.25).contains(&x));
        assert!((1.75..=2.25).contains(&y));
        assert!((-3.25..=-2.75).contains(&z));
    }

    #[test]
    fn sequence_wraps_from_255_to_0() {
        let seq = AtomicU8::new(255);
        assert_eq!(seq.fetch_add(1, Ordering::SeqCst), 255);
        assert_eq!(seq.fetch_add(1, Ordering::SeqCst), 0);
    }

    #[test]
    fn did_response_contains_signature_and_na_fragment() {
        let identity = Identity::random();
        let response = identity.response_payload(b"challenge").unwrap();
        let sig_len = u16::from_le_bytes([response[0], response[1]]) as usize;
        assert_eq!(sig_len, 64);
        let frag_start = 2 + sig_len;
        let frag_len =
            u16::from_le_bytes([response[frag_start], response[frag_start + 1]]) as usize;
        assert_eq!(&response[frag_start + 2..frag_start + 2 + frag_len], b"N/A");
        let sig = Signature::from_slice(&response[2..2 + sig_len]).unwrap();
        identity.verifying_key.verify(b"challenge", &sig).unwrap();
    }

    #[test]
    fn duplicate_start_is_rejected_statefully() {
        let flag = AtomicBool::new(false);
        assert!(!flag.swap(true, Ordering::SeqCst));
        assert!(flag.swap(true, Ordering::SeqCst));
    }

    #[test]
    fn transport_mappings_are_shared_with_server_transport() {
        assert_eq!(
            PacketProperty::from_byte(PacketProperty::ConnectRequest as u8 | (2 << 5)),
            Some(PacketProperty::ConnectRequest)
        );
        assert_eq!(
            DeliveryMethod::channel_id(channels::AUTH_IDENTITY, DeliveryMethod::ReliableOrdered),
            2
        );
        assert_eq!(
            DeliveryMethod::from_channel_id(DeliveryMethod::channel_id(
                channels::PLAYER_AVATAR_HIGH,
                DeliveryMethod::ReliableUnordered
            )),
            DeliveryMethod::ReliableUnordered
        );
        assert_eq!(
            DeliveryMethod::from_channel_id(DeliveryMethod::channel_id(
                channels::PLAYER_AVATAR_HIGH,
                DeliveryMethod::Sequenced
            )),
            DeliveryMethod::Sequenced
        );
    }

    fn read_lnl_string(bytes: &[u8], offset: usize) -> (String, usize) {
        let len_plus = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        if len_plus == 0 {
            return (String::new(), offset + 2);
        }
        let len = len_plus - 1;
        (
            String::from_utf8(bytes[offset + 2..offset + 2 + len].to_vec()).unwrap(),
            offset + 2 + len,
        )
    }

    fn read_raw_len_string(bytes: &[u8], offset: usize) -> (String, usize) {
        let len = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        (
            String::from_utf8(bytes[offset + 2..offset + 2 + len].to_vec()).unwrap(),
            offset + 2 + len,
        )
    }
}
