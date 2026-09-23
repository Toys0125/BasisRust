use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap, HashSet, VecDeque},
    io::Write,
    net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
    path::{Path, PathBuf},
    process::Command,
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU8, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(target_os = "linux")]
use std::sync::mpsc as std_mpsc;

use anyhow::{anyhow, Context, Result};
use basis_protocol::{
    application::{NetworkApplication, DEFAULT_COMPANY_NAME, DEFAULT_PRODUCT_NAME},
    avatar::{compress_scale, write_neutral_rotation_region, BitQuality as ProtocolBitQuality},
    channels,
    io::NetWriter as ProtocolNetWriter,
    messages::{
        BasisSerialize, ClientAvatarChangeMessage as ProtocolClientAvatarChangeMessage,
        ClientMetaDataMessage as ProtocolClientMetaDataMessage,
    },
    version::LITENETLIB_PROTOCOL_ID,
    version::SERVER_VERSION,
};
#[cfg(test)]
use basis_protocol::{io::NetReader as ProtocolNetReader, messages::BasisDeserialize};
use basis_transport::{DeliveryMethod, PacketProperty};
use clap::Parser;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use flate2::{write::DeflateEncoder, Compression};
use rand::{rngs::OsRng, Rng, RngCore};
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, Socket, Type};
use tokio::{
    io::{self, AsyncBufReadExt, BufReader},
    net::UdpSocket,
    sync::{mpsc, Mutex, Notify},
    time,
};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

const DEFAULT_WINDOW_SIZE: usize = 128;
const MAX_SEQUENCE: u16 = 32768;
const LITENETLIB_INITIAL_MTU: usize = 1024;
const LITENETLIB_CHANNELED_HEADER_SIZE: usize = 4;
const LITENETLIB_FRAGMENT_HEADER_SIZE: usize = 6;
const LITENETLIB_FRAGMENTED_HEADER_SIZE: usize =
    LITENETLIB_CHANNELED_HEADER_SIZE + LITENETLIB_FRAGMENT_HEADER_SIZE;
const RELIABLE_FRAGMENT_PAYLOAD_SIZE: usize =
    LITENETLIB_INITIAL_MTU - LITENETLIB_FRAGMENTED_HEADER_SIZE;
const MAINTENANCE_INTERVAL: Duration = Duration::from_millis(100);
const PING_INTERVAL_TICKS: usize = 15;
const SNAPSHOT_REFRESH_TICKS: usize = 10;
const INITIAL_START_ATTEMPTS: usize = 3;
const MOVEMENT_INTERVAL: Duration = Duration::from_millis(90);
const SOCKET_BUFFER_SIZE: usize = 32 * 1024 * 1024;
const SOCKET_TTL: u32 = 255;
const DEFAULT_VOICE_AUDIO_FOLDER: &str = "audio";
const DEFAULT_VOICE_SPEAKER_PERCENT: u8 = 10;
const DEFAULT_VOICE_HEARING_DISTANCE: f32 = 25.0;
const DEFAULT_VOICE_FRAME_DURATION_MS: u64 = 20;
const MAX_UNITY_VOICE_FRAME_DURATION_MS: u64 = 40;
const MAX_VOICE_PACKET_BYTES: usize = 1200;
#[cfg(target_os = "linux")]
static SHARED_RECEIVE_ACTIVE: AtomicBool = AtomicBool::new(false);

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
    #[arg(long, default_value_t = 60)]
    reconnect_min_secs: u64,
    #[arg(long, default_value_t = 1200)]
    reconnect_max_secs: u64,
    #[arg(long)]
    no_movement: bool,
    /// Use synchronized worker batches instead of the default per-client randomized cadence.
    #[arg(long)]
    sync_batching: bool,
    /// Maximum movement interval jitter in percent when randomized cadence is active.
    #[arg(long, default_value_t = 10)]
    movement_jitter_percent: u8,
    /// Maximum voice packet timing jitter in percent when randomized cadence is active.
    #[arg(long, default_value_t = 5)]
    voice_jitter_percent: u8,
    #[arg(long)]
    duration_secs: Option<u64>,
    #[arg(long, default_value_t = 100)]
    connect_batch_size: usize,
    #[arg(long, default_value_t = 250)]
    connect_batch_delay_ms: u64,
    /// Number of clients to disconnect at a time during graceful shutdown.
    #[arg(long, default_value_t = 100)]
    quit_batch_size: usize,
    /// Delay between graceful shutdown disconnect batches.
    #[arg(long, default_value_t = 250)]
    quit_batch_delay_ms: u64,
    #[arg(long, default_value_t = 5000)]
    connect_timeout_ms: u64,
    #[arg(long, default_value_t = 0)]
    spawn_group_size: usize,
    #[arg(long, default_value_t = 1000.0)]
    spawn_group_spacing: f32,
    #[arg(long)]
    voice: bool,
    #[arg(long)]
    voice_audio_folder: Option<PathBuf>,
    #[arg(long)]
    voice_speaker_percent: Option<u8>,
    #[arg(long)]
    voice_hearing_distance: Option<f32>,
    #[arg(long)]
    voice_frame_duration_ms: Option<u64>,
    #[arg(long)]
    no_voice_reencode: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "Configuration", rename_all = "PascalCase")]
struct Config {
    password: String,
    ip: String,
    port: u16,
    client_count: usize,
    company_name: String,
    product_name: String,
    avatar_password: String,
    avatar_url: String,
    avatar_load_mode: u8,
    voice_enabled: bool,
    voice_audio_folder: String,
    voice_speaker_percent: u8,
    voice_hearing_distance: f32,
    voice_frame_duration_ms: u64,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename = "Configuration", rename_all = "PascalCase")]
struct RawConfig {
    password: Option<String>,
    ip: Option<String>,
    port: Option<String>,
    client_count: Option<String>,
    company_name: Option<String>,
    product_name: Option<String>,
    avatar_password: Option<String>,
    avatar_url: Option<String>,
    avatar_load_mode: Option<String>,
    voice_enabled: Option<String>,
    voice_audio_folder: Option<String>,
    voice_speaker_percent: Option<String>,
    voice_hearing_distance: Option<String>,
    voice_frame_duration_ms: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            password: "default_password".to_string(),
            ip: "localhost".to_string(),
            port: 4296,
            client_count: 250,
            company_name: DEFAULT_COMPANY_NAME.to_string(),
            product_name: DEFAULT_PRODUCT_NAME.to_string(),
            avatar_password: "N/A".to_string(),
            avatar_url: "LoadingAvatar".to_string(),
            avatar_load_mode: 1,
            voice_enabled: false,
            voice_audio_folder: DEFAULT_VOICE_AUDIO_FOLDER.to_string(),
            voice_speaker_percent: DEFAULT_VOICE_SPEAKER_PERCENT,
            voice_hearing_distance: DEFAULT_VOICE_HEARING_DISTANCE,
            voice_frame_duration_ms: DEFAULT_VOICE_FRAME_DURATION_MS,
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
            company_name: raw.company_name.unwrap_or(defaults.company_name),
            product_name: raw.product_name.unwrap_or(defaults.product_name),
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
            voice_enabled: parse_or_default(
                raw.voice_enabled,
                defaults.voice_enabled,
                "VoiceEnabled",
            ),
            voice_audio_folder: raw
                .voice_audio_folder
                .filter(|s| !s.is_empty())
                .unwrap_or(defaults.voice_audio_folder),
            voice_speaker_percent: parse_or_default(
                raw.voice_speaker_percent,
                defaults.voice_speaker_percent,
                "VoiceSpeakerPercent",
            )
            .min(100),
            voice_hearing_distance: sanitize_voice_distance(parse_or_default(
                raw.voice_hearing_distance,
                defaults.voice_hearing_distance,
                "VoiceHearingDistance",
            )),
            voice_frame_duration_ms: sanitize_voice_frame_duration(parse_or_default(
                raw.voice_frame_duration_ms,
                defaults.voice_frame_duration_ms,
                "VoiceFrameDurationMs",
            )),
        }
    }

    fn to_pretty_xml(&self) -> String {
        format!(
            "<Configuration>\n  <Password>{}</Password>\n  <Ip>{}</Ip>\n  <Port>{}</Port>\n  <ClientCount>{}</ClientCount>\n  <CompanyName>{}</CompanyName>\n  <ProductName>{}</ProductName>\n  <AvatarPassword>{}</AvatarPassword>\n  <AvatarUrl>{}</AvatarUrl>\n  <AvatarLoadMode>{}</AvatarLoadMode>\n  <VoiceEnabled>{}</VoiceEnabled>\n  <VoiceAudioFolder>{}</VoiceAudioFolder>\n  <VoiceSpeakerPercent>{}</VoiceSpeakerPercent>\n  <VoiceHearingDistance>{}</VoiceHearingDistance>\n  <VoiceFrameDurationMs>{}</VoiceFrameDurationMs>\n</Configuration>\n",
            escape_xml(&self.password),
            escape_xml(&self.ip),
            self.port,
            self.client_count,
            escape_xml(&self.company_name),
            escape_xml(&self.product_name),
            escape_xml(&self.avatar_password),
            escape_xml(&self.avatar_url),
            self.avatar_load_mode,
            self.voice_enabled,
            escape_xml(&self.voice_audio_folder),
            self.voice_speaker_percent,
            self.voice_hearing_distance,
            self.voice_frame_duration_ms
        )
    }
}

fn sanitize_voice_distance(value: f32) -> f32 {
    if value.is_finite() && value >= 0.0 {
        value
    } else {
        warn!(
            "invalid voice hearing distance {value}; using default {}",
            DEFAULT_VOICE_HEARING_DISTANCE
        );
        DEFAULT_VOICE_HEARING_DISTANCE
    }
}

fn sanitize_voice_frame_duration(value: u64) -> u64 {
    if value > 0 {
        value
    } else {
        warn!(
            "invalid voice frame duration {value}; using default {}ms",
            DEFAULT_VOICE_FRAME_DURATION_MS
        );
        DEFAULT_VOICE_FRAME_DURATION_MS
    }
}

fn resolve_relative_to_config(config_path: &Path, configured_path: &str) -> String {
    let path = PathBuf::from(configured_path);
    if path.is_absolute() {
        return path.to_string_lossy().to_string();
    }
    let base = config_path.parent().unwrap_or_else(|| Path::new("."));
    base.join(path).to_string_lossy().to_string()
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
                warn!("invalid config field {field}={text:?}; using default");
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

    fn put_bytes(&mut self, value: &[u8]) {
        self.data.extend_from_slice(value);
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
        let message = ProtocolClientMetaDataMessage {
            player_uuid: non_empty_or_failure(&self.player_uuid).to_owned(),
            player_display_name: non_empty_or_failure(&self.player_display_name).to_owned(),
            player_platform: non_empty_or_failure(&self.player_platform).to_owned(),
        };
        let mut encoded = ProtocolNetWriter::new();
        message.serialize(&mut encoded);
        writer.put_bytes(encoded.as_slice());
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
        let message = ProtocolClientAvatarChangeMessage {
            load_mode: self.load_mode,
            byte_array: self.byte_array.clone(),
            local_avatar_index: self.local_avatar_index,
            arm_scale: 1.0,
            leg_scale: 1.0,
            torso_scale: 1.0,
        };
        let mut encoded = ProtocolNetWriter::new();
        message.serialize(&mut encoded);
        writer.put_bytes(encoded.as_slice());
    }
}

fn encode_avatar_network_load(url: &str, unlock_password: &str) -> Result<Vec<u8>> {
    let mut raw = NetWriter::with_capacity(url.len() + unlock_password.len() + 6);
    raw.put_raw_len_string(url);
    raw.put_raw_len_string(unlock_password);
    // Current Basis clients append an optional content version tag. The built-in loading avatar has
    // no external content version, so the canonical value is an empty string.
    raw.put_raw_len_string("");

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
            Self::VeryLow => 74,
            Self::Low => 83,
            Self::Medium => 97,
            Self::High => 159,
        }
    }

    fn rotation_len(self) -> usize {
        match self {
            Self::VeryLow => 44,
            Self::Low => 53,
            Self::Medium => 67,
            Self::High => 94,
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
    datagram: Vec<u8>,
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
        // Final hot-path datagram layout:
        // [LiteNetLib Unreliable][Basis channel][movement sequence][159-byte High payload].
        // Keeping the complete datagram here avoids allocating/copying twice per movement send.
        let mut datagram = vec![0; 3 + BitQuality::High.payload_len()];
        datagram[0] = PacketProperty::Unreliable as u8;
        datagram[1] = channels::PLAYER_AVATAR_HIGH;
        initialize_static_synthetic_payload(&mut datagram[3..]);
        Self { base, datagram }
    }

    fn drift(&mut self) {
        let mut rng = rand::thread_rng();
        self.base[0] += rng.gen_range(-0.25..=0.25);
        self.base[1] += rng.gen_range(-0.25..=0.25);
        self.base[2] += rng.gen_range(-0.25..=0.25);
    }

    fn position(&self) -> [f32; 3] {
        self.base
    }

    fn update_dynamic_payload(&mut self, elapsed_secs: f32) {
        self.drift();
        let payload = &mut self.datagram[3..];
        payload[0..3].copy_from_slice(&encode_axis_mm(self.base[0]));
        payload[3..6].copy_from_slice(&encode_axis_mm(self.base[1] + elapsed_secs.sin() * 0.015));
        payload[6..9].copy_from_slice(&encode_axis_mm(self.base[2]));
    }

    fn high_quality_payload(&mut self, elapsed_secs: f32) -> Vec<u8> {
        self.update_dynamic_payload(elapsed_secs);
        self.datagram[3..].to_vec()
    }

    fn write_movement_datagram(&mut self, sequence: u8, start: SystemTime) -> &[u8] {
        let elapsed = start.elapsed().unwrap_or_default().as_secs_f32();
        self.update_dynamic_payload(elapsed);
        self.datagram[2] = sequence;
        &self.datagram
    }
}

fn initialize_static_synthetic_payload(payload: &mut [u8]) {
    debug_assert_eq!(payload.len(), BitQuality::High.payload_len());
    write_neutral_rotation_region(payload, ProtocolBitQuality::High)
        .expect("high-quality synthetic pose has the protocol-defined payload size");

    let tail = 9 + BitQuality::High.rotation_len();
    payload[tail..tail + 2].copy_from_slice(&compress_scale(1.0).to_le_bytes());
    let identity = smallest_three_quaternion([0.0, 0.0, 0.0, 1.0]);
    payload[tail + 2..tail + 9].copy_from_slice(&identity);
    payload[tail + 9..tail + 14].fill(0);
    payload[tail + 14..tail + 21].copy_from_slice(&identity);
    payload[tail + 21..].fill(0);
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
    let inv_sqrt_2 = std::f32::consts::FRAC_1_SQRT_2;
    for (i, component) in q.iter().copied().enumerate() {
        if i == largest {
            continue;
        }
        let value = (component * sign).clamp(-inv_sqrt_2, inv_sqrt_2);
        let quantized =
            (((value + inv_sqrt_2) / std::f32::consts::SQRT_2) * 65535.0).round() as u16;
        out[offset..offset + 2].copy_from_slice(&quantized.to_le_bytes());
        offset += 2;
    }
    out
}

fn encode_axis_mm(meters: f32) -> [u8; 3] {
    const LIMIT: i32 = (1 << 23) - 1;
    let mm_f = meters * 1000.0;
    let mm = if mm_f.is_nan() {
        0
    } else if mm_f >= LIMIT as f32 {
        LIMIT
    } else if mm_f <= -(LIMIT as f32) {
        -LIMIT
    } else {
        mm_f.round() as i32
    };
    [mm as u8, (mm >> 8) as u8, (mm >> 16) as u8]
}

fn build_connection_payload(config: &Config, ready: &ReadyMessage) -> Vec<u8> {
    let auth = config.password.as_bytes();
    let mut writer = NetWriter::with_capacity(512);
    writer.put_u16(SERVER_VERSION);
    writer.put_bytes(&NetworkApplication::encode(
        &config.company_name,
        &config.product_name,
    ));
    put_bytes_message(&mut writer, auth);
    ready.serialize(&mut writer);
    writer.into_vec()
}

#[cfg(test)]
fn build_movement_packet(sequence: u8, pose: &mut PoseState, start: SystemTime) -> Vec<u8> {
    // Test/helper form excludes the LiteNetLib property/channel bytes and matches the Basis
    // movement payload handed to send_unreliable(). The production hot path sends the reusable
    // complete datagram directly instead.
    pose.write_movement_datagram(sequence, start)[2..].to_vec()
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

    fn did_key(&self) -> String {
        // did:key for Ed25519 is multibase(base58-btc(multicodec-varint(0xED) || pubkey)).
        // Unsigned LEB128 encoding of the Ed25519 multicodec id 0xED is [0xED, 0x01].
        let mut multicodec = [0u8; 34];
        multicodec[0] = 0xED;
        multicodec[1] = 0x01;
        multicodec[2..].copy_from_slice(&self.verifying_key.to_bytes());
        format!("did:key:z{}", bs58::encode(multicodec).into_string())
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

#[derive(Clone)]
struct MaintenanceOptions {
    shared: bool,
    refresh: Arc<Notify>,
}

#[derive(Clone, Copy)]
struct ConnectOptions {
    batch_size: usize,
    batch_delay: Duration,
    timeout: Duration,
    shared_maintenance: bool,
}

#[derive(Debug, Clone)]
struct ReliableSend {
    channel_id: u8,
    sequence: u16,
    bytes: Vec<u8>,
    last_sent: Option<SystemTime>,
}

#[derive(Debug)]
struct ReliableReceiveState {
    seen: [bool; 256],
    highest: [u16; 256],
    windows: [u128; 256],
}

impl Default for ReliableReceiveState {
    fn default() -> Self {
        Self {
            seen: [false; 256],
            highest: [0; 256],
            windows: [0; 256],
        }
    }
}

impl ReliableReceiveState {
    fn mark_new(&mut self, channel_id: u8, sequence: u16) -> bool {
        let index = channel_id as usize;
        if !self.seen[index] {
            self.seen[index] = true;
            self.highest[index] = sequence;
            self.windows[index] = 1;
            return true;
        }

        let relative = relative_sequence(sequence, self.highest[index]);
        if relative > 0 {
            let advance = relative as usize;
            self.windows[index] = if advance >= DEFAULT_WINDOW_SIZE {
                1
            } else {
                (self.windows[index] << advance) | 1
            };
            self.highest[index] = sequence;
            return true;
        }

        let age = (-relative) as usize;
        if age >= DEFAULT_WINDOW_SIZE {
            return false;
        }
        let bit = 1u128 << age;
        if self.windows[index] & bit != 0 {
            return false;
        }
        self.windows[index] |= bit;
        true
    }
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
    intentional_reconnect: AtomicBool,
    movement_sequence: AtomicU8,
    voice_sequence: AtomicU8,
    reliable_sequences: [AtomicU16; 256],
    fragment_id: AtomicU16,
    ping_sequence: AtomicU16,
    pending_reliable: Mutex<VecDeque<ReliableSend>>,
    pending_reliable_active: AtomicBool,
    shared_receive: AtomicBool,
    shared_receive_eligible: AtomicBool,
    receive_shutdown: Notify,
    received_reliable: StdMutex<ReliableReceiveState>,
    pose: Mutex<PoseState>,
    identity: Identity,
}

impl BasisClient {
    async fn start(
        index: usize,
        config: &Config,
        mut ready: ReadyMessage,
        spawn_base: [f32; 3],
        shared_maintenance_enabled: bool,
    ) -> Result<Arc<Self>> {
        let server_addr = resolve_addr(&config.ip, config.port)?;
        let socket = bind_udp_socket(any_local_addr(server_addr))?;
        socket.connect(server_addr).await?;
        let socket = Arc::new(socket);
        let connect_time = dotnet_utc_ticks();
        let local_peer_id = index as i32;
        let identity = Identity::random();
        ready.metadata.player_uuid = identity.did_key();
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
            intentional_reconnect: AtomicBool::new(false),
            movement_sequence: AtomicU8::new(0),
            voice_sequence: AtomicU8::new(0),
            reliable_sequences: std::array::from_fn(|_| AtomicU16::new(0)),
            fragment_id: AtomicU16::new(0),
            ping_sequence: AtomicU16::new(0),
            pending_reliable: Mutex::new(VecDeque::new()),
            pending_reliable_active: AtomicBool::new(false),
            shared_receive: AtomicBool::new(false),
            shared_receive_eligible: AtomicBool::new(false),
            receive_shutdown: Notify::new(),
            received_reliable: StdMutex::new(ReliableReceiveState::default()),
            pose: Mutex::new(PoseState::new_at(spawn_base)),
            identity,
        });

        client
            .start_client(config, &ready, shared_maintenance_enabled)
            .await?;
        Ok(client)
    }

    async fn start_client(
        self: &Arc<Self>,
        config: &Config,
        ready: &ReadyMessage,
        shared_maintenance_enabled: bool,
    ) -> Result<()> {
        if self.in_use.swap(true, Ordering::SeqCst) {
            error!("Call Shutdown First!");
            return Err(anyhow!("Call Shutdown First!"));
        }
        let payload = build_connection_payload(config, ready);
        let request = self.make_connect_request(&payload);
        self.send_connected(&request).await?;
        debug!(
            "client {} sent connect request to {}",
            self.index, self.server_addr
        );

        // Every freshly-created client starts with a dedicated receive task so reconnect/auth
        // always has a path for the DID challenge. On Linux, the shared receiver takes over only
        // after the client has observed post-auth server traffic and marks itself eligible.
        let client = self.clone();
        tokio::spawn(async move {
            let index = client.index;
            if let Err(err) = client.receive_loop().await {
                debug!("client {index} receive loop ended: {err}");
            }
        });

        if !shared_maintenance_enabled {
            let client = self.clone();
            tokio::spawn(async move {
                client.maintenance_loop().await;
            });
        }

        Ok(())
    }

    async fn send_connected(&self, bytes: &[u8]) -> Result<()> {
        self.socket.send(bytes).await?;
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

    fn stop_receive_loop(&self) {
        // A UDP recv future otherwise keeps this Arc (and its socket FD) alive indefinitely when
        // a connection attempt is replaced without receiving a final server packet.
        self.receive_shutdown.notify_one();
    }

    fn deactivate(&self) {
        self.in_use.store(false, Ordering::SeqCst);
        self.connected.store(false, Ordering::SeqCst);
        self.stop_receive_loop();
    }

    async fn disconnect(&self) {
        self.deactivate();
        info!("client {} called disconnect", self.index);
        let mut packet = NetWriter::with_capacity(9);
        packet.put_u8(PacketProperty::Disconnect as u8 | (self.connection_number << 5));
        packet.put_i64(self.connect_time);
        let _ = self.send_connected(&packet.into_vec()).await;
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
        self.send_connected(&packet).await?;
        Ok(())
    }

    async fn current_position(&self) -> [f32; 3] {
        self.pose.lock().await.position()
    }

    async fn remote_peer_id(&self) -> Option<u16> {
        let id = *self.remote_peer_id.lock().await;
        id.and_then(|id| {
            if (0..=u16::MAX as i32).contains(&id) {
                Some(id as u16)
            } else {
                None
            }
        })
    }

    fn next_reliable_sequence(&self, channel_id: u8) -> u16 {
        self.reliable_sequences[channel_id as usize].fetch_add(1, Ordering::SeqCst) % MAX_SEQUENCE
    }

    async fn mark_reliable_sent(&self, sent: &ReliableSend, sent_at: SystemTime) {
        let mut pending = self.pending_reliable.lock().await;
        if let Some(item) = pending.iter_mut().find(|item| {
            item.channel_id == sent.channel_id
                && item.sequence == sent.sequence
                && item.bytes == sent.bytes
        }) {
            item.last_sent = Some(sent_at);
        }
    }

    async fn send_reliable_ordered(&self, channel: u8, payload: &[u8]) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }

        let channel_id = DeliveryMethod::channel_id(channel, DeliveryMethod::ReliableOrdered);
        let mut packets = Vec::new();
        if payload.len() + LITENETLIB_CHANNELED_HEADER_SIZE <= LITENETLIB_INITIAL_MTU {
            let sequence = self.next_reliable_sequence(channel_id);
            let mut packet = Vec::with_capacity(LITENETLIB_CHANNELED_HEADER_SIZE + payload.len());
            packet.push(PacketProperty::Channeled as u8 | (self.connection_number << 5));
            packet.extend_from_slice(&sequence.to_le_bytes());
            packet.push(channel_id);
            packet.extend_from_slice(payload);
            packets.push(ReliableSend {
                channel_id,
                sequence,
                bytes: packet,
                last_sent: None,
            });
        } else {
            let total_fragments = payload.len().div_ceil(RELIABLE_FRAGMENT_PAYLOAD_SIZE);
            if total_fragments > u16::MAX as usize {
                return Err(anyhow!(
                    "reliable payload requires {} fragments, exceeding LiteNetLib limit",
                    total_fragments
                ));
            }

            let fragment_id = self
                .fragment_id
                .fetch_add(1, Ordering::SeqCst)
                .wrapping_add(1);
            packets.reserve(total_fragments);
            for (part, chunk) in payload.chunks(RELIABLE_FRAGMENT_PAYLOAD_SIZE).enumerate() {
                let sequence = self.next_reliable_sequence(channel_id);
                let mut packet =
                    Vec::with_capacity(LITENETLIB_FRAGMENTED_HEADER_SIZE + chunk.len());
                packet.push(PacketProperty::Channeled as u8 | (self.connection_number << 5) | 0x80);
                packet.extend_from_slice(&sequence.to_le_bytes());
                packet.push(channel_id);
                packet.extend_from_slice(&fragment_id.to_le_bytes());
                packet.extend_from_slice(&(part as u16).to_le_bytes());
                packet.extend_from_slice(&(total_fragments as u16).to_le_bytes());
                packet.extend_from_slice(chunk);
                packets.push(ReliableSend {
                    channel_id,
                    sequence,
                    bytes: packet,
                    last_sent: None,
                });
            }
        }

        // Record the complete message before its first datagram can elicit an ACK. If a send
        // fails, every packet remains queued (unsent entries have last_sent == None) for retry.
        {
            let mut pending = self.pending_reliable.lock().await;
            pending.extend(packets.iter().cloned());
            self.pending_reliable_active.store(true, Ordering::Relaxed);
        }

        for packet in &packets {
            self.send_connected(&packet.bytes).await?;
            self.mark_reliable_sent(packet, SystemTime::now()).await;
        }
        Ok(())
    }

    async fn receive_loop(self: Arc<Self>) -> Result<()> {
        let mut buffer = vec![0u8; 65535];
        loop {
            if !self.in_use.load(Ordering::Relaxed) || self.shared_receive.load(Ordering::Relaxed) {
                break;
            }
            let len = tokio::select! {
                result = self.socket.recv(&mut buffer) => result?,
                _ = self.receive_shutdown.notified() => break,
            };
            self.handle_packet(&buffer[..len]).await?;
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
                    if self.index != 0 {
                        if let Err(err) = configure_load_sink_socket(&self.socket) {
                            warn!(
                                "client {} failed to enable load-sink receive filter: {err}",
                                self.index
                            );
                        }
                    }
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
                let reason =
                    if packet.property == PacketProperty::Disconnect && packet.payload.len() >= 2 {
                        let length =
                            u16::from_le_bytes([packet.payload[0], packet.payload[1]]) as usize;
                        if packet.payload.len() >= 2 + length {
                            std::str::from_utf8(&packet.payload[2..2 + length]).ok()
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                warn!(
                    "client {} disconnected/rejected by server: {:?} reason={:?}",
                    self.index, packet.property, reason
                );
                self.deactivate();
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
            PacketProperty::CompactMerged => {
                const LONG_LENGTH_FLAG: u8 = 0x80;
                const RAW_PACKET_FLAG: u8 = 0x40;
                const CHANNEL_MASK: u8 = 0x3f;
                let connection_number = (bytes[0] & 0x60) >> 5;
                let mut pos = 1usize;
                while pos < bytes.len() {
                    if bytes.len() - pos < 2 {
                        break;
                    }
                    let tag = bytes[pos];
                    pos += 1;
                    let is_raw = tag & RAW_PACKET_FLAG != 0;
                    let channel = tag & CHANNEL_MASK;
                    if is_raw && channel != 0 {
                        break;
                    }
                    let payload_len = if tag & LONG_LENGTH_FLAG != 0 {
                        if bytes.len() - pos < 2 {
                            break;
                        }
                        let len = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
                        pos += 2;
                        if len <= u8::MAX as usize {
                            break;
                        }
                        len
                    } else {
                        let len = bytes[pos] as usize;
                        pos += 1;
                        len
                    };
                    if payload_len > bytes.len() - pos {
                        break;
                    }
                    let payload = &bytes[pos..pos + payload_len];
                    pos += payload_len;
                    if is_raw {
                        if payload_len < 4 {
                            break;
                        }
                        let property = PacketProperty::from_byte(payload[0]);
                        if !matches!(
                            property,
                            Some(PacketProperty::Ack | PacketProperty::Channeled)
                        ) {
                            break;
                        }
                        Box::pin(self.handle_packet(payload)).await?;
                    } else {
                        let mut packet = Vec::with_capacity(payload_len + 2);
                        packet.push(PacketProperty::Unreliable as u8 | (connection_number << 5));
                        packet.push(channel);
                        packet.extend_from_slice(payload);
                        Box::pin(self.handle_packet(&packet)).await?;
                    }
                }
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_channeled(&self, channel_id: u8, sequence: u16, payload: &[u8]) -> Result<()> {
        let channel = channel_id / 4;
        let delivery = DeliveryMethod::from_channel_id(channel_id);
        if channel != channels::AUTH_IDENTITY && self.connected.load(Ordering::Relaxed) {
            self.shared_receive_eligible.store(true, Ordering::Release);
        }
        if matches!(
            delivery,
            DeliveryMethod::ReliableOrdered | DeliveryMethod::ReliableUnordered
        ) {
            self.send_ack(channel_id, sequence).await?;
            if !self
                .received_reliable
                .lock()
                .expect("reliable receive state mutex poisoned")
                .mark_new(channel_id, sequence)
            {
                trace!(
                    "client {} suppressed duplicate reliable packet channel_id={} sequence={}",
                    self.index,
                    channel_id,
                    sequence
                );
                return Ok(());
            }
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
            | channels::CREATE_REMOTE_PLAYER
            | channels::CREATE_REMOTE_PLAYERS_FOR_NEW_PEER
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
        self.send_connected(&packet).await?;
        Ok(())
    }

    async fn send_pong(&self, sequence: u16) -> Result<()> {
        let mut writer = NetWriter::with_capacity(11);
        writer.put_u8(PacketProperty::Pong as u8 | (self.connection_number << 5));
        writer.put_u16(sequence);
        writer.put_i64(dotnet_utc_ticks());
        self.send_connected(&writer.into_vec()).await?;
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
        self.send_connected(&writer.into_vec()).await?;
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
        self.pending_reliable_active
            .store(!pending.is_empty(), Ordering::Relaxed);
    }

    async fn maintenance_loop(self: Arc<Self>) {
        let mut tick = time::interval(MAINTENANCE_INTERVAL);
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut ping_ticks = 0usize;
        loop {
            tick.tick().await;
            if !self.in_use.load(Ordering::Relaxed) {
                break;
            }

            if self.pending_reliable_active.load(Ordering::Relaxed) {
                let _ = self.resend_reliable().await;
            }

            ping_ticks = ping_ticks.wrapping_add(1);
            if ping_ticks >= PING_INTERVAL_TICKS {
                ping_ticks = 0;
                let _ = self.send_ping().await;
            }
        }
    }

    async fn resend_reliable(&self) -> Result<()> {
        if !self.connected.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Reserve due packets while holding the queue lock, but never await a socket operation
        // while holding it. This lets an ACK remove packets while a resend batch is in flight.
        let now = SystemTime::now();
        let due = {
            let mut pending = self.pending_reliable.lock().await;
            if pending.is_empty() {
                self.pending_reliable_active.store(false, Ordering::Relaxed);
                return Ok(());
            }

            let mut due = Vec::new();
            for item in pending.iter_mut() {
                let should_send = item
                    .last_sent
                    .and_then(|sent| now.duration_since(sent).ok())
                    .map(|elapsed| elapsed >= Duration::from_millis(150))
                    .unwrap_or(true);
                if should_send {
                    // Mark before sending so another maintenance pass cannot select the same
                    // packet while this pass is awaiting the UDP write.
                    item.last_sent = Some(now);
                    due.push(item.clone());
                }
            }
            due
        };

        for item in due {
            if let Err(err) = self.send_connected(&item.bytes).await {
                // Keep failed packets immediately eligible for the next retry. The full message
                // remains in the queue, including fragments that were not reached yet.
                let mut pending = self.pending_reliable.lock().await;
                if let Some(current) = pending.iter_mut().find(|current| {
                    current.channel_id == item.channel_id
                        && current.sequence == item.sequence
                        && current.bytes == item.bytes
                }) {
                    current.last_sent = None;
                }
                return Err(err);
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

#[derive(Debug, Clone)]
struct VoiceLibrary {
    clips: Vec<VoiceClip>,
}

#[derive(Debug, Clone)]
struct VoiceClip {
    path: PathBuf,
    packets: Arc<OggOpusPackets>,
}

impl VoiceLibrary {
    fn load(folder: &str, reencode: bool, frame_duration_ms: u64) -> Result<Option<Self>> {
        let folder = PathBuf::from(folder);
        let folder = if folder.is_absolute() {
            folder
        } else {
            std::env::current_dir()?.join(folder)
        };
        info!(
            "scanning voice audio folder {} (ffmpeg_reencode={})",
            folder.display(),
            reencode
        );
        if !folder.exists() {
            warn!("voice audio folder does not exist: {}", folder.display());
            return Ok(None);
        }
        if !folder.is_dir() {
            warn!("voice audio path is not a folder: {}", folder.display());
            return Ok(None);
        }

        let mut clips = Vec::new();
        for entry in std::fs::read_dir(&folder)
            .with_context(|| format!("reading voice audio folder {}", folder.display()))?
        {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() && is_ogg_opus_path(&path) {
                let load_result = if reencode {
                    OggOpusPackets::load_reencoded(&path, frame_duration_ms)
                } else {
                    OggOpusPackets::load(&path)
                };
                match load_result {
                    Ok(packets) => clips.push(VoiceClip {
                        path,
                        packets: Arc::new(packets),
                    }),
                    Err(err) => warn!(
                        "excluding unusable voice audio file {}: {err:#}",
                        path.display()
                    ),
                }
            }
        }
        clips.sort_by(|a, b| a.path.cmp(&b.path));

        if clips.is_empty() {
            warn!(
                "voice audio folder contains no .opus or .ogg files: {}",
                folder.display()
            );
            return Ok(None);
        }

        info!(
            "voice audio folder loaded {} usable Ogg Opus file(s)",
            clips.len()
        );
        Ok(Some(Self { clips }))
    }

    fn random_clip(&self) -> Option<VoiceClip> {
        if self.clips.is_empty() {
            return None;
        }
        let idx = rand::thread_rng().gen_range(0..self.clips.len());
        Some(self.clips[idx].clone())
    }
}

fn is_ogg_opus_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("opus") || ext.eq_ignore_ascii_case("ogg"))
        .unwrap_or(false)
}

#[derive(Debug, Clone)]
struct OggOpusPackets {
    packets: Vec<OpusPacket>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OpusPacket {
    data: Vec<u8>,
    duration_ms: u64,
}

impl OggOpusPackets {
    fn load(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("reading Opus file {}", path.display()))?;
        Self::parse(&bytes).with_context(|| format!("parsing Ogg Opus file {}", path.display()))
    }

    fn load_reencoded(path: &Path, frame_duration_ms: u64) -> Result<Self> {
        let output_path =
            std::env::temp_dir().join(format!("basis-rust-client-voice-{}.opus", Uuid::new_v4()));
        let output = Command::new("ffmpeg")
            .arg("-hide_banner")
            .arg("-loglevel")
            .arg("error")
            .arg("-y")
            .arg("-i")
            .arg(path)
            .arg("-vn")
            .arg("-ac")
            .arg("1")
            .arg("-ar")
            .arg("48000")
            .arg("-c:a")
            .arg("libopus")
            .arg("-b:a")
            .arg("32000")
            .arg("-application")
            .arg("audio")
            .arg("-frame_duration")
            .arg(frame_duration_ms.to_string())
            .arg(&output_path)
            .output()
            .with_context(|| {
                format!(
                    "running ffmpeg to re-encode {} to 48kHz mono Opus",
                    path.display()
                )
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let _ = std::fs::remove_file(&output_path);
            return Err(anyhow!(
                "ffmpeg failed to re-encode {}: {}",
                path.display(),
                stderr.trim()
            ));
        }

        let result = Self::load(&output_path).with_context(|| {
            format!(
                "reading ffmpeg re-encoded Opus output for {}",
                path.display()
            )
        });
        let _ = std::fs::remove_file(&output_path);
        result
    }

    fn parse(bytes: &[u8]) -> Result<Self> {
        let mut pos = 0;
        let mut current_packet = Vec::new();
        let mut packets = Vec::new();

        while pos < bytes.len() {
            if bytes.len() - pos < 27 {
                return Err(anyhow!("truncated Ogg page header at byte {pos}"));
            }
            if &bytes[pos..pos + 4] != b"OggS" {
                return Err(anyhow!("invalid Ogg capture pattern at byte {pos}"));
            }
            let page_segments = bytes[pos + 26] as usize;
            let segment_table_start = pos + 27;
            let segment_table_end = segment_table_start + page_segments;
            if segment_table_end > bytes.len() {
                return Err(anyhow!("truncated Ogg segment table at byte {pos}"));
            }
            let data_len: usize = bytes[segment_table_start..segment_table_end]
                .iter()
                .map(|segment| *segment as usize)
                .sum();
            let data_start = segment_table_end;
            let data_end = data_start + data_len;
            if data_end > bytes.len() {
                return Err(anyhow!("truncated Ogg page data at byte {pos}"));
            }

            let mut data_pos = data_start;
            for segment_len in &bytes[segment_table_start..segment_table_end] {
                let segment_len = *segment_len as usize;
                current_packet.extend_from_slice(&bytes[data_pos..data_pos + segment_len]);
                data_pos += segment_len;
                if segment_len < 255 {
                    if is_playable_opus_packet(&current_packet) {
                        let data = std::mem::take(&mut current_packet);
                        let duration_ms = opus_packet_duration_ms(&data)
                            .unwrap_or(DEFAULT_VOICE_FRAME_DURATION_MS);
                        packets.push(OpusPacket { data, duration_ms });
                    } else {
                        current_packet.clear();
                    }
                }
            }

            pos = data_end;
        }

        if !current_packet.is_empty() {
            return Err(anyhow!("truncated Ogg packet at end of stream"));
        }
        if packets.is_empty() {
            return Err(anyhow!("Ogg Opus file contains no playable Opus packets"));
        }

        Ok(Self { packets })
    }
}

fn is_playable_opus_packet(packet: &[u8]) -> bool {
    !packet.is_empty() && !packet.starts_with(b"OpusHead") && !packet.starts_with(b"OpusTags")
}

fn opus_packet_duration_ms(packet: &[u8]) -> Option<u64> {
    let toc = *packet.first()?;
    let frame_count = match toc & 0x03 {
        0 => 1,
        1 | 2 => 2,
        3 => {
            let count_byte = *packet.get(1)?;
            (count_byte & 0x3f) as u64
        }
        _ => return None,
    };
    if frame_count == 0 {
        return None;
    }
    let config = toc >> 3;
    let frame_duration_us = match config {
        0..=11 => {
            let index = config & 0x03;
            match index {
                0 => 10_000,
                1 => 20_000,
                2 => 40_000,
                3 => 60_000,
                _ => return None,
            }
        }
        12..=15 => {
            if (config & 0x01) == 0 {
                10_000
            } else {
                20_000
            }
        }
        16..=19 => {
            let index = config & 0x03;
            match index {
                0 => 2_500,
                1 => 5_000,
                2 => 10_000,
                3 => 20_000,
                _ => return None,
            }
        }
        20..=31 => {
            let index = config & 0x03;
            match index {
                0 => 2_500,
                1 => 5_000,
                2 => 10_000,
                3 => 20_000,
                _ => return None,
            }
        }
        _ => return None,
    };
    Some((frame_duration_us * frame_count).div_ceil(1000))
}

fn voice_speaker_target(connected_count: usize, percent: u8) -> usize {
    let percent = percent.min(100) as usize;
    if connected_count == 0 || percent == 0 {
        return 0;
    }
    (connected_count * percent).div_ceil(100)
}

fn choose_next_speaker(
    connected: &[usize],
    active: &HashSet<usize>,
    avoid: &HashSet<usize>,
) -> Option<usize> {
    let eligible = connected
        .iter()
        .copied()
        .filter(|idx| !active.contains(idx))
        .collect::<Vec<_>>();
    if eligible.is_empty() {
        return None;
    }
    let preferred = eligible
        .iter()
        .copied()
        .filter(|idx| !avoid.contains(idx))
        .collect::<Vec<_>>();
    let candidates = if preferred.is_empty() {
        eligible
    } else {
        preferred
    };
    Some(candidates[rand::thread_rng().gen_range(0..candidates.len())])
}

fn serialize_voice_recipients_small(recipients: &[u16]) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(1 + recipients.len() * 2);
    writer.put_u8(recipients.len().min(u8::MAX as usize) as u8);
    for recipient in recipients.iter().take(u8::MAX as usize) {
        writer.put_u16(*recipient);
    }
    writer.into_vec()
}

fn serialize_voice_recipients_large(recipients: &[u16]) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(2 + recipients.len() * 2);
    writer.put_u16(recipients.len().min(u16::MAX as usize) as u16);
    for recipient in recipients.iter().take(u16::MAX as usize) {
        writer.put_u16(*recipient);
    }
    writer.into_vec()
}

fn serialize_audio_segment(sequence: u8, opus_packet: &[u8]) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(2 + opus_packet.len());
    writer.put_u8(sequence);
    writer.put_u8(0);
    writer.put_bytes(opus_packet);
    writer.into_vec()
}

fn distance_within(a: [f32; 3], b: [f32; 3], max_distance: f32) -> bool {
    let dx = a[0] - b[0];
    let dy = a[1] - b[1];
    let dz = a[2] - b[2];
    dx * dx + dy * dy + dz * dz <= max_distance * max_distance
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

#[cfg(target_os = "linux")]
fn configure_load_sink_socket(socket: &UdpSocket) -> std::io::Result<()> {
    // A simulated load client only needs reliable/control traffic after it has connected.
    // Basis server avatar fanout is sent as top-level Unreliable, Merged, or CompactMerged
    // datagrams. For non-observer load-sink clients we intentionally discard all three after the
    // connection/authentication handshake; client 0 remains unfiltered and exercises the complete
    // receive protocol. This is load-generator behavior, not general client semantics. These sockets
    // are connected UDP sockets; the socket-filter view starts at the UDP header, so LiteNetLib byte
    // 0 is at offset 8.
    const BPF_LD_B_ABS: u16 = 0x30;
    const BPF_ALU_AND_K: u16 = 0x54;
    const BPF_JMP_JEQ_K: u16 = 0x15;
    const BPF_RET_K: u16 = 0x06;
    const ACCEPT_ALL: u32 = u32::MAX;

    let mut filters = [
        libc::sock_filter {
            code: BPF_LD_B_ABS,
            jt: 0,
            jf: 0,
            k: 8,
        },
        libc::sock_filter {
            code: BPF_ALU_AND_K,
            jt: 0,
            jf: 0,
            k: 0x1f,
        },
        libc::sock_filter {
            code: BPF_JMP_JEQ_K,
            jt: 3,
            jf: 0,
            k: PacketProperty::Unreliable as u32,
        },
        libc::sock_filter {
            code: BPF_JMP_JEQ_K,
            jt: 2,
            jf: 0,
            k: PacketProperty::Merged as u32,
        },
        libc::sock_filter {
            code: BPF_JMP_JEQ_K,
            jt: 1,
            jf: 0,
            k: PacketProperty::CompactMerged as u32,
        },
        libc::sock_filter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: ACCEPT_ALL,
        },
        libc::sock_filter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: 0,
        },
    ];
    let program = libc::sock_fprog {
        len: filters.len() as u16,
        filter: filters.as_mut_ptr(),
    };
    let fd = socket.as_raw_fd();
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ATTACH_FILTER,
            (&program as *const libc::sock_fprog).cast(),
            std::mem::size_of::<libc::sock_fprog>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }

    // Once bulk unreliable receive is filtered, synthetic peers do not need multi-megabyte
    // socket queues. Keep enough room for bursts of ACK/auth/control traffic while reducing
    // kernel memory pressure for 1000 sockets.
    let buffer_bytes: libc::c_int = 64 * 1024;
    for option in [libc::SO_RCVBUF, libc::SO_SNDBUF] {
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                option,
                (&buffer_bytes as *const libc::c_int).cast(),
                std::mem::size_of_val(&buffer_bytes) as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn configure_load_sink_socket(_socket: &UdpSocket) -> std::io::Result<()> {
    Ok(())
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
        let group_offset = index
            .checked_div(self.group_size)
            .map(|group| group as f32 * self.group_spacing)
            .unwrap_or(0.0);
        [
            group_offset + rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
        ]
    }
}

#[derive(Debug, Clone, Copy)]
struct CadenceOptions {
    sync_batching: bool,
    movement_jitter_percent: u8,
    voice_jitter_percent: u8,
}

fn cadence_seed(index: usize, stream: u64) -> u64 {
    let mut x = (index as u64)
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(stream.rotate_left(17));
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn cadence_next(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn jittered_duration(base: Duration, jitter_percent: u8, state: &mut u64) -> Duration {
    let base_us = base.as_micros().max(1) as u64;
    let jitter = jitter_percent.min(95) as u64;
    if jitter == 0 {
        return Duration::from_micros(base_us);
    }
    let max_delta = base_us.saturating_mul(jitter) / 100;
    let span = max_delta.saturating_mul(2).saturating_add(1);
    let offset = cadence_next(state) % span;
    Duration::from_micros(
        base_us
            .saturating_sub(max_delta)
            .saturating_add(offset)
            .max(1),
    )
}

async fn movement_workers(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    shutdown: Arc<AtomicBool>,
    cadence: CadenceOptions,
) {
    let initial_snapshot = clients.lock().await.clone();
    let initial_len = initial_snapshot.len();
    let worker_count = num_cpus::get().max(1).min(initial_len.max(1));
    let start = SystemTime::now();
    for worker in 0..worker_count {
        let clients = clients.clone();
        let shutdown = shutdown.clone();
        let mut snapshot = initial_snapshot.clone();
        tokio::spawn(async move {
            if cadence.sync_batching {
                time::sleep(Duration::from_millis((worker * 12) as u64)).await;
                let mut ticker = time::interval(MOVEMENT_INTERVAL);
                ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
                let mut refresh_ticks = 0u8;
                loop {
                    ticker.tick().await;
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    refresh_ticks = refresh_ticks.wrapping_add(1);
                    if refresh_ticks >= 10 {
                        snapshot = clients.lock().await.clone();
                        refresh_ticks = 0;
                    }
                    let mut idx = worker;
                    while idx < snapshot.len() {
                        let client = &snapshot[idx];
                        if client.connected.load(Ordering::Relaxed) {
                            let sequence = client.movement_sequence.fetch_add(1, Ordering::Relaxed);
                            let mut pose = client.pose.lock().await;
                            let datagram = pose.write_movement_datagram(sequence, start);
                            if let Err(err) = client.send_connected(datagram).await {
                                trace!("movement send failed for {}: {err}", client.index);
                            }
                        }
                        idx += worker_count;
                    }
                }
                return;
            }

            // Random phases make deadlines dense. A min-heap preserves those phases while
            // allowing clients added from the console to join the schedule on the next refresh.
            let now = time::Instant::now();
            let interval_us = MOVEMENT_INTERVAL.as_micros() as u64;
            let mut deadlines = BinaryHeap::<Reverse<(time::Instant, usize, u64)>>::with_capacity(
                initial_len.div_ceil(worker_count),
            );
            let mut scheduled_len = 0;
            let mut next_snapshot_refresh = now;

            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let wake_at = deadlines
                    .peek()
                    .map(|Reverse((deadline, _, _))| (*deadline).min(next_snapshot_refresh))
                    .unwrap_or(next_snapshot_refresh);
                time::sleep_until(wake_at).await;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let current = time::Instant::now();
                if current >= next_snapshot_refresh {
                    snapshot = clients.lock().await.clone();
                    for index in scheduled_len..snapshot.len() {
                        if index % worker_count != worker {
                            continue;
                        }
                        let mut cadence_state = cadence_seed(index, 0x4d4f_5645_4d45_4e54);
                        let phase_us = cadence_next(&mut cadence_state) % interval_us.max(1);
                        deadlines.push(Reverse((
                            current + Duration::from_micros(phase_us),
                            index,
                            cadence_state,
                        )));
                    }
                    scheduled_len = snapshot.len();
                    next_snapshot_refresh = current + Duration::from_secs(1);
                }

                while let Some(Reverse((next, _, _))) = deadlines.peek() {
                    if *next > current {
                        break;
                    }
                    let Reverse((scheduled, index, mut cadence_state)) = deadlines
                        .pop()
                        .expect("movement deadline heap was non-empty");
                    if let Some(client) = snapshot.get(index) {
                        if client.connected.load(Ordering::Relaxed) {
                            let sequence = client.movement_sequence.fetch_add(1, Ordering::Relaxed);
                            let mut pose = client.pose.lock().await;
                            let datagram = pose.write_movement_datagram(sequence, start);
                            if let Err(err) = client.send_connected(datagram).await {
                                trace!("movement send failed for {}: {err}", client.index);
                            }
                        }
                    }
                    let interval = jittered_duration(
                        MOVEMENT_INTERVAL,
                        cadence.movement_jitter_percent,
                        &mut cadence_state,
                    );
                    let mut next = scheduled + interval;
                    if next < current {
                        next = current + interval;
                    }
                    deadlines.push(Reverse((next, index, cadence_state)));
                }
            }
        });
    }
}

#[derive(Clone)]
struct VoicePlaybackContext {
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    hearing_distance: f32,
    frame_duration: Duration,
    shutdown: Arc<AtomicBool>,
    done_tx: mpsc::UnboundedSender<usize>,
    cadence: CadenceOptions,
}

async fn voice_workers(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    config: Config,
    library: Arc<VoiceLibrary>,
    shutdown: Arc<AtomicBool>,
    cadence: CadenceOptions,
) {
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<usize>();
    let mut active = HashSet::<usize>::new();
    let frame_duration = Duration::from_millis(config.voice_frame_duration_ms);
    let mut refill_tick = time::interval(Duration::from_millis(250));

    loop {
        refill_tick.tick().await;
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let mut recently_finished = HashSet::new();
        while let Ok(index) = done_rx.try_recv() {
            active.remove(&index);
            recently_finished.insert(index);
        }

        let snapshot = clients.lock().await.clone();
        let connected = snapshot
            .iter()
            .filter(|client| client.connected.load(Ordering::Relaxed))
            .map(|client| client.index)
            .collect::<Vec<_>>();
        let connected_set = connected.iter().copied().collect::<HashSet<_>>();
        active.retain(|index| connected_set.contains(index));

        let target = voice_speaker_target(connected.len(), config.voice_speaker_percent);
        while active.len() < target {
            let Some(index) = choose_next_speaker(&connected, &active, &recently_finished) else {
                break;
            };
            let Some(client) = snapshot
                .iter()
                .find(|client| client.index == index)
                .cloned()
            else {
                break;
            };
            let Some(clip) = library.random_clip() else {
                active.remove(&index);
                break;
            };
            active.insert(index);

            let context = VoicePlaybackContext {
                clients: clients.clone(),
                hearing_distance: config.voice_hearing_distance,
                frame_duration,
                shutdown: shutdown.clone(),
                done_tx: done_tx.clone(),
                cadence,
            };
            tokio::spawn(async move {
                voice_playback_task(client, clip, context).await;
            });
        }
    }
}

async fn voice_playback_task(
    client: Arc<BasisClient>,
    clip: VoiceClip,
    context: VoicePlaybackContext,
) {
    let VoicePlaybackContext {
        clients,
        hearing_distance,
        frame_duration,
        shutdown,
        done_tx,
        cadence,
    } = context;
    let index = client.index;
    let result = async {
        let non_default_packets = clip
            .packets
            .packets
            .iter()
            .filter(|packet| packet.duration_ms != frame_duration.as_millis() as u64)
            .count();
        if non_default_packets > 0 {
            debug!(
                "voice file {} contains {}/{} packets not matching configured {}ms pacing; using per-packet Opus durations",
                clip.path.display(),
                non_default_packets,
                clip.packets.packets.len(),
                frame_duration.as_millis()
            );
        }
        let mut last_recipients = Vec::<u16>::new();
        let mut last_refresh = None::<time::Instant>;
        let mut published_once = false;
        let mut logged_first_send = false;
        let mut cadence_state = cadence_seed(client.index, 0x564f_4943_455f_5458);
        let initial_phase = if cadence.sync_batching {
            Duration::ZERO
        } else {
            Duration::from_micros(
                cadence_next(&mut cadence_state)
                    % (frame_duration.as_micros().max(1) as u64),
            )
        };
        let mut next_packet_at = time::Instant::now() + initial_phase;
        let max_lag = Duration::from_millis(DEFAULT_VOICE_FRAME_DURATION_MS * 5);
        let mut packets_sent = 0usize;
        let mut packets_skipped = 0usize;
        let mut playback_started = time::Instant::now();

        for packet in clip.packets.packets.iter() {
            if shutdown.load(Ordering::Relaxed) || !client.connected.load(Ordering::Relaxed) {
                break;
            }

            let now = time::Instant::now();
            if next_packet_at > now {
                time::sleep_until(next_packet_at).await;
            } else if now.duration_since(next_packet_at) > max_lag {
                next_packet_at = now;
            }

            let should_refresh = last_refresh
                .map(|instant| instant.elapsed() >= Duration::from_secs(1))
                .unwrap_or(true);
            if !published_once || should_refresh {
                let snapshot = clients.lock().await.clone();
                let exclusions =
                    voice_exclusions_for_inverted_recipients(&client, &snapshot, hearing_distance)
                        .await;
                if !published_once || exclusions != last_recipients {
                    publish_voice_recipient_exclusions(&client, &exclusions).await?;
                    last_recipients = exclusions;
                    published_once = true;
                }
                last_refresh = Some(time::Instant::now());
            }

            if packet.data.len() > MAX_VOICE_PACKET_BYTES {
                packets_skipped += 1;
                warn!(
                    "skipping oversized voice packet for client {} from {}: {} bytes",
                    client.index,
                    clip.path.display(),
                    packet.data.len()
                );
            } else if packet.duration_ms > MAX_UNITY_VOICE_FRAME_DURATION_MS {
                packets_skipped += 1;
                warn!(
                    "skipping voice packet for client {} from {}: {}ms exceeds Unity voice max {}ms",
                    client.index,
                    clip.path.display(),
                    packet.duration_ms,
                    MAX_UNITY_VOICE_FRAME_DURATION_MS
                );
            } else {
                let sequence = client.voice_sequence.fetch_add(1, Ordering::SeqCst);
                let payload = serialize_audio_segment(sequence, &packet.data);
                client.send_unreliable(channels::VOICE, &payload).await?;
                packets_sent += 1;
                if !logged_first_send {
                    logged_first_send = true;
                    playback_started = time::Instant::now();
                    debug!(
                        "client {} sent first voice packet from {} (opus_bytes={} wire_bytes={})",
                        client.index,
                        clip.path.display(),
                        packet.data.len(),
                        payload.len()
                    );
                }
            }
            let packet_duration = Duration::from_millis(packet.duration_ms.max(1));
            next_packet_at += if cadence.sync_batching {
                packet_duration
            } else {
                jittered_duration(
                    packet_duration,
                    cadence.voice_jitter_percent,
                    &mut cadence_state,
                )
            };
        }

        if packets_sent > 0 || packets_skipped > 0 {
            debug!(
                "client {} finished voice playback from {} (sent={} skipped={} elapsed_ms={})",
                client.index,
                clip.path.display(),
                packets_sent,
                packets_skipped,
                playback_started.elapsed().as_millis()
            );
        }

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(err) = result {
        warn!(
            "voice playback failed for client {} using {}: {err:#}",
            index,
            clip.path.display()
        );
    }
    let _ = done_tx.send(index);
}

async fn voice_exclusions_for_inverted_recipients(
    speaker: &BasisClient,
    clients: &[Arc<BasisClient>],
    hearing_distance: f32,
) -> Vec<u16> {
    let speaker_position = speaker.current_position().await;
    let mut exclusions = Vec::new();
    for candidate in clients {
        if candidate.index == speaker.index || !candidate.connected.load(Ordering::Relaxed) {
            continue;
        }
        let Some(peer_id) = candidate.remote_peer_id().await else {
            continue;
        };
        let position = candidate.current_position().await;
        if !distance_within(speaker_position, position, hearing_distance) {
            exclusions.push(peer_id);
        }
    }
    exclusions.sort_unstable();
    exclusions.dedup();
    exclusions
}

async fn publish_voice_recipient_exclusions(
    client: &BasisClient,
    exclusions: &[u16],
) -> Result<()> {
    if exclusions.len() <= u8::MAX as usize {
        let payload = serialize_voice_recipients_small(exclusions);
        client
            .send_reliable_ordered(channels::AUDIO_RECIPIENTS_INVERTED, &payload)
            .await
    } else {
        let payload = serialize_voice_recipients_large(exclusions);
        client
            .send_reliable_ordered(channels::AUDIO_RECIPIENTS_INVERTED_LARGE, &payload)
            .await
    }
}

async fn publish_client_batch(
    managed_clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    batch_start: usize,
    batch_clients: &[Arc<BasisClient>],
    maintenance: &MaintenanceOptions,
) -> Result<()> {
    let mut managed = managed_clients.lock().await;
    if managed.len() != batch_start {
        return Err(anyhow!(
            "client batch {batch_start} published at dense index {}",
            managed.len()
        ));
    }
    if batch_clients
        .iter()
        .enumerate()
        .any(|(offset, client)| client.index != batch_start + offset)
    {
        return Err(anyhow!(
            "client batch {batch_start} contains a non-dense index"
        ));
    }
    managed.extend(batch_clients.iter().cloned());
    drop(managed);
    if maintenance.shared {
        maintenance.refresh.notify_one();
    }
    Ok(())
}

async fn replace_client_if_current(
    clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    index: usize,
    old: &Arc<BasisClient>,
    replacement: &Arc<BasisClient>,
    maintenance: &MaintenanceOptions,
) -> bool {
    let replaced = {
        let mut managed = clients.lock().await;
        match managed.get(index) {
            Some(current) if Arc::ptr_eq(current, old) => {
                managed[index] = replacement.clone();
                true
            }
            _ => false,
        }
    };
    if replaced {
        old.deactivate();
        if maintenance.shared {
            maintenance.refresh.notify_one();
        }
    }
    replaced
}

async fn start_client_with_retries(
    index: usize,
    config: &Config,
    spawn_base: [f32; 3],
    shared_maintenance_enabled: bool,
) -> Result<Arc<BasisClient>> {
    let mut last_error = None;
    for attempt in 1..=INITIAL_START_ATTEMPTS {
        let ready = match ReadyMessage::new(config, spawn_base) {
            Ok(ready) => ready,
            Err(err) => {
                last_error = Some(err);
                break;
            }
        };
        match BasisClient::start(index, config, ready, spawn_base, shared_maintenance_enabled).await
        {
            Ok(client) => return Ok(client),
            Err(err) => {
                warn!(
                    "failed to start client {index} (attempt {attempt}/{INITIAL_START_ATTEMPTS}): {err}"
                );
                last_error = Some(err);
            }
        }
    }

    Err(anyhow!(
        "failed to start client {index} after {INITIAL_START_ATTEMPTS} attempts: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    ))
}

async fn failure_reconnect_loop(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    config: Config,
    shutdown: Arc<AtomicBool>,
    spawn_layout: SpawnLayout,
    connect_timeout: Duration,
    maintenance: MaintenanceOptions,
) {
    let mut pending_since = HashMap::<usize, time::Instant>::new();
    let mut tick = time::interval(Duration::from_millis(250));

    loop {
        tick.tick().await;
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let snapshot = clients.lock().await.clone();
        for (idx, client) in snapshot.into_iter().enumerate() {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            if client.connected.load(Ordering::Relaxed) {
                pending_since.remove(&idx);
                continue;
            }
            if client.intentional_reconnect.load(Ordering::Relaxed) {
                pending_since.remove(&idx);
                continue;
            }

            if client.in_use.load(Ordering::Relaxed) {
                let started = pending_since.entry(idx).or_insert_with(time::Instant::now);
                if started.elapsed() < connect_timeout {
                    continue;
                }
                client.deactivate();
                pending_since.remove(&idx);
                warn!("client {idx} reconnect attempt timed out; recycling connection");
            }

            let spawn_base = spawn_layout.base_for_client(idx);
            let result = match ReadyMessage::new(&config, spawn_base) {
                Ok(ready) => {
                    BasisClient::start(idx, &config, ready, spawn_base, maintenance.shared).await
                }
                Err(err) => Err(err),
            };
            match result {
                Ok(new_client) => {
                    if replace_client_if_current(&clients, idx, &client, &new_client, &maintenance)
                        .await
                    {
                        pending_since.insert(idx, time::Instant::now());
                        info!("failure reconnect started for client {idx}");
                    } else {
                        warn!("discarding stale failure reconnect for client {idx}");
                        new_client.disconnect().await;
                    }
                }
                Err(err) => warn!("failed to restart client {idx}: {err}"),
            }
        }
    }
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct SharedReceivePacket {
    index: usize,
    fd: RawFd,
    offset: usize,
    len: usize,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
struct SharedReceiveBatch {
    data: Vec<u8>,
    packets: Vec<SharedReceivePacket>,
}

#[cfg(target_os = "linux")]
fn shared_receiver_send_ack(fd: RawFd, first_byte: u8, channel_id: u8, sequence: u16) {
    let mut packet = [0u8; 4 + ((DEFAULT_WINDOW_SIZE - 1) / 8 + 2)];
    packet[0] = PacketProperty::Ack as u8 | (first_byte & 0x60);
    packet[1..3].copy_from_slice(&sequence.to_le_bytes());
    packet[3] = channel_id;
    let bit_index = sequence as usize % DEFAULT_WINDOW_SIZE;
    packet[4 + bit_index / 8] |= 1 << (bit_index % 8);
    unsafe {
        libc::send(fd, packet.as_ptr().cast(), packet.len(), libc::MSG_DONTWAIT);
    }
}

#[cfg(target_os = "linux")]
fn shared_receiver_send_pong(fd: RawFd, first_byte: u8, sequence: u16) {
    let mut packet = [0u8; 11];
    packet[0] = PacketProperty::Pong as u8 | (first_byte & 0x60);
    packet[1..3].copy_from_slice(&sequence.to_le_bytes());
    packet[3..11].copy_from_slice(&dotnet_utc_ticks().to_le_bytes());
    unsafe {
        libc::send(fd, packet.as_ptr().cast(), packet.len(), libc::MSG_DONTWAIT);
    }
}

#[cfg(target_os = "linux")]
fn run_shared_epoll_receiver(
    registrations: std_mpsc::Receiver<(usize, RawFd)>,
    batches: mpsc::UnboundedSender<SharedReceiveBatch>,
    shutdown: Arc<AtomicBool>,
) {
    let profile_packet_mix = std::env::var("BASIS_CLIENT_PROFILE_RX_MIX")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(false);
    let mut packet_mix = [0u64; 32];
    let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if epoll_fd < 0 {
        warn!(
            "failed to create shared epoll receiver: {}",
            std::io::Error::last_os_error()
        );
        return;
    }

    let mut events = vec![unsafe { std::mem::zeroed::<libc::epoll_event>() }; 128];
    let mut buffer = vec![0u8; 65535];
    while !shutdown.load(Ordering::Relaxed) {
        while let Ok((index, fd)) = registrations.try_recv() {
            let mut event = libc::epoll_event {
                events: libc::EPOLLIN as u32,
                u64: ((index as u64) << 32) | (fd as u32 as u64),
            };
            let rc = unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut event) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EEXIST) {
                    warn!("failed to register client {index} fd {fd} with shared epoll receiver: {err}");
                }
            }
        }

        let ready =
            unsafe { libc::epoll_wait(epoll_fd, events.as_mut_ptr(), events.len() as i32, 100) };
        if ready < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            warn!("shared epoll receiver wait failed: {err}");
            break;
        }

        let mut batch = Vec::with_capacity(ready as usize);
        let mut batch_data = Vec::with_capacity((ready as usize).saturating_mul(64));
        for event in events.iter().take(ready as usize) {
            let index = (event.u64 >> 32) as usize;
            let fd = event.u64 as u32 as RawFd;
            loop {
                let len = unsafe {
                    libc::recv(
                        fd,
                        buffer.as_mut_ptr().cast(),
                        buffer.len(),
                        libc::MSG_DONTWAIT,
                    )
                };
                if len > 0 {
                    let len = len as usize;
                    let property = buffer[0] & 0x1f;
                    if profile_packet_mix {
                        packet_mix[property as usize] += 1;
                    }

                    // Registered sockets are authenticated load sinks. Handle the overwhelmingly
                    // common post-auth control traffic here so it never allocates/copies into the
                    // epoll-thread -> Tokio handoff. Reliable payloads still receive protocol ACKs,
                    // but their application data is intentionally discarded for synthetic peers.
                    match property {
                        p if p == PacketProperty::Channeled as u8 => {
                            if len >= 4 {
                                let sequence = u16::from_le_bytes([buffer[1], buffer[2]]);
                                let channel_id = buffer[3];
                                if matches!(channel_id % 4, 0 | 2) {
                                    shared_receiver_send_ack(fd, buffer[0], channel_id, sequence);
                                }
                            }
                            continue;
                        }
                        p if p == PacketProperty::Ping as u8 => {
                            if len >= 3 {
                                let sequence = u16::from_le_bytes([buffer[1], buffer[2]]);
                                shared_receiver_send_pong(fd, buffer[0], sequence);
                            }
                            continue;
                        }
                        p if p == PacketProperty::Pong as u8
                            || p == PacketProperty::MtuCheck as u8
                            || p == PacketProperty::MtuOk as u8 =>
                        {
                            continue;
                        }
                        _ => {}
                    }

                    let offset = batch_data.len();
                    batch_data.extend_from_slice(&buffer[..len]);
                    batch.push(SharedReceivePacket {
                        index,
                        fd,
                        offset,
                        len,
                    });
                    continue;
                }
                if len == 0 {
                    break;
                }
                let err = std::io::Error::last_os_error();
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) {
                    break;
                }
                break;
            }
        }
        if !batch.is_empty()
            && batches
                .send(SharedReceiveBatch {
                    data: batch_data,
                    packets: batch,
                })
                .is_err()
        {
            unsafe { libc::close(epoll_fd) };
            return;
        }
    }

    if profile_packet_mix {
        let names = [
            "Unreliable",
            "Channeled",
            "Ack",
            "Ping",
            "Pong",
            "ConnectRequest",
            "ConnectAccept",
            "Disconnect",
            "UnconnectedMessage",
            "MtuCheck",
            "MtuOk",
            "Broadcast",
            "Merged",
            "ShutdownOk",
            "PeerNotFound",
            "InvalidProtocol",
            "NatMessage",
            "Empty",
            "CompactMerged",
        ];
        for (property, count) in packet_mix.iter().copied().enumerate() {
            if count != 0 {
                let name = names.get(property).copied().unwrap_or("Unknown");
                info!(
                    "shared receive packet mix property={}({}) count={}",
                    property, name, count
                );
            }
        }
    }
    unsafe { libc::close(epoll_fd) };
}

#[cfg(target_os = "linux")]
async fn shared_receive_loop(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    shutdown: Arc<AtomicBool>,
) {
    let (registration_tx, registration_rx) = std_mpsc::channel::<(usize, RawFd)>();
    let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<SharedReceiveBatch>();
    let thread_shutdown = shutdown.clone();
    if let Err(err) = std::thread::Builder::new()
        .name("basis-shared-rx".to_string())
        .spawn(move || run_shared_epoll_receiver(registration_rx, batch_tx, thread_shutdown))
    {
        warn!("failed to start shared epoll receiver thread: {err}");
        return;
    }

    SHARED_RECEIVE_ACTIVE.store(true, Ordering::Release);
    let mut refresh = time::interval(Duration::from_millis(250));
    refresh.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut snapshot = clients.lock().await.clone();
    let mut registered_fds = vec![-1; snapshot.len()];

    loop {
        tokio::select! {
            _ = refresh.tick() => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                snapshot = clients.lock().await.clone();
                if registered_fds.len() < snapshot.len() {
                    registered_fds.resize(snapshot.len(), -1);
                }
                for (index, client) in snapshot.iter().enumerate().skip(1) {
                    if !client.in_use.load(Ordering::Relaxed)
                        || !client.shared_receive_eligible.load(Ordering::Acquire)
                    {
                        continue;
                    }
                    let fd = client.socket.as_raw_fd();
                    if registered_fds[index] == fd {
                        continue;
                    }
                    client.shared_receive.store(true, Ordering::Release);
                    client.stop_receive_loop();
                    if registration_tx.send((index, fd)).is_err() {
                        return;
                    }
                    registered_fds[index] = fd;
                }
            }
            maybe_batch = batch_rx.recv() => {
                let Some(batch) = maybe_batch else { break; };
                for packet in batch.packets {
                    let Some(client) = snapshot.get(packet.index) else { continue; };
                    if client.socket.as_raw_fd() != packet.fd
                        || !client.in_use.load(Ordering::Relaxed)
                    {
                        continue;
                    }
                    let end = packet.offset.saturating_add(packet.len);
                    let Some(bytes) = batch.data.get(packet.offset..end) else { continue; };
                    if let Err(err) = client.handle_packet(bytes).await {
                        debug!("client {} shared receive packet failed: {err}", packet.index);
                    }
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn shared_receive_loop(
    _clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    _shutdown: Arc<AtomicBool>,
) {
}

fn ping_bucket_matches(slot: usize, tick: usize) -> bool {
    slot % PING_INTERVAL_TICKS == tick % PING_INTERVAL_TICKS
}

async fn shared_maintenance_loop(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    maintenance_refresh: Arc<Notify>,
    shutdown: Arc<AtomicBool>,
) {
    let mut ticker = time::interval(MAINTENANCE_INTERVAL);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut snapshot = clients.lock().await.clone();
    let mut tick_count = 0usize;

    loop {
        let ticked = tokio::select! {
            _ = ticker.tick() => true,
            _ = maintenance_refresh.notified() => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                snapshot = clients.lock().await.clone();
                false
            }
        };
        if !ticked {
            continue;
        }
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        tick_count = tick_count.wrapping_add(1);
        if tick_count.is_multiple_of(SNAPSHOT_REFRESH_TICKS) {
            snapshot = clients.lock().await.clone();
        }

        for client in &snapshot {
            if client.in_use.load(Ordering::Relaxed)
                && client.pending_reliable_active.load(Ordering::Relaxed)
            {
                let _ = client.resend_reliable().await;
            }
        }

        let ping_bucket = tick_count % PING_INTERVAL_TICKS;
        for (slot, client) in snapshot.iter().enumerate() {
            if !ping_bucket_matches(slot, ping_bucket) {
                continue;
            }
            if client.in_use.load(Ordering::Relaxed) && client.connected.load(Ordering::Relaxed) {
                let _ = client.send_ping().await;
            }
        }
    }
}

async fn wait_for_full_population(
    clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    expected_population: usize,
    shutdown: &AtomicBool,
    timeout: Duration,
) -> usize {
    let deadline = time::Instant::now() + timeout;
    loop {
        let snapshot = clients.lock().await.clone();
        let connected = snapshot
            .iter()
            .filter(|client| client.connected.load(Ordering::Relaxed))
            .count();
        if (snapshot.len() == expected_population && connected == expected_population)
            || shutdown.load(Ordering::Relaxed)
        {
            info!(
                "current connected population {}/{}",
                connected, expected_population
            );
            return connected;
        }
        if time::Instant::now() >= deadline {
            warn!(
                "timed out waiting for full population: {}/{} currently connected",
                connected, expected_population
            );
            return connected;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

async fn random_reconnect_loop(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    config: Config,
    shutdown: Arc<AtomicBool>,
    spawn_layout: SpawnLayout,
    reconnect_min_secs: u64,
    reconnect_max_secs: u64,
    maintenance: MaintenanceOptions,
) {
    let min_secs = reconnect_min_secs.max(1);
    let max_secs = reconnect_max_secs.max(min_secs);
    loop {
        let delay_secs = rand::thread_rng().gen_range(min_secs..=max_secs);
        time::sleep(Duration::from_secs(delay_secs)).await;
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let len = clients.lock().await.len();
        if len == 0 {
            continue;
        }
        let idx = rand::thread_rng().gen_range(0..len);
        let old = { clients.lock().await[idx].clone() };
        old.intentional_reconnect.store(true, Ordering::Relaxed);
        old.disconnect().await;
        time::sleep(Duration::from_secs(3)).await;
        let spawn_base = spawn_layout.base_for_client(idx);
        let result = match ReadyMessage::new(&config, spawn_base) {
            Ok(ready) => {
                BasisClient::start(idx, &config, ready, spawn_base, maintenance.shared).await
            }
            Err(err) => Err(err),
        };
        match result {
            Ok(new_client) => {
                if replace_client_if_current(&clients, idx, &old, &new_client, &maintenance).await {
                    info!("reconnected client {idx}");
                } else {
                    warn!("discarding stale reconnect for client {idx}");
                    new_client.disconnect().await;
                }
            }
            Err(err) => {
                old.intentional_reconnect.store(false, Ordering::Relaxed);
                warn!("failed to reconnect client {idx}: {err}");
            }
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

#[derive(Debug, PartialEq, Eq)]
enum ConsoleCommand {
    EnableVoice,
    AddClients(usize),
    Quit {
        batch_size: Option<usize>,
        delay_ms: Option<u64>,
    },
    Help,
}

fn parse_console_command(line: &str) -> Option<ConsoleCommand> {
    let mut parts = line.split_whitespace();
    let command = parts.next()?.to_ascii_lowercase();
    match command.as_str() {
        "voice" | "v" => Some(ConsoleCommand::EnableVoice),
        "enable"
            if parts
                .next()
                .is_some_and(|value| value.eq_ignore_ascii_case("voice")) =>
        {
            Some(ConsoleCommand::EnableVoice)
        }
        "add" | "clients" => {
            let count = parts
                .next()
                .map(str::parse::<usize>)
                .transpose()
                .ok()?
                .unwrap_or(100);
            (count > 0).then_some(ConsoleCommand::AddClients(count))
        }
        "quit" | "exit" | "q" => {
            let batch_size = parts.next().map(str::parse::<usize>).transpose().ok()?;
            let delay_ms = parts.next().map(str::parse::<u64>).transpose().ok()?;
            Some(ConsoleCommand::Quit {
                batch_size,
                delay_ms,
            })
        }
        "help" | "h" | "?" => Some(ConsoleCommand::Help),
        _ => None,
    }
}

fn print_console_help() {
    println!("Console commands:");
    println!("  voice              enable voice simulation");
    println!("  add [count]        add clients (default: 100)");
    println!("  quit [batch] [ms]  disconnect in batches (default batch/delay: 100/250ms)");
}

async fn console_input(commands: mpsc::UnboundedSender<ConsoleCommand>) {
    let stdin = BufReader::new(io::stdin());
    let mut lines = stdin.lines();
    print!("> ");
    let _ = std::io::stdout().flush();
    while let Ok(Some(line)) = lines.next_line().await {
        if let Some(command) = parse_console_command(&line) {
            // Do not start another stdin read after quit. Tokio's stdin uses a blocking
            // helper thread, which can otherwise keep the runtime alive after shutdown.
            let quitting = matches!(command, ConsoleCommand::Quit { .. });
            if commands.send(command).is_err() || quitting {
                break;
            }
        } else if !line.trim().is_empty() {
            println!("Unknown command. Type 'help' for available commands.");
        }
        print!("> ");
        let _ = std::io::stdout().flush();
    }
}

async fn add_clients(
    managed_clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    config: &Config,
    count: usize,
    spawn_layout: SpawnLayout,
    connect: ConnectOptions,
    maintenance: &MaintenanceOptions,
    shutdown: &AtomicBool,
) -> Result<usize> {
    let start_index = managed_clients.lock().await.len();
    let target = start_index
        .checked_add(count)
        .ok_or_else(|| anyhow!("client count overflow while adding {count} clients"))?;
    let connect_batch_size = connect.batch_size.max(1);
    let mut index = start_index;
    while index < target {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let batch_end = (index + connect_batch_size).min(target);
        let batch_start = index;
        let mut batch_clients = Vec::with_capacity(batch_end - batch_start);
        for client_index in batch_start..batch_end {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            let spawn_base = spawn_layout.base_for_client(client_index);
            match start_client_with_retries(
                client_index,
                config,
                spawn_base,
                connect.shared_maintenance,
            )
            .await
            {
                Ok(client) => batch_clients.push(client),
                Err(err) => {
                    for client in batch_clients {
                        client.disconnect().await;
                    }
                    let added = index.saturating_sub(start_index);
                    if added > 0 {
                        warn!(
                            "stopped adding clients after {added}/{count}: client {client_index} failed: {err:#}"
                        );
                        return Ok(added);
                    }
                    return Err(err)
                        .with_context(|| format!("failed adding client {client_index}"));
                }
            }
        }
        if shutdown.load(Ordering::Relaxed) {
            for client in batch_clients {
                client.disconnect().await;
            }
            break;
        }
        if batch_clients.len() != batch_end - batch_start {
            for client in batch_clients {
                client.disconnect().await;
            }
            return Err(anyhow!(
                "client batch {batch_start}-{end} did not produce a complete dense population",
                end = batch_end.saturating_sub(1)
            ));
        }
        if let Err(err) =
            publish_client_batch(managed_clients, batch_start, &batch_clients, maintenance).await
        {
            for client in batch_clients {
                client.disconnect().await;
            }
            return Err(err);
        }
        let connected_in_batch =
            wait_for_batch_connected(&batch_clients, connect.timeout, shutdown).await;
        if connected_in_batch < batch_clients.len() {
            for client in &batch_clients {
                if !client.connected.load(Ordering::Relaxed) {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    client.deactivate();
                    warn!(
                        "client {} did not connect within {}ms",
                        client.index,
                        connect.timeout.as_millis()
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
        if index < target && !connect.batch_delay.is_zero() {
            sleep_or_shutdown(connect.batch_delay, shutdown).await;
        }
    }
    Ok(index.saturating_sub(start_index))
}

async fn disconnect_clients_in_batches(
    clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    batch_size: usize,
    delay: Duration,
) {
    let snapshot = clients.lock().await.clone();
    let batch_size = batch_size.max(1);
    for (batch_number, batch) in snapshot.chunks(batch_size).enumerate() {
        info!(
            "disconnecting client batch {} ({}/{} clients)",
            batch_number + 1,
            batch.len(),
            snapshot.len()
        );
        for client in batch {
            client.disconnect().await;
        }
        if batch_number + 1 < snapshot.len().div_ceil(batch_size) && !delay.is_zero() {
            time::sleep(delay).await;
        }
    }
}

fn main() -> Result<()> {
    let worker_threads = std::env::var("BASIS_CLIENT_TOKIO_WORKERS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| num_cpus::get().saturating_sub(1).max(1))
        .clamp(1, num_cpus::get().max(1));
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .thread_name("basis-tokio")
        .build()?;
    let result = runtime.block_on(async_main(worker_threads));
    // Tokio's async stdin is backed by a blocking helper. Give normal tasks time to finish,
    // but do not wait forever for that helper when Ctrl+C interrupts the client.
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}

async fn async_main(worker_threads: usize) -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "basis_rust_client=info,info".to_string()),
        )
        .init();

    let args = Args::parse();
    info!("tokio runtime workers={worker_threads}");
    let config_path = args.config.clone();
    let mut config = Config::load_or_create(&config_path)?;
    if let Some(ip) = args.ip {
        config.ip = ip;
    }
    if let Some(port) = args.port {
        config.port = port;
    }
    if let Some(clients) = args.clients {
        config.client_count = clients;
    }
    if args.voice {
        config.voice_enabled = true;
    }
    if let Some(folder) = args.voice_audio_folder {
        config.voice_audio_folder = folder.to_string_lossy().to_string();
    } else {
        config.voice_audio_folder =
            resolve_relative_to_config(&config_path, &config.voice_audio_folder);
    }
    if let Some(percent) = args.voice_speaker_percent {
        config.voice_speaker_percent = percent.min(100);
    }
    if let Some(distance) = args.voice_hearing_distance {
        config.voice_hearing_distance = sanitize_voice_distance(distance);
    }
    if let Some(frame_duration) = args.voice_frame_duration_ms {
        config.voice_frame_duration_ms = sanitize_voice_frame_duration(frame_duration);
    }
    let voice_reencode = !args.no_voice_reencode;
    let cadence = CadenceOptions {
        sync_batching: args.sync_batching,
        movement_jitter_percent: args.movement_jitter_percent,
        voice_jitter_percent: args.voice_jitter_percent,
    };

    let mut voice_library = if config.voice_enabled {
        info!(
            "voice simulation requested: folder={} speaker_percent={} hearing_distance={} frame_duration_ms={} ffmpeg_reencode={}",
            config.voice_audio_folder,
            config.voice_speaker_percent,
            config.voice_hearing_distance,
            config.voice_frame_duration_ms,
            voice_reencode
        );
        match VoiceLibrary::load(
            &config.voice_audio_folder,
            voice_reencode,
            config.voice_frame_duration_ms,
        ) {
            Ok(Some(library)) => Some(Arc::new(library)),
            Ok(None) => None,
            Err(err) => {
                warn!("failed to load voice audio library: {err:#}");
                None
            }
        }
    } else {
        None
    };

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

    let shared_maintenance_enabled = std::env::var("BASIS_CLIENT_SHARED_MAINTENANCE")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(true);
    let managed_clients = Arc::new(Mutex::new(Vec::with_capacity(config.client_count)));
    let maintenance_refresh = Arc::new(Notify::new());
    let maintenance = MaintenanceOptions {
        shared: shared_maintenance_enabled,
        refresh: maintenance_refresh.clone(),
    };
    if shared_maintenance_enabled {
        info!("shared client maintenance enabled");
        let maintenance_task = tokio::spawn(shared_maintenance_loop(
            managed_clients.clone(),
            maintenance_refresh.clone(),
            shutdown.clone(),
        ));
        let maintenance_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let result = maintenance_task.await;
            if maintenance_shutdown.load(Ordering::Relaxed) {
                return;
            }
            match result {
                Ok(()) => error!("shared client maintenance worker stopped unexpectedly"),
                Err(err) => error!("shared client maintenance worker failed: {err}"),
            }
            maintenance_shutdown.store(true, Ordering::SeqCst);
        });
    } else {
        info!("shared client maintenance disabled; using per-client maintenance timers");
    }

    let connect = ConnectOptions {
        batch_size: args.connect_batch_size.max(1),
        batch_delay: Duration::from_millis(args.connect_batch_delay_ms),
        timeout: Duration::from_millis(args.connect_timeout_ms),
        shared_maintenance: shared_maintenance_enabled,
    };
    let initial_target = config.client_count;
    let initial_added = add_clients(
        &managed_clients,
        &config,
        initial_target,
        spawn_layout,
        connect,
        &maintenance,
        &shutdown,
    )
    .await?;
    if initial_added != initial_target && !shutdown.load(Ordering::Relaxed) {
        disconnect_clients_in_batches(&managed_clients, connect.batch_size, Duration::ZERO).await;
        return Err(anyhow!(
            "initial client startup stopped after {initial_added}/{initial_target} clients"
        ));
    }

    // Hand authenticated sockets to the shared Linux receiver before failure recovery. This keeps
    // the Tokio runtime free to complete/retry the small number of failed joins instead of asking
    // it to service hundreds of already-connected per-client receive tasks during the recovery
    // window.
    let shared_receive_enabled = std::env::var("BASIS_CLIENT_SHARED_RECEIVE")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(cfg!(target_os = "linux"));
    if shared_receive_enabled && !shutdown.load(Ordering::Relaxed) {
        info!("shared client receive enabled");
        tokio::spawn(shared_receive_loop(
            managed_clients.clone(),
            shutdown.clone(),
        ));
    } else if !shared_receive_enabled {
        info!("shared client receive disabled; using per-client receive tasks");
    }

    if !args.no_reconnect && !shutdown.load(Ordering::Relaxed) {
        tokio::spawn(failure_reconnect_loop(
            managed_clients.clone(),
            config.clone(),
            shutdown.clone(),
            spawn_layout,
            connect.timeout,
            maintenance.clone(),
        ));
        let _ = wait_for_full_population(
            &managed_clients,
            config.client_count,
            &shutdown,
            Duration::from_secs(120),
        )
        .await;
    }

    if !args.no_movement && !shutdown.load(Ordering::Relaxed) {
        movement_workers(managed_clients.clone(), shutdown.clone(), cadence).await;
    }
    let mut voice_running = false;
    if let Some(voice_library) = voice_library.take() {
        if !shutdown.load(Ordering::Relaxed) {
            info!(
                "voice simulation enabled: folder={} speaker_percent={} hearing_distance={} frame_duration_ms={} ffmpeg_reencode={}",
                config.voice_audio_folder,
                config.voice_speaker_percent,
                config.voice_hearing_distance,
                config.voice_frame_duration_ms,
                voice_reencode
            );
            tokio::spawn(voice_workers(
                managed_clients.clone(),
                config.clone(),
                voice_library,
                shutdown.clone(),
                cadence,
            ));
            voice_running = true;
        }
    }
    if !args.no_reconnect && !shutdown.load(Ordering::Relaxed) {
        tokio::spawn(random_reconnect_loop(
            managed_clients.clone(),
            config.clone(),
            shutdown.clone(),
            spawn_layout,
            args.reconnect_min_secs,
            args.reconnect_max_secs,
            maintenance.clone(),
        ));
    }

    let (commands_tx, mut commands_rx) = mpsc::unbounded_channel();
    tokio::spawn(console_input(commands_tx));
    let mut console_closed = false;
    let mut quit_batch_size = args.quit_batch_size.max(1);
    let mut quit_batch_delay = Duration::from_millis(args.quit_batch_delay_ms);
    let duration_deadline = args
        .duration_secs
        .map(|duration| time::Instant::now() + Duration::from_secs(duration));

    print_console_help();
    while !shutdown.load(Ordering::Relaxed) {
        if duration_deadline
            .map(|deadline| time::Instant::now() >= deadline)
            .unwrap_or(false)
        {
            shutdown.store(true, Ordering::SeqCst);
            break;
        }

        tokio::select! {
            command = commands_rx.recv(), if !console_closed => {
                match command {
                    Some(ConsoleCommand::EnableVoice) => {
                        if voice_running {
                            info!("voice simulation is already enabled");
                        } else {
                            config.voice_enabled = true;
                            match VoiceLibrary::load(
                                &config.voice_audio_folder,
                                voice_reencode,
                                config.voice_frame_duration_ms,
                            ) {
                                Ok(Some(library)) => {
                                    info!("voice simulation enabled from console");
                                    tokio::spawn(voice_workers(
                                        managed_clients.clone(),
                                        config.clone(),
                                        Arc::new(library),
                                        shutdown.clone(),
                                        cadence,
                                    ));
                                    voice_running = true;
                                }
                                Ok(None) => warn!("voice command ignored: no usable audio files found"),
                                Err(err) => warn!("voice command failed: {err:#}"),
                            }
                        }
                    }
                    Some(ConsoleCommand::AddClients(count)) => {
                        info!("adding {count} clients from console");
                        match add_clients(
                            &managed_clients,
                            &config,
                            count,
                            spawn_layout,
                            connect,
                            &maintenance,
                            &shutdown,
                        ).await {
                            Ok(added) => {
                                config.client_count = config.client_count.saturating_add(added);
                                info!("added {added} clients; population target is now {}", config.client_count);
                            }
                            Err(err) => {
                                config.client_count = managed_clients.lock().await.len();
                                warn!(
                                    "failed to add clients: {err:#}; population target is {}",
                                    config.client_count
                                );
                            }
                        }
                    }
                    Some(ConsoleCommand::Quit { batch_size, delay_ms }) => {
                        if let Some(batch_size) = batch_size {
                            quit_batch_size = batch_size.max(1);
                        }
                        if let Some(delay_ms) = delay_ms {
                            quit_batch_delay = Duration::from_millis(delay_ms);
                        }
                        info!(
                            "shutdown requested from console (batch_size={} delay_ms={})",
                            quit_batch_size,
                            quit_batch_delay.as_millis()
                        );
                        shutdown.store(true, Ordering::SeqCst);
                    }
                    Some(ConsoleCommand::Help) => print_console_help(),
                    None => console_closed = true,
                }
            }
            _ = time::sleep(Duration::from_millis(100)) => {}
        }
    }
    shutdown.store(true, Ordering::SeqCst);
    info!(
        "shutting down clients in batches of {} ({}ms between batches)",
        quit_batch_size,
        quit_batch_delay.as_millis()
    );
    disconnect_clients_in_batches(&managed_clients, quit_batch_size, quit_batch_delay).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::Signature;
    use flate2::read::DeflateDecoder;
    use std::io::Read;

    async fn test_client(index: usize, server_addr: SocketAddr) -> Arc<BasisClient> {
        let socket = bind_udp_socket(any_local_addr(server_addr)).unwrap();
        socket.connect(server_addr).await.unwrap();
        Arc::new(BasisClient {
            index,
            socket: Arc::new(socket),
            server_addr,
            connect_time: 0,
            connection_number: 0,
            local_peer_id: index as i32,
            remote_peer_id: Mutex::new(None),
            connected: AtomicBool::new(true),
            in_use: AtomicBool::new(true),
            intentional_reconnect: AtomicBool::new(false),
            movement_sequence: AtomicU8::new(0),
            voice_sequence: AtomicU8::new(0),
            reliable_sequences: std::array::from_fn(|_| AtomicU16::new(0)),
            fragment_id: AtomicU16::new(0),
            ping_sequence: AtomicU16::new(0),
            pending_reliable: Mutex::new(VecDeque::new()),
            pending_reliable_active: AtomicBool::new(false),
            shared_receive: AtomicBool::new(false),
            shared_receive_eligible: AtomicBool::new(false),
            receive_shutdown: Notify::new(),
            received_reliable: StdMutex::new(ReliableReceiveState::default()),
            pose: Mutex::new(PoseState::new_at([0.0; 3])),
            identity: Identity::random(),
        })
    }

    #[test]
    fn connection_payload_starts_with_v55_application_auth_and_ready() {
        let config = Config::default();
        let ready = ReadyMessage::new(&config, [0.0, 0.0, 0.0]).unwrap();
        let payload = build_connection_payload(&config, &ready);
        assert_eq!(SERVER_VERSION, 55);
        assert_eq!(&payload[0..2], &SERVER_VERSION.to_le_bytes());
        assert_eq!(payload[2], 1);
        let auth_len = u16::from_le_bytes([payload[3], payload[4]]) as usize;
        assert_eq!(&payload[5..5 + auth_len], b"default_password");
        assert!(payload.len() > 5 + auth_len);
    }

    #[test]
    fn connection_payload_supports_raw_application_names() {
        let config = Config {
            company_name: "Custom Company".to_string(),
            product_name: "Custom Product".to_string(),
            ..Config::default()
        };
        let ready = ReadyMessage::new(&config, [0.0, 0.0, 0.0]).unwrap();
        let payload = build_connection_payload(&config, &ready);

        let mut reader = ProtocolNetReader::new(&payload);
        assert_eq!(reader.get_u16().unwrap(), SERVER_VERSION);
        let application = NetworkApplication::try_read(&mut reader).unwrap();
        assert_eq!(application.company_name, "Custom Company");
        assert_eq!(application.product_name, "Custom Product");
        assert_eq!(reader.get_bytes_with_length().unwrap(), b"default_password");
    }

    #[test]
    fn voice_config_defaults_are_disabled_and_conservative() {
        let config = Config::default();
        assert!(!config.voice_enabled);
        assert_eq!(config.voice_audio_folder, "audio");
        assert_eq!(config.voice_speaker_percent, 10);
        assert_eq!(config.voice_hearing_distance, 25.0);
        assert_eq!(config.voice_frame_duration_ms, 20);
    }

    #[test]
    fn voice_config_parses_xml_fields() {
        let raw = quick_xml::de::from_str::<RawConfig>(
            r#"<Configuration>
                <VoiceEnabled>true</VoiceEnabled>
                <VoiceAudioFolder>samples</VoiceAudioFolder>
                <VoiceSpeakerPercent>35</VoiceSpeakerPercent>
                <VoiceHearingDistance>12.5</VoiceHearingDistance>
                <VoiceFrameDurationMs>40</VoiceFrameDurationMs>
            </Configuration>"#,
        )
        .unwrap();
        let config = Config::from_raw(raw);
        assert!(config.voice_enabled);
        assert_eq!(config.voice_audio_folder, "samples");
        assert_eq!(config.voice_speaker_percent, 35);
        assert_eq!(config.voice_hearing_distance, 12.5);
        assert_eq!(config.voice_frame_duration_ms, 40);
    }

    #[test]
    fn invalid_voice_config_values_fall_back_or_clamp() {
        let config = Config::from_raw(RawConfig {
            voice_speaker_percent: Some("250".to_string()),
            voice_hearing_distance: Some("NaN".to_string()),
            voice_frame_duration_ms: Some("0".to_string()),
            ..RawConfig::default()
        });
        assert_eq!(config.voice_speaker_percent, 100);
        assert_eq!(
            config.voice_hearing_distance,
            DEFAULT_VOICE_HEARING_DISTANCE
        );
        assert_eq!(
            config.voice_frame_duration_ms,
            DEFAULT_VOICE_FRAME_DURATION_MS
        );
    }

    #[test]
    fn relative_voice_audio_folder_resolves_from_config_directory() {
        let resolved = resolve_relative_to_config(
            Path::new("/work/BasisRustClient/Config.xml"),
            DEFAULT_VOICE_AUDIO_FOLDER,
        );
        assert!(resolved.ends_with("BasisRustClient/audio"));

        let absolute = resolve_relative_to_config(
            Path::new("/work/BasisRustClient/Config.xml"),
            "/samples/voice",
        );
        assert_eq!(absolute, "/samples/voice");
    }

    #[test]
    fn voice_recipient_serialization_uses_expected_wire_formats() {
        assert_eq!(
            serialize_voice_recipients_small(&[7, 300]),
            vec![2, 7, 0, 44, 1]
        );

        let large = serialize_voice_recipients_large(&[7, 300]);
        assert_eq!(large, vec![2, 0, 7, 0, 44, 1]);
    }

    #[test]
    fn audio_segment_serialization_matches_unity_wire_header() {
        assert_eq!(
            serialize_audio_segment(9, &[0xaa, 0xbb, 0xcc]),
            vec![9, 0, 0xaa, 0xbb, 0xcc]
        );
    }

    #[test]
    fn voice_distance_filter_includes_boundary() {
        assert!(distance_within([0.0, 0.0, 0.0], [25.0, 0.0, 0.0], 25.0));
        assert!(!distance_within([0.0, 0.0, 0.0], [25.01, 0.0, 0.0], 25.0));
    }

    #[test]
    fn ogg_opus_parser_skips_headers_and_extracts_audio_packets() {
        let mut bytes = Vec::new();
        bytes.extend(build_ogg_page(&[
            b"OpusHead".as_slice(),
            b"OpusTags".as_slice(),
            b"abc".as_slice(),
        ]));
        let packets = OggOpusPackets::parse(&bytes).unwrap();
        assert_eq!(packets.packets[0].data, b"abc".to_vec());
    }

    #[test]
    fn ogg_opus_parser_reconstructs_packet_across_pages() {
        let mut bytes = Vec::new();
        bytes.extend(build_ogg_page_from_segments(&[255], &[1; 255]));
        bytes.extend(build_ogg_page_from_segments(&[3], &[2, 3, 4]));
        let packets = OggOpusPackets::parse(&bytes).unwrap();
        let mut expected = vec![1; 255];
        expected.extend_from_slice(&[2, 3, 4]);
        assert_eq!(packets.packets[0].data, expected);
    }

    #[test]
    fn ogg_opus_parser_rejects_malformed_pages() {
        assert!(OggOpusPackets::parse(b"not ogg").is_err());
    }

    #[test]
    fn opus_packet_duration_parses_common_frame_sizes() {
        assert_eq!(opus_packet_duration_ms(&[0x08]), Some(20));
        assert_eq!(opus_packet_duration_ms(&[0x10]), Some(40));
        assert_eq!(opus_packet_duration_ms(&[0x18]), Some(60));
        assert_eq!(opus_packet_duration_ms(&[0x09]), Some(40));
        assert_eq!(opus_packet_duration_ms(&[0x0b, 0x03]), Some(60));
        assert_eq!(opus_packet_duration_ms(&[0xf8]), Some(20));
    }

    #[test]
    fn voice_speaker_target_uses_ceil_and_clamping() {
        assert_eq!(voice_speaker_target(250, 10), 25);
        assert_eq!(voice_speaker_target(1, 10), 1);
        assert_eq!(voice_speaker_target(250, 0), 0);
        assert_eq!(voice_speaker_target(3, 250), 3);
    }

    #[test]
    fn eof_replacement_prefers_another_peer_when_available() {
        let connected = [1, 2, 3];
        let active = HashSet::new();
        let avoid = HashSet::from([1]);
        let replacement = choose_next_speaker(&connected, &active, &avoid).unwrap();
        assert_ne!(replacement, 1);
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
        let mut reader = ProtocolNetReader::new(&bytes);
        let decoded = ProtocolClientMetaDataMessage::deserialize(&mut reader).unwrap();
        assert_eq!(decoded.player_uuid, "Failure");
        assert_eq!(decoded.player_display_name, "Failure");
        assert_eq!(decoded.player_platform, "Failure");
    }

    #[test]
    fn avatar_network_load_deflates_raw_len_strings() {
        let encoded = encode_avatar_network_load("http://localhost/avatar", "pw").unwrap();
        let mut decoder = DeflateDecoder::new(encoded.as_slice());
        let mut raw = Vec::new();
        decoder.read_to_end(&mut raw).unwrap();
        let (url, next) = read_raw_len_string(&raw, 0);
        let (pw, next) = read_raw_len_string(&raw, next);
        let (version_tag, _) = read_raw_len_string(&raw, next);
        assert_eq!(url, "http://localhost/avatar");
        assert_eq!(pw, "pw");
        assert_eq!(version_tag, "");
    }

    #[test]
    fn default_avatar_is_basis_loading_avatar() {
        let config = Config::default();
        let message = ClientAvatarChangeMessage::new(&config).unwrap();
        assert_eq!(message.load_mode, 1);

        let mut decoder = DeflateDecoder::new(message.byte_array.as_slice());
        let mut raw = Vec::new();
        decoder.read_to_end(&mut raw).unwrap();
        let (url, next) = read_raw_len_string(&raw, 0);
        let (unlock_password, next) = read_raw_len_string(&raw, next);
        let (version_tag, _) = read_raw_len_string(&raw, next);
        assert_eq!(url, "LoadingAvatar");
        assert_eq!(unlock_password, "N/A");
        assert_eq!(version_tag, "");
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
    fn console_commands_default_to_voice_add_100_and_batched_quit() {
        assert_eq!(
            parse_console_command("voice"),
            Some(ConsoleCommand::EnableVoice)
        );
        assert_eq!(
            parse_console_command("enable voice"),
            Some(ConsoleCommand::EnableVoice)
        );
        assert_eq!(
            parse_console_command("add"),
            Some(ConsoleCommand::AddClients(100))
        );
        assert_eq!(
            parse_console_command("add 250"),
            Some(ConsoleCommand::AddClients(250))
        );
        assert_eq!(
            parse_console_command("quit 25 500"),
            Some(ConsoleCommand::Quit {
                batch_size: Some(25),
                delay_ms: Some(500),
            })
        );
        assert_eq!(
            parse_console_command("q"),
            Some(ConsoleCommand::Quit {
                batch_size: None,
                delay_ms: None,
            })
        );
        assert_eq!(parse_console_command("unknown"), None);
    }

    #[test]
    fn randomized_cadence_is_default_and_sync_batching_is_opt_in() {
        let defaults = Args::try_parse_from(["basis-rust-client"]).unwrap();
        assert!(!defaults.sync_batching);
        assert_eq!(defaults.movement_jitter_percent, 10);
        assert_eq!(defaults.voice_jitter_percent, 5);

        let synchronized = Args::try_parse_from(["basis-rust-client", "--sync-batching"]).unwrap();
        assert!(synchronized.sync_batching);
    }

    #[test]
    fn cadence_jitter_is_bounded_and_preserves_long_run_mean() {
        let base = Duration::from_millis(50);
        let mut state = cadence_seed(42, 0x4d4f_5645_4d45_4e54);
        let mut total_us = 0u128;
        for _ in 0..100_000 {
            let interval = jittered_duration(base, 10, &mut state);
            let us = interval.as_micros();
            assert!((45_000..=55_000).contains(&us));
            total_us += us;
        }
        let mean_us = total_us as f64 / 100_000.0;
        assert!((49_900.0..=50_100.0).contains(&mean_us));
    }

    #[test]
    fn high_quality_payload_and_movement_packet_sizes_match() {
        let mut pose = PoseState::new_random();
        let payload = pose.high_quality_payload(0.0);
        assert_eq!(payload.len(), 159);
        assert_eq!(u16::from_le_bytes([payload[103], payload[104]]), 0x4000);
        assert_eq!(&payload[112..117], &[0, 0, 0, 0, 0]);
        assert_eq!(&payload[124..159], &[0; 35]);
        let packet = build_movement_packet(7, &mut pose, SystemTime::now());
        assert_eq!(packet.len(), 160);
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
        let decode_axis = |bytes: &[u8]| {
            let raw = (bytes[0] as i32) | ((bytes[1] as i32) << 8) | ((bytes[2] as i32) << 16);
            ((raw << 8) >> 8) as f32 * 0.001
        };
        let x = decode_axis(&payload[0..3]);
        let y = decode_axis(&payload[3..6]);
        let z = decode_axis(&payload[6..9]);

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
    fn shared_ping_buckets_visit_each_population_once_per_interval() {
        for population in [1, 14, 15, 1000] {
            let mut visits = vec![0usize; population];
            let mut bucket_total = 0;
            for tick in 0..PING_INTERVAL_TICKS {
                let bucket_count = (0..population)
                    .filter(|slot| ping_bucket_matches(*slot, tick))
                    .count();
                assert!(bucket_count <= population.div_ceil(PING_INTERVAL_TICKS));
                bucket_total += bucket_count;
                for (slot, visits) in visits.iter_mut().enumerate() {
                    if ping_bucket_matches(slot, tick) {
                        *visits += 1;
                    }
                }
            }
            assert_eq!(bucket_total, population);
            assert!(visits.iter().all(|visits| *visits == 1));
        }
    }

    #[test]
    fn reliable_receive_suppresses_duplicate_sequences() {
        let mut state = ReliableReceiveState::default();
        assert!(state.mark_new(7, 10));
        assert!(!state.mark_new(7, 10));
        assert!(state.mark_new(7, 11));
        assert!(!state.mark_new(7, 11));
    }

    #[test]
    fn reliable_receive_accepts_32767_to_0_wrap() {
        let mut state = ReliableReceiveState::default();
        assert!(state.mark_new(7, MAX_SEQUENCE - 1));
        assert!(state.mark_new(7, 0));
        assert!(!state.mark_new(7, 0));
    }

    #[tokio::test]
    async fn reliable_sequences_are_independent_per_channel() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = test_client(0, server.local_addr().unwrap()).await;
        let first_channel = 1;
        let second_channel = 2;
        client
            .send_reliable_ordered(first_channel, b"first")
            .await
            .unwrap();
        client
            .send_reliable_ordered(second_channel, b"second")
            .await
            .unwrap();

        let mut datagrams = Vec::new();
        for _ in 0..2 {
            let mut buffer = [0u8; LITENETLIB_INITIAL_MTU];
            let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            datagrams.push(buffer[..len].to_vec());
        }
        let mut sequences = HashMap::new();
        for datagram in datagrams {
            let packet = parse_packet(&datagram).unwrap();
            sequences.insert(packet.channel_id.unwrap(), packet.sequence.unwrap());
        }
        assert_eq!(
            sequences.get(&DeliveryMethod::channel_id(
                first_channel,
                DeliveryMethod::ReliableOrdered,
            )),
            Some(&0)
        );
        assert_eq!(
            sequences.get(&DeliveryMethod::channel_id(
                second_channel,
                DeliveryMethod::ReliableOrdered,
            )),
            Some(&0)
        );
    }

    #[tokio::test]
    async fn reliable_packets_are_enqueued_before_first_datagram() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = test_client(0, server.local_addr().unwrap()).await;
        let pending_guard = client.pending_reliable.lock().await;
        let sending = {
            let client = client.clone();
            tokio::spawn(async move { client.send_reliable_ordered(1, b"queued").await })
        };
        tokio::task::yield_now().await;

        let mut buffer = [0u8; LITENETLIB_INITIAL_MTU];
        assert!(
            time::timeout(Duration::from_millis(50), server.recv(&mut buffer))
                .await
                .is_err()
        );

        drop(pending_guard);
        let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        sending.await.unwrap().unwrap();
        assert_eq!(parse_packet(&buffer[..len]).unwrap().payload, b"queued");
    }

    #[tokio::test]
    async fn fragmented_reliable_matches_litenetlib_wire_and_ack_lifecycle() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = test_client(0, server.local_addr().unwrap()).await;

        let boundary_channel = 2;
        let boundary = vec![0x5a; LITENETLIB_INITIAL_MTU - LITENETLIB_CHANNELED_HEADER_SIZE];
        client
            .send_reliable_ordered(boundary_channel, &boundary)
            .await
            .unwrap();
        let mut buffer = [0u8; LITENETLIB_INITIAL_MTU];
        let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(len, LITENETLIB_INITIAL_MTU);
        assert_eq!(buffer[0] & 0x80, 0);
        assert_eq!(parse_packet(&buffer[..len]).unwrap().payload, boundary);
        let boundary_channel_id =
            DeliveryMethod::channel_id(boundary_channel, DeliveryMethod::ReliableOrdered);
        client.process_ack(boundary_channel_id, 0, &[1]).await;

        let channel = 1;
        let channel_id = DeliveryMethod::channel_id(channel, DeliveryMethod::ReliableOrdered);
        let payload_len = RELIABLE_FRAGMENT_PAYLOAD_SIZE * 2 + 1;
        let payload = (0..payload_len)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        client
            .send_reliable_ordered(channel, &payload)
            .await
            .unwrap();

        assert_eq!(
            LITENETLIB_FRAGMENTED_HEADER_SIZE + RELIABLE_FRAGMENT_PAYLOAD_SIZE,
            LITENETLIB_INITIAL_MTU
        );
        let mut fragments = Vec::new();
        for _ in 0..3 {
            let len = time::timeout(Duration::from_secs(1), server.recv(&mut buffer))
                .await
                .unwrap()
                .unwrap();
            assert!(len <= LITENETLIB_INITIAL_MTU);
            let packet = parse_packet(&buffer[..len]).unwrap();
            assert_eq!(packet.property, PacketProperty::Channeled);
            assert_eq!(packet.channel_id, Some(channel_id));
            assert_eq!(buffer[0] & 0x80, 0x80);
            assert!(packet.payload.len() >= LITENETLIB_FRAGMENT_HEADER_SIZE);
            let fragment_id = u16::from_le_bytes([packet.payload[0], packet.payload[1]]);
            let part = u16::from_le_bytes([packet.payload[2], packet.payload[3]]);
            let total = u16::from_le_bytes([packet.payload[4], packet.payload[5]]);
            fragments.push((
                part,
                total,
                fragment_id,
                packet.sequence.unwrap(),
                packet.payload[6..].to_vec(),
            ));
        }
        fragments.sort_by_key(|fragment| fragment.0);
        assert_eq!(fragments.len(), 3);
        let fragment_id = fragments[0].2;
        let mut reassembled = Vec::new();
        for (part, total, id, sequence, bytes) in &fragments {
            assert_eq!(*id, fragment_id);
            assert_eq!(*total, 3);
            assert_eq!(
                *part as usize,
                reassembled.len() / RELIABLE_FRAGMENT_PAYLOAD_SIZE
            );
            assert_eq!(*sequence, *part);
            reassembled.extend_from_slice(bytes);
        }
        assert_eq!(reassembled, payload);

        let mut middle_ack = vec![0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
        middle_ack[0] |= 1 << 1;
        client.process_ack(channel_id, 1, &middle_ack).await;
        assert_eq!(client.pending_reliable.lock().await.len(), 2);
        assert!(client.pending_reliable_active.load(Ordering::Relaxed));

        let mut final_ack = vec![0u8; (DEFAULT_WINDOW_SIZE - 1) / 8 + 2];
        final_ack[0] |= 1;
        final_ack[0] |= 1 << 2;
        client.process_ack(channel_id, 0, &final_ack).await;
        assert!(client.pending_reliable.lock().await.is_empty());
        assert!(!client.pending_reliable_active.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn batch_publish_preserves_dense_indices_and_notifies_complete_snapshot() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let managed = Arc::new(Mutex::new(Vec::new()));
        let refresh = Arc::new(Notify::new());
        let maintenance = MaintenanceOptions {
            shared: true,
            refresh: refresh.clone(),
        };
        let batch = vec![
            test_client(0, server_addr).await,
            test_client(1, server_addr).await,
            test_client(2, server_addr).await,
        ];
        let notified = refresh.notified();
        publish_client_batch(&managed, 0, &batch, &maintenance)
            .await
            .unwrap();
        time::timeout(Duration::from_secs(1), notified)
            .await
            .unwrap();
        let snapshot = managed.lock().await;
        assert_eq!(
            snapshot
                .iter()
                .map(|client| client.index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
    }

    #[tokio::test]
    async fn stale_reconnect_cannot_replace_or_notify() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let old = test_client(0, server_addr).await;
        let current = test_client(0, server_addr).await;
        let replacement = test_client(0, server_addr).await;
        let managed = Arc::new(Mutex::new(vec![current.clone()]));
        let refresh = Arc::new(Notify::new());
        let maintenance = MaintenanceOptions {
            shared: true,
            refresh: refresh.clone(),
        };
        let notified = refresh.notified();

        assert!(!replace_client_if_current(&managed, 0, &old, &replacement, &maintenance).await);
        assert!(time::timeout(Duration::from_millis(50), notified)
            .await
            .is_err());
        assert!(Arc::ptr_eq(&managed.lock().await[0], &current));
    }

    #[tokio::test]
    async fn replacement_releases_the_old_blocked_receive_task() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let old = test_client(0, server_addr).await;
        let replacement = test_client(0, server_addr).await;
        let managed = Arc::new(Mutex::new(vec![old.clone()]));
        let maintenance = MaintenanceOptions {
            shared: false,
            refresh: Arc::new(Notify::new()),
        };
        let weak = Arc::downgrade(&old);
        let receiver = tokio::spawn(old.clone().receive_loop());
        tokio::task::yield_now().await;

        assert!(replace_client_if_current(&managed, 0, &old, &replacement, &maintenance).await);
        time::timeout(Duration::from_secs(1), receiver)
            .await
            .expect("old receive task remained blocked")
            .unwrap()
            .unwrap();
        assert!(!old.in_use.load(Ordering::Relaxed));
        assert!(!old.connected.load(Ordering::Relaxed));
        assert!(Arc::ptr_eq(&managed.lock().await[0], &replacement));

        drop(old);
        assert!(weak.upgrade().is_none());
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn connect_accept_filters_non_observers_only() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let observer = test_client(0, server_addr).await;
        let load_sink = test_client(1, server_addr).await;
        let mut accept = vec![0u8; 15];
        accept[0] = PacketProperty::ConnectAccept as u8;
        accept[1..9].copy_from_slice(&0i64.to_le_bytes());
        accept[11..15].copy_from_slice(&1i32.to_le_bytes());
        observer.handle_packet(&accept).await.unwrap();
        load_sink.handle_packet(&accept).await.unwrap();

        let unreliable = vec![
            PacketProperty::Unreliable as u8,
            channels::PLAYER_AVATAR_HIGH,
            1,
            2,
            3,
        ];
        server
            .send_to(&unreliable, observer.socket.local_addr().unwrap())
            .await
            .unwrap();
        server
            .send_to(&unreliable, load_sink.socket.local_addr().unwrap())
            .await
            .unwrap();
        let mut buffer = [0u8; 64];
        let observer_len = time::timeout(Duration::from_secs(1), observer.socket.recv(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buffer[..observer_len], unreliable);
        assert!(time::timeout(
            Duration::from_millis(50),
            load_sink.socket.recv(&mut buffer)
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn pending_reliable_flag_tracks_enqueue_and_final_ack() {
        let server = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let server_addr = server.local_addr().unwrap();
        let client = test_client(0, server_addr).await;
        let channel_id = DeliveryMethod::channel_id(1, DeliveryMethod::ReliableOrdered);

        client.send_reliable_ordered(1, b"pending").await.unwrap();
        assert!(client.pending_reliable_active.load(Ordering::Relaxed));
        assert_eq!(client.pending_reliable.lock().await.len(), 1);

        client.process_ack(channel_id, 0, &[1]).await;
        assert!(!client.pending_reliable_active.load(Ordering::Relaxed));
        assert!(client.pending_reliable.lock().await.is_empty());
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

    fn build_ogg_page(packets: &[&[u8]]) -> Vec<u8> {
        let mut segments = Vec::new();
        let mut data = Vec::new();
        for packet in packets {
            assert!(packet.len() < 255);
            segments.push(packet.len() as u8);
            data.extend_from_slice(packet);
        }
        build_ogg_page_from_segments(&segments, &data)
    }

    fn build_ogg_page_from_segments(segments: &[u8], data: &[u8]) -> Vec<u8> {
        let mut page = vec![0; 27];
        page[0..4].copy_from_slice(b"OggS");
        page[26] = segments.len() as u8;
        page.extend_from_slice(segments);
        page.extend_from_slice(data);
        page
    }

    fn read_raw_len_string(bytes: &[u8], offset: usize) -> (String, usize) {
        let len = u16::from_le_bytes([bytes[offset], bytes[offset + 1]]) as usize;
        (
            String::from_utf8(bytes[offset + 2..offset + 2 + len].to_vec()).unwrap(),
            offset + 2 + len,
        )
    }
}
