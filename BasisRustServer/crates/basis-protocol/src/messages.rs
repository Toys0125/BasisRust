use crate::{
    avatar::BitQuality,
    io::{NetReader, NetWriter, Result as ReadResult},
    permissions,
};
use flate2::{read::DeflateDecoder, write::DeflateEncoder, Compression};
use serde_json::{Map, Number, Value};
use std::io::{Read, Write};
use uuid::Uuid;

pub trait BasisSerialize {
    fn serialize(&self, writer: &mut NetWriter);
}

pub trait BasisDeserialize: Sized {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BytesMessage {
    pub data: Vec<u8>,
}

impl BasisSerialize for BytesMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.data.len() as u16);
        writer.put_bytes(&self.data);
    }
}

impl BasisDeserialize for BytesMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let len = reader.get_u16()? as usize;
        Ok(Self {
            data: reader.get_bytes(len)?.to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerIdMessage {
    pub player_id: u16,
}

impl BasisSerialize for PlayerIdMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
    }
}

impl BasisDeserialize for PlayerIdMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientMetaDataMessage {
    pub player_uuid: String,
    pub player_display_name: String,
    pub player_platform: String,
}

impl BasisSerialize for ClientMetaDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        write_compact_id(writer, non_empty_or_failure(&self.player_uuid));
        writer.put_string(non_empty_or_failure(&self.player_display_name));
        write_platform(writer, non_empty_or_failure(&self.player_platform));
    }
}

impl BasisDeserialize for ClientMetaDataMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_uuid: read_compact_id(reader)?,
            player_display_name: reader.get_string()?,
            player_platform: read_platform(reader)?,
        })
    }
}

fn non_empty_or_failure(value: &str) -> &str {
    if value.is_empty() {
        "Failure"
    } else {
        value
    }
}

const COMPACT_ID_RAW: u8 = 0;
const COMPACT_ID_UUID: u8 = 1;
const COMPACT_ID_U64: u8 = 2;
const COMPACT_ID_HEX: u8 = 3;
const COMPACT_ID_DID_KEY: u8 = 4;
const DID_KEY_PREFIX: &str = "did:key:";

fn write_compact_id(writer: &mut NetWriter, value: &str) {
    if let Ok(uuid) = Uuid::parse_str(value) {
        if value.len() == 32 || value.len() == 36 {
            let format = if value.len() == 36 {
                if value.bytes().any(|b| matches!(b, b'A'..=b'F')) {
                    1
                } else {
                    0
                }
            } else if value.bytes().any(|b| matches!(b, b'A'..=b'F')) {
                3
            } else {
                2
            };
            let rendered = match format {
                1 => uuid.hyphenated().to_string().to_uppercase(),
                2 => uuid.simple().to_string(),
                3 => uuid.simple().to_string().to_uppercase(),
                _ => uuid.hyphenated().to_string(),
            };
            if rendered == value {
                writer.put_u8(COMPACT_ID_UUID);
                writer.put_u8(format);
                writer.put_bytes(&uuid.to_bytes_le());
                return;
            }
        }
    }

    if !value.is_empty() && value.len() <= 20 && value.bytes().all(|b| b.is_ascii_digit()) {
        if let Ok(parsed) = value.parse::<u64>() {
            if parsed.to_string() == value {
                writer.put_u8(COMPACT_ID_U64);
                writer.put_u64(parsed);
                return;
            }
        }
    }

    if let Some(body) = value.strip_prefix(DID_KEY_PREFIX) {
        let bytes = body.as_bytes();
        if !bytes.is_empty() && bytes.len() <= u8::MAX as usize {
            writer.put_u8(COMPACT_ID_DID_KEY);
            writer.put_u8(bytes.len() as u8);
            writer.put_bytes(bytes);
            return;
        }
    }

    let is_hex = !value.is_empty()
        && value.len() <= 510
        && value.len().is_multiple_of(2)
        && value.bytes().all(|b| b.is_ascii_hexdigit());
    if is_hex {
        let has_upper = value.bytes().any(|b| matches!(b, b'A'..=b'F'));
        let has_lower = value.bytes().any(|b| matches!(b, b'a'..=b'f'));
        if !(has_upper && has_lower) {
            let mut bytes = Vec::with_capacity(value.len() / 2);
            for chunk in value.as_bytes().chunks_exact(2) {
                let pair = std::str::from_utf8(chunk).expect("hex is ASCII");
                bytes.push(u8::from_str_radix(pair, 16).expect("validated hex"));
            }
            writer.put_u8(COMPACT_ID_HEX);
            writer.put_u8(u8::from(has_upper));
            writer.put_u8(bytes.len() as u8);
            writer.put_bytes(&bytes);
            return;
        }
    }

    writer.put_u8(COMPACT_ID_RAW);
    writer.put_string(value);
}

fn read_compact_id(reader: &mut NetReader<'_>) -> ReadResult<String> {
    match reader.get_u8()? {
        COMPACT_ID_UUID => {
            let format = reader.get_u8()?;
            let raw: [u8; 16] = reader.get_bytes(16)?.try_into().expect("length checked");
            let uuid = Uuid::from_bytes_le(raw);
            Ok(match format {
                1 => uuid.hyphenated().to_string().to_uppercase(),
                2 => uuid.simple().to_string(),
                3 => uuid.simple().to_string().to_uppercase(),
                _ => uuid.hyphenated().to_string(),
            })
        }
        COMPACT_ID_U64 => Ok(reader.get_u64()?.to_string()),
        COMPACT_ID_HEX => {
            let upper = reader.get_u8()? & 1 != 0;
            let len = reader.get_u8()? as usize;
            let bytes = reader.get_bytes(len)?;
            let alphabet = if upper {
                b"0123456789ABCDEF"
            } else {
                b"0123456789abcdef"
            };
            let mut out = String::with_capacity(len * 2);
            for &byte in bytes {
                out.push(alphabet[(byte >> 4) as usize] as char);
                out.push(alphabet[(byte & 0x0f) as usize] as char);
            }
            Ok(out)
        }
        COMPACT_ID_DID_KEY => {
            let len = reader.get_u8()? as usize;
            let body = String::from_utf8_lossy(reader.get_bytes(len)?).into_owned();
            Ok(format!("{DID_KEY_PREFIX}{body}"))
        }
        _ => reader.get_string(),
    }
}

const KNOWN_PLATFORMS: &[&str] = &[
    "WindowsPlayer",
    "WindowsEditor",
    "WindowsServer",
    "OSXPlayer",
    "OSXEditor",
    "OSXServer",
    "LinuxPlayer",
    "LinuxEditor",
    "LinuxServer",
    "Android",
    "IPhonePlayer",
    "VisionOS",
    "WebGLPlayer",
    "PS4",
    "PS5",
    "XboxOne",
    "GameCoreXboxOne",
    "GameCoreXboxSeries",
    "Switch",
    "tvOS",
    "WSAPlayerX86",
    "WSAPlayerX64",
    "WSAPlayerARM",
    "EmbeddedLinuxArm64",
    "EmbeddedLinuxArm32",
    "EmbeddedLinuxX64",
    "EmbeddedLinuxX86",
    "QNXArm32",
    "QNXArm64",
    "QNXX64",
    "QNXX86",
    "Stadia",
    "CloudRendering",
    "LinuxHeadlessSimulation",
    "Lumin",
    "Headless",
];

fn write_platform(writer: &mut NetWriter, value: &str) {
    if let Some(index) = KNOWN_PLATFORMS.iter().position(|known| *known == value) {
        writer.put_u8((index + 1) as u8);
    } else {
        writer.put_u8(0);
        writer.put_string(value);
    }
}

fn read_platform(reader: &mut NetReader<'_>) -> ReadResult<String> {
    let tag = reader.get_u8()?;
    if tag == 0 {
        return reader.get_string();
    }
    Ok(KNOWN_PLATFORMS
        .get(tag as usize - 1)
        .copied()
        .unwrap_or("")
        .to_owned())
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientAvatarChangeMessage {
    pub load_mode: u8,
    pub byte_array: Vec<u8>,
    pub local_avatar_index: u8,
    pub arm_scale: f32,
    pub leg_scale: f32,
    pub torso_scale: f32,
}

impl BasisSerialize for ClientAvatarChangeMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.load_mode);
        writer.put_u16(self.byte_array.len() as u16);
        writer.put_bytes(&self.byte_array);
        writer.put_u8(self.local_avatar_index);
        writer.put_u16(compress_fit_scale(self.arm_scale));
        writer.put_u16(compress_fit_scale(self.leg_scale));
        writer.put_u16(compress_fit_scale(self.torso_scale));
    }
}

impl BasisDeserialize for ClientAvatarChangeMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let load_mode = reader.get_u8()?;
        let len = reader.get_u16()? as usize;
        let byte_array = reader.get_bytes(len)?.to_vec();
        let local_avatar_index = reader.get_u8()?;
        let arm_scale = decompress_fit_scale(reader.get_u16()?);
        let leg_scale = decompress_fit_scale(reader.get_u16()?);
        let torso_scale = decompress_fit_scale(reader.get_u16()?);
        Ok(Self {
            load_mode,
            byte_array,
            local_avatar_index,
            arm_scale,
            leg_scale,
            torso_scale,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientBodyFitMessage {
    pub arm_scale: f32,
    pub leg_scale: f32,
    pub torso_scale: f32,
}

impl BasisSerialize for ClientBodyFitMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(compress_fit_scale(self.arm_scale));
        writer.put_u16(compress_fit_scale(self.leg_scale));
        writer.put_u16(compress_fit_scale(self.torso_scale));
    }
}

impl BasisDeserialize for ClientBodyFitMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            arm_scale: decompress_fit_scale(reader.get_u16()?),
            leg_scale: decompress_fit_scale(reader.get_u16()?),
            torso_scale: decompress_fit_scale(reader.get_u16()?),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerBodyFitMessage {
    pub player_id: u16,
    pub body_fit: ClientBodyFitMessage,
}

impl BasisSerialize for ServerBodyFitMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        self.body_fit.serialize(writer);
    }
}

impl BasisDeserialize for ServerBodyFitMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
            body_fit: ClientBodyFitMessage::deserialize(reader)?,
        })
    }
}

fn sanitize_fit_scale(value: f32) -> f32 {
    if !value.is_finite() || value <= 0.0 {
        1.0
    } else {
        value.clamp(0.5, 1.5)
    }
}

fn compress_fit_scale(value: f32) -> u16 {
    (((sanitize_fit_scale(value) - 0.5) * 65535.0) + 0.5) as u16
}

fn decompress_fit_scale(value: u16) -> f32 {
    value as f32 / 65535.0 + 0.5
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdditionalAvatarData {
    pub message_index: u8,
    pub data: Vec<u8>,
}

impl BasisSerialize for AdditionalAvatarData {
    fn serialize(&self, writer: &mut NetWriter) {
        let len = self.data.len().min(u8::MAX as usize);
        writer.put_u8(len as u8);
        if len > 0 {
            writer.put_u8(self.message_index);
            writer.put_bytes(&self.data[..len]);
        }
    }
}

impl BasisDeserialize for AdditionalAvatarData {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let len = reader.get_u8()? as usize;
        if len == 0 {
            return Ok(Self {
                message_index: 0,
                data: Vec::new(),
            });
        }
        let message_index = reader.get_u8()?;
        Ok(Self {
            message_index,
            data: reader.get_bytes(len)?.to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalAvatarSyncMessage {
    pub data_quality_level: u8,
    pub array: Vec<u8>,
    pub additional_avatar_datas: Vec<AdditionalAvatarData>,
    pub linked_avatar_index: u8,
}

impl LocalAvatarSyncMessage {
    pub fn empty_high() -> Self {
        Self {
            data_quality_level: BitQuality::High as u8,
            array: vec![0; BitQuality::High.payload_len()],
            additional_avatar_datas: Vec::new(),
            linked_avatar_index: 0,
        }
    }

    pub fn serialize_for_channel(&self, writer: &mut NetWriter, has_additional_data: bool) {
        writer.put_bytes(&self.array);
        if has_additional_data {
            writer.put_u8(self.additional_avatar_datas.len() as u8);
            writer.put_u8(self.linked_avatar_index);
            for item in &self.additional_avatar_datas {
                item.serialize(writer);
            }
        }
    }
}

impl BasisSerialize for LocalAvatarSyncMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.data_quality_level);
        writer.put_bytes(&self.array);
        writer.put_u8(self.additional_avatar_datas.len() as u8);
        if !self.additional_avatar_datas.is_empty() {
            writer.put_u8(self.linked_avatar_index);
            for item in &self.additional_avatar_datas {
                item.serialize(writer);
            }
        }
    }
}

impl BasisDeserialize for LocalAvatarSyncMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let data_quality_level = reader.get_u8()?;
        let payload_len = match data_quality_level {
            0 => BitQuality::VeryLow.payload_len(),
            1 => BitQuality::Low.payload_len(),
            2 => BitQuality::Medium.payload_len(),
            _ => BitQuality::High.payload_len(),
        };
        let array = reader.get_bytes(payload_len)?.to_vec();
        let count = reader.get_u8()? as usize;
        let linked_avatar_index = if count > 0 { reader.get_u8()? } else { 0 };
        let mut additional_avatar_datas = Vec::with_capacity(count);
        for _ in 0..count {
            additional_avatar_datas.push(AdditionalAvatarData::deserialize(reader)?);
        }
        Ok(Self {
            data_quality_level,
            array,
            additional_avatar_datas,
            linked_avatar_index,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReadyMessage {
    pub player_meta_data_message: ClientMetaDataMessage,
    pub client_avatar_change_message: ClientAvatarChangeMessage,
    pub local_avatar_sync_message: LocalAvatarSyncMessage,
}

impl BasisSerialize for ReadyMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        self.player_meta_data_message.serialize(writer);
        self.client_avatar_change_message.serialize(writer);
        self.local_avatar_sync_message.serialize(writer);
    }
}

impl BasisDeserialize for ReadyMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_meta_data_message: ClientMetaDataMessage::deserialize(reader)?,
            client_avatar_change_message: ClientAvatarChangeMessage::deserialize(reader)?,
            local_avatar_sync_message: LocalAvatarSyncMessage::deserialize(reader)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerMetaDataMessage {
    pub client_meta_data_message: ClientMetaDataMessage,
    pub sync_interval: i32,
    pub base_multiplier: i32,
    pub increase_rate: f32,
    pub slowest_send_rate: f32,
    pub peer_limit: i32,
    pub allowed_permissions: Vec<String>,
    pub denied_permissions: Vec<String>,
    pub uplink_delta_enabled: bool,
    pub image_share_egress_megabits_per_second: i32,
    pub image_pickup_range_meters: f32,
}

impl BasisSerialize for ServerMetaDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        self.client_meta_data_message.serialize(writer);
        writer.put_i32(self.sync_interval);
        writer.put_i32(self.base_multiplier);
        writer.put_f32(self.increase_rate);
        writer.put_f32(self.slowest_send_rate);
        writer.put_i32(self.peer_limit);
        let (bitset, extras) =
            encode_permission_wire(&self.allowed_permissions, &self.denied_permissions);
        writer.put_bytes_with_length(&bitset);
        writer.put_u16(extras.len() as u16);
        if !extras.is_empty() {
            writer.put_bytes_with_length(&compress_permission_extras(&extras));
        }
        writer.put_u8(u8::from(self.uplink_delta_enabled));
        writer.put_i32(self.image_share_egress_megabits_per_second);
        writer.put_f32(self.image_pickup_range_meters);
    }
}

const PERMISSION_WIRE_NODES: &[&str] = &[
    permissions::nodes::ALL,
    permissions::nodes::SERVER_STATS,
    permissions::nodes::RESOURCE_LOAD_WORLD,
    permissions::nodes::RESOURCE_UNLOAD_WORLD,
    permissions::nodes::RESOURCE_LOAD_PROP,
    permissions::nodes::RESOURCE_UNLOAD_PROP,
    permissions::nodes::RESOURCE_LOAD_AVATAR,
    permissions::nodes::RESOURCE_UNLOAD_AVATAR,
    permissions::nodes::OWNERSHIP_TRANSFER,
    permissions::nodes::OWNERSHIP_REMOVE,
    permissions::nodes::OWNERSHIP_GET,
    permissions::nodes::CONTENT_SHARE_DELETE,
    permissions::nodes::CONTENT_SHARE_CREATE,
    permissions::nodes::PROTECTION,
    permissions::nodes::CONFIGURATION_EDITOR,
    permissions::nodes::PLAYER_MODERATION,
    permissions::nodes::MODERATION_BAN,
    permissions::nodes::MODERATION_KICK,
    permissions::nodes::MODERATION_IP_BAN,
    permissions::nodes::MODERATION_UNBAN,
    permissions::nodes::MODERATION_UNBAN_IP,
    permissions::nodes::MODERATION_MESSAGE,
    permissions::nodes::MODERATION_MESSAGE_ALL,
    permissions::nodes::MODERATION_TELEPORT,
    permissions::nodes::MODERATION_SHOUT,
    permissions::nodes::PERMISSIONS_VIEW,
    permissions::nodes::PERMISSIONS_EDIT,
    permissions::nodes::MODERATION_HEADLESS_AUDIO,
];

fn encode_permission_wire(allowed: &[String], denied: &[String]) -> (Vec<u8>, Vec<String>) {
    let mut bitset = vec![0u8; (PERMISSION_WIRE_NODES.len() + 7) >> 3];
    let mut extras = Vec::new();
    let has_wildcard = allowed.iter().any(|node| node == permissions::nodes::ALL);

    for node in allowed {
        if let Some(index) = permission_wire_index(node) {
            bitset[index >> 3] |= 1 << (index & 7);
        } else {
            extras.push(node.clone());
        }
    }
    if has_wildcard {
        for index in 0..PERMISSION_WIRE_NODES.len() {
            bitset[index >> 3] |= 1 << (index & 7);
        }
    }
    for node in denied {
        if let Some(index) = permission_wire_index(node) {
            bitset[index >> 3] &= !(1 << (index & 7));
        }
    }
    (bitset, extras)
}

fn permission_wire_index(node: &str) -> Option<usize> {
    PERMISSION_WIRE_NODES
        .iter()
        .position(|known| known.eq_ignore_ascii_case(node))
}

fn compress_permission_extras(extras: &[String]) -> Vec<u8> {
    let raw = extras.join("\0").into_bytes();
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::best());
    if encoder.write_all(&raw).is_ok() {
        if let Ok(deflated) = encoder.finish() {
            if deflated.len() < raw.len() {
                let mut out = Vec::with_capacity(1 + deflated.len());
                out.push(1);
                out.extend_from_slice(&deflated);
                return out;
            }
        }
    }
    let mut out = Vec::with_capacity(1 + raw.len());
    out.push(0);
    out.extend_from_slice(&raw);
    out
}

pub fn decompress_permission_extras(data: &[u8], expected_count: usize) -> Vec<String> {
    const MAX_DECOMPRESSED_BYTES: usize = 1024 * 1024;
    if data.is_empty() || expected_count == 0 {
        return Vec::new();
    }
    let raw = if data[0] == 1 {
        let mut decoder = DeflateDecoder::new(&data[1..]);
        let mut output = Vec::new();
        let mut limited = decoder.by_ref().take((MAX_DECOMPRESSED_BYTES + 1) as u64);
        if limited.read_to_end(&mut output).is_err() || output.len() > MAX_DECOMPRESSED_BYTES {
            return Vec::new();
        }
        output
    } else {
        if data.len().saturating_sub(1) > MAX_DECOMPRESSED_BYTES {
            return Vec::new();
        }
        data[1..].to_vec()
    };
    String::from_utf8_lossy(&raw)
        .split('\0')
        .map(str::to_string)
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasisMessageDescriptor {
    pub id: u16,
    pub version: u8,
    pub channel: u8,
    pub flags: u8,
    pub name: String,
}

impl BasisSerialize for BasisMessageDescriptor {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.id);
        writer.put_u8(self.version);
        writer.put_u8(self.channel);
        writer.put_u8(self.flags);
        writer.put_string(&self.name);
    }
}

impl BasisDeserialize for BasisMessageDescriptor {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            id: reader.get_u16()?,
            version: reader.get_u8()?,
            channel: reader.get_u8()?,
            flags: reader.get_u8()?,
            name: reader.get_string()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasisMessageSupply {
    pub descriptors: Vec<BasisMessageDescriptor>,
}

impl BasisSerialize for BasisMessageSupply {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.descriptors.len() as u16);
        for descriptor in &self.descriptors {
            descriptor.serialize(writer);
        }
    }
}

impl BasisDeserialize for BasisMessageSupply {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let count = reader.get_u16()? as usize;
        let mut descriptors = Vec::with_capacity(count);
        for _ in 0..count {
            descriptors.push(BasisMessageDescriptor::deserialize(reader)?);
        }
        Ok(Self { descriptors })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasisMessageSubscribe {
    pub ids: Vec<u16>,
}

impl BasisSerialize for BasisMessageSubscribe {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.ids.len() as u16);
        for id in &self.ids {
            writer.put_u16(*id);
        }
    }
}

impl BasisDeserialize for BasisMessageSubscribe {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let count = reader.get_u16()? as usize;
        if count.saturating_mul(2) > reader.remaining() {
            return Err(crate::io::NetReadError::Underflow {
                needed: count.saturating_mul(2),
                remaining: reader.remaining(),
            });
        }
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(reader.get_u16()?);
        }
        Ok(Self { ids })
    }
}

pub fn core_message_supply() -> BasisMessageSupply {
    const CORE_VERSION: u8 = 1;
    const CORE: &[(u8, &str)] = &[
        (0, "basis.core.auth.identity"),
        (1, "basis.core.metadata"),
        (2, "basis.core.disconnection"),
        (3, "basis.core.voice"),
        (4, "basis.core.voice.shout"),
        (5, "basis.core.voice.recipients"),
        (6, "basis.core.avatar.verylow"),
        (7, "basis.core.avatar.verylow.additional"),
        (8, "basis.core.avatar.low"),
        (9, "basis.core.avatar.low.additional"),
        (10, "basis.core.avatar.medium"),
        (11, "basis.core.avatar.medium.additional"),
        (12, "basis.core.avatar.high"),
        (13, "basis.core.avatar.high.additional"),
        (14, "basis.core.avatar.change"),
        (15, "basis.core.avatar.data"),
        (16, "basis.core.player.create"),
        (17, "basis.core.player.create.bulk"),
        (18, "basis.core.chat"),
        (19, "basis.core.ownership.get"),
        (20, "basis.core.ownership.change"),
        (21, "basis.core.ownership.remove"),
        (22, "basis.core.netid.assign"),
        (23, "basis.core.netid.assigns"),
        (24, "basis.core.scene.data"),
        (25, "basis.core.resource.load"),
        (26, "basis.core.resource.unload"),
        (27, "basis.core.resource.preloadready"),
        (28, "basis.core.resource.spawnpreloaded"),
        (29, "basis.core.contentshare"),
        (30, "basis.core.avatar.delta"),
        (31, "basis.core.serverbound"),
        (34, "basis.core.admin"),
        (35, "basis.core.statistics"),
        (36, "basis.core.camera.pip.state"),
        (37, "basis.core.camera.pip.position"),
        (38, "basis.core.events"),
        (39, "basis.core.voice.recipients.large"),
        (40, "basis.core.voice.large"),
        (41, "basis.core.avatar.verylow.large"),
        (42, "basis.core.avatar.verylow.additional.large"),
        (43, "basis.core.avatar.low.large"),
        (44, "basis.core.avatar.low.additional.large"),
        (45, "basis.core.avatar.medium.large"),
        (46, "basis.core.avatar.medium.additional.large"),
        (47, "basis.core.avatar.high.large"),
        (48, "basis.core.avatar.high.additional.large"),
        (49, "basis.core.voice.recipients.inverted"),
        (50, "basis.core.voice.recipients.inverted.large"),
        (51, "basis.core.voice.recipients.bitfield"),
        (52, "basis.core.avatar.bundle.compressed"),
        (53, "basis.core.library"),
        (54, "basis.core.p2p"),
        (55, "basis.core.resource.modify"),
        (56, "basis.core.scene.direct"),
        (57, "basis.core.scene.direct.server"),
        (58, "basis.core.avatar.direct"),
        (59, "basis.core.avatar.direct.server"),
        (60, "basis.core.registry.control"),
    ];
    BasisMessageSupply {
        descriptors: CORE
            .iter()
            .map(|(channel, name)| BasisMessageDescriptor {
                id: *channel as u16,
                version: CORE_VERSION,
                channel: *channel,
                flags: 0,
                name: (*name).to_owned(),
            })
            .collect(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerReadyBatchMessage {
    pub count: u16,
    pub payload: Vec<u8>,
}

impl ServerReadyBatchMessage {
    pub const MAX_PAYLOAD_BYTES: usize = 32 * 1024;
    pub const MIN_COMPRESS_BYTES: usize = 256;
}

impl BasisSerialize for ServerReadyBatchMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        let mut compressed_payload = None;
        if self.payload.len() >= Self::MIN_COMPRESS_BYTES {
            let mut encoder = DeflateEncoder::new(Vec::new(), Compression::best());
            if encoder.write_all(&self.payload).is_ok() {
                if let Ok(deflated) = encoder.finish() {
                    if deflated.len() < self.payload.len() {
                        compressed_payload = Some(deflated);
                    }
                }
            }
        }

        writer.put_u16(self.count);
        writer.put_bool(compressed_payload.is_some());
        if let Some(compressed) = compressed_payload.as_ref() {
            writer.put_i32(compressed.len() as i32);
            writer.put_bytes(compressed);
        } else {
            writer.put_i32(self.payload.len() as i32);
            writer.put_bytes(&self.payload);
        }
    }
}

impl BasisDeserialize for ServerReadyBatchMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let count = reader.get_u16()?;
        let compressed = reader.get_bool()?;
        let len = reader.get_i32()?;
        if len < 0 {
            return Err(crate::io::NetReadError::Underflow {
                needed: 1,
                remaining: 0,
            });
        }
        let framed = reader.get_bytes(len as usize)?;
        let payload = if compressed {
            let mut decoder = DeflateDecoder::new(framed);
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|_| crate::io::NetReadError::Underflow {
                    needed: 1,
                    remaining: 0,
                })?;
            out
        } else {
            framed.to_vec()
        };
        Ok(Self { count, payload })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerReadyMessage {
    pub local_ready_message: ReadyMessage,
    pub player_id_message: PlayerIdMessage,
}

impl BasisSerialize for ServerReadyMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        self.player_id_message.serialize(writer);
        self.local_ready_message.serialize(writer);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetIdMessage {
    pub player_id: String,
}

impl BasisSerialize for NetIdMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        if !self.player_id.is_empty() {
            writer.put_string(&self.player_id);
        }
    }
}

impl BasisDeserialize for NetIdMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        if reader.remaining() == 0 {
            return Ok(Self {
                player_id: String::new(),
            });
        }
        Ok(Self {
            player_id: reader.get_string()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UshortUniqueIdMessage {
    pub unique_id_ushort: u16,
}

impl BasisSerialize for UshortUniqueIdMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.unique_id_ushort);
    }
}

impl BasisDeserialize for UshortUniqueIdMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            unique_id_ushort: reader.get_u16()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerNetIdMessage {
    pub net_id_message: NetIdMessage,
    pub ushort_unique_id_message: UshortUniqueIdMessage,
}

impl BasisSerialize for ServerNetIdMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        self.net_id_message.serialize(writer);
        self.ushort_unique_id_message.serialize(writer);
    }
}

impl BasisDeserialize for ServerNetIdMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            net_id_message: NetIdMessage::deserialize(reader)?,
            ushort_unique_id_message: UshortUniqueIdMessage::deserialize(reader)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerUniqueIdMessages {
    pub messages: Vec<ServerNetIdMessage>,
}

impl BasisSerialize for ServerUniqueIdMessages {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.messages.len() as u16);
        for message in &self.messages {
            message.serialize(writer);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChatMessage {
    pub payload: Vec<u8>,
}

impl BasisSerialize for ChatMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        let len = self.payload.len().min(512);
        writer.put_u16(len as u16);
        writer.put_bytes(&self.payload[..len]);
    }
}

impl BasisDeserialize for ChatMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let len = (reader.get_u16()? as usize).min(512);
        Ok(Self {
            payload: reader.get_bytes(len)?.to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerChatMessage {
    pub player_id: u16,
    pub chat_message: ChatMessage,
}

impl BasisSerialize for ServerChatMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        self.chat_message.serialize(writer);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AudioSegmentDataMessage {
    pub audio_segment: Vec<u8>,
}

impl BasisDeserialize for AudioSegmentDataMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            audio_segment: reader.remaining_slice().to_vec(),
        })
    }
}

impl BasisSerialize for AudioSegmentDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_bytes(&self.audio_segment);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerAudioSegmentMessage {
    pub player_id: u16,
    pub audio_segment: Vec<u8>,
}

impl ServerAudioSegmentMessage {
    pub fn serialize_with_id_size(&self, writer: &mut NetWriter, large_id: bool) {
        if large_id {
            writer.put_u16(self.player_id);
        } else {
            writer.put_u8(self.player_id as u8);
        }
        writer.put_bytes(&self.audio_segment);
    }
}

impl BasisSerialize for ServerAudioSegmentMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        writer.put_bytes(&self.audio_segment);
    }
}

impl BasisDeserialize for ServerAudioSegmentMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
            audio_segment: AudioSegmentDataMessage::deserialize(reader)?.audio_segment,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerSideSyncPlayerMessage {
    pub player_id: u16,
    pub interval: u8,
    pub sequence: u8,
    pub avatar_serialization: LocalAvatarSyncMessage,
}

impl BasisSerialize for ServerSideSyncPlayerMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        writer.put_u8(self.interval);
        writer.put_u8(self.sequence);
        self.avatar_serialization.serialize(writer);
    }
}

impl BasisDeserialize for ServerSideSyncPlayerMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
            interval: reader.get_u8()?,
            sequence: reader.get_u8()?,
            avatar_serialization: LocalAvatarSyncMessage::deserialize(reader)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvatarDataMessage {
    pub player_id: u16,
    pub avatar_link_index: u8,
    pub message_index: u8,
    pub recipients: Vec<u16>,
    pub payload: Vec<u8>,
}

impl BasisSerialize for AvatarDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        writer.put_u8(self.avatar_link_index);
        writer.put_u8(self.message_index);
        writer.put_u16(self.recipients.len() as u16);
        for recipient in &self.recipients {
            writer.put_u16(*recipient);
        }
        writer.put_bytes(&self.payload);
    }
}

impl BasisDeserialize for AvatarDataMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let player_id = reader.get_u16()?;
        let avatar_link_index = reader.get_u8()?;
        let message_index = reader.get_u8()?;
        let count = reader.get_u16()? as usize;
        let mut recipients = Vec::with_capacity(count);
        for _ in 0..count {
            recipients.push(reader.get_u16()?);
        }
        Ok(Self {
            player_id,
            avatar_link_index,
            message_index,
            recipients,
            payload: reader.remaining_slice().to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteAvatarDataMessage {
    pub player_id: u16,
    pub avatar_link_index: u8,
    pub message_index: u8,
    pub payload: Vec<u8>,
}

impl BasisSerialize for RemoteAvatarDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        writer.put_u8(self.avatar_link_index);
        writer.put_u8(self.message_index);
        writer.put_bytes(&self.payload);
    }
}

impl BasisDeserialize for RemoteAvatarDataMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
            avatar_link_index: reader.get_u8()?,
            message_index: reader.get_u8()?,
            payload: reader.remaining_slice().to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerAvatarChangeMessage {
    pub player_id: u16,
    pub client_avatar_change_message: ClientAvatarChangeMessage,
}

impl BasisSerialize for ServerAvatarChangeMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        self.client_avatar_change_message.serialize(writer);
    }
}

impl BasisDeserialize for ServerAvatarChangeMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
            client_avatar_change_message: ClientAvatarChangeMessage::deserialize(reader)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerAvatarDataMessage {
    pub player_id: u16,
    pub avatar_data_message: RemoteAvatarDataMessage,
}

impl BasisSerialize for ServerAvatarDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        self.avatar_data_message.serialize(writer);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VoiceReceiversMessage {
    pub users: Vec<u16>,
}

impl VoiceReceiversMessage {
    pub fn deserialize(reader: &mut NetReader<'_>, large_count: bool) -> ReadResult<Self> {
        let count = if large_count {
            reader.get_u16()? as usize
        } else {
            reader.get_u8()? as usize
        };
        let mut users = Vec::with_capacity(count);
        for _ in 0..count {
            users.push(reader.get_u16()?);
        }
        Ok(Self { users })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct LocalLoadResource {
    pub mode: u8,
    pub loaded_net_id: String,
    pub unlock_password: String,
    pub combined_url: String,
    pub uuid_of_creator: String,
    pub is_admin_locked: bool,
    pub position_x: f32,
    pub position_y: f32,
    pub position_z: f32,
    pub quaternion_x: f32,
    pub quaternion_y: f32,
    pub quaternion_z: f32,
    pub quaternion_w: f32,
    pub scale_x: f32,
    pub scale_y: f32,
    pub scale_z: f32,
    pub persist: bool,
    pub static_resource: bool,
    pub static_admin_locked: bool,
    pub modify_scale: bool,
    pub load_strategy: u8,
}

impl BasisSerialize for LocalLoadResource {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.mode);
        writer.put_string(&self.loaded_net_id);
        writer.put_string(&self.unlock_password);
        writer.put_string(&self.combined_url);
        writer.put_string(&self.uuid_of_creator);
        writer.put_bool(self.is_admin_locked);
        writer.put_bool(self.persist);
        writer.put_bool(self.static_resource);
        writer.put_bool(self.static_admin_locked);
        writer.put_bool(self.modify_scale);
        writer.put_u8(self.load_strategy);
        if self.mode == 0 {
            writer.put_f32(self.position_x);
            writer.put_f32(self.position_y);
            writer.put_f32(self.position_z);
            writer.put_f32(self.quaternion_x);
            writer.put_f32(self.quaternion_y);
            writer.put_f32(self.quaternion_z);
            writer.put_f32(self.quaternion_w);
            writer.put_f32(self.scale_x);
            writer.put_f32(self.scale_y);
            writer.put_f32(self.scale_z);
        }
    }
}

impl BasisDeserialize for LocalLoadResource {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let mode = reader.get_u8()?;
        let loaded_net_id = reader.get_string()?;
        let unlock_password = reader.get_string()?;
        let combined_url = reader.get_string()?;
        let uuid_of_creator = reader.get_string()?;
        let is_admin_locked = reader.get_bool()?;
        let persist = reader.get_bool()?;
        let static_resource = reader.get_bool()?;
        let static_admin_locked = reader.get_bool()?;
        let modify_scale = reader.get_bool()?;
        let load_strategy = reader.get_u8()?;
        let mut value = Self {
            mode,
            loaded_net_id,
            unlock_password,
            combined_url,
            uuid_of_creator,
            is_admin_locked,
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
            persist,
            static_resource,
            static_admin_locked,
            modify_scale,
            load_strategy,
        };
        value.mode = mode;
        if mode == 0 {
            value.position_x = reader.get_f32()?;
            value.position_y = reader.get_f32()?;
            value.position_z = reader.get_f32()?;
            value.quaternion_x = reader.get_f32()?;
            value.quaternion_y = reader.get_f32()?;
            value.quaternion_z = reader.get_f32()?;
            value.quaternion_w = reader.get_f32()?;
            value.scale_x = reader.get_f32()?;
            value.scale_y = reader.get_f32()?;
            value.scale_z = reader.get_f32()?;
        }
        Ok(value)
    }
}

pub type ResourceManagementMessage = LocalLoadResource;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModifyResource {
    pub loaded_net_id: String,
    pub mode: u8,
    pub static_resource: bool,
    pub static_admin_locked: bool,
}

impl BasisSerialize for ModifyResource {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_string(&self.loaded_net_id);
        writer.put_u8(self.mode);
        writer.put_bool(self.static_resource);
        writer.put_bool(self.static_admin_locked);
    }
}

impl BasisDeserialize for ModifyResource {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            loaded_net_id: reader.get_string()?,
            mode: reader.get_u8()?,
            static_resource: reader.get_bool()?,
            static_admin_locked: reader.get_bool()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SceneDataMessage {
    pub message_index: u16,
    pub recipients: Vec<u16>,
    pub payload: Vec<u8>,
}

impl BasisSerialize for SceneDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.message_index);
        writer.put_u16(self.recipients.len() as u16);
        for recipient in &self.recipients {
            writer.put_u16(*recipient);
        }
        writer.put_bytes(&self.payload);
    }
}

impl BasisDeserialize for SceneDataMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let message_index = reader.get_u16()?;
        let count = reader.get_u16()? as usize;
        let mut recipients = Vec::with_capacity(count);
        for _ in 0..count {
            recipients.push(reader.get_u16()?);
        }
        Ok(Self {
            message_index,
            recipients,
            payload: reader.remaining_slice().to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RemoteSceneDataMessage {
    pub message_index: u16,
    pub payload: Vec<u8>,
}

impl BasisSerialize for RemoteSceneDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.message_index);
        writer.put_bytes(&self.payload);
    }
}

impl BasisDeserialize for RemoteSceneDataMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            message_index: reader.get_u16()?,
            payload: reader.remaining_slice().to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerSceneDataMessage {
    pub player_id: u16,
    pub scene_data_message: RemoteSceneDataMessage,
}

impl BasisSerialize for ServerSceneDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        self.scene_data_message.serialize(writer);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnloadResource {
    pub mode: u8,
    pub loaded_net_id: String,
}

impl BasisSerialize for UnloadResource {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.mode);
        writer.put_string(&self.loaded_net_id);
    }
}

impl BasisDeserialize for UnloadResource {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            mode: reader.get_u8()?,
            loaded_net_id: reader.get_string()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreloadReadyMessage {
    pub loaded_net_id: String,
    pub is_ready: bool,
}

impl BasisSerialize for PreloadReadyMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_string(&self.loaded_net_id);
        writer.put_bool(self.is_ready);
    }
}

impl BasisDeserialize for PreloadReadyMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            loaded_net_id: reader.get_string()?,
            is_ready: reader.get_bool()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpawnPreloadedMessage {
    pub loaded_net_id: String,
}

impl BasisSerialize for SpawnPreloadedMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_string(&self.loaded_net_id);
    }
}

impl BasisDeserialize for SpawnPreloadedMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            loaded_net_id: reader.get_string()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabasePrimitiveMessage {
    pub name: String,
    pub json_payload: String,
}

impl BasisSerialize for DatabasePrimitiveMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_string(&self.name);
        write_database_payload(writer, &self.json_payload);
    }
}

impl BasisDeserialize for DatabasePrimitiveMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let name = reader.get_string()?;
        let json_payload = read_database_payload(reader)?;
        Ok(Self { name, json_payload })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataBaseRequest {
    pub database_id: String,
}

impl BasisSerialize for DataBaseRequest {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_string(&self.database_id);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorMessage {
    pub message: String,
}

impl BasisSerialize for ErrorMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_string(&self.message);
    }
}

impl BasisDeserialize for ErrorMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            message: reader.get_string()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasisAvatarCloneRequest {
    pub requesting_user: u16,
}

impl BasisSerialize for BasisAvatarCloneRequest {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.requesting_user);
    }
}

impl BasisDeserialize for BasisAvatarCloneRequest {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            requesting_user: reader.get_u16()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasisAvatarCloneResponse {
    pub requesting_user: u16,
}

impl BasisSerialize for BasisAvatarCloneResponse {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.requesting_user);
    }
}

impl BasisDeserialize for BasisAvatarCloneResponse {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            requesting_user: reader.get_u16()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnershipTransferMessage {
    pub player_id: u16,
    pub ownership_id: String,
}

impl BasisSerialize for OwnershipTransferMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        writer.put_string(&self.ownership_id);
    }
}

impl BasisDeserialize for OwnershipTransferMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
            ownership_id: reader.get_string()?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ContentShareType {
    Avatar = 0,
    Prop = 1,
    World = 2,
    Server = 3,
}

impl From<u8> for ContentShareType {
    fn from(value: u8) -> Self {
        match value {
            1 => Self::Prop,
            2 => Self::World,
            3 => Self::Server,
            _ => Self::Avatar,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ContentShareMessage {
    pub sphere_net_id: String,
    pub content_url: String,
    pub unlock_password: String,
    pub content_type: ContentShareType,
    pub position_x: f32,
    pub position_y: f32,
    pub position_z: f32,
}

impl BasisSerialize for ContentShareMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_string(&self.sphere_net_id);
        writer.put_string(&self.content_url);
        writer.put_string(&self.unlock_password);
        writer.put_u8(self.content_type as u8);
        writer.put_f32(self.position_x);
        writer.put_f32(self.position_y);
        writer.put_f32(self.position_z);
    }
}

impl BasisDeserialize for ContentShareMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            sphere_net_id: reader.get_string()?,
            content_url: reader.get_string()?,
            unlock_password: reader.get_string()?,
            content_type: ContentShareType::from(reader.get_u8()?),
            position_x: reader.get_f32()?,
            position_y: reader.get_f32()?,
            position_z: reader.get_f32()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ServerContentShareMessage {
    pub player_id: u16,
    pub sharer_uuid: String,
    pub sharer_display_name: String,
    pub content_share_message: ContentShareMessage,
}

impl BasisSerialize for ServerContentShareMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        writer.put_string(&self.sharer_uuid);
        writer.put_string(&self.sharer_display_name);
        self.content_share_message.serialize(writer);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContentShareCleanupMessage {
    pub sphere_net_id: String,
}

impl BasisSerialize for ContentShareCleanupMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_string(&self.sphere_net_id);
    }
}

impl BasisDeserialize for ContentShareCleanupMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            sphere_net_id: reader.get_string()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerContentShareCleanupMessage {
    pub player_id: u16,
    pub content_share_cleanup_message: ContentShareCleanupMessage,
}

impl BasisSerialize for ServerContentShareCleanupMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        self.content_share_cleanup_message.serialize(writer);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CameraPipStateMessage {
    pub player_id: u16,
    pub is_active: bool,
    pub position_x: f32,
    pub position_y: f32,
    pub position_z: f32,
    pub rotation_x: f32,
    pub rotation_y: f32,
    pub rotation_z: f32,
    pub rotation_w: f32,
}

impl BasisSerialize for CameraPipStateMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        writer.put_bool(self.is_active);
        if self.is_active {
            write_pip_transform(self, writer);
        }
    }
}

impl BasisDeserialize for CameraPipStateMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let player_id = reader.get_u16()?;
        let is_active = reader.get_bool()?;
        let mut message = Self {
            player_id,
            is_active,
            position_x: 0.0,
            position_y: 0.0,
            position_z: 0.0,
            rotation_x: 0.0,
            rotation_y: 0.0,
            rotation_z: 0.0,
            rotation_w: 1.0,
        };
        if is_active {
            read_pip_transform(&mut message, reader)?;
        }
        Ok(message)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientCameraPipStateMessage {
    pub is_active: bool,
    pub position_x: f32,
    pub position_y: f32,
    pub position_z: f32,
    pub rotation_x: f32,
    pub rotation_y: f32,
    pub rotation_z: f32,
    pub rotation_w: f32,
}

impl BasisDeserialize for ClientCameraPipStateMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let is_active = reader.get_bool()?;
        let mut message = Self {
            is_active,
            position_x: 0.0,
            position_y: 0.0,
            position_z: 0.0,
            rotation_x: 0.0,
            rotation_y: 0.0,
            rotation_z: 0.0,
            rotation_w: 1.0,
        };
        if is_active {
            message.position_x = reader.get_f32()?;
            message.position_y = reader.get_f32()?;
            message.position_z = reader.get_f32()?;
            message.rotation_x = reader.get_f32()?;
            message.rotation_y = reader.get_f32()?;
            message.rotation_z = reader.get_f32()?;
            message.rotation_w = reader.get_f32()?;
        }
        Ok(message)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CameraPipPositionMessage {
    pub player_id: u16,
    pub position_x: f32,
    pub position_y: f32,
    pub position_z: f32,
    pub rotation_x: f32,
    pub rotation_y: f32,
    pub rotation_z: f32,
    pub rotation_w: f32,
}

impl BasisSerialize for CameraPipPositionMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        writer.put_f32(self.position_x);
        writer.put_f32(self.position_y);
        writer.put_f32(self.position_z);
        writer.put_f32(self.rotation_x);
        writer.put_f32(self.rotation_y);
        writer.put_f32(self.rotation_z);
        writer.put_f32(self.rotation_w);
    }
}

impl BasisDeserialize for CameraPipPositionMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
            position_x: reader.get_f32()?,
            position_y: reader.get_f32()?,
            position_z: reader.get_f32()?,
            rotation_x: reader.get_f32()?,
            rotation_y: reader.get_f32()?,
            rotation_z: reader.get_f32()?,
            rotation_w: reader.get_f32()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClientCameraPipPositionMessage {
    pub position_x: f32,
    pub position_y: f32,
    pub position_z: f32,
    pub rotation_x: f32,
    pub rotation_y: f32,
    pub rotation_z: f32,
    pub rotation_w: f32,
}

impl BasisDeserialize for ClientCameraPipPositionMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            position_x: reader.get_f32()?,
            position_y: reader.get_f32()?,
            position_z: reader.get_f32()?,
            rotation_x: reader.get_f32()?,
            rotation_y: reader.get_f32()?,
            rotation_z: reader.get_f32()?,
            rotation_w: reader.get_f32()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CameraShutterSoundMessage {
    pub player_id: u16,
}

impl BasisSerialize for CameraShutterSoundMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
    }
}

impl BasisDeserialize for CameraShutterSoundMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CameraCountdownMessage {
    pub player_id: u16,
    pub seconds: u8,
}

impl BasisSerialize for CameraCountdownMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.player_id);
        writer.put_u8(self.seconds);
    }
}

impl BasisDeserialize for CameraCountdownMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_id: reader.get_u16()?,
            seconds: reader.get_u8()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientCameraCountdownMessage {
    pub seconds: u8,
}

impl BasisSerialize for ClientCameraCountdownMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.seconds);
    }
}

impl BasisDeserialize for ClientCameraCountdownMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            seconds: reader.get_u8()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerStatisticMessage {
    pub data: Vec<u8>,
}

impl BasisSerialize for ServerStatisticMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_bytes(&self.data);
    }
}

impl BasisDeserialize for ServerStatisticMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            data: reader.remaining_slice().to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerLibraryItem {
    pub mode: u8,
    pub url: String,
    pub password: String,
}

impl BasisSerialize for ServerLibraryItem {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.mode);
        writer.put_string(&self.url);
        writer.put_string(&self.password);
    }
}

impl BasisDeserialize for ServerLibraryItem {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            mode: reader.get_u8()?,
            url: reader.get_string()?,
            password: reader.get_string()?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerLibraryMessage {
    pub items: Vec<ServerLibraryItem>,
}

impl BasisSerialize for ServerLibraryMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.items.len() as u16);
        for item in &self.items {
            item.serialize(writer);
        }
    }
}

impl BasisDeserialize for ServerLibraryMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let count = reader.get_u16()? as usize;
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(ServerLibraryItem::deserialize(reader)?);
        }
        Ok(Self { items })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsoleData {
    pub message_index: u8,
    pub array: Vec<u8>,
}

impl BasisSerialize for ConsoleData {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.message_index);
        writer.put_u16(self.array.len() as u16);
        writer.put_bytes(&self.array);
    }
}

impl BasisDeserialize for ConsoleData {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let message_index = reader.get_u8()?;
        let len = reader.get_u16()? as usize;
        Ok(Self {
            message_index,
            array: reader.get_bytes(len)?.to_vec(),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvatarLoadDataMessage {
    pub message_index: u8,
    pub who_sent_us_this: u16,
    pub payload: Vec<u8>,
}

impl BasisSerialize for AvatarLoadDataMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.message_index);
        writer.put_u16(self.who_sent_us_this);
        writer.put_u16(self.payload.len() as u16);
        writer.put_bytes(&self.payload);
    }
}

impl BasisDeserialize for AvatarLoadDataMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let message_index = reader.get_u8()?;
        let who_sent_us_this = reader.get_u16()?;
        let len = reader.get_u16()? as usize;
        Ok(Self {
            message_index,
            who_sent_us_this,
            payload: reader.get_bytes(len)?.to_vec(),
        })
    }
}

fn write_pip_transform(message: &CameraPipStateMessage, writer: &mut NetWriter) {
    writer.put_f32(message.position_x);
    writer.put_f32(message.position_y);
    writer.put_f32(message.position_z);
    writer.put_f32(message.rotation_x);
    writer.put_f32(message.rotation_y);
    writer.put_f32(message.rotation_z);
    writer.put_f32(message.rotation_w);
}

fn read_pip_transform(
    message: &mut CameraPipStateMessage,
    reader: &mut NetReader<'_>,
) -> ReadResult<()> {
    message.position_x = reader.get_f32()?;
    message.position_y = reader.get_f32()?;
    message.position_z = reader.get_f32()?;
    message.rotation_x = reader.get_f32()?;
    message.rotation_y = reader.get_f32()?;
    message.rotation_z = reader.get_f32()?;
    message.rotation_w = reader.get_f32()?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AdminRequestMode {
    Ban = 0,
    Kick = 1,
    IpAndBan = 2,
    Message = 3,
    MessageAll = 4,
    UnBanIP = 5,
    UnBan = 6,
    TeleportAll = 7,
    TeleportPlayer = 8,
    GetPermissions = 9,
    SetUserGroup = 10,
    SetUserNode = 11,
    SetGroupNode = 12,
    CreateGroup = 13,
    DeleteGroup = 14,
    SetGroupParent = 15,
    EnableShoutMode = 16,
    DisableShoutMode = 17,
    GlobalToggleAvatars = 18,
    GlobalToggleProps = 19,
    GlobalToggleWorlds = 20,
    GlobalGetLockState = 21,
    GlobalGetHeadlessAudioState = 22,
    SetGlobalHeadlessAudio = 23,
    GlobalGetHeadlessDisallowState = 24,
    SetGlobalHeadlessDisallow = 25,
    SetGlobalOpusPacketLoss = 26,
    GlobalGetOpusPacketLossState = 27,
    SetUserOpusBitrate = 28,
    UserOpusBitrateOverride = 29,
    SetGlobalOpusFrameDuration = 30,
    GlobalGetOpusFrameDurationState = 31,
    SetServerName = 32,
    SetServerMotd = 33,
    SetAllowlistMode = 34,
    AddAllowlist = 35,
    RemoveAllowlist = 36,
    GlobalToggleServers = 37,
    GlobalToggleThirdPerson = 38,
    AddDefaultLibraryItem = 39,
    RemoveDefaultLibraryItem = 40,
    GlobalToggleAdditionalAvatarDataLock = 41,
    SetGlobalCameraPolicy = 42,
    GlobalGetCrashReportState = 43,
    SetGlobalCrashReporting = 44,
    GlobalGetAudioRangeLimits = 45,
    SetGlobalAudioRangeLimits = 46,
    RequestAllLogs = 47,
    LogBundleBegin = 48,
    LogBundleChunk = 49,
    LogBundleEnd = 50,
    ClearAllScenes = 51,
    DeleteAllLogs = 52,
    GlobalTogglePlayspaceMover = 53,
    GlobalToggleDirectConnect = 54,
    GlobalGetAvatarScaleLimits = 55,
    SetGlobalAvatarScaleLimits = 56,
    GlobalGetResourceLimits = 57,
    SetGlobalResourceLimits = 58,
    GlobalToggleCilbox = 59,
    GlobalToggleImages = 60,
    SetFullQualityBroadcast = 61,
    SetGlobalReductionSettings = 62,
    GlobalGetReductionSettings = 63,
    SetGlobalOpusBitrate = 64,
    GlobalGetOpusBitrateState = 65,
    GlobalToggleEndEffectorIK = 66,
    GlobalToggleTextChat = 67,
    GlobalToggleVoiceChat = 68,
    GlobalToggleMediaPlayer = 69,
    GlobalToggleCameraCapture = 70,
    GlobalTogglePropGrabbing = 71,
    GlobalToggleSafeDisplayNames = 72,
    ForceAvatar = 73,
    ForceAvatarApply = 74,
    ForceAvatarAll = 75,
    SetLocomotionOverride = 76,
    LocomotionOverrideApply = 77,
    SetLocomotionOverrideAll = 78,
    SetGlobalImageBandwidth = 79,
    GlobalGetImageBandwidth = 80,
    SetGlobalPeerLimit = 81,
    GlobalGetPeerLimit = 82,
    Unknown = 255,
}

impl From<u8> for AdminRequestMode {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Ban,
            1 => Self::Kick,
            2 => Self::IpAndBan,
            3 => Self::Message,
            4 => Self::MessageAll,
            5 => Self::UnBanIP,
            6 => Self::UnBan,
            7 => Self::TeleportAll,
            8 => Self::TeleportPlayer,
            9 => Self::GetPermissions,
            10 => Self::SetUserGroup,
            11 => Self::SetUserNode,
            12 => Self::SetGroupNode,
            13 => Self::CreateGroup,
            14 => Self::DeleteGroup,
            15 => Self::SetGroupParent,
            16 => Self::EnableShoutMode,
            17 => Self::DisableShoutMode,
            18 => Self::GlobalToggleAvatars,
            19 => Self::GlobalToggleProps,
            20 => Self::GlobalToggleWorlds,
            21 => Self::GlobalGetLockState,
            22 => Self::GlobalGetHeadlessAudioState,
            23 => Self::SetGlobalHeadlessAudio,
            24 => Self::GlobalGetHeadlessDisallowState,
            25 => Self::SetGlobalHeadlessDisallow,
            26 => Self::SetGlobalOpusPacketLoss,
            27 => Self::GlobalGetOpusPacketLossState,
            28 => Self::SetUserOpusBitrate,
            29 => Self::UserOpusBitrateOverride,
            30 => Self::SetGlobalOpusFrameDuration,
            31 => Self::GlobalGetOpusFrameDurationState,
            32 => Self::SetServerName,
            33 => Self::SetServerMotd,
            34 => Self::SetAllowlistMode,
            35 => Self::AddAllowlist,
            36 => Self::RemoveAllowlist,
            37 => Self::GlobalToggleServers,
            38 => Self::GlobalToggleThirdPerson,
            39 => Self::AddDefaultLibraryItem,
            40 => Self::RemoveDefaultLibraryItem,
            41 => Self::GlobalToggleAdditionalAvatarDataLock,
            42 => Self::SetGlobalCameraPolicy,
            43 => Self::GlobalGetCrashReportState,
            44 => Self::SetGlobalCrashReporting,
            45 => Self::GlobalGetAudioRangeLimits,
            46 => Self::SetGlobalAudioRangeLimits,
            47 => Self::RequestAllLogs,
            48 => Self::LogBundleBegin,
            49 => Self::LogBundleChunk,
            50 => Self::LogBundleEnd,
            51 => Self::ClearAllScenes,
            52 => Self::DeleteAllLogs,
            53 => Self::GlobalTogglePlayspaceMover,
            54 => Self::GlobalToggleDirectConnect,
            55 => Self::GlobalGetAvatarScaleLimits,
            56 => Self::SetGlobalAvatarScaleLimits,
            57 => Self::GlobalGetResourceLimits,
            58 => Self::SetGlobalResourceLimits,
            59 => Self::GlobalToggleCilbox,
            60 => Self::GlobalToggleImages,
            61 => Self::SetFullQualityBroadcast,
            62 => Self::SetGlobalReductionSettings,
            63 => Self::GlobalGetReductionSettings,
            64 => Self::SetGlobalOpusBitrate,
            65 => Self::GlobalGetOpusBitrateState,
            66 => Self::GlobalToggleEndEffectorIK,
            67 => Self::GlobalToggleTextChat,
            68 => Self::GlobalToggleVoiceChat,
            69 => Self::GlobalToggleMediaPlayer,
            70 => Self::GlobalToggleCameraCapture,
            71 => Self::GlobalTogglePropGrabbing,
            72 => Self::GlobalToggleSafeDisplayNames,
            73 => Self::ForceAvatar,
            74 => Self::ForceAvatarApply,
            75 => Self::ForceAvatarAll,
            76 => Self::SetLocomotionOverride,
            77 => Self::LocomotionOverrideApply,
            78 => Self::SetLocomotionOverrideAll,
            79 => Self::SetGlobalImageBandwidth,
            80 => Self::GlobalGetImageBandwidth,
            81 => Self::SetGlobalPeerLimit,
            82 => Self::GlobalGetPeerLimit,
            _ => Self::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdminRequest {
    pub mode: AdminRequestMode,
}

impl BasisSerialize for AdminRequest {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.mode as u8);
    }
}

impl BasisDeserialize for AdminRequest {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            mode: AdminRequestMode::from(reader.get_u8()?),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BasisP2PSignalMessage {
    pub other_player_id: u16,
    pub session_token: String,
    pub ephemeral_public_key: Option<[u8; 32]>,
}

impl BasisSerialize for BasisP2PSignalMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u16(self.other_player_id);
        writer.put_string(&self.session_token);
        if let Some(key) = self.ephemeral_public_key {
            writer.put_u8(1);
            writer.put_bytes(&key);
        } else {
            writer.put_u8(0);
        }
    }
}

impl BasisDeserialize for BasisP2PSignalMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let other_player_id = reader.get_u16()?;
        let session_token = reader.get_string()?;
        let has_key = reader.get_u8()?;
        let ephemeral_public_key = if has_key == 1 {
            let bytes = reader.get_bytes(32)?;
            let mut key = [0u8; 32];
            key.copy_from_slice(bytes);
            Some(key)
        } else {
            None
        };
        Ok(Self {
            other_player_id,
            session_token,
            ephemeral_public_key,
        })
    }
}

impl BasisDeserialize for DataBaseRequest {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            database_id: reader.get_string()?,
        })
    }
}

fn write_database_payload(writer: &mut NetWriter, json_payload: &str) {
    let value = serde_json::from_str::<Value>(json_payload)
        .unwrap_or(Value::String(json_payload.to_string()));
    let Some(map) = value.as_object() else {
        writer.put_i32(1);
        writer.put_string("value");
        write_database_value(writer, &value);
        return;
    };
    writer.put_i32(map.len() as i32);
    for (key, value) in map {
        writer.put_string(key);
        write_database_value(writer, value);
    }
}

fn write_database_value(writer: &mut NetWriter, value: &Value) {
    match value {
        Value::Null => writer.put_u8(0),
        Value::String(value) => {
            writer.put_u8(1);
            writer.put_string(value);
        }
        Value::Number(number) => write_database_number(writer, number),
        Value::Bool(value) => {
            writer.put_u8(3);
            writer.put_bool(*value);
        }
        other => {
            writer.put_u8(1);
            writer.put_string(&other.to_string());
        }
    }
}

fn write_database_number(writer: &mut NetWriter, number: &Number) {
    if let Some(value) = number.as_i64() {
        if let Ok(value) = i32::try_from(value) {
            writer.put_u8(2);
            writer.put_i32(value);
        } else {
            writer.put_u8(6);
            writer.put_i64(value);
        }
    } else if let Some(value) = number.as_u64() {
        writer.put_u8(7);
        writer.put_u64(value);
    } else if let Some(value) = number.as_f64() {
        writer.put_u8(5);
        writer.put_f64(value);
    } else {
        writer.put_u8(0);
    }
}

fn read_database_payload(reader: &mut NetReader<'_>) -> ReadResult<String> {
    let count = reader.get_i32()?.max(0) as usize;
    let mut map = Map::new();
    for _ in 0..count {
        let key = reader.get_string()?;
        let marker = reader.get_u8()?;
        map.insert(key, read_database_value(reader, marker)?);
    }
    Ok(Value::Object(map).to_string())
}

fn read_database_value(reader: &mut NetReader<'_>, marker: u8) -> ReadResult<Value> {
    Ok(match marker {
        0 => Value::Null,
        1 | 13 => Value::String(reader.get_string()?),
        2 => Value::Number(Number::from(reader.get_i32()?)),
        3 => Value::Bool(reader.get_bool()?),
        4 => Number::from_f64(reader.get_f32()? as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        5 => Number::from_f64(reader.get_f64()?)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        6 => Value::Number(Number::from(reader.get_i64()?)),
        7 => Value::Number(Number::from(reader.get_u64()?)),
        8 => Value::Number(Number::from(reader.get_i16()?)),
        9 => Value::Number(Number::from(reader.get_u16()?)),
        10 => Value::Number(Number::from(reader.get_u8()?)),
        11 => Value::Number(Number::from(reader.get_i8()?)),
        12 => Value::String(reader.get_u16()?.to_string()),
        _ => Value::Null,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_message_round_trips() {
        let mut writer = NetWriter::new();
        BytesMessage {
            data: vec![1, 2, 3],
        }
        .serialize(&mut writer);
        assert_eq!(writer.as_slice(), &[3, 0, 1, 2, 3]);
        let mut reader = NetReader::new(writer.as_slice());
        assert_eq!(
            BytesMessage::deserialize(&mut reader).unwrap(),
            BytesMessage {
                data: vec![1, 2, 3]
            }
        );
    }

    #[test]
    fn empty_metadata_fields_serialize_as_failure() {
        let mut writer = NetWriter::new();
        ClientMetaDataMessage {
            player_uuid: String::new(),
            player_display_name: String::new(),
            player_platform: String::new(),
        }
        .serialize(&mut writer);
        let mut reader = NetReader::new(writer.as_slice());
        let decoded = ClientMetaDataMessage::deserialize(&mut reader).unwrap();
        assert_eq!(decoded.player_uuid, "Failure");
        assert_eq!(decoded.player_display_name, "Failure");
        assert_eq!(decoded.player_platform, "Failure");
    }

    #[test]
    fn ready_message_order_is_metadata_avatar_sync() {
        let ready = ReadyMessage {
            player_meta_data_message: ClientMetaDataMessage {
                player_uuid: "uuid".to_string(),
                player_display_name: "name".to_string(),
                player_platform: "Headless".to_string(),
            },
            client_avatar_change_message: ClientAvatarChangeMessage {
                load_mode: 1,
                byte_array: vec![9, 8, 7],
                local_avatar_index: 0,
                arm_scale: 1.0,
                leg_scale: 1.0,
                torso_scale: 1.0,
            },
            local_avatar_sync_message: LocalAvatarSyncMessage::empty_high(),
        };
        let mut writer = NetWriter::new();
        ready.serialize(&mut writer);
        let mut reader = NetReader::new(writer.as_slice());
        assert_eq!(
            ClientMetaDataMessage::deserialize(&mut reader)
                .unwrap()
                .player_uuid,
            "uuid"
        );
        assert_eq!(
            ClientAvatarChangeMessage::deserialize(&mut reader)
                .unwrap()
                .byte_array,
            vec![9, 8, 7]
        );
        assert_eq!(reader.get_u8().unwrap(), BitQuality::High as u8);
    }

    #[test]
    fn movement_channel_sync_omits_quality_byte() {
        let sync = LocalAvatarSyncMessage::empty_high();
        let mut writer = NetWriter::new();
        sync.serialize_for_channel(&mut writer, false);
        assert_eq!(writer.len(), BitQuality::High.payload_len());
    }

    #[test]
    fn server_ready_serializes_player_id_before_ready_message() {
        let message = ServerReadyMessage {
            player_id_message: PlayerIdMessage { player_id: 513 },
            local_ready_message: ReadyMessage {
                player_meta_data_message: ClientMetaDataMessage {
                    player_uuid: "uuid".to_string(),
                    player_display_name: "name".to_string(),
                    player_platform: "platform".to_string(),
                },
                client_avatar_change_message: ClientAvatarChangeMessage {
                    load_mode: 0,
                    byte_array: Vec::new(),
                    local_avatar_index: 0,
                    arm_scale: 1.0,
                    leg_scale: 1.0,
                    torso_scale: 1.0,
                },
                local_avatar_sync_message: LocalAvatarSyncMessage::empty_high(),
            },
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
        assert_eq!(&writer.as_slice()[..2], &513u16.to_le_bytes());
    }

    #[test]
    fn core_message_supply_matches_current_registry_shape() {
        let supply = core_message_supply();
        assert!(supply
            .descriptors
            .iter()
            .any(|d| d.id == 30 && d.name == "basis.core.avatar.delta"));
        assert!(supply
            .descriptors
            .iter()
            .any(|d| d.id == 55 && d.name == "basis.core.resource.modify"));
        assert!(supply
            .descriptors
            .iter()
            .any(|d| d.id == 60 && d.name == "basis.core.registry.control"));
        assert!(!supply.descriptors.iter().any(|d| d.id == 32 || d.id == 33));
        assert!(supply
            .descriptors
            .iter()
            .all(|d| d.version == 1 && d.flags == 0));

        let mut writer = NetWriter::new();
        supply.serialize(&mut writer);
        let mut reader = NetReader::new(writer.as_slice());
        assert_eq!(
            BasisMessageSupply::deserialize(&mut reader).unwrap(),
            supply
        );
    }

    #[test]
    fn server_ready_batch_round_trips_current_v54_framing() {
        let payload = vec![0x41; 512];
        let message = ServerReadyBatchMessage {
            count: 3,
            payload: payload.clone(),
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
        let mut reader = NetReader::new(writer.as_slice());
        let decoded = ServerReadyBatchMessage::deserialize(&mut reader).unwrap();
        assert_eq!(decoded.count, 3);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn avatar_change_v54_body_fit_round_trips() {
        let message = ClientAvatarChangeMessage {
            load_mode: 1,
            byte_array: vec![1, 2, 3],
            local_avatar_index: 7,
            arm_scale: 0.75,
            leg_scale: 1.25,
            torso_scale: 1.0,
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
        let mut reader = NetReader::new(writer.as_slice());
        let decoded = ClientAvatarChangeMessage::deserialize(&mut reader).unwrap();
        assert_eq!(decoded.load_mode, message.load_mode);
        assert_eq!(decoded.byte_array, message.byte_array);
        assert_eq!(decoded.local_avatar_index, message.local_avatar_index);
        assert!((decoded.arm_scale - message.arm_scale).abs() < 0.00002);
        assert!((decoded.leg_scale - message.leg_scale).abs() < 0.00002);
        assert!((decoded.torso_scale - message.torso_scale).abs() < 0.00002);
    }

    #[test]
    fn body_fit_only_payload_round_trips_current_channel14_wire() {
        let message = ClientBodyFitMessage {
            arm_scale: 0.75,
            leg_scale: 1.25,
            torso_scale: 1.0,
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
        assert_eq!(writer.len(), 6);

        let mut reader = NetReader::new(writer.as_slice());
        let decoded = ClientBodyFitMessage::deserialize(&mut reader).unwrap();
        assert!((decoded.arm_scale - message.arm_scale).abs() < 0.00002);
        assert!((decoded.leg_scale - message.leg_scale).abs() < 0.00002);
        assert!((decoded.torso_scale - message.torso_scale).abs() < 0.00002);

        let server = ServerBodyFitMessage {
            player_id: 513,
            body_fit: message,
        };
        let mut writer = NetWriter::new();
        server.serialize(&mut writer);
        assert_eq!(&writer.as_slice()[..2], &513u16.to_le_bytes());
        assert_eq!(writer.len(), 8);
    }

    #[test]
    fn chat_uses_payload_length_not_basis_string() {
        let chat = ChatMessage {
            payload: b"hello".to_vec(),
        };
        let mut writer = NetWriter::new();
        chat.serialize(&mut writer);
        assert_eq!(writer.as_slice(), &[5, 0, b'h', b'e', b'l', b'l', b'o']);
        let mut reader = NetReader::new(writer.as_slice());
        assert_eq!(ChatMessage::deserialize(&mut reader).unwrap(), chat);
    }

    #[test]
    fn voice_recipient_small_count_still_reads_ushort_ids() {
        let bytes = [2, 7, 0, 44, 1];
        let mut reader = NetReader::new(&bytes);
        assert_eq!(
            VoiceReceiversMessage::deserialize(&mut reader, false).unwrap(),
            VoiceReceiversMessage {
                users: vec![7, 300]
            }
        );
    }

    #[test]
    fn server_metadata_uses_permission_bitset_wire_format() {
        let message = ServerMetaDataMessage {
            client_meta_data_message: ClientMetaDataMessage {
                player_uuid: "uuid".to_string(),
                player_display_name: "name".to_string(),
                player_platform: "platform".to_string(),
            },
            sync_interval: 50,
            base_multiplier: 1,
            increase_rate: 0.005,
            slowest_send_rate: 2.55,
            peer_limit: 128,
            allowed_permissions: vec![permissions::nodes::RESOURCE_LOAD_PROP.to_string()],
            denied_permissions: Vec::new(),
            uplink_delta_enabled: false,
            image_share_egress_megabits_per_second: 0,
            image_pickup_range_meters: 0.0,
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
        let mut reader = NetReader::new(writer.as_slice());
        let _ = ClientMetaDataMessage::deserialize(&mut reader).unwrap();
        let _ = reader.get_i32().unwrap();
        let _ = reader.get_i32().unwrap();
        let _ = reader.get_f32().unwrap();
        let _ = reader.get_f32().unwrap();
        let _ = reader.get_i32().unwrap();
        let bitset = reader.get_bytes_with_length().unwrap();
        assert_eq!(bitset.len(), 4);
        assert_ne!(bitset[0] & (1 << 4), 0);
        assert_eq!(reader.get_u16().unwrap(), 0);
    }

    #[test]
    fn ownership_message_order_matches_csharp() {
        let mut writer = NetWriter::new();
        OwnershipTransferMessage {
            player_id: 7,
            ownership_id: "object".to_string(),
        }
        .serialize(&mut writer);
        assert_eq!(&writer.as_slice()[0..2], &7u16.to_le_bytes());
        let mut reader = NetReader::new(writer.as_slice());
        assert_eq!(
            OwnershipTransferMessage::deserialize(&mut reader).unwrap(),
            OwnershipTransferMessage {
                player_id: 7,
                ownership_id: "object".to_string()
            }
        );
    }

    #[test]
    fn inactive_pip_state_omits_transform() {
        let mut writer = NetWriter::new();
        CameraPipStateMessage {
            player_id: 1,
            is_active: false,
            position_x: 1.0,
            position_y: 2.0,
            position_z: 3.0,
            rotation_x: 0.0,
            rotation_y: 0.0,
            rotation_z: 0.0,
            rotation_w: 1.0,
        }
        .serialize(&mut writer);
        assert_eq!(writer.len(), 3);
    }

    #[test]
    fn additional_avatar_data_uses_byte_len_and_message_index() {
        let item = AdditionalAvatarData {
            message_index: 9,
            data: vec![1, 2, 3],
        };
        let mut writer = NetWriter::new();
        item.serialize(&mut writer);
        assert_eq!(writer.as_slice(), &[3, 9, 1, 2, 3]);
        let mut reader = NetReader::new(writer.as_slice());
        assert_eq!(
            AdditionalAvatarData::deserialize(&mut reader).unwrap(),
            item
        );
    }

    #[test]
    fn p2p_signal_carries_current_ephemeral_key_field() {
        let message = BasisP2PSignalMessage {
            other_player_id: 42,
            session_token: "token".to_string(),
            ephemeral_public_key: Some([7u8; 32]),
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
        let mut reader = NetReader::new(writer.as_slice());
        assert_eq!(
            BasisP2PSignalMessage::deserialize(&mut reader).unwrap(),
            message
        );
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn scene_data_message_order_matches_csharp() {
        let message = SceneDataMessage {
            message_index: 42,
            recipients: vec![7, 8],
            payload: vec![1, 2],
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
        assert_eq!(writer.as_slice(), &[42, 0, 2, 0, 7, 0, 8, 0, 1, 2]);
    }

    #[test]
    fn server_library_message_has_ushort_count() {
        let message = ServerLibraryMessage {
            items: vec![ServerLibraryItem {
                mode: 2,
                url: "u".to_string(),
                password: "p".to_string(),
            }],
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
        assert_eq!(&writer.as_slice()[..3], &[1, 0, 2]);
        let mut reader = NetReader::new(writer.as_slice());
        assert_eq!(
            ServerLibraryMessage::deserialize(&mut reader).unwrap(),
            message
        );
    }

    #[test]
    fn camera_countdown_message_is_player_id_then_seconds() {
        let mut writer = NetWriter::new();
        CameraCountdownMessage {
            player_id: 300,
            seconds: 5,
        }
        .serialize(&mut writer);
        assert_eq!(writer.as_slice(), &[44, 1, 5]);
    }
}
