use crate::{
    avatar::BitQuality,
    io::{NetReader, NetWriter, Result as ReadResult},
    permissions,
};
use flate2::{write::DeflateEncoder, Compression};
use serde_json::{Map, Number, Value};
use std::io::Write;

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
        writer.put_string(non_empty_or_failure(&self.player_uuid));
        writer.put_string(non_empty_or_failure(&self.player_display_name));
        writer.put_string(non_empty_or_failure(&self.player_platform));
    }
}

impl BasisDeserialize for ClientMetaDataMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        Ok(Self {
            player_uuid: reader.get_string()?,
            player_display_name: reader.get_string()?,
            player_platform: reader.get_string()?,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientAvatarChangeMessage {
    pub load_mode: u8,
    pub byte_array: Vec<u8>,
    pub local_avatar_index: u8,
}

impl BasisSerialize for ClientAvatarChangeMessage {
    fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.load_mode);
        writer.put_u16(self.byte_array.len() as u16);
        writer.put_bytes(&self.byte_array);
        writer.put_u8(self.local_avatar_index);
    }
}

impl BasisDeserialize for ClientAvatarChangeMessage {
    fn deserialize(reader: &mut NetReader<'_>) -> ReadResult<Self> {
        let load_mode = reader.get_u8()?;
        let len = reader.get_u16()? as usize;
        let byte_array = reader.get_bytes(len)?.to_vec();
        let local_avatar_index = reader.get_u8()?;
        Ok(Self {
            load_mode,
            byte_array,
            local_avatar_index,
        })
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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
    SetWhitelistMode = 34,
    AddWhitelist = 35,
    RemoveWhitelist = 36,
    GlobalToggleServers = 37,
    GlobalToggleThirdPerson = 38,
    AddDefaultLibraryItem = 39,
    RemoveDefaultLibraryItem = 40,
}

impl From<u8> for AdminRequestMode {
    fn from(value: u8) -> Self {
        match value {
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
            34 => Self::SetWhitelistMode,
            35 => Self::AddWhitelist,
            36 => Self::RemoveWhitelist,
            37 => Self::GlobalToggleServers,
            38 => Self::GlobalToggleThirdPerson,
            39 => Self::AddDefaultLibraryItem,
            40 => Self::RemoveDefaultLibraryItem,
            _ => Self::Ban,
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
        assert_eq!(reader.get_string().unwrap(), "Failure");
        assert_eq!(reader.get_string().unwrap(), "Failure");
        assert_eq!(reader.get_string().unwrap(), "Failure");
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
                },
                local_avatar_sync_message: LocalAvatarSyncMessage::empty_high(),
            },
        };
        let mut writer = NetWriter::new();
        message.serialize(&mut writer);
        assert_eq!(&writer.as_slice()[..2], &513u16.to_le_bytes());
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
