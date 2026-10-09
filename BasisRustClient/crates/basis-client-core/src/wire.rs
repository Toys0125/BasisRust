use crate::avatar::PoseState;
use crate::config::Config;
use anyhow::Result;
use basis_protocol::application::NetworkApplication;
use basis_protocol::avatar::{encode_avatar_network_load_with_version, BitQuality};
use basis_protocol::io::NetWriter;
use basis_protocol::messages::{
    AdditionalAvatarData, BasisSerialize, ClientAvatarChangeMessage, ClientMetaDataMessage,
    LocalAvatarSyncMessage, ReadyMessage,
};
use basis_protocol::version::SERVER_VERSION;
use rand::Rng;
use uuid::Uuid;

pub(crate) fn random_metadata() -> ClientMetaDataMessage {
    ClientMetaDataMessage {
        player_uuid: Uuid::new_v4().to_string(),
        player_display_name: random_display_name(),
        player_platform: "Headless".to_string(),
    }
}

pub(crate) fn avatar_change(config: &Config) -> Result<ClientAvatarChangeMessage> {
    Ok(ClientAvatarChangeMessage {
        load_mode: config.avatar_load_mode,
        byte_array: encode_avatar_network_load_with_version(
            &config.avatar_url,
            &config.avatar_password,
            "",
        )?,
        local_avatar_index: 0,
        arm_scale: 1.0,
        leg_scale: 1.0,
        torso_scale: 1.0,
    })
}

pub(crate) fn ready_message(config: &Config, spawn_base: [f32; 3]) -> Result<ReadyMessage> {
    let mut pose = PoseState::new_at(spawn_base);
    let additional_avatar_datas =
        synthetic_additional_avatar_data(config.additional_avatar_bytes, 0);
    Ok(ReadyMessage {
        player_meta_data_message: random_metadata(),
        client_avatar_change_message: avatar_change(config)?,
        local_avatar_sync_message: LocalAvatarSyncMessage {
            data_quality_level: BitQuality::High as u8,
            array: pose.high_quality_payload(0.0),
            additional_avatar_datas,
            linked_avatar_index: 0,
        },
    })
}

/// Generates deterministic script-like bytes that change with each avatar send sequence.
pub(crate) fn synthetic_additional_avatar_data(
    byte_count: u8,
    sequence: u8,
) -> Vec<AdditionalAvatarData> {
    if byte_count == 0 {
        return Vec::new();
    }
    let data = (0..byte_count)
        .map(|offset| sequence.wrapping_add(offset.wrapping_mul(37)))
        .collect();
    vec![AdditionalAvatarData {
        message_index: 0,
        data,
    }]
}

/// Appends the LocalAvatarSyncMessage additional-data section to an avatar payload.
pub(crate) fn append_synthetic_additional_avatar_data(
    payload: &mut Vec<u8>,
    byte_count: u8,
    sequence: u8,
) {
    if byte_count == 0 {
        return;
    }
    payload.push(1); // one AdditionalAvatarData item
    payload.push(0); // linked avatar index
    payload.push(byte_count);
    payload.push(0); // synthetic script message index
    payload.extend((0..byte_count).map(|offset| sequence.wrapping_add(offset.wrapping_mul(37))));
}

pub(crate) fn build_connection_payload(config: &Config, ready: &ReadyMessage) -> Result<Vec<u8>> {
    let auth = config.password.as_bytes();
    let mut writer = NetWriter::with_capacity(512);
    writer.put_u16(SERVER_VERSION);
    writer.put_bytes(&NetworkApplication::encode(
        &config.company_name,
        &config.product_name,
    )?);
    writer.put_bytes_with_length(auth)?;
    ready.serialize(&mut writer)?;
    Ok(writer.into_vec())
}

pub(crate) fn random_display_name() -> String {
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
