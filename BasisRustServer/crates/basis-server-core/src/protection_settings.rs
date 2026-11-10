//! Protocol-57 protection editor. Advertise only settings backed by this server's runtime.
//! Layout: BasisVR 2e6b44de394d73a17ebba5a58ec4812464e120e8 BasisProtectionSettings.
use anyhow::{ensure, Result};
use basis_protocol::{
    config::ServerConfig,
    io::{NetReader, NetWriter},
    messages::AdminRequestMode,
};

const MAX_ENTRIES: usize = 256;
const MAX_KEY_LENGTH: usize = 512;

struct SettingDescriptor {
    key: &'static str,
    group: &'static str,
    title: &'static str,
    description: &'static str,
    whole_number: bool,
    min: f64,
    max: f64,
    get: fn(&ServerConfig) -> f64,
    set: fn(&mut ServerConfig, f64),
}
macro_rules! integer_setting {
    ($field:ident, $key:literal, $group:literal, $title:literal, $description:literal, $min:literal, $max:literal) => {
        SettingDescriptor {
            key: $key,
            group: $group,
            title: $title,
            description: $description,
            whole_number: true,
            min: $min as f64,
            max: $max as f64,
            get: |config| config.$field as f64,
            set: |config, value| config.$field = value as i32,
        }
    };
}
const SETTINGS: &[SettingDescriptor] = &[
    integer_setting!(
        max_scene_relay_megabits_per_second_per_player,
        "MaxSceneRelayMegabitsPerSecondPerPlayer",
        "Relays",
        "Scene relay bandwidth per player",
        "Maximum scene relay megabits per second per player. Zero removes the limit.",
        0,
        100000
    ),
    integer_setting!(
        max_loaded_resources_per_player,
        "MaxLoadedResourcesPerPlayer",
        "Per-player caps",
        "Loaded resources per player",
        "Maximum number of loaded resources owned by one player.",
        1,
        1000000
    ),
    integer_setting!(
        max_network_ids_per_player,
        "MaxNetworkIdsPerPlayer",
        "Per-player caps",
        "Network IDs per player",
        "Maximum number of network identifiers allocated to one player.",
        1,
        65536
    ),
    integer_setting!(
        screen_share_max_kilobits_per_second,
        "ScreenShareMaxKilobitsPerSecond",
        "Screen share",
        "Screen share bitrate",
        "Maximum kilobits per second for each screen sharer.",
        500,
        100000
    ),
    integer_setting!(
        screen_share_egress_megabits_per_second,
        "ScreenShareEgressMegabitsPerSecond",
        "Screen share",
        "Screen share outgoing bandwidth",
        "Maximum megabits per second forwarded per screen session. Zero removes the limit.",
        0,
        100000
    ),
    integer_setting!(
        screen_share_max_concurrent,
        "ScreenShareMaxConcurrent",
        "Screen share",
        "Concurrent screen shares",
        "Maximum active screen shares. Zero removes the limit.",
        0,
        1024
    ),
    integer_setting!(
        avatar_host_max_megabytes,
        "AvatarHostMaxMegabytes",
        "Local avatar sharing",
        "Avatar host range size",
        "Maximum megabytes in one hosted-avatar range request. Zero disables hosting.",
        0,
        4096
    ),
    integer_setting!(
        avatar_host_cache_megabytes,
        "AvatarHostCacheMegabytes",
        "Local avatar sharing",
        "Avatar host cache size",
        "Maximum megabytes retained in the hosted-avatar range cache. Zero disables caching.",
        0,
        65536
    ),
];

pub(crate) fn encode(config: &ServerConfig) -> Result<Vec<u8>> {
    let mut writer = NetWriter::new();
    writer.put_u8(AdminRequestMode::GlobalGetProtectionSettings as u8);
    writer.put_u16(u16::try_from(SETTINGS.len())?);
    for setting in SETTINGS {
        writer.put_string(setting.key)?;
        writer.put_string(setting.group)?;
        writer.put_string(setting.title)?;
        writer.put_string(setting.description)?;
        writer.put_bool(setting.whole_number);
        writer.put_f64(setting.min);
        writer.put_f64(setting.max);
        writer.put_f64((setting.get)(config));
    }
    Ok(writer.into_vec())
}

pub(crate) fn read_changes(reader: &mut NetReader<'_>) -> Result<Vec<(String, f64)>> {
    let count = reader.get_u16()? as usize;
    ensure!(count <= MAX_ENTRIES, "too many protection settings");
    let mut changes = Vec::with_capacity(count);
    for _ in 0..count {
        let key = reader.get_string()?;
        ensure!(
            key.chars().count() <= MAX_KEY_LENGTH,
            "protection setting key too long"
        );
        changes.push((key, reader.get_f64()?));
    }
    Ok(changes)
}

pub(crate) fn apply(config: &mut ServerConfig, changes: Vec<(String, f64)>) {
    for (key, value) in changes {
        if !value.is_finite() {
            continue;
        }
        if let Some(setting) = SETTINGS.iter().find(|s| s.key == key) {
            let value = value.clamp(setting.min, setting.max);
            let value = if setting.whole_number {
                value.round_ties_even()
            } else {
                value
            };
            (setting.set)(config, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn v57_lock_state_appends_hosting_video_and_limits_in_upstream_order() {
        let config = ServerConfig {
            avatar_hosting_locked: true,
            video_clips_locked: true,
            image_max_resolution_k: 8,
            video_clip_max_megabytes: 512,
            video_clip_max_per_player: 7,
            ..ServerConfig::default()
        };
        let state = basis_server_admin::GlobalState::from(&config);
        let payload = crate::encode_lock_state_payload(&state);
        assert_eq!(payload.len(), 29);
        assert_eq!(payload[0], AdminRequestMode::GlobalGetLockState as u8);
        assert_eq!(&payload[23..], &[1, 1, 8, 0, 2, 7]);
        let mut outside = config;
        outside.image_max_resolution_k = 0;
        outside.video_clip_max_megabytes = 0;
        outside.video_clip_max_per_player = u8::MAX;
        let clamped = basis_server_admin::GlobalState::from(&outside);
        assert_eq!(clamped.image_max_resolution_k, 1);
        assert_eq!(clamped.video_clip_max_megabytes, 16);
        assert_eq!(clamped.video_clip_max_per_player, 16);
    }
    #[test]
    fn v57_admin_modes_enforce_permissions_and_persist_mutations() {
        use basis_protocol::permissions::nodes;
        for mode in [
            AdminRequestMode::GlobalToggleAvatarHosting,
            AdminRequestMode::GlobalToggleVideoClips,
            AdminRequestMode::SetGlobalImageMaxResolution,
            AdminRequestMode::SetGlobalVideoClipMaxSize,
            AdminRequestMode::SetGlobalVideoClipMaxPerPlayer,
        ] {
            assert_eq!(
                crate::admin_mode_required_permission(mode),
                Some(nodes::MODERATION_GLOBAL_LOCK)
            );
            assert!(crate::admin_mode_persists_config(mode));
        }
        assert_eq!(
            crate::admin_mode_required_permission(AdminRequestMode::GetProtectionSettings),
            Some(nodes::CONFIGURATION_EDITOR)
        );
        assert_eq!(
            crate::admin_mode_required_permission(AdminRequestMode::SetProtectionSettings),
            Some(nodes::CONFIGURATION_EDITOR)
        );
        assert!(crate::admin_mode_persists_config(
            AdminRequestMode::SetProtectionSettings
        ));
    }
    #[test]
    fn protection_changes_clamp_ignore_nonfinite_and_preserve_unknown_settings() {
        let mut config = ServerConfig::default();
        apply(
            &mut config,
            vec![
                ("ScreenShareMaxConcurrent".into(), 3.5),
                ("ScreenShareMaxKilobitsPerSecond".into(), -2.0),
                ("AvatarHostMaxMegabytes".into(), f64::NAN),
                ("ScreenShareEgressMegabitsPerSecond".into(), 12.25),
                ("Password".into(), 3.0),
            ],
        );
        assert_eq!(config.screen_share_max_concurrent, 4);
        assert_eq!(config.screen_share_max_kilobits_per_second, 500);
        assert_eq!(
            config.avatar_host_max_megabytes,
            ServerConfig::default().avatar_host_max_megabytes
        );
        assert_eq!(config.screen_share_egress_megabits_per_second, 12);
        assert_eq!(config.password, ServerConfig::default().password);
    }
    #[test]
    fn protection_state_wire_has_complete_entries_and_no_extra_bytes() {
        let config = ServerConfig::default();
        let payload = encode(&config).unwrap();
        let mut reader = NetReader::new(&payload);
        assert_eq!(reader.get_u8().unwrap(), 99);
        assert_eq!(reader.get_u16().unwrap() as usize, SETTINGS.len());
        for descriptor in SETTINGS {
            assert_eq!(reader.get_string().unwrap(), descriptor.key);
            assert_eq!(reader.get_string().unwrap(), descriptor.group);
            let title = reader.get_string().unwrap();
            assert_eq!(title, descriptor.title);
            assert_ne!(title, descriptor.key);
            assert!(!reader.get_string().unwrap().is_empty());
            reader.get_bool().unwrap();
            let min = reader.get_f64().unwrap();
            let max = reader.get_f64().unwrap();
            let value = reader.get_f64().unwrap();
            assert!(value >= min && value <= max);
        }
        assert_eq!(reader.remaining(), 0);
    }
    #[test]
    fn malformed_change_batch_is_not_partially_applied() {
        let mut writer = NetWriter::new();
        writer.put_u16(2);
        writer.put_string("ScreenShareMaxConcurrent").unwrap();
        writer.put_f64(1.0);
        assert!(read_changes(&mut NetReader::new(writer.as_slice())).is_err());
        writer.clear();
        writer.put_u16(257);
        assert!(read_changes(&mut NetReader::new(writer.as_slice())).is_err());
    }
}
