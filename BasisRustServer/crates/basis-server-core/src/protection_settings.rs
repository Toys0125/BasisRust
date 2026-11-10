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

macro_rules! settings {
    ($call:ident, $context:expr) => {
        $call!(
            $context,
            "MaxSceneRelayMegabitsPerSecondPerPlayer",
            "Relays",
            max_scene_relay_megabits_per_second_per_player,
            i32,
            0,
            100000
        );
        $call!(
            $context,
            "MaxLoadedResourcesPerPlayer",
            "Per-player caps",
            max_loaded_resources_per_player,
            i32,
            1,
            1000000
        );
        $call!(
            $context,
            "MaxNetworkIdsPerPlayer",
            "Per-player caps",
            max_network_ids_per_player,
            i32,
            1,
            65536
        );
        $call!(
            $context,
            "ScreenShareMaxKilobitsPerSecond",
            "Screen share",
            screen_share_max_kilobits_per_second,
            i32,
            500,
            100000
        );
        $call!(
            $context,
            "ScreenShareEgressMegabitsPerSecond",
            "Screen share",
            screen_share_egress_megabits_per_second,
            i32,
            0,
            100000
        );
        $call!(
            $context,
            "ScreenShareMaxConcurrent",
            "Screen share",
            screen_share_max_concurrent,
            i32,
            0,
            1024
        );
        $call!(
            $context,
            "AvatarHostMaxMegabytes",
            "Local avatar sharing",
            avatar_host_max_megabytes,
            i32,
            0,
            4096
        );
        $call!(
            $context,
            "AvatarHostCacheMegabytes",
            "Local avatar sharing",
            avatar_host_cache_megabytes,
            i32,
            0,
            65536
        );
    };
}

pub(crate) fn encode(config: &ServerConfig) -> Result<Vec<u8>> {
    let mut writer = NetWriter::new();
    writer.put_u8(AdminRequestMode::GlobalGetProtectionSettings as u8);
    writer.put_u16(8);
    macro_rules! write {
        ($c:expr, $key:literal, $group:literal, $field:ident, $kind:ident, $min:literal, $max:literal) => {
            writer.put_string($key)?;
            writer.put_string($group)?;
            writer.put_string($key)?;
            writer.put_string("")?;
            writer.put_bool(stringify!($kind) != "f64");
            writer.put_f64($min as f64);
            writer.put_f64($max as f64);
            writer.put_f64($c.$field as f64);
        };
    }
    settings!(write, config);
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
        macro_rules! set {
            ($c:expr, $key:literal, $group:literal, $field:ident, $kind:ident, $min:literal, $max:literal) => {
                if key == $key {
                    let mut value = value.clamp($min as f64, $max as f64);
                    if stringify!($kind) != "f64" {
                        value = value.round_ties_even();
                    }
                    $c.$field = value as $kind;
                }
            };
        }
        settings!(set, config);
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
        assert_eq!(reader.get_u16().unwrap(), 8);
        for _ in 0..8 {
            assert!(!reader.get_string().unwrap().is_empty());
            reader.get_string().unwrap();
            reader.get_string().unwrap();
            reader.get_string().unwrap();
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
