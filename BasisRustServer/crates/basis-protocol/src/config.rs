use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{env, fs, path::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BasisUserRestrictionMode {
    #[default]
    None,
    WhiteList,
    BlackList,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename = "Configuration", rename_all = "PascalCase", default)]
pub struct ServerConfig {
    pub config_version: i32,
    pub peer_limit: i32,
    pub network_stack_id: String,
    pub set_port: u16,
    pub server_name: String,
    pub server_motd: String,
    pub company_name: String,
    pub product_name: String,
    pub use_native_sockets: bool,
    pub nat_punch_enabled: bool,
    pub ping_interval: i32,
    pub disconnect_timeout: i32,
    pub simulate_packet_loss: bool,
    pub simulate_latency: bool,
    pub simulation_packet_loss_chance: i32,
    pub simulation_min_latency: i32,
    pub simulation_max_latency: i32,
    pub reconnect_delay: i32,
    pub max_connect_attempts: i32,
    pub reuse_addresss: bool,
    pub dont_route: bool,
    pub enable_statistics: bool,
    pub ipv6_enabled: bool,
    pub mtu_override: i32,
    pub mtu_discovery: bool,
    pub disconnect_on_unreachable: bool,
    pub allow_peer_address_change: bool,
    pub has_file_support: bool,
    pub health_check_host: String,
    pub health_check_port: u16,
    pub health_path: String,
    #[serde(
        rename = "HealthIncludeBSRProfiling",
        alias = "HealthIncludeBsrProfiling"
    )]
    pub health_include_bsr_profiling: bool,
    pub idle_memory_reclaim_enabled: bool,
    pub idle_memory_reclaim_settle_seconds: i32,
    pub idle_memory_reclaim_minimum_peak: i32,
    #[serde(
        rename = "BSRSMillisecondDefaultInterval",
        alias = "BsrsmillisecondDefaultInterval"
    )]
    pub bsrsmillisecond_default_interval: i32,
    #[serde(rename = "BSRBaseMultiplier", alias = "BsrbaseMultiplier")]
    pub bsrbase_multiplier: i32,
    #[serde(rename = "BSRSIncreaseRate", alias = "BsrsincreaseRate")]
    pub bsrsincrease_rate: f32,
    #[serde(rename = "BSRSlowestSendRate", alias = "BsrslowestSendRate")]
    pub bsrslowest_send_rate: f32,
    pub distance_update_interval_ticks: i32,
    pub enable_compute_offload: bool,
    pub compute_device: String,
    pub compute_distance_update_interval_ticks: i32,
    pub high_quality_distance: f32,
    pub medium_quality_distance: f32,
    pub low_quality_distance: f32,
    pub override_auto_discovery_of_ipv: bool,
    #[serde(rename = "IPv4Address", alias = "Ipv4Address")]
    pub ipv4_address: String,
    #[serde(rename = "IPv6Address", alias = "Ipv6Address")]
    pub ipv6_address: String,
    pub password: String,
    pub use_auth: bool,
    pub use_auth_identity: bool,
    pub basis_user_restriction_mode: BasisUserRestrictionMode,
    pub how_many_duplicate_auth_can_exist: i32,
    pub auth_validation_time_out_miliseconds: i32,
    pub enable_console: bool,
    pub disable_write_unless_admin_persistent_flag: bool,
    pub disable_read_unless_admin_persistent_flag: bool,
    pub enable_avatar_bundle_compression: bool,
    pub avatar_bundle_min_messages: i32,
    pub avatar_bundle_min_bytes: i32,
    pub enable_avatar_bundle_zstd: bool,
    pub avatar_bundle_zstd_delta_bundles: bool,
    pub avatar_bundle_zstd_level: i32,
    pub avatar_bundle_zstd_max_shed_tier: i32,
    pub enable_avatar_delta_compression: bool,
    pub avatar_delta_keyframe_interval_ms: i32,
    pub avatar_delta_keyframe_max_interval_ms: i32,
    pub strip_additional_data_at_low_quality: bool,
    pub enable_uplink_avatar_delta: bool,
    pub image_cache_enabled: bool,
    pub image_cache_max_megabytes: i32,
    pub image_cache_minimum_per_owner_megabytes: i32,
    pub image_share_egress_megabits_per_second: i32,
    pub image_share_download_megabits_per_second: i32,
    pub image_share_egress_enforcement_percent: i32,
    pub image_pickup_range_meters: f32,
    #[serde(rename = "EnableBSRProfiling", alias = "EnableBsrprofiling")]
    pub enable_bsrprofiling: bool,
    pub log_connection_handshake: bool,
    #[serde(
        rename = "BSRMaxDegreeOfParallelism",
        alias = "BsrmaxDegreeOfParallelism"
    )]
    pub bsrmax_degree_of_parallelism: i32,
    #[serde(
        rename = "BSRSendPhaseBudgetPercent",
        alias = "BsrsendPhaseBudgetPercent"
    )]
    pub bsrsend_phase_budget_percent: i32,
    #[serde(rename = "BSRMaxSliceCount", alias = "BsrmaxSliceCount")]
    pub bsrmax_slice_count: i32,
    pub voice_frame_duration_ms: i32,
    pub disallow_headless: bool,
    pub avatars_locked: bool,
    pub props_locked: bool,
    pub worlds_locked: bool,
    pub servers_locked: bool,
    pub third_person_disabled: bool,
    pub additional_avatar_data_lock: bool,
    pub camera_metadata_disallow_mask: u8,
    pub crash_reporting_enabled: bool,
    pub max_microphone_range_meters: f32,
    pub max_hearing_range_meters: f32,
    pub min_avatar_eye_height_meters: f32,
    pub max_avatar_eye_height_meters: f32,
    pub max_content_spheres_per_player: i32,
    pub max_network_ids_per_player: i32,
    pub max_loaded_resources_per_player: i32,
    pub max_scene_relay_megabits_per_second_per_player: i32,
    pub playspace_mover_locked: bool,
    pub direct_connect_locked: bool,
    pub cilbox_locked: bool,
    pub images_locked: bool,
    #[serde(rename = "EndEffectorIKDisabled", alias = "EndEffectorIkDisabled")]
    pub end_effector_ik_disabled: bool,
    pub text_chat_locked: bool,
    pub voice_chat_locked: bool,
    pub media_player_locked: bool,
    pub camera_capture_locked: bool,
    pub prop_grabbing_locked: bool,
    pub safe_display_names_forced: bool,
    pub api_enabled: bool,
    pub api_host: String,
    pub api_port: u16,
    pub api_key: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            config_version: 14,
            peer_limit: u16::MAX as i32,
            network_stack_id: String::new(),
            set_port: 4296,
            server_name: "Basis Server".to_string(),
            server_motd: String::new(),
            company_name: crate::application::DEFAULT_COMPANY_NAME.to_string(),
            product_name: crate::application::DEFAULT_PRODUCT_NAME.to_string(),
            use_native_sockets: true,
            nat_punch_enabled: false,
            ping_interval: 1500,
            disconnect_timeout: 30000,
            simulate_packet_loss: false,
            simulate_latency: false,
            simulation_packet_loss_chance: 10,
            simulation_min_latency: 50,
            simulation_max_latency: 150,
            reconnect_delay: 500,
            max_connect_attempts: 10,
            reuse_addresss: false,
            dont_route: false,
            enable_statistics: true,
            ipv6_enabled: true,
            mtu_override: 0,
            mtu_discovery: true,
            disconnect_on_unreachable: false,
            allow_peer_address_change: true,
            has_file_support: true,
            health_check_host: "localhost".to_string(),
            health_check_port: 10666,
            health_path: "/health".to_string(),
            health_include_bsr_profiling: false,
            idle_memory_reclaim_enabled: true,
            idle_memory_reclaim_settle_seconds: 30,
            idle_memory_reclaim_minimum_peak: 8,
            bsrsmillisecond_default_interval: 50,
            bsrbase_multiplier: 1,
            bsrsincrease_rate: 0.005,
            bsrslowest_send_rate: 2.55,
            distance_update_interval_ticks: 125,
            enable_compute_offload: true,
            compute_device: String::new(),
            compute_distance_update_interval_ticks: 32,
            high_quality_distance: 10.0,
            medium_quality_distance: 20.0,
            low_quality_distance: 40.0,
            override_auto_discovery_of_ipv: false,
            ipv4_address: "0.0.0.0".to_string(),
            ipv6_address: "::1".to_string(),
            password: "default_password".to_string(),
            use_auth: true,
            use_auth_identity: true,
            basis_user_restriction_mode: BasisUserRestrictionMode::None,
            how_many_duplicate_auth_can_exist: 2,
            auth_validation_time_out_miliseconds: 9000,
            enable_console: true,
            disable_write_unless_admin_persistent_flag: true,
            disable_read_unless_admin_persistent_flag: false,
            enable_avatar_bundle_compression: true,
            avatar_bundle_min_messages: 2,
            avatar_bundle_min_bytes: 128,
            enable_avatar_bundle_zstd: true,
            avatar_bundle_zstd_delta_bundles: false,
            avatar_bundle_zstd_level: -2,
            avatar_bundle_zstd_max_shed_tier: 1,
            enable_avatar_delta_compression: true,
            avatar_delta_keyframe_interval_ms: 500,
            avatar_delta_keyframe_max_interval_ms: 2000,
            strip_additional_data_at_low_quality: true,
            enable_uplink_avatar_delta: true,
            image_cache_enabled: true,
            image_cache_max_megabytes: 512,
            image_cache_minimum_per_owner_megabytes: 32,
            image_share_egress_megabits_per_second: 200,
            image_share_download_megabits_per_second: 200,
            image_share_egress_enforcement_percent: 150,
            image_pickup_range_meters: 64.0,
            enable_bsrprofiling: false,
            log_connection_handshake: false,
            bsrmax_degree_of_parallelism: 0,
            bsrsend_phase_budget_percent: 0,
            bsrmax_slice_count: 0,
            voice_frame_duration_ms: 20,
            disallow_headless: false,
            avatars_locked: false,
            props_locked: false,
            worlds_locked: true,
            servers_locked: false,
            third_person_disabled: false,
            additional_avatar_data_lock: false,
            camera_metadata_disallow_mask: 0,
            crash_reporting_enabled: true,
            max_microphone_range_meters: 25.0,
            max_hearing_range_meters: 25.0,
            min_avatar_eye_height_meters: 0.1,
            max_avatar_eye_height_meters: 100.0,
            max_content_spheres_per_player: 32,
            max_network_ids_per_player: 32768,
            max_loaded_resources_per_player: 16384,
            max_scene_relay_megabits_per_second_per_player: 0,
            playspace_mover_locked: false,
            direct_connect_locked: false,
            cilbox_locked: false,
            images_locked: false,
            end_effector_ik_disabled: false,
            text_chat_locked: false,
            voice_chat_locked: false,
            media_player_locked: false,
            camera_capture_locked: false,
            prop_grabbing_locked: false,
            safe_display_names_forced: false,
            api_enabled: false,
            api_host: "localhost".to_string(),
            api_port: 10667,
            api_key: String::new(),
        }
    }
}

impl ServerConfig {
    pub const CURRENT_CONFIG_VERSION: i32 = 14;
    pub const CONFIG_FOLDER_NAME: &'static str = "config";
    pub const LOGS_FOLDER_NAME: &'static str = "logs";
    pub const INITIAL_RESOURCES_FOLDER_NAME: &'static str = "initialresources";
    pub const DEFAULT_LIBRARY_FOLDER_NAME: &'static str = "defaultlibrary";

    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            let text = fs::read_to_string(path)
                .with_context(|| format!("reading config {}", path.display()))?;
            let mut config = quick_xml::de::from_str::<Self>(&text)
                .with_context(|| format!("parsing config {}", path.display()))?;
            if config.config_version != Self::CURRENT_CONFIG_VERSION {
                config.config_version = Self::CURRENT_CONFIG_VERSION;
                config.save(path)?;
            }
            return Ok(config);
        }

        let config = Self::default();
        config.save(path)?;
        Ok(config)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating config directory {}", parent.display()))?;
        }
        let mut xml = String::new();
        let mut serializer = quick_xml::se::Serializer::new(&mut xml);
        serializer.indent(' ', 2);
        self.serialize(serializer)?;
        xml.push('\n');

        fs::write(path, xml).with_context(|| format!("writing config {}", path.display()))?;
        Ok(())
    }

    pub fn process_environment_overrides(&mut self) {
        macro_rules! override_field {
            ($env_name:literal, $field:ident, $ty:ty) => {
                if let Ok(value) = env::var($env_name) {
                    if let Ok(parsed) = value.parse::<$ty>() {
                        self.$field = parsed;
                    }
                }
            };
        }
        macro_rules! override_string {
            ($env_name:literal, $field:ident) => {
                if let Ok(value) = env::var($env_name) {
                    self.$field = value;
                }
            };
        }

        override_field!("ConfigVersion", config_version, i32);
        override_field!("PeerLimit", peer_limit, i32);
        override_string!("NetworkStackId", network_stack_id);
        override_field!("SetPort", set_port, u16);
        override_string!("ServerName", server_name);
        override_string!("ServerMotd", server_motd);
        override_string!("CompanyName", company_name);
        override_string!("ProductName", product_name);
        override_field!("UseNativeSockets", use_native_sockets, bool);
        override_field!("NatPunchEnabled", nat_punch_enabled, bool);
        override_field!("PingInterval", ping_interval, i32);
        override_field!("DisconnectTimeout", disconnect_timeout, i32);
        override_field!("SimulatePacketLoss", simulate_packet_loss, bool);
        override_field!("SimulateLatency", simulate_latency, bool);
        override_field!(
            "SimulationPacketLossChance",
            simulation_packet_loss_chance,
            i32
        );
        override_field!("SimulationMinLatency", simulation_min_latency, i32);
        override_field!("SimulationMaxLatency", simulation_max_latency, i32);
        override_field!("ReconnectDelay", reconnect_delay, i32);
        override_field!("MaxConnectAttempts", max_connect_attempts, i32);
        override_field!("ReuseAddresss", reuse_addresss, bool);
        override_field!("DontRoute", dont_route, bool);
        override_field!("EnableStatistics", enable_statistics, bool);
        override_field!("IPv6Enabled", ipv6_enabled, bool);
        override_field!("MtuOverride", mtu_override, i32);
        override_field!("MtuDiscovery", mtu_discovery, bool);
        override_field!("DisconnectOnUnreachable", disconnect_on_unreachable, bool);
        override_field!("AllowPeerAddressChange", allow_peer_address_change, bool);
        override_field!("HasFileSupport", has_file_support, bool);
        override_string!("HealthCheckHost", health_check_host);
        override_field!("HealthCheckPort", health_check_port, u16);
        override_string!("HealthPath", health_path);
        override_field!(
            "BSRSMillisecondDefaultInterval",
            bsrsmillisecond_default_interval,
            i32
        );
        override_field!("BSRBaseMultiplier", bsrbase_multiplier, i32);
        override_field!("BSRSIncreaseRate", bsrsincrease_rate, f32);
        override_field!("BSRSlowestSendRate", bsrslowest_send_rate, f32);
        override_field!("HighQualityDistance", high_quality_distance, f32);
        override_field!("MediumQualityDistance", medium_quality_distance, f32);
        override_field!("LowQualityDistance", low_quality_distance, f32);
        override_field!(
            "OverrideAutoDiscoveryOfIpv",
            override_auto_discovery_of_ipv,
            bool
        );
        override_string!("IPv4Address", ipv4_address);
        override_string!("IPv6Address", ipv6_address);
        override_string!("Password", password);
        override_field!("UseAuth", use_auth, bool);
        override_field!("UseAuthIdentity", use_auth_identity, bool);
        override_field!(
            "HowManyDuplicateAuthCanExist",
            how_many_duplicate_auth_can_exist,
            i32
        );
        override_field!(
            "AuthValidationTimeOutMiliseconds",
            auth_validation_time_out_miliseconds,
            i32
        );
        override_field!("EnableConsole", enable_console, bool);
        override_field!(
            "DisableWriteUnlessAdminPersistentFlag",
            disable_write_unless_admin_persistent_flag,
            bool
        );
        override_field!(
            "DisableReadUnlessAdminPersistentFlag",
            disable_read_unless_admin_persistent_flag,
            bool
        );
        override_field!(
            "EnableAvatarBundleCompression",
            enable_avatar_bundle_compression,
            bool
        );
        override_field!("AvatarBundleMinMessages", avatar_bundle_min_messages, i32);
        override_field!("AvatarBundleMinBytes", avatar_bundle_min_bytes, i32);
        override_field!("EnableBSRProfiling", enable_bsrprofiling, bool);
        override_field!("DisallowHeadless", disallow_headless, bool);
        override_field!("AvatarsLocked", avatars_locked, bool);
        override_field!("PropsLocked", props_locked, bool);
        override_field!("WorldsLocked", worlds_locked, bool);
        override_field!("ServersLocked", servers_locked, bool);
        override_field!("ThirdPersonDisabled", third_person_disabled, bool);
        override_field!(
            "AdditionalAvatarDataLock",
            additional_avatar_data_lock,
            bool
        );
        override_field!(
            "HealthIncludeBSRProfiling",
            health_include_bsr_profiling,
            bool
        );
        override_field!(
            "IdleMemoryReclaimEnabled",
            idle_memory_reclaim_enabled,
            bool
        );
        override_field!(
            "IdleMemoryReclaimSettleSeconds",
            idle_memory_reclaim_settle_seconds,
            i32
        );
        override_field!(
            "IdleMemoryReclaimMinimumPeak",
            idle_memory_reclaim_minimum_peak,
            i32
        );
        override_field!(
            "DistanceUpdateIntervalTicks",
            distance_update_interval_ticks,
            i32
        );
        override_field!("EnableComputeOffload", enable_compute_offload, bool);
        override_string!("ComputeDevice", compute_device);
        override_field!(
            "ComputeDistanceUpdateIntervalTicks",
            compute_distance_update_interval_ticks,
            i32
        );
        override_field!("EnableAvatarBundleZstd", enable_avatar_bundle_zstd, bool);
        override_field!(
            "AvatarBundleZstdDeltaBundles",
            avatar_bundle_zstd_delta_bundles,
            bool
        );
        override_field!("AvatarBundleZstdLevel", avatar_bundle_zstd_level, i32);
        override_field!(
            "AvatarBundleZstdMaxShedTier",
            avatar_bundle_zstd_max_shed_tier,
            i32
        );
        override_field!(
            "EnableAvatarDeltaCompression",
            enable_avatar_delta_compression,
            bool
        );
        override_field!(
            "AvatarDeltaKeyframeIntervalMs",
            avatar_delta_keyframe_interval_ms,
            i32
        );
        override_field!(
            "AvatarDeltaKeyframeMaxIntervalMs",
            avatar_delta_keyframe_max_interval_ms,
            i32
        );
        override_field!(
            "StripAdditionalDataAtLowQuality",
            strip_additional_data_at_low_quality,
            bool
        );
        override_field!("EnableUplinkAvatarDelta", enable_uplink_avatar_delta, bool);
        override_field!("ImageCacheEnabled", image_cache_enabled, bool);
        override_field!("ImageCacheMaxMegabytes", image_cache_max_megabytes, i32);
        override_field!(
            "ImageCacheMinimumPerOwnerMegabytes",
            image_cache_minimum_per_owner_megabytes,
            i32
        );
        override_field!(
            "ImageShareEgressMegabitsPerSecond",
            image_share_egress_megabits_per_second,
            i32
        );
        override_field!(
            "ImageShareDownloadMegabitsPerSecond",
            image_share_download_megabits_per_second,
            i32
        );
        override_field!(
            "ImageShareEgressEnforcementPercent",
            image_share_egress_enforcement_percent,
            i32
        );
        override_field!("ImagePickupRangeMeters", image_pickup_range_meters, f32);
        override_field!("LogConnectionHandshake", log_connection_handshake, bool);
        override_field!(
            "BSRMaxDegreeOfParallelism",
            bsrmax_degree_of_parallelism,
            i32
        );
        override_field!(
            "BSRSendPhaseBudgetPercent",
            bsrsend_phase_budget_percent,
            i32
        );
        override_field!("BSRMaxSliceCount", bsrmax_slice_count, i32);
        override_field!("VoiceFrameDurationMs", voice_frame_duration_ms, i32);
        override_field!(
            "CameraMetadataDisallowMask",
            camera_metadata_disallow_mask,
            u8
        );
        override_field!("CrashReportingEnabled", crash_reporting_enabled, bool);
        override_field!("MaxMicrophoneRangeMeters", max_microphone_range_meters, f32);
        override_field!("MaxHearingRangeMeters", max_hearing_range_meters, f32);
        override_field!(
            "MinAvatarEyeHeightMeters",
            min_avatar_eye_height_meters,
            f32
        );
        override_field!(
            "MaxAvatarEyeHeightMeters",
            max_avatar_eye_height_meters,
            f32
        );
        override_field!(
            "MaxContentSpheresPerPlayer",
            max_content_spheres_per_player,
            i32
        );
        override_field!("MaxNetworkIdsPerPlayer", max_network_ids_per_player, i32);
        override_field!(
            "MaxLoadedResourcesPerPlayer",
            max_loaded_resources_per_player,
            i32
        );
        override_field!(
            "MaxSceneRelayMegabitsPerSecondPerPlayer",
            max_scene_relay_megabits_per_second_per_player,
            i32
        );
        override_field!("PlayspaceMoverLocked", playspace_mover_locked, bool);
        override_field!("DirectConnectLocked", direct_connect_locked, bool);
        override_field!("CilboxLocked", cilbox_locked, bool);
        override_field!("ImagesLocked", images_locked, bool);
        override_field!("EndEffectorIKDisabled", end_effector_ik_disabled, bool);
        override_field!("TextChatLocked", text_chat_locked, bool);
        override_field!("VoiceChatLocked", voice_chat_locked, bool);
        override_field!("MediaPlayerLocked", media_player_locked, bool);
        override_field!("CameraCaptureLocked", camera_capture_locked, bool);
        override_field!("PropGrabbingLocked", prop_grabbing_locked, bool);
        override_field!("SafeDisplayNamesForced", safe_display_names_forced, bool);
        override_field!("ApiEnabled", api_enabled, bool);
        override_string!("ApiHost", api_host);
        override_field!("ApiPort", api_port, u16);
        override_string!("ApiKey", api_key);

        if let Ok(value) = env::var("BasisUserRestrictionMode") {
            self.basis_user_restriction_mode = match value.as_str() {
                "WhiteList" | "Whitelist" | "whitelist" => BasisUserRestrictionMode::WhiteList,
                "BlackList" | "Blacklist" | "blacklist" => BasisUserRestrictionMode::BlackList,
                _ => BasisUserRestrictionMode::None,
            };
        }
    }

    pub fn is_secret_field_name(name: &str) -> bool {
        let name = name.to_ascii_lowercase();
        name.contains("password")
            || name.contains("apikey")
            || name.contains("secret")
            || name.contains("token")
    }

    pub fn field_names(&self) -> Vec<String> {
        serde_json::to_value(self)
            .ok()
            .and_then(|value| value.as_object().cloned())
            .map(|object| object.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn get_field(&self, name: &str) -> Option<String> {
        let value = serde_json::to_value(self).ok()?;
        let object = value.as_object()?;
        let (_, field) = object
            .iter()
            .find(|(field_name, _)| field_name.eq_ignore_ascii_case(name))?;
        Some(match field {
            serde_json::Value::String(value) => value.clone(),
            serde_json::Value::Bool(value) => value.to_string(),
            serde_json::Value::Number(value) => value.to_string(),
            serde_json::Value::Null => String::new(),
            other => other.to_string(),
        })
    }

    pub fn set_field(&mut self, name: &str, value: &str) -> Result<()> {
        let mut serialized = serde_json::to_value(&*self)?;
        let object = serialized
            .as_object_mut()
            .context("server configuration did not serialize as an object")?;
        let field_name = object
            .keys()
            .find(|field_name| field_name.eq_ignore_ascii_case(name))
            .cloned()
            .with_context(|| format!("unknown config field {name}"))?;
        let current = object
            .get(&field_name)
            .cloned()
            .context("configuration field disappeared during update")?;
        let updated = match current {
            serde_json::Value::Bool(_) => serde_json::Value::Bool(value.parse()?),
            serde_json::Value::Number(number) if number.is_f64() => {
                let parsed: f64 = value.parse()?;
                serde_json::Value::Number(
                    serde_json::Number::from_f64(parsed)
                        .context("floating-point config value must be finite")?,
                )
            }
            serde_json::Value::Number(number) if number.is_u64() => {
                serde_json::Value::Number(serde_json::Number::from(value.parse::<u64>()?))
            }
            serde_json::Value::Number(_) => {
                serde_json::Value::Number(serde_json::Number::from(value.parse::<i64>()?))
            }
            serde_json::Value::String(_)
                if field_name.eq_ignore_ascii_case("BasisUserRestrictionMode") =>
            {
                let mode = if value.eq_ignore_ascii_case("WhiteList")
                    || value.eq_ignore_ascii_case("Whitelist")
                {
                    "WhiteList"
                } else if value.eq_ignore_ascii_case("BlackList")
                    || value.eq_ignore_ascii_case("Blacklist")
                {
                    "BlackList"
                } else if value.eq_ignore_ascii_case("None") {
                    "None"
                } else {
                    anyhow::bail!("invalid BasisUserRestrictionMode {value}");
                };
                serde_json::Value::String(mode.to_string())
            }
            serde_json::Value::String(_) => serde_json::Value::String(value.to_string()),
            _ => anyhow::bail!("unsupported config field type for {field_name}"),
        };
        object.insert(field_name, updated);
        *self = serde_json::from_value(serialized)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn defaults_match_current_csharp_server() {
        let config = ServerConfig::default();
        assert_eq!(config.peer_limit, u16::MAX as i32);
        assert_eq!(config.set_port, 4296);
        assert_eq!(config.password, "default_password");
        assert_eq!(config.config_version, ServerConfig::CURRENT_CONFIG_VERSION);
        assert_eq!(
            config.company_name,
            crate::application::DEFAULT_COMPANY_NAME
        );
        assert_eq!(
            config.product_name,
            crate::application::DEFAULT_PRODUCT_NAME
        );
        assert!(config.use_auth);
        assert!(config.use_auth_identity);
        assert!(config.worlds_locked);
        assert!(!config.avatars_locked);
    }

    #[test]
    fn config_round_trips_xml() {
        let config = ServerConfig::default();
        let xml = quick_xml::se::to_string(&config).unwrap();
        assert!(xml.contains("<SetPort>4296</SetPort>"));
        assert!(xml.contains("<ServerName>Basis Server</ServerName>"));
        assert!(xml.contains("<CompanyName>Basis Unity</CompanyName>"));
        assert!(xml.contains("<ProductName>Basis Unity</ProductName>"));
        assert!(xml.contains("<BSRSMillisecondDefaultInterval>50</BSRSMillisecondDefaultInterval>"));
        assert!(xml.contains("<IPv4Address>0.0.0.0</IPv4Address>"));
        assert!(xml.contains("<EndEffectorIKDisabled>false</EndEffectorIKDisabled>"));
        let parsed: ServerConfig = quick_xml::de::from_str(&xml).unwrap();
        assert_eq!(parsed, config);
    }

    #[test]
    fn dynamic_config_access_covers_current_fields() {
        let mut config = ServerConfig::default();
        assert!(config
            .field_names()
            .iter()
            .any(|name| name == "AvatarBundleZstdLevel"));
        assert_eq!(
            config.get_field("BSRMaxDegreeOfParallelism").as_deref(),
            Some("0")
        );
        config.set_field("AvatarBundleZstdLevel", "-5").unwrap();
        config.set_field("ImagePickupRangeMeters", "42.5").unwrap();
        config.set_field("CompanyName", "Custom Company").unwrap();
        config.set_field("ProductName", "Custom Product").unwrap();
        config
            .set_field("BasisUserRestrictionMode", "blacklist")
            .unwrap();
        assert_eq!(config.avatar_bundle_zstd_level, -5);
        assert_eq!(config.image_pickup_range_meters, 42.5);
        assert_eq!(config.company_name, "Custom Company");
        assert_eq!(config.product_name, "Custom Product");
        assert_eq!(
            config.basis_user_restriction_mode,
            BasisUserRestrictionMode::BlackList
        );
        assert!(config.set_field("DefinitelyNotAField", "1").is_err());
        assert!(ServerConfig::is_secret_field_name("ApiKey"));
        assert!(ServerConfig::is_secret_field_name("Password"));
    }

    #[test]
    fn missing_config_is_created() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("basis-config-test-{unique}"));
        let path = dir.join("config.xml");
        let config = ServerConfig::load_or_create(&path).unwrap();
        assert_eq!(config.set_port, 4296);
        assert!(path.exists());

        let xml = std::fs::read_to_string(&path).unwrap();
        assert!(xml.starts_with("<Configuration>\n"));
        assert!(
            xml.contains("\n  <ConfigVersion>14</ConfigVersion>\n  <PeerLimit>65535</PeerLimit>\n")
        );
        assert!(xml.contains(
            "\n  <CompanyName>Basis Unity</CompanyName>\n  <ProductName>Basis Unity</ProductName>\n"
        ));
        assert!(xml.ends_with("\n</Configuration>\n"));

        let _ = std::fs::remove_dir_all(dir);
    }
}
