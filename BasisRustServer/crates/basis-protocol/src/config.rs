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
#[serde(rename = "Configuration", rename_all = "PascalCase")]
pub struct ServerConfig {
    pub peer_limit: i32,
    pub network_stack_id: String,
    pub set_port: u16,
    pub server_name: String,
    pub server_motd: String,
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
    pub bsrsmillisecond_default_interval: i32,
    pub bsrbase_multiplier: i32,
    pub bsrsincrease_rate: f32,
    pub bsrslowest_send_rate: f32,
    pub high_quality_distance: f32,
    pub medium_quality_distance: f32,
    pub low_quality_distance: f32,
    pub override_auto_discovery_of_ipv: bool,
    pub ipv4_address: String,
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
    pub enable_bsrprofiling: bool,
    pub disallow_headless: bool,
    pub avatars_locked: bool,
    pub props_locked: bool,
    pub worlds_locked: bool,
    pub servers_locked: bool,
    pub third_person_disabled: bool,
    pub additional_avatar_data_lock: bool,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            peer_limit: u16::MAX as i32,
            network_stack_id: String::new(),
            set_port: 4296,
            server_name: "Basis Server".to_string(),
            server_motd: String::new(),
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
            bsrsmillisecond_default_interval: 50,
            bsrbase_multiplier: 1,
            bsrsincrease_rate: 0.005,
            bsrslowest_send_rate: 2.55,
            high_quality_distance: 3.0,
            medium_quality_distance: 10.0,
            low_quality_distance: 20.0,
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
            enable_avatar_bundle_compression: false,
            avatar_bundle_min_messages: 4,
            avatar_bundle_min_bytes: 128,
            enable_bsrprofiling: false,
            disallow_headless: false,
            avatars_locked: false,
            props_locked: false,
            worlds_locked: true,
            servers_locked: false,
            third_person_disabled: false,
            additional_avatar_data_lock: false,
        }
    }
}

impl ServerConfig {
    pub const CONFIG_FOLDER_NAME: &'static str = "config";
    pub const LOGS_FOLDER_NAME: &'static str = "logs";
    pub const INITIAL_RESOURCES_FOLDER_NAME: &'static str = "initialresources";
    pub const DEFAULT_LIBRARY_FOLDER_NAME: &'static str = "defaultlibrary";

    pub fn load_or_create(path: &Path) -> Result<Self> {
        if path.exists() {
            let text = fs::read_to_string(path)
                .with_context(|| format!("reading config {}", path.display()))?;
            let config = quick_xml::de::from_str::<Self>(&text)
                .with_context(|| format!("parsing config {}", path.display()))?;
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
        let xml = quick_xml::se::to_string(self)?;
        fs::write(path, format!("{xml}\n"))
            .with_context(|| format!("writing config {}", path.display()))?;
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

        override_field!("PeerLimit", peer_limit, i32);
        override_string!("NetworkStackId", network_stack_id);
        override_field!("SetPort", set_port, u16);
        override_string!("ServerName", server_name);
        override_string!("ServerMotd", server_motd);
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

        if let Ok(value) = env::var("BasisUserRestrictionMode") {
            self.basis_user_restriction_mode = match value.as_str() {
                "WhiteList" | "Whitelist" | "whitelist" => BasisUserRestrictionMode::WhiteList,
                "BlackList" | "Blacklist" | "blacklist" => BasisUserRestrictionMode::BlackList,
                _ => BasisUserRestrictionMode::None,
            };
        }
    }

    pub fn get_field(&self, name: &str) -> Option<String> {
        let key = name.to_ascii_lowercase();
        Some(match key.as_str() {
            "peerlimit" => self.peer_limit.to_string(),
            "networkstackid" => self.network_stack_id.clone(),
            "setport" => self.set_port.to_string(),
            "servername" => self.server_name.clone(),
            "servermotd" => self.server_motd.clone(),
            "enableconsole" => self.enable_console.to_string(),
            "password" => self.password.clone(),
            "useauth" => self.use_auth.to_string(),
            "useauthidentity" => self.use_auth_identity.to_string(),
            "healthcheckhost" => self.health_check_host.clone(),
            "healthcheckport" => self.health_check_port.to_string(),
            "healthpath" => self.health_path.clone(),
            "avatarslocked" => self.avatars_locked.to_string(),
            "propslocked" => self.props_locked.to_string(),
            "worldslocked" => self.worlds_locked.to_string(),
            "serverslocked" => self.servers_locked.to_string(),
            "thirdpersondisabled" => self.third_person_disabled.to_string(),
            "additionalavatardatalock" => self.additional_avatar_data_lock.to_string(),
            _ => return None,
        })
    }

    pub fn set_field(&mut self, name: &str, value: &str) -> Result<()> {
        let key = name.to_ascii_lowercase();
        match key.as_str() {
            "peerlimit" => self.peer_limit = value.parse()?,
            "networkstackid" => self.network_stack_id = value.to_string(),
            "setport" => self.set_port = value.parse()?,
            "servername" => self.server_name = value.to_string(),
            "servermotd" => self.server_motd = value.to_string(),
            "enableconsole" => self.enable_console = value.parse()?,
            "password" => self.password = value.to_string(),
            "useauth" => self.use_auth = value.parse()?,
            "useauthidentity" => self.use_auth_identity = value.parse()?,
            "healthcheckhost" => self.health_check_host = value.to_string(),
            "healthcheckport" => self.health_check_port = value.parse()?,
            "healthpath" => self.health_path = value.to_string(),
            "avatarslocked" => self.avatars_locked = value.parse()?,
            "propslocked" => self.props_locked = value.parse()?,
            "worldslocked" => self.worlds_locked = value.parse()?,
            "serverslocked" => self.servers_locked = value.parse()?,
            "thirdpersondisabled" => self.third_person_disabled = value.parse()?,
            "additionalavatardatalock" => self.additional_avatar_data_lock = value.parse()?,
            _ => anyhow::bail!("unknown config field {name}"),
        }
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
        let parsed: ServerConfig = quick_xml::de::from_str(&xml).unwrap();
        assert_eq!(parsed, config);
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
        let _ = std::fs::remove_dir_all(dir);
    }
}
