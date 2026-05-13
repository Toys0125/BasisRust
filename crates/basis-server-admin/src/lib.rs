use basis_protocol::config::ServerConfig;
use parking_lot::RwLock;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
pub struct ModerationLists {
    banned_uuids: Arc<RwLock<Vec<String>>>,
    banned_ips: Arc<RwLock<Vec<String>>>,
    whitelist: Arc<RwLock<Vec<String>>>,
}

impl ModerationLists {
    pub fn is_uuid_banned(&self, uuid: &str) -> bool {
        self.banned_uuids.read().iter().any(|item| item == uuid)
    }

    pub fn is_ip_banned(&self, ip: &str) -> bool {
        self.banned_ips.read().iter().any(|item| item == ip)
    }

    pub fn is_whitelisted(&self, uuid: &str) -> bool {
        self.whitelist.read().iter().any(|item| item == uuid)
    }

    pub fn add_whitelist(&self, uuid: impl Into<String>) {
        push_unique(&self.whitelist, uuid.into());
    }

    pub fn remove_whitelist(&self, uuid: &str) -> bool {
        remove_value(&self.whitelist, uuid)
    }

    pub fn add_ban(&self, uuid: impl Into<String>) {
        push_unique(&self.banned_uuids, uuid.into());
    }

    pub fn remove_ban(&self, uuid: &str) -> bool {
        remove_value(&self.banned_uuids, uuid)
    }

    pub fn add_ip_ban(&self, ip: impl Into<String>) {
        push_unique(&self.banned_ips, ip.into());
    }

    pub fn remove_ip_ban(&self, ip: &str) -> bool {
        remove_value(&self.banned_ips, ip)
    }
}

fn push_unique(lock: &RwLock<Vec<String>>, value: String) {
    if value.trim().is_empty() {
        return;
    }
    let mut values = lock.write();
    if !values.iter().any(|item| item == &value) {
        values.push(value);
    }
}

fn remove_value(lock: &RwLock<Vec<String>>, value: &str) -> bool {
    let mut values = lock.write();
    let before = values.len();
    values.retain(|item| item != value);
    values.len() != before
}

#[derive(Debug, Clone)]
pub struct GlobalState {
    pub avatars_locked: bool,
    pub props_locked: bool,
    pub worlds_locked: bool,
    pub servers_locked: bool,
    pub third_person_disabled: bool,
    pub disallow_headless: bool,
    pub headless_audio_off: bool,
    pub opus_packet_loss_percent: u8,
    pub opus_frame_duration_ms: u8,
}

impl From<&ServerConfig> for GlobalState {
    fn from(config: &ServerConfig) -> Self {
        Self {
            avatars_locked: config.avatars_locked,
            props_locked: config.props_locked,
            worlds_locked: config.worlds_locked,
            servers_locked: config.servers_locked,
            third_person_disabled: config.third_person_disabled,
            disallow_headless: config.disallow_headless,
            headless_audio_off: false,
            opus_packet_loss_percent: 10,
            opus_frame_duration_ms: 20,
        }
    }
}
