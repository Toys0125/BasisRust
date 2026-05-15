use anyhow::{Context, Result};
use basis_protocol::config::ServerConfig;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone, Default)]
pub struct ModerationLists {
    banned_uuids: Arc<RwLock<Vec<String>>>,
    banned_ips: Arc<RwLock<Vec<String>>>,
    banned_players: Arc<RwLock<Vec<BannedPlayer>>>,
    blacklist: Arc<RwLock<Vec<String>>>,
    whitelist: Arc<RwLock<Vec<String>>>,
    paths: Arc<RwLock<Option<ModerationPaths>>>,
}

#[derive(Debug, Clone)]
struct ModerationPaths {
    banned_players: PathBuf,
    whitelist: PathBuf,
    blacklist: PathBuf,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct BannedPlayer {
    #[serde(default, rename = "UUID")]
    pub uuid: String,
    #[serde(default)]
    pub banned_ip: String,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub has_banned_ip: bool,
    #[serde(default)]
    pub time_of_ban: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename = "ArrayOfBannedPlayer")]
struct BannedPlayersXml {
    #[serde(rename = "BannedPlayer", default)]
    players: Vec<BannedPlayer>,
}

impl ModerationLists {
    pub fn file_backed(config_dir: impl AsRef<Path>) -> Result<Self> {
        let config_dir = config_dir.as_ref();
        fs::create_dir_all(config_dir).with_context(|| {
            format!(
                "creating moderation config directory {}",
                config_dir.display()
            )
        })?;
        let lists = Self {
            paths: Arc::new(RwLock::new(Some(ModerationPaths {
                banned_players: config_dir.join("banned_players.xml"),
                whitelist: config_dir.join("BasisWhiteList.txt"),
                blacklist: config_dir.join("BasisBlackList.txt"),
            }))),
            ..Self::default()
        };
        lists.reload()?;
        Ok(lists)
    }

    pub fn reload(&self) -> Result<()> {
        let Some(paths) = self.paths.read().clone() else {
            return Ok(());
        };
        self.load_banned_players(&paths.banned_players)?;
        replace_values(&self.whitelist, read_line_list(&paths.whitelist)?);
        replace_values(&self.blacklist, read_line_list(&paths.blacklist)?);
        Ok(())
    }

    pub fn is_uuid_banned(&self, uuid: &str) -> bool {
        self.banned_uuids.read().iter().any(|item| item == uuid)
    }

    pub fn is_ip_banned(&self, ip: &str) -> bool {
        self.banned_ips.read().iter().any(|item| item == ip)
    }

    pub fn is_whitelisted(&self, uuid: &str) -> bool {
        self.whitelist.read().iter().any(|item| item == uuid)
    }

    pub fn is_blacklisted(&self, uuid: &str) -> bool {
        self.blacklist.read().iter().any(|item| item == uuid)
    }

    pub fn add_whitelist(&self, uuid: impl Into<String>) -> Result<()> {
        push_unique(&self.whitelist, uuid.into());
        self.save_whitelist()
    }

    pub fn remove_whitelist(&self, uuid: &str) -> Result<bool> {
        let changed = remove_value(&self.whitelist, uuid);
        if changed {
            self.save_whitelist()?;
        }
        Ok(changed)
    }

    pub fn add_blacklist(&self, uuid: impl Into<String>) -> Result<()> {
        push_unique(&self.blacklist, uuid.into());
        self.save_blacklist()
    }

    pub fn remove_blacklist(&self, uuid: &str) -> Result<bool> {
        let changed = remove_value(&self.blacklist, uuid);
        if changed {
            self.save_blacklist()?;
        }
        Ok(changed)
    }

    pub fn add_ban(&self, uuid: impl Into<String>) -> Result<()> {
        self.add_ban_with_details(uuid, "", None)
    }

    pub fn add_ban_with_details(
        &self,
        uuid: impl Into<String>,
        reason: impl Into<String>,
        banned_ip: Option<String>,
    ) -> Result<()> {
        let uuid = uuid.into();
        if uuid.trim().is_empty() {
            return Ok(());
        }
        let reason = reason.into();
        let has_banned_ip = banned_ip
            .as_ref()
            .map(|ip| !ip.trim().is_empty())
            .unwrap_or(false);
        let banned_player = BannedPlayer {
            uuid: uuid.clone(),
            banned_ip: banned_ip.unwrap_or_default(),
            reason,
            has_banned_ip,
            time_of_ban: utc_timestamp_string(),
        };
        {
            let mut players = self.banned_players.write();
            if let Some(existing) = players.iter_mut().find(|player| player.uuid == uuid) {
                *existing = banned_player;
            } else {
                players.push(banned_player);
            }
        }
        self.rebuild_ban_indexes();
        self.save_banned_players()
    }

    pub fn remove_ban(&self, uuid: &str) -> Result<bool> {
        let mut players = self.banned_players.write();
        let before = players.len();
        players.retain(|player| player.uuid != uuid);
        let changed = players.len() != before;
        drop(players);
        if changed {
            self.rebuild_ban_indexes();
            self.save_banned_players()?;
        }
        Ok(changed)
    }

    pub fn add_ip_ban(&self, ip: impl Into<String>) -> Result<()> {
        let ip = ip.into();
        if ip.trim().is_empty() {
            return Ok(());
        }
        {
            let mut players = self.banned_players.write();
            let synthetic_uuid = format!("ip-ban:{ip}");
            let banned_player = BannedPlayer {
                uuid: synthetic_uuid.clone(),
                banned_ip: ip,
                reason: "IP banned".to_string(),
                has_banned_ip: true,
                time_of_ban: utc_timestamp_string(),
            };
            if let Some(existing) = players
                .iter_mut()
                .find(|player| player.uuid == synthetic_uuid)
            {
                *existing = banned_player;
            } else {
                players.push(banned_player);
            }
        }
        self.rebuild_ban_indexes();
        self.save_banned_players()
    }

    pub fn remove_ip_ban(&self, ip: &str) -> Result<bool> {
        let mut players = self.banned_players.write();
        let before = players.len();
        players.retain(|player| !(player.has_banned_ip && player.banned_ip == ip));
        let changed = players.len() != before;
        drop(players);
        if changed {
            self.rebuild_ban_indexes();
            self.save_banned_players()?;
            return Ok(true);
        }
        let changed = remove_value(&self.banned_ips, ip);
        if changed {
            self.save_banned_players()?;
        }
        Ok(changed)
    }

    fn load_banned_players(&self, path: &Path) -> Result<()> {
        if !path.exists() {
            self.save_banned_players_to(path)?;
            return Ok(());
        }
        let xml = fs::read_to_string(path)
            .with_context(|| format!("reading banned player file {}", path.display()))?;
        let parsed = quick_xml::de::from_str::<BannedPlayersXml>(&xml).unwrap_or_default();
        *self.banned_players.write() = parsed.players;
        self.rebuild_ban_indexes();
        Ok(())
    }

    fn rebuild_ban_indexes(&self) {
        let players = self.banned_players.read();
        let mut uuids = Vec::with_capacity(players.len());
        let mut ips = Vec::new();
        for player in players.iter() {
            if !player.uuid.trim().is_empty() && !uuids.iter().any(|item| item == &player.uuid) {
                uuids.push(player.uuid.clone());
            }
            if player.has_banned_ip
                && !player.banned_ip.trim().is_empty()
                && !ips.iter().any(|item| item == &player.banned_ip)
            {
                ips.push(player.banned_ip.clone());
            }
        }
        *self.banned_uuids.write() = uuids;
        *self.banned_ips.write() = ips;
    }

    fn save_banned_players(&self) -> Result<()> {
        if let Some(paths) = self.paths.read().clone() {
            self.save_banned_players_to(&paths.banned_players)?;
        }
        Ok(())
    }

    fn save_banned_players_to(&self, path: &Path) -> Result<()> {
        let xml = quick_xml::se::to_string(&BannedPlayersXml {
            players: self.banned_players.read().clone(),
        })
        .context("serializing banned_players.xml")?;
        fs::write(path, xml)
            .with_context(|| format!("writing banned player file {}", path.display()))
    }

    fn save_whitelist(&self) -> Result<()> {
        if let Some(paths) = self.paths.read().clone() {
            write_line_list(&paths.whitelist, &self.whitelist.read())?;
        }
        Ok(())
    }

    fn save_blacklist(&self) -> Result<()> {
        if let Some(paths) = self.paths.read().clone() {
            write_line_list(&paths.blacklist, &self.blacklist.read())?;
        }
        Ok(())
    }
}

fn read_line_list(path: &Path) -> Result<Vec<String>> {
    if !path.exists() {
        fs::write(path, "").with_context(|| format!("creating {}", path.display()))?;
        return Ok(Vec::new());
    }
    let text = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}

fn write_line_list(path: &Path, values: &[String]) -> Result<()> {
    let mut text = String::new();
    for value in values {
        if !value.trim().is_empty() {
            text.push_str(value);
            text.push('\n');
        }
    }
    fs::write(path, text).with_context(|| format!("writing {}", path.display()))
}

fn replace_values(lock: &RwLock<Vec<String>>, mut values: Vec<String>) {
    values.sort();
    values.dedup();
    *lock.write() = values;
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

fn utc_timestamp_string() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs().to_string())
        .unwrap_or_default()
}

#[derive(Debug, Clone)]
pub struct GlobalState {
    pub avatars_locked: bool,
    pub props_locked: bool,
    pub worlds_locked: bool,
    pub servers_locked: bool,
    pub third_person_disabled: bool,
    pub additional_avatar_data_lock: bool,
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
            additional_avatar_data_lock: config.additional_avatar_data_lock,
            disallow_headless: config.disallow_headless,
            headless_audio_off: false,
            opus_packet_loss_percent: 10,
            opus_frame_duration_ms: 20,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn moderation_lists_persist_whitelist_blacklist_and_bans() {
        let dir = unique_temp_dir();
        fs::create_dir_all(&dir).unwrap();

        let moderation = ModerationLists::file_backed(&dir).unwrap();
        moderation.add_whitelist("user-a").unwrap();
        moderation.add_blacklist("user-b").unwrap();
        moderation
            .add_ban_with_details("user-c", "reason", Some("127.0.0.1".to_string()))
            .unwrap();

        let reloaded = ModerationLists::file_backed(&dir).unwrap();
        assert!(reloaded.is_whitelisted("user-a"));
        assert!(reloaded.is_blacklisted("user-b"));
        assert!(reloaded.is_uuid_banned("user-c"));
        assert!(reloaded.is_ip_banned("127.0.0.1"));

        reloaded.remove_whitelist("user-a").unwrap();
        reloaded.remove_blacklist("user-b").unwrap();
        reloaded.remove_ban("user-c").unwrap();

        let reloaded_again = ModerationLists::file_backed(&dir).unwrap();
        assert!(!reloaded_again.is_whitelisted("user-a"));
        assert!(!reloaded_again.is_blacklisted("user-b"));
        assert!(!reloaded_again.is_uuid_banned("user-c"));

        let _ = fs::remove_dir_all(dir);
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("basis-admin-test-{nanos}"))
    }
}
