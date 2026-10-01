use anyhow::{Context, Result};
use basis_protocol::{config::ServerConfig, NetReader, NetWriter};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::Arc,
};

#[derive(Debug, Clone, Default)]
pub struct ModerationLists {
    state: Arc<RwLock<ModerationState>>,
    paths: Option<Arc<ModerationPaths>>,
}

#[derive(Debug, Clone, Default)]
struct ModerationState {
    banned_players: Vec<BannedPlayer>,
    muted_players: Vec<MutedPlayer>,
    whitelist: Vec<String>,
    blacklist: Vec<String>,
}

#[derive(Debug, Clone)]
struct ModerationPaths {
    banned_players: PathBuf,
    muted_players: PathBuf,
    whitelist: PathBuf,
    blacklist: PathBuf,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct BannedPlayer {
    #[serde(rename = "UUID")]
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct MutedPlayer {
    #[serde(default, rename = "UUID")]
    pub uuid: String,
    #[serde(default)]
    pub voice_muted: bool,
    #[serde(default)]
    pub text_muted: bool,
    #[serde(default)]
    pub time_of_mute: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename = "ArrayOfBannedPlayer")]
struct BannedPlayersXml {
    #[serde(rename = "BannedPlayer", default)]
    players: Vec<BannedPlayer>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename = "ArrayOfMutedPlayer")]
struct MutedPlayersXml {
    #[serde(rename = "MutedPlayer", default)]
    players: Vec<MutedPlayer>,
}

impl ModerationLists {
    pub fn file_backed(config_dir: impl AsRef<Path>) -> Result<Self> {
        let dir = config_dir.as_ref();
        fs::create_dir_all(dir)
            .with_context(|| format!("creating moderation directory {}", dir.display()))?;
        let lists = Self {
            paths: Some(Arc::new(ModerationPaths {
                banned_players: dir.join("banned_players.xml"),
                muted_players: dir.join("muted_players.xml"),
                whitelist: dir.join("BasisAllowList.txt"),
                blacklist: dir.join("BasisBanList.txt"),
            })),
            ..Self::default()
        };
        lists.reload()?;
        Ok(lists)
    }

    /// Reads every file before replacing state. Invalid files never discard live restrictions.
    pub fn reload(&self) -> Result<()> {
        let Some(paths) = &self.paths else {
            return Ok(());
        };
        let mut state = self.state.write();
        let mut banned_players: Vec<BannedPlayer> = Vec::new();
        for player in
            read_xml::<BannedPlayersXml>(&paths.banned_players, "ArrayOfBannedPlayer")?.players
        {
            if let Some(existing) = banned_players.iter_mut().find(|p| p.uuid == player.uuid) {
                *existing = player;
            } else {
                banned_players.push(player);
            }
        }
        let mut muted_players: Vec<MutedPlayer> = Vec::new();
        for player in
            read_xml::<MutedPlayersXml>(&paths.muted_players, "ArrayOfMutedPlayer")?.players
        {
            if player.uuid.is_empty() || !(player.voice_muted || player.text_muted) {
                continue;
            }
            if let Some(existing) = muted_players.iter_mut().find(|p| p.uuid == player.uuid) {
                *existing = player;
            } else {
                muted_players.push(player);
            }
        }
        let whitelist = read_line_list_with_fallback(&paths.whitelist, "BasisWhiteList.txt")?;
        let blacklist = read_line_list_with_fallback(&paths.blacklist, "BasisBlackList.txt")?;
        // Canonical files become the write target even when migrating legacy names.
        if !paths.banned_players.exists() {
            write_xml(&paths.banned_players, &BannedPlayersXml::default())?;
        }
        if !paths.muted_players.exists() {
            write_xml(&paths.muted_players, &MutedPlayersXml::default())?;
        }
        if !paths.whitelist.exists() {
            write_line_list(&paths.whitelist, &whitelist)?;
        }
        if !paths.blacklist.exists() {
            write_line_list(&paths.blacklist, &blacklist)?;
        }
        *state = ModerationState {
            banned_players,
            muted_players,
            whitelist,
            blacklist,
        };
        Ok(())
    }

    pub fn is_uuid_banned(&self, uuid: &str) -> bool {
        self.state
            .read()
            .banned_players
            .iter()
            .any(|p| p.uuid == uuid)
    }
    pub fn is_ip_banned(&self, ip: &str) -> bool {
        self.state
            .read()
            .banned_players
            .iter()
            .any(|p| p.has_banned_ip && p.banned_ip == ip)
    }
    pub fn is_whitelisted(&self, uuid: &str) -> bool {
        self.state.read().whitelist.iter().any(|p| p == uuid)
    }
    pub fn is_blacklisted(&self, uuid: &str) -> bool {
        self.state.read().blacklist.iter().any(|p| p == uuid)
    }
    pub fn mute_state(&self, uuid: &str) -> (bool, bool) {
        self.state
            .read()
            .muted_players
            .iter()
            .find(|p| p.uuid == uuid)
            .map(|p| (p.voice_muted, p.text_muted))
            .unwrap_or_default()
    }
    pub fn set_voice_mute(&self, uuid: &str, muted: bool) -> Result<()> {
        self.set_mute(uuid, true, muted)
    }
    pub fn set_text_mute(&self, uuid: &str, muted: bool) -> Result<()> {
        self.set_mute(uuid, false, muted)
    }
    fn set_mute(&self, uuid: &str, voice: bool, muted: bool) -> Result<()> {
        anyhow::ensure!(!uuid.trim().is_empty(), "UUID invalid");
        let mut state = self.state.write();
        let mut players = state.muted_players.clone();
        let index = players
            .iter()
            .position(|p| p.uuid == uuid)
            .unwrap_or_else(|| {
                players.push(MutedPlayer {
                    uuid: uuid.to_owned(),
                    time_of_mute: utc_timestamp_string(),
                    ..MutedPlayer::default()
                });
                players.len() - 1
            });
        if voice {
            players[index].voice_muted = muted;
        } else {
            players[index].text_muted = muted;
        }
        players.retain(|p| p.voice_muted || p.text_muted);
        if let Some(paths) = &self.paths {
            write_xml(
                &paths.muted_players,
                &MutedPlayersXml {
                    players: players.clone(),
                },
            )?;
        }
        state.muted_players = players;
        Ok(())
    }

    pub fn add_whitelist(&self, uuid: impl Into<String>) -> Result<()> {
        self.change_list(uuid.into(), true, true).map(|_| ())
    }
    pub fn remove_whitelist(&self, uuid: &str) -> Result<bool> {
        self.change_list(uuid.to_owned(), true, false)
    }
    pub fn add_blacklist(&self, uuid: impl Into<String>) -> Result<()> {
        self.change_list(uuid.into(), false, true).map(|_| ())
    }
    pub fn remove_blacklist(&self, uuid: &str) -> Result<bool> {
        self.change_list(uuid.to_owned(), false, false)
    }
    fn change_list(&self, uuid: String, whitelist: bool, add: bool) -> Result<bool> {
        if uuid.trim().is_empty() {
            return Ok(false);
        }
        let mut state = self.state.write();
        let mut values = if whitelist {
            state.whitelist.clone()
        } else {
            state.blacklist.clone()
        };
        let before = values.clone();
        if add {
            if !values.contains(&uuid) {
                values.push(uuid);
            }
        } else {
            values.retain(|p| p != &uuid);
        }
        if values == before {
            return Ok(false);
        }
        if let Some(paths) = &self.paths {
            write_line_list(
                if whitelist {
                    &paths.whitelist
                } else {
                    &paths.blacklist
                },
                &values,
            )?;
        }
        if whitelist {
            state.whitelist = values;
        } else {
            state.blacklist = values;
        }
        Ok(true)
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
        let banned_ip = banned_ip.unwrap_or_default();
        let player = BannedPlayer {
            uuid,
            has_banned_ip: !banned_ip.trim().is_empty(),
            banned_ip,
            reason: reason.into(),
            time_of_ban: utc_timestamp_string(),
        };
        self.change_bans(|players| {
            if let Some(existing) = players.iter_mut().find(|p| p.uuid == player.uuid) {
                *existing = player;
            } else {
                players.push(player);
            }
            true
        })
        .map(|_| ())
    }
    pub fn remove_ban(&self, uuid: &str) -> Result<bool> {
        self.change_bans(|players| {
            let before = players.len();
            players.retain(|p| p.uuid != uuid);
            before != players.len()
        })
    }
    pub fn add_ip_ban(&self, ip: impl Into<String>) -> Result<()> {
        let ip = ip.into();
        if ip.trim().is_empty() {
            return Ok(());
        }
        self.add_ban_with_details(format!("ip-ban:{ip}"), "IP banned", Some(ip))
    }
    pub fn remove_ip_ban(&self, ip: &str) -> Result<bool> {
        self.change_bans(|players| {
            let before = players.len();
            players.retain(|p| !(p.has_banned_ip && p.banned_ip == ip));
            before != players.len()
        })
    }
    fn change_bans(&self, change: impl FnOnce(&mut Vec<BannedPlayer>) -> bool) -> Result<bool> {
        let mut state = self.state.write();
        let mut players = state.banned_players.clone();
        if !change(&mut players) {
            return Ok(false);
        }
        if let Some(paths) = &self.paths {
            write_xml(
                &paths.banned_players,
                &BannedPlayersXml {
                    players: players.clone(),
                },
            )?;
        }
        state.banned_players = players;
        Ok(true)
    }
}

fn read_xml<T: for<'de> Deserialize<'de> + Default>(path: &Path, root: &str) -> Result<T> {
    if !path.exists() {
        return Ok(T::default());
    }
    let xml = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let xml = xml.strip_prefix('\u{feff}').unwrap_or(&xml);
    let mut reader = quick_xml::Reader::from_str(xml);
    let mut root_seen = false;
    let mut depth = 0usize;
    loop {
        match reader
            .read_event()
            .with_context(|| format!("parsing {}", path.display()))?
        {
            quick_xml::events::Event::Start(e) => {
                if depth == 0 {
                    anyhow::ensure!(
                        !root_seen && e.name().as_ref() == root.as_bytes(),
                        "invalid root in {}: expected {root}",
                        path.display()
                    );
                    root_seen = true;
                }
                depth += 1;
            }
            quick_xml::events::Event::Empty(e) if depth == 0 => {
                anyhow::ensure!(
                    !root_seen && e.name().as_ref() == root.as_bytes(),
                    "invalid root in {}: expected {root}",
                    path.display()
                );
                root_seen = true;
            }
            quick_xml::events::Event::End(_) => {
                anyhow::ensure!(depth > 0, "unexpected closing tag in {}", path.display());
                depth -= 1;
            }
            quick_xml::events::Event::Text(e) if depth == 0 => {
                anyhow::ensure!(
                    e.as_ref().iter().all(u8::is_ascii_whitespace),
                    "text outside root in {}",
                    path.display()
                );
            }
            quick_xml::events::Event::Eof => {
                anyhow::ensure!(
                    root_seen && depth == 0,
                    "incomplete XML in {}",
                    path.display()
                );
                break;
            }
            _ => {}
        }
    }
    quick_xml::de::from_str(xml).with_context(|| format!("parsing {}", path.display()))
}
fn write_xml<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let xml = quick_xml::se::to_string(value)
        .with_context(|| format!("serializing {}", path.display()))?;
    atomic_write(path, xml.as_bytes())
}
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    atomic_write_with_directory_sync(path, bytes, sync_parent_directory)
}
fn atomic_write_with_directory_sync(
    path: &Path,
    bytes: &[u8],
    sync_directory: impl FnOnce(&Path) -> std::io::Result<()>,
) -> Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = fs::File::create(&temporary)
        .with_context(|| format!("creating {}", temporary.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("writing {}", temporary.display()))?;
    file.sync_all()
        .with_context(|| format!("syncing {}", temporary.display()))?;
    drop(file);
    fs::rename(&temporary, path).with_context(|| format!("replacing {}", path.display()))?;
    // Rename commits the change. Returning an error afterward would prevent callers
    // from updating live restrictions even though their saved file has changed.
    if let Err(error) = sync_directory(path) {
        tracing::warn!(path = %path.display(), %error,
            "Moderation file replaced, but directory sync failed; crash durability is uncertain");
    }
    Ok(())
}
fn sync_parent_directory(path: &Path) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        fs::File::open(parent)?.sync_all()?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
fn read_line_list_with_fallback(path: &Path, legacy: &str) -> Result<Vec<String>> {
    let fallback = path.with_file_name(legacy);
    let source = if path.exists() { path } else { &fallback };
    if !source.exists() {
        return Ok(Vec::new());
    }
    let text =
        fs::read_to_string(source).with_context(|| format!("reading {}", source.display()))?;
    let mut values: Vec<_> = text
        .strip_prefix('\u{feff}')
        .unwrap_or(&text)
        .lines()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    values.sort();
    values.dedup();
    Ok(values)
}
fn write_line_list(path: &Path, values: &[String]) -> Result<()> {
    let text = values
        .iter()
        .filter(|v| !v.trim().is_empty())
        .map(|v| format!("{v}\n"))
        .collect::<String>();
    atomic_write(path, text.as_bytes())
}
fn utc_timestamp_string() -> String {
    time::OffsetDateTime::now_utc()
        .format(
            &time::format_description::parse_borrowed::<2>(
                "[year]-[month]-[day] [hour]:[minute]:[second]",
            )
            .expect("valid timestamp format"),
        )
        .expect("UTC timestamp formats")
}

/// Persisted instance locomotion policy, with BasisVR's field mask and wire order.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LocomotionPolicy {
    pub fields: u8,
    pub jump_height: f32,
    pub walk_speed: f32,
    pub run_speed: f32,
    pub gravity: f32,
    pub mode: u8,
}
impl From<&ServerConfig> for LocomotionPolicy {
    fn from(config: &ServerConfig) -> Self {
        Self {
            fields: config.locomotion_policy_fields,
            jump_height: config.locomotion_policy_jump_height,
            walk_speed: config.locomotion_policy_walk_speed,
            run_speed: config.locomotion_policy_run_speed,
            gravity: config.locomotion_policy_gravity,
            mode: config.locomotion_policy_mode,
        }
        .sanitized()
    }
}
impl LocomotionPolicy {
    pub fn sanitized(mut self) -> Self {
        fn distance(value: f32, fallback: f32) -> f32 {
            if value.is_finite() {
                value.clamp(0.0, 1000.0)
            } else {
                fallback
            }
        }
        self.fields &= 31;
        self.jump_height = distance(self.jump_height, 1.0);
        self.walk_speed = distance(self.walk_speed, 2.5);
        self.run_speed = distance(self.run_speed, 4.0);
        self.gravity = if self.gravity.is_finite() {
            self.gravity.clamp(-1000.0, 0.0)
        } else {
            -9.81
        };
        if self.mode > 2 {
            self.mode = 0;
        }
        self
    }
    pub fn write_to_config(&self, config: &mut ServerConfig) {
        let policy = self.sanitized();
        config.locomotion_policy_fields = policy.fields;
        config.locomotion_policy_jump_height = policy.jump_height;
        config.locomotion_policy_walk_speed = policy.walk_speed;
        config.locomotion_policy_run_speed = policy.run_speed;
        config.locomotion_policy_gravity = policy.gravity;
        config.locomotion_policy_mode = policy.mode;
    }
    pub fn serialize(&self, writer: &mut NetWriter) {
        writer.put_u8(self.fields);
        writer.put_f32(self.jump_height);
        writer.put_f32(self.walk_speed);
        writer.put_f32(self.run_speed);
        writer.put_f32(self.gravity);
        writer.put_u8(self.mode);
    }
    pub fn deserialize(reader: &mut NetReader<'_>) -> Result<Self> {
        Ok(Self {
            fields: reader.get_u8()?,
            jump_height: reader.get_f32()?,
            walk_speed: reader.get_f32()?,
            run_speed: reader.get_f32()?,
            gravity: reader.get_f32()?,
            mode: reader.get_u8()?,
        }
        .sanitized())
    }
}
#[derive(Debug, Clone)]
pub struct GlobalState {
    pub avatars_locked: bool,
    pub props_locked: bool,
    pub worlds_locked: bool,
    pub servers_locked: bool,
    pub third_person_disabled: bool,
    pub additional_avatar_data_lock: bool,
    pub camera_metadata_disallow_mask: u8,
    pub restriction_mode: u8,
    pub playspace_mover_locked: bool,
    pub direct_connect_locked: bool,
    pub cilbox_locked: bool,
    pub images_locked: bool,
    pub gifs_locked: bool,
    pub end_effector_ik_disabled: bool,
    pub text_chat_locked: bool,
    pub voice_chat_locked: bool,
    pub media_player_locked: bool,
    pub camera_capture_locked: bool,
    pub prop_grabbing_locked: bool,
    pub safe_display_names_forced: bool,
    pub disallow_headless: bool,
    pub headless_audio_off: bool,
    pub opus_packet_loss_percent: u8,
    pub opus_frame_duration_ms: u8,
    pub global_opus_bitrate: i32,
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
            camera_metadata_disallow_mask: config.camera_metadata_disallow_mask,
            restriction_mode: config.basis_user_restriction_mode as u8,
            playspace_mover_locked: config.playspace_mover_locked,
            direct_connect_locked: config.direct_connect_locked,
            cilbox_locked: config.cilbox_locked,
            images_locked: config.images_locked,
            gifs_locked: config.gifs_locked,
            end_effector_ik_disabled: config.end_effector_ik_disabled,
            text_chat_locked: config.text_chat_locked,
            voice_chat_locked: config.voice_chat_locked,
            media_player_locked: config.media_player_locked,
            camera_capture_locked: config.camera_capture_locked,
            prop_grabbing_locked: config.prop_grabbing_locked,
            safe_display_names_forced: config.safe_display_names_forced,
            disallow_headless: config.disallow_headless,
            headless_audio_off: false,
            opus_packet_loss_percent: 10,
            opus_frame_duration_ms: 20,
            global_opus_bitrate: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn directory_sync_failure_still_reports_committed_moderation_write() {
        let dir = unique_temp_dir();
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("BasisBanList.txt");
        fs::write(&path, "old-user\n").unwrap();
        let result = atomic_write_with_directory_sync(&path, b"new-user\n", |path| {
            assert_eq!(fs::read_to_string(path).unwrap(), "new-user\n");
            Err(std::io::Error::other("injected directory-sync failure"))
        });
        assert!(
            result.is_ok(),
            "a committed write must update live restrictions"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "new-user\n");
        let reloaded = ModerationLists::file_backed(&dir).unwrap();
        assert!(reloaded.is_blacklisted("new-user"));
        assert!(!reloaded.is_blacklisted("old-user"));
        fs::remove_dir_all(dir).unwrap();
    }

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

    #[test]
    fn reads_basisvr_fixtures_and_legacy_lists_then_writes_canonical_files() {
        let dir = unique_temp_dir();
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("BasisWhiteList.txt"), " player-a\nplayer-a\n").unwrap();
        fs::write(dir.join("BasisBlackList.txt"), "player-b\n").unwrap();
        fs::write(dir.join("banned_players.xml"), r#"<?xml version="1.0"?>
<ArrayOfBannedPlayer xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <BannedPlayer><UUID>player-c</UUID><BannedIp>10.0.0.2</BannedIp><Reason>test</Reason><HasBannedIp>true</HasBannedIp><TimeOfBan>2026-09-30 14:15:16</TimeOfBan></BannedPlayer>
</ArrayOfBannedPlayer>"#).unwrap();
        fs::write(dir.join("muted_players.xml"), r#"<?xml version="1.0"?>
<ArrayOfMutedPlayer xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <MutedPlayer><UUID>player-d</UUID><VoiceMuted>true</VoiceMuted><TextMuted>false</TextMuted><TimeOfMute>2026-09-30 14:15:16</TimeOfMute></MutedPlayer>
</ArrayOfMutedPlayer>"#).unwrap();
        let lists = ModerationLists::file_backed(&dir).unwrap();
        assert!(lists.is_whitelisted("player-a"));
        assert!(lists.is_blacklisted("player-b"));
        assert!(lists.is_uuid_banned("player-c"));
        assert!(lists.is_ip_banned("10.0.0.2"));
        assert_eq!(lists.mute_state("player-d"), (true, false));
        assert_eq!(
            fs::read_to_string(dir.join("BasisAllowList.txt")).unwrap(),
            "player-a\n"
        );
        assert_eq!(
            fs::read_to_string(dir.join("BasisBanList.txt")).unwrap(),
            "player-b\n"
        );
        // Canonical files take precedence when both old and current filenames exist.
        fs::write(dir.join("BasisWhiteList.txt"), "legacy-only").unwrap();
        lists.reload().unwrap();
        assert!(!lists.is_whitelisted("legacy-only"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn reads_bom_prefixed_canonical_lists_and_dotnet_xml() {
        let dir = unique_temp_dir();
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("BasisAllowList.txt"), "\u{feff}allowed-user\r\n").unwrap();
        fs::write(dir.join("BasisBanList.txt"), "\u{feff}blocked-user\r\n").unwrap();
        fs::write(dir.join("banned_players.xml"), concat!("\u{feff}", r#"<?xml version="1.0" encoding="utf-8"?>
<ArrayOfBannedPlayer xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <BannedPlayer><UUID>banned-user</UUID><BannedIp>10.0.0.3</BannedIp><Reason>fixture</Reason><HasBannedIp>true</HasBannedIp><TimeOfBan>2026-09-30 14:15:16</TimeOfBan></BannedPlayer>
</ArrayOfBannedPlayer>"#)).unwrap();
        fs::write(dir.join("muted_players.xml"), concat!("\u{feff}", r#"<?xml version="1.0" encoding="utf-8"?>
<ArrayOfMutedPlayer xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <MutedPlayer><UUID>muted-user</UUID><VoiceMuted>true</VoiceMuted><TextMuted>true</TextMuted><TimeOfMute>2026-09-30 14:15:16</TimeOfMute></MutedPlayer>
</ArrayOfMutedPlayer>"#)).unwrap();
        let lists = ModerationLists::file_backed(&dir).unwrap();
        assert!(lists.is_whitelisted("allowed-user"));
        assert!(lists.is_blacklisted("blocked-user"));
        assert!(lists.is_uuid_banned("banned-user"));
        assert!(lists.is_ip_banned("10.0.0.3"));
        assert_eq!(lists.mute_state("muted-user"), (true, true));
        lists.reload().unwrap();
        assert!(lists.is_whitelisted("allowed-user"));
        assert_eq!(lists.mute_state("muted-user"), (true, true));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn persisted_mutes_are_independent_and_clear_when_both_flags_clear() {
        let dir = unique_temp_dir();
        let lists = ModerationLists::file_backed(&dir).unwrap();
        lists.set_voice_mute("offline-user", true).unwrap();
        lists.set_text_mute("offline-user", true).unwrap();
        lists.set_voice_mute("offline-user", false).unwrap();
        let reloaded = ModerationLists::file_backed(&dir).unwrap();
        assert_eq!(reloaded.mute_state("offline-user"), (false, true));
        reloaded.set_text_mute("offline-user", false).unwrap();
        assert_eq!(
            ModerationLists::file_backed(&dir)
                .unwrap()
                .mute_state("offline-user"),
            (false, false)
        );
        assert!(!fs::read_to_string(dir.join("muted_players.xml"))
            .unwrap()
            .contains("offline-user"));
        assert!(lists.set_voice_mute("  ", true).is_err());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn reload_failure_preserves_every_live_list_and_file() {
        let dir = unique_temp_dir();
        let lists = ModerationLists::file_backed(&dir).unwrap();
        lists.add_ban("banned").unwrap();
        lists.set_text_mute("muted", true).unwrap();
        lists.add_whitelist("allowed").unwrap();
        // Earlier successfully parsed files must not be applied before a later failure.
        fs::write(dir.join("banned_players.xml"), "<ArrayOfBannedPlayer/>").unwrap();
        let invalid = "<ArrayOfMutedPlayer><MutedPlayer><VoiceMuted>invalid</VoiceMuted></MutedPlayer></ArrayOfMutedPlayer>";
        fs::write(dir.join("muted_players.xml"), invalid).unwrap();
        assert!(lists.reload().is_err());
        assert!(lists.is_uuid_banned("banned"));
        assert!(lists.is_whitelisted("allowed"));
        assert_eq!(lists.mute_state("muted"), (false, true));
        assert_eq!(
            fs::read_to_string(dir.join("muted_players.xml")).unwrap(),
            invalid
        );
        fs::write(dir.join("muted_players.xml"), "<WrongRoot/>").unwrap();
        assert!(lists.reload().is_err());
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn malformed_bans_fail_closed_and_duplicate_records_use_latest_value() {
        let dir = unique_temp_dir();
        let lists = ModerationLists::file_backed(&dir).unwrap();
        lists.add_ban("user").unwrap();
        for malformed in ["", "<ArrayOfBannedPlayer>", "<ArrayOfBannedPlayer/><ArrayOfBannedPlayer/>", "<ArrayOfBannedPlayer><BannedPlayer><Reason>missing uuid</Reason></BannedPlayer></ArrayOfBannedPlayer>"] {
            fs::write(dir.join("banned_players.xml"), malformed).unwrap();
            assert!(lists.reload().is_err(), "accepted malformed XML {malformed}");
            assert!(lists.is_uuid_banned("user"));
        }
        fs::write(dir.join("banned_players.xml"), "<ArrayOfBannedPlayer/>").unwrap();
        fs::write(dir.join("muted_players.xml"), "<ArrayOfMutedPlayer><MutedPlayer><UUID>user</UUID><VoiceMuted>true</VoiceMuted></MutedPlayer><MutedPlayer><UUID>user</UUID><TextMuted>true</TextMuted></MutedPlayer></ArrayOfMutedPlayer>").unwrap();
        lists.reload().unwrap();
        assert_eq!(lists.mute_state("user"), (false, true));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn failed_mutation_does_not_change_live_state() {
        let dir = unique_temp_dir();
        let lists = ModerationLists::file_backed(&dir).unwrap();
        fs::remove_file(dir.join("muted_players.xml")).unwrap();
        fs::create_dir(dir.join("muted_players.xml")).unwrap();
        assert!(lists.set_voice_mute("user", true).is_err());
        assert_eq!(lists.mute_state("user"), (false, false));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn memory_only_moderation_and_basisvr_timestamp_format() {
        let lists = ModerationLists::default();
        lists.add_ban("user").unwrap();
        lists.set_voice_mute("user", true).unwrap();
        assert!(lists.is_uuid_banned("user"));
        assert_eq!(lists.mute_state("user"), (true, false));
        let timestamp = &lists.state.read().banned_players[0].time_of_ban;
        assert_eq!(timestamp.len(), 19);
        assert_eq!(&timestamp[10..11], " ");
    }

    #[test]
    fn locomotion_policy_matches_basisvr_sanitization_and_wire_order() {
        let policy = LocomotionPolicy {
            fields: 255,
            jump_height: f32::NAN,
            walk_speed: -4.0,
            run_speed: 1001.0,
            gravity: f32::INFINITY,
            mode: 3,
        }
        .sanitized();
        assert_eq!(
            policy,
            LocomotionPolicy {
                fields: 31,
                jump_height: 1.0,
                walk_speed: 0.0,
                run_speed: 1000.0,
                gravity: -9.81,
                mode: 0
            }
        );
        let mut config = ServerConfig::default();
        policy.write_to_config(&mut config);
        assert_eq!(LocomotionPolicy::from(&config), policy);
        let mut writer = NetWriter::new();
        policy.serialize(&mut writer);
        assert_eq!(writer.len(), 18);
        assert_eq!(
            LocomotionPolicy::deserialize(&mut NetReader::new(writer.as_slice())).unwrap(),
            policy
        );
        assert_eq!(
            LocomotionPolicy {
                gravity: 1.0,
                ..policy
            }
            .sanitized()
            .gravity,
            0.0
        );
        assert_eq!(
            LocomotionPolicy {
                gravity: -1001.0,
                ..policy
            }
            .sanitized()
            .gravity,
            -1000.0
        );
    }

    fn unique_temp_dir() -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("basis-admin-test-{nanos}"))
    }
}
