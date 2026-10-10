use crate::{observer_session::ObserverSession, strict_config};
use anyhow::anyhow;
use anyhow::{Context, Result};
use basis_protocol::application::{DEFAULT_COMPANY_NAME, DEFAULT_PRODUCT_NAME};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

pub(crate) const DEFAULT_VOICE_AUDIO_FOLDER: &str = "audio";
pub(crate) const DEFAULT_VOICE_SPEAKER_PERCENT: u8 = 10;
pub(crate) const DEFAULT_VOICE_HEARING_DISTANCE: f32 = 25.0;
pub(crate) const DEFAULT_VOICE_FRAME_DURATION_MS: u64 = 20;
pub(crate) const MAX_UNITY_VOICE_FRAME_DURATION_MS: u64 = 40;
/// Options accepted by the headless client runtime, independent of CLI parsing.
#[derive(Debug, Clone, PartialEq)]
pub struct ClientOptions {
    pub config: PathBuf,
    /// Reject malformed XML and invalid supplied scalar fields.
    pub strict_config: bool,
    pub ip: Option<String>,
    pub port: Option<u16>,
    pub clients: Option<usize>,
    pub no_reconnect: bool,
    pub reconnect_min_secs: u64,
    pub reconnect_max_secs: u64,
    pub no_movement: bool,
    /// Bytes in a synthetic AdditionalAvatarData item (0 disables it).
    pub additional_avatar_bytes: u8,
    /// Synthetic prop/scene script payload size (0 disables the workload).
    pub scene_data_bytes: usize,
    pub scene_data_interval_ms: u64,
    pub scene_data_reliable: bool,
    pub observe_scene_csv: Option<PathBuf>,
    pub scene_start_file: Option<PathBuf>,
    /// Use synchronized worker batches instead of the default per-client randomized cadence.
    pub sync_batching: bool,
    /// Simulate Unity's frame-quantized avatar tick, keyframes, deltas, and idle policy.
    /// Requires server metadata from an explicitly configured, colocated server workload.
    pub unity_avatar_policy: bool,
    /// Simulated Unity frame rate used by --unity-avatar-policy.
    pub unity_frame_rate: u32,
    /// Synthetic hips-yaw animation amplitude for the opt-in Unity policy workload.
    pub unity_pose_amplitude_degrees: f32,
    /// Base interval between avatar movement sends in the synthetic Rust load mode.
    pub movement_interval_ms: u64,
    /// Maximum movement interval jitter in percent when randomized cadence is active.
    pub movement_jitter_percent: u8,
    /// Maximum voice packet timing jitter in percent when randomized cadence is active.
    pub voice_jitter_percent: u8,
    pub duration_secs: Option<u64>,
    pub connect_batch_size: usize,
    pub connect_batch_delay_ms: u64,
    /// Number of clients to disconnect at a time during graceful shutdown.
    pub quit_batch_size: usize,
    /// Delay between graceful shutdown disconnect batches.
    pub quit_batch_delay_ms: u64,
    pub connect_timeout_ms: u64,
    pub spawn_group_size: usize,
    pub spawn_group_spacing: f32,
    /// Use exact group centers instead of random ±0.25-unit spawn offsets.
    pub fixed_spawn_positions: bool,
    /// Place every client at the origin and hold its position there for the full run.
    /// This is a dense colocated workload; it disables the per-send random positional walk.
    pub no_spread: bool,
    pub voice: bool,
    pub voice_audio_folder: Option<PathBuf>,
    pub voice_speaker_percent: Option<u8>,
    pub voice_hearing_distance: Option<f32>,
    pub voice_frame_duration_ms: Option<u64>,
    pub no_voice_reencode: bool,
    /// Enable applied-avatar metrics with failover among the first three clients.
    pub observe_avatar_csv: Option<PathBuf>,
    /// Radius around the observing client's current position used to select nearby senders.
    pub avatar_observe_radius: f32,
    /// Expected number of nearby senders, used to report senders never observed.
    pub avatar_observe_expected_peers: usize,
    /// Begin collecting observer cadence after this file appears; used to align with load readiness.
    pub observe_avatar_start_file: Option<PathBuf>,
    /// Length of the applied-avatar observation window.
    pub observe_avatar_window_secs: u64,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            config: PathBuf::from("Config.xml"),
            strict_config: false,
            ip: None,
            port: None,
            clients: None,
            no_reconnect: false,
            reconnect_min_secs: 60,
            reconnect_max_secs: 1200,
            no_movement: false,
            additional_avatar_bytes: 0,
            scene_data_bytes: 0,
            scene_data_interval_ms: 50,
            scene_data_reliable: false,
            observe_scene_csv: None,
            scene_start_file: None,
            sync_batching: false,
            unity_avatar_policy: false,
            unity_frame_rate: 60,
            unity_pose_amplitude_degrees: 20.0,
            movement_interval_ms: 90,
            movement_jitter_percent: 10,
            voice_jitter_percent: 5,
            duration_secs: None,
            connect_batch_size: 100,
            connect_batch_delay_ms: 250,
            quit_batch_size: 100,
            quit_batch_delay_ms: 250,
            connect_timeout_ms: 5000,
            spawn_group_size: 0,
            spawn_group_spacing: 1000.0,
            fixed_spawn_positions: false,
            no_spread: false,
            voice: false,
            voice_audio_folder: None,
            voice_speaker_percent: None,
            voice_hearing_distance: None,
            voice_frame_duration_ms: None,
            no_voice_reencode: false,
            observe_avatar_csv: None,
            avatar_observe_radius: 40.0,
            avatar_observe_expected_peers: 9,
            observe_avatar_start_file: None,
            observe_avatar_window_secs: 60,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename = "Configuration", rename_all = "PascalCase")]
pub struct Config {
    pub password: String,
    pub ip: String,
    pub port: u16,
    pub client_count: usize,
    pub company_name: String,
    pub product_name: String,
    pub avatar_password: String,
    pub avatar_url: String,
    pub avatar_load_mode: u8,
    pub voice_enabled: bool,
    pub voice_audio_folder: String,
    pub voice_speaker_percent: u8,
    pub voice_hearing_distance: f32,
    pub voice_frame_duration_ms: u64,
    #[serde(skip)]
    pub observe_avatar_csv: Option<PathBuf>,
    #[serde(skip)]
    pub avatar_observe_radius: f32,
    #[serde(skip)]
    pub avatar_observe_expected_peers: usize,
    #[serde(skip)]
    pub observe_avatar_start_file: Option<PathBuf>,
    #[serde(skip)]
    pub observe_avatar_window: Duration,
    #[serde(skip)]
    pub(crate) observer_session: Option<Arc<ObserverSession>>,
    #[serde(skip)]
    pub additional_avatar_bytes: u8,
    #[serde(skip)]
    pub(crate) scene_session: Option<Arc<crate::scene::SceneSession>>,
    #[serde(skip)]
    pub(crate) image_benchmark: Option<Arc<crate::image_benchmark::ImageBenchmarkSession>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename = "Configuration", rename_all = "PascalCase")]
pub(crate) struct RawConfig {
    pub(crate) password: Option<String>,
    pub(crate) ip: Option<String>,
    pub(crate) port: Option<String>,
    pub(crate) client_count: Option<String>,
    pub(crate) company_name: Option<String>,
    pub(crate) product_name: Option<String>,
    pub(crate) avatar_password: Option<String>,
    pub(crate) avatar_url: Option<String>,
    pub(crate) avatar_load_mode: Option<String>,
    pub(crate) voice_enabled: Option<String>,
    pub(crate) voice_audio_folder: Option<String>,
    pub(crate) voice_speaker_percent: Option<String>,
    pub(crate) voice_hearing_distance: Option<String>,
    pub(crate) voice_frame_duration_ms: Option<String>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            password: "default_password".to_string(),
            ip: "localhost".to_string(),
            port: 4296,
            client_count: 250,
            company_name: DEFAULT_COMPANY_NAME.to_string(),
            product_name: DEFAULT_PRODUCT_NAME.to_string(),
            avatar_password: "N/A".to_string(),
            avatar_url: "LoadingAvatar".to_string(),
            avatar_load_mode: 1,
            voice_enabled: false,
            voice_audio_folder: DEFAULT_VOICE_AUDIO_FOLDER.to_string(),
            voice_speaker_percent: DEFAULT_VOICE_SPEAKER_PERCENT,
            voice_hearing_distance: DEFAULT_VOICE_HEARING_DISTANCE,
            voice_frame_duration_ms: DEFAULT_VOICE_FRAME_DURATION_MS,
            observe_avatar_csv: None,
            avatar_observe_radius: 40.0,
            avatar_observe_expected_peers: 9,
            observe_avatar_start_file: None,
            observe_avatar_window: Duration::from_secs(60),
            observer_session: None,
            additional_avatar_bytes: 0,
            scene_session: None,
            image_benchmark: None,
        }
    }
}

impl Config {
    pub fn load_or_create(path: &Path, strict: bool) -> Result<Self> {
        if !path.exists() {
            let config = Self::default();
            let xml = config.to_pretty_xml();
            std::fs::write(path, xml)?;
            info!("created default config at {}", path.display());
            return Ok(config);
        }

        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        Self::from_xml(&text, path, strict)
    }

    pub(crate) fn from_xml(text: &str, path: &Path, strict: bool) -> Result<Self> {
        if strict {
            strict_config::validate_xml(text, path)?;
        }
        match quick_xml::de::from_str::<RawConfig>(text) {
            Ok(raw) => {
                if strict {
                    strict_config::validate_scalars(&raw, path)?;
                }
                Ok(Self::from_raw(raw))
            }
            Err(err) if strict => Err(anyhow!("config {}: malformed XML: {err}", path.display())),
            Err(err) => {
                warn!(
                    "failed to parse config {}, using defaults: {err}",
                    path.display()
                );
                Ok(Self::default())
            }
        }
    }

    pub(crate) fn from_raw(raw: RawConfig) -> Self {
        let defaults = Self::default();
        Self {
            password: raw
                .password
                .filter(|s| !s.is_empty())
                .unwrap_or(defaults.password),
            ip: raw.ip.filter(|s| !s.is_empty()).unwrap_or(defaults.ip),
            port: parse_or_default(raw.port, defaults.port, "Port"),
            client_count: parse_or_default(raw.client_count, defaults.client_count, "ClientCount"),
            company_name: raw.company_name.unwrap_or(defaults.company_name),
            product_name: raw.product_name.unwrap_or(defaults.product_name),
            avatar_password: raw
                .avatar_password
                .filter(|s| !s.is_empty())
                .unwrap_or(defaults.avatar_password),
            avatar_url: raw
                .avatar_url
                .filter(|s| !s.is_empty())
                .unwrap_or(defaults.avatar_url),
            avatar_load_mode: parse_or_default(
                raw.avatar_load_mode,
                defaults.avatar_load_mode,
                "AvatarLoadMode",
            ),
            voice_enabled: parse_or_default(
                raw.voice_enabled,
                defaults.voice_enabled,
                "VoiceEnabled",
            ),
            voice_audio_folder: raw
                .voice_audio_folder
                .filter(|s| !s.is_empty())
                .unwrap_or(defaults.voice_audio_folder),
            voice_speaker_percent: parse_or_default(
                raw.voice_speaker_percent,
                defaults.voice_speaker_percent,
                "VoiceSpeakerPercent",
            )
            .min(100),
            voice_hearing_distance: sanitize_voice_distance(parse_or_default(
                raw.voice_hearing_distance,
                defaults.voice_hearing_distance,
                "VoiceHearingDistance",
            )),
            voice_frame_duration_ms: sanitize_voice_frame_duration(parse_or_default(
                raw.voice_frame_duration_ms,
                defaults.voice_frame_duration_ms,
                "VoiceFrameDurationMs",
            )),
            observe_avatar_csv: defaults.observe_avatar_csv,
            avatar_observe_radius: defaults.avatar_observe_radius,
            avatar_observe_expected_peers: defaults.avatar_observe_expected_peers,
            observe_avatar_start_file: defaults.observe_avatar_start_file,
            observe_avatar_window: defaults.observe_avatar_window,
            observer_session: None,
            additional_avatar_bytes: 0,
            scene_session: None,
            image_benchmark: None,
        }
    }

    pub(crate) fn to_pretty_xml(&self) -> String {
        format!(
            "<Configuration>\n  <Password>{}</Password>\n  <Ip>{}</Ip>\n  <Port>{}</Port>\n  <ClientCount>{}</ClientCount>\n  <CompanyName>{}</CompanyName>\n  <ProductName>{}</ProductName>\n  <AvatarPassword>{}</AvatarPassword>\n  <AvatarUrl>{}</AvatarUrl>\n  <AvatarLoadMode>{}</AvatarLoadMode>\n  <VoiceEnabled>{}</VoiceEnabled>\n  <VoiceAudioFolder>{}</VoiceAudioFolder>\n  <VoiceSpeakerPercent>{}</VoiceSpeakerPercent>\n  <VoiceHearingDistance>{}</VoiceHearingDistance>\n  <VoiceFrameDurationMs>{}</VoiceFrameDurationMs>\n</Configuration>\n",
            escape_xml(&self.password),
            escape_xml(&self.ip),
            self.port,
            self.client_count,
            escape_xml(&self.company_name),
            escape_xml(&self.product_name),
            escape_xml(&self.avatar_password),
            escape_xml(&self.avatar_url),
            self.avatar_load_mode,
            self.voice_enabled,
            escape_xml(&self.voice_audio_folder),
            self.voice_speaker_percent,
            self.voice_hearing_distance,
            self.voice_frame_duration_ms
        )
    }
}

pub(crate) fn sanitize_voice_distance(value: f32) -> f32 {
    if value.is_finite() && value >= 0.0 {
        value
    } else {
        warn!(
            "invalid voice hearing distance {value}; using default {}",
            DEFAULT_VOICE_HEARING_DISTANCE
        );
        DEFAULT_VOICE_HEARING_DISTANCE
    }
}

pub(crate) fn sanitize_voice_frame_duration(value: u64) -> u64 {
    if value > 0 {
        value
    } else {
        warn!(
            "invalid voice frame duration {value}; using default {}ms",
            DEFAULT_VOICE_FRAME_DURATION_MS
        );
        DEFAULT_VOICE_FRAME_DURATION_MS
    }
}

pub(crate) fn resolve_relative_to_config(config_path: &Path, configured_path: &str) -> String {
    let path = PathBuf::from(configured_path);
    if path.is_absolute() {
        return path.to_string_lossy().to_string();
    }
    let base = config_path.parent().unwrap_or_else(|| Path::new("."));
    base.join(path).to_string_lossy().to_string()
}

pub(crate) fn escape_xml(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

pub(crate) fn parse_or_default<T>(value: Option<String>, default: T, field: &str) -> T
where
    T: std::str::FromStr + Copy,
{
    match value {
        Some(text) if !text.is_empty() => match text.parse::<T>() {
            Ok(value) => value,
            Err(_) => {
                warn!("invalid config field {field}={text:?}; using default");
                default
            }
        },
        _ => default,
    }
}
