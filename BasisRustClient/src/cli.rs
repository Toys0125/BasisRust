use basis_client_core::ClientOptions;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Basis LiteNetLib-compatible Rust headless client"
)]
pub(crate) struct Args {
    #[arg(long, default_value = "Config.xml")]
    config: PathBuf,
    /// Reject malformed XML and invalid supplied scalar fields instead of using defaults.
    #[arg(long)]
    strict_config: bool,
    #[arg(long)]
    ip: Option<String>,
    #[arg(long)]
    port: Option<u16>,
    #[arg(long)]
    clients: Option<usize>,
    #[arg(long)]
    no_reconnect: bool,
    #[arg(long, default_value_t = 60)]
    reconnect_min_secs: u64,
    #[arg(long, default_value_t = 1200)]
    reconnect_max_secs: u64,
    #[arg(long)]
    no_movement: bool,
    /// Bytes per synthetic AdditionalAvatarData item; 0 disables it.
    #[arg(long, default_value_t = 0)]
    additional_avatar_bytes: u8,
    /// Bytes per synthetic prop/scene script payload (24..=1024); 0 disables it.
    #[arg(long, default_value_t = 0)]
    scene_data_bytes: usize,
    #[arg(long, default_value_t = 50, value_parser = clap::value_parser!(u64).range(1..))]
    scene_data_interval_ms: u64,
    /// Send scene scripts over ReliableOrdered instead of Unreliable.
    #[arg(long)]
    scene_data_reliable: bool,
    /// Write scene throughput, integrity, coverage, and latency metrics at shutdown.
    #[arg(long)]
    observe_scene_csv: Option<PathBuf>,
    /// Begin scene sends when this marker file exists.
    #[arg(long)]
    scene_start_file: Option<PathBuf>,
    /// Use synchronized worker batches instead of the default per-client randomized cadence.
    #[arg(long)]
    sync_batching: bool,
    /// Simulate Unity's frame-quantized avatar tick, keyframes, deltas, and idle policy.
    /// Requires server metadata from an explicitly configured, colocated server workload.
    #[arg(long)]
    unity_avatar_policy: bool,
    /// Simulated Unity frame rate used by --unity-avatar-policy.
    #[arg(long, default_value_t = 60, value_parser = clap::value_parser!(u32).range(1..))]
    unity_frame_rate: u32,
    /// Synthetic hips-yaw animation amplitude for the opt-in Unity policy workload.
    #[arg(long, default_value_t = 20.0)]
    unity_pose_amplitude_degrees: f32,
    /// Base interval between avatar movement sends in the synthetic Rust load mode.
    #[arg(long, default_value_t = 90, value_parser = clap::value_parser!(u64).range(1..))]
    movement_interval_ms: u64,
    /// Maximum movement interval jitter in percent when randomized cadence is active.
    #[arg(long, default_value_t = 10)]
    movement_jitter_percent: u8,
    /// Maximum voice packet timing jitter in percent when randomized cadence is active.
    #[arg(long, default_value_t = 5)]
    voice_jitter_percent: u8,
    #[arg(long)]
    duration_secs: Option<u64>,
    #[arg(long, default_value_t = 100)]
    connect_batch_size: usize,
    #[arg(long, default_value_t = 250)]
    connect_batch_delay_ms: u64,
    /// Number of clients to disconnect at a time during graceful shutdown.
    #[arg(long, default_value_t = 100)]
    quit_batch_size: usize,
    /// Delay between graceful shutdown disconnect batches.
    #[arg(long, default_value_t = 250)]
    quit_batch_delay_ms: u64,
    #[arg(long, default_value_t = 5000)]
    connect_timeout_ms: u64,
    #[arg(long, default_value_t = 0)]
    spawn_group_size: usize,
    #[arg(long, default_value_t = 1000.0)]
    spawn_group_spacing: f32,
    /// Use exact group centers instead of random ±0.25-unit spawn offsets.
    #[arg(long)]
    fixed_spawn_positions: bool,
    /// Place every client at the origin and hold its position there for the full run.
    /// This is a dense colocated workload; it disables the per-send random positional walk.
    #[arg(long)]
    no_spread: bool,
    #[arg(long)]
    voice: bool,
    #[arg(long)]
    voice_audio_folder: Option<PathBuf>,
    #[arg(long)]
    voice_speaker_percent: Option<u8>,
    #[arg(long)]
    voice_hearing_distance: Option<f32>,
    #[arg(long)]
    voice_frame_duration_ms: Option<u64>,
    #[arg(long)]
    no_voice_reencode: bool,
    /// Enable applied-avatar metrics with failover among the first three clients; write at shutdown.
    #[arg(long)]
    observe_avatar_csv: Option<PathBuf>,
    /// Radius around the observing client's current position used to select nearby senders.
    #[arg(long, default_value_t = 40.0)]
    avatar_observe_radius: f32,
    /// Expected number of nearby senders, used to report senders never observed.
    #[arg(long, default_value_t = 9)]
    avatar_observe_expected_peers: usize,
    /// Begin collecting observer cadence after this file appears; used to align with load readiness.
    #[arg(long)]
    observe_avatar_start_file: Option<PathBuf>,
    /// Length of the applied-avatar observation window.
    #[arg(long, default_value_t = 60)]
    observe_avatar_window_secs: u64,
}

impl From<Args> for ClientOptions {
    fn from(args: Args) -> Self {
        Self {
            config: args.config,
            strict_config: args.strict_config,
            ip: args.ip,
            port: args.port,
            clients: args.clients,
            no_reconnect: args.no_reconnect,
            reconnect_min_secs: args.reconnect_min_secs,
            reconnect_max_secs: args.reconnect_max_secs,
            no_movement: args.no_movement,
            additional_avatar_bytes: args.additional_avatar_bytes,
            scene_data_bytes: args.scene_data_bytes,
            scene_data_interval_ms: args.scene_data_interval_ms,
            scene_data_reliable: args.scene_data_reliable,
            observe_scene_csv: args.observe_scene_csv,
            scene_start_file: args.scene_start_file,
            sync_batching: args.sync_batching,
            unity_avatar_policy: args.unity_avatar_policy,
            unity_frame_rate: args.unity_frame_rate,
            unity_pose_amplitude_degrees: args.unity_pose_amplitude_degrees,
            movement_interval_ms: args.movement_interval_ms,
            movement_jitter_percent: args.movement_jitter_percent,
            voice_jitter_percent: args.voice_jitter_percent,
            duration_secs: args.duration_secs,
            connect_batch_size: args.connect_batch_size,
            connect_batch_delay_ms: args.connect_batch_delay_ms,
            quit_batch_size: args.quit_batch_size,
            quit_batch_delay_ms: args.quit_batch_delay_ms,
            connect_timeout_ms: args.connect_timeout_ms,
            spawn_group_size: args.spawn_group_size,
            spawn_group_spacing: args.spawn_group_spacing,
            fixed_spawn_positions: args.fixed_spawn_positions,
            no_spread: args.no_spread,
            voice: args.voice,
            voice_audio_folder: args.voice_audio_folder,
            voice_speaker_percent: args.voice_speaker_percent,
            voice_hearing_distance: args.voice_hearing_distance,
            voice_frame_duration_ms: args.voice_frame_duration_ms,
            no_voice_reencode: args.no_voice_reencode,
            observe_avatar_csv: args.observe_avatar_csv,
            avatar_observe_radius: args.avatar_observe_radius,
            avatar_observe_expected_peers: args.avatar_observe_expected_peers,
            observe_avatar_start_file: args.observe_avatar_start_file,
            observe_avatar_window_secs: args.observe_avatar_window_secs,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cli_defaults_match_core_options() {
        assert!(
            Args::try_parse_from(["client", "--strict-config"])
                .unwrap()
                .strict_config
        );
        let parsed: ClientOptions = Args::try_parse_from(["basis-rust-client"]).unwrap().into();
        assert_eq!(parsed, ClientOptions::default());
    }

    #[test]
    fn randomized_cadence_is_default_and_sync_batching_is_opt_in() {
        let defaults = Args::try_parse_from(["basis-rust-client"]).unwrap();
        assert!(!defaults.sync_batching);
        assert!(!defaults.unity_avatar_policy);
        assert_eq!(defaults.unity_frame_rate, 60);
        assert_eq!(defaults.unity_pose_amplitude_degrees, 20.0);
        assert_eq!(defaults.movement_interval_ms, 90);
        assert_eq!(defaults.movement_jitter_percent, 10);
        assert_eq!(defaults.voice_jitter_percent, 5);

        let synchronized = Args::try_parse_from(["basis-rust-client", "--sync-batching"]).unwrap();
        assert!(synchronized.sync_batching);

        let dense = Args::try_parse_from([
            "basis-rust-client",
            "--movement-interval-ms",
            "20",
            "--movement-jitter-percent",
            "0",
            "--sync-batching",
            "--no-spread",
            "--unity-avatar-policy",
            "--unity-frame-rate",
            "60",
        ])
        .unwrap();
        assert_eq!(dense.movement_interval_ms, 20);
        assert_eq!(dense.movement_jitter_percent, 0);
        assert!(dense.sync_batching);
        assert!(dense.no_spread);
        assert!(dense.unity_avatar_policy);
        assert_eq!(dense.unity_frame_rate, 60);
    }
}
