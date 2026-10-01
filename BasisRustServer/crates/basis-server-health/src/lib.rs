use anyhow::Result;
use axum::{extract::State, routing::get, Json, Router};
use basis_protocol::{config::ServerConfig, version::SERVER_VERSION};
use serde::Serialize;
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};
use tokio::net::TcpListener;
use tracing::info;

#[derive(Debug, Clone, Copy, Default)]
pub struct HealthStatistics {
    pub sent: u64,
    pub recv: u64,
    pub packets_sent: u64,
    pub packets_recv: u64,
    pub dropped_unreliable: u64,
    pub dropped_voice: u64,
    pub queue_per_peer: usize,
    pub voice_queue_per_peer: usize,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ReliableMetrics {
    pub pending: usize,
    pub queued: usize,
    pub window_fills: u64,
    pub retransmits: u64,
    pub dispatch_passes: u64,
    pub peers_visited: u64,
    pub acks_in: u64,
    pub acks_released: u64,
    pub acks_unknown_channel: u64,
    pub window_stalls: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AppMessageMetrics {
    pub inbound: u64,
    pub outbound: u64,
    pub protocol_errors: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RawUdpMetrics {
    pub packets_in: u64,
    pub packets_out: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub would_block: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AvatarSyncMetrics {
    pub gpu_distance: GpuDistanceMetrics,
    pub inbound_updates: u64,
    pub outbound_messages: u64,
    pub outbound_logical_avatar_sends: u64,
    pub outbound_batches: u64,
    pub active_states: usize,
    pub pending_updates: usize,
    pub receiver_slices: usize,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GpuDistanceMetrics {
    pub enabled: bool,
    pub adapter: Option<String>,
    pub interval_ticks: u64,
    pub submissions: u64,
    pub swaps: u64,
    pub missed_swaps: u64,
    pub stale_fallbacks: u64,
    pub active_epoch: Option<u64>,
    pub last_error: Option<String>,
    pub computed_pairs: u64,
    pub corrected_pairs: u64,
    pub last_worker_micros: u64,
    pub max_worker_micros: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AvatarTimingMetrics {
    pub ticks: u64,
    pub avg_tick_us: u64,
    pub smooth_tick_us: u64,
    pub avg_build_us: u64,
    pub avg_flush_us: u64,
    pub max_tick_us: u64,
    pub receiver_cycle_ms: u64,
    pub cycle_budget_ms: u64,
    pub tick_budget_ms: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ExtendedHealthMetrics {
    pub reliable: ReliableMetrics,
    pub app_messages: AppMessageMetrics,
    pub raw_udp: RawUdpMetrics,
    pub avatar_sync: AvatarSyncMetrics,
    pub avatar_timing: AvatarTimingMetrics,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BsrLoadMetrics {
    pub tick_ms: f64,
    pub overrun_ratio: f64,
    pub interval_ms: u64,
    pub hz: u64,
    pub shed_tier: u64,
    pub shed_tier_name: String,
    pub slice_count: usize,
    pub send_workers: usize,
    pub send_worker_cap: usize,
    pub send_budget_percent: i32,
    pub send_duty: f64,
    pub pairs_per_worker_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct BsrHealthMetrics {
    pub load: BsrLoadMetrics,
    pub window: Option<BsrWindowMetrics>,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BsrMsPerTickMetrics {
    pub drain: f64,
    pub process: f64,
    pub distance: f64,
    pub update: f64,
    pub trigger: f64,
    pub total: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BsrZstdMetrics {
    pub dict_generation: u64,
    pub emitted: u64,
    pub share_of_bundles: f64,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
    pub ratio: f64,
    pub ms_per_tick: f64,
    pub avg_us: f64,
    pub lz4_ratio: f64,
    pub lz4_avg_us: f64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BsrBundleMetrics {
    pub emitted: u64,
    pub messages: u64,
    pub tail_uncompressed: u64,
    pub fallbacks: u64,
    pub retries: u64,
    pub raw_bytes: u64,
    pub compressed_bytes: u64,
    pub saved_bytes: u64,
    pub ratio: f64,
    pub per_tick: f64,
    pub avg_messages: f64,
    pub deflate_ms_per_tick: f64,
    pub avg_deflate_us: f64,
    pub zstd: BsrZstdMetrics,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BsrWindowMetrics {
    pub captured_time: String,
    pub ticks: u64,
    pub messages: u64,
    pub sends: u64,
    pub pre_serialized: u64,
    pub pre_serialized_skipped: u64,
    pub ms_per_tick: BsrMsPerTickMetrics,
    pub bundles: BsrBundleMetrics,
}

#[derive(Clone)]
pub struct HealthState {
    pub config: Arc<parking_lot::RwLock<ServerConfig>>,
    pub player_count: Arc<dyn Fn() -> usize + Send + Sync>,
    pub statistics: Arc<dyn Fn() -> HealthStatistics + Send + Sync>,
    pub extended_metrics: Arc<dyn Fn() -> ExtendedHealthMetrics + Send + Sync>,
    pub bsr_metrics: Arc<dyn Fn() -> BsrHealthMetrics + Send + Sync>,
}

#[derive(Clone)]
struct HealthRuntimeState {
    state: HealthState,
    start_time: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GcMetrics {
    pub gen0: u64,
    pub gen1: u64,
    pub gen2: u64,
    pub heap_mb: f64,
    pub allocated_mb: f64,
    pub committed_mb: f64,
    pub fragmented_mb: f64,
    pub pause_time_percent: f64,
    pub server_gc: bool,
    pub latency_mode: &'static str,
    pub supported: bool,
}

impl Default for GcMetrics {
    fn default() -> Self {
        Self {
            gen0: 0,
            gen1: 0,
            gen2: 0,
            heap_mb: 0.0,
            allocated_mb: 0.0,
            committed_mb: 0.0,
            fragmented_mb: 0.0,
            pause_time_percent: 0.0,
            server_gc: false,
            latency_mode: "not_applicable",
            supported: false,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: u16,
    pub server_name: String,
    pub motd: String,
    pub players_online: usize,
    pub peer_limit: i32,
    pub listening: bool,
    pub ready: bool,
    #[serde(rename = "currentTime")]
    pub current_time: String,
    #[serde(rename = "startTime")]
    pub start_time: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visitors: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capacity: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sent: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recv: Option<u64>,
    #[serde(rename = "packetsSent", skip_serializing_if = "Option::is_none")]
    pub packets_sent: Option<u64>,
    #[serde(rename = "packetsRecv", skip_serializing_if = "Option::is_none")]
    pub packets_recv: Option<u64>,
    #[serde(rename = "droppedUnreliable", skip_serializing_if = "Option::is_none")]
    pub dropped_unreliable: Option<u64>,
    #[serde(rename = "droppedVoice", skip_serializing_if = "Option::is_none")]
    pub dropped_voice: Option<u64>,
    #[serde(rename = "queuePerPeer", skip_serializing_if = "Option::is_none")]
    pub queue_per_peer: Option<usize>,
    #[serde(rename = "voiceQueuePerPeer", skip_serializing_if = "Option::is_none")]
    pub voice_queue_per_peer: Option<usize>,
    pub gc: GcMetrics,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bsr: Option<BsrHealthMetrics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extended: Option<ExtendedHealthMetrics>,
}

pub async fn start_health_server(state: HealthState) -> Result<SocketAddr> {
    let config = state.config.read().clone();
    let addr: SocketAddr = format!("{}:{}", config.health_check_host, config.health_check_port)
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], config.health_check_port)));
    let runtime_state = HealthRuntimeState {
        state,
        start_time: now_rfc3339(),
    };
    let app = Router::new()
        .route(&config.health_path, get(health))
        .with_state(runtime_state);
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app).await {
            tracing::warn!("health server stopped: {err}");
        }
    });
    info!(
        "health endpoint listening on http://{local_addr}{}",
        config.health_path
    );
    Ok(local_addr)
}

async fn health(State(state): State<HealthRuntimeState>) -> Json<HealthResponse> {
    Json(build_health_response(&state))
}

fn build_health_response(state: &HealthRuntimeState) -> HealthResponse {
    let config = state.state.config.read().clone();
    let players_online = (state.state.player_count)();
    let statistics = config.enable_statistics.then(|| (state.state.statistics)());
    let extended = config
        .health_include_extended_metrics
        .then(|| (state.state.extended_metrics)());
    let bsr = config
        .health_include_bsr_profiling
        .then(|| (state.state.bsr_metrics)());

    HealthResponse {
        status: "healthy",
        version: SERVER_VERSION,
        server_name: config.server_name,
        motd: config.server_motd,
        players_online,
        peer_limit: config.peer_limit,
        listening: true,
        ready: true,
        current_time: now_rfc3339(),
        start_time: state.start_time.clone(),
        visitors: statistics.map(|_| players_online),
        capacity: statistics.map(|_| config.peer_limit),
        sent: statistics.map(|stats| stats.sent),
        recv: statistics.map(|stats| stats.recv),
        packets_sent: statistics.map(|stats| stats.packets_sent),
        packets_recv: statistics.map(|stats| stats.packets_recv),
        dropped_unreliable: statistics.map(|stats| stats.dropped_unreliable),
        dropped_voice: statistics.map(|stats| stats.dropped_voice),
        queue_per_peer: statistics.map(|stats| stats.queue_per_peer),
        voice_queue_per_peer: statistics.map(|stats| stats.voice_queue_per_peer),
        gc: GcMetrics::default(),
        bsr,
        extended,
    }
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

pub fn format_system_time(value: SystemTime) -> String {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| {
            OffsetDateTime::from_unix_timestamp_nanos(duration.as_nanos() as i128).ok()
        })
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn state_with_config(config: ServerConfig) -> (HealthRuntimeState, Arc<AtomicUsize>) {
        let extended_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&extended_calls);
        (
            HealthRuntimeState {
                state: HealthState {
                    config: Arc::new(parking_lot::RwLock::new(config)),
                    player_count: Arc::new(|| 7),
                    statistics: Arc::new(|| HealthStatistics {
                        sent: 100,
                        recv: 200,
                        packets_sent: 3,
                        packets_recv: 4,
                        dropped_unreliable: 5,
                        dropped_voice: 6,
                        queue_per_peer: 7,
                        voice_queue_per_peer: 8,
                    }),
                    extended_metrics: Arc::new(move || {
                        calls.fetch_add(1, Ordering::Relaxed);
                        ExtendedHealthMetrics::default()
                    }),
                    bsr_metrics: Arc::new(|| BsrHealthMetrics {
                        load: BsrLoadMetrics::default(),
                        window: None,
                    }),
                },
                start_time: "2026-09-27T00:00:00Z".to_string(),
            },
            extended_calls,
        )
    }

    #[test]
    fn disabled_extended_metrics_are_not_sampled() {
        let config = ServerConfig::default();
        let (state, calls) = state_with_config(config);

        let response = build_health_response(&state);

        assert!(response.extended.is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn enabled_extended_metrics_are_sampled() {
        let config = ServerConfig {
            health_include_extended_metrics: true,
            ..ServerConfig::default()
        };
        let (state, calls) = state_with_config(config);

        let response = build_health_response(&state);

        assert!(response.extended.is_some());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn csharp_transport_statistics_follow_enable_statistics() {
        let config = ServerConfig::default();
        let (state, _) = state_with_config(config);
        let response = build_health_response(&state);

        assert_eq!(response.visitors, Some(7));
        assert_eq!(response.sent, Some(100));
        assert_eq!(response.recv, Some(200));
        assert_eq!(response.packets_sent, Some(3));
        assert_eq!(response.packets_recv, Some(4));
        assert_eq!(response.dropped_unreliable, Some(5));
        assert_eq!(response.dropped_voice, Some(6));
        assert_eq!(response.queue_per_peer, Some(7));
        assert_eq!(response.voice_queue_per_peer, Some(8));
    }
}
