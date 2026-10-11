//! Server core used by the image benchmark, with read-only one-second telemetry.
use anyhow::{Context, Result};
use basis_protocol::config::ServerConfig;
use basis_server_core::ServerState;
use serde_json::json;
use std::{io::Write, path::PathBuf, time::Instant};

fn main() -> Result<()> {
    let workers = std::env::var("TOKIO_WORKER_THREADS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map_or(4, usize::from))
        .max(1);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let config_path = PathBuf::from(args.next().context("server config path required")?);
    let base_dir = PathBuf::from(args.next().context("server base directory required")?);
    let telemetry_path = PathBuf::from(args.next().context("telemetry path required")?);
    anyhow::ensure!(args.next().is_none(), "unexpected argument");
    let config = ServerConfig::load_or_create(&config_path)?;
    let (state, shutdown) =
        ServerState::start_with_config_path(config, &base_dir, &config_path).await?;
    let mut output = std::io::BufWriter::new(std::fs::File::create(telemetry_path)?);
    let started = Instant::now();
    let mut ticker = tokio::time::interval(std::time::Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = ticker.tick() => {}
        }
        let (cached, complete, cache_bytes) = state.image_cache_stats();
        let (dropped_messages, dropped_fanout_bytes) = state.image_egress_dropped();
        let statistics = state.statistics.snapshot();
        let transport = state.transport.stats_snapshot();
        let depth = state.transport.depths_snapshot();
        let row = json!({
            "elapsed_seconds": started.elapsed().as_secs_f64(),
            "listen_address": state.transport.local_addr()?.to_string(),
            "players_online": state.player_count(),
            "cache_images": cached, "cache_complete": complete, "cache_bytes": cache_bytes,
            "image_dropped_messages": dropped_messages,
            "image_dropped_fanout_bytes": dropped_fanout_bytes,
            "protocol_errors": statistics.protocol_errors,
            "wire_bytes_sent": transport.raw_bytes_sent,
            "wire_bytes_received": transport.raw_bytes_received,
            "wire_packets_sent": transport.raw_packets_sent,
            "wire_packets_received": transport.raw_packets_received,
            "send_would_block": transport.raw_send_would_block,
            "reliable_retransmits": transport.reliable_retransmits,
            "reliable_window_stalls": transport.reliable_window_stalls,
            "reorder_budget_rejections": transport.ordered_reorder_budget_rejections,
            "reliable_pending": depth.reliable_pending,
            "reliable_queued": depth.reliable_queued,
            "pending_datagrams": depth.pending_datagrams,
        });
        serde_json::to_writer(&mut output, &row)?;
        writeln!(output)?;
        output.flush()?;
    }
    let _ = shutdown.send(());
    state.shutdown().await
}
