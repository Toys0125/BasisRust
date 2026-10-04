use crate::client::BasisClient;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tracing::info;

#[derive(Debug, Default)]
pub(crate) struct ClientAvatarDiagnostics {
    pub(crate) movement_frame_visits: AtomicU64,
    pub(crate) generated_full: AtomicU64,
    pub(crate) generated_delta: AtomicU64,
    pub(crate) socket_sent_full: AtomicU64,
    pub(crate) socket_sent_delta: AtomicU64,
    pub(crate) send_errors: AtomicU64,
    pub(crate) last_sequence: AtomicU8,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ClientAvatarDiagnosticSnapshot {
    pub(crate) index: usize,
    pub(crate) remote_peer_id: Option<i32>,
    pub(crate) local_port: Option<u16>,
    pub(crate) connected: bool,
    pub(crate) frame_visits: u64,
    pub(crate) generated_full: u64,
    pub(crate) generated_delta: u64,
    pub(crate) socket_sent_full: u64,
    pub(crate) socket_sent_delta: u64,
    pub(crate) send_errors: u64,
    pub(crate) last_sequence: u8,
}

pub(crate) async fn client_avatar_diagnostic_snapshot(
    clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
) -> Vec<ClientAvatarDiagnosticSnapshot> {
    let clients = clients.lock().await.clone();
    let mut snapshots = Vec::with_capacity(clients.len());
    for client in clients {
        let Some(diagnostics) = &client.avatar_diagnostics else {
            continue;
        };
        let remote_peer_id = *client.remote_peer_id.lock().await;
        let local_port = client
            .socket
            .local_addr()
            .ok()
            .map(|address| address.port());
        snapshots.push(ClientAvatarDiagnosticSnapshot {
            index: client.index,
            remote_peer_id,
            local_port,
            connected: client.connected.load(Ordering::Relaxed),
            frame_visits: diagnostics.movement_frame_visits.load(Ordering::Relaxed),
            generated_full: diagnostics.generated_full.load(Ordering::Relaxed),
            generated_delta: diagnostics.generated_delta.load(Ordering::Relaxed),
            socket_sent_full: diagnostics.socket_sent_full.load(Ordering::Relaxed),
            socket_sent_delta: diagnostics.socket_sent_delta.load(Ordering::Relaxed),
            send_errors: diagnostics.send_errors.load(Ordering::Relaxed),
            last_sequence: diagnostics.last_sequence.load(Ordering::Relaxed),
        });
    }
    snapshots.sort_unstable_by_key(|snapshot| snapshot.index);
    snapshots
}

pub(crate) async fn client_avatar_diagnostic_window(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    marker_path: PathBuf,
    output_path: PathBuf,
    window_duration: Duration,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    loop {
        if marker_path.exists() {
            break;
        }
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    let started = time::Instant::now();
    let start = client_avatar_diagnostic_snapshot(&clients).await;
    let socket_ports_path = output_path.with_extension("socket-ports.csv");
    let mut socket_ports_csv =
        String::from("boundary,logical_client_index,remote_peer_id,local_port\n");
    for snapshot in &start {
        socket_ports_csv.push_str(&format!(
            "start,{},{},{}\n",
            snapshot.index,
            snapshot
                .remote_peer_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            snapshot
                .local_port
                .map(|port| port.to_string())
                .unwrap_or_default(),
        ));
    }
    if let Some(parent) = socket_ports_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&socket_ports_path, &socket_ports_csv).with_context(|| {
        format!(
            "writing avatar sender socket ports {}",
            socket_ports_path.display()
        )
    })?;
    tokio::select! {
        _ = time::sleep(window_duration) => {},
        _ = async {
            while !shutdown.load(Ordering::Relaxed) {
                time::sleep(Duration::from_millis(100)).await;
            }
        } => {},
    }
    let elapsed_ms = started.elapsed().as_millis();
    let end = client_avatar_diagnostic_snapshot(&clients).await;
    for snapshot in &end {
        socket_ports_csv.push_str(&format!(
            "end,{},{},{}\n",
            snapshot.index,
            snapshot
                .remote_peer_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            snapshot
                .local_port
                .map(|port| port.to_string())
                .unwrap_or_default(),
        ));
    }
    std::fs::write(&socket_ports_path, &socket_ports_csv).with_context(|| {
        format!(
            "writing avatar sender socket ports {}",
            socket_ports_path.display()
        )
    })?;
    let by_index = start
        .into_iter()
        .map(|snapshot| (snapshot.index, snapshot))
        .collect::<HashMap<_, _>>();
    let mut csv = String::from(
        "logical_client_index,remote_peer_id_start,remote_peer_id_end,local_port_start,local_port_end,connected_at_end,window_ms,movement_frame_visits,generated_full,generated_delta,socket_sent_full,socket_sent_delta,send_errors,start_sequence,end_sequence\n",
    );
    let peer_count = end.len();
    for current in end {
        let previous = by_index.get(&current.index).copied().unwrap_or(current);
        let difference = |end: u64, start: u64| end.saturating_sub(start);
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            current.index,
            previous
                .remote_peer_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            current
                .remote_peer_id
                .map(|id| id.to_string())
                .unwrap_or_default(),
            previous
                .local_port
                .map(|port| port.to_string())
                .unwrap_or_default(),
            current
                .local_port
                .map(|port| port.to_string())
                .unwrap_or_default(),
            current.connected,
            elapsed_ms,
            difference(current.frame_visits, previous.frame_visits),
            difference(current.generated_full, previous.generated_full),
            difference(current.generated_delta, previous.generated_delta),
            difference(current.socket_sent_full, previous.socket_sent_full),
            difference(current.socket_sent_delta, previous.socket_sent_delta),
            difference(current.send_errors, previous.send_errors),
            previous.last_sequence,
            current.last_sequence,
        ));
    }
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&output_path, csv).with_context(|| {
        format!(
            "writing avatar sender diagnostics {}",
            output_path.display()
        )
    })?;
    info!(
        "avatar sender diagnostics: window_ms={} peers={} csv={}",
        elapsed_ms,
        peer_count,
        output_path.display()
    );
    Ok(())
}

impl ClientAvatarDiagnostics {
    pub(crate) fn enabled_from_env() -> bool {
        std::env::var("BASIS_AVATAR_DIAGNOSTICS")
            .map(|value| {
                matches!(
                    value.to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "on"
                )
            })
            .unwrap_or(false)
    }
}
