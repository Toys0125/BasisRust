use anyhow::Result;
use axum::{extract::State, routing::get, Json, Router};
use basis_protocol::{config::ServerConfig, version::SERVER_VERSION};
use serde::Serialize;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;
use tracing::info;

#[derive(Clone)]
pub struct HealthState {
    pub config: Arc<parking_lot::RwLock<ServerConfig>>,
    pub player_count: Arc<dyn Fn() -> usize + Send + Sync>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
    pub version: u16,
    pub server_name: String,
    pub motd: String,
    pub players_online: usize,
    pub peer_limit: i32,
}

pub async fn start_health_server(state: HealthState) -> Result<SocketAddr> {
    let config = state.config.read().clone();
    let addr: SocketAddr = format!("{}:{}", config.health_check_host, config.health_check_port)
        .parse()
        .unwrap_or_else(|_| SocketAddr::from(([127, 0, 0, 1], config.health_check_port)));
    let app = Router::new()
        .route(&config.health_path, get(health))
        .with_state(state);
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

async fn health(State(state): State<HealthState>) -> Json<HealthResponse> {
    let config = state.config.read().clone();
    Json(HealthResponse {
        status: "healthy",
        version: SERVER_VERSION,
        server_name: config.server_name,
        motd: config.server_motd,
        players_online: (state.player_count)(),
        peer_limit: config.peer_limit,
    })
}
