//! REST management API, mirroring BasisVR's `BasisRestApiHandler` and
//! `BasisRestApiRoutes`.
//!
//! The API binds an additional HTTP listener at `ApiHost:ApiPort` (only when
//! `ApiEnabled` starts the caller) and gates every request on a `Bearer` API
//! key. Endpoints cover announcements, player listing, and world load/unload
//! management; the JSON envelopes intentionally match the C# reference so the
//! same tooling drives both servers.
//!
//! Routing and validation parity with the C# implementation:
//! - `POST /api/announce` -> announce to every authenticated peer
//! - `POST /api/announce/{uuid}` -> announce to a single player (404 if absent)
//! - `GET /api/players` -> netId/uuid/displayName/platform/position listing
//! - `GET /api/worlds` -> netId/url/persistent/adminLocked/strategy listing
//! - `POST /api/worlds` -> load a world (url, password, persistent, strategy)
//! - `DELETE /api/worlds` -> unload every scene-world
//! - `DELETE /api/worlds/{netId}` -> unload one world by ID
//! - `POST /api/worlds/switch` -> announce + (optionally delayed) synchronized load
//!

use anyhow::Result;
use axum::{
    extract::{Path, Request, State},
    http::{
        header::{AUTHORIZATION, CACHE_CONTROL, WWW_AUTHENTICATE, X_CONTENT_TYPE_OPTIONS},
        HeaderMap, HeaderValue, StatusCode, Uri,
    },
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use basis_protocol::{
    channels,
    io::NetWriter,
    messages::{AdminRequest, AdminRequestMode, BasisSerialize, LocalLoadResource, UnloadResource},
};
use sha2::{Digest, Sha256};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;
use tracing::info;
use uuid::Uuid;

use crate::ServerState;
use basis_transport::DeliveryMethod;
use std::sync::atomic::Ordering;

const MAX_BODY_BYTES: usize = 1 << 20;
const MAX_MESSAGE_LENGTH: usize = 512;
const MAX_SWITCH_DELAY_SECONDS: i32 = 300;
const MAX_CONCURRENT_REQUESTS: usize = 32;
const MAX_PENDING_SWITCHES: usize = 32;
const MAX_WIRE_STRING_BYTES: usize = u16::MAX as usize - 1;

const LOAD_STRATEGY_IMMEDIATE: u8 = 0;
const LOAD_STRATEGY_SYNCHRONIZED: u8 = 2;
const LOAD_STRATEGY_PREDOWNLOAD: u8 = 3;

#[derive(Clone)]
struct RestApiRuntime {
    state: ServerState,
    key_hash: Option<Vec<u8>>,
    permits: Arc<Semaphore>,
    delayed: Arc<tokio::sync::Mutex<tokio::task::JoinSet<()>>>,
}

/// Start the REST API listener for `state`.
///
/// The server configuration is read once, matching the C# handler, which
/// captures `ApiHost`/`ApiPort`/`ApiKey` at construction. Callers must gate on
/// `ApiEnabled` and a nonempty `ApiKey` before starting this listener.
pub async fn start_rest_api_server(state: ServerState) -> Result<SocketAddr> {
    let (host, port, api_key) = {
        let config = state.config.read();
        (
            config.api_host.clone(),
            config.api_port,
            config.api_key.clone(),
        )
    };
    let key_hash = if api_key.is_empty() {
        Vec::new()
    } else {
        Sha256::digest(api_key.as_bytes()).to_vec()
    };

    let runtime = RestApiRuntime {
        state: state.clone(),
        key_hash: Some(key_hash),
        permits: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
        delayed: Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
    };
    // Resolve configured names and fail explicitly on invalid hosts. Never
    // silently expose a listener on a different interface.
    let listener = tokio::net::TcpListener::bind((host.as_str(), port)).await?;
    let local_addr = listener.local_addr()?;
    let app = registration_router(runtime.clone());
    let shutdown = state.shutdown.clone();
    let worker = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app)
            .with_graceful_shutdown(wait_for_shutdown(shutdown))
            .await
        {
            tracing::warn!("REST API server stopped: {err}");
        }
        let mut delayed = runtime.delayed.lock().await;
        delayed.abort_all();
        while delayed.join_next().await.is_some() {}
    });
    state.workers.lock().push(worker);
    info!("REST API listening on http://{local_addr}/api/");
    Ok(local_addr)
}

async fn wait_for_shutdown(shutdown: Arc<std::sync::atomic::AtomicBool>) {
    while !shutdown.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn registration_router(runtime: RestApiRuntime) -> Router {
    let routes = Router::new()
        .route("/api/announce", post(announce_all))
        .route("/api/announce/{uuid}", post(announce_player))
        .route(
            "/api/players",
            get(list_players)
                .head(|| async { (StatusCode::METHOD_NOT_ALLOWED, [("allow", "GET")]) }),
        )
        .route(
            "/api/worlds",
            get(list_worlds)
                .post(load_world)
                .delete(clear_worlds)
                .head(|| async {
                    (
                        StatusCode::METHOD_NOT_ALLOWED,
                        [("allow", "GET, POST, DELETE")],
                    )
                }),
        )
        .route("/api/worlds/switch", post(switch_world))
        .route("/api/worlds/{net_id}", delete(unload_world))
        .fallback(fallthrough)
        .with_state(runtime.clone());
    Router::new()
        .fallback_service(routes)
        .layer(middleware::from_fn_with_state(
            runtime.clone(),
            gate_request,
        ))
}

/// Bearer authentication plus the shared response headers, mirroring
/// `BasisRestApiHandler.HandleRequest`.
async fn gate_request(
    State(runtime): State<RestApiRuntime>,
    mut request: Request,
    next: Next,
) -> Response {
    let _permit = match runtime.permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            // Match the C# saturation response: trimmed, empty 503.
            return (StatusCode::SERVICE_UNAVAILABLE, "").into_response();
        }
    };
    // BasisVR trims trailing slashes before dispatch; rewrite before the inner
    // router matches while retaining the original query string.
    let uri = request.uri();
    if uri.path().len() > 1 && uri.path().ends_with('/') {
        let mut parts = uri.clone().into_parts();
        let path = uri.path().trim_end_matches('/');
        let normalized = match uri.query() {
            Some(query) => format!("{path}?{query}"),
            None => path.to_owned(),
        };
        if let Ok(path_and_query) = normalized.parse() {
            parts.path_and_query = Some(path_and_query);
            if let Ok(uri) = Uri::from_parts(parts) {
                *request.uri_mut() = uri;
            }
        }
    }
    let mut response = if !bearer_authorized(&runtime.key_hash, request.headers()) {
        (
            StatusCode::UNAUTHORIZED,
            [(WWW_AUTHENTICATE, "Bearer realm=\"basis-server\"")],
            "",
        )
            .into_response()
    } else {
        tokio::select! {
            _ = wait_for_shutdown(runtime.state.shutdown.clone()) => StatusCode::SERVICE_UNAVAILABLE.into_response(),
            result = tokio::time::timeout(Duration::from_secs(30), next.run(request)) => {
                result.unwrap_or_else(|_| StatusCode::REQUEST_TIMEOUT.into_response())
            }
        }
    };
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("no-store, max-age=0"),
    );
    response
        .headers_mut()
        .insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

/// Constant-time Bearer comparison against the configured API key's digest.
fn bearer_authorized(key_hash: &Option<Vec<u8>>, headers: &HeaderMap) -> bool {
    let Some(expected) = key_hash else {
        return false;
    };
    let Some(supplied) = headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .filter(|value| {
            value
                .get(..7)
                .is_some_and(|prefix| prefix.eq_ignore_ascii_case("bearer "))
        })
        .and_then(|value| value.get(7..))
    else {
        return false;
    };
    let digest = Sha256::digest(supplied.as_bytes()).to_vec();
    bool::from(digest.as_slice().ct_eq(expected))
}

async fn announce_all(State(runtime): State<RestApiRuntime>, request: Request) -> Response {
    let body = match read_json_body(request).await {
        Ok(body) => body,
        Err(response) => return response.into_response(),
    };
    let message = match extract_message(&body) {
        Ok(message) => message,
        Err(response) => return response.into_response(),
    };
    server_announce_all(&runtime.state, &message).await;
    ok_response()
}

async fn announce_player(
    State(runtime): State<RestApiRuntime>,
    Path(uuid): Path<String>,
    request: Request,
) -> Response {
    let body = match read_json_body(request).await {
        Ok(body) => body,
        Err(response) => return response.into_response(),
    };
    let message = match extract_message(&body) {
        Ok(message) => message,
        Err(response) => return response.into_response(),
    };
    if !server_announce_player(&runtime.state, &uuid, &message).await {
        return not_found("player not found");
    }
    ok_response()
}

async fn list_players(State(runtime): State<RestApiRuntime>) -> Response {
    let state = &runtime.state;
    let players: Vec<serde_json::Value> = state
        .authenticated_peers
        .iter()
        .map(|entry| {
            let peer = entry.value();
            let position = state.avatar_sync.player_position(*entry.key());
            serde_json::json!({
                "netId": entry.key(),
                "uuid": peer.metadata.player_uuid,
                "displayName": peer.metadata.player_display_name,
                "platform": peer.metadata.player_platform,
                "position": position,
            })
        })
        .collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({ "players": players })),
    )
        .into_response()
}

async fn list_worlds(State(runtime): State<RestApiRuntime>) -> Response {
    let mut worlds: Vec<serde_json::Value> = runtime
        .state
        .resources
        .all_resources()
        .into_iter()
        .filter(|resource| resource.mode == 1)
        .map(|resource| {
            serde_json::json!({
                "netId": resource.loaded_net_id,
                "url": resource.combined_url,
                "persistent": resource.persist,
                "adminLocked": resource.is_admin_locked,
                "strategy": resource.load_strategy,
            })
        })
        .collect();
    worlds.sort_by(|a, b| a["netId"].as_str().cmp(&b["netId"].as_str()));
    (
        StatusCode::OK,
        Json(serde_json::json!({ "worlds": worlds })),
    )
        .into_response()
}

async fn load_world(State(runtime): State<RestApiRuntime>, request: Request) -> Response {
    let body = match read_json_body(request).await {
        Ok(body) => body,
        Err(response) => return response.into_response(),
    };
    let (url, password, persistent, strategy) = match extract_world_params(&body) {
        Ok(params) => params,
        Err(response) => return response.into_response(),
    };
    let resource = build_world_resource(&url, &password, persistent, strategy);
    let net_id = resource.loaded_net_id.clone();
    match server_load_world(&runtime.state, resource).await {
        Ok(true) => (
            StatusCode::OK,
            Json(serde_json::json!({ "ok": true, "netId": net_id })),
        )
            .into_response(),
        Ok(false) => internal_error(),
        Err(err) => {
            tracing::warn!("REST API load world failed: {err:#}");
            internal_error()
        }
    }
}

async fn unload_world(
    State(runtime): State<RestApiRuntime>,
    Path(net_id): Path<String>,
) -> Response {
    match server_unload_world(&runtime.state, &net_id).await {
        Ok(true) => ok_response(),
        Ok(false) => not_found("world not found"),
        Err(err) => {
            tracing::warn!("REST API unload world failed: {err:#}");
            internal_error()
        }
    }
}

async fn clear_worlds(State(runtime): State<RestApiRuntime>) -> Response {
    match server_clear_worlds(&runtime.state).await {
        Ok(count) => (
            StatusCode::OK,
            Json(serde_json::json!({ "ok": true, "unloaded": count })),
        )
            .into_response(),
        Err(err) => {
            tracing::warn!("REST API clear worlds failed: {err:#}");
            internal_error()
        }
    }
}

async fn switch_world(State(runtime): State<RestApiRuntime>, request: Request) -> Response {
    let body = match read_json_body(request).await {
        Ok(body) => body,
        Err(response) => return response.into_response(),
    };
    let (url, password, persistent) = match extract_url_password(&body) {
        Ok(params) => params,
        Err(response) => return response.into_response(),
    };
    let announce = match extract_optional_message(&body, "announceMessage") {
        Ok(announce) => announce,
        Err(response) => return response.into_response(),
    };
    let delay = match extract_delay(&body) {
        Ok(delay) => delay,
        Err(response) => return response.into_response(),
    };
    let resource = build_world_resource(&url, &password, persistent, LOAD_STRATEGY_SYNCHRONIZED);
    let net_id = resource.loaded_net_id.clone();
    if delay > 0 {
        let state = runtime.state.clone();
        let net_id_for_task = net_id.clone();
        let mut tasks = runtime.delayed.lock().await;
        while tasks.try_join_next().is_some() {}
        if tasks.len() >= MAX_PENDING_SWITCHES {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"error": "too many pending world switches"})),
            )
                .into_response();
        }
        if !announce.is_empty() {
            server_announce_all(&runtime.state, &announce).await;
        }
        tasks.spawn(async move {
            info!("REST API switch world scheduled: {net_id_for_task} delay={delay}s");
            tokio::select! {
                _ = wait_for_shutdown(state.shutdown.clone()) => return,
                _ = tokio::time::sleep(Duration::from_secs(delay as u64)) => {}
            }
            match server_load_world(&state, resource).await {
                Ok(true) => {}
                Ok(false) => tracing::warn!("REST API delayed switch world was not admitted"),
                Err(err) => tracing::warn!("REST API delayed switch world failed: {err:#}"),
            }
        });
    } else {
        if !announce.is_empty() {
            server_announce_all(&runtime.state, &announce).await;
        }
        if !matches!(server_load_world(&runtime.state, resource).await, Ok(true)) {
            return internal_error();
        }
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({ "ok": true, "netId": net_id })),
    )
        .into_response()
}

/// Non-`/api/**` paths 404 empty like the C# handler; unknown resources under
/// `/api/**` answer with the structured not-found body.
async fn fallthrough(uri: Uri) -> Response {
    match uri.path().strip_prefix("/api/") {
        Some(resource) if !resource.is_empty() => not_found("not found"),
        _ => (StatusCode::NOT_FOUND, "").into_response(),
    }
}

/// Broadcast `AdminRequest(MessageAll)` on the admin channel; the C# control
/// implementation (`BasisServerControl.AnnounceAll`) sends the same message.
pub(crate) async fn server_announce_all(state: &ServerState, message: &str) {
    let mut writer = NetWriter::new();
    if let Err(err) = (|| -> anyhow::Result<()> {
        AdminRequest {
            mode: AdminRequestMode::MessageAll,
        }
        .serialize(&mut writer)?;
        writer.put_string(message)?;
        Ok(())
    })() {
        tracing::warn!("REST API announce serialization failed: {err:#}");
        return;
    }
    state
        .broadcast(
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
            None,
        )
        .await;
    info!("REST API announced to all: {message}");
}

/// Resolve a UUID to its authenticated peer and deliver an admin text, the
/// equivalent of `BasisPlayerModeration.SendBackMessage`.
pub(crate) async fn server_announce_player(state: &ServerState, uuid: &str, message: &str) -> bool {
    if uuid.is_empty() {
        return false;
    }
    let target = state
        .authenticated_peers
        .iter()
        .find(|entry| entry.value().metadata.player_uuid == uuid)
        .map(|entry| *entry.key());
    let Some(target) = target else {
        return false;
    };
    crate::send_admin_text(state, target, message).await.is_ok()
}

/// Register and broadcast a server-owned world exactly like the client-initiated
/// `LOAD_RESOURCE` path: synchronized strategies open a preload session first.
pub(crate) async fn server_load_world(
    state: &ServerState,
    resource: LocalLoadResource,
) -> Result<bool> {
    let _resource_operation = state.resource_operations.lock().await;
    if state.shutdown.load(Ordering::Acquire) {
        return Ok(false);
    }
    let peers: Vec<u16> = state.authenticated_peers.iter().map(|p| *p.key()).collect();
    let configured = state.config.read().max_loaded_resources_per_player;
    let max_loaded_resources = if configured > 0 {
        configured as usize
    } else {
        16_384
    };
    let mut writer = NetWriter::new();
    resource.serialize(&mut writer)?;
    let accepted = if resource.load_strategy == LOAD_STRATEGY_PREDOWNLOAD {
        // Predownload only warms client caches; it is never replayed to joiners.
        true
    } else if resource.load_strategy == LOAD_STRATEGY_SYNCHRONIZED {
        state
            .resources
            .start_preload(resource.clone(), &peers, max_loaded_resources)
    } else {
        state
            .resources
            .load_resource_with_limit(resource.clone(), max_loaded_resources)
    };
    if !accepted {
        return Ok(false);
    }
    state
        .broadcast(
            channels::LOAD_RESOURCE,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
            None,
        )
        .await;
    let spawn = (resource.load_strategy == LOAD_STRATEGY_SYNCHRONIZED)
        .then(|| {
            state
                .resources
                .complete_empty_preload(&resource.loaded_net_id)
        })
        .flatten();
    if let Some(spawn) = spawn {
        crate::broadcast_spawn_preloaded_locked(state, spawn).await;
    }
    info!(
        "REST API load world: {} strategy={} netId={}",
        resource.combined_url, resource.load_strategy, resource.loaded_net_id
    );
    Ok(true)
}

/// Remove one mode-1 resource and broadcast the unload. Returns false when the
/// ID is absent so the caller maps it to a structured 404.
pub(crate) async fn server_unload_world(state: &ServerState, net_id: &str) -> Result<bool> {
    let _resource_operation = state.resource_operations.lock().await;
    let Some(removed) = state
        .resources
        .unload_resource_if(net_id, |resource| resource.mode == 1)
    else {
        return Ok(false);
    };
    let mut writer = NetWriter::new();
    UnloadResource {
        loaded_net_id: removed.loaded_net_id,
        mode: removed.mode,
    }
    .serialize(&mut writer)?;
    state
        .broadcast(
            channels::UNLOAD_RESOURCE,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
            None,
        )
        .await;
    info!("REST API unload world: {net_id}");
    Ok(true)
}

/// Unload every scene (mode 1) resource, then broadcast `ClearAllScenes` as the
/// C# control does. Only removed scenes' preload sessions are cancelled;
/// synchronized props remain registered and can still finish loading.
pub(crate) async fn server_clear_worlds(state: &ServerState) -> Result<usize> {
    let _resource_operation = state.resource_operations.lock().await;
    let unloads = state.resources.all_scene_unloads();
    let count = unloads.len();
    for unload in &unloads {
        let mut writer = NetWriter::new();
        unload.serialize(&mut writer)?;
        state
            .broadcast(
                channels::UNLOAD_RESOURCE,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
                None,
            )
            .await;
    }
    let mut writer = NetWriter::new();
    AdminRequest {
        mode: AdminRequestMode::ClearAllScenes,
    }
    .serialize(&mut writer)?;
    state
        .broadcast(
            channels::ADMIN,
            DeliveryMethod::ReliableOrdered,
            writer.as_slice(),
            None,
        )
        .await;
    info!("REST API clear worlds: unloaded {count} scene(s)");
    Ok(count)
}

/// `BasisServerControl.SwitchWorld` builds a synchronized, admin-locked scene
/// and optionally announces before the delayed or immediate load.
fn build_world_resource(
    url: &str,
    password: &str,
    persistent: bool,
    strategy: u8,
) -> LocalLoadResource {
    LocalLoadResource {
        mode: 1,
        loaded_net_id: Uuid::new_v4().simple().to_string(),
        unlock_password: password.to_string(),
        combined_url: url.to_string(),
        uuid_of_creator: "server".to_string(),
        is_admin_locked: true,
        position_x: 0.0,
        position_y: 0.0,
        position_z: 0.0,
        quaternion_x: 0.0,
        quaternion_y: 0.0,
        quaternion_z: 0.0,
        quaternion_w: 1.0,
        scale_x: 1.0,
        scale_y: 1.0,
        scale_z: 1.0,
        persist: persistent,
        static_resource: false,
        static_admin_locked: false,
        modify_scale: false,
        load_strategy: strategy,
    }
}

/// Read the request body as object JSON; an empty body is `{}`.
async fn read_json_body(request: Request) -> std::result::Result<serde_json::Value, ApiError> {
    let bytes = match axum::body::to_bytes(request.into_body(), MAX_BODY_BYTES + 1).await {
        Ok(bytes) => bytes,
        Err(_) => return Err(too_large()),
    };
    if bytes.len() > MAX_BODY_BYTES {
        return Err(too_large());
    }
    if bytes.iter().all(u8::is_ascii_whitespace) {
        return Ok(serde_json::Value::Object(serde_json::Map::new()));
    }
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| bad_request("invalid JSON body"))?;
    if !value.is_object() {
        return Err(bad_request("JSON body must be an object"));
    }
    Ok(value)
}

fn extract_message(body: &serde_json::Value) -> std::result::Result<String, ApiError> {
    extract_optional_message(body, "message")
}

/// Shared body-field extraction; `announceMessage` (switch world) permits an
/// absent value while `message` (announce) requires it, matching the C# routes.
fn extract_optional_message(
    body: &serde_json::Value,
    field: &str,
) -> std::result::Result<String, ApiError> {
    let Some(value) = body.get(field) else {
        if field == "announceMessage" {
            return Ok(String::new());
        }
        return Err(bad_request("missing message"));
    };
    let Some(message) = value.as_str() else {
        return Err(bad_request(&format!("{field} must be a string")));
    };
    if message.is_empty() {
        if field == "announceMessage" {
            return Ok(String::new());
        }
        return Err(bad_request("message is empty"));
    }
    if message.encode_utf16().count() > MAX_MESSAGE_LENGTH {
        return Err(bad_request(&format!(
            "{field} exceeds {MAX_MESSAGE_LENGTH} characters"
        )));
    }
    Ok(message.to_string())
}

fn extract_field_as_str<'a>(
    body: &'a serde_json::Value,
    field: &str,
    error: &str,
) -> std::result::Result<&'a str, ApiError> {
    let Some(value) = body.get(field) else {
        return Err(bad_request(error));
    };
    value.as_str().ok_or_else(|| bad_request(error))
}

/// `url` must be https, `password` comes from the field or the URL fragment,
/// `persistent` defaults false and `strategy` mirrors the C# enum names.
fn extract_world_params(
    body: &serde_json::Value,
) -> std::result::Result<(String, String, bool, u8), ApiError> {
    let (url, password, persistent) = extract_url_password(body)?;
    Ok((url, password, persistent, extract_strategy(body)?))
}

fn extract_url_password(
    body: &serde_json::Value,
) -> std::result::Result<(String, String, bool), ApiError> {
    let raw_url = extract_field_as_str(body, "url", "missing url")?;
    let (url, fragment) = split_url_fragment(raw_url);

    let password = match body.get("password") {
        Some(value) => value
            .as_str()
            .ok_or_else(|| bad_request("password must be a string"))?
            .to_string(),
        None => fragment.unwrap_or_default(),
    };

    for (field, length) in [("url", url.len()), ("password", password.len())] {
        if length > MAX_WIRE_STRING_BYTES {
            return Err(bad_request(&format!(
                "{field} exceeds {MAX_WIRE_STRING_BYTES} UTF-8 bytes"
            )));
        }
    }

    if url.is_empty() {
        return Err(bad_request("url must not be empty"));
    }
    if !url
        .get(..8)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https://"))
        || !url::Url::parse(&url)
            .is_ok_and(|parsed| parsed.scheme() == "https" && parsed.host_str().is_some())
    {
        return Err(bad_request("url must use https://"));
    }
    if password.is_empty() {
        return Err(bad_request(
            "password required (provide password field or embed in url as #fragment)",
        ));
    }

    let persistent = body
        .get("persistent")
        .and_then(|value| value.as_bool())
        .unwrap_or(false);
    Ok((url, password, persistent))
}

fn extract_strategy(body: &serde_json::Value) -> std::result::Result<u8, ApiError> {
    let Some(value) = body.get("strategy") else {
        return Ok(LOAD_STRATEGY_IMMEDIATE);
    };
    if let Some(name) = value.as_str() {
        return match name {
            "immediate" => Ok(LOAD_STRATEGY_IMMEDIATE),
            "synchronized" => Ok(LOAD_STRATEGY_SYNCHRONIZED),
            "predownload" => Ok(LOAD_STRATEGY_PREDOWNLOAD),
            _ => Err(bad_request("unknown strategy")),
        };
    }
    if let Some(number) = value.as_u64() {
        return match number {
            0 | 2 | 3 => Ok(number as u8),
            _ => Err(bad_request("unknown strategy")),
        };
    }
    Err(bad_request("unknown strategy"))
}

fn extract_delay(body: &serde_json::Value) -> std::result::Result<i32, ApiError> {
    let Some(value) = body.get("delay") else {
        return Ok(0);
    };
    if value.is_null() {
        return Ok(0);
    }
    let Some(delay) = value.as_i64() else {
        return Err(bad_request("delay must be an integer 0–300 (seconds)"));
    };
    if !(0..=MAX_SWITCH_DELAY_SECONDS as i64).contains(&delay) {
        return Err(bad_request("delay must be an integer 0–300 (seconds)"));
    }
    Ok(delay as i32)
}

/// Split an optional password fragment, accepting `#fragment` or the encoded
/// `%23` form used by the C# parser.
fn split_url_fragment(raw_url: &str) -> (String, Option<String>) {
    let trimmed = raw_url.trim();
    let split_index = trimmed.find('#').or_else(|| find_percent_23(trimmed));
    let Some(index) = split_index else {
        return (trimmed.to_string(), None);
    };
    let url = trimmed[..index].to_string();
    let fragment = &trimmed[index
        + if trimmed[index..].starts_with('%') {
            3
        } else {
            1
        }..];
    (url, Some(decode_fragment_password(fragment)))
}

fn find_percent_23(haystack: &str) -> Option<usize> {
    haystack.find("%23")
}

/// The C# helper base64-decodes the fragment and falls back to the raw text.
fn decode_fragment_password(fragment: &str) -> String {
    if fragment.is_empty() {
        return fragment.to_string();
    }
    let compact: Vec<u8> = fragment
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect();
    match BASE64_STANDARD.decode(compact) {
        Ok(decoded) => String::from_utf8_lossy(&decoded).into_owned(),
        Err(_) => fragment.to_string(),
    }
}

fn ok_response() -> Response {
    (StatusCode::OK, Json(serde_json::json!({ "ok": true }))).into_response()
}

#[derive(Debug)]
struct ApiError {
    status: StatusCode,
    message: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}

fn bad_request(message: &str) -> ApiError {
    ApiError {
        status: StatusCode::BAD_REQUEST,
        message: message.to_owned(),
    }
}

fn not_found(message: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(serde_json::json!({ "error": message })),
    )
        .into_response()
}

fn too_large() -> ApiError {
    ApiError {
        status: StatusCode::PAYLOAD_TOO_LARGE,
        message: "payload too large".into(),
    }
}

fn internal_error() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(serde_json::json!({ "error": "internal server error" })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::{to_bytes, Body};
    use basis_protocol::config::ServerConfig;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    fn config() -> ServerConfig {
        ServerConfig {
            has_file_support: false,
            use_auth: false,
            set_port: 0,
            override_auto_discovery_of_ipv: true,
            ipv4_address: "127.0.0.1".into(),
            api_host: "127.0.0.1".into(),
            api_port: 0,
            api_key: "test-key".into(),
            ..ServerConfig::default()
        }
    }

    async fn fixture() -> (
        ServerState,
        tokio::sync::oneshot::Sender<()>,
        RestApiRuntime,
    ) {
        let (state, shutdown) = ServerState::start(config(), &std::env::temp_dir())
            .await
            .unwrap();
        let runtime = RestApiRuntime {
            state: state.clone(),
            key_hash: Some(Sha256::digest(b"test-key").to_vec()),
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            delayed: Arc::new(tokio::sync::Mutex::new(tokio::task::JoinSet::new())),
        };
        (state, shutdown, runtime)
    }

    async fn request(
        runtime: &RestApiRuntime,
        method: &str,
        path: &str,
        body: &str,
    ) -> (StatusCode, HeaderMap, Value) {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .header(AUTHORIZATION, "Bearer test-key")
            .body(Body::from(body.to_owned()))
            .unwrap();
        let response = registration_router(runtime.clone())
            .oneshot(req)
            .await
            .unwrap();
        let status = response.status();
        let headers = response.headers().clone();
        let bytes = to_bytes(response.into_body(), MAX_BODY_BYTES + 1024)
            .await
            .unwrap();
        (
            status,
            headers,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn authentication_applies_to_routes_errors_and_unknown_paths() {
        let (state, _shutdown, runtime) = fixture().await;
        for path in ["/api/players", "/api/worlds", "/api/unknown", "/"] {
            for token in [
                None,
                Some("Bearer wrong"),
                Some("Basic test-key"),
                Some("Bearer éééé"),
            ] {
                let mut req = Request::builder().uri(path);
                if let Some(token) = token {
                    req = req.header(AUTHORIZATION, token);
                }
                let response = registration_router(runtime.clone())
                    .oneshot(req.body(Body::empty()).unwrap())
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
                assert_eq!(
                    response.headers()[WWW_AUTHENTICATE],
                    "Bearer realm=\"basis-server\""
                );
                assert_eq!(response.headers()[CACHE_CONTROL], "no-store, max-age=0");
                assert_eq!(response.headers()[X_CONTENT_TYPE_OPTIONS], "nosniff");
            }
        }
        assert_eq!(
            request(&runtime, "GET", "/api/players", "").await.2,
            json!({"players": []})
        );
        assert_eq!(
            request(&runtime, "GET", "/api/players/?unused=true", "")
                .await
                .2,
            json!({"players": []})
        );
        let (status, headers, _) = request(&runtime, "HEAD", "/api/players", "").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(headers["allow"], "GET");
        assert_eq!(
            request(&runtime, "GET", "/api/unknown", "").await.0,
            StatusCode::NOT_FOUND
        );
        let (status, headers, _) = request(&runtime, "POST", "/api/players", "").await;
        assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
        assert!(headers.contains_key("allow"));
        let permits = runtime
            .permits
            .acquire_many(MAX_CONCURRENT_REQUESTS as u32)
            .await
            .unwrap();
        assert_eq!(
            request(&runtime, "GET", "/api/players", "").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(permits);
        state.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn worlds_load_list_unload_predownload_and_switch() {
        let (state, _shutdown, runtime) = fixture().await;
        let (_, _, loaded) = request(
            &runtime,
            "POST",
            "/api/worlds",
            r#"{"url":"https://example.com/world#c2VjcmV0","persistent":true}"#,
        )
        .await;
        let id = loaded["netId"].as_str().unwrap();
        assert_eq!(id.len(), 32);
        let resources = state.resources.all_resources();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].unlock_password, "secret");
        assert_eq!(resources[0].uuid_of_creator, "server");
        assert!(resources[0].is_admin_locked);
        let (_, _, listed) = request(&runtime, "GET", "/api/worlds", "").await;
        assert_eq!(
            listed,
            json!({"worlds": [{"netId": id, "url": "https://example.com/world", "persistent":true, "adminLocked":true, "strategy":0}]})
        );
        assert_eq!(
            request(&runtime, "DELETE", &format!("/api/worlds/{id}"), "")
                .await
                .0,
            StatusCode::OK
        );
        assert_eq!(
            request(&runtime, "DELETE", &format!("/api/worlds/{id}"), "")
                .await
                .0,
            StatusCode::NOT_FOUND
        );

        assert_eq!(
            request(
                &runtime,
                "POST",
                "/api/worlds",
                r#"{"url":"https://example.com/cache","password":"pw","strategy":"predownload"}"#
            )
            .await
            .0,
            StatusCode::OK
        );
        assert!(
            state.resources.all_resources().is_empty(),
            "predownload must not replay to late joiners"
        );

        request(
            &runtime,
            "POST",
            "/api/worlds",
            r#"{"url":"https://example.com/old","password":"pw"}"#,
        )
        .await;
        let (status, _, switched) = request(
            &runtime,
            "POST",
            "/api/worlds/switch",
            r#"{"url":"https://example.com/new","password":"pw"}"#,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let resources = state.resources.all_resources();
        assert_eq!(
            resources.len(),
            1,
            "empty-server switch completes and unloads old worlds"
        );
        assert_eq!(resources[0].loaded_net_id, switched["netId"]);
        assert_eq!(resources[0].load_strategy, LOAD_STRATEGY_SYNCHRONIZED);
        assert!(state.resources.remove_preload_peer(99).is_empty());

        let mut prop = build_world_resource("https://example.com/prop", "pw", false, 0);
        prop.mode = 0;
        state.resources.load_resource(prop.clone());
        assert_eq!(
            request(
                &runtime,
                "DELETE",
                &format!("/api/worlds/{}", prop.loaded_net_id),
                ""
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        let (_, _, cleared) = request(&runtime, "DELETE", "/api/worlds", "").await;
        assert_eq!(cleared, json!({"ok":true,"unloaded":1}));
        assert_eq!(state.resources.all_resources(), vec![prop]);
        state.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn validates_bodies_without_panics_or_state_changes() {
        let (state, _shutdown, runtime) = fixture().await;
        for body in [
            "{",
            "null",
            "[]",
            r#"{"url":"x","password":"p"}"#,
            r#"{"url":"é","password":"p"}"#,
            r#"{"url":"http://example.com","password":"p"}"#,
            r#"{"url":"https://" ,"password":"p"}"#,
            r#"{"url":"https://example.com"}"#,
            r#"{"url":"https://example.com","password":3}"#,
            r#"{"url":"https://example.com","password":"p","strategy":1}"#,
        ] {
            assert_eq!(
                request(&runtime, "POST", "/api/worlds", body).await.0,
                StatusCode::BAD_REQUEST,
                "{body}"
            );
        }
        assert!(state.resources.all_resources().is_empty());
        for body in ["", " ", "{}", r#"{"message":5}"#, r#"{"message":""}"#] {
            assert_eq!(
                request(&runtime, "POST", "/api/announce", body).await.0,
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            request(&runtime, "POST", "/api/announce", r#"{"message":"hello"}"#)
                .await
                .2,
            json!({"ok":true})
        );
        assert_eq!(
            request(
                &runtime,
                "POST",
                "/api/announce/missing",
                r#"{"message":"hello"}"#
            )
            .await
            .0,
            StatusCode::NOT_FOUND
        );
        let long = json!({"message": "😀".repeat(257)}).to_string();
        assert_eq!(
            request(&runtime, "POST", "/api/announce", &long).await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            request(
                &runtime,
                "POST",
                "/api/announce",
                &"x".repeat(MAX_BODY_BYTES + 1)
            )
            .await
            .0,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        for body in [
            json!({"delay":301}),
            json!({"delay":-1}),
            json!({"delay":"1"}),
            json!({"announceMessage":1}),
        ] {
            let mut body = body;
            body["url"] = json!("https://example.com");
            body["password"] = json!("pw");
            assert_eq!(
                request(&runtime, "POST", "/api/worlds/switch", &body.to_string())
                    .await
                    .0,
                StatusCode::BAD_REQUEST
            );
        }
        state.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn world_changes_wait_for_prior_commits_and_sends() {
        let (state, _shutdown, _) = fixture().await;
        let old = build_world_resource("https://example.com/old", "pw", false, 0);
        state.resources.load_resource(old.clone());
        let new = build_world_resource("https://example.com/new", "pw", false, 0);
        let guard = state.resource_operations.lock().await;
        let clear = server_clear_worlds(&state);
        let load = server_load_world(&state, new.clone());
        tokio::pin!(clear, load);
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut clear)
            .await
            .is_err());
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut load)
            .await
            .is_err());
        assert_eq!(state.resources.all_resources(), vec![old]);
        drop(guard);
        let (cleared, loaded) = tokio::join!(clear, load);
        assert_eq!(cleared.unwrap(), 1);
        assert!(loaded.unwrap());
        assert_eq!(state.resources.all_resources(), vec![new]);
        state.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn oversized_world_fields_fail_before_announcing_or_scheduling() {
        let (state, _shutdown, runtime) = fixture().await;
        for path in ["/api/worlds", "/api/worlds/switch"] {
            for body in [
                json!({"url":format!("https://example.com/{}", "x".repeat(MAX_WIRE_STRING_BYTES)),"password":"pw", "delay":300}),
                json!({"url":"https://example.com/world", "password":"é".repeat(MAX_WIRE_STRING_BYTES / 2 + 1), "delay":300}),
                json!({"url":format!("https://example.com/world#{}", BASE64_STANDARD.encode(vec![b'x'; MAX_WIRE_STRING_BYTES + 1])), "delay":300}),
            ] {
                assert_eq!(
                    request(&runtime, "POST", path, &body.to_string()).await.0,
                    StatusCode::BAD_REQUEST
                );
            }
        }
        assert!(runtime.delayed.lock().await.is_empty());
        assert!(state.resources.all_resources().is_empty());
        let prefix = "https://example.com/";
        let url = format!(
            "{prefix}{}",
            "x".repeat(MAX_WIRE_STRING_BYTES - prefix.len())
        );
        assert!(extract_url_password(
            &json!({"url":url,"password":"é".repeat(MAX_WIRE_STRING_BYTES / 2)})
        )
        .is_ok());
        state.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn delayed_switch_queue_is_bounded_and_releases_finished_slots() {
        let (state, _shutdown, runtime) = fixture().await;
        let body = r#"{"url":"https://example.com/new","password":"pw","delay":300}"#;
        for _ in 0..MAX_PENDING_SWITCHES {
            assert_eq!(
                request(&runtime, "POST", "/api/worlds/switch", body)
                    .await
                    .0,
                StatusCode::OK
            );
        }
        assert_eq!(runtime.delayed.lock().await.len(), MAX_PENDING_SWITCHES);
        assert_eq!(
            request(&runtime, "POST", "/api/worlds/switch", body)
                .await
                .0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        {
            let mut tasks = runtime.delayed.lock().await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
        assert_eq!(
            request(&runtime, "POST", "/api/worlds/switch", body)
                .await
                .0,
            StatusCode::OK
        );
        {
            let mut tasks = runtime.delayed.lock().await;
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
        assert!(state.resources.all_resources().is_empty());
        state.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn delayed_switch_returns_id_before_loading_then_completes() {
        let (state, _shutdown, runtime) = fixture().await;
        let (status, _, response) = request(&runtime, "POST", "/api/worlds/switch", r#"{"url":"https://example.com/new","password":"pw","persistent":true,"delay":1,"strategy":"ignored-for-switch"}"#).await;
        assert_eq!(status, StatusCode::OK);
        assert!(state.resources.all_resources().is_empty());
        tokio::time::timeout(
            Duration::from_secs(3),
            runtime.delayed.lock().await.join_next(),
        )
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        let resources = state.resources.all_resources();
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].loaded_net_id, response["netId"]);
        assert_eq!(resources[0].load_strategy, LOAD_STRATEGY_SYNCHRONIZED);
        assert!(resources[0].persist);
        state.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn listener_serves_http_and_shutdown_cancels_delayed_switches() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (state, _shutdown, _) = fixture().await;
        let address = start_rest_api_server(state.clone()).await.unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let body = r#"{"url":"https://example.com","password":"pw","delay":300}"#;
        let req = format!("POST /api/worlds/switch HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-key\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut response = String::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{response}");
        assert!(response.contains("\"netId\""));
        let mut slow_client = tokio::net::TcpStream::connect(address).await.unwrap();
        slow_client.write_all(b"POST /api/worlds HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer test-key\r\nContent-Length: 1000\r\n\r\n{").await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), state.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert!(state.resources.all_resources().is_empty());
        let rebound = tokio::net::TcpListener::bind(address).await.unwrap();
        drop(rebound);
    }

    #[tokio::test]
    async fn enabled_api_bind_failure_fails_startup_and_cleans_up() {
        let occupied = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = config();
        config.api_enabled = true;
        config.api_port = occupied.local_addr().unwrap().port();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            ServerState::start(config, &std::env::temp_dir()),
        )
        .await
        .unwrap();
        let error = result.err().expect("API bind conflict must fail startup");
        assert!(
            error.to_string().contains("REST management API"),
            "{error:#}"
        );
    }

    #[test]
    fn fragments_and_strategy_values_match_reference() {
        assert_eq!(
            split_url_fragment(" https://example.com/world#c2VjcmV0 "),
            ("https://example.com/world".into(), Some("secret".into()))
        );
        assert_eq!(
            split_url_fragment("https://example.com/world%23password!"),
            ("https://example.com/world".into(), Some("password!".into()))
        );
        for (name, number) in [("immediate", 0), ("synchronized", 2), ("predownload", 3)] {
            assert_eq!(extract_strategy(&json!({"strategy":name})).unwrap(), number);
            assert_eq!(
                extract_strategy(&json!({"strategy":number})).unwrap(),
                number
            );
        }
        let (_, password, _, _) = extract_world_params(
            &json!({"url":"https://example.com#c2VjcmV0", "password":"override"}),
        )
        .unwrap();
        assert_eq!(password, "override");
    }
}
