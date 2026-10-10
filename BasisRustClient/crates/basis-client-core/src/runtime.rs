use crate::config::{
    resolve_relative_to_config, sanitize_voice_distance, sanitize_voice_frame_duration,
    ClientOptions, Config,
};
use crate::diagnostics::{client_avatar_diagnostic_window, ClientAvatarDiagnostics};
use crate::observer::AvatarObserver;
use crate::observer_session::ObserverSession;
use crate::population::{
    add_clients, disconnect_clients_in_batches, failure_reconnect_loop, random_reconnect_loop,
    wait_for_full_population,
};
use crate::receiver::{shared_maintenance_loop, shared_receive_loop};
use crate::simulation::{movement_workers, CadenceOptions, SpawnLayout};
use crate::transport::{ConnectOptions, MaintenanceOptions};
use crate::voice::{voice_workers, VoiceLibrary};
use crate::voice_diagnostics;
use anyhow::{anyhow, Context, Result};
use basis_protocol::{
    channels,
    io::NetWriter,
    messages::{BasisSerialize, NetIdMessage},
};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time;
use tracing::{error, info, warn};

#[derive(Debug, PartialEq, Eq)]
pub enum ConsoleCommand {
    EnableVoice,
    AddClients(usize),
    Quit {
        batch_size: Option<usize>,
        delay_ms: Option<u64>,
    },
}

struct RunShutdown {
    shutdown: Arc<AtomicBool>,
    signal_task: tokio::task::AbortHandle,
}

impl Drop for RunShutdown {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::SeqCst);
        self.signal_task.abort();
    }
}

/// Run the client population on the caller's Tokio runtime.
///
/// The callback starts the command source after the initial population is connected.
/// A closed command channel disables interactive commands; Ctrl+C and duration limits
/// continue to control shutdown. Logging and runtime creation belong to the caller.
pub async fn run(
    args: ClientOptions,
    worker_threads: usize,
    start_console: impl FnOnce() -> mpsc::UnboundedReceiver<ConsoleCommand>,
) -> Result<()> {
    run_internal(args, worker_threads, start_console, None).await
}

pub async fn run_image_benchmark(
    options: crate::image_benchmark::ImageBenchmarkOptions,
    worker_threads: usize,
) -> Result<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    drop(tx);
    let args = ClientOptions {
        config: options.config_path.clone(),
        ip: Some(options.ip.clone()),
        port: Some(options.port),
        clients: Some(options.clients),
        no_reconnect: true,
        no_movement: true,
        ..ClientOptions::default()
    };
    run_internal(args, worker_threads, || rx, Some(options)).await
}

async fn run_internal(
    args: ClientOptions,
    worker_threads: usize,
    start_console: impl FnOnce() -> mpsc::UnboundedReceiver<ConsoleCommand>,
    image_options: Option<crate::image_benchmark::ImageBenchmarkOptions>,
) -> Result<()> {
    anyhow::ensure!(
        args.unity_frame_rate > 0,
        "Unity frame rate must be positive"
    );
    anyhow::ensure!(
        args.movement_interval_ms > 0,
        "movement interval must be positive"
    );
    let shutdown = Arc::new(AtomicBool::new(false));
    let signal_shutdown = shutdown.clone();
    let (signal_ready_tx, signal_ready_rx) = tokio::sync::oneshot::channel();
    let signal_task = tokio::spawn(async move {
        let result = tokio::select! {
            biased;
            result = tokio::signal::ctrl_c() => result,
            // The first branch registers the listener before this branch signals readiness.
            _ = async {
                let _ = signal_ready_tx.send(());
                std::future::pending::<()>().await;
            } => unreachable!(),
        };
        if let Err(err) = result {
            warn!("failed to listen for Ctrl+C: {err}");
            return;
        }
        info!("shutdown requested");
        signal_shutdown.store(true, Ordering::SeqCst);
    });
    let _run_shutdown = RunShutdown {
        shutdown: shutdown.clone(),
        signal_task: signal_task.abort_handle(),
    };
    let _ = signal_ready_rx.await;
    info!("tokio runtime workers={worker_threads}");
    anyhow::ensure!(
        args.scene_data_bytes == 0 || (24..=1024).contains(&args.scene_data_bytes),
        "scene data bytes must be 0 or 24..=1024"
    );
    anyhow::ensure!(
        args.scene_data_interval_ms > 0,
        "scene interval must be positive"
    );
    let config_path = args.config.clone();
    let mut config = Config::load_or_create(&config_path, args.strict_config)?;
    config.additional_avatar_bytes = args.additional_avatar_bytes;
    config.scene_session = (args.scene_data_bytes > 0).then(|| {
        Arc::new(crate::scene::SceneSession::new(
            args.scene_data_bytes,
            args.scene_start_file.clone(),
        ))
    });
    config.image_benchmark = image_options
        .map(crate::image_benchmark::ImageBenchmarkSession::new)
        .transpose()?;
    if let Some(ip) = args.ip {
        config.ip = ip;
    }
    if let Some(port) = args.port {
        config.port = port;
    }
    if let Some(clients) = args.clients {
        config.client_count = clients;
    }
    if args.voice {
        config.voice_enabled = true;
    }
    if let Some(folder) = args.voice_audio_folder {
        config.voice_audio_folder = folder.to_string_lossy().to_string();
    } else {
        config.voice_audio_folder =
            resolve_relative_to_config(&config_path, &config.voice_audio_folder);
    }
    if let Some(percent) = args.voice_speaker_percent {
        config.voice_speaker_percent = percent.min(100);
    }
    if let Some(distance) = args.voice_hearing_distance {
        config.voice_hearing_distance = sanitize_voice_distance(distance);
    }
    if let Some(frame_duration) = args.voice_frame_duration_ms {
        config.voice_frame_duration_ms = sanitize_voice_frame_duration(frame_duration);
    }
    config.observe_avatar_csv = args.observe_avatar_csv.clone();
    config.avatar_observe_radius = if args.avatar_observe_radius.is_finite() {
        args.avatar_observe_radius.max(0.0)
    } else {
        40.0
    };
    config.avatar_observe_expected_peers = args.avatar_observe_expected_peers;
    config.observe_avatar_start_file = args.observe_avatar_start_file.clone();
    config.observe_avatar_window = Duration::from_secs(args.observe_avatar_window_secs.max(1));
    config.observer_session = config.observe_avatar_csv.as_ref().map(|_| {
        Arc::new(ObserverSession::new(AvatarObserver::new(
            config.avatar_observe_radius,
            config.avatar_observe_expected_peers,
            config.observe_avatar_start_file.clone(),
            config.observe_avatar_window,
        )))
    });
    if args.unity_avatar_policy && !args.no_spread && !args.fixed_spawn_positions {
        return Err(anyhow!(
            "--unity-avatar-policy requires --no-spread or --fixed-spawn-positions"
        ));
    }
    let voice_reencode = !args.no_voice_reencode;
    let cadence = CadenceOptions {
        sync_batching: args.sync_batching,
        additional_avatar_bytes: config.additional_avatar_bytes,
        unity_avatar_policy: args.unity_avatar_policy,
        unity_frame_rate: args.unity_frame_rate,
        unity_pose_amplitude_radians: args.unity_pose_amplitude_degrees.to_radians(),
        movement_interval: Duration::from_millis(args.movement_interval_ms),
        movement_jitter_percent: args.movement_jitter_percent,
        voice_jitter_percent: args.voice_jitter_percent,
        allow_position_drift: !args.no_spread,
    };
    info!(
        "avatar movement cadence: {} ms, jitter {}%, scheduling {}; positional drift {}",
        args.movement_interval_ms,
        args.movement_jitter_percent,
        if args.sync_batching {
            "worker-synchronized"
        } else {
            "per-client randomized"
        },
        if args.no_spread {
            "disabled"
        } else {
            "enabled"
        },
    );
    if args.unity_avatar_policy {
        info!(
            "Unity avatar policy enabled: {} FPS frame gate; interval/deltas from server metadata; colocated synthetic yaw trace",
            args.unity_frame_rate
        );
    }

    let mut voice_library = if config.voice_enabled {
        info!(
            "voice simulation requested: folder={} speaker_percent={} hearing_distance={} frame_duration_ms={} ffmpeg_reencode={}",
            config.voice_audio_folder,
            config.voice_speaker_percent,
            config.voice_hearing_distance,
            config.voice_frame_duration_ms,
            voice_reencode
        );
        match VoiceLibrary::load(
            &config.voice_audio_folder,
            voice_reencode,
            config.voice_frame_duration_ms,
            &shutdown,
        )
        .await
        {
            Ok(Some(library)) => Some(Arc::new(library)),
            Ok(None) => None,
            Err(err) => {
                warn!("failed to load voice audio library: {err:#}");
                None
            }
        }
    } else {
        None
    };
    if shutdown.load(Ordering::Relaxed) {
        return Ok(());
    }

    info!(
        "starting {} clients against {}:{}",
        config.client_count, config.ip, config.port
    );
    let spawn_layout = SpawnLayout::new(args.spawn_group_size, args.spawn_group_spacing)
        .with_no_spread(args.no_spread)
        .with_fixed_positions(args.fixed_spawn_positions);
    if args.no_spread {
        info!("no-spread mode enabled: all client positions remain at the origin");
    }
    if args.spawn_group_size > 0 {
        info!(
            "spawning clients in groups of {} spaced {:.1} units apart; fixed centers={}",
            args.spawn_group_size, args.spawn_group_spacing, args.fixed_spawn_positions
        );
    }

    let shared_maintenance_enabled = std::env::var("BASIS_CLIENT_SHARED_MAINTENANCE")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(true);
    let managed_clients = Arc::new(Mutex::new(Vec::with_capacity(config.client_count)));
    let maintenance_refresh = Arc::new(Notify::new());
    let maintenance = MaintenanceOptions {
        shared: shared_maintenance_enabled,
        refresh: maintenance_refresh.clone(),
    };
    if shared_maintenance_enabled {
        info!("shared client maintenance enabled");
        let maintenance_task = tokio::spawn(shared_maintenance_loop(
            managed_clients.clone(),
            maintenance_refresh.clone(),
            shutdown.clone(),
        ));
        let maintenance_shutdown = shutdown.clone();
        tokio::spawn(async move {
            let result = maintenance_task.await;
            if maintenance_shutdown.load(Ordering::Relaxed) {
                return;
            }
            match result {
                Ok(()) => error!("shared client maintenance worker stopped unexpectedly"),
                Err(err) => error!("shared client maintenance worker failed: {err}"),
            }
            maintenance_shutdown.store(true, Ordering::SeqCst);
        });
    } else {
        info!("shared client maintenance disabled; using per-client maintenance timers");
    }

    let connect = ConnectOptions {
        batch_size: args.connect_batch_size.max(1),
        batch_delay: Duration::from_millis(args.connect_batch_delay_ms),
        timeout: Duration::from_millis(args.connect_timeout_ms),
        shared_maintenance: shared_maintenance_enabled,
    };
    let initial_target = config.client_count;
    let initial_added = add_clients(
        &managed_clients,
        &config,
        initial_target,
        spawn_layout,
        connect,
        &maintenance,
        &shutdown,
    )
    .await?;
    if initial_added != initial_target && !shutdown.load(Ordering::Relaxed) {
        disconnect_clients_in_batches(&managed_clients, connect.batch_size, Duration::ZERO).await;
        return Err(anyhow!(
            "initial client startup stopped after {initial_added}/{initial_target} clients"
        ));
    }

    // Hand authenticated sockets to the shared Linux receiver before failure recovery. This keeps
    // the Tokio runtime free to complete/retry the small number of failed joins instead of asking
    // it to service hundreds of already-connected per-client receive tasks during the recovery
    // window.
    let shared_receive_enabled = std::env::var("BASIS_CLIENT_SHARED_RECEIVE")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(cfg!(target_os = "linux"));
    if shared_receive_enabled && !shutdown.load(Ordering::Relaxed) {
        info!("shared client receive enabled");
        tokio::spawn(shared_receive_loop(
            managed_clients.clone(),
            shutdown.clone(),
        ));
    } else if !shared_receive_enabled {
        info!("shared client receive disabled; using per-client receive tasks");
    }

    let image_task = if let Some(session) = config.image_benchmark.clone() {
        let snapshot = managed_clients.lock().await.clone();
        let ready_deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let mut writer = NetWriter::new();
            NetIdMessage {
                player_id: crate::image_benchmark::IMAGE_MANAGER_IDENTIFIER.to_owned(),
            }
            .serialize(&mut writer)?;
            for client in &snapshot {
                if client.image_network_id.load(Ordering::Acquire) == u16::MAX {
                    client
                        .send_reliable_ordered(channels::NET_ID_ASSIGN, writer.as_slice())
                        .await?;
                }
            }
            let remaining = ready_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                session.wait_ready(Duration::ZERO).await?;
            }
            match session
                .wait_ready(remaining.min(Duration::from_secs(3)))
                .await
            {
                Ok(()) => break,
                Err(error) if Instant::now() < ready_deadline => {
                    warn!("retrying manager network-id assignment for clients still missing an id: {error:#}");
                }
                Err(error) => return Err(error),
            }
        }
        session.set_owner_peers(&snapshot)?;
        let session_for_task = session.clone();
        let clients_for_task = managed_clients.clone();
        let shutdown_for_task = shutdown.clone();
        Some(tokio::spawn(async move {
            let result = crate::image_benchmark::run_workload(
                clients_for_task,
                session_for_task,
                shutdown_for_task.clone(),
            )
            .await;
            if result.is_err() {
                shutdown_for_task.store(true, Ordering::SeqCst);
            }
            result
        }))
    } else {
        None
    };

    if !args.no_reconnect && !shutdown.load(Ordering::Relaxed) {
        tokio::spawn(failure_reconnect_loop(
            managed_clients.clone(),
            config.clone(),
            shutdown.clone(),
            spawn_layout,
            connect.timeout,
            maintenance.clone(),
        ));
        let _ = wait_for_full_population(
            &managed_clients,
            config.client_count,
            &shutdown,
            Duration::from_secs(120),
        )
        .await;
    }

    if !args.no_movement && !shutdown.load(Ordering::Relaxed) {
        movement_workers(managed_clients.clone(), shutdown.clone(), cadence).await;
    }
    let scene_stop = Arc::new(Notify::new());
    let scene_task = config.scene_session.as_ref().map(|session| {
        tokio::spawn(crate::scene::run(
            managed_clients.clone(),
            shutdown.clone(),
            scene_stop.clone(),
            session.clone(),
            Duration::from_millis(args.scene_data_interval_ms),
            args.scene_data_reliable,
        ))
    });
    let avatar_diagnostic_task = if ClientAvatarDiagnostics::enabled_from_env() {
        match (
            config.observe_avatar_start_file.clone(),
            config.observe_avatar_csv.as_ref(),
        ) {
            (Some(marker), Some(observer_csv)) => {
                let output = observer_csv.with_extension("sender.csv");
                Some(tokio::spawn(client_avatar_diagnostic_window(
                    managed_clients.clone(),
                    marker,
                    output,
                    config.observe_avatar_window,
                    shutdown.clone(),
                )))
            }
            _ => {
                warn!("avatar diagnostics enabled without observer marker/output; sender CSV disabled");
                None
            }
        }
    } else {
        None
    };
    let voice_diagnostic_task = match (
        std::env::var_os("BASIS_VOICE_DIAGNOSTIC_CSV"),
        config.observe_avatar_start_file.clone(),
    ) {
        (Some(output), Some(marker)) => Some(tokio::spawn(voice_diagnostics::capture_window(
            managed_clients.clone(),
            marker,
            PathBuf::from(output),
            config.observe_avatar_window,
            shutdown.clone(),
        ))),
        _ => None,
    };
    let mut voice_running = false;
    if let Some(voice_library) = voice_library.take() {
        if !shutdown.load(Ordering::Relaxed) {
            info!(
                "voice simulation enabled: folder={} speaker_percent={} hearing_distance={} frame_duration_ms={} ffmpeg_reencode={}",
                config.voice_audio_folder,
                config.voice_speaker_percent,
                config.voice_hearing_distance,
                config.voice_frame_duration_ms,
                voice_reencode
            );
            tokio::spawn(voice_workers(
                managed_clients.clone(),
                config.clone(),
                voice_library,
                shutdown.clone(),
                cadence,
            ));
            voice_running = true;
        }
    }
    if !args.no_reconnect && !shutdown.load(Ordering::Relaxed) {
        tokio::spawn(random_reconnect_loop(
            managed_clients.clone(),
            config.clone(),
            shutdown.clone(),
            spawn_layout,
            args.reconnect_min_secs,
            args.reconnect_max_secs,
            maintenance.clone(),
        ));
    }

    let mut commands_rx = start_console();
    let mut console_closed = false;
    let mut quit_batch_size = args.quit_batch_size.max(1);
    let mut quit_batch_delay = Duration::from_millis(args.quit_batch_delay_ms);
    let duration_deadline = args
        .duration_secs
        .map(|duration| time::Instant::now() + Duration::from_secs(duration));

    while !shutdown.load(Ordering::Relaxed) {
        if duration_deadline
            .map(|deadline| time::Instant::now() >= deadline)
            .unwrap_or(false)
        {
            shutdown.store(true, Ordering::SeqCst);
            break;
        }

        tokio::select! {
            command = commands_rx.recv(), if !console_closed => {
                match command {
                    Some(ConsoleCommand::EnableVoice) => {
                        if voice_running {
                            info!("voice simulation is already enabled");
                        } else {
                            config.voice_enabled = true;
                            match VoiceLibrary::load(
                                &config.voice_audio_folder,
                                voice_reencode,
                                config.voice_frame_duration_ms,
                                &shutdown,
                            ).await {
                                Ok(Some(library)) => {
                                    info!("voice simulation enabled from console");
                                    tokio::spawn(voice_workers(
                                        managed_clients.clone(),
                                        config.clone(),
                                        Arc::new(library),
                                        shutdown.clone(),
                                        cadence,
                                    ));
                                    voice_running = true;
                                }
                                Ok(None) if shutdown.load(Ordering::Relaxed) => {},
                                Ok(None) => warn!("voice command ignored: no usable audio files found"),
                                Err(err) => warn!("voice command failed: {err:#}"),
                            }
                        }
                    }
                    Some(ConsoleCommand::AddClients(count)) => {
                        info!("adding {count} clients from console");
                        match add_clients(
                            &managed_clients,
                            &config,
                            count,
                            spawn_layout,
                            connect,
                            &maintenance,
                            &shutdown,
                        ).await {
                            Ok(added) => {
                                config.client_count = config.client_count.saturating_add(added);
                                info!("added {added} clients; population target is now {}", config.client_count);
                            }
                            Err(err) => {
                                config.client_count = managed_clients.lock().await.len();
                                warn!(
                                    "failed to add clients: {err:#}; population target is {}",
                                    config.client_count
                                );
                            }
                        }
                    }
                    Some(ConsoleCommand::Quit { batch_size, delay_ms }) => {
                        if let Some(batch_size) = batch_size {
                            quit_batch_size = batch_size.max(1);
                        }
                        if let Some(delay_ms) = delay_ms {
                            quit_batch_delay = Duration::from_millis(delay_ms);
                        }
                        info!(
                            "shutdown requested from console (batch_size={} delay_ms={})",
                            quit_batch_size,
                            quit_batch_delay.as_millis()
                        );
                        shutdown.store(true, Ordering::SeqCst);
                    }
                    None => console_closed = true,
                }
            }
            _ = time::sleep(Duration::from_millis(100)) => {}
        }
    }
    shutdown.store(true, Ordering::SeqCst);
    scene_stop.notify_one();
    if let Some(task) = voice_diagnostic_task {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!("voice diagnostic window failed: {error:#}"),
            Err(error) => warn!("voice diagnostic task failed: {error}"),
        }
    }
    // Preserve diagnostic failures, but always disconnect the population before returning.
    let mut shutdown_result = match scene_task {
        Some(task) => task.await.context("scene workload worker failed"),
        None => Ok(()),
    };
    if let Some(task) = image_task {
        let image_result = task
            .await
            .context("image benchmark worker failed")
            .and_then(|result| result);
        shutdown_result = shutdown_result.and(image_result);
    }
    if let (Some(path), Some(session)) = (&args.observe_scene_csv, &config.scene_session) {
        shutdown_result = shutdown_result.and(session.write_csv(path));
    }
    if let Some(task) = avatar_diagnostic_task {
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => warn!("avatar sender diagnostic window failed: {error:#}"),
            Err(error) => warn!("avatar sender diagnostic task failed: {error}"),
        }
    }
    if let (Some(path), Some(session)) = (&config.observe_avatar_csv, &config.observer_session) {
        let csv_result = (|| -> Result<()> {
            if let Some(state) = session.lock() {
                let (summary, csv) = state.observer.summary_and_csv(std::time::Instant::now());
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(path, csv)
                    .with_context(|| format!("writing avatar observer CSV {}", path.display()))?;
                info!("{summary}; csv={}", path.display());
            } else {
                warn!("avatar observer measurement unavailable; no CSV written");
            }
            Ok(())
        })();
        shutdown_result = shutdown_result.and(csv_result);
    }
    info!(
        "shutting down clients in batches of {} ({}ms between batches)",
        quit_batch_size,
        quit_batch_delay.as_millis()
    );
    disconnect_clients_in_batches(&managed_clients, quit_batch_size, quit_batch_delay).await;
    shutdown_result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn run_shutdown_stops_maintenance_and_signal_listener_on_success_and_error() {
        fn finish_run(guard: RunShutdown, fail: bool) -> Result<()> {
            let _guard = guard;
            if fail {
                return Err(anyhow!("startup failed"));
            }
            Ok(())
        }

        for fail in [false, true] {
            let shutdown = Arc::new(AtomicBool::new(false));
            let signal_task = tokio::spawn(std::future::pending::<()>());
            let maintenance = tokio::spawn(shared_maintenance_loop(
                Arc::new(Mutex::new(Vec::new())),
                Arc::new(Notify::new()),
                shutdown.clone(),
            ));
            let guard = RunShutdown {
                shutdown: shutdown.clone(),
                signal_task: signal_task.abort_handle(),
            };

            assert_eq!(finish_run(guard, fail).is_err(), fail);
            assert!(shutdown.load(Ordering::SeqCst));
            assert!(signal_task.await.unwrap_err().is_cancelled());
            time::timeout(Duration::from_secs(1), maintenance)
                .await
                .expect("shared maintenance must stop when run exits")
                .unwrap();
        }
    }
}
