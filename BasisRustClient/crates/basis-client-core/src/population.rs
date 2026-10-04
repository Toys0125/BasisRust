use crate::client::BasisClient;
use crate::config::Config;
use crate::simulation::SpawnLayout;
use crate::transport::{ConnectOptions, MaintenanceOptions, INITIAL_START_ATTEMPTS};
use crate::wire::ready_message;
use anyhow::{anyhow, Context, Result};
use rand::Rng;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time;
use tracing::{info, warn};

pub(crate) async fn publish_client_batch(
    managed_clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    batch_start: usize,
    batch_clients: &[Arc<BasisClient>],
    maintenance: &MaintenanceOptions,
) -> Result<()> {
    let mut managed = managed_clients.lock().await;
    if managed.len() != batch_start {
        return Err(anyhow!(
            "client batch {batch_start} published at dense index {}",
            managed.len()
        ));
    }
    if batch_clients
        .iter()
        .enumerate()
        .any(|(offset, client)| client.index != batch_start + offset)
    {
        return Err(anyhow!(
            "client batch {batch_start} contains a non-dense index"
        ));
    }
    managed.extend(batch_clients.iter().cloned());
    drop(managed);
    if maintenance.shared {
        maintenance.refresh.notify_one();
    }
    Ok(())
}

pub(crate) async fn replace_client_if_current(
    clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    index: usize,
    old: &Arc<BasisClient>,
    replacement: &Arc<BasisClient>,
    maintenance: &MaintenanceOptions,
) -> bool {
    let replaced = {
        let mut managed = clients.lock().await;
        match managed.get(index) {
            Some(current) if Arc::ptr_eq(current, old) => {
                managed[index] = replacement.clone();
                true
            }
            _ => false,
        }
    };
    if replaced {
        old.deactivate();
        if maintenance.shared {
            maintenance.refresh.notify_one();
        }
    }
    replaced
}

pub(crate) async fn start_client_with_retries(
    index: usize,
    config: &Config,
    spawn_base: [f32; 3],
    shared_maintenance_enabled: bool,
) -> Result<Arc<BasisClient>> {
    let mut last_error = None;
    for attempt in 1..=INITIAL_START_ATTEMPTS {
        let ready = match ready_message(config, spawn_base) {
            Ok(ready) => ready,
            Err(err) => {
                last_error = Some(err);
                break;
            }
        };
        match BasisClient::start(index, config, ready, spawn_base, shared_maintenance_enabled).await
        {
            Ok(client) => return Ok(client),
            Err(err) => {
                warn!(
                    "failed to start client {index} (attempt {attempt}/{INITIAL_START_ATTEMPTS}): {err}"
                );
                last_error = Some(err);
            }
        }
    }

    Err(anyhow!(
        "failed to start client {index} after {INITIAL_START_ATTEMPTS} attempts: {}",
        last_error
            .map(|err| err.to_string())
            .unwrap_or_else(|| "unknown error".to_string())
    ))
}

pub(crate) async fn failure_reconnect_loop(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    config: Config,
    shutdown: Arc<AtomicBool>,
    spawn_layout: SpawnLayout,
    connect_timeout: Duration,
    maintenance: MaintenanceOptions,
) {
    let mut pending_since = HashMap::<usize, time::Instant>::new();
    let mut tick = time::interval(Duration::from_millis(250));

    loop {
        tick.tick().await;
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let snapshot = clients.lock().await.clone();
        for (idx, client) in snapshot.into_iter().enumerate() {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            if client.connected.load(Ordering::Relaxed) {
                pending_since.remove(&idx);
                continue;
            }
            if client.intentional_reconnect.load(Ordering::Relaxed) {
                pending_since.remove(&idx);
                continue;
            }

            if client.in_use.load(Ordering::Relaxed) {
                let started = pending_since.entry(idx).or_insert_with(time::Instant::now);
                if started.elapsed() < connect_timeout {
                    continue;
                }
                client.deactivate();
                pending_since.remove(&idx);
                warn!("client {idx} reconnect attempt timed out; recycling connection");
            }

            let spawn_base = spawn_layout.base_for_client(idx);
            let result = match ready_message(&config, spawn_base) {
                Ok(ready) => {
                    BasisClient::start(idx, &config, ready, spawn_base, maintenance.shared).await
                }
                Err(err) => Err(err),
            };
            match result {
                Ok(new_client) => {
                    if replace_client_if_current(&clients, idx, &client, &new_client, &maintenance)
                        .await
                    {
                        pending_since.insert(idx, time::Instant::now());
                        info!("failure reconnect started for client {idx}");
                    } else {
                        warn!("discarding stale failure reconnect for client {idx}");
                        new_client.disconnect().await;
                    }
                }
                Err(err) => warn!("failed to restart client {idx}: {err}"),
            }
        }
    }
}

pub(crate) async fn wait_for_full_population(
    clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    expected_population: usize,
    shutdown: &AtomicBool,
    timeout: Duration,
) -> usize {
    let deadline = time::Instant::now() + timeout;
    loop {
        let snapshot = clients.lock().await.clone();
        let connected = snapshot
            .iter()
            .filter(|client| client.connected.load(Ordering::Relaxed))
            .count();
        if (snapshot.len() == expected_population && connected == expected_population)
            || shutdown.load(Ordering::Relaxed)
        {
            info!(
                "current connected population {}/{}",
                connected, expected_population
            );
            return connected;
        }
        if time::Instant::now() >= deadline {
            warn!(
                "timed out waiting for full population: {}/{} currently connected",
                connected, expected_population
            );
            return connected;
        }
        time::sleep(Duration::from_millis(100)).await;
    }
}

pub(crate) async fn random_reconnect_loop(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    config: Config,
    shutdown: Arc<AtomicBool>,
    spawn_layout: SpawnLayout,
    reconnect_min_secs: u64,
    reconnect_max_secs: u64,
    maintenance: MaintenanceOptions,
) {
    let min_secs = reconnect_min_secs.max(1);
    let max_secs = reconnect_max_secs.max(min_secs);
    loop {
        let delay_secs = rand::thread_rng().gen_range(min_secs..=max_secs);
        time::sleep(Duration::from_secs(delay_secs)).await;
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let len = clients.lock().await.len();
        if len == 0 {
            continue;
        }
        let idx = rand::thread_rng().gen_range(0..len);
        let old = { clients.lock().await[idx].clone() };
        old.intentional_reconnect.store(true, Ordering::Relaxed);
        old.disconnect().await;
        time::sleep(Duration::from_secs(3)).await;
        let spawn_base = spawn_layout.base_for_client(idx);
        let result = match ready_message(&config, spawn_base) {
            Ok(ready) => {
                BasisClient::start(idx, &config, ready, spawn_base, maintenance.shared).await
            }
            Err(err) => Err(err),
        };
        match result {
            Ok(new_client) => {
                if replace_client_if_current(&clients, idx, &old, &new_client, &maintenance).await {
                    info!("reconnected client {idx}");
                } else {
                    warn!("discarding stale reconnect for client {idx}");
                    new_client.disconnect().await;
                }
            }
            Err(err) => {
                old.intentional_reconnect.store(false, Ordering::Relaxed);
                warn!("failed to reconnect client {idx}: {err}");
            }
        }
    }
}

pub(crate) async fn sleep_or_shutdown(duration: Duration, shutdown: &AtomicBool) {
    let deadline = time::Instant::now() + duration;
    loop {
        if shutdown.load(Ordering::Relaxed) || time::Instant::now() >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(time::Instant::now());
        time::sleep(remaining.min(Duration::from_millis(50))).await;
    }
}

pub(crate) async fn wait_for_batch_connected(
    clients: &[Arc<BasisClient>],
    timeout: Duration,
    shutdown: &AtomicBool,
) -> usize {
    let deadline = time::Instant::now() + timeout;
    loop {
        let connected = clients
            .iter()
            .filter(|client| client.connected.load(Ordering::Relaxed))
            .count();
        if connected == clients.len()
            || shutdown.load(Ordering::Relaxed)
            || time::Instant::now() >= deadline
        {
            return connected;
        }
        time::sleep(Duration::from_millis(10)).await;
    }
}

pub(crate) async fn add_clients(
    managed_clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    config: &Config,
    count: usize,
    spawn_layout: SpawnLayout,
    connect: ConnectOptions,
    maintenance: &MaintenanceOptions,
    shutdown: &AtomicBool,
) -> Result<usize> {
    let start_index = managed_clients.lock().await.len();
    let target = start_index
        .checked_add(count)
        .ok_or_else(|| anyhow!("client count overflow while adding {count} clients"))?;
    let connect_batch_size = connect.batch_size.max(1);
    let mut index = start_index;
    while index < target {
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        let batch_end = (index + connect_batch_size).min(target);
        let batch_start = index;
        let mut batch_clients = Vec::with_capacity(batch_end - batch_start);
        for client_index in batch_start..batch_end {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            let spawn_base = spawn_layout.base_for_client(client_index);
            match start_client_with_retries(
                client_index,
                config,
                spawn_base,
                connect.shared_maintenance,
            )
            .await
            {
                Ok(client) => batch_clients.push(client),
                Err(err) => {
                    for client in batch_clients {
                        client.disconnect().await;
                    }
                    let added = index.saturating_sub(start_index);
                    if added > 0 {
                        warn!(
                            "stopped adding clients after {added}/{count}: client {client_index} failed: {err:#}"
                        );
                        return Ok(added);
                    }
                    return Err(err)
                        .with_context(|| format!("failed adding client {client_index}"));
                }
            }
        }
        if shutdown.load(Ordering::Relaxed) {
            for client in batch_clients {
                client.disconnect().await;
            }
            break;
        }
        if batch_clients.len() != batch_end - batch_start {
            for client in batch_clients {
                client.disconnect().await;
            }
            return Err(anyhow!(
                "client batch {batch_start}-{end} did not produce a complete dense population",
                end = batch_end.saturating_sub(1)
            ));
        }
        if let Err(err) =
            publish_client_batch(managed_clients, batch_start, &batch_clients, maintenance).await
        {
            for client in batch_clients {
                client.disconnect().await;
            }
            return Err(err);
        }
        let connected_in_batch =
            wait_for_batch_connected(&batch_clients, connect.timeout, shutdown).await;
        if connected_in_batch < batch_clients.len() {
            for client in &batch_clients {
                if !client.connected.load(Ordering::Relaxed) {
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    client.deactivate();
                    warn!(
                        "client {} did not connect within {}ms",
                        client.index,
                        connect.timeout.as_millis()
                    );
                }
            }
        }
        info!(
            "connection batch {}-{} accepted {}/{} clients",
            batch_start,
            batch_end.saturating_sub(1),
            connected_in_batch,
            batch_clients.len()
        );
        index = batch_end;
        if index < target && !connect.batch_delay.is_zero() {
            sleep_or_shutdown(connect.batch_delay, shutdown).await;
        }
    }
    Ok(index.saturating_sub(start_index))
}

pub(crate) async fn disconnect_clients_in_batches(
    clients: &Arc<Mutex<Vec<Arc<BasisClient>>>>,
    batch_size: usize,
    delay: Duration,
) {
    let snapshot = clients.lock().await.clone();
    let batch_size = batch_size.max(1);
    for (batch_number, batch) in snapshot.chunks(batch_size).enumerate() {
        info!(
            "disconnecting client batch {} ({}/{} clients)",
            batch_number + 1,
            batch.len(),
            snapshot.len()
        );
        for client in batch {
            client.disconnect().await;
        }
        if batch_number + 1 < snapshot.len().div_ceil(batch_size) && !delay.is_zero() {
            time::sleep(delay).await;
        }
    }
}
