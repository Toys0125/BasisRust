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
    scene_session: Option<&crate::scene::SceneSession>,
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
    if let Some(session) = scene_session {
        // Sender indices are dense client slots. Grow observer capacity before
        // the published population becomes visible to the receive loop.
        session.update_sender_capacity(managed.len());
    }
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
    let mut reconnect_state = FailureReconnectState::default();
    let mut tick = time::interval(Duration::from_millis(250));

    loop {
        tokio::select! {
            _ = tick.tick() => {},
            _ = wait_for_shutdown(&shutdown) => break,
        }
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        let snapshot = clients.lock().await.clone();
        for (idx, client) in snapshot.into_iter().enumerate() {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }

            if client.connected.load(Ordering::Relaxed) {
                reconnect_state.connected(idx);
                continue;
            }
            if client.intentional_reconnect.load(Ordering::Relaxed) {
                reconnect_state.cancel_pending(idx);
                continue;
            }

            match reconnect_state.observe(
                idx,
                client.in_use.load(Ordering::Relaxed),
                time::Instant::now(),
                connect_timeout,
            ) {
                ReconnectProgress::Connecting | ReconnectProgress::Backoff => continue,
                ReconnectProgress::Failed { delay, timed_out } => {
                    if timed_out {
                        client.deactivate();
                        warn!("client {idx} reconnect attempt timed out; recycling connection");
                    } else {
                        warn!("client {idx} reconnect attempt ended before authentication");
                    }
                    warn!("client {idx} will retry in {}s", delay.as_secs());
                    continue;
                }
                ReconnectProgress::Ready => {}
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
                        reconnect_state.attempt_started(idx, time::Instant::now());
                        info!("failure reconnect started for client {idx}");
                    } else {
                        warn!("discarding stale failure reconnect for client {idx}");
                        new_client.disconnect().await;
                    }
                }
                Err(err) => {
                    let delay = reconnect_state.attempt_failed(idx, time::Instant::now());
                    warn!(
                        "failed to restart client {idx}; retrying in {}s: {err}",
                        delay.as_secs()
                    );
                }
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
    // Avoid overflowing Tokio's sleep deadline or leaving the client in a long, uninterruptible
    // wait when callers provide an accidental extreme value.
    let max_secs = reconnect_max_secs.max(min_secs).min(3600);
    loop {
        let delay_secs = rand::thread_rng().gen_range(min_secs.min(max_secs)..=max_secs);
        sleep_or_shutdown(Duration::from_secs(delay_secs), &shutdown).await;
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
        sleep_or_shutdown(Duration::from_secs(3), &shutdown).await;
        if shutdown.load(Ordering::Relaxed) {
            break;
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

async fn wait_for_shutdown(shutdown: &AtomicBool) {
    while !shutdown.load(Ordering::Relaxed) {
        time::sleep(Duration::from_millis(25)).await;
    }
}

#[derive(Default)]
struct FailureReconnectState {
    pending_since: HashMap<usize, time::Instant>,
    retry_after: HashMap<usize, (u32, time::Instant)>,
}

enum ReconnectProgress {
    Connecting,
    Backoff,
    Failed { delay: Duration, timed_out: bool },
    Ready,
}

impl FailureReconnectState {
    fn connected(&mut self, index: usize) {
        self.pending_since.remove(&index);
        self.retry_after.remove(&index);
    }

    fn cancel_pending(&mut self, index: usize) {
        self.pending_since.remove(&index);
    }

    fn attempt_started(&mut self, index: usize, now: time::Instant) {
        self.pending_since.insert(index, now);
    }

    fn attempt_failed(&mut self, index: usize, now: time::Instant) -> Duration {
        self.pending_since.remove(&index);
        let failures = self
            .retry_after
            .get(&index)
            .map_or(1, |(failures, _)| failures.saturating_add(1));
        let delay = reconnect_retry_delay(failures);
        self.retry_after.insert(index, (failures, now + delay));
        delay
    }

    fn observe(
        &mut self,
        index: usize,
        in_use: bool,
        now: time::Instant,
        connect_timeout: Duration,
    ) -> ReconnectProgress {
        if let Some(started) = self.pending_since.get(&index).copied() {
            if in_use && now.saturating_duration_since(started) < connect_timeout {
                return ReconnectProgress::Connecting;
            }
            let timed_out = in_use;
            let delay = self.attempt_failed(index, now);
            return ReconnectProgress::Failed { delay, timed_out };
        }

        if in_use {
            // This covers clients that were already connecting when reliability monitoring began.
            self.pending_since.insert(index, now);
            return ReconnectProgress::Connecting;
        }

        if self
            .retry_after
            .get(&index)
            .is_some_and(|(_, deadline)| now < *deadline)
        {
            return ReconnectProgress::Backoff;
        }
        ReconnectProgress::Ready
    }
}

fn reconnect_retry_delay(failures: u32) -> Duration {
    Duration::from_secs(1u64 << failures.saturating_sub(1).min(6))
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
        if let Err(err) = publish_client_batch(
            managed_clients,
            batch_start,
            &batch_clients,
            maintenance,
            config.scene_session.as_deref(),
        )
        .await
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

#[cfg(test)]
mod tests {
    use super::{reconnect_retry_delay, FailureReconnectState, ReconnectProgress};
    use std::time::Duration;
    use tokio::time::Instant;

    #[test]
    fn reconnect_retry_delay_grows_and_stays_bounded() {
        assert_eq!(reconnect_retry_delay(1), Duration::from_secs(1));
        assert_eq!(reconnect_retry_delay(2), Duration::from_secs(2));
        assert_eq!(reconnect_retry_delay(3), Duration::from_secs(4));
        assert_eq!(reconnect_retry_delay(7), Duration::from_secs(64));
        assert_eq!(reconnect_retry_delay(u32::MAX), Duration::from_secs(64));
    }

    #[test]
    fn rejected_login_and_timeout_advance_backoff_once_with_spacing() {
        let mut state = FailureReconnectState::default();
        let start = Instant::now();
        let connect_timeout = Duration::from_secs(5);

        // BasisClient::start succeeded and installed a client, but login rejection deactivated it.
        state.attempt_started(3, start);
        let rejected_at = start + Duration::from_millis(100);
        assert!(matches!(
            state.observe(3, false, rejected_at, connect_timeout),
            ReconnectProgress::Failed { delay, timed_out: false }
                if delay == Duration::from_secs(1)
        ));
        assert!(matches!(
            state.observe(
                3,
                false,
                rejected_at + Duration::from_millis(250),
                connect_timeout
            ),
            ReconnectProgress::Backoff
        ));
        let retry_at = rejected_at + Duration::from_secs(1);
        assert!(matches!(
            state.observe(3, false, retry_at, connect_timeout),
            ReconnectProgress::Ready
        ));

        // A second accepted start that never authenticates times out and advances only once.
        state.attempt_started(3, retry_at);
        let timeout_at = retry_at + connect_timeout;
        assert!(matches!(
            state.observe(3, true, timeout_at, connect_timeout),
            ReconnectProgress::Failed { delay, timed_out: true }
                if delay == Duration::from_secs(2)
        ));
        assert!(matches!(
            state.observe(
                3,
                false,
                timeout_at + Duration::from_millis(250),
                connect_timeout
            ),
            ReconnectProgress::Backoff
        ));
        assert!(matches!(
            state.observe(
                3,
                false,
                timeout_at + Duration::from_secs(2),
                connect_timeout
            ),
            ReconnectProgress::Ready
        ));
    }

    #[test]
    fn only_observed_connection_resets_retry_backoff() {
        let mut state = FailureReconnectState::default();
        let start = Instant::now();
        state.attempt_started(1, start);
        assert!(matches!(
            state.observe(1, false, start + Duration::from_millis(10), Duration::from_secs(5)),
            ReconnectProgress::Failed { delay, .. } if delay == Duration::from_secs(1)
        ));

        // Merely starting another asynchronous client does not reset the failure history.
        let second_start = start + Duration::from_secs(2);
        state.attempt_started(1, second_start);
        assert!(matches!(
            state.observe(1, false, second_start + Duration::from_millis(10), Duration::from_secs(5)),
            ReconnectProgress::Failed { delay, .. } if delay == Duration::from_secs(2)
        ));

        state.connected(1);
        let third_start = start + Duration::from_secs(5);
        state.attempt_started(1, third_start);
        assert!(matches!(
            state.observe(1, false, third_start + Duration::from_millis(10), Duration::from_secs(5)),
            ReconnectProgress::Failed { delay, .. } if delay == Duration::from_secs(1)
        ));
    }
}
