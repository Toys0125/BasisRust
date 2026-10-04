use crate::client::BasisClient;
use basis_protocol::channels;
use rand::Rng;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::Mutex;
use tokio::time;
use tracing::trace;

#[derive(Debug, Clone, Copy)]
pub(crate) struct SpawnLayout {
    pub(crate) group_size: usize,
    pub(crate) group_spacing: f32,
    pub(crate) no_spread: bool,
    pub(crate) fixed_positions: bool,
}

impl SpawnLayout {
    pub(crate) fn disabled() -> Self {
        Self {
            group_size: 0,
            group_spacing: 0.0,
            no_spread: false,
            fixed_positions: false,
        }
    }

    pub(crate) fn new(group_size: usize, group_spacing: f32) -> Self {
        if group_size == 0 {
            Self::disabled()
        } else {
            Self {
                group_size,
                group_spacing,
                no_spread: false,
                fixed_positions: false,
            }
        }
    }

    pub(crate) fn with_no_spread(mut self, no_spread: bool) -> Self {
        self.no_spread = no_spread;
        self
    }

    pub(crate) fn with_fixed_positions(mut self, fixed_positions: bool) -> Self {
        self.fixed_positions = fixed_positions;
        self
    }

    pub(crate) fn base_for_client(self, index: usize) -> [f32; 3] {
        if self.no_spread {
            return [0.0; 3];
        }
        let group_offset = index
            .checked_div(self.group_size)
            .map(|group| group as f32 * self.group_spacing)
            .unwrap_or(0.0);
        if self.fixed_positions {
            return [group_offset, 0.0, 0.0];
        }
        let mut rng = rand::thread_rng();
        [
            group_offset + rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
        ]
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct CadenceOptions {
    pub(crate) sync_batching: bool,
    pub(crate) unity_avatar_policy: bool,
    pub(crate) unity_frame_rate: u32,
    pub(crate) unity_pose_amplitude_radians: f32,
    pub(crate) movement_interval: Duration,
    pub(crate) movement_jitter_percent: u8,
    pub(crate) voice_jitter_percent: u8,
    pub(crate) allow_position_drift: bool,
}

pub(crate) fn worker_phase_offset(
    interval: Duration,
    worker: usize,
    worker_count: usize,
) -> Duration {
    let worker_count = worker_count.max(1) as u128;
    let micros = interval.as_micros().saturating_mul(worker as u128) / worker_count;
    Duration::from_micros(micros.min(u64::MAX as u128) as u64)
}

pub(crate) fn cadence_seed(index: usize, stream: u64) -> u64 {
    let mut x = (index as u64)
        .wrapping_add(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(stream.rotate_left(17));
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

pub(crate) fn cadence_next(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

pub(crate) fn jittered_duration(base: Duration, jitter_percent: u8, state: &mut u64) -> Duration {
    let base_us = base.as_micros().max(1) as u64;
    let jitter = jitter_percent.min(95) as u64;
    if jitter == 0 {
        return Duration::from_micros(base_us);
    }
    let max_delta = base_us.saturating_mul(jitter) / 100;
    let span = max_delta.saturating_mul(2).saturating_add(1);
    let offset = cadence_next(state) % span;
    Duration::from_micros(
        base_us
            .saturating_sub(max_delta)
            .saturating_add(offset)
            .max(1),
    )
}

pub(crate) async fn movement_workers(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    shutdown: Arc<AtomicBool>,
    cadence: CadenceOptions,
) {
    let initial_snapshot = clients.lock().await.clone();
    let initial_len = initial_snapshot.len();
    let worker_count = num_cpus::get().max(1).min(initial_len.max(1));
    let start = SystemTime::now();
    for worker in 0..worker_count {
        let clients = clients.clone();
        let shutdown = shutdown.clone();
        let mut snapshot = initial_snapshot.clone();
        tokio::spawn(async move {
            if cadence.unity_avatar_policy {
                let frame_interval =
                    Duration::from_secs_f64(1.0 / cadence.unity_frame_rate.max(1) as f64);
                let mut ticker =
                    time::interval_at(time::Instant::now() + frame_interval, frame_interval);
                ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
                let mut previous_frame = time::Instant::now();
                let mut refresh_ticks = 0u8;
                loop {
                    ticker.tick().await;
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    let now = time::Instant::now();
                    let frame_delta = now.saturating_duration_since(previous_frame).as_secs_f64();
                    previous_frame = now;
                    refresh_ticks = refresh_ticks.wrapping_add(1);
                    if refresh_ticks >= cadence.unity_frame_rate.min(255) as u8 {
                        snapshot = clients.lock().await.clone();
                        refresh_ticks = 0;
                    }
                    let elapsed = start.elapsed().unwrap_or_default().as_secs_f64();
                    let mut idx = worker;
                    while idx < snapshot.len() {
                        let client = &snapshot[idx];
                        if client.connected.load(Ordering::Relaxed) {
                            if let Some(diagnostics) = &client.avatar_diagnostics {
                                diagnostics
                                    .movement_frame_visits
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                            let metadata = *client.metadata_state();
                            if let Some(metadata) = metadata {
                                let force = client.force_avatar_keyframe.load(Ordering::Acquire);
                                let mut pose = client.pose.lock().await;
                                if let Some(datagram) = pose.write_unity_avatar_datagram(
                                    frame_delta,
                                    elapsed,
                                    metadata,
                                    force,
                                    cadence.unity_pose_amplitude_radians,
                                ) {
                                    let channel = datagram.get(1).copied().unwrap_or_default();
                                    let sequence = match channel {
                                        channels::PLAYER_AVATAR_HIGH => datagram.get(2).copied(),
                                        channels::DELTA_AVATAR => datagram.get(3).copied(),
                                        _ => None,
                                    };
                                    if let (Some(diagnostics), Some(sequence)) =
                                        (&client.avatar_diagnostics, sequence)
                                    {
                                        diagnostics
                                            .last_sequence
                                            .store(sequence, Ordering::Relaxed);
                                        if channel == channels::DELTA_AVATAR {
                                            diagnostics
                                                .generated_delta
                                                .fetch_add(1, Ordering::Relaxed);
                                        } else {
                                            diagnostics
                                                .generated_full
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                    if force
                                        && datagram.get(1) == Some(&channels::PLAYER_AVATAR_HIGH)
                                    {
                                        client
                                            .force_avatar_keyframe
                                            .store(false, Ordering::Release);
                                    }
                                    match client.send_connected(datagram).await {
                                        Ok(()) => {
                                            if let Some(diagnostics) = &client.avatar_diagnostics {
                                                if channel == channels::DELTA_AVATAR {
                                                    diagnostics
                                                        .socket_sent_delta
                                                        .fetch_add(1, Ordering::Relaxed);
                                                } else {
                                                    diagnostics
                                                        .socket_sent_full
                                                        .fetch_add(1, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                        Err(err) => {
                                            if let Some(diagnostics) = &client.avatar_diagnostics {
                                                diagnostics
                                                    .send_errors
                                                    .fetch_add(1, Ordering::Relaxed);
                                            }
                                            trace!(
                                                "Unity-policy avatar send failed for {}: {err}",
                                                client.index
                                            );
                                        }
                                    }
                                }
                            }
                        }
                        idx += worker_count;
                    }
                }
                return;
            }

            if cadence.sync_batching {
                time::sleep(worker_phase_offset(
                    cadence.movement_interval,
                    worker,
                    worker_count,
                ))
                .await;
                let mut ticker = time::interval(cadence.movement_interval);
                ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
                let mut refresh_ticks = 0u8;
                loop {
                    ticker.tick().await;
                    if shutdown.load(Ordering::Relaxed) {
                        break;
                    }
                    refresh_ticks = refresh_ticks.wrapping_add(1);
                    if refresh_ticks >= 10 {
                        snapshot = clients.lock().await.clone();
                        refresh_ticks = 0;
                    }
                    let mut idx = worker;
                    while idx < snapshot.len() {
                        let client = &snapshot[idx];
                        if client.connected.load(Ordering::Relaxed) {
                            let sequence = client.movement_sequence.fetch_add(1, Ordering::Relaxed);
                            let mut pose = client.pose.lock().await;
                            let datagram = pose.write_movement_datagram(
                                sequence,
                                start,
                                cadence.allow_position_drift,
                            );
                            if let Err(err) = client.send_connected(datagram).await {
                                trace!("movement send failed for {}: {err}", client.index);
                            }
                        }
                        idx += worker_count;
                    }
                }
                return;
            }

            // Random phases make deadlines dense. A min-heap preserves those phases while
            // allowing clients added from the console to join the schedule on the next refresh.
            let now = time::Instant::now();
            let interval_us = cadence.movement_interval.as_micros() as u64;
            let mut deadlines = BinaryHeap::<Reverse<(time::Instant, usize, u64)>>::with_capacity(
                initial_len.div_ceil(worker_count),
            );
            let mut scheduled_len = 0;
            let mut next_snapshot_refresh = now;

            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let wake_at = deadlines
                    .peek()
                    .map(|Reverse((deadline, _, _))| (*deadline).min(next_snapshot_refresh))
                    .unwrap_or(next_snapshot_refresh);
                time::sleep_until(wake_at).await;
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }

                let current = time::Instant::now();
                if current >= next_snapshot_refresh {
                    snapshot = clients.lock().await.clone();
                    for index in scheduled_len..snapshot.len() {
                        if index % worker_count != worker {
                            continue;
                        }
                        let mut cadence_state = cadence_seed(index, 0x4d4f_5645_4d45_4e54);
                        let phase_us = cadence_next(&mut cadence_state) % interval_us.max(1);
                        deadlines.push(Reverse((
                            current + Duration::from_micros(phase_us),
                            index,
                            cadence_state,
                        )));
                    }
                    scheduled_len = snapshot.len();
                    next_snapshot_refresh = current + Duration::from_secs(1);
                }

                while let Some(Reverse((next, _, _))) = deadlines.peek() {
                    if *next > current {
                        break;
                    }
                    let Reverse((scheduled, index, mut cadence_state)) = deadlines
                        .pop()
                        .expect("movement deadline heap was non-empty");
                    if let Some(client) = snapshot.get(index) {
                        if client.connected.load(Ordering::Relaxed) {
                            let sequence = client.movement_sequence.fetch_add(1, Ordering::Relaxed);
                            let mut pose = client.pose.lock().await;
                            let datagram = pose.write_movement_datagram(
                                sequence,
                                start,
                                cadence.allow_position_drift,
                            );
                            if let Err(err) = client.send_connected(datagram).await {
                                trace!("movement send failed for {}: {err}", client.index);
                            }
                        }
                    }
                    let interval = jittered_duration(
                        cadence.movement_interval,
                        cadence.movement_jitter_percent,
                        &mut cadence_state,
                    );
                    let mut next = scheduled + interval;
                    if next < current {
                        next = current + interval;
                    }
                    deadlines.push(Reverse((next, index, cadence_state)));
                }
            }
        });
    }
}
