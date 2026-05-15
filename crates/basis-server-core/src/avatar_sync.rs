use anyhow::Result;
use basis_protocol::{
    avatar::{
        encode_avatar_bundle, read_position, repack_high_to_lower_into, AvatarBundleItem,
        BitQuality,
    },
    channels,
};
use basis_transport::{PeerId, TransportHandle};
use bytes::Bytes;
use dashmap::DashMap;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    env,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::{runtime::Handle, task::JoinSet};
use tracing::warn;

const DISTANCE_UPDATE_INTERVAL_TICKS: usize = 125;
const MAX_SLICE_COUNT: usize = 32;
const AVATAR_BUNDLE_RAW_TARGET_BYTES: usize = 900;
pub(crate) const DEFAULT_AVATAR_TICK_BUDGET_MS: f64 = 12.0;
pub(crate) const DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS: f64 = 180.0;

#[derive(Debug, Clone)]
pub struct AvatarSyncConfig {
    pub default_interval_ms: u64,
    pub base_multiplier: f32,
    pub increase_rate: f32,
    pub high_distance_sq: f32,
    pub medium_distance_sq: f32,
    pub low_distance_sq: f32,
    pub enable_bundle_compression: bool,
    pub bundle_min_messages: usize,
    pub bundle_min_bytes: usize,
    pub min_receiver_slices: usize,
    pub max_receiver_slices: usize,
    pub tick_budget_ms: f64,
    pub receiver_cycle_budget_ms: f64,
}

#[derive(Debug, Clone)]
struct PreSerializedQuality {
    channel_small: u8,
    channel_large: u8,
    bytes_small: Bytes,
    bytes_large: Bytes,
}

#[derive(Debug, Clone)]
struct PlayerAvatarState {
    peer_id: PeerId,
    position: [f32; 3],
    generation: u64,
    last_inbound_sequence: u8,
    outbound_sequence: u8,
    has_received_first: bool,
    qualities: [Option<PreSerializedQuality>; 4],
}

#[derive(Debug, Clone)]
struct PendingAvatarUpdate {
    channel: u8,
    payload: Vec<u8>,
}

#[derive(Debug)]
struct ProcessedAvatarUpdate {
    peer_id: PeerId,
    inbound_sequence: u8,
    position: [f32; 3],
    quality: BitQuality,
    payload: Vec<u8>,
    payload_len: usize,
}

#[derive(Debug, Clone)]
struct ReceiverTracking {
    last_seen_generation: u64,
    last_sent: Instant,
    cached_quality_index: u8,
    cached_interval_byte: u8,
    cached_interval_ms: u64,
}

#[derive(Debug, Clone, Copy)]
struct SliceState {
    slice_count: usize,
    slice_index: usize,
    distance_tick: usize,
    fanout_round: usize,
    smoothed_tick_micros: u64,
}

#[derive(Debug, Clone)]
struct OutboundAvatarSend {
    channel: u8,
    payload: Bytes,
}

#[derive(Debug, Clone)]
struct OutboundAvatarBatch {
    receiver: PeerId,
    sends: Vec<OutboundAvatarSend>,
}

#[derive(Debug, Clone, Default)]
pub struct AvatarSyncStats {
    pub inbound_updates: u64,
    pub outbound_messages: u64,
    pub outbound_batches: u64,
    pub active_states: usize,
    pub pending_updates: usize,
    pub slice_count: usize,
    pub tick_count: u64,
    pub build_micros: u64,
    pub flush_micros: u64,
    pub max_tick_micros: u64,
    pub avg_tick_micros: u64,
    pub smoothed_tick_micros: u64,
    pub receiver_cycle_micros: u64,
    pub tick_budget_micros: u64,
    pub receiver_cycle_budget_micros: u64,
}

#[derive(Debug, Default)]
struct AvatarSyncCounters {
    inbound_updates: AtomicU64,
    outbound_messages: AtomicU64,
    outbound_batches: AtomicU64,
    tick_count: AtomicU64,
    build_micros: AtomicU64,
    flush_micros: AtomicU64,
    tick_micros: AtomicU64,
    max_tick_micros: AtomicU64,
}

#[derive(Debug, Default)]
struct BytePool {
    buffers: parking_lot::Mutex<Vec<Vec<u8>>>,
}

impl BytePool {
    const MAX_RETAINED_BUFFERS: usize = 4096;
    const MAX_RETAINED_CAPACITY: usize = 64 * 1024;

    fn take(&self, size: usize) -> Vec<u8> {
        let mut buffers = self.buffers.lock();
        if let Some(index) = buffers.iter().position(|buffer| buffer.capacity() >= size) {
            let mut buffer = buffers.swap_remove(index);
            buffer.clear();
            buffer.resize(size, 0);
            return buffer;
        }
        vec![0; size]
    }

    fn put(&self, mut buffer: Vec<u8>) {
        if buffer.capacity() > Self::MAX_RETAINED_CAPACITY {
            return;
        }
        buffer.clear();
        let mut buffers = self.buffers.lock();
        if buffers.len() < Self::MAX_RETAINED_BUFFERS {
            buffers.push(buffer);
        }
    }
}

#[derive(Debug, Clone)]
pub struct AvatarSyncSystem {
    config: Arc<parking_lot::RwLock<AvatarSyncConfig>>,
    states: Arc<DashMap<PeerId, PlayerAvatarState>>,
    pending: Arc<DashMap<PeerId, PendingAvatarUpdate>>,
    tracking: Arc<DashMap<PeerId, HashMap<PeerId, ReceiverTracking>>>,
    generation: Arc<AtomicU64>,
    slice_state: Arc<parking_lot::Mutex<SliceState>>,
    payload_pool: Arc<BytePool>,
    counters: Arc<AvatarSyncCounters>,
}

impl AvatarSyncSystem {
    pub fn new(config: AvatarSyncConfig) -> Self {
        Self {
            config: Arc::new(parking_lot::RwLock::new(config)),
            states: Arc::new(DashMap::new()),
            pending: Arc::new(DashMap::new()),
            tracking: Arc::new(DashMap::new()),
            generation: Arc::new(AtomicU64::new(1)),
            slice_state: Arc::new(parking_lot::Mutex::new(SliceState {
                slice_count: 1,
                slice_index: 0,
                distance_tick: 0,
                fanout_round: 0,
                smoothed_tick_micros: 0,
            })),
            payload_pool: Arc::new(BytePool::default()),
            counters: Arc::new(AvatarSyncCounters::default()),
        }
    }

    pub fn update_config(&self, config: AvatarSyncConfig) {
        {
            let mut state = self.slice_state.lock();
            state.slice_count = state
                .slice_count
                .clamp(config.min_receiver_slices, config.max_receiver_slices);
            state.slice_index %= state.slice_count.max(1);
        }
        *self.config.write() = config;
    }

    pub fn upsert_from_channel_payload(
        &self,
        peer_id: PeerId,
        channel: u8,
        payload: &[u8],
    ) -> Result<()> {
        if payload.is_empty() {
            return Ok(());
        }
        self.counters
            .inbound_updates
            .fetch_add(1, Ordering::Relaxed);
        let quality = basis_protocol::channels::quality_from_channel(channel);
        let quality = match quality {
            0 => BitQuality::VeryLow,
            1 => BitQuality::Low,
            2 => BitQuality::Medium,
            _ => BitQuality::High,
        };
        let expected = quality.payload_len();
        if payload.len() < 1 + expected {
            anyhow::bail!(
                "avatar payload too small for {:?}: got {}, need {}",
                quality,
                payload.len(),
                1 + expected
            );
        }
        let mut pooled = self.payload_pool.take(1 + expected);
        pooled.copy_from_slice(&payload[..1 + expected]);
        if let Some(old) = self.pending.insert(
            peer_id,
            PendingAvatarUpdate {
                channel,
                payload: pooled,
            },
        ) {
            self.payload_pool.put(old.payload);
        }
        Ok(())
    }

    pub fn remove_player(&self, peer_id: PeerId) {
        self.states.remove(&peer_id);
        if let Some((_, pending)) = self.pending.remove(&peer_id) {
            self.payload_pool.put(pending.payload);
        }
        self.tracking.remove(&peer_id);
        for mut entry in self.tracking.iter_mut() {
            entry.value_mut().remove(&peer_id);
        }
    }

    pub fn stats(&self) -> AvatarSyncStats {
        let state = self.slice_state.lock();
        let config = self.config.read();
        let tick_count = self.counters.tick_count.load(Ordering::Relaxed);
        let avg_tick_micros = if tick_count == 0 {
            0
        } else {
            self.counters.tick_micros.load(Ordering::Relaxed) / tick_count
        };
        AvatarSyncStats {
            inbound_updates: self.counters.inbound_updates.load(Ordering::Relaxed),
            outbound_messages: self.counters.outbound_messages.load(Ordering::Relaxed),
            outbound_batches: self.counters.outbound_batches.load(Ordering::Relaxed),
            active_states: self.states.len(),
            pending_updates: self.pending.len(),
            slice_count: state.slice_count,
            tick_count,
            build_micros: self.counters.build_micros.load(Ordering::Relaxed),
            flush_micros: self.counters.flush_micros.load(Ordering::Relaxed),
            max_tick_micros: self.counters.max_tick_micros.load(Ordering::Relaxed),
            avg_tick_micros,
            smoothed_tick_micros: state.smoothed_tick_micros,
            receiver_cycle_micros: state
                .smoothed_tick_micros
                .saturating_mul(state.slice_count as u64),
            tick_budget_micros: (config.tick_budget_ms.max(1.0) * 1000.0) as u64,
            receiver_cycle_budget_micros: (config.receiver_cycle_budget_ms.max(1.0) * 1000.0)
                as u64,
        }
    }

    pub fn spawn_tick_loop<F>(
        &self,
        transport: TransportHandle,
        shutdown: Arc<AtomicBool>,
        peer_snapshot: F,
    ) where
        F: Fn() -> Vec<PeerId> + Send + Sync + 'static,
    {
        let system = self.clone();
        let runtime = Handle::current();
        let _ = thread::Builder::new()
            .name("BSR-TickLoop".to_string())
            .spawn(move || {
                set_avatar_thread_priority();
                let tick = Duration::from_millis(4);
                while !shutdown.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    if let Err(err) =
                        runtime.block_on(system.flush_tick(&transport, &peer_snapshot))
                    {
                        warn!("avatar sync tick failed: {err:#}");
                    }
                    let elapsed = started.elapsed();
                    if elapsed + Duration::from_millis(1) < tick {
                        thread::sleep(tick - elapsed - Duration::from_millis(1));
                    }
                    while started.elapsed() < tick {
                        if shutdown.load(Ordering::Relaxed) {
                            break;
                        }
                        std::hint::spin_loop();
                    }
                }
            });
    }

    async fn flush_tick<F>(&self, transport: &TransportHandle, peer_snapshot: &F) -> Result<()>
    where
        F: Fn() -> Vec<PeerId>,
    {
        let config = self.config.read().clone();
        let tick_start = Instant::now();
        let now = Instant::now();
        self.process_pending_updates();
        let peers = peer_snapshot();
        if peers.len() <= 1 {
            return Ok(());
        }
        let peer_states: Vec<(PeerId, PlayerAvatarState)> = peers
            .iter()
            .filter_map(|peer| {
                self.states
                    .get(peer)
                    .map(|state| (*peer, state.value().clone()))
            })
            .collect();
        let (slice_count, slice_index, update_distances, fanout_round) = self.advance_slice_state();
        let receiver_states = peer_states
            .iter()
            .enumerate()
            .filter_map(|(index, (peer, state))| {
                (index % slice_count == slice_index).then_some((*peer, state.clone()))
            })
            .collect::<Vec<_>>();

        let build_start = Instant::now();
        let receiver_groups = receiver_states
            .par_iter()
            .filter_map(|(receiver_id, receiver_state)| {
                self.build_sends_for_receiver(
                    *receiver_id,
                    receiver_state,
                    &peer_states,
                    &config,
                    now,
                    update_distances,
                    fanout_round,
                )
            })
            .collect::<Vec<_>>();
        self.counters
            .build_micros
            .fetch_add(build_start.elapsed().as_micros() as u64, Ordering::Relaxed);

        for batch in &receiver_groups {
            self.counters
                .outbound_messages
                .fetch_add(batch.sends.len() as u64, Ordering::Relaxed);
            self.counters
                .outbound_batches
                .fetch_add(1, Ordering::Relaxed);
        }
        let flush_start = Instant::now();
        flush_receiver_groups_parallel(transport.clone(), receiver_groups).await?;
        self.counters
            .flush_micros
            .fetch_add(flush_start.elapsed().as_micros() as u64, Ordering::Relaxed);
        let tick_elapsed = tick_start.elapsed();
        let tick_micros = tick_elapsed.as_micros() as u64;
        self.counters
            .tick_micros
            .fetch_add(tick_micros, Ordering::Relaxed);
        self.counters.tick_count.fetch_add(1, Ordering::Relaxed);
        update_max_atomic(
            &self.counters.max_tick_micros,
            tick_micros,
        );
        let tick_count = self.counters.tick_count.load(Ordering::Relaxed);
        if tick_count % 100 == 0 {
            self.adapt_slice_count(tick_micros, &config);
        }
        Ok(())
    }

    fn process_pending_updates(&self) {
        let keys = self
            .pending
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        let updates = keys
            .into_iter()
            .filter_map(|peer_id| {
                self.pending
                    .remove(&peer_id)
                    .map(|(_, update)| (peer_id, update))
            })
            .collect::<Vec<_>>();
        if updates.is_empty() {
            return;
        }

        let processed = updates
            .into_par_iter()
            .map(|(peer_id, update)| process_pending_update(peer_id, update))
            .collect::<Vec<_>>();

        for update in processed {
            let generation = self.generation.fetch_add(1, Ordering::Relaxed);
            let avatar_payload = &update.payload[1..1 + update.payload_len];
            if let Some(mut current) = self.states.get_mut(&update.peer_id) {
                if current.has_received_first {
                    let delta = update
                        .inbound_sequence
                        .wrapping_sub(current.last_inbound_sequence);
                    if delta == 0 || delta >= 128 {
                        continue;
                    }
                }
                current.last_inbound_sequence = update.inbound_sequence;
                current.outbound_sequence = current.outbound_sequence.wrapping_add(1);
                current.has_received_first = true;
                current.position = update.position;
                current.generation = generation;
                if let Ok(qualities) = build_quality_packets(
                    &self.payload_pool,
                    update.peer_id,
                    current.outbound_sequence,
                    update.quality,
                    avatar_payload,
                ) {
                    current.qualities = qualities;
                }
                self.payload_pool.put(update.payload);
                continue;
            }

            if let Ok(qualities) = build_quality_packets(
                &self.payload_pool,
                update.peer_id,
                0,
                update.quality,
                avatar_payload,
            ) {
                self.states.insert(
                    update.peer_id,
                    PlayerAvatarState {
                        peer_id: update.peer_id,
                        position: update.position,
                        generation,
                        last_inbound_sequence: update.inbound_sequence,
                        outbound_sequence: 0,
                        has_received_first: true,
                        qualities,
                    },
                );
            }
            self.payload_pool.put(update.payload);
        }
    }

    fn build_sends_for_receiver(
        &self,
        receiver_id: PeerId,
        receiver_state: &PlayerAvatarState,
        peer_states: &[(PeerId, PlayerAvatarState)],
        config: &AvatarSyncConfig,
        now: Instant,
        update_distances: bool,
        fanout_round: usize,
    ) -> Option<OutboundAvatarBatch> {
        let mut direct = Vec::new();
        let mut bundle = Vec::new();
        let mut bundle_raw_bytes = 0usize;
        let mut receiver_tracking = self.tracking.entry(receiver_id).or_default();

        let peer_count = peer_states.len();
        let start_index = if peer_count == 0 {
            0
        } else {
            (fanout_round.wrapping_add(receiver_id as usize)) % peer_count
        };
        for offset in 0..peer_count {
            let (sender_id, sender_state) = &peer_states[(start_index + offset) % peer_count];
            let sender_id = *sender_id;
            if sender_id == receiver_id {
                continue;
            }
            let tracking = receiver_tracking.entry(sender_id).or_insert_with(|| {
                let dist_sq = distance_sq(receiver_state, sender_state);
                let (interval_byte, interval_ms) =
                    calculate_interval_from_distance_sq(dist_sq, config);
                ReceiverTracking {
                    last_seen_generation: 0,
                    last_sent: now - Duration::from_secs(60),
                    cached_quality_index: quality_from_distance_sq(dist_sq, config),
                    cached_interval_byte: interval_byte,
                    cached_interval_ms: interval_ms,
                }
            });
            if update_distances {
                let dist_sq = distance_sq(receiver_state, sender_state);
                tracking.cached_quality_index = quality_from_distance_sq(dist_sq, config);
                let (interval_byte, interval_ms) =
                    calculate_interval_from_distance_sq(dist_sq, config);
                tracking.cached_interval_byte = interval_byte;
                tracking.cached_interval_ms = interval_ms;
            }
            let quality_index = tracking.cached_quality_index;
            let Some(packet) = sender_state.qualities[quality_index as usize].as_ref() else {
                continue;
            };
            if tracking.last_seen_generation == sender_state.generation {
                continue;
            }
            let required_interval_ms = tracking
                .cached_interval_ms
                .max(config.default_interval_ms.max(1));
            if now.duration_since(tracking.last_sent) < Duration::from_millis(required_interval_ms)
            {
                continue;
            }
            tracking.last_seen_generation = sender_state.generation;
            tracking.last_sent = now;
            let interval_byte = tracking.cached_interval_byte;

            let (channel, packet_bytes, interval_offset) = if sender_state.peer_id <= u8::MAX as u16
            {
                (packet.channel_small, &packet.bytes_small, 1)
            } else {
                (packet.channel_large, &packet.bytes_large, 2)
            };

            if config.enable_bundle_compression {
                let payload = patch_interval_vec(packet_bytes, interval_offset, interval_byte);
                let item_raw_bytes = 3 + payload.len();
                let item = AvatarBundleItem {
                    original_channel: channel,
                    payload,
                };
                if item_raw_bytes > AVATAR_BUNDLE_RAW_TARGET_BYTES {
                    direct.push(OutboundAvatarSend {
                        channel,
                        payload: Bytes::from(item.payload),
                    });
                    continue;
                }
                if !bundle.is_empty()
                    && bundle_raw_bytes + item_raw_bytes > AVATAR_BUNDLE_RAW_TARGET_BYTES
                {
                    flush_avatar_bundle_chunk(
                        &mut direct,
                        &mut bundle,
                        &mut bundle_raw_bytes,
                        config.bundle_min_messages,
                        config.bundle_min_bytes,
                    );
                }
                bundle_raw_bytes += item_raw_bytes;
                bundle.push(item);
            } else {
                direct.push(OutboundAvatarSend {
                    channel,
                    payload: Bytes::from(patch_interval_vec(
                        packet_bytes,
                        interval_offset,
                        interval_byte,
                    )),
                });
            }
        }

        if config.enable_bundle_compression {
            flush_avatar_bundle_chunk(
                &mut direct,
                &mut bundle,
                &mut bundle_raw_bytes,
                config.bundle_min_messages,
                config.bundle_min_bytes,
            );
        }
        (!direct.is_empty()).then_some(OutboundAvatarBatch {
            receiver: receiver_id,
            sends: direct,
        })
    }

    fn advance_slice_state(&self) -> (usize, usize, bool, usize) {
        let mut state = self.slice_state.lock();
        let slice_count = state.slice_count.max(1);
        let slice_index = state.slice_index % slice_count;
        state.slice_index = (state.slice_index + 1) % slice_count;
        state.fanout_round = state.fanout_round.wrapping_add(1);
        state.distance_tick += 1;
        let update_distances = state.distance_tick >= DISTANCE_UPDATE_INTERVAL_TICKS;
        if update_distances {
            state.distance_tick = 0;
        }
        (
            slice_count,
            slice_index,
            update_distances,
            state.fanout_round,
        )
    }

    fn adapt_slice_count(&self, elapsed_micros: u64, config: &AvatarSyncConfig) {
        let mut state = self.slice_state.lock();
        state.smoothed_tick_micros = if state.smoothed_tick_micros == 0 {
            elapsed_micros
        } else {
            ((state.smoothed_tick_micros as f64 * 0.85) + (elapsed_micros as f64 * 0.15)) as u64
        };

        let min_slices = config.min_receiver_slices.max(1);
        let max_slices = config.max_receiver_slices.max(min_slices).min(MAX_SLICE_COUNT);
        state.slice_count = state.slice_count.clamp(min_slices, max_slices);
        let tick_budget_micros = (config.tick_budget_ms.max(1.0) * 1000.0) as u64;
        let cycle_budget_micros =
            (config.receiver_cycle_budget_ms.max(1.0) * 1000.0) as u64;
        let estimated_cycle_micros =
            state.smoothed_tick_micros.saturating_mul(state.slice_count as u64);

        let mut next_slice_count = state.slice_count;
        if state.smoothed_tick_micros > tick_budget_micros {
            let scale = state.smoothed_tick_micros as f64 / tick_budget_micros as f64;
            let minimum_increase = (state.slice_count + 1).min(max_slices);
            next_slice_count = ((state.slice_count as f64 * scale).ceil() as usize)
                .clamp(minimum_increase, max_slices);
        } else if estimated_cycle_micros > cycle_budget_micros && state.slice_count > min_slices {
            let target = (cycle_budget_micros / state.smoothed_tick_micros.max(1)) as usize;
            next_slice_count = target.clamp(min_slices, state.slice_count.saturating_sub(1));
        } else if state.smoothed_tick_micros < tick_budget_micros / 2
            && state.slice_count > min_slices
        {
            next_slice_count = state.slice_count - 1;
        }

        if next_slice_count != state.slice_count {
            state.slice_count = next_slice_count;
            state.slice_index %= state.slice_count.max(1);
        }
    }
}

impl AvatarSyncConfig {
    pub fn apply_env_tuning(mut self) -> Self {
        self.min_receiver_slices = env_usize("BASIS_AVATAR_MIN_RECEIVER_SLICES")
            .unwrap_or(self.min_receiver_slices)
            .max(1)
            .min(MAX_SLICE_COUNT);
        self.max_receiver_slices = env_usize("BASIS_AVATAR_MAX_RECEIVER_SLICES")
            .unwrap_or(self.max_receiver_slices)
            .max(self.min_receiver_slices)
            .min(MAX_SLICE_COUNT);
        self.tick_budget_ms =
            env_f64("BASIS_AVATAR_TICK_BUDGET_MS").unwrap_or(self.tick_budget_ms);
        self.receiver_cycle_budget_ms = env_f64("BASIS_AVATAR_RECEIVER_CYCLE_BUDGET_MS")
            .unwrap_or(self.receiver_cycle_budget_ms);
        self
    }
}

fn env_usize(name: &str) -> Option<usize> {
    env::var(name).ok()?.parse().ok()
}

fn env_f64(name: &str) -> Option<f64> {
    env::var(name).ok()?.parse().ok()
}

async fn flush_receiver_groups_parallel(
    transport: TransportHandle,
    receiver_groups: Vec<OutboundAvatarBatch>,
) -> Result<()> {
    if receiver_groups.is_empty() {
        return Ok(());
    }
    let worker_count = std::thread::available_parallelism()
        .map(|n| n.get().saturating_sub(1).max(1))
        .unwrap_or(4)
        .min(receiver_groups.len());
    let chunk_size = receiver_groups.len().div_ceil(worker_count);
    let mut chunks = Vec::with_capacity(worker_count);
    let mut current = Vec::with_capacity(chunk_size);
    for batch in receiver_groups {
        current.push(batch);
        if current.len() >= chunk_size {
            chunks.push(current);
            current = Vec::with_capacity(chunk_size);
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    let mut join_set = JoinSet::new();
    for chunk in chunks {
        let transport = transport.clone();
        join_set.spawn(async move {
            for batch in chunk {
                let packets = batch
                    .sends
                    .into_iter()
                    .map(|send| (send.channel, send.payload))
                    .collect::<Vec<_>>();
                transport.try_send_many_unreliable_bytes(batch.receiver, &packets)?;
            }
            Ok::<(), basis_transport::TransportError>(())
        });
    }
    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(err.into()),
            Err(err) => anyhow::bail!("avatar send worker failed: {err}"),
        }
    }
    Ok(())
}

fn flush_avatar_bundle_chunk(
    direct: &mut Vec<OutboundAvatarSend>,
    bundle: &mut Vec<AvatarBundleItem>,
    bundle_raw_bytes: &mut usize,
    min_messages: usize,
    min_bytes: usize,
) {
    if bundle.is_empty() {
        return;
    }
    if bundle.len() >= min_messages && *bundle_raw_bytes >= min_bytes {
        if let Ok(encoded) = encode_avatar_bundle(bundle) {
            direct.push(OutboundAvatarSend {
                channel: channels::COMPRESSED_AVATAR_BUNDLE,
                payload: Bytes::from(encoded),
            });
            bundle.clear();
            *bundle_raw_bytes = 0;
            return;
        }
    }
    direct.extend(bundle.drain(..).map(|item| OutboundAvatarSend {
        channel: item.original_channel,
        payload: Bytes::from(item.payload),
    }));
    *bundle_raw_bytes = 0;
}

fn update_max_atomic(value: &AtomicU64, candidate: u64) {
    let mut current = value.load(Ordering::Relaxed);
    while candidate > current {
        match value.compare_exchange_weak(current, candidate, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(next) => current = next,
        }
    }
}

fn process_pending_update(peer_id: PeerId, update: PendingAvatarUpdate) -> ProcessedAvatarUpdate {
    let inbound_sequence = update.payload[0];
    let quality = basis_protocol::channels::quality_from_channel(update.channel);
    let quality = match quality {
        0 => BitQuality::VeryLow,
        1 => BitQuality::Low,
        2 => BitQuality::Medium,
        _ => BitQuality::High,
    };
    let expected = quality.payload_len();
    debug_assert!(update.payload.len() >= 1 + expected);
    let payload_len = expected.min(update.payload.len().saturating_sub(1));
    let avatar_payload = &update.payload[1..1 + payload_len];
    let position = read_position(&avatar_payload).unwrap_or([0.0, 0.0, 0.0]);
    ProcessedAvatarUpdate {
        peer_id,
        inbound_sequence,
        position,
        quality,
        payload: update.payload,
        payload_len,
    }
}

fn build_quality_packets(
    pool: &BytePool,
    peer_id: PeerId,
    outbound_sequence: u8,
    inbound_quality: BitQuality,
    payload: &[u8],
) -> Result<[Option<PreSerializedQuality>; 4]> {
    let mut qualities: [Option<PreSerializedQuality>; 4] = [None, None, None, None];
    match inbound_quality {
        BitQuality::High => {
            qualities[BitQuality::High as usize] = Some(pre_serialize(
                peer_id,
                outbound_sequence,
                BitQuality::High,
                payload,
            ));
            let mut medium = pool.take(BitQuality::Medium.payload_len());
            let mut low = pool.take(BitQuality::Low.payload_len());
            let mut very_low = pool.take(BitQuality::VeryLow.payload_len());
            repack_high_to_lower_into(payload, BitQuality::Medium, &mut medium)?;
            repack_high_to_lower_into(payload, BitQuality::Low, &mut low)?;
            repack_high_to_lower_into(payload, BitQuality::VeryLow, &mut very_low)?;
            qualities[BitQuality::Medium as usize] = Some(pre_serialize(
                peer_id,
                outbound_sequence,
                BitQuality::Medium,
                &medium,
            ));
            qualities[BitQuality::Low as usize] = Some(pre_serialize(
                peer_id,
                outbound_sequence,
                BitQuality::Low,
                &low,
            ));
            qualities[BitQuality::VeryLow as usize] = Some(pre_serialize(
                peer_id,
                outbound_sequence,
                BitQuality::VeryLow,
                &very_low,
            ));
            pool.put(medium);
            pool.put(low);
            pool.put(very_low);
        }
        other => {
            qualities[other as usize] =
                Some(pre_serialize(peer_id, outbound_sequence, other, payload));
        }
    }
    Ok(qualities)
}

fn pre_serialize(
    peer_id: PeerId,
    outbound_sequence: u8,
    quality: BitQuality,
    payload: &[u8],
) -> PreSerializedQuality {
    let has_additional = false;
    let channel_small =
        basis_protocol::channels::player_avatar_channel_for_quality(quality as u8, has_additional);
    let channel_large = basis_protocol::channels::player_avatar_large_channel_for_quality(
        quality as u8,
        has_additional,
    );
    let mut bytes_small = Vec::with_capacity(3 + payload.len());
    bytes_small.push(peer_id as u8);
    bytes_small.push(0);
    bytes_small.push(outbound_sequence);
    bytes_small.extend_from_slice(payload);

    let mut bytes_large = Vec::with_capacity(4 + payload.len());
    bytes_large.extend_from_slice(&peer_id.to_le_bytes());
    bytes_large.push(0);
    bytes_large.push(outbound_sequence);
    bytes_large.extend_from_slice(payload);

    PreSerializedQuality {
        channel_small,
        channel_large,
        bytes_small: Bytes::from(bytes_small),
        bytes_large: Bytes::from(bytes_large),
    }
}

fn patch_interval_vec(payload: &Bytes, interval_offset: usize, interval: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(payload.len());
    bytes.extend_from_slice(payload);
    if interval_offset < bytes.len() {
        bytes[interval_offset] = interval;
    }
    bytes
}

#[cfg(windows)]
fn set_avatar_thread_priority() {
    use windows_sys::Win32::System::Threading::{
        GetCurrentThread, SetThreadPriority, THREAD_PRIORITY_ABOVE_NORMAL,
    };
    unsafe {
        SetThreadPriority(GetCurrentThread(), THREAD_PRIORITY_ABOVE_NORMAL);
    }
}

#[cfg(not(windows))]
fn set_avatar_thread_priority() {}

fn distance_sq(receiver: &PlayerAvatarState, sender: &PlayerAvatarState) -> f32 {
    let dx = receiver.position[0] - sender.position[0];
    let dy = receiver.position[1] - sender.position[1];
    let dz = receiver.position[2] - sender.position[2];
    dx * dx + dy * dy + dz * dz
}

fn quality_from_distance_sq(distance_sq: f32, config: &AvatarSyncConfig) -> u8 {
    if distance_sq <= config.high_distance_sq {
        BitQuality::High as u8
    } else if distance_sq <= config.medium_distance_sq {
        BitQuality::Medium as u8
    } else if distance_sq <= config.low_distance_sq {
        BitQuality::Low as u8
    } else {
        BitQuality::VeryLow as u8
    }
}

fn calculate_interval_from_distance_sq(distance_sq: f32, config: &AvatarSyncConfig) -> (u8, u64) {
    let base_interval = config.default_interval_ms.max(1) as f32;
    let raw_interval =
        (base_interval * (config.base_multiplier + distance_sq * config.increase_rate)) as i32;
    let encoded = raw_interval - config.default_interval_ms.max(1) as i32;
    let interval_byte = encoded.clamp(0, u8::MAX as i32) as u8;
    let actual_interval = config.default_interval_ms.max(1) + interval_byte as u64;
    (interval_byte, actual_interval)
}

#[cfg(test)]
mod tests {
    use super::*;
    use basis_protocol::avatar::decode_avatar_bundle;

    #[test]
    fn packet_preserialization_uses_small_and_large_ids() {
        let payload = vec![0u8; BitQuality::High.payload_len()];
        let packet = pre_serialize(300, 7, BitQuality::High, &payload);
        assert_eq!(packet.channel_small, channels::PLAYER_AVATAR_HIGH);
        assert_eq!(packet.channel_large, channels::PLAYER_AVATAR_HIGH_LARGE);
        assert_eq!(&packet.bytes_large[0..2], &300u16.to_le_bytes());
        assert_eq!(packet.bytes_large[3], 7);
    }

    #[test]
    fn interval_byte_matches_csharp_formula() {
        let config = AvatarSyncConfig {
            default_interval_ms: 50,
            base_multiplier: 1.0,
            increase_rate: 0.005,
            high_distance_sq: 9.0,
            medium_distance_sq: 100.0,
            low_distance_sq: 400.0,
            enable_bundle_compression: false,
            bundle_min_messages: 4,
            bundle_min_bytes: 128,
            min_receiver_slices: 1,
            max_receiver_slices: 32,
            tick_budget_ms: DEFAULT_AVATAR_TICK_BUDGET_MS,
            receiver_cycle_budget_ms: DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS,
        };
        let (interval_byte, actual_ms) = calculate_interval_from_distance_sq(100.0, &config);
        assert_eq!(interval_byte, 25);
        assert_eq!(actual_ms, 75);
    }

    #[test]
    fn bundle_decoder_accepts_encoded_sync_items() {
        let items = vec![AvatarBundleItem {
            original_channel: channels::PLAYER_AVATAR_HIGH,
            payload: vec![1, 2, 3],
        }];
        let encoded = encode_avatar_bundle(&items).unwrap();
        assert_eq!(decode_avatar_bundle(&encoded).unwrap(), items);
    }
}
