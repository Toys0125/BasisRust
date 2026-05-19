use anyhow::Result;
use basis_protocol::{
    avatar::{
        read_position, repack_high_to_lower_into, try_encode_avatar_bundle_slices,
        AvatarBundleSlice, BitQuality,
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
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{runtime::Handle, task::JoinSet};
use tracing::warn;

use crate::p2p::pack_pair;

const DISTANCE_UPDATE_INTERVAL_MS: u64 = 500;
const MAX_SLICE_COUNT: usize = 32;
const AVATAR_BUNDLE_WIRE_BUDGET_BYTES: usize = 1100;
const AVATAR_BUNDLE_INITIAL_RATIO: f32 = 0.60;
const AVATAR_BUNDLE_MIN_RATIO: f32 = 0.05;
const AVATAR_BUNDLE_MAX_RATIO: f32 = 0.95;
pub(crate) const DEFAULT_AVATAR_TICK_BUDGET_MS: f64 = 3.0;
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
    pub spatial_cull_enabled: bool,
    pub enable_bsr_profiling: bool,
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

struct SpatialGrid {
    cell_size: f32,
    cells: HashMap<(i32, i32, i32), Vec<usize>>,
}

impl SpatialGrid {
    fn build(peer_states: &[(PeerId, PlayerAvatarState)], low_distance_sq: f32) -> Option<Self> {
        let cell_size = low_distance_sq.sqrt();
        if !cell_size.is_finite() || cell_size <= f32::EPSILON {
            return None;
        }
        let mut cells = HashMap::with_capacity(peer_states.len());
        for (index, (_, state)) in peer_states.iter().enumerate() {
            cells
                .entry(Self::cell_for_position(state.position, cell_size))
                .or_insert_with(Vec::new)
                .push(index);
        }
        Some(Self { cell_size, cells })
    }

    fn ordered_indices(&self, position: [f32; 3], peer_count: usize) -> Vec<usize> {
        let (cx, cy, cz) = Self::cell_for_position(position, self.cell_size);
        let mut included = vec![false; peer_count];
        let mut indices = Vec::new();
        for x in (cx - 1)..=(cx + 1) {
            for y in (cy - 1)..=(cy + 1) {
                for z in (cz - 1)..=(cz + 1) {
                    if let Some(cell) = self.cells.get(&(x, y, z)) {
                        for index in cell {
                            if *index < peer_count && !included[*index] {
                                included[*index] = true;
                                indices.push(*index);
                            }
                        }
                    }
                }
            }
        }
        for (index, seen) in included.into_iter().enumerate() {
            if !seen {
                indices.push(index);
            }
        }
        indices
    }

    fn cell_for_position(position: [f32; 3], cell_size: f32) -> (i32, i32, i32) {
        (
            (position[0] / cell_size).floor() as i32,
            (position[1] / cell_size).floor() as i32,
            (position[2] / cell_size).floor() as i32,
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct SliceState {
    slice_count: usize,
    slice_index: usize,
    last_distance_update: Instant,
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

#[derive(Debug, Clone)]
struct BundleAvatarSend {
    original_channel: u8,
    payload: Bytes,
    interval_offset: usize,
    interval_byte: u8,
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
struct BsrProfiler {
    enabled: AtomicBool,
    last_print_micros: AtomicU64,
    drain_micros: AtomicU64,
    process_micros: AtomicU64,
    distance_micros: AtomicU64,
    update_micros: AtomicU64,
    trigger_micros: AtomicU64,
    tick_count: AtomicU64,
    messages_processed: AtomicU64,
    send_count: AtomicU64,
    pre_serializations: AtomicU64,
    pre_serializations_skipped: AtomicU64,
    bundles_emitted: AtomicU64,
    bundle_messages: AtomicU64,
    bundle_raw_bytes: AtomicU64,
    bundle_compressed_bytes: AtomicU64,
    bundle_deflate_micros: AtomicU64,
    bundle_retries: AtomicU64,
    bundle_fallbacks: AtomicU64,
    bundle_tail_uncompressed: AtomicU64,
}

impl BsrProfiler {
    const PRINT_INTERVAL_MICROS: u64 = 5_000_000;

    fn new(enabled: bool) -> Self {
        let profiler = Self::default();
        profiler.enabled.store(enabled, Ordering::Relaxed);
        profiler
            .last_print_micros
            .store(now_micros(), Ordering::Relaxed);
        profiler
    }

    fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::Relaxed);
        if enabled {
            self.last_print_micros
                .store(now_micros(), Ordering::Relaxed);
        }
    }

    fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    fn add_phase_micros(&self, phase: BsrPhase, micros: u64) {
        if !self.enabled() {
            return;
        }
        phase.counter(self).fetch_add(micros, Ordering::Relaxed);
    }

    fn add_tick(&self, messages: u64) {
        if !self.enabled() {
            return;
        }
        self.tick_count.fetch_add(1, Ordering::Relaxed);
        self.messages_processed
            .fetch_add(messages, Ordering::Relaxed);
    }

    fn add_sends(&self, sends: u64) {
        if self.enabled() {
            self.send_count.fetch_add(sends, Ordering::Relaxed);
        }
    }

    fn add_pre_serializations(&self, count: u64) {
        if self.enabled() {
            self.pre_serializations.fetch_add(count, Ordering::Relaxed);
        }
    }

    fn add_bundle_emitted(
        &self,
        messages: u64,
        raw_bytes: u64,
        compressed_bytes: u64,
        deflate_micros: u64,
    ) {
        if !self.enabled() {
            return;
        }
        self.bundles_emitted.fetch_add(1, Ordering::Relaxed);
        self.bundle_messages.fetch_add(messages, Ordering::Relaxed);
        self.bundle_raw_bytes
            .fetch_add(raw_bytes, Ordering::Relaxed);
        self.bundle_compressed_bytes
            .fetch_add(compressed_bytes, Ordering::Relaxed);
        self.bundle_deflate_micros
            .fetch_add(deflate_micros, Ordering::Relaxed);
    }

    fn add_bundle_tail_uncompressed(&self, messages: u64) {
        if self.enabled() {
            self.bundle_tail_uncompressed
                .fetch_add(messages, Ordering::Relaxed);
        }
    }

    fn add_bundle_fallback(&self, messages: u64) {
        if self.enabled() {
            self.bundle_fallbacks.fetch_add(1, Ordering::Relaxed);
            self.bundle_tail_uncompressed
                .fetch_add(messages, Ordering::Relaxed);
        }
    }

    fn try_print(&self) {
        if !self.enabled() {
            return;
        }
        let now = now_micros();
        let last = self.last_print_micros.load(Ordering::Relaxed);
        if now.saturating_sub(last) < Self::PRINT_INTERVAL_MICROS {
            return;
        }
        if self
            .last_print_micros
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        let ticks = self.tick_count.swap(0, Ordering::Relaxed);
        if ticks == 0 {
            return;
        }
        let msgs = self.messages_processed.swap(0, Ordering::Relaxed);
        let sends = self.send_count.swap(0, Ordering::Relaxed);
        let pre_ser = self.pre_serializations.swap(0, Ordering::Relaxed);
        let pre_skip = self.pre_serializations_skipped.swap(0, Ordering::Relaxed);

        let drain = self.drain_micros.swap(0, Ordering::Relaxed) as f64 / 1000.0;
        let process = self.process_micros.swap(0, Ordering::Relaxed) as f64 / 1000.0;
        let distance = self.distance_micros.swap(0, Ordering::Relaxed) as f64 / 1000.0;
        let update = self.update_micros.swap(0, Ordering::Relaxed) as f64 / 1000.0;
        let trigger = self.trigger_micros.swap(0, Ordering::Relaxed) as f64 / 1000.0;
        let total = (drain + process + distance + update + trigger).max(f64::EPSILON);
        let ticks_f = ticks as f64;

        println!(
            "\n[BSR Profile] {ticks} ticks, {msgs} msgs, {sends} sends, preSer {pre_ser}/{}",
            pre_ser + pre_skip
        );
        println!(
            "  drain:    {:.3} ms/tick ({:.1}%)",
            drain / ticks_f,
            drain / total * 100.0
        );
        println!(
            "  process:  {:.3} ms/tick ({:.1}%)",
            process / ticks_f,
            process / total * 100.0
        );
        println!(
            "  distance: {:.3} ms/tick ({:.1}%)",
            distance / ticks_f,
            distance / total * 100.0
        );
        println!(
            "  update:   {:.3} ms/tick ({:.1}%)",
            update / ticks_f,
            update / total * 100.0
        );
        println!(
            "  trigger:  {:.3} ms/tick ({:.1}%)",
            trigger / ticks_f,
            trigger / total * 100.0
        );
        println!("  total:    {:.3} ms/tick", total / ticks_f);

        let b_emit = self.bundles_emitted.swap(0, Ordering::Relaxed);
        let b_msg = self.bundle_messages.swap(0, Ordering::Relaxed);
        let b_raw = self.bundle_raw_bytes.swap(0, Ordering::Relaxed);
        let b_comp = self.bundle_compressed_bytes.swap(0, Ordering::Relaxed);
        let b_deflate_micros = self.bundle_deflate_micros.swap(0, Ordering::Relaxed);
        let b_retry = self.bundle_retries.swap(0, Ordering::Relaxed);
        let b_fallback = self.bundle_fallbacks.swap(0, Ordering::Relaxed);
        let b_tail = self.bundle_tail_uncompressed.swap(0, Ordering::Relaxed);

        if b_emit > 0 || b_tail > 0 || b_fallback > 0 {
            let ratio = if b_raw > 0 {
                b_comp as f64 / b_raw as f64
            } else {
                0.0
            };
            let avg_msgs_per_bundle = if b_emit > 0 {
                b_msg as f64 / b_emit as f64
            } else {
                0.0
            };
            let avg_raw_per_bundle = if b_emit > 0 {
                b_raw as f64 / b_emit as f64
            } else {
                0.0
            };
            let avg_comp_per_bundle = if b_emit > 0 {
                b_comp as f64 / b_emit as f64
            } else {
                0.0
            };
            let deflate_ms = b_deflate_micros as f64 / 1000.0;
            let avg_deflate_us = if b_emit > 0 {
                b_deflate_micros as f64 / b_emit as f64
            } else {
                0.0
            };
            let bundles_per_tick = b_emit as f64 / ticks_f;
            let retry_rate = if b_emit > 0 {
                b_retry as f64 / b_emit as f64 * 100.0
            } else {
                0.0
            };
            let saved_bytes = b_raw.saturating_sub(b_comp);
            println!("  bundles:  {b_emit} emitted ({bundles_per_tick:.2}/tick), {b_msg} msgs in bundles, {b_tail} msgs tail-uncompressed, {b_fallback} fallbacks");
            println!("            ratio {ratio:.3} ({:.1}% saved on bundled bytes), avg {avg_msgs_per_bundle:.1} msgs/bundle ({avg_raw_per_bundle:.0} B raw -> {avg_comp_per_bundle:.0} B compressed)", (1.0 - ratio) * 100.0);
            println!("            deflate {:.3} ms/tick ({:.1}% of tick), {avg_deflate_us:.1} us/bundle, retries {b_retry} ({retry_rate:.1}%)", deflate_ms / ticks_f, deflate_ms / total * 100.0);
            println!(
                "            saved ~{:.1} KB this window before per-message wire overhead",
                saved_bytes as f64 / 1024.0
            );
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum BsrPhase {
    Drain,
    Process,
    Distance,
    Update,
    Trigger,
}

impl BsrPhase {
    fn counter(self, profiler: &BsrProfiler) -> &AtomicU64 {
        match self {
            Self::Drain => &profiler.drain_micros,
            Self::Process => &profiler.process_micros,
            Self::Distance => &profiler.distance_micros,
            Self::Update => &profiler.update_micros,
            Self::Trigger => &profiler.trigger_micros,
        }
    }
}

#[derive(Debug, Default)]
struct BytePool {
    shards: Vec<parking_lot::Mutex<Vec<Vec<u8>>>>,
    next_shard: AtomicU64,
}

impl BytePool {
    const SHARD_COUNT: usize = 32;
    const MAX_RETAINED_BUFFERS: usize = 4096;
    const MAX_RETAINED_CAPACITY: usize = 64 * 1024;

    fn new() -> Self {
        Self {
            shards: (0..Self::SHARD_COUNT)
                .map(|_| parking_lot::Mutex::new(Vec::new()))
                .collect(),
            next_shard: AtomicU64::new(0),
        }
    }

    fn take(&self, size: usize) -> Vec<u8> {
        let start = self.next_index();
        for offset in 0..Self::SHARD_COUNT {
            let index = (start + offset) % Self::SHARD_COUNT;
            let mut buffers = self.shards[index].lock();
            if let Some(buffer_index) = buffers.iter().position(|buffer| buffer.capacity() >= size)
            {
                let mut buffer = buffers.swap_remove(buffer_index);
                buffer.clear();
                buffer.resize(size, 0);
                return buffer;
            }
        }
        vec![0; size]
    }

    fn put(&self, mut buffer: Vec<u8>) {
        if buffer.capacity() > Self::MAX_RETAINED_CAPACITY {
            return;
        }
        buffer.clear();
        let mut buffers = self.shards[self.next_index()].lock();
        if buffers.len() < Self::MAX_RETAINED_BUFFERS / Self::SHARD_COUNT {
            buffers.push(buffer);
        }
    }

    fn next_index(&self) -> usize {
        self.next_shard.fetch_add(1, Ordering::Relaxed) as usize % Self::SHARD_COUNT
    }
}

#[derive(Debug, Clone)]
pub struct AvatarSyncSystem {
    config: Arc<parking_lot::RwLock<AvatarSyncConfig>>,
    states: Arc<DashMap<PeerId, PlayerAvatarState>>,
    pending: Arc<DashMap<PeerId, PendingAvatarUpdate>>,
    tracking: Arc<DashMap<PeerId, HashMap<PeerId, ReceiverTracking>>>,
    bundle_ratios: Arc<DashMap<PeerId, f32>>,
    generation: Arc<AtomicU64>,
    slice_state: Arc<parking_lot::Mutex<SliceState>>,
    payload_pool: Arc<BytePool>,
    counters: Arc<AvatarSyncCounters>,
    profiler: Arc<BsrProfiler>,
    offloaded_pairs: Arc<DashMap<u64, ()>>,
}

impl AvatarSyncSystem {
    pub fn new(config: AvatarSyncConfig) -> Self {
        let profiler_enabled = config.enable_bsr_profiling;
        Self {
            config: Arc::new(parking_lot::RwLock::new(config)),
            states: Arc::new(DashMap::new()),
            pending: Arc::new(DashMap::new()),
            tracking: Arc::new(DashMap::new()),
            bundle_ratios: Arc::new(DashMap::new()),
            generation: Arc::new(AtomicU64::new(1)),
            slice_state: Arc::new(parking_lot::Mutex::new(SliceState {
                slice_count: 1,
                slice_index: 0,
                last_distance_update: Instant::now(),
                smoothed_tick_micros: 0,
            })),
            payload_pool: Arc::new(BytePool::new()),
            counters: Arc::new(AvatarSyncCounters::default()),
            profiler: Arc::new(BsrProfiler::new(profiler_enabled)),
            offloaded_pairs: Arc::new(DashMap::new()),
        }
    }

    pub fn set_offloaded_pairs(&mut self, offloaded_pairs: Arc<DashMap<u64, ()>>) {
        self.offloaded_pairs = offloaded_pairs;
    }

    pub fn update_config(&self, config: AvatarSyncConfig) {
        {
            let mut state = self.slice_state.lock();
            state.slice_count = state
                .slice_count
                .clamp(config.min_receiver_slices, config.max_receiver_slices);
            state.slice_index %= state.slice_count.max(1);
        }
        self.profiler.set_enabled(config.enable_bsr_profiling);
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
        self.bundle_ratios.remove(&peer_id);
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
        let messages_processed = self.process_pending_updates();
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
        let (_, slice_start, slice_end, update_distances) =
            self.advance_slice_state(peer_states.len());
        let receiver_states = peer_states
            .get(slice_start..slice_end)
            .unwrap_or(&[])
            .iter()
            .map(|(peer, state)| (*peer, state.clone()))
            .collect::<Vec<_>>();
        let spatial_grid = if config.spatial_cull_enabled {
            SpatialGrid::build(&peer_states, config.low_distance_sq)
        } else {
            None
        };
        self.profiler.add_phase_micros(BsrPhase::Distance, 0);

        let build_start = Instant::now();
        let receiver_groups = receiver_states
            .par_iter()
            .filter_map(|(receiver_id, receiver_state)| {
                self.build_sends_for_receiver(
                    *receiver_id,
                    receiver_state,
                    &peer_states,
                    spatial_grid.as_ref(),
                    &config,
                    now,
                    update_distances,
                )
            })
            .collect::<Vec<_>>();
        self.counters
            .build_micros
            .fetch_add(build_start.elapsed().as_micros() as u64, Ordering::Relaxed);
        self.profiler
            .add_phase_micros(BsrPhase::Update, build_start.elapsed().as_micros() as u64);

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
        self.profiler
            .add_phase_micros(BsrPhase::Update, flush_start.elapsed().as_micros() as u64);
        self.profiler.add_phase_micros(BsrPhase::Trigger, 0);
        let tick_elapsed = tick_start.elapsed();
        let tick_micros = tick_elapsed.as_micros() as u64;
        self.counters
            .tick_micros
            .fetch_add(tick_micros, Ordering::Relaxed);
        self.counters.tick_count.fetch_add(1, Ordering::Relaxed);
        self.profiler.add_tick(messages_processed as u64);
        update_max_atomic(&self.counters.max_tick_micros, tick_micros);
        self.adapt_slice_count(tick_micros, &config);
        self.profiler.try_print();
        Ok(())
    }

    fn process_pending_updates(&self) -> usize {
        let drain_start = Instant::now();
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
        self.profiler
            .add_phase_micros(BsrPhase::Drain, drain_start.elapsed().as_micros() as u64);
        let update_count = updates.len();
        if updates.is_empty() {
            return 0;
        }

        let process_start = Instant::now();
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
                        self.payload_pool.put(update.payload);
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
                    &self.profiler,
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
                &self.profiler,
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
        self.profiler.add_phase_micros(
            BsrPhase::Process,
            process_start.elapsed().as_micros() as u64,
        );
        update_count
    }

    fn build_sends_for_receiver(
        &self,
        receiver_id: PeerId,
        receiver_state: &PlayerAvatarState,
        peer_states: &[(PeerId, PlayerAvatarState)],
        spatial_grid: Option<&SpatialGrid>,
        config: &AvatarSyncConfig,
        now: Instant,
        update_distances: bool,
    ) -> Option<OutboundAvatarBatch> {
        let mut direct = Vec::new();
        let mut bundle = Vec::new();
        let mut bundle_raw_bytes = 0usize;
        let mut receiver_tracking = self.tracking.entry(receiver_id).or_default();
        let mut logical_sends = 0u64;
        let mut bundle_ratio = self
            .bundle_ratios
            .get(&receiver_id)
            .map(|ratio| *ratio)
            .unwrap_or(AVATAR_BUNDLE_INITIAL_RATIO);

        let spatial_candidates = spatial_grid
            .map(|grid| grid.ordered_indices(receiver_state.position, peer_states.len()));
        let peer_count = spatial_candidates
            .as_ref()
            .map_or(peer_states.len(), |indices| indices.len());
        for offset in 0..peer_count {
            let peer_index = if let Some(indices) = spatial_candidates.as_ref() {
                indices[offset]
            } else {
                offset
            };
            let (sender_id, sender_state) = &peer_states[peer_index];
            let sender_id = *sender_id;
            if sender_id == receiver_id {
                continue;
            }
            if self
                .offloaded_pairs
                .contains_key(&pack_pair(receiver_id, sender_id))
            {
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
            if sender_state.generation <= tracking.last_seen_generation {
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
            logical_sends += 1;

            if config.enable_bundle_compression {
                let item_raw_bytes = 3 + packet_bytes.len();
                bundle_raw_bytes += item_raw_bytes;
                bundle.push(BundleAvatarSend {
                    original_channel: channel,
                    payload: packet_bytes.clone(),
                    interval_offset,
                    interval_byte,
                });
            } else {
                direct.push(OutboundAvatarSend {
                    channel,
                    payload: patch_interval_bytes(packet_bytes, interval_offset, interval_byte),
                });
            }
        }

        if config.enable_bundle_compression {
            emit_greedy_avatar_bundles(
                &mut direct,
                &mut bundle,
                &mut bundle_raw_bytes,
                &mut bundle_ratio,
                &self.profiler,
                config.bundle_min_messages,
                config.bundle_min_bytes,
            );
            self.bundle_ratios.insert(receiver_id, bundle_ratio);
        }
        self.profiler.add_sends(logical_sends);
        (!direct.is_empty()).then_some(OutboundAvatarBatch {
            receiver: receiver_id,
            sends: direct,
        })
    }

    fn advance_slice_state(&self, receiver_count: usize) -> (usize, usize, usize, bool) {
        let mut state = self.slice_state.lock();
        let now = Instant::now();
        let slice_count = state.slice_count.max(1);
        let slice_index = state.slice_index % slice_count;
        state.slice_index = (state.slice_index + 1) % slice_count;
        let slice_size = receiver_count.div_ceil(slice_count);
        let slice_start = slice_index.saturating_mul(slice_size).min(receiver_count);
        let slice_end = slice_start.saturating_add(slice_size).min(receiver_count);
        let update_distances = now.duration_since(state.last_distance_update)
            >= Duration::from_millis(DISTANCE_UPDATE_INTERVAL_MS);
        if update_distances {
            state.last_distance_update = now;
        }
        (slice_count, slice_start, slice_end, update_distances)
    }

    fn adapt_slice_count(&self, elapsed_micros: u64, config: &AvatarSyncConfig) {
        let mut state = self.slice_state.lock();
        state.smoothed_tick_micros = if state.smoothed_tick_micros == 0 {
            elapsed_micros
        } else {
            ((state.smoothed_tick_micros as f64 * 0.85) + (elapsed_micros as f64 * 0.15)) as u64
        };

        let min_slices = config.min_receiver_slices.max(1);
        let max_slices = config
            .max_receiver_slices
            .max(min_slices)
            .min(MAX_SLICE_COUNT);
        state.slice_count = state.slice_count.clamp(min_slices, max_slices);
        let tick_budget_micros = (config.tick_budget_ms.max(1.0) * 1000.0) as u64;
        let cycle_budget_micros = (config.receiver_cycle_budget_ms.max(1.0) * 1000.0) as u64;
        let estimated_cycle_micros = state
            .smoothed_tick_micros
            .saturating_mul(state.slice_count as u64);
        let projected_larger_cycle_micros = state
            .smoothed_tick_micros
            .saturating_mul((state.slice_count + 1) as u64);

        let mut next_slice_count = state.slice_count;
        if estimated_cycle_micros > cycle_budget_micros {
            if elapsed_micros > tick_budget_micros && state.slice_count < max_slices {
                next_slice_count = state.slice_count + 1;
            } else if state.slice_count > min_slices {
                next_slice_count = state.slice_count - 1;
            }
        } else if elapsed_micros < tick_budget_micros.saturating_mul(3) / 4
            && state.slice_count > min_slices
        {
            next_slice_count = state.slice_count - 1;
        } else if elapsed_micros > tick_budget_micros
            && projected_larger_cycle_micros <= cycle_budget_micros
            && state.slice_count < max_slices
        {
            next_slice_count = state.slice_count + 1;
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
        self.tick_budget_ms = env_f64("BASIS_AVATAR_TICK_BUDGET_MS").unwrap_or(self.tick_budget_ms);
        self.receiver_cycle_budget_ms = env_f64("BASIS_AVATAR_RECEIVER_CYCLE_BUDGET_MS")
            .unwrap_or(self.receiver_cycle_budget_ms);
        self.spatial_cull_enabled =
            env_bool("BASIS_AVATAR_SPATIAL_CULL").unwrap_or(self.spatial_cull_enabled);
        self.enable_bsr_profiling =
            env_bool("EnableBSRProfiling").unwrap_or(self.enable_bsr_profiling);
        self
    }
}

fn env_usize(name: &str) -> Option<usize> {
    env::var(name).ok()?.parse().ok()
}

fn env_f64(name: &str) -> Option<f64> {
    env::var(name).ok()?.parse().ok()
}

fn env_bool(name: &str) -> Option<bool> {
    let value = env::var(name).ok()?;
    match value.to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
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

fn emit_greedy_avatar_bundles(
    direct: &mut Vec<OutboundAvatarSend>,
    bundle: &mut Vec<BundleAvatarSend>,
    bundle_raw_bytes: &mut usize,
    bundle_ratio: &mut f32,
    profiler: &BsrProfiler,
    min_messages: usize,
    min_bytes: usize,
) {
    if bundle.is_empty() {
        return;
    }

    let mut cursor = 0usize;
    let count = bundle.len();
    let mut ratio = valid_bundle_ratio(*bundle_ratio);

    while count - cursor >= min_messages {
        let target_raw = ((AVATAR_BUNDLE_WIRE_BUDGET_BYTES as f32 * 0.95) / ratio) as usize;
        let chunk_end = pick_bundle_chunk_end(bundle, cursor, count, target_raw);
        if chunk_end <= cursor {
            break;
        }
        let raw_len = bundle_range_raw_len(bundle, cursor, chunk_end);
        if raw_len < min_bytes {
            break;
        }

        match try_emit_bundle_range(direct, bundle, cursor, chunk_end, profiler) {
            Ok(BundleEmit::Emitted {
                raw_len,
                compressed_len,
            }) => {
                update_bundle_ratio(bundle_ratio, compressed_len, raw_len, 0.3);
                ratio = valid_bundle_ratio(*bundle_ratio);
                cursor = chunk_end;
                continue;
            }
            Ok(BundleEmit::Overshot {
                raw_len,
                compressed_len,
            }) => {
                update_bundle_ratio(bundle_ratio, compressed_len, raw_len, 0.7);
                let observed = (compressed_len as f32 / raw_len.max(1) as f32)
                    .clamp(AVATAR_BUNDLE_MIN_RATIO, 0.99);
                let retry_target_raw =
                    ((AVATAR_BUNDLE_WIRE_BUDGET_BYTES as f32 * 0.92) / observed) as usize;
                let mut retry_end =
                    pick_bundle_chunk_end(bundle, cursor, chunk_end, retry_target_raw);
                if retry_end >= chunk_end {
                    retry_end = cursor + ((chunk_end - cursor) * 3 / 4).max(1);
                }
                if retry_end <= cursor {
                    break;
                }
                let retry_raw_len = bundle_range_raw_len(bundle, cursor, retry_end);
                if retry_raw_len < min_bytes {
                    break;
                }
                profiler.bundle_retries.fetch_add(1, Ordering::Relaxed);
                match try_emit_bundle_range(direct, bundle, cursor, retry_end, profiler) {
                    Ok(BundleEmit::Emitted {
                        raw_len,
                        compressed_len,
                    }) => {
                        update_bundle_ratio(bundle_ratio, compressed_len, raw_len, 0.5);
                        ratio = valid_bundle_ratio(*bundle_ratio);
                        cursor = retry_end;
                    }
                    _ => break,
                }
            }
            Err(_) => {
                profiler.add_bundle_fallback((count - cursor) as u64);
                break;
            }
        }
    }

    if cursor < count {
        profiler.add_bundle_tail_uncompressed((count - cursor) as u64);
    }
    direct.extend(bundle.drain(cursor..).map(|item| OutboundAvatarSend {
        channel: item.original_channel,
        payload: patch_interval_bytes(&item.payload, item.interval_offset, item.interval_byte),
    }));
    bundle.clear();
    *bundle_raw_bytes = 0;
}

enum BundleEmit {
    Emitted {
        raw_len: usize,
        compressed_len: usize,
    },
    Overshot {
        raw_len: usize,
        compressed_len: usize,
    },
}

fn try_emit_bundle_range(
    direct: &mut Vec<OutboundAvatarSend>,
    bundle: &[BundleAvatarSend],
    start: usize,
    end: usize,
    profiler: &BsrProfiler,
) -> Result<BundleEmit> {
    let slices = bundle[start..end]
        .iter()
        .map(|item| AvatarBundleSlice {
            original_channel: item.original_channel,
            payload: &item.payload,
            interval_patch: Some((item.interval_offset, item.interval_byte)),
        })
        .collect::<Vec<_>>();
    let deflate_start = Instant::now();
    let encoded = try_encode_avatar_bundle_slices(&slices)?;
    let deflate_micros = deflate_start.elapsed().as_micros() as u64;
    let compressed_len = encoded.compressed_len;
    if encoded.bytes.len() > AVATAR_BUNDLE_WIRE_BUDGET_BYTES {
        return Ok(BundleEmit::Overshot {
            raw_len: encoded.raw_len,
            compressed_len,
        });
    }
    direct.push(OutboundAvatarSend {
        channel: channels::COMPRESSED_AVATAR_BUNDLE,
        payload: Bytes::from(encoded.bytes),
    });
    profiler.add_bundle_emitted(
        (end - start) as u64,
        encoded.raw_len as u64,
        compressed_len as u64,
        deflate_micros,
    );
    Ok(BundleEmit::Emitted {
        raw_len: encoded.raw_len,
        compressed_len,
    })
}

fn pick_bundle_chunk_end(
    bundle: &[BundleAvatarSend],
    cursor: usize,
    hard_end: usize,
    target_raw: usize,
) -> usize {
    let mut chunk_end = cursor;
    let mut raw_accum = 0usize;
    while chunk_end < hard_end {
        let entry_size = 3 + bundle[chunk_end].payload.len();
        if chunk_end > cursor && raw_accum + entry_size > target_raw {
            break;
        }
        raw_accum += entry_size;
        chunk_end += 1;
    }
    chunk_end
}

fn bundle_range_raw_len(bundle: &[BundleAvatarSend], start: usize, end: usize) -> usize {
    bundle[start..end]
        .iter()
        .map(|item| 3 + item.payload.len())
        .sum()
}

fn valid_bundle_ratio(ratio: f32) -> f32 {
    if (AVATAR_BUNDLE_MIN_RATIO..=AVATAR_BUNDLE_MAX_RATIO).contains(&ratio) {
        ratio
    } else {
        AVATAR_BUNDLE_INITIAL_RATIO
    }
}

fn update_bundle_ratio(
    ratio: &mut f32,
    compressed_len: usize,
    raw_len: usize,
    observed_weight: f32,
) {
    if raw_len == 0 {
        return;
    }
    let observed = (compressed_len as f32 / raw_len as f32)
        .clamp(AVATAR_BUNDLE_MIN_RATIO, AVATAR_BUNDLE_MAX_RATIO);
    *ratio = (*ratio * (1.0 - observed_weight) + observed * observed_weight)
        .clamp(AVATAR_BUNDLE_MIN_RATIO, AVATAR_BUNDLE_MAX_RATIO);
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

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
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
    profiler: &BsrProfiler,
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
            profiler.add_pre_serializations(4);
        }
        other => {
            qualities[other as usize] =
                Some(pre_serialize(peer_id, outbound_sequence, other, payload));
            profiler.add_pre_serializations(1);
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

fn patch_interval_bytes(payload: &Bytes, interval_offset: usize, interval: u8) -> Bytes {
    if payload
        .get(interval_offset)
        .is_some_and(|current| *current == interval)
    {
        return payload.clone();
    }
    let mut bytes = Vec::with_capacity(payload.len());
    bytes.extend_from_slice(payload);
    if interval_offset < bytes.len() {
        bytes[interval_offset] = interval;
    }
    Bytes::from(bytes)
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
#[inline(always)]
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
    use basis_protocol::avatar::{decode_avatar_bundle, encode_avatar_bundle, AvatarBundleItem};

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
            spatial_cull_enabled: false,
            enable_bsr_profiling: false,
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
