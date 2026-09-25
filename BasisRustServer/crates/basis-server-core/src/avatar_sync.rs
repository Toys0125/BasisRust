use anyhow::Result;
use basis_protocol::{
    avatar::{
        read_position, repack_high_to_lower_into, try_encode_avatar_bundle_slices_with_compression,
        AvatarBundleCompression, AvatarBundleSlice, BitQuality,
    },
    avatar_delta::build_delta,
    channels,
};
use basis_transport::{PeerId, TransportHandle, UnreliablePacket};
use bytes::Bytes;
use dashmap::DashMap;
use rayon::prelude::*;
use std::{
    collections::HashMap,
    env,
    hash::{BuildHasherDefault, Hasher},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::runtime::Handle;
use tracing::warn;

use crate::p2p::pack_pair;

const DISTANCE_UPDATE_INTERVAL_MS: u64 = 500;
const AVATAR_TICK_INTERVAL_MS: u64 = 4;
const RECEIVER_BUILD_MIN_BATCH: usize = 16;
const RECEIVER_FLUSH_MIN_BATCH: usize = 8;
const TICK_SPIN_RESERVE_MICROS: u64 = 100;
const MAX_SLICE_COUNT: usize = 32;
const NO_RECEIVER_BASELINE: u64 = u64::MAX;
const AVATAR_BUNDLE_WIRE_BUDGET_BYTES: usize = 1100;
const AVATAR_BUNDLE_INITIAL_RATIO: f32 = 0.60;
const AVATAR_BUNDLE_MIN_RATIO: f32 = 0.05;
const AVATAR_BUNDLE_MAX_RATIO: f32 = 0.95;
const SMALL_HIGH_DELTA_BYTES: usize = 40;
const SMALL_DELTA_STREAK_TO_STRETCH: u8 = 4;
pub(crate) const DEFAULT_AVATAR_TICK_BUDGET_MS: f64 = 3.0;
pub(crate) const DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS: f64 = 180.0;

#[derive(Default)]
struct PeerIdHasher {
    state: u64,
}

impl Hasher for PeerIdHasher {
    fn finish(&self) -> u64 {
        self.state
    }

    fn write(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.state = (self.state << 8) | u64::from(*byte);
        }
    }

    fn write_u16(&mut self, value: u16) {
        self.state = u64::from(value);
    }
}

type PeerIdMap<V> = HashMap<PeerId, V, BuildHasherDefault<PeerIdHasher>>;

#[derive(Debug, Clone)]
pub struct AvatarSyncConfig {
    pub default_interval_ms: u64,
    pub base_multiplier: f32,
    pub increase_rate: f32,
    pub high_distance_sq: f32,
    pub medium_distance_sq: f32,
    pub low_distance_sq: f32,
    pub enable_bundle_compression: bool,
    pub enable_bundle_zstd: bool,
    pub bundle_zstd_delta_bundles: bool,
    pub bundle_zstd_level: i32,
    pub enable_delta_compression: bool,
    pub delta_keyframe_interval_ms: u64,
    pub delta_keyframe_max_interval_ms: u64,
    pub strip_additional_data_at_low_quality: bool,
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
    additional_data: Bytes,
}

#[derive(Debug, Clone)]
struct PreSerializedDelta {
    bytes_small: Bytes,
    bytes_large: Bytes,
}

#[derive(Debug, Clone)]
struct PlayerAvatarState {
    peer_id: PeerId,
    small_id: bool,
    position: [f32; 3],
    generation: u64,
    last_inbound_sequence: u8,
    outbound_sequence: u8,
    has_received_first: bool,
    qualities: [Option<PreSerializedQuality>; 4],
    keyframe_qualities: [Option<PreSerializedQuality>; 4],
    keyframe_payloads: [Option<Bytes>; 4],
    deltas: [Option<PreSerializedDelta>; 4],
    keyframe_generation: u64,
    keyframe_sequence: u8,
    last_keyframe: Instant,
    keyframe_stretch_shift: u8,
    small_delta_streak: u8,
    current_is_keyframe: bool,
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
    last_sent_ms: u64,
    cached_quality_index: u8,
    cached_interval_byte: u8,
    cached_interval_ms: u64,
    baseline_keyframe_generation: u64,
    baseline_quality: u8,
}

struct SpatialGrid {
    cell_size: f32,
    cells: HashMap<(i32, i32, i32), Vec<usize>>,
}

impl SpatialGrid {
    fn build(
        peer_states: &[(PeerId, Arc<PlayerAvatarState>)],
        low_distance_sq: f32,
    ) -> Option<Self> {
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
    receiver_cursor: usize,
    last_distance_update: Instant,
    smoothed_tick_micros: u64,
}

#[derive(Debug)]
enum OutboundAvatarSend<'a> {
    Borrowed {
        channel: u8,
        payload: &'a Bytes,
        patch: Option<(usize, u8)>,
    },
    Owned {
        channel: u8,
        payload: Bytes,
        patch: Option<(usize, u8)>,
    },
}

impl UnreliablePacket for OutboundAvatarSend<'_> {
    #[inline]
    fn channel(&self) -> u8 {
        match self {
            Self::Borrowed { channel, .. } | Self::Owned { channel, .. } => *channel,
        }
    }

    #[inline]
    fn payload(&self) -> &[u8] {
        match self {
            Self::Borrowed { payload, .. } => payload.as_ref(),
            Self::Owned { payload, .. } => payload.as_ref(),
        }
    }

    #[inline]
    fn interval_patch(&self) -> Option<(usize, u8)> {
        match self {
            Self::Borrowed { patch, .. } | Self::Owned { patch, .. } => *patch,
        }
    }
}

#[derive(Debug)]
struct OutboundAvatarBatch<'a> {
    receiver: PeerId,
    sends: Vec<OutboundAvatarSend<'a>>,
}

#[derive(Debug, Clone)]
struct BundleAvatarSend {
    original_channel: u8,
    payload: Bytes,
    interval_offset: usize,
    interval_byte: u8,
}

#[derive(Default)]
struct ReceiverBuildScratch {
    bundle: Vec<BundleAvatarSend>,
}

#[derive(Debug, Clone, Default)]
pub struct AvatarSyncStats {
    pub inbound_updates: u64,
    pub outbound_messages: u64,
    pub outbound_logical_avatar_sends: u64,
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
    outbound_logical_avatar_sends: AtomicU64,
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
    states: Arc<DashMap<PeerId, Arc<PlayerAvatarState>>>,
    pending: Arc<DashMap<PeerId, PendingAvatarUpdate>>,
    tracking: Arc<DashMap<PeerId, PeerIdMap<ReceiverTracking>>>,
    bundle_ratios: Arc<DashMap<PeerId, f32>>,
    generation: Arc<AtomicU64>,
    monotonic_origin: Instant,
    slice_state: Arc<parking_lot::Mutex<SliceState>>,
    payload_pool: Arc<BytePool>,
    counters: Arc<AvatarSyncCounters>,
    profiler: Arc<BsrProfiler>,
    offloaded_pairs: Arc<DashMap<u64, ()>>,
    bypass_reduction_ids: Arc<DashMap<PeerId, ()>>,
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
            monotonic_origin: Instant::now(),
            slice_state: Arc::new(parking_lot::Mutex::new(SliceState {
                slice_count: 1,
                receiver_cursor: 0,
                last_distance_update: Instant::now(),
                smoothed_tick_micros: 0,
            })),
            payload_pool: Arc::new(BytePool::new()),
            counters: Arc::new(AvatarSyncCounters::default()),
            profiler: Arc::new(BsrProfiler::new(profiler_enabled)),
            offloaded_pairs: Arc::new(DashMap::new()),
            bypass_reduction_ids: Arc::new(DashMap::new()),
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
        let fixed_end = 1 + expected;
        let retained_len = if basis_protocol::channels::channel_has_additional_data(channel) {
            validate_additional_avatar_data(&payload[fixed_end..])?;
            payload.len()
        } else {
            fixed_end
        };
        let mut pooled = self.payload_pool.take(retained_len);
        pooled.copy_from_slice(&payload[..retained_len]);
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

    #[inline]
    fn monotonic_millis(&self) -> u64 {
        self.monotonic_origin.elapsed().as_millis() as u64
    }

    pub fn set_bypass_reduction(&self, sender_id: PeerId, enabled: bool) {
        if enabled {
            self.bypass_reduction_ids.insert(sender_id, ());
        } else {
            self.bypass_reduction_ids.remove(&sender_id);
        }
        for mut receiver in self.tracking.iter_mut() {
            if let Some(tracking) = receiver.value_mut().get_mut(&sender_id) {
                tracking.baseline_keyframe_generation = 0;
                tracking.baseline_quality = u8::MAX;
                tracking.last_seen_generation = 0;
                tracking.last_sent_ms = 0;
            }
        }
    }

    pub fn request_keyframe(&self, sender_id: PeerId, receiver_id: PeerId) {
        if let Some(mut receiver) = self.tracking.get_mut(&receiver_id) {
            if let Some(tracking) = receiver.get_mut(&sender_id) {
                tracking.baseline_keyframe_generation = 0;
                tracking.baseline_quality = u8::MAX;
                tracking.last_seen_generation = 0;
                tracking.last_sent_ms = 0;
            }
        }
    }

    pub fn remove_player(&self, peer_id: PeerId) {
        self.states.remove(&peer_id);
        if let Some((_, pending)) = self.pending.remove(&peer_id) {
            self.payload_pool.put(pending.payload);
        }
        self.tracking.remove(&peer_id);
        self.bundle_ratios.remove(&peer_id);
        self.bypass_reduction_ids.remove(&peer_id);
        for mut entry in self.tracking.iter_mut() {
            entry.value_mut().remove(&peer_id);
        }
    }

    pub fn player_position(&self, peer_id: PeerId) -> Option<[f32; 3]> {
        self.states.get(&peer_id).map(|state| state.position)
    }

    pub fn stats(&self) -> AvatarSyncStats {
        let state = self.slice_state.lock();
        let config = self.config.read();
        let tick_count = self.counters.tick_count.load(Ordering::Relaxed);
        let avg_tick_micros = self
            .counters
            .tick_micros
            .load(Ordering::Relaxed)
            .checked_div(tick_count)
            .unwrap_or(0);
        AvatarSyncStats {
            inbound_updates: self.counters.inbound_updates.load(Ordering::Relaxed),
            outbound_messages: self.counters.outbound_messages.load(Ordering::Relaxed),
            outbound_logical_avatar_sends: self
                .counters
                .outbound_logical_avatar_sends
                .load(Ordering::Relaxed),
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
                let tick = Duration::from_millis(AVATAR_TICK_INTERVAL_MS);
                let spin_reserve = Duration::from_micros(TICK_SPIN_RESERVE_MICROS);
                while !shutdown.load(Ordering::Relaxed) {
                    let started = Instant::now();
                    if let Err(err) =
                        runtime.block_on(system.flush_tick(&transport, &peer_snapshot))
                    {
                        warn!("avatar sync tick failed: {err:#}");
                    }
                    let elapsed = started.elapsed();
                    if elapsed + spin_reserve < tick {
                        thread::sleep(tick - elapsed - spin_reserve);
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
        let now_ms = self.monotonic_millis();
        let messages_processed = self.process_pending_updates(&config);
        let peers = peer_snapshot();
        if peers.len() <= 1 {
            return Ok(());
        }
        let mut peer_states = Vec::with_capacity(peers.len());
        for peer in &peers {
            if let Some(state) = self.states.get(peer) {
                peer_states.push((*peer, Arc::clone(state.value())));
            }
        }
        let (slice_count, slice_start, slice_end, update_distances, effective_tick_interval_ms) =
            self.advance_slice_state(peer_states.len());
        let spatial_grid = if config.spatial_cull_enabled {
            SpatialGrid::build(&peer_states, config.low_distance_sq)
        } else {
            None
        };
        self.profiler.add_phase_micros(BsrPhase::Distance, 0);

        let build_start = Instant::now();
        // Avoid cloning states for receiver_states: par_iter over slice directly.
        // build_sends_for_receiver only needs position from receiver state.
        let receiver_slice =
            &peer_states[slice_start.min(peer_states.len())..slice_end.min(peer_states.len())];
        let offloaded_empty = self.offloaded_pairs.is_empty();
        let bypass_empty = self.bypass_reduction_ids.is_empty();
        let receiver_groups = receiver_slice
            .par_iter()
            .with_min_len(RECEIVER_BUILD_MIN_BATCH)
            .map_init(
                ReceiverBuildScratch::default,
                |scratch, (receiver_id, receiver_state)| {
                    self.build_sends_for_receiver(
                        *receiver_id,
                        receiver_state.position,
                        &peer_states,
                        spatial_grid.as_ref(),
                        &config,
                        now_ms,
                        slice_count,
                        effective_tick_interval_ms,
                        update_distances,
                        offloaded_empty,
                        bypass_empty,
                        scratch,
                    )
                },
            )
            .filter_map(|batch| batch)
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
        flush_receiver_groups_parallel(transport.clone(), receiver_groups)?;
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

    fn process_pending_updates(&self, config: &AvatarSyncConfig) -> usize {
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
            let additional_data = &update.payload[1 + update.payload_len..];
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
                let current = Arc::make_mut(&mut current);
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
                    additional_data,
                    config.strip_additional_data_at_low_quality,
                ) {
                    update_outbound_delta_state(
                        current,
                        qualities,
                        generation,
                        config,
                        Instant::now(),
                    );
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
                additional_data,
                config.strip_additional_data_at_low_quality,
            ) {
                let now = Instant::now();
                let keyframe_payloads = quality_payloads(&qualities);
                self.states.insert(
                    update.peer_id,
                    Arc::new(PlayerAvatarState {
                        peer_id: update.peer_id,
                        small_id: update.peer_id <= u8::MAX as u16,
                        position: update.position,
                        generation,
                        last_inbound_sequence: update.inbound_sequence,
                        outbound_sequence: 0,
                        has_received_first: true,
                        keyframe_qualities: qualities.clone(),
                        keyframe_payloads,
                        deltas: [None, None, None, None],
                        keyframe_generation: generation,
                        keyframe_sequence: 0,
                        last_keyframe: now,
                        keyframe_stretch_shift: 0,
                        small_delta_streak: 0,
                        current_is_keyframe: true,
                        qualities,
                    }),
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

    #[allow(clippy::too_many_arguments)]
    fn build_sends_for_receiver<'a>(
        &self,
        receiver_id: PeerId,
        receiver_position: [f32; 3],
        peer_states: &'a [(PeerId, Arc<PlayerAvatarState>)],
        spatial_grid: Option<&SpatialGrid>,
        config: &AvatarSyncConfig,
        now_ms: u64,
        slice_count: usize,
        effective_tick_interval_ms: u64,
        update_distances: bool,
        offloaded_empty: bool,
        bypass_empty: bool,
        scratch: &mut ReceiverBuildScratch,
    ) -> Option<OutboundAvatarBatch<'a>> {
        let peer_count = peer_states.len();
        let mut spatial_candidates: Option<Vec<usize>> = None;
        if let Some(grid) = spatial_grid {
            spatial_candidates = Some(grid.ordered_indices(receiver_position, peer_count));
        }
        let initial_send_capacity = peer_count.saturating_sub(1);
        let mut direct = if config.enable_bundle_compression {
            Vec::new()
        } else {
            Vec::with_capacity(initial_send_capacity)
        };
        let bundle = &mut scratch.bundle;
        bundle.clear();
        if config.enable_bundle_compression && bundle.capacity() < initial_send_capacity {
            // Keep the existing sender-count capacity policy, but reserve it once per Rayon
            // folder instead of allocating a new vector for every receiver in that folder.
            bundle.reserve_exact(initial_send_capacity);
        }
        let mut receiver_tracking = self.tracking.entry(receiver_id).or_default();
        let mut logical_sends = 0u64;
        let mut bundle_ratio = if config.enable_bundle_compression {
            self.bundle_ratios
                .get(&receiver_id)
                .map(|ratio| *ratio)
                .unwrap_or(AVATAR_BUNDLE_INITIAL_RATIO)
        } else {
            AVATAR_BUNDLE_INITIAL_RATIO
        };
        let minimum_interval_ms = config.default_interval_ms.max(1);
        for offset in 0..peer_count {
            let peer_index = match &spatial_candidates {
                Some(indices) => indices[offset],
                None => offset,
            };
            let (sender_id, sender_state) = &peer_states[peer_index];
            let sender_id = *sender_id;
            if sender_id == receiver_id {
                continue;
            }
            if !offloaded_empty
                && self
                    .offloaded_pairs
                    .contains_key(&pack_pair(receiver_id, sender_id))
            {
                continue;
            }
            let bypass_reduction =
                !bypass_empty && self.bypass_reduction_ids.contains_key(&sender_id);
            let tracking = receiver_tracking.entry(sender_id).or_insert_with(|| {
                let dist_sq = distance_sq_position(receiver_position, sender_state.position);
                let (interval_byte, interval_ms) =
                    calculate_interval_from_distance_sq(dist_sq, config);
                ReceiverTracking {
                    last_seen_generation: 0,
                    last_sent_ms: 0,
                    cached_quality_index: quality_from_distance_sq(dist_sq, config),
                    cached_interval_byte: interval_byte,
                    cached_interval_ms: interval_ms,
                    baseline_keyframe_generation: 0,
                    baseline_quality: u8::MAX,
                }
            });
            if update_distances {
                let dist_sq = distance_sq_position(receiver_position, sender_state.position);
                tracking.cached_quality_index = quality_from_distance_sq(dist_sq, config);
                let (interval_byte, interval_ms) =
                    calculate_interval_from_distance_sq(dist_sq, config);
                tracking.cached_interval_byte = interval_byte;
                tracking.cached_interval_ms = interval_ms;
            }
            let quality_index = if bypass_reduction {
                BitQuality::High as u8
            } else {
                tracking.cached_quality_index
            };
            if sender_state.generation <= tracking.last_seen_generation {
                continue;
            }
            if !bypass_reduction
                && tracking.last_seen_generation != 0
                && now_ms.saturating_sub(tracking.last_sent_ms)
                    < tracking.cached_interval_ms.max(minimum_interval_ms)
            {
                continue;
            }
            let Some(current_packet) = sender_state.qualities[quality_index as usize].as_ref()
            else {
                continue;
            };
            tracking.last_seen_generation = sender_state.generation;
            tracking.last_sent_ms = now_ms;
            let interval_byte = if bypass_reduction {
                0
            } else {
                advertised_interval_byte(
                    tracking.cached_interval_byte,
                    tracking.cached_interval_ms,
                    slice_count,
                    effective_tick_interval_ms,
                    config.default_interval_ms,
                )
            };

            let send_delta = config.enable_delta_compression
                && !bypass_reduction
                && !sender_state.current_is_keyframe
                && tracking.baseline_keyframe_generation == sender_state.keyframe_generation
                && tracking.baseline_quality == quality_index
                && sender_state.deltas[quality_index as usize].is_some();

            let (channel, packet_bytes, interval_offset) = if send_delta {
                let delta = sender_state.deltas[quality_index as usize]
                    .as_ref()
                    .expect("checked above");
                if sender_state.small_id {
                    (channels::DELTA_AVATAR, &delta.bytes_small, 2)
                } else {
                    (channels::DELTA_AVATAR, &delta.bytes_large, 3)
                }
            } else {
                // A receiver can need a full frame after a quality/baseline transition even
                // though the sender's newest state is already newer than the global keyframe.
                // Re-sending that older keyframe makes additional avatar data move backwards.
                // Send the current full frame instead and keep deltas paused until the next
                // real keyframe establishes a baseline both sides agree on.
                let current_is_newer_than_keyframe = config.enable_delta_compression
                    && !bypass_reduction
                    && !sender_state.current_is_keyframe
                    && sender_state.generation != sender_state.keyframe_generation;
                let packet = if config.enable_delta_compression && !bypass_reduction {
                    if current_is_newer_than_keyframe {
                        current_packet
                    } else {
                        let Some(keyframe) =
                            sender_state.keyframe_qualities[quality_index as usize].as_ref()
                        else {
                            continue;
                        };
                        keyframe
                    }
                } else {
                    current_packet
                };
                if config.enable_delta_compression && !bypass_reduction {
                    tracking.baseline_keyframe_generation = if current_is_newer_than_keyframe {
                        NO_RECEIVER_BASELINE
                    } else {
                        sender_state.keyframe_generation
                    };
                    tracking.baseline_quality = quality_index;
                }
                if sender_state.small_id {
                    (packet.channel_small, &packet.bytes_small, 1)
                } else {
                    (packet.channel_large, &packet.bytes_large, 2)
                }
            };
            logical_sends += 1;

            if config.enable_bundle_compression {
                bundle.push(BundleAvatarSend {
                    original_channel: channel,
                    payload: packet_bytes.clone(),
                    interval_offset,
                    interval_byte,
                });
            } else {
                direct.push(OutboundAvatarSend::Borrowed {
                    channel,
                    payload: packet_bytes,
                    patch: Some((interval_offset, interval_byte)),
                });
            }
        }

        if config.enable_bundle_compression {
            emit_greedy_avatar_bundles(
                &mut direct,
                bundle,
                &mut bundle_ratio,
                &self.profiler,
                config,
            );
            self.bundle_ratios.insert(receiver_id, bundle_ratio);
        }
        self.profiler.add_sends(logical_sends);
        self.counters
            .outbound_logical_avatar_sends
            .fetch_add(logical_sends as u64, Ordering::Relaxed);
        // Only the independently owned `direct` sends escape this receiver build. In
        // particular, release every Bytes clone held by scratch before it is reused.
        bundle.clear();
        (!direct.is_empty()).then_some(OutboundAvatarBatch {
            receiver: receiver_id,
            sends: direct,
        })
    }

    fn advance_slice_state(&self, receiver_count: usize) -> (usize, usize, usize, bool, u64) {
        let mut state = self.slice_state.lock();
        let now = Instant::now();
        let slice_count = state.slice_count.max(1);
        let (slice_start, slice_end, next_cursor) =
            next_receiver_slice(receiver_count, slice_count, state.receiver_cursor);
        state.receiver_cursor = next_cursor;
        let update_distances = now.duration_since(state.last_distance_update)
            >= Duration::from_millis(DISTANCE_UPDATE_INTERVAL_MS);
        if update_distances {
            state.last_distance_update = now;
        }
        let effective_tick_interval_ms =
            AVATAR_TICK_INTERVAL_MS.max(state.smoothed_tick_micros.div_ceil(1_000));
        (
            slice_count,
            slice_start,
            slice_end,
            update_distances,
            effective_tick_interval_ms,
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
            if elapsed_micros > tick_budget_micros {
                if state.slice_count < max_slices {
                    next_slice_count = state.slice_count + 1;
                }
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
        }
    }
}

fn next_receiver_slice(
    receiver_count: usize,
    slice_count: usize,
    cursor: usize,
) -> (usize, usize, usize) {
    if receiver_count == 0 {
        return (0, 0, 0);
    }

    let start = if cursor >= receiver_count { 0 } else { cursor };
    let slice_size = receiver_count.div_ceil(slice_count.max(1));
    let end = start.saturating_add(slice_size).min(receiver_count);
    let next_cursor = if end == receiver_count { 0 } else { end };
    (start, end, next_cursor)
}

impl AvatarSyncConfig {
    pub fn apply_env_tuning(mut self) -> Self {
        self.min_receiver_slices = env_usize("BASIS_AVATAR_MIN_RECEIVER_SLICES")
            .unwrap_or(self.min_receiver_slices)
            .clamp(1, MAX_SLICE_COUNT);
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

fn flush_receiver_groups_parallel<'a>(
    transport: TransportHandle,
    receiver_groups: Vec<OutboundAvatarBatch<'a>>,
) -> Result<()> {
    receiver_groups
        .par_iter()
        .with_min_len(RECEIVER_FLUSH_MIN_BATCH)
        .try_for_each(|batch| {
            transport
                .try_send_many_unreliable_packets(batch.receiver, &batch.sends)
                .map(|_| ())
        })?;
    Ok(())
}

fn emit_greedy_avatar_bundles<'a>(
    direct: &mut Vec<OutboundAvatarSend<'a>>,
    bundle: &mut Vec<BundleAvatarSend>,
    bundle_ratio: &mut f32,
    profiler: &BsrProfiler,
    config: &AvatarSyncConfig,
) {
    if bundle.is_empty() {
        return;
    }

    let mut cursor = 0usize;
    let count = bundle.len();
    let mut ratio = valid_bundle_ratio(*bundle_ratio);

    while count - cursor >= config.bundle_min_messages {
        let target_raw = ((AVATAR_BUNDLE_WIRE_BUDGET_BYTES as f32 * 0.95) / ratio) as usize;
        let chunk_end = pick_bundle_chunk_end(bundle, cursor, count, target_raw);
        if chunk_end <= cursor {
            break;
        }
        let raw_len = bundle_range_raw_len(bundle, cursor, chunk_end);
        if raw_len < config.bundle_min_bytes {
            break;
        }

        match try_emit_bundle_range(direct, bundle, cursor, chunk_end, profiler, config) {
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
                if retry_raw_len < config.bundle_min_bytes {
                    break;
                }
                profiler.bundle_retries.fetch_add(1, Ordering::Relaxed);
                match try_emit_bundle_range(direct, bundle, cursor, retry_end, profiler, config) {
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
    direct.extend(
        bundle
            .drain(cursor..)
            .map(|item| OutboundAvatarSend::Owned {
                channel: item.original_channel,
                payload: patch_interval_bytes(
                    &item.payload,
                    item.interval_offset,
                    item.interval_byte,
                ),
                patch: None,
            }),
    );
    bundle.clear();
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

fn try_emit_bundle_range<'a>(
    direct: &mut Vec<OutboundAvatarSend<'a>>,
    bundle: &[BundleAvatarSend],
    start: usize,
    end: usize,
    profiler: &BsrProfiler,
    config: &AvatarSyncConfig,
) -> Result<BundleEmit> {
    let slices = bundle[start..end]
        .iter()
        .map(|item| AvatarBundleSlice {
            original_channel: item.original_channel,
            payload: &item.payload,
            interval_patch: Some((item.interval_offset, item.interval_byte)),
        })
        .collect::<Vec<_>>();
    let delta_only = bundle[start..end]
        .iter()
        .all(|item| item.original_channel == channels::DELTA_AVATAR);
    let compression =
        if config.enable_bundle_zstd && (config.bundle_zstd_delta_bundles || !delta_only) {
            AvatarBundleCompression::ZstdDictionary {
                level: config.bundle_zstd_level,
            }
        } else {
            AvatarBundleCompression::Lz4
        };
    let deflate_start = Instant::now();
    let encoded = try_encode_avatar_bundle_slices_with_compression(&slices, compression)?;
    let deflate_micros = deflate_start.elapsed().as_micros() as u64;
    let compressed_len = encoded.compressed_len;
    if encoded.bytes.len() > AVATAR_BUNDLE_WIRE_BUDGET_BYTES {
        return Ok(BundleEmit::Overshot {
            raw_len: encoded.raw_len,
            compressed_len,
        });
    }
    direct.push(OutboundAvatarSend::Owned {
        channel: channels::COMPRESSED_AVATAR_BUNDLE,
        payload: Bytes::from(encoded.bytes),
        patch: None,
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

fn validate_additional_avatar_data(bytes: &[u8]) -> Result<()> {
    if bytes.is_empty() {
        anyhow::bail!("avatar channel marks additional data but the section is missing");
    }
    let count = bytes[0] as usize;
    if count == 0 {
        anyhow::ensure!(
            bytes.len() == 1,
            "trailing bytes after empty additional avatar data section"
        );
        return Ok(());
    }
    anyhow::ensure!(bytes.len() >= 2, "missing linked avatar index");
    let mut offset = 2usize;
    for _ in 0..count {
        anyhow::ensure!(offset < bytes.len(), "missing additional avatar data size");
        let len = bytes[offset] as usize;
        offset += 1;
        if len == 0 {
            continue;
        }
        anyhow::ensure!(
            offset < bytes.len(),
            "missing additional avatar data message index"
        );
        offset += 1;
        anyhow::ensure!(
            offset + len <= bytes.len(),
            "truncated additional avatar data payload"
        );
        offset += len;
    }
    anyhow::ensure!(
        offset == bytes.len(),
        "trailing bytes after additional avatar data section"
    );
    Ok(())
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
    debug_assert!(update.payload.len() > expected);
    let payload_len = expected.min(update.payload.len().saturating_sub(1));
    let avatar_payload = &update.payload[1..1 + payload_len];
    let position = read_position(avatar_payload).unwrap_or([0.0, 0.0, 0.0]);
    ProcessedAvatarUpdate {
        peer_id,
        inbound_sequence,
        position,
        quality,
        payload: update.payload,
        payload_len,
    }
}

fn quality_payloads(qualities: &[Option<PreSerializedQuality>; 4]) -> [Option<Bytes>; 4] {
    std::array::from_fn(|index| {
        qualities[index].as_ref().and_then(|packet| {
            let quality = match index {
                0 => BitQuality::VeryLow,
                1 => BitQuality::Low,
                2 => BitQuality::Medium,
                _ => BitQuality::High,
            };
            let end = 3 + quality.payload_len();
            (packet.bytes_small.len() >= end).then(|| packet.bytes_small.slice(3..end))
        })
    })
}

fn effective_keyframe_interval_ms(config: &AvatarSyncConfig, stretch_shift: u8) -> u64 {
    let base_ms = config.delta_keyframe_interval_ms.max(1);
    let max_ms = config.delta_keyframe_max_interval_ms;
    if max_ms <= base_ms || stretch_shift == 0 {
        return base_ms;
    }
    let shift = stretch_shift.min(8) as u32;
    base_ms.checked_shl(shift).unwrap_or(u64::MAX).min(max_ms)
}

fn update_keyframe_stretch(
    state: &mut PlayerAvatarState,
    config: &AvatarSyncConfig,
    high_delta_len: usize,
) {
    if high_delta_len > SMALL_HIGH_DELTA_BYTES {
        state.keyframe_stretch_shift = 0;
        state.small_delta_streak = 0;
        return;
    }
    if effective_keyframe_interval_ms(config, state.keyframe_stretch_shift.saturating_add(1))
        == effective_keyframe_interval_ms(config, state.keyframe_stretch_shift)
    {
        return;
    }
    state.small_delta_streak = state.small_delta_streak.saturating_add(1);
    if state.small_delta_streak >= SMALL_DELTA_STREAK_TO_STRETCH {
        state.small_delta_streak = 0;
        state.keyframe_stretch_shift = state.keyframe_stretch_shift.saturating_add(1);
    }
}

fn update_outbound_delta_state(
    state: &mut PlayerAvatarState,
    qualities: [Option<PreSerializedQuality>; 4],
    generation: u64,
    config: &AvatarSyncConfig,
    now: Instant,
) {
    let current_payloads = quality_payloads(&qualities);
    let keyframe_interval = effective_keyframe_interval_ms(config, state.keyframe_stretch_shift);
    let mut is_keyframe = !config.enable_delta_compression
        || state.keyframe_payloads[BitQuality::High as usize].is_none()
        || now.duration_since(state.last_keyframe)
            >= Duration::from_millis(keyframe_interval.max(1));

    let high_delta = if !is_keyframe {
        match (
            state.keyframe_payloads[BitQuality::High as usize].as_ref(),
            current_payloads[BitQuality::High as usize].as_ref(),
        ) {
            (Some(baseline), Some(current)) => {
                match build_delta(baseline.as_ref(), current.as_ref(), BitQuality::High) {
                    Ok(delta) if delta.len() < BitQuality::High.payload_len() => {
                        update_keyframe_stretch(state, config, delta.len());
                        Some(delta)
                    }
                    Ok(_) | Err(_) => {
                        is_keyframe = true;
                        state.keyframe_stretch_shift = 0;
                        state.small_delta_streak = 0;
                        None
                    }
                }
            }
            _ => {
                is_keyframe = true;
                None
            }
        }
    } else {
        None
    };

    if is_keyframe {
        state.keyframe_qualities = qualities.clone();
        state.keyframe_payloads = current_payloads;
        state.deltas = [None, None, None, None];
        state.keyframe_generation = generation;
        state.keyframe_sequence = state.outbound_sequence;
        state.last_keyframe = now;
        state.current_is_keyframe = true;
    } else {
        state.deltas = build_delta_packets(
            state.peer_id,
            state.outbound_sequence,
            state.keyframe_sequence,
            &state.keyframe_payloads,
            &current_payloads,
            &qualities,
            high_delta.as_deref(),
        );
        state.current_is_keyframe = false;
    }
    state.qualities = qualities;
}

fn build_delta_packets(
    peer_id: PeerId,
    outbound_sequence: u8,
    base_sequence: u8,
    baselines: &[Option<Bytes>; 4],
    current: &[Option<Bytes>; 4],
    qualities: &[Option<PreSerializedQuality>; 4],
    reusable_high_delta: Option<&[u8]>,
) -> [Option<PreSerializedDelta>; 4] {
    std::array::from_fn(|index| {
        let baseline = baselines[index].as_ref()?;
        let current = current[index].as_ref()?;
        let quality = match index {
            0 => BitQuality::VeryLow,
            1 => BitQuality::Low,
            2 => BitQuality::Medium,
            _ => BitQuality::High,
        };
        let body = if quality == BitQuality::High {
            if let Some(delta) = reusable_high_delta {
                std::borrow::Cow::Borrowed(delta)
            } else {
                std::borrow::Cow::Owned(
                    build_delta(baseline.as_ref(), current.as_ref(), quality).ok()?,
                )
            }
        } else {
            std::borrow::Cow::Owned(build_delta(baseline.as_ref(), current.as_ref(), quality).ok()?)
        };
        let additional_data = qualities[index]
            .as_ref()
            .map(|packet| packet.additional_data.as_ref())
            .unwrap_or(&[]);
        Some(pre_serialize_delta(
            peer_id,
            outbound_sequence,
            base_sequence,
            quality,
            body.as_ref(),
            additional_data,
        ))
    })
}

fn pre_serialize_delta(
    peer_id: PeerId,
    outbound_sequence: u8,
    base_sequence: u8,
    quality: BitQuality,
    body: &[u8],
    additional_data: &[u8],
) -> PreSerializedDelta {
    let has_additional = !additional_data.is_empty();
    let header = quality as u8
        | if has_additional {
            channels::DELTA_HEADER_ADDITIONAL_DATA
        } else {
            0
        };
    let mut small = Vec::with_capacity(5 + body.len() + additional_data.len());
    small.push(header);
    small.push(peer_id as u8);
    small.push(0); // interval placeholder
    small.push(outbound_sequence);
    small.push(base_sequence);
    small.extend_from_slice(body);
    small.extend_from_slice(additional_data);

    let mut large = Vec::with_capacity(6 + body.len() + additional_data.len());
    large.push(header | channels::DELTA_HEADER_LARGE_ID);
    large.extend_from_slice(&peer_id.to_le_bytes());
    large.push(0); // interval placeholder
    large.push(outbound_sequence);
    large.push(base_sequence);
    large.extend_from_slice(body);
    large.extend_from_slice(additional_data);

    PreSerializedDelta {
        bytes_small: Bytes::from(small),
        bytes_large: Bytes::from(large),
    }
}

#[allow(clippy::too_many_arguments)]
fn build_quality_packets(
    pool: &BytePool,
    profiler: &BsrProfiler,
    peer_id: PeerId,
    outbound_sequence: u8,
    inbound_quality: BitQuality,
    payload: &[u8],
    additional_data: &[u8],
    strip_additional_data_at_low_quality: bool,
) -> Result<[Option<PreSerializedQuality>; 4]> {
    let mut qualities: [Option<PreSerializedQuality>; 4] = [None, None, None, None];
    match inbound_quality {
        BitQuality::High => {
            qualities[BitQuality::High as usize] = Some(pre_serialize(
                peer_id,
                outbound_sequence,
                BitQuality::High,
                payload,
                additional_data,
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
                additional_data,
            ));
            let low_additional = if strip_additional_data_at_low_quality {
                &[][..]
            } else {
                additional_data
            };
            qualities[BitQuality::Low as usize] = Some(pre_serialize(
                peer_id,
                outbound_sequence,
                BitQuality::Low,
                &low,
                low_additional,
            ));
            qualities[BitQuality::VeryLow as usize] = Some(pre_serialize(
                peer_id,
                outbound_sequence,
                BitQuality::VeryLow,
                &very_low,
                low_additional,
            ));
            pool.put(medium);
            pool.put(low);
            pool.put(very_low);
            profiler.add_pre_serializations(4);
        }
        other => {
            let target_additional = if strip_additional_data_at_low_quality
                && matches!(other, BitQuality::Low | BitQuality::VeryLow)
            {
                &[][..]
            } else {
                additional_data
            };
            qualities[other as usize] = Some(pre_serialize(
                peer_id,
                outbound_sequence,
                other,
                payload,
                target_additional,
            ));
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
    additional_data: &[u8],
) -> PreSerializedQuality {
    let has_additional = !additional_data.is_empty();
    let channel_small =
        basis_protocol::channels::player_avatar_channel_for_quality(quality as u8, has_additional);
    let channel_large = basis_protocol::channels::player_avatar_large_channel_for_quality(
        quality as u8,
        has_additional,
    );
    let mut bytes_small = Vec::with_capacity(3 + payload.len() + additional_data.len());
    bytes_small.push(peer_id as u8);
    bytes_small.push(0);
    bytes_small.push(outbound_sequence);
    bytes_small.extend_from_slice(payload);
    bytes_small.extend_from_slice(additional_data);

    let mut bytes_large = Vec::with_capacity(4 + payload.len() + additional_data.len());
    bytes_large.extend_from_slice(&peer_id.to_le_bytes());
    bytes_large.push(0);
    bytes_large.push(outbound_sequence);
    bytes_large.extend_from_slice(payload);
    bytes_large.extend_from_slice(additional_data);

    PreSerializedQuality {
        channel_small,
        channel_large,
        bytes_small: Bytes::from(bytes_small),
        bytes_large: Bytes::from(bytes_large),
        additional_data: Bytes::copy_from_slice(additional_data),
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
fn distance_sq_position(receiver: [f32; 3], sender: [f32; 3]) -> f32 {
    let dx = receiver[0] - sender[0];
    let dy = receiver[1] - sender[1];
    let dz = receiver[2] - sender[2];
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
    let base_interval_ms = config.default_interval_ms.max(1) as i32;
    let raw_interval = (base_interval_ms as f32
        * (config.base_multiplier + distance_sq * config.increase_rate))
        as i32;
    let interval_byte = channels::encode_avatar_interval_byte(raw_interval, base_interval_ms);
    let actual_interval =
        channels::decode_avatar_interval_ms(interval_byte, base_interval_ms).max(1) as u64;
    (interval_byte, actual_interval)
}

fn advertised_interval_byte(
    cached_interval_byte: u8,
    cached_interval_ms: u64,
    slice_count: usize,
    effective_tick_interval_ms: u64,
    base_interval_ms: u64,
) -> u8 {
    let deliverable_interval_ms = effective_tick_interval_ms
        .max(AVATAR_TICK_INTERVAL_MS)
        .saturating_mul(slice_count.max(1) as u64);
    if deliverable_interval_ms <= cached_interval_ms {
        return cached_interval_byte;
    }

    channels::encode_avatar_interval_byte(
        deliverable_interval_ms.min(i32::MAX as u64) as i32,
        base_interval_ms.max(1).min(i32::MAX as u64) as i32,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use basis_protocol::avatar::{decode_avatar_bundle, encode_avatar_bundle, AvatarBundleItem};

    fn receiver_build_test_config() -> AvatarSyncConfig {
        AvatarSyncConfig {
            default_interval_ms: 1,
            base_multiplier: 1.0,
            increase_rate: 0.005,
            high_distance_sq: 1.0e6,
            medium_distance_sq: 1.0e7,
            low_distance_sq: 1.0e8,
            enable_bundle_compression: true,
            enable_bundle_zstd: false,
            bundle_zstd_delta_bundles: false,
            bundle_zstd_level: -2,
            enable_delta_compression: false,
            delta_keyframe_interval_ms: 500,
            delta_keyframe_max_interval_ms: 2000,
            strip_additional_data_at_low_quality: true,
            bundle_min_messages: 4,
            bundle_min_bytes: 128,
            min_receiver_slices: 1,
            max_receiver_slices: MAX_SLICE_COUNT,
            tick_budget_ms: DEFAULT_AVATAR_TICK_BUDGET_MS,
            receiver_cycle_budget_ms: DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS,
            spatial_cull_enabled: false,
            enable_bsr_profiling: true,
        }
    }

    fn reference_build_delta_packets(
        peer_id: PeerId,
        outbound_sequence: u8,
        base_sequence: u8,
        baselines: &[Option<Bytes>; 4],
        current: &[Option<Bytes>; 4],
        qualities: &[Option<PreSerializedQuality>; 4],
    ) -> [Option<PreSerializedDelta>; 4] {
        std::array::from_fn(|index| {
            let baseline = baselines[index].as_ref()?;
            let current = current[index].as_ref()?;
            let quality = match index {
                0 => BitQuality::VeryLow,
                1 => BitQuality::Low,
                2 => BitQuality::Medium,
                _ => BitQuality::High,
            };
            let body = build_delta(baseline.as_ref(), current.as_ref(), quality).ok()?;
            let additional_data = qualities[index]
                .as_ref()
                .map(|packet| packet.additional_data.as_ref())
                .unwrap_or(&[]);
            Some(pre_serialize_delta(
                peer_id,
                outbound_sequence,
                base_sequence,
                quality,
                &body,
                additional_data,
            ))
        })
    }

    fn delta_test_qualities(
        peer_id: PeerId,
        sequence: u8,
        payload_byte: u8,
        with_additional_data: bool,
    ) -> [Option<PreSerializedQuality>; 4] {
        std::array::from_fn(|index| {
            let quality = match index {
                0 => BitQuality::VeryLow,
                1 => BitQuality::Low,
                2 => BitQuality::Medium,
                _ => BitQuality::High,
            };
            let payload = vec![payload_byte; quality.payload_len()];
            let additional = if with_additional_data {
                if matches!(quality, BitQuality::Low | BitQuality::VeryLow) {
                    Vec::new()
                } else {
                    vec![0]
                }
            } else {
                Vec::new()
            };
            Some(pre_serialize(
                peer_id,
                sequence,
                quality,
                &payload,
                &additional,
            ))
        })
    }

    fn delta_test_payloads(qualities: &[Option<PreSerializedQuality>; 4]) -> [Option<Bytes>; 4] {
        quality_payloads(qualities)
    }

    fn receiver_build_test_peers(
        changed: &[(PeerId, u64)],
    ) -> Vec<(PeerId, Arc<PlayerAvatarState>)> {
        (1..=8)
            .map(|id| id as PeerId)
            .chain([300])
            .map(|peer_id| {
                let generation = changed
                    .iter()
                    .find_map(|(id, generation)| (*id == peer_id).then_some(*generation))
                    .unwrap_or(1);
                let qualities = std::array::from_fn(|index| {
                    let quality = match index {
                        0 => BitQuality::VeryLow,
                        1 => BitQuality::Low,
                        2 => BitQuality::Medium,
                        _ => BitQuality::High,
                    };
                    let payload = (0..quality.payload_len())
                        .map(|offset| (offset as u8).wrapping_mul(31).wrapping_add(peer_id as u8))
                        .collect::<Vec<_>>();
                    Some(pre_serialize(
                        peer_id,
                        generation as u8,
                        quality,
                        &payload,
                        &[],
                    ))
                });
                (
                    peer_id,
                    Arc::new(PlayerAvatarState {
                        peer_id,
                        small_id: peer_id <= u8::MAX as PeerId,
                        position: [peer_id as f32, 0.0, 0.0],
                        generation,
                        last_inbound_sequence: generation as u8,
                        outbound_sequence: generation as u8,
                        has_received_first: true,
                        qualities: qualities.clone(),
                        keyframe_qualities: qualities,
                        keyframe_payloads: [None, None, None, None],
                        deltas: [None, None, None, None],
                        keyframe_generation: generation,
                        keyframe_sequence: generation as u8,
                        last_keyframe: Instant::now(),
                        keyframe_stretch_shift: 0,
                        small_delta_streak: 0,
                        current_is_keyframe: true,
                    }),
                )
            })
            .collect()
    }

    type AvatarBatchSnapshot = (PeerId, Vec<(u8, Vec<u8>, Option<(usize, u8)>)>);

    fn snapshot_avatar_batch(
        batch: Option<&OutboundAvatarBatch<'_>>,
    ) -> Option<AvatarBatchSnapshot> {
        batch.map(|batch| {
            (
                batch.receiver,
                batch
                    .sends
                    .iter()
                    .map(|send| {
                        (
                            send.channel(),
                            send.payload().to_vec(),
                            send.interval_patch(),
                        )
                    })
                    .collect(),
            )
        })
    }

    fn build_fresh_and_reused<'a>(
        fresh_system: &AvatarSyncSystem,
        reused_system: &AvatarSyncSystem,
        scratch: &mut ReceiverBuildScratch,
        config: &AvatarSyncConfig,
        peers: &'a [(PeerId, Arc<PlayerAvatarState>)],
        receiver: PeerId,
        now_ms: u64,
    ) -> (Option<AvatarBatchSnapshot>, Option<OutboundAvatarBatch<'a>>) {
        let receiver_position = peers
            .iter()
            .find(|(id, _)| *id == receiver)
            .unwrap()
            .1
            .position;
        let build = |system: &AvatarSyncSystem, scratch: &mut ReceiverBuildScratch| {
            system.build_sends_for_receiver(
                receiver,
                receiver_position,
                peers,
                None,
                config,
                now_ms,
                8,
                4,
                false,
                true,
                true,
                scratch,
            )
        };
        let mut fresh_scratch = ReceiverBuildScratch::default();
        let fresh = build(fresh_system, &mut fresh_scratch);
        let reused = build(reused_system, scratch);
        let expected = snapshot_avatar_batch(fresh.as_ref());
        assert_eq!(expected, snapshot_avatar_batch(reused.as_ref()));
        assert!(fresh_scratch.bundle.is_empty());
        assert!(scratch.bundle.is_empty());
        (expected, reused)
    }

    fn logical_channels(snapshot: &Option<AvatarBatchSnapshot>) -> Vec<u8> {
        snapshot
            .as_ref()
            .into_iter()
            .flat_map(|(_, sends)| sends)
            .flat_map(|(channel, payload, _)| {
                if *channel == channels::COMPRESSED_AVATAR_BUNDLE {
                    decode_avatar_bundle(payload)
                        .unwrap()
                        .into_iter()
                        .map(|item| item.original_channel)
                        .collect::<Vec<_>>()
                } else {
                    vec![*channel]
                }
            })
            .collect()
    }

    #[test]
    fn receiver_build_scratch_reuse_preserves_output_across_receivers_and_empty_builds() {
        let config = receiver_build_test_config();
        let fresh_system = AvatarSyncSystem::new(config.clone());
        let reused_system = AvatarSyncSystem::new(config.clone());
        let mut scratch = ReceiverBuildScratch::default();
        let base = receiver_build_test_peers(&[]);

        // Populate for one receiver, then another. Keep the first owned result alive while
        // later calls clear and refill scratch; it must remain unchanged.
        let (initial, held_batch) = build_fresh_and_reused(
            &fresh_system,
            &reused_system,
            &mut scratch,
            &config,
            &base,
            1,
            1_000,
        );
        let held_batch = held_batch.expect("initial receiver sends keyframes");
        assert_eq!(logical_channels(&initial).len(), 8);
        assert!(initial
            .as_ref()
            .unwrap()
            .1
            .iter()
            .any(|(channel, _, _)| { *channel == channels::COMPRESSED_AVATAR_BUNDLE }));
        let scratch_ptr = scratch.bundle.as_ptr();

        let (alternate, _) = build_fresh_and_reused(
            &fresh_system,
            &reused_system,
            &mut scratch,
            &config,
            &base,
            300,
            1_001,
        );
        assert_eq!(logical_channels(&alternate).len(), 8);
        assert_eq!(scratch_ptr, scratch.bundle.as_ptr());

        // An unchanged receiver is empty; the next changed frame repopulates the same vector.
        let (empty, _) = build_fresh_and_reused(
            &fresh_system,
            &reused_system,
            &mut scratch,
            &config,
            &base,
            1,
            1_002,
        );
        assert!(empty.is_none());

        let four_changed = receiver_build_test_peers(&[(2, 2), (3, 2), (4, 2), (5, 2)]);
        let (boundary, _) = build_fresh_and_reused(
            &fresh_system,
            &reused_system,
            &mut scratch,
            &config,
            &four_changed,
            1,
            1_003,
        );
        assert_eq!(logical_channels(&boundary).len(), 4);
        assert!(boundary
            .as_ref()
            .unwrap()
            .1
            .iter()
            .any(|(channel, _, _)| { *channel == channels::COMPRESSED_AVATAR_BUNDLE }));

        let three_changed = receiver_build_test_peers(&[(2, 3), (3, 3), (4, 3), (5, 2)]);
        let (fallback, _) = build_fresh_and_reused(
            &fresh_system,
            &reused_system,
            &mut scratch,
            &config,
            &three_changed,
            1,
            1_004,
        );
        assert_eq!(logical_channels(&fallback).len(), 3);
        assert!(fallback
            .as_ref()
            .unwrap()
            .1
            .iter()
            .all(|(channel, _, _)| { *channel != channels::COMPRESSED_AVATAR_BUNDLE }));
        assert_eq!(scratch_ptr, scratch.bundle.as_ptr());
        assert_eq!(snapshot_avatar_batch(Some(&held_batch)), initial);
    }

    #[test]
    fn packet_preserialization_uses_small_and_large_ids() {
        let payload = vec![0u8; BitQuality::High.payload_len()];
        let packet = pre_serialize(300, 7, BitQuality::High, &payload, &[]);
        assert_eq!(packet.channel_small, channels::PLAYER_AVATAR_HIGH);
        assert_eq!(packet.channel_large, channels::PLAYER_AVATAR_HIGH_LARGE);
        assert_eq!(&packet.bytes_large[0..2], &300u16.to_le_bytes());
        assert_eq!(packet.bytes_large[3], 7);
    }

    #[test]
    fn additional_avatar_data_selects_odd_channels_and_is_preserved() {
        let payload = vec![0u8; BitQuality::High.payload_len()];
        let additional = [1, 0, 3, 9, 1, 2, 3];
        validate_additional_avatar_data(&additional).unwrap();
        let packet = pre_serialize(12, 7, BitQuality::High, &payload, &additional);
        assert_eq!(
            packet.channel_small,
            channels::PLAYER_AVATAR_HIGH_ADDITIONAL
        );
        assert_eq!(
            packet.channel_large,
            channels::PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE
        );
        assert_eq!(&packet.bytes_small[3 + payload.len()..], &additional);
        assert_eq!(packet.additional_data.as_ref(), &additional);

        let delta = pre_serialize_delta(12, 8, 7, BitQuality::High, &[0, 0, 0, 0, 0], &additional);
        assert_ne!(
            delta.bytes_small[0] & channels::DELTA_HEADER_ADDITIONAL_DATA,
            0
        );
        assert_eq!(&delta.bytes_small[5 + 5..], &additional);
    }

    #[test]
    fn additional_avatar_data_is_stripped_from_low_tiers_by_default_policy() {
        let pool = BytePool::new();
        let profiler = BsrProfiler::new(false);
        let payload = vec![0u8; BitQuality::High.payload_len()];
        let additional = [1, 0, 3, 9, 1, 2, 3];
        let packets = build_quality_packets(
            &pool,
            &profiler,
            12,
            7,
            BitQuality::High,
            &payload,
            &additional,
            true,
        )
        .unwrap();
        assert_eq!(
            packets[BitQuality::High as usize]
                .as_ref()
                .unwrap()
                .channel_small,
            channels::PLAYER_AVATAR_HIGH_ADDITIONAL
        );
        assert_eq!(
            packets[BitQuality::Medium as usize]
                .as_ref()
                .unwrap()
                .channel_small,
            channels::PLAYER_AVATAR_MEDIUM_ADDITIONAL
        );
        assert_eq!(
            packets[BitQuality::Low as usize]
                .as_ref()
                .unwrap()
                .channel_small,
            channels::PLAYER_AVATAR_LOW
        );
        assert_eq!(
            packets[BitQuality::VeryLow as usize]
                .as_ref()
                .unwrap()
                .channel_small,
            channels::PLAYER_AVATAR_VERY_LOW
        );
    }

    #[test]
    fn malformed_additional_avatar_data_is_rejected() {
        assert!(validate_additional_avatar_data(&[]).is_err());
        assert!(validate_additional_avatar_data(&[1]).is_err());
        assert!(validate_additional_avatar_data(&[1, 0, 3, 9, 1]).is_err());
        assert!(validate_additional_avatar_data(&[0, 1]).is_err());
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
            enable_bundle_zstd: false,
            bundle_zstd_delta_bundles: false,
            bundle_zstd_level: -2,
            enable_delta_compression: false,
            delta_keyframe_interval_ms: 500,
            delta_keyframe_max_interval_ms: 2000,
            strip_additional_data_at_low_quality: true,
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
    fn adaptive_keyframe_interval_matches_current_csharp_rules() {
        let mut config = AvatarSyncConfig {
            default_interval_ms: 50,
            base_multiplier: 1.0,
            increase_rate: 0.005,
            high_distance_sq: 9.0,
            medium_distance_sq: 100.0,
            low_distance_sq: 400.0,
            enable_bundle_compression: false,
            enable_bundle_zstd: false,
            bundle_zstd_delta_bundles: false,
            bundle_zstd_level: -2,
            enable_delta_compression: true,
            delta_keyframe_interval_ms: 500,
            delta_keyframe_max_interval_ms: 2000,
            strip_additional_data_at_low_quality: true,
            bundle_min_messages: 4,
            bundle_min_bytes: 128,
            min_receiver_slices: 1,
            max_receiver_slices: 32,
            tick_budget_ms: DEFAULT_AVATAR_TICK_BUDGET_MS,
            receiver_cycle_budget_ms: DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS,
            spatial_cull_enabled: false,
            enable_bsr_profiling: false,
        };
        assert_eq!(effective_keyframe_interval_ms(&config, 0), 500);
        assert_eq!(effective_keyframe_interval_ms(&config, 1), 1000);
        assert_eq!(effective_keyframe_interval_ms(&config, 2), 2000);
        assert_eq!(effective_keyframe_interval_ms(&config, 3), 2000);

        config.delta_keyframe_max_interval_ms = 500;
        assert_eq!(effective_keyframe_interval_ms(&config, 4), 500);
    }

    #[test]
    fn reused_high_delta_matches_recomputed_packet_bytes() {
        for (peer_id, base_sequence, outbound_sequence, base_byte, current_byte, additional) in [
            (42, 255, 0, 0, 0, false),
            (300, 17, 18, 0, 1, true),
            (65_000, 128, 129, 0x55, 0xa1, true),
        ] {
            let baseline_qualities =
                delta_test_qualities(peer_id, base_sequence, base_byte, additional);
            let current_qualities =
                delta_test_qualities(peer_id, outbound_sequence, current_byte, additional);
            let mut baselines = delta_test_payloads(&baseline_qualities);
            let mut current = delta_test_payloads(&current_qualities);
            // Missing non-High qualities preserve the all-or-nothing behavior for those slots.
            if peer_id == 300 {
                baselines[BitQuality::Low as usize] = None;
                current[BitQuality::Medium as usize] = None;
            }
            let high_delta = build_delta(
                baselines[BitQuality::High as usize].as_ref().unwrap(),
                current[BitQuality::High as usize].as_ref().unwrap(),
                BitQuality::High,
            )
            .unwrap();

            let expected = reference_build_delta_packets(
                peer_id,
                outbound_sequence,
                base_sequence,
                &baselines,
                &current,
                &current_qualities,
            );
            let actual = build_delta_packets(
                peer_id,
                outbound_sequence,
                base_sequence,
                &baselines,
                &current,
                &current_qualities,
                Some(&high_delta),
            );
            for (expected, actual) in expected.iter().zip(&actual) {
                assert_eq!(
                    expected.as_ref().map(|packet| packet.bytes_small.as_ref()),
                    actual.as_ref().map(|packet| packet.bytes_small.as_ref()),
                );
                assert_eq!(
                    expected.as_ref().map(|packet| packet.bytes_large.as_ref()),
                    actual.as_ref().map(|packet| packet.bytes_large.as_ref()),
                );
            }

            // A malformed lower-quality baseline still drops only that slot.
            let mut malformed_baseline = baselines.clone();
            malformed_baseline[BitQuality::Medium as usize] = Some(Bytes::from_static(&[0, 1, 2]));
            let expected = reference_build_delta_packets(
                peer_id,
                outbound_sequence,
                base_sequence,
                &malformed_baseline,
                &current,
                &current_qualities,
            );
            let actual = build_delta_packets(
                peer_id,
                outbound_sequence,
                base_sequence,
                &malformed_baseline,
                &current,
                &current_qualities,
                Some(&high_delta),
            );
            assert!(expected[BitQuality::Medium as usize].is_none());
            assert!(actual[BitQuality::Medium as usize].is_none());
            for (expected, actual) in expected.iter().zip(&actual) {
                assert_eq!(
                    expected.as_ref().map(|packet| packet.bytes_small.as_ref()),
                    actual.as_ref().map(|packet| packet.bytes_small.as_ref()),
                );
            }

            // A missing High baseline has no reusable delta and follows the old recompute path.
            let mut missing_high = baselines.clone();
            missing_high[BitQuality::High as usize] = None;
            assert_eq!(
                reference_build_delta_packets(
                    peer_id,
                    outbound_sequence,
                    base_sequence,
                    &missing_high,
                    &current,
                    &current_qualities,
                )
                .map(|packet| packet.map(|p| (p.bytes_small, p.bytes_large))),
                build_delta_packets(
                    peer_id,
                    outbound_sequence,
                    base_sequence,
                    &missing_high,
                    &current,
                    &current_qualities,
                    None,
                )
                .map(|packet| packet.map(|p| (p.bytes_small, p.bytes_large))),
            );
        }
    }

    #[test]
    fn high_delta_reuse_keeps_stretch_and_keyframe_transitions() {
        let mut config = receiver_build_test_config();
        config.enable_delta_compression = true;
        let now = Instant::now();
        let baseline_qualities = delta_test_qualities(42, 10, 0, false);
        let baseline_payloads = delta_test_payloads(&baseline_qualities);
        let make_state = |keyframe_payloads: [Option<Bytes>; 4],
                          last_keyframe: Instant,
                          keyframe_stretch_shift,
                          small_delta_streak| PlayerAvatarState {
            peer_id: 42,
            small_id: true,
            position: [0.0; 3],
            generation: 1,
            last_inbound_sequence: 10,
            outbound_sequence: 10,
            has_received_first: true,
            qualities: baseline_qualities.clone(),
            keyframe_qualities: baseline_qualities.clone(),
            keyframe_payloads,
            deltas: [None, None, None, None],
            keyframe_generation: 1,
            keyframe_sequence: 10,
            last_keyframe,
            keyframe_stretch_shift,
            small_delta_streak,
            current_is_keyframe: true,
        };

        let mut small_delta_state = make_state(baseline_payloads.clone(), now, 0, 3);
        let identical = delta_test_qualities(42, 11, 0, false);
        update_outbound_delta_state(&mut small_delta_state, identical.clone(), 2, &config, now);
        assert!(!small_delta_state.current_is_keyframe);
        assert_eq!(small_delta_state.keyframe_stretch_shift, 1);
        assert_eq!(small_delta_state.small_delta_streak, 0);
        assert!(small_delta_state.deltas[BitQuality::High as usize].is_some());

        let mut large_delta_state = make_state(baseline_payloads.clone(), now, 2, 3);
        let changed = delta_test_qualities(42, 12, u8::MAX, false);
        let large_high_delta = build_delta(
            baseline_payloads[BitQuality::High as usize]
                .as_ref()
                .unwrap(),
            delta_test_payloads(&changed)[BitQuality::High as usize]
                .as_ref()
                .unwrap(),
            BitQuality::High,
        )
        .unwrap();
        assert!(large_high_delta.len() > SMALL_HIGH_DELTA_BYTES);
        assert!(large_high_delta.len() < BitQuality::High.payload_len());
        update_outbound_delta_state(&mut large_delta_state, changed.clone(), 3, &config, now);
        assert!(!large_delta_state.current_is_keyframe);
        assert_eq!(large_delta_state.keyframe_stretch_shift, 0);
        assert_eq!(large_delta_state.small_delta_streak, 0);

        let mut seed = 0x1357_9bdfu32;
        let noisy_high: Vec<_> = (0..BitQuality::High.payload_len())
            .map(|_| {
                seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (seed >> 24) as u8
            })
            .collect();
        let noisy_high_delta = build_delta(
            baseline_payloads[BitQuality::High as usize]
                .as_ref()
                .unwrap(),
            &noisy_high,
            BitQuality::High,
        )
        .unwrap();
        assert!(noisy_high_delta.len() >= BitQuality::High.payload_len());
        let mut oversized_current = changed;
        oversized_current[BitQuality::High as usize] =
            Some(pre_serialize(42, 13, BitQuality::High, &noisy_high, &[]));
        let mut oversized_delta_state = make_state(baseline_payloads.clone(), now, 2, 3);
        update_outbound_delta_state(
            &mut oversized_delta_state,
            oversized_current,
            4,
            &config,
            now,
        );
        assert!(oversized_delta_state.current_is_keyframe);
        assert_eq!(oversized_delta_state.keyframe_stretch_shift, 0);
        assert_eq!(oversized_delta_state.small_delta_streak, 0);
        assert_eq!(oversized_delta_state.keyframe_sequence, 10);

        let mut missing_baseline_state = make_state([None, None, None, None], now, 1, 2);
        update_outbound_delta_state(
            &mut missing_baseline_state,
            identical.clone(),
            4,
            &config,
            now,
        );
        assert!(missing_baseline_state.current_is_keyframe);
        assert_eq!(missing_baseline_state.keyframe_stretch_shift, 1);
        assert_eq!(missing_baseline_state.small_delta_streak, 2);

        let due_time = now - Duration::from_millis(config.delta_keyframe_interval_ms);
        let mut due_keyframe_state = make_state(baseline_payloads, due_time, 0, 2);
        update_outbound_delta_state(&mut due_keyframe_state, identical, 5, &config, now);
        assert!(due_keyframe_state.current_is_keyframe);
        assert_eq!(due_keyframe_state.keyframe_sequence, 10);
    }

    #[test]
    fn slicing_remains_load_adaptive() {
        let config = AvatarSyncConfig {
            default_interval_ms: 50,
            base_multiplier: 1.0,
            increase_rate: 0.005,
            high_distance_sq: 9.0,
            medium_distance_sq: 100.0,
            low_distance_sq: 400.0,
            enable_bundle_compression: false,
            enable_bundle_zstd: false,
            bundle_zstd_delta_bundles: false,
            bundle_zstd_level: -2,
            enable_delta_compression: false,
            delta_keyframe_interval_ms: 500,
            delta_keyframe_max_interval_ms: 2000,
            strip_additional_data_at_low_quality: true,
            bundle_min_messages: 4,
            bundle_min_bytes: 128,
            min_receiver_slices: 1,
            max_receiver_slices: 32,
            tick_budget_ms: DEFAULT_AVATAR_TICK_BUDGET_MS,
            receiver_cycle_budget_ms: DEFAULT_AVATAR_RECEIVER_CYCLE_BUDGET_MS,
            spatial_cull_enabled: false,
            enable_bsr_profiling: false,
        };
        let system = AvatarSyncSystem::new(config.clone());

        system.adapt_slice_count(1_000, &config);
        assert_eq!(system.slice_state.lock().slice_count, 1);

        system.adapt_slice_count(4_000, &config);
        assert_eq!(system.slice_state.lock().slice_count, 2);
    }

    #[test]
    fn overloaded_slice_count_saturates_at_maximum() {
        let mut config = receiver_build_test_config();
        config.min_receiver_slices = 1;
        config.max_receiver_slices = 32;
        config.tick_budget_ms = 1.0;
        config.receiver_cycle_budget_ms = 1.0;
        let system = AvatarSyncSystem::new(config.clone());
        system.slice_state.lock().slice_count = 32;

        for _ in 0..8 {
            system.adapt_slice_count(10_000, &config);
            assert_eq!(system.slice_state.lock().slice_count, 32);
        }
    }

    #[test]
    fn absolute_receiver_cursor_covers_each_receiver_once_as_slice_count_changes() {
        for receiver_count in [1, 17, 1_500] {
            let mut cursor = 0;
            let mut visits = vec![0_u8; receiver_count];
            let mut complete_cycles = 0;
            for tick in 0..500 {
                let slices = [32, 31, 1, 30, 2, 31, 7, 32, 3, 17];
                let (start, end, next_cursor) =
                    next_receiver_slice(receiver_count, slices[tick % slices.len()], cursor);
                assert!(start <= end && end <= receiver_count);
                for visit in &mut visits[start..end] {
                    *visit += 1;
                }
                cursor = next_cursor;
                if cursor == 0 {
                    assert!(visits.iter().all(|count| *count == 1));
                    visits.fill(0);
                    complete_cycles += 1;
                }
            }
            assert!(complete_cycles >= 2);
        }
    }

    #[test]
    fn receiver_cursor_handles_empty_and_changing_populations() {
        assert_eq!(next_receiver_slice(0, 32, 120), (0, 0, 0));
        assert_eq!(next_receiver_slice(17, 32, 120), (0, 1, 1));
        assert_eq!(next_receiver_slice(4, 2, 3), (3, 4, 0));
        assert_eq!(next_receiver_slice(9, 3, 2), (2, 5, 5));
        assert_eq!(next_receiver_slice(9, 3, 9), (0, 3, 3));
    }

    #[test]
    fn advertised_interval_accounts_for_receiver_slicing() {
        let base_interval_ms = 50;
        let cached_interval_byte = channels::encode_avatar_interval_byte(50, base_interval_ms);
        let cached_interval_ms =
            channels::decode_avatar_interval_ms(cached_interval_byte, base_interval_ms) as u64;

        assert_eq!(
            advertised_interval_byte(
                cached_interval_byte,
                cached_interval_ms,
                1,
                AVATAR_TICK_INTERVAL_MS,
                base_interval_ms as u64,
            ),
            cached_interval_byte
        );

        let sliced = advertised_interval_byte(
            cached_interval_byte,
            cached_interval_ms,
            32,
            AVATAR_TICK_INTERVAL_MS,
            base_interval_ms as u64,
        );
        assert_eq!(
            channels::decode_avatar_interval_ms(sliced, base_interval_ms),
            (AVATAR_TICK_INTERVAL_MS * 32) as i32
        );

        let distant_byte = channels::encode_avatar_interval_byte(500, base_interval_ms);
        let distant_ms = channels::decode_avatar_interval_ms(distant_byte, base_interval_ms) as u64;
        assert_eq!(
            advertised_interval_byte(
                distant_byte,
                distant_ms,
                32,
                AVATAR_TICK_INTERVAL_MS,
                base_interval_ms as u64,
            ),
            distant_byte
        );

        let overloaded = advertised_interval_byte(
            cached_interval_byte,
            cached_interval_ms,
            32,
            10,
            base_interval_ms as u64,
        );
        assert!(channels::decode_avatar_interval_ms(overloaded, base_interval_ms) >= 320);
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
