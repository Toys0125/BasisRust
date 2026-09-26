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
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, OnceLock,
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreSerializedQuality {
    channel_small: u8,
    channel_large: u8,
    bytes_small: Bytes,
    bytes_large: Bytes,
    additional_data: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreSerializedDelta {
    bytes_small: Bytes,
    bytes_large: Bytes,
}

/// Immutable source data for quality packets from one sender generation.
/// High is materialized eagerly; lower qualities are cached on their first
/// receiver request. The captured fields keep lazy work independent of later
/// config changes and pooled inbound buffers.
#[derive(Debug)]
struct LazyQualityFrame {
    peer_id: PeerId,
    outbound_sequence: u8,
    source_quality: BitQuality,
    source_payload: Bytes,
    additional_data: Bytes,
    strip_additional_data_at_low_quality: bool,
    qualities: [OnceLock<Result<Option<PreSerializedQuality>, String>>; 4],
    #[cfg(test)]
    init_counts: [AtomicU64; 4],
}

impl LazyQualityFrame {
    fn new(
        profiler: &BsrProfiler,
        peer_id: PeerId,
        outbound_sequence: u8,
        source_quality: BitQuality,
        payload: &[u8],
        additional_data: &[u8],
        strip_additional_data_at_low_quality: bool,
    ) -> Result<Arc<Self>> {
        // Preserve the eager builder's validation timing. These are the only
        // fallible preconditions in repack_high_to_lower_into; inbound High
        // payloads have exactly the validated fixed length and outputs below
        // are allocated at the target's fixed length.
        if source_quality == BitQuality::High {
            anyhow::ensure!(
                payload.len() >= BitQuality::High.payload_len(),
                "high payload too small"
            );
            for target in [BitQuality::Medium, BitQuality::Low, BitQuality::VeryLow] {
                validate_repack_preconditions(payload.len(), target, target.payload_len())?;
            }
        }

        let source_additional = if strip_additional_data_at_low_quality
            && matches!(source_quality, BitQuality::Low | BitQuality::VeryLow)
        {
            &[][..]
        } else {
            additional_data
        };
        let source_packet = pre_serialize(
            peer_id,
            outbound_sequence,
            source_quality,
            payload,
            source_additional,
        );
        profiler.add_pre_serializations(1);
        let source_payload_len = source_quality.payload_len();
        let source_payload = source_packet
            .bytes_small
            .get(3..3 + source_payload_len)
            .map(|_| source_packet.bytes_small.slice(3..3 + source_payload_len))
            .ok_or_else(|| anyhow::anyhow!("serialized avatar payload is truncated"))?;
        let qualities: [OnceLock<Result<Option<PreSerializedQuality>, String>>; 4] =
            std::array::from_fn(|_| OnceLock::new());
        let source_index = source_quality as usize;
        let _ = qualities[source_index].set(Ok(Some(source_packet.clone())));
        Ok(Arc::new(Self {
            peer_id,
            outbound_sequence,
            source_quality,
            source_payload,
            additional_data: source_packet.additional_data.clone(),
            strip_additional_data_at_low_quality,
            qualities,
            #[cfg(test)]
            init_counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }))
    }

    fn quality(
        &self,
        pool: &BytePool,
        profiler: &BsrProfiler,
        quality: BitQuality,
    ) -> Result<Option<&PreSerializedQuality>> {
        let slot = &self.qualities[quality as usize];
        let result = slot.get_or_init(|| {
            #[cfg(test)]
            self.init_counts[quality as usize].fetch_add(1, Ordering::Relaxed);
            let built = (|| -> Result<Option<PreSerializedQuality>> {
                if self.source_quality != BitQuality::High || quality == BitQuality::High {
                    return Ok(None);
                }
                let mut repacked = pool.take(quality.payload_len());
                let repack_result =
                    repack_high_to_lower_into(&self.source_payload, quality, &mut repacked);
                if let Err(error) = repack_result {
                    pool.put(repacked);
                    return Err(error);
                }
                let additional_data = if self.strip_additional_data_at_low_quality
                    && matches!(quality, BitQuality::Low | BitQuality::VeryLow)
                {
                    &[][..]
                } else {
                    self.additional_data.as_ref()
                };
                let packet = pre_serialize(
                    self.peer_id,
                    self.outbound_sequence,
                    quality,
                    &repacked,
                    additional_data,
                );
                pool.put(repacked);
                profiler.add_pre_serializations(1);
                Ok(Some(packet))
            })();
            built.map_err(|error| format!("{error:#}"))
        });
        match result {
            Ok(packet) => Ok(packet.as_ref()),
            Err(error) => anyhow::bail!("{error}"),
        }
    }

    fn payload(
        &self,
        pool: &BytePool,
        profiler: &BsrProfiler,
        quality: BitQuality,
    ) -> Result<Option<Bytes>> {
        let Some(packet) = self.quality(pool, profiler, quality)? else {
            return Ok(None);
        };
        let end = 3 + quality.payload_len();
        Ok(packet
            .bytes_small
            .get(3..end)
            .map(|_| packet.bytes_small.slice(3..end)))
    }
}

/// Per-current-generation lazy deltas against the frozen keyframe frame.
/// Each quality is initialized at most once across parallel receiver builds.
#[derive(Debug)]
struct LazyDeltaFrame {
    current: Arc<LazyQualityFrame>,
    baseline: Arc<LazyQualityFrame>,
    outbound_sequence: u8,
    base_sequence: u8,
    deltas: [OnceLock<Option<PreSerializedDelta>>; 4],
    #[cfg(test)]
    init_counts: [AtomicU64; 4],
}

impl LazyDeltaFrame {
    fn new(
        current: Arc<LazyQualityFrame>,
        baseline: Arc<LazyQualityFrame>,
        outbound_sequence: u8,
        base_sequence: u8,
        high_delta: &[u8],
        pool: &BytePool,
        profiler: &BsrProfiler,
    ) -> Result<Arc<Self>> {
        let frame = Self {
            current,
            baseline,
            outbound_sequence,
            base_sequence,
            deltas: std::array::from_fn(|_| OnceLock::new()),
            #[cfg(test)]
            init_counts: std::array::from_fn(|_| AtomicU64::new(0)),
        };
        let high_additional = frame
            .current
            .quality(pool, profiler, BitQuality::High)?
            .map(|packet| packet.additional_data.as_ref())
            .unwrap_or(&[]);
        let high = pre_serialize_delta(
            frame.current.peer_id,
            outbound_sequence,
            base_sequence,
            BitQuality::High,
            high_delta,
            high_additional,
        );
        let _ = frame.deltas[BitQuality::High as usize].set(Some(high));
        Ok(Arc::new(frame))
    }

    fn quality(
        &self,
        pool: &BytePool,
        profiler: &BsrProfiler,
        quality: BitQuality,
    ) -> Result<Option<&PreSerializedDelta>> {
        let slot = &self.deltas[quality as usize];
        let result = slot.get_or_init(|| {
            #[cfg(test)]
            self.init_counts[quality as usize].fetch_add(1, Ordering::Relaxed);
            let built = (|| -> Result<Option<PreSerializedDelta>> {
                let Some(baseline) = self.baseline.payload(pool, profiler, quality)? else {
                    return Ok(None);
                };
                let Some(current) = self.current.payload(pool, profiler, quality)? else {
                    return Ok(None);
                };
                let Ok(body) = build_delta(baseline.as_ref(), current.as_ref(), quality) else {
                    return Ok(None);
                };
                let additional_data = self
                    .current
                    .quality(pool, profiler, quality)?
                    .map(|packet| packet.additional_data.as_ref())
                    .unwrap_or(&[]);
                Ok(Some(pre_serialize_delta(
                    self.current.peer_id,
                    self.outbound_sequence,
                    self.base_sequence,
                    quality,
                    &body,
                    additional_data,
                )))
            })();
            built.ok().flatten()
        });
        Ok(result.as_ref())
    }
}

fn validate_repack_preconditions(
    source_len: usize,
    target: BitQuality,
    output_len: usize,
) -> Result<()> {
    anyhow::ensure!(target != BitQuality::High, "target must be lower than High");
    anyhow::ensure!(
        source_len >= BitQuality::High.payload_len(),
        "high payload too small"
    );
    anyhow::ensure!(
        output_len >= target.payload_len(),
        "target output buffer too small"
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct PlayerAvatarState {
    #[cfg(test)]
    peer_id: PeerId,
    incarnation: u64,
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
    lazy_current: Option<Arc<LazyQualityFrame>>,
    lazy_keyframe: Option<Arc<LazyQualityFrame>>,
    lazy_deltas: Option<Arc<LazyDeltaFrame>>,
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

#[derive(Debug, Clone)]
struct ReceiverCycle {
    roster: Vec<(PeerId, u64)>,
    cursor: usize,
    slice_count: usize,
}

struct ReceiverSlicePlan {
    receiver_cycle: usize,
    receivers: Vec<(PeerId, Arc<PlayerAvatarState>)>,
    update_distances: bool,
    effective_tick_interval_ms: u64,
}

#[derive(Debug, Clone)]
struct SliceState {
    slice_count: usize,
    cycle: Option<ReceiverCycle>,
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
    diagnostic_items: Vec<(PeerId, u8, bool)>,
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

#[derive(Debug, Clone, Copy, Default)]
struct AvatarDiagnosticPairCounts {
    built_full: u64,
    built_delta: u64,
    queue_ok_full: u64,
    queue_ok_delta: u64,
    quality: [u64; 4],
}

#[derive(Debug, Default)]
struct AvatarDiagnosticWindow {
    started_at: Option<Instant>,
    last_sample_elapsed_ms: u64,
    pair_counts: HashMap<PeerId, AvatarDiagnosticPairCounts>,
}

/// Disabled unless an observer ID, start marker and output path are all set.
/// When enabled, this writes one compact row per active sender at five-second
/// boundaries for a single receiver; it never logs individual packets.
#[derive(Debug)]
struct AvatarSyncDiagnostics {
    observer_id: PeerId,
    start_marker: PathBuf,
    output_path: PathBuf,
    global_output_path: PathBuf,
    window_duration: Duration,
    counters: Arc<AvatarSyncCounters>,
    state: parking_lot::Mutex<AvatarDiagnosticWindow>,
}

impl AvatarSyncDiagnostics {
    fn from_env(counters: Arc<AvatarSyncCounters>) -> Option<Arc<Self>> {
        let observer_id = env::var("BASIS_AVATAR_DIAGNOSTIC_OBSERVER_ID")
            .ok()?
            .parse::<PeerId>()
            .ok()?;
        let start_marker = PathBuf::from(env::var_os("BASIS_AVATAR_DIAGNOSTIC_START_FILE")?);
        let output_path = PathBuf::from(env::var_os("BASIS_AVATAR_DIAGNOSTIC_CSV")?);
        let window_secs = env::var("BASIS_AVATAR_DIAGNOSTIC_WINDOW_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(60)
            .max(1);
        Some(Arc::new(Self {
            observer_id,
            start_marker,
            global_output_path: output_path.with_extension("global.csv"),
            output_path,
            window_duration: Duration::from_secs(window_secs),
            counters,
            state: parking_lot::Mutex::new(AvatarDiagnosticWindow::default()),
        }))
    }

    fn write_global_sample(&self, elapsed_ms: u64) {
        let line = format!(
            "{elapsed_ms},{},{},{},{},{},{},{}\n",
            self.counters.inbound_updates.load(Ordering::Relaxed),
            self.counters
                .outbound_logical_avatar_sends
                .load(Ordering::Relaxed),
            self.counters.tick_count.load(Ordering::Relaxed),
            self.counters.tick_micros.load(Ordering::Relaxed),
            self.counters.build_micros.load(Ordering::Relaxed),
            self.counters.flush_micros.load(Ordering::Relaxed),
            self.counters.outbound_messages.load(Ordering::Relaxed),
        );
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.global_output_path)
        {
            use std::io::Write;
            let _ = file.write_all(line.as_bytes());
        }
    }

    fn record_batch(&self, receiver_id: PeerId, items: &[(PeerId, u8, bool)], queue_ok: bool) {
        if receiver_id != self.observer_id || items.is_empty() {
            return;
        }
        let mut state = self.state.lock();
        for (sender_id, quality, is_delta) in items {
            let counts = state.pair_counts.entry(*sender_id).or_default();
            if *is_delta {
                if queue_ok {
                    counts.queue_ok_delta = counts.queue_ok_delta.saturating_add(1);
                } else {
                    counts.built_delta = counts.built_delta.saturating_add(1);
                }
            } else {
                if queue_ok {
                    counts.queue_ok_full = counts.queue_ok_full.saturating_add(1);
                } else {
                    counts.built_full = counts.built_full.saturating_add(1);
                }
            }
            if !queue_ok {
                if let Some(quality_count) = counts.quality.get_mut(*quality as usize) {
                    *quality_count = quality_count.saturating_add(1);
                }
            }
        }
    }

    fn maybe_emit(
        &self,
        states: &DashMap<PeerId, Arc<PlayerAvatarState>>,
        tracking: &DashMap<PeerId, PeerIdMap<ReceiverTracking>>,
    ) {
        let now = Instant::now();
        let mut window = self.state.lock();
        if window.started_at.is_none() {
            if !self.start_marker.exists() {
                return;
            }
            if let Some(parent) = self.output_path.parent() {
                if std::fs::create_dir_all(parent).is_err() {
                    return;
                }
            }
            if std::fs::write(
                &self.output_path,
                "elapsed_ms,observer_id,sender_id,generation,inbound_sequence,outbound_sequence,keyframe_generation,current_is_keyframe,pair_last_seen_generation,pair_last_sent_ms,pair_baseline_quality,built_full,built_delta,queue_ok_full,queue_ok_delta,built_quality_very_low,built_quality_low,built_quality_medium,built_quality_high\n",
            )
            .is_err()
            {
                return;
            }
            if std::fs::write(
                &self.global_output_path,
                "elapsed_ms,inbound_updates,outbound_logical_avatar_sends,tick_count,tick_micros,build_micros,flush_micros,outbound_messages\n",
            )
            .is_err()
            {
                return;
            }
            window.pair_counts.clear();
            window.last_sample_elapsed_ms = 0;
            window.started_at = Some(now);
            self.write_global_sample(0);
            return;
        }
        let started_at = window.started_at.expect("checked above");
        let elapsed = now.saturating_duration_since(started_at);
        let nominal_window_ms = self.window_duration.as_millis().min(u64::MAX as u128) as u64;
        if elapsed > self.window_duration && window.last_sample_elapsed_ms >= nominal_window_ms {
            return;
        }
        let elapsed_ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
        if elapsed < self.window_duration
            && elapsed_ms.saturating_sub(window.last_sample_elapsed_ms) < 5_000
        {
            return;
        }
        window.last_sample_elapsed_ms = elapsed_ms;
        self.write_global_sample(elapsed_ms);
        let counts = std::mem::take(&mut window.pair_counts);
        let receiver_tracking = tracking.get(&self.observer_id);
        let mut rows = String::with_capacity(states.len().saturating_mul(160));
        for entry in states.iter() {
            let sender_id = *entry.key();
            let sender = entry.value();
            let pair = receiver_tracking
                .as_ref()
                .and_then(|map| map.get(&sender_id));
            let count = counts.get(&sender_id).copied().unwrap_or_default();
            let (last_seen, last_sent, baseline_quality) = pair
                .map(|value| {
                    (
                        value.last_seen_generation,
                        value.last_sent_ms,
                        value.baseline_quality,
                    )
                })
                .unwrap_or((0, 0, u8::MAX));
            rows.push_str(&format!(
                "{elapsed_ms},{},{},{},{},{},{},{},{last_seen},{last_sent},{baseline_quality},{},{},{},{},{},{},{},{}\n",
                self.observer_id,
                sender_id,
                sender.generation,
                sender.last_inbound_sequence,
                sender.outbound_sequence,
                sender.keyframe_generation,
                sender.current_is_keyframe,
                count.built_full,
                count.built_delta,
                count.queue_ok_full,
                count.queue_ok_delta,
                count.quality[0],
                count.quality[1],
                count.quality[2],
                count.quality[3],
            ));
        }
        if !rows.is_empty() {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&self.output_path)
            {
                use std::io::Write;
                let _ = file.write_all(rows.as_bytes());
            }
        }
    }
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
    diagnostics: Option<Arc<AvatarSyncDiagnostics>>,
}

impl AvatarSyncSystem {
    pub fn new(config: AvatarSyncConfig) -> Self {
        let profiler_enabled = config.enable_bsr_profiling;
        let counters = Arc::new(AvatarSyncCounters::default());
        let diagnostics = AvatarSyncDiagnostics::from_env(Arc::clone(&counters));
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
                cycle: None,
                last_distance_update: Instant::now(),
                smoothed_tick_micros: 0,
            })),
            payload_pool: Arc::new(BytePool::new()),
            counters,
            profiler: Arc::new(BsrProfiler::new(profiler_enabled)),
            offloaded_pairs: Arc::new(DashMap::new()),
            bypass_reduction_ids: Arc::new(DashMap::new()),
            diagnostics,
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
            slice_count: state
                .cycle
                .as_ref()
                .map_or(state.slice_count, |cycle| cycle.slice_count),
            tick_count,
            build_micros: self.counters.build_micros.load(Ordering::Relaxed),
            flush_micros: self.counters.flush_micros.load(Ordering::Relaxed),
            max_tick_micros: self.counters.max_tick_micros.load(Ordering::Relaxed),
            avg_tick_micros,
            smoothed_tick_micros: state.smoothed_tick_micros,
            receiver_cycle_micros: state.smoothed_tick_micros.saturating_mul(
                state.cycle.as_ref().map_or(0, |cycle| {
                    receiver_cycle_length(cycle.roster.len(), cycle.slice_count)
                }) as u64,
            ),
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
        let mut peer_states = Vec::with_capacity(peers.len());
        for peer in &peers {
            if let Some(state) = self.states.get(peer) {
                peer_states.push((*peer, Arc::clone(state.value())));
            }
        }
        let receiver_plan = self.advance_slice_state(&peer_states, &config);
        if receiver_plan.receivers.is_empty() {
            let tick_micros = tick_start.elapsed().as_micros() as u64;
            self.counters
                .tick_micros
                .fetch_add(tick_micros, Ordering::Relaxed);
            self.counters.tick_count.fetch_add(1, Ordering::Relaxed);
            update_max_atomic(&self.counters.max_tick_micros, tick_micros);
            self.adapt_slice_count(tick_micros, &config);
            return Ok(());
        }
        let spatial_grid = if config.spatial_cull_enabled {
            SpatialGrid::build(&peer_states, config.low_distance_sq)
        } else {
            None
        };
        self.profiler.add_phase_micros(BsrPhase::Distance, 0);

        let build_start = Instant::now();
        // Avoid cloning states for receiver_states: par_iter over slice directly.
        // build_sends_for_receiver only needs position from receiver state.
        let offloaded_empty = self.offloaded_pairs.is_empty();
        let bypass_empty = self.bypass_reduction_ids.is_empty();
        let receiver_groups = receiver_plan
            .receivers
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
                        receiver_plan.receiver_cycle,
                        receiver_plan.effective_tick_interval_ms,
                        receiver_plan.update_distances,
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
        flush_receiver_groups_parallel(
            transport.clone(),
            receiver_groups,
            self.diagnostics.as_deref(),
        )?;
        if let Some(diagnostics) = self.diagnostics.as_ref() {
            diagnostics.maybe_emit(&self.states, &self.tracking);
        }
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
                if let Ok(frame) = LazyQualityFrame::new(
                    &self.profiler,
                    update.peer_id,
                    current.outbound_sequence,
                    update.quality,
                    avatar_payload,
                    additional_data,
                    config.strip_additional_data_at_low_quality,
                ) {
                    let _ = update_lazy_outbound_delta_state(
                        current,
                        frame,
                        generation,
                        config,
                        Instant::now(),
                        &self.payload_pool,
                        &self.profiler,
                    );
                }
                self.payload_pool.put(update.payload);
                continue;
            }

            if let Ok(frame) = LazyQualityFrame::new(
                &self.profiler,
                update.peer_id,
                0,
                update.quality,
                avatar_payload,
                additional_data,
                config.strip_additional_data_at_low_quality,
            ) {
                let now = Instant::now();
                let qualities = lazy_frame_eager_view(&frame);
                let keyframe_payloads = quality_payloads(&qualities);
                self.states.insert(
                    update.peer_id,
                    Arc::new(PlayerAvatarState {
                        #[cfg(test)]
                        peer_id: update.peer_id,
                        incarnation: generation,
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
                        lazy_current: Some(Arc::clone(&frame)),
                        lazy_keyframe: Some(frame),
                        lazy_deltas: None,
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
        receiver_cycle: usize,
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
        let diagnostic_active = self
            .diagnostics
            .as_ref()
            .is_some_and(|diagnostics| diagnostics.observer_id == receiver_id);
        let mut diagnostic_items = Vec::new();
        if diagnostic_active {
            diagnostic_items.reserve(initial_send_capacity);
        }
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
            let current_quality = bit_quality_from_index(quality_index);
            let current_packet = match sender_state.lazy_current.as_ref() {
                Some(frame) => frame
                    .quality(&self.payload_pool, &self.profiler, current_quality)
                    .ok()
                    .flatten(),
                None => sender_state.qualities[quality_index as usize].as_ref(),
            };
            let Some(current_packet) = current_packet else {
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
                    receiver_cycle,
                    effective_tick_interval_ms,
                    config.default_interval_ms,
                )
            };

            let delta_packet = match sender_state.lazy_deltas.as_ref() {
                Some(deltas) => deltas
                    .quality(&self.payload_pool, &self.profiler, current_quality)
                    .ok()
                    .flatten(),
                None => sender_state.deltas[quality_index as usize].as_ref(),
            };
            let send_delta = config.enable_delta_compression
                && !bypass_reduction
                && !sender_state.current_is_keyframe
                && tracking.baseline_keyframe_generation == sender_state.keyframe_generation
                && tracking.baseline_quality == quality_index
                && delta_packet.is_some();

            let (channel, packet_bytes, interval_offset) = if send_delta {
                let delta = delta_packet.expect("checked above");
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
                        let keyframe = match sender_state.lazy_keyframe.as_ref() {
                            Some(frame) => frame
                                .quality(&self.payload_pool, &self.profiler, current_quality)
                                .ok()
                                .flatten(),
                            None => {
                                sender_state.keyframe_qualities[quality_index as usize].as_ref()
                            }
                        };
                        let Some(keyframe) = keyframe else {
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
            if diagnostic_active {
                diagnostic_items.push((sender_id, quality_index, send_delta));
            }

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
            .fetch_add(logical_sends, Ordering::Relaxed);
        // Only the independently owned `direct` sends escape this receiver build. In
        // particular, release every Bytes clone held by scratch before it is reused.
        bundle.clear();
        (!direct.is_empty()).then_some(OutboundAvatarBatch {
            receiver: receiver_id,
            sends: direct,
            diagnostic_items,
        })
    }

    fn advance_slice_state(
        &self,
        peer_states: &[(PeerId, Arc<PlayerAvatarState>)],
        config: &AvatarSyncConfig,
    ) -> ReceiverSlicePlan {
        let mut state = self.slice_state.lock();
        let now = Instant::now();

        if peer_states.len() <= 1 {
            // There is no recipient work with zero or one authenticated active avatar.
            // Terminate the old roster instead of letting departed peers linger in it.
            state.cycle = None;
        } else if state
            .cycle
            .as_ref()
            .is_none_or(|cycle| cycle.cursor >= cycle.roster.len())
        {
            // Membership is frozen for one bounded cycle. New arrivals join at the next
            // boundary; departures are skipped as their IDs are resolved below.
            let roster = peer_states
                .iter()
                .map(|(peer_id, avatar)| (*peer_id, avatar.incarnation))
                .collect::<Vec<_>>();
            let minimum = config.min_receiver_slices.max(1);
            let maximum = config.max_receiver_slices.max(minimum).min(MAX_SLICE_COUNT);
            state.slice_count = state.slice_count.clamp(minimum, maximum);
            state.cycle = Some(ReceiverCycle {
                roster,
                cursor: 0,
                slice_count: state.slice_count,
            });
        }

        let mut selected = Vec::new();
        let receiver_cycle = if let Some(cycle) = state.cycle.as_mut() {
            let mut current =
                PeerIdMap::with_capacity_and_hasher(peer_states.len(), Default::default());
            for (index, (peer_id, avatar)) in peer_states.iter().enumerate() {
                current.insert(*peer_id, (index, avatar.incarnation));
            }
            let selected_indices = select_receiver_cycle_indices(cycle, &current);
            selected.reserve(selected_indices.len());
            for index in selected_indices {
                selected.push((peer_states[index].0, Arc::clone(&peer_states[index].1)));
            }
            receiver_cycle_length(cycle.roster.len(), cycle.slice_count)
        } else {
            0
        };

        let update_distances = now.duration_since(state.last_distance_update)
            >= Duration::from_millis(DISTANCE_UPDATE_INTERVAL_MS);
        if update_distances {
            state.last_distance_update = now;
        }
        let effective_tick_interval_ms =
            AVATAR_TICK_INTERVAL_MS.max(state.smoothed_tick_micros.div_ceil(1_000));
        ReceiverSlicePlan {
            receiver_cycle,
            receivers: selected,
            update_distances,
            effective_tick_interval_ms,
        }
    }

    fn adapt_slice_count(&self, elapsed_micros: u64, config: &AvatarSyncConfig) {
        let mut state = self.slice_state.lock();
        state.smoothed_tick_micros = if state.smoothed_tick_micros == 0 {
            elapsed_micros
        } else {
            ((state.smoothed_tick_micros as f64 * 0.85) + (elapsed_micros as f64 * 0.15)) as u64
        };

        let Some(cycle) = state.cycle.as_ref() else {
            return;
        };
        if cycle.cursor < cycle.roster.len() {
            return;
        }

        // Choose the next geometry only once per completed cycle. This keeps the
        // in-flight roster and its slice width stable while adapting from effective
        // receiver-cycle lengths rather than requested slice counts.
        let active_count = cycle.roster.len();
        let active_slices = cycle.slice_count;
        let min_slices = config.min_receiver_slices.max(1);
        let max_slices = config
            .max_receiver_slices
            .max(min_slices)
            .min(MAX_SLICE_COUNT);
        let current_target = state.slice_count.clamp(min_slices, max_slices);
        let tick_budget_micros = (config.tick_budget_ms.max(1.0) * 1000.0) as u64;
        let cycle_budget_micros = (config.receiver_cycle_budget_ms.max(1.0) * 1000.0) as u64;
        let estimated_cycle_micros = state
            .smoothed_tick_micros
            .saturating_mul(receiver_cycle_length(active_count, active_slices) as u64);
        let projected_larger_cycle_micros = state
            .smoothed_tick_micros
            .saturating_mul(receiver_cycle_length(active_count, current_target + 1) as u64);

        let mut next_slice_count = current_target;
        if estimated_cycle_micros > cycle_budget_micros {
            if elapsed_micros > tick_budget_micros {
                if current_target < max_slices {
                    next_slice_count = current_target + 1;
                }
            } else if current_target > min_slices {
                next_slice_count = current_target - 1;
            }
        } else if elapsed_micros < tick_budget_micros.saturating_mul(3) / 4
            && current_target > min_slices
        {
            next_slice_count = current_target - 1;
        } else if elapsed_micros > tick_budget_micros
            && projected_larger_cycle_micros <= cycle_budget_micros
            && current_target < max_slices
        {
            next_slice_count = current_target + 1;
        }
        state.slice_count = next_slice_count;
    }
}

fn receiver_cycle_bounds(
    receiver_count: usize,
    slice_count: usize,
    cursor: usize,
) -> (usize, usize) {
    let start = cursor.min(receiver_count);
    let width = receiver_count.div_ceil(slice_count.max(1));
    (start, start.saturating_add(width).min(receiver_count))
}

fn receiver_cycle_length(receiver_count: usize, slice_count: usize) -> usize {
    if receiver_count == 0 {
        0
    } else {
        receiver_count.div_ceil(receiver_count.div_ceil(slice_count.max(1)))
    }
}

fn select_receiver_cycle_indices(
    cycle: &mut ReceiverCycle,
    current: &PeerIdMap<(usize, u64)>,
) -> Vec<usize> {
    let (start, end) = receiver_cycle_bounds(cycle.roster.len(), cycle.slice_count, cycle.cursor);
    cycle.cursor = end;
    cycle.roster[start..end]
        .iter()
        .filter_map(|(peer_id, incarnation)| {
            current
                .get(peer_id)
                .and_then(|(index, current_incarnation)| {
                    (current_incarnation == incarnation).then_some(*index)
                })
        })
        .collect()
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
    diagnostics: Option<&AvatarSyncDiagnostics>,
) -> Result<()> {
    receiver_groups
        .par_iter()
        .with_min_len(RECEIVER_FLUSH_MIN_BATCH)
        .try_for_each(|batch| {
            if let Some(diagnostics) = diagnostics {
                diagnostics.record_batch(batch.receiver, &batch.diagnostic_items, false);
            }
            transport
                .try_send_many_unreliable_packets(batch.receiver, &batch.sends)
                .map(|_| {
                    if let Some(diagnostics) = diagnostics {
                        diagnostics.record_batch(batch.receiver, &batch.diagnostic_items, true);
                    }
                })
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

fn lazy_frame_eager_view(frame: &LazyQualityFrame) -> [Option<PreSerializedQuality>; 4] {
    let mut qualities = [None, None, None, None];
    if let Some(Ok(Some(packet))) = frame.qualities[frame.source_quality as usize].get() {
        qualities[frame.source_quality as usize] = Some(packet.clone());
    }
    qualities
}

fn bit_quality_from_index(index: u8) -> BitQuality {
    match index {
        0 => BitQuality::VeryLow,
        1 => BitQuality::Low,
        2 => BitQuality::Medium,
        _ => BitQuality::High,
    }
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

#[cfg(test)]
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

fn update_lazy_outbound_delta_state(
    state: &mut PlayerAvatarState,
    current: Arc<LazyQualityFrame>,
    generation: u64,
    config: &AvatarSyncConfig,
    now: Instant,
    pool: &BytePool,
    profiler: &BsrProfiler,
) -> Result<()> {
    let current_payload = current.payload(pool, profiler, BitQuality::High)?;
    let keyframe_interval = effective_keyframe_interval_ms(config, state.keyframe_stretch_shift);
    let mut is_keyframe = !config.enable_delta_compression
        || state
            .lazy_keyframe
            .as_ref()
            .and_then(|frame| {
                frame
                    .quality(pool, profiler, BitQuality::High)
                    .ok()
                    .flatten()
            })
            .is_none()
        || now.duration_since(state.last_keyframe)
            >= Duration::from_millis(keyframe_interval.max(1));

    let high_delta = if !is_keyframe {
        let baseline = state
            .lazy_keyframe
            .as_ref()
            .map(|frame| frame.payload(pool, profiler, BitQuality::High))
            .transpose()?
            .flatten();
        match (baseline, current_payload) {
            (Some(baseline), Some(current_payload)) => {
                match build_delta(
                    baseline.as_ref(),
                    current_payload.as_ref(),
                    BitQuality::High,
                ) {
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

    let current_qualities = lazy_frame_eager_view(&current);
    if is_keyframe {
        state.keyframe_qualities = current_qualities.clone();
        state.keyframe_payloads = quality_payloads(&current_qualities);
        state.lazy_keyframe = Some(Arc::clone(&current));
        state.deltas = [None, None, None, None];
        state.lazy_deltas = None;
        state.keyframe_generation = generation;
        state.keyframe_sequence = state.outbound_sequence;
        state.last_keyframe = now;
        state.current_is_keyframe = true;
    } else {
        let baseline = state
            .lazy_keyframe
            .as_ref()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing lazy keyframe frame"))?;
        let high_delta = high_delta
            .as_deref()
            .ok_or_else(|| anyhow::anyhow!("missing High delta for non-keyframe"))?;
        state.lazy_deltas = Some(LazyDeltaFrame::new(
            Arc::clone(&current),
            baseline,
            state.outbound_sequence,
            state.keyframe_sequence,
            high_delta,
            pool,
            profiler,
        )?);
        state.deltas = [None, None, None, None];
        state.current_is_keyframe = false;
    }
    state.qualities = current_qualities;
    state.lazy_current = Some(current);
    Ok(())
}

#[cfg(test)]
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
#[cfg(test)]
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
    receiver_cycle: usize,
    effective_tick_interval_ms: u64,
    base_interval_ms: u64,
) -> u8 {
    let deliverable_interval_ms = effective_tick_interval_ms
        .max(AVATAR_TICK_INTERVAL_MS)
        .saturating_mul(receiver_cycle.max(1) as u64);
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

    fn generation_map(states: &[(PeerId, u64)]) -> PeerIdMap<(usize, u64)> {
        let mut map = PeerIdMap::with_capacity_and_hasher(states.len(), Default::default());
        for (index, (peer_id, incarnation)) in states.iter().enumerate() {
            map.insert(*peer_id, (index, *incarnation));
        }
        map
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
                        incarnation: generation,
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
                        lazy_current: None,
                        lazy_keyframe: None,
                        lazy_deltas: None,
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
    fn lazy_quality_packets_match_eager_packets_for_all_inputs_and_policies() {
        let pool = BytePool::new();
        let profiler = BsrProfiler::new(false);
        let additional = [1, 0, 1, 7, 42];
        for peer_id in [42, 300] {
            for source_quality in [
                BitQuality::VeryLow,
                BitQuality::Low,
                BitQuality::Medium,
                BitQuality::High,
            ] {
                for strip in [false, true] {
                    let payload = (0..source_quality.payload_len())
                        .map(|index| (index as u8).wrapping_mul(17).wrapping_add(31))
                        .collect::<Vec<_>>();
                    let eager = build_quality_packets(
                        &pool,
                        &profiler,
                        peer_id,
                        251,
                        source_quality,
                        &payload,
                        &additional,
                        strip,
                    )
                    .unwrap();
                    let lazy = LazyQualityFrame::new(
                        &profiler,
                        peer_id,
                        251,
                        source_quality,
                        &payload,
                        &additional,
                        strip,
                    )
                    .unwrap();
                    for quality in [
                        BitQuality::VeryLow,
                        BitQuality::Low,
                        BitQuality::Medium,
                        BitQuality::High,
                    ] {
                        let actual = lazy.quality(&pool, &profiler, quality).unwrap().cloned();
                        assert_eq!(actual, eager[quality as usize]);
                        if source_quality == BitQuality::High && quality != BitQuality::High {
                            assert_eq!(
                                lazy.init_counts[quality as usize].load(Ordering::Relaxed),
                                1
                            );
                        }
                    }
                }
            }
        }

        let malformed = vec![0; BitQuality::High.payload_len() - 1];
        assert!(
            LazyQualityFrame::new(&profiler, 42, 0, BitQuality::High, &malformed, &[], false,)
                .is_err()
        );
    }

    #[test]
    fn lazy_deltas_match_eager_packets_and_keep_frozen_keyframe_across_generations() {
        let pool = BytePool::new();
        let profiler = BsrProfiler::new(false);
        let additional = [1, 0, 1, 7, 42];
        let baseline_payload = (0..BitQuality::High.payload_len())
            .map(|index| (index as u8).wrapping_mul(13).wrapping_add(9))
            .collect::<Vec<_>>();
        let mut current_payload = baseline_payload.clone();
        for index in [0usize, 12, 28, 55, 91, 127, 158] {
            current_payload[index] = current_payload[index].wrapping_add(1);
        }
        let baseline = LazyQualityFrame::new(
            &profiler,
            300,
            77,
            BitQuality::High,
            &baseline_payload,
            &additional,
            false,
        )
        .unwrap();
        let current = LazyQualityFrame::new(
            &profiler,
            300,
            78,
            BitQuality::High,
            &current_payload,
            &additional,
            false,
        )
        .unwrap();
        let eager_baseline = build_quality_packets(
            &pool,
            &profiler,
            300,
            77,
            BitQuality::High,
            &baseline_payload,
            &additional,
            false,
        )
        .unwrap();
        let eager_current = build_quality_packets(
            &pool,
            &profiler,
            300,
            78,
            BitQuality::High,
            &current_payload,
            &additional,
            false,
        )
        .unwrap();
        let baseline_payloads = quality_payloads(&eager_baseline);
        let current_payloads = quality_payloads(&eager_current);
        let high = build_delta(
            baseline_payloads[BitQuality::High as usize]
                .as_ref()
                .unwrap(),
            current_payloads[BitQuality::High as usize]
                .as_ref()
                .unwrap(),
            BitQuality::High,
        )
        .unwrap();
        let expected = build_delta_packets(
            300,
            78,
            77,
            &baseline_payloads,
            &current_payloads,
            &eager_current,
            Some(&high),
        );
        let lazy = LazyDeltaFrame::new(
            Arc::clone(&current),
            Arc::clone(&baseline),
            78,
            77,
            &high,
            &pool,
            &profiler,
        )
        .unwrap();
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            assert_eq!(
                lazy.quality(&pool, &profiler, quality).unwrap().cloned(),
                expected[quality as usize]
            );
        }
        let old_low = lazy
            .quality(&pool, &profiler, BitQuality::Low)
            .unwrap()
            .unwrap()
            .clone();

        let mut later_payload = current_payload;
        later_payload[63] ^= 0x80;
        let later = LazyQualityFrame::new(
            &profiler,
            300,
            79,
            BitQuality::High,
            &later_payload,
            &additional,
            false,
        )
        .unwrap();
        let later_high = build_delta(
            baseline_payloads[BitQuality::High as usize]
                .as_ref()
                .unwrap(),
            &later
                .payload(&pool, &profiler, BitQuality::High)
                .unwrap()
                .unwrap(),
            BitQuality::High,
        )
        .unwrap();
        let later_deltas = LazyDeltaFrame::new(
            Arc::clone(&later),
            baseline,
            79,
            77,
            &later_high,
            &pool,
            &profiler,
        )
        .unwrap();
        assert_eq!(
            lazy.quality(&pool, &profiler, BitQuality::Low)
                .unwrap()
                .unwrap(),
            &old_low
        );
        assert!(later_deltas
            .quality(&pool, &profiler, BitQuality::Low)
            .unwrap()
            .is_some());
    }

    #[test]
    fn cold_old_delta_demand_survives_keyframe_replacement_and_peer_id_reuse() {
        let pool = BytePool::new();
        let profiler = BsrProfiler::new(false);
        let additional = [1, 0, 1, 3, 91];
        let payload = |seed: u8| {
            (0..BitQuality::High.payload_len())
                .map(|index| (index as u8).wrapping_mul(23).wrapping_add(seed))
                .collect::<Vec<_>>()
        };

        // Keep an old High keyframe and a later generation whose Low delta has
        // never been requested. The same numeric peer ID is then reused after
        // a new keyframe, with sequence numbers wrapping across the reuse.
        let old_baseline_payload = payload(11);
        let mut old_current_payload = old_baseline_payload.clone();
        for index in [2usize, 17, 45, 90, 131] {
            old_current_payload[index] ^= 0x40;
        }
        let old_baseline = LazyQualityFrame::new(
            &profiler,
            42,
            254,
            BitQuality::High,
            &old_baseline_payload,
            &additional,
            false,
        )
        .unwrap();
        let old_current = LazyQualityFrame::new(
            &profiler,
            42,
            255,
            BitQuality::High,
            &old_current_payload,
            &additional,
            false,
        )
        .unwrap();
        let old_high_delta = build_delta(
            &old_baseline_payload,
            &old_current_payload,
            BitQuality::High,
        )
        .unwrap();
        let old_delta = LazyDeltaFrame::new(
            Arc::clone(&old_current),
            Arc::clone(&old_baseline),
            255,
            254,
            &old_high_delta,
            &pool,
            &profiler,
        )
        .unwrap();
        assert!(old_delta.deltas[BitQuality::Low as usize].get().is_none());

        // Replacement/keyframe materialization and reuse do not mutate old
        // snapshots or change the source bytes they own.
        let replacement_payload = payload(99);
        let replacement_keyframe = LazyQualityFrame::new(
            &profiler,
            42,
            0,
            BitQuality::High,
            &replacement_payload,
            &additional,
            true,
        )
        .unwrap();
        let reused_id_payload = payload(177);
        let reused_id_current = LazyQualityFrame::new(
            &profiler,
            42,
            1,
            BitQuality::High,
            &reused_id_payload,
            &additional,
            true,
        )
        .unwrap();
        let replacement_low = replacement_keyframe
            .quality(&pool, &profiler, BitQuality::Low)
            .unwrap()
            .unwrap()
            .clone();
        let reused_low = reused_id_current
            .quality(&pool, &profiler, BitQuality::Low)
            .unwrap()
            .unwrap()
            .clone();

        let eager_old_baseline = build_quality_packets(
            &pool,
            &profiler,
            42,
            254,
            BitQuality::High,
            &old_baseline_payload,
            &additional,
            false,
        )
        .unwrap();
        let eager_old_current = build_quality_packets(
            &pool,
            &profiler,
            42,
            255,
            BitQuality::High,
            &old_current_payload,
            &additional,
            false,
        )
        .unwrap();
        let baseline_payloads = quality_payloads(&eager_old_baseline);
        let current_payloads = quality_payloads(&eager_old_current);
        let expected_old = build_delta_packets(
            42,
            255,
            254,
            &baseline_payloads,
            &current_payloads,
            &eager_old_current,
            Some(&old_high_delta),
        );

        // First demand is deliberately cold and happens after both replacement
        // and ID reuse. It must still use the old sequence, additional data,
        // current frame, and frozen keyframe bytes.
        assert_eq!(
            old_delta
                .quality(&pool, &profiler, BitQuality::Low)
                .unwrap()
                .cloned(),
            expected_old[BitQuality::Low as usize]
        );
        assert_eq!(
            old_delta.init_counts[BitQuality::Low as usize].load(Ordering::Relaxed),
            1
        );
        assert_ne!(
            old_delta
                .quality(&pool, &profiler, BitQuality::Low)
                .unwrap()
                .unwrap()
                .bytes_small,
            reused_low.bytes_small
        );
        assert_ne!(replacement_low.bytes_small, reused_low.bytes_small);
        assert_eq!(old_delta.baseline.peer_id, 42);
        assert_eq!(old_delta.baseline.outbound_sequence, 254);
        assert_eq!(old_delta.current.outbound_sequence, 255);
    }

    #[test]
    fn concurrent_first_quality_demand_publishes_one_immutable_cache_entry() {
        let pool = BytePool::new();
        let profiler = BsrProfiler::new(false);
        let payload = vec![0x53; BitQuality::High.payload_len()];
        let frame =
            LazyQualityFrame::new(&profiler, 42, 10, BitQuality::High, &payload, &[], false)
                .unwrap();
        let results = (0..64usize)
            .into_par_iter()
            .map(|_| {
                frame
                    .quality(&pool, &profiler, BitQuality::Medium)
                    .unwrap()
                    .unwrap()
                    .bytes_small
                    .clone()
            })
            .collect::<Vec<_>>();
        assert!(results.iter().all(|bytes| bytes == &results[0]));
        assert_eq!(
            frame.init_counts[BitQuality::Medium as usize].load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            frame.init_counts[BitQuality::Low as usize].load(Ordering::Relaxed),
            0
        );
    }

    #[test]
    fn lazy_state_trace_matches_eager_keyframe_and_delta_transitions() {
        let pool = BytePool::new();
        let profiler = BsrProfiler::new(false);
        let mut config = receiver_build_test_config();
        config.enable_delta_compression = true;
        config.delta_keyframe_interval_ms = 500;
        config.delta_keyframe_max_interval_ms = 500;
        let start = Instant::now();
        let additional = [1, 0, 1, 4, 22];
        let make_payload = |seed: u8| {
            (0..BitQuality::High.payload_len())
                .map(|index| (index as u8).wrapping_mul(19).wrapping_add(seed))
                .collect::<Vec<_>>()
        };
        let initial_payload = make_payload(8);
        let initial_frame = LazyQualityFrame::new(
            &profiler,
            42,
            20,
            BitQuality::High,
            &initial_payload,
            &additional,
            true,
        )
        .unwrap();
        let initial_eager = build_quality_packets(
            &pool,
            &profiler,
            42,
            20,
            BitQuality::High,
            &initial_payload,
            &additional,
            true,
        )
        .unwrap();
        let make_state = |qualities: [Option<PreSerializedQuality>; 4],
                          current_frame: Option<Arc<LazyQualityFrame>>,
                          keyframe_frame: Option<Arc<LazyQualityFrame>>| {
            let payloads = quality_payloads(&qualities);
            PlayerAvatarState {
                peer_id: 42,
                incarnation: 1,
                small_id: true,
                position: [0.0; 3],
                generation: 1,
                last_inbound_sequence: 20,
                outbound_sequence: 20,
                has_received_first: true,
                qualities: qualities.clone(),
                keyframe_qualities: qualities,
                keyframe_payloads: payloads,
                deltas: [None, None, None, None],
                keyframe_generation: 1,
                keyframe_sequence: 20,
                last_keyframe: start,
                keyframe_stretch_shift: 0,
                small_delta_streak: 0,
                current_is_keyframe: true,
                lazy_current: current_frame,
                lazy_keyframe: keyframe_frame,
                lazy_deltas: None,
            }
        };
        let mut lazy_state = make_state(
            lazy_frame_eager_view(&initial_frame),
            Some(Arc::clone(&initial_frame)),
            Some(Arc::clone(&initial_frame)),
        );
        let mut eager_state = make_state(initial_eager, None, None);

        let mut second_payload = initial_payload.clone();
        second_payload[12] ^= 0x01;
        second_payload[85] ^= 0x02;
        let second_frame = LazyQualityFrame::new(
            &profiler,
            42,
            21,
            BitQuality::High,
            &second_payload,
            &additional,
            true,
        )
        .unwrap();
        let second_eager = build_quality_packets(
            &pool,
            &profiler,
            42,
            21,
            BitQuality::High,
            &second_payload,
            &additional,
            true,
        )
        .unwrap();
        // The later config change must not alter policy captured for this frame.
        config.strip_additional_data_at_low_quality = false;
        let second_time = start + Duration::from_millis(100);
        update_lazy_outbound_delta_state(
            &mut lazy_state,
            second_frame,
            2,
            &config,
            second_time,
            &pool,
            &profiler,
        )
        .unwrap();
        update_outbound_delta_state(&mut eager_state, second_eager, 2, &config, second_time);
        assert_eq!(
            lazy_state.current_is_keyframe,
            eager_state.current_is_keyframe
        );
        assert_eq!(
            lazy_state.keyframe_generation,
            eager_state.keyframe_generation
        );
        assert_eq!(lazy_state.keyframe_sequence, eager_state.keyframe_sequence);
        assert_eq!(lazy_state.last_keyframe, eager_state.last_keyframe);
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            assert_eq!(
                lazy_state
                    .lazy_current
                    .as_ref()
                    .unwrap()
                    .quality(&pool, &profiler, quality)
                    .unwrap(),
                eager_state.qualities[quality as usize].as_ref()
            );
            assert_eq!(
                lazy_state
                    .lazy_deltas
                    .as_ref()
                    .unwrap()
                    .quality(&pool, &profiler, quality)
                    .unwrap(),
                eager_state.deltas[quality as usize].as_ref()
            );
        }

        let mut third_payload = second_payload.clone();
        third_payload[63] ^= 0x04;
        let third_frame = LazyQualityFrame::new(
            &profiler,
            42,
            22,
            BitQuality::High,
            &third_payload,
            &additional,
            false,
        )
        .unwrap();
        let third_eager = build_quality_packets(
            &pool,
            &profiler,
            42,
            22,
            BitQuality::High,
            &third_payload,
            &additional,
            false,
        )
        .unwrap();
        let keyframe_time = start + Duration::from_millis(600);
        update_lazy_outbound_delta_state(
            &mut lazy_state,
            third_frame,
            3,
            &config,
            keyframe_time,
            &pool,
            &profiler,
        )
        .unwrap();
        update_outbound_delta_state(&mut eager_state, third_eager, 3, &config, keyframe_time);
        assert_eq!(
            lazy_state.current_is_keyframe,
            eager_state.current_is_keyframe
        );
        assert!(lazy_state.current_is_keyframe);
        assert_eq!(
            lazy_state.keyframe_generation,
            eager_state.keyframe_generation
        );
        assert_eq!(lazy_state.keyframe_sequence, eager_state.keyframe_sequence);
        assert_eq!(lazy_state.last_keyframe, eager_state.last_keyframe);
        assert!(lazy_state.lazy_deltas.is_none());
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
            incarnation: 1,
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
            lazy_current: None,
            lazy_keyframe: None,
            lazy_deltas: None,
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

        system.slice_state.lock().cycle = Some(ReceiverCycle {
            roster: (0..4).map(|id| (id, id as u64)).collect(),
            cursor: 4,
            slice_count: 1,
        });
        system.adapt_slice_count(1_000, &config);
        assert_eq!(system.slice_state.lock().slice_count, 1);

        system.slice_state.lock().cycle = Some(ReceiverCycle {
            roster: (0..4).map(|id| (id, id as u64)).collect(),
            cursor: 4,
            slice_count: 1,
        });
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
            system.slice_state.lock().cycle = Some(ReceiverCycle {
                roster: (0..1_500).map(|id| (id, id as u64)).collect(),
                cursor: 1_500,
                slice_count: 32,
            });
            system.adapt_slice_count(10_000, &config);
            assert_eq!(system.slice_state.lock().slice_count, 32);
        }
    }

    #[test]
    fn slice_target_changes_only_after_active_cycle_completes() {
        let mut config = receiver_build_test_config();
        config.min_receiver_slices = 1;
        config.max_receiver_slices = 32;
        config.tick_budget_ms = 3.0;
        config.receiver_cycle_budget_ms = 180.0;
        let system = AvatarSyncSystem::new(config.clone());
        system.slice_state.lock().cycle = Some(ReceiverCycle {
            roster: (0..17).map(|id| (id, id as u64 + 1)).collect(),
            cursor: 1,
            slice_count: 1,
        });

        for _ in 0..10 {
            system.adapt_slice_count(1_000, &config);
        }
        {
            let state = system.slice_state.lock();
            assert_eq!(state.slice_count, 1);
            assert_eq!(state.cycle.as_ref().unwrap().slice_count, 1);
        }

        system.slice_state.lock().cycle.as_mut().unwrap().cursor = 17;
        system.adapt_slice_count(4_000, &config);
        let state = system.slice_state.lock();
        assert_eq!(state.slice_count, 2);
        assert_eq!(state.cycle.as_ref().unwrap().slice_count, 1);
    }

    #[test]
    fn identity_cycle_visits_stable_roster_once_across_slice_changes() {
        for receiver_count in [2, 17, 100, 1_500] {
            let mut visits = vec![0_u8; receiver_count];
            for slices in [32, 31, 7, 1, 32, 2] {
                let roster = (0..receiver_count)
                    .map(|id| (id as PeerId, id as u64 + 1))
                    .collect::<Vec<_>>();
                let mut cycle = ReceiverCycle {
                    roster,
                    cursor: 0,
                    slice_count: slices,
                };
                let current = generation_map(
                    &(0..receiver_count)
                        .map(|id| (id as PeerId, id as u64 + 1))
                        .collect::<Vec<_>>(),
                );
                while cycle.cursor < cycle.roster.len() {
                    for index in select_receiver_cycle_indices(&mut cycle, &current) {
                        visits[index] += 1;
                    }
                }
                assert!(visits.iter().all(|count| *count == 1));
                visits.fill(0);
            }
        }
    }

    #[test]
    fn identity_cycle_skips_departures_and_reused_ids_then_admits_joins_next_cycle() {
        let mut cycle = ReceiverCycle {
            roster: vec![(10, 1), (20, 2), (30, 3), (40, 4)],
            cursor: 0,
            slice_count: 2,
        };
        let first = generation_map(&[(30, 3), (10, 1), (20, 99), (50, 5)]);
        assert_eq!(select_receiver_cycle_indices(&mut cycle, &first), vec![1]);
        assert_eq!(cycle.cursor, 2);
        // The same ID with a new incarnation is a replacement; new ID 50 waits for
        // the next cycle, and remaining old-roster peers are still progressed.
        let reordered = generation_map(&[(40, 4), (20, 99), (30, 3), (50, 5)]);
        assert_eq!(
            select_receiver_cycle_indices(&mut cycle, &reordered),
            vec![2, 0]
        );
        assert_eq!(cycle.cursor, 4);
        assert_eq!(cycle.slice_count, 2);

        let next = ReceiverCycle {
            roster: vec![(40, 4), (20, 99), (30, 3), (50, 5)],
            cursor: 0,
            slice_count: 3,
        };
        assert_eq!(
            receiver_cycle_length(next.roster.len(), next.slice_count),
            2
        );
        assert_eq!(receiver_cycle_bounds(0, 32, 0), (0, 0));
        assert_eq!(receiver_cycle_bounds(1, 32, 0), (0, 1));
    }

    #[test]
    fn current_snapshot_order_resolves_latest_pose_for_same_incarnation() {
        let mut cycle = ReceiverCycle {
            roster: vec![(10, 1), (20, 2)],
            cursor: 0,
            slice_count: 1,
        };
        // The map index addresses this tick's current snapshot, whose position data
        // changed since roster creation; cycle entries retain identity, never poses.
        let current_positions = [[20.0, 0.0, 0.0], [10.0, 0.0, 0.0]];
        let reordered = generation_map(&[(20, 2), (10, 1)]);
        let selected = select_receiver_cycle_indices(&mut cycle, &reordered);
        assert_eq!(selected, vec![1, 0]);
        assert_eq!(
            selected
                .iter()
                .map(|index| current_positions[*index])
                .collect::<Vec<_>>(),
            vec![[10.0, 0.0, 0.0], [20.0, 0.0, 0.0]]
        );
    }

    #[test]
    fn sustained_churn_does_not_reset_a_surviving_tail_peer() {
        let tail = 1499;
        let mut cycle = ReceiverCycle {
            roster: (0..=tail).map(|id| (id as PeerId, id as u64 + 1)).collect(),
            cursor: 0,
            slice_count: 32,
        };
        let mut selected_tail = false;
        let mut joins = 0_u16;
        while cycle.cursor < cycle.roster.len() {
            let tick = cycle.cursor;
            let mut current = vec![(tail as PeerId, (0, tail as u64 + 1))];
            // Vary the lookup order and add IDs every selection window while keeping
            // the old-roster tail continuously authenticated.
            for id in tick..tick + 8 {
                if id < tail {
                    current.push((id as PeerId, (current.len(), id as u64 + 1)));
                }
            }
            joins = joins.wrapping_add(1);
            current.push((joins.wrapping_add(2_000), (current.len(), u64::from(joins))));
            let map = current.into_iter().collect::<PeerIdMap<_>>();
            let selected = select_receiver_cycle_indices(&mut cycle, &map);
            if selected.contains(&0) {
                selected_tail = true;
            }
        }
        assert!(
            selected_tail,
            "the roster tail must receive its turn within one cycle"
        );
    }

    #[test]
    fn effective_receiver_cycle_length_uses_ceil_geometry() {
        assert_eq!(receiver_cycle_length(0, 32), 0);
        assert_eq!(receiver_cycle_length(1, 32), 1);
        assert_eq!(receiver_cycle_length(17, 32), 17);
        assert_eq!(receiver_cycle_length(100, 32), 25);
        assert_eq!(receiver_cycle_length(1_500, 31), 31);
        assert_eq!(receiver_cycle_length(1_500, 32), 32);
        assert_eq!(receiver_cycle_length(1_500, 33), 33);

        for (count, slices, cycle_ms) in [
            (17, 32, 68),
            (100, 32, 100),
            (1_500, 31, 124),
            (1_500, 32, 128),
        ] {
            let cycle = receiver_cycle_length(count, slices);
            let encoded = advertised_interval_byte(0, 0, cycle, 4, 50);
            assert_eq!(channels::decode_avatar_interval_ms(encoded, 50), cycle_ms);
        }
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
