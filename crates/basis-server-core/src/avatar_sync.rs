use anyhow::Result;
use basis_protocol::{
    avatar::{
        encode_avatar_bundle, read_position, repack_high_to_lower_into, AvatarBundleItem,
        BitQuality,
    },
    channels,
};
use basis_transport::{DeliveryMethod, PeerId, TransportHandle};
use bytes::Bytes;
use dashmap::DashMap;
use rayon::prelude::*;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};
use tokio::runtime::Handle;
use tracing::warn;

const MAX_AVATAR_SENDERS_PER_RECEIVER_PER_TICK: usize = 96;
const DISTANCE_UPDATE_INTERVAL_TICKS: usize = 125;
const MAX_SLICE_COUNT: usize = 32;

#[derive(Debug, Clone)]
pub struct AvatarSyncConfig {
    pub default_interval_ms: u64,
    pub high_distance_sq: f32,
    pub medium_distance_sq: f32,
    pub low_distance_sq: f32,
    pub enable_bundle_compression: bool,
    pub bundle_min_messages: usize,
    pub bundle_min_bytes: usize,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ReceiverSenderKey {
    receiver: PeerId,
    sender: PeerId,
}

#[derive(Debug, Clone)]
struct ReceiverTracking {
    last_seen_generation: u64,
    last_sent: Instant,
    cached_quality_index: u8,
}

#[derive(Debug, Clone, Copy)]
struct SliceState {
    slice_count: usize,
    slice_index: usize,
    distance_tick: usize,
}

#[derive(Debug)]
struct OutboundAvatarSend {
    receiver: PeerId,
    channel: u8,
    payload: Bytes,
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
    tracking: Arc<DashMap<ReceiverSenderKey, ReceiverTracking>>,
    generation: Arc<AtomicU64>,
    slice_state: Arc<parking_lot::Mutex<SliceState>>,
    payload_pool: Arc<BytePool>,
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
            })),
            payload_pool: Arc::new(BytePool::default()),
        }
    }

    pub fn update_config(&self, config: AvatarSyncConfig) {
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
        self.tracking
            .retain(|key, _| key.receiver != peer_id && key.sender != peer_id);
    }

    pub fn spawn_tick_loop<F>(&self, transport: TransportHandle, peer_snapshot: F)
    where
        F: Fn() -> Vec<PeerId> + Send + Sync + 'static,
    {
        let system = self.clone();
        let runtime = Handle::current();
        let _ = thread::Builder::new()
            .name("BSR-TickLoop".to_string())
            .spawn(move || {
                set_avatar_thread_priority();
                let tick = Duration::from_millis(4);
                loop {
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
        let states: HashMap<PeerId, PlayerAvatarState> = self
            .states
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();
        let (slice_count, slice_index, update_distances) = self.advance_slice_state();
        let receiver_ids = peers
            .iter()
            .enumerate()
            .filter_map(|(index, peer)| (index % slice_count == slice_index).then_some(*peer))
            .collect::<Vec<_>>();

        let sends = receiver_ids
            .par_iter()
            .flat_map_iter(|receiver_id| {
                self.build_sends_for_receiver(
                    *receiver_id,
                    &peers,
                    &states,
                    &config,
                    now,
                    update_distances,
                )
            })
            .collect::<Vec<_>>();

        let mut by_receiver: BTreeMap<PeerId, Vec<OutboundAvatarSend>> = BTreeMap::new();
        for send in sends {
            by_receiver.entry(send.receiver).or_default().push(send);
        }
        for (receiver, sends) in by_receiver {
            if sends.len() == 1 {
                let send = &sends[0];
                transport
                    .send(
                        receiver,
                        send.channel,
                        DeliveryMethod::Unreliable,
                        send.payload.as_ref(),
                    )
                    .await?;
                continue;
            }
            let packets = sends
                .iter()
                .map(|send| {
                    (
                        send.channel,
                        DeliveryMethod::Unreliable,
                        send.payload.as_ref(),
                    )
                })
                .collect::<Vec<_>>();
            transport.send_many_slices(receiver, &packets).await?;
        }
        self.adapt_slice_count(tick_start.elapsed());
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
                current.has_received_first = true;
                current.position = update.position;
                current.generation = generation;
                if let Ok(qualities) = build_quality_packets(
                    &self.payload_pool,
                    update.peer_id,
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
        peers: &[PeerId],
        states: &HashMap<PeerId, PlayerAvatarState>,
        config: &AvatarSyncConfig,
        now: Instant,
        update_distances: bool,
    ) -> Vec<OutboundAvatarSend> {
        let Some(receiver_state) = states.get(&receiver_id) else {
            return Vec::new();
        };
        let mut direct = Vec::new();
        let mut bundle = Vec::new();
        let mut bundle_raw_bytes = 0usize;
        let mut sent_this_receiver = 0usize;

        for sender_id in peers {
            if *sender_id == receiver_id {
                continue;
            }
            if sent_this_receiver >= MAX_AVATAR_SENDERS_PER_RECEIVER_PER_TICK {
                break;
            }
            let Some(sender_state) = states.get(sender_id) else {
                continue;
            };
            let key = ReceiverSenderKey {
                receiver: receiver_id,
                sender: *sender_id,
            };
            let mut tracking = self
                .tracking
                .entry(key)
                .or_insert_with(|| ReceiverTracking {
                    last_seen_generation: 0,
                    last_sent: now - Duration::from_secs(60),
                    cached_quality_index: choose_quality(receiver_state, sender_state, config),
                });
            if update_distances {
                tracking.cached_quality_index =
                    choose_quality(receiver_state, sender_state, config);
            }
            let quality_index = tracking.cached_quality_index;
            let Some(packet) = sender_state.qualities[quality_index as usize].as_ref() else {
                continue;
            };
            if tracking.last_seen_generation == sender_state.generation {
                continue;
            }
            if now.duration_since(tracking.last_sent)
                < Duration::from_millis(config.default_interval_ms.max(1))
            {
                continue;
            }
            tracking.last_seen_generation = sender_state.generation;
            tracking.last_sent = now;
            sent_this_receiver += 1;

            let (channel, payload) = if sender_state.peer_id <= u8::MAX as u16 {
                (packet.channel_small, &packet.bytes_small)
            } else {
                (packet.channel_large, &packet.bytes_large)
            };

            if config.enable_bundle_compression {
                bundle_raw_bytes += 3 + payload.len();
                bundle.push(AvatarBundleItem {
                    original_channel: channel,
                    payload: payload.to_vec(),
                });
            } else {
                direct.push(OutboundAvatarSend {
                    receiver: receiver_id,
                    channel,
                    payload: payload.clone(),
                });
            }
        }

        if config.enable_bundle_compression
            && bundle.len() >= config.bundle_min_messages
            && bundle_raw_bytes >= config.bundle_min_bytes
        {
            if let Ok(encoded) = encode_avatar_bundle(&bundle) {
                direct.push(OutboundAvatarSend {
                    receiver: receiver_id,
                    channel: channels::COMPRESSED_AVATAR_BUNDLE,
                    payload: Bytes::from(encoded),
                });
            }
        } else if config.enable_bundle_compression {
            direct.extend(bundle.into_iter().map(|item| OutboundAvatarSend {
                receiver: receiver_id,
                channel: item.original_channel,
                payload: Bytes::from(item.payload),
            }));
        }
        direct
    }

    fn advance_slice_state(&self) -> (usize, usize, bool) {
        let mut state = self.slice_state.lock();
        let slice_count = state.slice_count.max(1);
        let slice_index = state.slice_index % slice_count;
        state.slice_index = (state.slice_index + 1) % slice_count;
        state.distance_tick += 1;
        let update_distances = state.distance_tick >= DISTANCE_UPDATE_INTERVAL_TICKS;
        if update_distances {
            state.distance_tick = 0;
        }
        (slice_count, slice_index, update_distances)
    }

    fn adapt_slice_count(&self, elapsed: Duration) {
        let mut state = self.slice_state.lock();
        let elapsed_ms = elapsed.as_secs_f64() * 1000.0;
        if elapsed_ms > 3.0 && state.slice_count < MAX_SLICE_COUNT {
            state.slice_count += 1;
            state.slice_index %= state.slice_count;
        } else if elapsed_ms < 1.0 && state.slice_count > 1 {
            state.slice_count -= 1;
            state.slice_index %= state.slice_count;
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
    inbound_quality: BitQuality,
    payload: &[u8],
) -> Result<[Option<PreSerializedQuality>; 4]> {
    let mut qualities: [Option<PreSerializedQuality>; 4] = [None, None, None, None];
    match inbound_quality {
        BitQuality::High => {
            qualities[BitQuality::High as usize] =
                Some(pre_serialize(peer_id, BitQuality::High, payload));
            let mut medium = pool.take(BitQuality::Medium.payload_len());
            let mut low = pool.take(BitQuality::Low.payload_len());
            let mut very_low = pool.take(BitQuality::VeryLow.payload_len());
            repack_high_to_lower_into(payload, BitQuality::Medium, &mut medium)?;
            repack_high_to_lower_into(payload, BitQuality::Low, &mut low)?;
            repack_high_to_lower_into(payload, BitQuality::VeryLow, &mut very_low)?;
            qualities[BitQuality::Medium as usize] =
                Some(pre_serialize(peer_id, BitQuality::Medium, &medium));
            qualities[BitQuality::Low as usize] =
                Some(pre_serialize(peer_id, BitQuality::Low, &low));
            qualities[BitQuality::VeryLow as usize] =
                Some(pre_serialize(peer_id, BitQuality::VeryLow, &very_low));
            pool.put(medium);
            pool.put(low);
            pool.put(very_low);
        }
        other => {
            qualities[other as usize] = Some(pre_serialize(peer_id, other, payload));
        }
    }
    Ok(qualities)
}

fn pre_serialize(peer_id: PeerId, quality: BitQuality, payload: &[u8]) -> PreSerializedQuality {
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
    bytes_small.push(0);
    bytes_small.extend_from_slice(payload);

    let mut bytes_large = Vec::with_capacity(4 + payload.len());
    bytes_large.extend_from_slice(&peer_id.to_le_bytes());
    bytes_large.push(0);
    bytes_large.push(0);
    bytes_large.extend_from_slice(payload);

    PreSerializedQuality {
        channel_small,
        channel_large,
        bytes_small: Bytes::from(bytes_small),
        bytes_large: Bytes::from(bytes_large),
    }
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

fn choose_quality(
    receiver: &PlayerAvatarState,
    sender: &PlayerAvatarState,
    config: &AvatarSyncConfig,
) -> u8 {
    let dx = receiver.position[0] - sender.position[0];
    let dy = receiver.position[1] - sender.position[1];
    let dz = receiver.position[2] - sender.position[2];
    let distance_sq = dx * dx + dy * dy + dz * dz;
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

#[cfg(test)]
mod tests {
    use super::*;
    use basis_protocol::avatar::decode_avatar_bundle;

    #[test]
    fn packet_preserialization_uses_small_and_large_ids() {
        let payload = vec![0u8; BitQuality::High.payload_len()];
        let packet = pre_serialize(300, BitQuality::High, &payload);
        assert_eq!(packet.channel_small, channels::PLAYER_AVATAR_HIGH);
        assert_eq!(packet.channel_large, channels::PLAYER_AVATAR_HIGH_LARGE);
        assert_eq!(&packet.bytes_large[0..2], &300u16.to_le_bytes());
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
