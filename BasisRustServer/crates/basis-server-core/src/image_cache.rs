//! Bounded server-side cache for opaque image scene payloads.
//!
//! This mirrors the Basis image pickup wire protocol. The cache only interprets the opcode,
//! image id, chunk index, chunk counts, and the variable-length owner name in spawn headers.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;

use basis_protocol::config::ServerConfig;
use bytes::Bytes;

pub const IMAGE_MANAGER_IDENTIFIER: &str = "BasisImagePickupManager";

#[derive(Clone, Copy)]
pub(crate) struct CachePeers<'a> {
    pub connected: &'a [u16],
    pub animation_allowed: &'a [u16],
}

const OP_SPAWN: u8 = 1;
const OP_CHUNK: u8 = 2;
const OP_TRANSFORM: u8 = 3;
const OP_DESPAWN: u8 = 4;
const OP_ANIMATION_SPAWN: u8 = 6;
const OP_ANIMATION_CHUNK: u8 = 7;
const OP_SERVER_CACHE_STATE: u8 = 8;
const OP_SERVER_CACHE_OFFER: u8 = 9;
const OP_SERVER_CACHE_REQUEST: u8 = 10;
const HEADER_BYTES: usize = 17;
const POSE_BYTES: usize = 28;
const TRANSFORM_BYTES: usize = HEADER_BYTES + POSE_BYTES + 4;
const MAX_OWNER_NAME_BYTES: usize = 1024;
const BYTES_PER_MEGABYTE: u64 = 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CacheSend {
    /// Recipient of the server scene payload.
    pub(crate) recipient: u16,
    /// Player id stamped onto the scene payload. Offers and cache-state notices use the recipient;
    /// image data replays use the original image owner.
    pub(crate) owner: u16,
    pub(crate) payload: Bytes,
    /// Image data replay may be queued through the download governor. Offers and status notices are
    /// small control messages and are sent inline.
    pub(crate) paced: bool,
}

#[derive(Debug, Default)]
pub(crate) struct CacheEffects {
    pub(crate) sends: Vec<CacheSend>,
}

#[derive(Debug)]
struct CachedImage {
    owner: u16,
    sequence: u64,
    offered: HashSet<u16>,
    delivered: HashSet<u16>,
    animation_delivered: HashSet<u16>,
    spawn: Bytes,
    chunks: Vec<Option<Bytes>>,
    chunks_held: usize,
    pose_offset: usize,
    transform: Option<Bytes>,
    animation_spawn: Option<Bytes>,
    animation_chunks: Vec<Option<Bytes>>,
    animation_chunks_held: usize,
    bytes: u64,
}

impl CachedImage {
    fn still_complete(&self) -> bool {
        self.chunks_held == self.chunks.len()
    }

    fn animation_complete(&self) -> bool {
        self.animation_spawn.is_some() && self.animation_chunks_held == self.animation_chunks.len()
    }
}

#[derive(Debug, Default)]
pub(crate) struct ImageCache {
    images: HashMap<[u8; 16], CachedImage>,
    total_bytes: u64,
    sequence: u64,
}

impl ImageCache {
    pub(crate) fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(crate) fn count(&self) -> usize {
        self.images.len()
    }

    pub(crate) fn servable_count(&self) -> usize {
        self.images
            .values()
            .filter(|image| image.still_complete())
            .count()
    }

    #[cfg(test)]
    fn bytes_held_for(&self, owner: u16) -> u64 {
        self.owner_bytes(owner)
    }

    pub(crate) fn reset(&mut self) {
        self.images.clear();
        self.total_bytes = 0;
        self.sequence = 0;
    }

    /// Observe one image payload without changing live relay behavior. `recipients` is the explicit
    /// target list for targeted scene messages; an empty list means the original was broadcast.
    /// `peers` supplies the authenticated roster and peers allowed to receive cached animations.
    pub(crate) fn observe(
        &mut self,
        owner: u16,
        payload: &[u8],
        recipients: &[u16],
        peers: CachePeers<'_>,
        allow_animation: bool,
        config: &ServerConfig,
    ) -> CacheEffects {
        let mut effects = CacheEffects::default();
        if !config.image_cache_enabled || payload.len() < HEADER_BYTES {
            return effects;
        }
        match payload[0] {
            OP_SPAWN => {
                self.observe_spawn(
                    owner,
                    payload,
                    recipients,
                    peers.connected,
                    config,
                    &mut effects,
                );
            }
            OP_CHUNK => {
                self.observe_chunk(owner, payload, false, peers, config, &mut effects);
            }
            OP_TRANSFORM => {
                self.observe_transform(payload, config, &mut effects);
            }
            OP_SERVER_CACHE_REQUEST => {
                effects
                    .sends
                    .extend(self.request(owner, payload, allow_animation, config));
            }
            OP_ANIMATION_SPAWN => {
                self.observe_animation_spawn(owner, payload, config, &mut effects);
            }
            OP_ANIMATION_CHUNK => {
                self.observe_chunk(owner, payload, true, peers, config, &mut effects);
            }
            OP_DESPAWN => {
                if let Some(id) = read_id(payload) {
                    self.remove_image(id, owner, true, &mut effects);
                }
            }
            _ => {}
        }
        effects
    }

    fn observe_spawn(
        &mut self,
        owner: u16,
        payload: &[u8],
        recipients: &[u16],
        connected: &[u16],
        config: &ServerConfig,
        effects: &mut CacheEffects,
    ) {
        let Some(id) = read_id(payload) else { return };
        let Some((chunk_count, pose_offset)) = read_spawn_header(payload) else {
            return;
        };
        if chunk_count <= 0
            || pose_offset + POSE_BYTES > payload.len()
            || self.images.contains_key(&id)
        {
            return;
        }
        let count = chunk_count as usize;
        let Some(backbone) = (count as u64).checked_mul(size_of::<Option<Bytes>>() as u64) else {
            return;
        };
        let Some(cost) = (payload.len() as u64).checked_add(backbone) else {
            return;
        };
        if !self.try_reserve(owner, cost, id, config, effects) {
            return;
        }
        self.sequence = self.sequence.wrapping_add(1);
        let mut image = CachedImage {
            owner,
            sequence: self.sequence,
            offered: HashSet::new(),
            delivered: HashSet::new(),
            animation_delivered: HashSet::new(),
            spawn: Bytes::copy_from_slice(payload),
            chunks: vec![None; count],
            chunks_held: 0,
            pose_offset,
            transform: None,
            animation_spawn: None,
            animation_chunks: Vec::new(),
            animation_chunks_held: 0,
            bytes: cost,
        };
        seed_already_held(&mut image, recipients, connected);
        self.total_bytes += cost;
        self.images.insert(id, image);
    }

    fn observe_animation_spawn(
        &mut self,
        owner: u16,
        payload: &[u8],
        config: &ServerConfig,
        effects: &mut CacheEffects,
    ) {
        // opcode + id + format + total bytes + chunk count + epoch
        const CHUNK_COUNT_OFFSET: usize = HEADER_BYTES + 1 + 4;
        if payload.len() < CHUNK_COUNT_OFFSET + 4 {
            return;
        }
        let Some(id) = read_id(payload) else { return };
        let Some(count) = read_i32(payload, CHUNK_COUNT_OFFSET) else {
            return;
        };
        if count <= 0 {
            return;
        }
        let Some(image) = self.images.get(&id) else {
            return;
        };
        if image.owner != owner || image.animation_spawn.is_some() {
            return;
        }
        let Some(backbone) = (count as u64).checked_mul(size_of::<Option<Bytes>>() as u64) else {
            return;
        };
        let Some(cost) = (payload.len() as u64).checked_add(backbone) else {
            return;
        };
        if !self.try_reserve(owner, cost, id, config, effects) {
            return;
        }
        let image = self
            .images
            .get_mut(&id)
            .expect("entry remains after reservation");
        image.animation_spawn = Some(Bytes::copy_from_slice(payload));
        image.animation_chunks = vec![None; count as usize];
        image.bytes += cost;
        self.total_bytes += cost;
    }

    fn observe_chunk(
        &mut self,
        owner: u16,
        payload: &[u8],
        animation: bool,
        peers: CachePeers<'_>,
        config: &ServerConfig,
        effects: &mut CacheEffects,
    ) {
        const CHUNK_INDEX_OFFSET: usize = HEADER_BYTES;
        if payload.len() < CHUNK_INDEX_OFFSET + 8 {
            return;
        }
        let Some(id) = read_id(payload) else { return };
        let Some(index) = read_i32(payload, CHUNK_INDEX_OFFSET) else {
            return;
        };
        if index < 0 {
            return;
        }
        let Some(image) = self.images.get(&id) else {
            return;
        };
        if image.owner != owner {
            return;
        }
        let slots = if animation {
            &image.animation_chunks
        } else {
            &image.chunks
        };
        let Some(slot) = slots.get(index as usize) else {
            return;
        };
        if slot.is_some() {
            return;
        }
        if animation && image.animation_spawn.is_none() {
            return;
        }
        let cost = payload.len() as u64;
        if !self.try_reserve(owner, cost, id, config, effects) {
            return;
        }
        let (still_complete, animation_complete) = {
            let image = self
                .images
                .get_mut(&id)
                .expect("entry remains after reservation");
            if animation {
                image.animation_chunks[index as usize] = Some(Bytes::copy_from_slice(payload));
                image.animation_chunks_held += 1;
            } else {
                image.chunks[index as usize] = Some(Bytes::copy_from_slice(payload));
                image.chunks_held += 1;
            }
            image.bytes += cost;
            (
                image.still_complete(),
                image.animation_complete() && image.still_complete(),
            )
        };
        self.total_bytes += cost;

        if !animation && still_complete {
            effects.sends.push(cache_state_send(owner, id, true));
            for peer in peers.connected {
                if let Some(offer) = self.mark_offered(id, *peer) {
                    effects.sends.push(offer);
                }
            }
        }
        if animation && animation_complete {
            let recipients: Vec<u16> = self
                .images
                .get(&id)
                .map(|image| {
                    image
                        .delivered
                        .iter()
                        .filter(|peer| {
                            **peer != image.owner
                                && peers.animation_allowed.contains(peer)
                                && !image.animation_delivered.contains(peer)
                        })
                        .copied()
                        .collect()
                })
                .unwrap_or_default();
            for recipient in recipients {
                if let Some(sends) = self.deliver_animation(id, recipient) {
                    effects.sends.extend(sends);
                }
            }
        }
    }

    fn observe_transform(
        &mut self,
        payload: &[u8],
        config: &ServerConfig,
        effects: &mut CacheEffects,
    ) {
        if payload.len() != TRANSFORM_BYTES {
            return;
        }
        let Some(id) = read_id(payload) else { return };
        let Some(image) = self.images.get(&id) else {
            return;
        };
        let first = image.transform.is_none();
        let owner = image.owner;
        if first && !self.try_reserve(owner, TRANSFORM_BYTES as u64, id, config, effects) {
            return;
        }
        let image = self
            .images
            .get_mut(&id)
            .expect("entry remains after reservation");
        if first {
            image.bytes += TRANSFORM_BYTES as u64;
            self.total_bytes += TRANSFORM_BYTES as u64;
        }
        image.transform = Some(Bytes::copy_from_slice(payload));
    }

    fn try_reserve(
        &mut self,
        owner: u16,
        cost: u64,
        exclude: [u8; 16],
        config: &ServerConfig,
        effects: &mut CacheEffects,
    ) -> bool {
        let cap = if config.image_cache_max_megabytes > 0 {
            (config.image_cache_max_megabytes as u64).saturating_mul(BYTES_PER_MEGABYTE)
        } else {
            0
        };
        if cap == 0 || cost == 0 || cost > cap {
            return false;
        }
        let share = self.owner_share(owner, cap, config);
        if cost > share {
            return false;
        }
        while self.owner_bytes(owner).saturating_add(cost) > share {
            let Some(id) = self.oldest_owned(owner, Some(exclude)) else {
                return false;
            };
            self.drop_image(id, effects);
        }
        while self.total_bytes.saturating_add(cost) > cap {
            let Some(id) = self.oldest_of_heaviest_owner(exclude) else {
                return false;
            };
            self.drop_image(id, effects);
        }
        true
    }

    fn owner_share(&self, owner: u16, cap: u64, config: &ServerConfig) -> u64 {
        let mut owners: HashSet<u16> = self.images.values().map(|image| image.owner).collect();
        owners.insert(owner);
        let fair = cap / owners.len().max(1) as u64;
        let floor = if config.image_cache_minimum_per_owner_megabytes > 0 {
            (config.image_cache_minimum_per_owner_megabytes as u64)
                .saturating_mul(BYTES_PER_MEGABYTE)
                .min(cap)
        } else {
            0
        };
        fair.max(floor)
    }

    fn owner_bytes(&self, owner: u16) -> u64 {
        self.images
            .values()
            .filter(|image| image.owner == owner)
            .map(|image| image.bytes)
            .sum()
    }

    fn oldest_owned(&self, owner: u16, exclude: Option<[u8; 16]>) -> Option<[u8; 16]> {
        self.images
            .iter()
            .filter(|(id, image)| image.owner == owner && Some(**id) != exclude)
            .min_by_key(|(_, image)| image.sequence)
            .map(|(id, _)| *id)
    }

    fn oldest_of_heaviest_owner(&self, exclude: [u8; 16]) -> Option<[u8; 16]> {
        let mut held: HashMap<u16, u64> = HashMap::new();
        for image in self.images.values() {
            *held.entry(image.owner).or_default() += image.bytes;
        }
        let heaviest = held.into_iter().max_by_key(|(_, bytes)| *bytes)?.0;
        self.oldest_owned(heaviest, Some(exclude))
    }

    fn drop_image(&mut self, id: [u8; 16], effects: &mut CacheEffects) -> bool {
        let Some(image) = self.images.remove(&id) else {
            return false;
        };
        self.total_bytes = self.total_bytes.saturating_sub(image.bytes);
        if image.still_complete() {
            effects.sends.push(cache_state_send(image.owner, id, false));
        }
        true
    }

    fn remove_image(
        &mut self,
        id: [u8; 16],
        requester: u16,
        owner_only: bool,
        effects: &mut CacheEffects,
    ) -> bool {
        if self
            .images
            .get(&id)
            .is_some_and(|image| !owner_only || image.owner == requester)
        {
            self.drop_image(id, effects)
        } else {
            false
        }
    }

    pub(crate) fn remove_player(&mut self, peer: u16) -> CacheEffects {
        let mut effects = CacheEffects::default();
        let owned: Vec<_> = self
            .images
            .iter()
            .filter(|(_, image)| image.owner == peer)
            .map(|(id, _)| *id)
            .collect();
        for id in owned {
            self.drop_image(id, &mut effects);
        }
        for image in self.images.values_mut() {
            image.offered.remove(&peer);
            image.delivered.remove(&peer);
            image.animation_delivered.remove(&peer);
        }
        effects
    }

    pub(crate) fn offer_peer(&mut self, peer: u16, config: &ServerConfig) -> Vec<CacheSend> {
        if !config.image_cache_enabled {
            return Vec::new();
        }
        let mut ids: Vec<_> = self
            .images
            .iter()
            .filter(|(_, image)| image.still_complete())
            .map(|(id, image)| (*id, image.sequence))
            .collect();
        ids.sort_by_key(|(_, sequence)| *sequence);
        ids.into_iter()
            .filter_map(|(id, _)| self.mark_offered(id, peer))
            .collect()
    }

    fn mark_offered(&mut self, id: [u8; 16], peer: u16) -> Option<CacheSend> {
        let image = self.images.get_mut(&id)?;
        if !image.still_complete()
            || image.owner == peer
            || image.offered.contains(&peer)
            || image.delivered.contains(&peer)
        {
            return None;
        }
        image.offered.insert(peer);
        let mut payload = build_spawn(image);
        *payload.first_mut()? = OP_SERVER_CACHE_OFFER;
        Some(CacheSend {
            recipient: peer,
            owner: peer,
            payload: Bytes::from(payload),
            paced: false,
        })
    }

    /// Serve an opcode-10 request. The C# cache trusts the offered-payload request handshake and
    /// only requires a complete still; it does not require server-side proof of the prior offer.
    pub(crate) fn request(
        &mut self,
        peer: u16,
        payload: &[u8],
        allow_animation: bool,
        config: &ServerConfig,
    ) -> Vec<CacheSend> {
        if !config.image_cache_enabled
            || payload.len() < HEADER_BYTES
            || payload[0] != OP_SERVER_CACHE_REQUEST
        {
            return Vec::new();
        }
        let Some(id) = read_id(payload) else {
            return Vec::new();
        };
        let Some(image) = self.images.get_mut(&id) else {
            return Vec::new();
        };
        if !image.still_complete() || image.owner == peer || !image.delivered.insert(peer) {
            return Vec::new();
        }
        image.offered.insert(peer);
        let owner = image.owner;
        let mut payloads = vec![Bytes::from(build_spawn(image))];
        if let Some(transform) = &image.transform {
            payloads.push(transform.clone());
        }
        payloads.extend(image.chunks.iter().filter_map(|chunk| chunk.clone()));
        if allow_animation && image.animation_complete() && image.animation_delivered.insert(peer) {
            append_animation(image, &mut payloads);
        }
        payloads
            .into_iter()
            .map(|payload| CacheSend {
                recipient: peer,
                owner,
                payload,
                paced: true,
            })
            .collect()
    }

    pub(crate) fn resume_animations(
        &mut self,
        allowed_peers: &[u16],
        config: &ServerConfig,
    ) -> Vec<CacheSend> {
        if !config.image_cache_enabled {
            return Vec::new();
        }
        let candidates: Vec<_> = self
            .images
            .iter()
            .flat_map(|(id, image)| {
                if image.still_complete() && image.animation_complete() {
                    image
                        .delivered
                        .iter()
                        .filter(|peer| {
                            **peer != image.owner
                                && allowed_peers.contains(peer)
                                && !image.animation_delivered.contains(peer)
                        })
                        .map(|peer| (*id, *peer))
                        .collect::<Vec<_>>()
                } else {
                    Vec::new()
                }
            })
            .collect();
        let mut sends = Vec::new();
        for (id, peer) in candidates {
            if let Some(mut batch) = self.deliver_animation(id, peer) {
                sends.append(&mut batch);
            }
        }
        sends
    }

    fn deliver_animation(&mut self, id: [u8; 16], peer: u16) -> Option<Vec<CacheSend>> {
        let image = self.images.get_mut(&id)?;
        if !image.still_complete()
            || !image.animation_complete()
            || peer == image.owner
            || !image.delivered.contains(&peer)
            || !image.animation_delivered.insert(peer)
        {
            return None;
        }
        let owner = image.owner;
        let mut payloads = Vec::new();
        append_animation(image, &mut payloads);
        Some(
            payloads
                .into_iter()
                .map(|payload| CacheSend {
                    recipient: peer,
                    owner,
                    payload,
                    paced: true,
                })
                .collect(),
        )
    }
}

fn seed_already_held(image: &mut CachedImage, recipients: &[u16], connected: &[u16]) {
    let peers = if recipients.is_empty() {
        connected
    } else {
        recipients
    };
    for peer in peers {
        image.offered.insert(*peer);
        image.delivered.insert(*peer);
        image.animation_delivered.insert(*peer);
    }
}

fn read_id(payload: &[u8]) -> Option<[u8; 16]> {
    let raw = payload.get(1..HEADER_BYTES)?;
    let mut id = [0; 16];
    id.copy_from_slice(raw);
    Some(id)
}

fn read_i32(payload: &[u8], offset: usize) -> Option<i32> {
    let end = offset.checked_add(4)?;
    Some(i32::from_le_bytes(
        payload.get(offset..end)?.try_into().ok()?,
    ))
}

fn read_spawn_header(payload: &[u8]) -> Option<(i32, usize)> {
    let mut offset = HEADER_BYTES.checked_add(2)?;
    let mut length: usize = 0;
    let mut shift = 0;
    loop {
        let piece = *payload.get(offset)?;
        offset += 1;
        if shift > 28 {
            return None;
        }
        length |= ((piece & 0x7f) as usize).checked_shl(shift)?;
        if piece & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    if length > MAX_OWNER_NAME_BYTES || offset.checked_add(length)? > payload.len() {
        return None;
    }
    offset += length;
    offset = offset.checked_add(12)?;
    let total_chunks = read_i32(payload, offset)?;
    Some((total_chunks, offset.checked_add(4)?))
}

fn build_spawn(image: &CachedImage) -> Vec<u8> {
    let mut spawn = image.spawn.to_vec();
    if let Some(transform) = &image.transform {
        let transform_pose = &transform[HEADER_BYTES..HEADER_BYTES + POSE_BYTES];
        let end = image.pose_offset.saturating_add(POSE_BYTES);
        if image.pose_offset > 0 && end <= spawn.len() {
            spawn[image.pose_offset..end].copy_from_slice(transform_pose);
        }
    }
    spawn
}

fn append_animation(image: &CachedImage, out: &mut Vec<Bytes>) {
    if let Some(spawn) = &image.animation_spawn {
        out.push(spawn.clone());
    }
    out.extend(
        image
            .animation_chunks
            .iter()
            .filter_map(|chunk| chunk.clone()),
    );
}

fn cache_state_send(owner: u16, id: [u8; 16], held: bool) -> CacheSend {
    let mut payload = Vec::with_capacity(HEADER_BYTES + 1);
    payload.push(OP_SERVER_CACHE_STATE);
    payload.extend_from_slice(&id);
    payload.push(u8::from(held));
    CacheSend {
        recipient: owner,
        owner,
        payload: Bytes::from(payload),
        paced: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ServerConfig {
        ServerConfig::default()
    }

    fn spawn(id: u8, owner: u16, chunks: i32, x: f32) -> Vec<u8> {
        let mut payload = vec![OP_SPAWN];
        payload.extend([id; 16]);
        payload.extend_from_slice(&owner.to_le_bytes());
        payload.push(5); // BinaryWriter's seven-bit string length for "owner".
        payload.extend_from_slice(b"owner");
        for value in [64_i32, 64, chunks.saturating_mul(16), chunks] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        payload.extend_from_slice(&x.to_le_bytes());
        for value in [0.0_f32, 0.0, 0.0, 0.0, 0.0, 0.0] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        payload
    }

    fn chunk(id: u8, index: i32, size: usize, opcode: u8) -> Vec<u8> {
        let mut payload = vec![opcode];
        payload.extend([id; 16]);
        payload.extend_from_slice(&index.to_le_bytes());
        payload.extend_from_slice(&(size as i32).to_le_bytes());
        payload.resize(payload.len() + size, 0xab);
        payload
    }

    fn transform(id: u8, x: f32) -> Vec<u8> {
        let mut payload = vec![OP_TRANSFORM];
        payload.extend([id; 16]);
        payload.extend_from_slice(&x.to_le_bytes());
        for value in [0.0_f32, 0.0, 0.0, 0.0, 0.0, 1.0, 1.0] {
            payload.extend_from_slice(&value.to_le_bytes());
        }
        payload
    }

    fn observe(
        cache: &mut ImageCache,
        owner: u16,
        payload: &[u8],
        connected: &[u16],
        config: &ServerConfig,
    ) -> CacheEffects {
        cache.observe(
            owner,
            payload,
            &[],
            CachePeers {
                connected,
                animation_allowed: connected,
            },
            true,
            config,
        )
    }

    #[test]
    fn complete_image_offers_pose_header_and_request_replays_owner_stamped_order() {
        let config = config();
        let mut cache = ImageCache::default();
        cache.observe(
            7,
            &spawn(1, 7, 2, 12.5),
            &[7],
            CachePeers {
                connected: &[7, 9],
                animation_allowed: &[7, 9],
            },
            true,
            &config,
        );
        observe(&mut cache, 7, &chunk(1, 0, 4, OP_CHUNK), &[7, 9], &config);
        let completed = observe(&mut cache, 7, &chunk(1, 1, 4, OP_CHUNK), &[7, 9], &config);
        assert_eq!(cache.servable_count(), 1);
        assert_eq!(completed.sends.len(), 2); // held notice + offer to peer 9
        assert_eq!(completed.sends[0].payload[0], OP_SERVER_CACHE_STATE);
        assert_eq!(completed.sends[1].payload[0], OP_SERVER_CACHE_OFFER);
        assert_eq!(completed.sends[1].owner, 9);
        assert_eq!(
            f32::from_le_bytes(completed.sends[1].payload[41..45].try_into().unwrap()),
            12.5
        );

        let mut request = vec![OP_SERVER_CACHE_REQUEST];
        request.extend([1; 16]);
        let replay = cache.request(9, &request, true, &config);
        assert_eq!(replay.len(), 3);
        assert!(replay
            .iter()
            .all(|send| send.owner == 7 && send.recipient == 9 && send.paced));
        assert_eq!(replay[0].payload[0], OP_SPAWN);
        assert_eq!(replay[1].payload[0], OP_CHUNK);
        assert_eq!(replay[2].payload[0], OP_CHUNK);
        assert!(cache.request(9, &request, true, &config).is_empty());
    }

    #[test]
    fn targeted_share_does_not_reoffer_existing_recipient_and_despawn_requires_owner() {
        let config = config();
        let mut cache = ImageCache::default();
        cache.observe(
            7,
            &spawn(2, 7, 1, 0.0),
            &[9],
            CachePeers {
                connected: &[7, 9, 11],
                animation_allowed: &[7, 9, 11],
            },
            true,
            &config,
        );
        let done = observe(
            &mut cache,
            7,
            &chunk(2, 0, 2, OP_CHUNK),
            &[7, 9, 11],
            &config,
        );
        assert_eq!(
            done.sends
                .iter()
                .filter(|send| send.payload[0] == OP_SERVER_CACHE_OFFER)
                .count(),
            1
        );
        assert_eq!(
            done.sends
                .iter()
                .find(|send| send.payload[0] == OP_SERVER_CACHE_OFFER)
                .unwrap()
                .recipient,
            11
        );
        let wrong_owner = observe(
            &mut cache,
            11,
            &[OP_DESPAWN].into_iter().chain([2; 16]).collect::<Vec<_>>(),
            &[],
            &config,
        );
        assert_eq!(cache.count(), 1);
        assert!(wrong_owner.sends.is_empty());
        observe(
            &mut cache,
            7,
            &[OP_DESPAWN].into_iter().chain([2; 16]).collect::<Vec<_>>(),
            &[],
            &config,
        );
        assert_eq!(cache.count(), 0);
    }

    #[test]
    fn incomplete_and_unbounded_chunk_headers_are_not_served_or_allocated() {
        let mut config = config();
        config.image_cache_max_megabytes = 1;
        let mut cache = ImageCache::default();
        observe(&mut cache, 7, &spawn(3, 7, 100_000_000, 0.0), &[], &config);
        assert_eq!(cache.count(), 0);

        observe(&mut cache, 7, &spawn(4, 7, 2, 0.0), &[], &config);
        let mut request = vec![OP_SERVER_CACHE_REQUEST];
        request.extend([4; 16]);
        assert!(cache.request(9, &request, true, &config).is_empty());
    }

    #[test]
    fn animation_waits_for_complete_still_and_is_sent_once_after_unlock() {
        let config = config();
        let mut cache = ImageCache::default();
        observe(&mut cache, 7, &spawn(5, 7, 1, 0.0), &[], &config);
        let mut anim = vec![OP_ANIMATION_SPAWN];
        anim.extend([5; 16]);
        anim.push(2);
        anim.extend_from_slice(&4_i32.to_le_bytes());
        anim.extend_from_slice(&1_i32.to_le_bytes());
        anim.extend_from_slice(&0_i64.to_le_bytes());
        observe(&mut cache, 7, &anim, &[], &config);
        observe(
            &mut cache,
            7,
            &chunk(5, 0, 4, OP_ANIMATION_CHUNK),
            &[],
            &config,
        );
        observe(&mut cache, 7, &chunk(5, 0, 4, OP_CHUNK), &[], &config);
        let mut request = vec![OP_SERVER_CACHE_REQUEST];
        request.extend([5; 16]);
        assert_eq!(cache.request(9, &request, false, &config).len(), 2);
        let resumed = cache.resume_animations(&[9], &config);
        assert_eq!(resumed.len(), 2);
        assert_eq!(resumed[0].payload[0], OP_ANIMATION_SPAWN);
        assert_eq!(resumed[1].payload[0], OP_ANIMATION_CHUNK);
        assert!(cache.resume_animations(&[9], &config).is_empty());
    }

    #[test]
    fn owner_disconnect_releases_its_bytes_and_peer_delivery_marks() {
        let config = config();
        let mut cache = ImageCache::default();
        observe(&mut cache, 7, &spawn(6, 7, 1, 0.0), &[], &config);
        observe(&mut cache, 7, &chunk(6, 0, 8, OP_CHUNK), &[], &config);
        assert!(cache.total_bytes() > 0);
        let effects = cache.remove_player(7);
        assert_eq!(cache.total_bytes(), 0);
        assert_eq!(cache.count(), 0);
        assert!(effects
            .sends
            .iter()
            .any(|send| send.payload[0] == OP_SERVER_CACHE_STATE));
    }

    #[test]
    fn transform_replaces_offer_pose_and_precedes_chunks_on_replay() {
        let config = config();
        let mut cache = ImageCache::default();
        observe(&mut cache, 7, &spawn(8, 7, 1, 1.0), &[7], &config);
        observe(&mut cache, 7, &transform(8, 42.25), &[7], &config);
        let completed = observe(&mut cache, 7, &chunk(8, 0, 3, OP_CHUNK), &[7, 9], &config);
        let offer = completed
            .sends
            .iter()
            .find(|send| send.payload[0] == OP_SERVER_CACHE_OFFER)
            .unwrap();
        assert_eq!(
            f32::from_le_bytes(offer.payload[41..45].try_into().unwrap()),
            42.25
        );

        let mut request = vec![OP_SERVER_CACHE_REQUEST];
        request.extend([8; 16]);
        let replay = cache.request(9, &request, false, &config);
        assert_eq!(replay.len(), 3);
        assert_eq!(replay[0].payload[0], OP_SPAWN);
        assert_eq!(
            f32::from_le_bytes(replay[0].payload[41..45].try_into().unwrap()),
            42.25
        );
        assert_eq!(replay[1].payload[0], OP_TRANSFORM);
        assert_eq!(
            f32::from_le_bytes(replay[1].payload[17..21].try_into().unwrap()),
            42.25
        );
        assert_eq!(replay[2].payload[0], OP_CHUNK);
    }

    #[test]
    fn per_owner_budget_evicts_that_owners_oldest_image_without_evicting_neighbor() {
        let mut config = config();
        config.image_cache_max_megabytes = 1;
        config.image_cache_minimum_per_owner_megabytes = 0;
        let mut cache = ImageCache::default();

        observe(&mut cache, 7, &spawn(10, 7, 1, 0.0), &[7, 9], &config);
        observe(
            &mut cache,
            7,
            &chunk(10, 0, 400_000, OP_CHUNK),
            &[7, 9],
            &config,
        );
        observe(&mut cache, 9, &spawn(11, 9, 1, 0.0), &[7, 9], &config);
        observe(
            &mut cache,
            9,
            &chunk(11, 0, 400_000, OP_CHUNK),
            &[7, 9],
            &config,
        );
        observe(&mut cache, 7, &spawn(12, 7, 1, 0.0), &[7, 9], &config);
        let replacement = observe(
            &mut cache,
            7,
            &chunk(12, 0, 200_000, OP_CHUNK),
            &[7, 9],
            &config,
        );

        assert_eq!(cache.count(), 2);
        assert_eq!(cache.servable_count(), 2);
        assert!(cache.bytes_held_for(7) < 300_000);
        assert!(cache.bytes_held_for(9) > 400_000);
        assert!(replacement.sends.iter().any(|send| {
            send.payload[0] == OP_SERVER_CACHE_STATE && send.payload.last() == Some(&0)
        }));
    }

    #[test]
    fn every_truncated_spawn_prefix_and_malformed_owner_length_is_safe() {
        let config = config();
        let mut cache = ImageCache::default();
        let valid = spawn(14, 7, 1, 0.0);
        for end in 0..valid.len() {
            let _ = observe(&mut cache, 7, &valid[..end], &[], &config);
        }
        assert_eq!(cache.count(), 0);

        let mut malformed = valid;
        malformed[19] = 0xff;
        assert!(observe(&mut cache, 7, &malformed, &[], &config)
            .sends
            .is_empty());
        assert_eq!(cache.count(), 0);
    }
}
