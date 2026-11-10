//! Disk-backed BasisMediaClipShare cache, matching BasisVR 2e6b44de394d.
//! Chunk payloads stay on disk; bounded delivery cursors are pumped fairly under backpressure.
use basis_protocol::config::ServerConfig;
use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    time::Instant,
};
use tracing::warn;

pub(crate) const CLIP_MANAGER_IDENTIFIER: &str = "BasisMediaClipShare";
const OP_OFFER: u8 = 1;
const OP_CHUNK: u8 = 2;
const OP_REMOVE: u8 = 3;
const OP_POSE: u8 = 4;
const OP_NEED: u8 = 5;
const OP_CACHE_STATE: u8 = 6;
const OP_CACHE_OFFER: u8 = 7;
const OP_CACHE_REQUEST: u8 = 8;
const OP_CACHE_CATALOG: u8 = 9;
const ID_BYTES: usize = 16;
const CHUNK_BYTES: usize = 16 * 1024;
const HEADER_BYTES: usize = 21;
const OFFER_FIXED_BYTES: usize = 57;
const MAX_CLIP_BYTES: usize = 1024 * 1024 * 1024;
const MAX_NEED_INDICES: usize = 512;
const STATE_NOT_HELD: u8 = 0;
const STATE_UPLOAD: u8 = 1;
const STATE_HELD: u8 = 2;
const MAX_QUEUED_PACKETS: usize = 256;
type ClipId = [u8; ID_BYTES];
#[derive(Debug, Clone)]
pub(crate) struct CacheSend {
    pub(crate) recipient: u16,
    pub(crate) owner: u16,
    pub(crate) payload: Bytes,
    chunk: Option<(ClipId, usize, u64)>,
}
#[derive(Debug)]
struct DiskStore {
    file: Option<File>,
    path: PathBuf,
}
impl Drop for DiskStore {
    fn drop(&mut self) {
        self.file.take();
        let _ = std::fs::remove_file(&self.path);
    }
}
#[derive(Debug)]
struct Entry {
    owner: u16,
    sequence: u64,
    total_bytes: usize,
    held: Vec<bool>,
    held_count: usize,
    offer: Bytes,
    protected: bool,
    store: DiskStore,
}
impl Entry {
    fn complete(&self) -> bool {
        self.held_count == self.held.len()
    }
    fn chunk_length(&self, index: usize) -> usize {
        (self.total_bytes - index * CHUNK_BYTES).min(CHUNK_BYTES)
    }
}
#[derive(Debug)]
struct Delivery {
    recipient: u16,
    id: ClipId,
    next: usize,
    explicit: VecDeque<usize>,
}
#[derive(Debug, Default)]
pub(crate) struct VideoClipCache {
    entries: HashMap<ClipId, Entry>,
    ready: HashSet<u16>,
    bytes: usize,
    sequence: u64,
    directory: Option<PathBuf>,
    deliveries: VecDeque<Delivery>,
    tokens: f64,
    last_pump: Option<Instant>,
}
impl Drop for VideoClipCache {
    fn drop(&mut self) {
        self.entries.clear();
        if let Some(dir) = self.directory.take() {
            let _ = std::fs::remove_dir(dir);
        }
    }
}
impl VideoClipCache {
    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }
    pub(crate) fn remove_player(&mut self, peer: u16) {
        self.ready.remove(&peer);
        let ids: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, e)| (e.owner == peer).then_some(*id))
            .collect();
        for id in ids {
            self.remove(&id);
        }
        self.deliveries.retain(|d| d.recipient != peer);
    }
    fn remove(&mut self, id: &ClipId) -> Option<u16> {
        let e = self.entries.remove(id)?;
        self.bytes = self.bytes.saturating_sub(e.total_bytes);
        self.deliveries.retain(|d| d.id != *id);
        Some(e.owner)
    }
    fn enabled(config: &ServerConfig) -> bool {
        config.video_clip_cache_enabled && config.video_clip_cache_max_megabytes > 0
    }
    fn directory(&mut self, config: &ServerConfig) -> std::io::Result<PathBuf> {
        if let Some(p) = &self.directory {
            return Ok(p.clone());
        }
        let root = if config.video_clip_cache_directory.trim().is_empty() {
            std::env::temp_dir().join("BasisClipCache")
        } else {
            PathBuf::from(&config.video_clip_cache_directory)
        };
        let path = root.join(format!("{}-{}", std::process::id(), uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path)?;
        self.directory = Some(path.clone());
        Ok(path)
    }
    pub(crate) fn observe(
        &mut self,
        owner: u16,
        payload: &[u8],
        recipients: &[u16],
        connected: &[u16],
        config: &ServerConfig,
        may_share: impl Fn(u16) -> bool,
    ) -> Vec<CacheSend> {
        let mut sends = Vec::new();
        if payload.len() < 17 {
            return sends;
        }
        let id: ClipId = payload[1..17].try_into().expect("fixed id");
        let to_server = !recipients.is_empty() && recipients.iter().all(|p| *p == owner);
        match payload[0] {
            OP_OFFER => self.offer(owner, id, payload, config, &mut sends),
            OP_CHUNK => {
                let completed = self.chunk(owner, id, payload, &mut sends);
                if completed && may_share(owner) {
                    if let Some(e) = self.entries.get(&id) {
                        for peer in self
                            .ready
                            .iter()
                            .copied()
                            .filter(|p| *p != owner && connected.contains(p))
                        {
                            sends.push(cache_offer(peer, e));
                        }
                    }
                }
            }
            OP_POSE => {
                if payload.len() == 49 {
                    if let Some(e) = self.entries.get_mut(&id) {
                        let mut offer = e.offer.to_vec();
                        offer[23..55].copy_from_slice(&payload[17..49]);
                        e.offer = Bytes::from(offer);
                    }
                }
            }
            OP_REMOVE => {
                if self.entries.get(&id).is_some_and(|e| {
                    e.owner == owner || (!e.protected && !config.content_removal_locked)
                }) {
                    self.remove(&id);
                }
            }
            OP_CACHE_CATALOG if to_server => {
                self.ready.insert(owner);
                if Self::enabled(config) {
                    let mut entries: Vec<_> = self
                        .entries
                        .values()
                        .filter(|e| e.owner != owner && e.complete() && may_share(e.owner))
                        .collect();
                    entries.sort_by_key(|e| e.sequence);
                    sends.extend(entries.into_iter().map(|e| cache_offer(owner, e)));
                }
            }
            OP_CACHE_REQUEST if to_server && payload.len() == 17 && Self::enabled(config) => {
                if let Some(e) = self
                    .entries
                    .get(&id)
                    .filter(|e| e.owner != owner && e.complete() && may_share(e.owner))
                {
                    let count = e.held.len();
                    let d = self.delivery(owner, id, count);
                    if d.next >= count {
                        d.next = 0;
                    }
                }
            }
            OP_NEED if to_server && Self::enabled(config) => {
                const FIXED: usize = 19;
                if payload.len() >= FIXED {
                    let count = u16::from_le_bytes(payload[17..19].try_into().unwrap()) as usize;
                    if count <= MAX_NEED_INDICES && payload.len() == FIXED + count * 4 {
                        if let Some(e) = self
                            .entries
                            .get(&id)
                            .filter(|e| e.owner != owner && may_share(e.owner))
                        {
                            let total = e.held.len();
                            let indices: Vec<_> = payload[FIXED..]
                                .as_chunks::<4>()
                                .0
                                .iter()
                                .map(|v| i32::from_le_bytes(*v))
                                .filter(|i| *i >= 0 && (*i as usize) < total && e.held[*i as usize])
                                .map(|i| i as usize)
                                .collect();
                            let d = self.delivery(owner, id, total);
                            for i in indices {
                                if d.explicit.len() >= total {
                                    break;
                                }
                                d.explicit.push_back(i);
                            }
                            if d.next >= total && d.explicit.is_empty() {
                                self.deliveries
                                    .retain(|d| !(d.recipient == owner && d.id == id));
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        sends
    }
    fn delivery(&mut self, recipient: u16, id: ClipId, total: usize) -> &mut Delivery {
        if let Some(index) = self
            .deliveries
            .iter()
            .position(|d| d.recipient == recipient && d.id == id)
        {
            return &mut self.deliveries[index];
        }
        self.deliveries.push_back(Delivery {
            recipient,
            id,
            next: total,
            explicit: VecDeque::new(),
        });
        self.deliveries.back_mut().unwrap()
    }
    fn offer(
        &mut self,
        owner: u16,
        id: ClipId,
        payload: &[u8],
        config: &ServerConfig,
        sends: &mut Vec<CacheSend>,
    ) {
        if !Self::enabled(config)
            || payload.len() < OFFER_FIXED_BYTES
            || self.entries.contains_key(&id)
            || id == [0; 16]
            || payload[22] & !3 != 0
        {
            return;
        }
        let total = i32::from_le_bytes(payload[17..21].try_into().unwrap());
        let name_len = u16::from_le_bytes(payload[55..57].try_into().unwrap()) as usize;
        if total <= 0
            || total as usize
                > MAX_CLIP_BYTES
                    .min(config.video_clip_max_megabytes.clamp(16, 1024) as usize * 1024 * 1024)
            || payload.len() != OFFER_FIXED_BYTES + name_len
            || name_len > 160
        {
            return;
        }
        if self.entries.values().filter(|e| e.owner == owner).count()
            >= config.video_clip_max_per_player.clamp(1, 16) as usize
        {
            return;
        }
        let cap =
            (config.video_clip_cache_max_megabytes.max(0) as usize).saturating_mul(1024 * 1024);
        if total as usize > cap {
            return;
        }
        while self.bytes.saturating_add(total as usize) > cap {
            let mut by_owner = HashMap::<u16, usize>::new();
            for e in self.entries.values() {
                *by_owner.entry(e.owner).or_default() += e.total_bytes;
            }
            let Some((&heaviest, _)) = by_owner
                .iter()
                .max_by_key(|(owner, bytes)| (**bytes, std::cmp::Reverse(**owner)))
            else {
                return;
            };
            let Some(victim) = self
                .entries
                .iter()
                .filter(|(_, e)| e.owner == heaviest)
                .min_by_key(|(_, e)| e.sequence)
                .map(|(id, _)| *id)
            else {
                return;
            };
            if let Some(owner) = self.remove(&victim) {
                sends.push(state_send(owner, victim, STATE_NOT_HELD));
            }
        }
        let store = (|| {
            let dir = self.directory(config)?;
            let path = dir.join(format!("{}.clip", uuid::Uuid::from_bytes(id)));
            let file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)?;
            Ok::<_, std::io::Error>(DiskStore {
                file: Some(file),
                path,
            })
        })();
        let store = match store {
            Ok(s) => s,
            Err(err) => {
                warn!("video clip cache file creation failed: {err}");
                return;
            }
        };
        self.sequence = self.sequence.wrapping_add(1);
        self.bytes += total as usize;
        self.entries.insert(
            id,
            Entry {
                owner,
                sequence: self.sequence,
                total_bytes: total as usize,
                held: vec![false; (total as usize).div_ceil(CHUNK_BYTES)],
                held_count: 0,
                offer: Bytes::copy_from_slice(payload),
                protected: payload[22] & 1 != 0,
                store,
            },
        );
        sends.push(state_send(owner, id, STATE_UPLOAD));
    }
    fn chunk(
        &mut self,
        owner: u16,
        id: ClipId,
        payload: &[u8],
        sends: &mut Vec<CacheSend>,
    ) -> bool {
        if payload.len() <= HEADER_BYTES {
            return false;
        }
        let index = i32::from_le_bytes(payload[17..21].try_into().unwrap());
        let Some(e) = self.entries.get_mut(&id).filter(|e| e.owner == owner) else {
            return false;
        };
        if index < 0
            || index as usize >= e.held.len()
            || e.held[index as usize]
            || payload.len() - HEADER_BYTES != e.chunk_length(index as usize)
        {
            return false;
        }
        let file = e.store.file.as_mut().expect("open cache file");
        let result = file
            .seek(SeekFrom::Start(index as u64 * CHUNK_BYTES as u64))
            .and_then(|_| file.write_all(&payload[HEADER_BYTES..]));
        if let Err(err) = result {
            warn!("video clip cache write failed: {err}");
            self.remove(&id);
            sends.push(state_send(owner, id, STATE_NOT_HELD));
            return false;
        }
        e.held[index as usize] = true;
        e.held_count += 1;
        if e.held_count == e.held.len() {
            if let Err(err) = file.flush() {
                warn!("video clip cache flush failed: {err}");
                self.remove(&id);
                sends.push(state_send(owner, id, STATE_NOT_HELD));
                return false;
            }
            sends.push(state_send(owner, id, STATE_HELD));
            return true;
        }
        false
    }
    pub(crate) fn valid(&self, send: &CacheSend) -> bool {
        send.chunk.is_none_or(|(id, index, sequence)| {
            self.entries.get(&id).is_some_and(|e| {
                e.sequence == sequence && e.owner == send.owner && e.held.get(index) == Some(&true)
            })
        })
    }

    pub(crate) fn retry(&mut self, send: &CacheSend) {
        if let Some((id, index, sequence)) = send.chunk {
            if let Some(e) = self.entries.get(&id).filter(|e| e.sequence == sequence) {
                let count = e.held.len();
                let d = self.delivery(send.recipient, id, count);
                if d.explicit.len() < count {
                    d.explicit.push_front(index);
                }
            }
        }
    }
    /// Select a bounded round-robin batch, leaving blocked peers' cursors untouched.
    pub(crate) fn pump(
        &mut self,
        now: Instant,
        config: &ServerConfig,
        connected: &[u16],
        may_share: impl Fn(u16) -> bool,
        queued: impl Fn(u16) -> usize,
    ) -> Vec<CacheSend> {
        if !Self::enabled(config) {
            self.deliveries.clear();
            return Vec::new();
        }
        let rate = config.video_clip_cache_serve_megabytes_per_second.max(0) as f64 * 1024. * 1024.;
        let elapsed = self
            .last_pump
            .map_or(0., |last| now.duration_since(last).as_secs_f64());
        self.last_pump = Some(now);
        self.tokens = if rate <= 0. {
            f64::MAX
        } else {
            (self.tokens + rate * elapsed).min(rate * 0.25)
        };
        let mut sends = Vec::new();
        let mut idle = 0;
        let mut admitted = HashMap::<u16, usize>::new();
        while !self.deliveries.is_empty()
            && idle < self.deliveries.len()
            && self.tokens > 0.
            && sends.len() < 256
        {
            let mut d = self.deliveries.pop_front().unwrap();
            let Some(e) = self.entries.get_mut(&d.id) else {
                continue;
            };
            if !connected.contains(&d.recipient)
                || !may_share(e.owner)
                || (d.next >= e.held.len() && d.explicit.is_empty())
            {
                continue;
            }
            // Account for the fragments selected in this same batch as well as the transport queue.
            if queued(d.recipient).saturating_add(*admitted.get(&d.recipient).unwrap_or(&0))
                >= MAX_QUEUED_PACKETS
            {
                idle += 1;
                self.deliveries.push_back(d);
                continue;
            }
            idle = 0;
            let index = if let Some(i) = d.explicit.pop_front() {
                i
            } else {
                let i = d.next;
                d.next += 1;
                i
            };
            if index >= e.held.len() || !e.held[index] {
                self.deliveries.push_back(d);
                continue;
            }
            let mut p = vec![0; HEADER_BYTES + e.chunk_length(index)];
            p[0] = OP_CHUNK;
            p[1..17].copy_from_slice(&d.id);
            p[17..21].copy_from_slice(&(index as i32).to_le_bytes());
            let file = e.store.file.as_mut().expect("open cache file");
            let result = file
                .seek(SeekFrom::Start(index as u64 * CHUNK_BYTES as u64))
                .and_then(|_| file.read_exact(&mut p[HEADER_BYTES..]));
            if let Err(err) = result {
                warn!("video clip cache read failed: {err}");
                let owner = e.owner;
                self.remove(&d.id);
                sends.push(state_send(owner, d.id, STATE_NOT_HELD));
                continue;
            }
            self.tokens -= p.len() as f64;
            *admitted.entry(d.recipient).or_default() += p.len().div_ceil(512);
            sends.push(CacheSend {
                recipient: d.recipient,
                owner: e.owner,
                payload: Bytes::from(p),
                chunk: Some((d.id, index, e.sequence)),
            });
            if d.next < e.held.len() || !d.explicit.is_empty() {
                self.deliveries.push_back(d);
            }
        }
        sends
    }
}
fn cache_offer(recipient: u16, e: &Entry) -> CacheSend {
    let mut p = e.offer.to_vec();
    p[0] = OP_CACHE_OFFER;
    CacheSend {
        recipient,
        owner: e.owner,
        payload: Bytes::from(p),
        chunk: None,
    }
}
fn state_send(recipient: u16, id: ClipId, state: u8) -> CacheSend {
    let mut p = Vec::with_capacity(18);
    p.push(OP_CACHE_STATE);
    p.extend_from_slice(&id);
    p.push(state);
    CacheSend {
        recipient,
        owner: recipient,
        payload: Bytes::from(p),
        chunk: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn offer(id: ClipId, size: usize, flags: u8) -> Vec<u8> {
        let mut p = vec![OP_OFFER];
        p.extend(id);
        p.extend((size as i32).to_le_bytes());
        p.extend([0, flags]);
        p.extend([0; 32]);
        p.extend(0u16.to_le_bytes());
        p
    }
    fn chunk(id: ClipId, index: i32, data: &[u8]) -> Vec<u8> {
        let mut p = vec![OP_CHUNK];
        p.extend(id);
        p.extend(index.to_le_bytes());
        p.extend(data);
        p
    }
    fn request(op: u8, id: ClipId) -> Vec<u8> {
        let mut p = vec![op];
        p.extend(id);
        p
    }
    fn config() -> ServerConfig {
        ServerConfig {
            video_clip_cache_enabled: true,
            video_clip_cache_max_megabytes: 4,
            video_clip_cache_serve_megabytes_per_second: 1,
            ..ServerConfig::default()
        }
    }
    fn complete(
        cache: &mut VideoClipCache,
        id: ClipId,
        owner: u16,
        size: usize,
        cfg: &ServerConfig,
    ) {
        let a = cache.observe(owner, &offer(id, size, 0), &[], &[1, 2, 3], cfg, |_| true);
        assert_eq!(a.last().unwrap().payload[17], STATE_UPLOAD);
        for index in 0..size.div_ceil(CHUNK_BYTES) {
            let length = (size - index * CHUNK_BYTES).min(CHUNK_BYTES);
            cache.observe(
                owner,
                &chunk(id, index as i32, &vec![index as u8; length]),
                &[],
                &[1, 2, 3],
                cfg,
                |_| true,
            );
        }
    }
    #[test]
    fn disk_cache_catalog_and_paced_request_roundtrip() {
        let cfg = config();
        let mut cache = VideoClipCache::default();
        let id = [3; 16];
        complete(&mut cache, id, 1, CHUNK_BYTES + 3, &cfg);
        assert_eq!(
            cache.entries[&id].store.path.metadata().unwrap().len(),
            (CHUNK_BYTES + 3) as u64
        );
        let catalog = request(OP_CACHE_CATALOG, [0; 16]);
        assert!(cache
            .observe(2, &catalog, &[], &[1, 2], &cfg, |_| true)
            .is_empty());
        let offers = cache.observe(2, &catalog, &[2], &[1, 2], &cfg, |_| true);
        assert_eq!(offers.len(), 1);
        assert_eq!(offers[0].payload[0], OP_CACHE_OFFER);
        assert!(cache
            .observe(
                2,
                &request(OP_CACHE_REQUEST, id),
                &[2],
                &[1, 2],
                &cfg,
                |_| true
            )
            .is_empty());
        let now = Instant::now();
        assert!(cache.pump(now, &cfg, &[1, 2], |_| true, |_| 0).is_empty());
        let chunks = cache.pump(
            now + std::time::Duration::from_millis(50),
            &cfg,
            &[1, 2],
            |_| true,
            |_| 0,
        );
        assert_eq!(chunks.len(), 2);
        assert_eq!(&chunks[1].payload[HEADER_BYTES..], &[1; 3]);
        let dir = cache.directory.clone().unwrap();
        cache.reset();
        assert!(!dir.exists());
    }
    #[test]
    fn pump_preserves_backlogged_cursor_and_retries_without_losing_chunks() {
        let mut cfg = config();
        cfg.video_clip_cache_serve_megabytes_per_second = 0;
        let mut cache = VideoClipCache::default();
        let id = [4; 16];
        complete(&mut cache, id, 1, 3, &cfg);
        cache.observe(
            2,
            &request(OP_CACHE_REQUEST, id),
            &[2],
            &[1, 2],
            &cfg,
            |_| true,
        );
        let now = Instant::now();
        assert!(cache.pump(now, &cfg, &[1, 2], |_| true, |_| 256).is_empty());
        let sends = cache.pump(now, &cfg, &[1, 2], |_| true, |_| 0);
        assert_eq!(sends.len(), 1);
        cache.retry(&sends[0]);
        let retry = cache.pump(now, &cfg, &[1, 2], |_| true, |_| 0);
        assert_eq!(retry[0].payload, sends[0].payload);
        cache.remove(&id);
        assert!(!cache.valid(&retry[0]));
        cache.retry(&retry[0]);
        assert!(cache.deliveries.is_empty());
    }
    #[test]
    fn eviction_prefers_heaviest_owner_then_oldest_clip() {
        let cfg = config();
        let mut cache = VideoClipCache::default();
        cache.observe(
            1,
            &offer([1; 16], 1024 * 1024, 0),
            &[],
            &[1, 2, 3],
            &cfg,
            |_| true,
        );
        cache.observe(
            2,
            &offer([2; 16], 3 * 1024 * 1024, 0),
            &[],
            &[1, 2, 3],
            &cfg,
            |_| true,
        );
        let sends = cache.observe(
            3,
            &offer([3; 16], 1024 * 1024, 0),
            &[],
            &[1, 2, 3],
            &cfg,
            |_| true,
        );
        assert!(cache.entries.contains_key(&[1; 16]));
        assert!(!cache.entries.contains_key(&[2; 16]));
        assert!(sends
            .iter()
            .any(|s| s.recipient == 2 && s.payload[17] == STATE_NOT_HELD));
    }
    #[test]
    fn protected_removal_and_locked_owner_filtering() {
        let mut cfg = config();
        let mut cache = VideoClipCache::default();
        let id = [5; 16];
        cache.observe(1, &offer(id, 3, 1), &[], &[1, 2], &cfg, |_| true);
        cache.observe(1, &chunk(id, 0, &[1, 2, 3]), &[], &[1, 2], &cfg, |_| true);
        cache.observe(2, &request(OP_REMOVE, id), &[], &[1, 2], &cfg, |_| true);
        assert!(cache.entries.contains_key(&id));
        let catalog = request(OP_CACHE_CATALOG, [0; 16]);
        assert!(cache
            .observe(2, &catalog, &[2], &[1, 2], &cfg, |_| false)
            .is_empty());
        cache.observe(
            2,
            &request(OP_CACHE_REQUEST, id),
            &[2],
            &[1, 2],
            &cfg,
            |_| false,
        );
        assert!(cache.deliveries.is_empty());
        cache.observe(1, &request(OP_REMOVE, id), &[], &[1, 2], &cfg, |_| true);
        assert!(cache.entries.is_empty());
        cfg.content_removal_locked = true;
        complete(&mut cache, [6; 16], 1, 3, &cfg);
        cache.observe(2, &request(OP_REMOVE, [6; 16]), &[], &[1, 2], &cfg, |_| {
            true
        });
        assert!(cache.entries.contains_key(&[6; 16]));
    }
    #[test]
    fn round_robin_deliveries_do_not_starve_other_peers() {
        let mut cfg = config();
        cfg.video_clip_cache_serve_megabytes_per_second = 0;
        let mut cache = VideoClipCache::default();
        let id = [9; 16];
        complete(&mut cache, id, 1, CHUNK_BYTES * 2, &cfg);
        for peer in [2, 3] {
            cache.observe(
                peer,
                &request(OP_CACHE_REQUEST, id),
                &[peer],
                &[1, 2, 3],
                &cfg,
                |_| true,
            );
        }
        let sends = cache.pump(Instant::now(), &cfg, &[1, 2, 3], |_| true, |_| 0);
        assert_eq!(
            sends.iter().map(|s| s.recipient).collect::<Vec<_>>(),
            [2, 3, 2, 3]
        );
    }
}
