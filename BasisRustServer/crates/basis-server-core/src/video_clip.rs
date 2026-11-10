//! Generation-checked clip metadata and bounded replay; all file operations run on a storage worker.
mod storage;
use basis_protocol::{
    config::ServerConfig,
    video_clip::{self, CacheState, ClipId, Opcode, CHUNK_BYTES, OFFER_FIXED_BYTES},
};
use basis_transport::PeerSession;
use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::PathBuf,
    time::Instant,
};
use tracing::warn;
pub(crate) const CLIP_MANAGER_IDENTIFIER: &str = video_clip::MANAGER_IDENTIFIER;
const HEADER_BYTES: usize = video_clip::CHUNK_HEADER_BYTES;
const MAX_CLIP_BYTES: usize = 1024 * 1024 * 1024;
const MAX_NEED_INDICES: usize = 512;
const MAX_QUEUED_PACKETS: usize = 256;
const MAX_PENDING_CONTROLS: usize = 4096;
const MAX_ENTRIES: usize = 4096;
const MAX_DELIVERIES: usize = 4096;

#[derive(Debug, Clone)]
pub(crate) struct SendTarget {
    recipient: u16,
    session: Option<PeerSession>,
    epoch: u64,
}
#[derive(Debug, Clone)]
pub(crate) enum CacheSend {
    Chunk {
        target: SendTarget,
        owner: u16,
        id: ClipId,
        index: usize,
        generation: u64,
        payload: Bytes,
    },
    Offer {
        target: SendTarget,
        owner: u16,
        id: ClipId,
        generation: u64,
        payload: Bytes,
    },
    State {
        target: SendTarget,
        id: ClipId,
        revision: u64,
        state: CacheState,
        payload: Bytes,
    },
}
impl CacheSend {
    fn target(&self) -> &SendTarget {
        match self {
            Self::Chunk { target, .. }
            | Self::Offer { target, .. }
            | Self::State { target, .. } => target,
        }
    }
    fn id(&self) -> ClipId {
        match self {
            Self::Chunk { id, .. } | Self::Offer { id, .. } | Self::State { id, .. } => *id,
        }
    }
    pub(crate) fn recipient(&self) -> u16 {
        self.target().recipient
    }
    pub(crate) fn recipient_session(&self) -> Option<&PeerSession> {
        self.target().session.as_ref()
    }
    pub(crate) fn owner(&self) -> u16 {
        match self {
            Self::Chunk { owner, .. } | Self::Offer { owner, .. } => *owner,
            Self::State { target, .. } => target.recipient,
        }
    }
    pub(crate) fn payload(&self) -> &Bytes {
        match self {
            Self::Chunk { payload, .. }
            | Self::Offer { payload, .. }
            | Self::State { payload, .. } => payload,
        }
    }
    pub(crate) fn requires_share_permission(&self) -> bool {
        matches!(self, Self::Chunk { .. } | Self::Offer { .. })
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryOutcome {
    Retained,
    Stale,
    RecipientGone,
    CapacityExceeded,
}
#[derive(Debug)]
struct Entry {
    owner: u16,
    generation: u64,
    total_bytes: usize,
    held: Vec<bool>,
    pending: Vec<bool>,
    held_count: usize,
    offer: Bytes,
    protected: bool,
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
    target: SendTarget,
    id: ClipId,
    generation: u64,
    next: usize,
    explicit: VecDeque<usize>,
}
#[derive(Debug, Default)]
pub(crate) struct VideoClipCache {
    entries: HashMap<ClipId, Entry>,
    ready: HashSet<u16>,
    bytes: usize,
    sequence: u64,
    sessions: HashMap<u16, SendTarget>,
    deliveries: VecDeque<Delivery>,
    controls: VecDeque<CacheSend>,
    state_revisions: HashMap<(u16, ClipId), u64>,
    pending_removals: VecDeque<(ClipId, u64)>,
    worker: Option<storage::Worker>,
    tokens: f64,
    last_pump: Option<Instant>,
}
impl VideoClipCache {
    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }
    pub(crate) fn bind_session(&mut self, session: PeerSession) {
        let peer = session.peer_id();
        if self.sessions.get(&peer).is_some_and(|old| {
            old.session
                .as_ref()
                .is_some_and(|old| old.same_connection(&session))
        }) {
            return;
        }
        self.remove_player(peer);
        self.sequence = self.sequence.wrapping_add(1);
        self.sessions.insert(
            peer,
            SendTarget {
                recipient: peer,
                session: Some(session),
                epoch: self.sequence,
            },
        );
    }
    fn target(&self, recipient: u16) -> SendTarget {
        self.sessions
            .get(&recipient)
            .cloned()
            .unwrap_or(SendTarget {
                recipient,
                session: None,
                epoch: 0,
            })
    }
    fn target_valid(&self, target: &SendTarget) -> bool {
        match self.sessions.get(&target.recipient) {
            Some(current) => current.epoch == target.epoch,
            None => target.epoch == 0 && target.session.is_none(),
        }
    }
    pub(crate) fn remove_player(&mut self, peer: u16) {
        self.ready.remove(&peer);
        self.sessions.remove(&peer);
        let ids: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, e)| (e.owner == peer).then_some(*id))
            .collect();
        for id in ids {
            self.remove(&id);
        }
        self.deliveries.retain(|d| d.target.recipient != peer);
        self.controls.retain(|s| s.recipient() != peer);
        self.state_revisions.retain(|(p, _), _| *p != peer);
    }
    fn remove(&mut self, id: &ClipId) -> Option<u16> {
        let e = self.entries.remove(id)?;
        self.state_revisions.remove(&(e.owner, *id));
        self.bytes = self.bytes.saturating_sub(e.total_bytes);
        self.deliveries.retain(|d| d.id != *id);
        self.pending_removals.push_back((*id, e.generation));
        self.flush_removals();
        Some(e.owner)
    }
    fn flush_removals(&mut self) {
        if let Some(worker) = &self.worker {
            while let Some(&(id, generation)) = self.pending_removals.front() {
                if !worker.submit(storage::Command::Remove { id, generation }) {
                    break;
                }
                self.pending_removals.pop_front();
            }
        }
    }
    fn enabled(config: &ServerConfig) -> bool {
        config.video_clip_cache_enabled && config.video_clip_cache_max_megabytes > 0
    }
    fn state_send(&mut self, recipient: u16, id: ClipId, state: CacheState) -> CacheSend {
        self.sequence = self.sequence.wrapping_add(1);
        let revision = self.sequence;
        self.state_revisions.insert((recipient, id), revision);
        CacheSend::State {
            target: self.target(recipient),
            id,
            revision,
            state,
            payload: Bytes::from(video_clip::state_payload(id, state)),
        }
    }
    fn offer_send(&self, recipient: u16, e: &Entry) -> CacheSend {
        let mut payload = e.offer.to_vec();
        payload[0] = Opcode::CacheOffer as u8;
        CacheSend::Offer {
            target: self.target(recipient),
            owner: e.owner,
            id: payload[1..17].try_into().unwrap(),
            generation: e.generation,
            payload: Bytes::from(payload),
        }
    }
    pub(crate) fn valid(&self, send: &CacheSend) -> bool {
        if !self.target_valid(send.target()) {
            return false;
        }
        match send {
            CacheSend::Chunk {
                id,
                index,
                generation,
                owner,
                ..
            } => self.entries.get(id).is_some_and(|e| {
                e.generation == *generation
                    && e.owner == *owner
                    && e.held.get(*index) == Some(&true)
            }),
            CacheSend::Offer {
                id,
                generation,
                owner,
                ..
            } => self
                .entries
                .get(id)
                .is_some_and(|e| e.generation == *generation && e.owner == *owner && e.complete()),
            CacheSend::State {
                target,
                id,
                revision,
                ..
            } => self.state_revisions.get(&(target.recipient, *id)) == Some(revision),
        }
    }
    pub(crate) fn acknowledge(&mut self, send: &CacheSend) {
        if let CacheSend::State {
            target,
            id,
            revision,
            state: CacheState::NotHeld,
            ..
        } = send
        {
            if self.state_revisions.get(&(target.recipient, *id)) == Some(revision) {
                self.state_revisions.remove(&(target.recipient, *id));
            }
        }
    }
    pub(crate) fn retry(&mut self, send: &CacheSend) -> RetryOutcome {
        if !self.target_valid(send.target()) {
            return RetryOutcome::RecipientGone;
        }
        if !self.valid(send) {
            return RetryOutcome::Stale;
        }
        if let CacheSend::Chunk {
            target,
            id,
            index,
            generation,
            ..
        } = send
        {
            let count = self.entries[id].held.len();
            if let Some(delivery) = self.delivery(target.clone(), *id, *generation, count) {
                if !delivery.explicit.contains(index) {
                    delivery.explicit.push_front(*index);
                }
                return RetryOutcome::Retained;
            }
            return RetryOutcome::CapacityExceeded;
        }
        let pending = std::mem::take(&mut self.controls);
        self.controls = pending
            .into_iter()
            .filter(|old| self.valid(old) && self_key(old) != self_key(send))
            .collect();
        if self.controls.len() >= MAX_PENDING_CONTROLS {
            // The caller closes the pressured connection rather than losing an authoritative
            // state or a late-join advertisement while the owner believes the server holds it.
            return RetryOutcome::CapacityExceeded;
        }
        self.controls.push_back(send.clone());
        RetryOutcome::Retained
    }
    fn delivery(
        &mut self,
        target: SendTarget,
        id: ClipId,
        generation: u64,
        total: usize,
    ) -> Option<&mut Delivery> {
        if let Some(index) = self.deliveries.iter().position(|d| {
            d.target.recipient == target.recipient
                && d.target.epoch == target.epoch
                && d.id == id
                && d.generation == generation
        }) {
            return Some(&mut self.deliveries[index]);
        }
        if self.deliveries.len() >= MAX_DELIVERIES {
            return None;
        }
        self.deliveries.push_back(Delivery {
            target,
            id,
            generation,
            next: total,
            explicit: VecDeque::new(),
        });
        self.deliveries.back_mut()
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
        let mut sends = self.drain_storage(connected, &may_share);
        self.flush_removals();
        if payload.len() < 17 {
            return sends;
        }
        let id: ClipId = payload[1..17].try_into().unwrap();
        let to_server = !recipients.is_empty() && recipients.iter().all(|p| *p == owner);
        match Opcode::from_payload(payload) {
            Some(Opcode::Offer) if may_share(owner) => {
                self.offer(owner, id, payload, config, &mut sends)
            }
            Some(Opcode::Chunk) if may_share(owner) => self.chunk(owner, id, payload, &mut sends),
            Some(Opcode::Pose) if payload.len() == 49 => {
                if let Some(e) = self.entries.get_mut(&id).filter(|e| e.owner == owner) {
                    let mut offer = e.offer.to_vec();
                    offer[23..55].copy_from_slice(&payload[17..49]);
                    e.offer = Bytes::from(offer);
                }
            }
            Some(Opcode::Remove)
                if self.entries.get(&id).is_some_and(|e| {
                    e.owner == owner || (!e.protected && !config.content_removal_locked)
                }) =>
            {
                self.remove(&id);
            }
            Some(Opcode::CacheCatalog) if to_server => {
                self.ready.insert(owner);
                if Self::enabled(config) {
                    let mut entries: Vec<_> = self
                        .entries
                        .values()
                        .filter(|e| e.owner != owner && e.complete() && may_share(e.owner))
                        .collect();
                    entries.sort_by_key(|e| e.generation);
                    sends.extend(entries.into_iter().map(|e| self.offer_send(owner, e)));
                }
            }
            Some(Opcode::CacheRequest)
                if to_server && payload.len() == 17 && Self::enabled(config) =>
            {
                if let Some(e) = self
                    .entries
                    .get(&id)
                    .filter(|e| e.owner != owner && e.complete() && may_share(e.owner))
                {
                    let (count, generation) = (e.held.len(), e.generation);
                    let target = self.target(owner);
                    if let Some(d) = self.delivery(target, id, generation, count) {
                        if d.next >= count {
                            d.next = 0;
                        }
                    }
                }
            }
            Some(Opcode::Need) if to_server && Self::enabled(config) => {
                const FIXED: usize = 19;
                if payload.len() >= FIXED {
                    let count = u16::from_le_bytes(payload[17..19].try_into().unwrap()) as usize;
                    if count <= MAX_NEED_INDICES && payload.len() == FIXED + count * 4 {
                        if let Some(e) = self
                            .entries
                            .get(&id)
                            .filter(|e| e.owner != owner && may_share(e.owner))
                        {
                            let (count, generation) = (e.held.len(), e.generation);
                            let indices: Vec<_> = payload[FIXED..]
                                .as_chunks::<4>()
                                .0
                                .iter()
                                .map(|v| i32::from_le_bytes(*v))
                                .filter(|i| *i >= 0 && (*i as usize) < count && e.held[*i as usize])
                                .map(|i| i as usize)
                                .collect();
                            let target = self.target(owner);
                            if let Some(d) = self.delivery(target, id, generation, count) {
                                for i in indices {
                                    if d.explicit.len() >= count {
                                        break;
                                    }
                                    if !d.explicit.contains(&i) {
                                        d.explicit.push_back(i);
                                    }
                                }
                            }
                        }
                    }
                }
            }
            _ => {}
        }
        sends
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
            || !self.pending_removals.is_empty()
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
        while self.bytes.saturating_add(total as usize) > cap || self.entries.len() >= MAX_ENTRIES {
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
                .min_by_key(|(_, e)| e.generation)
                .map(|(id, _)| *id)
            else {
                return;
            };
            if let Some(owner) = self.remove(&victim) {
                sends.push(self.state_send(owner, victim, CacheState::NotHeld));
            }
        }
        if self.worker.is_none() {
            match storage::Worker::spawn() {
                Ok(worker) => self.worker = Some(worker),
                Err(err) => {
                    warn!("video storage worker failed: {err}");
                    return;
                }
            }
        }
        self.sequence = self.sequence.wrapping_add(1);
        let generation = self.sequence;
        let root = if config.video_clip_cache_directory.trim().is_empty() {
            std::env::temp_dir().join("BasisClipCache")
        } else {
            PathBuf::from(&config.video_clip_cache_directory)
        };
        if !self
            .worker
            .as_ref()
            .unwrap()
            .submit(storage::Command::Create {
                id,
                generation,
                root,
            })
        {
            return;
        }
        // A new generation invalidates any delayed NotHeld/Offer from the replaced clip id.
        self.state_revisions.remove(&(owner, id));
        self.bytes += total as usize;
        let count = (total as usize).div_ceil(CHUNK_BYTES);
        self.entries.insert(
            id,
            Entry {
                owner,
                generation,
                total_bytes: total as usize,
                held: vec![false; count],
                pending: vec![false; count],
                held_count: 0,
                offer: Bytes::copy_from_slice(payload),
                protected: payload[22] & 1 != 0,
            },
        );
    }
    fn chunk(&mut self, owner: u16, id: ClipId, payload: &[u8], sends: &mut Vec<CacheSend>) {
        if payload.len() <= HEADER_BYTES {
            return;
        }
        let index = i32::from_le_bytes(payload[17..21].try_into().unwrap());
        let Some(e) = self.entries.get_mut(&id).filter(|e| e.owner == owner) else {
            return;
        };
        if index < 0
            || index as usize >= e.held.len()
            || e.held[index as usize]
            || e.pending[index as usize]
            || payload.len() - HEADER_BYTES != e.chunk_length(index as usize)
        {
            return;
        }
        let index = index as usize;
        let accepted = self.worker.as_ref().is_some_and(|worker| {
            worker.submit(storage::Command::Write {
                id,
                generation: e.generation,
                index,
                data: Bytes::copy_from_slice(&payload[HEADER_BYTES..]),
            })
        });
        if accepted {
            e.pending[index] = true;
        } else {
            self.remove(&id);
            sends.push(self.state_send(owner, id, CacheState::NotHeld));
        }
    }
    fn drain_storage(
        &mut self,
        connected: &[u16],
        may_share: &impl Fn(u16) -> bool,
    ) -> Vec<CacheSend> {
        let events = self
            .worker
            .as_ref()
            .map_or_else(Vec::new, storage::Worker::drain);
        let mut sends = Vec::new();
        for event in events {
            match event {
                storage::Event::Created { id, generation } => {
                    if let Some(e) = self.entries.get(&id).filter(|e| e.generation == generation) {
                        let owner = e.owner;
                        sends.push(self.state_send(owner, id, CacheState::Upload));
                    }
                }
                storage::Event::Written {
                    id,
                    generation,
                    index,
                } => {
                    if let Some(e) = self
                        .entries
                        .get_mut(&id)
                        .filter(|e| e.generation == generation)
                    {
                        if !e.held[index] {
                            e.held[index] = true;
                            e.pending[index] = false;
                            e.held_count += 1;
                        }
                        if e.complete() {
                            let owner = e.owner;
                            sends.push(self.state_send(owner, id, CacheState::Held));
                            if may_share(owner) {
                                let e = &self.entries[&id];
                                for peer in self
                                    .ready
                                    .iter()
                                    .copied()
                                    .filter(|p| *p != owner && connected.contains(p))
                                {
                                    sends.push(self.offer_send(peer, e));
                                }
                            }
                        }
                    }
                }
                storage::Event::Read {
                    id,
                    generation,
                    index,
                    target,
                    data,
                } => {
                    if let Some(e) = self
                        .entries
                        .get(&id)
                        .filter(|e| e.generation == generation && may_share(e.owner))
                    {
                        let mut payload = video_clip::chunk_header(id, index);
                        payload.extend_from_slice(&data);
                        sends.push(CacheSend::Chunk {
                            target,
                            owner: e.owner,
                            id,
                            index,
                            generation,
                            payload: Bytes::from(payload),
                        });
                    }
                }
                storage::Event::Failed {
                    id,
                    generation,
                    message,
                } => {
                    if let Some(e) = self.entries.get(&id).filter(|e| e.generation == generation) {
                        let owner = e.owner;
                        warn!("video clip storage failed: {message}");
                        self.remove(&id);
                        sends.push(self.state_send(owner, id, CacheState::NotHeld));
                    }
                }
            }
        }
        sends
    }
    pub(crate) fn pump(
        &mut self,
        now: Instant,
        config: &ServerConfig,
        connected: &[u16],
        may_share: impl Fn(u16) -> bool,
        queued: impl Fn(u16) -> usize,
    ) -> Vec<CacheSend> {
        self.flush_removals();
        let mut sends = self.drain_storage(connected, &may_share);
        for _ in 0..self.controls.len().min(256) {
            let send = self.controls.pop_front().unwrap();
            if self.valid(&send)
                && connected.contains(&send.recipient())
                && (!send.requires_share_permission() || may_share(send.owner()))
            {
                sends.push(send);
            }
        }
        if !Self::enabled(config) {
            self.deliveries.clear();
            let ids: Vec<_> = self.entries.keys().copied().collect();
            for id in ids {
                if let Some(owner) = self.remove(&id) {
                    sends.push(self.state_send(owner, id, CacheState::NotHeld));
                }
            }
            return sends;
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
        let mut idle = 0;
        let mut submitted = 0;
        let mut admitted = HashMap::<u16, usize>::new();
        while !self.deliveries.is_empty()
            && idle < self.deliveries.len()
            && self.tokens > 0.
            && submitted < 32
        {
            let mut d = self.deliveries.pop_front().unwrap();
            let Some(e) = self
                .entries
                .get(&d.id)
                .filter(|e| e.generation == d.generation)
            else {
                continue;
            };
            if !connected.contains(&d.target.recipient)
                || !self.target_valid(&d.target)
                || !may_share(e.owner)
                || (d.next >= e.held.len() && d.explicit.is_empty())
            {
                continue;
            }
            if queued(d.target.recipient)
                .saturating_add(*admitted.get(&d.target.recipient).unwrap_or(&0))
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
            let length = e.chunk_length(index);
            let command = storage::Command::Read {
                id: d.id,
                generation: d.generation,
                index,
                length,
                target: d.target.clone(),
            };
            if !self
                .worker
                .as_ref()
                .is_some_and(|worker| worker.submit(command))
            {
                d.explicit.push_front(index);
                self.deliveries.push_front(d);
                break;
            }
            submitted += 1;
            self.tokens -= (HEADER_BYTES + length) as f64;
            *admitted.entry(d.target.recipient).or_default() +=
                (HEADER_BYTES + length).div_ceil(512);
            if d.next < e.held.len() || !d.explicit.is_empty() {
                self.deliveries.push_back(d);
            }
        }
        sends
    }
}
fn self_key(send: &CacheSend) -> (u16, u64, ClipId, bool) {
    (
        send.recipient(),
        send.target().epoch,
        send.id(),
        matches!(send, CacheSend::State { .. }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    fn offer(id: ClipId, size: usize, flags: u8) -> Vec<u8> {
        let mut p = vec![Opcode::Offer as u8];
        p.extend(id);
        p.extend((size as i32).to_le_bytes());
        p.extend([0, flags]);
        p.extend([0; 32]);
        p.extend(0u16.to_le_bytes());
        p
    }
    fn chunk(id: ClipId, index: i32, data: &[u8]) -> Vec<u8> {
        let mut p = video_clip::chunk_header(id, index as usize);
        p.extend(data);
        p
    }
    fn request(op: Opcode, id: ClipId) -> Vec<u8> {
        let mut p = vec![op as u8];
        p.extend(id);
        p
    }
    fn config() -> ServerConfig {
        ServerConfig {
            video_clip_cache_enabled: true,
            video_clip_cache_max_megabytes: 4,
            video_clip_cache_serve_megabytes_per_second: 0,
            ..ServerConfig::default()
        }
    }
    fn poll_until(
        cache: &mut VideoClipCache,
        cfg: &ServerConfig,
        mut done: impl FnMut(&VideoClipCache, &[CacheSend]) -> bool,
    ) -> Vec<CacheSend> {
        let mut sends = Vec::new();
        for _ in 0..1000 {
            sends.extend(cache.pump(Instant::now(), cfg, &[1, 2, 3], |_| true, |_| 0));
            if done(cache, &sends) {
                return sends;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        panic!("storage worker did not complete")
    }
    fn complete(
        cache: &mut VideoClipCache,
        id: ClipId,
        owner: u16,
        size: usize,
        cfg: &ServerConfig,
    ) {
        cache.observe(owner, &offer(id, size, 0), &[], &[1, 2, 3], cfg, |_| true);
        let states = poll_until(cache, cfg, |_, s| {
            s.iter().any(|s| {
                matches!(
                    s,
                    CacheSend::State {
                        state: CacheState::Upload,
                        ..
                    }
                )
            })
        });
        assert!(states
            .iter()
            .any(|s| s.payload()[17] == CacheState::Upload as u8));
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
            poll_until(cache, cfg, |c, _| c.entries[&id].held[index]);
        }
        assert!(cache.entries[&id].complete());
    }
    #[test]
    fn disk_cache_catalog_and_paced_request_roundtrip() {
        let mut cfg = config();
        cfg.video_clip_cache_serve_megabytes_per_second = 1;
        let mut cache = VideoClipCache::default();
        let id = [3; 16];
        complete(&mut cache, id, 1, CHUNK_BYTES + 3, &cfg);
        let catalog = request(Opcode::CacheCatalog, [0; 16]);
        assert!(cache
            .observe(2, &catalog, &[], &[1, 2], &cfg, |_| true)
            .is_empty());
        let offers = cache.observe(2, &catalog, &[2], &[1, 2], &cfg, |_| true);
        assert_eq!(offers.len(), 1);
        assert!(matches!(offers[0], CacheSend::Offer { .. }));
        cache.observe(
            2,
            &request(Opcode::CacheRequest, id),
            &[2],
            &[1, 2],
            &cfg,
            |_| true,
        );
        let sends = poll_until(&mut cache, &cfg, |_, s| {
            s.iter()
                .filter(|s| matches!(s, CacheSend::Chunk { .. }))
                .count()
                == 2
        });
        let chunks: Vec<_> = sends
            .iter()
            .filter(|s| matches!(s, CacheSend::Chunk { .. }))
            .collect();
        assert_eq!(&chunks[1].payload()[HEADER_BYTES..], &[1; 3]);
    }
    #[test]
    fn pump_preserves_backlogged_cursor_and_retries_without_losing_chunks() {
        let cfg = config();
        let mut cache = VideoClipCache::default();
        let id = [4; 16];
        complete(&mut cache, id, 1, 3, &cfg);
        cache.observe(
            2,
            &request(Opcode::CacheRequest, id),
            &[2],
            &[1, 2],
            &cfg,
            |_| true,
        );
        assert!(cache
            .pump(Instant::now(), &cfg, &[1, 2], |_| true, |_| 256)
            .is_empty());
        assert_eq!(cache.deliveries[0].next, 0);
        let sends = poll_until(&mut cache, &cfg, |_, s| {
            s.iter().any(|s| matches!(s, CacheSend::Chunk { .. }))
        });
        let original = sends
            .iter()
            .find(|s| matches!(s, CacheSend::Chunk { .. }))
            .unwrap();
        assert_eq!(cache.retry(original), RetryOutcome::Retained);
        let retry = poll_until(&mut cache, &cfg, |_, s| {
            s.iter().any(|s| matches!(s, CacheSend::Chunk { .. }))
        });
        assert_eq!(retry.last().unwrap().payload(), original.payload());
        cache.remove(&id);
        assert!(!cache.valid(original));
        assert_eq!(cache.retry(original), RetryOutcome::Stale);
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
        assert!(sends.iter().any(|s| s.recipient() == 2
            && matches!(
                s,
                CacheSend::State {
                    state: CacheState::NotHeld,
                    ..
                }
            )));
    }
    #[test]
    fn protected_removal_and_locked_owner_filtering() {
        let mut cfg = config();
        let mut cache = VideoClipCache::default();
        let id = [5; 16];
        cache.observe(1, &offer(id, 3, 1), &[], &[1, 2], &cfg, |_| true);
        cache.observe(2, &request(Opcode::Remove, id), &[], &[1, 2], &cfg, |_| {
            true
        });
        assert!(cache.entries.contains_key(&id));
        assert!(cache
            .observe(
                2,
                &request(Opcode::CacheCatalog, [0; 16]),
                &[2],
                &[1, 2],
                &cfg,
                |_| false
            )
            .is_empty());
        cache.observe(
            2,
            &request(Opcode::CacheRequest, id),
            &[2],
            &[1, 2],
            &cfg,
            |_| false,
        );
        assert!(cache.deliveries.is_empty());
        cache.observe(1, &request(Opcode::Remove, id), &[], &[1, 2], &cfg, |_| {
            true
        });
        assert!(cache.entries.is_empty());
        cfg.content_removal_locked = true;
        complete(&mut cache, [6; 16], 1, 3, &cfg);
        cache.observe(
            2,
            &request(Opcode::Remove, [6; 16]),
            &[],
            &[1, 2],
            &cfg,
            |_| true,
        );
        assert!(cache.entries.contains_key(&[6; 16]));
    }
    #[test]
    fn round_robin_deliveries_do_not_starve_other_peers() {
        let cfg = config();
        let mut cache = VideoClipCache::default();
        let id = [9; 16];
        complete(&mut cache, id, 1, CHUNK_BYTES * 2, &cfg);
        for peer in [2, 3] {
            cache.observe(
                peer,
                &request(Opcode::CacheRequest, id),
                &[peer],
                &[1, 2, 3],
                &cfg,
                |_| true,
            );
        }
        let sends = poll_until(&mut cache, &cfg, |_, s| {
            s.iter()
                .filter(|s| matches!(s, CacheSend::Chunk { .. }))
                .count()
                == 4
        });
        assert_eq!(
            sends
                .iter()
                .filter(|s| matches!(s, CacheSend::Chunk { .. }))
                .map(CacheSend::recipient)
                .collect::<Vec<_>>(),
            [2, 3, 2, 3]
        );
    }
    #[test]
    fn eviction_control_is_retained_coalesced_and_generation_checked() {
        let cfg = config();
        let mut cache = VideoClipCache::default();
        let id = [7; 16];
        complete(&mut cache, id, 1, 3, &cfg);
        let held = cache.state_send(1, id, CacheState::Held);
        assert_eq!(cache.retry(&held), RetryOutcome::Retained);
        let old_offer = cache.offer_send(2, &cache.entries[&id]);
        cache.remove(&id);
        let evicted = cache.state_send(1, id, CacheState::NotHeld);
        assert_eq!(cache.retry(&evicted), RetryOutcome::Retained);
        assert_eq!(cache.controls.len(), 1);
        assert_eq!(cache.retry(&held), RetryOutcome::Stale);
        assert_eq!(cache.retry(&old_offer), RetryOutcome::Stale);
        let sends = cache.pump(Instant::now(), &cfg, &[1, 2], |_| true, |_| 256);
        assert!(sends.iter().any(|s| matches!(
            s,
            CacheSend::State {
                state: CacheState::NotHeld,
                ..
            }
        )));
        assert!(!sends.iter().any(|s| matches!(
            s,
            CacheSend::State {
                state: CacheState::Held,
                ..
            }
        )));
        // The same id's new generation invalidates an eviction delayed by transport admission.
        cache.observe(1, &offer(id, 3, 0), &[], &[1, 2], &cfg, |_| true);
        assert!(!cache.valid(&evicted));
        cache.remove(&id);
        assert!(!cache.state_revisions.contains_key(&(1, id)));
    }
    #[test]
    fn bounded_control_capacity_reports_pressure_without_losing_retained_controls() {
        let cfg = config();
        let mut cache = VideoClipCache::default();
        complete(&mut cache, [1; 16], 1, 3, &cfg);
        for i in 0..MAX_PENDING_CONTROLS {
            let mut id = [0; 16];
            id[..8].copy_from_slice(&(i as u64).to_le_bytes());
            let state = cache.state_send(1, id, CacheState::NotHeld);
            assert_eq!(cache.retry(&state), RetryOutcome::Retained);
        }
        let extra = cache.state_send(1, [255; 16], CacheState::NotHeld);
        assert_eq!(cache.retry(&extra), RetryOutcome::CapacityExceeded);
        assert_eq!(cache.controls.len(), MAX_PENDING_CONTROLS);
    }
    #[test]
    fn delayed_work_retains_recipient_incarnation() {
        let mut cache = VideoClipCache::default();
        cache.sessions.insert(
            2,
            SendTarget {
                recipient: 2,
                session: None,
                epoch: 41,
            },
        );
        let send = cache.state_send(2, [1; 16], CacheState::NotHeld);
        cache.sessions.insert(
            2,
            SendTarget {
                recipient: 2,
                session: None,
                epoch: 42,
            },
        );
        assert_eq!(cache.retry(&send), RetryOutcome::RecipientGone);
        assert!(!cache.valid(&send));
    }
}
