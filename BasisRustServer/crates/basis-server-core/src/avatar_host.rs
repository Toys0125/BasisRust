//! Avatar range relay, pinned to BasisVR 2e6b44de (protocol 57).
use super::*;
use std::collections::VecDeque;

pub(crate) const MESSAGE_ID: u16 = 64;
const CHUNK: usize = 16 * 1024;
type Id = [u8; 16];
type Range = (Id, i64, usize);
#[derive(Default)]
pub(crate) struct Ledger {
    offers: HashMap<Id, (i64, Vec<PeerId>)>,
    fetches: HashMap<u32, Fetch>,
    cache: VecDeque<(Range, Bytes)>,
    cached: usize,
    next: u32,
    budgets: HashMap<PeerId, (f64, Instant)>,
    outgoing: VecDeque<Outgoing>,
    outgoing_bytes: usize,
}
struct Fetch {
    range: Range,
    owner: PeerId,
    data: Vec<u8>,
    waiters: Vec<(PeerId, u32)>,
}
struct Outgoing {
    session: PeerSession,
    request: Option<u32>,
    payload: OutPayload,
}
enum OutPayload {
    Raw(Vec<u8>),
    Chunks { data: Bytes, offset: usize },
}
impl Outgoing {
    fn len(&self) -> usize {
        match &self.payload {
            OutPayload::Raw(p) => p.len(),
            OutPayload::Chunks { data, .. } => data.len(),
        }
    }
    fn wire(&self) -> Vec<u8> {
        match &self.payload {
            OutPayload::Raw(p) => p.clone(),
            OutPayload::Chunks { data, offset } => {
                let mut w = NetWriter::new();
                w.put_u16(MESSAGE_ID);
                w.put_u8(12);
                w.put_u32(self.request.expect("chunk request"));
                w.put_i32(*offset as i32);
                let n = data.len().min(CHUNK);
                w.put_i32(n as i32);
                w.put_bytes(&data[..n]);
                w.into_vec()
            }
        }
    }
}
#[derive(Clone, Copy)]
struct Limits {
    range: usize,
    cache: usize,
    flight: usize,
}
impl Limits {
    fn from_config(config: &ServerConfig) -> Self {
        let range = (config.avatar_host_max_megabytes.max(0) as usize).saturating_mul(1024 * 1024);
        let cache =
            (config.avatar_host_cache_megabytes.max(0) as usize).saturating_mul(1024 * 1024);
        Self {
            range,
            cache,
            flight: cache.max(range.saturating_mul(2)),
        }
    }
}
impl Ledger {
    fn send(
        &mut self,
        state: &ServerState,
        peer: PeerId,
        request: Option<u32>,
        body: &[u8],
    ) -> bool {
        if self.outgoing.len() >= 4096 {
            return false;
        }
        if !state
            .message_subscriptions
            .get(&peer)
            .is_some_and(|s| s.contains(&MESSAGE_ID))
        {
            return true;
        }
        if let Some(session) = state.transport.peer_session(peer) {
            let mut payload = MESSAGE_ID.to_le_bytes().to_vec();
            payload.extend_from_slice(body);
            self.outgoing_bytes += payload.len();
            self.outgoing.push_back(Outgoing {
                session,
                request,
                payload: OutPayload::Raw(payload),
            });
        }
        true
    }
    fn failed(&mut self, state: &ServerState, peer: PeerId, request: u32, reason: u8) {
        for fetch in self.fetches.values_mut() {
            fetch.waiters.retain(|&(p, r)| p != peer || r != request);
        }
        self.outgoing
            .retain(|out| !(out.session.peer_id() == peer && out.request == Some(request)));
        self.outgoing_bytes = self.outgoing.iter().map(|o| o.len()).sum();
        let mut w = NetWriter::new();
        w.put_u8(13);
        w.put_u32(request);
        w.put_u8(reason);
        self.send(state, peer, None, w.as_slice());
    }
    fn chunks(
        &mut self,
        state: &ServerState,
        peer: PeerId,
        request: u32,
        offset: usize,
        data: Bytes,
    ) {
        if data.is_empty() {
            return;
        }
        let limit = Limits::from_config(&state.config.read())
            .flight
            .max(data.len());
        if self.outgoing.len() >= 4096 || data.len() > limit.saturating_sub(self.outgoing_bytes) {
            self.failed(state, peer, request, 6);
            return;
        }
        if !state
            .message_subscriptions
            .get(&peer)
            .is_some_and(|s| s.contains(&MESSAGE_ID))
        {
            return;
        }
        if let Some(session) = state.transport.peer_session(peer) {
            self.outgoing_bytes += data.len();
            self.outgoing.push_back(Outgoing {
                session,
                request: Some(request),
                payload: OutPayload::Chunks { data, offset },
            });
        }
    }
    fn trim(&mut self, keep: usize) {
        while self.cached > keep {
            if let Some((_, data)) = self.cache.pop_front() {
                self.cached -= data.len();
            } else {
                break;
            }
        }
    }
    fn fail_fetch(&mut self, state: &ServerState, id: u32, reason: u8) {
        if let Some(f) = self.fetches.remove(&id) {
            for (peer, request) in f.waiters {
                self.failed(state, peer, request, reason);
            }
        }
    }
    fn withdraw(&mut self, state: &ServerState, owner: PeerId, id: Id, reason: u8) {
        if let Some((_, owners)) = self.offers.get_mut(&id) {
            if !owners.contains(&owner) {
                return;
            }
            owners.retain(|p| *p != owner);
            if owners.is_empty() {
                self.offers.remove(&id);
                self.cache.retain(|(r, _)| r.0 != id);
                self.cached = self.cache.iter().map(|(_, d)| d.len()).sum();
            }
            let failed: Vec<_> = self
                .fetches
                .iter()
                .filter(|(_, f)| f.owner == owner && f.range.0 == id)
                .map(|(&n, _)| n)
                .collect();
            for n in failed {
                self.fail_fetch(state, n, reason);
            }
        }
    }
    fn offer(&mut self, state: &ServerState, peer: PeerId, id: Id, length: i64, enabled: bool) {
        let reason = if !enabled {
            1
        } else if length <= 0 || length > 16 * 1024 * 1024 * 1024 {
            2
        } else if self.offers.get(&id).is_some_and(|(n, _)| *n != length) {
            5
        } else if !self.offers.get(&id).is_some_and(|(_, p)| p.contains(&peer))
            && self
                .offers
                .values()
                .filter(|(_, p)| p.contains(&peer))
                .count()
                >= 4
        {
            3
        } else {
            0
        };
        if reason == 0 {
            let (_, owners) = self.offers.entry(id).or_insert((length, Vec::new()));
            if !owners.contains(&peer) {
                owners.push(peer);
            }
        }
        let mut w = NetWriter::new();
        w.put_u8(10);
        w.put_bytes(&id);
        w.put_bool(reason == 0);
        w.put_u8(reason);
        self.send(state, peer, None, w.as_slice());
    }
    fn request(
        &mut self,
        state: &ServerState,
        peer: PeerId,
        request: u32,
        range: Range,
        limits: Limits,
    ) {
        let Some((total, owners)) = self.offers.get(&range.0) else {
            self.failed(state, peer, request, 4);
            return;
        };
        let owner = owners[0];
        let reason =
            if range.1 < 0 || range.2 == 0 || range.1 > total.saturating_sub(range.2 as i64) {
                5
            } else if range.2 > limits.range {
                2
            } else {
                0
            };
        if reason != 0 {
            self.failed(state, peer, request, reason);
            return;
        }
        let now = Instant::now();
        let ceiling = limits.range.saturating_mul(2) as f64;
        let budget = self.budgets.entry(peer).or_insert((ceiling, now));
        budget.0 =
            (budget.0 + now.duration_since(budget.1).as_secs_f64() * ceiling / 60.0).min(ceiling);
        budget.1 = now;
        if budget.0 < range.2 as f64 {
            self.failed(state, peer, request, 6);
            return;
        }
        budget.0 -= range.2 as f64;
        if let Some(index) = self.cache.iter().position(|(r, _)| *r == range) {
            let entry = self.cache.remove(index).unwrap();
            self.chunks(state, peer, request, 0, entry.1.clone());
            self.cache.push_back(entry);
            return;
        }
        if self
            .fetches
            .values()
            .flat_map(|f| &f.waiters)
            .filter(|(p, _)| *p == peer)
            .count()
            >= 4
        {
            self.failed(state, peer, request, 6);
            return;
        }
        if let Some(fetch) = self.fetches.values_mut().find(|f| f.range == range) {
            fetch.waiters.push((peer, request));
            let data = fetch.data.clone();
            self.chunks(state, peer, request, 0, Bytes::from(data));
            return;
        }
        let used: usize = self.fetches.values().map(|f| f.range.2).sum();
        if range.2 > limits.flight.saturating_sub(used) {
            self.failed(state, peer, request, 6);
            return;
        }
        if owner == peer {
            self.failed(state, peer, request, 5);
            return;
        }
        let mut data = Vec::new();
        if data.try_reserve_exact(range.2).is_err() {
            self.failed(state, peer, request, 6);
            return;
        }
        self.next = self.next.wrapping_add(1);
        while self.fetches.contains_key(&self.next) {
            self.next = self.next.wrapping_add(1);
        }
        let id = self.next;
        self.fetches.insert(
            id,
            Fetch {
                range,
                owner,
                data,
                waiters: vec![(peer, request)],
            },
        );
        let mut w = NetWriter::new();
        w.put_u8(11);
        w.put_u32(id);
        w.put_bytes(&range.0);
        w.put_i64(range.1);
        w.put_i32(range.2 as i32);
        self.send(state, owner, None, w.as_slice());
    }
    fn owner_chunk(
        &mut self,
        state: &ServerState,
        peer: PeerId,
        id: u32,
        offset: i32,
        data: &[u8],
        limits: Limits,
    ) {
        let Some(f) = self.fetches.get_mut(&id) else {
            return;
        };
        if f.owner != peer {
            return;
        }
        if offset < 0
            || offset as usize != f.data.len()
            || data.is_empty()
            || data.len() > CHUNK
            || data.len() > f.range.2 - f.data.len()
        {
            self.fail_fetch(state, id, 7);
            return;
        }
        f.data.extend_from_slice(data);
        let waiters = f.waiters.clone();
        let done = f.data.len() == f.range.2;
        let outgoing_data = Bytes::copy_from_slice(data);
        for (p, r) in waiters {
            self.chunks(state, p, r, offset as usize, outgoing_data.clone());
        }
        if done {
            let f = self.fetches.remove(&id).unwrap();
            if f.data.len() <= limits.cache {
                self.trim(limits.cache - f.data.len());
                self.cached += f.data.len();
                self.cache.push_back((f.range, Bytes::from(f.data)));
            }
        }
    }
}

pub(crate) async fn handle(state: &ServerState, peer: PeerId, payload: &[u8]) -> Result<()> {
    let config = state.config.read().clone();
    let limits = Limits::from_config(&config);
    let enabled = limits.range > 0
        && (!state.global_state.read().avatar_hosting_locked
            || peer_has_permission(
                state,
                peer,
                basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
            ));
    let mut r = NetReader::new(payload);
    let op = r.get_u8()?;
    let mut ledger = state.avatar_host.lock();
    match op {
        0 => {
            let id: Id = r.get_bytes(16)?.try_into().unwrap();
            let length = r.get_i64()?;
            ledger.offer(state, peer, id, length, enabled);
        }
        1 => {
            let id: Id = r.get_bytes(16)?.try_into().unwrap();
            ledger.withdraw(state, peer, id, 8);
        }
        2 => {
            let request = r.get_u32()?;
            let id: Id = r.get_bytes(16)?.try_into().unwrap();
            let start = r.get_i64()?;
            let len = r.get_i32()?;
            ledger.request(
                state,
                peer,
                request,
                (id, start, len.max(0) as usize),
                limits,
            );
        }
        3 => {
            let id = r.get_u32()?;
            let offset = r.get_i32()?;
            let count = r.get_i32()?;
            if count <= 0 || count as usize > CHUNK || count as usize > r.remaining() {
                if ledger.fetches.get(&id).is_some_and(|f| f.owner == peer) {
                    ledger.fail_fetch(state, id, 7);
                }
            } else {
                ledger.owner_chunk(
                    state,
                    peer,
                    id,
                    offset,
                    r.get_bytes(count as usize)?,
                    limits,
                );
            }
        }
        4 => {
            let id = r.get_u32()?;
            if ledger.fetches.get(&id).is_some_and(|f| f.owner == peer) {
                ledger.fail_fetch(state, id, 7);
            }
        }
        _ => {}
    }
    pump_locked(state, &mut ledger);
    Ok(())
}
fn pump_locked(state: &ServerState, ledger: &mut Ledger) {
    // Bounded per pass; reliable transport admission supplies backpressure.
    let mut blocked = HashSet::new();
    let mut deferred = VecDeque::new();
    for _ in 0..512 {
        let Some(mut out) = ledger.outgoing.pop_front() else {
            break;
        };
        ledger.outgoing_bytes -= out.len();
        if !state.transport.is_current_session(&out.session)
            || !state
                .message_subscriptions
                .get(&out.session.peer_id())
                .is_some_and(|s| s.contains(&MESSAGE_ID))
        {
            continue;
        }
        if blocked.contains(&out.session.peer_id())
            || !state.transport.queue_reliable_ordered_session(
                &out.session,
                channels::PLUGIN_RELIABLE,
                &out.wire(),
            )
        {
            blocked.insert(out.session.peer_id());
            ledger.outgoing_bytes += out.len();
            deferred.push_back(out);
        } else if let OutPayload::Chunks { data, offset } = &mut out.payload {
            let sent = data.len().min(CHUNK);
            if sent < data.len() {
                *data = data.slice(sent..);
                *offset += sent;
                ledger.outgoing_bytes += out.len();
                let index = ledger
                    .outgoing
                    .iter()
                    .position(|o| {
                        o.session.same_connection(&out.session) && o.request == out.request
                    })
                    .unwrap_or(ledger.outgoing.len());
                ledger.outgoing.insert(index, out);
            }
        }
    }
    deferred.append(&mut ledger.outgoing);
    ledger.outgoing = deferred;
}
pub(crate) async fn remove_peer(state: &ServerState, peer: PeerId) {
    let mut ledger = state.avatar_host.lock();
    let ids: Vec<_> = ledger
        .offers
        .iter()
        .filter(|(_, (_, p))| p.contains(&peer))
        .map(|(&id, _)| id)
        .collect();
    for id in ids {
        ledger.withdraw(state, peer, id, 8);
    }
    for f in ledger.fetches.values_mut() {
        f.waiters.retain(|(p, _)| *p != peer);
    }
    ledger.budgets.remove(&peer);
    ledger.outgoing.retain(|o| o.session.peer_id() != peer);
    ledger.outgoing_bytes = ledger.outgoing.iter().map(|o| o.len()).sum();
    pump_locked(state, &mut ledger);
}
pub(crate) fn spawn_pump(state: ServerState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_millis(5));
        let mut previous = None;
        loop {
            timer.tick().await;
            if state.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let limits = Limits::from_config(&state.config.read());
            let locked = state.global_state.read().avatar_hosting_locked;
            let current = (limits.range, limits.cache, locked);
            if previous != Some(current) {
                previous = Some(current);
                let owners: HashSet<_> = state
                    .avatar_host
                    .lock()
                    .offers
                    .values()
                    .flat_map(|(_, p)| p.iter().copied())
                    .collect();
                for owner in owners {
                    if limits.range == 0
                        || (state.global_state.read().avatar_hosting_locked
                            && !peer_has_permission(
                                &state,
                                owner,
                                basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
                            ))
                    {
                        let mut ledger = state.avatar_host.lock();
                        let ids: Vec<_> = ledger
                            .offers
                            .iter()
                            .filter(|(_, (_, p))| p.contains(&owner))
                            .map(|(&id, _)| id)
                            .collect();
                        for id in ids {
                            ledger.withdraw(&state, owner, id, 1);
                        }
                    }
                }
            }
            let mut ledger = state.avatar_host.lock();
            ledger.trim(limits.cache);
            pump_locked(&state, &mut ledger);
        }
    })
}
