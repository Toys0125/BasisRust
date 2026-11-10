//! Bounded, connection-bound avatar range relay for BasisVR protocol 57.
use super::*;
use crate::queue_admission::Outcome;
use basis_protocol::avatar_host::{
    chunk_packet, AvatarHostFailure as Failure, AvatarId, AvatarRange, ClientMessage,
    ServerControl, CHUNK_BYTES, MAX_FILE_BYTES, MAX_OFFERS_PER_OWNER, MAX_REQUESTS_PER_REQUESTER,
};
use std::collections::VecDeque;

pub(crate) use basis_protocol::avatar_host::MESSAGE_ID;
const MAX_NORMAL_ENTRIES: usize = 4096;
const RESERVED_CONTROLS: usize = 256;
const SEND_BUDGET: usize = 512;

pub(crate) trait Connection: Clone {
    fn peer_id(&self) -> PeerId;
    fn same_connection(&self, other: &Self) -> bool;
}
impl Connection for PeerSession {
    fn peer_id(&self) -> PeerId {
        self.peer_id()
    }
    fn same_connection(&self, other: &Self) -> bool {
        self.same_connection(other)
    }
}
/// The ledger needs connection identity, time, and nonblocking admission, not the server runtime.
trait HostIo<C: Connection> {
    fn now(&self) -> Instant;
    fn available(&self, connection: &C) -> bool;
    fn admit(&mut self, connection: &C, packet: &[u8]) -> Outcome;
}
struct ServerIo<'a>(&'a ServerState);
impl HostIo<PeerSession> for ServerIo<'_> {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn available(&self, session: &PeerSession) -> bool {
        self.0.transport.is_current_session(session)
            && self
                .0
                .message_subscriptions
                .get(&session.peer_id())
                .is_some_and(|s| s.contains(&MESSAGE_ID))
    }
    fn admit(&mut self, session: &PeerSession, packet: &[u8]) -> Outcome {
        if !self.available(session) {
            return Outcome::RecipientGone;
        }
        crate::queue_admission::reliable(
            &self.0.transport,
            session,
            channels::PLUGIN_RELIABLE,
            packet,
        )
    }
}
struct HostedAvatar<C> {
    length_bytes: i64,
    owners: Vec<C>,
}
struct WaitingRequest<C> {
    connection: C,
    request_id: u32,
}
impl<C: Clone> Clone for WaitingRequest<C> {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            request_id: self.request_id,
        }
    }
}
struct Fetch<C> {
    range: AvatarRange,
    owner: C,
    data: Vec<u8>,
    waiters: Vec<WaitingRequest<C>>,
}
struct ChunkPart {
    offset: usize,
    data: Bytes,
}
struct RequestStream {
    request_id: u32,
    parts: VecDeque<ChunkPart>,
}
struct PeerQueue<C> {
    connection: C,
    /// Failure/offer-result responses have reserved capacity and priority over bulk data.
    responses: VecDeque<ServerControl>,
    commands: VecDeque<ServerControl>,
    streams: VecDeque<RequestStream>,
}
impl<C> PeerQueue<C> {
    fn new(connection: C) -> Self {
        Self {
            connection,
            responses: VecDeque::new(),
            commands: VecDeque::new(),
            streams: VecDeque::new(),
        }
    }
    fn is_empty(&self) -> bool {
        self.responses.is_empty() && self.commands.is_empty() && self.streams.is_empty()
    }
}
struct RequestBudget<C> {
    connection: C,
    tokens: f64,
    updated: Instant,
}
pub(crate) struct Ledger<C = PeerSession> {
    offers: HashMap<AvatarId, HostedAvatar<C>>,
    fetches: HashMap<u32, Fetch<C>>,
    cache: VecDeque<(AvatarRange, Bytes)>,
    cached_bytes: usize,
    next_fetch_id: u32,
    budgets: HashMap<PeerId, RequestBudget<C>>,
    outgoing: VecDeque<PeerQueue<C>>,
    normal_entries: usize,
    reserved_controls: usize,
    outgoing_bytes: usize,
    disconnects: Vec<C>,
}
impl<C> Default for Ledger<C> {
    fn default() -> Self {
        Self {
            offers: HashMap::new(),
            fetches: HashMap::new(),
            cache: VecDeque::new(),
            cached_bytes: 0,
            next_fetch_id: 0,
            budgets: HashMap::new(),
            outgoing: VecDeque::new(),
            normal_entries: 0,
            reserved_controls: 0,
            outgoing_bytes: 0,
            disconnects: Vec::new(),
        }
    }
}
#[derive(Clone, Copy)]
struct Limits {
    max_range_bytes: usize,
    cache_limit_bytes: usize,
    in_flight_limit_bytes: usize,
}
impl Limits {
    fn from_config(config: &ServerConfig) -> Self {
        let max_range_bytes =
            (config.avatar_host_max_megabytes.max(0) as usize).saturating_mul(1024 * 1024);
        let cache_limit_bytes =
            (config.avatar_host_cache_megabytes.max(0) as usize).saturating_mul(1024 * 1024);
        Self {
            max_range_bytes,
            cache_limit_bytes,
            in_flight_limit_bytes: cache_limit_bytes.max(max_range_bytes.saturating_mul(2)),
        }
    }
}
impl<C: Connection> Ledger<C> {
    fn queue_for(&mut self, connection: &C) -> &mut PeerQueue<C> {
        let index = self
            .outgoing
            .iter()
            .position(|q| q.connection.same_connection(connection));
        if let Some(index) = index {
            return self.outgoing.get_mut(index).expect("queue index");
        }
        self.outgoing.push_back(PeerQueue::new(connection.clone()));
        self.outgoing.back_mut().expect("inserted queue")
    }
    fn queue_control(
        &mut self,
        io: &impl HostIo<C>,
        connection: &C,
        message: ServerControl,
    ) -> Outcome {
        if !io.available(connection) {
            return Outcome::RecipientGone;
        }
        let critical = !matches!(message, ServerControl::Fetch { .. });
        if critical {
            // Only the latest response for the same request/offer is useful.
            let queue = self.queue_for(connection);
            let existing = queue
                .responses
                .iter_mut()
                .find(|old| match (&**old, &message) {
                    (
                        ServerControl::Failed { request_id: a, .. },
                        ServerControl::Failed { request_id: b, .. },
                    ) => a == b,
                    (
                        ServerControl::OfferResult { avatar_id: a, .. },
                        ServerControl::OfferResult { avatar_id: b, .. },
                    ) => a == b,
                    _ => false,
                });
            if let Some(existing) = existing {
                *existing = message;
                return Outcome::Queued;
            }
            if self.reserved_controls >= RESERVED_CONTROLS {
                // An unbounded failure flood cannot be retained. Close the connection explicitly
                // instead of pretending to have retained its failure response.
                if !self
                    .disconnects
                    .iter()
                    .any(|c| c.same_connection(connection))
                {
                    self.disconnects.push(connection.clone());
                }
                return Outcome::Backpressured;
            }
            self.reserved_controls += 1;
            self.queue_for(connection).responses.push_back(message);
        } else {
            if self.normal_entries >= MAX_NORMAL_ENTRIES {
                return Outcome::Backpressured;
            }
            self.normal_entries += 1;
            self.queue_for(connection).commands.push_back(message);
        }
        Outcome::Queued
    }
    fn cancel_request(&mut self, connection: &C, request_id: u32) {
        for fetch in self.fetches.values_mut() {
            fetch.waiters.retain(|w| {
                !w.connection.same_connection(connection) || w.request_id != request_id
            });
        }
        self.fetches.retain(|_, f| !f.waiters.is_empty());
        for queue in &mut self.outgoing {
            if queue.connection.same_connection(connection) {
                queue.streams.retain(|stream| {
                    if stream.request_id == request_id {
                        self.normal_entries -= stream.parts.len();
                        self.outgoing_bytes -=
                            stream.parts.iter().map(|p| p.data.len()).sum::<usize>();
                        false
                    } else {
                        true
                    }
                });
            }
        }
    }
    fn failed(&mut self, io: &impl HostIo<C>, connection: &C, request_id: u32, reason: Failure) {
        self.cancel_request(connection, request_id);
        match self.queue_control(io, connection, ServerControl::Failed { request_id, reason }) {
            Outcome::Queued | Outcome::RecipientGone => {}
            Outcome::Backpressured => { /* Reserved-control exhaustion schedules an explicit disconnect. */
            }
        }
    }
    fn queue_chunks(
        &mut self,
        io: &impl HostIo<C>,
        connection: &C,
        request_id: u32,
        offset: usize,
        data: Bytes,
        limits: Limits,
    ) -> Outcome {
        if !io.available(connection) {
            return Outcome::RecipientGone;
        }
        if data.is_empty() {
            return Outcome::Queued;
        }
        if self.normal_entries >= MAX_NORMAL_ENTRIES
            || data.len()
                > limits
                    .in_flight_limit_bytes
                    .saturating_sub(self.outgoing_bytes)
        {
            return Outcome::Backpressured;
        }
        self.normal_entries += 1;
        self.outgoing_bytes += data.len();
        let queue = self.queue_for(connection);
        let part = ChunkPart { offset, data };
        if let Some(stream) = queue
            .streams
            .iter_mut()
            .find(|s| s.request_id == request_id)
        {
            stream.parts.push_back(part);
        } else {
            queue.streams.push_back(RequestStream {
                request_id,
                parts: VecDeque::from([part]),
            });
        }
        Outcome::Queued
    }
    fn trim(&mut self, keep_bytes: usize) {
        while self.cached_bytes > keep_bytes {
            if let Some((_, data)) = self.cache.pop_front() {
                self.cached_bytes -= data.len();
            } else {
                break;
            }
        }
    }
    fn fail_fetch(&mut self, io: &impl HostIo<C>, fetch_id: u32, reason: Failure) {
        if let Some(fetch) = self.fetches.remove(&fetch_id) {
            for waiter in fetch.waiters {
                self.failed(io, &waiter.connection, waiter.request_id, reason);
            }
        }
    }
    fn withdraw(&mut self, io: &impl HostIo<C>, owner: &C, avatar_id: AvatarId, reason: Failure) {
        let Some(avatar) = self.offers.get_mut(&avatar_id) else {
            return;
        };
        if !avatar.owners.iter().any(|c| c.same_connection(owner)) {
            return;
        }
        avatar.owners.retain(|c| !c.same_connection(owner));
        if avatar.owners.is_empty() {
            self.offers.remove(&avatar_id);
            self.cache.retain(|(range, _)| range.avatar_id != avatar_id);
            self.cached_bytes = self.cache.iter().map(|(_, d)| d.len()).sum();
        }
        let failed: Vec<_> = self
            .fetches
            .iter()
            .filter(|(_, f)| f.owner.same_connection(owner) && f.range.avatar_id == avatar_id)
            .map(|(&id, _)| id)
            .collect();
        for fetch_id in failed {
            self.fail_fetch(io, fetch_id, reason);
        }
    }
    fn offer(
        &mut self,
        io: &impl HostIo<C>,
        connection: &C,
        avatar_id: AvatarId,
        length_bytes: i64,
        enabled: bool,
    ) {
        let already_owned = self
            .offers
            .get(&avatar_id)
            .is_some_and(|avatar| avatar.owners.iter().any(|c| c.same_connection(connection)));
        let reason = if !enabled {
            Failure::Disabled
        } else if length_bytes <= 0 || length_bytes > MAX_FILE_BYTES {
            Failure::TooLarge
        } else if self
            .offers
            .get(&avatar_id)
            .is_some_and(|avatar| avatar.length_bytes != length_bytes)
        {
            Failure::BadRange
        } else if !already_owned
            && self
                .offers
                .values()
                .filter(|avatar| avatar.owners.iter().any(|c| c.same_connection(connection)))
                .count()
                >= MAX_OFFERS_PER_OWNER
        {
            Failure::TooManyOffers
        } else {
            Failure::None
        };
        // Retain acknowledgement before committing an offer transition.
        if self.queue_control(
            io,
            connection,
            ServerControl::OfferResult {
                avatar_id,
                accepted: reason == Failure::None,
                reason,
            },
        ) != Outcome::Queued
        {
            return;
        }
        if reason == Failure::None && !already_owned {
            self.offers
                .entry(avatar_id)
                .or_insert(HostedAvatar {
                    length_bytes,
                    owners: Vec::new(),
                })
                .owners
                .push(connection.clone());
        }
    }
    fn request(
        &mut self,
        io: &impl HostIo<C>,
        connection: &C,
        request_id: u32,
        range: AvatarRange,
        limits: Limits,
    ) {
        let Some(avatar) = self.offers.get(&range.avatar_id) else {
            self.failed(io, connection, request_id, Failure::UnknownId);
            return;
        };
        let Some(owner) = avatar.owners.iter().find(|c| io.available(c)).cloned() else {
            self.failed(io, connection, request_id, Failure::OwnerLeft);
            return;
        };
        let reason = if range.start_offset < 0
            || range.length_bytes == 0
            || range.start_offset
                > avatar
                    .length_bytes
                    .saturating_sub(range.length_bytes as i64)
        {
            Failure::BadRange
        } else if range.length_bytes > limits.max_range_bytes {
            Failure::TooLarge
        } else {
            Failure::None
        };
        if reason != Failure::None {
            self.failed(io, connection, request_id, reason);
            return;
        }
        let now = io.now();
        let ceiling = limits.max_range_bytes.saturating_mul(2) as f64;
        let budget = self
            .budgets
            .entry(connection.peer_id())
            .or_insert_with(|| RequestBudget {
                connection: connection.clone(),
                tokens: ceiling,
                updated: now,
            });
        if !budget.connection.same_connection(connection) {
            *budget = RequestBudget {
                connection: connection.clone(),
                tokens: ceiling,
                updated: now,
            };
        }
        budget.tokens = (budget.tokens
            + now.duration_since(budget.updated).as_secs_f64() * ceiling / 60.)
            .min(ceiling);
        budget.updated = now;
        if budget.tokens < range.length_bytes as f64 {
            self.failed(io, connection, request_id, Failure::Busy);
            return;
        }
        budget.tokens -= range.length_bytes as f64;
        if let Some(index) = self.cache.iter().position(|(cached, _)| *cached == range) {
            let entry = self.cache.remove(index).expect("cache index");
            let outcome = self.queue_chunks(io, connection, request_id, 0, entry.1.clone(), limits);
            self.cache.push_back(entry);
            if outcome == Outcome::Backpressured {
                self.failed(io, connection, request_id, Failure::Busy);
            }
            return;
        }
        if self
            .fetches
            .values()
            .flat_map(|f| &f.waiters)
            .filter(|w| w.connection.same_connection(connection))
            .count()
            >= MAX_REQUESTS_PER_REQUESTER
        {
            self.failed(io, connection, request_id, Failure::Busy);
            return;
        }
        if let Some(fetch_id) = self
            .fetches
            .iter()
            .find(|(_, f)| f.range == range)
            .map(|(&id, _)| id)
        {
            let prefix = Bytes::copy_from_slice(&self.fetches[&fetch_id].data);
            match self.queue_chunks(io, connection, request_id, 0, prefix, limits) {
                Outcome::Queued => self
                    .fetches
                    .get_mut(&fetch_id)
                    .expect("existing fetch")
                    .waiters
                    .push(WaitingRequest {
                        connection: connection.clone(),
                        request_id,
                    }),
                Outcome::Backpressured => self.failed(io, connection, request_id, Failure::Busy),
                Outcome::RecipientGone => {}
            }
            return;
        }
        let in_flight_bytes: usize = self.fetches.values().map(|f| f.range.length_bytes).sum();
        if range.length_bytes > limits.in_flight_limit_bytes.saturating_sub(in_flight_bytes) {
            self.failed(io, connection, request_id, Failure::Busy);
            return;
        }
        if owner.same_connection(connection) {
            self.failed(io, connection, request_id, Failure::BadRange);
            return;
        }
        let mut data = Vec::new();
        if data.try_reserve_exact(range.length_bytes).is_err() {
            self.failed(io, connection, request_id, Failure::Busy);
            return;
        }
        self.next_fetch_id = self.next_fetch_id.wrapping_add(1);
        while self.fetches.contains_key(&self.next_fetch_id) {
            self.next_fetch_id = self.next_fetch_id.wrapping_add(1);
        }
        let fetch_id = self.next_fetch_id;
        // Queue admission is fallible. A fetch becomes pending only if its owner command
        // is retained in the ledger; failure cannot charge memory or attract later waiters.
        match self.queue_control(io, &owner, ServerControl::Fetch { fetch_id, range }) {
            Outcome::Queued => {
                self.fetches.insert(
                    fetch_id,
                    Fetch {
                        range,
                        owner,
                        data,
                        waiters: vec![WaitingRequest {
                            connection: connection.clone(),
                            request_id,
                        }],
                    },
                );
            }
            Outcome::Backpressured => self.failed(io, connection, request_id, Failure::Busy),
            Outcome::RecipientGone => self.failed(io, connection, request_id, Failure::OwnerLeft),
        }
    }
    fn owner_chunk(
        &mut self,
        io: &impl HostIo<C>,
        connection: &C,
        fetch_id: u32,
        offset: i32,
        data: &[u8],
        limits: Limits,
    ) {
        let Some(fetch) = self.fetches.get_mut(&fetch_id) else {
            return;
        };
        if !fetch.owner.same_connection(connection) {
            return;
        }
        if offset < 0
            || offset as usize != fetch.data.len()
            || data.is_empty()
            || data.len() > CHUNK_BYTES
            || data.len() > fetch.range.length_bytes - fetch.data.len()
        {
            self.fail_fetch(io, fetch_id, Failure::OwnerFailed);
            return;
        }
        fetch.data.extend_from_slice(data);
        let waiters = fetch.waiters.clone();
        let done = fetch.data.len() == fetch.range.length_bytes;
        let outgoing = Bytes::copy_from_slice(data);
        for waiter in waiters {
            match self.queue_chunks(
                io,
                &waiter.connection,
                waiter.request_id,
                offset as usize,
                outgoing.clone(),
                limits,
            ) {
                Outcome::Queued => {}
                Outcome::Backpressured => {
                    self.failed(io, &waiter.connection, waiter.request_id, Failure::Busy)
                }
                Outcome::RecipientGone => {
                    self.cancel_request(&waiter.connection, waiter.request_id)
                }
            }
        }
        if done {
            if let Some(fetch) = self.fetches.remove(&fetch_id) {
                if fetch.data.len() <= limits.cache_limit_bytes
                    && self.offers.contains_key(&fetch.range.avatar_id)
                {
                    self.trim(limits.cache_limit_bytes - fetch.data.len());
                    self.cached_bytes += fetch.data.len();
                    self.cache.push_back((fetch.range, Bytes::from(fetch.data)));
                }
            }
        }
    }
    fn remove_session(&mut self, io: &impl HostIo<C>, connection: &C) {
        let avatars: Vec<_> = self
            .offers
            .iter()
            .filter(|(_, avatar)| avatar.owners.iter().any(|c| c.same_connection(connection)))
            .map(|(&id, _)| id)
            .collect();
        for id in avatars {
            self.withdraw(io, connection, id, Failure::OwnerLeft);
        }
        for fetch in self.fetches.values_mut() {
            fetch
                .waiters
                .retain(|w| !w.connection.same_connection(connection));
        }
        self.fetches.retain(|_, f| !f.waiters.is_empty());
        if self
            .budgets
            .get(&connection.peer_id())
            .is_some_and(|b| b.connection.same_connection(connection))
        {
            self.budgets.remove(&connection.peer_id());
        }
        self.outgoing
            .retain(|queue| !queue.connection.same_connection(connection));
        self.recount();
    }
    fn recount(&mut self) {
        self.normal_entries = self
            .outgoing
            .iter()
            .map(|q| q.commands.len() + q.streams.iter().map(|s| s.parts.len()).sum::<usize>())
            .sum();
        self.reserved_controls = self.outgoing.iter().map(|q| q.responses.len()).sum();
        self.outgoing_bytes = self
            .outgoing
            .iter()
            .flat_map(|q| &q.streams)
            .flat_map(|s| &s.parts)
            .map(|p| p.data.len())
            .sum();
    }
    fn command_current(&self, connection: &C, command: &ServerControl) -> bool {
        match command {
            ServerControl::Fetch { fetch_id, .. } => self
                .fetches
                .get(fetch_id)
                .is_some_and(|f| f.owner.same_connection(connection)),
            _ => true,
        }
    }
    fn pump(&mut self, io: &mut impl HostIo<C>) {
        let mut blocked = HashSet::new();
        // Moving each visited peer to the tail guarantees progress on the next pass even
        // when more than SEND_BUDGET peers are backpressured. Bulk pieces of a request
        // stay together behind its cursor; control responses have separate capacity.
        for _ in 0..SEND_BUDGET {
            let Some(mut queue) = self.outgoing.pop_front() else {
                break;
            };
            if !io.available(&queue.connection) {
                self.normal_entries -= queue.commands.len()
                    + queue.streams.iter().map(|s| s.parts.len()).sum::<usize>();
                self.reserved_controls -= queue.responses.len();
                self.outgoing_bytes -= queue
                    .streams
                    .iter()
                    .flat_map(|s| &s.parts)
                    .map(|p| p.data.len())
                    .sum::<usize>();
                continue;
            }
            while queue
                .commands
                .front()
                .is_some_and(|command| !self.command_current(&queue.connection, command))
            {
                queue.commands.pop_front();
                self.normal_entries -= 1;
            }
            if queue.is_empty() {
                continue;
            }
            if blocked.contains(&queue.connection.peer_id()) {
                self.outgoing.push_back(queue);
                continue;
            }
            let control = queue
                .responses
                .front()
                .copied()
                .or_else(|| queue.commands.front().copied());
            let packet = if let Some(control) = control {
                control.packet()
            } else {
                let stream = queue.streams.front().expect("nonempty queue");
                let part = stream.parts.front().expect("stream part");
                chunk_packet(
                    stream.request_id,
                    part.offset,
                    &part.data[..part.data.len().min(CHUNK_BYTES)],
                )
            };
            match io.admit(&queue.connection, &packet) {
                Outcome::Queued => {
                    if queue.responses.pop_front().is_some() {
                        self.reserved_controls -= 1;
                    } else if queue.commands.pop_front().is_some() {
                        self.normal_entries -= 1;
                    } else {
                        let mut stream = queue.streams.pop_front().expect("bulk stream");
                        let part = stream.parts.front_mut().expect("bulk part");
                        let sent = part.data.len().min(CHUNK_BYTES);
                        self.outgoing_bytes -= sent;
                        if sent == part.data.len() {
                            stream.parts.pop_front();
                            self.normal_entries -= 1;
                        } else {
                            part.data = part.data.slice(sent..);
                            part.offset += sent;
                        }
                        if !stream.parts.is_empty() {
                            queue.streams.push_back(stream);
                        }
                    }
                }
                Outcome::Backpressured => {
                    blocked.insert(queue.connection.peer_id());
                }
                Outcome::RecipientGone => {
                    self.normal_entries -= queue.commands.len()
                        + queue.streams.iter().map(|s| s.parts.len()).sum::<usize>();
                    self.reserved_controls -= queue.responses.len();
                    self.outgoing_bytes -= queue
                        .streams
                        .iter()
                        .flat_map(|s| &s.parts)
                        .map(|p| p.data.len())
                        .sum::<usize>();
                    continue;
                }
            }
            if !queue.is_empty() {
                self.outgoing.push_back(queue);
            }
        }
    }
}

fn current_connection(state: &ServerState, peer: PeerId) -> Option<PeerSession> {
    state
        .authenticated_peers
        .get(&peer)
        .and_then(|p| p.session.clone())
        .filter(|s| state.transport.is_current_session(s))
}
pub(crate) async fn handle(state: &ServerState, peer: PeerId, payload: &[u8]) -> Result<()> {
    let Some(connection) = current_connection(state, peer) else {
        return Ok(());
    };
    let config = state.config.read().clone();
    let limits = Limits::from_config(&config);
    let enabled = limits.max_range_bytes > 0
        && (!state.global_state.read().avatar_hosting_locked
            || peer_has_permission(
                state,
                peer,
                basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
            ));
    let Some(message) = ClientMessage::read(&mut NetReader::new(payload))? else {
        return Ok(());
    };
    let disconnects = {
        let io = ServerIo(state);
        let mut ledger = state.avatar_host.lock();
        match message {
            ClientMessage::Offer {
                avatar_id,
                length_bytes,
            } => ledger.offer(&io, &connection, avatar_id, length_bytes, enabled),
            ClientMessage::Withdraw { avatar_id } => {
                ledger.withdraw(&io, &connection, avatar_id, Failure::OwnerLeft)
            }
            ClientMessage::Request { request_id, range } => {
                ledger.request(&io, &connection, request_id, range, limits)
            }
            ClientMessage::OwnerChunk {
                fetch_id,
                offset,
                data,
            } => ledger.owner_chunk(&io, &connection, fetch_id, offset, data, limits),
            ClientMessage::OwnerFailed { fetch_id } => {
                if ledger
                    .fetches
                    .get(&fetch_id)
                    .is_some_and(|f| f.owner.same_connection(&connection))
                {
                    ledger.fail_fetch(&io, fetch_id, Failure::OwnerFailed);
                }
            }
        }
        ledger.pump(&mut ServerIo(state));
        std::mem::take(&mut ledger.disconnects)
    };
    for connection in disconnects {
        request_disconnect(state, &connection, "Avatar hosting control queue exhausted").await?;
    }
    Ok(())
}
pub(crate) async fn remove_peer(state: &ServerState, peer: PeerId) {
    let connection = state
        .authenticated_peers
        .get(&peer)
        .and_then(|p| p.session.clone());
    if let Some(connection) = connection {
        let mut ledger = state.avatar_host.lock();
        ledger.remove_session(&ServerIo(state), &connection);
        ledger.pump(&mut ServerIo(state));
    }
}
pub(crate) fn spawn_pump(state: ServerState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_millis(5));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            if state.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let config = state.config.read().clone();
            let limits = Limits::from_config(&config);
            let locked = state.global_state.read().avatar_hosting_locked;
            let owners: Vec<_> = state
                .avatar_host
                .lock()
                .offers
                .values()
                .flat_map(|avatar| avatar.owners.iter().cloned())
                .collect();
            for owner in owners {
                if limits.max_range_bytes == 0
                    || (locked
                        && !peer_has_permission(
                            &state,
                            owner.peer_id(),
                            basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
                        ))
                    || !state.transport.is_current_session(&owner)
                {
                    let mut ledger = state.avatar_host.lock();
                    let avatars: Vec<_> = ledger
                        .offers
                        .iter()
                        .filter(|(_, avatar)| {
                            avatar.owners.iter().any(|c| c.same_connection(&owner))
                        })
                        .map(|(&id, _)| id)
                        .collect();
                    for avatar_id in avatars {
                        ledger.withdraw(&ServerIo(&state), &owner, avatar_id, Failure::Disabled);
                    }
                }
            }
            let disconnects = {
                let mut ledger = state.avatar_host.lock();
                ledger.trim(limits.cache_limit_bytes);
                ledger.pump(&mut ServerIo(&state));
                std::mem::take(&mut ledger.disconnects)
            };
            for connection in disconnects {
                if let Err(error) = request_disconnect(
                    &state,
                    &connection,
                    "Avatar hosting control queue exhausted",
                )
                .await
                {
                    warn!("failed to close overloaded avatar recipient: {error}");
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use basis_protocol::avatar_host::ServerMessage;
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct FakeConnection {
        peer: PeerId,
        generation: u32,
    }
    impl Connection for FakeConnection {
        fn peer_id(&self) -> PeerId {
            self.peer
        }
        fn same_connection(&self, other: &Self) -> bool {
            self == other
        }
    }
    struct FakeIo {
        now: Instant,
        connections: HashMap<PeerId, FakeConnection>,
        blocked: HashSet<PeerId>,
        sent: Vec<(FakeConnection, Vec<u8>)>,
    }
    impl FakeIo {
        fn new() -> Self {
            Self {
                now: Instant::now(),
                connections: HashMap::new(),
                blocked: HashSet::new(),
                sent: Vec::new(),
            }
        }
        fn connect(&mut self, peer: PeerId, generation: u32) -> FakeConnection {
            let c = FakeConnection { peer, generation };
            self.connections.insert(peer, c);
            c
        }
        fn controls(&self, peer: PeerId) -> Vec<ServerControl> {
            self.sent
                .iter()
                .filter(|(c, _)| c.peer == peer)
                .filter_map(|(_, packet)| {
                    let mut r = NetReader::new(packet);
                    assert_eq!(r.get_u16().unwrap(), MESSAGE_ID);
                    match ServerMessage::read(&mut r).unwrap().unwrap() {
                        ServerMessage::Control(c) => Some(c),
                        _ => None,
                    }
                })
                .collect()
        }
    }
    impl HostIo<FakeConnection> for FakeIo {
        fn now(&self) -> Instant {
            self.now
        }
        fn available(&self, c: &FakeConnection) -> bool {
            self.connections.get(&c.peer) == Some(c)
        }
        fn admit(&mut self, c: &FakeConnection, p: &[u8]) -> Outcome {
            if !self.available(c) {
                Outcome::RecipientGone
            } else if self.blocked.contains(&c.peer) {
                Outcome::Backpressured
            } else {
                self.sent.push((*c, p.to_vec()));
                Outcome::Queued
            }
        }
    }
    fn limits() -> Limits {
        Limits {
            max_range_bytes: 64 * 1024 * 1024,
            cache_limit_bytes: 64 * 1024 * 1024,
            in_flight_limit_bytes: 128 * 1024 * 1024,
        }
    }
    fn range() -> AvatarRange {
        AvatarRange {
            avatar_id: AvatarId([7; 16]),
            start_offset: 0,
            length_bytes: 8,
        }
    }
    fn offered(io: &mut FakeIo, ledger: &mut Ledger<FakeConnection>, owner: &FakeConnection) {
        ledger.offer(io, owner, range().avatar_id, 100, true);
        ledger.pump(io);
        io.sent.clear();
    }

    #[test]
    fn blocked_avatar_parts_do_not_starve_unrelated_controls() {
        let mut io = FakeIo::new();
        let blocked = io.connect(1, 1);
        let healthy = io.connect(2, 1);
        io.blocked.insert(1);
        let mut ledger = Ledger::default();
        for index in 0..512 {
            assert_eq!(
                ledger.queue_chunks(
                    &io,
                    &blocked,
                    9,
                    index * CHUNK_BYTES,
                    Bytes::from(vec![1; CHUNK_BYTES]),
                    limits()
                ),
                Outcome::Queued
            );
        }
        assert_eq!(
            ledger.queue_control(
                &io,
                &healthy,
                ServerControl::Failed {
                    request_id: 17,
                    reason: Failure::Busy
                }
            ),
            Outcome::Queued
        );
        ledger.pump(&mut io);
        assert_eq!(
            io.controls(2),
            vec![ServerControl::Failed {
                request_id: 17,
                reason: Failure::Busy
            }]
        );
        assert_eq!(ledger.normal_entries, 512);
        assert_eq!(ledger.outgoing_bytes, 512 * CHUNK_BYTES);
    }
    #[test]
    fn round_robin_moves_past_more_than_a_pass_of_blocked_peers() {
        let mut io = FakeIo::new();
        let mut ledger = Ledger::default();
        for peer in 0..512 {
            let c = io.connect(peer, 1);
            io.blocked.insert(peer);
            assert_eq!(
                ledger.queue_chunks(&io, &c, 1, 0, Bytes::from_static(b"x"), limits()),
                Outcome::Queued
            );
        }
        let healthy = io.connect(512, 1);
        assert_eq!(
            ledger.queue_control(
                &io,
                &healthy,
                ServerControl::Failed {
                    request_id: 7,
                    reason: Failure::Busy
                }
            ),
            Outcome::Queued
        );
        ledger.pump(&mut io);
        assert!(io.sent.is_empty());
        ledger.pump(&mut io);
        assert_eq!(io.controls(512).len(), 1);
    }
    #[test]
    fn full_avatar_queue_rolls_back_fetch_and_retains_busy_response() {
        let mut io = FakeIo::new();
        let owner = io.connect(1, 1);
        let viewer = io.connect(2, 1);
        let mut ledger = Ledger::default();
        offered(&mut io, &mut ledger, &owner);
        io.blocked.insert(1);
        for index in 0..MAX_NORMAL_ENTRIES {
            assert_eq!(
                ledger.queue_chunks(&io, &owner, 9, index, Bytes::from_static(b"x"), limits()),
                Outcome::Queued
            );
        }
        ledger.request(&io, &viewer, 17, range(), limits());
        assert!(
            ledger.fetches.is_empty(),
            "an unretained Fetch cannot become pending"
        );
        assert_eq!(ledger.normal_entries, MAX_NORMAL_ENTRIES);
        ledger.pump(&mut io);
        assert_eq!(
            io.controls(2),
            vec![ServerControl::Failed {
                request_id: 17,
                reason: Failure::Busy
            }]
        );
        ledger.request(&io, &viewer, 18, range(), limits());
        assert!(ledger.fetches.is_empty());
        io.blocked.clear();
        for _ in 0..10 {
            ledger.pump(&mut io);
        }
        ledger.request(&io, &viewer, 19, range(), limits());
        assert_eq!(ledger.fetches.len(), 1);
        ledger.pump(&mut io);
        assert!(io
            .controls(1)
            .iter()
            .any(|c| matches!(c, ServerControl::Fetch { .. })));
    }
    #[test]
    fn reserved_failure_exhaustion_explicitly_closes_connection() {
        let mut io = FakeIo::new();
        let c = io.connect(1, 1);
        let mut ledger = Ledger::default();
        for request_id in 0..RESERVED_CONTROLS as u32 {
            ledger.failed(&io, &c, request_id, Failure::Busy);
        }
        ledger.failed(&io, &c, RESERVED_CONTROLS as u32, Failure::Busy);
        assert_eq!(ledger.reserved_controls, RESERVED_CONTROLS);
        assert_eq!(ledger.disconnects, vec![c]);
    }
    #[test]
    fn avatar_control_priority_preserves_bulk_offsets_and_order() {
        let mut io = FakeIo::new();
        let c = io.connect(1, 1);
        let mut ledger = Ledger::default();
        let data = Bytes::from(vec![3; CHUNK_BYTES * 2 + 1]);
        assert_eq!(
            ledger.queue_chunks(&io, &c, 1, 0, data.clone(), limits()),
            Outcome::Queued
        );
        assert_eq!(
            ledger.queue_chunks(
                &io,
                &c,
                1,
                data.len(),
                Bytes::from_static(b"tail"),
                limits()
            ),
            Outcome::Queued
        );
        ledger.failed(&io, &c, 2, Failure::Busy);
        ledger.pump(&mut io);
        assert!(matches!(
            ServerMessage::read(&mut NetReader::new(&io.sent[0].1[2..])).unwrap(),
            Some(ServerMessage::Control(ServerControl::Failed {
                request_id: 2,
                ..
            }))
        ));
        let mut bytes = Vec::new();
        for (_, packet) in &io.sent[1..] {
            match ServerMessage::read(&mut NetReader::new(&packet[2..]))
                .unwrap()
                .unwrap()
            {
                ServerMessage::Chunk {
                    request_id,
                    offset,
                    data,
                } => {
                    assert_eq!(request_id, 1);
                    assert_eq!(offset as usize, bytes.len());
                    bytes.extend_from_slice(data);
                }
                _ => panic!("unexpected control"),
            }
        }
        assert_eq!(&bytes[..data.len()], data.as_ref());
        assert_eq!(&bytes[data.len()..], b"tail");
        assert_eq!(ledger.normal_entries, 0);
        assert_eq!(ledger.outgoing_bytes, 0);
    }
    #[test]
    fn multipart_fetch_does_not_follow_recycled_requester_or_owner_ids() {
        let mut io = FakeIo::new();
        let owner = io.connect(1, 1);
        let viewer = io.connect(2, 1);
        let mut ledger = Ledger::default();
        offered(&mut io, &mut ledger, &owner);
        ledger.request(&io, &viewer, 17, range(), limits());
        let fetch_id = *ledger.fetches.keys().next().unwrap();
        ledger.pump(&mut io);
        io.sent.clear();
        let replacement_owner = io.connect(1, 2);
        ledger.owner_chunk(&io, &replacement_owner, fetch_id, 0, b"spoofed!", limits());
        assert!(ledger.fetches[&fetch_id].data.is_empty());
        io.connections.insert(1, owner);
        let replacement_viewer = io.connect(2, 2);
        ledger.owner_chunk(&io, &owner, fetch_id, 0, b"avatar!!", limits());
        ledger.pump(&mut io);
        assert!(io.sent.iter().all(|(c, _)| *c != replacement_viewer));
        assert!(ledger.fetches.is_empty());
    }
    #[test]
    fn withdraw_cancels_owner_command_and_releases_pending_memory() {
        let mut io = FakeIo::new();
        let owner = io.connect(1, 1);
        let viewer = io.connect(2, 1);
        let mut ledger = Ledger::default();
        offered(&mut io, &mut ledger, &owner);
        ledger.request(&io, &viewer, 17, range(), limits());
        assert_eq!(ledger.fetches.len(), 1);
        ledger.withdraw(&io, &owner, range().avatar_id, Failure::OwnerLeft);
        assert!(ledger.fetches.is_empty());
        ledger.pump(&mut io);
        assert!(io.controls(1).is_empty());
        assert_eq!(
            io.controls(2),
            vec![ServerControl::Failed {
                request_id: 17,
                reason: Failure::OwnerLeft
            }]
        );
    }
}
