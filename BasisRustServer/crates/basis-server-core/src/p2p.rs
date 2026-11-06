use basis_protocol::{
    channels,
    io::{NetReader, NetWriter},
    messages::{BasisDeserialize, BasisP2PSignalMessage, BasisSerialize},
};
use basis_transport::{DeliveryMethod, PeerId, TransportHandle};
use dashmap::{mapref::entry::Entry, DashMap};
use parking_lot::Mutex;
use std::{collections::HashSet, net::SocketAddr, sync::Arc};
use tracing::{info, warn};

const MAX_SESSIONS_PER_PEER: usize = 4096;
const MAX_SESSION_TOKEN_LEN: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum SessionState {
    Awaiting,
    ReadyForPunch,
    Punched,
}

#[derive(Debug, Clone)]
struct P2pSession {
    initiator_peer_id: PeerId,
    target_peer_id: PeerId,
    state: SessionState,
    endpoint_a_internal: Option<SocketAddr>,
    endpoint_a_external: Option<SocketAddr>,
    endpoint_b_internal: Option<SocketAddr>,
    endpoint_b_external: Option<SocketAddr>,
    initiator_link_up: bool,
    target_link_up: bool,
}

#[derive(Clone, Default)]
pub struct P2pBroker {
    // Serializes multi-map lifecycle transitions. Never held across an await.
    lifecycle: Arc<Mutex<()>>,
    sessions: Arc<DashMap<String, P2pSession>>,
    peer_sessions: Arc<DashMap<PeerId, HashSet<String>>>,
    offloaded_pairs: Arc<DashMap<u64, ()>>,
}

impl P2pBroker {
    pub fn is_offloaded(&self, a: PeerId, b: PeerId) -> bool {
        if a == b {
            return false;
        }
        self.offloaded_pairs.contains_key(&pack_pair(a, b))
    }

    pub fn offloaded_pairs(&self) -> Arc<DashMap<u64, ()>> {
        self.offloaded_pairs.clone()
    }

    pub async fn handle_signal<F>(
        &self,
        transport: &TransportHandle,
        sender: PeerId,
        payload: &[u8],
        direct_connect_allowed: bool,
        is_authenticated: F,
    ) where
        F: Fn(PeerId) -> bool,
    {
        let Some((&sub, rest)) = payload.split_first() else {
            return;
        };
        let mut reader = NetReader::new(rest);
        let Ok(message) = BasisP2PSignalMessage::deserialize(&mut reader) else {
            warn!("[P2P] malformed signal from peer {sender}");
            return;
        };
        match sub {
            channels::P2P_SUB_REQUEST => {
                if !direct_connect_allowed {
                    self.send_sub(
                        transport,
                        sender,
                        channels::P2P_SUB_CANCEL,
                        &message.session_token,
                        message.other_player_id,
                    )
                    .await;
                    return;
                }
                self.handle_request(transport, sender, message, is_authenticated)
                    .await;
            }
            channels::P2P_SUB_ACCEPT => self.handle_accept(transport, sender, message).await,
            channels::P2P_SUB_DECLINE => {
                self.forward_and_drop(transport, sender, message, channels::P2P_SUB_DECLINE, true)
                    .await;
            }
            channels::P2P_SUB_CANCEL => {
                self.forward_and_drop(transport, sender, message, channels::P2P_SUB_CANCEL, true)
                    .await;
            }
            channels::P2P_SUB_LINK_LOST => {
                self.handle_link_lost(transport, sender, message).await;
            }
            channels::P2P_SUB_LINK_UP => self.handle_link_up(transport, sender, message).await,
            _ => warn!("[P2P] unknown sub-type {sub} from peer {sender}"),
        }
    }

    pub async fn handle_nat_introduction_request(
        &self,
        transport: &TransportHandle,
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        token: String,
    ) {
        if token.is_empty() {
            return;
        }
        let Some(mut session) = self.sessions.get_mut(&token) else {
            warn!("[P2P] NAT request with unknown token {}", preview(&token));
            return;
        };
        if session.state < SessionState::ReadyForPunch {
            warn!("[P2P] NAT request before session ready {}", preview(&token));
            return;
        }
        // The Unity client retries introductions on a timer. Identify participants by
        // their observed UDP endpoint so a retry cannot fill the other participant's slot.
        if session.endpoint_a_external == Some(remote_addr) {
            session.endpoint_a_internal = Some(local_addr);
        } else if session.endpoint_b_external == Some(remote_addr) {
            session.endpoint_b_internal = Some(local_addr);
        } else if session.endpoint_a_external.is_none() {
            session.endpoint_a_internal = Some(local_addr);
            session.endpoint_a_external = Some(remote_addr);
        } else if session.endpoint_b_external.is_none() {
            session.endpoint_b_internal = Some(local_addr);
            session.endpoint_b_external = Some(remote_addr);
        } else {
            // Both participants are known. A third endpoint must not replace either.
            return;
        }
        let Some(a_internal) = session.endpoint_a_internal else {
            return;
        };
        let Some(a_external) = session.endpoint_a_external else {
            return;
        };
        let Some(b_internal) = session.endpoint_b_internal else {
            return;
        };
        let Some(b_external) = session.endpoint_b_external else {
            return;
        };
        session.state = SessionState::Punched;
        drop(session);

        let same_nat = a_external.ip() == b_external.ip();
        let same_host = same_nat && a_internal.ip() == b_internal.ip();
        let (a_internal, b_internal) = if same_host {
            (
                SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), a_internal.port()),
                SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), b_internal.port()),
            )
        } else {
            (a_internal, b_internal)
        };
        // Introductions are unacknowledged UDP: retries need the full prediction window
        // too, because either peer may have missed the previous response.
        let prediction_count = if same_nat { 0 } else { 32 };
        if let Err(err) = transport
            .send_nat_introduce(
                a_internal,
                a_external,
                prediction_count,
                b_internal,
                b_external,
                prediction_count,
                &token,
            )
            .await
        {
            warn!(
                "[P2P] failed NAT introduce for {}: {err:#}",
                preview(&token)
            );
        }
    }

    pub async fn remove_peer(&self, transport: &TransportHandle, peer: PeerId) {
        let notifications = {
            let _lifecycle = self.lifecycle.lock();
            let Some((_, tokens)) = self.peer_sessions.remove(&peer) else {
                return;
            };
            let mut notifications = Vec::with_capacity(tokens.len());
            for token in tokens {
                let Some((_, session)) = self.sessions.remove(&token) else {
                    continue;
                };
                let other = if session.initiator_peer_id == peer {
                    session.target_peer_id
                } else {
                    session.initiator_peer_id
                };
                self.untrack_session(&session, &token);
                notifications.push((other, token));
            }
            notifications
        };
        for (other, token) in notifications {
            self.send_sub(transport, other, channels::P2P_SUB_CANCEL, &token, peer)
                .await;
        }
    }

    async fn handle_link_up(
        &self,
        transport: &TransportHandle,
        sender: PeerId,
        message: BasisP2PSignalMessage,
    ) {
        let token = message.session_token.clone();
        let Some(mut session) = self.sessions.get_mut(&token) else {
            return;
        };
        if sender == session.initiator_peer_id {
            session.initiator_link_up = true;
        } else if sender == session.target_peer_id {
            session.target_link_up = true;
        } else {
            return;
        }
        if session.initiator_link_up && session.target_link_up {
            let initiator = session.initiator_peer_id;
            let target = session.target_peer_id;
            let newly_offloaded = self
                .offloaded_pairs
                .insert(pack_pair(initiator, target), ())
                .is_none();
            drop(session);
            if newly_offloaded {
                info!("[P2P] offloaded pair ({initiator},{target})");
                self.send_sub(
                    transport,
                    initiator,
                    channels::P2P_SUB_OFFLOADED,
                    &token,
                    target,
                )
                .await;
                self.send_sub(
                    transport,
                    target,
                    channels::P2P_SUB_OFFLOADED,
                    &token,
                    initiator,
                )
                .await;
            }
        }
    }

    async fn handle_request<F>(
        &self,
        transport: &TransportHandle,
        sender: PeerId,
        message: BasisP2PSignalMessage,
        is_authenticated: F,
    ) where
        F: Fn(PeerId) -> bool,
    {
        if message.session_token.is_empty()
            || message.session_token.len() > MAX_SESSION_TOKEN_LEN
            || message.other_player_id == sender
        {
            return;
        }
        if !is_authenticated(message.other_player_id) {
            self.send_sub(
                transport,
                sender,
                channels::P2P_SUB_CANCEL,
                &message.session_token,
                message.other_player_id,
            )
            .await;
            return;
        }
        let session = P2pSession {
            initiator_peer_id: sender,
            target_peer_id: message.other_player_id,
            state: SessionState::Awaiting,
            endpoint_a_internal: None,
            endpoint_a_external: None,
            endpoint_b_internal: None,
            endpoint_b_external: None,
            initiator_link_up: false,
            target_link_up: false,
        };
        if !self.claim_session(&message.session_token, session, &is_authenticated) {
            // Tokens identify one live pair. Claim atomically so simultaneous
            // colliding requests cannot replace its owner or bookkeeping.
            self.send_sub(
                transport,
                sender,
                channels::P2P_SUB_CANCEL,
                &message.session_token,
                message.other_player_id,
            )
            .await;
            return;
        }
        self.send_sub_with_key(
            transport,
            message.other_player_id,
            channels::P2P_SUB_REQUEST,
            &message.session_token,
            sender,
            message.ephemeral_public_key.as_ref(),
        )
        .await;
        self.send_sub(
            transport,
            sender,
            channels::P2P_SUB_SERVER_ARMED,
            &message.session_token,
            message.other_player_id,
        )
        .await;
    }

    async fn handle_accept(
        &self,
        transport: &TransportHandle,
        sender: PeerId,
        message: BasisP2PSignalMessage,
    ) {
        let Some(mut session) = self.sessions.get_mut(&message.session_token) else {
            return;
        };
        if session.target_peer_id != sender || session.initiator_peer_id != message.other_player_id
        {
            return;
        }
        session.state = SessionState::ReadyForPunch;
        let initiator = session.initiator_peer_id;
        drop(session);
        self.send_sub_with_key(
            transport,
            initiator,
            channels::P2P_SUB_ACCEPT,
            &message.session_token,
            sender,
            message.ephemeral_public_key.as_ref(),
        )
        .await;
    }

    async fn handle_link_lost(
        &self,
        transport: &TransportHandle,
        sender: PeerId,
        message: BasisP2PSignalMessage,
    ) {
        let Some(mut session) = self.sessions.get_mut(&message.session_token) else {
            return;
        };
        if !session.authorizes(sender, message.other_player_id) {
            return;
        }
        session.endpoint_a_internal = None;
        session.endpoint_a_external = None;
        session.endpoint_b_internal = None;
        session.endpoint_b_external = None;
        session.initiator_link_up = false;
        session.target_link_up = false;
        session.state = SessionState::ReadyForPunch;
        self.offloaded_pairs.remove(&pack_pair(
            session.initiator_peer_id,
            session.target_peer_id,
        ));
        drop(session);
        self.send_sub(
            transport,
            message.other_player_id,
            channels::P2P_SUB_LINK_LOST,
            &message.session_token,
            sender,
        )
        .await;
    }

    async fn forward_and_drop(
        &self,
        transport: &TransportHandle,
        sender: PeerId,
        message: BasisP2PSignalMessage,
        sub: u8,
        drop_session: bool,
    ) {
        let target = if drop_session {
            let Some(session) = self.remove_session_if_authorized(
                &message.session_token,
                sender,
                message.other_player_id,
            ) else {
                return;
            };
            if sender == session.initiator_peer_id {
                session.target_peer_id
            } else {
                session.initiator_peer_id
            }
        } else {
            let Some(session) = self.sessions.get(&message.session_token) else {
                return;
            };
            if !session.authorizes(sender, message.other_player_id) {
                return;
            }
            message.other_player_id
        };
        self.send_sub(transport, target, sub, &message.session_token, sender)
            .await;
    }

    /// Atomically admit a live pair and publish both peer-index entries.
    fn claim_session<F>(&self, token: &str, session: P2pSession, is_authenticated: F) -> bool
    where
        F: Fn(PeerId) -> bool,
    {
        let _lifecycle = self.lifecycle.lock();
        let initiator = session.initiator_peer_id;
        let target = session.target_peer_id;
        if !is_authenticated(initiator)
            || !is_authenticated(target)
            || self
                .peer_sessions
                .get(&initiator)
                .is_some_and(|tokens| tokens.len() >= MAX_SESSIONS_PER_PEER)
        {
            return false;
        }
        match self.sessions.entry(token.to_string()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(session);
                self.track_peer_session(initiator, token);
                self.track_peer_session(target, token);
                true
            }
        }
    }

    fn remove_session_if_authorized(
        &self,
        token: &str,
        sender: PeerId,
        other: PeerId,
    ) -> Option<P2pSession> {
        let _lifecycle = self.lifecycle.lock();
        let session = self
            .sessions
            .remove_if(token, |_, session| session.authorizes(sender, other))?
            .1;
        self.untrack_session(&session, token);
        Some(session)
    }

    fn untrack_session(&self, session: &P2pSession, token: &str) {
        self.untrack_peer_session(session.initiator_peer_id, token);
        self.untrack_peer_session(session.target_peer_id, token);
        self.offloaded_pairs.remove(&pack_pair(
            session.initiator_peer_id,
            session.target_peer_id,
        ));
    }

    fn track_peer_session(&self, peer: PeerId, token: &str) {
        self.peer_sessions
            .entry(peer)
            .or_default()
            .insert(token.to_string());
    }

    fn untrack_peer_session(&self, peer: PeerId, token: &str) {
        if let Some(mut sessions) = self.peer_sessions.get_mut(&peer) {
            sessions.remove(token);
        }
    }

    async fn send_sub(
        &self,
        transport: &TransportHandle,
        to: PeerId,
        sub: u8,
        token: &str,
        other_player_id: PeerId,
    ) {
        self.send_sub_with_key(transport, to, sub, token, other_player_id, None)
            .await;
    }

    async fn send_sub_with_key(
        &self,
        transport: &TransportHandle,
        to: PeerId,
        sub: u8,
        token: &str,
        other_player_id: PeerId,
        ephemeral_public_key: Option<&[u8; 32]>,
    ) {
        let mut writer = NetWriter::new();
        writer.put_u8(sub);
        if let Err(err) = (BasisP2PSignalMessage {
            other_player_id,
            session_token: token.to_string(),
            ephemeral_public_key: ephemeral_public_key.copied(),
        })
        .serialize(&mut writer)
        {
            tracing::warn!("failed to serialize P2P signal: {err}");
            return;
        }
        let _ = transport
            .send(
                to,
                channels::P2P,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
            )
            .await;
    }
}

impl P2pSession {
    fn authorizes(&self, sender: PeerId, other: PeerId) -> bool {
        (sender == self.initiator_peer_id && other == self.target_peer_id)
            || (sender == self.target_peer_id && other == self.initiator_peer_id)
    }
}

pub fn pack_pair(a: PeerId, b: PeerId) -> u64 {
    let lo = a.min(b) as u64;
    let hi = a.max(b) as u64;
    (lo << 32) | hi
}

fn preview(token: &str) -> &str {
    token.get(..8).unwrap_or(token)
}

#[cfg(test)]
mod tests {
    use super::*;
    use basis_transport::{PacketProperty, PeerSession, ServerEvent};
    use std::time::Duration;
    use tokio::{net::UdpSocket, time::timeout};

    #[tokio::test]
    async fn introduction_retries_require_two_distinct_endpoints_and_can_resend() {
        let (transport, _events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let b = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let other = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let a_addr = a.local_addr().unwrap();
        let b_addr = b.local_addr().unwrap();
        let token = "retry-test";
        let broker = ready_broker(token);
        for _ in 0..2 {
            broker
                .handle_nat_introduction_request(&transport, a_addr, a_addr, token.into())
                .await;
        }
        {
            let session = broker.sessions.get(token).unwrap();
            assert_eq!(session.state, SessionState::ReadyForPunch);
            assert_eq!(session.endpoint_a_external, Some(a_addr));
            assert_eq!(session.endpoint_b_external, None);
        }
        let mut packet = [0; 512];
        assert!(timeout(Duration::from_millis(40), a.recv_from(&mut packet))
            .await
            .is_err());

        broker
            .handle_nat_introduction_request(&transport, b_addr, b_addr, token.into())
            .await;
        // Each participant must receive the other participant's endpoint, never itself.
        let first_a = receive_introduction(&a, b_addr, token, 0).await;
        let first_b = receive_introduction(&b, a_addr, token, 0).await;
        assert_eq!(
            broker.sessions.get(token).unwrap().state,
            SessionState::Punched
        );

        // A retry still re-sends the correct introduction to recover a lost UDP response.
        broker
            .handle_nat_introduction_request(&transport, a_addr, a_addr, token.into())
            .await;
        assert_eq!(receive_introduction(&a, b_addr, token, 0).await, first_a);
        assert_eq!(receive_introduction(&b, a_addr, token, 0).await, first_b);

        broker
            .handle_nat_introduction_request(
                &transport,
                other.local_addr().unwrap(),
                other.local_addr().unwrap(),
                token.into(),
            )
            .await;
        assert!(timeout(Duration::from_millis(40), a.recv_from(&mut packet))
            .await
            .is_err());
        assert_eq!(
            broker.sessions.get(token).unwrap().endpoint_b_external,
            Some(b_addr)
        );
        transport.shutdown();
    }

    #[tokio::test]
    async fn introduction_retry_preserves_port_prediction_after_unconfirmed_response() {
        let (transport, _events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let a = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        // Receive on both loopback IPs while presenting a distinct observed IP to the broker.
        let b = UdpSocket::bind("0.0.0.0:0").await.unwrap();
        let a_addr = a.local_addr().unwrap();
        let b_addr = SocketAddr::from(([127, 0, 0, 2], b.local_addr().unwrap().port()));
        let token = "prediction-retry-test";
        let broker = ready_broker(token);
        broker
            .handle_nat_introduction_request(&transport, a_addr, a_addr, token.into())
            .await;
        broker
            .handle_nat_introduction_request(&transport, b_addr, b_addr, token.into())
            .await;
        // Discard the first unacknowledged responses, as if lost before either client used them.
        receive_introduction(&a, b_addr, token, 32).await;
        receive_introduction(&b, a_addr, token, 32).await;
        assert_eq!(
            broker.sessions.get(token).unwrap().state,
            SessionState::Punched
        );

        // Either participant's retry must restore the full prediction window for both peers.
        for address in [a_addr, b_addr] {
            broker
                .handle_nat_introduction_request(&transport, address, address, token.into())
                .await;
            receive_introduction(&a, b_addr, token, 32).await;
            receive_introduction(&b, a_addr, token, 32).await;
        }
        transport.shutdown();
    }

    #[tokio::test]
    async fn token_collision_does_not_replace_either_pair_bookkeeping() {
        let (transport, _events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let broker = P2pBroker::default();
        broker.sessions.insert("shared".into(), session(1, 2));
        broker.track_peer_session(1, "shared");
        broker.track_peer_session(2, "shared");
        for (sender, other) in [(1, 3), (4, 2), (1, 2)] {
            let signal = encode_signal(channels::P2P_SUB_REQUEST, other, "shared");
            broker
                .handle_signal(&transport, sender, &signal, true, |_| true)
                .await;
            let current = broker.sessions.get("shared").unwrap();
            assert_eq!(current.initiator_peer_id, 1);
            assert_eq!(current.target_peer_id, 2);
            assert!(broker.peer_sessions.get(&1).unwrap().contains("shared"));
            assert!(broker.peer_sessions.get(&2).unwrap().contains("shared"));
            assert!(broker.peer_sessions.get(&3).is_none());
            assert!(broker.peer_sessions.get(&4).is_none());
        }
        transport.shutdown();
    }

    #[test]
    fn concurrent_token_claims_have_a_single_owner() {
        let broker = P2pBroker::default();
        let start = Arc::new(std::sync::Barrier::new(3));
        let claims = [(1, 2), (3, 4)].map(|(initiator, target)| {
            let broker = broker.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                broker.claim_session("collision", session(initiator, target), |_| true)
            })
        });
        start.wait();
        let winners = claims.map(|claim| claim.join().unwrap());
        assert_eq!(winners.into_iter().filter(|won| *won).count(), 1);
        let winner = broker.sessions.get("collision").unwrap();
        assert!(matches!(
            (winner.initiator_peer_id, winner.target_peer_id),
            (1, 2) | (3, 4)
        ));
    }

    #[tokio::test]
    async fn forged_destructive_signals_preserve_session_and_offload_state() {
        let (transport, _events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        for sub in [
            channels::P2P_SUB_CANCEL,
            channels::P2P_SUB_DECLINE,
            channels::P2P_SUB_LINK_LOST,
        ] {
            let broker = P2pBroker::default();
            let mut existing = session(1, 2);
            existing.state = SessionState::Punched;
            existing.endpoint_a_external = Some("127.0.0.1:1001".parse().unwrap());
            existing.initiator_link_up = true;
            broker.sessions.insert("token".into(), existing.clone());
            broker.track_peer_session(1, "token");
            broker.track_peer_session(2, "token");
            broker.offloaded_pairs.insert(pack_pair(1, 2), ());
            for (sender, other) in [(9, 2), (1, 9)] {
                let signal = encode_signal(sub, other, "token");
                broker
                    .handle_signal(&transport, sender, &signal, true, |_| true)
                    .await;
                let current = broker.sessions.get("token").unwrap();
                assert_eq!(current.initiator_peer_id, 1);
                assert_eq!(current.target_peer_id, 2);
                assert_eq!(current.state, existing.state);
                assert_eq!(current.endpoint_a_external, existing.endpoint_a_external);
                assert_eq!(current.initiator_link_up, existing.initiator_link_up);
                assert!(broker.peer_sessions.get(&1).unwrap().contains("token"));
                assert!(broker.peer_sessions.get(&2).unwrap().contains("token"));
                assert!(broker.is_offloaded(1, 2));
            }
        }
        transport.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_token_reuse_and_cleanup_keep_peer_indexes_consistent() {
        const ROUNDS: usize = 2000;
        let (transport, _events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let broker = P2pBroker::default();
        let phase = Arc::new(std::sync::Barrier::new(3));
        let requester = {
            let broker = broker.clone();
            let transport = transport.clone();
            let phase = phase.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                for _ in 0..ROUNDS {
                    phase.wait();
                    runtime.block_on(broker.handle_request(
                        &transport,
                        1,
                        BasisP2PSignalMessage {
                            other_player_id: 2,
                            session_token: "reuse".into(),
                            ephemeral_public_key: None,
                        },
                        |_| true,
                    ));
                    phase.wait();
                }
            })
        };
        let cleaner = {
            let broker = broker.clone();
            let transport = transport.clone();
            let phase = phase.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .build()
                    .unwrap();
                for round in 0..ROUNDS {
                    phase.wait();
                    if round % 2 == 0 {
                        let _ = broker.remove_session_if_authorized("reuse", 1, 2);
                    } else {
                        runtime.block_on(
                            broker.remove_peer(&transport, if round % 4 == 1 { 1 } else { 2 }),
                        );
                    }
                    phase.wait();
                }
            })
        };
        let mut inconsistent = 0;
        for _ in 0..ROUNDS {
            // Both workers are parked at the next phase while the fixture and
            // invariant snapshots are accessed; no assertion races a writer.
            broker.sessions.clear();
            broker.peer_sessions.clear();
            broker.sessions.insert("reuse".into(), session(1, 2));
            broker.track_peer_session(1, "reuse");
            broker.track_peer_session(2, "reuse");
            phase.wait();
            phase.wait();
            let live = broker.sessions.contains_key("reuse");
            let tracked_a = broker
                .peer_sessions
                .get(&1)
                .is_some_and(|tokens| tokens.contains("reuse"));
            let tracked_b = broker
                .peer_sessions
                .get(&2)
                .is_some_and(|tokens| tokens.contains("reuse"));
            inconsistent += usize::from(live != tracked_a || live != tracked_b);
        }
        requester.join().unwrap();
        cleaner.join().unwrap();
        transport.shutdown();
        assert_eq!(
            inconsistent, 0,
            "session claims and cleanup must update both peer indexes atomically"
        );
    }

    #[tokio::test]
    async fn valid_destructive_signals_and_peer_cleanup_remove_pair_state() {
        let (transport, _events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        for sub in [channels::P2P_SUB_CANCEL, channels::P2P_SUB_DECLINE] {
            let broker = P2pBroker::default();
            broker.sessions.insert("token".into(), session(1, 2));
            broker.track_peer_session(1, "token");
            broker.track_peer_session(2, "token");
            let signal = encode_signal(sub, 2, "token");
            broker
                .handle_signal(&transport, 1, &signal, true, |_| true)
                .await;
            assert!(!broker.sessions.contains_key("token"));
            assert!(!broker
                .peer_sessions
                .get(&1)
                .is_some_and(|s| s.contains("token")));
            assert!(!broker
                .peer_sessions
                .get(&2)
                .is_some_and(|s| s.contains("token")));
        }
        let broker = P2pBroker::default();
        let mut current = session(1, 2);
        current.state = SessionState::Punched;
        current.initiator_link_up = true;
        current.target_link_up = true;
        broker.sessions.insert("token".into(), current);
        broker.track_peer_session(1, "token");
        broker.track_peer_session(2, "token");
        broker.offloaded_pairs.insert(pack_pair(1, 2), ());
        let signal = encode_signal(channels::P2P_SUB_LINK_LOST, 2, "token");
        broker
            .handle_signal(&transport, 1, &signal, true, |_| true)
            .await;
        let current = broker.sessions.get("token").unwrap();
        assert_eq!(current.state, SessionState::ReadyForPunch);
        assert!(!current.initiator_link_up && !current.target_link_up);
        assert!(!broker.is_offloaded(1, 2));
        drop(current);
        broker.remove_peer(&transport, 1).await;
        assert!(!broker.sessions.contains_key("token"));
        assert!(!broker
            .peer_sessions
            .get(&2)
            .is_some_and(|s| s.contains("token")));
        transport.shutdown();
    }

    #[tokio::test]
    async fn request_rechecks_participant_authentication_before_claim() {
        let (transport, _events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let broker = P2pBroker::default();
        let target_checks = std::sync::atomic::AtomicUsize::new(0);
        broker
            .handle_request(
                &transport,
                1,
                BasisP2PSignalMessage {
                    other_player_id: 2,
                    session_token: "disconnect-race".into(),
                    ephemeral_public_key: None,
                },
                |peer| {
                    peer != 2
                        || target_checks.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0
                },
            )
            .await;
        // The target disconnects after the optimistic lookup, before admission.
        let claimed = broker.sessions.contains_key("disconnect-race");
        transport.shutdown();
        assert!(
            !claimed,
            "a stale authentication result must not admit a new session"
        );
        assert!(broker.peer_sessions.is_empty());
    }

    #[tokio::test]
    async fn destructive_signals_reach_only_the_registered_counterpart() {
        for sub in [
            channels::P2P_SUB_CANCEL,
            channels::P2P_SUB_DECLINE,
            channels::P2P_SUB_LINK_LOST,
        ] {
            for reverse in [false, true] {
                let (transport, mut events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
                    .await
                    .unwrap();
                transport.set_compact_merge_send(false);
                let (a_socket, a) = connected_signal_peer(&transport, &mut events).await;
                let (b_socket, b) = connected_signal_peer(&transport, &mut events).await;
                let (_outsider_socket, outsider) =
                    connected_signal_peer(&transport, &mut events).await;
                let broker = P2pBroker::default();
                broker.claim_session("routing", session(a.peer_id(), b.peer_id()), |_| true);
                for (sender, other) in [
                    (outsider.peer_id(), b.peer_id()),
                    (a.peer_id(), outsider.peer_id()),
                    (b.peer_id(), outsider.peer_id()),
                ] {
                    broker
                        .handle_signal(
                            &transport,
                            sender,
                            &encode_signal(sub, other, "routing"),
                            true,
                            |_| true,
                        )
                        .await;
                    for peer in [&a, &b, &outsider] {
                        assert_no_signal_queued(&transport, peer).await;
                    }
                }
                let (sender, recipient, socket) = if reverse {
                    (&b, &a, &a_socket)
                } else {
                    (&a, &b, &b_socket)
                };
                broker
                    .handle_signal(
                        &transport,
                        sender.peer_id(),
                        &encode_signal(sub, recipient.peer_id(), "routing"),
                        true,
                        |_| true,
                    )
                    .await;
                let received = receive_signal(socket).await;
                assert_eq!(received, encode_signal(sub, sender.peer_id(), "routing"));
                // Receivers deliberately send no ACKs. Any wrongly forwarded
                // signal remains queued or pending and cannot become idle.
                assert!(!transport.reliable_idle(recipient));
                assert_no_signal_queued(&transport, sender).await;
                assert_no_signal_queued(&transport, &outsider).await;
                transport.shutdown();
            }
        }
    }

    async fn connected_signal_peer(
        transport: &TransportHandle,
        events: &mut tokio::sync::mpsc::Receiver<ServerEvent>,
    ) -> (UdpSocket, PeerSession) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let mut request = NetWriter::new();
        request.put_u8(PacketProperty::ConnectRequest as u8);
        request.put_i32(basis_protocol::version::LITENETLIB_PROTOCOL_ID);
        request.put_i64(123);
        request.put_i32(0);
        request.put_u8(16);
        request.put_bytes(&[0; 16]);
        socket
            .send_to(request.as_slice(), transport.local_addr().unwrap())
            .await
            .unwrap();
        let request = timeout(Duration::from_secs(2), async {
            loop {
                if let ServerEvent::ConnectionRequest(request) = events.recv().await.unwrap() {
                    break request;
                }
            }
        })
        .await
        .unwrap();
        let peer = transport.accept_session(&request).await.unwrap();
        let mut packet = [0; 1024];
        timeout(Duration::from_secs(2), socket.recv_from(&mut packet))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            PacketProperty::from_byte(packet[0]),
            Some(PacketProperty::ConnectAccept)
        );
        (socket, peer)
    }

    async fn assert_no_signal_queued(transport: &TransportHandle, peer: &PeerSession) {
        timeout(Duration::from_secs(2), async {
            // reliable_idle takes the send-turn lock, so dispatch cannot hide
            // a packet while moving it between queued and pending state.
            while !transport.reliable_idle(peer) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("unexpected reliable signal queued for peer");
    }

    async fn receive_signal(socket: &UdpSocket) -> Vec<u8> {
        timeout(Duration::from_secs(2), async {
            let mut packet = [0; 2048];
            loop {
                let (len, _) = socket.recv_from(&mut packet).await.unwrap();
                let mut frame = &packet[..len];
                if PacketProperty::from_byte(frame[0]) == Some(PacketProperty::Merged) {
                    let mut reader = NetReader::new(&frame[1..]);
                    let size = reader.get_u16().unwrap() as usize;
                    frame = reader.get_bytes(size).unwrap();
                }
                if frame.len() >= 4
                    && PacketProperty::from_byte(frame[0]) == Some(PacketProperty::Channeled)
                {
                    assert_eq!(
                        frame[3],
                        DeliveryMethod::channel_id(channels::P2P, DeliveryMethod::ReliableOrdered)
                    );
                    return frame[4..].to_vec();
                }
            }
        })
        .await
        .expect("registered counterpart did not receive its P2P signal")
    }

    fn session(initiator_peer_id: PeerId, target_peer_id: PeerId) -> P2pSession {
        P2pSession {
            initiator_peer_id,
            target_peer_id,
            state: SessionState::Awaiting,
            endpoint_a_internal: None,
            endpoint_a_external: None,
            endpoint_b_internal: None,
            endpoint_b_external: None,
            initiator_link_up: false,
            target_link_up: false,
        }
    }

    fn encode_signal(sub: u8, other_player_id: PeerId, token: &str) -> Vec<u8> {
        let mut writer = NetWriter::new();
        writer.put_u8(sub);
        BasisP2PSignalMessage {
            other_player_id,
            session_token: token.into(),
            ephemeral_public_key: None,
        }
        .serialize(&mut writer)
        .unwrap();
        writer.as_slice().to_vec()
    }

    fn ready_broker(token: &str) -> P2pBroker {
        let broker = P2pBroker::default();
        broker.sessions.insert(
            token.into(),
            P2pSession {
                initiator_peer_id: 0,
                target_peer_id: 1,
                state: SessionState::ReadyForPunch,
                endpoint_a_internal: None,
                endpoint_a_external: None,
                endpoint_b_internal: None,
                endpoint_b_external: None,
                initiator_link_up: false,
                target_link_up: false,
            },
        );
        broker
    }

    async fn receive_introduction(
        socket: &UdpSocket,
        other: SocketAddr,
        token: &str,
        prediction_count: u8,
    ) -> Vec<u8> {
        let mut packet = [0; 512];
        let (len, _) = timeout(Duration::from_secs(2), socket.recv_from(&mut packet))
            .await
            .expect("NAT introduction not sent")
            .unwrap();
        assert_eq!(packet[0], basis_transport::PacketProperty::NatMessage as u8);
        let mut reader = NetReader::new(&packet[9..len]);
        for _ in 0..2 {
            assert_eq!(reader.get_u8().unwrap(), 0); // IPv4
            let std::net::IpAddr::V4(other_ip) = other.ip() else {
                panic!("expected IPv4 test endpoint");
            };
            assert_eq!(reader.get_bytes(4).unwrap(), other_ip.octets());
            assert_eq!(reader.get_u16().unwrap(), other.port());
        }
        assert_eq!(reader.get_string().unwrap(), token);
        assert_eq!(reader.get_u8().unwrap(), prediction_count);
        packet[..len].to_vec()
    }
}
