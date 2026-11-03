use basis_protocol::{
    channels,
    io::{NetReader, NetWriter},
    messages::{BasisDeserialize, BasisP2PSignalMessage, BasisSerialize},
};
use basis_transport::{DeliveryMethod, PeerId, TransportHandle};
use dashmap::{mapref::entry::Entry, DashMap};
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
        let Some((_, tokens)) = self.peer_sessions.remove(&peer) else {
            return;
        };
        for token in tokens {
            let Some(session) = self.sessions.get(&token).map(|entry| entry.clone()) else {
                continue;
            };
            let other = if session.initiator_peer_id == peer {
                session.target_peer_id
            } else {
                session.initiator_peer_id
            };
            self.send_sub(transport, other, channels::P2P_SUB_CANCEL, &token, peer)
                .await;
            self.remove_session(&token);
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
        if self.peer_sessions.get(&sender).is_some_and(|sessions| {
            sessions.len() >= MAX_SESSIONS_PER_PEER && !sessions.contains(&message.session_token)
        }) {
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
        if !self.claim_session(&message.session_token, session) {
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
        self.track_peer_session(sender, &message.session_token);
        self.track_peer_session(message.other_player_id, &message.session_token);
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

    fn remove_session(&self, token: &str) {
        let Some((_, session)) = self.sessions.remove(token) else {
            return;
        };
        self.untrack_session(&session, token);
    }

    fn claim_session(&self, token: &str, session: P2pSession) -> bool {
        match self.sessions.entry(token.to_string()) {
            Entry::Occupied(_) => false,
            Entry::Vacant(entry) => {
                entry.insert(session);
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
                broker.claim_session("collision", session(initiator, target))
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
