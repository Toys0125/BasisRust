use basis_protocol::{
    channels,
    io::{NetReader, NetWriter},
    messages::{BasisDeserialize, BasisP2PSignalMessage, BasisSerialize},
};
use basis_transport::{DeliveryMethod, PeerId, TransportHandle};
use dashmap::DashMap;
use std::{collections::HashSet, net::SocketAddr, sync::Arc};
use tracing::{info, warn};

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
            channels::P2P_SUB_LINK_UP => self.handle_link_up(sender, message),
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
        if session.endpoint_a_internal.is_none() {
            session.endpoint_a_internal = Some(local_addr);
            session.endpoint_a_external = Some(remote_addr);
            return;
        }
        if session.endpoint_b_internal.is_none() {
            session.endpoint_b_internal = Some(local_addr);
            session.endpoint_b_external = Some(remote_addr);
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
        if let Err(err) = transport
            .send_nat_introduce(a_internal, a_external, b_internal, b_external, &token)
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

    fn handle_link_up(&self, sender: PeerId, message: BasisP2PSignalMessage) {
        let Some(mut session) = self.sessions.get_mut(&message.session_token) else {
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
            self.offloaded_pairs.insert(
                pack_pair(session.initiator_peer_id, session.target_peer_id),
                (),
            );
            info!(
                "[P2P] offloaded pair ({},{})",
                session.initiator_peer_id, session.target_peer_id
            );
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
        if message.session_token.is_empty() || message.other_player_id == sender {
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
        self.sessions.insert(message.session_token.clone(), session);
        self.track_peer_session(sender, &message.session_token);
        self.track_peer_session(message.other_player_id, &message.session_token);
        self.send_sub(
            transport,
            message.other_player_id,
            channels::P2P_SUB_REQUEST,
            &message.session_token,
            sender,
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
        self.send_sub(
            transport,
            initiator,
            channels::P2P_SUB_ACCEPT,
            &message.session_token,
            sender,
        )
        .await;
    }

    async fn handle_link_lost(
        &self,
        transport: &TransportHandle,
        sender: PeerId,
        message: BasisP2PSignalMessage,
    ) {
        if let Some(mut session) = self.sessions.get_mut(&message.session_token) {
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
        }
        self.forward_and_drop(
            transport,
            sender,
            message,
            channels::P2P_SUB_LINK_LOST,
            false,
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
        self.send_sub(
            transport,
            message.other_player_id,
            sub,
            &message.session_token,
            sender,
        )
        .await;
        if drop_session && !message.session_token.is_empty() {
            self.remove_session(&message.session_token);
        }
    }

    fn remove_session(&self, token: &str) {
        let Some((_, session)) = self.sessions.remove(token) else {
            return;
        };
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
        let mut writer = NetWriter::new();
        writer.put_u8(sub);
        BasisP2PSignalMessage {
            other_player_id,
            session_token: token.to_string(),
        }
        .serialize(&mut writer);
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

pub fn pack_pair(a: PeerId, b: PeerId) -> u64 {
    let lo = a.min(b) as u64;
    let hi = a.max(b) as u64;
    (lo << 32) | hi
}

fn preview(token: &str) -> &str {
    token.get(..8).unwrap_or(token)
}
