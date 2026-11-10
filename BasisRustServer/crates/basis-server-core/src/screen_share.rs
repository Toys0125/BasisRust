//! Screen share control and bounded forwarding; payloads follow BasisVR 2e6b44de394d.
use super::*;
use basis_protocol::screen_share::*;
use std::time::{Duration, Instant};

const VIEWER_QUEUE_LIMIT: usize = 900;
#[derive(Default)]
struct Bucket {
    tokens: f64,
    last: Option<Instant>,
}
impl Bucket {
    fn take(&mut self, rate: f64, bytes: usize, now: Instant) -> bool {
        if rate <= 0. {
            return true;
        }
        let cap = rate * 2.;
        self.tokens = self.last.map_or(cap, |last| {
            (self.tokens + rate * now.duration_since(last).as_secs_f64()).min(cap)
        });
        self.last = Some(now);
        if self.tokens < bytes as f64 {
            return false;
        }
        self.tokens -= bytes as f64;
        true
    }
}
struct Viewer {
    needs_key: bool,
    forwarding: bool,
    sequence: u32,
}
impl Default for Viewer {
    fn default() -> Self {
        Self {
            needs_key: true,
            forwarding: false,
            sequence: u32::MAX,
        }
    }
}
struct Session {
    info: StreamInfo,
    pose: ScreenPose,
    viewers: HashMap<PeerId, Viewer>,
    ingress: Bucket,
    egress: Bucket,
    last_key: Option<Instant>,
    dropping_video: bool,
    frame_sequence: u32,
    frame_targets: Vec<PeerSession>,
}
#[derive(Default)]
pub(crate) struct Ledger {
    sessions: HashMap<PeerId, Session>,
    dropped_chunks: u64,
}
struct Action {
    recipient: PeerSession,
    plugin_id: u16,
    payload: Vec<u8>,
}
type Actions = Vec<Action>;
fn payload(sub: u8, write: impl FnOnce(&mut NetWriter)) -> Vec<u8> {
    let mut w = NetWriter::new();
    w.put_u8(sub);
    write(&mut w);
    w.into_vec()
}
fn subscribed(state: &ServerState, peer: PeerId, id: u16) -> bool {
    state
        .message_subscriptions
        .get(&peer)
        .is_some_and(|s| s.contains(&id))
}
fn control(state: &ServerState, actions: &mut Actions, to: PeerId, bytes: Vec<u8>) {
    if let Some(recipient) = state.transport.peer_session(to) {
        actions.push(Action {
            recipient,
            plugin_id: CONTROL_ID,
            payload: bytes,
        });
    }
}
fn broadcast(state: &ServerState, actions: &mut Actions, except: PeerId, bytes: Vec<u8>) {
    for peer in state.authenticated_peers.iter() {
        if *peer.key() != except {
            control(state, actions, *peer.key(), bytes.clone());
        }
    }
}
fn announce(sharer: PeerId, s: &Session) -> Vec<u8> {
    payload(ANNOUNCE, |w| {
        w.put_u16(sharer);
        s.info.serialize(w).expect("bounded title");
        s.pose.serialize(w).expect("fixed pose");
    })
}
fn refuse(state: &ServerState, actions: &mut Actions, peer: PeerId, reason: u8) {
    control(state, actions, peer, payload(REFUSED, |w| w.put_u8(reason)));
}
fn send_viewer_count(state: &ServerState, actions: &mut Actions, peer: PeerId, s: &Session) {
    control(
        state,
        actions,
        peer,
        payload(VIEWERS, |w| {
            w.put_u16(s.viewers.len().min(u16::MAX as usize) as u16)
        }),
    );
}
fn request_keyframe(
    state: &ServerState,
    actions: &mut Actions,
    sharer: PeerId,
    s: &mut Session,
    force: bool,
) {
    let now = Instant::now();
    if force
        || s.last_key
            .is_none_or(|last| now.duration_since(last) >= Duration::from_millis(500))
    {
        s.last_key = Some(now);
        control(state, actions, sharer, payload(KEYFRAME, |_| {}));
    }
}
fn end_session(state: &ServerState, ledger: &mut Ledger, actions: &mut Actions, sharer: PeerId) {
    if ledger.sessions.remove(&sharer).is_some() {
        broadcast(
            state,
            actions,
            sharer,
            payload(ENDED, |w| w.put_u16(sharer)),
        );
    }
}
async fn flush(state: &ServerState, actions: Actions) {
    for action in actions {
        // Effects retain the connection selected by the transition. Never resolve a
        // queued recipient ID to a replacement connection when applying the effect.
        if !state.transport.is_current_session(&action.recipient)
            || !subscribed(state, action.recipient.peer_id(), action.plugin_id)
        {
            continue;
        }
        let mut writer = NetWriter::with_capacity(action.payload.len() + 2);
        writer.put_u16(action.plugin_id);
        writer.put_bytes(&action.payload);
        if let Err(err) = state
            .transport
            .send_session(
                &action.recipient,
                channels::PLUGIN_RELIABLE,
                DeliveryMethod::ReliableOrdered,
                writer.as_slice(),
            )
            .await
        {
            warn!("screen share send failed: {err}");
        }
    }
}
pub(crate) async fn subscribed_peer(state: &ServerState, peer: PeerId) {
    let actions = {
        let ledger = state.screen_share.lock();
        let mut a = Vec::new();
        control(
            state,
            &mut a,
            peer,
            payload(POLICY, |w| {
                w.put_bool(state.config.read().screen_share_locked)
            }),
        );
        for (&sharer, s) in &ledger.sessions {
            if sharer != peer {
                control(state, &mut a, peer, announce(sharer, s));
            }
        }
        a
    };
    flush(state, actions).await;
}
pub(crate) async fn remove_peer(state: &ServerState, peer: PeerId) {
    let actions = {
        let mut ledger = state.screen_share.lock();
        let mut a = Vec::new();
        end_session(state, &mut ledger, &mut a, peer);
        for (&sharer, s) in &mut ledger.sessions {
            s.frame_targets.retain(|target| target.peer_id() != peer);
            if s.viewers.remove(&peer).is_some() {
                send_viewer_count(state, &mut a, sharer, s);
            }
        }
        a
    };
    flush(state, actions).await;
}
pub(crate) async fn handle(state: &ServerState, peer: PeerId, id: u16, bytes: &[u8]) -> Result<()> {
    let mut r = NetReader::new(bytes);
    if id == MEDIA_ID {
        return media(state, peer, bytes).await;
    }
    let sub = r.get_u8()?;
    if sub == LOCK {
        let locked = r.get_bool()?;
        if !peer_has_permission(
            state,
            peer,
            basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
        ) {
            return Ok(());
        }
        state.config.write().screen_share_locked = locked;
        let actions = {
            let mut ledger = state.screen_share.lock();
            let mut a = Vec::new();
            broadcast(
                state,
                &mut a,
                u16::MAX,
                payload(POLICY, |w| w.put_bool(locked)),
            );
            if locked {
                let peers: Vec<_> = ledger.sessions.keys().copied().collect();
                for sharer in peers {
                    if !peer_has_permission(
                        state,
                        sharer,
                        basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
                    ) {
                        refuse(state, &mut a, sharer, REFUSED_LOCKED);
                        end_session(state, &mut ledger, &mut a, sharer);
                    }
                }
            }
            a
        };
        flush(state, actions).await;
        let notification = if locked {
            "Screen sharing has been globally DISABLED by an admin."
        } else {
            "Screen sharing has been globally ENABLED by an admin."
        };
        let mut w = NetWriter::new();
        w.put_u8(AdminRequestMode::MessageAll as u8);
        w.put_string(notification)?;
        state
            .broadcast(
                channels::ADMIN,
                DeliveryMethod::ReliableOrdered,
                w.as_slice(),
                None,
            )
            .await;
        // Persistence is secondary to the live moderation action. A full disk or
        // invalid path must not leave existing streams active under a locked policy.
        let persistence_error = {
            let config = state.config.read();
            if config.has_file_support {
                config.save(&state.config_path).err()
            } else {
                None
            }
        };
        if let Some(err) = persistence_error {
            warn!("screen share policy applied but configuration save failed: {err:#}");
            if let Err(report_err) = send_admin_text(
                state,
                peer,
                "Screen sharing policy applied, but saving the configuration failed.",
            )
            .await
            {
                warn!("failed to report screen share persistence error: {report_err:#}");
            }
        }
        return Ok(());
    }
    let actions = {
        let mut ledger = state.screen_share.lock();
        let mut a = Vec::new();
        match sub {
            START => {
                let parsed =
                    StreamInfo::read(&mut r).and_then(|info| Ok((info, ScreenPose::read(&mut r)?)));
                match parsed {
                    Err(_) => refuse(state, &mut a, peer, REFUSED_INVALID),
                    Ok((info, pose)) => {
                        let config = state.config.read();
                        if !peer_has_permission(
                            state,
                            peer,
                            basis_server_permissions::nodes::SCREEN_SHARE,
                        ) {
                            refuse(state, &mut a, peer, REFUSED_NO_PERMISSION);
                        } else if config.screen_share_locked
                            && !peer_has_permission(
                                state,
                                peer,
                                basis_server_permissions::nodes::MODERATION_GLOBAL_LOCK,
                            )
                        {
                            refuse(state, &mut a, peer, REFUSED_LOCKED);
                        } else if !ledger.sessions.contains_key(&peer)
                            && config.screen_share_max_concurrent > 0
                            && ledger.sessions.len() >= config.screen_share_max_concurrent as usize
                        {
                            refuse(state, &mut a, peer, REFUSED_TOO_MANY);
                        } else {
                            let s = ledger.sessions.entry(peer).or_insert_with(|| Session {
                                info: info.clone(),
                                pose: pose.clone(),
                                viewers: HashMap::new(),
                                ingress: Bucket::default(),
                                egress: Bucket::default(),
                                last_key: None,
                                dropping_video: false,
                                frame_sequence: u32::MAX,
                                frame_targets: Vec::new(),
                            });
                            s.info = info;
                            s.pose = pose;
                            for v in s.viewers.values_mut() {
                                v.needs_key = true;
                            }
                            control(
                                state,
                                &mut a,
                                peer,
                                payload(ACCEPTED, |w| {
                                    w.put_u32(
                                        config.screen_share_max_kilobits_per_second.max(500) as u32
                                    );
                                    w.put_u16(s.viewers.len().min(u16::MAX as usize) as u16);
                                }),
                            );
                            broadcast(state, &mut a, peer, announce(peer, s));
                        }
                    }
                }
            }
            STOP => end_session(state, &mut ledger, &mut a, peer),
            INFO => {
                if let Some(s) = ledger.sessions.get_mut(&peer) {
                    let info = StreamInfo::read(&mut r)?;
                    s.info = info;
                    for v in s.viewers.values_mut() {
                        v.needs_key = true;
                    }
                    broadcast(state, &mut a, peer, announce(peer, s));
                }
            }
            POSE | MOVE => {
                let sharer = if sub == MOVE { r.get_u16()? } else { peer };
                if let Some(s) = ledger.sessions.get_mut(&sharer) {
                    s.pose = ScreenPose::read(&mut r)?;
                    broadcast(
                        state,
                        &mut a,
                        peer,
                        payload(POSE, |w| {
                            w.put_u16(sharer);
                            s.pose.serialize(w).expect("fixed pose");
                        }),
                    );
                }
            }
            WATCH => {
                let sharer = r.get_u16()?;
                let watching = r.get_bool()?;
                if sharer != peer {
                    if let Some(s) = ledger.sessions.get_mut(&sharer) {
                        let changed = if watching {
                            let changed = !s.viewers.contains_key(&peer);
                            s.viewers.entry(peer).or_default();
                            request_keyframe(state, &mut a, sharer, s, changed);
                            changed
                        } else {
                            s.frame_targets.retain(|target| target.peer_id() != peer);
                            s.viewers.remove(&peer).is_some()
                        };
                        if changed {
                            send_viewer_count(state, &mut a, sharer, s);
                        }
                    }
                }
            }
            KEYFRAME => {
                let sharer = r.get_u16()?;
                if let Some(s) = ledger.sessions.get_mut(&sharer) {
                    if let Some(v) = s.viewers.get_mut(&peer) {
                        v.needs_key = true;
                        request_keyframe(state, &mut a, sharer, s, false);
                    }
                }
            }
            _ => {}
        }
        a
    };
    flush(state, actions).await;
    Ok(())
}
fn drop_chunk(
    state: &ServerState,
    ledger: &mut Ledger,
    sharer: PeerId,
    h: &ChunkHeader,
    a: &mut Actions,
) {
    ledger.dropped_chunks += 1;
    let s = ledger.sessions.get_mut(&sharer).expect("existing session");
    if h.kind != KIND_VIDEO {
        return;
    }
    s.frame_sequence = u32::MAX;
    s.frame_targets.clear();
    for v in s.viewers.values_mut() {
        if v.forwarding && v.sequence == h.sequence {
            v.forwarding = false;
            v.needs_key = true;
        }
    }
    if !s.dropping_video {
        s.dropping_video = true;
        request_keyframe(state, a, sharer, s, false);
    }
}
async fn media(state: &ServerState, peer: PeerId, bytes: &[u8]) -> Result<()> {
    let h = ChunkHeader::read(&mut NetReader::new(bytes))?;
    let actions = {
        let mut ledger = state.screen_share.lock();
        let mut a = Vec::new();
        if !ledger.sessions.contains_key(&peer) {
            return Ok(());
        }
        let config = state.config.read();
        let now = Instant::now();
        let s = ledger.sessions.get_mut(&peer).expect("session");
        if !s.ingress.take(
            config.screen_share_max_kilobits_per_second.max(500) as f64 * 125. * 1.5,
            bytes.len(),
            now,
        ) {
            drop_chunk(state, &mut ledger, peer, &h, &mut a);
            a
        } else if s.viewers.is_empty() {
            a
        } else {
            let targets = if h.kind == KIND_VIDEO && h.index != 0 {
                if h.sequence != s.frame_sequence {
                    return Ok(());
                }
                s.frame_targets.clone()
            } else {
                let mut targets = Vec::new();
                let mut request_key = false;
                for (&viewer, v) in &mut s.viewers {
                    if !state.authenticated_peers.contains_key(&viewer)
                        || !subscribed(state, viewer, MEDIA_ID)
                    {
                        continue;
                    }
                    let Some(recipient) = state
                        .authenticated_peers
                        .get(&viewer)
                        .and_then(|p| p.session.clone())
                    else {
                        continue;
                    };
                    if !state.transport.is_current_session(&recipient) {
                        continue;
                    }
                    let backlogged = state.transport.peer_channel_queued_count(
                        viewer,
                        channels::PLUGIN_RELIABLE,
                        DeliveryMethod::ReliableOrdered,
                    ) > VIEWER_QUEUE_LIMIT;
                    if h.kind == KIND_AUDIO {
                        if !backlogged {
                            targets.push(recipient.clone());
                        }
                        continue;
                    }
                    if h.index == 0 {
                        v.sequence = h.sequence;
                        if backlogged {
                            v.forwarding = false;
                            v.needs_key = true;
                            request_key = true;
                        } else if v.needs_key && !h.is_key() {
                            v.forwarding = false;
                            request_key = true;
                        } else {
                            v.forwarding = true;
                            if h.is_key() {
                                v.needs_key = false;
                            }
                        }
                    }
                    if v.forwarding && v.sequence == h.sequence {
                        targets.push(recipient.clone());
                    }
                }
                if request_key {
                    request_keyframe(state, &mut a, peer, s, false);
                }
                if h.kind == KIND_VIDEO {
                    s.frame_sequence = h.sequence;
                    s.frame_targets = targets.clone();
                }
                targets
            };
            if !targets.is_empty() {
                if !s.egress.take(
                    config.screen_share_egress_megabits_per_second as f64 * 125000.,
                    (bytes.len() + 4) * targets.len(),
                    now,
                ) {
                    drop_chunk(state, &mut ledger, peer, &h, &mut a);
                } else {
                    if h.kind == KIND_VIDEO && h.index == 0 {
                        s.dropping_video = false;
                    }
                    let mut w = NetWriter::new();
                    w.put_u16(peer);
                    w.put_bytes(bytes);
                    let data = w.into_vec();
                    for target in targets {
                        a.push(Action {
                            recipient: target,
                            plugin_id: MEDIA_ID,
                            payload: data.clone(),
                        });
                    }
                }
            }
            a
        }
    };
    flush(state, actions).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn bandwidth_bucket_has_bounded_burst_and_refills() {
        let now = Instant::now();
        let mut b = Bucket::default();
        assert!(b.take(100., 200, now));
        assert!(!b.take(100., 1, now));
        assert!(b.take(100., 100, now + Duration::from_secs(1)));
        assert!(!b.take(100., 1, now + Duration::from_secs(1)));
        assert!(b.take(0., usize::MAX, now));
    }
}
