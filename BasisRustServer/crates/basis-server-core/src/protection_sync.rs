//! Coalesced editor snapshots retain only the newest state for each connection.
use super::*;

#[derive(Default)]
pub(crate) struct Pending {
    updates: HashMap<PeerId, (PeerSession, Vec<u8>)>,
}
pub(crate) fn queue(state: &ServerState, session: PeerSession, payload: Vec<u8>) {
    // At most one snapshot per authenticated editor, bounded by the admitted roster.
    if state.transport.is_current_session(&session) {
        state
            .protection_sync
            .lock()
            .updates
            .insert(session.peer_id(), (session, payload));
    }
}
fn pump(state: &ServerState) {
    state
        .protection_sync
        .lock()
        .updates
        .retain(|peer, (session, payload)| {
            if !protection_editor_eligible(state, *peer) {
                return false;
            }
            matches!(
                queue_admission::reliable(&state.transport, session, channels::ADMIN, payload),
                queue_admission::Outcome::Backpressured
            )
        });
}
pub(crate) fn remove_peer(state: &ServerState, peer: PeerId) {
    state.protection_sync.lock().updates.remove(&peer);
}
pub(crate) fn reset(state: &ServerState) {
    state.protection_sync.lock().updates.clear();
}
pub(crate) fn spawn_pump(state: ServerState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut timer = tokio::time::interval(Duration::from_millis(25));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            timer.tick().await;
            if state.shutdown.load(Ordering::Relaxed) {
                break;
            }
            pump(&state);
        }
    })
}
