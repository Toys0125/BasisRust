//! Reliable admission is distinct from a completed send or a live recipient.
use basis_transport::{PeerSession, TransportHandle};

#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    Queued,
    Backpressured,
    RecipientGone,
}

pub(crate) fn reliable(
    transport: &TransportHandle,
    session: &PeerSession,
    channel: u8,
    payload: &[u8],
) -> Outcome {
    if !transport.is_current_session(session) {
        return Outcome::RecipientGone;
    }
    if transport.queue_reliable_ordered_session(session, channel, payload) {
        Outcome::Queued
    } else if transport.is_current_session(session) {
        Outcome::Backpressured
    } else {
        Outcome::RecipientGone
    }
}
