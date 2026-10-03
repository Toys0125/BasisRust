//! One measurement session survives client replacement. Nonowners avoid the mutex.
use super::{AvatarObserver, StdMutex};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    MutexGuard,
};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub(super) struct ObserverState {
    pub(super) observer: AvatarObserver,
    pub(super) owner: Option<(usize, i64)>,
    ever_owned: bool,
    last_packet_at: Option<Instant>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn silent_owner_expires_but_control_packets_and_generations_are_checked() {
        let session =
            ObserverSession::new(AvatarObserver::new(40.0, 1, None, Duration::from_secs(60)));
        let start = session.epoch;
        assert!(session.claim(&mut session.lock().unwrap(), (0, 1), start));
        assert!(!session.could_own_at(1, start + Duration::from_secs(5)));
        // A stale generation must not renew the current owner's lease.
        session.note_packet((0, 2), start + Duration::from_secs(5));
        assert!(session.could_own_at(1, start + Duration::from_secs(6)));
        assert!(session.claim(
            &mut session.lock().unwrap(),
            (1, 3),
            start + Duration::from_secs(6)
        ));
        assert!(!session.claim(
            &mut session.lock().unwrap(),
            (0, 1),
            start + Duration::from_secs(7)
        ));
        session.release((0, 1));
        assert_eq!(session.lock().unwrap().owner, Some((1, 3)));
        session.note_packet((1, 3), start + Duration::from_secs(11));
        assert!(!session.could_own_at(0, start + Duration::from_secs(12)));
        assert!(session.could_own_at(0, start + Duration::from_secs(17)));
    }

    #[test]
    fn renewed_owner_rejects_takeover_after_a_stale_fast_check() {
        let session =
            ObserverSession::new(AvatarObserver::new(40.0, 1, None, Duration::from_secs(60)));
        let start = session.epoch;
        assert!(session.claim(&mut session.lock().unwrap(), (0, 1), start));
        let later = start + ObserverSession::LEASE_TIMEOUT;
        assert!(session.could_own_at(1, later));
        session.note_packet((0, 1), later);
        assert!(!session.claim(&mut session.lock().unwrap(), (1, 2), later));
    }
}

#[derive(Debug)]
pub(super) struct ObserverSession {
    state: StdMutex<ObserverState>,
    owner_index: AtomicUsize,
    disabled: AtomicBool,
    epoch: Instant,
    last_packet_millis: AtomicU64,
}

impl ObserverSession {
    // Longer than the existing five-second idle heartbeat; control traffic also renews it.
    pub(super) const LEASE_TIMEOUT: Duration = Duration::from_secs(6);

    pub(super) fn new(observer: AvatarObserver) -> Self {
        Self {
            state: StdMutex::new(ObserverState {
                observer,
                owner: None,
                ever_owned: false,
                last_packet_at: None,
            }),
            owner_index: AtomicUsize::new(0),
            disabled: AtomicBool::new(false),
            epoch: Instant::now(),
            last_packet_millis: AtomicU64::new(0),
        }
    }

    pub(super) fn lock(&self) -> Option<MutexGuard<'_, ObserverState>> {
        if self.disabled.load(Ordering::Acquire) {
            return None;
        }
        match self.state.lock() {
            Ok(state) => Some(state),
            Err(_) => {
                // Partial measurement mutation cannot be trusted. Retain poison and disable
                // collection/reporting instead of exporting plausible but corrupt statistics.
                if !self.disabled.swap(true, Ordering::AcqRel) {
                    tracing::warn!(
                        "avatar observer mutex poisoned; measurement disabled, CSV unavailable"
                    );
                }
                None
            }
        }
    }

    pub(super) fn could_own(&self, index: usize) -> bool {
        self.could_own_at(index, Instant::now())
    }

    pub(super) fn could_own_at(&self, index: usize, now: Instant) -> bool {
        let owner = self.owner_index.load(Ordering::Acquire);
        let elapsed = now.saturating_duration_since(self.epoch).as_millis() as u64;
        let expired = elapsed.saturating_sub(self.last_packet_millis.load(Ordering::Acquire))
            >= Self::LEASE_TIMEOUT.as_millis() as u64;
        !self.disabled.load(Ordering::Relaxed) && (owner == 0 || owner == index + 1 || expired)
    }

    pub(super) fn claim(&self, state: &mut ObserverState, key: (usize, i64), now: Instant) -> bool {
        if let Some(owner) = state.owner {
            if owner == key {
                self.touch(state, now);
                return true;
            }
            // Recheck under the guard: a packet may have renewed the lease after the fast check.
            if state
                .last_packet_at
                .is_some_and(|last| now.saturating_duration_since(last) < Self::LEASE_TIMEOUT)
            {
                return false;
            }
        }
        if state.ever_owned {
            state.observer.mark_discontinuity(now);
            tracing::warn!(
                client = key.0,
                "avatar observer ownership changed; cadence discontinuity"
            );
        }
        state.ever_owned = true;
        state.owner = Some(key);
        self.touch(state, now);
        self.owner_index.store(key.0 + 1, Ordering::Release);
        true
    }

    fn touch(&self, state: &mut ObserverState, now: Instant) {
        let now = state
            .last_packet_at
            .map(|last| last.max(now))
            .unwrap_or(now);
        state.last_packet_at = Some(now);
        self.last_packet_millis.store(
            now.saturating_duration_since(self.epoch).as_millis() as u64,
            Ordering::Release,
        );
    }

    pub(super) fn note_packet(&self, key: (usize, i64), now: Instant) {
        if self.owner_index.load(Ordering::Acquire) != key.0 + 1 {
            return;
        }
        if let Some(mut state) = self.lock() {
            if state.owner == Some(key) {
                self.touch(&mut state, now);
            }
        }
    }

    pub(super) fn release(&self, key: (usize, i64)) {
        if !self.could_own(key.0) {
            return;
        }
        if let Some(mut state) = self.lock() {
            if state.owner == Some(key) {
                state.owner = None;
                self.owner_index.store(0, Ordering::Release);
            }
        }
    }
}
