//! One measurement session survives client replacement. Nonowners avoid the mutex.
use super::{AvatarObserver, StdMutex};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    MutexGuard,
};

#[derive(Debug)]
pub(super) struct ObserverState {
    pub(super) observer: AvatarObserver,
    pub(super) owner: Option<(usize, i64)>,
    ever_owned: bool,
}

#[derive(Debug)]
pub(super) struct ObserverSession {
    state: StdMutex<ObserverState>,
    owner_index: AtomicUsize,
    disabled: AtomicBool,
}

impl ObserverSession {
    pub(super) fn new(observer: AvatarObserver) -> Self {
        Self {
            state: StdMutex::new(ObserverState {
                observer,
                owner: None,
                ever_owned: false,
            }),
            owner_index: AtomicUsize::new(0),
            disabled: AtomicBool::new(false),
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
        let owner = self.owner_index.load(Ordering::Acquire);
        !self.disabled.load(Ordering::Relaxed) && (owner == 0 || owner == index + 1)
    }

    pub(super) fn claim(&self, state: &mut ObserverState, key: (usize, i64)) -> bool {
        if let Some(owner) = state.owner {
            return owner == key;
        }
        if state.ever_owned {
            state.observer.mark_discontinuity(std::time::Instant::now());
            tracing::warn!(
                client = key.0,
                "avatar observer ownership changed; cadence discontinuity"
            );
        }
        state.ever_owned = true;
        state.owner = Some(key);
        self.owner_index.store(key.0 + 1, Ordering::Release);
        true
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
