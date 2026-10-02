//! Population-drop allocator reclamation shared by the console runtime and its
//! long-lived owner threads.

use std::cell::Cell;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

static NEXT_EPOCH_ID: AtomicU64 = AtomicU64::new(1);
thread_local! {
    static LAST_POLLED_EPOCH: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

struct EpochState {
    id: u64,
    epoch: AtomicU64,
    collect: Arc<dyn Fn() + Send + Sync>,
}

/// A process-wide reclaim generation polled by each allocator-owning thread.
///
/// mimalloc's `mi_collect` collects the calling thread's heap, so callers must
/// poll this on Tokio, Rayon, and other long-lived allocation owners.
#[derive(Clone)]
pub struct MemoryReclaimEpoch(Arc<EpochState>);

impl MemoryReclaimEpoch {
    pub fn new(collect: Arc<dyn Fn() + Send + Sync>) -> Self {
        Self(Arc::new(EpochState {
            id: NEXT_EPOCH_ID.fetch_add(1, Ordering::Relaxed),
            epoch: AtomicU64::new(0),
            collect,
        }))
    }

    /// Publish a new generation after reclaimable application caches are freed.
    pub fn request_reclaim(&self) -> u64 {
        self.0.epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub fn requested_epoch(&self) -> u64 {
        self.0.epoch.load(Ordering::Acquire)
    }

    /// Collect once on this thread for the latest requested generation.
    /// Returns true when this call performed collection.
    pub fn poll_current_thread(&self) -> bool {
        let epoch = self.requested_epoch();
        if epoch == 0 {
            LAST_POLLED_EPOCH.with(|last| {
                if last.get().0 != self.0.id {
                    last.set((self.0.id, 0));
                }
            });
            return false;
        }

        let should_collect = LAST_POLLED_EPOCH.with(|last| {
            let (id, previous) = last.get();
            if id == self.0.id && previous >= epoch {
                false
            } else {
                // Mark before invoking the callback, so an allocator callback
                // that itself polls cannot recursively collect this epoch.
                last.set((self.0.id, epoch));
                true
            }
        });
        if !should_collect {
            return false;
        }

        basis_protocol::avatar::clear_current_thread_avatar_bundle_zstd_context();
        (self.0.collect)();
        true
    }
}

impl fmt::Debug for MemoryReclaimEpoch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryReclaimEpoch")
            .field("requested_epoch", &self.requested_epoch())
            .finish_non_exhaustive()
    }
}

/// Implements the historical `IdleMemoryReclaim*` population-drop policy.
#[derive(Debug, Default)]
pub struct IdleMemoryReclaimPolicy {
    peak_since_pass: usize,
    eligible_since: Option<Instant>,
    last_pass: Option<Instant>,
}

impl IdleMemoryReclaimPolicy {
    const DROP_DIVISOR: usize = 4;
    const MINIMUM_PASS_INTERVAL: Duration = Duration::from_secs(120);

    /// Returns the peak population associated with a reclaim decision.
    pub fn observe(
        &mut self,
        now: Instant,
        enabled: bool,
        players: usize,
        settle_seconds: i32,
        minimum_peak: i32,
    ) -> Option<usize> {
        if !enabled {
            self.eligible_since = None;
            return None;
        }

        self.peak_since_pass = self.peak_since_pass.max(players);
        let minimum_peak = minimum_peak.max(1) as usize;
        let population_down = players.saturating_mul(Self::DROP_DIVISOR) <= self.peak_since_pass;
        if self.peak_since_pass < minimum_peak || !population_down {
            self.eligible_since = None;
            return None;
        }

        let eligible_since = *self.eligible_since.get_or_insert(now);
        if now.duration_since(eligible_since) < Duration::from_secs(settle_seconds.max(1) as u64) {
            return None;
        }
        if self
            .last_pass
            .is_some_and(|last| now.duration_since(last) < Self::MINIMUM_PASS_INTERVAL)
        {
            return None;
        }

        let peak = self.peak_since_pass;
        if players == 0 {
            self.peak_since_pass = 0;
        }
        self.eligible_since = None;
        self.last_pass = Some(now);
        Some(peak)
    }

    pub fn peak_since_pass(&self) -> usize {
        self.peak_since_pass
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn at(base: Instant, seconds: u64) -> Instant {
        base + Duration::from_secs(seconds)
    }

    #[test]
    fn requires_minimum_peak_and_quarter_population_for_settle_period() {
        let mut policy = IdleMemoryReclaimPolicy::default();
        let base = Instant::now();
        assert_eq!(policy.observe(at(base, 0), true, 7, 30, 8), None);
        assert_eq!(policy.observe(at(base, 1), true, 1, 30, 8), None);
        assert_eq!(policy.observe(at(base, 2), true, 8, 30, 8), None);
        assert_eq!(policy.observe(at(base, 3), true, 2, 30, 8), None);
        assert_eq!(policy.observe(at(base, 33), true, 2, 30, 8), Some(8));
    }

    #[test]
    fn population_recovery_and_disable_cancel_the_settle_window() {
        let mut policy = IdleMemoryReclaimPolicy::default();
        let base = Instant::now();
        policy.observe(at(base, 0), true, 20, 5, 8);
        policy.observe(at(base, 1), true, 5, 5, 8);
        assert_eq!(policy.observe(at(base, 4), true, 20, 5, 8), None);
        policy.observe(at(base, 5), true, 5, 5, 8);
        assert_eq!(policy.observe(at(base, 6), false, 5, 5, 8), None);
        policy.observe(at(base, 7), true, 5, 5, 8);
        assert_eq!(policy.observe(at(base, 11), true, 5, 5, 8), None);
        assert_eq!(policy.observe(at(base, 12), true, 5, 5, 8), Some(20));
    }

    #[test]
    fn nonempty_pass_preserves_peak_but_empty_pass_rebases_it() {
        let mut policy = IdleMemoryReclaimPolicy::default();
        let base = Instant::now();
        policy.observe(at(base, 0), true, 40, 1, 8);
        policy.observe(at(base, 1), true, 10, 1, 8);
        assert_eq!(policy.observe(at(base, 2), true, 10, 1, 8), Some(40));
        assert_eq!(policy.peak_since_pass(), 40);

        policy.observe(at(base, 123), true, 0, 1, 8);
        assert_eq!(policy.observe(at(base, 124), true, 0, 1, 8), Some(40));
        assert_eq!(policy.peak_since_pass(), 0);
    }

    #[test]
    fn owner_collection_is_once_per_thread_per_epoch() {
        let calls = Arc::new(AtomicUsize::new(0));
        let collector_calls = Arc::clone(&calls);
        let epoch = MemoryReclaimEpoch::new(Arc::new(move || {
            collector_calls.fetch_add(1, Ordering::Relaxed);
        }));

        assert!(!epoch.poll_current_thread());
        epoch.request_reclaim();
        assert!(epoch.poll_current_thread());
        assert!(!epoch.poll_current_thread());
        epoch.request_reclaim();
        assert!(epoch.poll_current_thread());
        assert_eq!(calls.load(Ordering::Relaxed), 2);

        epoch.request_reclaim();
        let threads: Vec<_> = (0..4)
            .map(|_| {
                let epoch = epoch.clone();
                std::thread::spawn(move || {
                    assert!(epoch.poll_current_thread());
                    assert!(!epoch.poll_current_thread());
                })
            })
            .collect();
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(calls.load(Ordering::Relaxed), 6);
    }
}
