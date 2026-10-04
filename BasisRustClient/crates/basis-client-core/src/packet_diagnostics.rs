use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Clone, Copy, Debug)]
#[repr(usize)]
pub(super) enum DropReason {
    Empty,
    UnknownProperty,
    ShortHeader,
    InvalidAckSize,
    InvalidMerged,
}

/// Fixed storage, saturating counts, and logarithmically sampled debug messages.
#[derive(Debug, Default)]
pub(super) struct PacketDiagnostics {
    counts: [AtomicU64; 5],
}

impl PacketDiagnostics {
    pub(super) fn record(&self, client: usize, reason: DropReason) {
        let counter = &self.counts[reason as usize];
        let mut previous = counter.load(Ordering::Relaxed);
        loop {
            if previous == u64::MAX {
                return;
            }
            match counter.compare_exchange_weak(
                previous,
                previous + 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => previous = current,
            }
        }
        let count = previous + 1;
        if count.is_power_of_two() {
            tracing::debug!(client, ?reason, count, "discarded malformed packet data");
        }
    }

    pub(super) fn snapshot(&self) -> [u64; 5] {
        self.counts
            .each_ref()
            .map(|value| value.load(Ordering::Relaxed))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_saturate_and_retain_concurrent_updates() {
        let diagnostics = PacketDiagnostics::default();
        diagnostics.counts[DropReason::Empty as usize].store(u64::MAX - 1, Ordering::Relaxed);
        diagnostics.record(0, DropReason::Empty);
        diagnostics.record(0, DropReason::Empty);
        assert_eq!(diagnostics.snapshot()[DropReason::Empty as usize], u64::MAX);
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    for _ in 0..1000 {
                        diagnostics.record(0, DropReason::UnknownProperty);
                    }
                });
            }
        });
        assert_eq!(
            diagnostics.snapshot()[DropReason::UnknownProperty as usize],
            4000
        );
    }
}
