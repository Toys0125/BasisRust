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
        let previous = counter
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(1))
            })
            .unwrap_or_else(|value| value);
        let count = previous.saturating_add(1);
        if previous != u64::MAX && count.is_power_of_two() {
            tracing::debug!(client, ?reason, count, "discarded malformed packet data");
        }
    }

    pub(super) fn snapshot(&self) -> [u64; 5] {
        self.counts
            .each_ref()
            .map(|value| value.load(Ordering::Relaxed))
    }
}
