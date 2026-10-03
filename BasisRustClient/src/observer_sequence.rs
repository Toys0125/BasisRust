use std::time::{Duration, Instant};

/// A byte cannot establish order after a half-range gap or complete wrap.
/// After silence, require two advancing full frames to establish a new baseline.
/// Deltas and a single arbitrary stale full frame cannot resynchronize it.
#[derive(Debug, Default)]
pub(super) struct ObserverSequence {
    last_applied_at: Option<Instant>,
    resyncing: bool,
    candidate: Option<(u8, Instant)>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum SequenceDecision {
    Apply,
    Reject,
    Ambiguous,
    Pending { began: bool },
    Resynced,
}

impl ObserverSequence {
    pub(super) const RESYNC_AFTER: Duration = Duration::from_secs(2);
    // Permit the existing five-second idle keyframe heartbeat to confirm a baseline.
    const CONFIRM_WITHIN: Duration = Duration::from_secs(10);

    pub(super) fn consider(
        &mut self,
        sequence: u8,
        last: Option<u8>,
        full: bool,
        now: Instant,
    ) -> SequenceDecision {
        let began = !self.resyncing
            && self
                .last_applied_at
                .is_some_and(|time| now.saturating_duration_since(time) >= Self::RESYNC_AFTER);
        if began {
            self.resyncing = true;
            self.candidate = None;
        }
        if self.resyncing {
            if full {
                if let Some((candidate, time)) = self.candidate {
                    let advance = sequence.wrapping_sub(candidate);
                    if now.saturating_duration_since(time) < Self::CONFIRM_WITHIN
                        && advance != 0
                        && advance < 128
                    {
                        self.resyncing = false;
                        self.candidate = None;
                        return SequenceDecision::Resynced;
                    }
                    if advance == 0 && now.saturating_duration_since(time) < Self::CONFIRM_WITHIN {
                        return SequenceDecision::Pending { began };
                    }
                }
                self.candidate = Some((sequence, now));
            }
            return SequenceDecision::Pending { began };
        }
        match last.map(|last| sequence.wrapping_sub(last)) {
            Some(0) => SequenceDecision::Reject,
            Some(128..=255) => SequenceDecision::Ambiguous,
            _ => SequenceDecision::Apply,
        }
    }

    pub(super) fn applied(&mut self, now: Instant) {
        self.last_applied_at = Some(now);
    }
}
