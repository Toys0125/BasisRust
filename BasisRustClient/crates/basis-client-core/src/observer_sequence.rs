use std::time::{Duration, Instant};

/// A byte cannot establish order after a half-range gap or complete wrap.
/// After silence plus ambiguous ordering, require two advancing full frames for a baseline.
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
    ApplyAfterGap,
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
        let delta = last.map(|last| sequence.wrapping_sub(last));
        let silence = self
            .last_applied_at
            .is_some_and(|time| now.saturating_duration_since(time) >= Self::RESYNC_AFTER);
        // A regular idle heartbeat is an advancing full frame, not evidence of a reset.
        // It independently supplies a baseline; deltas cannot do that after a long gap.
        let began = !self.resyncing && silence && (!full || !matches!(delta, Some(1..=127)));
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
        match delta {
            Some(0) => SequenceDecision::Reject,
            Some(128..=255) => SequenceDecision::Ambiguous,
            _ if silence => SequenceDecision::ApplyAfterGap,
            _ => SequenceDecision::Apply,
        }
    }

    pub(super) fn applied(&mut self, now: Instant) {
        self.last_applied_at = Some(now);
    }
}
