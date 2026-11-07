//! Image relay enforcement and paced cache downloads, matching the C# governor.
use crate::image_cache::ReplayPayload;
use basis_protocol::config::ServerConfig;
use basis_transport::{PeerId, PeerSession};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, VecDeque},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

const MEGABITS_TO_BYTES: f64 = 125_000.0;
const BURST_SECONDS: f64 = 2.0;

struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

impl Bucket {
    fn new(rate: f64, now: Instant) -> Self {
        Self {
            tokens: rate * BURST_SECONDS,
            last_refill: now,
        }
    }

    fn refill(&mut self, rate: f64, now: Instant) {
        self.tokens = (self.tokens
            + rate
                * now
                    .saturating_duration_since(self.last_refill)
                    .as_secs_f64())
        .min(rate * BURST_SECONDS);
        // Callers can capture time before another task wins the bucket lock.
        // A stale observation must not rewind the refill clock and mint credit.
        self.last_refill = self.last_refill.max(now);
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PendingPayload {
    pub owner: PeerId,
    pub payload: ReplayPayload,
}

struct QueuedPayload {
    pending: PendingPayload,
    bypass_pacing: bool,
}

struct ReplayJob<T> {
    target: T,
    payloads: VecDeque<QueuedPayload>,
    bucket: Bucket,
    in_flight: bool,
}

pub(crate) trait SameReplayTarget {
    fn same_replay_target(&self, other: &Self) -> bool;
}

impl SameReplayTarget for () {
    fn same_replay_target(&self, _other: &Self) -> bool {
        true
    }
}

impl SameReplayTarget for u16 {
    fn same_replay_target(&self, other: &Self) -> bool {
        self == other
    }
}

impl SameReplayTarget for PeerSession {
    fn same_replay_target(&self, other: &Self) -> bool {
        self.same_connection(other)
    }
}

// T is the recipient's transport session in production. Keeping it in the job
// prevents queued data reaching a different connection that reuses the peer ID.
pub(crate) struct ImageBandwidthGovernor<T> {
    egress: Mutex<HashMap<PeerId, Bucket>>,
    replays: Mutex<HashMap<PeerId, Vec<ReplayJob<T>>>>,
    dropped_messages: AtomicU64,
    dropped_bytes: AtomicU64,
}

impl<T> Default for ImageBandwidthGovernor<T> {
    fn default() -> Self {
        Self {
            egress: Mutex::new(HashMap::new()),
            replays: Mutex::new(HashMap::new()),
            dropped_messages: AtomicU64::new(0),
            dropped_bytes: AtomicU64::new(0),
        }
    }
}

impl<T: Clone + SameReplayTarget> ImageBandwidthGovernor<T> {
    pub fn try_consume_egress(
        &self,
        sender: PeerId,
        bytes: u64,
        config: &ServerConfig,
        now: Instant,
    ) -> bool {
        if config.image_share_egress_megabits_per_second <= 0 || bytes == 0 {
            return true;
        }
        let rate = f64::from(config.image_share_egress_megabits_per_second)
            * MEGABITS_TO_BYTES
            * (f64::from(config.image_share_egress_enforcement_percent.max(100)) / 100.0);
        let mut egress = self.egress.lock();
        let bucket = egress
            .entry(sender)
            .or_insert_with(|| Bucket::new(rate, now));
        bucket.refill(rate, now);
        // Admit one whole chunk when there is credit, even if its fan-out costs
        // more than the burst. Debt must refill before another chunk can pass.
        if bucket.tokens <= 0.0 {
            self.dropped_messages.fetch_add(1, Ordering::Relaxed);
            self.dropped_bytes.fetch_add(bytes, Ordering::Relaxed);
            return false;
        }
        bucket.tokens -= bytes as f64;
        true
    }

    /// Append work to this connection's FIFO. Inline work is returned only if no
    /// earlier batch for the same connection is queued or currently being sent.
    pub fn enqueue_replay(
        &self,
        peer: PeerId,
        target: T,
        payloads: Vec<PendingPayload>,
        config: &ServerConfig,
        now: Instant,
    ) -> Vec<PendingPayload> {
        self.enqueue_mode(peer, target, payloads, true, config, now)
    }

    pub fn enqueue_inline(
        &self,
        peer: PeerId,
        target: T,
        payloads: Vec<PendingPayload>,
        config: &ServerConfig,
        now: Instant,
    ) -> Vec<PendingPayload> {
        self.enqueue_mode(peer, target, payloads, false, config, now)
    }

    fn enqueue_mode(
        &self,
        peer: PeerId,
        target: T,
        payloads: Vec<PendingPayload>,
        paced: bool,
        config: &ServerConfig,
        now: Instant,
    ) -> Vec<PendingPayload> {
        let rate = f64::from(config.image_share_download_megabits_per_second) * MEGABITS_TO_BYTES;
        if payloads.is_empty() {
            return payloads;
        }
        let mut replays = self.replays.lock();
        let jobs = replays.entry(peer).or_default();
        let index = jobs
            .iter()
            .position(|job| job.target.same_replay_target(&target));
        let index = index.unwrap_or_else(|| {
            jobs.push(ReplayJob {
                target: target.clone(),
                payloads: VecDeque::new(),
                bucket: Bucket::new(rate.max(0.0), now),
                in_flight: false,
            });
            jobs.len() - 1
        });
        let job = &mut jobs[index];
        // Eviction/despawn clears the buffers immediately. Discard their empty
        // handles here so churn cannot accumulate stale queue metadata.
        job.payloads
            .retain(|queued| queued.pending.payload.len() > 0);
        // Append without renewing credit: repeated pickup requests share a budget.
        job.payloads
            .extend(payloads.into_iter().map(|pending| QueuedPayload {
                pending,
                bypass_pacing: !paced || rate <= 0.0,
            }));
        if !job.in_flight && (!paced || rate <= 0.0) {
            if rate > 0.0 {
                job.bucket.refill(rate, now);
            }
            let mut batch = Vec::new();
            while let Some(front) = job.payloads.front() {
                if rate > 0.0 && !front.bypass_pacing && job.bucket.tokens <= 0.0 {
                    break;
                }
                let queued = job.payloads.pop_front().unwrap();
                let size = queued.pending.payload.len();
                if size == 0 {
                    continue;
                }
                if rate > 0.0 && !queued.bypass_pacing {
                    job.bucket.tokens -= size as f64;
                }
                batch.push(queued.pending);
            }
            if !batch.is_empty() {
                job.in_flight = true;
                return batch;
            }
        }
        Vec::new()
    }

    /// Retain payloads that were removed by `pump` but refused by the transport queue.
    /// These retries bypass download pacing because the payload was already selected by its
    /// pump pass, or pacing was disabled for the original send.
    pub fn finish_replay_batch(&self, peer: PeerId, target: &T, rejected: Vec<PendingPayload>) {
        let mut replays = self.replays.lock();
        let Some(jobs) = replays.get_mut(&peer) else {
            return;
        };
        let Some(job) = jobs
            .iter_mut()
            .find(|job| job.target.same_replay_target(target))
        else {
            return;
        };
        if !job.in_flight {
            return;
        }
        job.in_flight = false;
        for pending in rejected.into_iter().rev() {
            job.payloads.push_front(QueuedPayload {
                pending,
                bypass_pacing: true,
            });
        }
    }

    /// Take one pump pass; transport sends run after releasing the state lock.
    pub fn pump(&self, config: &ServerConfig, now: Instant) -> Vec<(T, Vec<PendingPayload>)> {
        let rate = f64::from(config.image_share_download_megabits_per_second) * MEGABITS_TO_BYTES;
        let mut replays = self.replays.lock();
        if rate <= 0.0 {
            for jobs in replays.values_mut() {
                for job in jobs.iter_mut() {
                    if !job.in_flight {
                        job.payloads.retain(|queued| queued.bypass_pacing);
                    }
                }
                jobs.retain(|job| job.in_flight || !job.payloads.is_empty());
            }
            replays.retain(|_, jobs| !jobs.is_empty());
        }
        let mut batches = Vec::new();
        for jobs in replays.values_mut() {
            for job in jobs.iter_mut() {
                job.payloads
                    .retain(|queued| queued.pending.payload.len() > 0);
                if job.in_flight || job.payloads.is_empty() {
                    continue;
                }
                if rate > 0.0 {
                    job.bucket.refill(rate, now);
                }
                let mut batch = Vec::new();
                while let Some(front) = job.payloads.front() {
                    if rate > 0.0 && !front.bypass_pacing && job.bucket.tokens <= 0.0 {
                        break;
                    }
                    let queued = job.payloads.pop_front().unwrap();
                    let size = queued.pending.payload.len();
                    if size == 0 {
                        continue;
                    }
                    if rate > 0.0 && !queued.bypass_pacing {
                        job.bucket.tokens -= size as f64;
                    }
                    batch.push(queued.pending);
                }
                if !batch.is_empty() {
                    job.in_flight = true;
                    batches.push((job.target.clone(), batch));
                }
            }
        }
        batches
    }

    pub fn remove_peer(&self, peer: PeerId, target: &T) {
        self.egress.lock().remove(&peer);
        let mut replays = self.replays.lock();
        if let Some(jobs) = replays.get_mut(&peer) {
            jobs.retain(|job| !job.target.same_replay_target(target));
            if jobs.is_empty() {
                replays.remove(&peer);
            }
        }
    }

    pub fn reset(&self) {
        self.egress.lock().clear();
        self.replays.lock().clear();
        self.dropped_messages.store(0, Ordering::Relaxed);
        self.dropped_bytes.store(0, Ordering::Relaxed);
    }

    pub fn dropped(&self) -> (u64, u64) {
        (
            self.dropped_messages.load(Ordering::Relaxed),
            self.dropped_bytes.load(Ordering::Relaxed),
        )
    }

    #[cfg(test)]
    pub(crate) fn queued_replay_bytes(&self) -> usize {
        self.replays
            .lock()
            .values()
            .flatten()
            .flat_map(|job| &job.payloads)
            .map(|queued| queued.pending.payload.len())
            .sum::<usize>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Duration;

    fn config() -> ServerConfig {
        ServerConfig {
            image_share_egress_megabits_per_second: 1,
            image_share_egress_enforcement_percent: 100,
            image_share_download_megabits_per_second: 1,
            ..ServerConfig::default()
        }
    }

    fn payload(owner: u16, value: u8) -> PendingPayload {
        PendingPayload {
            owner,
            payload: ReplayPayload::new(Bytes::from(vec![value; 125_000])),
        }
    }

    #[test]
    fn fanout_debt_refills_and_senders_are_independent() {
        let governor = ImageBandwidthGovernor::<()>::default();
        let now = Instant::now();
        let config = config();
        assert!(governor.try_consume_egress(1, 500_000, &config, now));
        assert!(!governor.try_consume_egress(1, 50, &config, now));
        assert!(governor.try_consume_egress(2, 50, &config, now));
        assert!(!governor.try_consume_egress(1, 50, &config, now + Duration::from_secs(2)));
        assert!(governor.try_consume_egress(1, 50, &config, now + Duration::from_secs(3)));
        assert_eq!(governor.dropped(), (2, 100));
    }

    #[test]
    fn headroom_clamp_disable_and_live_rate_changes() {
        let governor = ImageBandwidthGovernor::<()>::default();
        let now = Instant::now();
        let mut config = config();
        config.image_share_egress_enforcement_percent = 50;
        assert!(governor.try_consume_egress(1, 250_000, &config, now));
        assert!(!governor.try_consume_egress(1, 1, &config, now));
        config.image_share_egress_enforcement_percent = 300;
        assert!(governor.try_consume_egress(1, 750_000, &config, now + Duration::from_secs(2)));
        config.image_share_egress_megabits_per_second = 0;
        assert!(governor.try_consume_egress(1, u64::MAX, &config, now));
        config.image_share_egress_megabits_per_second = -1;
        assert!(governor.try_consume_egress(1, 50, &config, now));
    }

    #[test]
    fn idle_credit_is_bounded_and_departure_resets_debt() {
        let governor = ImageBandwidthGovernor::<()>::default();
        let now = Instant::now();
        let config = config();
        assert!(governor.try_consume_egress(1, 1, &config, now));
        assert!(governor.try_consume_egress(1, 250_000, &config, now + Duration::from_secs(100)));
        assert!(!governor.try_consume_egress(1, 1, &config, now + Duration::from_secs(100)));
        governor.remove_peer(1, &());
        assert!(governor.try_consume_egress(1, 250_000, &config, now + Duration::from_secs(100)));
        governor.reset();
        assert_eq!(governor.dropped(), (0, 0));
    }

    #[test]
    fn stale_time_observations_do_not_refill_the_same_interval_twice() {
        let governor = ImageBandwidthGovernor::<()>::default();
        let now = Instant::now();
        let config = config();
        assert!(governor.try_consume_egress(1, 250_000, &config, now));
        assert!(governor.try_consume_egress(1, 125_000, &config, now + Duration::from_secs(1)));
        assert!(!governor.try_consume_egress(1, 1, &config, now));
        assert!(!governor.try_consume_egress(1, 1, &config, now + Duration::from_secs(1)));
    }

    #[test]
    fn replay_paces_preserves_order_and_appends_without_new_credit() {
        let governor = ImageBandwidthGovernor::<u16>::default();
        let now = Instant::now();
        let config = config();
        governor.enqueue_replay(9, 9, (0..4).map(|i| payload(7, i)).collect(), &config, now);
        let batches = governor.pump(&config, now);
        assert_eq!(
            batches[0]
                .1
                .iter()
                .map(|p| (p.owner, p.payload.read().as_ref().unwrap()[0]))
                .collect::<Vec<_>>(),
            vec![(7, 0), (7, 1)]
        );
        governor.finish_replay_batch(9, &9, Vec::new());
        governor.enqueue_replay(9, 9, vec![payload(8, 4)], &config, now);
        assert!(governor.pump(&config, now).is_empty());
        let batches = governor.pump(&config, now + Duration::from_secs(2));
        assert_eq!(
            batches[0]
                .1
                .iter()
                .map(|p| p.payload.read().as_ref().unwrap()[0])
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        governor.finish_replay_batch(9, &9, Vec::new());
        let batches = governor.pump(&config, now + Duration::from_secs(3));
        assert_eq!(batches[0].1[0].payload.read().as_ref().unwrap()[0], 4);
        governor.finish_replay_batch(9, &9, Vec::new());
        assert!(governor
            .replays
            .lock()
            .values()
            .flatten()
            .all(|job| job.payloads.is_empty()));
    }

    #[test]
    fn replay_large_payload_progress_disable_and_departure() {
        let governor = ImageBandwidthGovernor::<()>::default();
        let now = Instant::now();
        let mut config = config();
        let large = PendingPayload {
            owner: 1,
            payload: ReplayPayload::new(Bytes::from(vec![1; 500_000])),
        };
        governor.enqueue_replay(1, (), vec![large.clone(), large], &config, now);
        let batch = governor.pump(&config, now);
        assert_eq!(batch[0].1.len(), 1);
        governor.finish_replay_batch(1, &(), Vec::new());
        assert!(governor
            .pump(&config, now + Duration::from_secs(2))
            .is_empty());
        let batch = governor.pump(&config, now + Duration::from_secs(3));
        assert_eq!(batch[0].1.len(), 1);
        governor.finish_replay_batch(1, &(), Vec::new());
        governor.enqueue_replay(1, (), vec![payload(1, 1)], &config, now);
        governor.remove_peer(1, &());
        assert!(governor.pump(&config, now).is_empty());
        governor.enqueue_replay(1, (), vec![payload(1, 1)], &config, now);
        config.image_share_download_megabits_per_second = 0;
        assert!(governor.pump(&config, now).is_empty());
        assert!(governor.replays.lock().is_empty());
        assert_eq!(
            governor
                .enqueue_replay(1, (), vec![payload(1, 1)], &config, now)
                .len(),
            1
        );
    }

    #[test]
    fn transport_retries_survive_when_image_pacing_is_disabled_and_keep_order() {
        let config = config();
        let governor = ImageBandwidthGovernor::<u16>::default();
        let now = Instant::now();
        let first = governor.enqueue_inline(7, 7, vec![payload(1, 1)], &config, now);
        assert_eq!(first.len(), 1);
        governor.finish_replay_batch(7, &7, vec![payload(1, 2)]);
        assert_eq!(governor.queued_replay_bytes(), 125_000);

        let batches = governor.pump(&config, now);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].0, 7);
        assert_eq!(
            batches[0]
                .1
                .iter()
                .map(|pending| pending.payload.read().as_ref().unwrap()[0])
                .collect::<Vec<_>>(),
            [2]
        );
        governor.finish_replay_batch(7, &7, Vec::new());
        assert_eq!(governor.queued_replay_bytes(), 0);
    }

    #[test]
    fn refused_suffix_precedes_later_inline_work_without_duplicating_prefix() {
        let mut config = config();
        config.image_share_download_megabits_per_second = 0;
        let governor = ImageBandwidthGovernor::<u16>::default();
        let now = Instant::now();
        let initial =
            governor.enqueue_inline(7, 7, vec![payload(1, 10), payload(1, 11)], &config, now);
        assert_eq!(
            initial
                .iter()
                .map(|p| p.payload.read().as_ref().unwrap()[0])
                .collect::<Vec<_>>(),
            [10, 11]
        );

        // 10 was accepted; 11 was refused. A later inline replay cannot pass it.
        governor.finish_replay_batch(7, &7, vec![payload(1, 11)]);
        let later = governor.enqueue_inline(7, 7, vec![payload(1, 12)], &config, now);
        assert_eq!(
            later
                .iter()
                .map(|p| p.payload.read().as_ref().unwrap()[0])
                .collect::<Vec<_>>(),
            [11, 12]
        );
        governor.finish_replay_batch(7, &7, Vec::new());
        assert_eq!(governor.queued_replay_bytes(), 0);
    }

    #[test]
    fn late_old_session_retry_cannot_replace_or_mix_reused_peer_work() {
        let config = config();
        let governor = ImageBandwidthGovernor::<u16>::default();
        let now = Instant::now();
        let old = governor.enqueue_inline(7, 1, vec![payload(1, 20)], &config, now);
        assert_eq!(old.len(), 1);
        governor.remove_peer(7, &1);

        assert!(governor
            .enqueue_replay(7, 2, vec![payload(2, 30)], &config, now)
            .is_empty());
        // Completion from the removed connection is delayed until after ID reuse.
        governor.finish_replay_batch(7, &1, vec![payload(1, 21)]);
        assert!(governor
            .enqueue_replay(7, 3, vec![payload(3, 40)], &config, now)
            .is_empty());
        let batches = governor.pump(&config, now);
        assert_eq!(batches.len(), 2);
        assert!(batches.iter().any(|(target, batch)| *target == 2
            && batch
                .iter()
                .map(|p| p.payload.read().as_ref().unwrap()[0])
                .collect::<Vec<_>>()
                == [30]));
        assert!(batches.iter().any(|(target, batch)| *target == 3
            && batch
                .iter()
                .map(|p| p.payload.read().as_ref().unwrap()[0])
                .collect::<Vec<_>>()
                == [40]));
        governor.finish_replay_batch(7, &2, Vec::new());
        governor.finish_replay_batch(7, &3, Vec::new());
    }

    #[test]
    fn retry_suffix_remains_ahead_of_new_paced_work_while_batch_is_in_flight() {
        let governor = ImageBandwidthGovernor::<u16>::default();
        let now = Instant::now();
        let config = config();
        let first = governor.enqueue_inline(7, 7, vec![payload(1, 50)], &config, now);
        assert_eq!(first.len(), 1);
        // A second producer queues paced work while the first batch is awaiting completion.
        assert!(governor
            .enqueue_replay(7, 7, vec![payload(1, 52)], &config, now)
            .is_empty());
        governor.finish_replay_batch(7, &7, vec![payload(1, 51)]);
        let next = governor.pump(&config, now + Duration::from_secs(2));
        assert_eq!(
            next[0]
                .1
                .iter()
                .map(|p| p.payload.read().as_ref().unwrap()[0])
                .collect::<Vec<_>>(),
            [51, 52]
        );
        governor.finish_replay_batch(7, &7, Vec::new());
    }

    #[test]
    fn successive_downloads_keep_spent_credit_and_debt_after_draining() {
        let governor = ImageBandwidthGovernor::<()>::default();
        let now = Instant::now();
        let config = config();
        governor.enqueue_replay(9, (), vec![payload(7, 0), payload(7, 1)], &config, now);
        assert_eq!(governor.pump(&config, now)[0].1.len(), 2);
        governor.finish_replay_batch(9, &(), Vec::new());
        governor.enqueue_replay(9, (), vec![payload(7, 2)], &config, now);
        assert!(governor.pump(&config, now).is_empty());
        assert!(
            governor.pump(&config, now + Duration::from_millis(25))[0]
                .1
                .len()
                == 1
        );
        governor.finish_replay_batch(9, &(), Vec::new());
        // The preceding oversized charge leaves debt even though the queue emptied.
        governor.enqueue_replay(
            9,
            (),
            vec![payload(7, 3)],
            &config,
            now + Duration::from_millis(25),
        );
        assert!(governor
            .pump(&config, now + Duration::from_millis(50))
            .is_empty());
        assert!(governor
            .pump(&config, now + Duration::from_secs(1))
            .is_empty());
        assert_eq!(
            governor.pump(&config, now + Duration::from_millis(1001))[0]
                .1
                .len(),
            1
        );
        governor.finish_replay_batch(9, &(), Vec::new());
        governor.remove_peer(9, &());
        governor.enqueue_replay(9, (), vec![payload(7, 4), payload(7, 5)], &config, now);
        assert_eq!(governor.pump(&config, now)[0].1.len(), 2);
    }
}
