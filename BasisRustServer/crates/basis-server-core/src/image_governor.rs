//! Image relay enforcement and paced cache downloads, matching the C# governor.
use crate::image_cache::ReplayPayload;
use basis_protocol::config::ServerConfig;
use basis_transport::PeerId;
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

struct ReplayJob<T> {
    target: T,
    payloads: VecDeque<PendingPayload>,
    bucket: Bucket,
}

// T is the recipient's transport session in production. Keeping it in the job
// prevents queued data reaching a different connection that reuses the peer ID.
pub(crate) struct ImageBandwidthGovernor<T> {
    egress: Mutex<HashMap<PeerId, Bucket>>,
    replays: Mutex<HashMap<PeerId, ReplayJob<T>>>,
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

impl<T: Clone> ImageBandwidthGovernor<T> {
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

    /// Returns the payloads to the caller for inline delivery when pacing is off.
    pub fn enqueue_replay(
        &self,
        peer: PeerId,
        target: T,
        payloads: Vec<PendingPayload>,
        config: &ServerConfig,
        now: Instant,
    ) -> Vec<PendingPayload> {
        let rate = f64::from(config.image_share_download_megabits_per_second) * MEGABITS_TO_BYTES;
        if rate <= 0.0 || payloads.is_empty() {
            return payloads;
        }
        let mut replays = self.replays.lock();
        let job = replays.entry(peer).or_insert_with(|| ReplayJob {
            target: target.clone(),
            payloads: VecDeque::new(),
            bucket: Bucket::new(rate, now),
        });
        job.target = target;
        // Eviction/despawn clears the buffers immediately. Discard their empty
        // handles here so churn cannot accumulate stale queue metadata.
        job.payloads.retain(|pending| pending.payload.len() > 0);
        // Append without renewing credit: repeated pickup requests share a budget.
        job.payloads.extend(payloads);
        Vec::new()
    }

    /// Take one pump pass; transport sends run after releasing the state lock.
    pub fn pump(&self, config: &ServerConfig, now: Instant) -> Vec<(T, Vec<PendingPayload>)> {
        let rate = f64::from(config.image_share_download_megabits_per_second) * MEGABITS_TO_BYTES;
        let mut replays = self.replays.lock();
        if rate <= 0.0 {
            replays.clear();
            return Vec::new();
        }
        let mut batches = Vec::new();
        for job in replays.values_mut() {
            job.payloads.retain(|pending| pending.payload.len() > 0);
            if job.payloads.is_empty() {
                continue;
            }
            job.bucket.refill(rate, now);
            let mut batch = Vec::new();
            while job.bucket.tokens > 0.0 {
                let Some(payload) = job.payloads.pop_front() else {
                    break;
                };
                let size = payload.payload.len();
                if size == 0 {
                    continue;
                }
                job.bucket.tokens -= size as f64;
                batch.push(payload);
            }
            if !batch.is_empty() {
                batches.push((job.target.clone(), batch));
            }
            // Keep credit/debt after the queue empties. A new request must not
            // receive a fresh burst until the recipient disconnects.
        }
        batches
    }

    pub fn remove_peer(&self, peer: PeerId) {
        self.egress.lock().remove(&peer);
        self.replays.lock().remove(&peer);
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
            .flat_map(|job| &job.payloads)
            .map(|pending| pending.payload.len())
            .sum()
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
        governor.remove_peer(1);
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
        let batches = governor.pump(&config, now + Duration::from_secs(3));
        assert_eq!(batches[0].1[0].payload.read().as_ref().unwrap()[0], 4);
        assert!(governor
            .replays
            .lock()
            .values()
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
        assert_eq!(governor.pump(&config, now)[0].1.len(), 1);
        assert!(governor
            .pump(&config, now + Duration::from_secs(2))
            .is_empty());
        assert_eq!(
            governor.pump(&config, now + Duration::from_secs(3))[0]
                .1
                .len(),
            1
        );
        governor.enqueue_replay(1, (), vec![payload(1, 1)], &config, now);
        governor.remove_peer(1);
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
    fn successive_downloads_keep_spent_credit_and_debt_after_draining() {
        let governor = ImageBandwidthGovernor::<()>::default();
        let now = Instant::now();
        let config = config();
        governor.enqueue_replay(9, (), vec![payload(7, 0), payload(7, 1)], &config, now);
        assert_eq!(governor.pump(&config, now)[0].1.len(), 2);
        governor.enqueue_replay(9, (), vec![payload(7, 2)], &config, now);
        assert!(governor.pump(&config, now).is_empty());
        assert!(
            governor.pump(&config, now + Duration::from_millis(25))[0]
                .1
                .len()
                == 1
        );
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
        governor.remove_peer(9);
        governor.enqueue_replay(9, (), vec![payload(7, 4), payload(7, 5)], &config, now);
        assert_eq!(governor.pump(&config, now)[0].1.len(), 2);
    }
}
