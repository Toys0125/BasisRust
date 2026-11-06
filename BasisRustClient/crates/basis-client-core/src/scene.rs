//! Opaque prop/scene script relay workload. No Unity script execution is simulated.
use crate::client::BasisClient;
use anyhow::Result;
use basis_protocol::{
    channels,
    io::NetWriter,
    messages::{BasisSerialize, SceneDataMessage},
};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, Notify},
    time,
};

pub(crate) const MESSAGE_INDEX: u16 = 60000;
const MAGIC: &[u8; 4] = b"BSCR";
const SENDER_STATE_TTL: Duration = Duration::from_secs(10 * 60);
const SENDER_EXPIRY_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
fn unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}
fn script_payload(size: usize, sender: usize, sequence: u64, timestamp: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(sender as u32).to_le_bytes());
    bytes.extend_from_slice(&sequence.to_le_bytes());
    bytes.extend_from_slice(&timestamp.to_le_bytes());
    bytes.extend((24..size).map(|i| {
        (i as u8)
            .wrapping_add(sequence as u8)
            .wrapping_add(sender as u8)
    }));
    bytes
}
#[derive(Debug, Default)]
struct Metrics {
    started: Option<Instant>,
    sent: u64,
    expected: u64,
    send_errors: u64,
    backpressure: u64,
    received: u64,
    malformed: u64,
    duplicates: u64,
    out_of_order: u64,
    // Valid sender IDs are dense client indices below max_senders, so this
    // cumulative coverage set grows only with the published client population.
    covered_senders: HashSet<usize>,
    senders: HashMap<usize, SenderState>,
    next_expiry_sweep: Option<Instant>,
    expiry_sweeps: u64,
    // Bounded log2 microsecond histogram; avoid retaining one sample per message.
    latency: [u64; 32],
    max_latency_us: u64,
}
#[derive(Debug)]
struct SenderState {
    highest: u64,
    seen: BTreeSet<u64>,
    last_seen: Instant,
}
#[derive(Debug)]
pub(crate) struct SceneSession {
    size: usize,
    max_senders: AtomicUsize,
    marker: Option<PathBuf>,
    metrics: StdMutex<Metrics>,
}
impl SceneSession {
    pub(crate) fn new(size: usize, marker: Option<PathBuf>, client_count: usize) -> Self {
        Self {
            size,
            // The observer tracks workload sender indices, so its state need only cover the
            // configured client population (including the observer slot).
            max_senders: AtomicUsize::new(client_count.max(1)),
            marker,
            metrics: StdMutex::new(Metrics::default()),
        }
    }
    fn active(&self) -> bool {
        self.marker.as_ref().is_none_or(|p| p.exists())
    }
    pub(crate) fn observe(&self, bytes: &[u8]) {
        self.observe_at(bytes, Instant::now());
    }
    pub(crate) fn update_sender_capacity(&self, client_count: usize) {
        self.max_senders
            .fetch_max(client_count.max(1), Ordering::AcqRel);
    }
    #[cfg(test)]
    pub(crate) fn sender_capacity(&self) -> usize {
        self.max_senders.load(Ordering::Acquire)
    }
    fn observe_at(&self, bytes: &[u8], now: Instant) {
        if !self.active() {
            return;
        }
        let mut m = self.metrics.lock().unwrap();
        if bytes.len() != self.size + 4
            || bytes[2..4] != MESSAGE_INDEX.to_le_bytes()
            || &bytes[4..8] != MAGIC
        {
            m.malformed += 1;
            return;
        }
        let sender = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let max_senders = self.max_senders.load(Ordering::Acquire);
        if sender == 0 || sender >= max_senders {
            m.malformed += 1;
            return;
        }
        let seq = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        let stamp = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
        if bytes[4..] != script_payload(self.size, sender, seq, stamp) || stamp > unix_micros() {
            m.malformed += 1;
            return;
        }
        // Server peer IDs can change on reconnect and be reused by other senders.
        // The workload's stable sender index owns its sequence counter.
        if m.next_expiry_sweep.is_none_or(|deadline| now >= deadline) {
            m.senders.retain(|_, state| {
                now.saturating_duration_since(state.last_seen) <= SENDER_STATE_TTL
            });
            m.next_expiry_sweep = Some(now + SENDER_EXPIRY_SWEEP_INTERVAL);
            m.expiry_sweeps = m.expiry_sweeps.saturating_add(1);
        }
        if !m.senders.contains_key(&sender) && m.senders.len() >= max_senders {
            if let Some(oldest) = m
                .senders
                .iter()
                .min_by_key(|(_, state)| state.last_seen)
                .map(|(sender, _)| *sender)
            {
                m.senders.remove(&oldest);
            }
        }
        let state = m.senders.entry(sender).or_insert_with(|| SenderState {
            highest: seq,
            seen: BTreeSet::new(),
            last_seen: now,
        });
        let highest = state.highest;
        // Keep a bounded 256-sequence replay window per sender. Older arrivals cannot
        // establish unique delivery, so they are excluded conservatively.
        if seq.saturating_add(256) <= highest || !state.seen.insert(seq) {
            m.duplicates += 1;
            return;
        }
        let newest = highest.max(seq);
        state.seen.retain(|s| s.saturating_add(256) > newest);
        state.last_seen = now;
        state.highest = newest;
        m.covered_senders.insert(sender);
        if seq < highest {
            m.out_of_order += 1;
        }
        m.received += 1;
        let latency = unix_micros().saturating_sub(stamp);
        m.max_latency_us = m.max_latency_us.max(latency);
        let bucket = (64 - latency.leading_zeros()).min(31) as usize;
        m.latency[bucket] += 1;
    }
    pub(crate) fn write_csv(&self, path: &Path) -> Result<()> {
        let m = self.metrics.lock().unwrap();
        let seconds = m.started.map(|t| t.elapsed().as_secs_f64()).unwrap_or(0.0);
        let quantile = |percent: u64| {
            let target = (m.received * percent).div_ceil(100);
            if target == 0 {
                return 0;
            }
            let mut count = 0;
            for (i, n) in m.latency.iter().enumerate() {
                count += n;
                if count >= target {
                    return (1u64 << i).saturating_sub(1);
                }
            }
            0
        };
        let csv = format!("metric,value\nwindow_seconds,{seconds:.6}\nsent_messages,{}\nexpected_observer_messages,{}\nreceived_messages,{}\nreceived_bytes,{}\nobserved_senders,{}\nsend_errors,{}\nbackpressure_skips,{}\nmalformed_messages,{}\nduplicate_messages,{}\nout_of_order_messages,{}\nlatency_p50_upper_us,{}\nlatency_p95_upper_us,{}\nlatency_p99_upper_us,{}\nlatency_max_us,{}\n", m.sent, m.expected, m.received, m.received * self.size as u64, m.covered_senders.len(), m.send_errors, m.backpressure, m.malformed, m.duplicates, m.out_of_order, quantile(50), quantile(95), quantile(99), m.max_latency_us);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, csv)?;
        Ok(())
    }
}
pub(crate) async fn run(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    shutdown: Arc<AtomicBool>,
    stop: Arc<Notify>,
    session: Arc<SceneSession>,
    interval: Duration,
    reliable: bool,
) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut sequences = HashMap::<usize, u64>::new();
    let mut first_sent = HashSet::<(usize, i64)>::new();
    let mut pending_first_send = false;
    loop {
        let regular_send = tokio::select! {
            biased;
            _ = stop.notified() => break,
            _ = ticker.tick() => true,
            _ = time::sleep(if pending_first_send {
                Duration::from_millis(5)
            } else {
                Duration::from_millis(100)
            }) => false,
        };
        if shutdown.load(Ordering::Relaxed) {
            break;
        }
        if !session.active() {
            continue;
        }
        session
            .metrics
            .lock()
            .unwrap()
            .started
            .get_or_insert_with(Instant::now);
        let snapshot = clients.lock().await.clone();
        let active_connections = snapshot
            .iter()
            .filter(|client| client.in_use.load(Ordering::Acquire))
            .map(|client| (client.index, client.connect_time))
            .collect::<HashSet<_>>();
        first_sent.retain(|connection| active_connections.contains(connection));
        pending_first_send = false;
        for client in snapshot {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            // ConnectAccept precedes DID authentication and the v56 uplink key.
            // Metadata confirms admission, including servers with DID auth disabled.
            if !client.in_use.load(Ordering::Acquire) {
                continue;
            }
            let connection = (client.index, client.connect_time);
            let already_sent = first_sent.contains(&connection);
            if already_sent && !regular_send {
                continue;
            }
            if !client.connected.load(Ordering::Acquire) || client.metadata_state().is_none() {
                pending_first_send |= !already_sent;
                continue;
            }
            if reliable && client.pending_reliable.lock().await.len() >= 256 {
                session.metrics.lock().unwrap().backpressure += 1;
                pending_first_send |= !already_sent;
                continue;
            }
            let sequence = sequences.entry(client.index).or_default();
            let message = SceneDataMessage {
                message_index: MESSAGE_INDEX,
                recipients: Vec::new(),
                payload: script_payload(session.size, client.index, *sequence, unix_micros()),
            };
            *sequence += 1;
            let mut writer = NetWriter::with_capacity(session.size + 4);
            if message.serialize(&mut writer).is_err() {
                session.metrics.lock().unwrap().send_errors += 1;
                continue;
            }
            let result = if reliable {
                client
                    .send_reliable_ordered(channels::SCENE, writer.as_slice())
                    .await
            } else {
                client
                    .send_unreliable(channels::SCENE, writer.as_slice())
                    .await
            };
            let mut m = session.metrics.lock().unwrap();
            if result.is_ok() {
                m.sent += 1;
                if client.index != 0 {
                    m.expected += 1;
                }
                first_sent.insert(connection);
            } else {
                m.send_errors += 1;
                pending_first_send |= !already_sent;
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scene_observer_rejects_corruption_and_counts_coverage_and_replays() {
        let session = SceneSession::new(64, None, 2);
        let mut bytes = vec![7, 0];
        bytes.extend_from_slice(&MESSAGE_INDEX.to_le_bytes());
        bytes.extend(script_payload(64, 1, 10, unix_micros()));
        session.observe(&bytes);
        session.observe(&bytes);
        bytes[30] ^= 1;
        session.observe(&bytes);
        let m = session.metrics.lock().unwrap();
        assert_eq!(
            (m.received, m.duplicates, m.malformed, m.senders.len()),
            (1, 1, 1, 1)
        );
    }
    #[test]
    fn reordered_unique_messages_count_once_and_old_replays_are_excluded() {
        let session = SceneSession::new(24, None, 2);
        for seq in [2, 1, 2, 300, 1] {
            let mut bytes = vec![7, 0];
            bytes.extend_from_slice(&MESSAGE_INDEX.to_le_bytes());
            bytes.extend(script_payload(24, 1, seq, unix_micros()));
            session.observe(&bytes);
        }
        let m = session.metrics.lock().unwrap();
        assert_eq!((m.received, m.duplicates, m.out_of_order), (3, 2, 1));
        assert!(m.senders[&1].seen.len() <= 256);
    }
    #[test]
    fn reconnect_and_peer_id_reuse_preserve_sender_coverage_and_replay_windows() {
        let session = SceneSession::new(24, None, 4);
        for (peer, sender, seq) in [(7u16, 1, 300), (8, 1, 301), (8, 1, 300), (7, 2, 1)] {
            let mut bytes = peer.to_le_bytes().to_vec();
            bytes.extend_from_slice(&MESSAGE_INDEX.to_le_bytes());
            bytes.extend(script_payload(24, sender, seq, unix_micros()));
            session.observe(&bytes);
        }
        let m = session.metrics.lock().unwrap();
        assert_eq!((m.received, m.duplicates, m.senders.len()), (3, 1, 2));
        assert_eq!(m.senders[&1].highest, 301);
        assert_eq!(m.senders[&2].highest, 1);
    }

    #[test]
    fn out_of_range_scene_senders_cannot_displace_replay_state_or_inflate_metrics() {
        let session = SceneSession::new(24, None, 2);
        for (sender, seq) in [
            (1, 1),
            (2, 1),
            (1, 2),
            (3, 1),
            (u32::MAX as usize, 1),
            (1, 1),
        ] {
            let mut bytes = vec![7, 0];
            bytes.extend_from_slice(&MESSAGE_INDEX.to_le_bytes());
            bytes.extend(script_payload(24, sender, seq, unix_micros()));
            session.observe(&bytes);
        }
        let m = session.metrics.lock().unwrap();
        assert_eq!(m.senders.len(), 1);
        assert!(m.senders.contains_key(&1));
        assert_eq!(m.senders[&1].highest, 2);
        assert_eq!((m.received, m.malformed, m.duplicates), (2, 3, 1));
        assert_eq!(m.covered_senders.len(), 1);
        assert_eq!(m.expiry_sweeps, 1);
    }

    #[test]
    fn client_population_growth_tracks_partial_additions_without_eviction() {
        let session = SceneSession::new(24, None, 2);
        let now = Instant::now();
        let observe = |sender, seq, at| {
            let mut bytes = vec![7, 0];
            bytes.extend_from_slice(&MESSAGE_INDEX.to_le_bytes());
            bytes.extend(script_payload(24, sender, seq, unix_micros()));
            session.observe_at(&bytes, at);
        };
        observe(1, 1, now);

        // AddClients may publish a partial first batch, then another partial batch.
        session.update_sender_capacity(3);
        observe(2, 1, now + Duration::from_millis(1));
        session.update_sender_capacity(4);
        observe(3, 1, now + Duration::from_millis(2));

        let m = session.metrics.lock().unwrap();
        assert_eq!(m.senders.len(), 3);
        assert!(m.senders.contains_key(&1));
        assert!(m.senders.contains_key(&2));
        assert!(m.senders.contains_key(&3));
        assert_eq!(m.covered_senders.len(), 3);
    }

    #[test]
    fn expired_replay_state_is_swept_on_schedule_but_coverage_is_cumulative() {
        let session = SceneSession::new(24, None, 3);
        let now = Instant::now();
        let observe = |sender, seq, at| {
            let mut bytes = vec![7, 0];
            bytes.extend_from_slice(&MESSAGE_INDEX.to_le_bytes());
            bytes.extend(script_payload(24, sender, seq, unix_micros()));
            session.observe_at(&bytes, at);
        };
        observe(1, 9, now);
        observe(2, 1, now + Duration::from_millis(1));
        assert_eq!(
            session.metrics.lock().unwrap().next_expiry_sweep,
            Some(now + SENDER_EXPIRY_SWEEP_INTERVAL)
        );
        assert_eq!(session.metrics.lock().unwrap().expiry_sweeps, 1);

        // Advance beyond both the sender TTL and the next maintenance sweep without sleeping.
        let after_expiry = now + SENDER_STATE_TTL + SENDER_EXPIRY_SWEEP_INTERVAL;
        observe(2, 2, after_expiry);

        let m = session.metrics.lock().unwrap();
        assert_eq!(m.received, 3);
        assert_eq!(m.senders.len(), 1);
        assert!(!m.senders.contains_key(&1));
        assert!(m.senders.contains_key(&2));
        assert_eq!(m.covered_senders.len(), 2);
        assert_eq!(m.expiry_sweeps, 2);
    }
}
