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
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::Mutex, time};

pub(crate) const MESSAGE_INDEX: u16 = 60000;
const MAGIC: &[u8; 4] = b"BSCR";
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
    peers: HashSet<usize>,
    last: HashMap<usize, u64>,
    seen: HashMap<usize, BTreeSet<u64>>,
    // Bounded log2 microsecond histogram; avoid retaining one sample per message.
    latency: [u64; 32],
    max_latency_us: u64,
}
#[derive(Debug)]
pub(crate) struct SceneSession {
    size: usize,
    marker: Option<PathBuf>,
    metrics: StdMutex<Metrics>,
}
impl SceneSession {
    pub(crate) fn new(size: usize, marker: Option<PathBuf>) -> Self {
        Self {
            size,
            marker,
            metrics: StdMutex::new(Metrics::default()),
        }
    }
    fn active(&self) -> bool {
        self.marker.as_ref().is_none_or(|p| p.exists())
    }
    pub(crate) fn observe(&self, bytes: &[u8]) {
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
        let seq = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        let stamp = u64::from_le_bytes(bytes[20..28].try_into().unwrap());
        if bytes[4..] != script_payload(self.size, sender, seq, stamp)
            || sender == 0
            || stamp > unix_micros()
        {
            m.malformed += 1;
            return;
        }
        // Server peer IDs can change on reconnect and be reused by other senders.
        // The workload's stable sender index owns its sequence counter.
        let highest = m.last.get(&sender).copied().unwrap_or(seq);
        let seen = m.seen.entry(sender).or_default();
        // Keep a bounded 256-sequence replay window per sender. Older arrivals cannot
        // establish unique delivery, so they are excluded conservatively.
        if seq.saturating_add(256) <= highest || !seen.insert(seq) {
            m.duplicates += 1;
            return;
        }
        let newest = highest.max(seq);
        seen.retain(|s| s.saturating_add(256) > newest);
        if seq < highest {
            m.out_of_order += 1;
        }
        m.last.insert(sender, newest);
        m.peers.insert(sender);
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
        let csv = format!("metric,value\nwindow_seconds,{seconds:.6}\nsent_messages,{}\nexpected_observer_messages,{}\nreceived_messages,{}\nreceived_bytes,{}\nobserved_senders,{}\nsend_errors,{}\nbackpressure_skips,{}\nmalformed_messages,{}\nduplicate_messages,{}\nout_of_order_messages,{}\nlatency_p50_upper_us,{}\nlatency_p95_upper_us,{}\nlatency_p99_upper_us,{}\nlatency_max_us,{}\n", m.sent, m.expected, m.received, m.received * self.size as u64, m.peers.len(), m.send_errors, m.backpressure, m.malformed, m.duplicates, m.out_of_order, quantile(50), quantile(95), quantile(99), m.max_latency_us);
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
    session: Arc<SceneSession>,
    interval: Duration,
    reliable: bool,
) {
    let mut ticker = time::interval(interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut sequences = HashMap::<usize, u64>::new();
    loop {
        ticker.tick().await;
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
        for client in snapshot {
            if shutdown.load(Ordering::Relaxed) {
                break;
            }
            if !client.connected.load(Ordering::Acquire) || !client.in_use.load(Ordering::Acquire) {
                continue;
            }
            if reliable && client.pending_reliable.lock().await.len() >= 256 {
                session.metrics.lock().unwrap().backpressure += 1;
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
            } else {
                m.send_errors += 1;
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn scene_observer_rejects_corruption_and_counts_coverage_and_replays() {
        let session = SceneSession::new(64, None);
        let mut bytes = vec![7, 0];
        bytes.extend_from_slice(&MESSAGE_INDEX.to_le_bytes());
        bytes.extend(script_payload(64, 1, 10, unix_micros()));
        session.observe(&bytes);
        session.observe(&bytes);
        bytes[30] ^= 1;
        session.observe(&bytes);
        let m = session.metrics.lock().unwrap();
        assert_eq!(
            (m.received, m.duplicates, m.malformed, m.peers.len()),
            (1, 1, 1, 1)
        );
    }
    #[test]
    fn reordered_unique_messages_count_once_and_old_replays_are_excluded() {
        let session = SceneSession::new(24, None);
        for seq in [2, 1, 2, 300, 1] {
            let mut bytes = vec![7, 0];
            bytes.extend_from_slice(&MESSAGE_INDEX.to_le_bytes());
            bytes.extend(script_payload(24, 1, seq, unix_micros()));
            session.observe(&bytes);
        }
        let m = session.metrics.lock().unwrap();
        assert_eq!((m.received, m.duplicates, m.out_of_order), (3, 2, 1));
        assert!(m.seen[&1].len() <= 256);
    }
    #[test]
    fn reconnect_and_peer_id_reuse_preserve_sender_coverage_and_replay_windows() {
        let session = SceneSession::new(24, None);
        for (peer, sender, seq) in [(7u16, 1, 300), (8, 1, 301), (8, 1, 300), (7, 2, 1)] {
            let mut bytes = peer.to_le_bytes().to_vec();
            bytes.extend_from_slice(&MESSAGE_INDEX.to_le_bytes());
            bytes.extend(script_payload(24, sender, seq, unix_micros()));
            session.observe(&bytes);
        }
        let m = session.metrics.lock().unwrap();
        assert_eq!((m.received, m.duplicates, m.peers.len()), (3, 1, 2));
        assert_eq!(m.last[&1], 301);
        assert_eq!(m.last[&2], 1);
    }
}
