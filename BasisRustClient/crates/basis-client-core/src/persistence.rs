//! Opt-in 16 KiB storage writes with correlated readback verification.
use crate::{client::BasisClient, simulation::cadence_seed};
use anyhow::Result;
use basis_protocol::{channels, storage::StorageOp, NetReader, NetWriter};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex, Notify},
    time,
};

pub(crate) const VALUE_BYTES: usize = 16_384;
const TIMEOUT: Duration = Duration::from_secs(10);
const HISTOGRAM_US: u64 = 100;
const HISTOGRAM_BINS: usize = 200_001; // bounded 0.1ms bins through 20s

#[derive(Debug)]
struct Histogram {
    bins: Vec<u64>,
    count: u64,
    overflow: u64,
}
impl Default for Histogram {
    fn default() -> Self {
        Self {
            bins: vec![0; HISTOGRAM_BINS],
            count: 0,
            overflow: 0,
        }
    }
}
impl Histogram {
    fn record(&mut self, elapsed: Duration) {
        let bucket = (elapsed.as_micros() / HISTOGRAM_US as u128) as usize;
        self.overflow += u64::from(bucket >= HISTOGRAM_BINS);
        self.bins[bucket.min(HISTOGRAM_BINS - 1)] += 1;
        self.count += 1;
    }
    fn percentile_ms(&self, percentile: u64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        let target = (self.count * percentile).div_ceil(100);
        let mut seen = 0;
        for (index, count) in self.bins.iter().enumerate() {
            seen += count;
            if seen >= target {
                return (index as u64 * HISTOGRAM_US) as f64 / 1000.0;
            }
        }
        unreachable!()
    }
}

#[derive(Debug, Default)]
struct Metrics {
    sets_sent: u64,
    sets_ok: u64,
    gets_sent: u64,
    gets_ok: u64,
    verified: u64,
    statuses: [u64; 11],
    corrupt: u64,
    malformed: u64,
    send_errors: u64,
    timeouts: u64,
    canceled: u64,
    stale: u64,
    backpressure: u64,
    warmup_failures: u64,
    writers: HashSet<usize>,
    verified_writers: HashSet<usize>,
    set_latency: Histogram,
    get_latency: Histogram,
    cycle_latency: Histogram,
}
#[derive(Debug)]
struct Pending {
    op: StorageOp,
    id: u16,
    sequence: u64,
    cycle: Instant,
    phase: Instant,
    measured: bool,
}
#[derive(Debug)]
struct Connection {
    generation: i64,
    next_due: Instant,
    next_id: u16,
    sequence: u64,
    pending: Option<Pending>,
}
impl Connection {
    fn id(&mut self) -> u16 {
        self.next_id = self.next_id.wrapping_add(1).max(1);
        self.next_id
    }
}
#[derive(Debug, Default)]
struct State {
    connections: HashMap<usize, Connection>,
    metrics: Metrics,
    measurement_start: Option<Instant>,
}
#[derive(Debug)]
pub(crate) struct PersistenceSession {
    enabled: bool,
    interval: Duration,
    start_file: Option<PathBuf>,
    window: Option<Duration>,
    state: StdMutex<State>,
}

fn value(index: usize, generation: i64, sequence: u64) -> Vec<u8> {
    let mut bytes = vec![0; VALUE_BYTES];
    let mut seed = cadence_seed(index, sequence) ^ generation as u64;
    for chunk in bytes.chunks_mut(8) {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        chunk.copy_from_slice(&seed.to_le_bytes()[..chunk.len()]);
    }
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    bytes[8..16].copy_from_slice(&generation.to_le_bytes());
    bytes[16..24].copy_from_slice(&sequence.to_le_bytes());
    bytes
}
fn request(op: StorageOp, id: u16, bytes: &[u8]) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(bytes.len() + 32);
    writer.put_u8(op as u8);
    writer.put_u16(id);
    writer.put_bytes_with_length(b"load-test").unwrap();
    writer.put_bytes_with_length(b"state").unwrap();
    if op == StorageOp::Set {
        writer.put_bytes_with_length(bytes).unwrap();
    }
    writer.into_vec()
}
struct Reply<'a> {
    op: StorageOp,
    id: u16,
    status: usize,
    value: &'a [u8],
}
fn reply(payload: &[u8]) -> Option<Reply<'_>> {
    let mut reader = NetReader::new(payload);
    let op = StorageOp::try_from(reader.get_u8().ok()?).ok()?;
    if !matches!(op, StorageOp::Set | StorageOp::Get) {
        return None;
    }
    let id = reader.get_u16().ok()?;
    let status = reader.get_u8().ok()? as usize;
    if id == 0 || status > 10 || reader.get_i32().ok()? < 0 || reader.get_i32().ok()? < 0 {
        return None;
    }
    let value = if status == 0 && op == StorageOp::Get {
        let size = reader.get_u16().ok()? as usize;
        reader.get_bytes(size).ok()?
    } else {
        &[]
    };
    (reader.remaining() == 0).then_some(Reply {
        op,
        id,
        status,
        value,
    })
}

impl PersistenceSession {
    pub(crate) fn new(
        enabled: bool,
        interval: Duration,
        start_file: Option<PathBuf>,
        window: Option<Duration>,
    ) -> Self {
        Self {
            enabled,
            interval,
            start_file,
            window,
            state: StdMutex::new(State::default()),
        }
    }
    fn measurement(&self, state: &mut State, now: Instant) -> (bool, bool) {
        if state.measurement_start.is_none()
            && self.start_file.as_ref().is_none_or(|path| path.exists())
        {
            state.measurement_start = Some(now);
        }
        match state.measurement_start {
            Some(start) => {
                let finished = self
                    .window
                    .is_some_and(|window| now.duration_since(start) >= window);
                (!finished, finished)
            }
            None => (false, false),
        }
    }
    pub(crate) fn cancel(&self, index: usize, generation: i64) {
        let mut state = self.state.lock().unwrap();
        if state
            .connections
            .get(&index)
            .is_some_and(|connection| connection.generation == generation)
        {
            if let Some(connection) = state.connections.remove(&index) {
                if let Some(pending) = connection.pending {
                    if pending.measured {
                        state.metrics.canceled += 1;
                    } else {
                        state.metrics.warmup_failures += 1;
                    }
                }
            }
        }
    }
    fn send_failed(&self, index: usize, generation: i64, id: u16) {
        let mut state = self.state.lock().unwrap();
        if let Some(connection) = state
            .connections
            .get_mut(&index)
            .filter(|connection| connection.generation == generation)
        {
            if connection
                .pending
                .as_ref()
                .is_some_and(|pending| pending.id == id)
            {
                let pending = connection.pending.take().unwrap();
                if pending.measured {
                    state.metrics.send_errors += 1;
                } else {
                    state.metrics.warmup_failures += 1;
                }
            }
        }
    }
    pub(crate) async fn observe(&self, client: &BasisClient, payload: &[u8]) {
        let now = Instant::now();
        let next = {
            let mut state = self.state.lock().unwrap();
            let State {
                connections,
                metrics,
                ..
            } = &mut *state;
            let Some(connection) = connections
                .get_mut(&client.index)
                .filter(|connection| connection.generation == client.connect_time)
            else {
                return;
            };
            let Some(parsed) = reply(payload) else {
                metrics.malformed += 1;
                return;
            };
            if !connection
                .pending
                .as_ref()
                .is_some_and(|pending| pending.id == parsed.id && pending.op == parsed.op)
            {
                metrics.stale += 1;
                return;
            }
            let mut pending = connection.pending.take().unwrap();
            if pending.measured {
                metrics.statuses[parsed.status] += 1;
                if parsed.op == StorageOp::Set {
                    metrics
                        .set_latency
                        .record(now.duration_since(pending.phase));
                } else {
                    metrics
                        .get_latency
                        .record(now.duration_since(pending.phase));
                }
            } else if parsed.status != 0 {
                metrics.warmup_failures += 1;
            }
            if parsed.status != 0 {
                return;
            }
            if parsed.op == StorageOp::Set {
                if pending.measured {
                    metrics.sets_ok += 1;
                    metrics.gets_sent += 1;
                }
                let id = connection.id();
                pending.id = id;
                pending.op = StorageOp::Get;
                pending.phase = now;
                connection.pending = Some(pending);
                Some((id, request(StorageOp::Get, id, &[])))
            } else {
                let correct =
                    parsed.value == value(client.index, client.connect_time, pending.sequence);
                if pending.measured {
                    metrics.gets_ok += 1;
                    if correct {
                        metrics.verified += 1;
                        metrics.verified_writers.insert(client.index);
                        metrics
                            .cycle_latency
                            .record(now.duration_since(pending.cycle));
                    } else {
                        metrics.corrupt += 1;
                    }
                } else if !correct {
                    metrics.warmup_failures += 1;
                }
                None
            }
        };
        if let Some((id, bytes)) = next {
            if client
                .send_reliable_ordered(channels::STORAGE, &bytes)
                .await
                .is_err()
            {
                self.send_failed(client.index, client.connect_time, id);
            }
        }
    }
    pub(crate) fn summary(&self) -> String {
        let state = self.state.lock().unwrap();
        let m = &state.metrics;
        format!("sets={} verified={} failed_replies={} timeouts={} corrupt={} send_errors={} canceled={} cycle_p95_ms={:.3}",m.sets_sent,m.verified,m.statuses[1..].iter().sum::<u64>(),m.timeouts,m.corrupt,m.send_errors,m.canceled,m.cycle_latency.percentile_ms(95))
    }

    pub(crate) fn write_csv(&self, path: &Path) -> Result<()> {
        let state = self.state.lock().unwrap();
        let m = &state.metrics;
        let duration = state
            .measurement_start
            .map(|start| start.elapsed().min(self.window.unwrap_or(Duration::MAX)))
            .unwrap_or_default();
        let pending = state
            .connections
            .values()
            .filter(|c| c.pending.as_ref().is_some_and(|p| p.measured))
            .count();
        let headers = "duration_secs,payload_bytes,interval_secs,sets_sent,sets_ok,gets_sent,gets_ok,verified,writers,verified_writers,not_found,denied,full,rate_limited,invalid,unavailable,busy,timed_out,not_connected,not_allowed,corrupt,malformed,send_errors,timeouts,canceled,stale,backpressure,warmup_failures,pending,set_p50_ms,set_p95_ms,set_p99_ms,get_p50_ms,get_p95_ms,get_p99_ms,cycle_p50_ms,cycle_p95_ms,cycle_p99_ms,latency_overflow";
        let mut fields = vec![
            format!("{:.6}", duration.as_secs_f64()),
            VALUE_BYTES.to_string(),
            self.interval.as_secs_f64().to_string(),
            m.sets_sent.to_string(),
            m.sets_ok.to_string(),
            m.gets_sent.to_string(),
            m.gets_ok.to_string(),
            m.verified.to_string(),
            m.writers.len().to_string(),
            m.verified_writers.len().to_string(),
        ];
        fields.extend(m.statuses[1..].iter().map(u64::to_string));
        fields.extend(
            [
                m.corrupt,
                m.malformed,
                m.send_errors,
                m.timeouts,
                m.canceled,
                m.stale,
                m.backpressure,
                m.warmup_failures,
                pending as u64,
            ]
            .iter()
            .map(u64::to_string),
        );
        for hist in [&m.set_latency, &m.get_latency, &m.cycle_latency] {
            for percentile in [50, 95, 99] {
                fields.push(format!("{:.3}", hist.percentile_ms(percentile)));
            }
        }
        fields.push(
            (m.set_latency.overflow + m.get_latency.overflow + m.cycle_latency.overflow)
                .to_string(),
        );
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, format!("{headers}\n{}\n", fields.join(",")))?;
        Ok(())
    }
}

pub(crate) async fn run(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    shutdown: Arc<AtomicBool>,
    stop: Arc<Notify>,
    session: Arc<PersistenceSession>,
) {
    let mut ticker = time::interval(Duration::from_millis(25));
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    loop {
        tokio::select! { _ = stop.notified() => break, _ = ticker.tick() => {} }
        if shutdown.load(Ordering::Acquire) {
            break;
        }
        let now = Instant::now();
        {
            let mut state = session.state.lock().unwrap();
            session.measurement(&mut state, now);
        }
        if !session.enabled {
            continue;
        }
        let snapshot = clients.lock().await.clone();
        for client in snapshot {
            if shutdown.load(Ordering::Acquire) {
                break;
            }
            if !client.in_use.load(Ordering::Acquire)
                || !client.connected.load(Ordering::Acquire)
                || client.metadata_state().is_none()
            {
                continue;
            }
            let backpressure = client.pending_reliable.lock().await.len() >= 256;
            let now = Instant::now();
            let send = {
                let mut state = session.state.lock().unwrap();
                let (measured, finished) = session.measurement(&mut state, now);
                let State {
                    connections,
                    metrics,
                    ..
                } = &mut *state;
                if connections
                    .get(&client.index)
                    .is_some_and(|connection| connection.generation != client.connect_time)
                {
                    if let Some(pending) = connections
                        .remove(&client.index)
                        .and_then(|connection| connection.pending)
                    {
                        if pending.measured {
                            metrics.canceled += 1;
                        } else {
                            metrics.warmup_failures += 1;
                        }
                    }
                }
                let connection = connections
                    .entry(client.index)
                    .or_insert_with(|| Connection {
                        generation: client.connect_time,
                        next_due: now
                            + Duration::from_micros(
                                (cadence_seed(client.index, 91)
                                    % session.interval.as_micros() as u64)
                                    .max(1),
                            ),
                        next_id: 0,
                        sequence: 0,
                        pending: None,
                    });
                if connection.generation != client.connect_time {
                    continue;
                }
                if connection
                    .pending
                    .as_ref()
                    .is_some_and(|pending| now.duration_since(pending.phase) >= TIMEOUT)
                {
                    let pending = connection.pending.take().unwrap();
                    if pending.measured {
                        metrics.timeouts += 1;
                    } else {
                        metrics.warmup_failures += 1;
                    }
                }
                if finished || connection.pending.is_some() || now < connection.next_due {
                    continue;
                }
                if backpressure {
                    if measured {
                        metrics.backpressure += 1;
                    }
                    continue;
                }
                let id = connection.id();
                let sequence = connection.sequence;
                connection.sequence = connection.sequence.wrapping_add(1);
                connection.next_due = now + session.interval;
                connection.pending = Some(Pending {
                    op: StorageOp::Set,
                    id,
                    sequence,
                    cycle: now,
                    phase: now,
                    measured,
                });
                if measured {
                    metrics.sets_sent += 1;
                    metrics.writers.insert(client.index);
                }
                Some((
                    id,
                    request(
                        StorageOp::Set,
                        id,
                        &value(client.index, client.connect_time, sequence),
                    ),
                ))
            };
            if let Some((id, bytes)) = send {
                if client
                    .send_reliable_ordered(channels::STORAGE, &bytes)
                    .await
                    .is_err()
                {
                    session.send_failed(client.index, client.connect_time, id);
                }
            }
        }
    }
    let mut state = session.state.lock().unwrap();
    let pending: Vec<_> = state
        .connections
        .values_mut()
        .filter_map(|connection| connection.pending.take())
        .collect();
    for pending in pending {
        if pending.measured {
            state.metrics.canceled += 1;
        } else {
            state.metrics.warmup_failures += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use basis_protocol::storage::{read_request, write_reply, StorageReply, StorageStatus};
    #[test]
    fn storage_payload_is_exact_size_varies_and_uses_real_protocol() {
        let bytes = value(7, 99, 1);
        assert_eq!(bytes.len(), 16_384);
        assert_ne!(bytes, value(7, 99, 2));
        assert_ne!(bytes, value(7, 100, 1));
        let parsed = read_request(&request(StorageOp::Set, 12, &bytes)).unwrap();
        assert_eq!(parsed.value, bytes);
        assert_eq!(parsed.id, 12);
        assert_eq!(parsed.scope, "load-test");
        let mut reply_bytes = write_reply(&StorageReply {
            op: StorageOp::Get,
            id: 13,
            status: StorageStatus::Ok,
            used: 16404,
            limit: 32768,
            value: bytes.clone(),
            keys: vec![],
        });
        assert_eq!(reply(&reply_bytes).unwrap().value, bytes);
        reply_bytes.pop();
        assert!(reply(&reply_bytes).is_none());
    }
    #[tokio::test]
    async fn replies_are_correlated_and_readbacks_are_verified_before_success() {
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client = crate::tests::test_client(1, socket.local_addr().unwrap()).await;
        let session = PersistenceSession::new(true, Duration::from_secs(1), None, None);
        let now = Instant::now();
        session.state.lock().unwrap().connections.insert(
            1,
            Connection {
                generation: client.connect_time,
                next_due: now,
                next_id: 3,
                sequence: 1,
                pending: Some(Pending {
                    op: StorageOp::Set,
                    id: 3,
                    sequence: 0,
                    cycle: now,
                    phase: now,
                    measured: true,
                }),
            },
        );
        let mut response = StorageReply {
            op: StorageOp::Set,
            id: 2,
            status: StorageStatus::Ok,
            used: 16404,
            limit: 32768,
            value: vec![],
            keys: vec![],
        };
        session.observe(&client, &write_reply(&response)).await;
        assert_eq!(session.state.lock().unwrap().metrics.stale, 1);
        response.id = 3;
        session.observe(&client, &write_reply(&response)).await;
        response.op = StorageOp::Get;
        response.id = 4;
        response.value = value(1, client.connect_time, 0);
        session.observe(&client, &write_reply(&response)).await;
        assert_eq!(session.state.lock().unwrap().metrics.verified, 1);
        assert!(session.state.lock().unwrap().connections[&1]
            .pending
            .is_none());
        session
            .state
            .lock()
            .unwrap()
            .connections
            .get_mut(&1)
            .unwrap()
            .pending = Some(Pending {
            op: StorageOp::Get,
            id: 5,
            sequence: 1,
            cycle: now,
            phase: now,
            measured: true,
        });
        response.id = 5; // old bytes, successful protocol status: integrity must still fail.
        session.observe(&client, &write_reply(&response)).await;
        assert_eq!(session.state.lock().unwrap().metrics.corrupt, 1);
        session.cancel(1, client.connect_time + 1);
        assert!(session.state.lock().unwrap().connections.contains_key(&1));
        session.cancel(1, client.connect_time);
        assert!(!session.state.lock().unwrap().connections.contains_key(&1));
    }
    #[test]
    fn measurement_window_excludes_warmup_and_histograms_are_bounded() {
        let dir =
            std::env::temp_dir().join(format!("basis-persistence-window-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&dir).unwrap();
        let marker = dir.join("start");
        let session = PersistenceSession::new(
            true,
            Duration::from_secs(1),
            Some(marker.clone()),
            Some(Duration::from_secs(2)),
        );
        let now = Instant::now();
        let mut state = State::default();
        assert_eq!(session.measurement(&mut state, now), (false, false));
        std::fs::write(marker, []).unwrap();
        assert_eq!(session.measurement(&mut state, now), (true, false));
        assert_eq!(
            session.measurement(&mut state, now + Duration::from_secs(2)),
            (false, true)
        );
        let mut hist = Histogram::default();
        hist.record(Duration::from_micros(550));
        assert_eq!(hist.percentile_ms(95), 0.5);
        hist.record(Duration::from_secs(30));
        assert_eq!(hist.overflow, 1);
        assert_eq!(hist.bins.len(), HISTOGRAM_BINS);
        let csv = dir.join("nested/results/persistence.csv");
        session.write_csv(&csv).unwrap();
        assert!(std::fs::read_to_string(csv).unwrap().contains("sets_sent"));
        std::fs::remove_dir_all(dir).unwrap();
    }
}
