//! Benchmark-only Basis static-image share/replay workload.
use crate::client::BasisClient;
use anyhow::{bail, Context, Result};
use basis_protocol::{
    channels,
    io::NetWriter,
    messages::{BasisSerialize, SceneDataMessage},
};
use serde::Serialize;
use std::{
    collections::HashSet,
    fs::File,
    io::Write,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify},
    time,
};

pub const IMAGE_MANAGER_IDENTIFIER: &str = "BasisImagePickupManager";
const CHUNK_BYTES: usize = 16 * 1024;
const CHUNK_HEADER: usize = 25;
const OP_SPAWN: u8 = 1;
const OP_CHUNK: u8 = 2;
const OP_SERVER_CACHE_OFFER: u8 = 9;
const OP_SERVER_CACHE_REQUEST: u8 = 10;

fn fanout_pacing_duration(
    payload_bytes: usize,
    recipients: usize,
    megabits_per_second: u32,
) -> Duration {
    let aggregate_bytes_per_second = f64::from(megabits_per_second) * 1_000_000.0 / 8.0;
    Duration::from_secs_f64(
        payload_bytes as f64 * recipients.max(1) as f64 / aggregate_bytes_per_second,
    )
}

#[derive(Debug, Clone)]
pub struct ImageBenchmarkOptions {
    pub config_path: PathBuf,
    pub ip: String,
    pub port: u16,
    pub image_path: PathBuf,
    pub output_path: PathBuf,
    pub live_start_file: PathBuf,
    pub cache_start_file: PathBuf,
    pub clients: usize,
    pub sharers: usize,
    pub cache_recipients: usize,
    pub egress_megabits_per_second: u32,
}

#[derive(Debug, Clone, Serialize)]
struct Event {
    event: &'static str,
    connected: usize,
    net_id: usize,
    live_completed: usize,
    live_expected: usize,
    cache_completed: usize,
    cache_expected: usize,
    uploads_done: usize,
    send_errors: u64,
    malformed: u64,
    integrity_errors: u64,
    duplicates: u64,
    unique_chunks: u64,
    missing_chunks: u64,
    disconnects: usize,
    fragment_errors: u64,
    fragment_duplicates: u64,
    live_latency_us: Option<LatencySummary>,
    cache_latency_us: Option<LatencySummary>,
    pairs: Option<Vec<PairSummary>>,
}

#[derive(Debug, Clone, Serialize)]
struct LatencySummary {
    min: u64,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}
#[derive(Debug, Clone, Serialize)]
struct PairSummary {
    receiver: usize,
    owner: usize,
    phase: &'static str,
    latency_us: u64,
    unique_chunks: usize,
}

struct Pair {
    expected_cache: bool,
    seen: Vec<bool>,
    received: usize,
    started: Option<Instant>,
    completed_us: Option<u64>,
    phase: u8,
    spawn_seen: bool,
}

struct Metrics {
    pairs: Vec<Pair>,
    connected: usize,
    net_id: usize,
    uploads_done: usize,
    send_errors: u64,
    malformed: u64,
    integrity_errors: u64,
    duplicates: u64,
    unique_chunks: u64,
    disconnects: usize,
    fragment_errors: u64,
    fragment_duplicates: u64,
    live_epoch: Option<Instant>,
    cache_epoch: Option<Instant>,
    offers: HashSet<(usize, usize)>,
    requested_offers: HashSet<(usize, usize)>,
    connected_indices: HashSet<usize>,
    net_id_indices: HashSet<usize>,
}

pub struct ImageBenchmarkSession {
    options: ImageBenchmarkOptions,
    bytes: Arc<Vec<u8>>,
    ids: [[u8; 16]; 3],
    chunks: usize,
    owner_peers: Mutex<Vec<u16>>,
    metrics: Mutex<Metrics>,
    log_lock: Mutex<()>,
    notify: Notify,
}

impl std::fmt::Debug for ImageBenchmarkSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageBenchmarkSession")
            .field("options", &self.options)
            .field("image_bytes", &self.bytes.len())
            .finish()
    }
}

impl ImageBenchmarkSession {
    pub fn new(options: ImageBenchmarkOptions) -> Result<Arc<Self>> {
        anyhow::ensure!(
            options.clients >= 4,
            "image benchmark requires at least 4 clients"
        );
        anyhow::ensure!(
            options.sharers == 3,
            "image benchmark currently requires exactly 3 sharers"
        );
        anyhow::ensure!(
            options.cache_recipients > 0
                && options.cache_recipients + options.sharers <= options.clients,
            "cache recipients must be positive and cannot overlap sharers"
        );
        anyhow::ensure!(
            options.egress_megabits_per_second > 0,
            "egress rate must be positive"
        );
        let bytes = std::fs::read(&options.image_path)
            .with_context(|| format!("reading image {}", options.image_path.display()))?;
        anyhow::ensure!(
            bytes.len() == 5 * 1024 * 1024,
            "fixture must be exactly 5 MiB, got {} bytes",
            bytes.len()
        );
        anyhow::ensure!(
            bytes.starts_with(&[0xff, 0xd8]) && bytes.ends_with(&[0xff, 0xd9]),
            "fixture must be a complete JPEG"
        );
        anyhow::ensure!(
            jpeg_dimensions(&bytes) == Some((2048, 2048)),
            "fixture must be a decodable 2048x2048 JPEG"
        );
        let chunks = bytes.len().div_ceil(CHUNK_BYTES);
        let ids = [guid_for_sharer(0), guid_for_sharer(1), guid_for_sharer(2)];
        let pair_count = options.clients * options.sharers;
        let mut pairs = Vec::with_capacity(pair_count);
        for client in 0..options.clients {
            for owner in 0..options.sharers {
                let expected_cache = client >= options.clients - options.cache_recipients;
                pairs.push(Pair {
                    expected_cache,
                    seen: vec![false; chunks],
                    received: 0,
                    started: None,
                    completed_us: None,
                    phase: 0,
                    spawn_seen: false,
                });
                let _ = owner;
            }
        }
        Ok(Arc::new(Self {
            options,
            bytes: Arc::new(bytes),
            ids,
            chunks,
            owner_peers: Mutex::new(Vec::new()),
            metrics: Mutex::new(Metrics {
                pairs,
                connected: 0,
                net_id: 0,
                uploads_done: 0,
                send_errors: 0,
                malformed: 0,
                integrity_errors: 0,
                duplicates: 0,
                unique_chunks: 0,
                disconnects: 0,
                fragment_errors: 0,
                fragment_duplicates: 0,
                live_epoch: None,
                cache_epoch: None,
                offers: HashSet::new(),
                requested_offers: HashSet::new(),
                connected_indices: HashSet::new(),
                net_id_indices: HashSet::new(),
            }),
            log_lock: Mutex::new(()),
            notify: Notify::new(),
        }))
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        let mut last_pending_log = Instant::now();
        loop {
            let ready = {
                let m = self.metrics.lock().unwrap();
                m.connected == self.options.clients && m.net_id == self.options.clients
            };
            if ready {
                self.log("ready")?;
                return Ok(());
            }
            if Instant::now() >= deadline {
                self.log("ready_pending")?;
                let (connected, net_id) = {
                    let m = self.metrics.lock().unwrap();
                    (m.connected, m.net_id)
                };
                bail!("image benchmark clients not ready: connected={connected} net_id={net_id}");
            }
            if last_pending_log.elapsed() >= Duration::from_secs(2) {
                self.log("ready_pending")?;
                last_pending_log = Instant::now();
            }
            time::timeout(Duration::from_millis(250), self.notify.notified())
                .await
                .ok();
        }
    }

    pub fn note_connected(&self, index: usize) {
        let mut m = self.metrics.lock().unwrap();
        if m.connected_indices.insert(index) {
            m.connected = m.connected_indices.len();
        }
        self.notify.notify_waiters();
    }

    pub fn note_disconnect(&self, _index: usize) {
        self.metrics.lock().unwrap().disconnects += 1;
    }

    pub fn note_fragment_error(&self) {
        self.metrics.lock().unwrap().fragment_errors += 1;
    }
    pub fn note_fragment_duplicate(&self) {
        self.metrics.lock().unwrap().fragment_duplicates += 1;
    }

    pub fn note_net_id(&self, index: usize) {
        let mut m = self.metrics.lock().unwrap();
        if m.net_id_indices.insert(index) {
            m.net_id = m.net_id_indices.len();
        }
        self.notify.notify_waiters();
    }

    pub fn set_owner_peers(&self, clients: &[Arc<BasisClient>]) -> Result<()> {
        let mut ids = Vec::with_capacity(self.options.sharers);
        for client in clients.iter().take(self.options.sharers) {
            ids.push(
                client
                    .remote_peer_id
                    .try_lock()
                    .ok()
                    .and_then(|p| *p)
                    .ok_or_else(|| {
                        anyhow::anyhow!("sharer {} has no server peer id", client.index)
                    })? as u16,
            );
        }
        *self.owner_peers.lock().unwrap() = ids;
        Ok(())
    }

    fn snapshot(&self, event: &'static str) -> Event {
        let m = self.metrics.lock().unwrap();
        let live_expected =
            self.options.sharers * (self.options.clients - self.options.cache_recipients - 1);
        let cache_expected = self.options.sharers * self.options.cache_recipients;
        let live_completed = m
            .pairs
            .iter()
            .filter(|p| !p.expected_cache && p.completed_us.is_some())
            .count();
        let cache_completed = m
            .pairs
            .iter()
            .filter(|p| p.expected_cache && p.completed_us.is_some())
            .count();
        let missing_chunks = m
            .pairs
            .iter()
            .enumerate()
            .filter(|(i, _)| i / self.options.sharers != i % self.options.sharers)
            .map(|(_, p)| self.chunks.saturating_sub(p.received) as u64)
            .sum();
        let final_pairs = (event == "final").then(|| {
            m.pairs
                .iter()
                .enumerate()
                .filter_map(|(i, p)| {
                    p.completed_us.map(|latency_us| PairSummary {
                        receiver: i / self.options.sharers,
                        owner: i % self.options.sharers,
                        phase: if p.expected_cache { "cache" } else { "live" },
                        latency_us,
                        unique_chunks: p.received,
                    })
                })
                .collect::<Vec<_>>()
        });
        let quantiles = |phase: &str| {
            final_pairs.as_ref().and_then(|pairs| {
                let mut values = pairs
                    .iter()
                    .filter(|p| p.phase == phase)
                    .map(|p| p.latency_us)
                    .collect::<Vec<_>>();
                if values.is_empty() {
                    return None;
                }
                values.sort_unstable();
                let q = |pct: usize| values[((values.len() - 1) * pct).div_ceil(100)];
                Some(LatencySummary {
                    min: values[0],
                    p50: q(50),
                    p95: q(95),
                    p99: q(99),
                    max: *values.last().unwrap(),
                })
            })
        };
        Event {
            event,
            connected: m.connected,
            net_id: m.net_id,
            live_completed,
            live_expected,
            cache_completed,
            cache_expected,
            uploads_done: m.uploads_done,
            send_errors: m.send_errors,
            malformed: m.malformed,
            integrity_errors: m.integrity_errors,
            duplicates: m.duplicates,
            unique_chunks: m.unique_chunks,
            missing_chunks,
            disconnects: m.disconnects,
            fragment_errors: m.fragment_errors,
            fragment_duplicates: m.fragment_duplicates,
            live_latency_us: quantiles("live"),
            cache_latency_us: quantiles("cache"),
            pairs: final_pairs,
        }
    }

    fn complete_pair_if_ready(&self, m: &mut Metrics, pair_index: usize) -> bool {
        let (ready, expected_cache, started) = {
            let pair = &m.pairs[pair_index];
            (
                pair.spawn_seen && pair.received == self.chunks && pair.completed_us.is_none(),
                pair.expected_cache,
                pair.started,
            )
        };
        if !ready {
            return false;
        }
        let epoch = if expected_cache {
            m.cache_epoch
        } else {
            m.live_epoch
        };
        let latency = epoch
            .map(|t| t.elapsed().as_micros() as u64)
            .or_else(|| started.map(|t| t.elapsed().as_micros() as u64))
            .unwrap_or_default();
        m.pairs[pair_index].completed_us = Some(latency);
        self.notify.notify_waiters();
        true
    }

    fn log(&self, event: &'static str) -> Result<()> {
        let _lock = self.log_lock.lock().unwrap();
        let path = &self.options.output_path;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut line = serde_json::to_vec(&self.snapshot(event))?;
        line.push(b'\n');
        let mut file = File::options().create(true).append(true).open(path)?;
        file.write_all(&line)?;
        Ok(())
    }

    pub fn observe(&self, receiver: usize, network_id: u16, wire: &[u8]) {
        if wire.len() < 4 + 1 {
            self.metrics.lock().unwrap().malformed += 1;
            return;
        }
        let sender_peer = u16::from_le_bytes([wire[0], wire[1]]);
        let got_id = u16::from_le_bytes([wire[2], wire[3]]);
        if got_id != network_id {
            return;
        }
        let payload = &wire[4..];
        let Some(op) = payload.first().copied() else {
            return;
        };
        if op == OP_SERVER_CACHE_OFFER {
            if payload.len() < 17 {
                self.metrics.lock().unwrap().malformed += 1;
                return;
            }
            if let Some(owner) = self
                .ids
                .iter()
                .position(|id| id.as_slice() == &payload[1..17])
            {
                if receiver < self.options.clients - self.options.cache_recipients {
                    return;
                }
                self.metrics
                    .lock()
                    .unwrap()
                    .offers
                    .insert((receiver, owner));
                self.notify.notify_waiters();
            }
            return;
        }
        if op != OP_SPAWN && op != OP_CHUNK {
            return;
        }
        let id = if payload.len() >= 17 {
            &payload[1..17]
        } else {
            self.metrics.lock().unwrap().malformed += 1;
            return;
        };
        let Some(owner) = self
            .ids
            .iter()
            .position(|candidate| candidate.as_slice() == id)
        else {
            return;
        };
        if self.owner_peers.lock().unwrap().get(owner).copied() != Some(sender_peer) {
            self.metrics.lock().unwrap().malformed += 1;
            return;
        }
        let (chunk_index, data_offset, data_len) = if op == OP_CHUNK {
            if payload.len() < CHUNK_HEADER {
                self.metrics.lock().unwrap().malformed += 1;
                return;
            }
            let idx = i32::from_le_bytes(payload[17..21].try_into().unwrap());
            let len = i32::from_le_bytes(payload[21..25].try_into().unwrap());
            if idx < 0
                || len <= 0
                || len as usize > CHUNK_BYTES
                || payload.len() != CHUNK_HEADER + len as usize
                || idx as usize >= self.chunks
            {
                self.metrics.lock().unwrap().malformed += 1;
                return;
            }
            (Some(idx as usize), CHUNK_HEADER, len as usize)
        } else {
            if payload.len() < 1 + 16 + 2 + 1 + 16 + 28 {
                self.metrics.lock().unwrap().malformed += 1;
                return;
            }
            let mut cursor = 17;
            let owner_id = u16::from_le_bytes([payload[cursor], payload[cursor + 1]]);
            cursor += 2;
            let Some((prefix, prefix_bytes)) = read_7bit_len(&payload[cursor..]) else {
                self.metrics.lock().unwrap().malformed += 1;
                return;
            };
            cursor += prefix_bytes;
            if cursor + prefix + 44 > payload.len() || owner_id != sender_peer {
                self.metrics.lock().unwrap().malformed += 1;
                return;
            }
            cursor += prefix;
            let width = i32::from_le_bytes(payload[cursor..cursor + 4].try_into().unwrap());
            let height = i32::from_le_bytes(payload[cursor + 4..cursor + 8].try_into().unwrap());
            let bytes = i32::from_le_bytes(payload[cursor + 8..cursor + 12].try_into().unwrap());
            let chunks = i32::from_le_bytes(payload[cursor + 12..cursor + 16].try_into().unwrap());
            if width != 2048
                || height != 2048
                || bytes as usize != self.bytes.len()
                || chunks as usize != self.chunks
            {
                self.metrics.lock().unwrap().malformed += 1;
                return;
            }
            let mut m = self.metrics.lock().unwrap();
            let pair = &mut m.pairs[receiver * self.options.sharers + owner];
            if pair.expected_cache && !Path::new(&self.options.cache_start_file).exists() {
                return;
            }
            pair.phase = if pair.expected_cache { 2 } else { 1 };
            pair.spawn_seen = true;
            pair.started.get_or_insert_with(Instant::now);
            self.complete_pair_if_ready(&mut m, receiver * self.options.sharers + owner);
            return;
        };
        let idx = chunk_index.unwrap();
        let source_offset = idx * CHUNK_BYTES;
        if data_len != (self.bytes.len() - source_offset).min(CHUNK_BYTES)
            || payload[data_offset..data_offset + data_len]
                != self.bytes[source_offset..source_offset + data_len]
        {
            self.metrics.lock().unwrap().integrity_errors += 1;
            return;
        }
        let mut m = self.metrics.lock().unwrap();
        let pair_index = receiver * self.options.sharers + owner;
        let new_unique = {
            let pair = &mut m.pairs[pair_index];
            if pair.expected_cache && !Path::new(&self.options.cache_start_file).exists() {
                return;
            }
            pair.phase = if pair.expected_cache { 2 } else { 1 };
            pair.started.get_or_insert_with(Instant::now);
            if pair.seen[idx] {
                false
            } else {
                pair.seen[idx] = true;
                pair.received += 1;
                true
            }
        };
        if !new_unique {
            m.duplicates += 1;
            return;
        }
        m.unique_chunks += 1;
        self.complete_pair_if_ready(&mut m, pair_index);
    }

    fn pending_offers(&self) -> Vec<(usize, usize)> {
        let mut m = self.metrics.lock().unwrap();
        let pending = m
            .offers
            .iter()
            .copied()
            .filter(|offer| !m.requested_offers.contains(offer))
            .collect::<Vec<_>>();
        m.requested_offers.extend(pending.iter().copied());
        pending
    }
}

fn guid_for_sharer(index: usize) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[..4].copy_from_slice(&(0x42415300u32 + index as u32).to_le_bytes());
    id[6] = 0x40;
    id[8] = 0x80;
    id
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(i32, i32)> {
    if !bytes.starts_with(&[0xff, 0xd8]) {
        return None;
    }
    let mut i = 2usize;
    while i + 4 <= bytes.len() {
        if bytes[i] != 0xff {
            i += 1;
            continue;
        }
        while i < bytes.len() && bytes[i] == 0xff {
            i += 1;
        }
        if i >= bytes.len() {
            return None;
        }
        let marker = bytes[i];
        i += 1;
        if marker == 0xd9 || marker == 0xda {
            return None;
        }
        if matches!(marker, 0xd8 | 0x01 | 0xd0..=0xd7) {
            continue;
        }
        if i + 2 > bytes.len() {
            return None;
        }
        let length = u16::from_be_bytes([bytes[i], bytes[i + 1]]) as usize;
        if length < 2 || i + length > bytes.len() {
            return None;
        }
        if matches!(
            marker,
            0xc0 | 0xc1
                | 0xc2
                | 0xc3
                | 0xc5
                | 0xc6
                | 0xc7
                | 0xc9
                | 0xca
                | 0xcb
                | 0xcd
                | 0xce
                | 0xcf
        ) && length >= 7
        {
            return Some((
                i32::from(u16::from_be_bytes([bytes[i + 5], bytes[i + 6]])),
                i32::from(u16::from_be_bytes([bytes[i + 3], bytes[i + 4]])),
            ));
        }
        i += length;
    }
    None
}

fn read_7bit_len(data: &[u8]) -> Option<(usize, usize)> {
    let mut value = 0usize;
    for (i, byte) in data.iter().copied().take(5).enumerate() {
        if i == 4 && byte & 0xf0 != 0 {
            return None;
        }
        value |= usize::from(byte & 0x7f) << (i * 7);
        if byte & 0x80 == 0 {
            return Some((value, i + 1));
        }
    }
    None
}

fn event_scene_payload(network_id: u16, recipients: &[u16], event: &[u8]) -> Vec<u8> {
    let mut writer = NetWriter::new();
    SceneDataMessage {
        message_index: network_id,
        recipients: recipients.to_vec(),
        payload: event.to_vec(),
    }
    .serialize(&mut writer)
    .unwrap();
    writer.as_slice().to_vec()
}

async fn send_image(
    client: Arc<BasisClient>,
    session: Arc<ImageBenchmarkSession>,
    recipients: Vec<u16>,
    owner_id: u16,
) {
    let owner = client.index;
    let image_id = session.ids[owner];
    let mut spawn = Vec::with_capacity(72);
    spawn.push(OP_SPAWN);
    spawn.extend_from_slice(&image_id);
    spawn.extend_from_slice(&owner_id.to_le_bytes());
    spawn.push(0); // BinaryWriter's 7-bit byte length for the empty owner name
    spawn.extend_from_slice(&2048i32.to_le_bytes());
    spawn.extend_from_slice(&2048i32.to_le_bytes());
    spawn.extend_from_slice(&(session.bytes.len() as i32).to_le_bytes());
    spawn.extend_from_slice(&(session.chunks as i32).to_le_bytes());
    for value in [0f32, 0f32, 1f32, 0f32, 0f32, 0f32, 1f32] {
        spawn.extend_from_slice(&value.to_le_bytes());
    }
    let payload = event_scene_payload(
        client
            .image_network_id
            .load(std::sync::atomic::Ordering::Acquire),
        &recipients,
        &spawn,
    );
    if client
        .send_reliable_ordered(channels::SCENE, &payload)
        .await
        .is_err()
    {
        session.metrics.lock().unwrap().send_errors += 1;
    }
    let mut next = Instant::now()
        + fanout_pacing_duration(
            spawn.len(),
            recipients.len(),
            session.options.egress_megabits_per_second,
        );
    for index in 0..session.chunks {
        while client.pending_reliable.lock().await.len() >= 64 {
            time::sleep(Duration::from_millis(20)).await;
        }
        let offset = index * CHUNK_BYTES;
        let len = (session.bytes.len() - offset).min(CHUNK_BYTES);
        let mut chunk = Vec::with_capacity(CHUNK_HEADER + len);
        chunk.push(OP_CHUNK);
        chunk.extend_from_slice(&image_id);
        chunk.extend_from_slice(&(index as i32).to_le_bytes());
        chunk.extend_from_slice(&(len as i32).to_le_bytes());
        chunk.extend_from_slice(&session.bytes[offset..offset + len]);
        let payload = event_scene_payload(
            client
                .image_network_id
                .load(std::sync::atomic::Ordering::Acquire),
            &recipients,
            &chunk,
        );
        if client
            .send_reliable_ordered(channels::SCENE, &payload)
            .await
            .is_err()
        {
            session.metrics.lock().unwrap().send_errors += 1;
        }
        next += fanout_pacing_duration(
            chunk.len(),
            recipients.len(),
            session.options.egress_megabits_per_second,
        );
        time::sleep_until(time::Instant::from_std(next)).await;
    }
    session.metrics.lock().unwrap().uploads_done += 1;
    let _ = session.log("progress");
}

pub async fn run_workload(
    clients: Arc<AsyncMutex<Vec<Arc<BasisClient>>>>,
    session: Arc<ImageBenchmarkSession>,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    while !Path::new(&session.options.live_start_file).exists() {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("shutdown before live marker");
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    session.metrics.lock().unwrap().live_epoch = Some(Instant::now());
    let population = clients.lock().await.clone();
    let owner_peers = session.owner_peers.lock().unwrap().clone();
    let cache_first = session.options.clients - session.options.cache_recipients;
    let mut senders = Vec::new();
    for owner in 0..session.options.sharers {
        let mut recipients = Vec::new();
        for client in population
            .iter()
            .filter(|c| c.index != owner && c.index < cache_first)
        {
            let peer_id = client
                .remote_peer_id()
                .await
                .ok_or_else(|| anyhow::anyhow!("client {} missing peer id", client.index))?;
            recipients.push(peer_id);
        }
        let me = population[owner].clone();
        let session_copy = session.clone();
        let owner_id = owner_peers[owner];
        senders.push(tokio::spawn(send_image(
            me,
            session_copy,
            recipients,
            owner_id,
        )));
    }
    for sender in senders {
        let _ = sender.await;
    }
    loop {
        let progress = session.snapshot("progress");
        if progress.live_completed >= progress.live_expected {
            // Persist the exact state that released this predicate before the runner waits
            // for the cache marker; the last periodic progress line may be stale.
            session.log("live_complete")?;
            break;
        }
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("shutdown during live image transfer");
        }
        let _ = session.log("progress");
        time::timeout(Duration::from_secs(1), session.notify.notified())
            .await
            .ok();
    }
    while !Path::new(&session.options.cache_start_file).exists() {
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("shutdown before cache marker");
        }
        time::sleep(Duration::from_millis(50)).await;
    }
    session.metrics.lock().unwrap().cache_epoch = Some(Instant::now());
    loop {
        for (receiver, owner) in session.pending_offers() {
            let client = &population[receiver];
            let recipient = client
                .remote_peer_id
                .lock()
                .await
                .ok_or_else(|| anyhow::anyhow!("client {} missing peer id", receiver))?
                as u16;
            let request = [OP_SERVER_CACHE_REQUEST]
                .into_iter()
                .chain(session.ids[owner])
                .collect::<Vec<_>>();
            let payload = event_scene_payload(
                client
                    .image_network_id
                    .load(std::sync::atomic::Ordering::Acquire),
                &[recipient],
                &request,
            );
            client
                .send_reliable_ordered(channels::SCENE, &payload)
                .await?;
        }
        let progress = session.snapshot("progress");
        let expected = progress.live_expected + progress.cache_expected;
        if progress.live_completed + progress.cache_completed >= expected {
            break;
        }
        if shutdown.load(std::sync::atomic::Ordering::Relaxed) {
            bail!("shutdown during image cache replay");
        }
        let _ = session.log("progress");
        time::timeout(Duration::from_secs(1), session.notify.notified())
            .await
            .ok();
    }
    session.log("final")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fanout_pacing_charges_bytes_once_per_recipient_at_aggregate_rate() {
        let image = fanout_pacing_duration(5 * 1024 * 1024, 496, 200).as_secs_f64();
        assert!(
            (104.0..104.1).contains(&image),
            "5 MiB fanout pacing was {image:.3}s"
        );

        let spawn = fanout_pacing_duration(73, 496, 200).as_secs_f64();
        assert!(
            (0.001..0.002).contains(&spawn),
            "spawn fanout pacing was {spawn:.6}s"
        );
    }

    fn fixture() -> Vec<u8> {
        let mut bytes = vec![0x55; 5 * 1024 * 1024];
        bytes[..2].copy_from_slice(&[0xff, 0xd8]);
        bytes[2..21].copy_from_slice(&[
            0xff, 0xc0, 0x00, 0x11, 0x08, 0x08, 0x00, 0x08, 0x00, 0x08, 0x03, 0x01, 0x11, 0x00,
            0x02, 0x11, 0x00, 0x03, 0x11,
        ]);
        let last = bytes.len();
        bytes[last - 2..].copy_from_slice(&[0xff, 0xd9]);
        bytes
    }

    fn session(
        clients: usize,
        cache_recipients: usize,
        cache_marker: &Path,
    ) -> Arc<ImageBenchmarkSession> {
        let image =
            std::env::temp_dir().join(format!("basis-image-bench-{}.jpg", uuid::Uuid::new_v4()));
        std::fs::write(&image, fixture()).unwrap();
        let result = ImageBenchmarkSession::new(ImageBenchmarkOptions {
            config_path: PathBuf::new(),
            ip: "127.0.0.1".into(),
            port: 4296,
            image_path: image.clone(),
            output_path: std::env::temp_dir().join(format!("{}.jsonl", uuid::Uuid::new_v4())),
            live_start_file: PathBuf::new(),
            cache_start_file: cache_marker.to_path_buf(),
            clients,
            sharers: 3,
            cache_recipients,
            egress_megabits_per_second: 200,
        })
        .unwrap();
        let _ = std::fs::remove_file(image);
        *result.owner_peers.lock().unwrap() = vec![100, 101, 102];
        result
    }

    fn observe_packet(
        session: &ImageBenchmarkSession,
        receiver: usize,
        owner: usize,
        op: u8,
        chunk_index: usize,
        data: &[u8],
    ) {
        let mut payload = vec![op];
        payload.extend_from_slice(&session.ids[owner]);
        if op == OP_SPAWN {
            payload.extend_from_slice(&(100 + owner as u16).to_le_bytes());
            payload.push(0); // BinaryWriter's 7-bit length for empty owner name
            for value in [
                2048i32,
                2048,
                session.bytes.len() as i32,
                session.chunks as i32,
            ] {
                payload.extend_from_slice(&value.to_le_bytes());
            }
            for value in [0f32, 0f32, 1f32, 0f32, 0f32, 0f32, 1f32] {
                payload.extend_from_slice(&value.to_le_bytes());
            }
        } else {
            payload.extend_from_slice(&(chunk_index as i32).to_le_bytes());
            payload.extend_from_slice(&(data.len() as i32).to_le_bytes());
            payload.extend_from_slice(data);
        }
        let mut wire = Vec::new();
        wire.extend_from_slice(&(100 + owner as u16).to_le_bytes());
        wire.extend_from_slice(&77u16.to_le_bytes());
        wire.extend_from_slice(&payload);
        session.observe(receiver, 77, &wire);
    }

    #[test]
    fn full_fixture_integrity_reassembles_every_expected_receiver_pair() {
        let marker =
            std::env::temp_dir().join(format!("basis-cache-marker-{}", uuid::Uuid::new_v4()));
        std::fs::write(&marker, "ready").unwrap();
        let session = session(12, 3, &marker);
        {
            let mut m = session.metrics.lock().unwrap();
            m.live_epoch = Some(Instant::now());
            m.cache_epoch = Some(Instant::now());
        }
        for receiver in 0..12 {
            for owner in 0..3 {
                if receiver == owner {
                    continue;
                }
                observe_packet(&session, receiver, owner, OP_SPAWN, 0, &[]);
                for chunk_index in (0..session.chunks).rev() {
                    let offset = chunk_index * CHUNK_BYTES;
                    let end = (offset + CHUNK_BYTES).min(session.bytes.len());
                    observe_packet(
                        &session,
                        receiver,
                        owner,
                        OP_CHUNK,
                        chunk_index,
                        &session.bytes[offset..end],
                    );
                }
            }
        }
        let progress = session.snapshot("final");
        assert_eq!(progress.live_completed, progress.live_expected);
        assert_eq!(progress.cache_completed, progress.cache_expected);
        assert_eq!(
            progress.unique_chunks,
            (progress.live_expected + progress.cache_expected) as u64 * session.chunks as u64
        );
        assert_eq!(progress.integrity_errors, 0);
        assert_eq!(progress.malformed, 0);
        assert_eq!(progress.missing_chunks, 0);
        assert_eq!(
            progress.pairs.as_ref().unwrap().len(),
            progress.live_expected + progress.cache_expected
        );
        session.log("live_complete").unwrap();
        let logged: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&session.options.output_path).unwrap()).unwrap();
        assert_eq!(logged["event"], "live_complete");
        assert_eq!(logged["live_completed"], progress.live_expected);
        let _ = std::fs::remove_file(marker);
    }

    #[test]
    fn duplicate_out_of_order_and_corrupt_chunks_are_counted_without_false_completion() {
        let marker =
            std::env::temp_dir().join(format!("basis-cache-marker-{}", uuid::Uuid::new_v4()));
        let session = session(6, 1, &marker);
        let receiver = 3;
        observe_packet(&session, receiver, 0, OP_SPAWN, 0, &[]);
        let offset = (session.chunks - 1) * CHUNK_BYTES;
        let end = session.bytes.len();
        observe_packet(
            &session,
            receiver,
            0,
            OP_CHUNK,
            session.chunks - 1,
            &session.bytes[offset..end],
        );
        observe_packet(
            &session,
            receiver,
            0,
            OP_CHUNK,
            session.chunks - 1,
            &session.bytes[offset..end],
        );
        let mut wrong = session.bytes[..CHUNK_BYTES].to_vec();
        wrong[0] ^= 1;
        observe_packet(&session, receiver, 0, OP_CHUNK, 0, &wrong);
        let progress = session.snapshot("progress");
        assert_eq!(progress.duplicates, 1);
        assert_eq!(progress.integrity_errors, 1);
        assert_eq!(progress.live_completed, 0);
        assert_eq!(progress.malformed, 0);
        let _ = std::fs::remove_file(marker);
    }

    #[test]
    fn completes_pair_when_spawn_arrives_after_all_integrity_checked_chunks() {
        let marker =
            std::env::temp_dir().join(format!("basis-cache-marker-{}", uuid::Uuid::new_v4()));
        let session = session(6, 1, &marker);
        session.metrics.lock().unwrap().live_epoch = Some(Instant::now());
        let receiver = 3;
        for chunk_index in 0..session.chunks {
            let offset = chunk_index * CHUNK_BYTES;
            let end = (offset + CHUNK_BYTES).min(session.bytes.len());
            observe_packet(
                &session,
                receiver,
                0,
                OP_CHUNK,
                chunk_index,
                &session.bytes[offset..end],
            );
        }
        assert_eq!(session.snapshot("progress").live_completed, 0);

        observe_packet(&session, receiver, 0, OP_SPAWN, 0, &[]);

        let final_event = session.snapshot("final");
        assert_eq!(final_event.live_completed, 1);
        assert_eq!(final_event.integrity_errors, 0);
        let pair = final_event
            .pairs
            .unwrap()
            .into_iter()
            .find(|pair| pair.receiver == receiver && pair.owner == 0)
            .unwrap();
        assert_eq!(pair.unique_chunks, session.chunks);
        assert!(pair.latency_us > 0);
        let _ = std::fs::remove_file(marker);
    }

    #[test]
    fn malformed_chunk_lengths_are_rejected() {
        let marker =
            std::env::temp_dir().join(format!("basis-cache-marker-{}", uuid::Uuid::new_v4()));
        let session = session(6, 1, &marker);
        let mut wire = vec![0u8; 4 + CHUNK_HEADER + 1];
        wire[0..2].copy_from_slice(&100u16.to_le_bytes());
        wire[2..4].copy_from_slice(&77u16.to_le_bytes());
        wire[4] = OP_CHUNK;
        wire[5..21].copy_from_slice(&session.ids[0]);
        wire[21..25].copy_from_slice(&0i32.to_le_bytes());
        wire[25..29].copy_from_slice(&(CHUNK_BYTES as i32).to_le_bytes());
        session.observe(3, 77, &wire);
        assert_eq!(session.snapshot("progress").malformed, 1);
        let _ = std::fs::remove_file(marker);
    }

    #[test]
    fn repeated_server_net_id_broadcasts_do_not_inflate_readiness() {
        let marker =
            std::env::temp_dir().join(format!("basis-cache-marker-{}", uuid::Uuid::new_v4()));
        let session = session(6, 1, &marker);
        session.note_connected(0);
        session.note_connected(0);
        session.note_net_id(0);
        session.note_net_id(0);
        let ready = session.snapshot("ready");
        assert_eq!(ready.connected, 1);
        assert_eq!(ready.net_id, 1);
        let _ = std::fs::remove_file(marker);
    }
}
