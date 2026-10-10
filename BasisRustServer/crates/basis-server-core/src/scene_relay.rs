//! Opt-in transport packing for unreliable SCENE broadcasts. Messages are never coalesced.
use super::{ConnectedPeer, DeliveryMethod, PeerId, PeerSession, TransportHandle};
use anyhow::{Context, Result};
use basis_protocol::channels;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc as sync_mpsc, Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

const INBOX_CAPACITY: usize = 1024;
const FANOUT_LANES: usize = 4;
const LANE_CAPACITY: usize = 2;
const MAX_BATCH_MESSAGES: usize = 128;
const MAX_BATCH_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Copy, Default)]
pub struct SceneRelaySnapshot {
    pub enqueued_messages: u64,
    pub dequeued_messages: u64,
    /// Logical recipient attempts, not delivered messages.
    pub attempted_recipient_messages: u64,
    /// Successfully submitted UDP datagrams, not logical-message receipts.
    pub sent_datagrams: u64,
    pub stale_recipient_messages: u64,
    pub queue_backpressure: u64,
    pub fanout_backpressure: u64,
    pub enqueue_errors: u64,
    pub send_errors: u64,
}

#[derive(Default)]
struct Counters {
    enqueued: AtomicU64,
    dequeued: AtomicU64,
    attempted: AtomicU64,
    datagrams: AtomicU64,
    stale: AtomicU64,
    inbox_waits: AtomicU64,
    lane_waits: AtomicU64,
    enqueue_errors: AtomicU64,
    send_errors: AtomicU64,
}

struct Input {
    payload: Bytes,
    recipients: Vec<PeerSession>,
}

struct TargetBatch {
    session: PeerSession,
    packets: Vec<(u8, Bytes)>,
}

type LaneBatch = Vec<TargetBatch>;

pub(super) struct SceneRelay {
    sender: Mutex<Option<mpsc::Sender<Input>>>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    counters: Arc<Counters>,
}

pub(super) fn batch_interval(value: Option<&str>) -> Result<Option<Duration>> {
    let millis = value
        .unwrap_or("0")
        .parse::<u64>()
        .context("BASIS_SCENE_BATCH_MS must be 0 (disabled), 1, or 2")?;
    anyhow::ensure!(
        millis <= 2,
        "BASIS_SCENE_BATCH_MS must be 0 (disabled), 1, or 2"
    );
    Ok((millis > 0).then_some(Duration::from_millis(millis)))
}

pub(super) fn eligible(channel: u8, delivery: DeliveryMethod, recipients: &[PeerId]) -> bool {
    channel == channels::SCENE && delivery == DeliveryMethod::Unreliable && recipients.is_empty()
}

pub(super) fn snapshot_recipients(
    peers: &DashMap<PeerId, ConnectedPeer>,
    source: PeerId,
) -> Vec<PeerSession> {
    peers
        .iter()
        .filter(|peer| *peer.key() != source)
        .filter_map(|peer| peer.session.clone())
        .collect()
}

impl SceneRelay {
    pub(super) fn from_env(
        transport: &TransportHandle,
        peers: Arc<DashMap<PeerId, ConnectedPeer>>,
    ) -> Result<Option<Self>> {
        let value = std::env::var("BASIS_SCENE_BATCH_MS").ok();
        batch_interval(value.as_deref())?
            .map(|interval| Self::start(transport, peers, interval))
            .transpose()
    }

    fn start(
        transport: &TransportHandle,
        peers: Arc<DashMap<PeerId, ConnectedPeer>>,
        interval: Duration,
    ) -> Result<Self> {
        let counters = Arc::new(Counters::default());
        let mut threads = Vec::new();
        let mut lanes = Vec::new();
        for index in 0..FANOUT_LANES {
            let sender = match transport.dedicated_unreliable_sender() {
                Ok(sender) => sender,
                Err(error) => {
                    drop(lanes);
                    join_threads(threads)?;
                    return Err(error.into());
                }
            };
            let (tx, rx) = sync_mpsc::sync_channel::<LaneBatch>(LANE_CAPACITY);
            let stats = counters.clone();
            let authenticated = peers.clone();
            match thread::Builder::new()
                .name(format!("scene-fanout-{index}"))
                .spawn(move || {
                    while let Ok(batch) = rx.recv() {
                        for target in batch {
                            send_target(&sender, &authenticated, &stats, target);
                        }
                    }
                }) {
                Ok(handle) => {
                    threads.push(handle);
                    lanes.push(tx);
                }
                Err(error) => {
                    drop(lanes);
                    join_threads(threads)?;
                    return Err(error.into());
                }
            }
        }
        let (sender, receiver) = mpsc::channel(INBOX_CAPACITY);
        let stats = counters.clone();
        match thread::Builder::new()
            .name("scene-collector".into())
            .spawn(move || {
                collect(receiver, lanes, stats, interval);
            }) {
            Ok(handle) => threads.insert(0, handle),
            Err(error) => {
                // The failed spawn drops its closure and lane senders, closing receivers.
                join_threads(threads)?;
                return Err(error.into());
            }
        }
        tracing::info!(
            batch_ms = interval.as_millis(),
            lanes = FANOUT_LANES,
            inbox_capacity = INBOX_CAPACITY,
            "scene batching enabled"
        );
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            threads: Mutex::new(threads),
            counters,
        })
    }

    pub(super) async fn enqueue(&self, payload: Bytes, recipients: Vec<PeerSession>) -> Result<()> {
        let sender = self.sender.lock().clone();
        let Some(sender) = sender else {
            self.counters.enqueue_errors.fetch_add(1, Ordering::Relaxed);
            anyhow::bail!("scene relay admission closed");
        };
        let input = Input {
            payload,
            recipients,
        };
        let result = match sender.try_send(input) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(input)) => {
                self.counters.inbox_waits.fetch_add(1, Ordering::Relaxed);
                sender
                    .send(input)
                    .await
                    .map_err(|_| anyhow::anyhow!("scene relay inbox closed"))
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err(anyhow::anyhow!("scene relay inbox closed"))
            }
        };
        if result.is_ok() {
            self.counters.enqueued.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.enqueue_errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    pub(super) fn snapshot(&self) -> SceneRelaySnapshot {
        let stats = &self.counters;
        SceneRelaySnapshot {
            enqueued_messages: stats.enqueued.load(Ordering::Relaxed),
            dequeued_messages: stats.dequeued.load(Ordering::Relaxed),
            attempted_recipient_messages: stats.attempted.load(Ordering::Relaxed),
            sent_datagrams: stats.datagrams.load(Ordering::Relaxed),
            stale_recipient_messages: stats.stale.load(Ordering::Relaxed),
            queue_backpressure: stats.inbox_waits.load(Ordering::Relaxed),
            fanout_backpressure: stats.lane_waits.load(Ordering::Relaxed),
            enqueue_errors: stats.enqueue_errors.load(Ordering::Relaxed),
            send_errors: stats.send_errors.load(Ordering::Relaxed),
        }
    }

    /// Call after ingress handlers finish; closing the last sender drains accepted input.
    pub(super) async fn drain(&self) -> Result<()> {
        self.sender.lock().take();
        let threads = std::mem::take(&mut *self.threads.lock());
        tokio::task::spawn_blocking(move || join_threads(threads))
            .await
            .context("joining scene relay threads")??;
        tracing::info!(snapshot = ?self.snapshot(), "scene relay drained");
        Ok(())
    }
}

impl Drop for SceneRelay {
    fn drop(&mut self) {
        self.sender.get_mut().take();
        let threads = std::mem::take(self.threads.get_mut());
        if let Err(error) = join_threads(threads) {
            tracing::error!(%error, "scene relay cleanup failed");
        }
    }
}

fn join_threads(threads: Vec<JoinHandle<()>>) -> Result<()> {
    let mut failed = false;
    for thread in threads {
        failed |= thread.join().is_err();
    }
    anyhow::ensure!(!failed, "scene relay thread panicked");
    Ok(())
}

fn collect(
    mut receiver: mpsc::Receiver<Input>,
    lanes: Vec<sync_mpsc::SyncSender<LaneBatch>>,
    counters: Arc<Counters>,
    interval: Duration,
) {
    let mut pending = None;
    loop {
        let Some(first) = pending.take().or_else(|| receiver.blocking_recv()) else {
            break;
        };
        let deadline = Instant::now() + interval;
        let mut bytes = first.payload.len();
        let mut batch = vec![first];
        while batch.len() < MAX_BATCH_MESSAGES
            && bytes < MAX_BATCH_BYTES
            && Instant::now() < deadline
        {
            match receiver.try_recv() {
                Ok(input) => {
                    if bytes.saturating_add(input.payload.len()) > MAX_BATCH_BYTES {
                        pending = Some(input);
                        break;
                    }
                    bytes += input.payload.len();
                    batch.push(input);
                }
                Err(mpsc::error::TryRecvError::Disconnected) => break,
                Err(mpsc::error::TryRecvError::Empty) => {
                    thread::sleep(
                        deadline
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_micros(100)),
                    );
                }
            }
        }
        counters
            .dequeued
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        let grouped = group_recipients(batch);
        for (lane, batch) in lanes.iter().zip(grouped) {
            if batch.is_empty() {
                continue;
            }
            let result = match lane.try_send(batch) {
                Ok(()) => continue,
                Err(sync_mpsc::TrySendError::Full(batch)) => {
                    counters.lane_waits.fetch_add(1, Ordering::Relaxed);
                    lane.send(batch).map_err(|error| error.0)
                }
                Err(sync_mpsc::TrySendError::Disconnected(batch)) => Err(batch),
            };
            if let Err(batch) = result {
                counters.send_errors.fetch_add(
                    batch
                        .iter()
                        .map(|target| target.packets.len() as u64)
                        .sum::<u64>(),
                    Ordering::Relaxed,
                );
                tracing::error!("scene fanout lane closed before draining accepted input");
            }
        }
    }
    // Dropping senders closes each lane after its queued batches; owners join every lane.
}

fn group_recipients(batch: Vec<Input>) -> Vec<LaneBatch> {
    let mut targets: HashMap<PeerId, Vec<TargetBatch>> = HashMap::new();
    for input in batch {
        for session in input.recipients {
            let groups = targets.entry(session.peer_id()).or_default();
            if let Some(target) = groups
                .iter_mut()
                .find(|target| target.session.same_connection(&session))
            {
                target
                    .packets
                    .push((channels::SCENE, input.payload.clone()));
            } else {
                groups.push(TargetBatch {
                    session,
                    packets: vec![(channels::SCENE, input.payload.clone())],
                });
            }
        }
    }
    let mut lanes: Vec<LaneBatch> = (0..FANOUT_LANES).map(|_| Vec::new()).collect();
    for (peer, targets) in targets {
        lanes[peer as usize % FANOUT_LANES].extend(targets);
    }
    lanes
}

fn send_target(
    transport: &TransportHandle,
    peers: &DashMap<PeerId, ConnectedPeer>,
    counters: &Counters,
    target: TargetBatch,
) {
    let count = target.packets.len() as u64;
    let authenticated = peers.get(&target.session.peer_id()).is_some_and(|peer| {
        peer.session
            .as_ref()
            .is_some_and(|session| session.same_connection(&target.session))
    });
    let lease = target.session.try_read_lease();
    if !authenticated || lease.is_none() || !transport.is_current_session(&target.session) {
        counters.stale.fetch_add(count, Ordering::Relaxed);
        return;
    }
    // The read lease survives every direct send. The transport also validates the
    // incarnation; snapshots never turn into a fresh numeric-peer lookup.
    let _lease = lease;
    counters.attempted.fetch_add(count, Ordering::Relaxed);
    match transport.try_send_session_many_unreliable_packets(&target.session, &target.packets) {
        Ok(datagrams) => {
            counters
                .datagrams
                .fetch_add(datagrams as u64, Ordering::Relaxed);
        }
        Err(error) => {
            counters.send_errors.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(peer = target.session.peer_id(), %error, "scene batch send failed");
        }
    }
    // A partial result is a datagram count only. Socket refusals are recorded by
    // the existing transport wouldBlock/nonReliableDroppedDatagrams counters.
}

#[cfg(test)]
mod tests {
    use super::*;
    use basis_protocol::{
        io::NetWriter,
        messages::{BasisSerialize, RemoteSceneDataMessage, ServerSceneDataMessage},
        version::LITENETLIB_PROTOCOL_ID,
    };
    use basis_transport::{PacketProperty, ServerEvent};
    use tokio::{net::UdpSocket, time::timeout};

    async fn connect(
        transport: &TransportHandle,
        events: &mut mpsc::Receiver<ServerEvent>,
        stamp: i64,
    ) -> (PeerSession, UdpSocket) {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket
            .connect(transport.local_addr().unwrap())
            .await
            .unwrap();
        let mut writer = NetWriter::new();
        writer.put_u8(PacketProperty::ConnectRequest as u8);
        writer.put_i32(LITENETLIB_PROTOCOL_ID);
        writer.put_i64(stamp);
        writer.put_i32(0);
        writer.put_u8(16);
        writer.put_bytes(&[0; 16]);
        socket.send(writer.as_slice()).await.unwrap();
        let request = loop {
            if let ServerEvent::ConnectionRequest(request) =
                timeout(Duration::from_secs(2), events.recv())
                    .await
                    .unwrap()
                    .unwrap()
            {
                break request;
            }
        };
        let session = transport.accept_session(&request).await.unwrap();
        let mut accept = [0; 64];
        socket.recv(&mut accept).await.unwrap();
        (session, socket)
    }

    fn admit(peers: &DashMap<PeerId, ConnectedPeer>, session: &PeerSession) {
        let mut peer = crate::tests::test_connected_peer(session.peer_id());
        peer.session = Some(session.clone());
        peers.insert(session.peer_id(), peer);
    }

    // Independent readers pin both transport formats rather than treating a
    // successful API return as proof of payload conservation.
    fn scene_payloads(packet: &[u8]) -> Vec<Vec<u8>> {
        match PacketProperty::from_byte(packet[0]) {
            Some(PacketProperty::Merged) => {
                let mut bytes = &packet[1..];
                let mut messages = Vec::new();
                while !bytes.is_empty() {
                    let len = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
                    messages.extend(scene_payloads(&bytes[2..2 + len]));
                    bytes = &bytes[2 + len..];
                }
                messages
            }
            Some(PacketProperty::CompactMerged) => {
                let mut bytes = &packet[1..];
                let mut messages = Vec::new();
                while !bytes.is_empty() {
                    let tag = bytes[0];
                    bytes = &bytes[1..];
                    let len = if tag & 0x80 != 0 {
                        let len = u16::from_le_bytes(bytes[..2].try_into().unwrap()) as usize;
                        bytes = &bytes[2..];
                        len
                    } else {
                        let len = bytes[0] as usize;
                        bytes = &bytes[1..];
                        len
                    };
                    assert_eq!(tag & 0x40, 0);
                    assert_eq!(tag & 0x3f, channels::SCENE);
                    messages.push(bytes[..len].to_vec());
                    bytes = &bytes[len..];
                }
                messages
            }
            Some(PacketProperty::Unreliable) if packet[1] == channels::SCENE => {
                vec![packet[2..].to_vec()]
            }
            _ => Vec::new(),
        }
    }

    async fn receive(socket: &UdpSocket, count: usize) -> (Vec<Vec<u8>>, bool) {
        let mut output = Vec::new();
        let mut merged = false;
        let mut bytes = [0; 65535];
        while output.len() < count {
            let len = timeout(Duration::from_secs(2), socket.recv(&mut bytes))
                .await
                .unwrap()
                .unwrap();
            merged |= matches!(
                PacketProperty::from_byte(bytes[0]),
                Some(PacketProperty::Merged | PacketProperty::CompactMerged)
            );
            output.extend(scene_payloads(&bytes[..len]));
        }
        output.sort();
        (output, merged)
    }

    #[test]
    fn disabled_and_ineligible_paths_do_not_enter_batching() {
        assert!(batch_interval(None).unwrap().is_none());
        assert!(batch_interval(Some("0")).unwrap().is_none());
        assert_eq!(
            batch_interval(Some("2")).unwrap(),
            Some(Duration::from_millis(2))
        );
        assert!(batch_interval(Some("3")).is_err());
        assert!(batch_interval(Some("garbage")).is_err());
        assert!(eligible(channels::SCENE, DeliveryMethod::Unreliable, &[]));
        for delivery in [
            DeliveryMethod::ReliableOrdered,
            DeliveryMethod::ReliableUnordered,
            DeliveryMethod::Sequenced,
        ] {
            assert!(!eligible(channels::SCENE, delivery, &[]));
        }
        assert!(!eligible(
            channels::DIRECT_SCENE_SERVER,
            DeliveryMethod::Unreliable,
            &[]
        ));
        assert!(!eligible(channels::SCENE, DeliveryMethod::Unreliable, &[1]));
    }

    #[tokio::test]
    async fn packing_conserves_each_envelope_and_excludes_source_in_both_formats() {
        for compact in [false, true] {
            let (transport, mut events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            transport.set_compact_merge_send(compact);
            let (source_a, socket_a) = connect(&transport, &mut events, 1).await;
            let (source_b, socket_b) = connect(&transport, &mut events, 2).await;
            let (observer, socket_observer) = connect(&transport, &mut events, 3).await;
            let peers = Arc::new(DashMap::new());
            for session in [&source_a, &source_b, &observer] {
                admit(&peers, session);
            }
            let relay =
                SceneRelay::start(&transport, peers.clone(), Duration::from_millis(2)).unwrap();
            let mut expected_a = Vec::new();
            let mut expected_b = Vec::new();
            let mut expected_observer = Vec::new();
            for index in 0..40u8 {
                let source = if index % 2 == 0 { &source_a } else { &source_b };
                let mut writer = NetWriter::new();
                ServerSceneDataMessage {
                    player_id: source.peer_id(),
                    scene_data_message: RemoteSceneDataMessage {
                        message_index: 60000,
                        payload: vec![index; 128],
                    },
                }
                .serialize(&mut writer)
                .unwrap();
                let bytes = writer.into_vec();
                expected_observer.push(bytes.clone());
                if source.same_connection(&source_a) {
                    expected_b.push(bytes.clone());
                } else {
                    expected_a.push(bytes.clone());
                }
                relay
                    .enqueue(
                        Bytes::from(bytes),
                        snapshot_recipients(&peers, source.peer_id()),
                    )
                    .await
                    .unwrap();
            }
            timeout(Duration::from_secs(2), relay.drain())
                .await
                .unwrap()
                .unwrap();
            for (socket, mut expected) in [
                (&socket_a, expected_a),
                (&socket_b, expected_b),
                (&socket_observer, expected_observer),
            ] {
                expected.sort();
                let (actual, merged) = receive(socket, expected.len()).await;
                assert_eq!(actual, expected);
                assert!(
                    merged,
                    "multiple scene messages must use merged transport framing"
                );
            }
            let counters = relay.snapshot();
            assert_eq!(counters.enqueued_messages, 40);
            assert_eq!(counters.dequeued_messages, 40);
            assert_eq!(counters.attempted_recipient_messages, 80);
            assert!(counters.sent_datagrams < counters.attempted_recipient_messages);
            assert_eq!(
                counters.stale_recipient_messages + counters.enqueue_errors + counters.send_errors,
                0
            );
            assert!(relay
                .enqueue(Bytes::from_static(b"closed"), vec![observer])
                .await
                .is_err());
            assert!(relay.threads.lock().is_empty());
            transport.shutdown();
        }
    }

    #[tokio::test]
    async fn session_replacement_cannot_redirect_queued_messages() {
        let (old_transport, mut old_events) = TransportHandle::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let (old, _old_socket) = connect(&old_transport, &mut old_events, 1).await;
        let (replacement_transport, mut events) =
            TransportHandle::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
        let (replacement, socket) = connect(&replacement_transport, &mut events, 2).await;
        assert_eq!(old.peer_id(), replacement.peer_id());
        let peers = DashMap::new();
        admit(&peers, &replacement);
        let mut grouped = group_recipients(vec![
            Input {
                payload: Bytes::from_static(b"old-session"),
                recipients: vec![old],
            },
            Input {
                payload: Bytes::from_static(b"new-session"),
                recipients: vec![replacement],
            },
        ]);
        let targets = &mut grouped[0];
        assert_eq!(
            targets.len(),
            2,
            "distinct incarnations cannot share a target batch"
        );
        let counters = Counters::default();
        let sender = replacement_transport.dedicated_unreliable_sender().unwrap();
        for target in targets.drain(..) {
            send_target(&sender, &peers, &counters, target);
        }
        let (received, _) = receive(&socket, 1).await;
        assert_eq!(received, vec![b"new-session".to_vec()]);
        assert_eq!(counters.stale.load(Ordering::Relaxed), 1);
        assert_eq!(counters.attempted.load(Ordering::Relaxed), 1);
        old_transport.shutdown();
        replacement_transport.shutdown();
    }

    #[tokio::test]
    async fn full_inbox_waits_for_capacity_without_discarding_messages() {
        let (sender, mut receiver) = mpsc::channel(1);
        let relay = Arc::new(SceneRelay {
            sender: Mutex::new(Some(sender)),
            threads: Mutex::new(Vec::new()),
            counters: Arc::new(Counters::default()),
        });
        relay
            .enqueue(Bytes::from_static(b"first"), Vec::new())
            .await
            .unwrap();
        let owned = relay.clone();
        let second = tokio::spawn(async move {
            owned
                .enqueue(Bytes::from_static(b"second"), Vec::new())
                .await
        });
        tokio::task::yield_now().await;
        assert!(!second.is_finished());
        assert_eq!(relay.snapshot().queue_backpressure, 1);
        assert_eq!(receiver.recv().await.unwrap().payload.as_ref(), b"first");
        second.await.unwrap().unwrap();
        assert_eq!(receiver.recv().await.unwrap().payload.as_ref(), b"second");
        assert_eq!(relay.snapshot().enqueued_messages, 2);
        relay.drain().await.unwrap();
        assert!(receiver.recv().await.is_none());
    }
}
