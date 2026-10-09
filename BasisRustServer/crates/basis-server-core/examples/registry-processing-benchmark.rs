//! Release-mode UDP -> transport dispatcher -> server-core registry benchmark.
//!
//! Copy this file to `BasisRustServer/crates/basis-server-core/examples/registry-processing-benchmark.rs`
//! in each frozen checkout, then run with `cargo run --release -p basis-server-core
//! --example registry-processing-benchmark -- --config <fixture> --peers 1 --ops 512 --batch 1`.
//! Repeat with `--peers 64`; execute from the `BasisRustServer` workspace root and
//! pass that checkout's `../docs/performance/fixtures/avatar-cpu-only-server.xml`. Compare
//! `--batch 1` (one outstanding request per peer) with `--batch 64` (bounded pipelining).
//! The client retries unacknowledged ordered requests after 100 ms. Server and driver CPU
//! share this process, so the probe reports combined scope and does not claim server-only CPU.

use basis_protocol::{
    application::NetworkApplication,
    channels,
    config::ServerConfig,
    io::{NetReader, NetWriter},
    messages::{
        BasisMessageSubscribe, BasisSerialize, ClientAvatarChangeMessage, ClientMetaDataMessage,
        LocalAvatarSyncMessage, ReadyMessage,
    },
    version::SERVER_VERSION,
};
use basis_server_core::ServerState;
use basis_transport::{DeliveryMethod, PacketProperty};
use std::{
    collections::HashMap,
    env,
    net::SocketAddr,
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    net::UdpSocket,
    time::{interval, timeout, MissedTickBehavior},
};

#[derive(Clone)]
struct Args {
    peers: usize,
    operations: usize,
    batch_size: usize,
    config: String,
}

fn args() -> anyhow::Result<Args> {
    let mut peers: usize = 1;
    let mut operations: usize = 512;
    let mut batch_size: usize = 64;
    let mut config = None;
    let mut it = env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--peers" => {
                peers = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing --peers"))?
                    .parse()?
            }
            "--ops" => {
                operations = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing --ops"))?
                    .parse()?
            }
            "--batch" => {
                batch_size = it
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("missing --batch"))?
                    .parse()?
            }
            "--config" => config = it.next(),
            "-h" | "--help" => {
                println!(
                    "registry-processing-benchmark --config <server.xml> [--peers 1|64] [--ops 512] [--batch 1|64]"
                );
                std::process::exit(0);
            }
            other => anyhow::bail!("unknown argument {other}"),
        }
    }
    let config = config.ok_or_else(|| anyhow::anyhow!("--config is required"))?;
    if !(1..=64).contains(&peers)
        || !(1..=32768).contains(&operations)
        || operations
            .checked_mul(peers)
            .is_none_or(|total| total > u16::MAX as usize)
        || !matches!(batch_size, 1 | 64)
    {
        anyhow::bail!(
            "peers must be 1..=64, ops must be 1..=32768, peers*ops <= 65535, and batch must be 1 or 64"
        );
    }
    Ok(Args {
        peers,
        operations,
        batch_size,
        config,
    })
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    let args = args()?;
    let config_path = PathBuf::from(&args.config);
    let mut config = ServerConfig::load_or_create(&config_path)?;
    config.set_port = 0;
    config.health_check_host = "127.0.0.1".into();
    config.health_check_port = 0;
    config.override_auto_discovery_of_ipv = true;
    config.ipv4_address = "127.0.0.1".into();
    config.enable_console = false;
    config.enable_statistics = true;
    config.health_include_extended_metrics = true;
    config.log_connection_handshake = false;
    config.use_auth = false;
    config.use_auth_identity = false;
    config.peer_limit = args.peers as i32;
    let application = (config.company_name.clone(), config.product_name.clone());

    let base_dir = env::temp_dir().join(format!("basis-registry-bench-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&base_dir)?;
    let (state, shutdown) =
        ServerState::start_with_config_path(config, &base_dir, &config_path).await?;
    state.transport.set_extended_statistics_enabled(true);
    let result = run(&state, args, &application.0, &application.1).await;
    let _ = shutdown.send(());
    let shutdown_result = state.shutdown().await;
    let _ = std::fs::remove_dir_all(&base_dir);
    result?;
    shutdown_result?;
    Ok(())
}

async fn run(
    state: &ServerState,
    args: Args,
    company_name: &str,
    product_name: &str,
) -> anyhow::Result<()> {
    let address = state.transport.local_addr()?;
    let mut clients = Vec::with_capacity(args.peers);
    let mut ack_tasks = Vec::with_capacity(args.peers);
    for index in 0..args.peers {
        let client = Client::connect(address, index, company_name, product_name).await?;
        ack_tasks.push(client.start_ack_loop());
        clients.push(client);
    }
    wait_until(Duration::from_secs(30), || {
        state.authenticated_peers.len() == args.peers
    })
    .await?;
    let peer_ids: HashMap<usize, _> = state
        .authenticated_peers
        .iter()
        .filter_map(|entry| {
            let index = entry
                .value()
                .metadata
                .player_display_name
                .strip_prefix("Probe ")?
                .parse::<usize>()
                .ok()?;
            Some((index, *entry.key()))
        })
        .collect();
    if peer_ids.len() != args.peers {
        anyhow::bail!("expected {} distinct authenticated peers", args.peers);
    }
    // Allow initial join traffic to drain before taking operation-counter baselines.
    tokio::time::sleep(Duration::from_millis(250)).await;
    wait_until(Duration::from_secs(5), || {
        let depth = state.transport.depths_snapshot();
        depth.reliable_pending == 0 && depth.reliable_queued == Some(0)
    })
    .await?;
    let inbound_start = state.statistics.snapshot().inbound_packets;
    let protocol_start = state.statistics.snapshot().protocol_errors;
    let transport_start = state.transport.stats_snapshot();
    let mut batch_ms = Vec::new();
    let mut completed = 0usize;
    let run_start = Instant::now();
    while completed < args.operations {
        let count = args.batch_size.min(args.operations - completed);
        let batch_start = Instant::now();
        for (client_index, client) in clients.iter_mut().enumerate() {
            for operation in completed..completed + count {
                client
                    .subscribe((client_index * args.operations + operation + 1) as u16)
                    .await?;
            }
        }
        let expected = completed + count;
        let completion = wait_until(Duration::from_secs(5), || {
            let received = state
                .statistics
                .snapshot()
                .inbound_packets
                .saturating_sub(inbound_start);
            received == (expected * args.peers) as u64
                && peer_ids.iter().all(|(client_index, peer)| {
                    let expected_id = (*client_index * args.operations + expected) as u16;
                    state
                        .message_subscriptions
                        .get(peer)
                        .is_some_and(|set| set.len() == 1 && set.contains(&expected_id))
                })
        })
        .await;
        if completion.is_err() {
            let received = state
                .statistics
                .snapshot()
                .inbound_packets
                .saturating_sub(inbound_start);
            let stale: Vec<_> = peer_ids
                .iter()
                .filter_map(|(client_index, peer)| {
                    let expected_id = (*client_index * args.operations + expected) as u16;
                    let actual = state
                        .message_subscriptions
                        .get(peer)
                        .map(|set| set.iter().copied().collect::<Vec<_>>());
                    (actual.as_deref() != Some(&[expected_id][..]))
                        .then_some(format!("{peer:?}:{actual:?}"))
                })
                .collect();
            anyhow::bail!(
                "batch completion timed out at operation {expected}: expected processed={}, observed={received}; stale final sets={stale:?}",
                expected * args.peers
            );
        }
        // The exact handler counter proves the batch's message count; the map check confirms
        // each peer committed its newest unique ID after the ordered batch.
        batch_ms.push(batch_start.elapsed().as_secs_f64() * 1000.0);
        completed = expected;
    }
    let elapsed = run_start.elapsed();
    wait_until(Duration::from_secs(5), || {
        clients
            .iter()
            .all(|client| client.pending.lock().unwrap().is_empty())
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out draining client reliable-send windows"))?;
    let stats = state.statistics.snapshot();
    let transport_end = state.transport.stats_snapshot();
    let inbound_delta = stats.inbound_packets.saturating_sub(inbound_start);
    let protocol_errors = stats.protocol_errors.saturating_sub(protocol_start);
    if inbound_delta != (args.operations * args.peers) as u64 {
        anyhow::bail!(
            "processed counter mismatch: expected {}, observed {inbound_delta}",
            args.operations * args.peers
        );
    }
    if peer_ids.iter().any(|(client_index, peer)| {
        let expected_id = (*client_index * args.operations + args.operations) as u16;
        state
            .message_subscriptions
            .get(peer)
            .is_none_or(|set| set.len() != 1 || !set.contains(&expected_id))
    }) {
        anyhow::bail!("final subscription set did not contain the final unique ID");
    }
    let retries = transport_end
        .reliable_retransmits
        .saturating_sub(transport_start.reliable_retransmits);
    let acks = transport_end
        .reliable_acks_received
        .saturating_sub(transport_start.reliable_acks_received);
    let ack_packets_sent: u64 = clients
        .iter()
        .map(|client| client.ack_packets_sent.load(Ordering::Relaxed))
        .sum();
    let ack_errors: u64 = clients
        .iter()
        .map(|client| client.ack_errors.load(Ordering::Relaxed))
        .sum();
    let client_transmissions: u64 = clients
        .iter()
        .map(|client| client.transmissions.load(Ordering::Relaxed))
        .sum();
    let client_retransmits: u64 = clients
        .iter()
        .map(|client| client.retransmits.load(Ordering::Relaxed))
        .sum();
    anyhow::ensure!(
        protocol_errors == 0,
        "server reported {protocol_errors} protocol errors"
    );
    anyhow::ensure!(
        ack_errors == 0,
        "client reported {ack_errors} ACK or parse errors"
    );
    batch_ms.sort_by(f64::total_cmp);
    let total = (args.operations * args.peers) as f64;
    let result = serde_json::json!({
        "benchmark": "registry_subscribe_udp_dispatch",
        "peers": args.peers,
        "unique_operations_per_peer": args.operations,
        "processed_operations": inbound_delta,
        "elapsed_seconds": elapsed.as_secs_f64(),
        "operations_per_second": total / elapsed.as_secs_f64(),
        "batch_size_per_peer": args.batch_size,
        "batch_completion_p50_ms": percentile(&batch_ms, 0.50),
        "batch_completion_p95_ms": percentile(&batch_ms, 0.95),
        "protocol_errors_delta": protocol_errors,
        "reliable_retransmits_delta": retries,
        "reliable_acks_received_delta": acks,
        "client_ack_packets_sent": ack_packets_sent,
        "client_ack_errors": ack_errors,
        "client_transmissions": client_transmissions,
        "client_retransmits": client_retransmits,
        "client_pending_at_end": 0,
        "cpu_scope": "server and UDP driver share this process; CPU not measured separately",
        "tokio_workers": 4,
        "config": args.config,
    });
    println!("RESULT {result}");
    for task in ack_tasks {
        task.abort();
    }
    Ok(())
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    let index = ((sorted.len() as f64 * q).ceil() as usize).saturating_sub(1);
    sorted[index.min(sorted.len() - 1)]
}

async fn wait_until(
    timeout_for: Duration,
    mut condition: impl FnMut() -> bool,
) -> anyhow::Result<()> {
    timeout(timeout_for, async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("timed out waiting for peer or registry completion"))
}

struct Client {
    socket: Arc<UdpSocket>,
    reliable_sequence: u16,
    ack_windows: HashMap<u8, u16>,
    ack_packets_sent: Arc<AtomicU64>,
    ack_errors: Arc<AtomicU64>,
    pending: Arc<Mutex<HashMap<u16, PendingPacket>>>,
    transmissions: Arc<AtomicU64>,
    retransmits: Arc<AtomicU64>,
}

#[derive(Clone)]
struct PendingPacket {
    bytes: Vec<u8>,
    last_sent: Instant,
}

impl Client {
    async fn connect(
        address: SocketAddr,
        peer: usize,
        company_name: &str,
        product_name: &str,
    ) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind("127.0.0.1:0").await?;
        socket.connect(address).await?;
        let mut request = NetWriter::new();
        request.put_u8(PacketProperty::ConnectRequest as u8);
        request.put_i32(basis_protocol::version::LITENETLIB_PROTOCOL_ID);
        request.put_i64(1);
        request.put_i32(0);
        request.put_u8(16);
        request.put_bytes(&[0; 16]);
        request.put_u16(SERVER_VERSION);
        NetworkApplication::write(&mut request, company_name, product_name)?;
        basis_protocol::messages::BytesMessage { data: Vec::new() }.serialize(&mut request)?;
        let ready = ready_message(peer);
        ready.serialize(&mut request)?;
        socket.send(request.as_slice()).await?;
        let mut client = Self {
            socket: Arc::new(socket),
            reliable_sequence: 0,
            ack_windows: HashMap::new(),
            ack_packets_sent: Arc::new(AtomicU64::new(0)),
            ack_errors: Arc::new(AtomicU64::new(0)),
            pending: Arc::new(Mutex::new(HashMap::new())),
            transmissions: Arc::new(AtomicU64::new(0)),
            retransmits: Arc::new(AtomicU64::new(0)),
        };
        timeout(Duration::from_secs(30), client.receive_metadata()).await??;
        Ok(client)
    }

    async fn subscribe(&mut self, id: u16) -> anyhow::Result<()> {
        // Keep the oldest unacknowledged request inside the 128-sequence window.
        // Application completion alone does not imply that its ACK has arrived.
        wait_until(Duration::from_secs(5), || {
            self.pending.lock().unwrap().keys().all(|sequence| {
                basis_transport::relative_sequence(self.reliable_sequence, *sequence)
                    < basis_transport::DEFAULT_WINDOW_SIZE as i32
            })
        })
        .await?;
        let mut payload = NetWriter::new();
        payload.put_u8(channels::REGISTRY_SUB_SUBSCRIBE);
        BasisMessageSubscribe { ids: vec![id] }.serialize(&mut payload)?;
        let mut packet = NetWriter::new();
        packet.put_u8(PacketProperty::Channeled as u8);
        let sequence = self.reliable_sequence;
        packet.put_u16(sequence);
        self.reliable_sequence = self.reliable_sequence.wrapping_add(1);
        packet.put_u8(DeliveryMethod::channel_id(
            channels::REGISTRY_CONTROL,
            DeliveryMethod::ReliableOrdered,
        ));
        packet.put_bytes(payload.as_slice());
        let bytes = packet.as_slice().to_vec();
        self.pending.lock().unwrap().insert(
            sequence,
            PendingPacket {
                bytes: bytes.clone(),
                last_sent: Instant::now(),
            },
        );
        self.socket.send(&bytes).await?;
        self.transmissions.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn start_ack_loop(&self) -> tokio::task::JoinHandle<()> {
        let socket = Arc::clone(&self.socket);
        let mut ack_windows = self.ack_windows.clone();
        let ack_packets_sent = Arc::clone(&self.ack_packets_sent);
        let ack_errors = Arc::clone(&self.ack_errors);
        let pending = Arc::clone(&self.pending);
        let transmissions = Arc::clone(&self.transmissions);
        let retransmits = Arc::clone(&self.retransmits);
        tokio::spawn(async move {
            let mut retry_tick = interval(Duration::from_millis(5));
            retry_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                let mut packet = [0u8; 65535];
                tokio::select! {
                    received = socket.recv(&mut packet) => {
                        let Ok(length) = received else { return };
                        let mut frames = Vec::new();
                        let mut acknowledgments = Vec::new();
                        if unpack(&packet[..length], &mut frames, &mut acknowledgments).is_err() {
                            ack_errors.fetch_add(1, Ordering::Relaxed);
                            continue;
                        }
                        for frame in frames {
                            if let Frame::Reliable { channel_id, sequence, .. } = frame {
                                match ack_packet(&socket, channel_id, sequence, &mut ack_windows).await {
                                    Ok(()) => { ack_packets_sent.fetch_add(1, Ordering::Relaxed); }
                                    Err(_) => { ack_errors.fetch_add(1, Ordering::Relaxed); }
                                }
                            }
                        }
                        for ack in acknowledgments {
                            apply_ack(&pending, ack);
                        }
                    }
                    _ = retry_tick.tick() => {
                        let now = Instant::now();
                        let due: Vec<_> = {
                            let mut pending = pending.lock().unwrap();
                            pending.iter_mut().filter_map(|(sequence, item)| {
                                if now.duration_since(item.last_sent) >= Duration::from_millis(100) {
                                    item.last_sent = now;
                                    Some((*sequence, item.bytes.clone()))
                                } else { None }
                            }).collect()
                        };
                        for (_, bytes) in due {
                            match socket.send(&bytes).await {
                                Ok(_) => {
                                    transmissions.fetch_add(1, Ordering::Relaxed);
                                    retransmits.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(_) => { ack_errors.fetch_add(1, Ordering::Relaxed); }
                            }
                        }
                    }
                }
            }
        })
    }

    async fn receive_metadata(&mut self) -> anyhow::Result<()> {
        loop {
            let mut packet = [0u8; 65535];
            let length = self.socket.recv(&mut packet).await?;
            let datagram = &packet[..length];
            if PacketProperty::from_byte(datagram[0]) == Some(PacketProperty::ConnectAccept) {
                continue;
            }
            let mut frames = Vec::new();
            let mut acknowledgments = Vec::new();
            unpack(datagram, &mut frames, &mut acknowledgments)?;
            for ack in acknowledgments {
                apply_ack(&self.pending, ack);
            }
            for frame in frames {
                if let Frame::Reliable {
                    channel,
                    channel_id,
                    sequence,
                    payload,
                } = frame
                {
                    ack_packet(&self.socket, channel_id, sequence, &mut self.ack_windows).await?;
                    self.ack_packets_sent.fetch_add(1, Ordering::Relaxed);
                    if channel == channels::META_DATA && !payload.is_empty() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn ack_packet(
    socket: &UdpSocket,
    channel_id: u8,
    sequence: u16,
    ack_windows: &mut HashMap<u8, u16>,
) -> std::io::Result<()> {
    let window_start = ack_windows.entry(channel_id).or_insert(0);
    if sequence < *window_start {
        return Ok(());
    }
    if sequence - *window_start >= 128 {
        *window_start = sequence - 127;
    }
    let mut ack = vec![PacketProperty::Ack as u8];
    ack.extend_from_slice(&window_start.to_le_bytes());
    ack.push(channel_id);
    ack.extend_from_slice(&[0; (basis_transport::DEFAULT_WINDOW_SIZE - 1) / 8 + 2]);
    ack[4 + (sequence as usize % 128) / 8] |= 1 << (sequence % 8);
    socket.send(&ack).await?;
    Ok(())
}

fn ready_message(peer: usize) -> ReadyMessage {
    ReadyMessage {
        player_meta_data_message: ClientMetaDataMessage {
            player_uuid: format!("00000000-0000-4000-8000-{:012x}", peer + 1),
            player_display_name: format!("Probe {peer}"),
            player_platform: "linux".into(),
        },
        client_avatar_change_message: ClientAvatarChangeMessage {
            load_mode: 0,
            byte_array: Vec::new(),
            local_avatar_index: 0,
            arm_scale: 1.0,
            leg_scale: 1.0,
            torso_scale: 1.0,
        },
        local_avatar_sync_message: LocalAvatarSyncMessage::empty_high(),
    }
}

enum Frame {
    Reliable {
        channel: u8,
        channel_id: u8,
        sequence: u16,
        payload: Vec<u8>,
    },
    Unreliable,
}

struct AckFrame {
    channel_id: u8,
    window_start: u16,
    bits: Vec<u8>,
}

fn unpack(
    packet: &[u8],
    frames: &mut Vec<Frame>,
    acknowledgments: &mut Vec<AckFrame>,
) -> anyhow::Result<()> {
    if packet.is_empty() {
        anyhow::bail!("empty server datagram");
    }
    match PacketProperty::from_byte(packet[0]) {
        Some(PacketProperty::Channeled) if packet.len() >= 4 => {
            let sequence = u16::from_le_bytes([packet[1], packet[2]]);
            let channel_id = packet[3];
            let offset = if packet[0] & 128 != 0 { 10 } else { 4 };
            if packet.len() < offset {
                anyhow::bail!("short reliable frame");
            }
            frames.push(Frame::Reliable {
                channel: channel_id / 4,
                channel_id,
                sequence,
                payload: packet[offset..].to_vec(),
            });
        }
        Some(PacketProperty::Unreliable) if packet.len() >= 2 => frames.push(Frame::Unreliable),
        Some(PacketProperty::Ack)
            if packet.len() == 4 + (basis_transport::DEFAULT_WINDOW_SIZE - 1) / 8 + 2 =>
        {
            acknowledgments.push(AckFrame {
                channel_id: packet[3],
                window_start: u16::from_le_bytes([packet[1], packet[2]]),
                bits: packet[4..].to_vec(),
            });
        }
        Some(PacketProperty::Merged) => {
            let mut reader = NetReader::new(&packet[1..]);
            while reader.remaining() > 0 {
                let size = reader.get_u16()? as usize;
                let inner = reader.get_bytes(size)?;
                unpack(inner, frames, acknowledgments)?;
            }
        }
        Some(PacketProperty::CompactMerged) => {
            let mut reader = NetReader::new(&packet[1..]);
            while reader.remaining() > 0 {
                let tag = reader.get_u8()?;
                let size = if tag & 128 != 0 {
                    reader.get_u16()? as usize
                } else {
                    reader.get_u8()? as usize
                };
                let payload = reader.get_bytes(size)?;
                if tag & 64 != 0 {
                    unpack(payload, frames, acknowledgments)?;
                } else {
                    frames.push(Frame::Unreliable);
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn apply_ack(pending: &Arc<Mutex<HashMap<u16, PendingPacket>>>, ack: AckFrame) {
    if ack.channel_id
        != DeliveryMethod::channel_id(channels::REGISTRY_CONTROL, DeliveryMethod::ReliableOrdered)
    {
        return;
    }
    let mut pending = pending.lock().unwrap();
    pending.retain(|sequence, _| {
        let relative = basis_transport::relative_sequence(*sequence, ack.window_start);
        let in_window = (0..128).contains(&relative);
        let acknowledged = in_window
            && ack
                .bits
                .get((*sequence as usize % 128) / 8)
                .is_some_and(|byte| byte & (1 << (*sequence % 8)) != 0);
        !acknowledged
    });
}
