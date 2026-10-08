//! Controlled UDP loss/reordering harness for ReliableOrdered receive processing.
//!
//! Run scenarios one at a time:
//! `cargo run --release -p basis-transport --example reliable-loss-benchmark -- --scenario nofault`
//! Both endpoints are real `TransportHandle`s. The proxy faults only first transmissions.

use basis_protocol::version::LITENETLIB_PROTOCOL_ID;
use basis_transport::{DeliveryMethod, PacketProperty, ServerEvent, TransportHandle};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{net::UdpSocket, sync::oneshot, time};

const MESSAGE_COUNT: usize = 512;
const PAYLOAD_BYTES: usize = 16;
const CHANNEL: u8 = 0;
const CHANNEL_ID: u8 = CHANNEL * 4 + DeliveryMethod::ReliableOrdered as u8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scenario {
    NoFault,
    Loss,
    Reorder,
}

impl Scenario {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "nofault" => Ok(Self::NoFault),
            "loss" => Ok(Self::Loss),
            "reorder" => Ok(Self::Reorder),
            _ => Err(format!(
                "unknown scenario {value:?}; expected nofault, loss, or reorder"
            )),
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::NoFault => "nofault",
            Self::Loss => "loss",
            Self::Reorder => "reorder",
        }
    }
}

#[derive(Clone, Default)]
struct ProxyStats {
    transmissions_by_sequence: HashMap<u16, usize>,
    drop_to_retransmission_ms: Vec<f64>,
    dropped_first_transmissions: usize,
    reordered_pairs: usize,
    ack_packets_forwarded: usize,
    ack_bits_forwarded: usize,
}

#[derive(Default)]
struct ReceiverStats {
    unique_received: usize,
    duplicates: usize,
    delivered: usize,
    ordering_or_payload_errors: usize,
    delivery_gap_ms: Vec<f64>,
    first_transmission_to_delivery_ms: Vec<f64>,
}

struct ReceiverConfig {
    expected_addr: SocketAddr,
    count: usize,
    benchmark_start: Arc<Mutex<Option<Instant>>>,
    first_transmitted_at: Arc<Mutex<HashMap<u16, Instant>>>,
}

struct ProxyState {
    stats: ProxyStats,
    held_for_reorder: Option<(u16, Vec<u8>)>,
    dropped_sequences: HashSet<u16>,
    first_transmitted_at: Arc<Mutex<HashMap<u16, Instant>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let (scenario, count) = parse_args()?;
    run(scenario, count).await
}

fn parse_args() -> Result<(Scenario, usize), Box<dyn Error>> {
    let mut scenario = Scenario::NoFault;
    let mut count = MESSAGE_COUNT;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--scenario" => scenario = Scenario::parse(&args.next().ok_or("missing scenario")?)?,
            "--messages" => count = args.next().ok_or("missing message count")?.parse()?,
            "--help" | "-h" => {
                println!("--scenario nofault|loss|reorder [--messages N]");
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument {arg:?}").into()),
        }
    }
    if count == 0 || count >= 32768 {
        return Err("message count must be in 1..32768".into());
    }
    Ok((scenario, count))
}

async fn run(scenario: Scenario, count: usize) -> Result<(), Box<dyn Error>> {
    let (sender, sender_events) =
        TransportHandle::bind_with_statistics_options(loopback_addr(0), true, true).await?;
    let (receiver, receiver_events) =
        TransportHandle::bind_with_statistics_options(loopback_addr(0), true, true).await?;
    let sender_addr = sender.local_addr()?;
    let receiver_addr = receiver.local_addr()?;

    // Each side sees a distinct proxy source address for the reverse route, so each
    // TransportHandle's peer address matches the source of its returned ACKs.
    let front = Arc::new(UdpSocket::bind(loopback_addr(0)).await?);
    let back = Arc::new(UdpSocket::bind(loopback_addr(0)).await?);
    let front_addr = front.local_addr()?;
    let back_addr = back.local_addr()?;
    let proxy_stats = Arc::new(Mutex::new(ProxyStats::default()));
    let first_transmitted_at = Arc::new(Mutex::new(HashMap::new()));
    let proxy_task = tokio::spawn(run_proxy(
        front.clone(),
        back.clone(),
        sender_addr,
        receiver_addr,
        scenario,
        proxy_stats.clone(),
        first_transmitted_at.clone(),
    ));

    let (sender_peer_tx, sender_peer_rx) = oneshot::channel();
    let sender_task = tokio::spawn(accept_peer(
        sender.clone(),
        sender_events,
        front_addr,
        sender_peer_tx,
    ));
    let (receiver_peer_tx, receiver_peer_rx) = oneshot::channel();
    let (receiver_complete_tx, receiver_complete_rx) = oneshot::channel();
    let (sender_drained_tx, sender_drained_rx) = oneshot::channel();
    let benchmark_start = Arc::new(Mutex::new(None));
    let receiver_task = tokio::spawn(receive_messages(
        receiver.clone(),
        receiver_events,
        ReceiverConfig {
            expected_addr: back_addr,
            count,
            benchmark_start: benchmark_start.clone(),
            first_transmitted_at,
        },
        receiver_peer_tx,
        receiver_complete_tx,
        sender_drained_rx,
    ));

    // The proxy sockets themselves originate the ConnectRequests. Consequently, the
    // accepted peers on each endpoint retain the proxy address used on their data route.
    front
        .send_to(&connect_request(0x1234_5678), sender_addr)
        .await?;
    back.send_to(&connect_request(0x1234_5679), receiver_addr)
        .await?;
    let sender_peer = time::timeout(Duration::from_secs(5), sender_peer_rx).await??;
    let receiver_peer = time::timeout(Duration::from_secs(5), receiver_peer_rx).await??;

    let started = Instant::now();
    *benchmark_start.lock().unwrap() = Some(started);
    for index in 0..count {
        let payload = make_payload(index as u32);
        sender
            .send(
                sender_peer,
                CHANNEL,
                DeliveryMethod::ReliableOrdered,
                &payload,
            )
            .await?;
    }

    let receiver_completion_ms =
        time::timeout(Duration::from_secs(30), receiver_complete_rx).await??;
    time::timeout(Duration::from_secs(5), async {
        while sender.pending_reliable_count() != 0 || sender.queued_reliable_count() != 0 {
            time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    let ack_drain_elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    let _ = sender_drained_tx.send(());
    let receiver_stats = time::timeout(Duration::from_secs(5), receiver_task)
        .await??
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let proxy_stats = proxy_stats.lock().unwrap();
    let transmissions: usize = proxy_stats.transmissions_by_sequence.values().sum();
    let retransmissions: usize = proxy_stats
        .transmissions_by_sequence
        .values()
        .map(|transmissions| transmissions.saturating_sub(1))
        .sum();
    let transport_retransmits = sender.stats_snapshot().reliable_retransmits;
    let delivery_gap_p95_ms = percentile95(&receiver_stats.delivery_gap_ms);
    let first_transmission_to_delivery_p95_ms =
        percentile95(&receiver_stats.first_transmission_to_delivery_ms);
    let drop_to_retransmission_p95_ms = percentile95(&proxy_stats.drop_to_retransmission_ms);

    println!(
        "{{\"scenario\":\"{}\",\"messages\":{},\"payload_bytes\":{},\"transmissions\":{},\"retransmissions\":{},\"transport_retransmits\":{},\"ack_packets_forwarded\":{},\"ack_bits_forwarded\":{},\"dropped_first_transmissions\":{},\"reordered_pairs\":{},\"unique_received\":{},\"duplicates\":{},\"delivered\":{},\"ordering_or_payload_errors\":{},\"receiver_completion_ms\":{:.3},\"ack_drain_elapsed_ms\":{:.3},\"delivery_gap_p95_ms\":{},\"first_transmission_to_delivery_p95_ms\":{},\"proxy_drop_to_retransmission_p95_ms\":{},\"cpu_ms\":null}}",
        scenario.as_str(),
        count,
        PAYLOAD_BYTES,
        transmissions,
        retransmissions,
        transport_retransmits,
        proxy_stats.ack_packets_forwarded,
        proxy_stats.ack_bits_forwarded,
        proxy_stats.dropped_first_transmissions,
        proxy_stats.reordered_pairs,
        receiver_stats.unique_received,
        receiver_stats.duplicates,
        receiver_stats.delivered,
        receiver_stats.ordering_or_payload_errors,
        receiver_completion_ms,
        ack_drain_elapsed_ms,
        delivery_gap_p95_ms
            .map(|value| format!("{value:.3}"))
            .unwrap_or_else(|| "null".to_owned()),
        first_transmission_to_delivery_p95_ms
            .map(|value| format!("{value:.3}"))
            .unwrap_or_else(|| "null".to_owned()),
        drop_to_retransmission_p95_ms
            .map(|value| format!("{value:.3}"))
            .unwrap_or_else(|| "null".to_owned()),
    );

    let _ = receiver_peer;
    sender.shutdown();
    receiver.shutdown();
    proxy_task.abort();
    sender_task.abort();
    Ok(())
}

async fn accept_peer(
    handle: TransportHandle,
    mut events: tokio::sync::mpsc::Receiver<ServerEvent>,
    expected_addr: SocketAddr,
    ready: oneshot::Sender<basis_transport::PeerId>,
) -> Result<(), basis_transport::TransportError> {
    let mut ready = Some(ready);
    while let Some(event) = events.recv().await {
        if let ServerEvent::ConnectionRequest(request) = event {
            if request.remote_addr != expected_addr {
                continue;
            }
            let peer = handle.accept(&request).await?;
            if let Some(ready) = ready.take() {
                let _ = ready.send(peer);
            }
        }
    }
    Ok(())
}

async fn receive_messages(
    handle: TransportHandle,
    mut events: tokio::sync::mpsc::Receiver<ServerEvent>,
    config: ReceiverConfig,
    ready: oneshot::Sender<basis_transport::PeerId>,
    complete: oneshot::Sender<f64>,
    drained: oneshot::Receiver<()>,
) -> Result<ReceiverStats, basis_transport::TransportError> {
    let mut ready = Some(ready);
    let mut complete = Some(complete);
    let mut stats = ReceiverStats::default();
    let mut seen_payloads = HashSet::with_capacity(config.count);
    let mut expected_index = 0u32;
    let mut last_delivery_at = None;
    while let Some(event) = events.recv().await {
        match event {
            ServerEvent::ConnectionRequest(request)
                if request.remote_addr == config.expected_addr =>
            {
                let peer = handle.accept(&request).await?;
                if let Some(ready) = ready.take() {
                    let _ = ready.send(peer);
                }
            }
            event @ ServerEvent::Message { .. } => {
                record_message(
                    event,
                    &mut stats,
                    &mut seen_payloads,
                    &mut expected_index,
                    &mut last_delivery_at,
                    &config.first_transmitted_at,
                );
                if stats.unique_received >= config.count {
                    let elapsed = config
                        .benchmark_start
                        .lock()
                        .unwrap()
                        .map(|start| start.elapsed().as_secs_f64() * 1000.0)
                        .unwrap_or_default();
                    if let Some(complete) = complete.take() {
                        let _ = complete.send(elapsed);
                    }
                    break;
                }
            }
            _ => {}
        }
    }

    let _ = drained.await;
    let tail = time::sleep(Duration::from_millis(50));
    tokio::pin!(tail);
    loop {
        tokio::select! {
            _ = &mut tail => break,
            event = events.recv() => {
                let Some(event) = event else { break; };
                record_message(
                    event,
                    &mut stats,
                    &mut seen_payloads,
                    &mut expected_index,
                    &mut last_delivery_at,
                    &config.first_transmitted_at,
                );
            }
        }
    }
    Ok(stats)
}

fn record_message(
    event: ServerEvent,
    stats: &mut ReceiverStats,
    seen_payloads: &mut HashSet<u32>,
    expected_index: &mut u32,
    last_delivery_at: &mut Option<Instant>,
    first_transmitted_at: &Mutex<HashMap<u16, Instant>>,
) {
    let ServerEvent::Message {
        channel,
        delivery,
        payload,
        ..
    } = event
    else {
        return;
    };
    if channel != CHANNEL || delivery != DeliveryMethod::ReliableOrdered {
        return;
    }
    let Some(index) = read_payload_index(&payload) else {
        stats.ordering_or_payload_errors += 1;
        return;
    };
    if !seen_payloads.insert(index) {
        stats.duplicates += 1;
        return;
    }
    let now = Instant::now();
    stats.unique_received += 1;
    if let Some(previous) = last_delivery_at.replace(now) {
        stats
            .delivery_gap_ms
            .push(now.duration_since(previous).as_secs_f64() * 1000.0);
    }
    if let Some(first) = first_transmitted_at
        .lock()
        .unwrap()
        .get(&(index as u16))
        .copied()
    {
        stats
            .first_transmission_to_delivery_ms
            .push(now.duration_since(first).as_secs_f64() * 1000.0);
    } else {
        stats.ordering_or_payload_errors += 1;
    }
    if index != *expected_index {
        stats.ordering_or_payload_errors += 1;
    } else {
        stats.delivered += 1;
        *expected_index += 1;
    }
}

async fn run_proxy(
    front: Arc<UdpSocket>,
    back: Arc<UdpSocket>,
    sender_addr: SocketAddr,
    receiver_addr: SocketAddr,
    scenario: Scenario,
    shared_stats: Arc<Mutex<ProxyStats>>,
    first_transmitted_at: Arc<Mutex<HashMap<u16, Instant>>>,
) -> Result<(), std::io::Error> {
    let mut state = ProxyState {
        stats: ProxyStats::default(),
        held_for_reorder: None,
        dropped_sequences: HashSet::new(),
        first_transmitted_at,
    };
    let mut front_buf = vec![0; 64 * 1024];
    let mut back_buf = vec![0; 64 * 1024];
    loop {
        tokio::select! {
            result = front.recv_from(&mut front_buf) => {
                let (len, from) = result?;
                if from != sender_addr { continue; }
                let datagram = &front_buf[..len];
                if is_connect_accept(datagram) { continue; }
                forward_sender_datagram(
                    datagram,
                    &back,
                    receiver_addr,
                    scenario,
                    &mut state,
                ).await?;
                *shared_stats.lock().unwrap() = state.stats.clone();
            }
            result = back.recv_from(&mut back_buf) => {
                let (len, from) = result?;
                if from != receiver_addr { continue; }
                let datagram = &back_buf[..len];
                if is_connect_accept(datagram) { continue; }
                state.stats.ack_packets_forwarded += split_datagram(datagram)
                    .iter()
                    .filter(|packet| packet.first().is_some_and(|header| {
                        header & 0x1f == PacketProperty::Ack as u8
                    }))
                    .count();
                state.stats.ack_bits_forwarded += split_datagram(datagram)
                    .iter()
                    .filter(|packet| {
                        packet.len() >= 21
                            && packet[0] & 0x1f == PacketProperty::Ack as u8
                            && packet[3] == CHANNEL_ID
                    })
                    .map(|packet| packet[4..].iter().map(|byte| byte.count_ones() as usize).sum::<usize>())
                    .sum::<usize>();
                *shared_stats.lock().unwrap() = state.stats.clone();
                front.send_to(datagram, sender_addr).await?;
            }
        }
    }
}

async fn forward_sender_datagram(
    datagram: &[u8],
    back: &UdpSocket,
    receiver_addr: SocketAddr,
    scenario: Scenario,
    state: &mut ProxyState,
) -> Result<(), std::io::Error> {
    let is_merged = datagram
        .first()
        .is_some_and(|header| header & 0x1f == PacketProperty::Merged as u8);
    let packets = split_datagram(datagram);
    let mut outgoing = Vec::with_capacity(packets.len() + 1);
    let mut changed = false;
    let mut delayed_packet = None;
    for packet in packets {
        let Some(sequence) = reliable_ordered_sequence(&packet) else {
            outgoing.push(packet);
            continue;
        };
        let now = Instant::now();
        state
            .first_transmitted_at
            .lock()
            .unwrap()
            .entry(sequence)
            .or_insert(now);
        let transmission = {
            let stats = &mut state.stats;
            let count = stats.transmissions_by_sequence.entry(sequence).or_default();
            *count += 1;
            *count
        };
        if scenario == Scenario::Loss && transmission == 1 && (sequence == 10 || sequence % 20 == 7)
        {
            state.stats.dropped_first_transmissions += 1;
            state.dropped_sequences.insert(sequence);
            changed = true;
            continue;
        }
        if transmission > 1 && state.dropped_sequences.remove(&sequence) {
            if let Some(first) = state
                .first_transmitted_at
                .lock()
                .unwrap()
                .get(&sequence)
                .copied()
            {
                state
                    .stats
                    .drop_to_retransmission_ms
                    .push(now.duration_since(first).as_secs_f64() * 1000.0);
            }
        }
        if scenario == Scenario::Reorder
            && transmission == 1
            && sequence == 10
            && state.held_for_reorder.is_none()
        {
            state.held_for_reorder = Some((sequence, packet));
            changed = true;
            continue;
        }
        if scenario == Scenario::Reorder {
            if let Some((held_sequence, held)) = state.held_for_reorder.take() {
                if sequence > held_sequence {
                    outgoing.push(packet);
                    delayed_packet = Some((held_sequence, held));
                    state.stats.reordered_pairs += 1;
                    changed = true;
                    continue;
                }
                state.held_for_reorder = Some((held_sequence, held));
            }
        }
        outgoing.push(packet);
    }

    if changed {
        if !outgoing.is_empty() {
            let bytes = if is_merged {
                build_merged(datagram[0], &outgoing)
            } else if outgoing.len() == 1 {
                outgoing[0].clone()
            } else {
                build_merged(PacketProperty::Merged as u8, &outgoing)
            };
            back.send_to(&bytes, receiver_addr).await?;
        }
    } else {
        back.send_to(datagram, receiver_addr).await?;
    }
    if let Some((_, packet)) = delayed_packet {
        time::sleep(Duration::from_millis(10)).await;
        back.send_to(&packet, receiver_addr).await?;
    }
    Ok(())
}

fn connect_request(connect_time: i64) -> Vec<u8> {
    let mut packet = Vec::with_capacity(34);
    packet.push(PacketProperty::ConnectRequest as u8);
    packet.extend_from_slice(&LITENETLIB_PROTOCOL_ID.to_le_bytes());
    packet.extend_from_slice(&connect_time.to_le_bytes());
    packet.extend_from_slice(&0i32.to_le_bytes());
    packet.push(16);
    packet.extend_from_slice(&[0; 16]);
    packet
}

fn is_connect_accept(packet: &[u8]) -> bool {
    packet
        .first()
        .is_some_and(|header| header & 0x1f == PacketProperty::ConnectAccept as u8)
}

fn split_datagram(datagram: &[u8]) -> Vec<Vec<u8>> {
    match datagram.first() {
        Some(header) if header & 0x1f == PacketProperty::Merged as u8 => {}
        _ => return vec![datagram.to_vec()],
    }
    let mut packets = Vec::new();
    let mut position = 1;
    while position + 2 <= datagram.len() {
        let size = u16::from_le_bytes([datagram[position], datagram[position + 1]]) as usize;
        position += 2;
        if size == 0 || position + size > datagram.len() {
            break;
        }
        packets.push(datagram[position..position + size].to_vec());
        position += size;
    }
    packets
}

fn build_merged(header: u8, packets: &[Vec<u8>]) -> Vec<u8> {
    if packets.len() == 1 {
        return packets[0].clone();
    }
    let mut datagram = vec![header];
    for packet in packets {
        datagram.extend_from_slice(&(packet.len() as u16).to_le_bytes());
        datagram.extend_from_slice(packet);
    }
    datagram
}

fn reliable_ordered_sequence(packet: &[u8]) -> Option<u16> {
    if packet.len() < 4
        || packet[0] & 0x1f != PacketProperty::Channeled as u8
        || packet[3] != CHANNEL_ID
    {
        return None;
    }
    Some(u16::from_le_bytes([packet[1], packet[2]]))
}

fn make_payload(index: u32) -> [u8; PAYLOAD_BYTES] {
    let mut payload = [0x5a; PAYLOAD_BYTES];
    payload[..4].copy_from_slice(&index.to_le_bytes());
    payload
}

fn read_payload_index(payload: &[u8]) -> Option<u32> {
    Some(u32::from_le_bytes(payload.get(..4)?.try_into().ok()?))
}

fn percentile95(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let index = ((values.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    sorted.get(index).copied()
}

#[cfg(test)]
mod tests {
    use super::percentile95;

    #[test]
    fn percentile_sorts_observations_without_changing_input() {
        let values: Vec<f64> = (1..=20).rev().map(f64::from).collect();
        assert_eq!(percentile95(&values), Some(19.0));
        assert_eq!(values[0], 20.0);
        assert_eq!(percentile95(&[]), None);
        assert_eq!(percentile95(&[7.0]), Some(7.0));
    }
}

fn loopback_addr(port: u16) -> SocketAddr {
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
}
