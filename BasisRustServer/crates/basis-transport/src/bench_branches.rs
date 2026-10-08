//! Branch-miss benchmark for the transport hot path.
//!
//! Exercises the synchronous helpers every datagram crosses —
//! `PacketProperty::from_byte`, `parse_message_packet`, `build_outbound_packet`,
//! `build_merged_datagrams`, and `process_ack` — under a realistic distribution where
//! valid traffic dominates (>=99%) and malformed/error branches stay rare.
//!
//! Timing A/B compares two builds of this same harness, so only library code differs
//! between baseline and optimized runs. Run explicitly in release mode from
//! `BasisRustServer/`:
//!
//! ```sh
//! cargo test --release -p basis-transport -- --ignored bench_branches --nocapture
//! ```
//!
//! On a host that provides `perf`, wrap the same command for hardware branch-miss counters:
//!
//! ```sh
//! perf stat -e branches,branch-misses \
//!     cargo test --release -p basis-transport -- --ignored bench_branches --nocapture
//! ```

use std::hint::black_box;

use super::*;

/// Deterministic PRNG so every run sees identical inputs and runs stay comparable.
struct XorShift64(u64);

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
}

const PASSES: usize = 9;
const PROPERTY_OPS: usize = 2_000_000;
const MESSAGE_OPS: usize = 1_000_000;
const OUTBOUND_OPS: usize = 100_000;
const MERGED_BATCHES: usize = 2_000;
const MERGED_BATCH: usize = 32;
const ACK_OPS: usize = 200_000;
/// The bench drives one reliable channel; LiteNetLib channel ids encode
/// `channel * 4 + delivery`, so this is channel 1, ReliableOrdered.
const CHANNEL_ID: u8 = 6;
const RELEASE_PER_ACK: usize = 16;

/// Times `PASSES` executions of `f` and returns the median ns/op. Inputs are precomputed
/// outside the timed closure by each phase; the closure only runs the code under test.
fn measure(label: &str, ops_per_pass: usize, mut f: impl FnMut() -> u64) -> f64 {
    // Warm caches and predictors once; keep the result alive so nothing is elided.
    let mut checksum = black_box(f());
    let mut times = Vec::with_capacity(PASSES);
    for _ in 0..PASSES {
        let start = Instant::now();
        checksum = checksum.wrapping_add(black_box(f()));
        times.push(start.elapsed());
    }
    times.sort_unstable();
    let median = times[PASSES / 2];
    let ns_per_op = median.as_nanos() as f64 / ops_per_pass as f64;
    println!("  {label}: {ns_per_op:.2} ns/op (median of {PASSES} passes)");
    black_box(checksum);
    ns_per_op
}

fn test_peer_state() -> PeerState {
    PeerState {
        id: 1,
        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 40000),
        connection_number: 0,
        connect_time: 1,
        last_seen: parking_lot::Mutex::new(Instant::now()),
        last_ping_sent: parking_lot::Mutex::new(Instant::now()),
        next_ping_sequence: AtomicU16::new(0),
        next_reliable_sequence: parking_lot::Mutex::new(HashMap::new()),
        next_sequenced_sequence: parking_lot::Mutex::new(HashMap::new()),
        remote_sequenced_sequence: parking_lot::Mutex::new(HashMap::new()),
        next_fragment_id: AtomicU16::new(0),
        pending_reliable: parking_lot::Mutex::new(HashMap::new()),
        pending_total: AtomicUsize::new(0),
        pending_datagrams: parking_lot::Mutex::new(VecDeque::new()),
        reliable_send_turn: parking_lot::Mutex::new(()),
        outgoing_reliable: parking_lot::Mutex::new(HashMap::new()),
        outgoing_acks: parking_lot::Mutex::new(HashMap::new()),
        reliable_active: AtomicBool::new(false),
        confirmed_mtu: AtomicUsize::new(MAX_MERGED_PACKET_SIZE),
        mtu_probe: parking_lot::Mutex::new(MtuProbeState::new(Instant::now())),
    }
}

/// The per-datagram property dispatch. 1% of headers carry an invalid property so the
/// rejection branch stays rare exactly as it is on a healthy wire.
fn parse_property_dispatch_phase() {
    println!("phase: from_byte property dispatch ({PROPERTY_OPS} ops/pass)");
    let mut rng = XorShift64::new(0x9E37_79B9_7F4A_7C15);
    let mut headers = Vec::with_capacity(PROPERTY_OPS);
    for index in 0..PROPERTY_OPS {
        let property = if rng.next() % 100 == 0 {
            19 + (index % 13) as u8
        } else {
            (index % 19) as u8
        };
        headers.push(property | (((index % 4) as u8) << 5));
    }
    let mut parsed_valid = 0u64;
    let mut parsed_invalid = 0u64;
    measure("from_byte", headers.len(), || {
        for &header in headers.iter() {
            if PacketProperty::from_byte(black_box(header)).is_some() {
                parsed_valid = parsed_valid.wrapping_add(1);
            } else {
                parsed_invalid = parsed_invalid.wrapping_add(1);
            }
        }
        parsed_valid ^ parsed_invalid
    });
    println!("    branch counts: valid={parsed_valid} invalid={parsed_invalid}");
}

/// Channeled message parsing. 1% of packets are truncated below the 4-byte header.
fn parse_message_phase() {
    println!("phase: parse_message_packet ({MESSAGE_OPS} ops/pass)");
    let mut rng = XorShift64::new(0x0DDB_1A5E_5EED_5EED);
    let mut packets = Vec::with_capacity(MESSAGE_OPS);
    for index in 0..MESSAGE_OPS {
        let payload_len = 8 + (index % 56);
        let mut packet = Vec::with_capacity(4 + payload_len);
        packet.push(PacketProperty::Channeled as u8 | (((index % 4) as u8) << 5));
        packet.extend_from_slice(&0u16.to_le_bytes());
        packet.push((index % 8) as u8);
        packet.extend(std::iter::repeat_n(0u8, payload_len));
        if rng.next() % 100 == 0 {
            packet.truncate(2);
        }
        packets.push(packet);
    }
    let mut parsed_ok = 0u64;
    let mut parsed_none = 0u64;
    measure("parse_message_packet", packets.len(), || {
        for packet in packets.iter() {
            if let Some((channel, delivery, payload)) =
                parse_message_packet(PacketProperty::Channeled, black_box(packet))
            {
                parsed_ok = parsed_ok.wrapping_add(
                    u64::from(black_box(channel))
                        ^ u64::from(black_box(delivery as u8))
                        ^ black_box(payload.len() as u64),
                );
            } else {
                parsed_none = parsed_none.wrapping_add(1);
            }
        }
        parsed_ok ^ parsed_none
    });
    println!("    branch counts: parsed={parsed_ok} rejected={parsed_none}");
}

/// Outbound packet building across every delivery method the send path serves.
fn build_outbound_phase() {
    println!("phase: build_outbound_packet ({OUTBOUND_OPS} ops/pass)");
    let state = test_peer_state();
    let payloads: Vec<Vec<u8>> = (0..8).map(|size| vec![0u8; 64 + size * 128]).collect();
    let deliveries = [
        DeliveryMethod::Unreliable,
        DeliveryMethod::Sequenced,
        DeliveryMethod::ReliableOrdered,
        DeliveryMethod::ReliableSequenced,
    ];
    let mut built_bytes = 0u64;
    measure("build_outbound_packet", OUTBOUND_OPS, || {
        for index in 0..OUTBOUND_OPS {
            let payload = &payloads[index % payloads.len()];
            let delivery = deliveries[index % deliveries.len()];
            let built = build_outbound_packet(&state, (index % 4) as u8, delivery, payload);
            built_bytes = built_bytes.wrapping_add(black_box(built.bytes.len() as u64));
        }
        built_bytes
    });
    println!("    built bytes total: {built_bytes}");
}

/// Datagram merging for the tick-flush batch shape.
fn build_merged_phase() {
    println!("phase: build_merged_datagrams ({MERGED_BATCHES} batches of {MERGED_BATCH}/pass)");
    let mut datagram_count = 0u64;
    measure("build_merged_datagrams", MERGED_BATCHES, || {
        for batch_index in 0..MERGED_BATCHES {
            let batch: Vec<Vec<u8>> = (0..MERGED_BATCH)
                .map(|packet_index| {
                    let index = batch_index * MERGED_BATCH + packet_index;
                    vec![0u8; 20 + (index % 180)]
                })
                .collect();
            datagram_count = datagram_count
                .wrapping_add(black_box(build_merged_datagrams(0, batch).len() as u64));
        }
        datagram_count
    });
    println!("    datagrams merged total: {datagram_count}");
}

/// Inbound ACK window processing at a steady state: each op releases
/// `RELEASE_PER_ACK` in-flight sequences and refills the window behind them, so the
/// deque walk and the refill bookkeeping both stay exercised. 1% of ACKs are malformed.
fn process_ack_phase() {
    println!("phase: process_ack ({ACK_OPS} ops/pass, includes refill bookkeeping)");
    let peer = test_peer_state();
    let stats = TransportStats::new(true, true);
    let ack_bits_len = (DEFAULT_WINDOW_SIZE - 1) / 8 + 2;
    {
        let mut pending = peer.pending_reliable.lock();
        let mut queue = VecDeque::with_capacity(DEFAULT_WINDOW_SIZE);
        for sequence in 0..DEFAULT_WINDOW_SIZE as u16 {
            queue.push_back(PendingReliable {
                sequence,
                bytes: vec![0u8; 16],
                last_sent: Instant::now(),
            });
        }
        pending.insert(CHANNEL_ID, queue);
        peer.pending_total
            .store(DEFAULT_WINDOW_SIZE, Ordering::Relaxed);
    }
    let mut next_new: u16 = DEFAULT_WINDOW_SIZE as u16;
    let mut malformed_count = 0u64;
    let mut released_total = 0u64;
    measure("process_ack", ACK_OPS, || {
        for index in 0..ACK_OPS {
            let (window_start, bits) = {
                let pending = peer.pending_reliable.lock();
                let front = pending
                    .get(&CHANNEL_ID)
                    .and_then(|queue| queue.front())
                    .map(|item| item.sequence)
                    .unwrap_or(0);
                let mut bits = vec![0u8; ack_bits_len];
                for offset in 0..RELEASE_PER_ACK as u16 {
                    let sequence = front.wrapping_add(offset) % MAX_SEQUENCE;
                    let bit = (sequence % DEFAULT_WINDOW_SIZE as u16) as usize;
                    bits[bit / 8] |= 1 << (bit % 8);
                }
                (front, bits)
            };
            let mut ack = Vec::with_capacity(LITENETLIB_CHANNELED_HEADER_SIZE + ack_bits_len);
            ack.push(PacketProperty::Ack as u8);
            ack.extend_from_slice(&window_start.to_le_bytes());
            ack.push(CHANNEL_ID);
            ack.extend_from_slice(&bits);
            if index % 100 == 99 {
                ack.truncate(3);
            }
            let before = peer.pending_total.load(Ordering::Relaxed);
            process_ack(&peer, black_box(&ack), Some(&stats));
            let after = peer.pending_total.load(Ordering::Relaxed);
            let released = before - after;
            released_total = released_total.wrapping_add(released as u64);
            if released == 0 {
                malformed_count = malformed_count.wrapping_add(1);
            }
            // Refill exactly what the ACK released so the window and `pending_total` stay
            // steady across the whole measurement.
            let refilled = {
                let mut pending = peer.pending_reliable.lock();
                if let Some(queue) = pending.get_mut(&CHANNEL_ID) {
                    for _ in 0..released {
                        queue.push_back(PendingReliable {
                            sequence: next_new,
                            bytes: vec![0u8; 16],
                            last_sent: Instant::now(),
                        });
                        next_new = (next_new + 1) % MAX_SEQUENCE;
                    }
                    released
                } else {
                    let mut queue = VecDeque::with_capacity(DEFAULT_WINDOW_SIZE);
                    for _ in 0..DEFAULT_WINDOW_SIZE {
                        queue.push_back(PendingReliable {
                            sequence: next_new,
                            bytes: vec![0u8; 16],
                            last_sent: Instant::now(),
                        });
                        next_new = (next_new + 1) % MAX_SEQUENCE;
                    }
                    pending.insert(CHANNEL_ID, queue);
                    DEFAULT_WINDOW_SIZE
                }
            };
            peer.pending_total.fetch_add(refilled, Ordering::Relaxed);
        }
        released_total ^ malformed_count
    });
    println!("    branch counts: released_total={released_total} malformed={malformed_count}");
}

/// Per-ACK hot path without refill churn: every ACK is valid (window start at the current
/// front) but carries no set bits, so the parse, bounds checks, lock, and full deque scan run
/// while nothing is released. 1% of ACKs are malformed to keep the rejection branch warm.
fn process_ack_noop_phase() {
    println!("phase: process_ack no-op scan ({ACK_OPS} ops/pass, zero-bit ACKs)");
    let peer = test_peer_state();
    let stats = TransportStats::new(true, true);
    let ack_bits_len = (DEFAULT_WINDOW_SIZE - 1) / 8 + 2;
    {
        let mut pending = peer.pending_reliable.lock();
        let mut queue = VecDeque::with_capacity(DEFAULT_WINDOW_SIZE);
        for sequence in 0..DEFAULT_WINDOW_SIZE as u16 {
            queue.push_back(PendingReliable {
                sequence,
                bytes: vec![0u8; 16],
                last_sent: Instant::now(),
            });
        }
        pending.insert(CHANNEL_ID, queue);
        peer.pending_total
            .store(DEFAULT_WINDOW_SIZE, Ordering::Relaxed);
    }
    let mut malformed_count = 0u64;
    let mut front_total = 0u64;
    measure("process_ack_noop", ACK_OPS, || {
        for index in 0..ACK_OPS {
            let front = {
                let pending = peer.pending_reliable.lock();
                pending
                    .get(&CHANNEL_ID)
                    .and_then(|queue| queue.front())
                    .map(|item| item.sequence)
                    .unwrap_or(0)
            };
            let mut ack = Vec::with_capacity(LITENETLIB_CHANNELED_HEADER_SIZE + ack_bits_len);
            ack.push(PacketProperty::Ack as u8);
            ack.extend_from_slice(&front.to_le_bytes());
            ack.push(CHANNEL_ID);
            ack.extend(std::iter::repeat_n(0u8, ack_bits_len));
            if index % 100 == 99 {
                ack.truncate(3);
                malformed_count = malformed_count.wrapping_add(1);
            }
            process_ack(&peer, black_box(&ack), Some(&stats));
            front_total = front_total.wrapping_add(u64::from(black_box(front)));
        }
        front_total ^ malformed_count
    });
    println!("    branch counts: malformed={malformed_count}");
}

#[test]
#[ignore = "explicit release-mode benchmark: cargo test --release -p basis-transport -- --ignored bench_branches --nocapture"]
fn branch_miss_bench() {
    println!("branch-miss bench (valid traffic >=99%, error branches rare)");
    parse_property_dispatch_phase();
    parse_message_phase();
    build_outbound_phase();
    build_merged_phase();
    process_ack_phase();
    process_ack_noop_phase();
}
