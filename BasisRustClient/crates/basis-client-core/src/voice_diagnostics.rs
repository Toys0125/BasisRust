//! Opt-in Windows load-test measurements; normal clients allocate no counters.
use crate::client::BasisClient;
use crate::config::MAX_UNITY_VOICE_FRAME_DURATION_MS;
use crate::voice::{opus_packet_duration_ms, MAX_VOICE_PACKET_BYTES};
use anyhow::Result;
use basis_protocol::channels;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;
use tokio::{sync::Mutex, time};
use tracing::info;

static WINDOW_ACTIVE: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Default)]
pub(super) struct VoiceDiagnostics {
    sent: AtomicU64,
    sent_bytes: AtomicU64,
    send_errors: AtomicU64,
    skipped: AtomicU64,
    received: AtomicU64,
    received_bytes: AtomicU64,
    malformed: AtomicU64,
    self_received: AtomicU64,
    own_peer: AtomicU16,
    sequence_peers: StdMutex<HashMap<u16, SequencePeer>>,
    observer: Option<StdMutex<VoiceObserver>>,
}

#[derive(Debug)]
struct SequencePeer {
    high_sequence: Option<u8>,
    last_time: Option<std::time::Instant>,
    seen_window: u128,
    history_depth: u8,
    unique_packets: u64,
    duplicates: u64,
    reordered: u64,
    wrapped: u64,
    ambiguous: u64,
    unique_available: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SequenceObservation {
    Unique,
    Duplicate,
    Reordered,
    Ambiguous,
}

fn unique_counts_available(peers: &HashMap<u16, SequencePeer>) -> bool {
    !peers.is_empty() && peers.values().all(|peer| peer.unique_available)
}

impl Default for SequencePeer {
    fn default() -> Self {
        Self {
            high_sequence: None,
            last_time: None,
            seen_window: 0,
            history_depth: 0,
            unique_packets: 0,
            duplicates: 0,
            reordered: 0,
            wrapped: 0,
            ambiguous: 0,
            unique_available: true,
        }
    }
}

impl SequencePeer {
    fn receive(
        &mut self,
        sequence: u8,
        duration_ms: u8,
        now: std::time::Instant,
    ) -> SequenceObservation {
        if self.high_sequence.is_none() {
            self.high_sequence = Some(sequence);
            self.last_time = Some(now);
            self.seen_window = 1;
            self.unique_packets += 1;
            return SequenceObservation::Unique;
        }

        let previous_time = self
            .last_time
            .replace(now)
            .expect("sequence baseline has a timestamp");
        let long_silence =
            now.duration_since(previous_time).as_millis() > u128::from(duration_ms.max(1)) * 127;
        let previous_sequence = self.high_sequence.expect("sequence baseline exists");
        let forward = sequence.wrapping_sub(previous_sequence);
        if long_silence || forward == 128 {
            self.ambiguous += 1;
            self.unique_available = false;
            self.high_sequence = Some(sequence);
            self.seen_window = 1;
            self.history_depth = 0;
            return SequenceObservation::Ambiguous;
        }

        if (1..=127).contains(&forward) {
            if sequence < previous_sequence {
                self.wrapped += 1;
            }
            self.high_sequence = Some(sequence);
            self.seen_window = (self.seen_window << forward) | 1;
            self.history_depth = self.history_depth.saturating_add(forward).min(127);
            self.unique_packets += 1;
            return SequenceObservation::Unique;
        }

        if forward == 0 {
            self.duplicates += 1;
            SequenceObservation::Duplicate
        } else {
            let behind = 256 - i64::from(forward);
            let seen = self.seen_window & (1u128 << behind) != 0;
            if seen {
                self.duplicates += 1;
                SequenceObservation::Duplicate
            } else if behind <= i64::from(self.history_depth) {
                self.seen_window |= 1u128 << behind;
                self.reordered += 1;
                self.unique_packets += 1;
                SequenceObservation::Reordered
            } else {
                // The sequence could be an unseen old frame or a long forward
                // loss; without a wider wire counter, do not assert uniqueness.
                self.ambiguous += 1;
                self.unique_available = false;
                SequenceObservation::Ambiguous
            }
        }
    }
}

#[derive(Debug, Default)]
struct VoicePeer {
    packets: u64,
    last: Option<(u8, std::time::Instant)>,
    forward_missing_mod256: u64,
    duplicates: u64,
    reordered_or_ambiguous: u64,
    max_gap_us: u64,
}

#[derive(Debug)]
struct VoiceObserver {
    peers: HashMap<u16, VoicePeer>,
    gap_histogram_ms: Vec<u64>,
    samples: HashMap<u16, Vec<(u8, u8, Vec<u8>)>>,
}

impl Default for VoiceObserver {
    fn default() -> Self {
        Self {
            peers: HashMap::new(),
            gap_histogram_ms: vec![0; 10_001],
            samples: HashMap::new(),
        }
    }
}

impl VoiceDiagnostics {
    pub(super) fn enabled() -> bool {
        std::env::var_os("BASIS_VOICE_DIAGNOSTIC_CSV").is_some()
    }

    pub(super) fn new(index: usize) -> Self {
        Self {
            own_peer: AtomicU16::new(u16::MAX),
            sequence_peers: StdMutex::new(HashMap::new()),
            observer: (index == 0).then(|| StdMutex::new(VoiceObserver::default())),
            ..Self::default()
        }
    }

    pub(super) fn sent(&self, bytes: usize, success: bool) {
        if WINDOW_ACTIVE.load(Ordering::Relaxed) {
            if success {
                self.sent.fetch_add(1, Ordering::Relaxed);
                self.sent_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
            } else {
                self.send_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub(super) fn skipped(&self) {
        if WINDOW_ACTIVE.load(Ordering::Relaxed) {
            self.skipped.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(super) fn receive(&self, channel: u8, payload: &[u8]) {
        if !matches!(
            channel,
            channels::VOICE | channels::VOICE_LARGE | channels::SHOUT_VOICE
        ) || !WINDOW_ACTIVE.load(Ordering::Relaxed)
        {
            return;
        }
        let id_bytes = if channel == channels::VOICE { 1 } else { 2 };
        let parsed = (|| {
            if payload.len() < id_bytes + 3 {
                return None;
            }
            let peer = if id_bytes == 1 {
                payload[0] as u16
            } else {
                u16::from_le_bytes([payload[0], payload[1]])
            };
            let sequence = payload[id_bytes];
            let opus = &payload[id_bytes + 2..];
            let duration = opus_packet_duration_ms(opus)?;
            if opus.len() > MAX_VOICE_PACKET_BYTES
                || duration == 0
                || duration > MAX_UNITY_VOICE_FRAME_DURATION_MS
            {
                return None;
            }
            Some((peer, sequence, opus, duration))
        })();
        let Some((peer, sequence, opus, duration)) = parsed else {
            self.malformed.fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.received.fetch_add(1, Ordering::Relaxed);
        self.received_bytes
            .fetch_add(opus.len() as u64, Ordering::Relaxed);
        let self_packet = peer == self.own_peer.load(Ordering::Relaxed);
        if self_packet {
            self.self_received.fetch_add(1, Ordering::Relaxed);
        } else {
            self.sequence_peers
                .lock()
                .expect("voice sequence tracker mutex poisoned")
                .entry(peer)
                .or_default()
                .receive(sequence, duration as u8, std::time::Instant::now());
        }
        if let Some(observer) = &self.observer {
            observer
                .lock()
                .expect("voice observer mutex poisoned")
                .receive(
                    peer,
                    sequence,
                    opus,
                    duration as u8,
                    std::time::Instant::now(),
                );
        }
    }
}

impl VoiceObserver {
    fn receive(
        &mut self,
        peer: u16,
        sequence: u8,
        opus: &[u8],
        duration: u8,
        now: std::time::Instant,
    ) {
        let state = self.peers.entry(peer).or_default();
        state.packets += 1;
        if let Some((last_sequence, last_time)) = state.last {
            let gap_us = now.duration_since(last_time).as_micros() as u64;
            state.max_gap_us = state.max_gap_us.max(gap_us);
            self.gap_histogram_ms[((gap_us / 1000) as usize).min(10_000)] += 1;
            match sequence.wrapping_sub(last_sequence) {
                0 => state.duplicates += 1,
                delta @ 1..=127 => state.forward_missing_mod256 += u64::from(delta - 1),
                _ => state.reordered_or_ambiguous += 1,
            }
        }
        state.last = Some((sequence, now));
        if self.samples.contains_key(&peer) || self.samples.len() < 4 {
            let packets = self.samples.entry(peer).or_default();
            if packets.len() < 250 {
                packets.push((sequence, duration, opus.to_vec()));
            }
        }
    }
}

pub(super) async fn capture_window(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    marker: PathBuf,
    output: PathBuf,
    duration: Duration,
    shutdown: Arc<AtomicBool>,
) -> Result<()> {
    while !marker.exists() {
        if shutdown.load(Ordering::Relaxed) {
            return Ok(());
        }
        time::sleep(Duration::from_millis(20)).await;
    }
    let snapshot = clients.lock().await.clone();
    for client in &snapshot {
        if let Some(diagnostics) = &client.voice_diagnostics {
            if let Some(peer) = *client.remote_peer_id.lock().await {
                diagnostics.own_peer.store(peer as u16, Ordering::Relaxed);
            }
        }
    }
    let started = time::Instant::now();
    WINDOW_ACTIVE.store(true, Ordering::Relaxed);
    while started.elapsed() < duration && !shutdown.load(Ordering::Relaxed) {
        time::sleep(Duration::from_millis(20)).await;
    }
    WINDOW_ACTIVE.store(false, Ordering::Relaxed);
    let elapsed = started.elapsed();
    // Give receive handlers already in progress a chance to finish their counters.
    time::sleep(Duration::from_millis(50)).await;
    let mut csv = String::from("client_index,remote_peer_id,connected,sent_packets,sent_opus_bytes,send_errors,skipped_packets,received_packets,received_opus_bytes,malformed_packets,self_received_packets,received_unique_packets,duplicate_packets,reordered_packets,wrapped_sequence_packets,ambiguous_sequence_packets,unique_count_available\n");
    let mut totals = [0u64; 14];
    for client in &snapshot {
        let Some(d) = &client.voice_diagnostics else {
            continue;
        };
        let values = [
            &d.sent,
            &d.sent_bytes,
            &d.send_errors,
            &d.skipped,
            &d.received,
            &d.received_bytes,
            &d.malformed,
            &d.self_received,
        ]
        .map(|counter| counter.load(Ordering::Relaxed));
        let (unique, duplicates, reordered, wrapped, ambiguous, available) = {
            let peers = d
                .sequence_peers
                .lock()
                .expect("voice sequence tracker mutex poisoned");
            (
                peers.values().map(|peer| peer.unique_packets).sum(),
                peers.values().map(|peer| peer.duplicates).sum(),
                peers.values().map(|peer| peer.reordered).sum(),
                peers.values().map(|peer| peer.wrapped).sum(),
                peers.values().map(|peer| peer.ambiguous).sum(),
                unique_counts_available(&peers),
            )
        };
        let row_values = [
            values[0],
            values[1],
            values[2],
            values[3],
            values[4],
            values[5],
            values[6],
            values[7],
            unique,
            duplicates,
            reordered,
            wrapped,
            ambiguous,
            u64::from(available),
        ];
        for (total, value) in totals.iter_mut().zip(row_values) {
            *total += value;
        }
        let serialized_values = row_values[..13]
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        csv.push_str(&format!(
            "{},{},{},{},{}\n",
            client.index,
            d.own_peer.load(Ordering::Relaxed),
            client.connected.load(Ordering::Relaxed),
            serialized_values,
            available
        ));
    }
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut sequence_peers = String::from("client_index,remote_peer_id,received_unique_packets,duplicates,reordered,wrapped_sequence_packets,ambiguous,unique_count_available\n");
    for client in &snapshot {
        let Some(d) = &client.voice_diagnostics else {
            continue;
        };
        let peers = d
            .sequence_peers
            .lock()
            .expect("voice sequence tracker mutex poisoned");
        let mut sorted = peers.iter().collect::<Vec<_>>();
        sorted.sort_unstable_by_key(|(id, _)| **id);
        for (id, peer) in sorted {
            sequence_peers.push_str(&format!(
                "{},{},{},{},{},{},{},{}\n",
                client.index,
                id,
                peer.unique_packets,
                peer.duplicates,
                peer.reordered,
                peer.wrapped,
                peer.ambiguous,
                peer.unique_available
            ));
        }
    }
    std::fs::write(output.with_extension("unique-peers.csv"), sequence_peers)?;
    std::fs::write(&output, csv)?;
    let mut summary = format!(
        "metric,value\nwindow_ms,{}\nclients,{}\n",
        elapsed.as_millis(),
        snapshot.len()
    );
    for (name, value) in [
        "sent_packets",
        "sent_opus_bytes",
        "send_errors",
        "skipped_packets",
        "received_packets",
        "received_opus_bytes",
        "malformed_packets",
        "self_received_packets",
        "received_unique_packets",
        "duplicate_packets",
        "reordered_packets",
        "wrapped_sequence_packets",
        "ambiguous_sequence_packets",
        "unique_count_available_clients",
    ]
    .iter()
    .zip(totals)
    {
        summary.push_str(&format!("{name},{value}\n"));
    }
    if let Some(d) = snapshot
        .first()
        .and_then(|client| client.voice_diagnostics.as_ref())
    {
        if let Some(observer) = &d.observer {
            let observer = observer.lock().expect("voice observer mutex poisoned");
            summary.push_str(&format!("observer_sources,{}\n", observer.peers.len()));
            let mut peers = String::from("remote_peer_id,packets,forward_missing_mod256,duplicates,reordered_or_ambiguous,max_gap_ms\n");
            let mut sorted = observer.peers.iter().collect::<Vec<_>>();
            sorted.sort_unstable_by_key(|(id, _)| **id);
            for (id, state) in sorted {
                peers.push_str(&format!(
                    "{id},{},{},{},{},{}\n",
                    state.packets,
                    state.forward_missing_mod256,
                    state.duplicates,
                    state.reordered_or_ambiguous,
                    state.max_gap_us as f64 / 1000.0
                ));
            }
            std::fs::write(output.with_extension("peers.csv"), peers)?;
            let mut gaps = String::from("gap_floor_ms,count\n");
            for (ms, count) in observer.gap_histogram_ms.iter().enumerate() {
                if *count > 0 {
                    gaps.push_str(&format!("{ms},{count}\n"));
                }
            }
            std::fs::write(output.with_extension("gaps.csv"), gaps)?;
            let mut sample = Vec::new();
            for (id, packets) in &observer.samples {
                for (sequence, duration, packet) in packets {
                    sample.extend_from_slice(&id.to_le_bytes());
                    sample.extend_from_slice(&[*sequence, *duration]);
                    sample.extend_from_slice(&(packet.len() as u16).to_le_bytes());
                    sample.extend_from_slice(packet);
                }
            }
            std::fs::write(output.with_extension("sample.bin"), sample)?;
        }
    }
    std::fs::write(output.with_extension("summary.csv"), summary)?;
    info!(
        "voice diagnostic window completed; csv={}",
        output.display()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unique_counts_require_an_observed_source() {
        let mut peers = HashMap::new();
        assert!(!unique_counts_available(&peers));
        let mut peer = SequencePeer::default();
        peer.receive(1, 20, std::time::Instant::now());
        peers.insert(7, peer);
        assert!(unique_counts_available(&peers));
        peers.get_mut(&7).unwrap().unique_available = false;
        assert!(!unique_counts_available(&peers));
    }

    #[test]
    fn sequence_tracker_counts_wraps_nonadjacent_duplicates_and_reordered_frames() {
        let mut tracker = SequencePeer::default();
        let now = std::time::Instant::now();
        let observations = [254, 255, 0, 2, 2, 1]
            .into_iter()
            .enumerate()
            .map(|(i, sequence)| {
                tracker.receive(sequence, 20, now + Duration::from_millis(i as u64 * 20))
            })
            .collect::<Vec<_>>();
        assert_eq!(
            observations,
            [
                SequenceObservation::Unique,
                SequenceObservation::Unique,
                SequenceObservation::Unique,
                SequenceObservation::Unique,
                SequenceObservation::Duplicate,
                SequenceObservation::Reordered,
            ]
        );
        assert_eq!(tracker.unique_packets, 5);
        assert_eq!(tracker.duplicates, 1);
        assert_eq!(tracker.reordered, 1);
        assert_eq!(tracker.wrapped, 1);
        assert_eq!(tracker.ambiguous, 0);
        assert!(tracker.unique_available);
    }

    #[test]
    fn sequence_tracker_preserves_unique_frames_across_multiple_wraps() {
        let mut tracker = SequencePeer::default();
        let now = std::time::Instant::now();
        for frame in 0..300u16 {
            let result = tracker.receive(
                frame as u8,
                20,
                now + Duration::from_millis(u64::from(frame) * 20),
            );
            assert_eq!(result, SequenceObservation::Unique);
        }
        assert_eq!(tracker.unique_packets, 300);
        assert_eq!(tracker.wrapped, 1);
        assert_eq!(tracker.duplicates, 0);
        assert!(tracker.unique_available);
        assert_eq!(tracker.seen_window.count_ones(), 128);
    }

    #[test]
    fn sequence_tracker_handles_half_range_and_neighboring_deltas_conservatively() {
        let now = std::time::Instant::now();
        let mut forward_127 = SequencePeer::default();
        forward_127.receive(0, 20, now);
        assert_eq!(
            forward_127.receive(127, 20, now + Duration::from_millis(20)),
            SequenceObservation::Unique
        );
        assert!(forward_127.unique_available);

        let mut half_range = SequencePeer::default();
        half_range.receive(0, 20, now);
        assert_eq!(
            half_range.receive(128, 20, now + Duration::from_millis(20)),
            SequenceObservation::Ambiguous
        );
        assert_eq!(half_range.ambiguous, 1);
        assert!(!half_range.unique_available);

        let mut forward_129_without_history = SequencePeer::default();
        forward_129_without_history.receive(0, 20, now);
        assert_eq!(
            forward_129_without_history.receive(129, 20, now + Duration::from_millis(20)),
            SequenceObservation::Ambiguous
        );
        assert!(!forward_129_without_history.unique_available);

        let mut reordered_129 = SequencePeer::default();
        for sequence in (0..=127).chain(129..=255) {
            reordered_129.receive(
                sequence,
                20,
                now + Duration::from_millis(u64::from(sequence) * 20),
            );
        }
        assert_eq!(
            reordered_129.receive(128, 20, now + Duration::from_millis(256 * 20)),
            SequenceObservation::Reordered
        );
        assert_eq!(reordered_129.reordered, 1);
        assert!(reordered_129.unique_available);
    }

    #[test]
    fn sequence_tracker_marks_long_silence_ambiguous_and_resets_bounded_history() {
        let mut tracker = SequencePeer::default();
        let now = std::time::Instant::now();
        tracker.receive(10, 20, now);
        assert_eq!(
            tracker.receive(11, 20, now + Duration::from_millis(2541)),
            SequenceObservation::Ambiguous
        );
        assert_eq!(tracker.ambiguous, 1);
        assert!(!tracker.unique_available);
        assert_eq!(tracker.seen_window.count_ones(), 1);
        assert_eq!(tracker.history_depth, 0);
    }

    #[test]
    fn sequence_tracker_window_has_constant_bounded_storage() {
        let mut tracker = SequencePeer::default();
        let now = std::time::Instant::now();
        for frame in 0..10_000u64 {
            tracker.receive(frame as u8, 20, now + Duration::from_millis(frame * 20));
        }
        assert_eq!(tracker.seen_window.count_ones(), 128);
        assert_eq!(std::mem::size_of_val(&tracker.seen_window), 16);
    }

    #[test]
    fn observer_handles_wrap_loss_duplicate_and_ambiguous_sequence() {
        let mut observer = VoiceObserver::default();
        let now = std::time::Instant::now();
        for (i, sequence) in [254, 255, 0, 2, 2, 1].into_iter().enumerate() {
            observer.receive(
                300,
                sequence,
                &[0xf8, 0],
                20,
                now + Duration::from_millis(i as u64 * 20),
            );
        }
        let state = &observer.peers[&300];
        assert_eq!(state.packets, 6);
        assert_eq!(state.forward_missing_mod256, 1);
        assert_eq!(state.duplicates, 1);
        assert_eq!(state.reordered_or_ambiguous, 1);
        assert_eq!(state.max_gap_us, 20_000);
        assert_eq!(observer.gap_histogram_ms[20], 5);
    }

    #[test]
    fn sample_storage_is_bounded() {
        let mut observer = VoiceObserver::default();
        let now = std::time::Instant::now();
        for peer in 0..10 {
            for sequence in 0..300 {
                observer.receive(peer, sequence as u8, &[0xf8, 0], 20, now);
            }
        }
        assert_eq!(observer.peers.len(), 10);
        assert_eq!(observer.samples.len(), 4);
        assert!(observer
            .samples
            .values()
            .all(|packets| packets.len() == 250));
    }

    #[test]
    fn receive_checks_small_large_headers_and_rejects_invalid_frames() {
        let diagnostics = VoiceDiagnostics::new(0);
        diagnostics.own_peer.store(300, Ordering::Relaxed);
        WINDOW_ACTIVE.store(true, Ordering::Relaxed);
        diagnostics.receive(channels::VOICE, &[7, 1, 0, 0xf8, 0]);
        diagnostics.receive(channels::VOICE_LARGE, &[44, 1, 2, 0, 0xf8, 0]);
        diagnostics.receive(channels::VOICE_LARGE, &[44]);
        diagnostics.receive(channels::VOICE, &[7, 3, 0, 0x18]); // 60 ms is incompatible.
        diagnostics.receive(channels::VOICE, &[7, 4, 0]); // Missing Opus packet.
        diagnostics.receive(channels::AVATAR, &[7, 5, 0, 0xf8]);
        WINDOW_ACTIVE.store(false, Ordering::Relaxed);
        diagnostics.receive(channels::VOICE, &[7, 6, 0, 0xf8, 0]);
        assert_eq!(diagnostics.received.load(Ordering::Relaxed), 2);
        assert_eq!(diagnostics.received_bytes.load(Ordering::Relaxed), 4);
        assert_eq!(diagnostics.self_received.load(Ordering::Relaxed), 1);
        assert_eq!(diagnostics.malformed.load(Ordering::Relaxed), 3);
        assert_eq!(
            diagnostics
                .sequence_peers
                .lock()
                .unwrap()
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            vec![7]
        );
        assert_eq!(
            diagnostics
                .observer
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .peers
                .len(),
            2
        );
    }
}
