use crate::observer_sequence::{ObserverSequence, SequenceDecision};
use basis_protocol::avatar::{decode_avatar_bundle, read_position, BitQuality};
use basis_protocol::avatar_delta::apply_delta;
use basis_protocol::channels;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use tracing::debug;

#[derive(Debug, Clone)]
pub(crate) struct AvatarObservationBaseline {
    pub(crate) sequence: u8,
    pub(crate) payload: Vec<u8>,
}

#[derive(Debug, Default)]
pub(crate) struct ObservedAvatarPeer {
    pub(crate) baselines: [Option<AvatarObservationBaseline>; 4],
    pub(crate) last_sequence: Option<u8>,
    pub(crate) sequence_tracker: ObserverSequence,
    pub(crate) last_near_update: Option<std::time::Instant>,
    pub(crate) near_update_count: u64,
    pub(crate) near_gaps_micros: Vec<u64>,
    pub(crate) was_near: bool,
    pub(crate) last_position: Option<[f32; 3]>,
}

#[derive(Debug)]
pub(crate) struct AvatarObserver {
    pub(crate) radius: f32,
    pub(crate) expected_near_peers: usize,
    pub(crate) peers: HashMap<u16, ObservedAvatarPeer>,
    pub(crate) decode_errors: u64,
    pub(crate) unapplied_deltas: u64,
    pub(crate) observed_channels: HashMap<u8, u64>,
    pub(crate) accepted_avatar_items: u64,
    pub(crate) applied_full_items: u64,
    pub(crate) applied_delta_items: u64,
    pub(crate) malformed_items: u64,
    pub(crate) ignored_channels: u64,
    pub(crate) non_newer_sequences: u64,
    pub(crate) decoded_delta_items: u64,
    pub(crate) discontinuities: u64,
    pub(crate) sequence_ambiguities: u64,
    pub(crate) sequence_resyncs: u64,
    pub(crate) sequence_order_unknown_gaps: u64,
    pub(crate) completed_near_segments: Vec<String>,
    pub(crate) near_segment_started_at: Option<std::time::Instant>,
    pub(crate) start_marker_path: Option<PathBuf>,
    pub(crate) window_duration: Duration,
    pub(crate) window_started_at: Option<std::time::Instant>,
}

impl AvatarObserver {
    pub(crate) const STALE_AFTER: Duration = Duration::from_millis(500);

    pub(crate) fn new(
        radius: f32,
        expected_near_peers: usize,
        start_marker_path: Option<PathBuf>,
        window_duration: Duration,
    ) -> Self {
        Self {
            radius: if radius.is_finite() {
                radius.max(0.0)
            } else {
                40.0
            },
            expected_near_peers,
            peers: HashMap::new(),
            decode_errors: 0,
            unapplied_deltas: 0,
            observed_channels: HashMap::new(),
            accepted_avatar_items: 0,
            applied_full_items: 0,
            applied_delta_items: 0,
            malformed_items: 0,
            ignored_channels: 0,
            non_newer_sequences: 0,
            decoded_delta_items: 0,
            discontinuities: 0,
            sequence_ambiguities: 0,
            sequence_resyncs: 0,
            sequence_order_unknown_gaps: 0,
            completed_near_segments: Vec::new(),
            near_segment_started_at: None,
            start_marker_path,
            window_duration,
            window_started_at: None,
        }
    }

    pub(crate) fn mark_discontinuity(&mut self, now: std::time::Instant) {
        let tracking = self
            .window_started_at
            .is_some_and(|start| now.saturating_duration_since(start) <= self.window_duration);
        if tracking {
            self.discontinuities = self.discontinuities.saturating_add(1);
            let segment = self.completed_near_segments.len();
            let mut rows = String::new();
            for (peer_id, peer) in &self.peers {
                if peer.was_near {
                    rows.push_str(&format!(
                        "{segment},{}",
                        observer_peer_csv(*peer_id, peer, now)
                    ));
                }
            }
            self.completed_near_segments.push(rows);
            self.near_segment_started_at = Some(now);
        }
        for peer in self.peers.values_mut() {
            peer.baselines = Default::default();
            peer.last_sequence = None;
            peer.sequence_tracker = ObserverSequence::default();
            if tracking {
                peer.last_near_update = None;
                peer.near_update_count = 0;
                peer.near_gaps_micros.clear();
                peer.was_near = false;
            }
            peer.last_position = None;
        }
    }

    pub(crate) fn accept_sequence(
        &mut self,
        peer_id: u16,
        sequence: u8,
        full: bool,
        now: std::time::Instant,
        tracking: bool,
    ) -> bool {
        let state = self.peers.entry(peer_id).or_default();
        match state
            .sequence_tracker
            .consider(sequence, state.last_sequence, full, now)
        {
            SequenceDecision::Apply => true,
            SequenceDecision::ApplyAfterGap => {
                // An advancing full frame is independently decodable. Keep its arrival
                // cadence, but discard other-quality baselines that may alias after a wrap.
                state.baselines = Default::default();
                if tracking {
                    self.sequence_order_unknown_gaps =
                        self.sequence_order_unknown_gaps.saturating_add(1);
                }
                debug!(peer_id, "avatar observer retained advancing full frame after long gap; byte sequence ordering across gap unknown");
                true
            }
            SequenceDecision::Resynced => {
                if tracking {
                    self.sequence_resyncs = self.sequence_resyncs.saturating_add(1);
                }
                debug!(peer_id, "avatar observer sequence baseline resynchronized; ordering across silence unknown");
                true
            }
            SequenceDecision::Pending { began } => {
                if began {
                    state.baselines = Default::default();
                    if tracking {
                        state.last_near_update = None;
                        self.discontinuities = self.discontinuities.saturating_add(1);
                    }
                    debug!(peer_id, "avatar observer sequence ambiguous after silence; awaiting advancing full frames");
                }
                if !full && tracking {
                    self.unapplied_deltas = self.unapplied_deltas.saturating_add(1);
                }
                false
            }
            decision => {
                if tracking {
                    self.non_newer_sequences = self.non_newer_sequences.saturating_add(1);
                    if decision == SequenceDecision::Ambiguous {
                        self.sequence_ambiguities = self.sequence_ambiguities.saturating_add(1);
                    }
                }
                false
            }
        }
    }

    pub(crate) fn begin_window(&mut self, observer_position: [f32; 3], now: std::time::Instant) {
        self.window_started_at = Some(now);
        self.near_segment_started_at = Some(now);
        self.completed_near_segments.clear();
        self.decode_errors = 0;
        self.unapplied_deltas = 0;
        self.accepted_avatar_items = 0;
        self.applied_full_items = 0;
        self.applied_delta_items = 0;
        self.malformed_items = 0;
        self.ignored_channels = 0;
        self.non_newer_sequences = 0;
        self.decoded_delta_items = 0;
        self.discontinuities = 0;
        self.sequence_ambiguities = 0;
        self.sequence_resyncs = 0;
        self.sequence_order_unknown_gaps = 0;
        self.observed_channels.clear();
        for peer in self.peers.values_mut() {
            peer.near_gaps_micros.clear();
            peer.near_update_count = 0;
            peer.last_near_update = None;
            peer.was_near = peer
                .last_position
                .map(|position| within_avatar_radius(self.radius, position, observer_position))
                .unwrap_or(false);
        }
    }

    pub(crate) fn tracking_window(
        &mut self,
        observer_position: [f32; 3],
        now: std::time::Instant,
    ) -> bool {
        if self.window_started_at.is_none() {
            let should_start = self
                .start_marker_path
                .as_ref()
                .map(|path| path.exists())
                .unwrap_or(true);
            if should_start {
                self.begin_window(observer_position, now);
            }
        }
        self.window_started_at
            .map(|start| now.saturating_duration_since(start) <= self.window_duration)
            .unwrap_or(false)
    }

    pub(crate) fn observe_channel(
        &mut self,
        channel: u8,
        payload: &[u8],
        observer_position: [f32; 3],
        now: std::time::Instant,
    ) {
        let tracking = self.tracking_window(observer_position, now);
        if tracking {
            *self.observed_channels.entry(channel).or_default() += 1;
        }
        if channel == channels::COMPRESSED_AVATAR_BUNDLE {
            match decode_avatar_bundle(payload) {
                Ok(items) => {
                    for item in items {
                        if !self.observe_item(
                            item.original_channel,
                            &item.payload,
                            observer_position,
                            now,
                            tracking,
                        ) && tracking
                        {
                            self.decode_errors = self.decode_errors.saturating_add(1);
                            self.malformed_items = self.malformed_items.saturating_add(1);
                        }
                    }
                }
                Err(_) if tracking => {
                    self.decode_errors = self.decode_errors.saturating_add(1);
                    self.malformed_items = self.malformed_items.saturating_add(1);
                }
                Err(_) => {}
            }
        } else if !self.observe_item(channel, payload, observer_position, now, tracking) && tracking
        {
            self.decode_errors = self.decode_errors.saturating_add(1);
            self.malformed_items = self.malformed_items.saturating_add(1);
        }
    }

    pub(crate) fn observe_item(
        &mut self,
        channel: u8,
        payload: &[u8],
        observer_position: [f32; 3],
        now: std::time::Instant,
        tracking: bool,
    ) -> bool {
        let (quality_index, quality) = if (channels::PLAYER_AVATAR_VERY_LOW
            ..=channels::PLAYER_AVATAR_HIGH_ADDITIONAL)
            .contains(&channel)
            || (channels::PLAYER_AVATAR_VERY_LOW_LARGE
                ..=channels::PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE)
                .contains(&channel)
        {
            let quality_index = channels::quality_from_channel(channel);
            (quality_index, observer_quality(quality_index))
        } else if channel == channels::DELTA_AVATAR {
            if tracking {
                self.decoded_delta_items = self.decoded_delta_items.saturating_add(1);
            }
            return self.observe_delta(payload, observer_position, now, tracking);
        } else {
            if tracking {
                self.ignored_channels = self.ignored_channels.saturating_add(1);
            }
            return true;
        };
        let Some(quality) = quality else {
            return false;
        };
        let id_len: usize = if channels::is_large_player_id_channel(channel) {
            2
        } else {
            1
        };
        let Some(header_len) = id_len.checked_add(2) else {
            return false;
        };
        if payload.len() < header_len + quality.payload_len() {
            return false;
        }
        let peer_id = if id_len == 1 {
            payload[0] as u16
        } else {
            u16::from_le_bytes([payload[0], payload[1]])
        };
        let sequence = payload[id_len + 1];
        let avatar_payload = &payload[header_len..header_len + quality.payload_len()];
        let Some(position) = read_position(avatar_payload) else {
            return false;
        };
        if !self.accept_sequence(peer_id, sequence, true, now, tracking) {
            return true;
        }
        let state = self.peers.entry(peer_id).or_default();
        state.last_sequence = Some(sequence);
        state.sequence_tracker.applied(now);
        state.baselines[quality_index as usize] = Some(AvatarObservationBaseline {
            sequence,
            payload: avatar_payload.to_vec(),
        });
        state.last_position = Some(position);
        if tracking {
            self.accepted_avatar_items = self.accepted_avatar_items.saturating_add(1);
            self.applied_full_items = self.applied_full_items.saturating_add(1);
        }
        if tracking {
            record_observer_near_update(self.radius, position, observer_position, state, now);
        }
        true
    }

    pub(crate) fn observe_delta(
        &mut self,
        payload: &[u8],
        observer_position: [f32; 3],
        now: std::time::Instant,
        tracking: bool,
    ) -> bool {
        if payload.is_empty() || payload[0] & channels::DELTA_HEADER_CONTROL_BIT != 0 {
            return true;
        }
        let header = payload[0];
        let quality_index = header & channels::DELTA_HEADER_QUALITY_MASK;
        let Some(quality) = observer_quality(quality_index) else {
            return false;
        };
        let id_len = if header & channels::DELTA_HEADER_LARGE_ID != 0 {
            2
        } else {
            1
        };
        let id_start = 1;
        // Server-bound fanout delta layout is [flags, id, interval, sequence, base, body].
        // The uplink delta omits ID and interval; do not use its shorter offsets here.
        let sequence_offset = id_start + id_len + 1;
        let base_offset = sequence_offset + 1;
        let body_offset = base_offset + 1;
        if payload.len() <= body_offset {
            return false;
        }
        let peer_id = if id_len == 1 {
            payload[id_start] as u16
        } else {
            u16::from_le_bytes([payload[id_start], payload[id_start + 1]])
        };
        let sequence = payload[sequence_offset];
        let base_sequence = payload[base_offset];
        if !self.accept_sequence(peer_id, sequence, false, now, tracking) {
            return true;
        }
        let state = self.peers.entry(peer_id).or_default();
        let Some(baseline) = state.baselines[quality_index as usize]
            .as_ref()
            .filter(|baseline| baseline.sequence == base_sequence)
        else {
            self.unapplied_deltas = self.unapplied_deltas.saturating_add(1);
            return true;
        };
        let Ok((avatar_payload, _consumed)) =
            apply_delta(&baseline.payload, &payload[body_offset..], quality)
        else {
            return false;
        };
        let Some(position) = read_position(&avatar_payload) else {
            return false;
        };
        state.last_sequence = Some(sequence);
        state.sequence_tracker.applied(now);
        state.last_position = Some(position);
        if tracking {
            self.accepted_avatar_items = self.accepted_avatar_items.saturating_add(1);
            self.applied_delta_items = self.applied_delta_items.saturating_add(1);
        }
        if tracking {
            record_observer_near_update(self.radius, position, observer_position, state, now);
        }
        true
    }

    pub(crate) fn summary_and_csv(&self, now: std::time::Instant) -> (String, String) {
        let report_time = self
            .window_started_at
            .map(|start| now.min(start + self.window_duration))
            .unwrap_or(now);
        let observed_ms = self
            .window_started_at
            .map(|start| report_time.saturating_duration_since(start).as_millis())
            .unwrap_or(0);
        let near_segment_ms = self
            .near_segment_started_at
            .map(|start| report_time.saturating_duration_since(start).as_millis())
            .unwrap_or(0);
        let near = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.was_near)
            .collect::<Vec<_>>();
        let mut gaps = near
            .iter()
            .flat_map(|(_, peer)| peer.near_gaps_micros.iter().copied())
            .collect::<Vec<_>>();
        let p50 = avatar_gap_percentile(&mut gaps.clone(), 0.50);
        let p95 = avatar_gap_percentile(&mut gaps, 0.95);
        let stale = near
            .iter()
            .filter(|(_, peer)| {
                peer.last_near_update
                    .map(|last| report_time.saturating_duration_since(last) > Self::STALE_AFTER)
                    .unwrap_or(true)
            })
            .count();
        let missing = self.expected_near_peers.saturating_sub(near.len());
        let summary = format!(
            "avatar observer: window_started={} window_ms={} near_peers={} expected={} missing={} stale_500ms={} applied_gaps={} p50_ms={:.2} p95_ms={:.2} applied_full={} applied_delta={} malformed={} decode_errors={} unapplied_deltas={} non_newer_sequences={} ignored_channels={} discontinuities={} sequence_ambiguities={} sequence_resyncs={} near_segment={} near_segment_ms={} sequence_order_unknown_gaps={}",
            self.window_started_at.is_some(),
            observed_ms,
            near.len(),
            self.expected_near_peers,
            missing,
            stale,
            gaps.len(),
            p50 as f64 / 1000.0,
            p95 as f64 / 1000.0,
            self.applied_full_items,
            self.applied_delta_items,
            self.malformed_items,
            self.decode_errors,
            self.unapplied_deltas,
            self.non_newer_sequences,
            self.ignored_channels,
            self.discontinuities,
            self.sequence_ambiguities,
            self.sequence_resyncs,
            self.completed_near_segments.len(),
            near_segment_ms,
            self.sequence_order_unknown_gaps,
        );
        let mut csv = String::from("metric,value\n");
        let summary_values = format!(
            "{},{},{},{},{},{},{},{:.2},{:.2},{},{},{},{},{},{},{},{},{},{}",
            self.window_started_at.is_some(),
            observed_ms,
            near.len(),
            self.expected_near_peers,
            missing,
            stale,
            gaps.len(),
            p50 as f64 / 1000.0,
            p95 as f64 / 1000.0,
            self.decode_errors,
            self.unapplied_deltas,
            self.accepted_avatar_items,
            self.applied_full_items,
            self.applied_delta_items,
            self.malformed_items,
            self.ignored_channels,
            self.non_newer_sequences,
            self.decoded_delta_items,
            self.observed_channels.values().sum::<u64>(),
        );
        let metrics = [
            "window_started",
            "window_ms",
            "near_peers",
            "expected_near_peers",
            "missing_expected_peers",
            "stale_peers_500ms",
            "applied_gaps",
            "gap_p50_ms",
            "gap_p95_ms",
            "decode_errors",
            "unapplied_deltas",
            "accepted_avatar_items",
            "applied_full_items",
            "applied_delta_items",
            "malformed_items",
            "ignored_channels",
            "non_newer_sequences",
            "decoded_delta_items",
            "observed_channel_packets",
        ];
        for (key, value) in metrics.iter().zip(summary_values.split(',')) {
            csv.push_str(key);
            csv.push(',');
            csv.push_str(value);
            csv.push('\n');
        }
        csv.push_str(&format!(
            "discontinuities,{}\nsequence_ambiguities,{}\nsequence_resyncs,{}\n",
            self.discontinuities, self.sequence_ambiguities, self.sequence_resyncs
        ));
        csv.push_str(&format!(
            "near_metrics_scope,current_segment\nnear_segment,{}\nnear_segment_ms,{}\n",
            self.completed_near_segments.len(),
            near_segment_ms
        ));
        csv.push_str(&format!(
            "sequence_order_unknown_gaps,{}\n",
            self.sequence_order_unknown_gaps
        ));
        csv.push_str("observed_channel,packets\n");
        let mut observed_channels = self.observed_channels.iter().collect::<Vec<_>>();
        observed_channels.sort_unstable_by_key(|(channel, _)| **channel);
        for (channel, count) in observed_channels {
            csv.push_str(&format!("{channel},{count}\n"));
        }
        csv.push_str("peer_id,updates,p50_gap_ms,p95_gap_ms,last_near_age_ms,stale_500ms\n");
        for (peer_id, peer) in near {
            csv.push_str(&observer_peer_csv(*peer_id, peer, report_time));
        }
        if !self.completed_near_segments.is_empty() {
            csv.push_str("completed_observer_segment,peer_id,updates,p50_gap_ms,p95_gap_ms,last_near_age_ms,stale_500ms\n");
            for rows in &self.completed_near_segments {
                csv.push_str(rows);
            }
        }
        (summary, csv)
    }
}

pub(crate) fn record_observer_near_update(
    radius: f32,
    position: [f32; 3],
    observer_position: [f32; 3],
    state: &mut ObservedAvatarPeer,
    now: std::time::Instant,
) {
    if !within_avatar_radius(radius, position, observer_position) {
        return;
    }
    state.was_near = true;
    if let Some(last) = state.last_near_update {
        state
            .near_gaps_micros
            .push(now.saturating_duration_since(last).as_micros() as u64);
    }
    state.last_near_update = Some(now);
    state.near_update_count = state.near_update_count.saturating_add(1);
}

pub(crate) fn within_avatar_radius(
    radius: f32,
    position: [f32; 3],
    observer_position: [f32; 3],
) -> bool {
    let distance_sq = (0..3)
        .map(|axis| (position[axis] - observer_position[axis]).powi(2))
        .sum::<f32>();
    distance_sq <= radius * radius
}

pub(crate) fn observer_quality(index: u8) -> Option<BitQuality> {
    match index {
        0 => Some(BitQuality::VeryLow),
        1 => Some(BitQuality::Low),
        2 => Some(BitQuality::Medium),
        3 => Some(BitQuality::High),
        _ => None,
    }
}

pub(crate) fn avatar_gap_percentile(gaps_micros: &mut [u64], percentile: f64) -> u64 {
    if gaps_micros.is_empty() {
        return 0;
    }
    gaps_micros.sort_unstable();
    let index = ((gaps_micros.len() as f64 * percentile).ceil() as usize)
        .saturating_sub(1)
        .min(gaps_micros.len() - 1);
    gaps_micros[index]
}

pub(crate) fn observer_peer_csv(
    peer_id: u16,
    peer: &ObservedAvatarPeer,
    report_time: std::time::Instant,
) -> String {
    let last_age = peer
        .last_near_update
        .map(|last| report_time.saturating_duration_since(last).as_millis())
        .unwrap_or(u128::MAX);
    let p50 = avatar_gap_percentile(&mut peer.near_gaps_micros.clone(), 0.50);
    let p95 = avatar_gap_percentile(&mut peer.near_gaps_micros.clone(), 0.95);
    format!(
        "{peer_id},{},{:.2},{:.2},{last_age},{}\n",
        peer.near_update_count,
        p50 as f64 / 1000.0,
        p95 as f64 / 1000.0,
        last_age > AvatarObserver::STALE_AFTER.as_millis()
    )
}
