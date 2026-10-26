//! Wire fixtures and bounded downlink reconstruction for the accelerated week test.
//! Face weights are opaque additional-data bytes, not Unity blendshape execution.
use anyhow::{bail, ensure, Context, Result};
use basis_protocol::{
    avatar::{
        compress_scale, decode_avatar_bundle, repack_high_to_lower, write_neutral_rotation_region,
        BitQuality,
    },
    avatar_delta::{apply_delta, build_delta},
    channels,
    io::{NetReader, NetWriter},
    messages::{
        AdditionalAvatarData, BasisDeserialize, BasisSerialize, ServerAudioSegmentMessage,
        VoiceReceiversMessage,
    },
};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    time::{Duration, Instant},
};

const MAX_PEERS: usize = 2048;
pub const FACE_MESSAGE_INDEX: u8 = 71;

#[derive(Clone, Debug)]
pub struct AvatarFrame {
    pub body: Vec<u8>,
    pub additional: Vec<AdditionalAvatarData>,
    pub linked_avatar_index: u8,
}

impl AvatarFrame {
    pub fn suffix(&self) -> Result<Vec<u8>> {
        ensure!(self.additional.len() <= 255, "too many additional entries");
        let mut writer = NetWriter::new();
        writer.put_u8(self.additional.len() as u8);
        if !self.additional.is_empty() {
            writer.put_u8(self.linked_avatar_index);
            for item in &self.additional {
                item.serialize(&mut writer)?;
            }
        }
        Ok(writer.into_vec())
    }

    /// Uplink has only sequence + body; even a >255 sender uses the small channel.
    /// Always includes a suffix, including [0], to exercise explicit disabling.
    pub fn uplink_full(&self, sequence: u8) -> Result<(u8, Vec<u8>)> {
        let mut payload = vec![sequence];
        payload.extend_from_slice(&self.body);
        payload.extend_from_slice(&self.suffix()?);
        Ok((channels::PLAYER_AVATAR_HIGH_ADDITIONAL, payload))
    }

    pub fn uplink_delta(
        &self,
        sequence: u8,
        base_sequence: u8,
        baseline: &Self,
    ) -> Result<(u8, Vec<u8>)> {
        let mut payload = vec![
            BitQuality::High as u8 | channels::DELTA_HEADER_ADDITIONAL_DATA,
            sequence,
            base_sequence,
        ];
        payload.extend_from_slice(&build_delta(&baseline.body, &self.body, BitQuality::High)?);
        payload.extend_from_slice(&self.suffix()?);
        Ok((channels::DELTA_AVATAR, payload))
    }

    pub fn assert_matches(&self, received: &AvatarSnapshot) -> Result<()> {
        let quality = quality(received.quality)?;
        let expected = if quality == BitQuality::High {
            self.body.clone()
        } else {
            repack_high_to_lower(&self.body, quality)?
        };
        ensure!(
            received.payload == expected,
            "avatar body/position/bones mismatch at quality {}",
            received.quality
        );
        ensure!(
            received.additional == self.additional,
            "additional face/count/data mismatch"
        );
        let linked = if self.additional.is_empty() {
            0
        } else {
            self.linked_avatar_index
        };
        ensure!(
            received.linked_avatar_index == linked,
            "linked avatar index mismatch"
        );
        Ok(())
    }
}

/// Finite near-origin mm positions, valid quantized bone values and identity tail.
/// Generation is encoded in both pose and face values to expose ID-reuse leaks.
pub fn avatar_frame(
    peer: u16,
    generation: u32,
    tick: u32,
    face_enabled: bool,
    linked: u8,
) -> AvatarFrame {
    let mut body = vec![0; BitQuality::High.payload_len()];
    write_neutral_rotation_region(&mut body, BitQuality::High).expect("fixed high body");
    let axes = [
        ((peer as u32 * 11 + tick * 7 + generation * 13) % 12000) as i32 - 6000,
        1700,
        ((peer as u32 * 19 + tick * 3) % 10000) as i32 - 5000,
    ];
    for (axis, mm) in axes.iter().enumerate() {
        body[axis * 3..axis * 3 + 3].copy_from_slice(&mm.to_le_bytes()[..3]);
    }
    // First 3-DOF bone has [largest:2,x:12,y:12,z:12]; stay near neutral.
    let component = 2048 + ((tick.wrapping_add(generation * 17)) % 128) as u64;
    let packed = 3 | (component << 2) | (2048 << 14) | (2048 << 26);
    for bit in 0..38 {
        let offset = 9 * 8 + bit;
        let mask = 1 << (offset & 7);
        body[offset >> 3] =
            (body[offset >> 3] & !mask) | (((packed >> bit) as u8 & 1) << (offset & 7));
    }
    let tail = 9 + BitQuality::High.rotation_len();
    body[tail..tail + 2].copy_from_slice(&compress_scale(1.0).to_le_bytes());
    let identity = [3, 0, 128, 0, 128, 0, 128];
    body[tail + 2..tail + 9].copy_from_slice(&identity);
    body[tail + 14..tail + 21].copy_from_slice(&identity);
    let mut additional = Vec::new();
    if face_enabled {
        let mut data = Vec::with_capacity(52 * 4);
        for coefficient in 0..52u32 {
            let weight =
                ((tick.wrapping_mul(17) + peer as u32 * 3 + generation * 31 + coefficient * 11)
                    % 101) as f32
                    / 100.0;
            data.extend_from_slice(&weight.to_le_bytes());
        }
        additional.push(AdditionalAvatarData {
            message_index: FACE_MESSAGE_INDEX,
            data,
        });
        if tick % 2 == 1 {
            additional.push(AdditionalAvatarData {
                message_index: FACE_MESSAGE_INDEX + 1,
                data: vec![generation as u8, tick as u8, linked],
            });
        }
    }
    AvatarFrame {
        body,
        additional,
        linked_avatar_index: linked,
    }
}

fn quality(index: u8) -> Result<BitQuality> {
    Ok(match index {
        0 => BitQuality::VeryLow,
        1 => BitQuality::Low,
        2 => BitQuality::Medium,
        3 => BitQuality::High,
        _ => bail!("invalid quality {index}"),
    })
}

#[derive(Clone, Debug)]
pub struct AvatarSnapshot {
    pub sequence: u8,
    pub quality: u8,
    pub interval: u8,
    pub payload: Vec<u8>,
    pub additional: Vec<AdditionalAvatarData>,
    pub linked_avatar_index: u8,
}

#[derive(Clone, Debug)]
struct Baseline {
    sequence: u8,
    payload: Vec<u8>,
}
#[derive(Clone, Debug, Default)]
struct SequenceState {
    last_applied_at: Option<Instant>,
    resyncing: bool,
    candidate: Option<(u8, Instant)>,
}

#[derive(Clone, Debug, Default)]
pub struct AvatarReceiver {
    pub avatars: HashMap<u16, AvatarSnapshot>,
    baselines: HashMap<u16, [Option<Baseline>; 4]>,
    sequence_states: HashMap<u16, SequenceState>,
    keyframe_requests: HashSet<u16>,
    pub full_items: u64,
    pub delta_items: u64,
    pub unapplied_deltas: u64,
    pub stale_items: u64,
}

impl AvatarReceiver {
    pub fn remove_peer(&mut self, peer: u16) {
        self.avatars.remove(&peer);
        self.baselines.remove(&peer);
        self.sequence_states.remove(&peer);
        self.keyframe_requests.remove(&peer);
    }

    /// Missing full baselines must be recovered through the real control wire.
    /// Drain in stable order; no guessed payload or sequence is inserted here.
    pub fn take_keyframe_requests(&mut self) -> Vec<u16> {
        let mut requests: Vec<_> = self.keyframe_requests.drain().collect();
        requests.sort_unstable();
        requests
    }
    pub fn ingest(&mut self, channel: u8, payload: &[u8]) -> Result<()> {
        self.ingest_at(channel, payload, Instant::now())
    }
    pub fn ingest_at(&mut self, channel: u8, payload: &[u8], now: Instant) -> Result<()> {
        if channel == channels::COMPRESSED_AVATAR_BUNDLE {
            for item in decode_avatar_bundle(payload)? {
                self.ingest_at(item.original_channel, &item.payload, now)?;
            }
            return Ok(());
        }
        if channel == channels::DELTA_AVATAR {
            ensure!(!payload.is_empty(), "empty downlink delta");
            if payload[0] & channels::DELTA_HEADER_CONTROL_BIT != 0 {
                return Ok(());
            }
            let flags = payload[0];
            let qi = flags & channels::DELTA_HEADER_QUALITY_MASK;
            let q = quality(qi)?;
            let id_len = if flags & channels::DELTA_HEADER_LARGE_ID != 0 {
                2
            } else {
                1
            };
            let mut reader = NetReader::new(&payload[1..]);
            let peer = if id_len == 2 {
                reader.get_u16()?
            } else {
                u16::from(reader.get_u8()?)
            };
            let interval = reader.get_u8()?;
            let sequence = reader.get_u8()?;
            let base_sequence = reader.get_u8()?;
            if !self.accept_sequence(peer, sequence, false, now)? {
                return Ok(());
            }
            let Some(baseline) = self
                .baselines
                .get(&peer)
                .and_then(|a| a[qi as usize].as_ref())
                .filter(|b| b.sequence == base_sequence)
            else {
                self.unapplied_deltas += 1;
                // accept_sequence bounds all per-peer maps before this point.
                self.keyframe_requests.insert(peer);
                return Ok(());
            };
            let bytes = reader.remaining_slice();
            let (body, consumed) = apply_delta(&baseline.payload, bytes, q)?;
            ensure!(consumed <= bytes.len(), "delta consumed beyond packet");
            let (additional, linked) = parse_suffix(
                &bytes[consumed..],
                flags & channels::DELTA_HEADER_ADDITIONAL_DATA != 0,
            )?;
            self.avatars.insert(
                peer,
                AvatarSnapshot {
                    sequence,
                    quality: qi,
                    interval,
                    payload: body,
                    additional,
                    linked_avatar_index: linked,
                },
            );
            self.sequence_states
                .get_mut(&peer)
                .expect("sequence state")
                .last_applied_at = Some(now);
            self.delta_items += 1;
            return Ok(());
        }
        if !channels::PLAYER_AVATAR_QUALITY_CHANNELS.contains(&channel) {
            return Ok(());
        }
        let qi = channels::quality_from_channel(channel);
        let q = quality(qi)?;
        let mut reader = NetReader::new(payload);
        let peer = if channels::is_large_player_id_channel(channel) {
            reader.get_u16()?
        } else {
            u16::from(reader.get_u8()?)
        };
        let interval = reader.get_u8()?;
        let sequence = reader.get_u8()?;
        let body = reader.get_bytes(q.payload_len())?.to_vec();
        let (additional, linked) = parse_suffix(
            reader.remaining_slice(),
            channels::channel_has_additional_data(channel),
        )?;
        if !self.accept_sequence(peer, sequence, true, now)? {
            return Ok(());
        }
        self.baselines.entry(peer).or_default()[qi as usize] = Some(Baseline {
            sequence,
            payload: body.clone(),
        });
        self.keyframe_requests.remove(&peer);
        self.avatars.insert(
            peer,
            AvatarSnapshot {
                sequence,
                quality: qi,
                interval,
                payload: body,
                additional,
                linked_avatar_index: linked,
            },
        );
        self.sequence_states
            .get_mut(&peer)
            .expect("sequence state")
            .last_applied_at = Some(now);
        self.full_items += 1;
        Ok(())
    }

    // Same byte half-range/silence policy as client observer_sequence.rs.
    fn accept_sequence(
        &mut self,
        peer: u16,
        sequence: u8,
        full: bool,
        now: Instant,
    ) -> Result<bool> {
        ensure!(
            self.sequence_states.contains_key(&peer) || self.sequence_states.len() < MAX_PEERS,
            "avatar peer bound exceeded"
        );
        let delta = self
            .avatars
            .get(&peer)
            .map(|p| sequence.wrapping_sub(p.sequence));
        let state = self.sequence_states.entry(peer).or_default();
        let silence = state
            .last_applied_at
            .is_some_and(|t| now.saturating_duration_since(t) >= Duration::from_secs(2));
        if !state.resyncing && silence && (!full || !matches!(delta, Some(1..=127))) {
            state.resyncing = true;
            state.candidate = None;
            self.baselines.remove(&peer);
        }
        if state.resyncing {
            if full {
                if let Some((candidate, at)) = state.candidate {
                    let advance = sequence.wrapping_sub(candidate);
                    if now.saturating_duration_since(at) < Duration::from_secs(10) {
                        if matches!(advance, 1..=127) {
                            state.resyncing = false;
                            state.candidate = None;
                            return Ok(true);
                        }
                        if advance == 0 {
                            self.stale_items += 1;
                            return Ok(false);
                        }
                    }
                }
                state.candidate = Some((sequence, now));
            }
            self.stale_items += 1;
            return Ok(false);
        }
        if matches!(delta, Some(0 | 128..=255)) {
            self.stale_items += 1;
            return Ok(false);
        }
        if silence {
            self.baselines.remove(&peer);
        }
        Ok(true)
    }
}

fn parse_suffix(bytes: &[u8], has_additional: bool) -> Result<(Vec<AdditionalAvatarData>, u8)> {
    if !has_additional {
        ensure!(
            bytes.is_empty(),
            "trailing avatar bytes without additional flag"
        );
        return Ok((Vec::new(), 0));
    }
    let mut reader = NetReader::new(bytes);
    let count = reader.get_u8()?;
    let linked = if count == 0 { 0 } else { reader.get_u8()? };
    let mut items = Vec::with_capacity(count as usize);
    for _ in 0..count {
        items.push(AdditionalAvatarData::deserialize(&mut reader)?);
    }
    ensure!(reader.remaining() == 0, "trailing additional avatar bytes");
    Ok((items, linked))
}

#[derive(Clone, Copy, Debug)]
pub enum RecipientMode {
    Small,
    Large,
    InvertedSmall,
    InvertedLarge,
    Bitfield,
}

pub fn voice_recipients(users: &[u16], mode: RecipientMode) -> Result<(u8, Vec<u8>)> {
    let mut writer = NetWriter::new();
    let channel = match mode {
        RecipientMode::Bitfield => {
            let len = users.iter().max().map_or(0, |id| *id as usize / 8 + 1);
            let mut bits = vec![0u8; len];
            for id in users {
                bits[*id as usize / 8] |= 1 << (*id as usize % 8);
            }
            writer.put_u16(len as u16);
            writer.put_bytes(&bits);
            channels::AUDIO_RECIPIENTS_BITFIELD
        }
        _ => {
            let large = matches!(mode, RecipientMode::Large | RecipientMode::InvertedLarge);
            ensure!(
                users.len() <= if large { 65535 } else { 255 },
                "recipient count overflow"
            );
            if large {
                writer.put_u16(users.len() as u16);
            } else {
                writer.put_u8(users.len() as u8);
            }
            for id in users {
                writer.put_u16(*id);
            }
            // The production API only provides deserialization. Roundtrip actual layout.
            let mut reader = NetReader::new(writer.as_slice());
            ensure!(
                VoiceReceiversMessage::deserialize(&mut reader, large)?.users == users,
                "voice recipient roundtrip"
            );
            match mode {
                RecipientMode::Small => channels::AUDIO_RECIPIENTS,
                RecipientMode::Large => channels::AUDIO_RECIPIENTS_LARGE,
                RecipientMode::InvertedSmall => channels::AUDIO_RECIPIENTS_INVERTED,
                _ => channels::AUDIO_RECIPIENTS_INVERTED_LARGE,
            }
        }
    };
    Ok((channel, writer.into_vec()))
}

pub fn voice_payload(sequence: u8, opus: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(2 + opus.len());
    bytes.extend_from_slice(&[sequence, 0]);
    bytes.extend_from_slice(opus);
    bytes
}

pub fn expected_voice_downlink(sender: u16, payload: &[u8]) -> (u8, Vec<u8>) {
    let large = sender > 255;
    let mut writer = NetWriter::new();
    ServerAudioSegmentMessage {
        player_id: sender,
        audio_segment: payload.to_vec(),
    }
    .serialize_with_id_size(&mut writer, large);
    (
        if large {
            channels::VOICE_LARGE
        } else {
            channels::VOICE
        },
        writer.into_vec(),
    )
}

/// Extract complete playable packets using the existing client's Ogg lacing algorithm.
/// This validates framing, not Opus decoding/playback; root uses the real corpus.
pub fn load_ogg_opus(path: &Path) -> Result<Vec<Vec<u8>>> {
    let bytes = std::fs::read(path).with_context(|| format!("read Ogg {}", path.display()))?;
    let mut pos = 0;
    let mut packet = Vec::new();
    let mut packets = Vec::new();
    let mut saw_opus_head = false;
    while pos < bytes.len() {
        ensure!(bytes.len() - pos >= 27, "truncated Ogg header");
        ensure!(&bytes[pos..pos + 4] == b"OggS", "invalid Ogg capture");
        ensure!(bytes[pos + 4] == 0, "unsupported Ogg version");
        let end = pos + 27 + bytes[pos + 26] as usize;
        ensure!(end <= bytes.len(), "truncated Ogg lacing");
        let data_len: usize = bytes[pos + 27..end].iter().map(|n| *n as usize).sum();
        ensure!(end + data_len <= bytes.len(), "truncated Ogg data");
        let mut cursor = end;
        for len in &bytes[pos + 27..end] {
            packet.extend_from_slice(&bytes[cursor..cursor + *len as usize]);
            cursor += *len as usize;
            ensure!(packet.len() <= 65535, "oversized Opus packet");
            if *len < 255 {
                if packet.starts_with(b"OpusHead") {
                    saw_opus_head = true;
                    packet.clear();
                } else if packet.starts_with(b"OpusTags") || packet.is_empty() {
                    packet.clear();
                } else {
                    ensure!(saw_opus_head, "audio before OpusHead");
                    packets.push(std::mem::take(&mut packet));
                }
            }
        }
        pos = end + data_len;
    }
    ensure!(packet.is_empty(), "truncated final Ogg packet");
    ensure!(!packets.is_empty(), "no playable Opus packets");
    Ok(packets)
}

#[test]
fn week_receiver_preserves_dynamic_suffix_across_delta_wrap_and_reuse() -> Result<()> {
    use basis_protocol::avatar::{encode_avatar_bundle, AvatarBundleItem};
    for peer in [7u16, 300] {
        let mut receiver = AvatarReceiver::default();
        let base = avatar_frame(peer, 1, 2, true, 3);
        let changed = avatar_frame(peer, 1, 3, true, 9);
        let disabled = avatar_frame(peer, 1, 4, false, 12);
        ensure!(
            base.additional[0].data.len() == 52 * 4,
            "face coefficient size"
        );
        let large = peer > 255;
        let mut full = NetWriter::new();
        if large {
            full.put_u16(peer);
        } else {
            full.put_u8(peer as u8);
        }
        full.put_u8(0);
        full.put_u8(254);
        full.put_bytes(&base.body);
        full.put_bytes(&base.suffix()?);
        let channel = if large {
            channels::PLAYER_AVATAR_HIGH_ADDITIONAL_LARGE
        } else {
            channels::PLAYER_AVATAR_HIGH_ADDITIONAL
        };
        let bundle = encode_avatar_bundle(&[AvatarBundleItem {
            original_channel: channel,
            payload: full.into_vec(),
        }])?;
        receiver.ingest(channels::COMPRESSED_AVATAR_BUNDLE, &bundle)?;
        base.assert_matches(&receiver.avatars[&peer])?;
        for (sequence, frame) in [(255, &changed), (0, &disabled)] {
            // Reproduce production handle_uplink_avatar_delta offsets, not
            // downlink offsets (uplink has no sender ID or interval byte).
            let (uplink_channel, uplink) = frame.uplink_delta(sequence, 254, &base)?;
            ensure!(
                uplink_channel == channels::DELTA_AVATAR && uplink[..3] == [7, sequence, 254],
                "uplink delta flags/header"
            );
            let (uplink_body, consumed) = apply_delta(&base.body, &uplink[3..], BitQuality::High)?;
            ensure!(
                uplink_body == frame.body && uplink[3 + consumed..] == frame.suffix()?,
                "uplink body/suffix framing"
            );
            let mut delta = NetWriter::new();
            delta.put_u8(
                BitQuality::High as u8
                    | channels::DELTA_HEADER_ADDITIONAL_DATA
                    | if large {
                        channels::DELTA_HEADER_LARGE_ID
                    } else {
                        0
                    },
            );
            if large {
                delta.put_u16(peer);
            } else {
                delta.put_u8(peer as u8);
            }
            delta.put_u8(0);
            delta.put_u8(sequence);
            delta.put_u8(254);
            delta.put_bytes(&build_delta(&base.body, &frame.body, BitQuality::High)?);
            delta.put_bytes(&frame.suffix()?);
            receiver.ingest(channels::DELTA_AVATAR, delta.as_slice())?;
            frame.assert_matches(&receiver.avatars[&peer])?;
        }
        ensure!(
            receiver.delta_items == 2 && receiver.unapplied_deltas == 0,
            "delta reconstruction counts"
        );
        receiver.remove_peer(peer);
        ensure!(
            !receiver.avatars.contains_key(&peer) && !receiver.baselines.contains_key(&peer),
            "reused ID retained avatar baseline"
        );
        let (stale_channel, stale) = changed.uplink_delta(1, 254, &base)?;
        let flags = stale[0]
            | if large {
                channels::DELTA_HEADER_LARGE_ID
            } else {
                0
            };
        let mut downlink = vec![flags];
        if large {
            downlink.extend_from_slice(&peer.to_le_bytes());
        } else {
            downlink.push(peer as u8);
        }
        downlink.push(0);
        downlink.extend_from_slice(&stale[1..]);
        receiver.ingest(stale_channel, &downlink)?;
        ensure!(
            !receiver.avatars.contains_key(&peer) && receiver.unapplied_deltas == 1,
            "old incarnation delta applied after reset"
        );
        ensure!(
            receiver.take_keyframe_requests() == [peer],
            "missing baseline did not request a real keyframe"
        );
        ensure!(
            receiver.take_keyframe_requests().is_empty(),
            "keyframe requests not drained"
        );
        ensure!(
            receiver.ingest(channels::DELTA_AVATAR, &[]).is_err(),
            "empty delta accepted"
        );
        let reused = avatar_frame(peer, 2, 1, true, 1);
        let mut full = NetWriter::new();
        if large {
            full.put_u16(peer);
        } else {
            full.put_u8(peer as u8);
        }
        full.put_u8(0);
        full.put_u8(4);
        full.put_bytes(&reused.body);
        full.put_bytes(&reused.suffix()?);
        receiver.ingest(channel, full.as_slice())?;
        reused.assert_matches(&receiver.avatars[&peer])?;
    }
    Ok(())
}
