use crate::wire::append_synthetic_additional_avatar_data;
use anyhow::Result;
use basis_protocol::avatar::{compress_scale, write_neutral_rotation_region, BitQuality};
use basis_protocol::avatar_delta::build_delta;
use basis_protocol::channels;
use basis_protocol::io::NetReader as ProtocolNetReader;
use basis_protocol::messages::{
    BasisDeserialize, ClientMetaDataMessage as ProtocolClientMetaDataMessage,
};
use basis_transport::PacketProperty;
use rand::Rng;
use std::time::SystemTime;

pub(crate) fn parse_server_avatar_metadata(payload: &[u8]) -> Result<ServerAvatarMetadata> {
    let mut reader = ProtocolNetReader::new(payload);
    let _client_meta = ProtocolClientMetaDataMessage::deserialize(&mut reader)?;
    let sync_interval_ms = reader.get_i32()?.max(1) as u32;
    let base_multiplier = reader.get_i32()?.max(1) as f32;
    let increase_rate = reader.get_f32()?;
    let slowest_send_rate_secs = reader.get_f32()?;
    let _peer_limit = reader.get_i32()?;
    let _permission_bitset = reader.get_bytes_with_length()?;
    let extra_permission_count = reader.get_u16()? as usize;
    if extra_permission_count > 0 {
        let _permission_extras = reader.get_bytes_with_length()?;
    }
    let uplink_delta_enabled = reader.get_u8()? != 0;
    Ok(ServerAvatarMetadata {
        sync_interval_ms,
        base_multiplier,
        increase_rate,
        slowest_send_rate_secs,
        uplink_delta_enabled,
    })
}

pub(crate) fn shared_receive_handoff_ready(connected: bool, avatar_metadata_ready: bool) -> bool {
    connected && avatar_metadata_ready
}

pub(crate) fn unity_interval_tick_due(
    accumulator: &mut f64,
    frame_delta: f64,
    interval: f64,
) -> bool {
    *accumulator = (*accumulator + frame_delta).min(interval * 2.0);
    if *accumulator < interval {
        return false;
    }
    *accumulator = (*accumulator - interval).max(0.0);
    true
}

pub(crate) fn write_packed_bits_lsb(
    destination: &mut [u8],
    bit_offset: usize,
    value: u64,
    bit_count: usize,
) {
    for bit in 0..bit_count {
        let destination_bit = bit_offset + bit;
        let mask = 1u8 << (destination_bit & 7);
        if value & (1u64 << bit) == 0 {
            destination[destination_bit >> 3] &= !mask;
        } else {
            destination[destination_bit >> 3] |= mask;
        }
    }
}

pub(crate) fn encode_high_three_dof_quaternion(q: [f32; 4]) -> u64 {
    // Matches BasisBoneRotationCompression.EncodeSmallestThree at High quality: 2 index bits
    // and three 12-bit components quantized against MAX_COMPONENT=1/sqrt(2).
    const BITS: u32 = 12;
    const MAX_Q: f32 = ((1 << BITS) - 1) as f32;
    let q = normalize_quat(q);
    let (largest, sign) = largest_component(q);
    let mut encoded = largest as u64;
    let mut shift = 2;
    for (index, component) in q.iter().copied().enumerate() {
        if index == largest {
            continue;
        }
        let value =
            ((component * sign * std::f32::consts::SQRT_2).clamp(-1.0, 1.0) * 0.5 + 0.5) * MAX_Q;
        encoded |= (value.round() as u64) << shift;
        shift += BITS;
    }
    encoded
}

pub(crate) fn unity_synthetic_angles(elapsed_secs: f64, amplitude_radians: f32) -> [f32; 10] {
    let mut angles = [0.0; 10];
    angles[0] = amplitude_radians * (elapsed_secs * std::f64::consts::PI).sin() as f32;
    for slot in 0..9 {
        let amplitude = amplitude_radians * (0.15 + slot as f32 * 0.012);
        let frequency = 0.37 + slot as f64 * 0.07;
        let phase = slot as f64 * 0.73;
        angles[slot + 1] =
            amplitude * (elapsed_secs * std::f64::consts::TAU * frequency + phase).sin() as f32;
    }
    angles
}

#[derive(Debug, Clone)]
pub(crate) struct PoseState {
    pub(crate) base: [f32; 3],
    pub(crate) datagram: Vec<u8>,
    pub(crate) unity: UnityAvatarSendState,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct UnityAvatarSendState {
    pub(crate) tick_accumulator_secs: f64,
    pub(crate) current_payload: Vec<u8>,
    pub(crate) sequence: u8,
    pub(crate) last_sent_payload: Vec<u8>,
    pub(crate) last_sent_angles: [f32; 10],
    pub(crate) last_sent_time_secs: f64,
    pub(crate) has_last_sent: bool,
    pub(crate) keyframe_payload: Vec<u8>,
    pub(crate) keyframe_sequence: u8,
    pub(crate) last_keyframe_time_secs: f64,
    pub(crate) has_keyframe: bool,
    pub(crate) force_keyframe: bool,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ServerAvatarMetadata {
    pub(crate) sync_interval_ms: u32,
    pub(crate) base_multiplier: f32,
    pub(crate) increase_rate: f32,
    pub(crate) slowest_send_rate_secs: f32,
    pub(crate) uplink_delta_enabled: bool,
}

impl PoseState {
    #[cfg(test)]
    pub(crate) fn new_random() -> Self {
        let mut rng = rand::thread_rng();
        Self::new_at([
            rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
            rng.gen_range(-0.25..=0.25),
        ])
    }

    pub(crate) fn new_at(base: [f32; 3]) -> Self {
        // Final hot-path datagram layout:
        // [LiteNetLib Unreliable][Basis channel][movement sequence][159-byte High payload].
        // Keeping the complete datagram here avoids allocating/copying twice per movement send.
        let mut datagram = vec![0; 3 + BitQuality::High.payload_len()];
        datagram[0] = PacketProperty::Unreliable as u8;
        datagram[1] = channels::PLAYER_AVATAR_HIGH;
        initialize_static_synthetic_payload(&mut datagram[3..]);
        let mut unity_payload = vec![0; BitQuality::High.payload_len()];
        initialize_static_synthetic_payload(&mut unity_payload);
        unity_payload[0..3].copy_from_slice(&encode_axis_mm(base[0]));
        unity_payload[3..6].copy_from_slice(&encode_axis_mm(base[1]));
        unity_payload[6..9].copy_from_slice(&encode_axis_mm(base[2]));
        Self {
            base,
            datagram,
            unity: UnityAvatarSendState {
                current_payload: unity_payload,
                ..UnityAvatarSendState::default()
            },
        }
    }

    pub(crate) fn drift(&mut self) {
        let mut rng = rand::thread_rng();
        self.base[0] += rng.gen_range(-0.25..=0.25);
        self.base[1] += rng.gen_range(-0.25..=0.25);
        self.base[2] += rng.gen_range(-0.25..=0.25);
    }

    pub(crate) fn position(&self) -> [f32; 3] {
        self.base
    }

    pub(crate) fn update_dynamic_payload_with_drift(
        &mut self,
        elapsed_secs: f32,
        allow_position_drift: bool,
    ) {
        if allow_position_drift {
            self.drift();
        }
        let payload = &mut self.datagram[3..];
        payload[0..3].copy_from_slice(&encode_axis_mm(self.base[0]));
        payload[3..6].copy_from_slice(&encode_axis_mm(self.base[1] + elapsed_secs.sin() * 0.015));
        payload[6..9].copy_from_slice(&encode_axis_mm(self.base[2]));
    }

    pub(crate) fn high_quality_payload(&mut self, elapsed_secs: f32) -> Vec<u8> {
        self.update_dynamic_payload_with_drift(elapsed_secs, false);
        self.datagram[3..].to_vec()
    }

    #[cfg(test)]
    pub(crate) fn write_movement_datagram(
        &mut self,
        sequence: u8,
        start: SystemTime,
        allow_position_drift: bool,
    ) -> &[u8] {
        self.write_movement_datagram_with_additional(sequence, start, allow_position_drift, 0)
    }

    pub(crate) fn write_movement_datagram_with_additional(
        &mut self,
        sequence: u8,
        start: SystemTime,
        allow_position_drift: bool,
        additional_avatar_bytes: u8,
    ) -> &[u8] {
        let elapsed = start.elapsed().unwrap_or_default().as_secs_f32();
        self.update_dynamic_payload_with_drift(elapsed, allow_position_drift);
        self.datagram[2] = sequence;
        self.datagram.truncate(3 + BitQuality::High.payload_len());
        let has_additional = additional_avatar_bytes > 0;
        self.datagram[1] =
            channels::player_avatar_channel_for_quality(BitQuality::High as u8, has_additional);
        append_synthetic_additional_avatar_data(
            &mut self.datagram,
            additional_avatar_bytes,
            sequence,
        );
        &self.datagram
    }

    #[cfg(test)]
    pub(crate) fn write_unity_avatar_datagram(
        &mut self,
        frame_delta_secs: f64,
        elapsed_secs: f64,
        metadata: ServerAvatarMetadata,
        force_keyframe: bool,
        pose_amplitude_radians: f32,
    ) -> Option<&[u8]> {
        self.write_unity_avatar_datagram_with_additional(
            frame_delta_secs,
            elapsed_secs,
            metadata,
            force_keyframe,
            pose_amplitude_radians,
            0,
        )
    }

    pub(crate) fn write_unity_avatar_datagram_with_additional(
        &mut self,
        frame_delta_secs: f64,
        elapsed_secs: f64,
        metadata: ServerAvatarMetadata,
        force_keyframe: bool,
        pose_amplitude_radians: f32,
        additional_avatar_bytes: u8,
    ) -> Option<&[u8]> {
        if force_keyframe {
            self.unity.force_keyframe = true;
        }
        let default_interval = metadata.sync_interval_ms as f64 / 1000.0;
        let calculated = default_interval
            * (metadata.base_multiplier as f64
                + (self.base[0] as f64 * self.base[0] as f64
                    + self.base[1] as f64 * self.base[1] as f64
                    + self.base[2] as f64 * self.base[2] as f64)
                    * metadata.increase_rate as f64);
        let slowest = metadata.slowest_send_rate_secs.max(default_interval as f32) as f64;
        let interval_secs = calculated.clamp(default_interval, slowest);
        if !unity_interval_tick_due(
            &mut self.unity.tick_accumulator_secs,
            frame_delta_secs,
            interval_secs,
        ) {
            return None;
        }

        // Keep all clients colocated while animating valid High-quality hips and body channels.
        // Bone channel packing follows BasisBoneRotationCompression's 12-bit High layout.
        let angles = unity_synthetic_angles(elapsed_secs, pose_amplitude_radians);
        let yaw = angles[0];
        let tail = 9 + BitQuality::High.rotation_len();
        let encoded_yaw =
            smallest_three_quaternion([0.0, (yaw * 0.5).sin(), 0.0, (yaw * 0.5).cos()]);
        self.unity.current_payload[tail + 2..tail + 9].copy_from_slice(&encoded_yaw);
        for slot in 0..9 {
            let angle = angles[slot + 1];
            let axis = match slot % 3 {
                0 => [1.0, 0.0, 0.0],
                1 => [0.0, 1.0, 0.0],
                _ => [0.0, 0.0, 1.0],
            };
            let half_sine = (angle * 0.5).sin();
            let quaternion = [
                axis[0] * half_sine,
                axis[1] * half_sine,
                axis[2] * half_sine,
                (angle * 0.5).cos(),
            ];
            let packed = encode_high_three_dof_quaternion(quaternion);
            write_packed_bits_lsb(
                &mut self.unity.current_payload,
                9 * 8 + slot * 38,
                packed,
                38,
            );
        }

        let heartbeat_due =
            !self.unity.has_last_sent || elapsed_secs - self.unity.last_sent_time_secs >= 5.0;
        let pose_changed =
            !self.unity.has_last_sent || self.unity.current_payload != self.unity.last_sent_payload;
        if !heartbeat_due && !pose_changed {
            return None;
        }
        if !heartbeat_due && self.unity.has_last_sent {
            let root_delta = (yaw - self.unity.last_sent_angles[0]).abs();
            let bones_within_deadband = angles[1..]
                .iter()
                .zip(&self.unity.last_sent_angles[1..])
                .all(|(current, last)| (current - last).abs() <= 0.10_f32.to_radians());
            if root_delta <= 0.05_f32.to_radians() && bones_within_deadband {
                return None;
            }
        }

        let keyframe_due = force_keyframe
            || self.unity.force_keyframe
            || !metadata.uplink_delta_enabled
            || !self.unity.has_keyframe
            || elapsed_secs - self.unity.last_keyframe_time_secs >= 0.5;
        let current = self.unity.current_payload.clone();
        let sequence = self.unity.sequence;
        let mut keyframe = keyframe_due;
        let delta_body = if !keyframe {
            match build_delta(&self.unity.keyframe_payload, &current, BitQuality::High) {
                Ok(delta) if delta.len() < current.len() => Some(delta),
                _ => {
                    keyframe = true;
                    None
                }
            }
        } else {
            None
        };

        self.datagram.clear();
        self.datagram.push(PacketProperty::Unreliable as u8);
        if keyframe {
            self.datagram
                .push(channels::player_avatar_channel_for_quality(
                    BitQuality::High as u8,
                    additional_avatar_bytes > 0,
                ));
            self.datagram.push(sequence);
            self.datagram.extend_from_slice(&current);
            if metadata.uplink_delta_enabled {
                self.unity.keyframe_payload.clone_from(&current);
                self.unity.keyframe_sequence = sequence;
                self.unity.last_keyframe_time_secs = elapsed_secs;
                self.unity.has_keyframe = true;
            }
            self.unity.force_keyframe = false;
        } else if let Some(delta) = delta_body {
            self.datagram.push(channels::DELTA_AVATAR);
            self.datagram.push(
                BitQuality::High as u8
                    | if additional_avatar_bytes > 0 {
                        channels::DELTA_HEADER_ADDITIONAL_DATA
                    } else {
                        0
                    },
            );
            self.datagram.push(sequence);
            self.datagram.push(self.unity.keyframe_sequence);
            self.datagram.extend_from_slice(&delta);
        }
        append_synthetic_additional_avatar_data(
            &mut self.datagram,
            additional_avatar_bytes,
            sequence,
        );
        self.unity.sequence = self.unity.sequence.wrapping_add(1);
        self.unity.last_sent_payload.clone_from(&current);
        self.unity.last_sent_angles = angles;
        self.unity.last_sent_time_secs = elapsed_secs;
        self.unity.has_last_sent = true;
        Some(&self.datagram)
    }
}

pub(crate) fn initialize_static_synthetic_payload(payload: &mut [u8]) {
    debug_assert_eq!(payload.len(), BitQuality::High.payload_len());
    write_neutral_rotation_region(payload, BitQuality::High)
        .expect("high-quality synthetic pose has the protocol-defined payload size");

    let tail = 9 + BitQuality::High.rotation_len();
    payload[tail..tail + 2].copy_from_slice(&compress_scale(1.0).to_le_bytes());
    let identity = smallest_three_quaternion([0.0, 0.0, 0.0, 1.0]);
    payload[tail + 2..tail + 9].copy_from_slice(&identity);
    payload[tail + 9..tail + 14].fill(0);
    payload[tail + 14..tail + 21].copy_from_slice(&identity);
    payload[tail + 21..].fill(0);
}

pub(crate) fn normalize_quat(mut q: [f32; 4]) -> [f32; 4] {
    let len = (q[0] * q[0] + q[1] * q[1] + q[2] * q[2] + q[3] * q[3]).sqrt();
    if len > f32::EPSILON {
        for v in &mut q {
            *v /= len;
        }
    } else {
        q = [0.0, 0.0, 0.0, 1.0];
    }
    q
}

pub(crate) fn largest_component(q: [f32; 4]) -> (usize, f32) {
    let mut largest = 0;
    let mut largest_abs = q[0].abs();
    for (idx, value) in q.iter().enumerate().skip(1) {
        let abs = value.abs();
        if abs > largest_abs {
            largest = idx;
            largest_abs = abs;
        }
    }
    let sign = if q[largest] < 0.0 { -1.0 } else { 1.0 };
    (largest, sign)
}

pub(crate) fn smallest_three_quaternion(q: [f32; 4]) -> [u8; 7] {
    let q = normalize_quat(q);
    let (largest, sign) = largest_component(q);
    let mut out = [0u8; 7];
    out[0] = largest as u8;
    let mut offset = 1;
    let inv_sqrt_2 = std::f32::consts::FRAC_1_SQRT_2;
    for (i, component) in q.iter().copied().enumerate() {
        if i == largest {
            continue;
        }
        let value = (component * sign).clamp(-inv_sqrt_2, inv_sqrt_2);
        let quantized =
            (((value + inv_sqrt_2) / std::f32::consts::SQRT_2) * 65535.0).round() as u16;
        out[offset..offset + 2].copy_from_slice(&quantized.to_le_bytes());
        offset += 2;
    }
    out
}

pub(crate) fn encode_axis_mm(meters: f32) -> [u8; 3] {
    const LIMIT: i32 = (1 << 23) - 1;
    let mm_f = meters * 1000.0;
    let mm = if mm_f.is_nan() {
        0
    } else if mm_f >= LIMIT as f32 {
        LIMIT
    } else if mm_f <= -(LIMIT as f32) {
        -LIMIT
    } else {
        mm_f.round() as i32
    };
    [mm as u8, (mm >> 8) as u8, (mm >> 16) as u8]
}

#[cfg(test)]
pub(crate) fn build_movement_packet(
    sequence: u8,
    pose: &mut PoseState,
    start: SystemTime,
) -> Vec<u8> {
    // Test/helper form excludes the LiteNetLib property/channel bytes and matches the Basis
    // movement payload handed to send_unreliable(). The production hot path sends the reusable
    // complete datagram directly instead.
    pose.write_movement_datagram(sequence, start, true)[2..].to_vec()
}
