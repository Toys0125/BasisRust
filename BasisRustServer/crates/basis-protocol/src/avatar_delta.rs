use anyhow::{bail, ensure, Result};
use std::sync::OnceLock;

use crate::avatar::{
    bpc_table, rotation_field_offsets, BitQuality, BONE_DOF, CURL_BITS, END_EFFECTOR_BLOCK_BYTES,
    FINGER_CHANNEL_COUNT, HINGE_BITS, ROTATION_FIELD_COUNT, SINGLE_BITS, SPLAY_BITS, TAIL_BYTES,
    TWIST_BITS, WIRE_BONE_SLOT_COUNT, WRITE_HIPS_DELTA, WRITE_POSITION, WRITE_ROTATION,
    WRITE_SCALE,
};

pub const FIELD_COUNT: usize = 1 + ROTATION_FIELD_COUNT + 5;
pub const DIRTY_MASK_BYTES: usize = (FIELD_COUNT + 7) >> 3;

const FIELD_SCALE: usize = 1 + ROTATION_FIELD_COUNT;
const FIELD_BODY_ROTATION: usize = FIELD_SCALE + 1;
const FIELD_HIPS_DELTA: usize = FIELD_SCALE + 2;
const FIELD_HIPS_ROTATION: usize = FIELD_SCALE + 3;
const FIELD_END_EFFECTOR: usize = FIELD_SCALE + 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChannelKind {
    Delta,
    Raw,
}

#[derive(Debug, Clone, Copy)]
struct AvatarChannel {
    bit_offset: usize,
    width: usize,
    kind: ChannelKind,
}

impl AvatarChannel {
    fn mask(self) -> u32 {
        if self.width >= 32 {
            u32::MAX
        } else {
            (1u32 << self.width) - 1
        }
    }
}

#[derive(Debug, Clone)]
struct AvatarChannelLayout {
    fields: Vec<Vec<AvatarChannel>>,
    payload_bytes: usize,
}

static LAYOUTS: OnceLock<[AvatarChannelLayout; 4]> = OnceLock::new();

fn layout(quality: BitQuality) -> &'static AvatarChannelLayout {
    &LAYOUTS.get_or_init(|| {
        [
            build_layout(BitQuality::VeryLow),
            build_layout(BitQuality::Low),
            build_layout(BitQuality::Medium),
            build_layout(BitQuality::High),
        ]
    })[quality as usize]
}

fn build_layout(quality: BitQuality) -> AvatarChannelLayout {
    let mut fields = vec![Vec::new(); FIELD_COUNT];

    for axis in 0..3 {
        fields[0].push(AvatarChannel {
            bit_offset: axis * 24,
            width: 24,
            kind: ChannelKind::Delta,
        });
    }

    let rot_base = WRITE_POSITION * 8;
    let offsets = rotation_field_offsets(quality);
    let bpc = bpc_table(quality);
    let qi = quality as usize;

    for slot in 0..WIRE_BONE_SLOT_COUNT {
        let field = 1 + slot;
        let bit = rot_base + offsets[slot];
        match BONE_DOF[slot] {
            3 => {
                fields[field].push(AvatarChannel {
                    bit_offset: bit,
                    width: 2,
                    kind: ChannelKind::Raw,
                });
                for component in 0..3 {
                    fields[field].push(AvatarChannel {
                        bit_offset: bit + 2 + component * bpc[slot] as usize,
                        width: bpc[slot] as usize,
                        kind: ChannelKind::Delta,
                    });
                }
            }
            2 => {
                let hinge = HINGE_BITS[qi] as usize;
                fields[field].push(AvatarChannel {
                    bit_offset: bit,
                    width: hinge,
                    kind: ChannelKind::Delta,
                });
                fields[field].push(AvatarChannel {
                    bit_offset: bit + hinge,
                    width: TWIST_BITS[qi] as usize,
                    kind: ChannelKind::Delta,
                });
            }
            _ => fields[field].push(AvatarChannel {
                bit_offset: bit,
                width: SINGLE_BITS[qi] as usize,
                kind: ChannelKind::Delta,
            }),
        }
    }

    let finger_width = CURL_BITS[qi] as usize + SPLAY_BITS[qi] as usize;
    for finger in 0..FINGER_CHANNEL_COUNT {
        let field = 1 + WIRE_BONE_SLOT_COUNT + finger;
        let bit = rot_base + offsets[WIRE_BONE_SLOT_COUNT + finger];
        fields[field].push(AvatarChannel {
            bit_offset: bit,
            width: CURL_BITS[qi] as usize,
            kind: ChannelKind::Delta,
        });
        fields[field].push(AvatarChannel {
            bit_offset: bit + CURL_BITS[qi] as usize,
            width: SPLAY_BITS[qi] as usize,
            kind: ChannelKind::Delta,
        });
    }

    let rotation_bits = offsets[ROTATION_FIELD_COUNT - 1] + finger_width;
    let rotation_storage_bits = quality.rotation_len() * 8;
    if rotation_storage_bits > rotation_bits {
        fields[1 + ROTATION_FIELD_COUNT - 1].push(AvatarChannel {
            bit_offset: rot_base + rotation_bits,
            width: rotation_storage_bits - rotation_bits,
            kind: ChannelKind::Raw,
        });
    }

    let tail_start = WRITE_POSITION + quality.rotation_len();
    fields[FIELD_SCALE].push(AvatarChannel {
        bit_offset: tail_start * 8,
        width: WRITE_SCALE * 8,
        kind: ChannelKind::Raw,
    });

    let body_rotation = tail_start + WRITE_SCALE;
    add_byte_aligned_quaternion(&mut fields[FIELD_BODY_ROTATION], body_rotation * 8);

    let hips_delta = body_rotation + WRITE_ROTATION;
    for axis in 0..3 {
        fields[FIELD_HIPS_DELTA].push(AvatarChannel {
            bit_offset: hips_delta * 8 + axis * 13,
            width: 13,
            kind: ChannelKind::Delta,
        });
    }
    let hips_used_bits = 3 * 13;
    if hips_used_bits < WRITE_HIPS_DELTA * 8 {
        fields[FIELD_HIPS_DELTA].push(AvatarChannel {
            bit_offset: hips_delta * 8 + hips_used_bits,
            width: WRITE_HIPS_DELTA * 8 - hips_used_bits,
            kind: ChannelKind::Raw,
        });
    }

    let hips_rotation = hips_delta + WRITE_HIPS_DELTA;
    add_byte_aligned_quaternion(&mut fields[FIELD_HIPS_ROTATION], hips_rotation * 8);

    if quality == BitQuality::High {
        let effector_base = (tail_start + TAIL_BYTES) * 8;
        fields[FIELD_END_EFFECTOR].push(AvatarChannel {
            bit_offset: effector_base,
            width: 8,
            kind: ChannelKind::Raw,
        });
        const EFFECTOR_COUNT: usize = 4;
        const POS_BITS: usize = 12;
        const ROT_BPC: usize = 10;
        const STRIDE: usize = 3 * POS_BITS + 2 + 3 * ROT_BPC;
        for effector in 0..EFFECTOR_COUNT {
            let bit = effector_base + 8 + effector * STRIDE;
            for axis in 0..3 {
                fields[FIELD_END_EFFECTOR].push(AvatarChannel {
                    bit_offset: bit + axis * POS_BITS,
                    width: POS_BITS,
                    kind: ChannelKind::Delta,
                });
            }
            let rotation = bit + 3 * POS_BITS;
            fields[FIELD_END_EFFECTOR].push(AvatarChannel {
                bit_offset: rotation,
                width: 2,
                kind: ChannelKind::Raw,
            });
            for component in 0..3 {
                fields[FIELD_END_EFFECTOR].push(AvatarChannel {
                    bit_offset: rotation + 2 + component * ROT_BPC,
                    width: ROT_BPC,
                    kind: ChannelKind::Delta,
                });
            }
        }
        debug_assert_eq!(END_EFFECTOR_BLOCK_BYTES * 8, 8 + EFFECTOR_COUNT * STRIDE);
    }

    let payload_bytes = quality.payload_len();
    debug_assert_eq!(
        fields
            .iter()
            .flat_map(|field| field.iter())
            .map(|channel| channel.width)
            .sum::<usize>(),
        payload_bytes * 8
    );

    AvatarChannelLayout {
        fields,
        payload_bytes,
    }
}

fn add_byte_aligned_quaternion(field: &mut Vec<AvatarChannel>, bit_offset: usize) {
    field.push(AvatarChannel {
        bit_offset,
        width: 8,
        kind: ChannelKind::Raw,
    });
    for component in 0..3 {
        field.push(AvatarChannel {
            bit_offset: bit_offset + 8 + component * 16,
            width: 16,
            kind: ChannelKind::Delta,
        });
    }
}

/// Builds the current Basis dirty-mask/residual delta body against a keyframe baseline.
pub fn build_delta(baseline: &[u8], current: &[u8], quality: BitQuality) -> Result<Vec<u8>> {
    let layout = layout(quality);
    ensure!(
        baseline.len() >= layout.payload_bytes,
        "avatar delta baseline too small"
    );
    ensure!(
        current.len() >= layout.payload_bytes,
        "avatar delta current payload too small"
    );

    let mut mask = [0u8; DIRTY_MASK_BYTES];
    for field in 0..FIELD_COUNT {
        if layout.fields[field]
            .iter()
            .any(|channel| read_channel(current, *channel) != read_channel(baseline, *channel))
        {
            mask[field >> 3] |= 1 << (field & 7);
        }
    }

    // Raw mode can never cost more than the payload plus one mode bit per dirty field.
    let mut bytes = vec![0u8; DIRTY_MASK_BYTES + layout.payload_bytes + 8];
    bytes[..DIRTY_MASK_BYTES].copy_from_slice(&mask);
    let body_start_bit = DIRTY_MASK_BYTES * 8;
    let body_len = {
        let mut writer = ResidualBitWriter::new(&mut bytes, body_start_bit);

        for field in 0..FIELD_COUNT {
            if mask[field >> 3] & (1 << (field & 7)) == 0 {
                continue;
            }
            let mut residual_bits = 0usize;
            let mut raw_bits = 0usize;
            for channel in &layout.fields[field] {
                raw_bits += channel.width;
                if channel.kind == ChannelKind::Raw {
                    residual_bits += channel.width;
                } else {
                    let diff = wrap_signed(
                        read_channel(current, *channel) as i32
                            - read_channel(baseline, *channel) as i32,
                        channel.width,
                    );
                    residual_bits += signed_eg_bits(diff);
                }
            }

            let raw_mode = raw_bits < residual_bits;
            writer.write_bit(raw_mode as u32);
            for channel in &layout.fields[field] {
                let current_value = read_channel(current, *channel);
                if raw_mode || channel.kind == ChannelKind::Raw {
                    writer.write_bits(current_value as u64, channel.width);
                } else {
                    let diff = wrap_signed(
                        current_value as i32 - read_channel(baseline, *channel) as i32,
                        channel.width,
                    );
                    writer.write_signed_eg(diff);
                }
            }
        }

        let body_bits = writer.bit_position - body_start_bit;
        let pad = (8 - (body_bits & 7)) & 7;
        if pad > 0 {
            writer.write_bits(0, pad);
        }
        DIRTY_MASK_BYTES + ((body_bits + 7) >> 3)
    };
    bytes.truncate(body_len);
    Ok(bytes)
}

/// Applies a current Basis avatar delta against a keyframe baseline.
///
/// Returns the reconstructed fixed-size payload and the number of bytes consumed by the delta body.
/// Any bytes after that body belong to AdditionalAvatarData and are intentionally left to the caller.
pub fn apply_delta(
    baseline: &[u8],
    delta_and_trailing: &[u8],
    quality: BitQuality,
) -> Result<(Vec<u8>, usize)> {
    let layout = layout(quality);
    ensure!(
        baseline.len() >= layout.payload_bytes,
        "avatar delta baseline too small"
    );
    ensure!(
        delta_and_trailing.len() >= DIRTY_MASK_BYTES,
        "avatar delta missing dirty mask"
    );

    let mask = &delta_and_trailing[..DIRTY_MASK_BYTES];
    let mut output = baseline[..layout.payload_bytes].to_vec();
    let body_start_bit = DIRTY_MASK_BYTES * 8;
    let mut reader = ResidualBitReader::new(
        delta_and_trailing,
        body_start_bit,
        delta_and_trailing.len() * 8,
    );

    for field in 0..FIELD_COUNT {
        if mask[field >> 3] & (1 << (field & 7)) == 0 {
            continue;
        }
        let raw_mode = reader.read_bit()? != 0;
        for channel in &layout.fields[field] {
            let value = if raw_mode || channel.kind == ChannelKind::Raw {
                reader.read_bits(channel.width)? as u32
            } else {
                let diff = reader.read_signed_eg()?;
                let base = read_channel(baseline, *channel) as i32;
                ((base.wrapping_add(diff)) as u32) & channel.mask()
            };
            replace_channel(&mut output, *channel, value);
        }
    }

    let consumed_bits = reader.bit_position - body_start_bit;
    let body_len = DIRTY_MASK_BYTES + ((consumed_bits + 7) >> 3);
    ensure!(
        body_len <= delta_and_trailing.len(),
        "avatar delta body overrun"
    );
    Ok((output, body_len))
}

fn read_channel(payload: &[u8], channel: AvatarChannel) -> u32 {
    let mut value = 0u32;
    for i in 0..channel.width {
        let bit = channel.bit_offset + i;
        if ((payload[bit >> 3] >> (bit & 7)) & 1) != 0 {
            value |= 1u32 << i;
        }
    }
    value
}

fn replace_channel(payload: &mut [u8], channel: AvatarChannel, value: u32) {
    for i in 0..channel.width {
        let bit = channel.bit_offset + i;
        let mask = 1u8 << (bit & 7);
        if ((value >> i) & 1) != 0 {
            payload[bit >> 3] |= mask;
        } else {
            payload[bit >> 3] &= !mask;
        }
    }
}

fn wrap_signed(diff: i32, width: usize) -> i32 {
    if width >= 32 {
        return diff;
    }
    let shift = 32 - width;
    (diff << shift) >> shift
}

#[allow(dead_code)]
fn signed_eg_bits(value: i32) -> usize {
    let zz = ((value << 1) ^ (value >> 31)) as u32;
    2 * (32 - (zz + 1).leading_zeros()) as usize - 1
}

struct ResidualBitWriter<'a> {
    bytes: &'a mut [u8],
    bit_position: usize,
}

impl<'a> ResidualBitWriter<'a> {
    fn new(bytes: &'a mut [u8], bit_position: usize) -> Self {
        Self {
            bytes,
            bit_position,
        }
    }

    fn write_bit(&mut self, value: u32) {
        self.write_bits((value & 1) as u64, 1);
    }

    fn write_bits(&mut self, value: u64, count: usize) {
        for i in 0..count {
            let bit = self.bit_position + i;
            let mask = 1u8 << (bit & 7);
            if ((value >> i) & 1) != 0 {
                self.bytes[bit >> 3] |= mask;
            } else {
                self.bytes[bit >> 3] &= !mask;
            }
        }
        self.bit_position += count;
    }

    fn write_signed_eg(&mut self, value: i32) {
        let zz = ((value << 1) ^ (value >> 31)) as u32;
        let m = zz + 1;
        let num_bits = (32 - m.leading_zeros()) as usize;
        if num_bits > 1 {
            self.write_bits(0, num_bits - 1);
        }
        self.write_bit(1);
        for bit in (0..num_bits.saturating_sub(1)).rev() {
            self.write_bit((m >> bit) & 1);
        }
    }
}

struct ResidualBitReader<'a> {
    bytes: &'a [u8],
    bit_position: usize,
    end_bit: usize,
}

impl<'a> ResidualBitReader<'a> {
    fn new(bytes: &'a [u8], bit_position: usize, end_bit: usize) -> Self {
        Self {
            bytes,
            bit_position,
            end_bit,
        }
    }

    fn read_bit(&mut self) -> Result<u32> {
        self.read_bits(1).map(|value| value as u32)
    }

    fn read_bits(&mut self, count: usize) -> Result<u64> {
        if self.bit_position.saturating_add(count) > self.end_bit {
            bail!("truncated avatar delta bitstream");
        }
        let mut value = 0u64;
        for i in 0..count {
            let bit = self.bit_position + i;
            if ((self.bytes[bit >> 3] >> (bit & 7)) & 1) != 0 {
                value |= 1u64 << i;
            }
        }
        self.bit_position += count;
        Ok(value)
    }

    fn read_signed_eg(&mut self) -> Result<i32> {
        let mut zeros = 0usize;
        loop {
            if self.bit_position >= self.end_bit {
                bail!("truncated avatar delta Exp-Golomb code");
            }
            if self.read_bit()? != 0 {
                break;
            }
            zeros += 1;
            if zeros > 32 {
                bail!("invalid avatar delta Exp-Golomb prefix");
            }
        }

        let mut m = 1u32;
        for _ in 0..zeros {
            m = (m << 1) | self.read_bit()?;
        }
        let zz = m.wrapping_sub(1);
        Ok(((zz >> 1) as i32) ^ -((zz & 1) as i32))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::avatar::read_position;

    #[test]
    fn layouts_partition_current_payloads() {
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let layout = layout(quality);
            assert_eq!(layout.fields.len(), FIELD_COUNT);
            assert_eq!(layout.payload_bytes, quality.payload_len());
        }
    }

    #[test]
    fn simple_position_residual_delta_applies_exactly() {
        let baseline = vec![0u8; BitQuality::High.payload_len()];
        // Dirty field 0, residual mode. X moves +1 quantized step, Y/Z stay zero.
        // Bits after the 5-byte mask: mode=0, se(+1)=0,1,1, se(0)=1, se(0)=1.
        let delta = [1, 0, 0, 0, 0, 0x3c];
        let (full, consumed) = apply_delta(&baseline, &delta, BitQuality::High).unwrap();
        assert_eq!(consumed, delta.len());
        assert_eq!(read_position(&full), Some([0.001, 0.0, 0.0]));
    }

    #[test]
    fn build_and_apply_round_trip_all_qualities() {
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let baseline = (0..quality.payload_len())
                .map(|i| (i.wrapping_mul(37) & 0xff) as u8)
                .collect::<Vec<_>>();
            let mut current = baseline.clone();
            for index in (0..current.len()).step_by(7) {
                current[index] ^= 0x5a;
            }
            let delta = build_delta(&baseline, &current, quality).unwrap();
            let (rebuilt, consumed) = apply_delta(&baseline, &delta, quality).unwrap();
            assert_eq!(consumed, delta.len());
            assert_eq!(rebuilt, current);
        }
    }

    #[test]
    fn malformed_delta_is_rejected() {
        let baseline = vec![0u8; BitQuality::High.payload_len()];
        assert!(apply_delta(&baseline, &[1, 0, 0, 0, 0], BitQuality::High).is_err());
    }

    #[test]
    fn wrap_signed_matches_fixed_width_ring() {
        assert_eq!(wrap_signed(255, 8), -1);
        assert_eq!(wrap_signed(-255, 8), 1);
        assert_eq!(wrap_signed(12, 8), 12);
    }
}
