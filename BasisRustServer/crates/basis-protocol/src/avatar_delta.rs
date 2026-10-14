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

    // Equal fixed prefixes encode to the zero dirty mask; trailing data is ignored.
    if baseline[..layout.payload_bytes] == current[..layout.payload_bytes] {
        return Ok(vec![0u8; DIRTY_MASK_BYTES]);
    }

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
    debug_assert!((1..=32).contains(&channel.width));
    let first_byte = channel.bit_offset >> 3;
    let bit_shift = channel.bit_offset & 7;

    // Avatar payloads usually have at least eight bytes available from this
    // channel's first byte. Read a single bounded word in that case; near the
    // fixed payload tail, gather only the bytes the channel actually spans.
    if let Some(end) = first_byte.checked_add(8) {
        if let Some(bytes) = payload.get(first_byte..end) {
            let window = u64::from_le_bytes(bytes.try_into().expect("eight-byte window"));
            let mask = if channel.width == 32 {
                u64::from(u32::MAX)
            } else {
                (1u64 << channel.width) - 1
            };
            return ((window >> bit_shift) & mask) as u32;
        }
    }

    let byte_count = (bit_shift + channel.width + 7) >> 3;
    let bytes = &payload[first_byte..first_byte + byte_count];

    // A 32-bit channel can begin seven bits into a byte, so at most five bytes are
    // gathered. The layout guarantees these bytes are within the fixed avatar payload.
    let mut window = 0u64;
    for (index, byte) in bytes.iter().copied().enumerate() {
        window |= u64::from(byte) << (index * 8);
    }

    let mask = if channel.width == 32 {
        u64::from(u32::MAX)
    } else {
        (1u64 << channel.width) - 1
    };
    ((window >> bit_shift) & mask) as u32
}

fn replace_channel(payload: &mut [u8], channel: AvatarChannel, value: u32) {
    if channel.width == 0 {
        return;
    }

    let first_byte = channel.bit_offset >> 3;
    let bit_shift = channel.bit_offset & 7;
    let span_bits = bit_shift + channel.width;
    if span_bits <= 64 {
        let byte_count = (span_bits + 7) >> 3;
        // Avatar layouts are bounded by their fixed payload lengths. Keep the old
        // bit writer as a fallback if a future layout violates that invariant.
        if let Some(bytes) = payload.get_mut(first_byte..first_byte + byte_count) {
            let mut window = 0u64;
            for (index, byte) in bytes.iter().copied().enumerate() {
                window |= u64::from(byte) << (index * 8);
            }

            let channel_mask = if channel.width >= 64 {
                u64::MAX
            } else {
                (1u64 << channel.width) - 1
            };
            let shifted_mask = channel_mask << bit_shift;
            window = (window & !shifted_mask) | ((u64::from(value) & channel_mask) << bit_shift);
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = (window >> (index * 8)) as u8;
            }
            return;
        }
    }

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

    fn write_bits(&mut self, mut value: u64, count: usize) {
        debug_assert!(count <= u64::BITS as usize);
        let mut byte_index = self.bit_position >> 3;
        let mut shift = self.bit_position & 7;
        let mut remaining = count;
        while remaining != 0 {
            let bits = remaining.min(8 - shift);
            let mask = ((1u16 << bits) - 1) as u8;
            let byte = &mut self.bytes[byte_index];
            *byte = (*byte & !(mask << shift)) | (((value as u8) & mask) << shift);
            value >>= bits;
            remaining -= bits;
            byte_index += 1;
            shift = 0;
        }
        self.bit_position += count;
    }

    fn write_signed_eg(&mut self, value: i32) {
        if value == 0 {
            self.write_bit(1);
            return;
        }
        let zz = ((value << 1) ^ (value >> 31)) as u32;
        let m = zz + 1;
        let num_bits = 32 - m.leading_zeros();
        // Prefix zeros followed by m in MSB-first order, packed for the LSB writer.
        // Layout residuals are at most 24 bits; the complete code fits in u64.
        let reversed = m.reverse_bits() >> (32 - num_bits);
        let code = u64::from(reversed) << (num_bits - 1);
        self.write_bits(code, (2 * num_bits - 1) as usize);
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
        if self.bit_position.saturating_add(1) > self.end_bit {
            bail!("truncated avatar delta bitstream");
        }
        let Some(byte) = self.bytes.get(self.bit_position >> 3) else {
            bail!("truncated avatar delta bitstream");
        };
        let value = u32::from((*byte >> (self.bit_position & 7)) & 1);
        self.bit_position += 1;
        Ok(value)
    }

    fn read_bits(&mut self, count: usize) -> Result<u64> {
        if self.bit_position.saturating_add(count) > self.end_bit {
            bail!("truncated avatar delta bitstream");
        }

        if count == 0 {
            return Ok(0);
        }

        let bit_shift = self.bit_position & 7;
        let span_bits = bit_shift + count;
        if span_bits <= 64 {
            let first_byte = self.bit_position >> 3;
            let byte_count = (span_bits + 7) >> 3;
            let Some(bytes) = self.bytes.get(first_byte..first_byte + byte_count) else {
                bail!("truncated avatar delta bitstream");
            };
            let mut window = 0u64;
            for (index, byte) in bytes.iter().copied().enumerate() {
                window |= u64::from(byte) << (index * 8);
            }
            let mask = if count == 64 {
                u64::MAX
            } else {
                (1u64 << count) - 1
            };
            let value = (window >> bit_shift) & mask;
            self.bit_position += count;
            return Ok(value);
        }

        // Current channel widths fit in a single aligned u64 window. Retain the
        // original bounded behavior for any future wider private caller.
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

    fn bit_loop_reference(payload: &[u8], channel: AvatarChannel) -> u32 {
        let mut value = 0u32;
        for bit_index in 0..channel.width {
            let bit = channel.bit_offset + bit_index;
            if ((payload[bit >> 3] >> (bit & 7)) & 1) != 0 {
                value |= 1u32 << bit_index;
            }
        }
        value
    }

    fn read_bits_reference(bytes: &[u8], start: usize, count: usize) -> u64 {
        let mut value = 0u64;
        for index in 0..count {
            let bit = start + index;
            if ((bytes[bit >> 3] >> (bit & 7)) & 1) != 0 {
                value |= 1u64 << index;
            }
        }
        value
    }

    fn write_bits_reference(bytes: &mut [u8], position: &mut usize, value: u64, count: usize) {
        for index in 0..count {
            let bit = *position + index;
            let mask = 1u8 << (bit & 7);
            if ((value >> index) & 1) != 0 {
                bytes[bit >> 3] |= mask;
            } else {
                bytes[bit >> 3] &= !mask;
            }
        }
        *position += count;
    }

    fn write_signed_eg_reference(bytes: &mut [u8], position: &mut usize, value: i32) {
        let zz = ((value << 1) ^ (value >> 31)) as u32;
        let m = zz + 1;
        let num_bits = (32 - m.leading_zeros()) as usize;
        if num_bits > 1 {
            write_bits_reference(bytes, position, 0, num_bits - 1);
        }
        write_bits_reference(bytes, position, 1, 1);
        for bit in (0..num_bits.saturating_sub(1)).rev() {
            write_bits_reference(bytes, position, u64::from((m >> bit) & 1), 1);
        }
    }

    #[test]
    fn residual_writer_matches_bit_loop_at_boundaries_and_preserves_neighbors() {
        for start in 0usize..16 {
            for count in 0usize..=64 {
                for value in [0, u64::MAX, 0xaaaa_5555_1234_5678, 0x735a_0f1e_9a63_142b] {
                    for extra in [0, 3] {
                        let mut actual =
                            deterministic_payload((start + count).div_ceil(8) + extra, 73);
                        let mut expected = actual.clone();
                        let mut position = start;
                        write_bits_reference(&mut expected, &mut position, value, count);
                        let actual_position = {
                            let mut writer = ResidualBitWriter::new(&mut actual, start);
                            writer.write_bits(value, count);
                            writer.bit_position
                        };
                        assert_eq!(actual_position, position);
                        assert_eq!(
                            actual, expected,
                            "start={start} count={count} value={value}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn exp_golomb_writer_matches_legacy_codes_and_concatenated_streams() {
        let mut values = vec![0, -1, 1, i32::MAX, -i32::MAX];
        for width in 1..=24 {
            let sign = 1i32 << (width - 1);
            values.extend([-sign, -sign + 1, sign - 1, sign / 3, -(sign / 3)]);
        }
        for start in 0usize..8 {
            for &value in &values {
                let m = (((value << 1) ^ (value >> 31)) as u32) + 1;
                let count = 2 * (32 - m.leading_zeros()) as usize - 1;
                let mut actual = vec![0xa5; (start + count).div_ceil(8)];
                let mut expected = actual.clone();
                let mut position = start;
                write_signed_eg_reference(&mut expected, &mut position, value);
                let actual_position = {
                    let mut writer = ResidualBitWriter::new(&mut actual, start);
                    writer.write_signed_eg(value);
                    writer.bit_position
                };
                assert_eq!(actual_position, position);
                assert_eq!(actual, expected, "start={start} value={value}");
            }
            let mut actual = vec![0xff; 1024];
            let mut expected = actual.clone();
            let mut position = start;
            let actual_position = {
                let mut writer = ResidualBitWriter::new(&mut actual, start);
                for &value in &values {
                    writer.write_signed_eg(value);
                    write_signed_eg_reference(&mut expected, &mut position, value);
                    writer.write_bits(0x735a, 13);
                    write_bits_reference(&mut expected, &mut position, 0x735a, 13);
                }
                let pad = (8 - (position & 7)) & 7;
                writer.write_bits(0, pad);
                write_bits_reference(&mut expected, &mut position, 0, pad);
                writer.bit_position
            };
            assert_eq!(actual_position, position);
            assert_eq!(actual, expected);
        }
    }

    fn replace_channel_reference(payload: &mut [u8], channel: AvatarChannel, value: u32) {
        for index in 0..channel.width {
            let bit = channel.bit_offset + index;
            let mask = 1u8 << (bit & 7);
            if ((value >> index) & 1) != 0 {
                payload[bit >> 3] |= mask;
            } else {
                payload[bit >> 3] &= !mask;
            }
        }
    }

    fn read_bits_reference_checked(
        bytes: &[u8],
        bit_position: &mut usize,
        end_bit: usize,
        count: usize,
    ) -> Result<u64> {
        if bit_position.saturating_add(count) > end_bit {
            bail!("truncated avatar delta bitstream");
        }
        let value = read_bits_reference(bytes, *bit_position, count);
        *bit_position += count;
        Ok(value)
    }

    fn read_signed_eg_reference(
        bytes: &[u8],
        bit_position: &mut usize,
        end_bit: usize,
    ) -> Result<i32> {
        let mut zeros = 0usize;
        loop {
            if *bit_position >= end_bit {
                bail!("truncated avatar delta Exp-Golomb code");
            }
            if read_bits_reference_checked(bytes, bit_position, end_bit, 1)? != 0 {
                break;
            }
            zeros += 1;
            if zeros > 32 {
                bail!("invalid avatar delta Exp-Golomb prefix");
            }
        }
        let mut m = 1u32;
        for _ in 0..zeros {
            m = (m << 1) | read_bits_reference_checked(bytes, bit_position, end_bit, 1)? as u32;
        }
        let zz = m.wrapping_sub(1);
        Ok(((zz >> 1) as i32) ^ -((zz & 1) as i32))
    }

    fn apply_delta_reference(
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
        let end_bit = delta_and_trailing.len() * 8;
        let mut bit_position = body_start_bit;
        for field in 0..FIELD_COUNT {
            if mask[field >> 3] & (1 << (field & 7)) == 0 {
                continue;
            }
            let raw_mode =
                read_bits_reference_checked(delta_and_trailing, &mut bit_position, end_bit, 1)?
                    != 0;
            for channel in &layout.fields[field] {
                let value = if raw_mode || channel.kind == ChannelKind::Raw {
                    read_bits_reference_checked(
                        delta_and_trailing,
                        &mut bit_position,
                        end_bit,
                        channel.width,
                    )? as u32
                } else {
                    let diff =
                        read_signed_eg_reference(delta_and_trailing, &mut bit_position, end_bit)?;
                    let base = bit_loop_reference(baseline, *channel) as i32;
                    ((base.wrapping_add(diff)) as u32) & channel.mask()
                };
                replace_channel_reference(&mut output, *channel, value);
            }
        }
        let consumed_bits = bit_position - body_start_bit;
        let body_len = DIRTY_MASK_BYTES + ((consumed_bits + 7) >> 3);
        ensure!(
            body_len <= delta_and_trailing.len(),
            "avatar delta body overrun"
        );
        Ok((output, body_len))
    }

    fn deterministic_payload(len: usize, seed: u32) -> Vec<u8> {
        let mut state = seed;
        (0..len)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 24) as u8
            })
            .collect()
    }

    #[test]
    fn residual_word_reads_match_bit_loop_at_all_widths_and_boundaries() {
        let seeds = [0, 1, 0x1234_5678, 0xdead_beef, u32::MAX];
        for byte_len in [1usize, 8, 19] {
            let bit_len = byte_len * 8;
            let payloads = seeds.map(|seed| deterministic_payload(byte_len, seed));
            for count in 0..=64 {
                if count > bit_len {
                    continue;
                }
                for start in 0..=bit_len - count {
                    for bytes in &payloads {
                        let mut reader = ResidualBitReader::new(bytes, start, bit_len);
                        assert_eq!(
                            reader.read_bits(count).unwrap(),
                            read_bits_reference(bytes, start, count),
                            "start={start} count={count} bytes={byte_len}",
                        );
                        assert_eq!(reader.bit_position, start + count);
                    }
                }
            }
        }

        // A short logical end and a short backing slice both fail without advancing;
        // zero-width reads preserve their old no-op behavior at the end boundary.
        let bytes = [0xff, 0x5a];
        let mut reader = ResidualBitReader::new(&bytes, 7, 8);
        assert!(reader.read_bits(2).is_err());
        assert_eq!(reader.bit_position, 7);
        assert_eq!(reader.read_bits(0).unwrap(), 0);
        assert_eq!(reader.bit_position, 7);
        let mut reader = ResidualBitReader::new(&bytes[..1], 7, 16);
        assert!(reader.read_bits(2).is_err());
        assert_eq!(reader.bit_position, 7);
    }

    #[test]
    fn channel_window_writes_match_bit_loop_and_preserve_neighbors() {
        let seeds = [0, 1, 0x1234_5678, 0xdead_beef, u32::MAX];
        for byte_len in [1usize, 8, 19] {
            let bit_len = byte_len * 8;
            let payloads = seeds.map(|seed| deterministic_payload(byte_len, seed));
            for width in 0..=32 {
                if width > bit_len {
                    continue;
                }
                for bit_offset in 0..=bit_len - width {
                    let channel = AvatarChannel {
                        bit_offset,
                        width,
                        kind: ChannelKind::Raw,
                    };
                    for (seed_index, payload) in payloads.iter().enumerate() {
                        for value in [
                            0,
                            u32::MAX,
                            0xaaaa_5555,
                            (seed_index as u32).wrapping_mul(7919),
                        ] {
                            let mut expected = payload.clone();
                            let mut actual = payload.clone();
                            replace_channel_reference(&mut expected, channel, value);
                            replace_channel(&mut actual, channel, value);
                            assert_eq!(actual, expected, "offset={bit_offset} width={width}");
                        }
                    }
                }
            }
        }

        let mut payload = [0x5a];
        replace_channel(
            &mut payload,
            AvatarChannel {
                bit_offset: usize::MAX,
                width: 0,
                kind: ChannelKind::Raw,
            },
            u32::MAX,
        );
        assert_eq!(payload, [0x5a]);
    }

    #[test]
    fn public_decoder_matches_bit_loop_reference_for_all_qualities_and_truncations() {
        let seeds = [1, 0x1234_5678, 0xdead_beef];
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            for seed in seeds {
                let baseline = deterministic_payload(quality.payload_len(), seed);
                let mut current = baseline.clone();
                for (index, byte) in current.iter_mut().enumerate() {
                    if (index.wrapping_mul(31).wrapping_add(seed as usize)) % 7 == 0 {
                        *byte ^= (seed.rotate_left((index & 31) as u32) as u8) | 1;
                    }
                }
                let delta = build_delta(&baseline, &current, quality).unwrap();
                let mut with_trailing = delta.clone();
                with_trailing.extend_from_slice(&[0xa5, 0x5a, 0xc3]);
                assert_eq!(
                    apply_delta(&baseline, &with_trailing, quality).unwrap(),
                    apply_delta_reference(&baseline, &with_trailing, quality).unwrap(),
                    "quality={quality:?} seed={seed:#x}",
                );

                // Every truncated prefix must preserve the same public failure result.
                for end in 0..delta.len() {
                    let optimized = apply_delta(&baseline, &delta[..end], quality)
                        .unwrap_err()
                        .to_string();
                    let reference = apply_delta_reference(&baseline, &delta[..end], quality)
                        .unwrap_err()
                        .to_string();
                    assert_eq!(
                        optimized, reference,
                        "quality={quality:?} seed={seed:#x} end={end}"
                    );
                }
            }
        }
    }

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
    fn word_channel_read_matches_bit_loop_for_layouts_and_all_widths() {
        let qualities = [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ];
        let seeds = [0, 1, 0x1234_5678, 0xdead_beef, u32::MAX];

        // Exercise every channel in every protocol layout using different deterministic bytes.
        for quality in qualities {
            let layout = layout(quality);
            let payloads = seeds.map(|seed| deterministic_payload(layout.payload_bytes, seed));
            for (field_index, field) in layout.fields.iter().enumerate() {
                for (channel_index, channel) in field.iter().copied().enumerate() {
                    assert!((1..=32).contains(&channel.width));
                    assert!(channel.bit_offset + channel.width <= layout.payload_bytes * 8);
                    for payload in &payloads {
                        assert_eq!(
                            read_channel(payload, channel),
                            bit_loop_reference(payload, channel),
                            "quality={quality:?} field={field_index} channel={channel_index} offset={} width={}",
                            channel.bit_offset,
                            channel.width,
                        );
                    }
                }
            }
        }

        // Cover each supported width at every possible offset in differently sized buffers,
        // including ranges that end at the final bit and ranges crossing up to five bytes.
        for width in 1usize..=32 {
            for byte_len in [width.div_ceil(8), 8, 19] {
                let bit_len = byte_len * 8;
                if width > bit_len {
                    continue;
                }
                let payloads = seeds.map(|seed| deterministic_payload(byte_len, seed));
                for bit_offset in 0..=(bit_len - width) {
                    let channel = AvatarChannel {
                        bit_offset,
                        width,
                        kind: ChannelKind::Raw,
                    };
                    for payload in &payloads {
                        assert_eq!(
                            read_channel(payload, channel),
                            bit_loop_reference(payload, channel),
                            "offset={bit_offset} width={width} bytes={byte_len}",
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn build_delta_rejects_truncated_payloads_before_channel_reads() {
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let payload = deterministic_payload(quality.payload_len(), 0x1234_5678);
            assert!(build_delta(&payload[..payload.len() - 1], &payload, quality).is_err());
            assert!(build_delta(&payload, &payload[..payload.len() - 1], quality).is_err());
        }
    }

    #[test]
    #[ignore = "manual release-only comparison of word extraction against the former bit loop"]
    fn bench_word_channel_read_against_bit_loop_reference() {
        use std::hint::black_box;
        use std::time::Instant;

        let layout = layout(BitQuality::High);
        let channels = layout.fields.iter().flatten().copied().collect::<Vec<_>>();
        let payloads = [
            deterministic_payload(layout.payload_bytes, 0x1234_5678),
            deterministic_payload(layout.payload_bytes, 0xdead_beef),
            deterministic_payload(layout.payload_bytes, 0x7f4a_7c15),
            deterministic_payload(layout.payload_bytes, 0x9e37_79b9),
        ];
        let iterations = 50_000;

        let start = Instant::now();
        let mut word_checksum = 0u64;
        for iteration in 0..iterations {
            for (index, channel) in channels.iter().copied().enumerate() {
                let payload = black_box(&payloads[(iteration + index) & 3]);
                word_checksum = word_checksum
                    .wrapping_add(u64::from(black_box(read_channel(payload, channel))));
            }
        }
        let word_elapsed = start.elapsed();

        let start = Instant::now();
        let mut reference_checksum = 0u64;
        for iteration in 0..iterations {
            for (index, channel) in channels.iter().copied().enumerate() {
                let payload = black_box(&payloads[(iteration + index) & 3]);
                reference_checksum = reference_checksum
                    .wrapping_add(u64::from(black_box(bit_loop_reference(payload, channel))));
            }
        }
        let reference_elapsed = start.elapsed();

        assert_eq!(word_checksum, reference_checksum);
        eprintln!(
            "high-quality layout: {} channels x {iterations} passes; word={word_elapsed:?}, bit_loop={reference_elapsed:?}, speedup={:.2}x, checksum={word_checksum}",
            channels.len(),
            reference_elapsed.as_secs_f64() / word_elapsed.as_secs_f64(),
        );
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

    /// Full encoder cost, including allocation, on fixed clean, sparse and dense inputs.
    #[test]
    #[ignore]
    fn bench_build_delta_corpus() {
        use std::hint::black_box;
        use std::time::Instant;

        const PASSES: usize = 1024;
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let layout = layout(quality);
            let mut corpora = [
                ("clean", Vec::new()),
                ("single_axis", Vec::new()),
                ("dense", Vec::new()),
            ];
            for seed in 1..=32 {
                let baseline = deterministic_payload(layout.payload_bytes, seed * 0x13579);
                let mut sparse = baseline.clone();
                for (slot, field) in layout.fields.iter().enumerate().skip(1).take(9) {
                    let delta_channels = field
                        .iter()
                        .filter(|c| c.kind == ChannelKind::Delta)
                        .collect::<Vec<_>>();
                    if let Some(channel) =
                        delta_channels.get((seed as usize + slot) % delta_channels.len().max(1))
                    {
                        let value = read_channel(&baseline, **channel).wrapping_add(1 + seed % 3)
                            & channel.mask();
                        replace_channel_reference(&mut sparse, **channel, value);
                    }
                }
                let dense = deterministic_payload(layout.payload_bytes, seed * 0x2468b + 73);
                corpora[0].1.push((baseline.clone(), baseline.clone()));
                corpora[1].1.push((baseline.clone(), sparse));
                corpora[2].1.push((baseline, dense));
            }
            let mut identities = [0u64; 3];
            let mut zero_channels = [0usize; 3];
            for (index, (_, pairs)) in corpora.iter().enumerate() {
                for (baseline, current) in pairs {
                    let encoded = build_delta(baseline, current, quality).unwrap();
                    assert_eq!(
                        apply_delta(baseline, &encoded, quality).unwrap(),
                        (current.clone(), encoded.len())
                    );
                    for byte in &encoded {
                        identities[index] = identities[index]
                            .wrapping_mul(257)
                            .wrapping_add(u64::from(*byte));
                    }
                    for field in &layout.fields {
                        if field
                            .iter()
                            .any(|c| read_channel(baseline, *c) != read_channel(current, *c))
                        {
                            zero_channels[index] += field
                                .iter()
                                .filter(|c| {
                                    c.kind == ChannelKind::Delta
                                        && read_channel(baseline, **c) == read_channel(current, **c)
                                })
                                .count();
                        }
                    }
                }
            }
            let run = |index: usize, passes: usize| {
                let pairs = &corpora[index].1;
                let mut checksum = 0u64;
                let started = Instant::now();
                for _ in 0..passes {
                    for (baseline, current) in black_box(pairs) {
                        let encoded = black_box(
                            build_delta(black_box(baseline), black_box(current), quality).unwrap(),
                        );
                        checksum = checksum
                            .wrapping_add(encoded.len() as u64)
                            .wrapping_add(u64::from(encoded[0]))
                            .wrapping_add(u64::from(*encoded.last().unwrap()));
                    }
                }
                (started.elapsed().as_nanos(), checksum)
            };
            for index in 0..3 {
                run(index, 8);
            }
            let mut checksums = [None; 3];
            for round in 0..8 {
                let order = if round % 2 == 0 { [0, 1, 2] } else { [2, 1, 0] };
                for index in order {
                    let (elapsed_ns, checksum) = run(index, PASSES);
                    assert_eq!(*checksums[index].get_or_insert(checksum), checksum);
                    println!("delta_build quality={} case={} round={round} operations={} elapsed_ns={elapsed_ns} checksum={checksum} corpus_checksum={} zero_delta_channels={}", quality as u8, corpora[index].0, PASSES * corpora[index].1.len(), identities[index], zero_channels[index]);
                }
            }
        }
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

    #[test]
    fn build_delta_clean_prefix_returns_exact_zero_mask_all_qualities() {
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let payload = deterministic_payload(quality.payload_len(), 0x1234_5678);
            assert_eq!(
                build_delta(&payload, &payload, quality).unwrap(),
                vec![0u8; DIRTY_MASK_BYTES]
            );
            // Distinct but byte-equal allocations take the same path.
            let current = payload.clone();
            assert_eq!(
                build_delta(&payload, &current, quality).unwrap(),
                vec![0u8; DIRTY_MASK_BYTES]
            );
            // All-zero payloads are clean at every quality.
            let zeros = vec![0u8; quality.payload_len()];
            assert_eq!(
                build_delta(&zeros, &zeros, quality).unwrap(),
                vec![0u8; DIRTY_MASK_BYTES]
            );
        }
    }

    #[test]
    fn build_delta_clean_ignores_extra_tails() {
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let prefix_len = quality.payload_len();
            let mut baseline = deterministic_payload(prefix_len + 4, 0x51ab_3c10);
            let mut current = baseline.clone();
            // Mutations past the fixed prefix stay an exact clean mask.
            current[prefix_len] ^= 0xff;
            current[prefix_len + 3] = current[prefix_len + 3].wrapping_add(1);
            assert_eq!(
                build_delta(&baseline, &current, quality).unwrap(),
                vec![0u8; DIRTY_MASK_BYTES]
            );
            // Tails on the baseline side are ignored the same way.
            baseline[prefix_len + 1] ^= 0x01;
            assert_eq!(
                build_delta(&baseline, &current, quality).unwrap(),
                vec![0u8; DIRTY_MASK_BYTES]
            );
        }
    }

    #[test]
    fn build_delta_clean_apply_reconstructs_prefix_and_consumes_mask_only() {
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let prefix_len = quality.payload_len();
            let baseline = deterministic_payload(prefix_len, 0xdead_beef);
            let current = baseline.clone();
            let delta = build_delta(&baseline, &current, quality).unwrap();
            assert_eq!(delta.len(), DIRTY_MASK_BYTES);
            // Trailing AdditionalAvatarData after the mask-only body is left
            // to the caller.
            let mut with_trailing = delta.clone();
            with_trailing.extend_from_slice(&[0xa5, 0x5a, 0xc3]);
            let (rebuilt, consumed) = apply_delta(&baseline, &with_trailing, quality).unwrap();
            assert_eq!(consumed, DIRTY_MASK_BYTES);
            assert_eq!(rebuilt, baseline[..prefix_len].to_vec());
        }
    }

    #[test]
    fn build_delta_dirty_prefix_with_unequal_tails_falls_through() {
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let prefix_len = quality.payload_len();
            let baseline = deterministic_payload(prefix_len + 2, 0x1234_5678);
            let mut current = baseline.clone();
            current[0] ^= 0x01;
            current[prefix_len] ^= 0xff;
            current[prefix_len + 1] = current[prefix_len + 1].wrapping_add(7);
            let delta = build_delta(&baseline, &current, quality).unwrap();
            assert_ne!(delta, vec![0u8; DIRTY_MASK_BYTES]);
            let (rebuilt, consumed) = apply_delta(&baseline, &delta, quality).unwrap();
            assert_eq!(consumed, delta.len());
            assert_eq!(rebuilt, current[..prefix_len].to_vec());
        }
    }

    #[test]
    fn build_delta_length_errors_keep_message_and_order() {
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let full = deterministic_payload(quality.payload_len(), 0x1234_5678);
            let short = &full[..full.len() - 1];
            assert_eq!(
                build_delta(short, &full, quality).unwrap_err().to_string(),
                "avatar delta baseline too small"
            );
            assert_eq!(
                build_delta(&full, short, quality).unwrap_err().to_string(),
                "avatar delta current payload too small"
            );
            // Both truncated: the baseline guard still fires first.
            assert_eq!(
                build_delta(short, short, quality).unwrap_err().to_string(),
                "avatar delta baseline too small"
            );
            // Empty inputs hit the baseline guard first at every quality.
            assert_eq!(
                build_delta(&[], &[], quality).unwrap_err().to_string(),
                "avatar delta baseline too small"
            );
        }
    }

    #[test]
    fn build_delta_single_bit_flips_cover_every_prefix_bit() {
        // Differential for the fast path's premise: every fixed-prefix bit is
        // semantically covered, so any byte-unequal prefix falls through to a
        // dirty encoding that still round-trips. A bit that produced a clean
        // mask here would be padding/dead; the fall-through keeps even that
        // case as correct as the original path, and this test pins the
        // current full-coverage layout.
        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let prefix_len = quality.payload_len();
            let baseline = deterministic_payload(prefix_len, 0x9e37_79b9);
            for bit in 0..prefix_len * 8 {
                let mut current = baseline.clone();
                current[bit >> 3] ^= 1u8 << (bit & 7);
                let delta = build_delta(&baseline, &current, quality).unwrap();
                assert_ne!(
                    delta,
                    vec![0u8; DIRTY_MASK_BYTES],
                    "quality={quality:?} bit={bit}"
                );
                let (rebuilt, consumed) = apply_delta(&baseline, &delta, quality).unwrap();
                assert_eq!(consumed, delta.len());
                assert_eq!(rebuilt, current);
            }
        }
    }

    #[test]
    fn residual_read_bit_matches_checked_single_bit_semantics() {
        // Value + advance at representative positions, including byte
        // boundaries and the last bit before end_bit.
        let bytes = [0b1010_0101, 0b0111_1000];
        for start in 0..16 {
            let mut reader = ResidualBitReader::new(&bytes, start, 16);
            assert_eq!(
                reader.read_bit().unwrap(),
                read_bits_reference(&bytes, start, 1) as u32,
                "start={start}",
            );
            assert_eq!(reader.bit_position, start + 1);
        }

        // Every failure keeps the exact read_bits(1) error string and leaves
        // bit_position unadvanced.
        let failures: Vec<(&[u8], usize, usize)> = vec![
            (&bytes, 16, 16),                 // pos == end_bit
            (&bytes, 15, 15),                 // pos + 1 > end_bit
            (&bytes[..1], 8, 16),             // end_bit passes, backing byte missing
            (&bytes, 16, usize::MAX),         // end_bit huge, slice short
            (&bytes, usize::MAX, usize::MAX), // saturating_add must not overflow
        ];
        for (slice, start, end) in failures {
            let mut reader = ResidualBitReader::new(slice, start, end);
            assert_eq!(
                reader.read_bit().unwrap_err().to_string(),
                "truncated avatar delta bitstream",
                "start={start} end={end}",
            );
            assert_eq!(reader.bit_position, start);
            // Same state through the general reader fails identically.
            let mut general = ResidualBitReader::new(slice, start, end);
            let general_err = general.read_bits(1).unwrap_err().to_string();
            assert_eq!(general_err, "truncated avatar delta bitstream");
            assert_eq!(general.bit_position, start);
        }

        // read_signed_eg truncation strings are unchanged by the specialization.
        let mut reader = ResidualBitReader::new(&[0x00], 8, 8);
        assert_eq!(
            reader.read_signed_eg().unwrap_err().to_string(),
            "truncated avatar delta Exp-Golomb code",
        );
        let zeros = [0x00; 5];
        let mut reader = ResidualBitReader::new(&zeros, 0, 40);
        assert_eq!(
            reader.read_signed_eg().unwrap_err().to_string(),
            "invalid avatar delta Exp-Golomb prefix",
        );
    }

    /// Full decode cost, including allocation, on a fixed prebuilt corpus.
    ///
    /// Ignored screen harness for the `read_bit` follow-up. The encoded corpus
    /// is built once with the accepted encoder outside timing and validated
    /// against the bit-loop reference (roundtrip, consumed length, truncation
    /// errors, checksums, operation counts) before any timing. Timing covers
    /// only `apply_delta` with `black_box` inputs/outputs and a live checksum.
    /// Expected decoded bytes come from the validated reference decoder, never
    /// fabricated: encoder quantization must not be assumed away.
    #[test]
    #[ignore]
    fn bench_apply_delta_corpus() {
        use std::hint::black_box;
        use std::time::Instant;

        const SEED_COUNT: u32 = 32;
        const ROUNDS: usize = 8;
        // ~20k decodes per round for clean/single-axis, ~3k for dense.
        const PASSES_CLEAN: usize = 625;
        const PASSES_DENSE: usize = 94;

        struct Entry {
            baseline: Vec<u8>,
            encoded: Vec<u8>,
            expected: Vec<u8>,
            consumed: usize,
        }
        fn fold_byte(checksum: u64, byte: u8) -> u64 {
            checksum.wrapping_mul(257).wrapping_add(u64::from(byte))
        }

        for quality in [
            BitQuality::VeryLow,
            BitQuality::Low,
            BitQuality::Medium,
            BitQuality::High,
        ] {
            let layout = layout(quality);
            // Same deterministic constructors as bench_build_delta_corpus.
            type PayloadPair = (Vec<u8>, Vec<u8>);
            let mut corpora: [(&str, Vec<PayloadPair>); 3] = [
                ("clean", Vec::new()),
                ("single_axis", Vec::new()),
                ("dense", Vec::new()),
            ];
            for seed in 1..=SEED_COUNT {
                let baseline = deterministic_payload(layout.payload_bytes, seed * 0x13579);
                let mut sparse = baseline.clone();
                for (slot, field) in layout.fields.iter().enumerate().skip(1).take(9) {
                    let delta_channels = field
                        .iter()
                        .filter(|c| c.kind == ChannelKind::Delta)
                        .collect::<Vec<_>>();
                    if let Some(channel) =
                        delta_channels.get((seed as usize + slot) % delta_channels.len().max(1))
                    {
                        let value = read_channel(&baseline, **channel).wrapping_add(1 + seed % 3)
                            & channel.mask();
                        replace_channel_reference(&mut sparse, **channel, value);
                    }
                }
                let dense = deterministic_payload(layout.payload_bytes, seed * 0x2468b + 73);
                corpora[0].1.push((baseline.clone(), baseline.clone()));
                corpora[1].1.push((baseline.clone(), sparse));
                corpora[2].1.push((baseline, dense));
            }

            // Encode once outside timing; validate everything before timing.
            let mut encoded_corpora: Vec<(&str, Vec<Entry>)> = Vec::new();
            let mut corpus_checksums = Vec::new();
            let mut dirty_fields = Vec::new();
            let mut expected_totals = Vec::new();
            for (case, pairs) in &corpora {
                let mut entries = Vec::with_capacity(pairs.len());
                let mut corpus_checksum = 0u64;
                let mut dirty = 0usize;
                for (baseline, current) in pairs {
                    let encoded = build_delta(baseline, current, quality).unwrap();
                    let (decoded, consumed) = apply_delta(baseline, &encoded, quality).unwrap();
                    let (ref_decoded, ref_consumed) =
                        apply_delta_reference(baseline, &encoded, quality).unwrap();
                    assert_eq!(decoded, ref_decoded, "quality={quality:?} case={case}");
                    assert_eq!(consumed, ref_consumed);
                    assert_eq!(consumed, encoded.len());
                    for byte in &encoded {
                        corpus_checksum = fold_byte(corpus_checksum, *byte);
                    }
                    dirty += encoded
                        .iter()
                        .take(DIRTY_MASK_BYTES)
                        .map(|b| b.count_ones() as usize)
                        .sum::<usize>();
                    entries.push(Entry {
                        baseline: baseline.clone(),
                        encoded,
                        expected: decoded,
                        consumed,
                    });
                }
                // Truncation errors on the first entry must match the reference
                // for every prefix; failure aborts via `?` so post-failure
                // advance is unobservable publicly.
                if let Some(first) = entries.first() {
                    for end in 0..first.encoded.len() {
                        let optimized =
                            apply_delta(&first.baseline, &first.encoded[..end], quality)
                                .unwrap_err()
                                .to_string();
                        let reference =
                            apply_delta_reference(&first.baseline, &first.encoded[..end], quality)
                                .unwrap_err()
                                .to_string();
                        assert_eq!(
                            optimized, reference,
                            "quality={quality:?} case={case} end={end}"
                        );
                    }
                }
                // Expected live checksum over the same fold the timed loop uses.
                let passes = if *case == "dense" {
                    PASSES_DENSE
                } else {
                    PASSES_CLEAN
                };
                let mut expected = 0u64;
                for _ in 0..passes {
                    for entry in &entries {
                        for byte in &entry.expected {
                            expected = fold_byte(expected, *byte);
                        }
                        expected = expected.wrapping_add(entry.consumed as u64);
                    }
                }
                corpus_checksums.push(corpus_checksum);
                dirty_fields.push(dirty);
                expected_totals.push((passes, expected));
                encoded_corpora.push((*case, entries));
            }

            let run = |index: usize, pass_count: usize| {
                let entries = black_box(&encoded_corpora[index].1);
                let mut checksum = 0u64;
                let mut operations = 0usize;
                let started = Instant::now();
                for _ in 0..pass_count {
                    for entry in black_box(entries) {
                        let (decoded, consumed) = black_box(
                            apply_delta(
                                black_box(&entry.baseline),
                                black_box(&entry.encoded),
                                quality,
                            )
                            .unwrap(),
                        );
                        for byte in black_box(decoded.iter()) {
                            checksum = fold_byte(checksum, *byte);
                        }
                        checksum = checksum.wrapping_add(black_box(consumed) as u64);
                        operations += 1;
                    }
                }
                (started.elapsed().as_nanos(), checksum, operations)
            };
            for index in 0..3 {
                run(index, 8);
            }
            let mut checksums = [None; 3];
            for round in 0..ROUNDS {
                let order = if round % 2 == 0 { [0, 1, 2] } else { [2, 1, 0] };
                for index in order {
                    let (passes, expected) = expected_totals[index];
                    let (elapsed_ns, checksum, operations) = run(index, passes);
                    assert_eq!(checksum, expected, "live checksum drift");
                    assert_eq!(*checksums[index].get_or_insert(checksum), checksum);
                    println!("delta_apply quality={} case={} round={round} operations={operations} elapsed_ns={elapsed_ns} checksum={checksum} corpus_checksum={} dirty_fields={} expected_checksum={expected}", quality as u8, encoded_corpora[index].0, corpus_checksums[index], dirty_fields[index]);
                }
            }
        }
    }
}
