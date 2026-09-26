use anyhow::{Context, Result};
use flate2::{write::DeflateEncoder, Compression};
use lz4_flex::{compress, decompress};
use std::{cell::RefCell, io::Write};

use crate::{
    avatar_bundle_dictionary,
    io::{NetReader, NetWriter},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BitQuality {
    VeryLow = 0,
    Low = 1,
    Medium = 2,
    High = 3,
}

impl BitQuality {
    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    pub fn payload_len(self) -> usize {
        match self {
            Self::VeryLow => 74,
            Self::Low => 83,
            Self::Medium => 97,
            Self::High => 159,
        }
    }

    pub fn rotation_len(self) -> usize {
        match self {
            Self::VeryLow => 44,
            Self::Low => 53,
            Self::Medium => 67,
            Self::High => 94,
        }
    }
}

pub const FLOAT_SIZE: usize = 4;
pub const USHORT_SIZE: usize = 2;
pub const VECTOR3_SIZE: usize = 12;
const POSIT_SCALE_BITS: i32 = 16;
const POSIT_SCALE_EXPONENT_BITS: i32 = 1;
const POSIT_SCALE_USEED_SHIFT: i32 = 1 << POSIT_SCALE_EXPONENT_BITS;
pub const WRITE_POSITION: usize = 9;
pub const WRITE_SCALE: usize = 2;
pub const WRITE_ROTATION: usize = 7;
pub const WRITE_HIPS_DELTA: usize = 5;
pub const WRITE_HIPS_ROTATION: usize = 7;
pub const TAIL_BYTES: usize = WRITE_SCALE + WRITE_ROTATION + WRITE_HIPS_DELTA + WRITE_HIPS_ROTATION;
pub const WIRE_BONE_SLOT_COUNT: usize = 21;
pub const FINGER_CHANNEL_COUNT: usize = 10;
pub const ROTATION_FIELD_COUNT: usize = WIRE_BONE_SLOT_COUNT + FINGER_CHANNEL_COUNT;
pub const END_EFFECTOR_BLOCK_BYTES: usize = 35;

pub(crate) const BONE_DOF: [u8; WIRE_BONE_SLOT_COUNT] = [
    3, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 1, 1,
];

const BPC_HIGH: [u8; WIRE_BONE_SLOT_COUNT] = [
    12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 12, 5, 5,
];
const BPC_MEDIUM: [u8; WIRE_BONE_SLOT_COUNT] = [
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 6, 3, 3,
];
const BPC_LOW: [u8; WIRE_BONE_SLOT_COUNT] = [
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 5, 5, 3, 3,
];
const BPC_VERY_LOW: [u8; WIRE_BONE_SLOT_COUNT] = [
    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 4, 2, 2,
];

pub(crate) const HINGE_BITS: [u8; 4] = [6, 7, 9, 13];
pub(crate) const TWIST_BITS: [u8; 4] = [5, 6, 8, 12];
pub(crate) const SINGLE_BITS: [u8; 4] = [4, 4, 5, 7];
pub(crate) const CURL_BITS: [u8; 4] = [5, 6, 7, 8];
pub(crate) const SPLAY_BITS: [u8; 4] = [3, 4, 5, 6];

pub fn encode_avatar_network_load(url: &str, unlock_password: &str) -> Result<Vec<u8>> {
    let mut raw = NetWriter::with_capacity(url.len() + unlock_password.len() + 4);
    raw.put_raw_len_string(url);
    raw.put_raw_len_string(unlock_password);
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&raw.into_vec())?;
    Ok(encoder.finish()?)
}

pub fn compress_scale(scale: f32) -> u16 {
    if !scale.is_finite() || scale <= 0.0 {
        return 0;
    }

    let max_scale = decompress_scale(0x7fff);
    let scale = scale.min(max_scale);
    let remaining_bits = POSIT_SCALE_BITS - 1;
    let k = (scale.log2() / POSIT_SCALE_USEED_SHIFT as f32).floor() as i32;
    let useed_power = 2.0f32.powi(k * POSIT_SCALE_USEED_SHIFT);
    let mut normalized = scale / useed_power;

    let mut exponent = 0u32;
    if normalized >= 2.0 {
        exponent = 1;
        normalized *= 0.5;
    }

    let mut fraction = (normalized - 1.0).clamp(0.0, 1.0);
    let mut payload = 0u32;
    let mut bit = remaining_bits - 1;

    if k >= 0 {
        let ones = k + 1;
        for _ in 0..ones {
            if bit < 0 {
                break;
            }
            payload |= 1u32 << bit;
            bit -= 1;
        }
        if bit >= 0 {
            bit -= 1;
        }
    } else {
        bit -= -k;
        if bit >= 0 {
            payload |= 1u32 << bit;
            bit -= 1;
        }
    }

    if bit >= 0 {
        if exponent != 0 {
            payload |= 1u32 << bit;
        }
        bit -= 1;
    }

    while bit >= 0 {
        fraction *= 2.0;
        if fraction >= 1.0 {
            payload |= 1u32 << bit;
            fraction -= 1.0;
        }
        bit -= 1;
    }

    let truncated = payload as u16;
    let rounded_up = if truncated < 0x7fff {
        truncated + 1
    } else {
        truncated
    };
    let lower = decompress_scale(truncated);
    let upper = decompress_scale(rounded_up);
    if (upper - scale).abs() < (scale - lower).abs() {
        rounded_up
    } else {
        truncated
    }
}

pub fn decompress_scale(mut value: u16) -> f32 {
    if value == 0 {
        return 0.0;
    }

    if value & 0x8000 != 0 {
        value = (!value).wrapping_add(1);
    }

    let raw = value as u32;
    let mut bit = POSIT_SCALE_BITS - 2;
    let regime_sign = ((raw >> bit) & 1) != 0;
    let mut run_length = 0i32;
    while bit >= 0 && (((raw >> bit) & 1) != 0) == regime_sign {
        run_length += 1;
        bit -= 1;
    }
    if bit >= 0 {
        bit -= 1;
    }

    let k = if regime_sign {
        run_length - 1
    } else {
        -run_length
    };
    let mut exponent = 0i32;
    if bit >= 0 {
        exponent = ((raw >> bit) & 1) as i32;
        bit -= 1;
    }

    let mut fraction = 1.0f32;
    let mut fraction_bit = 0.5f32;
    while bit >= 0 {
        if ((raw >> bit) & 1) != 0 {
            fraction += fraction_bit;
        }
        fraction_bit *= 0.5;
        bit -= 1;
    }

    2.0f32.powi(k * POSIT_SCALE_USEED_SHIFT + exponent) * fraction
}

pub fn write_neutral_rotation_region(payload: &mut [u8], quality: BitQuality) -> Result<()> {
    anyhow::ensure!(
        payload.len() >= quality.payload_len(),
        "avatar payload too small for {:?}",
        quality
    );

    let offsets = rotation_field_offsets(quality);
    let bpc = bpc_table(quality);
    let rot_base = WRITE_POSITION;
    payload[rot_base..rot_base + quality.rotation_len()].fill(0);

    for slot in 0..WIRE_BONE_SLOT_COUNT {
        let bit = offsets[slot];
        let packed = match BONE_DOF[slot] {
            3 => {
                let bits = bpc[slot] as usize;
                let midpoint = 1u64 << (bits - 1);
                3u64 | (midpoint << 2) | (midpoint << (2 + bits)) | (midpoint << (2 + 2 * bits))
            }
            2 => {
                let hinge_bits = HINGE_BITS[quality.index()] as usize;
                let twist_bits = TWIST_BITS[quality.index()] as usize;
                let hinge_mid = 1u64 << (hinge_bits - 1);
                let twist_mid = 1u64 << (twist_bits - 1);
                hinge_mid | (twist_mid << hinge_bits)
            }
            _ => {
                let bits = SINGLE_BITS[quality.index()] as usize;
                1u64 << (bits - 1)
            }
        };
        write_bits(
            payload,
            rot_base,
            bit,
            packed,
            bone_field_width(quality, slot),
        );
    }

    let curl_bits = CURL_BITS[quality.index()] as usize;
    let splay_bits = SPLAY_BITS[quality.index()] as usize;
    let curl_mid = 1u64 << (curl_bits - 1);
    let splay_mid = 1u64 << (splay_bits - 1);
    let finger_packed = curl_mid | (splay_mid << curl_bits);
    let finger_width = curl_bits + splay_bits;
    for finger in 0..FINGER_CHANNEL_COUNT {
        write_bits(
            payload,
            rot_base,
            offsets[WIRE_BONE_SLOT_COUNT + finger],
            finger_packed,
            finger_width,
        );
    }

    Ok(())
}

pub fn read_position(payload: &[u8]) -> Option<[f32; 3]> {
    if payload.len() < WRITE_POSITION {
        return None;
    }
    fn axis(src: &[u8]) -> f32 {
        let raw = (src[0] as i32) | ((src[1] as i32) << 8) | ((src[2] as i32) << 16);
        let signed = (raw << 8) >> 8;
        signed as f32 * 0.001
    }
    Some([
        axis(&payload[0..3]),
        axis(&payload[3..6]),
        axis(&payload[6..9]),
    ])
}

pub fn repack_high_to_lower(high_payload: &[u8], target: BitQuality) -> Result<Vec<u8>> {
    let mut out = vec![0u8; target.payload_len()];
    repack_high_to_lower_into(high_payload, target, &mut out)?;
    Ok(out)
}

pub fn repack_high_to_lower_into(
    high_payload: &[u8],
    target: BitQuality,
    out: &mut [u8],
) -> Result<()> {
    anyhow::ensure!(target != BitQuality::High, "target must be lower than High");
    anyhow::ensure!(
        high_payload.len() >= BitQuality::High.payload_len(),
        "high payload too small"
    );
    anyhow::ensure!(
        out.len() >= target.payload_len(),
        "target output buffer too small"
    );

    let out = &mut out[..target.payload_len()];
    out.fill(0);
    out[..WRITE_POSITION].copy_from_slice(&high_payload[..WRITE_POSITION]);

    let high_offsets = rotation_field_offsets(BitQuality::High);
    let target_offsets = rotation_field_offsets(target);
    let target_bpc = bpc_table(target);
    let high_rot_base = WRITE_POSITION;
    let target_rot_base = WRITE_POSITION;

    for slot in 0..WIRE_BONE_SLOT_COUNT {
        let src_bit = high_offsets[slot];
        let dst_bit = target_offsets[slot];
        match BONE_DOF[slot] {
            3 => {
                let src_bpc = BPC_HIGH[slot] as usize;
                let dst_bpc = target_bpc[slot] as usize;
                let raw = read_bits(high_payload, high_rot_base, src_bit, 2 + 3 * src_bpc);
                let idx = raw & 3;
                let src_mask = (1u64 << src_bpc) - 1;
                let qa = (raw >> 2) & src_mask;
                let qb = (raw >> (2 + src_bpc)) & src_mask;
                let qc = (raw >> (2 + 2 * src_bpc)) & src_mask;
                let packed = idx
                    | (rescale_quant(qa, src_bpc, dst_bpc) << 2)
                    | (rescale_quant(qb, src_bpc, dst_bpc) << (2 + dst_bpc))
                    | (rescale_quant(qc, src_bpc, dst_bpc) << (2 + 2 * dst_bpc));
                write_bits(out, target_rot_base, dst_bit, packed, 2 + 3 * dst_bpc);
            }
            2 => {
                let src_hinge = HINGE_BITS[BitQuality::High.index()] as usize;
                let src_twist = TWIST_BITS[BitQuality::High.index()] as usize;
                let dst_hinge = HINGE_BITS[target.index()] as usize;
                let dst_twist = TWIST_BITS[target.index()] as usize;
                let hinge = read_bits(high_payload, high_rot_base, src_bit, src_hinge);
                let twist = read_bits(high_payload, high_rot_base, src_bit + src_hinge, src_twist);
                let packed = rescale_quant(hinge, src_hinge, dst_hinge)
                    | (rescale_quant(twist, src_twist, dst_twist) << dst_hinge);
                write_bits(out, target_rot_base, dst_bit, packed, dst_hinge + dst_twist);
            }
            _ => {
                let src_bits = SINGLE_BITS[BitQuality::High.index()] as usize;
                let dst_bits = SINGLE_BITS[target.index()] as usize;
                let value = read_bits(high_payload, high_rot_base, src_bit, src_bits);
                write_bits(
                    out,
                    target_rot_base,
                    dst_bit,
                    rescale_quant(value, src_bits, dst_bits),
                    dst_bits,
                );
            }
        }
    }

    let src_curl_bits = CURL_BITS[BitQuality::High.index()] as usize;
    let src_splay_bits = SPLAY_BITS[BitQuality::High.index()] as usize;
    let dst_curl_bits = CURL_BITS[target.index()] as usize;
    let dst_splay_bits = SPLAY_BITS[target.index()] as usize;
    for finger in 0..FINGER_CHANNEL_COUNT {
        let field = WIRE_BONE_SLOT_COUNT + finger;
        let src_bit = high_offsets[field];
        let curl = read_bits(high_payload, high_rot_base, src_bit, src_curl_bits);
        let splay = read_bits(
            high_payload,
            high_rot_base,
            src_bit + src_curl_bits,
            src_splay_bits,
        );
        let packed = rescale_quant(curl, src_curl_bits, dst_curl_bits)
            | (rescale_quant(splay, src_splay_bits, dst_splay_bits) << dst_curl_bits);
        write_bits(
            out,
            target_rot_base,
            target_offsets[field],
            packed,
            dst_curl_bits + dst_splay_bits,
        );
    }

    let src_tail = WRITE_POSITION + BitQuality::High.rotation_len();
    let dst_tail = WRITE_POSITION + target.rotation_len();
    out[dst_tail..dst_tail + TAIL_BYTES]
        .copy_from_slice(&high_payload[src_tail..src_tail + TAIL_BYTES]);
    Ok(())
}

pub(crate) fn bpc_table(quality: BitQuality) -> &'static [u8; WIRE_BONE_SLOT_COUNT] {
    match quality {
        BitQuality::VeryLow => &BPC_VERY_LOW,
        BitQuality::Low => &BPC_LOW,
        BitQuality::Medium => &BPC_MEDIUM,
        BitQuality::High => &BPC_HIGH,
    }
}

fn bone_field_width(quality: BitQuality, slot: usize) -> usize {
    match BONE_DOF[slot] {
        3 => 2 + 3 * bpc_table(quality)[slot] as usize,
        2 => HINGE_BITS[quality.index()] as usize + TWIST_BITS[quality.index()] as usize,
        _ => SINGLE_BITS[quality.index()] as usize,
    }
}

pub(crate) fn rotation_field_offsets(quality: BitQuality) -> [usize; ROTATION_FIELD_COUNT] {
    let mut offsets = [0usize; ROTATION_FIELD_COUNT];
    let mut bit = 0usize;
    for (slot, offset) in offsets.iter_mut().enumerate().take(WIRE_BONE_SLOT_COUNT) {
        *offset = bit;
        bit += bone_field_width(quality, slot);
    }
    let finger_width = CURL_BITS[quality.index()] as usize + SPLAY_BITS[quality.index()] as usize;
    for finger in 0..FINGER_CHANNEL_COUNT {
        offsets[WIRE_BONE_SLOT_COUNT + finger] = bit;
        bit += finger_width;
    }
    debug_assert_eq!((bit + 7) >> 3, quality.rotation_len());
    offsets
}

fn rescale_quant(value: u64, src_bits: usize, dst_bits: usize) -> u64 {
    if src_bits == dst_bits {
        return value;
    }
    if dst_bits == 0 {
        return 0;
    }
    let max_src = (1u64 << src_bits) - 1;
    let max_dst = (1u64 << dst_bits) - 1;
    (value * max_dst + (max_src >> 1)) / max_src
}

fn read_bits(src: &[u8], base_byte_offset: usize, bit_pos: usize, bit_count: usize) -> u64 {
    let absolute = (base_byte_offset << 3) + bit_pos;
    let first_byte = absolute >> 3;
    let bit_shift = absolute & 7;
    if bit_count <= 64 - bit_shift {
        let byte_count = (bit_shift + bit_count + 7) >> 3;
        if let Some(end) = first_byte.checked_add(byte_count) {
            if let Some(bytes) = src.get(first_byte..end) {
                let mut window = 0u64;
                for (index, byte) in bytes.iter().copied().enumerate() {
                    window |= u64::from(byte) << (index * 8);
                }
                let mask = if bit_count == 64 {
                    u64::MAX
                } else {
                    (1u64 << bit_count) - 1
                };
                return (window >> bit_shift) & mask;
            }
        }
    }

    // Keep the former path for future callers whose range cannot fit in the
    // bounded u64 window, and preserve its indexing behavior for invalid ranges.
    let mut result = 0u64;
    for i in 0..bit_count {
        let bit = absolute + i;
        if (src[bit >> 3] >> (bit & 7)) & 1 != 0 {
            result |= 1u64 << i;
        }
    }
    result
}

fn write_bits(
    dst: &mut [u8],
    base_byte_offset: usize,
    bit_pos: usize,
    value: u64,
    bit_count: usize,
) {
    let absolute = (base_byte_offset << 3) + bit_pos;
    let first_byte = absolute >> 3;
    let bit_shift = absolute & 7;
    if bit_count <= 64 - bit_shift {
        let byte_count = (bit_shift + bit_count + 7) >> 3;
        if let Some(end) = first_byte.checked_add(byte_count) {
            if let Some(bytes) = dst.get_mut(first_byte..end) {
                let mut window = 0u64;
                for (index, byte) in bytes.iter().copied().enumerate() {
                    window |= u64::from(byte) << (index * 8);
                }
                let mask = if bit_count == 64 {
                    u64::MAX
                } else {
                    (1u64 << bit_count) - 1
                };
                window |= (value & mask) << bit_shift;
                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = (window >> (index * 8)) as u8;
                }
                return;
            }
        }
    }

    // This is also the conservative fallback for widths that exceed one u64 window.
    for i in 0..bit_count {
        if (value >> i) & 1 != 0 {
            let bit = absolute + i;
            dst[bit >> 3] |= 1 << (bit & 7);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvatarBundleItem {
    pub original_channel: u8,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy)]
pub struct AvatarBundleSlice<'a> {
    pub original_channel: u8,
    pub payload: &'a [u8],
    pub interval_patch: Option<(usize, u8)>,
}

pub fn encode_avatar_bundle(items: &[AvatarBundleItem]) -> Result<Vec<u8>> {
    let slices = items
        .iter()
        .map(|item| AvatarBundleSlice {
            original_channel: item.original_channel,
            payload: &item.payload,
            interval_patch: None,
        })
        .collect::<Vec<_>>();
    encode_avatar_bundle_slices(&slices)
}

pub fn encode_avatar_bundle_slices(items: &[AvatarBundleSlice<'_>]) -> Result<Vec<u8>> {
    Ok(try_encode_avatar_bundle_slices(items)?.bytes)
}

#[derive(Debug, Clone)]
pub struct EncodedAvatarBundle {
    pub bytes: Vec<u8>,
    pub raw_len: usize,
    pub compressed_len: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AvatarBundleCompression {
    Lz4,
    ZstdDictionary { level: i32 },
}

const BUNDLE_CODEC_LZ4: u8 = 0;
const BUNDLE_CODEC_ZSTD_DICTIONARY: u8 = 1;
const BUNDLE_CODEC_MASK: u8 = 0x07;
const BUNDLE_DICTIONARY_SHIFT: u8 = 3;
const BUNDLE_ZSTD_WINDOW_LOG: u32 = 17;
const DELTA_CHANNEL: u8 = 30;

pub fn try_encode_avatar_bundle_slices(
    items: &[AvatarBundleSlice<'_>],
) -> Result<EncodedAvatarBundle> {
    try_encode_avatar_bundle_slices_with_compression(items, AvatarBundleCompression::Lz4)
}

pub fn try_encode_avatar_bundle_slices_with_compression(
    items: &[AvatarBundleSlice<'_>],
    compression: AvatarBundleCompression,
) -> Result<EncodedAvatarBundle> {
    let mut raw = NetWriter::new();
    // v50+ bundles are grouped by channel. Keep each channel's original item order while grouping
    // runs together; group counts are bytes, so runs longer than 255 continue as another group.
    let mut channels = items
        .iter()
        .map(|item| item.original_channel)
        .collect::<Vec<_>>();
    channels.sort_unstable();
    channels.dedup();

    for channel in channels {
        let matching = items
            .iter()
            .filter(|item| item.original_channel == channel)
            .collect::<Vec<_>>();
        for chunk in matching.chunks(u8::MAX as usize) {
            if chunk.is_empty() {
                continue;
            }
            raw.put_u8(channel);
            raw.put_u8(chunk.len() as u8);
            for item in chunk {
                anyhow::ensure!(
                    item.payload.len() <= u16::MAX as usize,
                    "bundle item payload too large"
                );
                raw.put_u16(item.payload.len() as u16);
            }

            if channel != DELTA_CHANNEL {
                for item in chunk {
                    write_bundle_payload(&mut raw, item);
                }
            } else {
                // Current Basis transposes only delta bodies by byte column before compression.
                let max_len = chunk
                    .iter()
                    .map(|item| item.payload.len())
                    .max()
                    .unwrap_or(0);
                for offset in 0..max_len {
                    for item in chunk {
                        if offset < item.payload.len() {
                            let value = item
                                .interval_patch
                                .filter(|(patch_offset, _)| *patch_offset == offset)
                                .map(|(_, value)| value)
                                .unwrap_or(item.payload[offset]);
                            raw.put_u8(value);
                        }
                    }
                }
            }
        }
    }

    let raw = raw.into_vec();
    anyhow::ensure!(
        raw.len() <= u16::MAX as usize,
        "bundle raw payload too large"
    );
    let (flags, compressed) = match compression {
        AvatarBundleCompression::Lz4 => (BUNDLE_CODEC_LZ4, compress(&raw)),
        AvatarBundleCompression::ZstdDictionary { level } => {
            let compressed = compress_avatar_bundle_zstd(&raw, level)?;
            let flags = BUNDLE_CODEC_ZSTD_DICTIONARY
                | (avatar_bundle_dictionary::GENERATION << BUNDLE_DICTIONARY_SHIFT);
            (flags, compressed)
        }
    };
    let mut out = NetWriter::with_capacity(compressed.len() + 3);
    out.put_u8(flags);
    out.put_u16(raw.len() as u16);
    out.put_bytes(&compressed);
    Ok(EncodedAvatarBundle {
        bytes: out.into_vec(),
        raw_len: raw.len(),
        compressed_len: compressed.len(),
    })
}

struct AvatarBundleZstdContext {
    level: i32,
    context: zstd_safe::CCtx<'static>,
}

impl AvatarBundleZstdContext {
    fn new(level: i32) -> Result<Self> {
        use zstd_safe::{CParameter, FrameFormat};

        let mut context = zstd_safe::CCtx::try_create()
            .ok_or_else(|| anyhow::anyhow!("creating avatar bundle zstd context failed"))?;
        context
            .set_parameter(CParameter::CompressionLevel(level))
            .map_err(|code| zstd_error("setting zstd compression level", code))?;
        context
            .set_parameter(CParameter::ContentSizeFlag(false))
            .map_err(|code| zstd_error("disabling zstd content-size flag", code))?;
        context
            .set_parameter(CParameter::ChecksumFlag(false))
            .map_err(|code| zstd_error("disabling zstd checksum", code))?;
        context
            .set_parameter(CParameter::DictIdFlag(false))
            .map_err(|code| zstd_error("disabling zstd dictionary id", code))?;
        context
            .set_parameter(CParameter::WindowLog(BUNDLE_ZSTD_WINDOW_LOG))
            .map_err(|code| zstd_error("setting zstd window log", code))?;
        context
            .set_parameter(CParameter::Format(FrameFormat::Magicless))
            .map_err(|code| zstd_error("setting magicless zstd format", code))?;
        context
            .load_dictionary(avatar_bundle_dictionary::bytes())
            .map_err(|code| zstd_error("loading avatar bundle zstd dictionary", code))?;

        Ok(Self { level, context })
    }

    fn compress(&mut self, raw: &[u8]) -> Result<Vec<u8>> {
        use zstd_safe::ResetDirective;

        // Reset only the frame session. zstd-safe guarantees that this preserves both the
        // parameters and the loaded dictionary for the next independent frame.
        self.context
            .reset(ResetDirective::SessionOnly)
            .map_err(|code| zstd_error("resetting avatar bundle zstd session", code))?;
        let mut compressed = vec![0u8; zstd_safe::compress_bound(raw.len())];
        let written = self
            .context
            .compress2(&mut compressed[..], raw)
            .map_err(|code| zstd_error("compressing avatar bundle with zstd", code))?;
        compressed.truncate(written);
        Ok(compressed)
    }
}

thread_local! {
    // Rayon may call this compressor on multiple workers. Each worker owns one context, so
    // compression stays lock-free and retained native workspace is bounded by worker count.
    static AVATAR_BUNDLE_ZSTD_CONTEXT: RefCell<Option<AvatarBundleZstdContext>> = const { RefCell::new(None) };
}

fn compress_avatar_bundle_zstd(raw: &[u8], level: i32) -> Result<Vec<u8>> {
    AVATAR_BUNDLE_ZSTD_CONTEXT.with(|cached| {
        let mut cached = cached.borrow_mut();
        if cached.as_ref().is_none_or(|context| context.level != level) {
            *cached = Some(AvatarBundleZstdContext::new(level)?);
        }
        cached
            .as_mut()
            .expect("avatar bundle zstd context initialized")
            .compress(raw)
    })
}

fn zstd_error(operation: &str, code: usize) -> anyhow::Error {
    anyhow::anyhow!("{operation}: {}", zstd_safe::get_error_name(code))
}

#[cfg(test)]
fn compress_avatar_bundle_zstd_fresh(raw: &[u8], level: i32) -> Result<Vec<u8>> {
    use zstd_safe::{CCtx, CParameter, FrameFormat};

    let mut context = CCtx::default();
    context
        .set_parameter(CParameter::CompressionLevel(level))
        .map_err(|code| zstd_error("setting zstd compression level", code))?;
    context
        .set_parameter(CParameter::ContentSizeFlag(false))
        .map_err(|code| zstd_error("disabling zstd content-size flag", code))?;
    context
        .set_parameter(CParameter::ChecksumFlag(false))
        .map_err(|code| zstd_error("disabling zstd checksum", code))?;
    context
        .set_parameter(CParameter::DictIdFlag(false))
        .map_err(|code| zstd_error("disabling zstd dictionary id", code))?;
    context
        .set_parameter(CParameter::WindowLog(BUNDLE_ZSTD_WINDOW_LOG))
        .map_err(|code| zstd_error("setting zstd window log", code))?;
    context
        .set_parameter(CParameter::Format(FrameFormat::Magicless))
        .map_err(|code| zstd_error("setting magicless zstd format", code))?;
    context
        .load_dictionary(avatar_bundle_dictionary::bytes())
        .map_err(|code| zstd_error("loading avatar bundle zstd dictionary", code))?;
    let mut compressed = vec![0u8; zstd_safe::compress_bound(raw.len())];
    let written = context
        .compress2(&mut compressed[..], raw)
        .map_err(|code| zstd_error("compressing avatar bundle with zstd", code))?;
    compressed.truncate(written);
    Ok(compressed)
}

fn write_bundle_payload(writer: &mut NetWriter, item: &AvatarBundleSlice<'_>) {
    if let Some((offset, value)) = item.interval_patch {
        if offset < item.payload.len() {
            writer.put_bytes(&item.payload[..offset]);
            writer.put_u8(value);
            writer.put_bytes(&item.payload[offset + 1..]);
            return;
        }
    }
    writer.put_bytes(item.payload);
}

pub fn decode_avatar_bundle(bytes: &[u8]) -> Result<Vec<AvatarBundleItem>> {
    anyhow::ensure!(bytes.len() >= 3, "bundle header too short");
    let flags = bytes[0];
    let codec = flags & BUNDLE_CODEC_MASK;
    let dictionary_generation = flags >> BUNDLE_DICTIONARY_SHIFT;
    let raw_len = u16::from_le_bytes([bytes[1], bytes[2]]) as usize;
    let raw = match codec {
        BUNDLE_CODEC_LZ4 => {
            anyhow::ensure!(
                dictionary_generation == 0,
                "LZ4 bundle has dictionary generation"
            );
            decompress(&bytes[3..], raw_len).context("decompressing LZ4 bundle")?
        }
        BUNDLE_CODEC_ZSTD_DICTIONARY => {
            anyhow::ensure!(
                dictionary_generation == avatar_bundle_dictionary::GENERATION,
                "unsupported avatar bundle dictionary generation {dictionary_generation}"
            );
            decompress_avatar_bundle_zstd(&bytes[3..], raw_len)?
        }
        _ => anyhow::bail!("unsupported bundle codec {codec}"),
    };
    anyhow::ensure!(raw.len() == raw_len, "bundle raw length mismatch");

    let mut reader = NetReader::new(&raw);
    let mut items = Vec::new();
    while reader.remaining() > 0 {
        let channel = reader.get_u8()?;
        let count = reader.get_u8()? as usize;
        anyhow::ensure!(count > 0, "bundle group has zero entries");
        let mut lengths = Vec::with_capacity(count);
        let mut body_total = 0usize;
        for _ in 0..count {
            let len = reader.get_u16()? as usize;
            anyhow::ensure!(len > 0, "bundle entry has zero length");
            body_total = body_total.saturating_add(len);
            lengths.push(len);
        }
        anyhow::ensure!(
            body_total <= reader.remaining(),
            "bundle bodies exceed group data"
        );

        if channel != DELTA_CHANNEL {
            for len in lengths {
                items.push(AvatarBundleItem {
                    original_channel: channel,
                    payload: reader.get_bytes(len)?.to_vec(),
                });
            }
        } else {
            let mut bodies = lengths
                .iter()
                .map(|len| Vec::with_capacity(*len))
                .collect::<Vec<_>>();
            let max_len = lengths.iter().copied().max().unwrap_or(0);
            for column in 0..max_len {
                for (index, len) in lengths.iter().copied().enumerate() {
                    if column < len {
                        bodies[index].push(reader.get_u8()?);
                    }
                }
            }
            for payload in bodies {
                items.push(AvatarBundleItem {
                    original_channel: channel,
                    payload,
                });
            }
        }
    }
    Ok(items)
}

fn decompress_avatar_bundle_zstd(compressed: &[u8], raw_len: usize) -> Result<Vec<u8>> {
    use zstd_safe::{DCtx, DParameter, FrameFormat};

    let mut context = DCtx::default();
    context
        .set_parameter(DParameter::Format(FrameFormat::Magicless))
        .map_err(|code| {
            anyhow::anyhow!(
                "setting magicless zstd decode format: {}",
                zstd_safe::get_error_name(code)
            )
        })?;
    context
        .load_dictionary(avatar_bundle_dictionary::bytes())
        .map_err(|code| {
            anyhow::anyhow!(
                "loading avatar bundle zstd dictionary for decode: {}",
                zstd_safe::get_error_name(code)
            )
        })?;
    let mut raw = vec![0u8; raw_len];
    let written = context
        .decompress(&mut raw[..], compressed)
        .map_err(|code| {
            anyhow::anyhow!(
                "decompressing avatar bundle zstd: {}",
                zstd_safe::get_error_name(code)
            )
        })?;
    anyhow::ensure!(written == raw_len, "zstd bundle raw length mismatch");
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::DeflateDecoder;
    use std::io::Read;

    fn reference_read_bits(
        source: &[u8],
        base_byte_offset: usize,
        bit_pos: usize,
        bit_count: usize,
    ) -> u64 {
        let absolute = (base_byte_offset << 3) + bit_pos;
        let mut result = 0u64;
        for index in 0..bit_count {
            let bit = absolute + index;
            if source[bit >> 3] & (1 << (bit & 7)) != 0 {
                result |= 1u64 << index;
            }
        }
        result
    }

    fn reference_write_bits(
        destination: &mut [u8],
        base_byte_offset: usize,
        bit_pos: usize,
        value: u64,
        bit_count: usize,
    ) {
        let absolute = (base_byte_offset << 3) + bit_pos;
        for index in 0..bit_count {
            if value & (1u64 << index) != 0 {
                let bit = absolute + index;
                destination[bit >> 3] |= 1 << (bit & 7);
            }
        }
    }

    fn deterministic_bytes(len: usize, seed: u32) -> Vec<u8> {
        let mut state = seed;
        (0..len)
            .map(|_| {
                state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
                (state >> 24) as u8
            })
            .collect()
    }

    fn reference_repack_high_to_lower(high: &[u8], target: BitQuality) -> Vec<u8> {
        let mut output = vec![0; target.payload_len()];
        output[..WRITE_POSITION].copy_from_slice(&high[..WRITE_POSITION]);
        let high_offsets = rotation_field_offsets(BitQuality::High);
        let target_offsets = rotation_field_offsets(target);
        let target_bpc = bpc_table(target);

        for slot in 0..WIRE_BONE_SLOT_COUNT {
            let src_bit = high_offsets[slot];
            let dst_bit = target_offsets[slot];
            match BONE_DOF[slot] {
                3 => {
                    let src_bpc = BPC_HIGH[slot] as usize;
                    let dst_bpc = target_bpc[slot] as usize;
                    let raw = reference_read_bits(high, WRITE_POSITION, src_bit, 2 + 3 * src_bpc);
                    let index = raw & 3;
                    let src_mask = (1u64 << src_bpc) - 1;
                    let qa = (raw >> 2) & src_mask;
                    let qb = (raw >> (2 + src_bpc)) & src_mask;
                    let qc = (raw >> (2 + 2 * src_bpc)) & src_mask;
                    let packed = index
                        | (rescale_quant(qa, src_bpc, dst_bpc) << 2)
                        | (rescale_quant(qb, src_bpc, dst_bpc) << (2 + dst_bpc))
                        | (rescale_quant(qc, src_bpc, dst_bpc) << (2 + 2 * dst_bpc));
                    reference_write_bits(
                        &mut output,
                        WRITE_POSITION,
                        dst_bit,
                        packed,
                        2 + 3 * dst_bpc,
                    );
                }
                2 => {
                    let src_hinge = HINGE_BITS[BitQuality::High.index()] as usize;
                    let src_twist = TWIST_BITS[BitQuality::High.index()] as usize;
                    let dst_hinge = HINGE_BITS[target.index()] as usize;
                    let dst_twist = TWIST_BITS[target.index()] as usize;
                    let hinge = reference_read_bits(high, WRITE_POSITION, src_bit, src_hinge);
                    let twist =
                        reference_read_bits(high, WRITE_POSITION, src_bit + src_hinge, src_twist);
                    let packed = rescale_quant(hinge, src_hinge, dst_hinge)
                        | (rescale_quant(twist, src_twist, dst_twist) << dst_hinge);
                    reference_write_bits(
                        &mut output,
                        WRITE_POSITION,
                        dst_bit,
                        packed,
                        dst_hinge + dst_twist,
                    );
                }
                _ => {
                    let src_bits = SINGLE_BITS[BitQuality::High.index()] as usize;
                    let dst_bits = SINGLE_BITS[target.index()] as usize;
                    let value = reference_read_bits(high, WRITE_POSITION, src_bit, src_bits);
                    reference_write_bits(
                        &mut output,
                        WRITE_POSITION,
                        dst_bit,
                        rescale_quant(value, src_bits, dst_bits),
                        dst_bits,
                    );
                }
            }
        }

        let src_curl = CURL_BITS[BitQuality::High.index()] as usize;
        let src_splay = SPLAY_BITS[BitQuality::High.index()] as usize;
        let dst_curl = CURL_BITS[target.index()] as usize;
        let dst_splay = SPLAY_BITS[target.index()] as usize;
        for finger in 0..FINGER_CHANNEL_COUNT {
            let field = WIRE_BONE_SLOT_COUNT + finger;
            let src_bit = high_offsets[field];
            let curl = reference_read_bits(high, WRITE_POSITION, src_bit, src_curl);
            let splay = reference_read_bits(high, WRITE_POSITION, src_bit + src_curl, src_splay);
            let packed = rescale_quant(curl, src_curl, dst_curl)
                | (rescale_quant(splay, src_splay, dst_splay) << dst_curl);
            reference_write_bits(
                &mut output,
                WRITE_POSITION,
                target_offsets[field],
                packed,
                dst_curl + dst_splay,
            );
        }

        let src_tail = WRITE_POSITION + BitQuality::High.rotation_len();
        let dst_tail = WRITE_POSITION + target.rotation_len();
        output[dst_tail..dst_tail + TAIL_BYTES]
            .copy_from_slice(&high[src_tail..src_tail + TAIL_BYTES]);
        output
    }

    #[test]
    fn word_bit_accessors_match_reference_for_supported_widths_and_offsets() {
        let seeds = [0, 0x1234_5678, 0xdead_beef, u32::MAX];
        for len in [6, 8, 19, BitQuality::High.payload_len()] {
            let patterns = [
                vec![0; len],
                vec![u8::MAX; len],
                (0..len)
                    .map(|i| if i % 2 == 0 { 0x55 } else { 0xaa })
                    .collect(),
                deterministic_bytes(len, seeds[len % seeds.len()]),
            ];
            let max_bit = len * 8;
            for width in 1..=38 {
                for bit_pos in 0..=max_bit - width {
                    for base_byte_offset in [0, if len > 9 { 9 } else { 0 }] {
                        if base_byte_offset * 8 + bit_pos + width > max_bit {
                            continue;
                        }
                        for source in &patterns {
                            assert_eq!(
                                read_bits(source, base_byte_offset, bit_pos, width),
                                reference_read_bits(source, base_byte_offset, bit_pos, width),
                                "read offset={bit_pos} base={base_byte_offset} width={width} len={len}",
                            );
                        }

                        let width_mask = (1u64 << width) - 1;
                        for value in [0, width_mask, u64::MAX, 0xaaaa_aaaa_aaaa_aaaa & width_mask] {
                            for initial in &patterns {
                                let mut actual = initial.clone();
                                let mut expected = initial.clone();
                                write_bits(&mut actual, base_byte_offset, bit_pos, value, width);
                                reference_write_bits(
                                    &mut expected,
                                    base_byte_offset,
                                    bit_pos,
                                    value,
                                    width,
                                );
                                assert_eq!(actual, expected);
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn repack_high_to_lower_matches_bit_loop_for_extreme_and_random_payloads() {
        let len = BitQuality::High.payload_len();
        let patterns = [
            vec![0; len],
            vec![u8::MAX; len],
            (0..len)
                .map(|i| if i % 2 == 0 { 0x55 } else { 0xaa })
                .collect(),
            deterministic_bytes(len, 0x1234_5678),
            deterministic_bytes(len, 0xdead_beef),
        ];
        for target in [BitQuality::Medium, BitQuality::Low, BitQuality::VeryLow] {
            for high in &patterns {
                assert_eq!(
                    repack_high_to_lower(high, target).unwrap(),
                    reference_repack_high_to_lower(high, target),
                    "target={target:?}",
                );
            }
        }
    }

    #[test]
    fn word_bit_writes_match_reference_when_packed_ranges_overlap() {
        let operations = [
            (3, 0x002a_5a35_5aa5_u64, 38),
            (32, 0xdead_beef, 32),
            (61, 0x7fff_ffff, 31),
        ];
        for initial in [
            vec![0; BitQuality::High.payload_len()],
            vec![u8::MAX; BitQuality::High.payload_len()],
            deterministic_bytes(BitQuality::High.payload_len(), 0x1357_9bdf),
        ] {
            let mut actual = initial.clone();
            let mut expected = initial;
            for (offset, value, width) in operations {
                write_bits(&mut actual, 0, offset, value, width);
                reference_write_bits(&mut expected, 0, offset, value, width);
            }
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn word_bit_accessors_fall_back_when_an_unaligned_window_exceeds_u64() {
        let source = deterministic_bytes(16, 0x2468_ace0);
        for (bit_pos, width) in [(0, 64), (1, 64), (7, 58)] {
            assert_eq!(
                read_bits(&source, 0, bit_pos, width),
                reference_read_bits(&source, 0, bit_pos, width),
            );
            let mut actual = source.clone();
            let mut expected = source.clone();
            let value = 0xdead_beef_cafe_babe;
            write_bits(&mut actual, 0, bit_pos, value, width);
            reference_write_bits(&mut expected, 0, bit_pos, value, width);
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn zero_width_bit_accessors_preserve_end_of_buffer_behavior() {
        let source = deterministic_bytes(4, 0x1020_3040);
        assert_eq!(read_bits(&source, 0, source.len() * 8, 0), 0);
        assert_eq!(read_bits(&source, 0, source.len() * 8 - 1, 0), 0);

        let mut actual = source.clone();
        write_bits(&mut actual, 0, source.len() * 8, u64::MAX, 0);
        assert_eq!(actual, source);
        write_bits(&mut actual, 0, source.len() * 8 - 1, u64::MAX, 0);
        assert_eq!(actual, source);
    }

    #[test]
    fn repack_input_and_output_errors_are_unchanged() {
        let high = vec![0; BitQuality::High.payload_len()];
        let mut too_small = vec![0; BitQuality::Low.payload_len() - 1];
        assert!(repack_high_to_lower(&high, BitQuality::High).is_err());
        assert!(repack_high_to_lower_into(
            &high[..high.len() - 1],
            BitQuality::Low,
            &mut too_small
        )
        .is_err());
        let short_output_len = too_small.len() - 1;
        assert!(repack_high_to_lower_into(
            &high,
            BitQuality::Low,
            &mut too_small[..short_output_len],
        )
        .is_err());
    }

    #[test]
    fn quality_sizes_match_basis_v54_constants() {
        assert_eq!(BitQuality::VeryLow.payload_len(), 74);
        assert_eq!(BitQuality::Low.payload_len(), 83);
        assert_eq!(BitQuality::Medium.payload_len(), 97);
        assert_eq!(BitQuality::High.payload_len(), 159);
        assert_eq!(WRITE_POSITION, 9);
        assert_eq!(WRITE_HIPS_DELTA, 5);
        assert_eq!(TAIL_BYTES, 21);
    }

    #[test]
    fn scale_posit16_matches_current_basis_neutral_value() {
        assert_eq!(compress_scale(0.0), 0);
        assert_eq!(compress_scale(1.0), 0x4000);
        assert_eq!(decompress_scale(0x4000), 1.0);

        for scale in [0.125f32, 0.5, 1.0, 2.0, 8.0, 64.0] {
            let decoded = decompress_scale(compress_scale(scale));
            let relative_error = ((decoded - scale) / scale).abs();
            assert!(relative_error < 0.001, "scale={scale} decoded={decoded}");
        }
    }

    #[test]
    fn neutral_rotation_region_encodes_midpoints_not_zero_bits() {
        let mut payload = vec![0u8; BitQuality::High.payload_len()];
        write_neutral_rotation_region(&mut payload, BitQuality::High).unwrap();

        let offsets = rotation_field_offsets(BitQuality::High);
        let rot_base = WRITE_POSITION;

        // 3-DOF identity: drop W (index 3), with all stored XYZ components at the signed midpoint.
        let bits = BPC_HIGH[0] as usize;
        let packed = read_bits(&payload, rot_base, offsets[0], 2 + 3 * bits);
        let mask = (1u64 << bits) - 1;
        assert_eq!(packed & 3, 3);
        assert_eq!((packed >> 2) & mask, 1u64 << (bits - 1));
        assert_eq!((packed >> (2 + bits)) & mask, 1u64 << (bits - 1));
        assert_eq!((packed >> (2 + 2 * bits)) & mask, 1u64 << (bits - 1));

        // Restricted and finger fields also encode zero at their signed midpoint, never as all-zero bits.
        let hinge_bits = HINGE_BITS[BitQuality::High.index()] as usize;
        let twist_bits = TWIST_BITS[BitQuality::High.index()] as usize;
        let restricted = read_bits(&payload, rot_base, offsets[9], hinge_bits + twist_bits);
        assert_eq!(
            restricted & ((1u64 << hinge_bits) - 1),
            1u64 << (hinge_bits - 1)
        );
        assert_eq!(restricted >> hinge_bits, 1u64 << (twist_bits - 1));

        let curl_bits = CURL_BITS[BitQuality::High.index()] as usize;
        let splay_bits = SPLAY_BITS[BitQuality::High.index()] as usize;
        let finger = read_bits(
            &payload,
            rot_base,
            offsets[WIRE_BONE_SLOT_COUNT],
            curl_bits + splay_bits,
        );
        assert_eq!(finger & ((1u64 << curl_bits) - 1), 1u64 << (curl_bits - 1));
        assert_eq!(finger >> curl_bits, 1u64 << (splay_bits - 1));
    }

    #[test]
    fn avatar_network_load_uses_raw_len_strings_inside_deflate() {
        let encoded = encode_avatar_network_load("http://localhost/avatar", "pw").unwrap();
        let mut decoder = DeflateDecoder::new(encoded.as_slice());
        let mut raw = Vec::new();
        decoder.read_to_end(&mut raw).unwrap();
        let mut reader = NetReader::new(&raw);
        assert_eq!(
            reader.get_raw_len_string().unwrap(),
            "http://localhost/avatar"
        );
        assert_eq!(reader.get_raw_len_string().unwrap(), "pw");
    }

    #[test]
    fn avatar_bundle_round_trips() {
        let items = vec![
            AvatarBundleItem {
                original_channel: 12,
                payload: vec![1, 2, 3],
            },
            AvatarBundleItem {
                original_channel: 47,
                payload: vec![4, 5],
            },
        ];
        let encoded = encode_avatar_bundle(&items).unwrap();
        let decoded = decode_avatar_bundle(&encoded).unwrap();
        assert_eq!(decoded, items);
    }

    #[test]
    fn avatar_bundle_zstd_dictionary_round_trips_current_wire_flags() {
        let payload_a = vec![7u8; 159];
        let payload_b = vec![9u8; 159];
        let slices = [
            AvatarBundleSlice {
                original_channel: 12,
                payload: &payload_a,
                interval_patch: None,
            },
            AvatarBundleSlice {
                original_channel: 12,
                payload: &payload_b,
                interval_patch: None,
            },
        ];
        let encoded = try_encode_avatar_bundle_slices_with_compression(
            &slices,
            AvatarBundleCompression::ZstdDictionary { level: -2 },
        )
        .unwrap();
        assert_eq!(
            encoded.bytes[0] & BUNDLE_CODEC_MASK,
            BUNDLE_CODEC_ZSTD_DICTIONARY
        );
        assert_eq!(
            encoded.bytes[0] >> BUNDLE_DICTIONARY_SHIFT,
            avatar_bundle_dictionary::GENERATION
        );
        let decoded = decode_avatar_bundle(&encoded.bytes).unwrap();
        assert_eq!(decoded[0].payload, payload_a);
        assert_eq!(decoded[1].payload, payload_b);
    }

    #[test]
    fn avatar_bundle_zstd_cached_context_matches_fresh_frames_across_levels() {
        let payloads = [
            vec![7u8; 159],
            (0..251).map(|index| (index * 37) as u8).collect(),
            vec![0x5a; 4096],
        ];
        let mut raw = NetWriter::new();
        raw.put_u8(12);
        raw.put_u8(payloads.len() as u8);
        for payload in &payloads {
            raw.put_u16(payload.len() as u16);
        }
        for payload in &payloads {
            raw.put_bytes(payload);
        }
        let raw = raw.into_vec();

        // Reusing the context must produce independent frames identical to the former fresh
        // context path, even after changing levels and returning to the previous level.
        for level in [-2, 3, -2] {
            let cached = compress_avatar_bundle_zstd(&raw, level).unwrap();
            let fresh = compress_avatar_bundle_zstd_fresh(&raw, level).unwrap();
            assert_eq!(cached, fresh, "compressed frame differs at level {level}");

            let mut frame = Vec::with_capacity(cached.len() + 3);
            frame.push(
                BUNDLE_CODEC_ZSTD_DICTIONARY
                    | (avatar_bundle_dictionary::GENERATION << BUNDLE_DICTIONARY_SHIFT),
            );
            frame.extend_from_slice(&(raw.len() as u16).to_le_bytes());
            frame.extend_from_slice(&cached);
            let decoded = decode_avatar_bundle(&frame).unwrap();
            assert_eq!(decoded.len(), payloads.len());
            for (item, expected) in decoded.iter().zip(&payloads) {
                assert_eq!(&item.payload, expected);
            }
        }
    }

    #[test]
    fn avatar_bundle_zstd_cached_contexts_are_thread_local_and_concurrent() {
        std::thread::scope(|scope| {
            for worker in 0..8 {
                scope.spawn(move || {
                    for iteration in 0..40 {
                        let level = if iteration % 2 == 0 { -2 } else { 1 };
                        let payload = (0..(128 + worker * 17 + iteration * 3))
                            .map(|index| (index * 31 + worker) as u8)
                            .collect::<Vec<_>>();
                        let items = [AvatarBundleItem {
                            original_channel: 12,
                            payload: payload.clone(),
                        }];
                        let slices = [AvatarBundleSlice {
                            original_channel: 12,
                            payload: &payload,
                            interval_patch: None,
                        }];
                        let encoded = try_encode_avatar_bundle_slices_with_compression(
                            &slices,
                            AvatarBundleCompression::ZstdDictionary { level },
                        )
                        .unwrap();
                        assert_eq!(decode_avatar_bundle(&encoded.bytes).unwrap(), items);
                    }
                });
            }
        });
    }

    #[test]
    fn v54_repack_sizes_and_shared_fields_match_current_layout() {
        let mut high = (0..BitQuality::High.payload_len())
            .map(|i| i as u8)
            .collect::<Vec<_>>();
        // Keep rotation bytes deterministic but valid as arbitrary bit fields; the repacker is a
        // pure quantized-field transform and does not require semantic quaternion validity.
        for target in [BitQuality::Medium, BitQuality::Low, BitQuality::VeryLow] {
            let repacked = repack_high_to_lower(&high, target).unwrap();
            assert_eq!(repacked.len(), target.payload_len());
            assert_eq!(&repacked[..WRITE_POSITION], &high[..WRITE_POSITION]);
            let high_tail = WRITE_POSITION + BitQuality::High.rotation_len();
            let lower_tail = WRITE_POSITION + target.rotation_len();
            assert_eq!(
                &repacked[lower_tail..lower_tail + TAIL_BYTES],
                &high[high_tail..high_tail + TAIL_BYTES]
            );
        }
        high.fill(0xff);
        assert!(repack_high_to_lower(&high, BitQuality::High).is_err());
    }

    #[test]
    fn int24_position_decodes_millimeters() {
        let payload = [0xe8, 0x03, 0x00, 0x18, 0xfc, 0xff, 0x00, 0x00, 0x00];
        assert_eq!(read_position(&payload), Some([1.0, -1.0, 0.0]));
    }
}
