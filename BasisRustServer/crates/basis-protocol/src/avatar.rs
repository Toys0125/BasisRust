use anyhow::{Context, Result};
use flate2::{write::DeflateEncoder, Compression};
use lz4_flex::{compress, decompress};
use std::io::Write;

use crate::io::{NetReader, NetWriter};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BitQuality {
    VeryLow = 0,
    Low = 1,
    Medium = 2,
    High = 3,
}

impl BitQuality {
    pub fn payload_len(self) -> usize {
        match self {
            Self::VeryLow => 112,
            Self::Low => 131,
            Self::Medium => 156,
            Self::High => 182,
        }
    }

    pub fn rotation_len(self) -> usize {
        match self {
            Self::VeryLow => 78,
            Self::Low => 97,
            Self::Medium => 122,
            Self::High => 148,
        }
    }
}

pub const FLOAT_SIZE: usize = 4;
pub const USHORT_SIZE: usize = 2;
pub const VECTOR3_SIZE: usize = 12;
pub const MIN_SCALE: f32 = 0.005;
pub const MAX_SCALE: f32 = 150.0;
pub const WRITE_POSITION: usize = 12;
pub const WRITE_SCALE: usize = 2;
pub const WRITE_ROTATION: usize = 7;
pub const WRITE_HIPS_DELTA: usize = 6;
pub const WRITE_HIPS_ROTATION: usize = 7;
pub const TAIL_BYTES: usize = WRITE_SCALE + WRITE_ROTATION + WRITE_HIPS_DELTA + WRITE_HIPS_ROTATION;
pub const SYNC_BONE_COUNT: usize = 51;

pub const BPC_HIGH: [u8; SYNC_BONE_COUNT] = [
    10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 9, 9, 5, 5, 6, 6, 6, 6, 5,
    6, 6, 6, 6, 5, 6, 6, 5, 5, 5, 6, 6, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5,
];
pub const BPC_MEDIUM: [u8; SYNC_BONE_COUNT] = [
    8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 8, 6, 6, 3, 3, 6, 6, 5, 5, 4, 6, 6, 5, 5, 4, 5,
    5, 4, 4, 4, 5, 5, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
];
pub const BPC_LOW: [u8; SYNC_BONE_COUNT] = [
    6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 5, 5, 3, 3, 5, 5, 4, 4, 3, 5, 5, 4, 4, 3, 4,
    4, 3, 3, 3, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,
];
pub const BPC_VERY_LOW: [u8; SYNC_BONE_COUNT] = [
    5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 4, 4, 2, 2, 4, 4, 3, 3, 2, 4, 4, 3, 3, 2, 3,
    3, 2, 2, 2, 3, 3, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,
];

const OFFSETS_HIGH: [usize; SYNC_BONE_COUNT] = bit_offsets(&BPC_HIGH);
const OFFSETS_MEDIUM: [usize; SYNC_BONE_COUNT] = bit_offsets(&BPC_MEDIUM);
const OFFSETS_LOW: [usize; SYNC_BONE_COUNT] = bit_offsets(&BPC_LOW);
const OFFSETS_VERY_LOW: [usize; SYNC_BONE_COUNT] = bit_offsets(&BPC_VERY_LOW);

pub fn encode_avatar_network_load(url: &str, unlock_password: &str) -> Result<Vec<u8>> {
    let mut raw = NetWriter::with_capacity(url.len() + unlock_password.len() + 4);
    raw.put_raw_len_string(url);
    raw.put_raw_len_string(unlock_password);
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::fast());
    encoder.write_all(&raw.into_vec())?;
    Ok(encoder.finish()?)
}

pub fn compress_scale(scale: f32) -> u16 {
    let range = MAX_SCALE - MIN_SCALE;
    (((scale - MIN_SCALE) / range) * u16::MAX as f32).trunc() as u16
}

pub fn read_position(payload: &[u8]) -> Option<[f32; 3]> {
    if payload.len() < WRITE_POSITION {
        return None;
    }
    Some([
        f32::from_le_bytes(payload[0..4].try_into().ok()?),
        f32::from_le_bytes(payload[4..8].try_into().ok()?),
        f32::from_le_bytes(payload[8..12].try_into().ok()?),
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
    anyhow::ensure!(
        high_payload.len() >= BitQuality::High.payload_len(),
        "high payload too small"
    );
    anyhow::ensure!(target != BitQuality::High, "target must be lower than high");
    anyhow::ensure!(
        out.len() >= target.payload_len(),
        "target output buffer too small"
    );

    let target_rot_len = target.rotation_len();
    let out = &mut out[..target.payload_len()];
    out.fill(0);
    out[..WRITE_POSITION].copy_from_slice(&high_payload[..WRITE_POSITION]);

    let high_offsets = &OFFSETS_HIGH;
    let (target_bpc, target_offsets): (&[u8], &[usize; SYNC_BONE_COUNT]) = match target {
        BitQuality::Medium => (&BPC_MEDIUM, &OFFSETS_MEDIUM),
        BitQuality::Low => (&BPC_LOW, &OFFSETS_LOW),
        BitQuality::VeryLow => (&BPC_VERY_LOW, &OFFSETS_VERY_LOW),
        BitQuality::High => unreachable!(),
    };

    let rot_base = WRITE_POSITION;
    for slot in 0..SYNC_BONE_COUNT {
        let src_bpc = BPC_HIGH[slot] as usize;
        let total_src_bits = 2 + 3 * src_bpc;
        let raw = read_bits_u64(high_payload, rot_base, high_offsets[slot], total_src_bits);
        let idx = raw & 3;
        let mask_src = (1u64 << src_bpc) - 1;
        let qa = (raw >> 2) & mask_src;
        let qb = (raw >> (2 + src_bpc)) & mask_src;
        let qc = (raw >> (2 + 2 * src_bpc)) & mask_src;
        let dst_bpc = target_bpc[slot] as usize;
        let packed = idx
            | (rescale_quant(qa, src_bpc, dst_bpc) << 2)
            | (rescale_quant(qb, src_bpc, dst_bpc) << (2 + dst_bpc))
            | (rescale_quant(qc, src_bpc, dst_bpc) << (2 + 2 * dst_bpc));
        write_bits_u64(out, rot_base, target_offsets[slot], packed, 2 + 3 * dst_bpc);
    }

    let src_tail = WRITE_POSITION + BitQuality::High.rotation_len();
    let dst_tail = WRITE_POSITION + target_rot_len;
    out[dst_tail..dst_tail + TAIL_BYTES]
        .copy_from_slice(&high_payload[src_tail..src_tail + TAIL_BYTES]);
    Ok(())
}

const fn bit_offsets<const N: usize>(bpc: &[u8; N]) -> [usize; N] {
    let mut offsets = [0usize; N];
    let mut bit = 0usize;
    let mut index = 0usize;
    while index < N {
        offsets[index] = bit;
        bit += 2 + 3 * (bpc[index] as usize);
        index += 1;
    }
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

fn read_bits_u64(src: &[u8], base_byte_offset: usize, bit_pos: usize, bit_count: usize) -> u64 {
    let mut byte_pos = base_byte_offset + (bit_pos >> 3);
    let mut bit_in_byte = bit_pos & 7;
    let mut result = 0u64;
    let mut out_shift = 0usize;
    let mut bits_left = bit_count;

    while bits_left > 0 {
        let room = 8 - bit_in_byte;
        let take = bits_left.min(room);
        let current = (src[byte_pos] as u64) >> bit_in_byte;
        let mask = (1u64 << take) - 1;
        result |= (current & mask) << out_shift;
        out_shift += take;
        bits_left -= take;
        byte_pos += 1;
        bit_in_byte = 0;
    }
    result
}

fn write_bits_u64(
    dst: &mut [u8],
    base_byte_offset: usize,
    bit_pos: usize,
    value: u64,
    bit_count: usize,
) {
    let mut byte_pos = base_byte_offset + (bit_pos >> 3);
    let mut bit_in_byte = bit_pos & 7;
    let mut value = value;
    let mut bits_left = bit_count;

    while bits_left > 0 {
        let room = 8 - bit_in_byte;
        let take = bits_left.min(room);
        let mask = (1u64 << take) - 1;
        let chunk = (value & mask) as u8;
        dst[byte_pos] |= chunk << bit_in_byte;
        value >>= take;
        bits_left -= take;
        byte_pos += 1;
        bit_in_byte = 0;
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
    anyhow::ensure!(items.len() <= u8::MAX as usize, "too many bundle items");
    let mut raw = NetWriter::new();
    for item in items {
        anyhow::ensure!(
            item.payload.len() <= u16::MAX as usize,
            "bundle item payload too large"
        );
        raw.put_u8(item.original_channel);
        raw.put_u16(item.payload.len() as u16);
        if let Some((offset, value)) = item.interval_patch {
            if offset < item.payload.len() {
                raw.put_bytes(&item.payload[..offset]);
                raw.put_u8(value);
                raw.put_bytes(&item.payload[offset + 1..]);
            } else {
                raw.put_bytes(item.payload);
            }
        } else {
            raw.put_bytes(item.payload);
        }
    }
    let raw = raw.into_vec();
    anyhow::ensure!(
        raw.len() <= u16::MAX as usize,
        "bundle raw payload too large"
    );
    let compressed = compress(&raw);
    let mut out = NetWriter::with_capacity(compressed.len() + 3);
    out.put_u8(items.len() as u8);
    out.put_u16(raw.len() as u16);
    out.put_bytes(&compressed);
    Ok(out.into_vec())
}

pub fn decode_avatar_bundle(bytes: &[u8]) -> Result<Vec<AvatarBundleItem>> {
    let mut reader = NetReader::new(bytes);
    let count = reader.get_u8()? as usize;
    let raw_len = reader.get_u16()? as usize;
    let raw = decompress(reader.remaining_slice(), raw_len).context("decompressing LZ4 bundle")?;
    anyhow::ensure!(raw.len() == raw_len, "bundle raw length mismatch");
    let mut raw_reader = NetReader::new(&raw);
    let mut items = Vec::with_capacity(count);
    for _ in 0..count {
        let original_channel = raw_reader.get_u8()?;
        let len = raw_reader.get_u16()? as usize;
        let payload = raw_reader.get_bytes(len)?.to_vec();
        items.push(AvatarBundleItem {
            original_channel,
            payload,
        });
    }
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::DeflateDecoder;
    use std::io::Read;

    #[test]
    fn quality_sizes_match_basis_constants() {
        assert_eq!(BitQuality::VeryLow.payload_len(), 112);
        assert_eq!(BitQuality::Low.payload_len(), 131);
        assert_eq!(BitQuality::Medium.payload_len(), 156);
        assert_eq!(BitQuality::High.payload_len(), 182);
        assert_eq!(TAIL_BYTES, 22);
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
    fn high_payload_repack_sizes_match_targets() {
        let high = vec![0u8; BitQuality::High.payload_len()];
        assert_eq!(
            repack_high_to_lower(&high, BitQuality::Medium)
                .unwrap()
                .len(),
            BitQuality::Medium.payload_len()
        );
        assert_eq!(
            repack_high_to_lower(&high, BitQuality::Low).unwrap().len(),
            BitQuality::Low.payload_len()
        );
        assert_eq!(
            repack_high_to_lower(&high, BitQuality::VeryLow)
                .unwrap()
                .len(),
            BitQuality::VeryLow.payload_len()
        );
    }

    #[test]
    fn high_payload_repack_into_matches_allocating_api() {
        let high = (0..BitQuality::High.payload_len())
            .map(|value| value as u8)
            .collect::<Vec<_>>();
        for target in [BitQuality::Medium, BitQuality::Low, BitQuality::VeryLow] {
            let allocated = repack_high_to_lower(&high, target).unwrap();
            let mut pooled = vec![255u8; target.payload_len()];
            repack_high_to_lower_into(&high, target, &mut pooled).unwrap();
            assert_eq!(allocated, pooled);
        }
    }
}
