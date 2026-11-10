//! BasisMediaClipShare wire definitions (BasisVR protocol 57).
use crate::io::{NetReadError, NetReader};
type ReadResult<T> = Result<T, NetReadError>;

pub const MANAGER_IDENTIFIER: &str = "BasisMediaClipShare";
pub const ID_BYTES: usize = 16;
pub const CHUNK_BYTES: usize = 16 * 1024;
pub const CHUNK_HEADER_BYTES: usize = 21;
pub const OFFER_FIXED_BYTES: usize = 57;
pub type ClipId = [u8; ID_BYTES];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Offer = 1,
    Chunk = 2,
    Remove = 3,
    Pose = 4,
    Need = 5,
    CacheState = 6,
    CacheOffer = 7,
    CacheRequest = 8,
    CacheCatalog = 9,
}
impl Opcode {
    pub fn from_payload(payload: &[u8]) -> Option<Self> {
        Self::from_byte(*payload.first()?)
    }
    pub fn from_byte(value: u8) -> Option<Self> {
        Some(match value {
            1 => Self::Offer,
            2 => Self::Chunk,
            3 => Self::Remove,
            4 => Self::Pose,
            5 => Self::Need,
            6 => Self::CacheState,
            7 => Self::CacheOffer,
            8 => Self::CacheRequest,
            9 => Self::CacheCatalog,
            _ => return None,
        })
    }
    pub fn is_upload(self) -> bool {
        matches!(self, Self::Offer | Self::Chunk)
    }
    pub fn is_cache_control(self) -> bool {
        matches!(
            self,
            Self::Remove | Self::Need | Self::CacheRequest | Self::CacheCatalog
        )
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum CacheState {
    NotHeld = 0,
    Upload = 1,
    Held = 2,
}

pub fn read_id(reader: &mut NetReader<'_>) -> ReadResult<ClipId> {
    Ok(reader
        .get_bytes(ID_BYTES)?
        .try_into()
        .expect("fixed clip id"))
}
pub fn chunk_header(id: ClipId, index: usize) -> Vec<u8> {
    let mut result = Vec::with_capacity(CHUNK_HEADER_BYTES + CHUNK_BYTES);
    result.push(Opcode::Chunk as u8);
    result.extend(id);
    result.extend((index as i32).to_le_bytes());
    result
}
pub fn state_payload(id: ClipId, state: CacheState) -> Vec<u8> {
    let mut result = Vec::with_capacity(ID_BYTES + 2);
    result.push(Opcode::CacheState as u8);
    result.extend(id);
    result.push(state as u8);
    result
}
pub fn read_chunk_header(payload: &[u8]) -> ReadResult<(ClipId, i32)> {
    let mut reader = NetReader::new(payload);
    let opcode = reader.get_u8()?;
    if opcode != Opcode::Chunk as u8 {
        return Err(NetReadError::Underflow {
            needed: CHUNK_HEADER_BYTES,
            remaining: 0,
        });
    }
    Ok((read_id(&mut reader)?, reader.get_i32()?))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn named_clip_wire_definitions_match_basisvr() {
        assert!(Opcode::Offer.is_upload());
        assert!(!Opcode::Remove.is_upload());
        assert!(Opcode::CacheCatalog.is_cache_control());
        assert_eq!(
            state_payload([4; 16], CacheState::NotHeld),
            [&[6][..], &[4; 16], &[0]].concat()
        );
        let header = chunk_header([7; 16], 42);
        assert_eq!(header.len(), 21);
        assert_eq!(read_chunk_header(&header).unwrap(), ([7; 16], 42));
    }
}
