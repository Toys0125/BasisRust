//! BasisVR server-storage channel 32 wire format.
//! These length prefixes are raw u16 byte lengths, not LiteNetLib strings.
use crate::{NetReader, NetWriter};
use unicode_general_category::{get_general_category, GeneralCategory};

pub const MAX_NAME_BYTES: usize = 64;
pub const MAX_VALUE_BYTES: usize = u16::MAX as usize;
pub const DEFAULT_BYTES_PER_PLAYER: usize = 16_384;
pub const MAX_BYTES_PER_PLAYER: usize = 1_048_576;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageOp {
    Get = 0,
    Set = 1,
    Delete = 2,
    Keys = 3,
    Usage = 4,
    Clear = 5,
}

impl TryFrom<u8> for StorageOp {
    type Error = ();
    fn try_from(n: u8) -> Result<Self, Self::Error> {
        Ok(match n {
            0 => Self::Get,
            1 => Self::Set,
            2 => Self::Delete,
            3 => Self::Keys,
            4 => Self::Usage,
            5 => Self::Clear,
            _ => return Err(()),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum StorageStatus {
    Ok = 0,
    NotFound = 1,
    Denied = 2,
    Full = 3,
    RateLimited = 4,
    Invalid = 5,
    Unavailable = 6,
    Busy = 7,
    TimedOut = 8,
    NotConnected = 9,
    NotAllowed = 10,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageRequest {
    pub op: StorageOp,
    pub id: u16,
    pub scope: String,
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StorageReply {
    pub op: StorageOp,
    pub id: u16,
    pub status: StorageStatus,
    pub used: i32,
    pub limit: i32,
    pub value: Vec<u8>,
    pub keys: Vec<String>,
}

pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_NAME_BYTES
        && name.trim() == name
        && !name.chars().any(|c| {
            matches!(
                get_general_category(c),
                GeneralCategory::Control
                    | GeneralCategory::Format
                    | GeneralCategory::LineSeparator
                    | GeneralCategory::ParagraphSeparator
            )
        })
}

fn read_name(reader: &mut NetReader<'_>) -> Option<String> {
    let size = reader.get_u16().ok()? as usize;
    if size == 0 || size > MAX_NAME_BYTES {
        return None;
    }
    let name = std::str::from_utf8(reader.get_bytes(size).ok()?)
        .ok()?
        .to_string();
    valid_name(&name).then_some(name)
}

/// Recover a correlation header even when the request body is malformed.
/// Unknown operations, truncated headers and zero IDs cannot be answered.
pub fn read_request_header(data: &[u8]) -> Option<(StorageOp, u16)> {
    let mut reader = NetReader::new(data);
    let op = StorageOp::try_from(reader.get_u8().ok()?).ok()?;
    let id = reader.get_u16().ok()?;
    (id != 0).then_some((op, id))
}

pub fn read_request(data: &[u8]) -> Option<StorageRequest> {
    let mut reader = NetReader::new(data);
    let op = StorageOp::try_from(reader.get_u8().ok()?).ok()?;
    let id = reader.get_u16().ok()?;
    if id == 0 {
        return None;
    }
    let scope = read_name(&mut reader)?;
    let key = if matches!(op, StorageOp::Get | StorageOp::Set | StorageOp::Delete) {
        read_name(&mut reader)?
    } else {
        String::new()
    };
    let value = if op == StorageOp::Set {
        let size = reader.get_u16().ok()? as usize;
        reader.get_bytes(size).ok()?.to_vec()
    } else {
        Vec::new()
    };
    (reader.remaining() == 0).then_some(StorageRequest {
        op,
        id,
        scope,
        key,
        value,
    })
}

pub fn write_reply(reply: &StorageReply) -> Vec<u8> {
    let mut writer = NetWriter::new();
    writer.put_u8(reply.op as u8);
    writer.put_u16(reply.id);
    writer.put_u8(reply.status as u8);
    writer.put_i32(reply.used);
    writer.put_i32(reply.limit);
    if reply.status == StorageStatus::Ok {
        match reply.op {
            StorageOp::Get => {
                writer
                    .put_bytes_with_length(&reply.value)
                    .expect("storage value <= 65535");
            }
            StorageOp::Keys => {
                writer.put_u16(reply.keys.len() as u16);
                for key in &reply.keys {
                    writer
                        .put_bytes_with_length(key.as_bytes())
                        .expect("validated storage key");
                }
            }
            _ => {}
        }
    }
    writer.into_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_invisible_or_separator_storage_names() {
        for name in [
            "a\u{202e}b",
            "a\u{2060}b",
            "a\u{2028}b",
            "a\u{2029}b",
            " score",
        ] {
            assert!(!valid_name(name), "{name:?}");
        }
        assert!(valid_name("hello world"));
    }
    #[test]
    fn reads_exact_little_endian_storage_request() {
        let mut writer = NetWriter::new();
        writer.put_u8(1);
        writer.put_u16(0x1234);
        writer.put_bytes_with_length(b"store:demo").unwrap();
        writer.put_bytes_with_length(b"score").unwrap();
        writer.put_bytes_with_length(&[0, 1, 255]).unwrap();
        let req = read_request(writer.as_slice()).unwrap();
        assert_eq!(req.op, StorageOp::Set);
        assert_eq!(req.id, 0x1234);
        assert_eq!(req.value, [0, 1, 255]);
        let mut corrupt = writer.into_vec();
        corrupt.push(0);
        assert!(read_request(&corrupt).is_none());
    }
    #[test]
    fn malformed_bodies_preserve_only_answerable_headers() {
        for payload in [
            &[1, 7, 0][..],
            &[1, 7, 0, 0, 0][..],
            &[4, 7, 0, 1, 0, 255][..],
        ] {
            assert!(read_request(payload).is_none());
            assert_eq!(read_request_header(payload).unwrap().1, 7);
        }
        for payload in [
            &[][..],
            &[1][..],
            &[1, 7][..],
            &[99, 7, 0][..],
            &[1, 0, 0][..],
        ] {
            assert!(read_request_header(payload).is_none());
        }
    }

    #[test]
    fn replies_have_basisvr_header_and_get_value() {
        let bytes = write_reply(&StorageReply {
            op: StorageOp::Get,
            id: 7,
            status: StorageStatus::Ok,
            used: 9,
            limit: 16384,
            value: vec![4, 5],
            keys: Vec::new(),
        });
        assert_eq!(bytes, [0, 7, 0, 0, 9, 0, 0, 0, 0, 64, 0, 0, 2, 0, 4, 5]);
    }
}
