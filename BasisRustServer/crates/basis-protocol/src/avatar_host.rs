//! BasisVR v57 avatar-hosting plugin wire contract.
use crate::io::{NetReader, NetWriter, Result as ReadResult};

pub const MESSAGE_ID: u16 = 64;
pub const MESSAGE_NAME: &str = "basis.avatarhost";
pub const CHUNK_BYTES: usize = 16 * 1024;
pub const MAX_FILE_BYTES: i64 = 16 * 1024 * 1024 * 1024;
pub const MAX_OFFERS_PER_OWNER: usize = 4;
pub const MAX_REQUESTS_PER_REQUESTER: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AvatarId(pub [u8; 16]);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AvatarRange {
    pub avatar_id: AvatarId,
    pub start_offset: i64,
    pub length_bytes: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Offer = 0,
    Withdraw = 1,
    Request = 2,
    OwnerChunk = 3,
    OwnerFailed = 4,
    OfferResult = 10,
    Fetch = 11,
    Chunk = 12,
    Failed = 13,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AvatarHostFailure {
    None = 0,
    Disabled = 1,
    TooLarge = 2,
    TooManyOffers = 3,
    UnknownId = 4,
    BadRange = 5,
    Busy = 6,
    OwnerFailed = 7,
    OwnerLeft = 8,
}
impl TryFrom<u8> for AvatarHostFailure {
    type Error = u8;
    fn try_from(value: u8) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            0 => Self::None,
            1 => Self::Disabled,
            2 => Self::TooLarge,
            3 => Self::TooManyOffers,
            4 => Self::UnknownId,
            5 => Self::BadRange,
            6 => Self::Busy,
            7 => Self::OwnerFailed,
            8 => Self::OwnerLeft,
            _ => return Err(value),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientMessage<'a> {
    Offer {
        avatar_id: AvatarId,
        length_bytes: i64,
    },
    Withdraw {
        avatar_id: AvatarId,
    },
    Request {
        request_id: u32,
        range: AvatarRange,
    },
    OwnerChunk {
        fetch_id: u32,
        offset: i32,
        data: &'a [u8],
    },
    OwnerFailed {
        fetch_id: u32,
    },
}
fn read_id(reader: &mut NetReader<'_>) -> ReadResult<AvatarId> {
    Ok(AvatarId(
        reader.get_bytes(16)?.try_into().expect("fixed id"),
    ))
}
impl<'a> ClientMessage<'a> {
    pub fn read(reader: &mut NetReader<'a>) -> ReadResult<Option<Self>> {
        Ok(Some(match reader.get_u8()? {
            op if op == Opcode::Offer as u8 => Self::Offer {
                avatar_id: read_id(reader)?,
                length_bytes: reader.get_i64()?,
            },
            op if op == Opcode::Withdraw as u8 => Self::Withdraw {
                avatar_id: read_id(reader)?,
            },
            op if op == Opcode::Request as u8 => Self::Request {
                request_id: reader.get_u32()?,
                range: AvatarRange {
                    avatar_id: read_id(reader)?,
                    start_offset: reader.get_i64()?,
                    length_bytes: reader.get_i32()?.max(0) as usize,
                },
            },
            op if op == Opcode::OwnerChunk as u8 => {
                let fetch_id = reader.get_u32()?;
                let offset = reader.get_i32()?;
                let count = reader.get_i32()?;
                // As upstream, malformed chunk lengths fail the identified fetch.
                if count <= 0 || count as usize > CHUNK_BYTES || count as usize > reader.remaining()
                {
                    Self::OwnerFailed { fetch_id }
                } else {
                    Self::OwnerChunk {
                        fetch_id,
                        offset,
                        data: reader.get_bytes(count as usize)?,
                    }
                }
            }
            op if op == Opcode::OwnerFailed as u8 => Self::OwnerFailed {
                fetch_id: reader.get_u32()?,
            },
            _ => return Ok(None),
        }))
    }
    pub fn write(&self, writer: &mut NetWriter) {
        match self {
            Self::Offer {
                avatar_id,
                length_bytes,
            } => {
                writer.put_u8(Opcode::Offer as u8);
                writer.put_bytes(&avatar_id.0);
                writer.put_i64(*length_bytes);
            }
            Self::Withdraw { avatar_id } => {
                writer.put_u8(Opcode::Withdraw as u8);
                writer.put_bytes(&avatar_id.0);
            }
            Self::Request { request_id, range } => {
                writer.put_u8(Opcode::Request as u8);
                writer.put_u32(*request_id);
                writer.put_bytes(&range.avatar_id.0);
                writer.put_i64(range.start_offset);
                writer.put_i32(range.length_bytes as i32);
            }
            Self::OwnerChunk {
                fetch_id,
                offset,
                data,
            } => {
                writer.put_u8(Opcode::OwnerChunk as u8);
                writer.put_u32(*fetch_id);
                writer.put_i32(*offset);
                writer.put_i32(data.len() as i32);
                writer.put_bytes(data);
            }
            Self::OwnerFailed { fetch_id } => {
                writer.put_u8(Opcode::OwnerFailed as u8);
                writer.put_u32(*fetch_id);
            }
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServerControl {
    OfferResult {
        avatar_id: AvatarId,
        accepted: bool,
        reason: AvatarHostFailure,
    },
    Fetch {
        fetch_id: u32,
        range: AvatarRange,
    },
    Failed {
        request_id: u32,
        reason: AvatarHostFailure,
    },
}
impl ServerControl {
    pub fn write(&self, writer: &mut NetWriter) {
        match self {
            Self::OfferResult {
                avatar_id,
                accepted,
                reason,
            } => {
                writer.put_u8(Opcode::OfferResult as u8);
                writer.put_bytes(&avatar_id.0);
                writer.put_bool(*accepted);
                writer.put_u8(*reason as u8);
            }
            Self::Fetch { fetch_id, range } => {
                writer.put_u8(Opcode::Fetch as u8);
                writer.put_u32(*fetch_id);
                writer.put_bytes(&range.avatar_id.0);
                writer.put_i64(range.start_offset);
                writer.put_i32(range.length_bytes as i32);
            }
            Self::Failed { request_id, reason } => {
                writer.put_u8(Opcode::Failed as u8);
                writer.put_u32(*request_id);
                writer.put_u8(*reason as u8);
            }
        }
    }
    pub fn packet(&self) -> Vec<u8> {
        let mut writer = NetWriter::new();
        writer.put_u16(MESSAGE_ID);
        self.write(&mut writer);
        writer.into_vec()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerMessage<'a> {
    Control(ServerControl),
    Chunk {
        request_id: u32,
        offset: i32,
        data: &'a [u8],
    },
}
impl<'a> ServerMessage<'a> {
    pub fn read(reader: &mut NetReader<'a>) -> ReadResult<Option<Self>> {
        Ok(Some(match reader.get_u8()? {
            op if op == Opcode::OfferResult as u8 => {
                let avatar_id = read_id(reader)?;
                let accepted = reader.get_bool()?;
                let Ok(reason) = AvatarHostFailure::try_from(reader.get_u8()?) else {
                    return Ok(None);
                };
                Self::Control(ServerControl::OfferResult {
                    avatar_id,
                    accepted,
                    reason,
                })
            }
            op if op == Opcode::Fetch as u8 => {
                let fetch_id = reader.get_u32()?;
                let range = AvatarRange {
                    avatar_id: read_id(reader)?,
                    start_offset: reader.get_i64()?,
                    length_bytes: reader.get_i32()?.max(0) as usize,
                };
                Self::Control(ServerControl::Fetch { fetch_id, range })
            }
            op if op == Opcode::Failed as u8 => {
                let request_id = reader.get_u32()?;
                let Ok(reason) = AvatarHostFailure::try_from(reader.get_u8()?) else {
                    return Ok(None);
                };
                Self::Control(ServerControl::Failed { request_id, reason })
            }
            op if op == Opcode::Chunk as u8 => {
                let request_id = reader.get_u32()?;
                let offset = reader.get_i32()?;
                let length = reader.get_i32()?;
                if length <= 0 || length as usize > CHUNK_BYTES {
                    return Ok(None);
                }
                Self::Chunk {
                    request_id,
                    offset,
                    data: reader.get_bytes(length as usize)?,
                }
            }
            _ => return Ok(None),
        }))
    }
}

pub fn chunk_packet(request_id: u32, offset: usize, data: &[u8]) -> Vec<u8> {
    let mut writer = NetWriter::with_capacity(data.len() + 15);
    writer.put_u16(MESSAGE_ID);
    writer.put_u8(Opcode::Chunk as u8);
    writer.put_u32(request_id);
    writer.put_i32(offset as i32);
    writer.put_i32(data.len() as i32);
    writer.put_bytes(data);
    writer.into_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn avatar_host_packets_match_v57_vectors() {
        let range = AvatarRange {
            avatar_id: AvatarId([7; 16]),
            start_offset: 10,
            length_bytes: 4,
        };
        let mut expected = vec![64, 0, 11, 42, 0, 0, 0];
        expected.extend([7; 16]);
        expected.extend(10i64.to_le_bytes());
        expected.extend(4i32.to_le_bytes());
        assert_eq!(
            ServerControl::Fetch {
                fetch_id: 42,
                range
            }
            .packet(),
            expected
        );
        assert_eq!(
            ServerControl::Failed {
                request_id: 42,
                reason: AvatarHostFailure::Busy
            }
            .packet(),
            [64, 0, 13, 42, 0, 0, 0, 6]
        );
        assert_eq!(
            chunk_packet(42, 8, b"BEE!"),
            [64, 0, 12, 42, 0, 0, 0, 8, 0, 0, 0, 4, 0, 0, 0, b'B', b'E', b'E', b'!']
        );
        let msg = ClientMessage::Request {
            request_id: 42,
            range,
        };
        let mut writer = NetWriter::new();
        msg.write(&mut writer);
        assert_eq!(
            ClientMessage::read(&mut NetReader::new(writer.as_slice())).unwrap(),
            Some(msg)
        );
    }
    #[test]
    fn malformed_owner_chunk_fails_only_identified_fetch() {
        let mut writer = NetWriter::new();
        writer.put_u8(Opcode::OwnerChunk as u8);
        writer.put_u32(7);
        writer.put_i32(0);
        writer.put_i32((CHUNK_BYTES + 1) as i32);
        assert_eq!(
            ClientMessage::read(&mut NetReader::new(writer.as_slice())).unwrap(),
            Some(ClientMessage::OwnerFailed { fetch_id: 7 })
        );
    }
}
