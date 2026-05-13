use crate::io::{NetReader, NetWriter, Result as ReadResult};

pub const SERVER_INFO_QUERY_MAGIC: u32 = 0xBA51_5101;
pub const SERVER_INFO_RESPONSE_MAGIC: u32 = 0xBA51_5102;
pub const SERVER_INFO_PROTOCOL_VERSION: u16 = 1;
pub const SERVER_INFO_NAME_MAX_LENGTH: usize = 64;
pub const SERVER_INFO_MOTD_MAX_LENGTH: usize = 256;
pub const SERVER_INFO_MIN_REQUEST_BYTES: usize = 384;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerInfoResponse {
    pub name: String,
    pub motd: String,
    pub online: u16,
    pub max: u16,
    pub version: u16,
}

impl ServerInfoResponse {
    pub fn serialize(&self) -> Vec<u8> {
        let mut writer = NetWriter::new();
        writer.put_u32(SERVER_INFO_RESPONSE_MAGIC);
        writer.put_u16(SERVER_INFO_PROTOCOL_VERSION);
        writer.put_u16(self.version);
        writer.put_u16(self.online);
        writer.put_u16(self.max);
        writer.put_string(trim_to_bytes(&self.name, SERVER_INFO_NAME_MAX_LENGTH));
        writer.put_string(trim_to_bytes(&self.motd, SERVER_INFO_MOTD_MAX_LENGTH));
        writer.into_vec()
    }

    pub fn deserialize(bytes: &[u8]) -> ReadResult<Self> {
        let mut reader = NetReader::new(bytes);
        let _magic = reader.get_u32()?;
        let _protocol = reader.get_u16()?;
        let version = reader.get_u16()?;
        let online = reader.get_u16()?;
        let max = reader.get_u16()?;
        let name = reader.get_string()?;
        let motd = reader.get_string()?;
        Ok(Self {
            name,
            motd,
            online,
            max,
            version,
        })
    }
}

fn trim_to_bytes(value: &str, max: usize) -> &str {
    if value.len() <= max {
        return value;
    }
    let mut end = max;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}
