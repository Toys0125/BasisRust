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
    pub nonce: u16,
}

impl ServerInfoResponse {
    pub fn serialize(&self) -> Vec<u8> {
        let mut writer = NetWriter::new();
        writer.put_u32(SERVER_INFO_RESPONSE_MAGIC);
        writer.put_u16(SERVER_INFO_PROTOCOL_VERSION);
        writer.put_u16(self.nonce);
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
        let nonce = reader.get_u16()?;
        let online = reader.get_u16()?;
        let max = reader.get_u16()?;
        let name = reader.get_string()?;
        let motd = reader.get_string()?;
        Ok(Self {
            name,
            motd,
            online,
            max,
            nonce,
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

pub fn parse_server_info_query_nonce(bytes: &[u8]) -> Option<u16> {
    if bytes.len() < 8 {
        return None;
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
    let protocol = u16::from_le_bytes(bytes[4..6].try_into().ok()?);
    if magic != SERVER_INFO_QUERY_MAGIC || protocol != SERVER_INFO_PROTOCOL_VERSION {
        return None;
    }
    Some(u16::from_le_bytes(bytes[6..8].try_into().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_info_response_echoes_nonce_layout() {
        let response = ServerInfoResponse {
            name: "Basis".to_string(),
            motd: "Hello".to_string(),
            online: 7,
            max: 42,
            nonce: 0xBEEF,
        };
        let bytes = response.serialize();
        assert_eq!(&bytes[0..4], &SERVER_INFO_RESPONSE_MAGIC.to_le_bytes());
        assert_eq!(&bytes[4..6], &SERVER_INFO_PROTOCOL_VERSION.to_le_bytes());
        assert_eq!(&bytes[6..8], &0xBEEFu16.to_le_bytes());
        assert_eq!(&bytes[8..10], &7u16.to_le_bytes());
        assert_eq!(&bytes[10..12], &42u16.to_le_bytes());
        assert_eq!(ServerInfoResponse::deserialize(&bytes).unwrap(), response);
    }

    #[test]
    fn parses_query_nonce() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&SERVER_INFO_QUERY_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&SERVER_INFO_PROTOCOL_VERSION.to_le_bytes());
        bytes.extend_from_slice(&1234u16.to_le_bytes());
        assert_eq!(parse_server_info_query_nonce(&bytes), Some(1234));
    }
}
