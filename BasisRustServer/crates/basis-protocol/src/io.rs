use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum NetReadError {
    #[error("reader underflow: need {needed} bytes, have {remaining}")]
    Underflow { needed: usize, remaining: usize },
    #[error("invalid UTF-8 string")]
    Utf8,
}

pub type Result<T> = std::result::Result<T, NetReadError>;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum NetWriteError {
    #[error("length-prefixed payload has {length} bytes, maximum is {max}")]
    LengthOverflow { length: usize, max: usize },
}

pub type WriteResult<T> = std::result::Result<T, NetWriteError>;

#[derive(Debug, Clone, Default)]
pub struct NetWriter {
    data: Vec<u8>,
}

impl NetWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn clear(&mut self) {
        self.data.clear();
    }

    pub fn put_bool(&mut self, value: bool) {
        self.put_u8(u8::from(value));
    }

    pub fn put_u8(&mut self, value: u8) {
        self.data.push(value);
    }

    pub fn put_i8(&mut self, value: i8) {
        self.data.push(value as u8);
    }

    pub fn put_u16(&mut self, value: u16) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    pub fn put_i16(&mut self, value: i16) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    pub fn put_u32(&mut self, value: u32) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    pub fn put_i32(&mut self, value: i32) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    pub fn put_i64(&mut self, value: i64) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    pub fn put_u64(&mut self, value: u64) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    pub fn put_f32(&mut self, value: f32) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    pub fn put_f64(&mut self, value: f64) {
        self.data.extend_from_slice(&value.to_le_bytes());
    }

    pub fn put_bytes(&mut self, value: &[u8]) {
        self.data.extend_from_slice(value);
    }

    /// Writes LiteNetLib's UTF-8 byte length plus one (without a terminator byte).
    /// Empty strings use a zero prefix. An oversized value leaves the writer unchanged.
    pub fn put_string(&mut self, value: &str) -> WriteResult<()> {
        if value.is_empty() {
            self.put_u16(0);
            return Ok(());
        }
        let bytes = value.as_bytes();
        let length = u16::try_from(bytes.len())
            .ok()
            .and_then(|length| length.checked_add(1))
            .ok_or(NetWriteError::LengthOverflow {
                length: bytes.len(),
                max: usize::from(u16::MAX) - 1,
            })?;
        self.put_u16(length);
        self.put_bytes(bytes);
        Ok(())
    }

    /// Writes a UTF-8 byte length and payload, leaving the writer unchanged on overflow.
    pub fn put_raw_len_string(&mut self, value: &str) -> WriteResult<()> {
        self.put_bytes_with_length(value.as_bytes())
    }

    /// Writes a byte length and payload, leaving the writer unchanged on overflow.
    pub fn put_bytes_with_length(&mut self, value: &[u8]) -> WriteResult<()> {
        let length = u16::try_from(value.len()).map_err(|_| NetWriteError::LengthOverflow {
            length: value.len(),
            max: usize::from(u16::MAX),
        })?;
        self.put_u16(length);
        self.put_bytes(value);
        Ok(())
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

#[derive(Debug, Clone, Copy)]
pub struct NetReader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> NetReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    pub fn remaining_slice(&self) -> &'a [u8] {
        &self.data[self.pos..]
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.remaining() < len {
            return Err(NetReadError::Underflow {
                needed: len,
                remaining: self.remaining(),
            });
        }
        let start = self.pos;
        self.pos += len;
        Ok(&self.data[start..start + len])
    }

    pub fn get_bool(&mut self) -> Result<bool> {
        Ok(self.get_u8()? != 0)
    }

    pub fn get_u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    pub fn get_i8(&mut self) -> Result<i8> {
        Ok(self.get_u8()? as i8)
    }

    pub fn get_u16(&mut self) -> Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub fn get_i16(&mut self) -> Result<i16> {
        let bytes = self.take(2)?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub fn get_u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn get_i32(&mut self) -> Result<i32> {
        let bytes = self.take(4)?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn get_i64(&mut self) -> Result<i64> {
        let bytes = self.take(8)?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn get_u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn get_f32(&mut self) -> Result<f32> {
        let bytes = self.take(4)?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub fn get_f64(&mut self) -> Result<f64> {
        let bytes = self.take(8)?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub fn get_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        self.take(len)
    }

    pub fn get_string(&mut self) -> Result<String> {
        let len_plus = self.get_u16()? as usize;
        if len_plus == 0 {
            return Ok(String::new());
        }
        let len = len_plus - 1;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| NetReadError::Utf8)
    }

    pub fn get_raw_len_string(&mut self) -> Result<String> {
        let len = self.get_u16()? as usize;
        let bytes = self.take(len)?;
        std::str::from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| NetReadError::Utf8)
    }

    pub fn get_bytes_with_length(&mut self) -> Result<&'a [u8]> {
        let len = self.get_u16()? as usize;
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn primitives_are_little_endian() {
        let mut writer = NetWriter::new();
        writer.put_u16(0x1234);
        writer.put_i32(0x01020304);
        writer.put_f32(1.0);
        assert_eq!(
            writer.into_vec(),
            vec![0x34, 0x12, 0x04, 0x03, 0x02, 0x01, 0, 0, 0x80, 0x3f]
        );
    }

    #[test]
    fn basis_strings_use_len_plus_one() {
        let mut writer = NetWriter::new();
        writer.put_string("").unwrap();
        writer.put_string("abc").unwrap();
        let bytes = writer.into_vec();
        assert_eq!(&bytes, &[0, 0, 4, 0, b'a', b'b', b'c']);

        let mut reader = NetReader::new(&bytes);
        assert_eq!(reader.get_string().unwrap(), "");
        assert_eq!(reader.get_string().unwrap(), "abc");
    }

    #[test]
    fn length_prefixed_writes_match_pinned_csharp_golden_vectors() {
        // Generated with the verbatim LiteNetLib NetDataWriter/FastBitConverter at
        // BasisVR 81f190b217c11c2b39231e0bc9db330fd4a2803c, using .NET 8.
        // C# null and empty strings/buffers both produce a zero prefix; Rust uses empty slices.
        let mut strings = NetWriter::new();
        for value in ["", "", "abc", "Aé😀\0"] {
            strings.put_string(value).unwrap();
        }
        assert_eq!(
            strings.as_slice(),
            &[
                0, 0, 0, 0, 4, 0, 0x61, 0x62, 0x63, 9, 0, 0x41, 0xc3, 0xa9, 0xf0, 0x9f, 0x98, 0x80,
                0,
            ]
        );

        let mut bytes = NetWriter::new();
        for value in [&[][..], &[][..], &[0, 1, 0xff][..]] {
            bytes.put_bytes_with_length(value).unwrap();
        }
        assert_eq!(bytes.as_slice(), &[0, 0, 0, 0, 3, 0, 0, 1, 0xff]);

        let mut raw = NetWriter::new();
        raw.put_raw_len_string("").unwrap();
        raw.put_raw_len_string("Aé😀\0").unwrap();
        assert_eq!(
            raw.as_slice(),
            &[0, 0, 8, 0, 0x41, 0xc3, 0xa9, 0xf0, 0x9f, 0x98, 0x80, 0,]
        );
    }

    #[test]
    fn string_byte_length_boundary_preserves_writer_on_overflow() {
        let max = usize::from(u16::MAX) - 1;
        // Both ASCII and multi-byte UTF-8 must be measured in bytes, not characters.
        for valid in ["a".repeat(max), "é".repeat(max / 2)] {
            let mut writer = NetWriter::new();
            writer.put_string(&valid).unwrap();
            assert_eq!(&writer.as_slice()[..2], &[0xff, 0xff]);
            assert_eq!(&writer.as_slice()[2..], valid.as_bytes());
            assert_eq!(
                NetReader::new(writer.as_slice()).get_string().unwrap(),
                valid
            );

            let before = writer.as_slice().to_vec();
            let invalid = valid + "a";
            assert_eq!(
                writer.put_string(&invalid),
                Err(NetWriteError::LengthOverflow {
                    length: max + 1,
                    max
                })
            );
            assert_eq!(writer.as_slice(), before);
            writer.put_string("ok").unwrap();
            assert_eq!(&writer.as_slice()[before.len()..], &[3, 0, b'o', b'k']);
        }
    }

    #[test]
    fn raw_string_byte_length_boundary_preserves_writer_on_overflow() {
        let max = usize::from(u16::MAX);
        for valid in ["a".repeat(max), "é".repeat(max / 2) + "a"] {
            let mut writer = NetWriter::new();
            writer.put_raw_len_string(&valid).unwrap();
            assert_eq!(&writer.as_slice()[..2], &[0xff, 0xff]);
            assert_eq!(&writer.as_slice()[2..], valid.as_bytes());
            assert_eq!(
                NetReader::new(writer.as_slice())
                    .get_raw_len_string()
                    .unwrap(),
                valid
            );

            let before = writer.as_slice().to_vec();
            let invalid = valid + "a";
            assert_eq!(
                writer.put_raw_len_string(&invalid),
                Err(NetWriteError::LengthOverflow {
                    length: max + 1,
                    max
                })
            );
            assert_eq!(writer.as_slice(), before);
        }
    }

    #[test]
    fn byte_buffer_length_boundary_preserves_writer_on_overflow() {
        let max = usize::from(u16::MAX);
        let mut value = vec![0xa5; max];
        let mut writer = NetWriter::new();
        writer.put_bytes_with_length(&value).unwrap();
        assert_eq!(&writer.as_slice()[..2], &[0xff, 0xff]);
        assert_eq!(&writer.as_slice()[2..], value);
        assert_eq!(
            NetReader::new(writer.as_slice())
                .get_bytes_with_length()
                .unwrap(),
            value
        );

        let before = writer.as_slice().to_vec();
        value.push(0xa5);
        assert_eq!(
            writer.put_bytes_with_length(&value),
            Err(NetWriteError::LengthOverflow {
                length: max + 1,
                max
            })
        );
        assert_eq!(writer.as_slice(), before);
    }

    #[test]
    fn underflow_is_reported() {
        let mut reader = NetReader::new(&[1]);
        assert!(matches!(
            reader.get_u16(),
            Err(NetReadError::Underflow {
                needed: 2,
                remaining: 1
            })
        ));
    }
}
