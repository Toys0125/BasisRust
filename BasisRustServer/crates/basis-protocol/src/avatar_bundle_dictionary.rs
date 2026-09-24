use base64::{engine::general_purpose::STANDARD, Engine as _};
use flate2::read::ZlibDecoder;
use std::{io::Read, sync::OnceLock};

pub const GENERATION: u8 = 1;
pub const RAW_LEN: usize = 16 * 1024;

const COMPRESSED_BASE64: &str = concat!(
    include_str!("avatar_bundle_dictionary_z0.b64"),
    include_str!("avatar_bundle_dictionary_z1.b64"),
    include_str!("avatar_bundle_dictionary_z2.b64"),
);

pub fn bytes() -> &'static [u8] {
    static DICTIONARY: OnceLock<Vec<u8>> = OnceLock::new();
    DICTIONARY
        .get_or_init(|| {
            let compressed = STANDARD
                .decode(COMPRESSED_BASE64)
                .expect("embedded avatar bundle dictionary base64 is valid");
            let mut decoder = ZlibDecoder::new(compressed.as_slice());
            let mut raw = Vec::with_capacity(RAW_LEN);
            decoder
                .read_to_end(&mut raw)
                .expect("embedded avatar bundle dictionary zlib stream is valid");
            assert_eq!(
                raw.len(),
                RAW_LEN,
                "embedded avatar bundle dictionary length drifted"
            );
            raw
        })
        .as_slice()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_dictionary_has_current_generation_and_size() {
        assert_eq!(GENERATION, 1);
        assert_eq!(bytes().len(), RAW_LEN);
    }
}
