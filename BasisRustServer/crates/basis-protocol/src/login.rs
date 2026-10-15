//! Wire-format shared with BasisVR's BasisDidLoginTranscript (protocol 56).
use sha2::{Digest, Sha256};

const DOMAIN: &[u8] = b"basis-did-login-v2";
pub const EPHEMERAL_KEY_SIZE: usize = 32;
pub const MIN_NONCE_SIZE: usize = 32;

pub fn connection_payload_hash(payload_after_auth: &[u8]) -> [u8; 32] {
    Sha256::digest(payload_after_auth).into()
}

/// Signs exactly the same fields in the same order as BasisVR. Lengths are raw
/// little-endian u16 (not LiteNetLib's string length plus one).
pub fn did_login_transcript(
    client_did: &str,
    server_id: &str,
    nonce: &[u8],
    client_public_key: &[u8; 32],
    server_public_key: &[u8; 32],
    payload_hash: &[u8; 32],
) -> anyhow::Result<Vec<u8>> {
    let fields: [&[u8]; 6] = [
        client_did.as_bytes(),
        server_id.as_bytes(),
        nonce,
        client_public_key,
        server_public_key,
        payload_hash,
    ];
    let mut out =
        Vec::with_capacity(DOMAIN.len() + fields.iter().map(|f| 2 + f.len()).sum::<usize>());
    out.extend_from_slice(DOMAIN);
    for field in fields {
        let length = u16::try_from(field.len())?;
        out.extend_from_slice(&length.to_le_bytes());
        out.extend_from_slice(field);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transcript_matches_csharp_field_encoding() {
        let client = [7; 32];
        let server = [8; 32];
        let hash = connection_payload_hash(b"payload");
        let transcript = did_login_transcript(
            "did:key:zX",
            "server-uuid",
            &[3; 32],
            &client,
            &server,
            &hash,
        )
        .unwrap();
        assert!(transcript
            .starts_with(b"basis-did-login-v2\x0a\x00did:key:zX\x0b\x00server-uuid\x20\x00"));
        assert!(transcript.ends_with(&hash));
        assert_ne!(
            connection_payload_hash(b"payload"),
            connection_payload_hash(b"payloaD")
        );
    }
}
