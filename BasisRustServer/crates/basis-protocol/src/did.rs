use anyhow::{Context, Result};
use ed25519_dalek::{Signature, VerifyingKey};

use crate::{
    io::{NetReader, NetWriter, WriteResult},
    messages::{BasisDeserialize, BasisSerialize, BytesMessage},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DidChallenge {
    pub bytes: Vec<u8>,
}

impl BasisSerialize for DidChallenge {
    fn serialize(&self, writer: &mut NetWriter) -> WriteResult<()> {
        BytesMessage {
            data: self.bytes.clone(),
        }
        .serialize(writer)?;
        Ok(())
    }
}

impl BasisDeserialize for DidChallenge {
    fn deserialize(reader: &mut NetReader<'_>) -> crate::io::Result<Self> {
        Ok(Self {
            bytes: BytesMessage::deserialize(reader)?.data,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DidResponse {
    pub signature: Vec<u8>,
    pub fragment: String,
}

impl DidResponse {
    pub fn verify(&self, challenge: &[u8], verifying_key: &VerifyingKey) -> Result<()> {
        let signature =
            Signature::from_slice(&self.signature).context("invalid Ed25519 signature")?;
        verifying_key
            .verify_strict(challenge, &signature)
            .context("DID signature verification failed")
    }
}

/// Resolve the Ed25519 key from the Rust client's supported `did:key` form.
///
/// This intentionally accepts only multibase base58btc Ed25519 keys with the
/// canonical two-byte multicodec prefix followed by exactly 32 public-key bytes.
pub fn did_key_verifying_key(did: &str) -> Result<VerifyingKey> {
    let encoded = did
        .strip_prefix("did:key:z")
        .context("unsupported identity; expected a did:key Ed25519 identity")?;
    anyhow::ensure!(
        !encoded.is_empty() && encoded.len() <= 64,
        "malformed did:key identity"
    );
    let decoded = bs58::decode(encoded)
        .into_vec()
        .context("malformed did:key base58 value")?;
    anyhow::ensure!(
        decoded.len() == 34 && decoded[..2] == [0xed, 0x01],
        "unsupported did:key method or key type"
    );
    anyhow::ensure!(
        bs58::encode(&decoded).into_string() == encoded,
        "non-canonical did:key base58 value"
    );
    let public_key: [u8; 32] = decoded[2..].try_into().expect("length checked above");
    VerifyingKey::from_bytes(&public_key).context("invalid Ed25519 public key")
}

impl BasisSerialize for DidResponse {
    fn serialize(&self, writer: &mut NetWriter) -> WriteResult<()> {
        BytesMessage {
            data: self.signature.clone(),
        }
        .serialize(writer)?;
        BytesMessage {
            data: if self.fragment.is_empty() {
                b"N/A".to_vec()
            } else {
                self.fragment.as_bytes().to_vec()
            },
        }
        .serialize(writer)?;
        Ok(())
    }
}

impl BasisDeserialize for DidResponse {
    fn deserialize(reader: &mut NetReader<'_>) -> crate::io::Result<Self> {
        let signature = BytesMessage::deserialize(reader)?.data;
        let fragment = String::from_utf8(BytesMessage::deserialize(reader)?.data)
            .map_err(|_| crate::io::NetReadError::Utf8)?;
        Ok(Self {
            signature,
            fragment,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};

    fn did_key(key: &SigningKey) -> String {
        let mut multicodec = [0u8; 34];
        multicodec[0] = 0xed;
        multicodec[1] = 0x01;
        multicodec[2..].copy_from_slice(&key.verifying_key().to_bytes());
        format!("did:key:z{}", bs58::encode(multicodec).into_string())
    }

    #[test]
    fn client_did_key_verifies_only_its_challenge_signature() {
        let signing_key = SigningKey::from_bytes(&[7; 32]);
        let challenge = b"fresh connection nonce";
        let signature = signing_key.sign(challenge);
        let did = did_key(&signing_key);
        let verifying_key = did_key_verifying_key(&did).unwrap();
        DidResponse {
            signature: signature.to_bytes().to_vec(),
            fragment: "N/A".into(),
        }
        .verify(challenge, &verifying_key)
        .unwrap();
        assert!(DidResponse {
            signature: signature.to_bytes().to_vec(),
            fragment: "N/A".into(),
        }
        .verify(b"another nonce", &verifying_key)
        .is_err());
    }

    #[test]
    fn did_key_parser_rejects_other_claims_and_key_types() {
        let signing_key = SigningKey::from_bytes(&[9; 32]);
        assert!(did_key_verifying_key("captured-uuid").is_err());
        let wrong_codec = format!(
            "did:key:z{}",
            bs58::encode(
                [0x80, 0x24]
                    .into_iter()
                    .chain(signing_key.verifying_key().to_bytes())
                    .collect::<Vec<_>>()
            )
            .into_string()
        );
        assert!(did_key_verifying_key(&wrong_codec).is_err());
    }
}
