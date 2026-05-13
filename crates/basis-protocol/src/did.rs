use anyhow::{Context, Result};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};

use crate::{
    io::{NetReader, NetWriter},
    messages::{BasisDeserialize, BasisSerialize, BytesMessage},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DidChallenge {
    pub bytes: Vec<u8>,
}

impl BasisSerialize for DidChallenge {
    fn serialize(&self, writer: &mut NetWriter) {
        BytesMessage {
            data: self.bytes.clone(),
        }
        .serialize(writer);
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
            .verify(challenge, &signature)
            .context("DID signature verification failed")
    }
}

impl BasisSerialize for DidResponse {
    fn serialize(&self, writer: &mut NetWriter) {
        BytesMessage {
            data: self.signature.clone(),
        }
        .serialize(writer);
        BytesMessage {
            data: if self.fragment.is_empty() {
                b"N/A".to_vec()
            } else {
                self.fragment.as_bytes().to_vec()
            },
        }
        .serialize(writer);
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
