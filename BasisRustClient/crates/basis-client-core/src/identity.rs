use anyhow::{Context, Result};
use basis_protocol::did::DidResponse;
use basis_protocol::io::NetWriter;
use basis_protocol::messages::BasisSerialize;
use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use rand::rngs::OsRng;
use rand::RngCore;

#[derive(Debug)]
pub(crate) struct Identity {
    pub(crate) signing_key: SigningKey,
    pub(crate) verifying_key: VerifyingKey,
    pub(crate) fragment: String,
}

impl Identity {
    pub(crate) fn random() -> Self {
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        let signing_key = SigningKey::from_bytes(&bytes);
        let verifying_key = signing_key.verifying_key();
        Self {
            signing_key,
            verifying_key,
            fragment: String::new(),
        }
    }

    pub(crate) fn did_key(&self) -> String {
        // did:key for Ed25519 is multibase(base58-btc(multicodec-varint(0xED) || pubkey)).
        // Unsigned LEB128 encoding of the Ed25519 multicodec id 0xED is [0xED, 0x01].
        let mut multicodec = [0u8; 34];
        multicodec[0] = 0xED;
        multicodec[1] = 0x01;
        multicodec[2..].copy_from_slice(&self.verifying_key.to_bytes());
        format!("did:key:z{}", bs58::encode(multicodec).into_string())
    }

    pub(crate) fn response_payload(&self, challenge: &[u8]) -> Result<Vec<u8>> {
        let signature = self.signing_key.sign(challenge);
        self.verifying_key
            .verify(challenge, &signature)
            .context("DID signature self-verification failed")?;
        let mut writer = NetWriter::with_capacity(96);
        DidResponse {
            signature: signature.to_bytes().to_vec(),
            fragment: self.fragment.clone(),
        }
        .serialize(&mut writer)?;
        Ok(writer.into_vec())
    }
}
