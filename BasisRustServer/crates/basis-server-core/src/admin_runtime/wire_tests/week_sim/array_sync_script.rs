//! Minimal simulated owner script using the real Basis scene protocol.
//! No Unity APIs are invented or executed by this Rust test actor.
use super::transport::WirePeer;
use basis_protocol::{
    channels,
    io::NetWriter,
    messages::{BasisSerialize, SceneDataMessage},
};

pub struct ArraySyncScript {
    pub index: u16,
    pub bytes: [u8; 100],
}

impl ArraySyncScript {
    pub fn update(&mut self, revision: usize) {
        for (i, byte) in self.bytes.iter_mut().enumerate() {
            *byte = (revision as u8).wrapping_mul(37).wrapping_add(i as u8 * 2);
        }
        self.bytes[..8].copy_from_slice(&(revision as u64).to_le_bytes());
    }

    pub async fn on_player_joined(&self, owner: &mut WirePeer, player: u16) {
        self.send(owner, vec![player]).await;
    }

    pub async fn broadcast(&self, owner: &mut WirePeer) {
        self.send(owner, Vec::new()).await;
    }

    async fn send(&self, owner: &mut WirePeer, recipients: Vec<u16>) {
        let mut writer = NetWriter::new();
        SceneDataMessage {
            message_index: self.index,
            recipients,
            payload: self.bytes.to_vec(),
        }
        .serialize(&mut writer)
        .unwrap();
        owner.send(channels::SCENE, writer.as_slice(), true).await;
    }
}
