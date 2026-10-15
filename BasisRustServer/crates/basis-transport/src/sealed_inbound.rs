//! Protocol-56 client-to-server LiteNetLib packet layer. Only inbound traffic is
//! sealed; the server's challenge and subsequent outbound packets stay cleartext.
use chacha20poly1305::{
    aead::{AeadInPlace, KeyInit},
    ChaCha20Poly1305, Nonce, Tag,
};
use std::borrow::Cow;

const OVERHEAD: usize = 24; // Poly1305 tag (16) + little-endian counter (8)
const WINDOW_SIZE: usize = 1024;

#[derive(Default)]
pub(crate) struct ReplayWindow {
    highest: u64,
    seen: [u64; WINDOW_SIZE / 64],
}

impl ReplayWindow {
    fn could_accept(&self, counter: u64) -> bool {
        counter > 0
            && (counter > self.highest
                || (self.highest - counter < WINDOW_SIZE as u64 && !self.is_set(counter)))
    }

    fn is_set(&self, counter: u64) -> bool {
        let index = (counter as usize) % WINDOW_SIZE;
        self.seen[index / 64] & (1u64 << (index % 64)) != 0
    }

    fn accept(&mut self, counter: u64) {
        if counter > self.highest {
            if counter - self.highest >= WINDOW_SIZE as u64 {
                self.seen.fill(0);
            } else {
                for n in (self.highest + 1)..counter {
                    let index = (n as usize) % WINDOW_SIZE;
                    self.seen[index / 64] &= !(1u64 << (index % 64));
                }
            }
            self.highest = counter;
        }
        let index = (counter as usize) % WINDOW_SIZE;
        self.seen[index / 64] |= 1u64 << (index % 64);
    }
}

#[derive(Default)]
pub(crate) enum InboundSeal {
    #[default]
    Plain,
    AwaitingKey,
    Active {
        cipher: ChaCha20Poly1305,
        window: ReplayWindow,
        proven: bool,
    },
}

impl std::fmt::Debug for InboundSeal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plain => f.write_str("Plain"),
            Self::AwaitingKey => f.write_str("AwaitingKey"),
            Self::Active { proven, .. } => {
                f.debug_struct("Active").field("proven", proven).finish()
            }
        }
    }
}

impl InboundSeal {
    pub(crate) fn activate(&mut self, key: &[u8; 32]) -> bool {
        if !matches!(self, Self::AwaitingKey) {
            return false;
        }
        *self = Self::Active {
            cipher: ChaCha20Poly1305::new(key.into()),
            window: ReplayWindow::default(),
            proven: false,
        };
        true
    }

    /// C#'s BasisPeerCipherLayer seals *every* outgoing property. Before a key is
    /// proven, non-user control datagrams can still be cleartext. After proof,
    /// only ShutdownOk may be cleartext. User packets always require a seal.
    pub(crate) fn open<'a>(&mut self, packet: &'a [u8]) -> Option<Cow<'a, [u8]>> {
        let property = packet.first()? & 0x1f;
        let user_data = matches!(property, 0 | 1 | 12 | 18);
        match self {
            Self::Plain => Some(Cow::Borrowed(packet)),
            Self::AwaitingKey => (!user_data).then_some(Cow::Borrowed(packet)),
            Self::Active {
                cipher,
                window,
                proven,
            } => {
                if packet.len() > OVERHEAD {
                    let tag_start = packet.len() - OVERHEAD;
                    let counter = i64::from_le_bytes(packet[packet.len() - 8..].try_into().ok()?);
                    if counter > 0 && window.could_accept(counter as u64) {
                        let mut plain = packet[..tag_start].to_vec();
                        let mut nonce = [0u8; 12];
                        nonce[..8].copy_from_slice(&counter.to_le_bytes());
                        let tag = Tag::from_slice(&packet[tag_start..tag_start + 16]);
                        if cipher
                            .decrypt_in_place_detached(
                                Nonce::from_slice(&nonce),
                                &packet[..1],
                                &mut plain[1..],
                                tag,
                            )
                            .is_ok()
                        {
                            window.accept(counter as u64);
                            *proven = true;
                            return Some(Cow::Owned(plain));
                        }
                    }
                }
                (!user_data && (!*proven || property == 13)).then_some(Cow::Borrowed(packet))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seal(key: &[u8; 32], packet: &[u8], counter: i64) -> Vec<u8> {
        let cipher = ChaCha20Poly1305::new(key.into());
        let mut data = packet.to_vec();
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&counter.to_le_bytes());
        let header = [data[0]];
        let tag = cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), &header, &mut data[1..])
            .unwrap();
        data.extend_from_slice(&tag);
        data.extend_from_slice(&counter.to_le_bytes());
        data
    }

    #[test]
    fn rejects_cleartext_replay_tamper_and_wrong_header() {
        let key = [9; 32];
        let mut seal_state = InboundSeal::AwaitingKey;
        assert!(seal_state.open(&[1, 1, 2]).is_none());
        assert!(seal_state.activate(&key));
        let datagram = seal(&key, &[1, 5, 6, 7], 1);
        assert_eq!(seal_state.open(&datagram).unwrap().as_ref(), &[1, 5, 6, 7]);
        assert!(seal_state.open(&datagram).is_none());
        assert!(seal_state.open(&[1, 5, 6, 7]).is_none());
        let mut tampered = seal(&key, &[1, 5, 6, 7], 2);
        tampered[0] = 0;
        assert!(seal_state.open(&tampered).is_none());
        assert!(seal_state.open(&[3, 0, 0]).is_none());
        assert!(seal_state.open(&[13]).is_some());
    }

    #[test]
    fn replay_window_allows_reordering_not_replays() {
        let mut win = ReplayWindow::default();
        for counter in [1, 4, 2, 3, 2048] {
            assert!(win.could_accept(counter));
            win.accept(counter);
            assert!(!win.could_accept(counter));
        }
        assert!(!win.could_accept(1));
        assert!(!win.could_accept(0));
    }
}
