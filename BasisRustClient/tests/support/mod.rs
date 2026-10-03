use std::{
    collections::HashMap,
    net::SocketAddr,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use basis_protocol::{channels, config::ServerConfig};
use basis_server_core::ServerState;
use basis_transport::PacketProperty;
use tempfile::TempDir;
use tokio::{
    io::AsyncWriteExt,
    net::UdpSocket,
    process::{Child, Command},
    sync::{mpsc, oneshot},
    task::{JoinHandle, JoinSet},
    time::{timeout, Instant},
};

pub const LIMIT: Duration = Duration::from_secs(10);
pub const SILENCE: &[u8] = &[0xf8, 0xff, 0xfe]; // Standard 20 ms Opus silence packet.

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    None,
    ReliableLoss,
    InvalidSignature,
}

#[derive(Debug)]
pub struct Capture {
    pub client: SocketAddr,
    pub from_server: bool,
    pub dropped: bool,
    pub bytes: Vec<u8>,
}

impl Capture {
    pub fn property(&self) -> PacketProperty {
        PacketProperty::from_byte(self.bytes[0]).unwrap()
    }

    pub fn channeled(&self, channel: u8) -> bool {
        self.property() == PacketProperty::Channeled && self.bytes[3] == channel * 4 + 2
    }

    pub fn sequence(&self) -> u16 {
        u16::from_le_bytes(self.bytes[1..3].try_into().unwrap())
    }
}

/// Real server + real executable. The proxy only forwards/captures datagrams and injects
/// specified faults; it never substitutes a fake client, server, or ACK generator.
pub struct Live {
    pub server: ServerState,
    pub dir: TempDir,
    pub child: Child,
    events: mpsc::UnboundedReceiver<Capture>,
    proxy: JoinHandle<()>,
    shutdown: Option<oneshot::Sender<()>>,
}

impl Live {
    pub async fn start(password: &str, clients: usize, traffic: bool, fault: Fault) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let config = ServerConfig {
            set_port: 0,
            override_auto_discovery_of_ipv: true,
            ipv4_address: "127.0.0.1".into(),
            ipv6_enabled: false,
            has_file_support: false,
            use_auth: true,
            use_auth_identity: true,
            // Test the uncompressed upstream layout; extension codecs have their own vectors.
            enable_avatar_bundle_compression: false,
            enable_avatar_bundle_zstd: false,
            enable_avatar_delta_compression: false,
            enable_compute_offload: false,
            ..ServerConfig::default()
        };
        let (server, shutdown) = ServerState::start(config, dir.path()).await.unwrap();
        let front = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let port = front.local_addr().unwrap().port();
        let (tx, events) = mpsc::unbounded_channel();
        let proxy = tokio::spawn(proxy(
            front,
            server.transport.local_addr().unwrap(),
            tx,
            fault,
        ));
        let config_path = dir.path().join("Config.xml");
        // Flat PascalCase XML and the upstream default password are deliberately retained.
        std::fs::write(
            &config_path,
            format!("<Configuration><Password>{password}</Password></Configuration>"),
        )
        .unwrap();
        let log = std::fs::File::create(dir.path().join("client.log")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_basis-rust-client"));
        command
            .current_dir(dir.path())
            .env("RUST_LOG", "info")
            .env("BASIS_CLIENT_TOKIO_WORKERS", "2")
            .env("BASIS_CLIENT_SHARED_MAINTENANCE", "1")
            .env("BASIS_CLIENT_SHARED_RECEIVE", "1")
            .env_remove("BASIS_AVATAR_DIAGNOSTICS")
            .args([
                "--config",
                config_path.to_str().unwrap(),
                "--ip",
                "127.0.0.1",
                "--port",
            ])
            .arg(port.to_string())
            .args([
                "--clients",
                &clients.to_string(),
                "--no-reconnect",
                "--no-spread",
            ])
            .args([
                "--connect-timeout-ms",
                "3000",
                "--connect-batch-delay-ms",
                "0",
            ])
            .args(["--quit-batch-delay-ms", "0", "--duration-secs", "30"])
            .stdin(Stdio::piped())
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .kill_on_drop(true);
        if traffic {
            let audio = dir.path().join("audio");
            std::fs::create_dir(&audio).unwrap();
            std::fs::write(audio.join("silence.opus"), ogg_silence()).unwrap();
            command
                .args([
                    "--voice",
                    "--no-voice-reencode",
                    "--voice-speaker-percent",
                    "100",
                ])
                .arg("--voice-audio-folder")
                .arg(audio)
                .args([
                    "--movement-jitter-percent",
                    "0",
                    "--voice-jitter-percent",
                    "0",
                ])
                .arg("--observe-avatar-csv")
                .arg(dir.path().join("avatar.csv"))
                .args(["--avatar-observe-expected-peers", "1"]);
        } else {
            command.arg("--no-movement");
        }
        let child = command.spawn().unwrap();
        Self {
            server,
            dir,
            child,
            events,
            proxy,
            shutdown: Some(shutdown),
        }
    }

    pub fn log(&self) -> String {
        std::fs::read_to_string(self.dir.path().join("client.log")).unwrap()
    }

    pub async fn next(&mut self, deadline: Instant) -> Capture {
        match tokio::time::timeout_at(deadline, self.events.recv()).await {
            Ok(Some(event)) => event,
            result => panic!("live packet deadline: {result:?}\n{}", self.log()),
        }
    }

    pub async fn until(&mut self, predicate: impl Fn(&Capture) -> bool) -> Capture {
        let deadline = Instant::now() + LIMIT;
        loop {
            let event = self.next(deadline).await;
            if predicate(&event) {
                return event;
            }
        }
    }

    pub async fn finish(&mut self) {
        self.child
            .stdin
            .as_mut()
            .unwrap()
            .write_all(b"quit\n")
            .await
            .unwrap();
        let status = timeout(LIMIT, self.child.wait())
            .await
            .expect("client exit deadline")
            .unwrap();
        assert!(status.success(), "client failed: {}", self.log());
        self.server.shutdown().await.unwrap();
    }

    pub async fn await_client_rejection(&mut self) {
        let result = timeout(LIMIT, async {
            let mut tick = tokio::time::interval(Duration::from_millis(10));
            loop {
                if self
                    .log()
                    .contains("disconnected/rejected by server: Disconnect")
                {
                    break;
                }
                assert!(
                    self.child.try_wait().unwrap().is_none(),
                    "client exited before observing rejection: {}",
                    self.log()
                );
                tick.tick().await;
            }
        })
        .await;
        assert!(
            result.is_ok(),
            "client did not observe rejection: {}",
            self.log()
        );
    }
}

impl Drop for Live {
    fn drop(&mut self) {
        self.server.transport.shutdown();
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        self.proxy.abort(); // Dropping its JoinSet also aborts all per-client forwarders.
        let _ = self.child.start_kill();
    }
}

async fn proxy(
    front: Arc<UdpSocket>,
    server: SocketAddr,
    tx: mpsc::UnboundedSender<Capture>,
    fault: Fault,
) {
    let mut upstreams = HashMap::new();
    let mut receivers = JoinSet::new();
    let mut dropped_auth = false;
    let loss = fault == Fault::ReliableLoss;
    let mut buffer = vec![0; 65535];
    loop {
        tokio::select! {
            result = front.recv_from(&mut buffer) => {
                let (len, client) = result.unwrap();
                if fault == Fault::InvalidSignature && buffer[0] & 31 == 1 && buffer[3] == 2 {
                    // Change one signature byte in the actual client's response. Retain
                    // its framing, channel, sequence, and all remaining bytes.
                    assert_eq!(len, 75);
                    assert_eq!(&buffer[4..6], &[64, 0]);
                    buffer[6] ^= 1;
                }
                if let std::collections::hash_map::Entry::Vacant(entry) = upstreams.entry(client) {
                    let upstream = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
                    upstream.connect(server).await.unwrap();
                    let gap_ack = Arc::new(AtomicBool::new(false));
                    entry.insert((upstream.clone(), gap_ack.clone()));
                    receivers.spawn(receive_server(upstream, front.clone(), client, tx.clone(), loss, gap_ack));
                }
                let frames = frames(&buffer[..len]);
                // Release sequence zero only after the client has ACKed the real gap.
                // This makes loss recovery deterministic even on a slow CI runner.
                if frames.iter().any(|p| p[0] & 31 == 2 && p[3] == channels::CHAT * 4 + 2
                    && p[1..3] == [0, 0] && p[4] & 1 == 0 && p[4..20].iter().any(|b| *b != 0)) {
                    upstreams[&client].1.store(true, Ordering::Relaxed);
                }
                let drop = loss && !dropped_auth && frames.iter().any(|p| {
                    p[0] & 31 == 1 && p[3] == channels::AUTH_IDENTITY * 4 + 2
                });
                dropped_auth |= drop;
                capture(&tx, client, false, drop, frames);
                if !drop { upstreams[&client].0.send(&buffer[..len]).await.unwrap(); }
            }
            result = receivers.join_next(), if !receivers.is_empty() => {
                panic!("proxy forwarder stopped: {result:?}");
            }
        }
    }
}

async fn receive_server(
    upstream: Arc<UdpSocket>,
    front: Arc<UdpSocket>,
    client: SocketAddr,
    tx: mpsc::UnboundedSender<Capture>,
    loss: bool,
    gap_ack: Arc<AtomicBool>,
) {
    let mut buffer = vec![0; 65535];
    loop {
        let len = upstream.recv(&mut buffer).await.unwrap();
        let frames = frames(&buffer[..len]);
        let drop = loss
            && !gap_ack.load(Ordering::Relaxed)
            && frames
                .iter()
                .any(|p| p[0] & 31 == 1 && p[3] == channels::CHAT * 4 + 2 && p[1..3] == [0, 0]);
        if drop {
            // A packed datagram may contain the entire send window. Forward its
            // unchanged child packets individually, withholding only sequence zero.
            for frame in frames {
                let drop = frame[0] & 31 == 1
                    && frame[3] == channels::CHAT * 4 + 2
                    && frame[1..3] == [0, 0];
                capture(&tx, client, true, drop, vec![frame.clone()]);
                if !drop {
                    front.send_to(&frame, client).await.unwrap();
                }
            }
        } else {
            capture(&tx, client, true, false, frames);
            front.send_to(&buffer[..len], client).await.unwrap();
        }
    }
}

fn capture(
    tx: &mpsc::UnboundedSender<Capture>,
    client: SocketAddr,
    from_server: bool,
    dropped: bool,
    frames: Vec<Vec<u8>>,
) {
    for bytes in frames {
        if tx
            .send(Capture {
                client,
                from_server,
                dropped,
                bytes,
            })
            .is_err()
        {
            return;
        }
    }
}

/// Independent framing reader for captures. Ordinary Merged is LiteNetLib's LE-u16
/// length framing. CompactMerged is a Rust/Basis extension, not a Unity parity claim.
fn frames(packet: &[u8]) -> Vec<Vec<u8>> {
    assert!(!packet.is_empty());
    match packet[0] & 31 {
        12 => {
            let mut remaining = &packet[1..];
            let mut output = Vec::new();
            while !remaining.is_empty() {
                let len = u16::from_le_bytes(remaining[..2].try_into().unwrap()) as usize;
                assert!(len > 0);
                output.extend(frames(&remaining[2..2 + len]));
                remaining = &remaining[2 + len..];
            }
            output
        }
        18 => {
            let mut remaining = &packet[1..];
            let mut output = Vec::new();
            while !remaining.is_empty() {
                let tag = remaining[0];
                remaining = &remaining[1..];
                let len = if tag & 0x80 != 0 {
                    let len = u16::from_le_bytes(remaining[..2].try_into().unwrap()) as usize;
                    remaining = &remaining[2..];
                    len
                } else {
                    let len = remaining[0] as usize;
                    remaining = &remaining[1..];
                    len
                };
                let body = &remaining[..len];
                if tag & 0x40 != 0 {
                    output.extend(frames(body));
                } else {
                    let mut unpacked = vec![packet[0] & 0x60, tag & 0x3f];
                    unpacked.extend_from_slice(body);
                    output.push(unpacked);
                }
                remaining = &remaining[len..];
            }
            output
        }
        _ => vec![packet.to_vec()],
    }
}

// A valid mono Ogg Opus stream built locally; no FFmpeg, audio device, or user audio.
fn ogg_silence() -> Vec<u8> {
    fn page(sequence: u32, flags: u8, granule: u64, packets: &[&[u8]]) -> Vec<u8> {
        let mut page = b"OggS\0".to_vec();
        page.push(flags);
        page.extend_from_slice(&granule.to_le_bytes());
        page.extend_from_slice(&1u32.to_le_bytes());
        page.extend_from_slice(&sequence.to_le_bytes());
        page.extend_from_slice(&[0; 4]);
        page.push(packets.len() as u8);
        for packet in packets {
            page.push(u8::try_from(packet.len()).unwrap());
        }
        for packet in packets {
            page.extend_from_slice(packet);
        }
        let mut crc = 0u32;
        for byte in &page {
            crc ^= (*byte as u32) << 24;
            for _ in 0..8 {
                crc = (crc << 1) ^ if crc & 0x80000000 != 0 { 0x04c11db7 } else { 0 };
            }
        }
        page[22..26].copy_from_slice(&crc.to_le_bytes());
        page
    }
    let mut output = page(
        0,
        2,
        0,
        &[b"OpusHead\x01\x01\x00\x00\x80\xbb\x00\x00\x00\x00\x00"],
    );
    output.extend(page(
        1,
        0,
        0,
        &[b"OpusTags\x00\x00\x00\x00\x00\x00\x00\x00"],
    ));
    output.extend(page(2, 4, 960 * 100, &vec![SILENCE; 100]));
    output
}
