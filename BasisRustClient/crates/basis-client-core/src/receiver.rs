use crate::client::BasisClient;
#[cfg(target_os = "linux")]
use crate::packet_diagnostics::DropReason;
#[cfg(target_os = "linux")]
use crate::transport::parse_packet;
#[cfg(target_os = "linux")]
use crate::transport::LITENETLIB_MAX_MTU;
use crate::transport::{
    ACK_FLUSH_INTERVAL, PING_INTERVAL_TICKS, RESEND_INTERVAL_TICKS, SHARED_SNAPSHOT_REFRESH_TICKS,
};
#[cfg(target_os = "linux")]
use basis_transport::DEFAULT_WINDOW_SIZE;
#[cfg(target_os = "linux")]
use basis_transport::{
    dotnet_utc_ticks, PacketProperty, LITENETLIB_CHANNELED_HEADER_SIZE, LITENETLIB_INITIAL_MTU,
};
#[cfg(target_os = "linux")]
use std::collections::HashMap;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::os::fd::RawFd;
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(target_os = "linux")]
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
#[cfg(target_os = "linux")]
use std::sync::Weak;
#[cfg(target_os = "linux")]
use std::time::Duration;
#[cfg(target_os = "linux")]
use tokio::sync::mpsc;
use tokio::sync::{Mutex, Notify};
use tokio::time;
#[cfg(target_os = "linux")]
use tracing::{debug, info, warn};

#[cfg(target_os = "linux")]
pub(crate) static SHARED_RECEIVE_ACTIVE: AtomicBool = AtomicBool::new(false);
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct SharedReceivePacket {
    pub(crate) index: usize,
    pub(crate) fd: RawFd,
    pub(crate) client: Weak<BasisClient>,
    pub(crate) offset: usize,
    pub(crate) len: usize,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct SharedReceiveBatch {
    pub(crate) data: Vec<u8>,
    pub(crate) packets: Vec<SharedReceivePacket>,
}

#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct SharedReceiveRegistration {
    pub(crate) index: usize,
    pub(crate) fd: RawFd,
    pub(crate) client: Weak<BasisClient>,
}

#[cfg(target_os = "linux")]
pub(crate) fn shared_receive_registration_matches(
    registered_fd: RawFd,
    fd: RawFd,
    registered_client: &Weak<BasisClient>,
    client: &Arc<BasisClient>,
) -> bool {
    registered_fd == fd && Weak::ptr_eq(registered_client, &Arc::downgrade(client))
}

#[cfg(target_os = "linux")]
pub(crate) fn shared_receiver_mark_reliable(client: &BasisClient, bytes: &[u8]) {
    if bytes.len() < LITENETLIB_CHANNELED_HEADER_SIZE {
        client.record_unparsed_packet(bytes);
        return;
    }
    if bytes[0] & 0x1f != PacketProperty::Channeled as u8 {
        return;
    }
    let sequence = u16::from_le_bytes([bytes[1], bytes[2]]);
    let channel_id = bytes[3];
    if !matches!(channel_id % 4, 0 | 2) {
        return;
    }
    let Some(()) = client.reliable_receive_state().map(|mut state| {
        state.mark_new(channel_id, sequence);
    }) else {
        return;
    };
    // Duplicates re-arm the same persistent ACK window. Invalid/too-old packets leave no dirty
    // channel, so the maintenance flush remains a cheap no-op for them.
    client.ack_pending.store(true, Ordering::Relaxed);
}

#[cfg(target_os = "linux")]
pub(crate) fn shared_receiver_send_pong(fd: RawFd, first_byte: u8, sequence: u16) {
    let mut packet = [0u8; 11];
    packet[0] = PacketProperty::Pong as u8 | (first_byte & 0x60);
    packet[1..3].copy_from_slice(&sequence.to_le_bytes());
    packet[3..11].copy_from_slice(&dotnet_utc_ticks().to_le_bytes());
    unsafe {
        libc::send(fd, packet.as_ptr().cast(), packet.len(), libc::MSG_DONTWAIT);
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn shared_receiver_send_mtu_ok(fd: RawFd, packet: &mut [u8], connection_number: u8) {
    if !(LITENETLIB_INITIAL_MTU..=LITENETLIB_MAX_MTU).contains(&packet.len())
        || packet[0] != (PacketProperty::MtuCheck as u8 | (connection_number << 5))
    {
        return;
    }
    let mtu = i32::from_le_bytes(packet[1..5].try_into().unwrap());
    if usize::try_from(mtu).ok() != Some(packet.len())
        || packet[13..packet.len() - 4].iter().any(|&byte| byte != 0)
        || packet[packet.len() - 4..] != packet[1..5]
    {
        return;
    }
    packet[0] = (packet[0] & 0xe0) | PacketProperty::MtuOk as u8;
    unsafe {
        libc::send(fd, packet.as_ptr().cast(), packet.len(), libc::MSG_DONTWAIT);
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn shared_receiver_process_merged(client: &BasisClient, fd: RawFd, bytes: &[u8]) {
    let mut pos = 1usize;
    while pos + 2 <= bytes.len() {
        let size = u16::from_le_bytes([bytes[pos], bytes[pos + 1]]) as usize;
        pos += 2;
        if size == 0 || pos + size > bytes.len() {
            client
                .packet_diagnostics
                .record(client.index, DropReason::InvalidMerged);
            return;
        }
        let packet = &bytes[pos..pos + size];
        pos += size;
        let property = packet.first().copied().unwrap_or_default() & 0x1f;
        if property == PacketProperty::Unreliable as u8 {
            // Load sinks discard unreliable application data: no parse, no
            // ACK state. A short header keeps the malformed-packet
            // diagnostic the full parse would have recorded.
            if packet.len() < 2 {
                client.record_unparsed_packet(packet);
            }
            continue;
        }
        match parse_packet(packet) {
            None => client.record_unparsed_packet(packet),
            Some(packet)
                if packet.property == PacketProperty::Ack
                    && packet.payload.len() != (DEFAULT_WINDOW_SIZE - 1) / 8 + 2 =>
            {
                client
                    .packet_diagnostics
                    .record(client.index, DropReason::InvalidAckSize);
            }
            _ => {}
        }
        match property {
            p if p == PacketProperty::Channeled as u8 && packet.len() >= 4 => {
                shared_receiver_mark_reliable(client, packet);
            }
            p if p == PacketProperty::Ping as u8 && packet.len() >= 3 => {
                let sequence = u16::from_le_bytes([packet[1], packet[2]]);
                shared_receiver_send_pong(fd, packet[0], sequence);
            }
            _ => {}
        }
    }
    if pos != bytes.len() {
        client
            .packet_diagnostics
            .record(client.index, DropReason::InvalidMerged);
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn run_shared_epoll_receiver(
    registrations: std_mpsc::Receiver<SharedReceiveRegistration>,
    batches: mpsc::UnboundedSender<SharedReceiveBatch>,
    shutdown: Arc<AtomicBool>,
) {
    let profile_packet_mix = std::env::var("BASIS_CLIENT_PROFILE_RX_MIX")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "False" | "FALSE"))
        .unwrap_or(false);
    let mut packet_mix = [0u64; 32];
    let epoll_fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
    if epoll_fd < 0 {
        warn!(
            "failed to create shared epoll receiver: {}",
            std::io::Error::last_os_error()
        );
        return;
    }

    let mut events = vec![unsafe { std::mem::zeroed::<libc::epoll_event>() }; 128];
    let mut buffer = vec![0u8; 65535];
    let mut registered = HashMap::<usize, (RawFd, Weak<BasisClient>)>::new();
    while !shutdown.load(Ordering::Relaxed) {
        while let Ok(registration) = registrations.try_recv() {
            let SharedReceiveRegistration { index, fd, client } = registration;
            if let Some((old_fd, old_client)) = registered.get(&index) {
                if *old_fd == fd && Weak::ptr_eq(old_client, &client) {
                    continue;
                }
                let mut old_event = libc::epoll_event {
                    events: libc::EPOLLIN as u32,
                    u64: ((index as u64) << 32) | (*old_fd as u32 as u64),
                };
                unsafe {
                    libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_DEL, *old_fd, &mut old_event);
                }
            }
            let mut event = libc::epoll_event {
                events: libc::EPOLLIN as u32,
                u64: ((index as u64) << 32) | (fd as u32 as u64),
            };
            let rc = unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_ADD, fd, &mut event) };
            if rc != 0 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::EEXIST) {
                    let rc =
                        unsafe { libc::epoll_ctl(epoll_fd, libc::EPOLL_CTL_MOD, fd, &mut event) };
                    if rc != 0 {
                        warn!("failed to update client {index} fd {fd} with shared epoll receiver: {}", std::io::Error::last_os_error());
                        continue;
                    }
                } else {
                    warn!("failed to register client {index} fd {fd} with shared epoll receiver: {err}");
                    continue;
                }
            }
            registered.insert(index, (fd, client));
        }

        let ready =
            unsafe { libc::epoll_wait(epoll_fd, events.as_mut_ptr(), events.len() as i32, 100) };
        if ready < 0 {
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            warn!("shared epoll receiver wait failed: {err}");
            break;
        }

        let mut batch = Vec::with_capacity(ready as usize);
        let mut batch_data = Vec::with_capacity((ready as usize).saturating_mul(64));
        for event in events.iter().take(ready as usize) {
            let index = (event.u64 >> 32) as usize;
            let fd = event.u64 as u32 as RawFd;
            let Some((registered_fd, registered_client)) = registered.get(&index) else {
                continue;
            };
            if *registered_fd != fd {
                continue;
            }
            let Some(client) = registered_client.upgrade() else {
                continue;
            };
            if !client.in_use.load(Ordering::Relaxed) || client.socket.as_raw_fd() != fd {
                continue;
            }
            loop {
                let len = unsafe {
                    libc::recv(
                        fd,
                        buffer.as_mut_ptr().cast(),
                        buffer.len(),
                        libc::MSG_DONTWAIT,
                    )
                };
                if len > 0 {
                    let len = len as usize;
                    let property = buffer[0] & 0x1f;
                    if profile_packet_mix {
                        packet_mix[property as usize] += 1;
                    }

                    // Registered sockets are authenticated load sinks. Handle the overwhelmingly
                    // common post-auth control traffic here so it never allocates/copies into the
                    // epoll-thread -> Tokio handoff. Reliable payloads still receive protocol ACKs,
                    // but their application data is intentionally discarded for synthetic peers.
                    match property {
                        p if p == PacketProperty::Merged as u8 => {
                            shared_receiver_process_merged(&client, fd, &buffer[..len]);
                            continue;
                        }
                        p if p == PacketProperty::Channeled as u8 => {
                            shared_receiver_mark_reliable(&client, &buffer[..len]);
                            continue;
                        }
                        p if p == PacketProperty::Ping as u8 => {
                            if len >= 3 {
                                let sequence = u16::from_le_bytes([buffer[1], buffer[2]]);
                                shared_receiver_send_pong(fd, buffer[0], sequence);
                            } else {
                                client.record_unparsed_packet(&buffer[..len]);
                            }
                            continue;
                        }
                        p if p == PacketProperty::MtuCheck as u8 => {
                            shared_receiver_send_mtu_ok(
                                fd,
                                &mut buffer[..len],
                                client.connection_number,
                            );
                            continue;
                        }
                        p if p == PacketProperty::Pong as u8
                            || p == PacketProperty::MtuOk as u8 =>
                        {
                            if property == PacketProperty::Pong as u8 && len < 11 {
                                client.record_unparsed_packet(&buffer[..len]);
                            }
                            continue;
                        }
                        _ => {}
                    }

                    let offset = batch_data.len();
                    batch_data.extend_from_slice(&buffer[..len]);
                    batch.push(SharedReceivePacket {
                        index,
                        fd,
                        client: Arc::downgrade(&client),
                        offset,
                        len,
                    });
                    continue;
                }
                if len == 0 {
                    client.record_unparsed_packet(&[]);
                    break;
                }
                let err = std::io::Error::last_os_error();
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) {
                    break;
                }
                break;
            }
        }
        if !batch.is_empty()
            && batches
                .send(SharedReceiveBatch {
                    data: batch_data,
                    packets: batch,
                })
                .is_err()
        {
            unsafe { libc::close(epoll_fd) };
            return;
        }
    }

    if profile_packet_mix {
        let names = [
            "Unreliable",
            "Channeled",
            "Ack",
            "Ping",
            "Pong",
            "ConnectRequest",
            "ConnectAccept",
            "Disconnect",
            "UnconnectedMessage",
            "MtuCheck",
            "MtuOk",
            "Broadcast",
            "Merged",
            "ShutdownOk",
            "PeerNotFound",
            "InvalidProtocol",
            "NatMessage",
            "Empty",
            "CompactMerged",
        ];
        for (property, count) in packet_mix.iter().copied().enumerate() {
            if count != 0 {
                let name = names.get(property).copied().unwrap_or("Unknown");
                info!(
                    "shared receive packet mix property={}({}) count={}",
                    property, name, count
                );
            }
        }
    }
    unsafe { libc::close(epoll_fd) };
}

#[cfg(target_os = "linux")]
pub(crate) async fn shared_receive_loop(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    shutdown: Arc<AtomicBool>,
) {
    let (registration_tx, registration_rx) = std_mpsc::channel::<SharedReceiveRegistration>();
    let (batch_tx, mut batch_rx) = mpsc::unbounded_channel::<SharedReceiveBatch>();
    let thread_shutdown = shutdown.clone();
    if let Err(err) = std::thread::Builder::new()
        .name("basis-shared-rx".to_string())
        .spawn(move || run_shared_epoll_receiver(registration_rx, batch_tx, thread_shutdown))
    {
        warn!("failed to start shared epoll receiver thread: {err}");
        return;
    }

    SHARED_RECEIVE_ACTIVE.store(true, Ordering::Release);
    let mut refresh = time::interval(Duration::from_millis(250));
    refresh.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut snapshot = clients.lock().await.clone();
    let mut registered_fds = vec![-1; snapshot.len()];
    let mut registered_clients = (0..snapshot.len()).map(|_| Weak::new()).collect::<Vec<_>>();

    loop {
        tokio::select! {
            _ = refresh.tick() => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                snapshot = clients.lock().await.clone();
                if registered_fds.len() < snapshot.len() {
                    registered_fds.resize(snapshot.len(), -1);
                    registered_clients.resize(snapshot.len(), Weak::new());
                }
                for (index, client) in snapshot.iter().enumerate().skip(1) {
                    if client.avatar_observer.is_some()
                        || !client.in_use.load(Ordering::Relaxed)
                        || !client.shared_receive_eligible.load(Ordering::Acquire)
                    {
                        continue;
                    }
                    let fd = client.socket.as_raw_fd();
                    let client_weak = Arc::downgrade(client);
                    if shared_receive_registration_matches(
                        registered_fds[index],
                        fd,
                        &registered_clients[index],
                        client,
                    ) {
                        continue;
                    }
                    client.shared_receive.store(true, Ordering::Release);
                    client.stop_receive_loop();
                    if registration_tx
                        .send(SharedReceiveRegistration {
                            index,
                            fd,
                            client: client_weak.clone(),
                        })
                        .is_err()
                    {
                        return;
                    }
                    registered_fds[index] = fd;
                    registered_clients[index] = client_weak;
                }
            }
            maybe_batch = batch_rx.recv() => {
                let Some(batch) = maybe_batch else { break; };
                for packet in batch.packets {
                    let Some(client) = snapshot.get(packet.index) else { continue; };
                    if client.socket.as_raw_fd() != packet.fd
                        || !Weak::ptr_eq(&packet.client, &Arc::downgrade(client))
                        || !client.in_use.load(Ordering::Relaxed)
                    {
                        continue;
                    }
                    let end = packet.offset.saturating_add(packet.len);
                    let Some(bytes) = batch.data.get(packet.offset..end) else { continue; };
                    if let Err(err) = client.handle_packet(bytes).await {
                        debug!("client {} shared receive packet failed: {err}", packet.index);
                    }
                }
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) async fn shared_receive_loop(
    _clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    _shutdown: Arc<AtomicBool>,
) {
}

pub(crate) fn ping_bucket_matches(slot: usize, tick: usize) -> bool {
    slot % PING_INTERVAL_TICKS == tick % PING_INTERVAL_TICKS
}

pub(crate) async fn shared_maintenance_loop(
    clients: Arc<Mutex<Vec<Arc<BasisClient>>>>,
    maintenance_refresh: Arc<Notify>,
    shutdown: Arc<AtomicBool>,
) {
    let mut ticker = time::interval(ACK_FLUSH_INTERVAL);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut snapshot = clients.lock().await.clone();
    let mut tick_count = 0usize;

    loop {
        let ticked = tokio::select! {
            _ = ticker.tick() => true,
            _ = maintenance_refresh.notified() => {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                snapshot = clients.lock().await.clone();
                false
            }
        };
        if !ticked {
            continue;
        }
        if shutdown.load(Ordering::Relaxed) {
            break;
        }

        tick_count = tick_count.wrapping_add(1);
        if tick_count.is_multiple_of(SHARED_SNAPSHOT_REFRESH_TICKS) {
            snapshot = clients.lock().await.clone();
        }

        // The ACK flush lives here, not only in the per-client `maintenance_loop`, because this
        // is the path that actually runs: shared maintenance is on by default, so the per-client
        // loop is never spawned. Ticking at LiteNetLib's update rate is what bounds how long a
        // received packet waits to be acknowledged, and the sender's window only refills as fast
        // as we acknowledge -- a slower cadence throttles throughput rather than batching it.
        let resend_due = tick_count.is_multiple_of(RESEND_INTERVAL_TICKS);
        for client in &snapshot {
            if !client.in_use.load(Ordering::Relaxed) {
                continue;
            }
            if client.ack_pending.swap(false, Ordering::Relaxed) {
                let _ = client.flush_acks().await;
            }
            if resend_due && client.pending_reliable_active.load(Ordering::Relaxed) {
                let _ = client.resend_reliable().await;
            }
        }

        // Both loops now tick at ACK_FLUSH_INTERVAL, so PING_INTERVAL_TICKS means the same ~1.5 s
        // period in each and pings stay spread across clients by slot.
        let ping_bucket = tick_count % PING_INTERVAL_TICKS;
        for (slot, client) in snapshot.iter().enumerate() {
            if !ping_bucket_matches(slot, ping_bucket) {
                continue;
            }
            if client.in_use.load(Ordering::Relaxed) && client.connected.load(Ordering::Relaxed) {
                let _ = client.send_ping().await;
            }
        }
    }
}
