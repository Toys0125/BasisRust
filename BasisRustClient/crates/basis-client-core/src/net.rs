#[cfg(any(windows, test))]
use crate::transport::LITENETLIB_MAX_MTU;
use crate::transport::{SOCKET_BUFFER_SIZE, SOCKET_TTL};
use anyhow::{anyhow, Result};
use basis_transport::PacketProperty;
#[cfg(any(windows, test))]
use basis_transport::LITENETLIB_INITIAL_MTU;
use socket2::{Domain, Protocol, Socket, Type};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs};
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
use tokio::net::UdpSocket;

pub(crate) fn resolve_addr(ip: &str, port: u16) -> Result<SocketAddr> {
    (ip, port)
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| anyhow!("failed to resolve {ip}:{port}"))
}

pub(crate) fn any_local_addr(remote: SocketAddr) -> SocketAddr {
    match remote {
        SocketAddr::V4(_) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0),
        SocketAddr::V6(_) => SocketAddr::new(IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED), 0),
    }
}

#[cfg(any(windows, test))]
pub(crate) fn windows_mtu_probe_reply(packet: &[u8], connection_number: u8) -> Option<Vec<u8>> {
    // Match Linux's shared receiver: only echo a complete LiteNetLib probe so
    // the server can safely increase its per-peer datagram packing limit.
    if !(LITENETLIB_INITIAL_MTU..=LITENETLIB_MAX_MTU).contains(&packet.len())
        || packet[0] != (PacketProperty::MtuCheck as u8 | (connection_number << 5))
    {
        return None;
    }
    let mtu = i32::from_le_bytes(packet[1..5].try_into().ok()?);
    if usize::try_from(mtu).ok() != Some(packet.len())
        || packet[13..packet.len() - 4].iter().any(|&byte| byte != 0)
        || packet[packet.len() - 4..] != packet[1..5]
    {
        return None;
    }
    let mut reply = packet.to_vec();
    reply[0] = (reply[0] & 0xe0) | PacketProperty::MtuOk as u8;
    Some(reply)
}

pub(crate) fn bind_udp_socket(addr: SocketAddr) -> std::io::Result<UdpSocket> {
    let domain = match addr {
        SocketAddr::V4(_) => Domain::IPV4,
        SocketAddr::V6(_) => Domain::IPV6,
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
    if matches!(addr, SocketAddr::V6(_)) {
        let _ = socket.set_only_v6(false);
    }
    let _ = socket.set_recv_buffer_size(SOCKET_BUFFER_SIZE);
    let _ = socket.set_send_buffer_size(SOCKET_BUFFER_SIZE);
    let _ = socket.set_ttl(SOCKET_TTL);
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    UdpSocket::from_std(socket.into())
}

#[cfg(target_os = "linux")]
pub(crate) fn configure_load_sink_socket(socket: &UdpSocket) -> std::io::Result<()> {
    // Drop only standalone unreliable fanout for non-observer load sinks.
    // Both merged formats can also carry raw reliable messages and ACKs, so
    // userspace must retain their control entries before discarding bulk data.
    // The connected UDP socket's filter view includes its 8-byte UDP header.
    const BPF_LD_B_ABS: u16 = 0x30;
    const BPF_ALU_AND_K: u16 = 0x54;
    const BPF_JMP_JEQ_K: u16 = 0x15;
    const BPF_RET_K: u16 = 0x06;
    const ACCEPT_ALL: u32 = u32::MAX;

    let mut filters = [
        libc::sock_filter {
            code: BPF_LD_B_ABS,
            jt: 0,
            jf: 0,
            k: 8,
        },
        libc::sock_filter {
            code: BPF_ALU_AND_K,
            jt: 0,
            jf: 0,
            k: 0x1f,
        },
        libc::sock_filter {
            code: BPF_JMP_JEQ_K,
            jt: 1,
            jf: 0,
            k: PacketProperty::Unreliable as u32,
        },
        libc::sock_filter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: ACCEPT_ALL,
        },
        libc::sock_filter {
            code: BPF_RET_K,
            jt: 0,
            jf: 0,
            k: 0,
        },
    ];
    let program = libc::sock_fprog {
        len: filters.len() as u16,
        filter: filters.as_mut_ptr(),
    };
    let fd = socket.as_raw_fd();
    let rc = unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ATTACH_FILTER,
            (&program as *const libc::sock_fprog).cast(),
            std::mem::size_of::<libc::sock_fprog>() as libc::socklen_t,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }

    // Once bulk unreliable receive is filtered, synthetic peers do not need multi-megabyte
    // socket queues. Keep enough room for bursts of ACK/auth/control traffic while reducing
    // kernel memory pressure for 1000 sockets.
    let buffer_bytes: libc::c_int = 64 * 1024;
    for option in [libc::SO_RCVBUF, libc::SO_SNDBUF] {
        let rc = unsafe {
            libc::setsockopt(
                fd,
                libc::SOL_SOCKET,
                option,
                (&buffer_bytes as *const libc::c_int).cast(),
                std::mem::size_of_val(&buffer_bytes) as libc::socklen_t,
            )
        };
        if rc != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn configure_load_sink_socket(_socket: &UdpSocket) -> std::io::Result<()> {
    Ok(())
}

pub(crate) fn socket_address_bytes(addr: SocketAddr) -> Vec<u8> {
    match addr {
        SocketAddr::V4(v4) => {
            let mut bytes = vec![0u8; 16];
            bytes[0] = 2;
            bytes[1] = 0;
            bytes[2..4].copy_from_slice(&v4.port().to_be_bytes());
            bytes[4..8].copy_from_slice(&v4.ip().octets());
            bytes
        }
        SocketAddr::V6(v6) => {
            let mut bytes = vec![0u8; 28];
            bytes[0] = 23;
            bytes[1] = 0;
            bytes[2..4].copy_from_slice(&v6.port().to_be_bytes());
            bytes[8..24].copy_from_slice(&v6.ip().octets());
            bytes
        }
    }
}
