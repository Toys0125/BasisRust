use super::*;
use tokio::time::timeout;

#[test]
fn nat_endpoint_serialization_uses_canonical_address_family() {
    for (input, expected) in [
        ("127.0.0.1:12345", "127.0.0.1:12345"),
        ("[::ffff:192.0.2.1]:54321", "192.0.2.1:54321"),
        ("[2001:db8::1]:23456", "[2001:db8::1]:23456"),
    ] {
        let expected: SocketAddr = expected.parse().unwrap();
        let mut packet = Vec::new();
        write_litenet_endpoint(&mut packet, input.parse().unwrap());
        assert_eq!(packet[0], u8::from(expected.is_ipv6()));
        assert_eq!(packet.len(), if expected.is_ipv4() { 7 } else { 19 });
        let mut position = 0;
        assert_eq!(
            read_litenet_endpoint(&packet, &mut position),
            Some(expected)
        );
        assert_eq!(position, packet.len());
    }
}

async fn exchange_nat_introductions(server_bind: &str, client_bind: &str) {
    let (server, mut events) = TransportHandle::bind(server_bind.parse().unwrap())
        .await
        .unwrap();
    let a = UdpSocket::bind(client_bind).await.unwrap();
    let b = UdpSocket::bind(client_bind).await.unwrap();
    let destination = SocketAddr::new(
        a.local_addr().unwrap().ip(),
        server.local_addr().unwrap().port(),
    );
    let token = "nat-address-family-test";
    let mut observed = Vec::new();
    for client in [&a, &b] {
        let client_addr = client.local_addr().unwrap();
        let mut request = vec![PacketProperty::NatMessage as u8];
        request.extend_from_slice(&NAT_INTRODUCE_REQUEST_HASH.to_le_bytes());
        write_litenet_endpoint(&mut request, client_addr);
        write_litenet_string(&mut request, token);
        client.send_to(&request, destination).await.unwrap();
        let event = timeout(Duration::from_secs(2), events.recv())
            .await
            .unwrap()
            .unwrap();
        let ServerEvent::NatIntroductionRequest {
            local_addr,
            remote_addr,
            token: received_token,
        } = event
        else {
            panic!("expected NAT introduction request");
        };
        assert_eq!(received_token, token);
        assert_eq!(local_addr, client_addr);
        // Keep the socket's mapped endpoint for server sends and broker bookkeeping.
        let expected_remote = if server.local_addr().unwrap().is_ipv6() {
            match client_addr {
                SocketAddr::V4(addr) => {
                    SocketAddr::new(IpAddr::V6(addr.ip().to_ipv6_mapped()), addr.port())
                }
                SocketAddr::V6(_) => client_addr,
            }
        } else {
            client_addr
        };
        assert_eq!(remote_addr, expected_remote);
        observed.push((local_addr, remote_addr));
    }

    server
        .send_nat_introduce(
            observed[0].0,
            observed[0].1,
            7,
            observed[1].0,
            observed[1].1,
            13,
            token,
        )
        .await
        .unwrap();
    for (client, other, prediction_count) in [(&a, &b, 13), (&b, &a, 7)] {
        let mut packet = [0; 512];
        let (len, _) = timeout(Duration::from_secs(2), client.recv_from(&mut packet))
            .await
            .expect("NAT introduction not sent")
            .unwrap();
        assert_eq!(packet[0], PacketProperty::NatMessage as u8);
        assert_eq!(&packet[1..9], &NAT_INTRODUCE_RESPONSE_HASH.to_le_bytes());
        let mut position = 9;
        let expected = other.local_addr().unwrap();
        for _ in 0..2 {
            assert_eq!(packet[position], u8::from(expected.is_ipv6()));
            assert_eq!(
                read_litenet_endpoint(&packet[..len], &mut position),
                Some(expected)
            );
        }
        assert_eq!(
            read_litenet_string(&packet[..len], &mut position).as_deref(),
            Some(token)
        );
        assert_eq!(&packet[position..len], &[prediction_count]);
    }
    server.shutdown();
}

#[tokio::test]
async fn nat_dual_stack_server_advertises_ipv4_for_ipv4_clients() {
    exchange_nat_introductions("[::]:0", "127.0.0.1:0").await;
}

#[tokio::test]
async fn nat_ipv4_server_advertises_ipv4_for_ipv4_clients() {
    exchange_nat_introductions("0.0.0.0:0", "127.0.0.1:0").await;
}

#[tokio::test]
async fn nat_dual_stack_server_preserves_native_ipv6_clients() {
    exchange_nat_introductions("[::]:0", "[::1]:0").await;
}
