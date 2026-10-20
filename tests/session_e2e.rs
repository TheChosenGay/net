use net::runtime::{NetRuntime, NetRuntimeConfig};
use net::session::{Session, SessionConfig};
use net::transport::packet::Packet;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

/// 申请一个空闲的本地 UDP 地址（先占位取端口再释放，短时间内被抢占的概率可忽略）。
fn free_addr() -> SocketAddr {
    UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

/// NetRuntime 需要 &'static 才能构造 Session，测试里用 Box::leak 换取进程级生命周期。
fn leak_runtime(addr: SocketAddr) -> &'static NetRuntime {
    let runtime = NetRuntime::new(NetRuntimeConfig { local_addr: addr }).unwrap();
    Box::leak(Box::new(runtime))
}

fn make_session(local: SocketAddr, peer: SocketAddr, runtime: &'static NetRuntime) -> Session<'static> {
    Session::new(SessionConfig {
        local_addr: local,
        peer_addr: peer,
        runtime,
    })
}

fn wait_recv(session: &Session, timeout: Duration) -> Packet {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(packet) = session.recv() {
            return packet;
        }
        assert!(Instant::now() < deadline, "等待 Session 收包超时");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn recv_raw(socket: &UdpSocket, buf: &mut [u8]) -> Packet {
    let (len, _from) = socket.recv_from(buf).expect("等待裸 UDP 包超时");
    Packet::decode(&buf[..len]).expect("裸 UDP 包解码失败")
}

#[test]
fn session_bidirectional_send_recv() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let session_a = make_session(addr_a, addr_b, leak_runtime(addr_a));
    let session_b = make_session(addr_b, addr_a, leak_runtime(addr_b));

    // A -> B
    session_a
        .send(Packet::new(1, 1001, b"ping".to_vec()).unwrap());
    let got = wait_recv(&session_b, Duration::from_secs(3));
    assert_eq!(got.sequence, 1001);
    assert_eq!(got.payload, b"ping");
    assert!(!got.is_ack());

    // B -> A
    session_b
        .send(Packet::new(1, 2002, b"pong".to_vec()).unwrap());
    let got = wait_recv(&session_a, Duration::from_secs(3));
    assert_eq!(got.sequence, 2002);
    assert_eq!(got.payload, b"pong");
    assert!(!got.is_ack());
}

#[test]
fn session_ack_with_raw_peer() {
    let addr_session = free_addr();

    // 裸 UDP 对端：先绑定拿到真实地址，再据此构造 Session，保证 peer 地址一致。
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr_raw = raw.local_addr().unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let session = make_session(addr_session, addr_raw, leak_runtime(addr_session));
    let mut buf = [0u8; 2048];

    // 1) 对端发数据包给 Session，应收到 Session 回的 ACK（seq 一致、is_ack 置位）。
    let inbound = Packet::new(1, 77, b"hello".to_vec()).unwrap();
    raw.send_to(&inbound.encode(), addr_session).unwrap();

    let got = wait_recv(&session, Duration::from_secs(3));
    assert_eq!(got.sequence, 77);
    assert_eq!(got.payload, b"hello");

    let ack = recv_raw(&raw, &mut buf);
    assert!(ack.is_ack(), "Session 应回 ACK 包，实际收到: {:?}", ack);
    assert_eq!(ack.sequence, 77);

    // 2) Session 发数据包，对端回 ACK 后，Session 不应再重传该包。
    session.send(Packet::new(1, 88, b"ackme".to_vec()).unwrap());
    let data = recv_raw(&raw, &mut buf);
    assert!(!data.is_ack());
    assert_eq!(data.sequence, 88);
    assert_eq!(data.payload, b"ackme");

    // 回 ACK；先给在途的重复包留一个排空窗口，再观察静默期。
    raw.send_to(&Packet::new_ack(1, 88).encode(), addr_session)
        .unwrap();
    std::thread::sleep(Duration::from_millis(150));
    raw.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
    while raw.recv_from(&mut buf).is_ok() {}

    raw.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
    if let Ok((len, _)) = raw.recv_from(&mut buf) {
        let extra = Packet::decode(&buf[..len]).unwrap();
        assert!(
            extra.is_ack() || extra.sequence != 88,
            "ACK 生效后不应再重传 seq=88 的数据包，实际又收到: {:?}",
            extra
        );
    }
}
