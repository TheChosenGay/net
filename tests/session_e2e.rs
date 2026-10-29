use net::runtime::{NetRuntime, NetRuntimeConfig};
use net::session::{Session, SessionConfig, SessionState};
use net::transport::packet::{Packet, PayloadType};
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
        // 本组测试只测传输层可靠性，绕过握手，会话直接以已连接状态创建
        init_session_state: SessionState::Connected,
    })
}

fn wait_recv(session: &mut Session, timeout: Duration) -> Packet {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(packet) = session.recv() {
            return packet;
        }
        assert!(Instant::now() < deadline, "等待 Session 收包超时");
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn bytes_packet(sequence: u64, bytes: Vec<u8>) -> Packet {
    Packet::new(1, sequence, PayloadType::Bytes(bytes)).unwrap()
}

fn payload_bytes(packet: &Packet) -> Vec<u8> {
    match &packet.payload {
        PayloadType::Bytes(bytes) => bytes.clone(),
        other => panic!("期望完整字节包, 实际: {:?}", other),
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
    let mut session_a = make_session(addr_a, addr_b, leak_runtime(addr_a));
    let mut session_b = make_session(addr_b, addr_a, leak_runtime(addr_b));

    // A -> B
    session_a.send(bytes_packet(1001, b"ping".to_vec()));
    let got = wait_recv(&mut session_b, Duration::from_secs(3));
    assert_eq!(got.sequence, 1001);
    assert_eq!(payload_bytes(&got), b"ping");
    assert!(!got.is_ack());

    // B -> A
    session_b.send(bytes_packet(2002, b"pong".to_vec()));
    let got = wait_recv(&mut session_a, Duration::from_secs(3));
    assert_eq!(got.sequence, 2002);
    assert_eq!(payload_bytes(&got), b"pong");
    assert!(!got.is_ack());
}

#[test]
fn session_ack_with_raw_peer() {
    let addr_session = free_addr();

    // 裸 UDP 对端：先绑定拿到真实地址，再据此构造 Session，保证 peer 地址一致。
    let raw = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr_raw = raw.local_addr().unwrap();
    raw.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut session = make_session(addr_session, addr_raw, leak_runtime(addr_session));
    let mut buf = [0u8; 2048];

    // Session 在传输层之上还有一层会话封装：数据包在线上是 [0x02 标签] + 原始字节
    // （见 SessionPacketType::Data），裸对端要按这个格式构造和解析。
    const SESSION_DATA_TAG: u8 = 0x02;
    let framed = |bytes: &[u8]| -> Vec<u8> {
        let mut framed = vec![SESSION_DATA_TAG];
        framed.extend_from_slice(bytes);
        framed
    };

    // 1) 对端发数据包给 Session，应收到 Session 回的 ACK（seq 一致、is_ack 置位）。
    // 裸对端没有传输层帮它重传，而 Session 的通道注册是异步的，首包可能赶在注册
    // 生效前到达被丢弃，所以像真实发送方一样重传，直到 Session 收到为止。
    let inbound = bytes_packet(77, framed(b"hello"));
    let got = {
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            raw.send_to(&inbound.encode(), addr_session).unwrap();
            let poll_deadline = Instant::now() + Duration::from_millis(100);
            let mut found = None;
            while Instant::now() < poll_deadline {
                if let Some(packet) = session.recv() {
                    found = Some(packet);
                    break;
                }
                std::thread::sleep(Duration::from_millis(2));
            }
            if let Some(packet) = found {
                break packet;
            }
            assert!(Instant::now() < deadline, "等待 Session 收包超时");
        }
    };
    assert_eq!(got.sequence, 77);
    assert_eq!(payload_bytes(&got), b"hello");

    let ack = recv_raw(&raw, &mut buf);
    assert!(ack.is_ack(), "Session 应回 ACK 包，实际收到: {:?}", ack);
    assert_eq!(ack.sequence, 77);

    // 重传可能让对端收到多份 ACK，排空后再进入下一步，避免读到残留 ACK
    raw.set_read_timeout(Some(Duration::from_millis(100))).unwrap();
    while raw.recv_from(&mut buf).is_ok() {}
    raw.set_read_timeout(Some(Duration::from_secs(3))).unwrap();

    // 2) Session 发数据包，对端回 ACK 后，Session 不应再重传该包。
    session.send(bytes_packet(88, b"ackme".to_vec()));
    let data = recv_raw(&raw, &mut buf);
    assert!(!data.is_ack());
    assert_eq!(data.sequence, 88);
    assert_eq!(payload_bytes(&data), framed(b"ackme"));

    // 回 ACK（携带目标包的 128 位 key）；先给在途的重复包留一个排空窗口，再观察静默期。
    raw.send_to(&Packet::new_ack(1, data.key()).encode(), addr_session)
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

#[test]
#[ignore]
fn session_10mb_packet_fragmented_and_merged() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let session_a = make_session(addr_a, addr_b, leak_runtime(addr_a));
    let mut session_b = make_session(addr_b, addr_a, leak_runtime(addr_b));

    // 10MiB 会被拆成约一万个分片，手动运行观察耗时与完整性
    let data: Vec<u8> = (0..10 * 1024 * 1024u32).map(|i| (i % 251) as u8).collect();
    let start = Instant::now();
    session_a.send(bytes_packet(600, data.clone()));

    let got = wait_recv(&mut session_b, Duration::from_secs(180));
    let elapsed = start.elapsed();
    assert_eq!(got.sequence, 600);
    let received = payload_bytes(&got);
    assert_eq!(received.len(), data.len());
    if let Some(index) = received
        .iter()
        .zip(data.iter())
        .position(|(actual, expected)| actual != expected)
    {
        panic!("10MiB 数据在下标 {} 处不一致", index);
    }
    println!(
        "10MiB fragmented transfer elapsed={:?}, throughput={:.2} MiB/s",
        elapsed,
        data.len() as f64 / 1024.0 / 1024.0 / elapsed.as_secs_f64()
    );
}

#[test]
fn session_large_packet_fragmented_and_merged() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let session_a = make_session(addr_a, addr_b, leak_runtime(addr_a));
    let mut session_b = make_session(addr_b, addr_a, leak_runtime(addr_b));

    // 5000 字节必然被拆成多个分片，接收端应组包后完整交付
    let data: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
    session_a.send(bytes_packet(500, data.clone()));

    let got = wait_recv(&mut session_b, Duration::from_secs(5));
    assert_eq!(got.sequence, 500);
    assert!(!got.is_fragment());
    assert_eq!(payload_bytes(&got), data);
}
