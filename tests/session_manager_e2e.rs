use net::runtime::{NetRuntime, NetRuntimeConfig};
use net::session::{Session, SessionConfig, SessionState};
use net::session_manager::SessionManager;
use net::transport::packet::{Packet, PayloadType};
use std::net::{SocketAddr, UdpSocket};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// 申请一个空闲的本地 UDP 地址（先占位取端口再释放，短时间内被抢占的概率可忽略）。
fn free_addr() -> SocketAddr {
    UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

/// Session / SessionManager 借用 NetRuntime，测试里用 Box::leak 换取进程级生命周期。
fn leak_runtime(addr: SocketAddr) -> &'static NetRuntime {
    let runtime = NetRuntime::new(NetRuntimeConfig { local_addr: addr }).unwrap();
    Box::leak(Box::new(runtime))
}

type Collected = Arc<Mutex<Vec<Packet>>>;

/// 服务端只有一个 SessionManager：监听新连接，并把各会话的数据汇聚到收集器。
fn make_server(runtime: &'static NetRuntime) -> (SessionManager<'static>, Collected) {
    let collected: Collected = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&collected);
    let manager = SessionManager::new(
        runtime,
        Box::new(move |packets: Vec<Packet>| {
            sink.lock().unwrap().extend(packets);
        }),
    );
    (manager, collected)
}

fn payload_bytes(packet: &Packet) -> Vec<u8> {
    match &packet.payload {
        PayloadType::Bytes(bytes) => bytes.clone(),
        other => panic!("期望完整字节包, 实际: {:?}", other),
    }
}

/// 交替驱动客户端 Session 与服务端 Manager，直到双方都进入 Connected 或超时。
fn drive_until_connected(
    client: &mut Session,
    server: &mut SessionManager,
    client_addr: SocketAddr,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        // 客户端没有数据要取时 recv_all 只推进握手状态机
        let _ = client.recv_all();
        server.exec_recv();
        if client.state() == SessionState::Connected
            && server.session_state(client_addr) == Some(SessionState::Connected)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "握手超时: client={:?}, server={:?}",
            client.state(),
            server.session_state(client_addr)
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// 交替驱动两端，直到服务端经理收到期望 sequence 的数据包。
fn drive_until_server_received(
    client: &mut Session,
    server: &mut SessionManager,
    collected: &Collected,
    sequence: u64,
) -> Packet {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let _ = client.recv_all();
        server.exec_recv();
        if let Some(packet) = collected
            .lock()
            .unwrap()
            .iter()
            .find(|packet| packet.sequence == sequence)
        {
            return packet.clone();
        }
        assert!(Instant::now() < deadline, "等待服务端收 seq={} 超时", sequence);
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn client_session_connects_to_server_manager_and_exchanges_data() {
    let client_addr = free_addr();
    let server_addr = free_addr();
    let (mut server, server_received) = make_server(leak_runtime(server_addr));

    // 客户端只用一个 Session 主动连接服务端
    let mut client = Session::new(SessionConfig {
        local_addr: client_addr,
        peer_addr: server_addr,
        runtime: leak_runtime(client_addr),
        init_session_state: SessionState::Disconnected,
    });
    client.connect_to(client_addr);

    // 服务端先只登记待握手项：此时不应创建正式 Session，避免未知请求直接占用会话资源
    server.exec_recv();
    assert_eq!(server.session_state(client_addr), None);

    drive_until_connected(&mut client, &mut server, client_addr);

    // 客户端 -> 服务端：数据经经理的回调汇聚
    let packet = Packet::new(1, 1001, PayloadType::Bytes(b"ping".to_vec())).unwrap();
    client.send(packet);
    let deadline = Instant::now() + Duration::from_secs(5);
    let got = loop {
        let _ = client.recv_all();
        server.exec_recv();
        if let Some(packet) = server_received
            .lock()
            .unwrap()
            .iter()
            .find(|packet| packet.sequence == 1001)
        {
            break packet.clone();
        }
        assert!(Instant::now() < deadline, "等待服务端收包超时");
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(payload_bytes(&got), b"ping");

    // 服务端 -> 客户端：经理按对端地址找到会话下发，客户端直接收
    let packet = Packet::new(1, 2002, PayloadType::Bytes(b"pong".to_vec())).unwrap();
    assert!(server.send_to(client_addr, packet));
    let deadline = Instant::now() + Duration::from_secs(5);
    let got = loop {
        server.exec_recv();
        if let Some(packets) = client.recv_all() {
            if let Some(packet) = packets.into_iter().find(|packet| packet.sequence == 2002) {
                break packet;
            }
        }
        assert!(Instant::now() < deadline, "等待客户端收包超时");
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(payload_bytes(&got), b"pong");
}

#[test]
fn disconnect_then_reconnect_and_resume_messaging() {
    let client_addr = free_addr();
    let server_addr = free_addr();
    let (mut server, server_received) = make_server(leak_runtime(server_addr));

    let mut client = Session::new(SessionConfig {
        local_addr: client_addr,
        peer_addr: server_addr,
        runtime: leak_runtime(client_addr),
        init_session_state: SessionState::Disconnected,
    });
    client.connect_to(client_addr);
    drive_until_connected(&mut client, &mut server, client_addr);

    // 1) 连接后先正常发一条消息
    let packet = Packet::new(1, 1001, PayloadType::Bytes(b"before-disconnect".to_vec())).unwrap();
    client.send(packet);
    let got = drive_until_server_received(&mut client, &mut server, &server_received, 1001);
    assert_eq!(payload_bytes(&got), b"before-disconnect");

    // 2) 中途主动断开：服务端应感知并清理掉旧会话
    client.disconnect();
    assert_eq!(client.state(), SessionState::Disconnected);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let _ = client.recv_all();
        server.exec_recv();
        if server.session_state(client_addr) == None {
            break;
        }
        assert!(Instant::now() < deadline, "等待服务端清理旧会话超时");
        std::thread::sleep(Duration::from_millis(5));
    }

    // 3) 断开期间发出的消息不应被投递
    let packet = Packet::new(1, 3001, PayloadType::Bytes(b"while-disconnected".to_vec())).unwrap();
    client.send(packet);
    for _ in 0..20 {
        let _ = client.recv_all();
        server.exec_recv();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert!(
        server_received.lock().unwrap().iter().all(|packet| packet.sequence != 3001),
        "断开期间的消息不应被投递"
    );

    // 4) 同一个 Session 再次发起连接请求，应能重新建链
    client.connect_to(client_addr);
    drive_until_connected(&mut client, &mut server, client_addr);

    // 5) 重连后恢复双向发消息
    let packet = Packet::new(1, 1002, PayloadType::Bytes(b"after-reconnect".to_vec())).unwrap();
    client.send(packet);
    let got = drive_until_server_received(&mut client, &mut server, &server_received, 1002);
    assert_eq!(payload_bytes(&got), b"after-reconnect");

    let packet = Packet::new(1, 2002, PayloadType::Bytes(b"welcome-back".to_vec())).unwrap();
    assert!(server.send_to(client_addr, packet));
    let deadline = Instant::now() + Duration::from_secs(5);
    let got = loop {
        server.exec_recv();
        if let Some(packets) = client.recv_all() {
            if let Some(packet) = packets.into_iter().find(|packet| packet.sequence == 2002) {
                break packet;
            }
        }
        assert!(Instant::now() < deadline, "等待客户端重连后收包超时");
        std::thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(payload_bytes(&got), b"welcome-back");
}
