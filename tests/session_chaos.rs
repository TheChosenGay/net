use net::runtime::{NetRuntime, NetRuntimeConfig};
use net::session::{Session, SessionConfig};
use net::transport::packet::{Packet, PayloadType};
use std::collections::{HashMap, HashSet};
use std::net::{SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// ---------- 基础工具 ----------

fn free_addr() -> SocketAddr {
    UdpSocket::bind("127.0.0.1:0").unwrap().local_addr().unwrap()
}

fn leak_runtime(addr: SocketAddr) -> &'static NetRuntime {
    Box::leak(Box::new(
        NetRuntime::new(NetRuntimeConfig { local_addr: addr }).unwrap(),
    ))
}

fn make_session(local: SocketAddr, peer: SocketAddr, runtime: &'static NetRuntime) -> Session<'static> {
    Session::new(SessionConfig {
        local_addr: local,
        peer_addr: peer,
        runtime,
    })
}

fn data_packet(sequence: u64, payload_len: usize) -> Packet {
    let mut payload = vec![(sequence % 251) as u8; payload_len];
    payload[..8].copy_from_slice(&sequence.to_le_bytes());
    Packet::new(1, sequence, PayloadType::Bytes(payload)).unwrap()
}

fn payload_sequence(packet: &Packet) -> u64 {
    match &packet.payload {
        PayloadType::Bytes(bytes) => {
            u64::from_le_bytes(bytes[..8].try_into().unwrap())
        }
        other => panic!("期望完整字节包, 实际: {:?}", other),
    }
}

// ---------- 弱网代理 ----------

/// 极简确定性随机数，避免给测试加依赖。
struct SimpleRng(u64);

impl SimpleRng {
    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    fn roll_permille(&mut self, permille: u64) -> bool {
        self.next_u64() % 1000 < permille
    }
}

#[derive(Default)]
struct DirectionStats {
    seen: AtomicU64,
    dropped: AtomicU64,
    forwarded: AtomicU64,
    extra_copies: AtomicU64,
}

struct ChaosProxy {
    addr_a: SocketAddr,
    addr_b: SocketAddr,
    ab: Arc<DirectionStats>,
    ba: Arc<DirectionStats>,
    drop_all: Arc<AtomicBool>,
}

/// 双向 UDP 代理：A->B 与 B->A 各一条转发线程，独立做丢包/复制判定。
/// drop_permille / dup_permille 均为千分比，对数据包和 ACK 一视同仁。
fn spawn_chaos_proxy(
    real_a: SocketAddr,
    real_b: SocketAddr,
    drop_permille: u64,
    dup_permille: u64,
    seed: u64,
) -> ChaosProxy {
    let socket_a = UdpSocket::bind("127.0.0.1:0").unwrap();
    let socket_b = UdpSocket::bind("127.0.0.1:0").unwrap();
    let addr_a = socket_a.local_addr().unwrap();
    let addr_b = socket_b.local_addr().unwrap();
    let ab = Arc::new(DirectionStats::default());
    let ba = Arc::new(DirectionStats::default());
    let drop_all = Arc::new(AtomicBool::new(false));

    fn forward_loop(
        recv_socket: UdpSocket,
        send_socket: UdpSocket,
        dest: SocketAddr,
        drop_permille: u64,
        dup_permille: u64,
        seed: u64,
        stats: Arc<DirectionStats>,
        drop_all: Arc<AtomicBool>,
    ) {
        let mut rng = SimpleRng(seed | 1);
        let mut buf = [0u8; 2048];
        loop {
            let (len, _from) = match recv_socket.recv_from(&mut buf) {
                Ok(value) => value,
                Err(_) => continue,
            };
            stats.seen.fetch_add(1, Ordering::Relaxed);
            if drop_all.load(Ordering::Relaxed) {
                stats.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if rng.roll_permille(drop_permille) {
                stats.dropped.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let _ = send_socket.send_to(&buf[..len], dest);
            stats.forwarded.fetch_add(1, Ordering::Relaxed);
            if rng.roll_permille(dup_permille) {
                let _ = send_socket.send_to(&buf[..len], dest);
                stats.extra_copies.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    {
        let (recv_socket, send_socket) = (
            socket_a.try_clone().unwrap(),
            socket_b.try_clone().unwrap(),
        );
        let stats = Arc::clone(&ab);
        let drop_all = Arc::clone(&drop_all);
        std::thread::spawn(move || {
            forward_loop(recv_socket, send_socket, real_b, drop_permille, dup_permille, seed, stats, drop_all)
        });
    }
    {
        let stats = Arc::clone(&ba);
        let drop_all = Arc::clone(&drop_all);
        std::thread::spawn(move || {
            forward_loop(socket_b, socket_a, real_a, drop_permille, dup_permille, seed ^ 0x9E3779B97F4A7C15, stats, drop_all)
        });
    }

    ChaosProxy {
        addr_a,
        addr_b,
        ab,
        ba,
        drop_all,
    }
}

// ---------- 收包收集 ----------

struct CollectResult {
    received: HashMap<u64, Packet>,
    app_duplicates: usize,
    elapsed: Duration,
}

/// 从 session 收齐 expected 里的序号；记录应用层是否收到重复投递。
fn collect(session: &Session, expected: &[u64], timeout: Duration) -> CollectResult {
    let start = Instant::now();
    let deadline = start + timeout;
    let expected_set: HashSet<u64> = expected.iter().copied().collect();
    let mut received = HashMap::new();
    let mut app_duplicates = 0usize;

    while received.len() < expected_set.len() {
        match session.recv() {
            Some(packet) => {
                assert!(!packet.is_ack(), "应用层不应收到 ACK 包");
                let seq = payload_sequence(&packet);
                assert_eq!(seq, packet.sequence);
                if !expected_set.contains(&seq) {
                    continue;
                }
                if received.insert(seq, packet).is_some() {
                    app_duplicates += 1;
                }
            }
            None => {
                assert!(
                    Instant::now() < deadline,
                    "弱网收包超时: 期望 {} 个, 只收到 {} 个",
                    expected_set.len(),
                    received.len()
                );
                std::thread::sleep(Duration::from_millis(1));
            }
        }
    }

    CollectResult {
        received,
        app_duplicates,
        elapsed: start.elapsed(),
    }
}

fn run_reliability_case(drop_permille: u64, dup_permille: u64, count: u64, seed: u64) {
    run_reliability_case_with_payload(drop_permille, dup_permille, count, 64, seed)
}

fn run_reliability_case_with_payload(
    drop_permille: u64,
    dup_permille: u64,
    count: u64,
    payload_len: usize,
    seed: u64,
) {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let proxy = spawn_chaos_proxy(addr_a, addr_b, drop_permille, dup_permille, seed);
    let session_a = make_session(addr_a, proxy.addr_a, leak_runtime(addr_a));
    let session_b = make_session(addr_b, proxy.addr_b, leak_runtime(addr_b));

    // A -> B
    let seqs_ab: Vec<u64> = (1..=count).collect();
    for &seq in &seqs_ab {
        session_a.send(data_packet(seq, payload_len));
    }
    let result_ab = collect(&session_b, &seqs_ab, Duration::from_secs(30));
    assert_eq!(result_ab.received.len() as u64, count);
    assert_eq!(result_ab.app_duplicates, 0, "应用层收到了重复投递");

    // B -> A（序号段错开，避免与上一段混淆）
    let seqs_ba: Vec<u64> = (10_001..=10_000 + count).collect();
    for &seq in &seqs_ba {
        session_b.send(data_packet(seq, payload_len));
    }
    let result_ba = collect(&session_a, &seqs_ba, Duration::from_secs(30));
    assert_eq!(result_ba.received.len() as u64, count);
    assert_eq!(result_ba.app_duplicates, 0, "应用层收到了重复投递");

    println!(
        "case drop={}‰ dup={}‰ n={} | A->B {:?} B->A {:?} | proxy A->B seen={} dropped={} extra_dup={} | proxy B->A seen={} dropped={} extra_dup={}",
        drop_permille,
        dup_permille,
        count,
        result_ab.elapsed,
        result_ba.elapsed,
        proxy.ab.seen.load(Ordering::Relaxed),
        proxy.ab.dropped.load(Ordering::Relaxed),
        proxy.ab.extra_copies.load(Ordering::Relaxed),
        proxy.ba.seen.load(Ordering::Relaxed),
        proxy.ba.dropped.load(Ordering::Relaxed),
        proxy.ba.extra_copies.load(Ordering::Relaxed),
    );
}

#[test]
fn chaos_moderate_loss_and_duplicates() {
    run_reliability_case(300, 200, 200, 0xA5A5_1234);
}

#[test]
fn chaos_severe_loss_and_duplicates() {
    run_reliability_case(700, 300, 100, 0x5A5A_9876);
}

#[test]
fn chaos_large_packets_moderate_loss() {
    // 每条 5000 字节的消息会被拆成多个分片，在丢包/重复环境下验证组包交付
    run_reliability_case_with_payload(300, 200, 5, 5000, 0x1A2B_3C4D);
}

#[test]
fn chaos_large_packets_severe_loss() {
    run_reliability_case_with_payload(700, 300, 3, 5000, 0x4D3C_2B1A);
}

#[test]
#[ignore]
fn chaos_10mb_packet_moderate_loss() {
    // 10MiB 单条消息约一万个分片，手动运行观察弱网下的组包效果
    run_reliability_case_with_payload(300, 200, 1, 10 * 1024 * 1024, 0x10AB_CDEF);
}

#[test]
#[ignore]
fn chaos_10mb_packet_severe_loss() {
    run_reliability_case_with_payload(700, 300, 1, 10 * 1024 * 1024, 0x70AB_CDEF);
}

#[test]
fn chaos_blackout_then_recover() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let proxy = spawn_chaos_proxy(addr_a, addr_b, 0, 0, 0xB1AC);
    // 从一开始就全丢，模拟持续断网
    proxy.drop_all.store(true, Ordering::Relaxed);
    let session_a = make_session(addr_a, proxy.addr_a, leak_runtime(addr_a));
    let session_b = make_session(addr_b, proxy.addr_b, leak_runtime(addr_b));

    let seqs: Vec<u64> = (1..=20).collect();
    for &seq in &seqs {
        session_a.send(data_packet(seq, 64));
    }

    // 断网持续 2 秒后恢复；可靠传输应在恢复后把包补齐
    std::thread::sleep(Duration::from_secs(2));
    proxy.drop_all.store(false, Ordering::Relaxed);

    let result = collect(&session_b, &seqs, Duration::from_secs(8));
    assert_eq!(result.received.len(), seqs.len());
    assert_eq!(result.app_duplicates, 0);
    println!(
        "blackout recovered in {:?} after link restore, proxy seen={}",
        result.elapsed,
        proxy.ab.seen.load(Ordering::Relaxed)
    );
}

// ---------- 性能（手动运行: cargo test --release --test session_chaos -- --ignored --nocapture） ----------

fn wait_one(session: &Session, timeout: Duration) -> Packet {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(packet) = session.recv() {
            return packet;
        }
        assert!(Instant::now() < deadline, "等待收包超时");
        std::thread::sleep(Duration::from_micros(100));
    }
}

#[test]
#[ignore]
fn perf_direct_rtt() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let session_a = make_session(addr_a, addr_b, leak_runtime(addr_a));
    let session_b = make_session(addr_b, addr_a, leak_runtime(addr_b));

    const ROUNDS: u64 = 200;
    let mut latencies = Vec::new();
    for i in 0..ROUNDS {
        let start = Instant::now();
        session_a.send(data_packet(30_000 + i, 64));
        let ping = wait_one(&session_b, Duration::from_secs(5));
        session_b.send(data_packet(40_000 + i, 64));
        let pong = wait_one(&session_a, Duration::from_secs(5));
        assert_eq!(payload_sequence(&ping), 30_000 + i);
        assert_eq!(payload_sequence(&pong), 40_000 + i);
        latencies.push(start.elapsed());
    }
    latencies.sort();
    let total: Duration = latencies.iter().sum();
    println!(
        "RTT rounds={} avg={:?} p50={:?} p95={:?} max={:?}",
        ROUNDS,
        total / ROUNDS as u32,
        latencies[ROUNDS as usize / 2],
        latencies[(ROUNDS as usize * 95) / 100],
        latencies[ROUNDS as usize - 1],
    );
}

#[test]
#[ignore]
fn perf_direct_throughput() {
    let addr_a = free_addr();
    let addr_b = free_addr();
    let session_a = make_session(addr_a, addr_b, leak_runtime(addr_a));
    let session_b = make_session(addr_b, addr_a, leak_runtime(addr_b));

    const TOTAL: u64 = 20_000;
    const CHUNK: u64 = 256;
    const PAYLOAD_LEN: usize = 256;

    let start = Instant::now();
    let mut sent = 0u64;
    let mut received = HashSet::new();
    while sent < TOTAL {
        let chunk_end = (sent + CHUNK).min(TOTAL);
        for seq in (sent + 1)..=chunk_end {
            session_a.send(data_packet(seq, PAYLOAD_LEN));
        }
        sent = chunk_end;

        // 等接收端追平再发下一批，避免把邮箱容量当成丢包因素混进吞吐结果
        let deadline = Instant::now() + Duration::from_secs(10);
        while (received.len() as u64) < sent {
            match session_b.recv() {
                Some(packet) => {
                    received.insert(packet.sequence);
                }
                None => {
                    assert!(Instant::now() < deadline, "吞吐测试收包超时");
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
        }
    }
    let elapsed = start.elapsed();
    let pps = TOTAL as f64 / elapsed.as_secs_f64();
    let mbps = pps * PAYLOAD_LEN as f64 / 1_000_000.0;
    println!(
        "throughput packets={} payload={}B elapsed={:?} -> {:.0} pkt/s, {:.2} MB/s",
        TOTAL, PAYLOAD_LEN, elapsed, pps, mbps
    );
}
