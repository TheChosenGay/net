use crate::runtime::DEFAULT_SOURCE_ADDR;
use crate::transport::{ITransport, default_transport_pair};
use std::collections::HashMap;
use crate::transport::packet::{PayloadType, PROTOCOL_VERSION};
use crate::runtime::NetRuntime;
use std::net::SocketAddr;
use crate::session::{Session, SessionState, SessionConfig, SessionConnector, SessionPacketType};
use crate::connect::ControlPacketType;
use std::time::{Duration, Instant};

use crate::transport::packet::Packet;

/// 待握手连接的上限：超过后直接丢弃新的 Request，防止未知请求把内存撑满。
pub const MAX_PENDING_CONNECTIONS: usize = 512;
/// 待握手连接的存活时间：超时未完成握手的直接清理。
pub const PENDING_TIMEOUT: Duration = Duration::from_secs(10);

pub struct SessionManager<'a> {
    session_map: HashMap<SocketAddr, Session<'a>>,
    /// 待握手连接表：对端地址 -> (Connector, 过期时间)。
    /// 握手完成前不创建 Session，只在这里保存轻量状态，通信都走 default_transport。
    connector_map: HashMap<SocketAddr, (SessionConnector, Instant)>,
    /// 默认通道的发包序号：这条通道长期存在，序号必须全局递增，
    /// 否则同一对端重连时新握手包会撞上旧序号，被对方去重窗口当重复包丢掉。
    next_sequence: u64,
    runtime: &'a NetRuntime,
    default_transport: Box<dyn ITransport >,
    on_recv_packets: Box<dyn FnMut(Vec<Packet>) + 'static>,
}


impl<'a> SessionManager<'a> {
    pub fn new(runtime: &'a NetRuntime, on_recv_packets: Box<dyn FnMut(Vec<Packet>) + 'static>) -> Self {
        let (default_transport, default_transport_cache) = default_transport_pair(1000);
        // 注册一个默认监听的transport，承接尚未建立会话的对端发来的连接请求
        runtime.register_transport_cache(DEFAULT_SOURCE_ADDR, Box::new(default_transport_cache));

        Self { session_map: HashMap::new(),
            connector_map: HashMap::new(),
            next_sequence: 0,
            runtime,
            default_transport: Box::new(default_transport),
            on_recv_packets,
        }
    }

    pub fn session_state(&self, peer: SocketAddr) -> Option<SessionState> {
        self.session_map.get(&peer).map(|session| session.state())
    }

    pub fn send_to(&mut self, peer: SocketAddr, packet: Packet) -> bool {
        match self.session_map.get(&peer) {
            Some(session) => {
                session.send(packet);
                true
            }
            None => false,
        }
    }

    pub fn exec_recv(&mut self) {
        // 先清理过期的待握手连接，再处理连接请求，最后轮询正式会话收数据
        self.sweep_expired_pending();
        self.handle_recv_packet();

        let mut result_packets = Vec::new();
        for (_, session) in self.session_map.iter_mut() {
            // 每个session返回的数据包
            if let Some(packets)  = session.recv_all() {
                // 将所有数据包整理后返回给上层
                result_packets.extend(packets);
            }
        }

        if !result_packets.is_empty() {
            (self.on_recv_packets)(result_packets);
        }

        // 清理已被对端断开的会话（Session 被 drop 时会注销它在 runtime 中的通道），
        // 否则旧会话会占着对端地址，后续重连请求无法再走默认通道进来。
        self.session_map.retain(|_, session| session.state() != SessionState::Disconnected);
    }

    fn sweep_expired_pending(&mut self) {
        let now = Instant::now();
        self.connector_map.retain(|_, (_, deadline)| *deadline > now);
    }

    fn handle_recv_packet(&mut self) {
        // Transport 只提供单包 recv_from，批量收取由经理自己循环；
        // 包带着来源地址，靠它区分是哪个对端的握手包。
        while let Some((source, packet)) = self.default_transport.recv_from() {
            self.handle_recv_conn_packet(source, packet);
        }
    }

    fn handle_recv_conn_packet(&mut self, source: SocketAddr, packet: Packet) {
        let data = match &packet.payload {
            // 只处理control包中的connect
            PayloadType::Bytes(data) => data,
            _ => return,
        };
        let control = match SessionPacketType::decode(data) {
            Ok(SessionPacketType::Control(control)) => control,
            _ => return,
        };
        match control {
            ControlPacketType::Request(_) => self.handle_connect_request(source),
            ControlPacketType::ChallengeResponse(_) => self.handle_challenge_response(source, control),
            _ => {}
        }
    }

    fn handle_connect_request(&mut self, source: SocketAddr) {
        if self.session_map.contains_key(&source) || self.connector_map.contains_key(&source) {
            return;
        }
        if self.connector_map.len() >= MAX_PENDING_CONNECTIONS {
            println!("SessionManager: too many pending connections, drop request from {}", source);
            return;
        }
        // 只在 connector_map 里登记轻量状态，不创建 Session；Challenge 经默认通道按地址发回
        self.connector_map.insert(
            source,
            (SessionConnector::new(SessionState::Disconnected), Instant::now() + PENDING_TIMEOUT),
        );
        let response = self
            .connector_map
            .get_mut(&source)
            .unwrap()
            .0
            .response_to(ControlPacketType::Request(source));
        match response {
            Ok(Some(control)) => self.send_pending_control(source, control),
            Ok(None) => {}
            Err(e) => {
                println!("SessionManager: invalid request from {}: {:?}", source, e);
                self.connector_map.remove(&source);
            }
        }
    }

    fn handle_challenge_response(&mut self, source: SocketAddr, control: ControlPacketType) {
        let response = match self.connector_map.get_mut(&source) {
            Some((connector, _)) => connector.response_to(control),
            None => return,
        };
        match response {
            // 校验通过：直接经默认通道发出 Accept，此时才以已连接状态创建正式 Session
            Ok(Some(ControlPacketType::Accept)) => {
                self.send_pending_control(source, ControlPacketType::Accept);
                self.connector_map.remove(&source);
                let config = SessionConfig {
                    local_addr: self.runtime.local_addr(),
                    peer_addr: source,
                    runtime: self.runtime,
                    init_session_state: SessionState::Connected,
                };
                let session = Session::new(config);
                self.session_map.insert(source, session);
            }
            Ok(Some(control)) => self.send_pending_control(source, control),
            Ok(None) => {}
            Err(e) => {
                println!("SessionManager: handshake failed with {}: {:?}", source, e);
                self.send_pending_control(source, ControlPacketType::Reject);
                self.connector_map.remove(&source);
            }
        }
    }

    /// 待握手阶段的控制包统一经默认通道按对端地址发送，序号用经理的全局计数。
    fn send_pending_control(&mut self, addr: SocketAddr, control: ControlPacketType) {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        let bytes = SessionPacketType::Control(control).encode();
        let packet = Packet::new(PROTOCOL_VERSION, sequence, PayloadType::Bytes(bytes)).unwrap();
        self.default_transport.send_to(addr, packet);
    }

}
