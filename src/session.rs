use crate::transport::{ITransport, default_transport_pair, packet::PayloadType};
use crate::runtime::NetRuntime;
use std::net::SocketAddr;
use crate::transport::packet::{Packet, PROTOCOL_VERSION};
use crate::connect::*;
use std::cell::Cell;


#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SessionState {
    Disconnected, // 未连接
    Connecting, // 正在进行connnect流程
    Connected, // 已连接
    Disconnecting, // 正在进行disconnect流程
}

#[derive(Debug, Clone)]
pub enum SessionPacketError {
    InvalidPacketType(String),
    SessionPacketErrorTooSmall,
}

pub enum SessionPacketType {
    Control(ControlPacketType), // control包，
    Data(Vec<u8>), // 上层数据包
}



pub struct SessionConnector {
    state: Cell<SessionState>,
    prev_challenge_seq: u64,
}


impl SessionConnector {
    pub fn new(state: SessionState) -> Self {
        Self {
            state: Cell::new(state),
            prev_challenge_seq: 0,
        }
    }
    
    pub fn state(&self) -> SessionState {
        self.state.get()
    }
    
    /// 处理一个连接控制包，返回需要回给对端的控制包；
    /// Accept / Reject / Disconnect 这类只推进状态的包返回 None，表示不用回包。
    pub fn response_to(&mut self, control: ControlPacketType) -> Result<Option<ControlPacketType>, SessionPacketError> {
        return self.handle_control(control);
    }

    fn handle_control(&mut self, control: ControlPacketType) -> Result<Option<ControlPacketType>, SessionPacketError> {
        match &control {
            ControlPacketType::Request(_) => {
                return self.handle_request();
            },
            ControlPacketType::Challenge(change_seq) => {
                return self.handle_challenge(*change_seq);
            },
            ControlPacketType::ChallengeResponse(challenge_seq) => {
                return self.handle_challenge_response(*challenge_seq);
            },
            ControlPacketType::Accept => {
                return self.handle_accept();
            },
            ControlPacketType::Reject => {
                return self.handle_reject();
            },
            ControlPacketType::Disconnect => {
                return self.handle_disconnect();
            },
        }
    }

    fn handle_disconnect(&mut self) -> Result<Option<ControlPacketType>, SessionPacketError> {
        // 对方断开：无论当前在连接中还是已连接，都回到未连接状态，不用回包
        self.state.set(SessionState::Disconnected);
        Ok(None)
    }

    fn handle_reject(&mut self) -> Result<Option<ControlPacketType>, SessionPacketError> {
        match self.state.get() {
            SessionState::Connected | SessionState::Disconnected | SessionState::Disconnecting => {
                return Err(SessionPacketError::InvalidPacketType(format!("Invalid session state: {:?}", self.state.get())));
            }
            SessionState::Connecting => {
                self.state.set(SessionState::Disconnected);
                return Ok(None);
            }
        }
    }

    fn handle_accept(&mut self) -> Result<Option<ControlPacketType>, SessionPacketError> {
        match self.state.get() {
            SessionState::Connected | SessionState::Disconnected | SessionState::Disconnecting => {
                return Err(SessionPacketError::InvalidPacketType(format!("Invalid session state: {:?}", self.state.get())));
            }
            SessionState::Connecting => {
                self.state.set(SessionState::Connected);
                return Ok(None);
            }
        }
    }

    fn handle_challenge_response(&mut self, challenge_seq: u64) -> Result<Option<ControlPacketType>, SessionPacketError> {
        match self.state.get() {
            SessionState::Connected | SessionState::Disconnected | SessionState::Disconnecting => {
                return Err(SessionPacketError::InvalidPacketType(format!("Invalid session state: {:?}", self.state.get())));
            }
            SessionState::Connecting => {
                if challenge_seq != self.prev_challenge_seq {
                    return Err(SessionPacketError::InvalidPacketType(format!("Invalid challenge sequence: {:?}", challenge_seq)));
                }
                self.state.set(SessionState::Connected);
                return Ok(Some(ControlPacketType::Accept));
            }
        }
    }

    fn handle_request(&mut self) -> Result<Option<ControlPacketType>, SessionPacketError> {
        match self.state.get() {
            SessionState::Connected | SessionState::Connecting | SessionState::Disconnecting => {
                return Err(SessionPacketError::InvalidPacketType(format!("Invalid session state: {:?}", self.state.get())));
            }
            SessionState::Disconnected => {
                self.prev_challenge_seq = ControlPacketType::gen_challenge_seq();
                self.state.set(SessionState::Connecting);
                return Ok(Some(ControlPacketType::Challenge(self.prev_challenge_seq)));
            }
        }
    }

    // 来了挑战包
    fn handle_challenge(&mut self, challenge_seq: u64) -> Result<Option<ControlPacketType>, SessionPacketError> {
        match self.state.get() {
            SessionState::Connected | SessionState::Disconnected | SessionState::Disconnecting => {
                Err(SessionPacketError::InvalidPacketType(format!("Invalid session state: {:?}", self.state.get())))
            }
            SessionState::Connecting => {
                self.state.set(SessionState::Connecting);
                Ok(Some(ControlPacketType::ChallengeResponse(challenge_seq)))
            }
        }
    }

}


impl SessionPacketType {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            SessionPacketType::Control(control) => {
                buf.push(0x01);
                buf.extend_from_slice(&control.encode());
            }
            SessionPacketType::Data(data) => {
                buf.push(0x02);
                buf.extend_from_slice(data);
            }
        }
        buf
    }
    
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, SessionPacketError> {
        if bytes.len() < 2 {
            return Err(SessionPacketError::SessionPacketErrorTooSmall);
        }
        match bytes[0] {
            0x01 =>  {
                match ControlPacketType::decode(&bytes[1..]) {
                    Ok(control) => Ok(Self::Control(control)),
                    Err(e) => return Err(SessionPacketError::InvalidPacketType(format!("Invalid control packet: {:?}", e))),
                }
            }
            0x02 => Ok(Self::Data(bytes[1..].to_vec())),
            _ => return Err(SessionPacketError::InvalidPacketType(format!("Invalid session packet type: {}", bytes[0]))),
        }
    }
}    

#[derive(Clone)]
pub struct SessionConfig<'a> {
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub runtime: &'a NetRuntime,
    pub init_session_state: SessionState,
}

pub struct Session<'a> {
    config: SessionConfig<'a>,
    transport: Box<dyn ITransport>,    
    runtime: &'a NetRuntime,
    sequence: u64,
    connector: SessionConnector,
}


/**
 * Session : 自己处理链接，管理单个会话状态
 * 向外提供拿取数据的方法
 */

impl<'a> Session<'a> {
    pub fn new(config: SessionConfig<'a>) -> Self {
        let (transport, cache) = default_transport_pair(1024);
        // 注册到IO线程中
        config.runtime.register_transport_cache(config.peer_addr.clone(), Box::new(cache));
        let runtime = config.runtime;
        Self {
            config: config.clone(),
            transport: Box::new(transport),
            runtime,
            sequence: 0,
            connector: SessionConnector::new(config.init_session_state),
        }
    }
    
    pub fn send(&self, packet: Packet) {
        // 应用数据统一包装成 SessionPacketType::Data，与控制包共用同一条会话通道，
        // 接收端解包后再把原始字节交还上层。
        let packet = match &packet.payload {
            PayloadType::Bytes(data) => Packet::new(
                packet.version,
                packet.sequence,
                PayloadType::Bytes(SessionPacketType::Data(data.clone()).encode()),
            )
            .unwrap(),
            _ => packet,
        };
        // 对端地址由 Session 自己保存，每次发送时交给 Transport
        self.transport.send_to(self.config.peer_addr, packet);
    }

    pub fn state(&self) -> SessionState {
        self.connector.state()
    }

    /// 主动发起连接：进入 Connecting 并发出携带本地地址的 Request，
    /// 后续 Challenge / Accept 由 recv 路径上的 Connector 状态机推进。
    pub fn connect_to(&mut self, local_addr: SocketAddr) {
        self.connector.state.set(SessionState::Connecting);
        self.send_control(ControlPacketType::Request(local_addr));
    }

    /// 主动断开连接：通知对端后回到 Disconnected，之后可再次 connect_to 重连。
    pub fn disconnect(&mut self) {
        if self.connector.state() != SessionState::Connected {
            return;
        }
        self.send_control(ControlPacketType::Disconnect);
        self.connector.state.set(SessionState::Disconnected);
    }

    
    pub fn recv_all(&mut self) -> Option<Vec<Packet>> {
        let mut result_packets = Vec::new();
        // Transport 只提供单包 recv_from，批量收取由 Session 自己循环；
        // 来源地址恒为本会话的对端，会话层无需再区分。
        while let Some((_, packet)) = self.transport.recv_from() {
            if let Some(result_packet) = self.handle_recv_packet(packet) {
                result_packets.push(result_packet);
            }
        }
        if result_packets.is_empty() {
            return None;
        }
        Some(result_packets)
    }

    pub fn recv(&mut self) -> Option<Packet> {
        if let Some((_, packet)) = self.transport.recv_from() {
            return self.handle_recv_packet(packet);
        }
        None
    }

    // 处理单个包
    fn handle_recv_packet(&mut self, packet: Packet) -> Option<Packet>{
        match &packet.payload {
            PayloadType::Bytes(data) => {
                match SessionPacketType::decode(&data) {
                    Ok(packet_type) => {
                        match packet_type {
                            SessionPacketType::Control(control) => {
                                self.handle_control(control);
                                return None;
                            }
                            SessionPacketType::Data(data) => {
                                if self.connector.state() == SessionState::Connected {
                                    // 解开会话层包装，把应用原始字节交还上层
                                    return Packet::new(
                                        packet.version,
                                        packet.sequence,
                                        PayloadType::Bytes(data),
                                    )
                                    .ok();
                                }
                                return None;
                            }
                        }
                    }
                    Err(e) => {
                        println!("Invalid session packet: {:?}", e);
                        return None;
                    }
                }
            },
            PayloadType::Fragment(_) | PayloadType::Ack(_) => {
                println!("Invalid session packet: {:?}", packet.payload);
                return None;
            }
        }
    }
    
    fn handle_control(&mut self, control: ControlPacketType) {
        match self.connector.response_to(control) {
            Ok(Some(control)) => {
                self.send_control(control);
            }
            Ok(None) => {}
            Err(e) => {
                println!("Invalid session packet: {:?}", e);
                // 只在握手阶段回 Reject 表示拒绝连接；其余状态下收到过期的
                // 重复控制包只记录，避免误伤已建立的连接或来回回声。
                if self.connector.state() == SessionState::Connecting {
                    self.send_control(ControlPacketType::Reject);
                }
            }
        }
    }

    fn send_control(&mut self, control: ControlPacketType) {
        let sequence = self.next_sequence();
        let bytes = SessionPacketType::Control(control).encode();
        let packet = Packet::new(PROTOCOL_VERSION, sequence, PayloadType::Bytes(bytes)).unwrap();
        self.transport.send_to(self.config.peer_addr, packet);
    }
    
    
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.wrapping_add(1);
        self.sequence
    }

    
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        self.runtime.unregister_transport_cache(self.config.peer_addr);
    }
}