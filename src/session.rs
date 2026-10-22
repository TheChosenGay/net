use crate::transport::{ITransport, default_transport_pair, packet::PayloadType};
use crate::runtime::NetRuntime;
use std::net::SocketAddr;
use crate::transport::packet::{Packet, PROTOCOL_VERSION};
use crate::connect::*;
use std::cell::Cell;


#[derive(Clone, Copy, PartialEq)]
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


impl SessionPacketType {
    fn encode(&self) -> Vec<u8> {
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
    
    fn decode(bytes: &[u8]) -> Result<Self, SessionPacketError> {
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
    state: Cell<SessionState>,
    runtime: &'a NetRuntime,
    sequence: u64,
}


/**
 * Session : 自己处理链接，管理单个会话状态
 * 向外提供拿取数据的方法
 */

impl<'a> Session<'a> {
    pub fn new(config: SessionConfig<'a>) -> Self {
        let (transport, cache) = default_transport_pair(config.peer_addr.clone(), 1024);
        // 注册到IO线程中
        config.runtime.register_transport_cache(config.peer_addr.clone(), Box::new(cache));
        let runtime = config.runtime;
        Self {
            config: config.clone(),
            transport: Box::new(transport),
            state: Cell::new(config.init_session_state),
            runtime,
            sequence: 0,
        }
    }
    
    pub fn send(&self, packet: Packet) {
        self.transport.send(packet);
    }
    
    pub fn recv_all(&mut self) -> Option<Vec<Packet>> {
        if let Some(packets) = self.transport.recv_all() {
            let mut result_packets = Vec::new();
            for packet in packets {
               if let Some(result_packet) = self.handle_recv_packet(packet) {
                    result_packets.push(result_packet);
               }
            }
            if result_packets.is_empty() {
                return None;
            }
            return Some(result_packets);
        }
        None
    }

    pub fn recv(&mut self) -> Option<Packet> {
        if let Some(packet) = self.transport.recv() {
            self.handle_recv_packet(packet);
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
                            SessionPacketType::Data(_) => {
                                if self.state.get() == SessionState::Connected {
                                    return Some(packet);
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
    
    pub fn connect_to(&mut self, peer: SocketAddr) {
        self.state.set(SessionState::Connecting);
        let sequence = self.next_sequence();
        self.send(Packet::new(PROTOCOL_VERSION, sequence, PayloadType::Bytes(ControlPacketType::Request(peer).encode())).unwrap());
    }
    
    pub fn disconnect(&mut self) {
        if self.state.get() != SessionState::Connected {
            return;            
        }
        
    }
    
    fn handle_control(&mut self, control: ControlPacketType) {
        match &control {
            ControlPacketType::Request(_) => {
                self.handle_request();
            },
            ControlPacketType::Challenge(change_seq) => {
                self.handle_challenge(*change_seq);
            },
            ControlPacketType::ChallengeResponse(_) => {
                self.state.set(SessionState::Connected);
            },
            ControlPacketType::Accept => {
                self.state.set(SessionState::Connected);
            },
            ControlPacketType::Reject => {
                self.state.set(SessionState::Connected);
            },
        }
    }
    
    fn handle_request(&mut self) {
        match self.state.get() {
            SessionState::Connected | SessionState::Connecting | SessionState::Disconnecting => {}
            SessionState::Disconnected => {
                self.state.set(SessionState::Connecting);
                let sequence = self.next_sequence();
                let challenge_seq = ControlPacketType::gen_challenge_seq();
                self.send(Packet::new(PROTOCOL_VERSION, sequence, PayloadType::Bytes(ControlPacketType::Challenge(challenge_seq).encode())).unwrap());
            }
        }
    }
    
    // 来了挑战包
    fn handle_challenge(&mut self, challenge_seq: u64) {
        match self.state.get() {
            SessionState::Connected | SessionState::Disconnected | SessionState::Disconnecting => {}
            SessionState::Connecting => {
                let sequence = self.next_sequence();
                self.send(Packet::new(PROTOCOL_VERSION, sequence, PayloadType::Bytes(ControlPacketType::ChallengeResponse(challenge_seq).encode())).unwrap());
            }
        }
    }

    
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.wrapping_add(1);
        self.sequence
    }

    pub fn state(&self) -> SessionState {
        self.state.get()
    }
    
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        self.runtime.unregister_transport_cache(self.config.peer_addr);
    }
}