use crate::runtime::DEFAULT_SOURCE_ADDR;
use crate::transport::{ITransport, default_transport_pair};
use std::collections::HashMap;
use crate::transport::packet::PayloadType;
use crate::runtime::NetRuntime;
use std::net::SocketAddr;
use crate::session::{Session, SessionState};
use crate::connect::ControlPacketType;

use crate::transport::packet::Packet;

pub struct SessionManager<'a> {
    session_map: HashMap<SocketAddr, Session<'a>>,
    connect_state_map: HashMap<SocketAddr, SessionState>,
    runtime: &'a NetRuntime,
    default_transport: Box<dyn ITransport >,
    on_recv_packets: Box<dyn FnMut(Vec<Packet>) + 'static>,
}


impl<'a> SessionManager<'a> {
    pub fn new(runtime: &'a NetRuntime, on_recv_packets: Box<dyn FnMut(Vec<Packet>) + 'static>) -> Self {
        let (default_transport, default_transport_cache) = default_transport_pair(DEFAULT_SOURCE_ADDR, 1000);
        // 注册一个默认监听的transport
        runtime.register_transport_cache(DEFAULT_SOURCE_ADDR, Box::new(default_transport_cache));

        Self { session_map: HashMap::new(), connect_state_map:HashMap::new() ,runtime, default_transport: Box::new(default_transport), on_recv_packets }
    }
    
    
    fn handle_recv_packet(&mut self) {
        if let Some(packets) = self.default_transport.recv_all() {
            for packet in packets {
                self.handle_recv_conn_packet(packet);
            }
        }
    }
    
    fn exec_recv(&mut self) {
        // 处理收到的连接包
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
    }
    
    fn handle_recv_conn_packet(&mut self, packet: Packet) {
        match &packet.payload {
            // 只处理control包中的connect
            PayloadType::Bytes(data) => {
                if let Ok(control_packet_type) = ControlPacketType::decode(data) {
                    match control_packet_type {
                        // 暂时只处理ConnectRequest包，然后申请session，让session自己处理，后续的话，维护state，确认连接后在
                        ControlPacketType::Request(addr) => {
                            let next_sequence = self.default_transport.next_sequence();
                            self.default_transport.send(Packet::new(addr, PayloadType::Bytes(control_packet_type.encode())));
                        }
                        _ => {}
                    }
                }
            },
            _ => {}
        }
    }

}