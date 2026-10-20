use crate::transport::{ITransport, default_transport_pair};
use crate::runtime::NetRuntime;
use std::net::SocketAddr;
use crate::transport::packet::Packet;

pub struct SessionConfig {
    pub local_addr: SocketAddr,
    pub peer_addr: SocketAddr,
    pub runtime: &'static NetRuntime,
}

pub struct Session<'a> {
    config: SessionConfig,
    transport: Box<dyn ITransport>,    
    runtime: &'a NetRuntime,
}

impl<'a> Session<'a> {
    pub fn new(config: SessionConfig) -> Self {
        let (transport, cache) = default_transport_pair(config.peer_addr.clone(), 1024);
        // 注册到IO线程中
        config.runtime.register_transport_cache(config.peer_addr.clone(), Box::new(cache));
        let runtime = config.runtime;
        Self {
            config,
            transport: Box::new(transport),
            runtime,
        }
    }
    
    pub fn send(&self, packet: Packet) {
        self.transport.send(packet);
    }

    pub fn recv(&self) -> Option<Packet> {
        self.transport.recv()
    }
    
}

impl Drop for Session<'_> {
    fn drop(&mut self) {
        self.runtime.unregister_transport_cache(self.config.peer_addr);
    }
}