mod io_state;


use std::net::SocketAddr;
use std::thread;
use std::sync::mpsc;
use super::transport::ITransportCache;
use std::net::UdpSocket;
use io_state::*;
use std::time::Duration;


#[derive(Debug, Clone)]
pub enum NetRuntimeError {
    NetRuntimeErrorInvalidCmd,
    NetRuntimeErrorThreadCreateFailed,
}

#[derive(Debug, Clone)]
pub struct NetRuntimeConfig {
    pub local_addr: SocketAddr,
}


pub enum NetRuntimeCmd {
    NetRuntimeCmdRegisterTransport((SocketAddr, Box<dyn ITransportCache + Send>)),
    NetRuntimeCmdUnregisterTransport(SocketAddr),
}


pub struct NetRuntime {
    config: NetRuntimeConfig,
    handler: thread::JoinHandle<()>,
    tx: mpsc::Sender<NetRuntimeCmd>,
}


impl NetRuntime {
    pub fn new(config: NetRuntimeConfig) -> Result<Self, NetRuntimeError> {
        let (tx, rx) = mpsc::channel();
        let socket = UdpSocket::bind(config.local_addr).unwrap();
        socket.set_nonblocking(false).unwrap();
        socket.set_read_timeout(Some(Duration::from_millis(5))).unwrap();
        let mut io_state = NetRuntimeIoState::new(rx, socket);
        match thread::Builder::new()
            .name(format!("net runtime io thread for addr: {}", config.local_addr))
            .spawn(move || {
            io_state.runtime_io_loop();
        }) {
            Ok(handler) => Ok(Self {
                config,
                handler,
                tx,
            }),
            Err(_) => Err(NetRuntimeError::NetRuntimeErrorThreadCreateFailed),
        }
        
    }
    
    pub fn register_transport_cache(&self, addr: SocketAddr, transport: Box<dyn ITransportCache + Send>) {
        self.tx.send(NetRuntimeCmd::NetRuntimeCmdRegisterTransport((addr, transport))).unwrap();
    }
    
    pub fn unregister_transport_cache(&self, addr: SocketAddr) { 
       self.tx.send(NetRuntimeCmd::NetRuntimeCmdUnregisterTransport(addr)).unwrap(); 
    }
}

