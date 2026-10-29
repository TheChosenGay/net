mod io_state;


use std::net::SocketAddr;
use std::thread;
use std::sync::mpsc;
use super::transport::ITransportCache;
use std::net::UdpSocket;
use io_state::*;
use std::time::Duration;
use std::net::{Ipv4Addr, SocketAddrV4};

pub const DEFAULT_SOURCE_ADDR: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 0), 0));
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
    
    NetRuntimeCmdStop,
}


pub struct NetRuntime {
    config: NetRuntimeConfig,
    handler: Option<thread::JoinHandle<()>>,
    tx: mpsc::Sender<NetRuntimeCmd>,
}


impl NetRuntime {
    pub fn new(mut config: NetRuntimeConfig) -> Result<Self, NetRuntimeError> {
        let (tx, rx) = mpsc::channel();
        let socket = UdpSocket::bind(config.local_addr).unwrap();
        // 以实际绑定地址为准：配置里 port 传 0 时，系统分配的端口才是真实本地地址。
        config.local_addr = socket.local_addr().unwrap_or(config.local_addr);
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
                handler: Some(handler),
                tx,
            }),
            Err(_) => Err(NetRuntimeError::NetRuntimeErrorThreadCreateFailed),
        }
        
    }
    
    pub fn local_addr(&self) -> SocketAddr {
        self.config.local_addr
    }

    fn stop_and_join(&mut self) {
        let Some(handler) = self.handler.take() else {
            println!("NetRuntime: stop_and_join: handler is None");
            return;
        };
        let _ =  self.tx.send(NetRuntimeCmd::NetRuntimeCmdStop);
        let _ = handler.join();
        
    }
    
    pub fn register_transport_cache(&self, addr: SocketAddr, transport: Box<dyn ITransportCache + Send>) {
        self.tx.send(NetRuntimeCmd::NetRuntimeCmdRegisterTransport((addr, transport))).unwrap();
    }
    
    pub fn unregister_transport_cache(&self, addr: SocketAddr) { 
       self.tx.send(NetRuntimeCmd::NetRuntimeCmdUnregisterTransport(addr)).unwrap(); 
    }
    
    pub fn shut_down(mut self) {
        self.stop_and_join();
    }
    
}

impl Drop for NetRuntime {
    fn drop(&mut self) {
        self.stop_and_join();
    }
}

