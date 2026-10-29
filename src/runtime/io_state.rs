use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::mpsc;
use crate::transport::ITransportCache;
use super::{NetRuntimeCmd, DEFAULT_SOURCE_ADDR};
use std::net::UdpSocket;
use crate::transport::packet::*;

pub struct NetRuntimeIoState {
    transport_map: HashMap<SocketAddr, Box<dyn ITransportCache + Send>>,
    rx: mpsc::Receiver<NetRuntimeCmd>,
    socket: UdpSocket,
    stop: bool
}

impl NetRuntimeIoState {
    pub fn new(rx: mpsc::Receiver<NetRuntimeCmd>, socket: UdpSocket) -> Self {
        Self {
            transport_map: HashMap::new(),
            rx,
            socket,
            stop: false
        }
    }

    pub fn runtime_io_loop(&mut self) {
        loop {
            self.handle_recv_cmd();
            if self.stop {
                println!("NetRuntimeIoState: runtime_io_loop: stop");
                break;
            }

            // 处理发送数据
            self.send_data();
            
            // 处理接收数据
            self.recv_data();
        }
    }
    
    pub fn send_data(&mut self) {
        for (_, transport) in self.transport_map.iter_mut() {
            let data = transport.get_send_queue().unwrap_or_default();
            // 每个包都带着自己的目的地：会话通道固定发给对端，默认通道按包上的地址发
            for (addr, packet) in data.iter() {
                self.socket.send_to(&packet.encode(), addr).unwrap();
            }
        }
    }
    
    pub fn recv_data(&mut self) {
        loop {
            let mut buf = [0; MAX_PACKET_SIZE];
            match self.socket.recv_from(&mut buf) {
                Ok((len, addr)) =>  {
                    match Packet::decode(&buf[..len]) {
                        Ok(packet) => {
                            // 未知来源（未注册的对端）交给默认通道处理，并把来源地址一并带上
                            if let Some(transport) = self.transport_map.get_mut(&addr) {
                                transport.on_recv(addr, packet);
                            } else {
                                if let Some(transport) = self.transport_map.get_mut(&DEFAULT_SOURCE_ADDR) {
                                    transport.on_recv(addr, packet);
                                }
                            }
                        }
                        Err(e) => {
                            println!("NetRuntimeIoState: runtime_io_loop: decode error: {:?}", e);
                            continue;
                        }
                    }
                },
                // 非阻塞 socket 暂时没数据，说明本批已收完
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(e) => {
                    println!("NetRuntimeIoState: runtime_io_loop: recv error: {:?}", e);
                    break;
                }
            };
        }

        // 本批数据收完后，统一把各 transport 暂存的包刷入 mailbox
        self.transport_map.values_mut().for_each(|transport| transport.flush_recv());
    }
    

    fn handle_recv_cmd(&mut self) {
        // 非阻塞地把当前积压的命令一次处理完；用阻塞 recv 会让整个 IO 循环卡在等命令上，后续收发都停摆
        loop {
            match self.rx.try_recv() {
                Ok(cmd) => self.handle_cmd(cmd),
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => break,
            }
        }
    }

    fn handle_cmd(&mut self, cmd: NetRuntimeCmd) {
        match cmd {
            NetRuntimeCmd::NetRuntimeCmdRegisterTransport((addr, transport)) => {
                self.transport_map.insert(addr, transport);
            }
            NetRuntimeCmd::NetRuntimeCmdUnregisterTransport(addr) => {
                self.transport_map.remove(&addr);
            }
            NetRuntimeCmd::NetRuntimeCmdStop => {
                self.stop = true;
            }
        }
    }
}
