pub mod packet;
mod mailbox;
pub (crate) mod ringbuffer;
mod sliding_window;


use packet::*;
use std::sync::Arc;
use std::collections::HashMap;
use std::net::SocketAddr;
use mailbox::Mailbox;
use sliding_window::{SlidingWindow, SlidingWindowInsertResult};
use std::time::SystemTime;

#[derive(Debug, Clone)]
pub enum TransportError {
    TransportErrorPacketTooLarge,
    TransportErrorPacketTooSmall,
}


pub const EXPIRE_INTERNAL: u128 = 50;
pub const EXP_MAX_ITER: u64 = 3;

#[derive(Debug, Clone)]
pub struct ExpireInfo {
    pub expired_time: u128, // ms
    pub iter: u64, // 迭代变化次数
    exp_max_iter: u64 // 指数退避增长的最大迭代次数
}

impl ExpireInfo {
    pub fn new() -> Self {
        Self {
            expired_time: EXPIRE_INTERNAL + Self::now(),
            iter: 1,
            exp_max_iter: EXP_MAX_ITER,
        }
    }
    
    fn with_expired_time_and_iter(expired_time: u128, iter: u64) -> Self {
        Self {
            expired_time,
            iter,
            exp_max_iter: EXP_MAX_ITER,
        } 
    }
    
    pub fn is_expired(&self) -> bool {
        self.expired_time <= Self::now()    
    }
    
    pub fn next(&self) -> Self {
        let iter = std::cmp::min(self.iter + 1, self.exp_max_iter); 
        let next_expired_time = Self::now() + 2_u128.pow(iter as u32) * EXPIRE_INTERNAL;
        Self::with_expired_time_and_iter(next_expired_time, iter)
    }
    
    
    fn now() -> u128 {
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis()
    }

}


// 数据包状态
// 1. 等待发送
// 2. 等待ack
// 3. 收到ack确认

pub trait ITransport {
    fn send(&self, packet: Packet);    
    fn recv(&self) -> Option<Packet>;
}

pub trait ITransportCache {
    fn get_send_queue(&mut self) -> Option<Vec<Packet>>;
    fn on_recv(&mut self, packet: Packet);
    fn flush_recv(&mut self);
}



// 每次允许发送到最大包数
pub const MAX_SEND_SIZE: usize = 1000;

// 接收去重滑动窗口大小（可覆盖的序号个数）
pub const RECV_WINDOW_SIZE: usize = 4096;

pub struct DefaultTransportCache {
    mailbox: Arc<Mailbox>,

    wait_ack_packets: HashMap<u64, (ExpireInfo, Packet)>, // 等待ack确认的包
    to_send_acks: Vec<Packet>, // 需要发送到ack包
    recv_packets:  Vec<Packet>, // 接收到的包
    recv_window: SlidingWindow, // 接收去重滑动窗口
}

impl DefaultTransportCache {
    pub fn new_with_mailbox(mailbox: Arc<Mailbox>) -> Self {
        Self {
            mailbox,
            wait_ack_packets: HashMap::new(),
            to_send_acks: Vec::new(),
            recv_packets: Vec::new(),
            recv_window: SlidingWindow::new(RECV_WINDOW_SIZE),
        }
    }
    
    pub fn get_expired_unacked_packets(&mut self) -> Option<Vec<Packet>> {
        let mut unacked_packets = Vec::new();
        self.wait_ack_packets.iter_mut().for_each(|(_, (expire_info, packet))| {
            if expire_info.is_expired() {
                unacked_packets.push(packet.clone());
                // 本次重传后刷新超时时间，避免到期后每个 IO 循环都重复发同一批包
                *expire_info = expire_info.next();
            }
        });
        
        if unacked_packets.is_empty() {
            None
        } else {
            Some(unacked_packets)
        }
    }
    
}

impl ITransportCache for DefaultTransportCache {
    fn get_send_queue(&mut self) -> Option<Vec<Packet>> {
        // 获取要发送的ack包
        let mut send_packets = Vec::new();

        // 获取之前一直未被确认的数据包
        if let Some(packets) = self.get_expired_unacked_packets() {
            send_packets.extend_from_slice(&packets);
        }
        
        // 获取需要发送的ack包
        send_packets.extend_from_slice(&self.to_send_acks);
        
        // 获取待发送的数据包
        if let Some(packets) = self.mailbox.pop_send_batch(MAX_PACKET_SIZE) {
            send_packets.extend_from_slice(&packets);
            for packet in packets {
                self.wait_ack_packets.insert(packet.sequence, (ExpireInfo::new(), packet));
            }
        }

        self.to_send_acks.clear();
        
        if send_packets.is_empty() {
            None
        } else {
            Some(send_packets)
        }

    }

    fn on_recv(&mut self, packet: Packet) {
        if !packet.is_ack() {
            // 重复包和过旧包同样要回 ACK（对方重传多半是因为上次 ACK 丢失），只是不再向上层投递
            self.to_send_acks.push(Packet::new_ack(packet.version, packet.sequence));
            match self.recv_window.insert(packet.sequence) {
                SlidingWindowInsertResult::New => self.recv_packets.push(packet),
                SlidingWindowInsertResult::Duplicate | SlidingWindowInsertResult::TooOld => {}
            }
        } else {
            // 确认ack
            self.wait_ack_packets.remove(&packet.sequence);
        }
    }
    
    fn flush_recv(&mut self) {
        // 有序返回
        self.recv_packets.sort_by_key(|packet| packet.sequence);
        self.mailbox.push_recv_batch(self.recv_packets.clone());
        self.recv_packets.clear();
    }
}


pub struct DefaultTransport {
    peer: SocketAddr,
    mailbox: Arc<Mailbox>,
}


impl DefaultTransport {
    pub fn new_with(peer: SocketAddr, mailbox: Arc<Mailbox>) -> Self {
        Self {
            peer,
            mailbox,
        }
    }
}

impl ITransport for DefaultTransport {
    fn send(&self, packet: Packet) {
        self.mailbox.push_send(packet);
    }
    fn recv(&self) -> Option<Packet> {
        self.mailbox.pop_recv()
    }
}

pub fn default_transport_pair(peer: SocketAddr, mail_size: usize) -> (DefaultTransport, DefaultTransportCache) {
    let mailbox = Arc::new(Mailbox::new(mail_size));
    let transport = DefaultTransport::new_with(peer, Arc::clone(&mailbox));
    let transport_cache = DefaultTransportCache::new_with_mailbox(Arc::clone(&mailbox));
    (transport, transport_cache)
}