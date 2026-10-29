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
    
    TransportErrorInvalidPayloadType(String),
    TransportErrorInvalidSequence(String),
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

/// Transport 只负责按地址收发包，不假设对端是谁、也不管连接状态：
/// 发包时由调用方给出目的地，收包时连同来源地址一起返回。
pub trait ITransport {
    fn send_to(&self, addr: SocketAddr, packet: Packet);
    fn recv_from(&self) -> Option<(SocketAddr, Packet)>;
}

pub trait ITransportCache {
    fn get_send_queue(&mut self) -> Option<Vec<(SocketAddr, Packet)>>;
    fn on_recv(&mut self, source: SocketAddr, packet: Packet);
    fn flush_recv(&mut self);
}



// 每次允许发送到最大包数
pub const MAX_SEND_SIZE: usize = 1000;

// 接收去重滑动窗口大小（可覆盖的序号个数）
pub const RECV_WINDOW_SIZE: usize = 4096;

pub struct DefaultTransportCache {
    mailbox: Arc<Mailbox>,

    wait_ack_packets: HashMap<u128, (ExpireInfo, SocketAddr, Packet)>, // 等待ack确认的包（带目的地）
    wait_fragment_packets: HashMap<u64, Vec<Packet>>, // 按消息 sequence 分桶等待组包的分片
    to_send_acks: Vec<(SocketAddr, Packet)>, // 需要发送到ack包（带目的地）
    recv_packets:  Vec<(SocketAddr, Packet)>, // 接收到的包（带来源地址）
    recv_window: SlidingWindow, // 接收去重滑动窗口
}

impl DefaultTransportCache {
    pub fn new_with_mailbox(mailbox: Arc<Mailbox>) -> Self {
        Self {
            mailbox,
            wait_ack_packets: HashMap::new(),
            wait_fragment_packets: HashMap::new(),
            to_send_acks: Vec::new(),
            recv_packets: Vec::new(),
            recv_window: SlidingWindow::new(RECV_WINDOW_SIZE),
        }
    }
    
    pub fn get_expired_unacked_packets(&mut self) -> Option<Vec<(SocketAddr, Packet)>> {
        let mut unacked_packets = Vec::new();
        self.wait_ack_packets.iter_mut().for_each(|(_, (expire_info, addr, packet))| {
            if expire_info.is_expired() {
                unacked_packets.push((*addr, packet.clone()));
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
    fn get_send_queue(&mut self) -> Option<Vec<(SocketAddr, Packet)>> {
        // 获取要发送的ack包
        let mut send_packets = Vec::new();

        // 获取之前一直未被确认的数据包
        if let Some(packets) = self.get_expired_unacked_packets() {
            send_packets.extend_from_slice(&packets);
        }

        // 获取需要发送的ack包
        send_packets.extend_from_slice(&self.to_send_acks);

        // 获取待发送的数据包（每个包都带着自己的目的地）
        if let Some(packets) = self.mailbox.pop_send_batch(MAX_PACKET_SIZE) {
            for (addr, packet) in packets {
                match seperate_packet(&packet) {
                    Ok(seperated) => {
                        for packet in seperated {
                            send_packets.push((addr, packet.clone()));
                            self.wait_ack_packets.insert(packet.key(), (ExpireInfo::new(), addr, packet));
                        }
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
        }

        self.to_send_acks.clear();
        
        if send_packets.is_empty() {
            None
        } else {
            Some(send_packets)
        }

    }

    fn on_recv(&mut self, source: SocketAddr, packet: Packet) {
        if packet.is_ack() {
            // ACK 携带目标包的 128 位 key，可精确删掉某个分片的待确认记录
            self.wait_ack_packets.remove(&packet.key());
            return;
        }

        // 重复包和过旧包同样要回 ACK（对方重传多半是因为上次 ACK 丢失），只是不再向上层投递
        self.to_send_acks.push((source, Packet::new_ack(packet.version, packet.key())));
        // 滑动窗口去重复
        match self.recv_window.insert(packet.key()) {
            SlidingWindowInsertResult::New => {
                if !packet.is_fragment() {
                    self.recv_packets.push((source, packet));
                    return;
                }

                // 按消息 sequence 分桶攒分片，收齐后整桶取出组包并清理
                let sequence = packet.sequence;
                let chunk_count = packet.chunk_count() as usize;
                if chunk_count == 0 {
                    println!("NetRuntimeIoState: on_recv: fragment with zero chunk_count, seq: {}", sequence);
                    return;
                }

                let complete = {
                    let bucket = self.wait_fragment_packets.entry(sequence).or_default();
                    bucket.push(packet);
                    bucket.len() == chunk_count
                };
                if complete {
                    if let Some(mut bucket) = self.wait_fragment_packets.remove(&sequence) {
                        match merge_packets(&mut bucket) {
                            Ok(packet) => self.recv_packets.push((source, packet)),
                            Err(e) => println!("NetRuntimeIoState: on_recv: merge_packets error: {:?}", e),
                        }
                    }
                }
            }
            SlidingWindowInsertResult::Duplicate | SlidingWindowInsertResult::TooOld => {}
        }
    }
    
    fn flush_recv(&mut self) {
        // 有序返回
        self.recv_packets.sort_by_key(|(_, packet)| packet.sequence);
        self.mailbox.push_recv_batch(self.recv_packets.clone());
        self.recv_packets.clear();
    }
}


pub struct DefaultTransport {
    mailbox: Arc<Mailbox>,
}


impl DefaultTransport {
    pub fn new_with(mailbox: Arc<Mailbox>) -> Self {
        Self {
            mailbox,
        }
    }
}

impl ITransport for DefaultTransport {
    fn send_to(&self, addr: SocketAddr, packet: Packet) {
        self.mailbox.push_send(addr, packet);
    }

    fn recv_from(&self) -> Option<(SocketAddr, Packet)> {
        self.mailbox.pop_recv()
    }
}

pub fn default_transport_pair(mail_size: usize) -> (DefaultTransport, DefaultTransportCache) {
    let mailbox = Arc::new(Mailbox::new(mail_size));
    let transport = DefaultTransport::new_with(Arc::clone(&mailbox));
    let transport_cache = DefaultTransportCache::new_with_mailbox(Arc::clone(&mailbox));
    (transport, transport_cache)
}