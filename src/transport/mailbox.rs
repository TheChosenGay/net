use super::packet::Packet;
use super::ringbuffer::RingBuffer;
use std::net::SocketAddr;
use std::sync::Mutex;

/// 邮箱里流转的都是带地址的包：发送侧的地址是目的地，接收侧的地址是来源。
/// 普通会话通道只用固定对端地址；默认监听通道靠这个地址区分不同对端。
pub struct Mailbox {
    send_queue: Mutex<RingBuffer<(SocketAddr, Packet)>>,
    recv_queue: Mutex<RingBuffer<(SocketAddr, Packet)>>,
}

impl Mailbox {
    pub fn new(mail_size: usize) -> Self {
        Self {
            send_queue: Mutex::new(RingBuffer::new(mail_size)),
            recv_queue: Mutex::new(RingBuffer::new(mail_size)),
        }
    }

    pub fn push_send(&self, addr: SocketAddr, packet: Packet) {
        self.send_queue.lock().unwrap().push((addr, packet));
    }

    pub fn push_recv_batch(&self, packets: Vec<(SocketAddr, Packet)>) {
        let mut recv_queue = self.recv_queue.lock().unwrap();
        for packet in packets {
            recv_queue.push(packet);
        }
        drop(recv_queue);
    }

    pub fn pop_recv(&self) -> Option<(SocketAddr, Packet)> {
        self.recv_queue.lock().unwrap().pop()
    }

    pub fn pop_send_batch(&self, batch: usize) -> Option<Vec<(SocketAddr, Packet)>> {
        let mut send_queue = self.send_queue.lock().unwrap();
        let mut packets = Vec::new();
        for _ in 0..batch {
            match send_queue.pop() {
                Some(packet) => packets.push(packet),
                None => break,
            }
        }
        drop(send_queue);
        if packets.is_empty() {
            None
        } else {
            Some(packets)
        }
    }
}
