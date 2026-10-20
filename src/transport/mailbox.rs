use super::packet::Packet;
use super::ringbuffer::RingBuffer;
use std::sync::Mutex;

pub struct Mailbox {
    send_queue: Mutex<RingBuffer<Packet>>,
    recv_queue: Mutex<RingBuffer<Packet>>,
}

impl Mailbox {
    pub fn new(mail_size: usize) -> Self {
        Self {
            send_queue: Mutex::new(RingBuffer::new(mail_size)),
            recv_queue: Mutex::new(RingBuffer::new(mail_size)),
        }
    }
    
    pub fn push_send(&self, packet: Packet) {
        self.send_queue.lock().unwrap().push(packet);
    }
    
    pub fn pop_send(&self) -> Option<Packet> {
        self.send_queue.lock().unwrap().pop()
    }
    
    pub fn push_recv(&self, packet: Packet) {
        self.recv_queue.lock().unwrap().push(packet);
    }

    pub fn pop_recv(&self) -> Option<Packet> {
        self.recv_queue.lock().unwrap().pop()
    }
    
    pub fn push_send_batch(&self, packets: Vec<Packet>) {
        let mut send_queue = self.send_queue.lock().unwrap();
        for packet in packets {
            send_queue.push(packet);
        }
        
        drop(send_queue);
    }
    
    pub fn push_recv_batch(&self, packets: Vec<Packet>) {
        let mut recv_queue = self.recv_queue.lock().unwrap();
        for packet in packets {
            recv_queue.push(packet);
        }
        drop(recv_queue);
    }
    
    
    
    pub fn pop_recv_batch(&self, batch: usize) -> Option<Vec<Packet>> {
        let mut recv_queue = self.recv_queue.lock().unwrap();
        let mut packets = Vec::new();
        for _ in 0..batch {
            match recv_queue.pop() {
                Some(packet) => packets.push(packet),
                None => break,
            }
        }
        drop(recv_queue);
        if packets.is_empty() {
            None
        } else {
            Some(packets)
        }
    }
    
    pub fn pop_send_batch(&self, batch: usize) -> Option<Vec<Packet>> {
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
    
    pub fn pop_send_all(&self) -> Option<Vec<Packet>> {
        let mut send_queue = self.send_queue.lock().unwrap();
        let mut packets = Vec::new();
        while let Some(packet) = send_queue.pop() {
            packets.push(packet);
        }
        drop(send_queue);
        if packets.is_empty() {
            None
        } else {
            Some(packets)
        }
    }
    
    pub fn pop_recv_all(&self) -> Option<Vec<Packet>> {
        let mut recv_queue = self.recv_queue.lock().unwrap();
        let mut packets = Vec::new();
        while let Some(packet) = recv_queue.pop() {
            packets.push(packet);
        }
        drop(recv_queue);
        if packets.is_empty() {
            None
        } else {
            Some(packets)
        }
    }
}
