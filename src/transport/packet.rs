use super::TransportError;

pub const MAX_PACKET_SIZE: usize = 1024;
pub const HEADER_SIZE: usize = 10;

#[derive(Debug, Clone)]
pub struct Packet {
    pub version: u8,
    pub is_ack: u8,
    pub sequence: u64,
    pub payload: Vec<u8>,
}

impl Packet {
    pub fn new(version: u8, sequence: u64, payload: Vec<u8>) -> Result<Self, TransportError> {
        if payload.len() >  MAX_PACKET_SIZE - HEADER_SIZE {
            return Err(TransportError::TransportErrorPacketTooLarge);
        }
        Ok(Self {version, is_ack: 0x00, sequence, payload})
    }
    
    pub fn new_ack(version: u8, sequence: u64) -> Self {
        Self {version, is_ack: 0x01, sequence, payload: vec![]}
    }
    
    pub fn is_ack(&self) -> bool {
        self.is_ack != 0
    }
    
    pub fn encode(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(self.version);
        data.push(self.is_ack);
        data.extend_from_slice(&self.sequence.to_le_bytes());
        data.extend_from_slice(&self.payload);
        data
    }
    
    pub fn decode(data: &[u8]) -> Result<Self, TransportError> {
        if data.len() < HEADER_SIZE {
            return Err(TransportError::TransportErrorPacketTooSmall);
        }
        let version = data[0];
        let is_ack = data[1];
        let sequence = u64::from_le_bytes(data[2..10].try_into().unwrap());
        let payload = data[10..].to_vec();
        Ok(Self {version, is_ack, sequence, payload})
    }
}