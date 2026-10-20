use super::TransportError;

pub const MAX_PACKET_SIZE: usize = 1024;
pub const PACKET_HEADER_SIZE: usize = 10; // 包头的大小



pub const FRAGMENT_HEADER_SIZE: usize = 8;
pub const PAYLOAD_TYPE_SIZE: usize = 1;
/// 单个分片可携带的最大业务字节数：总长上限扣掉包头、类型字节和分片头。
pub const MAX_FRAGMENT_PAYLOAD_SIZE: usize =
    MAX_PACKET_SIZE - PACKET_HEADER_SIZE - PAYLOAD_TYPE_SIZE - FRAGMENT_HEADER_SIZE;
/// 未分片时可携带的最大业务字节数：总长上限扣掉包头和类型字节。
pub const MAX_UNFRAGMENTED_PAYLOAD_SIZE: usize =
    MAX_PACKET_SIZE - PACKET_HEADER_SIZE - PAYLOAD_TYPE_SIZE;
#[derive(Debug, Clone)]
pub struct Fragment {
    pub fragment_id: u32,    
    pub fragment_count: u32,
    pub payload: Vec<u8>,
}

impl Fragment {
    
    pub fn new(fragment_id: u32, fragment_count: u32, payload: Vec<u8>) -> Self {
        Self { fragment_id, fragment_count, payload }
    }
    pub fn encode(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(self.fragment_id.to_le_bytes().as_slice());
        data.extend_from_slice(self.fragment_count.to_le_bytes().as_slice());
        data.extend_from_slice(&self.payload);
        data
    }
    
    pub fn size(&self) -> usize {
        FRAGMENT_HEADER_SIZE + self.payload.len()
    }
    
    fn decode(data: &[u8]) -> Result<Self, TransportError> {
        if data.len() < FRAGMENT_HEADER_SIZE {
            return Err(TransportError::TransportErrorPacketTooSmall);
        }
        let fragment_id = u32::from_le_bytes(data[0..4].try_into().unwrap());
        let fragment_count = u32::from_le_bytes(data[4..8].try_into().unwrap());
        let payload = data[8..].to_vec();
        Ok(Fragment { fragment_id, fragment_count, payload })
    }
}

#[derive(Debug, Clone)]
pub enum PayloadType {
    PayloadTypeFragment(Fragment),
    PayloadTypeBytes(Vec<u8>),
    /// ACK 确认的目标包 128 位 key（sequence 与 fragment_id 的组合键），使分片也能被逐个确认。
    PayloadTypeAck(u128)
}

impl PayloadType {
    pub fn size(&self) -> usize {
        match self {
            PayloadType::PayloadTypeFragment(fragment) => {
                fragment.size()
            }
            PayloadType::PayloadTypeBytes(bytes) => {
                bytes.len()
            }
            PayloadType::PayloadTypeAck(_) => {
                16
            }
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut data = Vec::new();
        match self {
            PayloadType::PayloadTypeFragment(fragment) => {
                data.push(0x01);
                data.extend_from_slice(&fragment.encode());
            }
            PayloadType::PayloadTypeBytes(bytes) => {
                data.push(0x02);
                data.extend_from_slice(bytes);
            }
            PayloadType::PayloadTypeAck(key) => {
                data.push(0x03);
                data.extend_from_slice(&key.to_le_bytes());
            }
        }
        data
    }
    
    fn decode(data: &[u8]) -> Result<Self, TransportError> {
        if data.len() < PAYLOAD_TYPE_SIZE {
            return Err(TransportError::TransportErrorPacketTooSmall);
        }
        match data[0] {
           0x01 => {
            Ok(PayloadType::PayloadTypeFragment(Fragment::decode(&data[1..])?))            
           } 
           0x02 => {
            Ok(PayloadType::PayloadTypeBytes(data[1..].to_vec()))
           }
           0x03 => {
            if data.len() < PAYLOAD_TYPE_SIZE + 16 {
                return Err(TransportError::TransportErrorPacketTooSmall);
            }
            let key = u128::from_le_bytes(data[1..17].try_into().unwrap());
            Ok(PayloadType::PayloadTypeAck(key))
           }
           _ => Err(TransportError::TransportErrorInvalidPayloadType(format!("invalid payload type: {}", data[0]))),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Packet {
    pub version: u8,
    pub is_ack_or_fragment: u8, // 0x01 标识ack， 0x02标识标识fragment
    pub sequence: u64,
    pub payload: PayloadType,
}

impl Packet {
    pub fn new(version: u8, sequence: u64, payload: PayloadType) -> Result<Self, TransportError> {
        let is_ack_or_fragment = match payload {
            PayloadType::PayloadTypeFragment(_) => 0x02,
            PayloadType::PayloadTypeBytes(_) => 0x00,
            PayloadType::PayloadTypeAck(_) => 0x01,
        };
        Ok(Self {version, is_ack_or_fragment, sequence, payload})
    }
    
    /// 包的 128 位唯一键：高 64 位是 sequence，低 64 位是 fragment_id（未分片为 0）。
    /// ACK 包返回它所确认的目标包的 key，使每个分片都能被单独确认和删除。
    pub fn key(&self) -> u128 {
        match &self.payload {
            PayloadType::PayloadTypeAck(ack_key) => *ack_key,
            PayloadType::PayloadTypeFragment(fragment) => {
                ((self.sequence as u128) << 64) | fragment.fragment_id as u128
            }
            PayloadType::PayloadTypeBytes(_) => (self.sequence as u128) << 64,
        }
    }
    
    pub fn new_ack(version: u8, ack_key: u128) -> Self {
        Self {
            version,
            is_ack_or_fragment: 0x01,
            sequence: (ack_key >> 64) as u64,
            payload: PayloadType::PayloadTypeAck(ack_key),
        }
    }
    
    pub fn chunk_count(&self) -> u32 {
        match &self.payload {
            PayloadType::PayloadTypeFragment(fragment) => {
                fragment.fragment_count
            }
            PayloadType::PayloadTypeBytes(_) => {
                1
            }
            PayloadType::PayloadTypeAck(_) => {
                0
            }
        }
    }
    
    pub fn is_ack(&self) -> bool {
        self.is_ack_or_fragment & 0x01 != 0
    }
    
    pub fn is_fragment(&self) -> bool {
        self.is_ack_or_fragment & 0x02 != 0
    }
    
    pub fn encode(&self) -> Vec<u8> {
        let mut data = Vec::new();
        data.push(self.version);
        data.push(self.is_ack_or_fragment);
        data.extend_from_slice(&self.sequence.to_le_bytes());
        data.extend_from_slice(&self.payload.encode());
        data
    }
    
    pub fn decode(data: &[u8]) -> Result<Self, TransportError> {
        if data.len() < PACKET_HEADER_SIZE {
            return Err(TransportError::TransportErrorPacketTooSmall);
        }
        let version = data[0];
        let is_ack_or_fragment = data[1];
        let sequence = u64::from_le_bytes(data[2..10].try_into().unwrap());
        let payload = data[10..].to_vec();
        Ok(Self {version, is_ack_or_fragment, sequence, payload:PayloadType::decode(&payload)?})
    }

}


pub fn seperate_packet(packet: &Packet) -> Result<Vec<Packet>, TransportError> {
    match &packet.payload {
        PayloadType::PayloadTypeAck(_) => Ok(vec![packet.clone()]),
        PayloadType::PayloadTypeFragment(fragment) => {
            if fragment.payload.len() > MAX_FRAGMENT_PAYLOAD_SIZE {
                return Err(TransportError::TransportErrorInvalidPayloadType(format!("fragment payload too large: {}", fragment.payload.len())));
            }
            Ok(vec![packet.clone()])
        }
        PayloadType::PayloadTypeBytes(bytes) => {
            // 未分片时整包线长 = 包头 + 类型字节 + 载荷，不能超过上限
            if bytes.len() <= MAX_UNFRAGMENTED_PAYLOAD_SIZE {
                return Ok(vec![packet.clone()]);
            }
            let count = bytes.len().div_ceil(MAX_FRAGMENT_PAYLOAD_SIZE);
            let mut packets = Vec::with_capacity(count);
            for (i, chunk) in bytes.chunks(MAX_FRAGMENT_PAYLOAD_SIZE).enumerate() {
                let payload = PayloadType::PayloadTypeFragment(Fragment::new(i as u32, count as u32, chunk.to_vec()));
                packets.push(Packet::new(packet.version, packet.sequence, payload)?);
            }
            Ok(packets)
        }
    }
}

pub fn merge_packets(packets: &mut Vec<Packet>) -> Result<Packet, TransportError> {
    packets.sort_by_key(|packet| { 
        match &packet.payload {
            PayloadType::PayloadTypeFragment(fragment) => {
                (packet.sequence, fragment.fragment_id)
            }
            PayloadType::PayloadTypeBytes(_) | PayloadType::PayloadTypeAck(_) => {
                (packet.sequence, 0)
            }
        }
    });
    
    if packets.is_empty() {
        return Err(TransportError::TransportErrorInvalidPayloadType("no packets to merge".to_string()));
    }
    let (sequence, version) = (packets[0].sequence, packets[0].version);
    let packet_count = packets.len();
    let mut payload = Vec::new();

    for (i, packet) in packets.into_iter().enumerate() {
        if packet.sequence != sequence || packet.version != version {
            return Err(TransportError::TransportErrorInvalidPayloadType(format!("the sequence or version of the packet to be merged is not the same: {}", packet.sequence)));
        }

        match &packet.payload {
            PayloadType::PayloadTypeBytes(_) | PayloadType::PayloadTypeAck(_) => {
                return Err(TransportError::TransportErrorInvalidPayloadType(format!("the payload to be merged is not fragment: {}", packet.payload.size())));
            }
            PayloadType::PayloadTypeFragment(fragment) => {
                if fragment.fragment_count as usize != packet_count {
                   return Err(TransportError::TransportErrorInvalidPayloadType(format!("the fragment count of the packet to be merged is not the same: {}", fragment.fragment_count)));
                }
                if fragment.fragment_id != i as u32 {
                   return Err(TransportError::TransportErrorInvalidPayloadType(format!("the fragment id of the packet to be merged is not the same: {}", fragment.fragment_id)));
                }
                payload.extend_from_slice(&fragment.payload);
            }
        }
    }
    Ok(Packet::new(version, sequence, PayloadType::PayloadTypeBytes(payload))?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ack_carries_fragment_key() {
        let fragment = Packet::new(
            1,
            10,
            PayloadType::PayloadTypeFragment(Fragment::new(2, 3, vec![1, 2, 3])),
        )
        .unwrap();
        let ack = Packet::new_ack(1, fragment.key());
        assert!(ack.is_ack());
        assert_eq!(ack.sequence, 10);
        assert_eq!(ack.key(), fragment.key());

        let decoded = Packet::decode(&ack.encode()).unwrap();
        assert!(decoded.is_ack());
        assert_eq!(decoded.key(), fragment.key());
    }

    #[test]
    fn seperate_and_merge_roundtrip() {
        let data: Vec<u8> = (0..3000u32).map(|i| (i % 251) as u8).collect();
        let packet = Packet::new(1, 42, PayloadType::PayloadTypeBytes(data.clone())).unwrap();

        let mut fragments = seperate_packet(&packet).unwrap();
        assert_eq!(fragments.len(), 3);
        for fragment in &fragments {
            assert!(fragment.is_fragment());
            assert!(fragment.encode().len() <= MAX_PACKET_SIZE);
        }

        // 模拟乱序到达
        fragments.reverse();
        let merged = merge_packets(&mut fragments).unwrap();
        assert_eq!(merged.sequence, 42);
        match merged.payload {
            PayloadType::PayloadTypeBytes(bytes) => assert_eq!(bytes, data),
            other => panic!("合并结果应为完整字节包, 实际: {:?}", other),
        }
    }

    #[test]
    fn unfragmented_size_boundary() {
        let packet = Packet::new(
            1,
            7,
            PayloadType::PayloadTypeBytes(vec![0u8; MAX_UNFRAGMENTED_PAYLOAD_SIZE]),
        )
        .unwrap();
        let packets = seperate_packet(&packet).unwrap();
        assert_eq!(packets.len(), 1);
        assert_eq!(packets[0].encode().len(), MAX_PACKET_SIZE);

        let packet = Packet::new(
            1,
            8,
            PayloadType::PayloadTypeBytes(vec![0u8; MAX_UNFRAGMENTED_PAYLOAD_SIZE + 1]),
        )
        .unwrap();
        let packets = seperate_packet(&packet).unwrap();
        assert_eq!(packets.len(), 2);
        assert!(packets.iter().all(|packet| packet.encode().len() <= MAX_PACKET_SIZE));
    }
}