use std::net::SocketAddr;

#[derive(Debug, Clone)]
pub enum ControlPacketError {
    InvalidPacketType(String),
}
pub enum ControlPacketType {
    Request(SocketAddr), // 请求链接
    Challenge(u64), // 挑战包
    ChallengeResponse(u64), // 挑战响应包
    Accept,  // 接收链接
    Reject,  // 拒绝链接
    Disconnect,  // 断开链接
}

impl ControlPacketType {

    /// 生成一个新的随机挑战值（nonce）。应在发送 Challenge 时调用，
    /// 接收方在 ChallengeResponse 中原样带回，发送方据此校验响应属于本次挑战。
    /// 注意不要用递增 sequence 充当挑战值，那是可预测的。
    pub fn gen_challenge_seq() -> u64 {
        rand::random::<u64>()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            ControlPacketType::Request(addr) => {
                buf.push(0x01);
                buf.extend_from_slice(addr.to_string().as_bytes());
            },
            ControlPacketType::Challenge(challenge) => {
                buf.push(0x02);
                buf.extend_from_slice(&challenge.to_le_bytes());
            },
            ControlPacketType::ChallengeResponse(challenge) => {
                buf.push(0x03);
                buf.extend_from_slice(&challenge.to_le_bytes());
            },
            ControlPacketType::Accept => {
                buf.push(0x04);
            },
            ControlPacketType::Reject => {
                buf.push(0x05);
            },
            ControlPacketType::Disconnect => {
                buf.push(0x06);
            },

        }
        buf
    }
    
    pub fn decode(bytes: &[u8]) -> Result<Self, ControlPacketError> {
        if bytes.is_empty() {
            return Err(ControlPacketError::InvalidPacketType("empty control packet".to_string()));
        }
        match bytes[0] {
            0x01 => {
                let addr_text = std::str::from_utf8(&bytes[1..])
                    .map_err(|e| ControlPacketError::InvalidPacketType(format!("invalid request address encoding: {}", e)))?;
                let addr = addr_text.parse::<SocketAddr>()
                    .map_err(|e| ControlPacketError::InvalidPacketType(format!("invalid request address: {}", e)))?;
                Ok(Self::Request(addr))
            }
            0x02 => Ok(Self::Challenge(u64::from_le_bytes(bytes[1..9].try_into().unwrap()))),
            0x03 => Ok(Self::ChallengeResponse(u64::from_le_bytes(bytes[1..9].try_into().unwrap()))),
            0x04 => Ok(Self::Accept),
            0x05 => Ok(Self::Reject),
            0x06 => Ok(Self::Disconnect),
            _ => return Err(ControlPacketError::InvalidPacketType(format!("Invalid connect packet type: {}", bytes[0]))),
        }
    }
    
}