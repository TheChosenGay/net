use net::transport::packet::{Packet, PayloadType};

fn main() {
    match Packet::new(1, 0, PayloadType::Bytes(vec![1, 2, 3, 4, 5]))  {
        Ok(p) => println!("{:?}", p),
        Err(e) => println!("error: {:?}", e),
    }
}