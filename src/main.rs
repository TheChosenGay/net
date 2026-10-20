use net::transport::packet::Packet;

fn main() {
    match Packet::new(1, 0, vec![1, 2, 3, 4, 5])  {
        Ok(p) => println!("{:?}", p),
        Err(e) => println!("error: {:?}", e),
    }
}