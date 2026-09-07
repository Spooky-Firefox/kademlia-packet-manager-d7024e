use std::io;
use tokio::net::UdpSocket;

mod debug_transport;
mod pending;
mod rpc;
mod rpc_transport_trait;
mod udp_transport;

#[tokio::main]
async fn main() -> io::Result<()> {
    let sock = UdpSocket::bind("0.0.0.0:8080").await?;
    let mut buf = [0; 1024];
    println!("Listening on 0.0.0.0:8080");
    sock.send_to("hello".as_bytes(), "127.0.0.1:8080").await?;
    loop {
        let (len, addr) = sock.recv_from(&mut buf).await?;

        let handle = tokio::spawn(async move {
            println!("{:?} bytes received from {:?}", len, addr);
            println!(
                "Received data as string: {}",
                String::from_utf8_lossy(&buf[..len])
            );
        });
        handle.await.unwrap();
    }
}
