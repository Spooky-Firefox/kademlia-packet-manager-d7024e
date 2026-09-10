use std::io;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;

mod close_nodes;
mod handle_rpc;
mod pending;
mod rpc;
mod rpc_transport;

use rpc_transport::RpcTransport;
use rpc_transport::udp_transport::UdpTransport;

/// Stand-in for a remote node: echoes every datagram back verbatim. The 8-byte
/// request id prefix survives the round trip, so `UdpTransport` can match the
/// reply to the request that is awaiting it.
async fn spawn_echo_peer() -> io::Result<SocketAddr> {
    let socket = UdpSocket::bind("127.0.0.1:0").await?;
    let addr = socket.local_addr()?;
    tokio::spawn(async move {
        let mut buf = vec![0u8; 1024];
        loop {
            let (len, from) = socket.recv_from(&mut buf).await.unwrap();
            println!(
                "received {} bytes from {}, waiting 500ms then echoing back",
                len, from
            );
            // Long enough that the caller resends once or twice first, short
            // enough to answer before its attempt budget runs out.
            tokio::time::sleep(Duration::from_millis(500)).await;
            socket.send_to(&buf[..len], from).await.unwrap();
        }
    });
    Ok(addr)
}

#[tokio::main]
async fn main() -> io::Result<()> {
    env_logger::init();

    let peer = spawn_echo_peer().await?;
    let transport = UdpTransport::new(UdpSocket::bind("127.0.0.1:0").await?);
    println!("Echo peer listening on {peer}");

    // send_receive resends on silence and gives up with a TimedOut error once
    // its attempt budget is spent, so no outer timeout is needed here.
    println!("Sending \"ping\" to {peer}");
    match transport.send_receive(b"ping".to_vec(), peer).await {
        Ok(payload) => println!(
            "Got {:?} back from {peer}",
            String::from_utf8_lossy(&payload)
        ),
        Err(e) => println!("No response from {peer}: {e}"),
    }

    // Nothing is listening here: every resend is swallowed. The 200ms outer
    // timeout fires well before send_receive's own budget, and dropping the
    // future deregisters the slot via PendingResponse::drop.
    let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
    println!("Sending \"ping\" to dead socket:{dead}");
    match tokio::time::timeout(
        Duration::from_millis(200),
        transport.send_receive(b"ping".to_vec(), dead),
    )
    .await
    {
        Ok(payload) => println!(
            "Got {:?} back from {dead}",
            String::from_utf8_lossy(&payload?)
        ),
        Err(_) => println!("No response from {dead} within 200ms, slot cleaned up"),
    }

    Ok(())
}
