use crate::rpc_transport::RpcTransport;
use std::net::SocketAddr;
use std::vec::Vec;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub struct TcpTransport;

impl RpcTransport for TcpTransport {
    async fn send_receive(
        &self,
        payload: Vec<u8>,
        address: SocketAddr,
    ) -> std::io::Result<Vec<u8>> {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream.write_all(&payload).await?;
        // Half-close the write side so the peer sees EOF once the whole
        // request has arrived, then read until it closes its own side
        // with the reply. There is no length framing, so EOF is the only
        // signal either end has that the other is done sending.
        stream.shutdown().await?;
        let mut outbuff = Vec::new();
        stream.read_to_end(&mut outbuff).await?;
        Ok(outbuff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_transport::RpcTransport;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Accepts one connection, reads until the client half-closes (EOF),
    /// and echoes the request back verbatim before closing its own write
    /// side.
    async fn spawn_echo_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            stream.read_to_end(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        addr
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn round_trips_a_small_payload() {
        let addr = spawn_echo_server().await;
        let transport = TcpTransport;

        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            transport.send_receive(b"ping".to_vec(), addr),
        )
        .await
        .expect("server answered within 1s")
        .unwrap();

        assert_eq!(reply, b"ping");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn round_trips_a_payload_larger_than_one_chunk() {
        // Larger than the 1024-byte mini_buff / write loop chunk size, to
        // exercise the multi-iteration write and read loops.
        let addr = spawn_echo_server().await;
        let transport = TcpTransport;
        let payload = vec![0xABu8; 5000];

        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            transport.send_receive(payload.clone(), addr),
        )
        .await
        .expect("server answered within 1s")
        .unwrap();

        assert_eq!(reply, payload);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn connect_failure_is_surfaced_as_an_error() {
        // Nothing listens on this port: expect a connection-refused io::Error
        // rather than a panic or a silently empty Ok(vec![]).
        let dead: SocketAddr = "127.0.0.1:1".parse().unwrap();
        let transport = TcpTransport;

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            transport.send_receive(b"ping".to_vec(), dead),
        )
        .await
        .expect("connect should fail fast, not hang");

        assert!(result.is_err());
    }
}
