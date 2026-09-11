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
        let id = rand::random::<u64>();
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream.write_all(&id.to_be_bytes()).await?;
        stream.write_all(&payload).await?;
        // Half-close the write side so the peer sees EOF once the whole
        // request has arrived, then read until it closes its own side
        // with the reply. There is no length framing, so EOF is the only
        // signal either end has that the other is done sending.
        stream.shutdown().await?;
        let mut outbuff = Vec::new();
        let mut id_bytes = [0u8; 8];
        stream.read_exact(&mut id_bytes).await?;
        let echoed_id = u64::from_be_bytes(id_bytes);
        assert_eq!(echoed_id, id);
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
    /// and echoes the request back verbatim (id prefix included) before
    /// closing its own write side.
    ///
    /// Because the id is part of what gets echoed, this does not exercise
    /// `send_receive`'s id-stripping on its own — see
    /// [`spawn_server_with_reply`] for a peer that answers with a body that
    /// isn't just the request played back, which is what a real STORE ack
    /// looks like.
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

    /// Accepts one connection, reads the client's 8-byte id and the request
    /// behind it, then replies with that same id framing `reply_body` —
    /// the shape a real peer's answer takes (e.g. `handle_rpc::frame_reply`),
    /// as opposed to an echo of the request itself.
    async fn spawn_server_with_reply(reply_body: Vec<u8>) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            let (id_bytes, _payload) = request.split_at(8);

            let mut reply = id_bytes.to_vec();
            reply.extend_from_slice(&reply_body);
            stream.write_all(&reply).await.unwrap();
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

    /// A reply that echoes the request's id but answers with its own body
    /// (not a copy of the payload) comes back as just that body: the id is
    /// read and matched, not left on the front of the returned bytes.
    #[tokio::test(flavor = "multi_thread")]
    async fn strips_the_echoed_id_and_returns_only_the_reply_body() {
        let addr = spawn_server_with_reply(b"STORED".to_vec()).await;
        let transport = TcpTransport;

        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            transport.send_receive(b"STORE...".to_vec(), addr),
        )
        .await
        .expect("server answered within 1s")
        .unwrap();

        assert_eq!(reply, b"STORED");
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
