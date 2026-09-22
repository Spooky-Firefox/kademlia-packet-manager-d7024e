//! The client half of the connection-shaped RPC framing, over any connection.
//!
//! One connection per request: write an 8-byte big-endian id and the payload,
//! half-close, read the echoed id and the reply behind it. There is no length
//! prefix, so the half-close and the final EOF are what delimit the two
//! messages. [`handle_rpc::serve`](crate::handle_rpc::serve) is the other end.
//!
//! [`tcp_transport`](super::tcp_transport) opens a real `TcpStream` and
//! [`networked_debug_transport`](super::networked_debug_transport) an
//! in-process pipe; the framing lives here so a test over the fake wire
//! exercises what `main` runs over real TCP.

use crate::rpc_transport::request_id;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Wire framing: an 8-byte big-endian request id, then the payload.
const ID_LEN: usize = size_of::<u64>();

/// Send `payload` over `stream` and hand back the reply body, id stripped.
/// `stream` is consumed: it is good for exactly one request.
pub async fn stream_send_receive<S>(mut stream: S, payload: Vec<u8>) -> std::io::Result<Vec<u8>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Masked, not just assumed clear: a random u64 sets the reply tag half the
    // time, and `frame_reply` is what sets it legitimately on the way back.
    let id = request_id(rand::random::<u64>());

    stream.write_all(&id.to_be_bytes()).await?;
    stream.write_all(&payload).await?;
    stream.shutdown().await?;

    let mut id_bytes = [0u8; ID_LEN];
    stream.read_exact(&mut id_bytes).await?;
    let echoed = u64::from_be_bytes(id_bytes);
    // An error, not an assert: the peer writes these bytes, so a panic here
    // would be a peer's choice to make about us.
    if request_id(echoed) != id {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("reply echoed request id {echoed:#x}, expected {id:#x}"),
        ));
    }

    let mut body = Vec::new();
    stream.read_to_end(&mut body).await?;
    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_transport::reply_id;

    /// Drive the peer end of a `duplex` pair: read the request to EOF, then
    /// answer with `reply_body` under the id that came in, tagged as a reply
    /// the way [`frame_reply`](crate::handle_rpc::frame_reply) does.
    ///
    /// `mangle_id` rewrites the echoed id first, for the malformed-reply case.
    fn spawn_peer(
        mut stream: tokio::io::DuplexStream,
        reply_body: Vec<u8>,
        mangle_id: impl Fn(u64) -> u64 + Send + 'static,
    ) {
        tokio::spawn(async move {
            let mut request = Vec::new();
            stream.read_to_end(&mut request).await.unwrap();
            let id = u64::from_be_bytes(request[..ID_LEN].try_into().unwrap());

            let mut reply = mangle_id(reply_id(id)).to_be_bytes().to_vec();
            reply.extend_from_slice(&reply_body);
            stream.write_all(&reply).await.unwrap();
            stream.shutdown().await.unwrap();
        });
    }

    #[tokio::test]
    async fn round_trips_a_request_and_strips_the_echoed_id() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        spawn_peer(server, b"STORED".to_vec(), |id| id);

        let reply = stream_send_receive(client, b"STORE...".to_vec())
            .await
            .unwrap();

        assert_eq!(reply, b"STORED");
    }

    /// A reply carrying somebody else's id is not this request's answer. It
    /// comes back as an error rather than panicking the caller — see the
    /// comment on the check itself.
    #[tokio::test]
    async fn a_reply_under_the_wrong_id_is_an_error_not_a_panic() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        spawn_peer(server, b"STORED".to_vec(), |id| id.wrapping_add(1));

        let result = stream_send_receive(client, b"STORE...".to_vec()).await;

        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::InvalidData,
            "a mismatched echo is a malformed reply"
        );
    }

    /// A peer that closes without answering ends the call, rather than leaving
    /// it parked on a connection nothing will ever write to.
    #[tokio::test]
    async fn a_peer_that_answers_nothing_ends_the_call() {
        let (client, server) = tokio::io::duplex(64 * 1024);
        tokio::spawn(async move {
            let mut request = Vec::new();
            let mut server = server;
            server.read_to_end(&mut request).await.unwrap();
            drop(server);
        });

        assert!(stream_send_receive(client, b"ping".to_vec()).await.is_err());
    }
}
