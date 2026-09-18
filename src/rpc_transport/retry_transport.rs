//! An [`RpcTransport`] over any datagram channel ([`DataRxTx`]).
//!
//! It frames an 8-byte big-endian request id onto every payload, matches
//! replies back to the caller that is awaiting them through [`Pending`], and
//! resends a request that goes unanswered until a reply arrives or the attempt
//! budget runs out. The concrete channel is supplied by the caller:
//! [`udp_transport`](super::udp_transport) plugs in a real `UdpSocket`,
//! [`networked_debug_transport`](super::networked_debug_transport) an in-process
//! fake wire.
//!
//! # The one receive loop
//!
//! A node's socket carries both directions: replies to what it asked, and
//! requests other nodes are asking it. There is exactly one loop reading that
//! socket — this module's — and it splits the two on
//! [`REPLY_TAG`](super::REPLY_TAG): a tagged datagram goes to [`Pending`], an
//! untagged one is a request and goes out the channel given to
//! [`with_requests`](RetryTransport::with_requests), for
//! [`handle_rpc`](crate::handle_rpc) to answer. A transport built with plain
//! [`new`](RetryTransport::new) has no channel and drops requests, which is
//! all a client-only node ever needs.

use crate::handle_rpc::Request;
use crate::pending::Pending;
use crate::rpc_transport::RpcTransport;
use crate::rpc_transport::data_rx_tx::DataRxTx;
use crate::rpc_transport::{is_reply, request_id};
use log::trace;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;

/// Wire framing: an 8-byte big-endian request id, then the payload.
const ID_LEN: usize = size_of::<u64>();

/// How long to wait for a reply before resending the request.
const RESEND_INTERVAL: Duration = Duration::from_millis(200);

/// How many times to send a request before giving up on it.
const MAX_ATTEMPTS: usize = 5;

#[non_exhaustive]
pub struct RetryTransport<T: DataRxTx + Send + Sync + 'static> {
    socket: Arc<T>,
    pending: Arc<Pending>,
}

impl<T: DataRxTx + Send + Sync + 'static> RetryTransport<T> {
    /// A client-only transport: it answers nothing, so inbound requests are
    /// dropped. Use [`with_requests`](Self::with_requests) to serve them.
    pub fn new(socket: T) -> Self {
        Self::build(socket, None)
    }

    /// A transport that also feeds inbound requests to `requests`, for
    /// [`handle_rpc::serve`](crate::handle_rpc::serve) to answer over the same
    /// socket this sends on — which is what makes the address a peer sees us
    /// send from the address it can reach us at.
    pub fn with_requests(socket: T, requests: mpsc::Sender<Request>) -> Self {
        Self::build(socket, Some(requests))
    }

    /// The socket this transport sends and receives on, so the server side can
    /// write its replies back out the same one.
    pub fn socket(&self) -> Arc<T> {
        Arc::clone(&self.socket)
    }

    fn build(socket: T, requests: Option<mpsc::Sender<Request>>) -> Self {
        // spawn receiving loop
        let socket = Arc::new(socket);
        let socket_clone = socket.clone();
        let pending = Pending::new();
        let pending_clone = pending.clone();
        // TODO deal with spawn handle
        tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                let (len, addr) = match socket_clone.receive_packet(&mut buf).await {
                    Ok(framed) => framed,
                    // A dead channel ends the loop rather than panicking the
                    // task: there is nothing left to receive.
                    Err(e) => {
                        trace!("Receive loop stopping: {e}");
                        break;
                    }
                };
                trace!("Received {len} bytes from {addr}");
                if len < ID_LEN {
                    trace!("Datagram from {addr} too short to carry an id, dropping");
                    continue;
                }
                let id = u64::from_be_bytes(buf[0..ID_LEN].try_into().unwrap());

                // Untagged: nobody here is waiting on this id, it is a
                // question being asked of us. See `REPLY_TAG`.
                if !is_reply(id) {
                    let Some(requests) = requests.as_ref() else {
                        trace!("Request {id} from {addr} but nothing serves them, dropping");
                        continue;
                    };
                    let request = Request::new(id, addr, buf[ID_LEN..len].into());
                    // try_send, not send: a full queue sheds this request the
                    // way a lossy wire already would, rather than parking the
                    // one loop that drains this socket.
                    if requests.try_send(request).is_err() {
                        trace!("Request {id} from {addr} shed, nothing draining the queue");
                    }
                    continue;
                }

                if !pending_clone.deliver(request_id(id), buf[ID_LEN..len].into()) {
                    trace!("No one waiting on id {id} from {addr}, dropping");
                }
            }
        });
        Self { socket, pending }
    }
}

impl<T: DataRxTx + Send + Sync + 'static> RpcTransport for RetryTransport<T> {
    async fn send_receive(
        &self,
        payload: Vec<u8>,
        address: SocketAddr,
    ) -> std::io::Result<Vec<u8>> {
        // Masked, not just assumed clear: the tag bit is what marks the reply
        // this registers for, and an id that set it would register under one
        // number and be answered under another.
        let id = request_id(self.pending.next_id());
        let msg = self.pending.register(id);
        // Borrowed (never moved) by every select! below: a timed-out attempt
        // must not drop this, or PendingResponse::drop deregisters the id and
        // the eventual reply is delivered to no one.
        tokio::pin!(msg);

        let mut datagram = Vec::with_capacity(ID_LEN + payload.len());
        datagram.extend_from_slice(&id.to_be_bytes());
        datagram.extend_from_slice(&payload);

        for _attempt in 0..MAX_ATTEMPTS {
            self.socket.send_packet(&datagram, address).await?;
            tokio::select! {
                // None is unreachable: the slot outlives this await.
                res = &mut msg => return Ok(res.unwrap()),
                _ = sleep(RESEND_INTERVAL) => {
                    trace!("No reply for id {id}, resending");
                }
            }
        }

        Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "no response after maximum attempts",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_transport::networked_debug_transport::Network;
    use crate::rpc_transport::reply_id;

    /// The reason requests and replies are told apart by a tag rather than by
    /// "did an id match something pending".
    ///
    /// Request ids are minted by whoever sends the request, and every node's
    /// counter starts at 0 — so a peer's first request carries id 0 while our
    /// own first outgoing call is also waiting on id 0. Matching on the id
    /// alone would hand that peer's question to whoever is awaiting our
    /// answer: the call resolves with a stranger's request body, and the
    /// request itself is never served.
    #[tokio::test]
    async fn a_request_reusing_a_pending_id_is_not_mistaken_for_its_reply() {
        let network = Network::new();
        let (requests, mut inbox) = mpsc::channel(8);
        let transport = RetryTransport::with_requests(network.bind_any(), requests);
        let node = transport.socket().local_addr();

        // An outgoing call that nobody will ever answer: it registers the
        // first id this node hands out, 0, and sits there resending.
        let nothing_bound: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let call = tokio::spawn(async move {
            transport
                .send_receive(b"a question of our own".to_vec(), nothing_bound)
                .await
        });
        // Let the call register its id before the collision arrives, or there
        // would be nothing for it to collide with.
        sleep(Duration::from_millis(50)).await;

        // A peer asks us something, numbering it from its own counter — which
        // starts where ours did.
        let peer = network.bind_any();
        let mut datagram = 0u64.to_be_bytes().to_vec();
        datagram.extend_from_slice(b"a question of theirs");
        peer.send_to(&datagram, node);

        // It is served as the request it is...
        let request = tokio::time::timeout(Duration::from_secs(1), inbox.recv())
            .await
            .expect("delivered within 1s")
            .expect("the channel is open");
        assert_eq!(request.id, 0);
        assert_eq!(request.payload, b"a question of theirs");

        // ...and our own call is still waiting, not resolved by it.
        let answered = call.await.expect("the call task did not panic");
        assert!(
            answered.is_err(),
            "the peer's request resolved our pending call: {answered:?}"
        );
    }

    /// A tagged datagram is a reply and goes to the caller awaiting it, not
    /// out the request channel.
    #[tokio::test]
    async fn a_tagged_datagram_is_delivered_as_a_reply() {
        let network = Network::new();
        let (requests, mut inbox) = mpsc::channel(8);
        let transport = RetryTransport::with_requests(network.bind_any(), requests);
        let node = transport.socket().local_addr();

        let peer = network.bind_any();
        let peer_addr = peer.local_addr();
        tokio::spawn(async move {
            while let Some((from, datagram)) = peer.recv_from().await {
                let id = u64::from_be_bytes(datagram[..ID_LEN].try_into().unwrap());
                let mut reply = reply_id(id).to_be_bytes().to_vec();
                reply.extend_from_slice(b"an answer");
                peer.send_to(&reply, from);
            }
        });

        let answered = tokio::time::timeout(
            Duration::from_secs(1),
            transport.send_receive(b"ping".to_vec(), peer_addr),
        )
        .await
        .expect("answered within 1s")
        .expect("the peer replied");

        assert_eq!(answered, b"an answer");
        assert_eq!(node, transport.socket().local_addr());
        assert!(
            inbox.try_recv().is_err(),
            "a reply must not be served as a request"
        );
    }
}
