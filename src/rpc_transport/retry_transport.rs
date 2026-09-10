//! An [`RpcTransport`] over any datagram channel ([`DataRxTx`]).
//!
//! It frames an 8-byte big-endian request id onto every payload, matches
//! replies back to the caller that is awaiting them through [`Pending`], and
//! resends a request that goes unanswered until a reply arrives or the attempt
//! budget runs out. The concrete channel is supplied by the caller:
//! [`udp_transport`](super::udp_transport) plugs in a real `UdpSocket`,
//! [`networked_debug_transport`](super::networked_debug_transport) an in-process
//! fake wire.

use crate::pending::Pending;
use crate::rpc_transport::RpcTransport;
use crate::rpc_transport::data_rx_tx::DataRxTx;
use log::trace;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
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
    pub fn new(socket: T) -> Self {
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
                if !pending_clone.deliver(id, buf[ID_LEN..len].into()) {
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
        let id = self.pending.next_id();
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
