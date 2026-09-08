use crate::{pending::Pending, rpc_transport::RpcTransport};
use log::trace;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

/// Wire framing: an 8-byte big-endian request id, then the payload.
const ID_LEN: usize = size_of::<u64>();

#[non_exhaustive]
pub struct UdpTransport {
    socket: std::sync::Arc<UdpSocket>,
    pending: std::sync::Arc<Pending>,
}

impl UdpTransport {
    pub fn new(socket: UdpSocket) -> Self {
        // spawn receiving loop
        let socket = std::sync::Arc::new(socket);
        let socket_clone = socket.clone();
        let pending = Pending::new();
        let pending_clone = pending.clone();
        // TODO deal with spawn handle
        tokio::spawn(async move {
            let mut buf = vec![0u8; 1024];
            loop {
                let (len, addr) = socket_clone.recv_from(&mut buf).await.unwrap();
                trace!("Received {} bytes from {}", len, addr);
                if len < ID_LEN {
                    trace!("Datagram from {} too short to carry an id, dropping", addr);
                    continue;
                }
                let id = u64::from_be_bytes(buf[0..ID_LEN].try_into().unwrap());
                if !pending_clone.deliver(id, buf[ID_LEN..len].into()) {
                    trace!("No one waiting on id {} from {}, dropping", id, addr);
                }
            }
        });
        Self { socket, pending }
    }
}

impl RpcTransport for UdpTransport {
    // TODO deal with unwrap properly ie change the transport to return Result instead of unwrapping
    async fn send_receive(&self, payload: Vec<u8>, address: SocketAddr) -> Vec<u8> {
        let id = self.pending.next_id();
        let msg = self.pending.register(id);
        let mut datagram = Vec::with_capacity(ID_LEN + payload.len());
        datagram.extend_from_slice(&id.to_be_bytes());
        datagram.extend_from_slice(&payload);
        self.socket.send_to(&datagram, address).await.unwrap();
        // None is unreachable: the slot outlives this await.
        msg.await.unwrap()
    }
}
