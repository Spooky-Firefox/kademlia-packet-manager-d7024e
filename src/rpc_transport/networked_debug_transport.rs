//! A fake network made of channels.
//!
//! [`Network`] plays the role of the wire: every [`Endpoint`] bound to it is a
//! stand-in for a `UdpSocket`, and a datagram sent to a [`SocketAddr`] is handed
//! to whichever endpoint bound that address. The addresses are real
//! `SocketAddr`s but nothing is bound in the OS and nothing leaves the process,
//! so tests get UDP semantics — unreliable, unordered, silently dropped when
//! nobody is home — without touching a port or waiting on a real timeout.
//!
//! [`NetworkedDebugTransport`] is then just
//! [`RetryTransport`](crate::rpc_transport::retry_transport::RetryTransport)
//! with an [`Endpoint`] in place of the `UdpSocket`: the id framing, the
//! pending-request bookkeeping, and the resend loop are all shared code, so a
//! test against the fake wire exercises the same transport `main` runs over
//! real UDP.
//!
//! # Two devices on one network
//!
//! Both devices bind an [`Endpoint`] to the same [`Network`]. The caller wraps
//! its endpoint in a [`NetworkedDebugTransport`], which gives it
//! [`send_receive`](RpcTransport::send_receive); the peer drives its raw
//! endpoint, because a transport's receive loop only resolves replies to
//! requests *it* sent and drops everything else.
//!
//! Doctests do not run in a binary crate, so this is illustrative; the tests at
//! the bottom of this file exercise the same path.
//!
//! ```ignore
//! use crate::rpc_transport::networked_debug_transport::{Endpoint, Network, NetworkedDebugTransport};
//! use crate::rpc_transport::RpcTransport;
//! use std::net::SocketAddr;
//! use std::time::Duration;
//!
//! let network: Network = Network::new();
//!
//! // Device B: a peer that answers requests. Note the framing contract — the
//! // 8-byte id prefix must come back untouched, or the caller cannot tell
//! // which of its in-flight requests this reply belongs to.
//! let peer: Endpoint = network.bind_any();
//! let peer_addr: SocketAddr = peer.local_addr();
//! tokio::spawn(async move {
//!     // recv_from yields Option<(SocketAddr, Vec<u8>)>: the sender and the
//!     // datagram, and None once the endpoint is unbound.
//!     while let Some((from, datagram)) = peer.recv_from().await {
//!         let (id, body): (&[u8], &[u8]) = datagram.split_at(size_of::<u64>());
//!         let mut reply: Vec<u8> = id.to_vec();
//!         reply.extend_from_slice(b"pong: ");
//!         reply.extend_from_slice(body);
//!         peer.send_to(&reply, from);
//!     }
//! });
//!
//! // Device A: a node that issues RPCs.
//! let node: NetworkedDebugTransport = NetworkedDebugTransport::new(network.bind_any());
//!
//! // send_receive resends on silence and, after a few attempts with no reply,
//! // gives up with an `io::Error` of kind `TimedOut`. Wrapping it in an outer
//! // `timeout` still works and simply bounds that from the outside.
//! let reply: Vec<u8> = tokio::time::timeout(
//!     Duration::from_secs(1),
//!     node.send_receive(b"ping".to_vec(), peer_addr),
//! )
//! .await
//! .expect("outer timeout not reached")
//! .expect("peer answered before the attempt budget ran out");
//! assert_eq!(reply, b"pong: ping");
//! ```
//!
//! To watch a request fail instead, aim it at an address nobody bound — the
//! network swallows every resend, exactly like datagrams to a dead port, and
//! `send_receive` returns `TimedOut` once the attempts are spent — or build the
//! network with [`Network::with_config`] and give it latency or loss.
use crate::rpc_transport::data_rx_tx::DataRxTx;
use crate::rpc_transport::retry_transport::RetryTransport;
use dashmap::DashMap;
use log::trace;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, mpsc};

/// Wire framing: an 8-byte big-endian request id, then the payload.
const ID_LEN: usize = size_of::<u64>();

/// Where [`Network::bind_any`] starts handing out ports.
const FIRST_EPHEMERAL_PORT: u16 = 49152;

/// `(sender, payload)`, as it appears at the receiving endpoint.
type Datagram = (SocketAddr, Vec<u8>);

/// How much the fake wire misbehaves. The default is a perfect network:
/// no delay, nothing lost.
#[derive(Clone, Copy, Debug, Default)]
pub struct NetworkConfig {
    /// Delay applied to every delivered datagram.
    pub latency: Duration,
    /// Fraction of datagrams dropped in flight, `0.0..=1.0`.
    pub loss: f64,
}

struct Inner {
    /// The bound endpoints' inboxes. A missing address is a dead port.
    sockets: DashMap<SocketAddr, mpsc::UnboundedSender<Datagram>>,
    config: NetworkConfig,
    next_port: AtomicU16,
}

/// The fake wire. Cheap to clone: every clone refers to the same network.
#[derive(Clone)]
pub struct Network {
    inner: Arc<Inner>,
}

impl Default for Network {
    fn default() -> Self {
        Self::with_config(NetworkConfig::default())
    }
}

impl Network {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_config(config: NetworkConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                sockets: DashMap::new(),
                config,
                next_port: AtomicU16::new(FIRST_EPHEMERAL_PORT),
            }),
        }
    }

    /// Bind `addr`, or `None` if some live endpoint already holds it.
    pub fn bind(&self, addr: SocketAddr) -> Option<Endpoint> {
        let (tx, rx) = mpsc::unbounded_channel();
        match self.inner.sockets.entry(addr) {
            dashmap::Entry::Occupied(_) => None,
            dashmap::Entry::Vacant(slot) => {
                slot.insert(tx);
                Some(Endpoint {
                    addr,
                    network: self.clone(),
                    inbox: Mutex::new(rx),
                })
            }
        }
    }

    /// Bind a fresh loopback address, the way `UdpSocket::bind("127.0.0.1:0")`
    /// does. Ports are handed out in sequence and wrap; port 0 is skipped,
    /// since it means "any" everywhere else.
    pub fn bind_any(&self) -> Endpoint {
        loop {
            let port = self.inner.next_port.fetch_add(1, Ordering::Relaxed);
            if port == 0 {
                continue;
            }
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
            if let Some(endpoint) = self.bind(addr) {
                return endpoint;
            }
        }
    }

    /// Put a datagram on the wire. Like UDP, this cannot fail and cannot
    /// promise delivery: an unbound address, a lossy wire, or a receiver that
    /// went away all look the same from here.
    fn send(&self, from: SocketAddr, to: SocketAddr, payload: Vec<u8>) {
        if self.inner.config.loss > 0.0 && rand::random::<f64>() < self.inner.config.loss {
            trace!("Dropped {} bytes {from} -> {to} (loss)", payload.len());
            return;
        }
        let latency = self.inner.config.latency;
        if latency.is_zero() {
            self.deliver(from, to, payload);
            return;
        }
        // In flight: the delay is per-datagram, so ordering is not preserved.
        let network = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(latency).await;
            network.deliver(from, to, payload);
        });
    }

    fn deliver(&self, from: SocketAddr, to: SocketAddr, payload: Vec<u8>) {
        let len = payload.len();
        match self.inner.sockets.get(&to) {
            // Err means the endpoint was dropped between lookup and send.
            Some(inbox) if inbox.send((from, payload)).is_ok() => {
                trace!("Delivered {len} bytes {from} -> {to}");
            }
            _ => trace!("Nothing bound at {to}, dropping {len} bytes from {from}"),
        }
    }
}

/// One endpoint on a [`Network`]: the fake network's answer to a `UdpSocket`.
/// Dropping it unbinds the address.
#[non_exhaustive]
pub struct Endpoint {
    addr: SocketAddr,
    network: Network,
    /// `&self` receive, to match `UdpSocket::recv_from`.
    inbox: Mutex<mpsc::UnboundedReceiver<Datagram>>,
}

impl Endpoint {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn network(&self) -> &Network {
        &self.network
    }

    pub fn send_to(&self, payload: &[u8], to: SocketAddr) {
        self.network.send(self.addr, to, payload.to_vec());
    }

    /// Wait for the next datagram. `None` once this endpoint has been unbound,
    /// which only happens when it is being dropped.
    pub async fn recv_from(&self) -> Option<Datagram> {
        self.inbox.lock().await.recv().await
    }
}

impl Drop for Endpoint {
    fn drop(&mut self) {
        self.network.inner.sockets.remove(&self.addr);
    }
}

/// A [`RetryTransport`] over a [`Network`]: the same transport that
/// [`udp_transport`](crate::rpc_transport::udp_transport) runs over a real
/// socket, with an [`Endpoint`] in place of the `UdpSocket`.
pub type NetworkedDebugTransport = RetryTransport<Endpoint>;

impl DataRxTx for Endpoint {
    async fn send_packet(&self, payload: &[u8], address: SocketAddr) -> std::io::Result<()> {
        // Like UDP, the fake wire cannot report a delivery failure from here.
        self.send_to(payload, address);
        Ok(())
    }

    async fn receive_packet(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        match self.recv_from().await {
            Some((from, datagram)) => {
                // Truncate to the buffer, as `UdpSocket::recv_from` does.
                let len = datagram.len().min(buf.len());
                buf[..len].copy_from_slice(&datagram[..len]);
                Ok((len, from))
            }
            // The endpoint has been unbound; nothing more will arrive.
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "endpoint unbound",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_transport::RpcTransport;

    /// Stand-in for a remote node, as in `main`: echoes every datagram back
    /// verbatim, so the id prefix survives the round trip.
    fn spawn_echo_peer(network: &Network) -> SocketAddr {
        let endpoint = network.bind_any();
        let addr = endpoint.local_addr();
        tokio::spawn(async move {
            while let Some((from, datagram)) = endpoint.recv_from().await {
                endpoint.send_to(&datagram, from);
            }
        });
        addr
    }

    #[tokio::test]
    async fn round_trips_through_the_fake_wire() {
        let network = Network::new();
        let peer = spawn_echo_peer(&network);
        let transport = NetworkedDebugTransport::new(network.bind_any());

        let reply = transport
            .send_receive(b"ping".to_vec(), peer)
            .await
            .unwrap();
        assert_eq!(reply, b"ping");
    }

    /// The module-level example, kept honest: a peer that parses the framing
    /// and answers with its own payload rather than echoing.
    #[tokio::test]
    async fn two_devices_talk_over_the_fake_network() {
        let network = Network::new();

        let peer = network.bind_any();
        let peer_addr = peer.local_addr();
        tokio::spawn(async move {
            while let Some((from, datagram)) = peer.recv_from().await {
                let (id, body) = datagram.split_at(ID_LEN);
                let mut reply = id.to_vec();
                reply.extend_from_slice(b"pong: ");
                reply.extend_from_slice(body);
                peer.send_to(&reply, from);
            }
        });

        let node = NetworkedDebugTransport::new(network.bind_any());
        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            node.send_receive(b"ping".to_vec(), peer_addr),
        )
        .await
        .expect("peer answered within 1s")
        .unwrap();
        assert_eq!(reply, b"pong: ping");
    }

    #[tokio::test]
    async fn concurrent_requests_get_their_own_replies() {
        let network = Network::new();
        let peer = spawn_echo_peer(&network);
        let transport = NetworkedDebugTransport::new(network.bind_any());

        let (a, b) = tokio::join!(
            transport.send_receive(b"first".to_vec(), peer),
            transport.send_receive(b"second".to_vec(), peer),
        );
        assert_eq!(a.unwrap(), b"first");
        assert_eq!(b.unwrap(), b"second");
    }

    #[tokio::test]
    async fn unbound_address_swallows_the_request() {
        let network = Network::new();
        let transport = NetworkedDebugTransport::new(network.bind_any());

        let dead: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        let timed_out = tokio::time::timeout(
            Duration::from_millis(50),
            transport.send_receive(b"ping".to_vec(), dead),
        )
        .await
        .is_err();
        assert!(timed_out);
    }

    #[tokio::test]
    async fn latency_delays_the_reply() {
        let network = Network::with_config(NetworkConfig {
            latency: Duration::from_millis(50),
            ..NetworkConfig::default()
        });
        let peer = spawn_echo_peer(&network);
        let transport = NetworkedDebugTransport::new(network.bind_any());

        // One hop each way, so 50ms is not enough and 500ms is plenty.
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                transport.send_receive(b"ping".to_vec(), peer)
            )
            .await
            .is_err()
        );
        assert_eq!(
            tokio::time::timeout(
                Duration::from_millis(500),
                transport.send_receive(b"ping".to_vec(), peer)
            )
            .await
            .unwrap()
            .unwrap(),
            b"ping"
        );
    }

    #[tokio::test]
    async fn dropping_an_endpoint_frees_its_address() {
        let network = Network::new();
        let addr: SocketAddr = "127.0.0.1:42".parse().unwrap();
        let endpoint = network.bind(addr).unwrap();
        assert!(network.bind(addr).is_none());
        drop(endpoint);
        assert!(network.bind(addr).is_some());
    }
}
