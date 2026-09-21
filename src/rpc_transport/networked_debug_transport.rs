//! A fake network made of channels.
//!
//! [`Network`] plays the role of the wire, and an [`Endpoint`] bound to it
//! stands in for both sockets a node answers on: a datagram sent to a
//! [`SocketAddr`] is handed to whichever endpoint bound that address, and so is
//! a connection opened to it. The addresses are real `SocketAddr`s but nothing
//! is bound in the OS and nothing leaves the process, so tests get UDP
//! semantics — unreliable, unordered, silently dropped when nobody is home —
//! and TCP's without touching a port or waiting on a real timeout.
//!
//! One endpoint carries both because a node has one address and answers both
//! shapes on it, the way `Node::bind` binds its `UdpSocket` and its
//! `TcpListener` to the same `SocketAddr`.
//!
//! [`NetworkedDebugTransport`] is then just
//! [`RetryTransport`](crate::rpc_transport::retry_transport::RetryTransport)
//! with an [`Endpoint`] in place of the `UdpSocket`: the id framing, the
//! pending-request bookkeeping, and the resend loop are all shared code, so a
//! test against the fake wire exercises the same transport `main` runs over
//! real UDP. [`NetworkedStreamTransport`] is the same trick on the other
//! shape — [`stream_send_receive`] over a fake connection rather than a
//! `TcpStream`.
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
//! // 8-byte id prefix must come back carrying the same number, with
//! // `REPLY_TAG` set to mark it an answer, or the caller's receive loop
//! // cannot tell which in-flight request this belongs to (or that it is a
//! // reply at all, rather than a question being asked of it).
//! let peer: Endpoint = network.bind_any();
//! let peer_addr: SocketAddr = peer.local_addr();
//! tokio::spawn(async move {
//!     // recv_from yields Option<(SocketAddr, Vec<u8>)>: the sender and the
//!     // datagram, and None once the endpoint is unbound.
//!     while let Some((from, datagram)) = peer.recv_from().await {
//!         let (id, body): (&[u8], &[u8]) = datagram.split_at(size_of::<u64>());
//!         let id: u64 = u64::from_be_bytes(id.try_into().unwrap());
//!         let mut reply: Vec<u8> = reply_id(id).to_be_bytes().to_vec();
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
//!
//! # Connections
//!
//! [`Network::connect`] opens a [`tokio::io::duplex`] pipe to a bound endpoint
//! and queues the far end for it to [`accept`](StreamListener::accept), so the
//! half-close the framing depends on behaves as it does on a `TcpStream`.
//! [`NetworkConfig`] does not apply: a lossy network still delivers every
//! STORE, which is the point of the shape.
//!
//! `connect` mints a fresh address it leaves unbound rather than handing over
//! the caller's, because real TCP shows the accepting side an ephemeral port —
//! the reason a request off a connection is not
//! [`from_is_reachable`](crate::handle_rpc::Request::from_is_reachable). A
//! change that started learning it therefore fails here the same way it would
//! fail on a real network, instead of passing because the fake was generous.
use crate::rpc_transport::RpcTransport;
use crate::rpc_transport::data_rx_tx::DataRxTx;
use crate::rpc_transport::retry_transport::RetryTransport;
use crate::rpc_transport::stream_framing::stream_send_receive;
use crate::rpc_transport::stream_listener::StreamListener;
use dashmap::DashMap;
use log::trace;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use tokio::io::DuplexStream;
use tokio::sync::{Mutex, mpsc};

/// Wire framing: an 8-byte big-endian request id, then the payload.
const ID_LEN: usize = size_of::<u64>();

/// Where [`Network::bind_any`] starts handing out ports.
const FIRST_EPHEMERAL_PORT: u16 = 49152;

/// How much a connection holds before the writer waits on the reader. A larger
/// request still goes through: both ends are always driven concurrently, since
/// [`handle_rpc::serve`](crate::handle_rpc::serve) spawns a task per connection.
const PIPE_CAPACITY: usize = 64 * 1024;

/// `(sender, payload)`, as it appears at the receiving endpoint.
type Datagram = (SocketAddr, Vec<u8>);

/// `(dialler, this end of the pipe)`, as it appears at the accepting endpoint.
type Incoming = (SocketAddr, DuplexStream);

/// How much the fake wire misbehaves. The default is a perfect network: no
/// delay, nothing lost.
///
/// Datagrams only. A connection is delivered whole whatever this says, because
/// a connection is what a node reaches for when it will not lose the message.
#[derive(Clone, Copy, Debug, Default)]
pub struct NetworkConfig {
    /// Delay applied to every delivered datagram.
    pub latency: Duration,
    /// Fraction of datagrams dropped in flight, `0.0..=1.0`.
    pub loss: f64,
}

/// The two queues an [`Endpoint`] drains, held together so a bound address
/// cannot be listening for one shape and deaf to the other.
struct Bound {
    datagrams: mpsc::UnboundedSender<Datagram>,
    connections: mpsc::UnboundedSender<Incoming>,
}

struct Inner {
    /// The bound endpoints' inboxes. A missing address is a dead port.
    sockets: DashMap<SocketAddr, Bound>,
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
    ///
    /// One endpoint, both shapes: the returned [`Endpoint`] receives the
    /// datagrams sent to `addr` and accepts the connections opened to it.
    pub fn bind(&self, addr: SocketAddr) -> Option<Endpoint> {
        let (datagrams, datagram_inbox) = mpsc::unbounded_channel();
        let (connections, connection_inbox) = mpsc::unbounded_channel();
        match self.inner.sockets.entry(addr) {
            dashmap::Entry::Occupied(_) => None,
            dashmap::Entry::Vacant(slot) => {
                slot.insert(Bound {
                    datagrams,
                    connections,
                });
                Some(Endpoint {
                    addr,
                    network: self.clone(),
                    inbox: Mutex::new(datagram_inbox),
                    connections: Mutex::new(connection_inbox),
                })
            }
        }
    }

    /// Bind a fresh loopback address, the way `UdpSocket::bind("127.0.0.1:0")`
    /// does.
    pub fn bind_any(&self) -> Endpoint {
        loop {
            if let Some(endpoint) = self.bind(self.next_addr()) {
                return endpoint;
            }
        }
    }

    /// Open a connection to `to` and return the dialling end of it.
    ///
    /// `ConnectionRefused` when nothing is bound there. Unlike a datagram,
    /// which this wire can only swallow, a connection attempt is answered one
    /// way or the other — as `TcpStream::connect` is.
    ///
    /// The accept queue is unbounded, so this never waits for the peer to get
    /// round to accepting: a caller that wrote its request and blocked on a
    /// full pipe would otherwise be waiting on a reader that had not been
    /// handed the connection yet.
    pub fn connect(&self, to: SocketAddr) -> std::io::Result<DuplexStream> {
        let (client, server) = tokio::io::duplex(PIPE_CAPACITY);
        // Somewhere nothing is bound, so the accepting side cannot dial us back
        // — see the module doc.
        let from = loop {
            let addr = self.next_addr();
            if !self.inner.sockets.contains_key(&addr) {
                break addr;
            }
        };

        // A send error means the endpoint was dropped between the lookup and
        // the handoff, which is the same dead port as no entry at all.
        let connected = matches!(
            self.inner.sockets.get(&to),
            Some(bound) if bound.connections.send((from, server)).is_ok()
        );
        if !connected {
            trace!("Nothing listening at {to}, refusing the connection from {from}");
            return Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("nothing listening at {to}"),
            ));
        }

        trace!("Connected {from} -> {to}");
        Ok(client)
    }

    /// The next loopback address in sequence. Ports wrap; port 0 is skipped,
    /// since it means "any" everywhere else.
    fn next_addr(&self) -> SocketAddr {
        loop {
            let port = self.inner.next_port.fetch_add(1, Ordering::Relaxed);
            if port != 0 {
                return SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
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
            Some(bound) if bound.datagrams.send((from, payload)).is_ok() => {
                trace!("Delivered {len} bytes {from} -> {to}");
            }
            _ => trace!("Nothing bound at {to}, dropping {len} bytes from {from}"),
        }
    }
}

/// One endpoint on a [`Network`]: the fake network's answer to a node's
/// `UdpSocket` and `TcpListener` at once. Dropping it unbinds the address.
#[non_exhaustive]
pub struct Endpoint {
    addr: SocketAddr,
    network: Network,
    /// `&self` receive, to match `UdpSocket::recv_from`.
    inbox: Mutex<mpsc::UnboundedReceiver<Datagram>>,
    /// `&self` accept, to match `TcpListener::accept`.
    connections: Mutex<mpsc::UnboundedReceiver<Incoming>>,
}

impl Endpoint {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
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

impl StreamListener for Endpoint {
    type Stream = DuplexStream;

    async fn accept(&self) -> std::io::Result<(DuplexStream, SocketAddr)> {
        match self.connections.lock().await.recv().await {
            Some((from, stream)) => Ok((stream, from)),
            // Unreachable: the sender sits in the network's map and is removed
            // only when this endpoint is dropped, which cannot happen while
            // `&self` is borrowed. It is an error rather than an `Option` in
            // the signature because the trait is also implemented by things
            // that really can fail to accept, like a `TcpListener`.
            None => Err(std::io::Error::new(
                std::io::ErrorKind::NotConnected,
                "endpoint unbound",
            )),
        }
    }
}

/// The connection-shaped transport over a [`Network`]: the same
/// [`stream_send_receive`] framing
/// [`TcpTransport`](crate::rpc_transport::tcp_transport::TcpTransport) runs
/// over real TCP, dialling an in-process pipe instead of a socket.
///
/// The datagram counterpart is [`NetworkedDebugTransport`]. A transport only
/// dials; the other direction is the node's own [`Endpoint`], handed to
/// [`handle_rpc::serve`](crate::handle_rpc::serve).
pub struct NetworkedStreamTransport {
    network: Network,
}

impl NetworkedStreamTransport {
    pub fn new(network: Network) -> Self {
        Self { network }
    }
}

impl RpcTransport for NetworkedStreamTransport {
    async fn send_receive(
        &self,
        payload: Vec<u8>,
        address: SocketAddr,
    ) -> std::io::Result<Vec<u8>> {
        let stream = self.network.connect(address)?;
        stream_send_receive(stream, payload).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc_transport::reply_id;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Stand-in for a remote node, as in `main`: echoes every datagram's body
    /// back under the id it came in with, tagged as the reply it is so the
    /// requester's receive loop matches it rather than reading it as a fresh
    /// question. See [`REPLY_TAG`].
    fn spawn_echo_peer(network: &Network) -> SocketAddr {
        let endpoint = network.bind_any();
        let addr = endpoint.local_addr();
        tokio::spawn(async move {
            while let Some((from, datagram)) = endpoint.recv_from().await {
                endpoint.send_to(&reply_to(&datagram, &datagram[ID_LEN..]), from);
            }
        });
        addr
    }

    /// Frame `body` as the answer to `request`: its id back on the front with
    /// [`REPLY_TAG`] set, the way `handle_rpc::frame_reply` does.
    fn reply_to(request: &[u8], body: &[u8]) -> Vec<u8> {
        let id = u64::from_be_bytes(request[..ID_LEN].try_into().unwrap());
        let mut reply = reply_id(id).to_be_bytes().to_vec();
        reply.extend_from_slice(body);
        reply
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
                let mut body = b"pong: ".to_vec();
                body.extend_from_slice(&datagram[ID_LEN..]);
                peer.send_to(&reply_to(&datagram, &body), from);
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

    /// Stand-in for a served node on the connection side: answers every
    /// connection by echoing the request body back under the id it came in
    /// with, tagged as the reply it is.
    ///
    /// One task per connection, as
    /// [`handle_rpc::serve`](crate::handle_rpc::serve) does — both ends have to
    /// be driven at once, or a request larger than [`PIPE_CAPACITY`] would
    /// deadlock its own writer.
    fn spawn_echo_listener(network: &Network) -> SocketAddr {
        let endpoint = network.bind_any();
        let addr = endpoint.local_addr();
        tokio::spawn(async move {
            while let Ok((mut stream, _from)) = endpoint.accept().await {
                tokio::spawn(async move {
                    let mut request = Vec::new();
                    stream.read_to_end(&mut request).await.unwrap();
                    stream
                        .write_all(&reply_to(&request, &request[ID_LEN..]))
                        .await
                        .unwrap();
                    stream.shutdown().await.unwrap();
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn a_connection_round_trips_through_the_fake_wire() {
        let network = Network::new();
        let peer = spawn_echo_listener(&network);
        let transport = NetworkedStreamTransport::new(network);

        let reply = transport
            .send_receive(b"ping".to_vec(), peer)
            .await
            .unwrap();
        assert_eq!(reply, b"ping");
    }

    /// The case the pipe capacity makes non-obvious: a request several times
    /// larger than a connection can hold blocks its writer partway through and
    /// only completes because the peer is reading at the same time. This is the
    /// size STORE and FIND_VALUE take the connection for, so it has to work.
    #[tokio::test]
    async fn a_connection_round_trips_a_payload_larger_than_the_pipe() {
        let network = Network::new();
        let peer = spawn_echo_listener(&network);
        let transport = NetworkedStreamTransport::new(network);
        let payload = vec![0xABu8; PIPE_CAPACITY * 3];

        let reply = tokio::time::timeout(
            Duration::from_secs(5),
            transport.send_receive(payload.clone(), peer),
        )
        .await
        .expect("a payload larger than the pipe must not deadlock")
        .unwrap();

        assert_eq!(reply, payload);
    }

    /// A dead port is refused rather than swallowed. This is the one place the
    /// two shapes differ on the same wire: the datagram aimed at nothing is
    /// dropped and times out (see [`unbound_address_swallows_the_request`]),
    /// the connection comes back refused, as on a real network.
    #[tokio::test]
    async fn connecting_to_an_unbound_address_is_refused() {
        let network = Network::new();
        let transport = NetworkedStreamTransport::new(network);
        let dead: SocketAddr = "127.0.0.1:9999".parse().unwrap();

        let result = tokio::time::timeout(
            Duration::from_millis(100),
            transport.send_receive(b"ping".to_vec(), dead),
        )
        .await
        .expect("connect should fail fast, not hang");

        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::ConnectionRefused
        );
    }

    /// Loss describes a datagram wire, and a connection is what a node reaches
    /// for when it will not accept loss — so a network that drops every
    /// datagram still carries every connection. See [`NetworkConfig`].
    #[tokio::test]
    async fn loss_does_not_touch_connections() {
        let network = Network::with_config(NetworkConfig {
            loss: 1.0,
            ..NetworkConfig::default()
        });
        let peer = spawn_echo_listener(&network);
        let transport = NetworkedStreamTransport::new(network);

        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            transport.send_receive(b"ping".to_vec(), peer),
        )
        .await
        .expect("answered within 1s")
        .unwrap();
        assert_eq!(reply, b"ping");
    }

    /// One endpoint, both shapes: the address a node binds answers its
    /// datagrams and accepts its connections, the way a real node's `UdpSocket`
    /// and `TcpListener` share one `SocketAddr`.
    #[tokio::test]
    async fn one_address_carries_both_shapes() {
        let network = Network::new();
        let endpoint = network.bind_any();
        let addr = endpoint.local_addr();

        let sender = network.bind_any();
        sender.send_to(b"a datagram", addr);
        let connection = network.connect(addr).unwrap();

        let (from, datagram) = tokio::time::timeout(Duration::from_secs(1), endpoint.recv_from())
            .await
            .expect("delivered within 1s")
            .expect("the endpoint is bound");
        assert_eq!(datagram, b"a datagram");
        assert_eq!(from, sender.local_addr());

        let (_stream, _from) = tokio::time::timeout(Duration::from_secs(1), endpoint.accept())
            .await
            .expect("accepted within 1s")
            .unwrap();
        drop(connection);
    }

    /// The honesty check on the fake, and the reason the dialling address is
    /// minted rather than reused: what the accepting side sees must not be
    /// somewhere the caller can be reached, or a change that started learning a
    /// stream request's sender as a contact would pass here and fail on a real
    /// network. See the module doc and
    /// [`Request::from_is_reachable`](crate::handle_rpc::Request::from_is_reachable).
    #[tokio::test]
    async fn the_dialling_address_is_not_one_anybody_is_bound_to() {
        let network = Network::new();
        let listener = network.bind_any();
        let listener_addr = listener.local_addr();
        // The caller is a bound node of its own, as a real one would be.
        let caller = network.bind_any();
        let caller_addr = caller.local_addr();

        let _client = network.connect(listener_addr).unwrap();
        let (_stream, from) = listener.accept().await.unwrap();

        assert_ne!(from, caller_addr, "not the caller's own bound address");
        assert_ne!(from, listener_addr);
        assert!(
            network.bind(from).is_some(),
            "the dialling address must be one nothing is bound to"
        );
    }
}
