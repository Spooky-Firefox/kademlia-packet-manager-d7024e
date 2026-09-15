//! The server side of the four Kademlia RPCs.
//!
//! [`Rpc`](crate::rpc::Rpc) is the client half: it encodes a request, hands it
//! to a transport and waits for the reply. This module is the other half:
//! [`UdpDispatcher`] and [`TcpDispatcher`] each read framed requests off their
//! own transport, route the method tag to a handler, and write the reply
//! back over the same transport it arrived on.
//!
//! # Why two dispatchers, not one
//!
//! Parsing and routing are identical for both transports — that part is
//! [`parse_framed`] and [`handle_body`], shared by both. They differ on
//! exactly one thing: whether `from` is safe to learn as a
//! [`Contact`]. A UDP request's `from` is the address its sender is actually
//! listening on, because send and receive share one bound socket
//! ([`UdpTransport`](crate::rpc_transport::udp_transport::UdpTransport)) — so
//! [`UdpDispatcher`] hands it straight to
//! [`CloseNodes::maybe_add_contact`]. A TCP request's `from` is the ephemeral
//! local port the OS picked for the sender's one-shot
//! [`TcpTransport::send_receive`](crate::rpc_transport::tcp_transport::TcpTransport::send_receive)
//! call, not the port it listens on for the next connection — so
//! [`TcpDispatcher`] has no `CloseNodes` handle at all, and cannot learn a
//! bad contact from it by accident.
//!
//! # Why a task per request
//!
//! A handler can block on work of its own — a STORE that hits disk, a
//! FIND_VALUE that has to ask someone else first. Awaiting it inline would
//! stall every other request behind it, on a wire where the sender has
//! already started its own timeout. So each dispatcher's receive loop does
//! nothing but parse and spawn, and the work happens in a task.

pub mod find_node;
pub mod find_value;
pub mod ping;
pub mod store;

use crate::close_nodes::{CloseNodes, Contact, Key, NodeId};
use dashmap::DashMap;
use log::trace;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};

/// Wire framing: an 8-byte big-endian request id, then the payload. The same
/// framing every transport writes — see
/// [`UdpTransport`](crate::rpc_transport::udp_transport::UdpTransport), which
/// strips exactly this much before a request ever reaches us.
pub const ID_LEN: usize = size_of::<u64>();

/// Wire framing: right after the request id comes the sender's [`NodeId`],
/// ahead of the method tag and body. Every RPC [`Rpc`](crate::rpc::Rpc)
/// issues is framed with this prefix, and [`parse_framed`] strips exactly
/// this much off a request's payload before looking at the method tag.
pub const NODE_ID_LEN: usize = size_of::<NodeId>();

/// Frame a reply to request `id`: the id back on the front, then `body`.
///
/// The echo is load-bearing. The requester registered a
/// [`Pending`](crate::pending::Pending) slot under this id and
/// [`Pending::deliver`](crate::pending::Pending::deliver) is what wakes the
/// task waiting in it — matching on the id alone, since a datagram carries
/// nothing else tying it to a request. Answer without the id, or with the
/// wrong one, and the reply arrives as an id nobody registered: dropped by the
/// recv loop, while the caller sits out its full timeout as though we had
/// never replied at all.
pub fn frame_reply(id: u64, body: &[u8]) -> Vec<u8> {
    let mut datagram = Vec::with_capacity(ID_LEN + body.len());
    datagram.extend_from_slice(&id.to_be_bytes());
    datagram.extend_from_slice(body);
    datagram
}

/// Which RPC a request is, identified by the tag its payload starts with.
///
/// The tags are the wire vocabulary, shared with the client side in
/// [`Rpc`](crate::rpc::Rpc) — a request encoded there has to be recognised
/// here, so both ends go through [`Method::tag`] rather than spelling the
/// bytes out twice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Method {
    Ping,
    Store,
    FindNode,
    FindValue,
}

impl Method {
    /// Every method, in the order [`split_tag`](Self::split_tag) tries them.
    pub const ALL: [Method; 4] = [
        Method::Ping,
        Method::Store,
        Method::FindNode,
        Method::FindValue,
    ];

    /// The bytes a request of this method starts with.
    ///
    /// No tag is a prefix of another — `FIND_NODE` and `FIND_VALUE` share
    /// `FIND_` but diverge before either one ends — so at most one of them can
    /// match a given payload and the matching order is not load-bearing. Keep
    /// it that way when adding a tag, or the scan below has to start caring.
    pub fn tag(self) -> &'static [u8] {
        match self {
            Method::Ping => b"PING",
            Method::Store => b"STORE",
            Method::FindNode => b"FIND_NODE",
            Method::FindValue => b"FIND_VALUE",
        }
    }

    /// Split the leading method tag off `payload`, returning it and the body
    /// that follows. `None` if the payload starts with nothing we serve.
    pub fn split_tag(payload: &[u8]) -> Option<(Self, &[u8])> {
        Self::ALL.iter().find_map(|&method| {
            payload
                .strip_prefix(method.tag())
                .map(|body| (method, body))
        })
    }
}

/// The local node's state, as a handler sees it.
///
/// Handlers take the whole context rather than the parts they happen to need
/// today: STORE and FIND_VALUE want a value store that does not exist yet, and
/// growing one struct is cheaper than re-threading four signatures.
#[non_exhaustive]
pub struct Context<A>
where
    A: CloseNodes,
{
    pub close_nodes: A,
    pub values: DashMap<Key, Vec<u8>>,
}

impl<A: CloseNodes> Context<A> {
    /// Shared by every spawned handler, so it is handed out behind an `Arc`.
    pub fn new(close_nodes: A) -> Arc<Self> {
        Arc::new(Self {
            close_nodes,
            values: DashMap::new(),
        })
    }
}

/// One inbound RPC, waiting to be answered.
#[non_exhaustive]
pub struct Request {
    /// The sender's request id, off the front of the datagram.
    ///
    /// It is the sender's name for this request, not ours, and it is only
    /// unique among *their* in-flight requests — two peers will hand us the
    /// same id routinely, so it identifies a request only together with
    /// [`from`](Self::from).
    pub id: u64,
    /// Who sent it, and where the reply goes.
    ///
    /// Paired with the [`NodeId`] [`parse_framed`] strips off the front of
    /// `payload`, this is how [`UdpDispatcher`] learns the sender as a
    /// [`Contact`] — [`TcpDispatcher`] never does, since a TCP request's
    /// `from` is not a dialable address (see the module doc).
    pub from: SocketAddr,
    /// The request as it arrived, minus the transport's id prefix: the
    /// sender's [`NodeId`], then the method tag and body. [`parse_framed`]
    /// strips the node id before splitting off the tag, so a handler only
    /// ever sees what follows it.
    pub payload: Vec<u8>,
}

impl Request {
    pub fn new(id: u64, from: SocketAddr, payload: Vec<u8>) -> Self {
        Self { id, from, payload }
    }

    /// Split a datagram as it came off the wire: [`ID_LEN`] bytes of id, then
    /// the request itself.
    ///
    /// `None` if it is too short to carry an id — the same length check the
    /// transports' receive loops already make before matching one against
    /// [`Pending`](crate::pending::Pending). Reading the id here rather than
    /// taking the transport's word for it keeps the framing in one place; a
    /// transport that has already parsed it can call [`new`](Self::new).
    pub fn from_datagram(from: SocketAddr, datagram: &[u8]) -> Option<Self> {
        let (id, payload) = datagram.split_at_checked(ID_LEN)?;
        // split_at_checked handed back exactly ID_LEN bytes.
        let id = u64::from_be_bytes(id.try_into().unwrap());
        Some(Self::new(id, from, payload.to_vec()))
    }
}

/// Strip the framing every [`Rpc`](crate::rpc::Rpc) writes: the sender's
/// [`NodeId`], then the method tag. `None` for a payload too short to carry
/// a node id, or one whose tag nothing here recognises.
fn parse_framed(payload: &[u8]) -> Option<(NodeId, Method, &[u8])> {
    let (sender_id, rest) = payload.split_at_checked(NODE_ID_LEN)?;
    let (method, body) = Method::split_tag(rest)?;
    // split_at_checked handed back exactly NODE_ID_LEN bytes.
    Some((sender_id.try_into().unwrap(), method, body))
}

/// Route to the matching handler. The one piece both dispatchers share, so
/// teaching the server a new method only means changing it here once.
async fn handle_body<A: CloseNodes>(
    context: &Context<A>,
    id: u64,
    from: SocketAddr,
    method: Method,
    body: &[u8],
) -> Option<Vec<u8>> {
    trace!("{method:?} {id} from {from}, {} byte body", body.len());
    let reply = match method {
        Method::Ping => ping::handle(context, id, from, body).await,
        Method::Store => store::handle(context, id, from, body).await,
        Method::FindNode => find_node::handle(context, id, from, body).await,
        Method::FindValue => find_value::handle(context, id, from, body).await,
    };
    if reply.is_none() {
        trace!("No reply to {method:?} {id} from {from}");
    }
    reply
}

/// Answers requests arriving over UDP — the one dispatcher that learns
/// contacts. See the module doc for why that is safe here and is not for
/// [`TcpDispatcher`].
pub struct UdpDispatcher<A: CloseNodes> {
    context: Arc<Context<A>>,
}

impl<A: CloseNodes> UdpDispatcher<A> {
    pub fn new(context: Arc<Context<A>>) -> Self {
        Self { context }
    }
}

impl<A: CloseNodes + Send + Sync + 'static> UdpDispatcher<A> {
    /// Parse `request`, learn its sender, and hand off to the shared
    /// handler. `None` for a malformed or unrecognised request, or one the
    /// handler chose not to answer.
    async fn dispatch(&self, request: &Request) -> Option<Vec<u8>> {
        let Some((sender_id, method, body)) = parse_framed(&request.payload) else {
            trace!(
                "Unrecognised or malformed UDP request {} from {}, dropping {} bytes",
                request.id,
                request.from,
                request.payload.len()
            );
            return None;
        };
        // Every arriving UDP request is evidence the sender is alive and
        // reachable at this address, so it is learned before the method is
        // even looked at.
        self.context.close_nodes.maybe_add_contact(Contact {
            id: sender_id,
            address: request.from,
        });

        let reply = handle_body(&self.context, request.id, request.from, method, body).await?;
        Some(frame_reply(request.id, &reply))
    }

    /// Read datagrams off `socket` until it errors, dispatching each on its
    /// own task so one slow handler cannot delay the next datagram's turn at
    /// `recv_from`.
    ///
    /// `socket` is accepted already bound: binding is the caller's address
    /// decision, not this method's.
    pub async fn run(self: Arc<Self>, socket: UdpSocket) {
        let socket = Arc::new(socket);
        let mut buf = vec![0u8; 1024];
        loop {
            let (len, from) = match socket.recv_from(&mut buf).await {
                Ok(received) => received,
                Err(e) => {
                    trace!("udp recv failed: {e}");
                    continue;
                }
            };

            let Some(request) = Request::from_datagram(from, &buf[..len]) else {
                trace!("udp datagram from {from} too short for an id, dropping {len} bytes");
                continue;
            };

            let this = Arc::clone(&self);
            let socket = Arc::clone(&socket);
            tokio::spawn(async move {
                let Some(datagram) = this.dispatch(&request).await else {
                    return;
                };
                if let Err(e) = socket.send_to(&datagram, request.from).await {
                    trace!("udp reply to {} failed: {e}", request.from);
                }
            });
        }
    }
}

/// Answers requests arriving over TCP.
///
/// Deliberately has no [`CloseNodes`] handle: a TCP request's `from` is the
/// ephemeral local port the OS picked for the sender's one-shot
/// `TcpStream::connect`, not the port it listens on for the next connection —
/// nothing here is safe to hand to `maybe_add_contact`. See [`UdpDispatcher`]
/// for the transport that can.
pub struct TcpDispatcher<A: CloseNodes> {
    context: Arc<Context<A>>,
}

impl<A: CloseNodes> TcpDispatcher<A> {
    pub fn new(context: Arc<Context<A>>) -> Self {
        Self { context }
    }
}

impl<A: CloseNodes + Send + Sync + 'static> TcpDispatcher<A> {
    async fn dispatch(&self, request: &Request) -> Option<Vec<u8>> {
        let Some((_sender_id, method, body)) = parse_framed(&request.payload) else {
            trace!(
                "Unrecognised or malformed TCP request {} from {}, dropping {} bytes",
                request.id,
                request.from,
                request.payload.len()
            );
            return None;
        };

        let reply = handle_body(&self.context, request.id, request.from, method, body).await?;
        Some(frame_reply(request.id, &reply))
    }
}

/// Answer inbound requests on both transports until either socket is closed.
///
/// Both sockets are accepted already bound: binding is the caller's address
/// decision, not this function's.
pub async fn serve<A>(context: Arc<Context<A>>, udp_socket: UdpSocket, tcp_listener: TcpListener)
where
    A: CloseNodes + Send + Sync + 'static,
{
    let udp_dispatcher = Arc::new(UdpDispatcher::new(Arc::clone(&context)));
    let tcp_dispatcher = Arc::new(TcpDispatcher::new(context));

    // TODO deal with spawn handle
    tokio::spawn(udp_dispatcher.run(udp_socket));
    serve_tcp(tcp_dispatcher, tcp_listener).await;
}

/// Accept requests arriving over TCP, one connection per request.
///
/// [`TcpTransport::send_receive`](crate::rpc_transport::tcp_transport::TcpTransport::send_receive)
/// writes the whole request, half-closes, then reads until we close our own
/// write side — so a connection here is read to EOF for the request, and
/// closed once the reply has been written, mirroring that framing from the
/// other end.
async fn serve_tcp<A>(dispatcher: Arc<TcpDispatcher<A>>, listener: TcpListener)
where
    A: CloseNodes + Send + Sync + 'static,
{
    loop {
        let (stream, from) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                trace!("tcp accept failed: {e}");
                continue;
            }
        };
        let dispatcher = Arc::clone(&dispatcher);
        // TODO deal with spawn handle
        tokio::spawn(async move {
            if let Err(e) = handle_tcp_connection(dispatcher, stream, from).await {
                trace!("tcp connection from {from} failed: {e}");
            }
        });
    }
}

/// Read one framed request off `stream`, dispatch it, and write the reply
/// back before closing.
async fn handle_tcp_connection<A>(
    dispatcher: Arc<TcpDispatcher<A>>,
    mut stream: TcpStream,
    from: SocketAddr,
) -> std::io::Result<()>
where
    A: CloseNodes + Send + Sync + 'static,
{
    let mut datagram = Vec::new();
    stream.read_to_end(&mut datagram).await?;

    let Some(request) = Request::from_datagram(from, &datagram) else {
        trace!(
            "tcp request from {from} too short for an id, dropping {} bytes",
            datagram.len()
        );
        return Ok(());
    };

    if let Some(reply) = dispatcher.dispatch(&request).await {
        stream.write_all(&reply).await?;
    }
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::recommended;
    use std::time::Duration;

    fn addr() -> SocketAddr {
        "127.0.0.1:4242".parse().unwrap()
    }

    fn sender_id() -> NodeId {
        [1u8; 20]
    }

    /// Frame a request payload the way a live `Rpc` does: [`sender_id`],
    /// then whatever follows (a method tag and body).
    fn framed(rest: &[u8]) -> Vec<u8> {
        let mut payload = sender_id().to_vec();
        payload.extend_from_slice(rest);
        payload
    }

    /// Frame a whole datagram the way a transport writes it: an 8-byte id,
    /// then [`framed`]'s payload.
    fn framed_datagram(id: u64, rest: &[u8]) -> Vec<u8> {
        let mut datagram = id.to_be_bytes().to_vec();
        datagram.extend(framed(rest));
        datagram
    }

    fn test_context() -> Arc<Context<crate::close_nodes::RecommendedCloseNodes>> {
        Context::new(recommended([0u8; 20]))
    }

    #[test]
    fn tags_are_split_off_the_front() {
        assert_eq!(
            Method::split_tag(b"FIND_NODEbody"),
            Some((Method::FindNode, &b"body"[..]))
        );
        assert_eq!(
            Method::split_tag(b"FIND_VALUE"),
            Some((Method::FindValue, &b""[..]))
        );
        assert_eq!(Method::split_tag(b"PING"), Some((Method::Ping, &b""[..])));
        assert_eq!(Method::split_tag(b"PIN"), None);
        assert_eq!(Method::split_tag(b"xPING"), None);
        assert_eq!(Method::split_tag(b""), None);
    }

    /// The framing every transport writes, read back: an 8-byte big-endian
    /// id, then the payload. Built here the way a transport builds it, so
    /// the two cannot drift apart unnoticed.
    #[test]
    fn a_datagram_splits_into_its_id_and_the_request() {
        let id = 0x0102_0304_0506_0708u64;
        let mut datagram = id.to_be_bytes().to_vec();
        datagram.extend_from_slice(Method::Ping.tag());

        let request = Request::from_datagram(addr(), &datagram).expect("carries an id");
        assert_eq!(request.id, id);
        assert_eq!(request.payload, Method::Ping.tag());
        assert_eq!(request.from, addr());
    }

    /// Too short to carry an id: dropped rather than parsed out of whatever
    /// bytes are there, which is the check both transports already make.
    #[test]
    fn a_datagram_too_short_for_an_id_is_rejected() {
        assert!(Request::from_datagram(addr(), b"").is_none());
        assert!(Request::from_datagram(addr(), &[0u8; ID_LEN - 1]).is_none());
        // Exactly an id and nothing else still parses; an empty payload is a
        // request with no node id or method tag, which dispatch drops on its
        // own.
        assert!(Request::from_datagram(addr(), &[0u8; ID_LEN]).is_some());
    }

    /// A reply reframed by `frame_reply` is a datagram `from_datagram` reads,
    /// so the two halves of the framing cannot drift apart.
    #[test]
    fn framing_round_trips() {
        let id = u64::MAX;
        let datagram = frame_reply(id, b"PONG");
        let parsed = Request::from_datagram(addr(), &datagram).expect("carries an id");
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.payload, b"PONG");
    }

    #[tokio::test]
    async fn ping_is_answered_with_a_pong() {
        let dispatcher = UdpDispatcher::new(test_context());
        let request = Request::new(7, addr(), framed(Method::Ping.tag()));

        assert_eq!(
            dispatcher.dispatch(&request).await,
            Some(frame_reply(7, ping::PONG))
        );
    }

    /// A payload too short to carry a node id is dropped the same way an
    /// unrecognised method is: no reply.
    #[tokio::test]
    async fn requests_too_short_for_a_node_id_are_dropped() {
        let dispatcher = UdpDispatcher::new(test_context());
        let request = Request::new(7, addr(), vec![0u8; NODE_ID_LEN - 1]);

        assert_eq!(dispatcher.dispatch(&request).await, None);
    }

    /// Every request `UdpDispatcher` handles learns its sender as a contact,
    /// whether or not the method itself is recognised — this is what lets a
    /// routing table build up from ordinary traffic instead of only from
    /// FIND_NODE replies.
    #[tokio::test]
    async fn udp_dispatch_learns_the_sender_as_a_contact() {
        let context = test_context();
        let dispatcher = UdpDispatcher::new(Arc::clone(&context));
        let request = Request::new(7, addr(), framed(Method::Ping.tag()));

        dispatcher
            .dispatch(&request)
            .await
            .expect("the handler replied");

        let learned = context.close_nodes.close_nodes(sender_id());
        assert!(
            learned
                .iter()
                .any(|c| c.id == sender_id() && c.address == addr())
        );
    }

    /// The whole point of splitting the dispatchers: a TCP request's `from`
    /// is not a dialable address (see the module doc), so `TcpDispatcher`
    /// must never learn it as a contact — unlike `UdpDispatcher` above.
    #[tokio::test]
    async fn tcp_dispatch_does_not_learn_the_sender_as_a_contact() {
        let context = test_context();
        let dispatcher = TcpDispatcher::new(Arc::clone(&context));
        let request = Request::new(7, addr(), framed(Method::Ping.tag()));

        dispatcher
            .dispatch(&request)
            .await
            .expect("the handler replied");

        let learned = context.close_nodes.close_nodes(sender_id());
        assert!(!learned.iter().any(|c| c.id == sender_id()));
    }

    /// An unrecognised request is dropped, not answered.
    #[tokio::test]
    async fn unknown_methods_go_unanswered() {
        let dispatcher = UdpDispatcher::new(test_context());
        let request = Request::new(7, addr(), framed(b"NOT_A_METHOD"));

        assert_eq!(dispatcher.dispatch(&request).await, None);
    }

    /// The echo the requester's `Pending` matches on: a reply goes back under
    /// the id it came in with, so `deliver` finds the slot the caller is
    /// parked in. An id that did not survive the round trip wakes nobody.
    #[tokio::test]
    async fn the_reply_carries_the_request_id_back() {
        let dispatcher = UdpDispatcher::new(test_context());
        let id = 0x0102_0304_0506_0708u64;
        let request = Request::new(id, addr(), framed(Method::Ping.tag()));

        let datagram = dispatcher
            .dispatch(&request)
            .await
            .expect("the handler replied");

        // Read back the way a recv loop reads it: id off the front, then body.
        let (echoed, body) = datagram.split_at(ID_LEN);
        assert_eq!(u64::from_be_bytes(echoed.try_into().unwrap()), id);
        assert_eq!(body, ping::PONG);
    }

    /// A served node with the address each of its two transports is
    /// listening on, and the context it is served with (so a test can check
    /// what actually landed in `values` or the routing table).
    ///
    /// Both are bound via `std::net::{TcpListener, UdpSocket}` and handed to
    /// Tokio through `from_std` so this helper can stay a plain (non-async)
    /// fn, matching its existing callers.
    fn spawn_serve() -> (
        SocketAddr,
        SocketAddr,
        Arc<Context<crate::close_nodes::RecommendedCloseNodes>>,
    ) {
        let tcp_listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let tcp_addr = tcp_listener.local_addr().unwrap();
        tcp_listener.set_nonblocking(true).unwrap();
        let tcp_listener = TcpListener::from_std(tcp_listener).unwrap();

        let udp_socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let udp_addr = udp_socket.local_addr().unwrap();
        udp_socket.set_nonblocking(true).unwrap();
        let udp_socket = UdpSocket::from_std(udp_socket).unwrap();

        let context = Context::new(recommended([0u8; 20]));
        tokio::spawn(serve(Arc::clone(&context), udp_socket, tcp_listener));
        (tcp_addr, udp_addr, context)
    }

    /// Two pings sent back-to-back over the real UDP loop come back
    /// correctly matched by id — proof `UdpDispatcher::run` spawns a task per
    /// datagram instead of answering them one at a time.
    #[tokio::test]
    async fn udp_requests_are_served_concurrently() {
        let (_tcp_addr, udp_addr, _context) = spawn_serve();
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        client
            .send_to(&framed_datagram(1, Method::Ping.tag()), udp_addr)
            .await
            .unwrap();
        client
            .send_to(&framed_datagram(2, Method::Ping.tag()), udp_addr)
            .await
            .unwrap();

        let mut buf = [0u8; 64];
        let mut seen = std::collections::HashSet::new();
        for _ in 0..2 {
            let (len, _) = tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
                .await
                .expect("answered within 1s")
                .unwrap();
            seen.insert(buf[..len].to_vec());
        }

        assert!(seen.contains(&frame_reply(1, ping::PONG)));
        assert!(seen.contains(&frame_reply(2, ping::PONG)));
    }

    /// A STORE arriving over the TCP loop, framed exactly the way
    /// [`TcpTransport`](crate::rpc_transport::tcp_transport::TcpTransport)
    /// sends it (write the request, half-close, read to EOF), is dispatched,
    /// lands in `values`, and the ack comes back over the same connection.
    #[tokio::test]
    async fn tcp_store_is_written_and_acked() {
        let (tcp_addr, _udp_addr, context) = spawn_serve();
        let key: Key = [9u8; 20];
        let value = b"stored over tcp".to_vec();
        let id = 0x1122_3344_5566_7788u64;

        let mut datagram = id.to_be_bytes().to_vec();
        datagram.extend(framed(Method::Store.tag()));
        datagram.extend(bincode::serialize(&(key, value.clone())).unwrap());

        let reply = tokio::time::timeout(Duration::from_secs(1), async {
            let mut stream = TcpStream::connect(tcp_addr).await.unwrap();
            stream.write_all(&datagram).await.unwrap();
            stream.shutdown().await.unwrap();
            let mut reply = Vec::new();
            stream.read_to_end(&mut reply).await.unwrap();
            reply
        })
        .await
        .expect("answered within 1s");

        assert_eq!(reply, frame_reply(id, store::STORED));
        assert_eq!(context.values.get(&key).as_deref(), Some(&value));
    }
}
