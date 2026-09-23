//! The server side of the four Kademlia RPCs.
//!
//! [`Rpc`](crate::rpc::Rpc) is the client half: it encodes a request, hands
//! it to a transport and waits for the reply. This module is the other half:
//! [`dispatch`] routes a request's method tag to a handler, and the serve loop
//! it came from sends the answer back the way it arrived.
//!
//! # Where the requests come from
//!
//! [`dispatch`] does not read a socket. A node has one receive loop per
//! transport and it lives with that transport:
//!
//! - Datagrams (UDP, or the fake wire):
//!   [`RetryTransport`](crate::rpc_transport::retry_transport::RetryTransport)'s
//!   loop already reads the socket to match replies against
//!   [`Pending`](crate::pending::Pending). Built with
//!   [`with_requests`](crate::rpc_transport::retry_transport::RetryTransport::with_requests)
//!   it splits the two on [`REPLY_TAG`](crate::rpc_transport::REPLY_TAG) and
//!   sends the requests down a channel to [`serve`], which answers over that
//!   same socket.
//! - Connections: [`serve`] accepts them off a [`StreamListener`] and a reply
//!   goes back down the connection it arrived on. That is a real `TcpListener`
//!   in `main` and an in-process
//!   [`Endpoint`](crate::rpc_transport::networked_debug_transport::Endpoint)
//!   under test.
//!
//! # What `from` means, and why it differs
//!
//! Both shapes go through the same [`dispatch`] and differ on one thing:
//! whether `from` is somewhere the sender can be reached, and so worth learning
//! as a [`Contact`]. A datagram's `from` *is* the address its sender listens
//! on. A connection's is the ephemeral port the OS picked for that one dial —
//! reply down it, then forget it.
//!
//! That is [`Request::from_is_reachable`], fixed by which constructor built the
//! request ([`Request::from_datagram`] or [`Request::from_connection`], one per
//! wire shape) and never a parameter, so no call site can pass the wrong one.
//! The rule follows from the *shape*, not the protocol — hence the names.
//!
//! # Why a task per request
//!
//! A handler can block on work of its own — a STORE that hits disk, a
//! FIND_VALUE that has to ask someone else first — while the sender has already
//! started its own timeout. So each serve loop only takes the next request and
//! spawns.

pub mod find_node;
pub mod find_value;
pub mod ping;
pub mod store;

use crate::close_nodes::{CloseNodes, Contact, Key, NodeId};
use crate::rpc_transport::data_rx_tx::DataRxTx;
use crate::rpc_transport::reply_id;
use crate::rpc_transport::stream_listener::StreamListener;
use dashmap::DashMap;
use log::trace;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

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

/// Frame a reply to request `id`: the id back on the front under
/// [`REPLY_TAG`](crate::rpc_transport::REPLY_TAG), then `body`.
///
/// The echo is load-bearing. The requester registered a
/// [`Pending`](crate::pending::Pending) slot under this id and
/// [`Pending::deliver`](crate::pending::Pending::deliver) is what wakes the
/// task waiting in it — matching on the id alone, since a datagram carries
/// nothing else tying it to a request. Answer without the id, or with the
/// wrong one, and the reply arrives as an id nobody registered: dropped by the
/// recv loop, while the caller sits out its full timeout as though we had
/// never replied at all.
///
/// The tag is what stops the requester's receive loop from reading this as a
/// fresh question rather than its answer — see
/// [`REPLY_TAG`](crate::rpc_transport::REPLY_TAG).
pub fn frame_reply(id: u64, body: &[u8]) -> Vec<u8> {
    let mut datagram = Vec::with_capacity(ID_LEN + body.len());
    datagram.extend_from_slice(&reply_id(id).to_be_bytes());
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
    /// Who we are. A [`CloseNodes`] knows this already, but does not expose
    /// it, and [`ping`] has to put it on the wire: an address alone does not
    /// tell a joining node whose address it is.
    pub my_id: NodeId,
    pub close_nodes: A,
    pub values: DashMap<Key, Vec<u8>>,
}

impl<A: CloseNodes> Context<A> {
    /// Shared by every spawned handler, so it is handed out behind an `Arc`.
    ///
    /// `my_id` must be the same id the routing table was built around, or we
    /// answer PINGs with a name our own siblings do not know us by.
    pub fn new(my_id: NodeId, close_nodes: A) -> Arc<Self> {
        Arc::new(Self {
            my_id,
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
    /// [`from`](Self::from). A handler wants it to name the request in a log
    /// line that the other node's log can be read against.
    pub id: u64,
    /// Who sent it, and where the reply goes.
    pub from: SocketAddr,
    /// Whether [`from`](Self::from) is an address the sender can be reached
    /// at, and so whether it may be learned as a [`Contact`].
    ///
    /// True off a datagram wire, false off a connection — see the module doc.
    /// Set by the constructor rather than passed to one, so the wire a request
    /// came off decides it and nothing downstream can get it wrong.
    pub from_is_reachable: bool,
    /// The request as it arrived, minus the transport's id prefix: the
    /// sender's [`NodeId`], then the method tag and body. [`parse_framed`]
    /// strips the node id before splitting off the tag, so a handler only
    /// ever sees what follows it.
    pub payload: Vec<u8>,
}

impl Request {
    /// Split a datagram as it came off a datagram wire: [`ID_LEN`] bytes of
    /// id, then the request itself.
    ///
    /// `None` if it is too short to carry an id — the same length check the
    /// transports' receive loops already make before matching one against
    /// [`Pending`](crate::pending::Pending). Reading the id here rather than
    /// taking the transport's word for it keeps the framing in one place.
    pub fn from_datagram(from: SocketAddr, datagram: &[u8]) -> Option<Self> {
        let (id, payload) = datagram.split_at_checked(ID_LEN)?;
        // split_at_checked handed back exactly ID_LEN bytes.
        let id = u64::from_be_bytes(id.try_into().unwrap());
        Some(Self {
            id,
            from,
            from_is_reachable: true,
            payload: payload.to_vec(),
        })
    }

    /// The same framing off a connection, where `from` is the ephemeral port
    /// the connection was dialled from rather than an address the sender
    /// answers on. That one difference is the whole difference.
    pub fn from_connection(from: SocketAddr, datagram: &[u8]) -> Option<Self> {
        Some(Self {
            from_is_reachable: false,
            ..Self::from_datagram(from, datagram)?
        })
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

/// Parse `request`, learn its sender if the wire it came off allows it, route
/// it to a handler, and frame the answer.
///
/// `None` for a malformed or unrecognised request, or one the handler chose
/// not to answer. Both wire shapes come through here: they differ only in
/// [`from_is_reachable`](Request::from_is_reachable), which is what the module
/// doc is about.
///
/// Teaching the server a new method means adding an arm below and a tag in
/// [`Method::tag`], and nothing else.
async fn dispatch<A: CloseNodes>(context: &Context<A>, request: &Request) -> Option<Vec<u8>> {
    let id = request.id;
    let from = request.from;

    let Some((sender_id, method, body)) = parse_framed(&request.payload) else {
        trace!(
            "Unrecognised or malformed request {id} from {from}, dropping {} bytes",
            request.payload.len()
        );
        return None;
    };

    // An arriving request is evidence its sender is alive, so it is learned
    // before the method is even looked at — but only off a wire where `from`
    // is somewhere it can be reached again.
    if request.from_is_reachable {
        context.close_nodes.maybe_add_contact(Contact {
            id: sender_id,
            address: from,
        });
    }

    trace!("{method:?} {id} from {from}, {} byte body", body.len());
    let reply = match method {
        Method::Ping => ping::handle(context, id, from, body).await,
        Method::Store => store::handle(context, id, from, body).await,
        Method::FindNode => find_node::handle(context, id, from, body).await,
        Method::FindValue => find_value::handle(context, id, from, body).await,
    };
    let Some(reply) = reply else {
        trace!("No reply to {method:?} {id} from {from}");
        return None;
    };

    Some(frame_reply(id, &reply))
}

/// Answer requests off `requests` until it closes, each on its own task so one
/// slow handler cannot hold up the next.
///
/// The requests come from the transport's own receive loop (see
/// [`RetryTransport::with_requests`](crate::rpc_transport::retry_transport::RetryTransport::with_requests)),
/// and `socket` is that same socket, so a reply leaves from the address the
/// request was sent to.
async fn serve_datagrams<A, T>(
    context: Arc<Context<A>>,
    mut requests: mpsc::Receiver<Request>,
    socket: Arc<T>,
) where
    A: CloseNodes + Send + Sync + 'static,
    T: DataRxTx + Send + Sync + 'static,
{
    while let Some(request) = requests.recv().await {
        let context = Arc::clone(&context);
        let socket = Arc::clone(&socket);
        // TODO deal with spawn handle
        tokio::spawn(async move {
            let Some(datagram) = dispatch(&context, &request).await else {
                return;
            };
            if let Err(e) = socket.send_packet(&datagram, request.from).await {
                trace!("reply to {} failed: {e}", request.from);
            }
        });
    }
    trace!("Request channel closed, stopping the datagram serve loop");
}

/// Answer inbound requests on both transports until they stop arriving.
///
/// `requests` is the channel the datagram transport's receive loop feeds (see
/// [`RetryTransport::with_requests`](crate::rpc_transport::retry_transport::RetryTransport::with_requests))
/// and `socket` is the socket that loop reads, handed over so replies go
/// back out of it. `listener` is handed over already bound: binding is the
/// caller's address decision, not this function's.
///
/// Both sides are generic over their channel rather than fixed to
/// `UdpSocket`/`TcpListener`, so a node can be served entirely in-process
/// against [`networked_debug_transport`](crate::rpc_transport::networked_debug_transport)
/// — which is the same code path, not a parallel one.
pub async fn serve<A, T, L>(
    context: Arc<Context<A>>,
    requests: mpsc::Receiver<Request>,
    socket: Arc<T>,
    listener: L,
) where
    A: CloseNodes + Send + Sync + 'static,
    T: DataRxTx + Send + Sync + 'static,
    L: StreamListener + Send + Sync + 'static,
{
    // TODO deal with spawn handle
    tokio::spawn(serve_datagrams(Arc::clone(&context), requests, socket));
    serve_streams(context, listener).await;
}

/// Accept requests arriving over connections, one connection per request.
///
/// [`stream_send_receive`](crate::rpc_transport::stream_framing::stream_send_receive)
/// writes the whole request, half-closes, then reads until we close our own
/// write side — so a connection here is read to EOF for the request, and
/// closed once the reply has been written, mirroring that framing from the
/// other end.
async fn serve_streams<A, L>(context: Arc<Context<A>>, listener: L)
where
    A: CloseNodes + Send + Sync + 'static,
    L: StreamListener + Send + Sync + 'static,
{
    loop {
        let (mut stream, from) = match listener.accept().await {
            Ok(accepted) => accepted,
            // Transient on a real listener — a momentary fd exhaustion should
            // not take the server down — so the loop carries on.
            Err(e) => {
                trace!("accept failed: {e}");
                continue;
            }
        };
        let context = Arc::clone(&context);
        // TODO deal with spawn handle
        tokio::spawn(async move {
            let mut datagram = Vec::new();
            if let Err(e) = stream.read_to_end(&mut datagram).await {
                trace!("reading the request from {from} failed: {e}");
                return;
            }

            // `from_connection`, not `from_datagram`: `from` is the port the
            // caller dialled from, and must not reach the routing table.
            let Some(request) = Request::from_connection(from, &datagram) else {
                trace!(
                    "stream request from {from} too short for an id, dropping {} bytes",
                    datagram.len()
                );
                return;
            };

            if let Some(reply) = dispatch(&context, &request).await
                && let Err(e) = stream.write_all(&reply).await
            {
                trace!("writing the reply to {from} failed: {e}");
                return;
            }
            if let Err(e) = stream.shutdown().await {
                trace!("closing the connection to {from} failed: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::recommended;
    use crate::rpc_transport::RpcTransport;
    use crate::rpc_transport::networked_debug_transport::{Network, NetworkedStreamTransport};
    use crate::rpc_transport::udp_transport::UdpTransport;
    use crate::rpc_transport::{is_reply, request_id};
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};

    fn addr() -> SocketAddr {
        "127.0.0.1:4242".parse().unwrap()
    }

    fn sender_id() -> NodeId {
        [1u8; 32]
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

    /// The id every context in these tests answers under.
    const TEST_ID: NodeId = [0u8; 32];

    fn test_context() -> Arc<Context<crate::close_nodes::RecommendedCloseNodes>> {
        Context::new(TEST_ID, recommended(TEST_ID))
    }

    /// What a PING is answered with: the responder's own id, which is how a
    /// joining node turns a bootstrap address into a [`Contact`].
    fn pong() -> Vec<u8> {
        bincode::serialize(&TEST_ID).unwrap()
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
        let id = 0x0102_0304_0506_0708u64;
        let datagram = frame_reply(id, b"PONG");
        let parsed = Request::from_datagram(addr(), &datagram).expect("carries an id");
        // `frame_reply` marks it a reply, so what comes back off the wire is
        // the request's id with the tag on top of it.
        assert_eq!(parsed.id, reply_id(id));
        assert_eq!(parsed.payload, b"PONG");
    }

    /// A request as it reaches [`dispatch`] off a datagram wire: an id, then
    /// `payload`, parsed the way the serve loop parses it. Built through
    /// [`Request::from_datagram`] rather than by hand, so a test cannot claim
    /// a `from_is_reachable` the real wire would not have given it.
    fn datagram_request(id: u64, payload: &[u8]) -> Request {
        let mut datagram = id.to_be_bytes().to_vec();
        datagram.extend_from_slice(payload);
        Request::from_datagram(addr(), &datagram).expect("carries an id")
    }

    /// The same, off a connection — the one wire whose `from` must not be
    /// learned.
    fn connection_request(id: u64, payload: &[u8]) -> Request {
        let mut datagram = id.to_be_bytes().to_vec();
        datagram.extend_from_slice(payload);
        Request::from_connection(addr(), &datagram).expect("carries an id")
    }

    #[tokio::test]
    async fn ping_is_answered_with_our_own_id() {
        let request = datagram_request(7, &framed(Method::Ping.tag()));

        assert_eq!(
            dispatch(&test_context(), &request).await,
            Some(frame_reply(7, &pong()))
        );
    }

    /// A payload too short to carry a node id is dropped the same way an
    /// unrecognised method is: no reply.
    #[tokio::test]
    async fn requests_too_short_for_a_node_id_are_dropped() {
        let request = datagram_request(7, &[0u8; NODE_ID_LEN - 1]);

        assert_eq!(dispatch(&test_context(), &request).await, None);
    }

    /// Every request off a datagram wire learns its sender as a contact,
    /// whether or not the method itself is recognised — this is what lets a
    /// routing table build up from ordinary traffic instead of only from
    /// FIND_NODE replies.
    #[tokio::test]
    async fn a_datagram_request_learns_the_sender_as_a_contact() {
        let context = test_context();
        let request = datagram_request(7, &framed(Method::Ping.tag()));

        dispatch(&context, &request)
            .await
            .expect("the handler replied");

        let learned = context.close_nodes.close_nodes(sender_id());
        assert!(
            learned
                .iter()
                .any(|c| c.id == sender_id() && c.address == addr())
        );
    }

    /// The rule the two wire shapes differ on: a connection's `from` is not a
    /// dialable address (see the module doc), so it must never be learned —
    /// unlike the datagram above, which is otherwise the identical request
    /// through the identical code.
    #[tokio::test]
    async fn a_connection_request_does_not_learn_the_sender_as_a_contact() {
        let context = test_context();
        let request = connection_request(7, &framed(Method::Ping.tag()));

        dispatch(&context, &request)
            .await
            .expect("the handler replied");

        let learned = context.close_nodes.close_nodes(sender_id());
        assert!(!learned.iter().any(|c| c.id == sender_id()));
    }

    /// An unrecognised request is dropped, not answered.
    #[tokio::test]
    async fn unknown_methods_go_unanswered() {
        let request = datagram_request(7, &framed(b"NOT_A_METHOD"));

        assert_eq!(dispatch(&test_context(), &request).await, None);
    }

    /// The echo the requester's `Pending` matches on: a reply goes back under
    /// the id it came in with, so `deliver` finds the slot the caller is
    /// parked in. An id that did not survive the round trip wakes nobody.
    #[tokio::test]
    async fn the_reply_carries_the_request_id_back() {
        let id = 0x0102_0304_0506_0708u64;
        let request = datagram_request(id, &framed(Method::Ping.tag()));

        let datagram = dispatch(&test_context(), &request)
            .await
            .expect("the handler replied");

        // Read back the way a recv loop reads it: id off the front, then body.
        // The tag marks it a reply; the number under it is the request's.
        let (echoed, body) = datagram.split_at(ID_LEN);
        let echoed = u64::from_be_bytes(echoed.try_into().unwrap());
        assert_eq!(echoed, reply_id(id));
        assert_eq!(body, pong());
    }

    /// A served node, wired the way a real one is: one UDP socket whose
    /// receive loop lives in the transport and feeds requests down a channel
    /// to `serve`, plus a TCP listener. Returns the address each transport
    /// answers on and the context it is served with (so a test can check what
    /// actually landed in `values` or the routing table).
    ///
    /// The transport is kept alive by the returned tuple: dropping it would
    /// take the socket, and with it the receive loop, down with it.
    async fn spawn_serve() -> (
        SocketAddr,
        SocketAddr,
        Arc<Context<crate::close_nodes::RecommendedCloseNodes>>,
        UdpTransport,
    ) {
        let tcp_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tcp_addr = tcp_listener.local_addr().unwrap();

        let udp_socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let udp_addr = udp_socket.local_addr().unwrap();

        let (requests, rx) = mpsc::channel(8);
        let transport = UdpTransport::with_requests(udp_socket, requests);

        let context = Context::new([0u8; 32], recommended([0u8; 32]));
        tokio::spawn(serve(
            Arc::clone(&context),
            rx,
            transport.socket(),
            tcp_listener,
        ));
        (tcp_addr, udp_addr, context, transport)
    }

    /// Two pings sent back-to-back down the real UDP path — transport receive
    /// loop, request channel, dispatcher, reply back out the same socket —
    /// both come back, each under its own id.
    #[tokio::test]
    async fn udp_requests_are_served_concurrently() {
        let (_tcp_addr, udp_addr, _context, _transport) = spawn_serve().await;
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

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

        assert!(seen.contains(&frame_reply(1, &pong())));
        assert!(seen.contains(&frame_reply(2, &pong())));
    }

    /// A request the transport hands over is untagged; the reply it sends back
    /// carries the same id with [`REPLY_TAG`] set, which is what stops the
    /// requester's receive loop from reading its own answer as a new question.
    #[tokio::test]
    async fn a_reply_comes_back_tagged_under_the_request_id() {
        let (_tcp_addr, udp_addr, _context, _transport) = spawn_serve().await;
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let id = 0x0102_0304_0506_0708u64;

        client
            .send_to(&framed_datagram(id, Method::Ping.tag()), udp_addr)
            .await
            .unwrap();

        let mut buf = [0u8; 64];
        let (len, _) = tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .expect("answered within 1s")
            .unwrap();

        let echoed = u64::from_be_bytes(buf[..ID_LEN].try_into().unwrap());
        assert_eq!(request_id(echoed), id, "the id must survive the round trip");
        assert!(is_reply(echoed), "a reply must be tagged as one");
        assert_eq!(&buf[ID_LEN..len], pong());
    }

    /// A FIND_NODE driven down the real UDP path, decoded by the same
    /// [`find_node::decode_reply`] a live [`Rpc`](crate::rpc::Rpc) uses.
    ///
    /// The one test that puts both halves of FIND_NODE together. Neither half
    /// alone catches a framing disagreement between them: the handler's own
    /// tests never reach a dispatcher, and `Rpc`'s never reach a handler. That
    /// gap is how #34 shipped — `handle` put the request id on the front of its
    /// reply, `frame_reply` put it there again, and every FIND_NODE on the wire
    /// decoded to nothing while both sides' tests stayed green.
    #[tokio::test]
    async fn udp_find_node_round_trips_through_the_dispatcher() {
        let (_tcp_addr, udp_addr, context, _transport) = spawn_serve().await;
        let client = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let known = Contact {
            id: [2u8; 32],
            address: "127.0.0.1:8001".parse().unwrap(),
        };
        context.close_nodes.maybe_add_contact(known);

        let target = [1u8; 32];
        let mut datagram = framed_datagram(7, Method::FindNode.tag());
        datagram.extend(find_node::encode_request(target).unwrap());
        client.send_to(&datagram, udp_addr).await.unwrap();

        let mut buf = [0u8; 1024];
        let (len, _) = tokio::time::timeout(Duration::from_secs(1), client.recv_from(&mut buf))
            .await
            .expect("answered within 1s")
            .unwrap();

        // Exactly what `Rpc::find_node` does with the bytes it gets back.
        let contacts = find_node::decode_reply(&buf[ID_LEN..len])
            .expect("the reply must decode as the contacts it carries");
        assert!(contacts.contains(&known));
        // The requester is learned while being answered, so it comes back too.
        assert!(contacts.iter().any(|c| c.id == sender_id()));
    }

    /// A STORE arriving over the TCP loop, framed exactly the way
    /// [`TcpTransport`](crate::rpc_transport::tcp_transport::TcpTransport)
    /// sends it (write the request, half-close, read to EOF), is dispatched,
    /// lands in `values`, and the ack comes back over the same connection.
    #[tokio::test]
    async fn tcp_store_is_written_and_acked() {
        let (tcp_addr, _udp_addr, context, _transport) = spawn_serve().await;
        let key: Key = [9u8; 32];
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

    /// The same STORE as [`tcp_store_is_written_and_acked`], served entirely
    /// in-process: same [`dispatch`], same [`serve_streams`] accept
    /// loop, same
    /// [`stream_send_receive`](crate::rpc_transport::stream_framing::stream_send_receive)
    /// framing — with an
    /// [`Endpoint`](crate::rpc_transport::networked_debug_transport::Endpoint)
    /// where the `TcpListener` was. Nothing is bound in the OS, which is what
    /// makes a network of a thousand nodes affordable.
    ///
    /// The value is larger than a datagram carries, so this also pins the
    /// reason the node has a second transport at all.
    #[tokio::test]
    async fn store_over_the_fake_stream_wire_is_written_and_acked() {
        let network = Network::new();
        let endpoint = network.bind_any();
        let addr = endpoint.local_addr();

        let context = Context::new(TEST_ID, recommended(TEST_ID));
        // TODO deal with spawn handle
        tokio::spawn(serve_streams(Arc::clone(&context), endpoint));

        let key: Key = [9u8; 32];
        let value = vec![0xABu8; 5000];
        let transport = NetworkedStreamTransport::new(network);

        let mut request = framed(Method::Store.tag());
        request.extend(bincode::serialize(&(key, value.clone())).unwrap());

        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            transport.send_receive(request, addr),
        )
        .await
        .expect("answered within 1s")
        .expect("the node acked the store");

        assert_eq!(reply, store::STORED);
        assert_eq!(context.values.get(&key).as_deref(), Some(&value));
    }

    /// The rule [`Request::from_is_reachable`] exists for, end to end on the
    /// fake wire: a request that arrives over a connection must not put its
    /// sender in the routing table, because the address it came from is not
    /// one the sender can be reached at.
    ///
    /// [`a_connection_request_does_not_learn_the_sender_as_a_contact`] covers
    /// the decision in [`dispatch`]; this one covers the wiring that gets it
    /// there — a real accept loop, a real `from`, and
    /// [`Request::from_connection`] rather than [`Request::from_datagram`]
    /// being the constructor [`serve_streams`] reaches for.
    #[tokio::test]
    async fn a_served_stream_request_does_not_learn_its_sender() {
        let network = Network::new();
        let endpoint = network.bind_any();
        let addr = endpoint.local_addr();

        let context = Context::new(TEST_ID, recommended(TEST_ID));
        // TODO deal with spawn handle
        tokio::spawn(serve_streams(Arc::clone(&context), endpoint));

        let transport = NetworkedStreamTransport::new(network);
        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            transport.send_receive(framed(Method::Ping.tag()), addr),
        )
        .await
        .expect("answered within 1s")
        .expect("the node answered the ping");
        assert_eq!(reply, pong());

        assert!(
            !context
                .close_nodes
                .close_nodes(sender_id())
                .iter()
                .any(|c| c.id == sender_id()),
            "a connection's sender must not be learned as a contact"
        );
    }
}
