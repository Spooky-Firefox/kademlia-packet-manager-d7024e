//! The server side of the four Kademlia RPCs.
//!
//! [`Rpc`](crate::rpc::Rpc) is the client half: it encodes a request, hands it
//! to a transport and waits for the reply. This module is the other half.
//! [`serve`] takes inbound requests off a channel, reads the method tag off
//! the front of each payload and spawns the matching handler.
//!
//! # Where the requests come from
//!
//! Neither transport routes inbound *requests* anywhere yet. Their receive
//! loops match each datagram's 8-byte id against
//! [`Pending`](crate::pending::Pending) and drop it when nobody is waiting —
//! which is exactly what an unsolicited request looks like from there. Handing
//! those datagrams to a [`Request`] channel instead of dropping them is the
//! seam this module is written against, and [`Request::from_datagram`] is the
//! split they need: the id off the front, the request behind it.
//!
//! The id goes back out on the reply. It is the only thing in the returning
//! datagram that says which of the requester's awaiting tasks this answers, so
//! [`frame_reply`] puts it back on the front before the answer leaves and
//! [`Request::respond`] carries the finished datagram — a transport sends
//! those bytes as they are rather than reconstructing the framing itself.
//!
//! # Why a task per request
//!
//! A handler can block on work of its own — a STORE that hits disk, a
//! FIND_VALUE that has to ask someone else first. Awaiting it inline would
//! stall every other request behind it, on a wire where the sender has already
//! started its own timeout. So the loop does nothing but parse and dispatch,
//! and the work happens in a task.

pub mod find_node;
pub mod find_value;
pub mod ping;
pub mod store;

use crate::close_nodes::CloseNodes;
use crate::close_nodes::Key;
use dashmap::DashMap;
use log::trace;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

/// Wire framing: an 8-byte big-endian request id, then the payload. The same
/// framing every transport writes — see
/// [`UdpTransport`](crate::rpc_transport::udp_transport::UdpTransport), which
/// strips exactly this much before a request ever reaches us.
pub const ID_LEN: usize = size_of::<u64>();

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
    /// [`from`](Self::from). A handler wants it to name the request in a log
    /// line that the other node's log can be read against, and to reference it
    /// in a reply it frames itself, as a STORE answering over TCP would.
    pub id: u64,
    /// Who sent it, and where the reply goes.
    ///
    /// A handler also wants this to learn the sender as a contact, which it
    /// cannot do yet: [`Contact`](crate::close_nodes::Contact) needs a
    /// [`NodeId`](crate::close_nodes::NodeId) and no request body carries one.
    pub from: SocketAddr,
    /// The request as it arrived — method tag and all, minus the transport's
    /// id prefix.
    pub payload: Vec<u8>,
    /// Where the finished reply goes: the complete datagram, id and all, ready
    /// to hand straight to `send_to`. Dropping this answers nothing, which
    /// over UDP is a legitimate answer.
    pub respond: oneshot::Sender<Vec<u8>>,
}

impl Request {
    /// A request and the half that resolves once it has been answered. `None`
    /// out of the receiver means the node chose not to reply.
    pub fn new(id: u64, from: SocketAddr, payload: Vec<u8>) -> (Self, oneshot::Receiver<Vec<u8>>) {
        let (respond, rx) = oneshot::channel();
        (
            Self {
                id,
                from,
                payload,
                respond,
            },
            rx,
        )
    }

    /// Split a datagram as it came off the wire: [`ID_LEN`] bytes of id, then
    /// the request itself.
    ///
    /// `None` if it is too short to carry an id — the same length check the
    /// transports' receive loops already make before matching one against
    /// [`Pending`](crate::pending::Pending). Reading the id here rather than
    /// taking the transport's word for it keeps the framing in one place; a
    /// transport that has already parsed it can call [`new`](Self::new).
    pub fn from_datagram(
        from: SocketAddr,
        datagram: &[u8],
    ) -> Option<(Self, oneshot::Receiver<Vec<u8>>)> {
        let (id, payload) = datagram.split_at_checked(ID_LEN)?;
        // split_at_checked handed back exactly ID_LEN bytes.
        let id = u64::from_be_bytes(id.try_into().unwrap());
        Some(Self::new(id, from, payload.to_vec()))
    }
}

/// Answer inbound requests until `requests` closes.
///
/// The channel is bounded on purpose: the side feeding it should `try_send`
/// and drop what does not fit, rather than park its receive loop behind a
/// backlog. Shedding load looks like a lost datagram to the sender, which is
/// the failure every caller already handles; blocking the socket does not.
pub async fn serve<A>(context: Arc<Context<A>>, mut requests: mpsc::Receiver<Request>)
where
    A: CloseNodes + Send + Sync + 'static,
{
    tokio::spawn(async move {
        tokio::task::yield_now().await;
        todo!("create a tcp loop to handle relibale transport of store data");
    });
    while let Some(request) = requests.recv().await {
        let context = Arc::clone(&context);
        // TODO deal with spawn handle
        tokio::spawn(dispatch(context, request));
    }
    trace!("Request channel closed, stopping the serve loop");
}

/// Parse one request and hand it to its handler.
///
/// Split out of [`serve`] so the spawned task owns the parse too: a malformed
/// payload then costs a task instead of a turn of the loop everyone else is
/// queued behind.
async fn dispatch<A: CloseNodes>(context: Arc<Context<A>>, request: Request) {
    let Request {
        id,
        from,
        payload,
        respond,
    } = request;

    let Some((method, body)) = Method::split_tag(&payload) else {
        // Not ours: a stray datagram, a peer speaking a later version of the
        // protocol, or a reply that arrived after its request was cancelled.
        trace!(
            "Unrecognised request {id} from {from}, dropping {} bytes",
            payload.len()
        );
        return;
    };
    trace!("{method:?} {id} from {from}, {} byte body", body.len());

    let reply = match method {
        Method::Ping => ping::handle(&context, id, from, body).await,
        Method::Store => store::handle(&context, id, from, body).await,
        Method::FindNode => find_node::handle(&context, id, from, body).await,
        Method::FindValue => find_value::handle(&context, id, from, body).await,
    };

    let Some(reply) = reply else {
        trace!("No reply to {method:?} {id} from {from}");
        return;
    };
    // Err means the requester gave up and dropped the responder.
    if respond.send(frame_reply(id, &reply)).is_err() {
        trace!("Nobody left to take the {method:?} {id} reply for {from}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::recommended;
    use std::time::Duration;

    fn addr() -> SocketAddr {
        "127.0.0.1:4242".parse().unwrap()
    }

    /// A served node with a channel to feed requests into.
    fn spawn_serve() -> mpsc::Sender<Request> {
        let (tx, rx) = mpsc::channel(8);
        tokio::spawn(serve(Context::new(recommended([0u8; 20])), rx));
        tx
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

    /// The framing `UdpTransport::send_receive` writes, read back: an 8-byte
    /// big-endian id, then the payload. Built here the way the transport
    /// builds it, so the two cannot drift apart unnoticed.
    #[test]
    fn a_datagram_splits_into_its_id_and_the_request() {
        let id = 0x0102_0304_0506_0708u64;
        let mut datagram = id.to_be_bytes().to_vec();
        datagram.extend_from_slice(Method::Ping.tag());

        let (request, _reply) = Request::from_datagram(addr(), &datagram).expect("carries an id");
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
        // request with no method tag, which dispatch drops on its own.
        assert!(Request::from_datagram(addr(), &[0u8; ID_LEN]).is_some());
    }

    #[tokio::test]
    async fn ping_is_answered_with_a_pong() {
        let requests = spawn_serve();
        let (request, reply) = Request::new(7, addr(), Method::Ping.tag().to_vec());
        requests.send(request).await.unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), reply)
                .await
                .expect("answered within 1s")
                .expect("the handler replied"),
            frame_reply(7, ping::PONG)
        );
    }

    /// An unrecognised request is dropped, not answered: the responder is
    /// closed without a value, and the sender sees a timeout as it would for
    /// any lost datagram.
    #[tokio::test]
    async fn unknown_methods_go_unanswered() {
        let requests = spawn_serve();
        let (request, reply) = Request::new(7, addr(), b"NOT_A_METHOD".to_vec());
        requests.send(request).await.unwrap();

        assert!(
            tokio::time::timeout(Duration::from_secs(1), reply)
                .await
                .expect("resolved within 1s")
                .is_err()
        );
    }

    /// One handler must not hold up the next request, which is the whole
    /// reason the loop spawns.
    #[tokio::test]
    async fn requests_are_served_concurrently() {
        let requests = spawn_serve();
        let (first, first_reply) = Request::new(1, addr(), Method::Ping.tag().to_vec());
        let (second, second_reply) = Request::new(2, addr(), Method::Ping.tag().to_vec());
        requests.send(first).await.unwrap();
        requests.send(second).await.unwrap();

        let (a, b) = tokio::join!(first_reply, second_reply);
        assert_eq!(a.unwrap(), frame_reply(1, ping::PONG));
        assert_eq!(b.unwrap(), frame_reply(2, ping::PONG));
    }

    /// The echo the requester's `Pending` matches on: a reply goes back under
    /// the id it came in with, so `deliver` finds the slot the caller is
    /// parked in. An id that did not survive the round trip wakes nobody.
    #[tokio::test]
    async fn the_reply_carries_the_request_id_back() {
        let requests = spawn_serve();
        let id = 0x0102_0304_0506_0708u64;
        let (request, reply) = Request::new(id, addr(), Method::Ping.tag().to_vec());
        requests.send(request).await.unwrap();

        let datagram = tokio::time::timeout(Duration::from_secs(1), reply)
            .await
            .expect("answered within 1s")
            .expect("the handler replied");

        // Read back the way a recv loop reads it: id off the front, then body.
        let (echoed, body) = datagram.split_at(ID_LEN);
        assert_eq!(u64::from_be_bytes(echoed.try_into().unwrap()), id);
        assert_eq!(body, ping::PONG);
    }

    /// A reply reframed by `frame_reply` is a datagram `from_datagram` reads,
    /// so the two halves of the framing cannot drift apart.
    #[test]
    fn framing_round_trips() {
        let id = u64::MAX;
        let datagram = frame_reply(id, b"PONG");
        let (parsed, _reply) = Request::from_datagram(addr(), &datagram).expect("carries an id");
        assert_eq!(parsed.id, id);
        assert_eq!(parsed.payload, b"PONG");
    }
}
