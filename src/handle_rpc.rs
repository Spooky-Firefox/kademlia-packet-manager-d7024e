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
//! seam this module is written against, and it keeps the id framing where it
//! already lives: [`Request::respond`] carries only the reply body, and the
//! transport re-attaches the id it stripped on the way in.
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
use log::trace;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

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
pub struct Context<A> {
    pub close_nodes: A,
    // TODO: the value store that STORE writes and FIND_VALUE reads.
}

impl<A: CloseNodes> Context<A> {
    /// Shared by every spawned handler, so it is handed out behind an `Arc`.
    pub fn new(close_nodes: A) -> Arc<Self> {
        Arc::new(Self { close_nodes })
    }
}

/// One inbound RPC, waiting to be answered.
#[non_exhaustive]
pub struct Request {
    /// Who sent it, and where the reply goes.
    ///
    /// A handler also wants this to learn the sender as a contact, which it
    /// cannot do yet: [`Contact`](crate::close_nodes::Contact) needs a
    /// [`NodeId`](crate::close_nodes::NodeId) and no request body carries one.
    pub from: SocketAddr,
    /// The request as it arrived — method tag and all, minus the transport's
    /// id prefix.
    pub payload: Vec<u8>,
    /// Where the reply body goes; the transport re-attaches the id framing.
    /// Dropping this answers nothing, which over UDP is a legitimate answer.
    pub respond: oneshot::Sender<Vec<u8>>,
}

impl Request {
    /// A request and the half that resolves once it has been answered. `None`
    /// out of the receiver means the node chose not to reply.
    pub fn new(from: SocketAddr, payload: Vec<u8>) -> (Self, oneshot::Receiver<Vec<u8>>) {
        let (respond, rx) = oneshot::channel();
        (
            Self {
                from,
                payload,
                respond,
            },
            rx,
        )
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
        from,
        payload,
        respond,
    } = request;

    let Some((method, body)) = Method::split_tag(&payload) else {
        // Not ours: a stray datagram, a peer speaking a later version of the
        // protocol, or a reply that arrived after its request was cancelled.
        trace!(
            "Unrecognised request from {from}, dropping {} bytes",
            payload.len()
        );
        return;
    };
    trace!("{method:?} from {from}, {} byte body", body.len());

    let reply = match method {
        Method::Ping => ping::handle(&context, from, body).await,
        Method::Store => store::handle(&context, from, body).await,
        Method::FindNode => find_node::handle(&context, from, body).await,
        Method::FindValue => find_value::handle(&context, from, body).await,
    };

    let Some(reply) = reply else {
        trace!("No reply to the {method:?} from {from}");
        return;
    };
    // Err means the requester gave up and dropped the responder.
    if respond.send(reply).is_err() {
        trace!("Nobody left to take the {method:?} reply for {from}");
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

    #[tokio::test]
    async fn ping_is_answered_with_a_pong() {
        let requests = spawn_serve();
        let (request, reply) = Request::new(addr(), Method::Ping.tag().to_vec());
        requests.send(request).await.unwrap();

        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), reply)
                .await
                .expect("answered within 1s")
                .expect("the handler replied"),
            ping::PONG
        );
    }

    /// An unrecognised request is dropped, not answered: the responder is
    /// closed without a value, and the sender sees a timeout as it would for
    /// any lost datagram.
    #[tokio::test]
    async fn unknown_methods_go_unanswered() {
        let requests = spawn_serve();
        let (request, reply) = Request::new(addr(), b"NOT_A_METHOD".to_vec());
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
        let (first, first_reply) = Request::new(addr(), Method::Ping.tag().to_vec());
        let (second, second_reply) = Request::new(addr(), Method::Ping.tag().to_vec());
        requests.send(first).await.unwrap();
        requests.send(second).await.unwrap();

        let (a, b) = tokio::join!(first_reply, second_reply);
        assert_eq!(a.unwrap(), ping::PONG);
        assert_eq!(b.unwrap(), ping::PONG);
    }
}
