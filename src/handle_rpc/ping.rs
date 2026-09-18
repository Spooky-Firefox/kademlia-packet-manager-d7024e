//! PING: prove the node is alive, and say who is alive.
//!
//! The one RPC with no lookup behind it, so it is also the liveness probe
//! every other part of the system leans on — bucket refresh, eviction, and a
//! caller deciding whether a contact is worth keeping.
//!
//! It carries the join handshake as well. A node joining the network starts
//! with a bootstrap *address* and nothing else, and
//! [`Contact`](crate::close_nodes::Contact) needs an id to go with it. So the
//! reply is our [`NodeId`]: one round trip turns an address into a contact the
//! joiner can route with — see [`bootstrap`](crate::bootstrap), which is the
//! caller that needs it.
//!
//! The request itself is empty. Who is asking travels ahead of the method tag,
//! in the framing every request carries, and is learned by the dispatcher
//! before any handler runs.

use crate::close_nodes::{CloseNodes, NodeId};
use crate::handle_rpc::Context;
use log::trace;
use std::net::SocketAddr;

/// Read a PING reply as the responder's id.
pub fn decode_reply(reply: &[u8]) -> Option<NodeId> {
    bincode::deserialize(reply).ok()
}

/// Answer a PING with our own [`NodeId`].
///
/// `body` is expected to be empty; a non-empty one is a peer that knows
/// something we do not, and is answered anyway. Refusing to reply would only
/// make us look dead to a node that is talking to us in good faith.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    id: u64,
    from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    if !body.is_empty() {
        trace!(
            "PING {id} from {from} carried {} unexpected bytes",
            body.len()
        );
    }
    bincode::serialize(&context.my_id).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::recommended;

    fn addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    #[tokio::test]
    async fn ping_answers_with_our_own_id() {
        let my_id = [7u8; 20];
        let context = Context::new(my_id, recommended(my_id));

        let reply = handle(&context, 1, addr(), b"").await.unwrap();

        assert_eq!(decode_reply(&reply), Some(my_id));
    }

    /// An unexpected body still gets an answer: we look alive either way.
    #[tokio::test]
    async fn ping_with_an_unexpected_body_is_still_answered() {
        let my_id = [7u8; 20];
        let context = Context::new(my_id, recommended(my_id));

        let reply = handle(&context, 1, addr(), b"not a ping").await.unwrap();

        assert_eq!(decode_reply(&reply), Some(my_id));
    }
}
