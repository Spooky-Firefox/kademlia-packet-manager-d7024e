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
//! joiner can route with. The request carries the sender's own contact for the
//! same reason in the other direction — see
//! [`bootstrap`](crate::bootstrap), where the two halves meet.

use crate::close_nodes::{CloseNodes, Contact, NodeId};
use crate::handle_rpc::Context;
use log::trace;
use std::net::SocketAddr;

/// Encode a PING body: just the sender, who is the whole request.
pub fn encode_request(sender: Contact) -> Option<Vec<u8>> {
    bincode::serialize(&sender).ok()
}

/// Read a PING reply as the responder's id.
pub fn decode_reply(reply: &[u8]) -> Option<NodeId> {
    bincode::deserialize(reply).ok()
}

/// Answer a PING with our own [`NodeId`].
///
/// A body we cannot read is answered anyway. It is a peer that knows something
/// we do not, and refusing to reply would only make us look dead to a node
/// talking to us in good faith — we just learn nothing from it.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    id: u64,
    from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    match bincode::deserialize::<Contact>(body) {
        // Every arriving RPC is evidence of liveness, and dropping it is how a
        // routing table goes stale.
        Ok(sender) => context.close_nodes.maybe_add_contact(sender),
        Err(e) => trace!("PING {id} from {from} carried no readable contact: {e}"),
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
        let sender = Contact {
            id: [1u8; 20],
            address: addr(),
        };

        let reply = handle(&context, 1, addr(), &encode_request(sender).unwrap())
            .await
            .unwrap();

        assert_eq!(decode_reply(&reply), Some(my_id));
    }

    #[tokio::test]
    async fn ping_learns_the_sender() {
        let my_id = [7u8; 20];
        let context = Context::new(my_id, recommended(my_id));
        let sender = Contact {
            id: [1u8; 20],
            address: addr(),
        };

        handle(&context, 1, addr(), &encode_request(sender).unwrap()).await;

        assert_eq!(context.close_nodes.close_nodes(sender.id), vec![sender]);
    }

    /// An unreadable body still gets an answer: we look alive either way, we
    /// just do not learn who asked.
    #[tokio::test]
    async fn ping_with_an_unreadable_body_is_still_answered() {
        let my_id = [7u8; 20];
        let context = Context::new(my_id, recommended(my_id));

        let reply = handle(&context, 1, addr(), b"not a contact").await.unwrap();

        assert_eq!(decode_reply(&reply), Some(my_id));
        assert!(context.close_nodes.close_nodes([0u8; 20]).is_empty());
    }
}
