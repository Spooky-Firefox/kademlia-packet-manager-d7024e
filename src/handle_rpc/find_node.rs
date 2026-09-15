//! FIND_NODE: answer with the contacts we know closest to a target.
//!
//! The RPC an iterative lookup is built out of: the caller asks the closest
//! nodes it knows for the ones they know, and repeats until the answers stop
//! getting closer. Our reply is [`CloseNodes::close_nodes`] verbatim — up to
//! [`K`](crate::close_nodes::K) contacts, nearest first — whether or not we
//! are anywhere near the target ourselves.
//!
//! The request carries the sender alongside the target, so answering it also
//! teaches us a contact. That is what makes a join propagate: a node nobody
//! has heard of becomes reachable the moment it asks its first question.

use crate::close_nodes::{CloseNodes, Contact, NodeId};
use crate::handle_rpc::Context;
use std::net::SocketAddr;

/// Encode a FIND_NODE body: who is asking, and what they are asking about.
///
/// One bincode value rather than two concatenated ones, so the decode cannot
/// disagree with the encode about where the first field ends.
pub fn encode_request(sender: Contact, target: NodeId) -> Option<Vec<u8>> {
    bincode::serialize(&(sender, target)).ok()
}

/// Read a FIND_NODE reply as the contacts it carries.
pub fn decode_reply(reply: &[u8]) -> Option<Vec<Contact>> {
    bincode::deserialize(reply).ok()
}

/// Answer a FIND_NODE for the target id in `body`.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    id: u64,
    from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    let (sender, target): (Contact, NodeId) = bincode::deserialize(body).ok()?;
    context.close_nodes.maybe_add_contact(sender);

    let contacts = context.close_nodes.close_nodes(target);
    // The body only. `dispatch` puts the request id back on the front via
    // `frame_reply`; doing it here as well would send the id twice and leave
    // the caller unable to decode its own reply.
    bincode::serialize(&contacts).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::recommended;

    fn addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    fn sender() -> Contact {
        Contact {
            id: [9u8; 20],
            address: "127.0.0.1:8009".parse().unwrap(),
        }
    }

    #[tokio::test]
    async fn find_node_returns_the_contacts_it_knows() {
        let my_id = [0u8; 20];
        let context = Context::new(my_id, recommended(my_id));

        let known = Contact {
            id: [2u8; 20],
            address: "127.0.0.1:8001".parse().unwrap(),
        };
        context.close_nodes.maybe_add_contact(known);

        let target = [1u8; 20];
        let body = encode_request(sender(), target).unwrap();
        let reply = handle(&context, 42, addr(), &body).await.unwrap();

        // The sender is learned while answering, so it comes back too.
        let mut contacts = decode_reply(&reply).unwrap();
        contacts.sort_by_key(|c| c.id);
        assert_eq!(contacts, vec![known, sender()]);
    }

    /// The reply is the body alone. `dispatch` adds the id, so a handler that
    /// also added one would make the reply undecodable.
    #[tokio::test]
    async fn find_node_reply_carries_no_id_of_its_own() {
        let my_id = [0u8; 20];
        let context = Context::new(my_id, recommended(my_id));

        let body = encode_request(sender(), [1u8; 20]).unwrap();
        let reply = handle(&context, 42, addr(), &body).await.unwrap();

        assert!(decode_reply(&reply).is_some());
    }

    #[tokio::test]
    async fn find_node_learns_the_sender() {
        let my_id = [0u8; 20];
        let context = Context::new(my_id, recommended(my_id));

        let body = encode_request(sender(), [1u8; 20]).unwrap();
        handle(&context, 42, addr(), &body).await.unwrap();

        assert_eq!(context.close_nodes.close_nodes(sender().id), vec![sender()]);
    }

    #[tokio::test]
    async fn find_node_rejects_invalid_body() {
        let my_id = [0u8; 20];
        let context = Context::new(my_id, recommended(my_id));

        assert!(handle(&context, 42, addr(), b"too short").await.is_none());
    }
}
