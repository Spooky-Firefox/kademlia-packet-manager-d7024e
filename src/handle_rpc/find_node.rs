//! FIND_NODE: answer with the contacts we know closest to a target.
//!
//! The RPC an iterative lookup is built out of: the caller asks the closest
//! nodes it knows for the ones they know, and repeats until the answers stop
//! getting closer. Our reply is [`CloseNodes::close_nodes`] verbatim — up to
//! [`K`](crate::close_nodes::K) contacts, nearest first — whether or not we
//! are anywhere near the target ourselves.

use crate::close_nodes::{CloseNodes, NodeId};
use crate::handle_rpc::Context;
use std::net::SocketAddr;

// TODO: drop this once the stub below has a real body.
#[allow(unused_variables)]
/// Answer a FIND_NODE for the target id in `body`.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    id: u64,
    from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    let target: NodeId = bincode::deserialize(body).ok()?;

    let contacts = context.close_nodes.close_nodes(target);
    let mut res = id.to_be_bytes().to_vec();
    res.extend(bincode::serialize(&contacts).ok()?);
    Some(res)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::Contact;

    struct FakeCloseNodes {
        target: NodeId,
        contacts: Vec<Contact>,
    }

    impl CloseNodes for FakeCloseNodes {
        fn close_nodes(&self, id: NodeId) -> Vec<Contact> {
            assert_eq!(id, self.target);
            self.contacts.clone()
        }

        fn maybe_add_contact(&self, _contact: Contact) {}
    }
    #[tokio::test]
    async fn find_node_returns_contacts() {
        let target = [1u8; 20];

        let expected_contacts = vec![
            Contact {
                id: [2u8; 20],
                address: "127.0.0.1:8001".parse().unwrap(),
            },
            Contact {
                id: [3u8; 20],
                address: "127.0.0.1:8002".parse().unwrap(),
            },
        ];

        let context = Context::new(FakeCloseNodes {
            target,
            contacts: expected_contacts.clone(),
        });

        let body = bincode::serialize(&target).unwrap();

        let response = handle(&context, 42, "127.0.0.1:9000".parse().unwrap(), &body)
            .await
            .unwrap();

        let (resp_id_bytes, contacts_bytes) = response.split_at(8);
        let resp_id = u64::from_be_bytes(resp_id_bytes.try_into().unwrap());
        let contacts: Vec<Contact> = bincode::deserialize(contacts_bytes).unwrap();

        assert_eq!(resp_id, 42);
        assert_eq!(contacts, expected_contacts);
    }

    #[tokio::test]
    async fn find_node_rejects_invalid_body() {
        let target = [1u8; 20];

        let context = Context::new(FakeCloseNodes {
            target,
            contacts: vec![],
        });

        let bad_body = b"too short";

        let response = handle(&context, 42, "127.0.0.1:9000".parse().unwrap(), bad_body).await;

        assert!(response.is_none());
    }
}
