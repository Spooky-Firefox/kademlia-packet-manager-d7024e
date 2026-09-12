//! FIND_VALUE: answer with a stored value, or with where to look next.
//!
//! [`find_node`](super::find_node) with a shortcut: if the key is one we
//! hold, the lookup ends here and the value comes back instead of the
//! contacts. The two cases share a request but not a reply, so the encoding
//! has to let the caller tell them apart — that is what
//! [`FindValue`](crate::rpc::FindValue) is on the other end.

use crate::close_nodes::{CloseNodes, Key};
use crate::handle_rpc::Context;
use crate::rpc::FindValue;
use std::net::SocketAddr;

/// Answer a FIND_VALUE for the key in `body`.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    _id: u64,
    _from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    let key: Key = bincode::deserialize(body).ok()?;

    let reply = if let Some(value) = context.values.get(&key) {
        FindValue::Value(value.value().clone())
    } else {
        FindValue::Closest(context.close_nodes.close_nodes(key))
    };

    bincode::serialize(&reply).ok()
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::{Contact, NodeId};
    use crate::rpc::FindValue;

    struct FakeCloseNodes {
        contacts: Vec<Contact>,
    }

    impl CloseNodes for FakeCloseNodes {
        fn close_nodes(&self, _id: NodeId) -> Vec<Contact> {
            self.contacts.clone()
        }

        fn maybe_add_contact(&self, _contact: Contact) {}
    }

    fn addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    #[tokio::test]
    async fn find_value_returns_stored_value() {
        let key = [1u8; 20];
        let value = b"hello".to_vec();

        let context = Context::new(FakeCloseNodes {
            contacts: Vec::new(),
        });

        context.values.insert(key, value.clone());

        let body = bincode::serialize(&key).unwrap();

        let reply = handle(&context, 1, addr(), &body).await.unwrap();

        let decoded: FindValue = bincode::deserialize(&reply).unwrap();

        assert_eq!(decoded, FindValue::Value(value));
    }

    #[tokio::test]
    async fn find_value_returns_closest_contacts_when_value_missing() {
        let key = [1u8; 20];

        let contacts = vec![
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
            contacts: contacts.clone(),
        });

        let body = bincode::serialize(&key).unwrap();

        let reply = handle(&context, 1, addr(), &body).await.unwrap();

        let decoded: FindValue = bincode::deserialize(&reply).unwrap();

        assert_eq!(decoded, FindValue::Closest(contacts));
    }
}
