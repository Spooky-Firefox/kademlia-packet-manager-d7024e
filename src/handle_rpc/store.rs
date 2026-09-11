//! STORE: hold a value on behalf of the network.
//!
//! We are asked to store a key that is close to us, so the sender's idea of
//! "close" is worth sanity-checking against our own before spending storage on
//! it — but not worth refusing over, since our routing table and theirs are
//! allowed to disagree while the network churns.

use crate::close_nodes::{CloseNodes, Key};
use crate::handle_rpc::Context;
use std::net::SocketAddr;

pub const STORED: &[u8] = b"STORED";

/// Store the key and value in `body`, and acknowledge it.
///
/// Returning `None` leaves the sender to time out, which is the right answer
/// for a request we decline to serve: it has no way to tell "refused" from
/// "unreachable" anyway, and both mean the value did not land here.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    id: u64,
    from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    let (key, value): (Key, Vec<u8>) = bincode::deserialize(body).ok()?;
    context.values.insert(key, value);
    Some(STORED.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::{Contact, NodeId};

    // STORE does not use routing table
    struct FakeCloseNodes;

    impl CloseNodes for FakeCloseNodes {
        fn close_nodes(&self, _id: NodeId) -> Vec<Contact> {
            Vec::new()
        }
        fn maybe_add_contact(&self, _contact: Contact) {}
    }

    fn addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    #[tokio::test]
    async fn store_writes_the_value_and_acks() {
        let key: Key = [1u8; 20];
        let value = b"hello".to_vec();
        let context = Context::new(FakeCloseNodes);

        let body = bincode::serialize(&(key, value.clone())).unwrap();

        let reply = handle(&context, 42, addr(), &body).await;

        assert_eq!(reply.as_deref(), Some(STORED));
        assert_eq!(context.values.get(&key).as_deref(), Some(&value));
    }

    #[tokio::test]
    async fn store_overwrites_an_existing_value() {
        let key = [1u8; 20];
        let context = Context::new(FakeCloseNodes);

        let first = bincode::serialize(&(key, b"old".to_vec())).unwrap();
        let second = bincode::serialize(&(key, b"new".to_vec())).unwrap();
        handle(&context, 1, addr(), &first).await;
        handle(&context, 2, addr(), &second).await;

        assert_eq!(context.values.get(&key).as_deref(), Some(&b"new".to_vec()));
    }

    #[tokio::test]
    async fn store_rejects_invalid_body() {
        let context = Context::new(FakeCloseNodes);

        let reply = handle(&context, 42, addr(), b"too short").await;

        assert!(reply.is_none());
        assert!(context.values.is_empty());
    }
}
