//! STORE: hold a value on behalf of the network.
//!
//! We are asked to store a key that is close to us, so the sender's idea of
//! "close" is worth sanity-checking against our own before spending storage on
//! it — but not worth refusing over, since our routing table and theirs are
//! allowed to disagree while the network churns.

use crate::close_nodes::{CloseNodes, Key};
use crate::handle_rpc::Context;
use crate::hashing::key_for_value;
use std::net::SocketAddr;

pub const STORED: &[u8] = b"STORED";

/// Encode a STORE body: the pair to hold.
///
/// Who is asking is not in here — it travels ahead of the method tag, in the
/// framing every request carries.
pub fn encode_request(key: Key, value: Vec<u8>) -> Option<Vec<u8>> {
    bincode::serialize(&(key, value)).ok()
}

/// Store the key and value in `body`, and acknowledge it.
///
/// Returning `None` leaves the sender to time out, which is the right answer
/// for a request we decline to serve: it has no way to tell "refused" from
/// "unreachable" anyway, and both mean the value did not land here.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    _id: u64,
    _from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    let (key, value): (Key, Vec<u8>) = bincode::deserialize(body).ok()?;

    // The datastore is content-addressed: K must equal hash(V).
    if key != key_for_value(&value) {
        return None;
    }

    context.values.insert(key, value);
    Some(STORED.to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::recommended;

    fn addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    #[tokio::test]
    async fn store_writes_the_value_and_acks() {
        let value = b"hello".to_vec();
        let key = key_for_value(&value);
        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id));

        let body = encode_request(key, value.clone()).unwrap();
        let reply = handle(&context, 42, addr(), &body).await;

        assert_eq!(reply.as_deref(), Some(STORED));
        assert_eq!(context.values.get(&key).as_deref(), Some(&value));
    }

    #[tokio::test]
    async fn invalid_overwrite_is_rejected_and_original_value_is_preserved() {
        let original = b"old".to_vec();
        let key = key_for_value(&original);

        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id));

        // Valid first STORE.
        let body = encode_request(key, original.clone()).unwrap();
        let reply = handle(&context, 1, addr(), &body).await;

        assert_eq!(reply.as_deref(), Some(STORED));
        assert_eq!(context.values.get(&key).as_deref(), Some(&original));

        // Try to replace it with different content under the same key.
        let replacement = b"new".to_vec();

        assert_ne!(key, key_for_value(&replacement));

        let body = encode_request(key, replacement).unwrap();
        let reply = handle(&context, 2, addr(), &body).await;

        // Must reject it.
        assert!(reply.is_none());

        // Original content must still be there.
        assert_eq!(context.values.get(&key).as_deref(), Some(&original));
    }

    #[tokio::test]
    async fn store_rejects_invalid_body() {
        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id));

        let reply = handle(&context, 42, addr(), b"too short").await;

        assert!(reply.is_none());
        assert!(context.values.is_empty());
    }

    #[tokio::test]
    async fn store_rejects_value_when_key_does_not_match_hash() {
        let value = b"hello".to_vec();

        // Deliberately incorrect key.
        let wrong_key: Key = [1u8; 32];

        assert_ne!(wrong_key, key_for_value(&value));

        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id));

        let body = encode_request(wrong_key, value).unwrap();
        let reply = handle(&context, 1, addr(), &body).await;

        assert!(reply.is_none());
        assert!(context.values.is_empty());
    }

    #[tokio::test]
    async fn storing_the_same_value_twice_is_allowed() {
        let value = b"hello".to_vec();
        let key = key_for_value(&value);

        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id));

        let body = encode_request(key, value.clone()).unwrap();

        assert_eq!(
            handle(&context, 1, addr(), &body).await.as_deref(),
            Some(STORED)
        );

        assert_eq!(
            handle(&context, 2, addr(), &body).await.as_deref(),
            Some(STORED)
        );

        assert_eq!(context.values.get(&key).as_deref(), Some(&value));
    }
}
