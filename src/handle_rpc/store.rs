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
        let key: Key = [1u8; 32];
        let value = b"hello".to_vec();
        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id));

        let body = encode_request(key, value.clone()).unwrap();
        let reply = handle(&context, 42, addr(), &body).await;

        assert_eq!(reply.as_deref(), Some(STORED));
        assert_eq!(context.values.get(&key).as_deref(), Some(&value));
    }

    #[tokio::test]
    async fn store_overwrites_an_existing_value() {
        let key: Key = [1u8; 32];
        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id));

        let first = encode_request(key, b"old".to_vec()).unwrap();
        let second = encode_request(key, b"new".to_vec()).unwrap();
        handle(&context, 1, addr(), &first).await;
        handle(&context, 2, addr(), &second).await;

        assert_eq!(context.values.get(&key).as_deref(), Some(&b"new".to_vec()));
    }

    #[tokio::test]
    async fn store_rejects_invalid_body() {
        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id));

        let reply = handle(&context, 42, addr(), b"too short").await;

        assert!(reply.is_none());
        assert!(context.values.is_empty());
    }
}
