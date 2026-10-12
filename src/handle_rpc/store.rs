//! STORE: hold a value on behalf of the network.
//!
//! We are asked to store a key that is close to us, so the sender's idea of
//! "close" is worth sanity-checking against our own before spending storage on
//! it — but not worth refusing over, since our routing table and theirs are
//! allowed to disagree while the network churns.
//!
//! What is worth refusing over is a value that does not belong under its key.
//! A blob must hash to it. A latest pointer must be signed by the key in its
//! domain's DNS record, and may only move to a higher version — otherwise
//! anyone could point a package at their own release, or roll it back.

use crate::close_nodes::{CloseNodes, Key};
use crate::dns_data::DnsData;
use crate::handle_rpc::Context;
use crate::package::{LatestPointer, Signed, latest_is_valid};
use crate::value::Value;
use dashmap::mapref::entry::Entry;
use std::net::SocketAddr;

pub const STORED: &[u8] = b"STORED";

/// Encode a STORE body: the pair to hold.
///
/// Who is asking is not in here — it travels ahead of the method tag, in the
/// framing every request carries.
pub fn encode_request(key: Key, value: Value) -> Option<Vec<u8>> {
    bincode::serialize(&(key, value)).ok()
}

/// Store the key and value in `body`, and acknowledge it.
///
/// Returning `None` leaves the sender to time out, which is the right answer
/// for a request we decline to serve: it has no way to tell "refused" from
/// "unreachable" anyway, and both mean the value did not land here.
pub async fn handle<A: CloseNodes, D: DnsData>(
    context: &Context<A, D>,
    _id: u64,
    _from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    let (key, value): (Key, Value) = bincode::deserialize(body).ok()?;

    // Each value has exactly one key it may live under: its hash for a blob,
    // `latest_key(domain, package)` for a pointer.
    if key != value.key() {
        return None;
    }

    match &value {
        Value::Blob(_) => {
            context.values.insert(key, value);
        }
        Value::Latest(pointer) => {
            let owner = context.dns.get_dns_vk(&pointer.body.domain).await?;
            if !latest_is_valid(pointer, &owner) {
                return None;
            }
            // Compare and insert under one entry lock, after the DNS await, so
            // two concurrent stores cannot both pass the version check.
            match context.values.entry(key) {
                Entry::Vacant(slot) => {
                    slot.insert(value);
                }
                Entry::Occupied(mut slot) => {
                    if !replaces(slot.get(), pointer) {
                        return None;
                    }
                    slot.insert(value);
                }
            }
        }
    }
    Some(STORED.to_vec())
}

/// Whether `new` may take the place of what is stored under its latest key: a
/// higher version, or the same pointer stored again (a republish).
fn replaces(stored: &Value, new: &Signed<LatestPointer>) -> bool {
    match stored {
        Value::Latest(old) => new.body.version > old.body.version || new == old,
        // Only a SHA-256 collision puts a blob under a latest key; keep it.
        Value::Blob(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::recommended;
    use crate::dns_data::fake_dns::FakeDns;

    fn addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    #[tokio::test]
    async fn store_writes_the_value_and_acks() {
        let value = Value::Blob(b"hello".to_vec());
        let key = value.key();
        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id), FakeDns::new());

        let body = encode_request(key, value.clone()).unwrap();
        let reply = handle(&context, 42, addr(), &body).await;

        assert_eq!(reply.as_deref(), Some(STORED));
        assert_eq!(context.values.get(&key).as_deref(), Some(&value));
    }

    #[tokio::test]
    async fn invalid_overwrite_is_rejected_and_original_value_is_preserved() {
        let original = Value::Blob(b"old".to_vec());
        let key = original.key();

        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id), FakeDns::new());

        // Valid first STORE.
        let body = encode_request(key, original.clone()).unwrap();
        let reply = handle(&context, 1, addr(), &body).await;

        assert_eq!(reply.as_deref(), Some(STORED));
        assert_eq!(context.values.get(&key).as_deref(), Some(&original));

        // Try to replace it with different content under the same key.
        let replacement = Value::Blob(b"new".to_vec());

        assert_ne!(key, replacement.key());

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
        let context = Context::new(my_id, recommended(my_id), FakeDns::new());

        let reply = handle(&context, 42, addr(), b"too short").await;

        assert!(reply.is_none());
        assert!(context.values.is_empty());
    }

    #[tokio::test]
    async fn store_rejects_value_when_key_does_not_match_hash() {
        let value = Value::Blob(b"hello".to_vec());

        // Deliberately incorrect key.
        let wrong_key: Key = [1u8; 32];

        assert_ne!(wrong_key, value.key());

        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id), FakeDns::new());

        let body = encode_request(wrong_key, value).unwrap();
        let reply = handle(&context, 1, addr(), &body).await;

        assert!(reply.is_none());
        assert!(context.values.is_empty());
    }

    #[tokio::test]
    async fn storing_the_same_value_twice_is_allowed() {
        let value = Value::Blob(b"hello".to_vec());
        let key = value.key();

        let my_id = [0u8; 32];
        let context = Context::new(my_id, recommended(my_id), FakeDns::new());

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

    use crate::dns_data::fake_dns::{FAKE_KEYS, FakeKey};
    use crate::value::signed_latest;

    fn context() -> std::sync::Arc<Context<crate::close_nodes::RecommendedCloseNodes, FakeDns>> {
        Context::new([0u8; 32], recommended([0u8; 32]), FakeDns::new())
    }

    /// A pointer for `pkg` on `owner`'s domain, signed by `signer`.
    fn latest(owner: &FakeKey, version: &str, signer: &FakeKey) -> Value {
        Value::Latest(signed_latest(
            owner.domain,
            "pkg",
            version,
            &signer.signing_key().unwrap(),
        ))
    }

    async fn store(
        context: &Context<crate::close_nodes::RecommendedCloseNodes, FakeDns>,
        value: &Value,
    ) -> bool {
        let body = encode_request(value.key(), value.clone()).unwrap();
        handle(context, 1, addr(), &body).await.as_deref() == Some(STORED)
    }

    #[tokio::test]
    async fn latest_signed_by_domain_owner_is_stored() {
        let context = context();
        let value = latest(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0]);

        assert!(store(&context, &value).await);
        assert_eq!(context.values.get(&value.key()).as_deref(), Some(&value));
    }

    #[tokio::test]
    async fn latest_signed_by_another_key_is_rejected() {
        let context = context();
        // a real key, just not the one in node0's DNS record
        let value = latest(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[1]);

        assert!(!store(&context, &value).await);
        assert!(context.values.is_empty());
    }

    #[tokio::test]
    async fn latest_for_domain_without_dns_key_is_rejected() {
        let context = context();
        let signer = FAKE_KEYS[0].signing_key().unwrap();
        let value = Value::Latest(signed_latest("nobody.d7024e.test", "pkg", "1.0.0", &signer));

        assert!(!store(&context, &value).await);
        assert!(context.values.is_empty());
    }

    #[tokio::test]
    async fn latest_changed_after_signing_is_rejected() {
        let context = context();
        let Value::Latest(mut pointer) = latest(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0]) else {
            unreachable!()
        };
        pointer.body.version_record_hash = [9; 32];
        let value = Value::Latest(pointer);

        assert!(!store(&context, &value).await);
        assert!(context.values.is_empty());
    }

    #[tokio::test]
    async fn latest_under_wrong_key_is_rejected() {
        let context = context();
        let value = latest(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0]);
        let body = encode_request([1u8; 32], value).unwrap();

        assert!(handle(&context, 1, addr(), &body).await.is_none());
        assert!(context.values.is_empty());
    }

    #[tokio::test]
    async fn newer_latest_replaces_older() {
        let context = context();
        let old = latest(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0]);
        let new = latest(&FAKE_KEYS[0], "1.1.0", &FAKE_KEYS[0]);

        assert!(store(&context, &old).await);
        assert!(store(&context, &new).await);
        assert_eq!(context.values.get(&new.key()).as_deref(), Some(&new));
    }

    #[tokio::test]
    async fn older_latest_does_not_replace_newer() {
        let context = context();
        let new = latest(&FAKE_KEYS[0], "1.10.0", &FAKE_KEYS[0]);
        let old = latest(&FAKE_KEYS[0], "1.9.0", &FAKE_KEYS[0]);

        assert!(store(&context, &new).await);
        assert!(!store(&context, &old).await, "a rollback must be refused");
        assert_eq!(context.values.get(&new.key()).as_deref(), Some(&new));
    }

    #[tokio::test]
    async fn same_version_with_other_contents_is_rejected() {
        let context = context();
        let first = latest(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0]);
        let Value::Latest(mut other) = first.clone() else {
            unreachable!()
        };
        other.body.version_record_hash = [7; 32];
        let other = Value::Latest(
            crate::package::sign(other.body, &FAKE_KEYS[0].signing_key().unwrap()).unwrap(),
        );

        assert!(store(&context, &first).await);
        assert!(!store(&context, &other).await);
        assert_eq!(context.values.get(&first.key()).as_deref(), Some(&first));
    }

    #[tokio::test]
    async fn same_latest_twice_is_allowed() {
        let context = context();
        let value = latest(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0]);

        assert!(store(&context, &value).await);
        assert!(
            store(&context, &value).await,
            "a republish must be accepted"
        );
    }
}
