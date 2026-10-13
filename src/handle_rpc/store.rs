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
use crate::package::{LatestPointer, Signed, VersionRecord, latest_is_valid, record_key};
use crate::value::Value;
use crate::version_chain::validate_chain_proof;
use dashmap::mapref::entry::Entry;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

pub const STORED: &[u8] = b"STORED";

#[derive(Debug, Serialize, Deserialize)]
struct StoreRequest {
    key: Key,
    value: Value,
    proof: Vec<Signed<VersionRecord>>,
}

/// Encode a STORE body: the pair to hold.
///
/// Who is asking is not in here — it travels ahead of the method tag, in the
/// framing every request carries.

pub fn encode_request(key: Key, value: Value) -> Option<Vec<u8>> {
    encode_request_with_proof(key, value, Vec::new())
}

pub(crate) fn encode_request_with_proof(
    key: Key,
    value: Value,
    proof: Vec<Signed<VersionRecord>>,
) -> Option<Vec<u8>> {
    let request = StoreRequest { key, value, proof };

    bincode::serialize(&request).ok()
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
    let StoreRequest { key, value, proof } = bincode::deserialize(body).ok()?;

    // Each value has exactly one key it may live under: its hash for a blob,
    // `latest_key(domain, package)` for a pointer.
    if key != value.key() {
        return None;
    }

    match &value {
        Value::Blob(_) => {
            // Blobs don't need version proofs.
            if !proof.is_empty() {
                return None;
            }

            // Prevent blobs from overwriting latest pointers.
            match context.values.entry(key) {
                Entry::Occupied(slot) if matches!(slot.get(), Value::Latest(_)) => {
                    return None;
                }

                mut entry => {
                    entry.insert(value);
                }
            }
        }

        Value::Latest(pointer) => {
            // find public key belonging to domain
            let owner = context.dns.get_dns_vk(&pointer.body.domain).await?;

            // verify pointer signature
            if !latest_is_valid(pointer, &owner) {
                return None;
            }

            // verify supplied history
            let verified_chain = validate_chain_proof(pointer, &owner, &proof).await.ok()?;

            // update storage only if proposed history continues previously accepted history
            match context.values.entry(key) {
                Entry::Vacant(slot) => {
                    // first publication
                    slot.insert(value);
                }

                Entry::Occupied(mut slot) => {
                    if let Value::Latest(old) = slot.get() {
                        if !continues_previous(old, pointer, &verified_chain) {
                            return None;
                        }
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
/* fn replaces(stored: &Value, new: &Signed<LatestPointer>) -> bool {
    match stored {
        Value::Latest(old) => new.body.version > old.body.version || new == old,
        // Anyone can store the text a latest key hashes as a blob, so a blob here
        // is a squatter. A pointer only reaches this point once the owner's
        // signature has been checked, so it wins.
        Value::Blob(_) => true, // prevents attacker from blocking owner
    }
} */
// replaces replaces() O_O meta
/// Checks whether a proposed latest pointer extends the previously accepted history.
///
/// Allows identical republishing or a newer version whose verified chain
/// contains the previously stored version record. Rejects rollbacks and forks.
///
/// Assumes `verified_chain` has already been validated.
fn continues_previous(
    old: &Signed<LatestPointer>,
    proposed: &Signed<LatestPointer>,
    verified_chain: &[Signed<VersionRecord>],
) -> bool {
    // Republishing exactly the same pointer is allowed.
    if old == proposed {
        return true;
    }

    // A new pointer must have a strictly higher version.
    if proposed.body.version <= old.body.version {
        return false;
    }

    // The new history must contain our previously accepted
    // version record, identified by its exact content hash.
    verified_chain.iter().any(|record| {
        record_key(record) == Some(old.body.version_record_hash)
            && record.body.version == old.body.version
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::recommended;
    use crate::dns_data::fake_dns::FakeDns;

    fn addr() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    #[test]
    fn new_latest_must_continue_previous_history() {
        use crate::package::{LATEST_POINTER_TAG, VERSION_RECORD_TAG, sign};
        use ed25519_dalek::SigningKey;

        let signer = SigningKey::from_bytes(&[1; 32]);

        // Create version 1.0.0.
        let first = sign(
            VersionRecord {
                tag: VERSION_RECORD_TAG.to_string(),
                domain: "node0.d7024e.test".to_string(),
                package: "pkg".to_string(),
                version: "1.0.0".parse().unwrap(),
                blob_hash: [7; 32],
                prev: [0; 32],
            },
            &signer,
        )
        .unwrap();

        let first_hash = record_key(&first).unwrap();

        let old = sign(
            LatestPointer {
                tag: LATEST_POINTER_TAG.to_string(),
                domain: first.body.domain.clone(),
                package: first.body.package.clone(),
                version: first.body.version,
                version_record_hash: first_hash,
            },
            &signer,
        )
        .unwrap();

        // Version 2.0.0 correctly points to 1.0.0.
        let second = sign(
            VersionRecord {
                version: "2.0.0".parse().unwrap(),
                prev: first_hash,
                ..first.body.clone()
            },
            &signer,
        )
        .unwrap();

        let newer = sign(
            LatestPointer {
                version: "2.0.0".parse().unwrap(),
                version_record_hash: record_key(&second).unwrap(),
                ..old.body.clone()
            },
            &signer,
        )
        .unwrap();

        let valid_chain = vec![second, first.clone()];

        assert!(continues_previous(&old, &newer, &valid_chain));

        // Repeating the exact same pointer is allowed.
        assert!(continues_previous(&old, &old, &valid_chain));

        // Rollback: 2.0.0 -> 1.0.0 is not allowed.
        assert!(!continues_previous(&newer, &old, &valid_chain));

        // A different branch starting at 3.0.0.
        let fork = sign(
            VersionRecord {
                version: "3.0.0".parse().unwrap(),
                blob_hash: [9; 32],
                prev: [0; 32],
                ..first.body.clone()
            },
            &signer,
        )
        .unwrap();

        let fork_pointer = sign(
            LatestPointer {
                version: "3.0.0".parse().unwrap(),
                version_record_hash: record_key(&fork).unwrap(),
                ..old.body.clone()
            },
            &signer,
        )
        .unwrap();

        assert!(!continues_previous(&old, &fork_pointer, &[fork],));
    }

    #[test]
    fn store_request_can_carry_version_proof() {
        use crate::package::{VERSION_RECORD_TAG, VersionRecord, sign};
        use ed25519_dalek::SigningKey;

        let signer = SigningKey::from_bytes(&[1; 32]);

        let record = sign(
            VersionRecord {
                tag: VERSION_RECORD_TAG.to_string(),
                domain: "node0.d7024e.test".to_string(),
                package: "calculator".to_string(),
                version: "1.0.0".parse().unwrap(),
                blob_hash: [7; 32],
                prev: [0; 32],
            },
            &signer,
        )
        .unwrap();

        let value = Value::Blob(b"hello".to_vec());
        let key = value.key();

        // Encode a STORE request with one version record.
        let encoded = encode_request_with_proof(key, value.clone(), vec![record.clone()]).unwrap();

        // Decode it, just like the receiving node would.
        let decoded: StoreRequest = bincode::deserialize(&encoded).unwrap();

        assert_eq!(decoded.key, key);
        assert_eq!(decoded.value, value);
        assert_eq!(decoded.proof, vec![record]);

        // Ordinary STORE requests carry no proof.
        let encoded = encode_request(key, value).unwrap();

        let decoded: StoreRequest = bincode::deserialize(&encoded).unwrap();

        assert!(decoded.proof.is_empty());
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
    use crate::package::{
        LATEST_POINTER_TAG, LatestPointer, Signed, VERSION_RECORD_TAG, VersionRecord, record_key,
        sign,
    };
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

    fn publication(
        owner: &FakeKey,
        version: &str,
        signer: &FakeKey,
        older: &[Signed<VersionRecord>],
        blob_hash: Key,
    ) -> (Value, Vec<Signed<VersionRecord>>) {
        let signing_key = signer.signing_key().unwrap();

        // Point to the most recent older record,
        // or all zeroes for the very first version.
        let prev = older.first().and_then(record_key).unwrap_or([0; 32]);

        let record = sign(
            VersionRecord {
                tag: VERSION_RECORD_TAG.to_string(),
                domain: owner.domain.to_string(),
                package: "pkg".to_string(),
                version: version.parse().unwrap(),
                blob_hash,
                prev,
            },
            &signing_key,
        )
        .unwrap();

        let pointer = sign(
            LatestPointer {
                tag: LATEST_POINTER_TAG.to_string(),
                domain: owner.domain.to_string(),
                package: "pkg".to_string(),
                version: version.parse().unwrap(),
                version_record_hash: record_key(&record).unwrap(),
            },
            &signing_key,
        )
        .unwrap();

        // Full history, newest first.
        let mut proof = vec![record];
        proof.extend_from_slice(older);

        (Value::Latest(pointer), proof)
    }

    async fn store(
        context: &Context<crate::close_nodes::RecommendedCloseNodes, FakeDns>,
        value: &Value,
    ) -> bool {
        let body = encode_request(value.key(), value.clone()).unwrap();
        handle(context, 1, addr(), &body).await.as_deref() == Some(STORED)
    }

    async fn store_proven(
        context: &Context<crate::close_nodes::RecommendedCloseNodes, FakeDns>,
        value: &Value,
        proof: &[Signed<VersionRecord>],
    ) -> bool {
        let body = encode_request_with_proof(value.key(), value.clone(), proof.to_vec()).unwrap();

        handle(context, 1, addr(), &body).await.as_deref() == Some(STORED)
    }

    #[tokio::test]
    async fn latest_signed_by_domain_owner_is_stored() {
        let context = context();

        let (value, proof) = publication(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0], &[], [7; 32]);

        assert!(store_proven(&context, &value, &proof).await);
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

        let (old, old_proof) = publication(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0], &[], [7; 32]);

        let (new, new_proof) =
            publication(&FAKE_KEYS[0], "1.1.0", &FAKE_KEYS[0], &old_proof, [8; 32]);

        assert!(store_proven(&context, &old, &old_proof).await);
        assert!(store_proven(&context, &new, &new_proof).await);
        assert_eq!(context.values.get(&new.key()).as_deref(), Some(&new));
    }

    #[tokio::test]
    async fn older_latest_does_not_replace_newer() {
        let context = context();

        let (old, old_proof) = publication(&FAKE_KEYS[0], "1.9.0", &FAKE_KEYS[0], &[], [7; 32]);

        let (new, new_proof) =
            publication(&FAKE_KEYS[0], "1.10.0", &FAKE_KEYS[0], &old_proof, [8; 32]);

        assert!(store_proven(&context, &new, &new_proof).await);
        assert!(
            !store_proven(&context, &old, &old_proof).await,
            "a rollback must be refused"
        );
        assert_eq!(context.values.get(&new.key()).as_deref(), Some(&new));
    }

    #[tokio::test]
    async fn same_version_with_other_contents_is_rejected() {
        let context = context();

        let (first, first_proof) = publication(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0], &[], [7; 32]);

        // Same version number, but different package content.
        let (other, other_proof) = publication(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0], &[], [9; 32]);

        assert_ne!(first, other);
        assert!(store_proven(&context, &first, &first_proof).await);
        assert!(!store_proven(&context, &other, &other_proof).await);
        assert_eq!(context.values.get(&first.key()).as_deref(), Some(&first));
    }

    #[tokio::test]
    async fn same_latest_twice_is_allowed() {
        let context = context();

        let (value, proof) = publication(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[0], &[], [7; 32]);

        assert!(store_proven(&context, &value, &proof).await);
        assert!(
            store_proven(&context, &value, &proof).await,
            "a republish must be accepted"
        );
    }

    /// A blob of the public text a latest key hashes, so it lands on that key.
    fn squatting_blob(owner: &FakeKey) -> Value {
        Value::Blob(format!("{}:pkg:latest", owner.domain).into_bytes())
    }

    #[tokio::test]
    async fn blob_does_not_replace_latest() {
        let context = context();
        let owner = &FAKE_KEYS[0];

        // Create a genuine signed latest pointer and its proof.
        let (latest, proof) = publication(owner, "1.0.0", owner, &[], [7; 32]);

        // A blob whose hash equals the latest pointer's key.
        let squatter = squatting_blob(owner);

        assert_eq!(latest.key(), squatter.key());

        // Legitimate publication succeeds.
        assert!(store_proven(&context, &latest, &proof).await);

        // The attacker must not overwrite it with a blob.
        assert!(!store(&context, &squatter).await);

        // The original latest pointer remains unchanged.
        assert_eq!(context.values.get(&latest.key()).as_deref(), Some(&latest));
    }

    #[tokio::test]
    async fn latest_replaces_squatting_blob() {
        let context = context();
        let owner = &FAKE_KEYS[0];

        let squatter = squatting_blob(owner);

        let (latest, proof) = publication(owner, "1.0.0", owner, &[], [7; 32]);

        assert_eq!(latest.key(), squatter.key());

        // Attacker occupies the latest pointer's key first.
        assert!(store(&context, &squatter).await);

        // Legitimate publication must still be accepted.
        assert!(store_proven(&context, &latest, &proof).await);

        // The legitimate pointer replaces the squatting blob.
        assert_eq!(context.values.get(&latest.key()).as_deref(), Some(&latest));
    }

    #[tokio::test]
    async fn forged_latest_does_not_replace_squatting_blob() {
        let context = context();
        let blob = squatting_blob(&FAKE_KEYS[0]);
        let forged = latest(&FAKE_KEYS[0], "1.0.0", &FAKE_KEYS[1]);

        assert!(store(&context, &blob).await);
        assert!(!store(&context, &forged).await);
        assert_eq!(context.values.get(&blob.key()).as_deref(), Some(&blob));
    }
}

/// What a node that wants to plant or replace a package might send, end to
/// end through the STORE handler.
#[cfg(test)]
mod attack_tests {
    use super::*;
    use crate::close_nodes::{RecommendedCloseNodes, recommended};
    use crate::dns_data::fake_dns::{FAKE_KEYS, FakeDns};
    use crate::package::{LATEST_POINTER_TAG, VERSION_RECORD_TAG, latest_key, sign};
    use ed25519_dalek::SigningKey;
    use std::sync::Arc;

    type Ctx = Arc<Context<RecommendedCloseNodes, FakeDns>>;

    fn context() -> Ctx {
        Context::new([0u8; 32], recommended([0u8; 32]), FakeDns::new())
    }

    fn from() -> SocketAddr {
        "127.0.0.1:9000".parse().unwrap()
    }

    fn domain() -> &'static str {
        FAKE_KEYS[0].domain
    }

    fn owner() -> SigningKey {
        FAKE_KEYS[0].signing_key().unwrap()
    }

    /// A publication of `version` continuing `older` (newest first), signed
    /// by `signer`: the pointer and its full proof.
    fn publish(
        version: &str,
        older: &[Signed<VersionRecord>],
        signer: &SigningKey,
        package: &str,
    ) -> (Signed<LatestPointer>, Vec<Signed<VersionRecord>>) {
        let record = sign(
            VersionRecord {
                tag: VERSION_RECORD_TAG.to_string(),
                domain: domain().to_string(),
                package: package.to_string(),
                version: version.parse().unwrap(),
                blob_hash: crate::hashing::key_for_value(version.as_bytes()),
                prev: older.first().and_then(record_key).unwrap_or([0; 32]),
            },
            signer,
        )
        .unwrap();
        let pointer = sign(
            LatestPointer {
                tag: LATEST_POINTER_TAG.to_string(),
                domain: domain().to_string(),
                package: package.to_string(),
                version: version.parse().unwrap(),
                version_record_hash: record_key(&record).unwrap(),
            },
            signer,
        )
        .unwrap();
        let mut proof = vec![record];
        proof.extend_from_slice(older);
        (pointer, proof)
    }

    async fn send(
        context: &Ctx,
        key: Key,
        value: Value,
        proof: Vec<Signed<VersionRecord>>,
    ) -> bool {
        let body = encode_request_with_proof(key, value, proof).unwrap();
        handle(context, 1, from(), &body).await.as_deref() == Some(STORED)
    }

    async fn store_pointer(
        context: &Ctx,
        pointer: &Signed<LatestPointer>,
        proof: &[Signed<VersionRecord>],
    ) -> bool {
        let value = Value::Latest(pointer.clone());
        send(context, value.key(), value, proof.to_vec()).await
    }

    fn stored_version(context: &Ctx, package: &str) -> Option<String> {
        match context
            .values
            .get(&latest_key(domain(), package))
            .as_deref()
        {
            Some(Value::Latest(pointer)) => Some(pointer.body.version.to_string()),
            _ => None,
        }
    }

    #[tokio::test]
    async fn first_pointer_without_its_proof_is_rejected() {
        let context = context();
        let (pointer, _) = publish("1.0.0", &[], &owner(), "pkg");

        assert!(!store_pointer(&context, &pointer, &[]).await);
        assert_eq!(stored_version(&context, "pkg"), None);
    }

    #[tokio::test]
    async fn blob_carrying_a_proof_is_rejected() {
        let context = context();
        let (_, proof) = publish("1.0.0", &[], &owner(), "pkg");
        let blob = Value::Blob(b"some package".to_vec());

        assert!(!send(&context, blob.key(), blob.clone(), proof).await);
        assert!(context.values.get(&blob.key()).is_none());
    }

    #[tokio::test]
    async fn pointer_under_another_packages_key_is_rejected() {
        let context = context();
        let (pointer, proof) = publish("1.0.0", &[], &owner(), "pkg");

        // Valid pointer for `pkg`, sent for the key of `victim`.
        let victim = latest_key(domain(), "victim");
        assert!(!send(&context, victim, Value::Latest(pointer), proof).await);
        assert!(context.values.get(&victim).is_none());
    }

    #[tokio::test]
    async fn proof_of_another_package_does_not_vouch_for_this_one() {
        let context = context();
        let (pointer, _) = publish("1.0.0", &[], &owner(), "pkg");
        let (_, other_proof) = publish("1.0.0", &[], &owner(), "other");

        assert!(!store_pointer(&context, &pointer, &other_proof).await);
    }

    #[tokio::test]
    async fn higher_version_on_a_fresh_history_does_not_replace_the_stored_one() {
        let context = context();
        let (v1, proof1) = publish("1.0.0", &[], &owner(), "pkg");
        assert!(store_pointer(&context, &v1, &proof1).await);

        // Validly signed by the owner, higher, but not built on 1.0.0: a
        // leaked key, or a second publisher with a stale view, cannot rewrite
        // the history nodes have already accepted.
        let (fork, fork_proof) = publish("9.0.0", &[], &owner(), "pkg");
        assert!(!store_pointer(&context, &fork, &fork_proof).await);
        assert_eq!(stored_version(&context, "pkg").as_deref(), Some("1.0.0"));
    }

    #[tokio::test]
    async fn every_step_of_a_long_history_is_accepted_in_order() {
        let context = context();
        let mut proof: Vec<Signed<VersionRecord>> = Vec::new();
        for major in 1..=30 {
            let (pointer, next) = publish(&format!("{major}.0.0"), &proof, &owner(), "pkg");
            assert!(
                store_pointer(&context, &pointer, &next).await,
                "version {major}"
            );
            proof = next;
        }
        assert_eq!(stored_version(&context, "pkg").as_deref(), Some("30.0.0"));
    }

    #[tokio::test]
    async fn skipping_ahead_is_fine_but_going_back_is_not() {
        let context = context();
        let (v1, p1) = publish("1.0.0", &[], &owner(), "pkg");
        let (v2, p2) = publish("2.0.0", &p1, &owner(), "pkg");
        let (v3, p3) = publish("3.0.0", &p2, &owner(), "pkg");

        // A node that missed 2.0.0 accepts 3.0.0 straight after 1.0.0: the
        // proof shows 1.0.0 in its history.
        assert!(store_pointer(&context, &v1, &p1).await);
        assert!(store_pointer(&context, &v3, &p3).await);
        // An older, valid pointer arriving late is turned away.
        assert!(!store_pointer(&context, &v2, &p2).await);
        assert_eq!(stored_version(&context, "pkg").as_deref(), Some("3.0.0"));
        // Republishing the current one is still accepted.
        assert!(store_pointer(&context, &v3, &p3).await);
    }

    #[tokio::test]
    async fn squatted_latest_key_is_taken_back_by_the_owner_but_not_by_a_forger() {
        let context = context();
        // Anyone can store the bytes that hash to a package's latest key.
        let squat_bytes = format!("{}:pkg:latest", domain()).into_bytes();
        let squat = Value::Blob(squat_bytes);
        assert_eq!(squat.key(), latest_key(domain(), "pkg"));
        assert!(send(&context, squat.key(), squat, Vec::new()).await);

        let (forged, forged_proof) =
            publish("1.0.0", &[], &FAKE_KEYS[1].signing_key().unwrap(), "pkg");
        assert!(!store_pointer(&context, &forged, &forged_proof).await);

        let (real, proof) = publish("1.0.0", &[], &owner(), "pkg");
        assert!(store_pointer(&context, &real, &proof).await);
        assert_eq!(stored_version(&context, "pkg").as_deref(), Some("1.0.0"));
    }

    #[tokio::test]
    async fn garbage_bodies_are_ignored() {
        // Truncations of a valid request, and random bytes: never stored,
        // never a panic.
        use rand::{RngExt, SeedableRng};
        let context = context();
        let (pointer, proof) = publish("1.0.0", &[], &owner(), "pkg");
        let value = Value::Latest(pointer);
        let body = encode_request_with_proof(value.key(), value, proof).unwrap();

        for len in 0..body.len() {
            assert!(
                handle(&context, 1, from(), &body[..len]).await.is_none(),
                "prefix {len}"
            );
        }
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        for _ in 0..2000 {
            let len = rng.random_range(0..512);
            let junk: Vec<u8> = (0..len).map(|_| rng.random()).collect();
            let _ = handle(&context, 1, from(), &junk).await;
        }
        assert!(context.values.get(&latest_key(domain(), "pkg")).is_none());
    }
}
