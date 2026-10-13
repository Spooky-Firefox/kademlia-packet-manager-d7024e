use crate::close_nodes::{CloseNodes, Key};
use crate::dns_data::DnsData;
use crate::hashing::key_for_value;
use crate::lookup;
use crate::package::{
    LatestPointer, Signed, VERSION_RECORD_TAG, Version, VersionRecord, latest_is_valid, record_key,
};
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;

use ed25519_dalek::VerifyingKey;
use std::collections::{HashMap, HashSet};
use std::future::Future;

const END_OF_CHAIN: Key = [0; 32];
const MAX_CHAIN_LENGTH: usize = 10_000;

#[derive(Debug, PartialEq, Eq)]
pub enum ChainError {
    NoDnsKey,
    NoLatestPointer,
    InvalidLatestPointer,
    MissingRecord(Key),
    HashMismatch(Key),
    InvalidEncoding(Key),
    InvalidRecord(Key),
    HeadVersionMismatch,
    NonDecreasingVersion,
    Cycle(Key),
    TooManyVersions,
    EmptyChain,
    DifferentPackage,
    UnexpectedRecords,
}

impl std::fmt::Display for ChainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for ChainError {}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainRelation {
    Same,
    Extends,
    Rollback,
    Fork,
}

pub fn compare_chains(
    stored: &[Signed<VersionRecord>],
    proposed: &[Signed<VersionRecord>],
) -> Result<ChainRelation, ChainError> {
    // both histories must contain records
    if stored.is_empty() || proposed.is_empty() {
        return Err(ChainError::EmptyChain);
    }

    // both histories must be in same package/domain
    let domain = stored[0].body.domain.as_str();
    let package = stored[0].body.package.as_str();

    if stored.iter().chain(proposed.iter()).any(|record| {
        record.body.domain.as_str() != domain || record.body.package.as_str() != package
    }) {
        return Err(ChainError::DifferentPackage);
    }

    // same history
    if stored == proposed {
        return Ok(ChainRelation::Same);
    }

    // history contains all stored records and continues with new ones
    if proposed.ends_with(stored) {
        return Ok(ChainRelation::Extends);
    }

    // history is older part of current history
    if stored.ends_with(proposed) {
        return Ok(ChainRelation::Rollback);
    }

    // neither chain continues other
    Ok(ChainRelation::Fork)
}

pub async fn load_chain<T, U, A, D>(
    rpc: &Rpc<T, U, A>,
    dns: &D,
    domain: &str,
    package: &str,
) -> Result<Vec<Signed<VersionRecord>>, ChainError>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
    D: DnsData,
{
    // 1. Find the domain owner's public key.
    let owner = dns.get_dns_vk(domain).await.ok_or(ChainError::NoDnsKey)?;

    // 2. Find the latest version pointer.
    let latest = lookup::get_latest(rpc, dns, domain, package)
        .await
        .ok_or(ChainError::NoLatestPointer)?;

    // 3. Follow and verify the history.
    walk_chain(&latest, &owner, domain, package, |key| async move {
        lookup::lookup_value(rpc, key).await.map(|(_, bytes)| bytes)
    })
    .await
}

/// Load and verify the history of a specific latest pointer.
///
/// Unlike load_chain(), this does not search for a newer pointer.
/// It verifies the exact pointer supplied by the caller.
pub async fn load_chain_from_pointer<T, U, A, D>(
    rpc: &Rpc<T, U, A>,
    dns: &D,
    pointer: &Signed<LatestPointer>,
) -> Result<Vec<Signed<VersionRecord>>, ChainError>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
    D: DnsData,
{
    let owner = dns
        .get_dns_vk(&pointer.body.domain)
        .await
        .ok_or(ChainError::NoDnsKey)?;

    walk_chain(
        pointer,
        &owner,
        &pointer.body.domain,
        &pointer.body.package,
        |key| async move { lookup::lookup_value(rpc, key).await.map(|(_, bytes)| bytes) },
    )
    .await
}

async fn walk_chain<F, Fut>(
    latest: &Signed<LatestPointer>,
    owner: &VerifyingKey,
    domain: &str,
    package: &str,
    mut fetch: F, // function that retrieves keys, lookup_value() in actual kademlia, directly from in-memory HashMap in tests
) -> Result<Vec<Signed<VersionRecord>>, ChainError>
where
    F: FnMut(Key) -> Fut,
    Fut: Future<Output = Option<Vec<u8>>>,
{
    // check latest pointer first
    if latest.body.domain != domain
        || latest.body.package != package
        || !latest_is_valid(latest, owner)
        || latest.body.version_record_hash == END_OF_CHAIN
    {
        return Err(ChainError::InvalidLatestPointer);
    }

    // start at newest version record
    let mut current = latest.body.version_record_hash;

    // remember visited record hashes
    let mut seen = HashSet::<Key>::new();

    // remember previous version
    let mut newer_version: Option<Version> = None;

    // store succesfully verified records (in reverse order)
    let mut chain = Vec::new();

    loop {
        if chain.len() == MAX_CHAIN_LENGTH {
            return Err(ChainError::TooManyVersions);
        }

        // visitng same record twice means cycle
        if !seen.insert(current) {
            return Err(ChainError::Cycle(current));
        }

        // get current version record
        let bytes = fetch(current)
            .await
            .ok_or(ChainError::MissingRecord(current))?;

        // match bytes to hash
        if key_for_value(&bytes) != current {
            return Err(ChainError::HashMismatch(current));
        }

        // decode record
        let record: Signed<VersionRecord> =
            bincode::deserialize(&bytes).map_err(|_| ChainError::InvalidEncoding(current))?;

        // verify record and owner signature and check that it matches the expected domain/package
        if record_key(&record) != Some(current)
            || record.body.tag != VERSION_RECORD_TAG
            || record.body.domain != domain
            || record.body.package != package
            || !record.verify(owner)
        {
            return Err(ChainError::InvalidRecord(current));
        }

        // first record match latest pointy pointer
        // older records must strictly decrease
        match newer_version {
            None if record.body.version != latest.body.version => {
                return Err(ChainError::HeadVersionMismatch);
            }

            Some(newer) if record.body.version >= newer => {
                return Err(ChainError::NonDecreasingVersion);
            }

            _ => {}
        }

        // save next record hash before moving ownership
        let previous = record.body.prev;

        newer_version = Some(record.body.version);
        chain.push(record);

        // reach first version, end of chain
        if previous == END_OF_CHAIN {
            return Ok(chain);
        }

        // continue with preceding version
        current = previous;
    }
}

/// Validate a complete version history supplied with a STORE request.
///
/// Returns the verified history, newest first.
/// The caller must obtain `owner` from the pointer's domain DNS record.
pub async fn validate_chain_proof(
    latest: &Signed<LatestPointer>,
    owner: &VerifyingKey,
    proof: &[Signed<VersionRecord>],
) -> Result<Vec<Signed<VersionRecord>>, ChainError> {
    // i aint reading allat
    if proof.len() > MAX_CHAIN_LENGTH {
        return Err(ChainError::TooManyVersions);
    }

    // Put the supplied records into a temporary store,
    // indexed by their content hashes.
    let mut records = HashMap::<Key, Vec<u8>>::new();

    for record in proof {
        let key = record_key(record).ok_or(ChainError::InvalidEncoding([0; 32]))?;

        let bytes = bincode::serialize(record).map_err(|_| ChainError::InvalidEncoding(key))?;

        // Reject duplicate records.
        if records.insert(key, bytes).is_some() {
            return Err(ChainError::UnexpectedRecords);
        }
    }

    // Reuse chain walker
    let validated = walk_chain(
        latest,
        owner,
        &latest.body.domain,
        &latest.body.package,
        |key| std::future::ready(records.get(&key).cloned()),
    )
    .await?;

    // Every supplied record must belong to this history.
    if validated.len() != proof.len() {
        return Err(ChainError::UnexpectedRecords);
    }

    Ok(validated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package::{LATEST_POINTER_TAG, sign};
    use ed25519_dalek::SigningKey;
    use std::collections::HashMap;

    const DOMAIN: &str = "node0.d7024e.test";
    const PACKAGE: &str = "calculator";

    fn create_record(signer: &SigningKey, version: &str, prev: Key) -> Signed<VersionRecord> {
        sign(
            VersionRecord {
                tag: VERSION_RECORD_TAG.to_string(),
                domain: DOMAIN.to_string(),
                package: PACKAGE.to_string(),
                version: version.parse().unwrap(),
                blob_hash: [7; 32],
                prev,
            },
            signer,
        )
        .unwrap()
    }

    fn add_record(store: &mut HashMap<Key, Vec<u8>>, record: &Signed<VersionRecord>) -> Key {
        let bytes = bincode::serialize(record).unwrap();
        let key = key_for_value(&bytes);
        store.insert(key, bytes);
        key
    }

    fn create_latest(signer: &SigningKey, version: &str, head: Key) -> Signed<LatestPointer> {
        sign(
            LatestPointer {
                tag: LATEST_POINTER_TAG.to_string(),
                domain: DOMAIN.to_string(),
                package: PACKAGE.to_string(),
                version: version.parse().unwrap(),
                version_record_hash: head,
            },
            signer,
        )
        .unwrap()
    }

    #[tokio::test]
    async fn valid_version_chain() {
        let signer = SigningKey::from_bytes(&[1; 32]);
        let mut store = HashMap::new();

        // First version has no predecessor.
        let first = create_record(&signer, "1.0.0", END_OF_CHAIN);
        let first_key = add_record(&mut store, &first);

        // Second version points to the first.
        let second = create_record(&signer, "1.1.0", first_key);
        let second_key = add_record(&mut store, &second);

        let pointer = create_latest(&signer, "1.1.0", second_key);

        let result = walk_chain(&pointer, &signer.verifying_key(), DOMAIN, PACKAGE, |key| {
            std::future::ready(store.get(&key).cloned())
        })
        .await
        .unwrap();

        assert_eq!(result, vec![second, first]);
    }

    #[tokio::test]
    async fn rejects_missing_record() {
        let signer = SigningKey::from_bytes(&[1; 32]);
        let mut store = HashMap::new();

        // This record points to a nonexistent predecessor.
        let record = create_record(&signer, "2.0.0", [9; 32]);
        let head = add_record(&mut store, &record);

        let pointer = create_latest(&signer, "2.0.0", head);

        let result = walk_chain(&pointer, &signer.verifying_key(), DOMAIN, PACKAGE, |key| {
            std::future::ready(store.get(&key).cloned())
        })
        .await;

        assert_eq!(result, Err(ChainError::MissingRecord([9; 32])));
    }

    #[test]
    fn compares_version_histories() {
        let signer = SigningKey::from_bytes(&[1; 32]);

        let first = create_record(&signer, "1.0.0", END_OF_CHAIN);

        let first_key = record_key(&first).unwrap();

        let second = create_record(&signer, "1.1.0", first_key);

        let second_key = record_key(&second).unwrap();

        let third = create_record(&signer, "2.0.0", second_key);

        // Existing history.
        let stored = vec![second.clone(), first.clone()];

        // A valid newer history.
        let extended = vec![third, second.clone(), first.clone()];

        assert_eq!(compare_chains(&stored, &stored), Ok(ChainRelation::Same));

        assert_eq!(
            compare_chains(&stored, &extended),
            Ok(ChainRelation::Extends)
        );

        assert_eq!(
            compare_chains(&extended, &stored),
            Ok(ChainRelation::Rollback)
        );

        // A competing branch skips version 1.1.0.
        let forked = create_record(&signer, "2.0.0", first_key);

        let fork = vec![forked, first.clone()];

        assert_eq!(compare_chains(&stored, &fork), Ok(ChainRelation::Fork));

        // Same version number, but different package bytes.
        let mut different_body = second.body.clone();
        different_body.blob_hash = [9; 32];

        let conflicting = sign(different_body, &signer).unwrap();

        let same_version_fork = vec![conflicting, first];

        assert_eq!(
            compare_chains(&stored, &same_version_fork),
            Ok(ChainRelation::Fork)
        );
    }

    #[tokio::test]
    async fn validates_store_chain_proof() {
        let signer = SigningKey::from_bytes(&[1; 32]);

        // Build the first version.
        let first = create_record(&signer, "1.0.0", END_OF_CHAIN);

        let first_key = record_key(&first).unwrap();

        // Build the second version.
        let second = create_record(&signer, "2.0.0", first_key);

        let second_key = record_key(&second).unwrap();

        // Latest points at version 2.0.0.
        let pointer = create_latest(&signer, "2.0.0", second_key);

        // Complete proof: should work.
        let complete = vec![second.clone(), first.clone()];

        let result = validate_chain_proof(&pointer, &signer.verifying_key(), &complete).await;

        assert_eq!(result, Ok(complete.clone()));

        // Missing first version: should fail.
        let incomplete = vec![second];

        let result = validate_chain_proof(&pointer, &signer.verifying_key(), &incomplete).await;

        assert_eq!(result, Err(ChainError::MissingRecord(first_key)));

        // Extra unrelated record: should fail.
        let extra = create_record(&signer, "9.0.0", END_OF_CHAIN);

        let mut with_extra = complete;
        with_extra.push(extra);

        let result = validate_chain_proof(&pointer, &signer.verifying_key(), &with_extra).await;

        assert_eq!(result, Err(ChainError::UnexpectedRecords));
    }
}
