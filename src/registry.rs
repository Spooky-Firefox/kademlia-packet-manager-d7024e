use crate::close_nodes::CloseNodes;
use crate::close_nodes::Key;
use crate::dns_data::DnsData;
use crate::hashing::key_for_value;
use crate::lookup;
use crate::package::{
    LATEST_POINTER_TAG, LatestPointer, Signed, VERSION_RECORD_TAG, Version, VersionRecord,
    record_key, sign,
};
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;
use crate::value::Value;
use crate::version_chain::{ChainError, load_chain_from_pointer};
use ed25519_dalek::SigningKey;

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PublishError {
    VersionNotNewer,
    PreviousNotFound,
    PreviousNotLatest,
    WrongPackageHistory,
    EncodingFailed,
    NoDnsKey,
    WrongSigningKey,
    InvalidExistingHistory(ChainError),
    BlobNotStored,
    RecordNotStored,
    PointerNotStored,
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for PublishError {}

#[derive(Debug)]
pub(crate) struct PreparedPublication {
    pub(crate) blob: Vec<u8>,
    pub(crate) record: Signed<VersionRecord>,
    pub(crate) pointer: Signed<LatestPointer>,
    pub(crate) proof: Vec<Signed<VersionRecord>>,
}
#[derive(Debug)]
pub(crate) struct PublishReceipt {
    pub(crate) blob_key: Key,
    pub(crate) record_key: Key,
    pub(crate) latest_key: Key,
    pub(crate) blob_accepted: usize,
    pub(crate) record_accepted: usize,
    pub(crate) latest_accepted: usize,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum InstallError {
    InvalidHistory(ChainError),
    VersionNotFound(Version),
    BlobUnavailable(Key),
}

impl std::fmt::Display for InstallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for InstallError {}

#[derive(Debug)]
pub(crate) struct InstalledPackage {
    pub(crate) version: Version,
    pub(crate) blob_key: Key,
    pub(crate) blob: Vec<u8>,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PublishedVersion {
    pub(crate) version: Version,
    pub(crate) blob_key: Key,
    pub(crate) is_latest: bool,
}
pub(crate) fn prepare_publication(
    domain: &str,
    package: &str,
    version: Version,
    blob: Vec<u8>,
    signer: &SigningKey,
    existing: &[Signed<VersionRecord>],
    previous: Option<Version>,
    force: bool,
) -> Result<PreparedPublication, PublishError> {
    // make sure we dont get antoher package history
    if existing
        .iter()
        .any(|record| record.body.domain != domain || record.body.package != package)
    {
        return Err(PublishError::WrongPackageHistory);
    }

    // find version record to use as predecessor, use latest by default
    let previous_index = match previous {
        Some(requested) => Some(
            existing
                .iter()
                .position(|record| record.body.version == requested)
                .ok_or(PublishError::PreviousNotFound)?,
        ),
        None if existing.is_empty() => None,
        None => Some(0),
    };

    // publishing must continue current history with a higher version
    if !force {
        if let Some(head) = existing.first() {
            if version <= head.body.version {
                return Err(PublishError::VersionNotNewer);
            }

            if previous_index != Some(0) {
                return Err(PublishError::PreviousNotLatest);
            }
        }
    }

    // calcualte prev hash, zeroes means first version
    let prev_hash: Key = match previous_index {
        Some(index) => record_key(&existing[index]).ok_or(PublishError::EncodingFailed)?,
        None => [0; 32],
    };

    // create and sign new version record
    let record = sign(
        VersionRecord {
            tag: VERSION_RECORD_TAG.to_string(),
            domain: domain.to_string(),
            package: package.to_string(),
            version,
            blob_hash: key_for_value(&blob),
            prev: prev_hash,
        },
        signer,
    )
    .ok_or(PublishError::EncodingFailed)?;

    // create and sign newest pointer
    let pointer = sign(
        LatestPointer {
            tag: LATEST_POINTER_TAG.to_string(),
            domain: domain.to_string(),
            package: package.to_string(),
            version,
            version_record_hash: record_key(&record).ok_or(PublishError::EncodingFailed)?,
        },
        signer,
    )
    .ok_or(PublishError::EncodingFailed)?;

    // build history proof, newest first
    let mut proof = vec![record.clone()];

    if let Some(index) = previous_index {
        proof.extend_from_slice(&existing[index..]);
    }

    Ok(PreparedPublication {
        blob,
        record,
        pointer,
        proof,
    })
}

pub(crate) async fn publish_package<T, U, A, D>(
    rpc: &Rpc<T, U, A>,
    dns: &D,
    domain: &str,
    package: &str,
    version: Version,
    blob: Vec<u8>,
    signer: &SigningKey,
    previous: Option<Version>,
    force: bool,
) -> Result<PublishReceipt, PublishError>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
    D: DnsData,
{
    // verify signing key belongs to domain
    let owner = dns.get_dns_vk(domain).await.ok_or(PublishError::NoDnsKey)?;

    if signer.verifying_key() != owner {
        return Err(PublishError::WrongSigningKey);
    }

    // get currently published latest pointer
    // None does not garauntee that there is no history and not a network error might want TODO: improve this
    let latest = lookup::get_latest(rpc, dns, domain, package).await;

    // load existing version history
    let existing = match latest {
        Some(pointer) => load_chain_from_pointer(rpc, dns, &pointer)
            .await
            .map_err(PublishError::InvalidExistingHistory)?,
        None => Vec::new(),
    };

    // create signed publication
    let prepared = prepare_publication(
        domain, package, version, blob, signer, &existing, previous, force,
    )?;

    // store blob
    let (blob_key, blob_accepted) =
        lookup::store_value_with_proof(rpc, Value::Blob(prepared.blob), &[]).await;

    if blob_accepted == 0 {
        return Err(PublishError::BlobNotStored);
    }

    // serialzie and store signed version record
    let record_bytes =
        bincode::serialize(&prepared.record).map_err(|_| PublishError::EncodingFailed)?;

    let (record_key, record_accepted) =
        lookup::store_value_with_proof(rpc, Value::Blob(record_bytes), &[]).await;

    if record_accepted == 0 {
        return Err(PublishError::RecordNotStored);
    }

    // store latest pointer, attaching the complete signed history
    let (latest_key, latest_accepted) =
        lookup::store_value_with_proof(rpc, Value::Latest(prepared.pointer), &prepared.proof).await;

    if latest_accepted == 0 {
        return Err(PublishError::PointerNotStored);
    }

    Ok(PublishReceipt {
        blob_key,
        record_key,
        latest_key,
        blob_accepted,
        record_accepted,
        latest_accepted,
    })
}

pub(crate) async fn install_package<T, U, A, D>(
    rpc: &Rpc<T, U, A>,
    dns: &D,
    domain: &str,
    package: &str,
    requested_version: Option<Version>,
) -> Result<InstalledPackage, InstallError>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
    D: DnsData,
{
    // get lataest pointer and verify its history
    let history = crate::version_chain::load_chain(rpc, dns, domain, package)
        .await
        .map_err(InstallError::InvalidHistory)?;

    // choose requested version, latest by default
    let record = match requested_version {
        Some(version) => history
            .iter()
            .find(|record| record.body.version == version)
            .ok_or(InstallError::VersionNotFound(version))?,

        None => history
            .first()
            .ok_or(InstallError::InvalidHistory(ChainError::EmptyChain))?,
    };

    // signed record says which package bytes to retrieve
    let blob_key = record.body.blob_hash;

    // lookup_value() only accepts blobs whose hash matches the requested key.
    let (_, blob) = lookup::lookup_value(rpc, blob_key)
        .await
        .ok_or(InstallError::BlobUnavailable(blob_key))?;

    Ok(InstalledPackage {
        version: record.body.version,
        blob_key,
        blob,
    })
}

/// List all verified versions of a package, newest first.
///
/// Version records are authenticated, but package blobs are not downloaded here.
pub(crate) async fn show_package<T, U, A, D>(
    rpc: &Rpc<T, U, A>,
    dns: &D,
    domain: &str,
    package: &str,
) -> Result<Vec<PublishedVersion>, ChainError>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
    D: DnsData,
{
    // Retrieve and verify the complete signed version history.
    let history = crate::version_chain::load_chain(rpc, dns, domain, package).await?;

    // Convert the signed records into information
    // suitable for displaying to a user.
    let versions = history
        .into_iter()
        .enumerate()
        .map(|(index, record)| PublishedVersion {
            version: record.body.version,
            blob_key: record.body.blob_hash,
            is_latest: index == 0,
        })
        .collect();

    Ok(versions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::version_chain::{ChainRelation, compare_chains, validate_chain_proof};

    #[tokio::test]
    async fn creates_valid_publication_history() {
        let signer = SigningKey::from_bytes(&[1; 32]);

        // Publish the first version.
        let first = prepare_publication(
            "example.com",
            "calculator",
            "1.0.0".parse().unwrap(),
            b"first version".to_vec(),
            &signer,
            &[],
            None,
            false,
        )
        .unwrap();

        assert_eq!(first.record.body.prev, [0; 32]);
        assert_eq!(first.proof.len(), 1);
        assert_eq!(first.record.body.blob_hash, key_for_value(&first.blob));

        // Publish a second version using the first
        // publication's history.
        let second = prepare_publication(
            "example.com",
            "calculator",
            "2.0.0".parse().unwrap(),
            b"second version".to_vec(),
            &signer,
            &first.proof,
            None,
            false,
        )
        .unwrap();

        assert_eq!(second.record.body.prev, record_key(&first.record).unwrap());

        // The proof must include both versions.
        assert_eq!(second.proof.len(), 2);

        // Verify the proof using the same validation
        // that our STORE handler uses.
        let validated =
            validate_chain_proof(&second.pointer, &signer.verifying_key(), &second.proof)
                .await
                .unwrap();

        assert_eq!(validated, second.proof);
    }

    #[test]
    fn rejects_invalid_update_unless_forced() {
        let signer = SigningKey::from_bytes(&[1; 32]);

        let first = prepare_publication(
            "example.com",
            "calculator",
            "1.0.0".parse().unwrap(),
            b"first".to_vec(),
            &signer,
            &[],
            None,
            false,
        )
        .unwrap();

        let second = prepare_publication(
            "example.com",
            "calculator",
            "2.0.0".parse().unwrap(),
            b"second".to_vec(),
            &signer,
            &first.proof,
            None,
            false,
        )
        .unwrap();

        // Attempt a rollback.
        let rollback = prepare_publication(
            "example.com",
            "calculator",
            "1.5.0".parse().unwrap(),
            b"rollback".to_vec(),
            &signer,
            &second.proof,
            None,
            false,
        );

        assert!(matches!(rollback, Err(PublishError::VersionNotNewer)));

        // Attempt to fork from 1.0.0 instead of 2.0.0.
        let fork = prepare_publication(
            "example.com",
            "calculator",
            "3.0.0".parse().unwrap(),
            b"fork".to_vec(),
            &signer,
            &second.proof,
            Some("1.0.0".parse().unwrap()),
            false,
        );

        assert!(matches!(fork, Err(PublishError::PreviousNotLatest)));

        // Force allows constructing the attempted fork.
        let forced = prepare_publication(
            "example.com",
            "calculator",
            "3.0.0".parse().unwrap(),
            b"fork".to_vec(),
            &signer,
            &second.proof,
            Some("1.0.0".parse().unwrap()),
            true,
        )
        .unwrap();

        // The two histories now form competing branches.
        assert_eq!(
            compare_chains(&second.proof, &forced.proof),
            Ok(ChainRelation::Fork)
        );
    }

    #[tokio::test]
    async fn publishes_package_to_network() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();
        let node = FakeNode::new(&network);

        let domain = FAKE_KEYS[0].domain;
        let signer = FAKE_KEYS[0].signing_key().unwrap();

        // Publish version 1.0.0.
        let first = publish_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            "1.0.0".parse().unwrap(),
            b"calculator version 1".to_vec(),
            &signer,
            None,
            false,
        )
        .await
        .unwrap();

        assert!(first.blob_accepted > 0);
        assert!(first.record_accepted > 0);
        assert!(first.latest_accepted > 0);

        // Publish version 2.0.0.
        let second = publish_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            "2.0.0".parse().unwrap(),
            b"calculator version 2".to_vec(),
            &signer,
            None,
            false,
        )
        .await
        .unwrap();

        assert!(second.latest_accepted > 0);

        // The package's latest key remains the same.
        assert_eq!(first.latest_key, second.latest_key);

        // But the actual package files have different hashes.
        assert_ne!(first.blob_key, second.blob_key);

        // Retrieve the newest pointer from Kademlia.
        let latest = lookup::get_latest(node.rpc(), node.dns().as_ref(), domain, "calculator")
            .await
            .unwrap();

        assert_eq!(latest.body.version, "2.0.0".parse().unwrap());

        // Verify we can walk the complete history.
        let history = load_chain_from_pointer(node.rpc(), node.dns().as_ref(), &latest)
            .await
            .unwrap();

        assert_eq!(history.len(), 2);
        assert_eq!(history[0].body.version, "2.0.0".parse().unwrap());
        assert_eq!(history[1].body.version, "1.0.0".parse().unwrap());
    }

    #[tokio::test]
    async fn rejects_publication_from_wrong_owner() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();
        let node = FakeNode::new(&network);

        let domain = FAKE_KEYS[0].domain;

        // This key belongs to a different domain.
        let imposter = FAKE_KEYS[1].signing_key().unwrap();

        let result = publish_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            "1.0.0".parse().unwrap(),
            b"malicious package".to_vec(),
            &imposter,
            None,
            false,
        )
        .await;

        assert!(matches!(result, Err(PublishError::WrongSigningKey)));

        // Ownership verification happens before any STORE.
        assert!(node.datastore_snapshot().is_empty());
    }

    #[tokio::test]
    async fn rollback_is_rejected_even_with_force() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();
        let node = FakeNode::new(&network);

        let domain = FAKE_KEYS[0].domain;
        let signer = FAKE_KEYS[0].signing_key().unwrap();

        // First, publish two legitimate versions.
        for version in ["1.0.0", "2.0.0"] {
            publish_package(
                node.rpc(),
                node.dns().as_ref(),
                domain,
                "calculator",
                version.parse().unwrap(),
                version.as_bytes().to_vec(),
                &signer,
                None,
                false,
            )
            .await
            .unwrap();
        }

        // Without --force, the publisher rejects the rollback.
        let normal = publish_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            "1.5.0".parse().unwrap(),
            b"rollback".to_vec(),
            &signer,
            None,
            false,
        )
        .await;

        assert!(matches!(normal, Err(PublishError::VersionNotNewer)));

        // With --force, we attempt the invalid update.
        let forced = publish_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            "1.5.0".parse().unwrap(),
            b"forced rollback".to_vec(),
            &signer,
            None,
            true,
        )
        .await;

        // The STORE handler must reject the latest pointer.
        assert!(matches!(forced, Err(PublishError::PointerNotStored)));

        // The accepted latest version must remain unchanged.
        let latest = lookup::get_latest(node.rpc(), node.dns().as_ref(), domain, "calculator")
            .await
            .unwrap();

        assert_eq!(latest.body.version, "2.0.0".parse().unwrap());
    }

    #[tokio::test]
    async fn fork_is_rejected_even_with_force() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();
        let node = FakeNode::new(&network);

        let domain = FAKE_KEYS[0].domain;
        let signer = FAKE_KEYS[0].signing_key().unwrap();

        // Establish the legitimate history:
        // 2.0.0 -> 1.0.0.
        for version in ["1.0.0", "2.0.0"] {
            publish_package(
                node.rpc(),
                node.dns().as_ref(),
                domain,
                "calculator",
                version.parse().unwrap(),
                version.as_bytes().to_vec(),
                &signer,
                None,
                false,
            )
            .await
            .unwrap();
        }

        // Try to create 3.0.0 based directly on 1.0.0.
        let normal = publish_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            "3.0.0".parse().unwrap(),
            b"forked package".to_vec(),
            &signer,
            Some("1.0.0".parse().unwrap()),
            false,
        )
        .await;

        assert!(matches!(normal, Err(PublishError::PreviousNotLatest)));

        // Force bypasses the publisher's local fork check.
        let forced = publish_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            "3.0.0".parse().unwrap(),
            b"forced fork".to_vec(),
            &signer,
            Some("1.0.0".parse().unwrap()),
            true,
        )
        .await;

        // The receiving STORE handler rejects the fork.
        assert!(matches!(forced, Err(PublishError::PointerNotStored)));

        // The legitimate 2.0.0 is still latest.
        let latest = lookup::get_latest(node.rpc(), node.dns().as_ref(), domain, "calculator")
            .await
            .unwrap();

        assert_eq!(latest.body.version, "2.0.0".parse().unwrap());
    }

    #[tokio::test]
    async fn package_can_be_published_and_updated_across_nodes() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();

        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);
        let c = FakeNode::new(&network);

        // Connect the network:
        // A <-> B <-> C
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        let domain = FAKE_KEYS[0].domain;
        let signer = FAKE_KEYS[0].signing_key().unwrap();

        // Node A publishes the first version.
        let first = publish_package(
            a.rpc(),
            a.dns().as_ref(),
            domain,
            "calculator",
            "1.0.0".parse().unwrap(),
            b"calculator version 1".to_vec(),
            &signer,
            None,
            false,
        )
        .await
        .unwrap();

        // More than just the publisher acknowledged STORE.
        assert!(first.latest_accepted >= 2);

        // Node C can discover that version.
        let latest_at_c = lookup::get_latest(c.rpc(), c.dns().as_ref(), domain, "calculator")
            .await
            .unwrap();

        assert_eq!(latest_at_c.body.version, "1.0.0".parse().unwrap());

        // Now Node C publishes the next version.
        let second = publish_package(
            c.rpc(),
            c.dns().as_ref(),
            domain,
            "calculator",
            "2.0.0".parse().unwrap(),
            b"calculator version 2".to_vec(),
            &signer,
            None,
            false,
        )
        .await
        .unwrap();

        assert!(second.latest_accepted >= 2);

        // Node B should discover the updated pointer.
        let latest_at_b = lookup::get_latest(b.rpc(), b.dns().as_ref(), domain, "calculator")
            .await
            .unwrap();

        assert_eq!(latest_at_b.body.version, "2.0.0".parse().unwrap());

        // Node B can verify the complete history.
        let history = load_chain_from_pointer(b.rpc(), b.dns().as_ref(), &latest_at_b)
            .await
            .unwrap();

        assert_eq!(history.len(), 2);

        assert_eq!(history[0].body.version, "2.0.0".parse().unwrap());

        assert_eq!(history[1].body.version, "1.0.0".parse().unwrap());

        // The newest package bytes are retrievable too.
        let (_, downloaded) = lookup::lookup_value(b.rpc(), history[0].body.blob_hash)
            .await
            .unwrap();

        assert_eq!(downloaded, b"calculator version 2");
    }

    #[tokio::test]
    async fn stale_node_catches_up_after_missing_update() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc::FindValue;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();
        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);

        let domain = FAKE_KEYS[0].domain;
        let signer = FAKE_KEYS[0].signing_key().unwrap();
        let package = "calculator";

        // 1. Node A publishes version 1.0.0.
        let first = publish_package(
            a.rpc(),
            a.dns().as_ref(),
            domain,
            package,
            "1.0.0".parse().unwrap(),
            b"version one".to_vec(),
            &signer,
            None,
            false,
        )
        .await
        .unwrap();

        // Retrieve the exact pointer and proof A published.
        let pointer_v1 = lookup::get_latest(a.rpc(), a.dns().as_ref(), domain, package)
            .await
            .unwrap();

        let proof_v1 = load_chain_from_pointer(a.rpc(), a.dns().as_ref(), &pointer_v1)
            .await
            .unwrap();

        assert_eq!(proof_v1.len(), 1);

        // 2. Give B version 1.0.0 directly.
        // B and A are not connected yet.
        assert!(
            b.rpc()
                .store_with_proof(
                    &b.contact(),
                    first.latest_key,
                    Value::Latest(pointer_v1.clone()),
                    proof_v1,
                )
                .await
        );

        // 3. A publishes 2.0.0 while B is disconnected.
        publish_package(
            a.rpc(),
            a.dns().as_ref(),
            domain,
            package,
            "2.0.0".parse().unwrap(),
            b"version two".to_vec(),
            &signer,
            None,
            false,
        )
        .await
        .unwrap();

        // Ask B directly, not through a DHT lookup.
        // B must still have the older pointer.
        assert_eq!(
            b.rpc().find_value(&b.contact(), first.latest_key).await,
            FindValue::Value(Value::Latest(pointer_v1.clone()))
        );

        // 4. Reconnect the two nodes.
        // B pings A so A learns B as a storage target.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        assert_eq!(a.rpc().ping(b.address()).await, Some(b.id()));
        // Clear cached lookup results from before the nodes connected.
        a.rpc().close_nodes().clear();
        b.rpc().close_nodes().clear();
        // 5. A publishes version 3.0.0.
        let third = publish_package(
            a.rpc(),
            a.dns().as_ref(),
            domain,
            package,
            "3.0.0".parse().unwrap(),
            b"version three".to_vec(),
            &signer,
            None,
            false,
        )
        .await
        .unwrap();

        // Both nodes should now acknowledge the latest pointer.
        assert!(
            third.latest_accepted >= 2,
            "the stale node did not accept version 3.0.0"
        );

        // 6. Check B's own datastore directly.
        let pointer_v3 = match b.rpc().find_value(&b.contact(), first.latest_key).await {
            FindValue::Value(Value::Latest(pointer)) => pointer,
            other => panic!("B has no updated latest pointer: {other:?}"),
        };

        assert_eq!(pointer_v3.body.version, "3.0.0".parse().unwrap());

        // 7. B must be able to verify all three versions.
        let history = load_chain_from_pointer(b.rpc(), b.dns().as_ref(), &pointer_v3)
            .await
            .unwrap();

        assert_eq!(history.len(), 3);

        assert_eq!(history[0].body.version, "3.0.0".parse().unwrap());

        assert_eq!(history[1].body.version, "2.0.0".parse().unwrap());

        assert_eq!(history[2].body.version, "1.0.0".parse().unwrap());

        // The history contains B's original accepted record.
        assert_eq!(
            record_key(&history[2]),
            Some(pointer_v1.body.version_record_hash)
        );
    }

    #[tokio::test]
    async fn installs_latest_and_older_version() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();
        let node = FakeNode::new(&network);

        let domain = FAKE_KEYS[0].domain;
        let signer = FAKE_KEYS[0].signing_key().unwrap();

        // Publish two versions of the same package.
        for (version, bytes) in [
            ("1.0.0", b"first version".as_slice()),
            ("2.0.0", b"second version".as_slice()),
        ] {
            publish_package(
                node.rpc(),
                node.dns().as_ref(),
                domain,
                "calculator",
                version.parse().unwrap(),
                bytes.to_vec(),
                &signer,
                None,
                false,
            )
            .await
            .unwrap();
        }

        // No version specified: install the latest.
        let latest = install_package(node.rpc(), node.dns().as_ref(), domain, "calculator", None)
            .await
            .unwrap();

        assert_eq!(latest.version, "2.0.0".parse().unwrap());
        assert_eq!(latest.blob, b"second version");
        assert_eq!(latest.blob_key, key_for_value(&latest.blob));

        // Explicit version: install the older release.
        let older = install_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            Some("1.0.0".parse().unwrap()),
        )
        .await
        .unwrap();

        assert_eq!(older.version, "1.0.0".parse().unwrap());
        assert_eq!(older.blob, b"first version");
        assert_eq!(older.blob_key, key_for_value(&older.blob));
    }

    #[tokio::test]
    async fn install_rejects_unknown_version() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();
        let node = FakeNode::new(&network);

        let domain = FAKE_KEYS[0].domain;
        let signer = FAKE_KEYS[0].signing_key().unwrap();

        publish_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            "1.0.0".parse().unwrap(),
            b"first version".to_vec(),
            &signer,
            None,
            false,
        )
        .await
        .unwrap();

        let missing: Version = "9.0.0".parse().unwrap();

        let result = install_package(
            node.rpc(),
            node.dns().as_ref(),
            domain,
            "calculator",
            Some(missing),
        )
        .await;

        assert!(matches!(
            result,
            Err(InstallError::VersionNotFound(v)) if v == missing
        ));
    }

    #[tokio::test]
    async fn install_fails_when_package_blob_is_missing() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();
        let node = FakeNode::new(&network);

        let domain = FAKE_KEYS[0].domain;
        let signer = FAKE_KEYS[0].signing_key().unwrap();

        // Construct a complete, valid publication.
        let prepared = prepare_publication(
            domain,
            "calculator",
            "1.0.0".parse().unwrap(),
            b"package contents".to_vec(),
            &signer,
            &[],
            None,
            false,
        )
        .unwrap();

        let missing_blob_key = key_for_value(&prepared.blob);

        // Store only the signed version record.
        let record_bytes = bincode::serialize(&prepared.record).unwrap();
        let version_key = record_key(&prepared.record).unwrap();

        assert!(
            node.rpc()
                .store(&node.contact(), version_key, Value::Blob(record_bytes),)
                .await
        );

        // Store the latest pointer with its valid history proof.
        let latest_value = Value::Latest(prepared.pointer);
        let latest_key = latest_value.key();

        assert!(
            node.rpc()
                .store_with_proof(&node.contact(), latest_key, latest_value, prepared.proof,)
                .await
        );

        // Deliberately never store prepared.blob.
        assert!(
            lookup::lookup_value(node.rpc(), missing_blob_key)
                .await
                .is_none()
        );

        // Installation must fail because the package file is missing.
        let result =
            install_package(node.rpc(), node.dns().as_ref(), domain, "calculator", None).await;

        assert!(matches!(
            result,
            Err(InstallError::BlobUnavailable(key))
                if key == missing_blob_key
        ));
    }

    #[tokio::test]
    async fn show_lists_verified_versions() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::node::FakeNode;
        use crate::rpc_transport::networked_debug_transport::Network;

        let network = Network::new();
        let node = FakeNode::new(&network);

        let domain = FAKE_KEYS[0].domain;
        let signer = FAKE_KEYS[0].signing_key().unwrap();

        // Publish two package versions.
        for (version, contents) in [
            ("1.0.0", b"first".as_slice()),
            ("2.0.0", b"second".as_slice()),
        ] {
            publish_package(
                node.rpc(),
                node.dns().as_ref(),
                domain,
                "calculator",
                version.parse().unwrap(),
                contents.to_vec(),
                &signer,
                None,
                false,
            )
            .await
            .unwrap();
        }

        let versions = show_package(node.rpc(), node.dns().as_ref(), domain, "calculator")
            .await
            .unwrap();

        assert_eq!(versions.len(), 2);

        // Newest version appears first.
        assert_eq!(versions[0].version, "2.0.0".parse().unwrap());
        assert!(versions[0].is_latest);
        assert_eq!(versions[0].blob_key, key_for_value(b"second"));

        // The older version is still listed.
        assert_eq!(versions[1].version, "1.0.0".parse().unwrap());
        assert!(!versions[1].is_latest);
        assert_eq!(versions[1].blob_key, key_for_value(b"first"));
    }
}

/// Publishing and installing across a network of many nodes, and the
/// publisher's own refusals.
#[cfg(test)]
mod network_tests {
    use super::*;
    use crate::dns_data::fake_dns::FAKE_KEYS;
    use crate::node::FakeNode;
    use crate::rpc_transport::networked_debug_transport::{Network, NetworkConfig};

    const PACKAGE: &str = "calculator";

    fn v(version: &str) -> Version {
        version.parse().unwrap()
    }

    /// `n` nodes, each joined through the first. A join that the network's
    /// loss defeats is tried again, as a user would.
    async fn network(n: usize, config: NetworkConfig) -> (Network, Vec<FakeNode>) {
        let network = Network::with_config(config);
        let nodes: Vec<FakeNode> = (0..n).map(|_| FakeNode::new(&network)).collect();
        for node in &nodes[1..] {
            let mut tries = 0;
            while crate::bootstrap::bootstrap(node.rpc(), nodes[0].address())
                .await
                .is_err()
            {
                tries += 1;
                assert!(tries < 5, "join failed {tries} times");
            }
        }
        (network, nodes)
    }

    async fn publish(
        node: &FakeNode,
        version: &str,
        blob: &[u8],
    ) -> Result<PublishReceipt, PublishError> {
        publish_package(
            node.rpc(),
            node.dns().as_ref(),
            FAKE_KEYS[0].domain,
            PACKAGE,
            v(version),
            blob.to_vec(),
            &FAKE_KEYS[0].signing_key().unwrap(),
            None,
            false,
        )
        .await
    }

    async fn install(
        node: &FakeNode,
        version: Option<&str>,
    ) -> Result<InstalledPackage, InstallError> {
        install_package(
            node.rpc(),
            node.dns().as_ref(),
            FAKE_KEYS[0].domain,
            PACKAGE,
            version.map(v),
        )
        .await
    }

    #[tokio::test]
    async fn every_node_of_a_network_installs_every_version() {
        let (_network, nodes) = network(25, NetworkConfig::default()).await;
        let blobs: Vec<Vec<u8>> = (1..=4).map(|i| vec![i as u8; 3000 * i]).collect();
        for (i, blob) in blobs.iter().enumerate() {
            let receipt = publish(&nodes[7], &format!("{}.0.0", i + 1), blob)
                .await
                .unwrap();
            assert_eq!(receipt.latest_accepted, crate::close_nodes::K);
        }

        for node in &nodes {
            let latest = install(node, None).await.unwrap();
            assert_eq!(latest.version, v("4.0.0"));
            assert_eq!(latest.blob, blobs[3]);
            for (i, blob) in blobs.iter().enumerate() {
                let pinned = install(node, Some(&format!("{}.0.0", i + 1)))
                    .await
                    .unwrap();
                assert_eq!(&pinned.blob, blob);
            }
        }
    }

    #[tokio::test]
    async fn updates_from_different_nodes_continue_one_history() {
        // The owner publishes each version from a different machine; every
        // publish must find the history the last one left.
        let (_network, nodes) = network(25, NetworkConfig::default()).await;
        for (i, node) in nodes.iter().take(5).enumerate() {
            publish(node, &format!("{}.0.0", i + 1), format!("v{i}").as_bytes())
                .await
                .unwrap();
        }
        let versions: Vec<Version> = show_package(
            nodes[20].rpc(),
            nodes[20].dns().as_ref(),
            FAKE_KEYS[0].domain,
            PACKAGE,
        )
        .await
        .unwrap()
        .into_iter()
        .map(|published| published.version)
        .collect();

        assert_eq!(
            versions,
            ["5.0.0", "4.0.0", "3.0.0", "2.0.0", "1.0.0"]
                .map(v)
                .to_vec()
        );
    }

    #[tokio::test]
    #[ignore = "about a minute of retries; run with cargo test -- --ignored"]
    async fn publish_and_install_survive_moderate_packet_loss() {
        // Every datagram is dropped with probability 0.3, publishing
        // included; retries have to carry each lookup through.
        let (_network, nodes) = network(
            12,
            NetworkConfig {
                loss: 0.3,
                ..NetworkConfig::default()
            },
        )
        .await;
        publish(&nodes[0], "1.0.0", b"one").await.unwrap();
        publish(&nodes[0], "2.0.0", b"two").await.unwrap();

        for node in nodes.iter().skip(1).step_by(3) {
            let installed = install(node, None).await.unwrap();
            assert_eq!(installed.version, v("2.0.0"));
            assert_eq!(installed.blob, b"two");
        }
    }

    #[tokio::test]
    async fn install_fails_cleanly_once_every_holder_is_gone() {
        let (network, nodes) = network(30, NetworkConfig::default()).await;
        publish(&nodes[0], "1.0.0", b"one").await.unwrap();

        let (holders, others): (Vec<&FakeNode>, Vec<&FakeNode>) = nodes
            .iter()
            .partition(|node| !node.datastore_snapshot().is_empty());
        assert!(!holders.is_empty() && !others.is_empty());
        for holder in &holders {
            network.unbind(holder.address());
        }

        // Nobody left has it: the install must say so, not return something.
        assert!(matches!(
            install(others[0], None).await,
            Err(InstallError::InvalidHistory(ChainError::NoLatestPointer))
        ));
    }

    #[tokio::test]
    async fn publishing_for_a_domain_without_a_dns_key_is_refused() {
        let (_network, nodes) = network(3, NetworkConfig::default()).await;
        let result = publish_package(
            nodes[0].rpc(),
            nodes[0].dns().as_ref(),
            "nobody.d7024e.test",
            PACKAGE,
            v("1.0.0"),
            b"x".to_vec(),
            &FAKE_KEYS[0].signing_key().unwrap(),
            None,
            false,
        )
        .await;

        assert!(matches!(result, Err(PublishError::NoDnsKey)));
        assert!(
            nodes
                .iter()
                .all(|node| node.datastore_snapshot().is_empty())
        );
    }

    #[test]
    fn publishing_on_top_of_another_packages_history_is_refused() {
        let signer = FAKE_KEYS[0].signing_key().unwrap();
        let other = prepare_publication(
            FAKE_KEYS[0].domain,
            "other",
            v("1.0.0"),
            b"o".to_vec(),
            &signer,
            &[],
            None,
            false,
        )
        .unwrap();

        let result = prepare_publication(
            FAKE_KEYS[0].domain,
            PACKAGE,
            v("2.0.0"),
            b"c".to_vec(),
            &signer,
            &other.proof,
            None,
            false,
        );

        assert!(matches!(result, Err(PublishError::WrongPackageHistory)));
    }

    #[test]
    fn naming_a_previous_version_that_does_not_exist_is_refused() {
        let signer = FAKE_KEYS[0].signing_key().unwrap();
        let first = prepare_publication(
            FAKE_KEYS[0].domain,
            PACKAGE,
            v("1.0.0"),
            b"1".to_vec(),
            &signer,
            &[],
            None,
            false,
        )
        .unwrap();

        let result = prepare_publication(
            FAKE_KEYS[0].domain,
            PACKAGE,
            v("2.0.0"),
            b"2".to_vec(),
            &signer,
            &first.proof,
            Some(v("1.5.0")),
            false,
        );

        assert!(matches!(result, Err(PublishError::PreviousNotFound)));
    }

    #[test]
    fn naming_the_latest_as_previous_is_the_normal_update() {
        let signer = FAKE_KEYS[0].signing_key().unwrap();
        let first = prepare_publication(
            FAKE_KEYS[0].domain,
            PACKAGE,
            v("1.0.0"),
            b"1".to_vec(),
            &signer,
            &[],
            None,
            false,
        )
        .unwrap();
        let implicit = prepare_publication(
            FAKE_KEYS[0].domain,
            PACKAGE,
            v("2.0.0"),
            b"2".to_vec(),
            &signer,
            &first.proof,
            None,
            false,
        )
        .unwrap();
        let explicit = prepare_publication(
            FAKE_KEYS[0].domain,
            PACKAGE,
            v("2.0.0"),
            b"2".to_vec(),
            &signer,
            &first.proof,
            Some(v("1.0.0")),
            false,
        )
        .unwrap();

        assert_eq!(implicit.proof, explicit.proof);
        assert_eq!(implicit.pointer, explicit.pointer);
    }
}
