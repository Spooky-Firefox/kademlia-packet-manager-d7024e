//! What the DHT stores: a typed value, so STORE and FIND_VALUE know which
//! check applies to it.
//!
//! - [`Value::Blob`] is content-addressed: stored under its own hash, so the
//!   key checks the bytes. Package blobs and signed
//!   [`VersionRecord`](crate::package::VersionRecord)s are both blobs, because
//!   [`record_key`](crate::package::record_key) is the hash of the record.
//! - [`Value::Latest`] is a package's [`LatestPointer`], stored under
//!   [`latest_key`] and replaced on every publish. The key cannot check it, so
//!   it is checked against the owner's key from DNS instead.

use crate::close_nodes::Key;
use crate::hashing::key_for_value;
use crate::package::{LatestPointer, Signed, latest_key};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Blob(Vec<u8>),
    Latest(Signed<LatestPointer>),
}

impl Value {
    /// The only key this value may be stored under.
    pub fn key(&self) -> Key {
        match self {
            Value::Blob(bytes) => key_for_value(bytes),
            Value::Latest(pointer) => latest_key(&pointer.body.domain, &pointer.body.package),
        }
    }

    /// Size of the payload, for listings.
    pub fn len(&self) -> usize {
        match self {
            Value::Blob(bytes) => bytes.len(),
            Value::Latest(pointer) => bincode::serialized_size(pointer).unwrap_or(0) as usize,
        }
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Self {
        Value::Blob(bytes)
    }
}

/// A latest pointer for `package` on `domain` at `version`, signed by
/// `signer`. For tests: pair a [`FAKE_KEYS`](crate::dns_data::fake_dns::FAKE_KEYS)
/// domain with its own key for a valid pointer, or with another's for a forgery.
#[cfg(test)]
pub fn signed_latest(
    domain: &str,
    package: &str,
    version: &str,
    signer: &ed25519_dalek::SigningKey,
) -> Signed<LatestPointer> {
    let body = LatestPointer {
        tag: crate::package::LATEST_POINTER_TAG.to_string(),
        domain: domain.to_string(),
        package: package.to_string(),
        version: version.parse().unwrap(),
        version_record_hash: key_for_value(version.as_bytes()),
    };
    crate::package::sign(body, signer).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package::LATEST_POINTER_TAG;

    #[test]
    fn blob_key_is_hash_of_bytes() {
        let value: Value = b"hello".to_vec().into();

        assert_eq!(value.key(), key_for_value(b"hello"));
    }

    #[test]
    fn latest_key_comes_from_domain_and_package() {
        let value = Value::Latest(Signed {
            body: LatestPointer {
                tag: LATEST_POINTER_TAG.to_string(),
                domain: "rfin.ch".to_string(),
                package: "java-pair".to_string(),
                version: "1.0.0".parse().unwrap(),
                version_record_hash: [2; 32],
            },
            sig: Vec::new(),
        });

        assert_eq!(value.key(), latest_key("rfin.ch", "java-pair"));
    }
}
