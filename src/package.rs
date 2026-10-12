use crate::{close_nodes::Key, hashing::hash_bytes};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// a `major.minor.patch` version. Ord lets us do math stuff
#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Clone, Copy, Serialize, Deserialize)]
pub(crate) struct Version {
    pub(crate) major: u32,
    pub(crate) minor: u32,
    pub(crate) patch: u32,
}

/// a body plus the owner's ed25519 signature over `body_hash(body)`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Signed<T> {
    pub(crate) body: T,
    pub(crate) sig: Vec<u8>,
}

/// VersionRecord points at its blob and at the previous version's record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct VersionRecord {
    pub(crate) tag: String,
    pub(crate) domain: String,
    pub(crate) package: String,
    pub(crate) version: Version,
    pub(crate) blob_hash: Key,
    pub(crate) prev: Key,
}

/// names the newest `VersionRecord` of a package, stored under `latest_key`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct LatestPointer {
    pub(crate) tag: String,
    pub(crate) domain: String,           // "rfin.ch"
    pub(crate) package: String,          // "java-pair"
    pub(crate) version: Version,         // 1.1.0
    pub(crate) version_record_hash: Key, // which record is the newest
}

/// `VersionRecord::tag`, telling a version record apart from other signed bodies.
pub(crate) const VERSION_RECORD_TAG: &str = "version-record";
/// `LatestPointer::tag`, telling a latest pointer apart from other signed bodies.
pub(crate) const LATEST_POINTER_TAG: &str = "latest-pointer";

/// returned when a string is not three dot-separated `u32`s.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParseVersionError;

impl FromStr for Version {
    type Err = ParseVersionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split('.').collect();
        let [major, minor, patch] = parts.as_slice() else {
            return Err(ParseVersionError);
        };

        let major_fromstr = major.parse::<u32>().map_err(|_| ParseVersionError)?;
        let minor_fromstr = minor.parse::<u32>().map_err(|_| ParseVersionError)?;
        let patch_fromstr = patch.parse::<u32>().map_err(|_| ParseVersionError)?;

        Ok(Version {
            major: major_fromstr,
            minor: minor_fromstr,
            patch: patch_fromstr,
        })
    }
}

impl std::fmt::Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// the DHT key a signed version record is stored under: the hash of the whole record.
pub(crate) fn record_key(record: &Signed<VersionRecord>) -> Option<Key> {
    Some(hash_bytes(&bincode::serialize(record).ok()?))
}

/// the fixed DHT key of a package's `LatestPointer`: `hash("domain:package:latest")`.
pub(crate) fn latest_key(domain: &str, package: &str) -> Key {
    hash_bytes(format!("{}:{}:latest", domain, package).as_bytes())
}

/// the hash of a body's bincode encoding, without its signature. `None` only
/// if the body cannot be encoded, which the record types here never hit.
fn body_hash<T: Serialize>(body: &T) -> Option<Key> {
    Some(hash_bytes(&bincode::serialize(body).ok()?))
}

/// sign `body` with the domain owner's secret key.
pub(crate) fn sign<T: Serialize>(body: T, key: &SigningKey) -> Option<Signed<T>> {
    let sig = key.sign(&body_hash(&body)?).to_bytes().to_vec();
    Some(Signed { body, sig })
}

impl<T: Serialize> Signed<T> {
    /// True if `sig` is a valid signature of this body by `owner`.
    pub(crate) fn verify(&self, owner: &VerifyingKey) -> bool {
        let Ok(sig) = Signature::from_slice(&self.sig) else {
            return false; // garbage bytes, not even a signature
        };
        let Some(hash) = body_hash(&self.body) else {
            return false;
        };
        owner.verify(&hash, &sig).is_ok()
    }
}

/// True if `pointer` is a latest pointer signed by `owner`, the key the DNS
/// record on `pointer.body.domain` names. Whether it is stored under the right
/// key is [`Value::key`](crate::value::Value::key)'s job, and whether it is
/// newer than what it replaces is the STORE handler's.
///
/// A domain with a `:` is refused. [`latest_key`] joins domain and package
/// with `:`, so without this the owner of `a` could sign package `b:c` and
/// land on the key of package `c` on domain `a:b`. A hostname never contains
/// a `:`, so the split is unambiguous for every real domain.
pub(crate) fn latest_is_valid(pointer: &Signed<LatestPointer>, owner: &VerifyingKey) -> bool {
    pointer.body.tag == LATEST_POINTER_TAG
        && !pointer.body.domain.contains(':')
        && pointer.verify(owner)
}

#[cfg(test)]
mod tests {
    use crate::package::{
        LATEST_POINTER_TAG, LatestPointer, ParseVersionError, Signed, SigningKey,
        VERSION_RECORD_TAG, Version, VersionRecord, hash_bytes, latest_is_valid, latest_key, sign,
    };

    #[test]
    fn version_ordering() {
        let a: Version = "1.9.0".parse().unwrap();
        let b: Version = "1.11.0".parse().unwrap();
        let c: Version = "2.0.0".parse().unwrap();
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn version_parsing_from_string() {
        assert_eq!(
            "1.2.3".parse::<Version>(),
            Ok(Version {
                major: 1,
                minor: 2,
                patch: 3
            })
        );
    }

    #[test]
    fn versioning_parsing_bad_string() {
        assert_eq!("1.2".parse::<Version>(), Err(ParseVersionError));
        assert_eq!("1.2.x".parse::<Version>(), Err(ParseVersionError));
        assert_eq!("latest".parse::<Version>(), Err(ParseVersionError));
    }

    #[test]
    fn version_display() {
        let v = Version {
            major: 1,
            minor: 10,
            patch: 0,
        };
        assert_eq!(v.to_string(), "1.10.0");
        assert_eq!(v.to_string().parse::<Version>(), Ok(v));
    }

    #[test]
    fn signed_version_record_survives_the_wire() {
        let record = VersionRecord {
            tag: VERSION_RECORD_TAG.to_string(),
            domain: "rfin.ch".to_string(),
            package: "java-pair".to_string(),
            version: "1.0.0".parse().unwrap(),
            blob_hash: [1; 32],
            prev: [0; 32],
        };
        let signed = Signed {
            body: record,
            sig: Vec::new(),
        };

        let bytes = bincode::serialize(&signed).unwrap();
        let decoded: Signed<VersionRecord> = bincode::deserialize(&bytes).unwrap();

        assert_eq!(decoded, signed);
    }

    #[test]
    fn signed_latest_pointer_survives_the_wire() {
        let latest_pointer = LatestPointer {
            tag: LATEST_POINTER_TAG.to_string(),
            domain: "rfin.ch".to_string(),
            package: "java-pair".to_string(),
            version: "1.0.0".parse().unwrap(),
            version_record_hash: [2; 32],
        };
        let signed = Signed {
            body: latest_pointer,
            sig: Vec::new(),
        };

        let bytes = bincode::serialize(&signed).unwrap();
        let decoded: Signed<LatestPointer> = bincode::deserialize(&bytes).unwrap();

        assert_eq!(decoded, signed);
    }

    #[test]
    fn latest_hashes_correctly() {
        assert_eq!(
            latest_key("rfin.ch", "java-pair"),
            latest_key("rfin.ch", "java-pair")
        );
        assert_ne!(
            latest_key("rfin.ch", "java-pair"),
            latest_key("rfin.ch", "other")
        );
        assert_ne!(
            latest_key("rfin.ch", "java-pair"),
            latest_key("other.ch", "java-pair")
        );
        assert_eq!(
            latest_key("rfin.ch", "java-pair"),
            hash_bytes(b"rfin.ch:java-pair:latest")
        );
    }

    fn test_record() -> VersionRecord {
        VersionRecord {
            tag: VERSION_RECORD_TAG.to_string(),
            domain: "rfin.ch".to_string(),
            package: "java-pair".to_string(),
            version: "1.0.0".parse().unwrap(),
            blob_hash: [1; 32],
            prev: [0; 32],
        }
    }

    #[test]
    fn owner_signature_verifies() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let signed = sign(test_record(), &owner).unwrap();
        assert!(signed.verify(&owner.verifying_key()));
    }

    #[test]
    fn imposter_signature_fails() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let imposter = SigningKey::from_bytes(&[8; 32]);
        let signed = sign(test_record(), &imposter).unwrap();
        // signed by someone else, checked against the real owner's PK
        assert!(!signed.verify(&owner.verifying_key()));
    }

    #[test]
    fn tampered_record_fails() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let mut signed = sign(test_record(), &owner).unwrap();
        signed.body.blob_hash = [9; 32]; // swap in a different binary after signing
        assert!(!signed.verify(&owner.verifying_key()));
    }

    #[test]
    fn garbage_signature_fails() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let mut signed = sign(test_record(), &owner).unwrap();
        signed.sig = vec![1, 2, 3];
        assert!(!signed.verify(&owner.verifying_key()));
    }

    #[test]
    fn latest_pointer_can_be_signed() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let pointer = LatestPointer {
            tag: LATEST_POINTER_TAG.to_string(),
            domain: "rfin.ch".to_string(),
            package: "java-pair".to_string(),
            version: "1.0.0".parse().unwrap(),
            version_record_hash: [2; 32],
        };
        assert!(
            sign(pointer, &owner)
                .unwrap()
                .verify(&owner.verifying_key())
        );
    }

    fn test_pointer(tag: &str) -> LatestPointer {
        LatestPointer {
            tag: tag.to_string(),
            domain: "rfin.ch".to_string(),
            package: "java-pair".to_string(),
            version: "1.0.0".parse().unwrap(),
            version_record_hash: [2; 32],
        }
    }

    #[test]
    fn latest_is_valid_accepts_owner_signed_pointer() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let signed = sign(test_pointer(LATEST_POINTER_TAG), &owner).unwrap();
        assert!(latest_is_valid(&signed, &owner.verifying_key()));
    }

    #[test]
    fn latest_is_valid_rejects_wrong_tag() {
        // a correctly signed body of another kind must not pass as a pointer
        let owner = SigningKey::from_bytes(&[7; 32]);
        let signed = sign(test_pointer(VERSION_RECORD_TAG), &owner).unwrap();
        assert!(!latest_is_valid(&signed, &owner.verifying_key()));
    }

    #[test]
    fn latest_is_valid_rejects_colon_in_domain() {
        // "a" + "b:c" would share latest_key with "a:b" + "c"
        assert_eq!(latest_key("a", "b:c"), latest_key("a:b", "c"));
        let owner = SigningKey::from_bytes(&[7; 32]);
        let mut pointer = test_pointer(LATEST_POINTER_TAG);
        pointer.domain = "a:b".to_string();
        let signed = sign(pointer, &owner).unwrap();
        assert!(!latest_is_valid(&signed, &owner.verifying_key()));
    }

    #[test]
    fn latest_is_valid_rejects_other_signer() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let imposter = SigningKey::from_bytes(&[8; 32]);
        let signed = sign(test_pointer(LATEST_POINTER_TAG), &imposter).unwrap();
        assert!(!latest_is_valid(&signed, &owner.verifying_key()));
    }
}
