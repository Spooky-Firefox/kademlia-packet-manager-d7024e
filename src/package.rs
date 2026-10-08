use crate::{close_nodes::Key, hashing::hash_bytes};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(PartialOrd, Ord, PartialEq, Eq, Debug, Clone, Copy, Serialize, Deserialize)]
struct Version {
    major: u32,
    minor: u32,
    patch: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Signed<T> {
    body: T,
    sig: Vec<u8>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct VersionRecord {
    tag: String,
    domain: String,
    package: String,
    version: Version,
    blob_hash: Key,
    prev: Key,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LatestPointer {
    tag: String,
    domain: String,           // "rfin.ch"
    package: String,          // "java-pair"
    version: Version,         // 1.1.0
    version_record_hash: Key, // which record is the newest
}

#[derive(Debug, PartialEq, Eq)]
struct ParseVersionError;

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

fn record_key(record: &Signed<VersionRecord>) -> Key {
    hash_bytes(&bincode::serialize(record).unwrap())
}

fn latest_key(domain: &str, package: &str) -> Key {
    hash_bytes(format!("{}:{}:latest", domain, package).as_bytes())
}

fn body_hash<T: Serialize>(body: &T) -> Key {
    hash_bytes(&bincode::serialize(body).unwrap())
}

fn sign<T: Serialize>(body: T, key: &SigningKey) -> Signed<T> {
    let sig = key.sign(&body_hash(&body)).to_bytes().to_vec();
    Signed { body, sig }
}

impl<T: Serialize> Signed<T> {
    fn verify(&self, owner: &VerifyingKey) -> bool {
        let Ok(sig) = Signature::from_slice(&self.sig) else {
            return false; // garbage bytes, not even a signature
        };
        owner.verify(&body_hash(&self.body), &sig).is_ok()
    }
}

#[cfg(test)]
mod tests {
    use crate::package::{
        LatestPointer, ParseVersionError, Signed, SigningKey, Version, VersionRecord, hash_bytes,
        latest_key, sign,
    };
    const VERSION_RECORD_TAG: &str = "version-record";
    const LATEST_POINTER_TAG: &str = "latest-pointer";

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
        let signed = sign(test_record(), &owner);
        assert!(signed.verify(&owner.verifying_key()));
    }

    #[test]
    fn imposter_signature_fails() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let imposter = SigningKey::from_bytes(&[8; 32]);
        let signed = sign(test_record(), &imposter);
        // signed by someone else, checked against the real owner's PK
        assert!(!signed.verify(&owner.verifying_key()));
    }

    #[test]
    fn tampered_record_fails() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let mut signed = sign(test_record(), &owner);
        signed.body.blob_hash = [9; 32]; // swap in a different binary after signing
        assert!(!signed.verify(&owner.verifying_key()));
    }

    #[test]
    fn garbage_signature_fails() {
        let owner = SigningKey::from_bytes(&[7; 32]);
        let mut signed = sign(test_record(), &owner);
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
        assert!(sign(pointer, &owner).verify(&owner.verifying_key()));
    }
}
