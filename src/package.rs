use crate::close_nodes::Key;
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
    domain: String,
    package: String,
    version: Version,
    blob_hash: Key,
    prev: Key,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LatestPointer {
    domain: String,              // "rfin.ch"
    package: String,             // "java-pair"
    version: Version,            // 1.1.0
    version_record_hash: Key,    // which record is the newest
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

#[cfg(test)]
mod tests {
    use crate::package::{LatestPointer, ParseVersionError, Signed, Version, VersionRecord};
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
}
