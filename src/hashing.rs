use crate::close_nodes::{Key, NodeId};
use sha2::{Digest, Sha256};
use std::net::SocketAddr;

/// Compute the SHA-256 digest of arbitrary bytes.
pub fn hash_bytes(data: &[u8]) -> [u8; 32] {
    let digest = Sha256::digest(data);

    let mut result = [0u8; 32];
    result.copy_from_slice(&digest);
    result
}

/// Kademlia key for a stored value.
///
/// The lab requires K = hash(V).
pub fn key_for_value(value: &[u8]) -> Key {
    hash_bytes(value)
}

/// Kademlia node ID for a network address.
///
/// We define IP|port as the canonical SocketAddr string, e.g.
/// "127.0.0.1:8000", and SHA-256 that byte representation.
pub fn node_id_from_address(address: SocketAddr) -> NodeId {
    hash_bytes(address.to_string().as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_known_vector() {
        assert_eq!(
            hash_bytes(b"abc"),
            [
                0xba, 0x78, 0x16, 0xbf, 0x8f, 0x01, 0xcf, 0xea, 0x41, 0x41, 0x40, 0xde, 0x5d, 0xae,
                0x22, 0x23, 0xb0, 0x03, 0x61, 0xa3, 0x96, 0x17, 0x7a, 0x9c, 0xb4, 0x10, 0xff, 0x61,
                0xf2, 0x00, 0x15, 0xad,
            ]
        );
    }

    #[test]
    fn value_key_is_sha256_of_value() {
        let value = b"hello kademlia";

        assert_eq!(key_for_value(value), hash_bytes(value));
    }

    #[test]
    fn node_id_is_hash_of_ip_and_port() {
        let address: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        assert_eq!(node_id_from_address(address), hash_bytes(b"127.0.0.1:8000"));
    }
}
