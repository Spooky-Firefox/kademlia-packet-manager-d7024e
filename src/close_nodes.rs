pub mod cache;
pub mod dumb_bucket;
pub mod sibling_list;
pub mod static_bucket;

use std::net::SocketAddr;

/// 160-bit Kademlia node id.
pub type NodeId = [u8; 20];
/// 160-bit key in the same space as [`NodeId`].
pub type Key = [u8; 20];

/// A routing-table entry: who a node is, and where to reach it.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Contact {
    pub id: NodeId,
    pub address: SocketAddr,
}

pub trait CloseNodes {
    fn close_nodes(&self, id: NodeId) -> Vec<Contact>;

    fn maybe_add_contact(&self, contact: Contact);
}

/// Kademlia's replication parameter: how many contacts a lookup returns.
pub const K: usize = 20;

/// The XOR distance between `id` and `target`.
///
/// Byte 0 is the most significant: an id is a big-endian 160-bit unsigned
/// integer, matching the `u64` request ids on the wire and the order a SHA-1
/// digest already arrives in. Reading it from the other end would not be the
/// XOR metric at all, so the direction is load-bearing — don't "fix" it.
///
/// It also means the derived lexicographic `Ord` on `[u8; 20]` *is* numeric
/// ordering, so the returned distance sorts correctly as a plain sort key.
pub fn xor_distance(id: NodeId, target: NodeId) -> [u8; 20] {
    std::array::from_fn(|i| id[i] ^ target[i])
}

/// Orders `a` and `b` by [`xor_distance`] to `target`; the closer id sorts
/// first. `Iterator::cmp` is lexicographic and lazy, so this compares the
/// first differing byte without materializing either distance.
pub fn xor_distance_cmp(a: NodeId, b: NodeId, target: NodeId) -> std::cmp::Ordering {
    a.iter()
        .zip(&target)
        .map(|(x, t)| x ^ t)
        .cmp(b.iter().zip(&target).map(|(y, t)| y ^ t))
}

/// Number of leading zero bits in `id`, counting down from the most
/// significant bit of byte 0 — the same big-endian direction
/// [`xor_distance`] documents. The all-zero id has all 160 bits zero.
///
/// `leading_zeros(xor_distance(a, b))` is the length of the common prefix a
/// and b share, which is what picks a routing bucket.
pub fn leading_zeros(id: NodeId) -> u32 {
    let mut zeros = 0;
    for byte in id {
        if byte != 0 {
            return zeros + byte.leading_zeros();
        }
        zeros += 8;
    }
    zeros
}
