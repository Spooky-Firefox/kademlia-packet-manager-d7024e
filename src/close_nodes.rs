pub mod cache;
pub mod dumb_bucket;
pub mod sibling_list;
pub mod static_bucket;

use cache::CachedCloseNodes;
use sibling_list::SiblingList;
use static_bucket::StaticBucket;
use std::net::SocketAddr;
use std::time::Duration;

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

/// How many routing buckets [`recommended`] gives the table under the
/// siblings.
///
/// A bucket at shared-prefix length `m` covers `2^-m` of the keyspace, so in a
/// network of `n` nodes every bucket past `log2(n)` sits empty essentially
/// always. Thirty-two of them cover networks into the billions before the
/// clamp at the near end starts throwing contacts together, and cost about
/// 34 KB at `K` contacts each — cheap enough that trading routing precision
/// for a smaller table (`SEARCH_SHIFT`) buys nothing worth having.
pub const ROUTING_BUCKETS: usize = 32;

/// The `s` of S/Kademlia: how many near nodes are held outside the buckets,
/// safe from bucket eviction. Sized to [`K`] so the sibling set alone can
/// answer a full lookup for a key in our own neighbourhood.
pub const SIBLINGS: usize = K;

/// How long [`recommended`] serves a cached answer.
///
/// The window is what a contact learned right now waits before it can be
/// routed to, so it is set by how stale an answer may be, not by how much
/// work it saves: half a second is beneath a lookup round trip, and a lookup
/// that misses a brand-new contact just takes one more round.
pub const CACHE_TTL: Duration = Duration::from_millis(500);

/// How many distinct targets [`recommended`] keeps cached.
///
/// The cache pays for our own in-flight lookups, which ask about one target
/// repeatedly as they converge; incoming queries name targets that are near
/// enough to random and mostly miss. So this needs to cover concurrent
/// lookups, not traffic, and every entry costs another `K` contacts of memory.
pub const CACHE_CAPACITY: usize = 64;

/// The stack [`recommended`] builds, named so it can be stored in a struct.
pub type RecommendedCloseNodes =
    CachedCloseNodes<SiblingList<StaticBucket<K, 0, ROUTING_BUCKETS>, SIBLINGS>>;

/// A [`CloseNodes`] assembled from all three layers, outermost first:
///
/// - [`CachedCloseNodes`] memoises answers for [`CACHE_TTL`]. It is built
///   [TTL-only](CachedCloseNodes::ttl_only), on the assumption that contacts
///   arrive often — with a write per RPC, invalidating on write would flush
///   the cache faster than anything could be reused.
/// - [`SiblingList`] pins the [`SIBLINGS`] nearest nodes where no bucket
///   eviction can reach them, so replication has an exact neighbourhood to
///   work with rather than whatever routing happened to keep.
/// - [`StaticBucket`] routes the rest of the keyspace, one bucket per bit of
///   shared prefix and no bucket count reduction.
///
/// Caching goes outside the sibling list so a hit skips the merge as well as
/// the sort.
pub fn recommended(my_id: NodeId) -> RecommendedCloseNodes {
    CachedCloseNodes::ttl_only(
        SiblingList::new(my_id, StaticBucket::new(my_id)),
        CACHE_TTL,
        CACHE_CAPACITY,
    )
}
