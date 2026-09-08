// this is not like the kademlia routing table, which dynamically splits buckets when reaching capacity.
// instead, it maintains a fixed set of buckets regardless of how many nodes are added.
// example with id of 4bits, with id y3 y2 y1 y0
// bucket 3  !y3 x x x
// bucket 2  y3 !y2 x x
// bucket 1  y3 y2 !y1 x
// bucket 0  y3 y2 y1 !y0
// the idea with this bucket structure is that compared to the dynamic one is that this can be easly stored in a contiguous array, making lookups and updates more cache-friendly.
//
// to find the bucket one should get close nodes quickly, its just target-id xor my-id count leading zeros and you get bucket id
//
// there is 3 generics in the bucket struct,
// bucket size, more known as replication factor
//
// count zer0 size shift. this is used to reduce the bucket number
// if we take the previous example with 4-bit ids, and have a shift of one
// this would be the buckets
// ie count leading zeros is shifted by one
// bucket 3  !y3 x x x
// bucket 1  y3 y2 !y1 x

// and max buckets, for example a 20 replication factor on the bucket with leading y3 y2 y1 x, would be useless as there only actually 2 nodes that can fit in that bucket.

use crate::close_nodes::{
    CloseNodes, Contact, K, NodeId, leading_zeros, xor_distance, xor_distance_cmp,
};
use std::sync::RwLock;

/// Bits in a [`NodeId`], and so the number of buckets an unshifted table
/// would need.
const ID_BITS: usize = 8 * std::mem::size_of::<NodeId>();

pub struct StaticBucket<
    const BUCKET_SIZE: usize,
    const SEARCH_SHIFT: usize,
    const MAX_BUCKETS: usize,
> {
    contacts: [RwLock<heapless::Vec<Contact, BUCKET_SIZE>>; MAX_BUCKETS],
    my_id: NodeId,
}

impl<const BUCKET_SIZE: usize, const SEARCH_SHIFT: usize, const MAX_BUCKETS: usize>
    StaticBucket<BUCKET_SIZE, SEARCH_SHIFT, MAX_BUCKETS>
{
    pub fn new(my_id: NodeId) -> Self {
        Self {
            contacts: [const { RwLock::new(heapless::Vec::new()) }; MAX_BUCKETS],
            my_id,
        }
    }

    /// The bucket `id` lives in: how many leading bits it shares with
    /// `my_id`, shifted down by `SEARCH_SHIFT` so several prefix lengths
    /// collapse into one bucket.
    ///
    /// Index 0 therefore holds the far half of the keyspace (ids differing
    /// in the top bit) and the last index the nearest sliver. Ids that share
    /// more bits than the table has buckets — `my_id` itself shares all 160 —
    /// clamp into the last bucket rather than running off the end.
    fn bucket_index(&self, id: NodeId) -> usize {
        let shared = leading_zeros(xor_distance(id, self.my_id)) as usize;
        (shared >> SEARCH_SHIFT).min(MAX_BUCKETS - 1)
    }

    fn extend_from_bucket(&self, out: &mut Vec<Contact>, index: usize) {
        out.extend_from_slice(self.contacts[index].read().unwrap().as_slice());
    }
}

impl<const BUCKET_SIZE: usize, const SEARCH_SHIFT: usize, const MAX_BUCKETS: usize> CloseNodes
    for StaticBucket<BUCKET_SIZE, SEARCH_SHIFT, MAX_BUCKETS>
{
    fn close_nodes(&self, id: NodeId) -> Vec<Contact> {
        let home = self.bucket_index(id);
        let mut out = Vec::with_capacity(K);
        self.extend_from_bucket(&mut out, home);

        // The home bucket is where the closest contacts are, but it may hold
        // fewer than k of them, so widen one bucket at a time in both
        // directions until there are enough candidates or the table runs out.
        // Neighbouring buckets are not ordered by distance to `id` among
        // themselves — only the sort below decides which candidates survive.
        let (mut low, mut high) = (home, home);
        while out.len() < K && (low > 0 || high + 1 < MAX_BUCKETS) {
            if high + 1 < MAX_BUCKETS {
                high += 1;
                self.extend_from_bucket(&mut out, high);
            }
            if low > 0 {
                low -= 1;
                self.extend_from_bucket(&mut out, low);
            }
        }

        out.sort_by(|a, b| xor_distance_cmp(a.id, b.id, id));
        out.truncate(K);
        out
    }

    fn maybe_add_contact(&self, contact: Contact) {
        // TODO, see if any of the nodes are dead and can be replaced.
        // A node is not its own contact: its distance to itself is zero, so it
        // would sit in the nearest bucket and crowd out a real peer.
        if contact.id == self.my_id {
            return;
        }
        let mut bucket = self.contacts[self.bucket_index(contact.id)]
            .write()
            .unwrap();
        if let Some(known) = bucket.iter_mut().find(|known| known.id == contact.id) {
            // Same node, new address: a peer that moved is still the peer we
            // want, so refresh where to reach it.
            known.address = contact.address;
            return;
        }
        // A full bucket keeps what it has. Kademlia prefers long-lived
        // contacts, and everything already in here has outlived this one.
        let _ = bucket.push(contact);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;

    fn contact(first_byte: u8, port: u16) -> Contact {
        let mut id = [0u8; 20];
        id[0] = first_byte;
        Contact {
            id,
            address: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    #[test]
    fn leading_zeros_counts_whole_id() {
        assert_eq!(leading_zeros([0u8; 20]), ID_BITS as u32);
        assert_eq!(leading_zeros([0x80; 20]), 0);
        let mut id = [0u8; 20];
        id[2] = 0x01;
        assert_eq!(leading_zeros(id), 23);
    }

    #[test]
    fn bucket_index_grows_with_shared_prefix_and_clamps() {
        let table = StaticBucket::<2, 0, 8>::new([0u8; 20]);
        // 0x80… differs in the top bit: no shared prefix, first bucket.
        assert_eq!(table.bucket_index(contact(0x80, 0).id), 0);
        // 0x40… shares one bit.
        assert_eq!(table.bucket_index(contact(0x40, 0).id), 1);
        // Own id shares all 160 bits, far past the eight buckets that exist.
        assert_eq!(table.bucket_index([0u8; 20]), 7);
    }

    #[test]
    fn close_nodes_widens_past_the_home_bucket() {
        let table = StaticBucket::<4, 0, 8>::new([0u8; 20]);
        for i in 0..4u8 {
            table.maybe_add_contact(contact(0x80 | i, 1000 + i as u16));
            table.maybe_add_contact(contact(0x40 | i, 2000 + i as u16));
        }
        // Only four contacts fit the home bucket, so the rest come from its
        // neighbours, and every one of the eight is returned.
        let close = table.close_nodes(contact(0x80, 0).id);
        assert_eq!(close.len(), 8);
        assert_eq!(close[0].id, contact(0x80, 0).id);
    }

    #[test]
    fn maybe_add_contact_refreshes_address_and_ignores_self() {
        let table = StaticBucket::<4, 0, 8>::new([0u8; 20]);
        table.maybe_add_contact(contact(0x80, 1000));
        table.maybe_add_contact(contact(0x80, 1001));
        let close = table.close_nodes(contact(0x80, 0).id);
        assert_eq!(close.len(), 1);
        assert_eq!(close[0].address.port(), 1001);

        table.maybe_add_contact(Contact {
            id: [0u8; 20],
            address: SocketAddr::from(([127, 0, 0, 1], 9)),
        });
        assert_eq!(table.close_nodes([0u8; 20]).len(), 1);
    }
}
