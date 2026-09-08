//! The nodes nearest to us, held separately from the routing table.
//!
//! A bucket table is built around the *far* half of the keyspace: half of its
//! buckets cover half the network, and the near region — the one that decides
//! where a key is actually stored — gets a single bucket that a table with a
//! bounded bucket count may even clamp several prefix lengths into. That is
//! fine for routing, where any contact in the right direction moves a lookup
//! forward, but it is not fine for replication, where the set of nodes closest
//! to a key has to be right.
//!
//! S/Kademlia's answer is a sibling list: a small sorted set of the `s` nodes
//! closest to us, maintained outside the buckets so no eviction policy can
//! drop them.

use crate::close_nodes::{CloseNodes, Contact, K, NodeId, xor_distance_cmp};
use std::cmp::Ordering;
use std::sync::RwLock;

/// The `SIBLINGS` contacts closest to `my_id`, layered over a `fallback`
/// [`CloseNodes`] that answers for the rest of the keyspace.
///
/// The two are an overlay, not a partition: every contact offered here is also
/// offered to the fallback, so the routing table stays complete and a contact
/// evicted from the sibling list is not lost. Queries merge both and let XOR
/// distance decide, which is why the overlap costs nothing.
pub struct SiblingList<C, const SIBLINGS: usize> {
    /// Sorted by XOR distance to `my_id`, closest first, so the farthest
    /// sibling — the only one eviction ever looks at — is last.
    siblings: RwLock<heapless::Vec<Contact, SIBLINGS>>,
    fallback: C,
    my_id: NodeId,
}

impl<C: CloseNodes, const SIBLINGS: usize> SiblingList<C, SIBLINGS> {
    pub fn new(my_id: NodeId, fallback: C) -> Self {
        Self {
            siblings: RwLock::new(heapless::Vec::new()),
            fallback,
            my_id,
        }
    }

    /// The current sibling set, closest first.
    ///
    /// This is the replication set: a value whose key falls in our
    /// neighbourhood belongs on these nodes, and unlike [`CloseNodes::close_nodes`]
    /// the answer is not diluted by routing contacts that merely point the
    /// right way.
    pub fn siblings(&self) -> Vec<Contact> {
        self.siblings.read().unwrap().as_slice().to_vec()
    }

    pub fn fallback(&self) -> &C {
        &self.fallback
    }
}

impl<C: CloseNodes, const SIBLINGS: usize> CloseNodes for SiblingList<C, SIBLINGS> {
    fn close_nodes(&self, id: NodeId) -> Vec<Contact> {
        let mut out = self.fallback.close_nodes(id);
        out.extend_from_slice(self.siblings.read().unwrap().as_slice());
        out.sort_by(|a, b| xor_distance_cmp(a.id, b.id, id));
        // XOR is a bijection, so two distinct ids never share a distance to
        // `id`: after the sort, duplicates of one contact are adjacent.
        out.dedup_by(|a, b| a.id == b.id);
        out.truncate(K);
        out
    }

    fn maybe_add_contact(&self, contact: Contact) {
        if contact.id == self.my_id {
            return;
        }
        self.fallback.maybe_add_contact(contact);

        let mut siblings = self.siblings.write().unwrap();
        if let Some(known) = siblings.iter_mut().find(|known| known.id == contact.id) {
            known.address = contact.address;
            return;
        }
        let pos = siblings
            .partition_point(|s| xor_distance_cmp(s.id, contact.id, self.my_id) == Ordering::Less);
        if siblings.is_full() {
            // Farther than every sibling we already hold — and with a full
            // list, that is the whole test. Unlike a k-bucket, seniority does
            // not enter into it: the sibling set is defined by distance.
            if pos == siblings.len() {
                return;
            }
            siblings.pop();
        }
        let _ = siblings.insert(pos, contact);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::static_bucket::StaticBucket;
    use std::net::SocketAddr;

    fn contact(first_byte: u8, port: u16) -> Contact {
        let mut id = [0u8; 20];
        id[0] = first_byte;
        Contact {
            id,
            address: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    fn table<const S: usize>() -> SiblingList<StaticBucket<2, 0, 4>, S> {
        SiblingList::new([0u8; 20], StaticBucket::new([0u8; 20]))
    }

    #[test]
    fn keeps_the_closest_and_drops_the_rest() {
        let table = table::<3>();
        for first in [0x80, 0x01, 0x40, 0x02, 0x04] {
            table.maybe_add_contact(contact(first, 1000));
        }
        let siblings: Vec<u8> = table.siblings().iter().map(|c| c.id[0]).collect();
        assert_eq!(siblings, vec![0x01, 0x02, 0x04]);
    }

    #[test]
    fn survives_eviction_from_the_fallback() {
        // The fallback holds two per bucket; 0x01, 0x02 and 0x04 all share a
        // bucket there, so one of them cannot fit.
        let table = table::<3>();
        for first in [0x01, 0x02, 0x04] {
            table.maybe_add_contact(contact(first, 1000));
        }
        let close: Vec<u8> = table
            .close_nodes([0u8; 20])
            .iter()
            .map(|c| c.id[0])
            .collect();
        assert_eq!(close, vec![0x01, 0x02, 0x04]);
    }

    #[test]
    fn merges_without_duplicating() {
        let table = table::<3>();
        table.maybe_add_contact(contact(0x01, 1000));
        // Present in both the sibling list and the fallback, returned once.
        assert_eq!(table.close_nodes([0u8; 20]).len(), 1);

        table.maybe_add_contact(contact(0x01, 1001));
        assert_eq!(table.siblings()[0].address.port(), 1001);
    }
}
