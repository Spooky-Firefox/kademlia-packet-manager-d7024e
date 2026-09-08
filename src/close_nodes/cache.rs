//! A memoising layer over any [`CloseNodes`].
//!
//! An iterative lookup asks for the contacts closest to one target over and
//! over as it converges, and every arriving RPC asks again to decide who to
//! reply with. The underlying answer is a sort over a table that changed by at
//! most one contact in between, so the same target is recomputed far more
//! often than it actually moves.

use crate::close_nodes::{CloseNodes, Contact, NodeId};
use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Caches [`CloseNodes::close_nodes`] answers per target id.
///
/// An entry is served while it is younger than `ttl` *and* no contact has been
/// added since it was computed. The generation counter is what makes the
/// second half cheap: a write bumps one atomic instead of walking the map, and
/// stale entries are recognised on the way out.
pub struct CachedCloseNodes<C> {
    inner: C,
    entries: DashMap<NodeId, Entry>,
    /// Bumped by every write, so entries computed before it are stale.
    generation: AtomicU64,
    ttl: Duration,
    capacity: usize,
    /// Whether a write bumps the generation. See [`CachedCloseNodes::ttl_only`].
    invalidate_on_write: bool,
}

struct Entry {
    contacts: Vec<Contact>,
    generation: u64,
    stored: Instant,
}

impl<C: CloseNodes> CachedCloseNodes<C> {
    /// A cache that never serves an answer computed before the last write.
    ///
    /// `capacity` bounds the number of cached targets; a lookup sweeps through
    /// many ids, so without it the map grows with traffic rather than with the
    /// routing table.
    pub fn new(inner: C, ttl: Duration, capacity: usize) -> Self {
        Self {
            inner,
            entries: DashMap::new(),
            generation: AtomicU64::new(0),
            ttl,
            capacity,
            invalidate_on_write: true,
        }
    }

    /// A cache bounded only by `ttl`: writes go straight through and leave
    /// cached answers standing until they expire.
    ///
    /// This is the variant to want when contacts arrive constantly — a table
    /// that learns one per RPC bumps the generation faster than entries can be
    /// reused, and [`new`](Self::new) then costs the bookkeeping of a cache
    /// while behaving like none at all. What it trades away is small: a
    /// contact learned just now may not be routed to for up to `ttl`, and a
    /// lookup that misses it simply takes another round.
    pub fn ttl_only(inner: C, ttl: Duration, capacity: usize) -> Self {
        Self {
            invalidate_on_write: false,
            ..Self::new(inner, ttl, capacity)
        }
    }

    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Drops every cached answer, whether or not it has expired.
    pub fn clear(&self) {
        self.entries.clear();
    }

    fn is_fresh(&self, entry: &Entry, generation: u64) -> bool {
        entry.generation == generation && entry.stored.elapsed() < self.ttl
    }

    /// Makes room for one more entry, preferring to drop what is already dead
    /// and falling back to dropping everything — an LRU's bookkeeping would
    /// cost more than recomputing a sort of the routing table.
    fn make_room(&self, generation: u64) {
        if self.entries.len() < self.capacity {
            return;
        }
        self.entries
            .retain(|_, entry| self.is_fresh(entry, generation));
        if self.entries.len() >= self.capacity {
            self.entries.clear();
        }
    }
}

impl<C: CloseNodes> CloseNodes for CachedCloseNodes<C> {
    fn close_nodes(&self, id: NodeId) -> Vec<Contact> {
        // Read the generation *before* querying: a write racing the query
        // leaves the counter ahead of what we store, so the entry we are about
        // to write is already stale and nobody serves a result that missed it.
        let generation = self.generation.load(Ordering::Acquire);
        if let Some(entry) = self.entries.get(&id)
            && self.is_fresh(&entry, generation)
        {
            return entry.contacts.clone();
        }

        let contacts = self.inner.close_nodes(id);
        self.make_room(generation);
        self.entries.insert(
            id,
            Entry {
                contacts: contacts.clone(),
                generation,
                stored: Instant::now(),
            },
        );
        contacts
    }

    fn maybe_add_contact(&self, contact: Contact) {
        self.inner.maybe_add_contact(contact);
        // A write invalidates every target, not just nearby ones: a contact
        // can enter the k closest of any id. That is why the write-heavy case
        // has its own constructor rather than a cheaper invalidation.
        if self.invalidate_on_write {
            self.generation.fetch_add(1, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::static_bucket::StaticBucket;
    use std::net::SocketAddr;
    use std::sync::atomic::AtomicUsize;

    /// Counts how often the cache actually reached the table below it.
    struct Counting {
        inner: StaticBucket<4, 0, 4>,
        queries: AtomicUsize,
    }

    impl CloseNodes for Counting {
        fn close_nodes(&self, id: NodeId) -> Vec<Contact> {
            self.queries.fetch_add(1, Ordering::Relaxed);
            self.inner.close_nodes(id)
        }

        fn maybe_add_contact(&self, contact: Contact) {
            self.inner.maybe_add_contact(contact);
        }
    }

    fn cache(ttl: Duration) -> CachedCloseNodes<Counting> {
        CachedCloseNodes::new(
            Counting {
                inner: StaticBucket::new([0u8; 20]),
                queries: AtomicUsize::new(0),
            },
            ttl,
            2,
        )
    }

    fn queries(cache: &CachedCloseNodes<Counting>) -> usize {
        cache.inner().queries.load(Ordering::Relaxed)
    }

    fn contact(first_byte: u8) -> Contact {
        let mut id = [0u8; 20];
        id[0] = first_byte;
        Contact {
            id,
            address: SocketAddr::from(([127, 0, 0, 1], 1000)),
        }
    }

    #[test]
    fn repeated_targets_hit_the_cache() {
        let cache = cache(Duration::from_secs(60));
        cache.close_nodes([1u8; 20]);
        cache.close_nodes([1u8; 20]);
        assert_eq!(queries(&cache), 1);

        cache.close_nodes([2u8; 20]);
        assert_eq!(queries(&cache), 2);
    }

    #[test]
    fn a_new_contact_invalidates_every_target() {
        let cache = cache(Duration::from_secs(60));
        cache.close_nodes([1u8; 20]);
        cache.maybe_add_contact(contact(0x80));
        assert_eq!(cache.close_nodes([1u8; 20]), vec![contact(0x80)]);
        assert_eq!(queries(&cache), 2);
    }

    #[test]
    fn ttl_only_serves_through_writes() {
        let cache = CachedCloseNodes::ttl_only(
            Counting {
                inner: StaticBucket::new([0u8; 20]),
                queries: AtomicUsize::new(0),
            },
            Duration::from_secs(60),
            2,
        );
        let before = cache.close_nodes([1u8; 20]);
        cache.maybe_add_contact(contact(0x80));
        // The new contact is in the table below, but the cached answer stands.
        assert_eq!(cache.close_nodes([1u8; 20]), before);
        assert_eq!(
            cache.inner().inner.close_nodes([1u8; 20]),
            vec![contact(0x80)]
        );
        assert_eq!(queries(&cache), 1);
    }

    #[test]
    fn expired_entries_are_recomputed() {
        let cache = cache(Duration::ZERO);
        cache.close_nodes([1u8; 20]);
        cache.close_nodes([1u8; 20]);
        assert_eq!(queries(&cache), 2);
    }

    #[test]
    fn stays_within_capacity() {
        let cache = cache(Duration::from_secs(60));
        for i in 0..8u8 {
            cache.close_nodes([i; 20]);
        }
        assert!(cache.entries.len() <= 2);
    }
}
