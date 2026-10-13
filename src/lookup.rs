use crate::close_nodes::{CloseNodes, Contact, K, Key, NodeId, xor_distance_cmp};
use crate::dns_data::DnsData;
use crate::hashing::key_for_value;
use crate::package::{LatestPointer, Signed, VersionRecord, latest_is_valid, latest_key};
use crate::rpc::{FindValue, Rpc};
use crate::rpc_transport::RpcTransport;
use crate::value::Value;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Kademlia's concurrency parameter: how many `find_node` RPCs a lookup keeps
/// in flight at once, unless [`set_alpha`] says otherwise.
pub const DEFAULT_ALPHA: usize = 3;

static ALPHA: AtomicUsize = AtomicUsize::new(DEFAULT_ALPHA);

/// Change alpha for every lookup in the process from now on. For experiments
/// that compare values of it; a value below 1 is taken as 1, since a lookup
/// with nothing in flight would never start.
pub fn set_alpha(alpha: usize) {
    ALPHA.store(alpha.max(1), Ordering::Relaxed);
}

/// The alpha lookups currently use: [`DEFAULT_ALPHA`] unless [`set_alpha`]
/// changed it.
pub fn alpha() -> usize {
    ALPHA.load(Ordering::Relaxed)
}

pub async fn lookup_node<T, U, A>(rpc: &Rpc<T, U, A>, target: NodeId) -> Vec<Contact>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    lookup_node_within(rpc, target, 0).await
}

/// [`lookup_node`], logged as part of lookup `parent` (0 for none).
async fn lookup_node_within<T, U, A>(
    rpc: &Rpc<T, U, A>,
    target: NodeId,
    parent: u64,
) -> Vec<Contact>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    let started = Instant::now();
    let alpha = alpha();
    let lookup_id = crate::instrumentation::next_lookup_id();
    let my_id = rpc.my_contact().id;

    crate::instrumentation::lookup_start(lookup_id, "node", my_id, target);

    let mut probe_count = 0usize;
    // contacts we know closest to target
    let mut candidates = rpc.close_nodes().close_nodes(target);
    candidates.sort_by(|a, b| xor_distance_cmp(a.id, b.id, target));
    candidates.truncate(K);

    // ids we have already fired a query at: in flight or answered, never re-sent
    let mut queried: HashSet<NodeId> = HashSet::new();
    // how many replies, one leading to the next, it took to hear of each
    // contact: 0 for what the routing table already held
    let mut depth: HashMap<NodeId, u32> = candidates.iter().map(|c| (c.id, 0)).collect();
    // up to alpha find_node calls resolving concurrently
    let mut in_flight = FuturesUnordered::new();

    loop {
        // top the pipeline back up to alpha with the closest un-queried nodes
        while in_flight.len() < alpha {
            let Some(next) = candidates
                .iter()
                .find(|contact| !queried.contains(&contact.id))
                .copied()
            else {
                break;
            };

            queried.insert(next.id);

            crate::instrumentation::lookup_probe(lookup_id, "node", my_id, next);

            probe_count += 1;

            let next_depth = depth[&next.id];
            in_flight.push(async move { (next_depth, rpc.find_node(&next, target).await) });
        }

        // nothing running and nothing left to start: the K closest are settled
        let Some((replied_depth, mut new_contacts)) = in_flight.next().await else {
            break;
        };
        // removes self from contacts, we must never be our own candidate
        // (tested by bootstrap::tests::bootstrap_never_returns_us)
        new_contacts.retain(|c| c.id != my_id);

        // every contact a response teaches us about is worth offering to the
        // routing table, whether or not it ends up among the K closest here
        for contact in &new_contacts {
            rpc.close_nodes().maybe_add_contact(*contact);
            depth.entry(contact.id).or_insert(replied_depth + 1);
        }

        candidates.extend(new_contacts);
        // sort by distance using Olles XOR thingamajig with a closure (rust voodoo)
        candidates.sort_by(|a, b| xor_distance_cmp(a.id, b.id, target));
        // flatline duplicate chooms (same id => same distance => adjacent here)
        candidates.dedup_by_key(|contact| contact.id);
        // yoink K closest contacts
        candidates.truncate(K);
    }

    let exact_match = candidates.iter().any(|contact| contact.id == target);
    // the hop count: how long a chain of replies led to the closest result
    let hops = candidates.first().map_or(0, |closest| depth[&closest.id]);

    // log end of lookup
    crate::instrumentation::node_lookup_end(crate::instrumentation::NodeLookupEnd {
        lookup_id,
        parent,
        node: my_id,
        probes: probe_count,
        result_count: candidates.len(),
        exact_match,
        hops,
        duration: started.elapsed(),
    });

    candidates
}

/// Ask the `K` nodes closest to `key` for it, `ALPHA` at a time, and collect
/// the replies `accept` passes. With `stop_at_first`, return as soon as one
/// does.
///
/// Checking here, inside the probe loop, is what makes a bad reply cost only
/// one probe: it is skipped and the next candidate is asked. Checked any later
/// (by the caller, or the CLI) the first reply would end the lookup, and a
/// node that answers fast with garbage would win every race.
async fn lookup_values<T, U, A>(
    rpc: &Rpc<T, U, A>,
    key: Key,
    accept: impl Fn(&Value) -> bool,
    stop_at_first: bool,
) -> Vec<(Contact, Value)>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    let started = Instant::now();
    let alpha = alpha();
    let lookup_id = crate::instrumentation::next_lookup_id();
    let my_id = rpc.my_contact().id;
    let mut probe_count = 0usize;
    let mut found = Vec::new();

    crate::instrumentation::lookup_start(lookup_id, "value", my_id, key);

    let mut candidates = lookup_node_within(rpc, key, lookup_id).await;

    // Routing tables do not contain ourselves, but we may hold the value.
    candidates.push(rpc.my_contact());

    candidates.sort_by(|a, b| xor_distance_cmp(a.id, b.id, key));
    candidates.dedup_by_key(|contact| contact.id);
    candidates.truncate(K);

    let mut in_flight = FuturesUnordered::new();
    let mut candidates = candidates.into_iter();

    loop {
        while in_flight.len() < alpha {
            let Some(next) = candidates.next() else {
                break;
            };

            crate::instrumentation::lookup_probe(lookup_id, "value", my_id, next);

            probe_count += 1;
            in_flight.push(async move {
                let reply = rpc.find_value(&next, key).await;
                (next, reply)
            });
        }

        let Some((source, reply)) = in_flight.next().await else {
            break;
        };

        match reply {
            FindValue::Value(value) => {
                if accept(&value) {
                    found.push((source, value));
                    if stop_at_first {
                        break;
                    }
                } else {
                    log::warn!(
                        "node {} returned a value that is not valid for the requested key",
                        source.address
                    );
                }
            }

            FindValue::Closest(contacts) => {
                for contact in &contacts {
                    rpc.close_nodes().maybe_add_contact(*contact);
                }
            }
        }
    }

    crate::instrumentation::value_lookup_end(
        lookup_id,
        my_id,
        probe_count,
        !found.is_empty(),
        started.elapsed(),
    );

    found
}

/// Find the blob stored under `key`, and who answered with it. Only a blob
/// that hashes to `key` is accepted.
pub async fn lookup_value<T, U, A>(rpc: &Rpc<T, U, A>, key: Key) -> Option<(Contact, Vec<u8>)>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    let accept = |value: &Value| matches!(value, Value::Blob(bytes) if key_for_value(bytes) == key);
    let (source, value) = lookup_values(rpc, key, accept, true)
        .await
        .into_iter()
        .next()?;
    match value {
        Value::Blob(bytes) => Some((source, bytes)),
        Value::Latest(_) => None,
    }
}

/// The newest valid latest pointer for `package` on `domain`.
///
/// Every one of the `K` closest nodes is asked rather than stopping at the
/// first answer: a replica that missed the last publish still holds an older
/// pointer, validly signed, and must not hide the newer one.
///
/// `None` if the domain has no key in DNS, or no node holds a pointer signed
/// with it.
pub async fn get_latest<T, U, A, D>(
    rpc: &Rpc<T, U, A>,
    dns: &D,
    domain: &str,
    package: &str,
) -> Option<Signed<LatestPointer>>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
    D: DnsData,
{
    // One DNS query up front, so the per-reply check stays synchronous.
    let owner = dns.get_dns_vk(domain).await?;
    let accept = |value: &Value| {
        matches!(value, Value::Latest(pointer)
            if pointer.body.domain == domain
                && pointer.body.package == package
                && latest_is_valid(pointer, &owner))
    };

    lookup_values(rpc, latest_key(domain, package), accept, false)
        .await
        .into_iter()
        .filter_map(|(_, value)| match value {
            Value::Latest(pointer) => Some(pointer),
            Value::Blob(_) => None,
        })
        .max_by_key(|pointer| pointer.body.version)
}

/// Store `value` on the `K` nodes closest to its key, and return the key.
pub async fn store_value<T, U, A>(rpc: &Rpc<T, U, A>, value: Value) -> Key
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    store_value_with_proof(rpc, value, &[]).await.0
}

/// Replicate a value to the K closest nodes, optionally including
/// a signed version history required for latest pointers.
///
/// Returns the value's key and how many nodes acknowledged STORE.
pub(crate) async fn store_value_with_proof<T, U, A>(
    rpc: &Rpc<T, U, A>,
    value: Value,
    proof: &[Signed<VersionRecord>],
) -> (Key, usize)
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    let key = value.key();

    // Find the K closest nodes to this key.
    let mut targets = lookup_node(rpc, key).await;

    // Include ourselves as a possible storage target.
    targets.push(rpc.my_contact());

    targets.sort_by(|a, b| xor_distance_cmp(a.id, b.id, key));
    targets.dedup_by_key(|contact| contact.id);
    targets.truncate(K);

    // Count how many nodes actually accepted the value.
    let mut accepted = 0;

    for target in targets {
        if rpc
            .store_with_proof(&target, key, value.clone(), proof.to_vec())
            .await
        {
            accepted += 1;
        }
    }

    (key, accepted)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::dumb_bucket::DumbBucket;
    use std::sync::{Arc, RwLock};

    /// The node running the lookups under test. Far from every target here, so
    /// it never competes with the contacts a lookup is meant to return.
    fn me() -> Contact {
        Contact {
            id: [0xffu8; 32],
            address: "127.0.0.1:9000".parse().unwrap(),
        }
    }

    struct FakeCloseNodes {
        initial: Vec<Contact>,
    }

    impl CloseNodes for FakeCloseNodes {
        fn close_nodes(&self, _id: NodeId) -> Vec<Contact> {
            self.initial.clone()
        }

        fn maybe_add_contact(&self, _contact: Contact) {}

        fn contacts_iter(&self) -> impl std::iter::Iterator<Item = Contact> {
            unimplemented!();
            // so the compiler wont complain
            #[allow(unreachable_code)]
            Vec::new().into_iter()
        }

        fn remove_contact(&self, _contact: &Contact) {
            unimplemented!()
        }
    }

    struct FakeTransport {
        a: Contact,
        b: Contact,
        c: Contact,
    }

    impl RpcTransport for FakeTransport {
        async fn send_receive(
            &self,
            _payload: Vec<u8>,
            address: std::net::SocketAddr,
        ) -> std::io::Result<Vec<u8>> {
            if address == self.a.address {
                Ok(bincode::serialize(&vec![self.b]).unwrap())
            } else if address == self.b.address {
                Ok(bincode::serialize(&vec![self.c]).unwrap())
            } else {
                Ok(bincode::serialize(&Vec::<Contact>::new()).unwrap())
            }
        }
    }
    struct FakeValueTransport {
        a: Contact,
        b: Contact,
        c: Contact,
        value: Vec<u8>,
    }

    impl RpcTransport for FakeValueTransport {
        async fn send_receive(
            &self,
            _payload: Vec<u8>,
            address: std::net::SocketAddr,
        ) -> std::io::Result<Vec<u8>> {
            let reply = if address == self.a.address {
                FindValue::Closest(vec![self.b])
            } else if address == self.b.address {
                FindValue::Closest(vec![self.c])
            } else if address == self.c.address {
                FindValue::Value(self.value.clone().into())
            } else {
                FindValue::Closest(Vec::new())
            };

            Ok(bincode::serialize(&reply).unwrap())
        }
    }

    #[tokio::test]
    async fn value_lookup_finds_value_after_node_lookup() {
        let value = b"hello".to_vec();
        let key = crate::hashing::key_for_value(&value);

        let a = Contact {
            id: [8u8; 32],
            address: "127.0.0.1:8001".parse().unwrap(),
        };

        let b = Contact {
            id: [4u8; 32],
            address: "127.0.0.1:8002".parse().unwrap(),
        };

        let c = Contact {
            id: [2u8; 32],
            address: "127.0.0.1:8003".parse().unwrap(),
        };

        let transport = FakeTransport { a, b, c };

        let robust_transport = FakeValueTransport {
            a,
            b,
            c,
            value: value.clone(),
        };

        let close_nodes = FakeCloseNodes { initial: vec![a] };

        let rpc = Rpc::new(me(), transport, robust_transport, close_nodes);

        let result = lookup_value(&rpc, key).await;

        assert_eq!(result, Some((c, value)));
    }
    struct FakeMissingValueTransport {
        a: Contact,
        b: Contact,
    }

    impl RpcTransport for FakeMissingValueTransport {
        async fn send_receive(
            &self,
            _payload: Vec<u8>,
            address: std::net::SocketAddr,
        ) -> std::io::Result<Vec<u8>> {
            let reply = if address == self.a.address {
                FindValue::Closest(vec![self.b])
            } else {
                FindValue::Closest(Vec::new())
            };

            Ok(bincode::serialize(&reply).unwrap())
        }
    }

    #[tokio::test]
    async fn value_lookup_returns_none_when_value_is_not_found() {
        let key = [0u8; 32];

        let a = Contact {
            id: [8u8; 32],
            address: "127.0.0.1:8001".parse().unwrap(),
        };

        let b = Contact {
            id: [4u8; 32],
            address: "127.0.0.1:8002".parse().unwrap(),
        };

        let c = Contact {
            id: [2u8; 32],
            address: "127.0.0.1:8003".parse().unwrap(),
        };

        let transport = FakeTransport { a, b, c };
        let robust_transport = FakeMissingValueTransport { a, b };

        let close_nodes = FakeCloseNodes { initial: vec![a] };

        let rpc = Rpc::new(me(), transport, robust_transport, close_nodes);

        let result = lookup_value(&rpc, key).await;

        assert_eq!(result, None);
    }

    struct FakeEmptyTransport;

    impl RpcTransport for FakeEmptyTransport {
        async fn send_receive(
            &self,
            _payload: Vec<u8>,
            _address: std::net::SocketAddr,
        ) -> std::io::Result<Vec<u8>> {
            Ok(bincode::serialize(&Vec::<Contact>::new()).unwrap())
        }
    }

    struct FakeClosestOnlyTransport {
        a: Contact,
        newly_discovered: Contact,
    }

    impl RpcTransport for FakeClosestOnlyTransport {
        async fn send_receive(
            &self,
            _payload: Vec<u8>,
            address: std::net::SocketAddr,
        ) -> std::io::Result<Vec<u8>> {
            let reply = if address == self.a.address {
                FindValue::Closest(vec![self.newly_discovered])
            } else {
                FindValue::Closest(Vec::new())
            };

            Ok(bincode::serialize(&reply).unwrap())
        }
    }

    /// `find_value` can answer with closest-known contacts just like
    /// `find_node` does, and any of those are worth offering to the routing
    /// table even when the lookup never turns up the value itself.
    #[tokio::test]
    async fn value_lookup_offers_closest_reply_contacts_to_the_routing_table() {
        let key = [0u8; 32];

        let a = Contact {
            id: [8u8; 32],
            address: "127.0.0.1:9101".parse().unwrap(),
        };
        // only ever surfaces through find_value's Closest reply, never
        // through find_node, so its presence proves this call path.
        let newly_discovered = Contact {
            id: [4u8; 32],
            address: "127.0.0.1:9102".parse().unwrap(),
        };

        // find_node (plain transport) discovers nothing new, so lookup_node
        // hands lookup_value only the single already-known contact `a`.
        let transport = FakeEmptyTransport;
        let robust_transport = FakeClosestOnlyTransport {
            a,
            newly_discovered,
        };

        // DumbBucket has no insertion path besides `maybe_add_contact`, so
        // whatever ends up in it must have gone through that call.
        let close_nodes = DumbBucket {
            contacts: Arc::new(RwLock::new(vec![a])),
        };

        let rpc = Rpc::new(me(), transport, robust_transport, close_nodes);

        let result = lookup_value(&rpc, key).await;

        assert_eq!(result, None);

        let learned = rpc.close_nodes().contacts.read().unwrap().clone();
        assert!(learned.contains(&newly_discovered));
    }

    #[tokio::test]
    async fn lookup_follows_newly_discovered_contacts() {
        let target = [0u8; 32];

        let a = Contact {
            id: [8u8; 32],
            address: "127.0.0.1:8001".parse().unwrap(),
        };

        let b = Contact {
            id: [4u8; 32],
            address: "127.0.0.1:8002".parse().unwrap(),
        };

        let c = Contact {
            id: [2u8; 32],
            address: "127.0.0.1:8003".parse().unwrap(),
        };

        let transport = FakeTransport { a, b, c };
        // lookup_node only calls find_node, which uses the plain transport,
        // so the robust transport is never invoked here.
        let robust_transport = FakeTransport { a, b, c };

        let close_nodes = FakeCloseNodes { initial: vec![a] };

        let rpc = Rpc::new(me(), transport, robust_transport, close_nodes);

        let result = lookup_node(&rpc, target).await;

        assert_eq!(result, vec![c, b, a]);
    }
    #[tokio::test]
    async fn lookup_offers_newly_discovered_contacts_to_the_routing_table() {
        let target = [0u8; 32];

        let a = Contact {
            id: [8u8; 32],
            address: "127.0.0.1:9001".parse().unwrap(),
        };
        let b = Contact {
            id: [4u8; 32],
            address: "127.0.0.1:9002".parse().unwrap(),
        };
        let c = Contact {
            id: [2u8; 32],
            address: "127.0.0.1:9003".parse().unwrap(),
        };

        let transport = FakeTransport { a, b, c };
        let robust_transport = FakeTransport { a, b, c };

        // DumbBucket has no insertion path besides `maybe_add_contact`, so
        // whatever ends up in it must have gone through that call.
        let close_nodes = DumbBucket {
            contacts: Arc::new(RwLock::new(vec![a])),
        };

        let rpc = Rpc::new(me(), transport, robust_transport, close_nodes);

        lookup_node(&rpc, target).await;

        let learned = rpc.close_nodes().contacts.read().unwrap().clone();
        assert_eq!(learned, vec![a, b, c]);
    }

    #[tokio::test]
    async fn lookup_returns_empty_when_no_contacts_are_known() {
        let target = [0u8; 32];

        let dummy = Contact {
            id: [1u8; 32],
            address: "127.0.0.1:8001".parse().unwrap(),
        };

        let transport = FakeTransport {
            a: dummy,
            b: dummy,
            c: dummy,
        };
        let robust_transport = FakeTransport {
            a: dummy,
            b: dummy,
            c: dummy,
        };

        let close_nodes = FakeCloseNodes { initial: vec![] };

        let rpc = Rpc::new(me(), transport, robust_transport, close_nodes);

        let result = lookup_node(&rpc, target).await;

        assert!(result.is_empty());
    }
    /// Other tests run lookups concurrently with this one, so it only ever
    /// sets alpha to values a lookup is correct under, and puts it back.
    #[test]
    fn alpha_is_clamped_to_at_least_one() {
        set_alpha(0);
        assert_eq!(alpha(), 1);
        set_alpha(DEFAULT_ALPHA);
        assert_eq!(alpha(), DEFAULT_ALPHA);
    }

    #[tokio::test]
    async fn value_lookup_rejects_value_that_does_not_match_key() {
        let expected_value = b"correct value".to_vec();
        let key = key_for_value(&expected_value);

        let a = Contact {
            id: [8u8; 32],
            address: "127.0.0.1:8001".parse().unwrap(),
        };

        let b = Contact {
            id: [4u8; 32],
            address: "127.0.0.1:8002".parse().unwrap(),
        };

        let c = Contact {
            id: [2u8; 32],
            address: "127.0.0.1:8003".parse().unwrap(),
        };

        let transport = FakeTransport { a, b, c };

        // C claims to have the requested key, but sends different content.
        let robust_transport = FakeValueTransport {
            a,
            b,
            c,
            value: b"tampered value".to_vec(),
        };

        let close_nodes = FakeCloseNodes { initial: vec![a] };

        let rpc = Rpc::new(me(), transport, robust_transport, close_nodes);

        let result = lookup_value(&rpc, key).await;

        assert_eq!(result, None);
    }

    /// Answers FIND_VALUE from a fixed table, and with no contacts for any
    /// address not in it.
    struct FakeRepliesTransport {
        replies: Vec<(std::net::SocketAddr, FindValue)>,
    }

    impl RpcTransport for FakeRepliesTransport {
        async fn send_receive(
            &self,
            _payload: Vec<u8>,
            address: std::net::SocketAddr,
        ) -> std::io::Result<Vec<u8>> {
            let reply = self
                .replies
                .iter()
                .find(|(at, _)| *at == address)
                .map(|(_, reply)| reply.clone())
                .unwrap_or(FindValue::Closest(Vec::new()));
            Ok(bincode::serialize(&reply).unwrap())
        }
    }

    /// Three contacts a lookup reaches through `FakeTransport` (a -> b -> c),
    /// answering FIND_VALUE for `package` on node0's domain with `pointers`.
    async fn get_latest_with(
        pointers: [Option<Signed<LatestPointer>>; 3],
        domain: &str,
    ) -> Option<Signed<LatestPointer>> {
        let a = Contact {
            id: [8u8; 32],
            address: "127.0.0.1:8001".parse().unwrap(),
        };
        let b = Contact {
            id: [4u8; 32],
            address: "127.0.0.1:8002".parse().unwrap(),
        };
        let c = Contact {
            id: [2u8; 32],
            address: "127.0.0.1:8003".parse().unwrap(),
        };
        let replies = [a, b, c]
            .into_iter()
            .zip(pointers)
            .filter_map(|(contact, pointer)| {
                Some((contact.address, FindValue::Value(Value::Latest(pointer?))))
            })
            .collect();
        let rpc = Rpc::new(
            me(),
            FakeTransport { a, b, c },
            FakeRepliesTransport { replies },
            FakeCloseNodes { initial: vec![a] },
        );

        get_latest(
            &rpc,
            &crate::dns_data::fake_dns::FakeDns::new(),
            domain,
            "pkg",
        )
        .await
    }

    fn pointer(version: &str, signer: usize) -> Option<Signed<LatestPointer>> {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        let signer = FAKE_KEYS[signer].signing_key().unwrap();
        Some(crate::value::signed_latest(
            FAKE_KEYS[0].domain,
            "pkg",
            version,
            &signer,
        ))
    }

    #[tokio::test]
    async fn get_latest_skips_forged_pointer() {
        let domain = crate::dns_data::fake_dns::FAKE_KEYS[0].domain;
        // a answers first with a high version signed by the wrong key
        let result =
            get_latest_with([pointer("9.0.0", 1), pointer("1.0.0", 0), None], domain).await;

        assert_eq!(result, pointer("1.0.0", 0));
    }

    #[tokio::test]
    async fn get_latest_returns_highest_version_when_replicas_disagree() {
        let domain = crate::dns_data::fake_dns::FAKE_KEYS[0].domain;
        let result = get_latest_with(
            [
                pointer("1.0.0", 0),
                pointer("1.10.0", 0),
                pointer("1.9.0", 0),
            ],
            domain,
        )
        .await;

        assert_eq!(result, pointer("1.10.0", 0));
    }

    #[tokio::test]
    async fn get_latest_is_none_without_dns_key() {
        let result = get_latest_with([pointer("1.0.0", 0), None, None], "nobody.d7024e.test").await;

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn get_latest_ignores_pointer_for_another_package() {
        // validly signed by the owner, but not the package asked for
        let domain = crate::dns_data::fake_dns::FAKE_KEYS[0].domain;
        let signer = crate::dns_data::fake_dns::FAKE_KEYS[0]
            .signing_key()
            .unwrap();
        let other = crate::value::signed_latest(domain, "other", "1.0.0", &signer);
        let result = get_latest_with([Some(other), None, None], domain).await;

        assert_eq!(result, None);
    }
}
