use crate::close_nodes::{CloseNodes, Contact, K, Key, NodeId, xor_distance_cmp};
use crate::hashing::key_for_value;
use crate::rpc::{FindValue, Rpc};
use crate::rpc_transport::RpcTransport;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashSet;
/// Kademlia's concurrency parameter: how many `find_node` RPCs a lookup keeps
/// in flight at once.
const ALPHA: usize = 3;

pub async fn lookup_node<T, U, A>(rpc: &Rpc<T, U, A>, target: NodeId) -> Vec<Contact>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    // contacts we know closest to target
    let mut candidates = rpc.close_nodes().close_nodes(target);
    candidates.sort_by(|a, b| xor_distance_cmp(a.id, b.id, target));
    candidates.truncate(K);

    // ids we have already fired a query at: in flight or answered, never re-sent
    let mut queried: HashSet<NodeId> = HashSet::new();
    // up to ALPHA find_node calls resolving concurrently
    let mut in_flight = FuturesUnordered::new();

    loop {
        // top the pipeline back up to ALPHA with the closest un-queried nodes
        while in_flight.len() < ALPHA {
            let Some(next) = candidates
                .iter()
                .find(|contact| !queried.contains(&contact.id))
                .copied()
            else {
                break;
            };

            queried.insert(next.id);
            in_flight.push(async move { rpc.find_node(next.address, target).await });
        }

        // nothing running and nothing left to start: the K closest are settled
        let Some(new_contacts) = in_flight.next().await else {
            break;
        };

        // every contact a response teaches us about is worth offering to the
        // routing table, whether or not it ends up among the K closest here
        for contact in &new_contacts {
            rpc.close_nodes().maybe_add_contact(*contact);
        }

        candidates.extend(new_contacts);
        // sort by distance using Olles XOR thingamajig with a closure (rust voodoo)
        candidates.sort_by(|a, b| xor_distance_cmp(a.id, b.id, target));
        // flatline duplicate chooms (same id => same distance => adjacent here)
        candidates.dedup_by_key(|contact| contact.id);
        // yoink K closest contacts
        candidates.truncate(K);
    }

    candidates
}

pub async fn lookup_value<T, U, A>(rpc: &Rpc<T, U, A>, key: Key) -> Option<Vec<u8>>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    let candidates = lookup_node(rpc, key).await;

    if candidates.is_empty() {
        return None;
    }

    let mut in_flight = FuturesUnordered::new();
    let mut candidates = candidates.into_iter();

    loop {
        while in_flight.len() < ALPHA {
            let Some(next) = candidates.next() else {
                break;
            };

            in_flight.push(async move { rpc.find_value(next.address, key).await });
        }

        let Some(reply) = in_flight.next().await else {
            break;
        };

        match reply {
            FindValue::Value(value) => {
                if key_for_value(&value) == key {
                    return Some(value);
                }
                // Invalid content for this key. Ignore it and continue looking
            }
            // every contact a response teaches us about is worth offering
            // to the routing table, whether or not the value turns up
            FindValue::Closest(contacts) => {
                for contact in &contacts {
                    rpc.close_nodes().maybe_add_contact(*contact);
                }
            }
        }
    }

    None
}
pub async fn store_value<T, U, A>(rpc: &Rpc<T, U, A>, value: Vec<u8>) -> Key
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    // K = SHA256(V).
    let key = key_for_value(&value);

    // Find the nodes closest to the key.
    let mut targets = lookup_node(rpc, key).await;

    // Our routing table does not contain ourselves, but we may still
    // be one of the K closest nodes to this key.
    targets.push(rpc.my_contact());

    // Select the actual K closest nodes, including ourselves.
    targets.sort_by(|a, b| xor_distance_cmp(a.id, b.id, key));
    targets.dedup_by_key(|contact| contact.id);
    targets.truncate(K);

    // Replicate the value to each of the K closest nodes.
    for target in targets {
        rpc.store(target.address, key, value.clone()).await;
    }

    key
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
                FindValue::Value(self.value.clone())
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

        assert_eq!(result, Some(value));
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
}
