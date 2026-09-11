use crate::close_nodes::{CloseNodes, Contact, K, Key, NodeId, xor_distance_cmp};
use crate::rpc::{FindValue, Rpc};
use crate::rpc_transport::RpcTransport;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashSet;

/// Kademlia's concurrency parameter: how many `find_node` RPCs a lookup keeps
/// in flight at once.
const ALPHA: usize = 3;

pub async fn lookup_node<T, A>(rpc: &Rpc<T, A>, target: NodeId) -> Vec<Contact>
where
    T: RpcTransport,
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

pub async fn lookup_value<T, A>(rpc: &Rpc<T, A>, key: Key) -> Option<Vec<u8>>
where
    T: RpcTransport,
    A: CloseNodes,
{
    let mut candidates = rpc.close_nodes().close_nodes(key);

    candidates.sort_by(|a, b| xor_distance_cmp(a.id, b.id, key));
    candidates.truncate(K);

    if candidates.is_empty() {
        return None;
    }

    let mut queried: HashSet<NodeId> = HashSet::new();

    let mut in_flight = FuturesUnordered::new();

    loop {
        while in_flight.len() < ALPHA {
            let Some(next) = candidates
                .iter()
                .find(|contact| !queried.contains(&contact.id))
                .copied()
            else {
                break;
            };

            queried.insert(next.id);

            in_flight.push(async move { rpc.find_value(next.address, key).await });
        }

        let Some(reply) = in_flight.next().await else {
            break;
        };

        match reply {
            FindValue::Value(value) => {
                return Some(value);
            }

            FindValue::Closest(new_contacts) => {
                candidates.extend(new_contacts);

                candidates.sort_by(|a, b| xor_distance_cmp(a.id, b.id, key));
                candidates.dedup_by_key(|contact| contact.id);
                candidates.truncate(K);
            }
        }
    }

    None
}
#[cfg(test)]
mod tests {
    use super::*;

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
    async fn value_lookup_follows_contacts_until_value_is_found() {
        let key = [0u8; 20];

        let a = Contact {
            id: [8u8; 20],
            address: "127.0.0.1:8001".parse().unwrap(),
        };

        let b = Contact {
            id: [4u8; 20],
            address: "127.0.0.1:8002".parse().unwrap(),
        };

        let c = Contact {
            id: [2u8; 20],
            address: "127.0.0.1:8003".parse().unwrap(),
        };

        let value = b"hello".to_vec();

        let transport = FakeValueTransport {
            a,
            b,
            c,
            value: value.clone(),
        };

        let close_nodes = FakeCloseNodes { initial: vec![a] };

        let rpc = Rpc::new(transport, close_nodes);

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
        let key = [0u8; 20];

        let a = Contact {
            id: [8u8; 20],
            address: "127.0.0.1:8001".parse().unwrap(),
        };

        let b = Contact {
            id: [4u8; 20],
            address: "127.0.0.1:8002".parse().unwrap(),
        };

        let transport = FakeMissingValueTransport { a, b };

        let close_nodes = FakeCloseNodes { initial: vec![a] };

        let rpc = Rpc::new(transport, close_nodes);

        let result = lookup_value(&rpc, key).await;

        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn lookup_follows_newly_discovered_contacts() {
        let target = [0u8; 20];

        let a = Contact {
            id: [8u8; 20],
            address: "127.0.0.1:8001".parse().unwrap(),
        };

        let b = Contact {
            id: [4u8; 20],
            address: "127.0.0.1:8002".parse().unwrap(),
        };

        let c = Contact {
            id: [2u8; 20],
            address: "127.0.0.1:8003".parse().unwrap(),
        };

        let transport = FakeTransport { a, b, c };

        let close_nodes = FakeCloseNodes { initial: vec![a] };

        let rpc = Rpc::new(transport, close_nodes);

        let result = lookup_node(&rpc, target).await;

        assert_eq!(result, vec![c, b, a]);
    }
    #[tokio::test]
    async fn lookup_returns_empty_when_no_contacts_are_known() {
        let target = [0u8; 20];

        let dummy = Contact {
            id: [1u8; 20],
            address: "127.0.0.1:8001".parse().unwrap(),
        };

        let transport = FakeTransport {
            a: dummy,
            b: dummy,
            c: dummy,
        };

        let close_nodes = FakeCloseNodes { initial: vec![] };

        let rpc = Rpc::new(transport, close_nodes);

        let result = lookup_node(&rpc, target).await;

        assert!(result.is_empty());
    }
}
