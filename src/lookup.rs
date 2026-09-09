use crate::close_nodes::{xor_distance_cmp, CloseNodes, Contact, NodeId, K};
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;
use std::collections::HashSet;

// alpha = 1 to start
pub async fn lookup_node<T, A>(
    rpc: &Rpc<T, A>,
    target: NodeId,
) -> Vec<Contact>
where
    T: RpcTransport,
    A: CloseNodes,
{
    let mut candidates = rpc.close_nodes().close_nodes(target);
    let mut queried: HashSet<NodeId> = HashSet::new();

    loop {
        let Some(next) = candidates
            .iter()
            .find(|contact| !queried.contains(&contact.id))
            .copied()
        else {
            break;
        };

        queried.insert(next.id);

        let new_contacts = rpc.find_node(next.address, target).await;

        candidates.extend(new_contacts);

        candidates.sort_by(|a, b| xor_distance_cmp(a.id, b.id, target));
        candidates.dedup_by_key(|contact| contact.id);
        candidates.truncate(K);
    }

    candidates
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
        ) -> Vec<u8> {
            if address == self.a.address {
                bincode::serialize(&vec![self.b]).unwrap()
            } else if address == self.b.address {
                bincode::serialize(&vec![self.c]).unwrap()
            } else {
                bincode::serialize(&Vec::<Contact>::new()).unwrap()
            }
        }
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

        let close_nodes = FakeCloseNodes {
            initial: vec![a],
        };

        let rpc = Rpc::new(transport, close_nodes);

        let result = lookup_node(&rpc, target).await;

        assert_eq!(result, vec![c, b, a]);
    }
}