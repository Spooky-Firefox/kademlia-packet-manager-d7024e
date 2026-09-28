use crate::close_nodes::{CloseNodes, Contact, K, Key, NodeId};
use crate::handle_rpc::Method;
use crate::rpc_transport::RpcTransport;
use log::trace;
use std::collections::HashSet;
use std::io::ErrorKind;
use std::net::SocketAddr;

/// Result of a FIND_VALUE: either the value itself, or the closest contacts
/// the peer knows about if it does not hold the key.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FindValue {
    Value(Vec<u8>),
    Closest(Vec<Contact>),
}

/// Whether a failed call says the peer itself is gone, rather than something
/// on our side (a send error on our own socket, a request we could not encode)
/// or a peer that answered with garbage.
fn peer_is_gone(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::TimedOut | ErrorKind::ConnectionRefused | ErrorKind::ConnectionReset
    )
}

/// The four Kademlia RPCs, issued over some [`RpcTransport`].
#[non_exhaustive]
pub struct Rpc<T, U, A>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    /// Us, as the peer we are calling should record us.
    ///
    /// Only the [`NodeId`] goes on the wire — the responder pairs it with the
    /// address our request arrived from, which it can observe and we cannot
    /// forge. The address is kept here because [`bootstrap`](crate::bootstrap)
    /// and the routing table both want the whole [`Contact`].
    my_contact: Contact,
    transport: T,
    robust_transport: U,
    close_nodes: A,
}

impl<T, U, A> Rpc<T, U, A>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    pub fn new(my_contact: Contact, transport: T, robust_transport: U, close_nodes: A) -> Self {
        Self {
            my_contact,
            transport,
            robust_transport,
            close_nodes,
        }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn close_nodes(&self) -> &A {
        &self.close_nodes
    }

    pub fn my_contact(&self) -> Contact {
        self.my_contact
    }

    pub fn my_id(&self) -> NodeId {
        self.my_contact.id
    }
}

impl<T: RpcTransport, U: RpcTransport, A: CloseNodes> Rpc<T, U, A> {
    /// Frame `body` as `method`, send it, and hand back the reply body.
    ///
    /// The request's mandatory prefix goes on here: our [`NodeId`], so the
    /// responder can learn us as a contact, then the method tag. `None` for any
    /// way the call can fail — the transport owns the deadline (see
    /// [`RetryTransport`](crate::rpc_transport::retry_transport::RetryTransport)),
    /// so a timeout arrives as an ordinary `Err`.
    ///
    /// `known` is the routing-table entry `peer` came from, if any. A call to
    /// it that fails in a way only a gone peer explains drops it from the
    /// table (see [`forget`](Self::forget)).
    ///
    /// The four RPCs differ only in what they put in the body and what they
    /// make of the answer, so the framing and the failure handling live here
    /// once.
    async fn call<X: RpcTransport>(
        &self,
        transport: &X,
        method: Method,
        body: Vec<u8>,
        peer: SocketAddr,
        known: Option<&Contact>,
    ) -> Option<Vec<u8>> {
        let mut request = self.my_contact.id.to_vec();
        request.extend_from_slice(method.tag());
        request.extend(body);

        match transport.send_receive(request, peer).await {
            Ok(reply) => Some(reply),
            Err(e) => {
                trace!("{method:?} to {peer} failed: {e}");
                if let Some(contact) = known
                    && peer_is_gone(&e)
                {
                    self.forget(contact);
                }
                None
            }
        }
    }

    /// Drop `contact` from the routing table as gone — unless the table holds
    /// `K` or fewer contacts. Every removal for a failed call goes through
    /// here, whether from [`call`](Self::call) or a
    /// [liveness round](crate::maintenance::check_liveness).
    ///
    /// A timeout from [`RetryTransport`](crate::rpc_transport::retry_transport::RetryTransport)
    /// is already several unanswered sends, and a wrongly dropped contact that
    /// is still alive comes back the next time it sends us a request. What a
    /// failed call cannot tell apart is a dead peer and our own connection
    /// being down, and in the second case every call fails: the floor keeps
    /// enough of the table to rejoin through once the network is back.
    pub fn forget(&self, contact: &Contact) {
        // `contacts_iter` may repeat a contact, so count distinct ids, and
        // stop as soon as there are enough rather than walking the table.
        let mut distinct = HashSet::new();
        let plenty = self.close_nodes.contacts_iter().any(|known| {
            distinct.insert(known.id);
            distinct.len() > K
        });
        if plenty {
            trace!("removing contact {}", contact.address);
            self.close_nodes.remove_contact(contact);
        } else {
            trace!(
                "keeping contact {}: routing table too small",
                contact.address
            );
        }
    }

    /// Probe `peer` for liveness, and learn whose address it is.
    ///
    /// `Some(id)` is the liveness answer as well as the identity one: a node
    /// that replied is alive. `None` covers every way the probe can fail, none
    /// of which the caller can tell apart on a lossy wire anyway.
    ///
    /// Returning the id is what lets a joining node turn a bare bootstrap
    /// address into a [`Contact`] it can route with.
    pub async fn ping(&self, peer: SocketAddr) -> Option<NodeId> {
        // No request id in the body: the transport frames one and only ever
        // resolves this future with the reply that carried it back. No sender
        // either — that rides ahead of the method tag.
        let reply = self
            .call(&self.transport, Method::Ping, Vec::new(), peer, None)
            .await?;
        crate::handle_rpc::ping::decode_reply(&reply)
    }

    /// Ask `peer` to store `value` under `key`.
    pub async fn store(&self, peer: &Contact, key: Key, value: Vec<u8>) -> bool {
        // NOTE lab spec allows for tcp transport of values, not forcing udp only
        let Some(body) = crate::handle_rpc::store::encode_request(key, value) else {
            return false;
        };
        let reply = self
            .call(
                &self.robust_transport,
                Method::Store,
                body,
                peer.address,
                Some(peer),
            )
            .await;

        matches!(reply, Some(bytes) if bytes == crate::handle_rpc::store::STORED)
    }

    /// Ask `peer` for the contacts it knows closest to `target`.
    ///
    /// An empty vector is every kind of "nothing useful came back": an
    /// unreachable peer, a garbled reply, or a peer that genuinely knows
    /// nobody. A lookup treats all three the same — it moves on to the next
    /// candidate — so they do not need telling apart here.
    pub async fn find_node(&self, peer: &Contact, target: NodeId) -> Vec<Contact> {
        let Some(body) = crate::handle_rpc::find_node::encode_request(target) else {
            return Vec::new();
        };
        let Some(reply) = self
            .call(
                &self.transport,
                Method::FindNode,
                body,
                peer.address,
                Some(peer),
            )
            .await
        else {
            return Vec::new();
        };
        crate::handle_rpc::find_node::decode_reply(&reply).unwrap_or_default()
    }

    /// Ask `peer` for `key`, falling back to its closest known contacts.
    pub async fn find_value(&self, peer: &Contact, key: Key) -> FindValue {
        // NOTE lab spec allows for tcp transport of values, not forcing udp only
        let Ok(body) = bincode::serialize(&key) else {
            return FindValue::Closest(Vec::new());
        };
        let Some(reply) = self
            .call(
                &self.robust_transport,
                Method::FindValue,
                body,
                peer.address,
                Some(peer),
            )
            .await
        else {
            return FindValue::Closest(Vec::new());
        };

        bincode::deserialize::<FindValue>(&reply).unwrap_or(FindValue::Closest(Vec::new()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::dumb_bucket::DumbBucket;
    use std::sync::{Arc, RwLock};

    struct FakeTransport {
        expected_payload: Vec<u8>,
        expected_address: SocketAddr,
        response: Vec<u8>,
    }

    impl RpcTransport for FakeTransport {
        async fn send_receive(
            &self,
            payload: Vec<u8>,
            address: SocketAddr,
        ) -> std::io::Result<Vec<u8>> {
            assert_eq!(payload, self.expected_payload);
            assert_eq!(address, self.expected_address);

            Ok(self.response.clone())
        }
    }
    struct FakeCloseNodes;

    impl CloseNodes for FakeCloseNodes {
        fn close_nodes(&self, _id: NodeId) -> Vec<Contact> {
            Vec::new()
        }

        fn maybe_add_contact(&self, _contact: Contact) {}

        fn contacts_iter(&self) -> impl std::iter::Iterator<Item = Contact> {
            unimplemented!();
            // so the compiler dont complain
            #[allow(unreachable_code)]
            Vec::new().into_iter()
        }
        fn remove_contact(&self, _contact: &Contact) {}
    }

    /// The node issuing the requests under test. Only its id reaches the wire;
    /// the address is what the responder would observe instead.
    fn me() -> Contact {
        Contact {
            id: [9u8; 32],
            address: "127.0.0.1:8010".parse().unwrap(),
        }
    }

    /// The routing-table entry the requests under test are sent to.
    fn peer_contact(address: SocketAddr) -> Contact {
        Contact {
            id: [7u8; 32],
            address,
        }
    }

    #[tokio::test]
    async fn find_node_sends_request_and_decodes_contacts() {
        let my_id = me().id;
        let target = [1u8; 32];

        let peer: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        let expected_contacts = vec![
            Contact {
                id: [2u8; 32],
                address: "127.0.0.1:8001".parse().unwrap(),
            },
            Contact {
                id: [3u8; 32],
                address: "127.0.0.1:8002".parse().unwrap(),
            },
        ];

        // The requester's own id comes right after the transport's request
        // id, ahead of the method tag, so the responder can learn us as a
        // contact.
        let mut expected_payload = my_id.to_vec();
        expected_payload.extend_from_slice(crate::handle_rpc::Method::FindNode.tag());

        expected_payload.extend(bincode::serialize(&target).unwrap());

        let response = bincode::serialize(&expected_contacts).unwrap();

        let transport = FakeTransport {
            expected_payload,
            expected_address: peer,
            response,
        };

        // find_node never touches the robust transport, so a transport that
        // asserts nothing and is never called stands in for it here.
        let robust_transport = FakeTransport {
            expected_payload: Vec::new(),
            expected_address: peer,
            response: Vec::new(),
        };

        let rpc = Rpc::new(me(), transport, robust_transport, FakeCloseNodes);

        let contacts = rpc.find_node(&peer_contact(peer), target).await;

        assert_eq!(contacts, expected_contacts);
    }

    #[tokio::test]
    async fn find_value_sends_request_and_decodes_value() {
        let my_id = me().id;
        let key = [1u8; 32];
        let peer: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        let expected_reply = FindValue::Value(b"hello".to_vec());

        let mut expected_payload = my_id.to_vec();
        expected_payload.extend_from_slice(crate::handle_rpc::Method::FindValue.tag());
        expected_payload.extend(bincode::serialize(&key).unwrap());

        let response = bincode::serialize(&expected_reply).unwrap();

        let transport = FakeTransport {
            expected_payload: Vec::new(),
            expected_address: peer,
            response: Vec::new(),
        };

        let robust_transport = FakeTransport {
            expected_payload,
            expected_address: peer,
            response,
        };

        let rpc = Rpc::new(me(), transport, robust_transport, FakeCloseNodes);

        let result = rpc.find_value(&peer_contact(peer), key).await;

        assert_eq!(result, expected_reply);
    }

    #[tokio::test]
    async fn find_value_decodes_closest_contacts() {
        let my_id = me().id;
        let key = [1u8; 32];
        let peer: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        let contacts = vec![Contact {
            id: [2u8; 32],
            address: "127.0.0.1:8001".parse().unwrap(),
        }];

        let expected_reply = FindValue::Closest(contacts);

        let mut expected_payload = my_id.to_vec();
        expected_payload.extend_from_slice(crate::handle_rpc::Method::FindValue.tag());
        expected_payload.extend(bincode::serialize(&key).unwrap());

        let response = bincode::serialize(&expected_reply).unwrap();

        let transport = FakeTransport {
            expected_payload: Vec::new(),
            expected_address: peer,
            response: Vec::new(),
        };

        let robust_transport = FakeTransport {
            expected_payload,
            expected_address: peer,
            response,
        };

        let rpc = Rpc::new(me(), transport, robust_transport, FakeCloseNodes);

        let result = rpc.find_value(&peer_contact(peer), key).await;

        assert_eq!(result, expected_reply);
    }

    /// Fails every call with `kind`.
    struct FailingTransport(ErrorKind);

    impl RpcTransport for FailingTransport {
        async fn send_receive(
            &self,
            _payload: Vec<u8>,
            _address: SocketAddr,
        ) -> std::io::Result<Vec<u8>> {
            Err(std::io::Error::from(self.0))
        }
    }

    /// An `Rpc` whose every call fails with `kind`, over a routing table
    /// holding `peer` and `others` more contacts.
    fn failing_rpc(
        kind: ErrorKind,
        peer: Contact,
        others: usize,
    ) -> Rpc<FailingTransport, FailingTransport, DumbBucket> {
        let mut contacts = vec![peer];
        contacts.extend((0..others).map(|i| Contact {
            id: [100 + i as u8; 32],
            address: SocketAddr::from(([127, 0, 0, 1], 9100 + i as u16)),
        }));
        let table = DumbBucket {
            contacts: Arc::new(RwLock::new(contacts)),
        };
        Rpc::new(me(), FailingTransport(kind), FailingTransport(kind), table)
    }

    fn holds(rpc: &Rpc<FailingTransport, FailingTransport, DumbBucket>, peer: &Contact) -> bool {
        rpc.close_nodes().contacts.read().unwrap().contains(peer)
    }

    #[tokio::test]
    async fn a_peer_that_is_gone_is_removed() {
        let peer = peer_contact("127.0.0.1:8000".parse().unwrap());
        for kind in [
            ErrorKind::TimedOut,
            ErrorKind::ConnectionRefused,
            ErrorKind::ConnectionReset,
        ] {
            let rpc = failing_rpc(kind, peer, K);
            assert!(rpc.find_node(&peer, [0u8; 32]).await.is_empty());
            assert!(!holds(&rpc, &peer), "kept after {kind:?}");
        }
    }

    #[tokio::test]
    async fn every_rpc_to_a_known_contact_removes_it() {
        let peer = peer_contact("127.0.0.1:8000".parse().unwrap());

        let rpc = failing_rpc(ErrorKind::TimedOut, peer, K);
        assert!(!rpc.store(&peer, [0u8; 32], b"v".to_vec()).await);
        assert!(!holds(&rpc, &peer));

        let rpc = failing_rpc(ErrorKind::TimedOut, peer, K);
        assert_eq!(
            rpc.find_value(&peer, [0u8; 32]).await,
            FindValue::Closest(Vec::new())
        );
        assert!(!holds(&rpc, &peer));
    }

    #[tokio::test]
    async fn a_small_table_keeps_a_failing_peer() {
        let peer = peer_contact("127.0.0.1:8000".parse().unwrap());
        // K contacts in all, the peer included: removing it would take the
        // table below the floor.
        let rpc = failing_rpc(ErrorKind::TimedOut, peer, K - 1);
        rpc.find_node(&peer, [0u8; 32]).await;
        assert!(holds(&rpc, &peer));
    }

    #[tokio::test]
    async fn a_failure_on_our_side_keeps_the_peer() {
        let peer = peer_contact("127.0.0.1:8000".parse().unwrap());
        for kind in [
            ErrorKind::InvalidData,
            ErrorKind::NotConnected,
            ErrorKind::AddrNotAvailable,
        ] {
            let rpc = failing_rpc(kind, peer, K);
            rpc.find_node(&peer, [0u8; 32]).await;
            assert!(holds(&rpc, &peer), "removed after {kind:?}");
        }
    }

    #[tokio::test]
    async fn a_failed_ping_removes_nothing() {
        let peer = peer_contact("127.0.0.1:8000".parse().unwrap());
        let rpc = failing_rpc(ErrorKind::TimedOut, peer, K);
        // A ping names only an address, which may not be in the table at all.
        assert_eq!(rpc.ping(peer.address).await, None);
        assert!(holds(&rpc, &peer));
    }
}
