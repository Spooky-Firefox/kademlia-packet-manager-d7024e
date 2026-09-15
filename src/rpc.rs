use crate::close_nodes::{CloseNodes, Contact, Key, NodeId};
use crate::handle_rpc::Method;
use crate::rpc_transport::RpcTransport;
use log::trace;
use std::net::SocketAddr;
use std::time::Duration;

/// How long to wait on a reply before giving up on a peer.
///
/// The datagram transports resend on silence and give up on their own, so this
/// is a backstop for the ones that do not — and a bound on how long a single
/// unreachable peer can hold up a lookup.
const RPC_TIMEOUT: Duration = Duration::from_secs(2);

/// Result of a FIND_VALUE: either the value itself, or the closest contacts
/// the peer knows about if it does not hold the key.
#[derive(Clone, Debug)]
pub enum FindValue {
    Value(Vec<u8>),
    Closest(Vec<Contact>),
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
    /// Every request carries it. A peer that answers a question has met a
    /// live node and should remember it, and the address a datagram arrives
    /// from is not enough on its own: routing needs the [`NodeId`] too.
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

// TODO: drop this once the stubs below have real bodies.
#[allow(unused_variables)]
impl<T: RpcTransport, U: RpcTransport, A: CloseNodes> Rpc<T, U, A> {
    /// Tag `body` as `method`, send it, and hand back the reply body.
    ///
    /// `None` for a timeout or a transport error. The four RPCs differ only in
    /// what they put in the body and what they make of the answer, so the
    /// framing and the failure handling live here once.
    async fn call<X: RpcTransport>(
        &self,
        transport: &X,
        method: Method,
        body: Vec<u8>,
        peer: SocketAddr,
    ) -> Option<Vec<u8>> {
        let mut request = method.tag().to_vec();
        request.extend(body);

        match tokio::time::timeout(RPC_TIMEOUT, transport.send_receive(request, peer)).await {
            Ok(Ok(reply)) => Some(reply),
            Ok(Err(e)) => {
                trace!("{method:?} to {peer} failed: {e}");
                None
            }
            Err(_) => {
                trace!("{method:?} to {peer} timed out");
                None
            }
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
        // resolves this future with the reply that carried it back.
        let body = crate::handle_rpc::ping::encode_request(self.my_contact)?;
        let reply = self.call(&self.transport, Method::Ping, body, peer).await?;
        crate::handle_rpc::ping::decode_reply(&reply)
    }

    /// Ask `peer` to store `value` under `key`.
    pub async fn store(&self, peer: SocketAddr, key: Key, value: Vec<u8>) -> bool {
        // NOTE lab spec allows for tcp transport of values, not forcing udp only
        let Some(body) = crate::handle_rpc::store::encode_request(self.my_contact, key, value)
        else {
            return false;
        };
        let reply = self
            .call(&self.robust_transport, Method::Store, body, peer)
            .await;

        matches!(reply, Some(bytes) if bytes == crate::handle_rpc::store::STORED)
    }

    /// Ask `peer` for the contacts it knows closest to `target`.
    ///
    /// An empty vector is every kind of "nothing useful came back": an
    /// unreachable peer, a garbled reply, or a peer that genuinely knows
    /// nobody. A lookup treats all three the same — it moves on to the next
    /// candidate — so they do not need telling apart here.
    pub async fn find_node(&self, peer: SocketAddr, target: NodeId) -> Vec<Contact> {
        let Some(body) = crate::handle_rpc::find_node::encode_request(self.my_contact, target)
        else {
            return Vec::new();
        };
        let Some(reply) = self.call(&self.transport, Method::FindNode, body, peer).await else {
            return Vec::new();
        };
        crate::handle_rpc::find_node::decode_reply(&reply).unwrap_or_default()
    }

    /// Ask `peer` for `key`, falling back to its closest known contacts.
    pub async fn find_value(&self, peer: SocketAddr, key: Key) -> FindValue {
        // NOTE lab spec allows for tcp transport of values, not forcing udp only

        todo!("encode FIND_VALUE, send_receive, decode the reply")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[tokio::test]
    async fn find_node_sends_request_and_decodes_contacts() {
        let target = [1u8; 20];

        let peer: SocketAddr = "127.0.0.1:8000".parse().unwrap();

        let expected_contacts = vec![
            Contact {
                id: [2u8; 20],
                address: "127.0.0.1:8001".parse().unwrap(),
            },
            Contact {
                id: [3u8; 20],
                address: "127.0.0.1:8002".parse().unwrap(),
            },
        ];

        let me = Contact {
            id: [1u8; 20],
            address: "127.0.0.1:8010".parse().unwrap(),
        };

        let mut expected_payload = crate::handle_rpc::Method::FindNode.tag().to_vec();
        expected_payload.extend(crate::handle_rpc::find_node::encode_request(me, target).unwrap());

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

        

        let rpc = Rpc::new(me, transport, robust_transport, FakeCloseNodes);

        let contacts = rpc.find_node(peer, target).await;

        assert_eq!(contacts, expected_contacts);
    }
}
