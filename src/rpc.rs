use crate::close_nodes::{CloseNodes, Contact, Key, NodeId};
use crate::rpc_transport::RpcTransport;
use std::net::SocketAddr;

/// Result of a FIND_VALUE: either the value itself, or the closest contacts
/// the peer knows about if it does not hold the key.
#[derive(Clone, Debug)]
pub enum FindValue {
    Value(Vec<u8>),
    Closest(Vec<Contact>),
}

/// The four Kademlia RPCs, issued over some [`RpcTransport`].
#[non_exhaustive]
pub struct Rpc<T, A>
where
    T: RpcTransport,
    A: CloseNodes,
{
    transport: T,
    close_nodes: A,
}

impl<T, A> Rpc<T, A>
where
    T: RpcTransport,
    A: CloseNodes,
{
    pub fn new(transport: T, close_nodes: A) -> Self {
        Self {
            transport,
            close_nodes,
        }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }

    pub fn close_nodes(&self) -> &A {
        &self.close_nodes
    }
}

// TODO: drop this once the stubs below have real bodies.
#[allow(unused_variables)]
impl<T: RpcTransport, A: CloseNodes> Rpc<T, A> {
    /// Probe `peer` for liveness.
    pub async fn ping(&self, peer: SocketAddr) -> bool {
        // No request id in the body: the transport frames one and only ever
        // resolves this future with the reply that carried it back.
        let res = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.transport
                .send_receive(crate::handle_rpc::Method::Ping.tag().to_vec(), peer),
        )
        .await;

        match res {
            Ok(receive_payload) => receive_payload == crate::handle_rpc::ping::PONG,
            Err(_) => false,
        }
    }

    /// Ask `peer` to store `value` under `key`.
    pub async fn store(&self, peer: SocketAddr, key: Key, value: Vec<u8>) -> bool {
        // NOTE lab spec allows for tcp transport of values, not forcing udp only
        let Ok(encoded) = bincode::serialize(&(key, value)) else {
            return false;
        };
        let mut request = crate::handle_rpc::Method::Store.tag().to_vec();
        request.extend(encoded);

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.transport.send_receive(request, peer),
        )
        .await;

        matches!(response, Ok(bytes) if bytes == crate::handle_rpc::store::STORED)
    }

    /// Ask `peer` for the contacts it knows closest to `target`.
    pub async fn find_node(&self, peer: SocketAddr, target: NodeId) -> Vec<Contact> {
        let encoded_target = match bincode::serialize(&target) {
            Ok(bytes) => bytes,
            Err(_) => return Vec::new(),
        };

        let mut request = crate::handle_rpc::Method::FindNode.tag().to_vec();
        request.extend(encoded_target);

        let response = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            self.transport.send_receive(request, peer),
        )
        .await;

        let response = match response {
            Ok(bytes) => bytes,
            Err(_) => return Vec::new(),
        };

        match bincode::deserialize::<Vec<Contact>>(&response) {
            Ok(contacts) => contacts,
            Err(_) => Vec::new(),
        }
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
        async fn send_receive(&self, payload: Vec<u8>, address: SocketAddr) -> Vec<u8> {
            assert_eq!(payload, self.expected_payload);
            assert_eq!(address, self.expected_address);

            self.response.clone()
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

        let mut expected_payload = crate::handle_rpc::Method::FindNode.tag().to_vec();

        expected_payload.extend(bincode::serialize(&target).unwrap());

        let response = bincode::serialize(&expected_contacts).unwrap();

        let transport = FakeTransport {
            expected_payload,
            expected_address: peer,
            response,
        };

        let rpc = Rpc::new(transport, FakeCloseNodes);

        let contacts = rpc.find_node(peer, target).await;

        assert_eq!(contacts, expected_contacts);
    }
}
