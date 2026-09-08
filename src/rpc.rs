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
            self.transport.send_receive(b"PING".to_vec(), peer),
        )
        .await;

        match res {
            Ok(receive_payload) => receive_payload == b"PONG",
            Err(_) => false,
        }
    }

    /// Ask `peer` to store `value` under `key`.
    pub async fn store(&self, peer: SocketAddr, key: Key, value: Vec<u8>) {
        // NOTE lab spec allows for tcp transport of values, not forcing udp only
        todo!("encode STORE, send_receive, decode the reply")
    }

    /// Ask `peer` for the contacts it knows closest to `target`.
    pub async fn find_node(&self, peer: SocketAddr, target: NodeId) -> Vec<Contact> {
        todo!("encode FIND_NODE, send_receive, decode the reply")
    }

    /// Ask `peer` for `key`, falling back to its closest known contacts.
    pub async fn find_value(&self, peer: SocketAddr, key: Key) -> FindValue {
        // NOTE lab spec allows for tcp transport of values, not forcing udp only

        todo!("encode FIND_VALUE, send_receive, decode the reply")
    }
}
