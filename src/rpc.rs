use crate::close_nodes::{Contact, Key, NodeId};
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
pub struct Rpc<T> {
    transport: T,
}

impl<T> Rpc<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    pub fn transport(&self) -> &T {
        &self.transport
    }
}

// TODO: drop this once the stubs below have real bodies.
#[allow(unused_variables)]
impl<T: RpcTransport> Rpc<T> {
    /// Probe `peer` for liveness.
    pub async fn ping(&self, peer: SocketAddr) {
        todo!("encode PING, send_receive, decode the reply")
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
