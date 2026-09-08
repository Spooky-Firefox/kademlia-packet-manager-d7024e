use std::net::SocketAddr;

/// 160-bit Kademlia node id.
pub type NodeId = [u8; 20];
/// 160-bit key in the same space as [`NodeId`].
pub type Key = [u8; 20];

/// A routing-table entry: who a node is, and where to reach it.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Contact {
    pub id: NodeId,
    pub address: SocketAddr,
}

pub trait CloseNodes {
    fn close_nodes(&self, id: NodeId) -> Vec<Contact>;
}
