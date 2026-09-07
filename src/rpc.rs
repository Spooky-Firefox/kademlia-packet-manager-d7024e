use crate::rpc_transport_trait::RpcTransport;

/// 160-bit Kademlia node id.
pub type NodeId = [u8; 20];
/// 160-bit key in the same space as [`NodeId`].
pub type Key = [u8; 20];

/// A routing-table entry: who a node is, and where to reach it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Contact<Address> {
    pub id: NodeId,
    pub address: Address,
}

/// Result of a FIND_VALUE: either the value itself, or the closest contacts
/// the peer knows about if it does not hold the key.
#[derive(Clone, Debug)]
pub enum FindValue<Address> {
    Value(Vec<u8>),
    Closest(Vec<Contact<Address>>),
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
//
// `Address` is a method-level generic rather than a parameter on `Rpc` itself,
// so one `Rpc` can talk to a transport that implements `RpcTransport` for more
// than one address type, and `Rpc` needs no `PhantomData`.
#[allow(unused_variables)]
impl<T> Rpc<T> {
    /// Probe `peer` for liveness.
    pub async fn ping<Address>(&self, peer: Address)
    where
        T: RpcTransport<Address>,
    {
        todo!("encode PING, send_receive, decode the reply")
    }

    /// Ask `peer` to store `value` under `key`.
    pub async fn store<Address>(&self, peer: Address, key: Key, value: Vec<u8>)
    where
        T: RpcTransport<Address>,
    {
        // NOTE lab spec allows for tcp transport of values, not forcing udp only
        todo!("encode STORE, send_receive, decode the reply")
    }

    /// Ask `peer` for the contacts it knows closest to `target`.
    pub async fn find_node<Address>(&self, peer: Address, target: NodeId) -> Vec<Contact<Address>>
    where
        T: RpcTransport<Address>,
    {
        todo!("encode FIND_NODE, send_receive, decode the reply")
    }

    /// Ask `peer` for `key`, falling back to its closest known contacts.
    pub async fn find_value<Address>(&self, peer: Address, key: Key) -> FindValue<Address>
    where
        T: RpcTransport<Address>,
    {
        // NOTE lab spec allows for tcp transport of values, not forcing udp only

        todo!("encode FIND_VALUE, send_receive, decode the reply")
    }
}
