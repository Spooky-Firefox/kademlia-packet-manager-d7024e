use crate::close_nodes::{Contact, Key, NodeId, RecommendedCloseNodes, recommended};
use crate::handle_rpc::{self, Context};
use crate::hashing::node_id_from_address;
use crate::maintenance;
use crate::node_scope;
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;
use crate::rpc_transport::networked_debug_transport::{
    Endpoint, Network, NetworkedDebugTransport, NetworkedStreamTransport,
};
use crate::rpc_transport::tcp_transport::TcpTransport;
use crate::rpc_transport::udp_transport::UdpTransport;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub type NodeRpc<T, U> = Arc<Rpc<T, U, Arc<RecommendedCloseNodes>>>;

pub struct Node<T, U>
where
    T: RpcTransport,
    U: RpcTransport,
{
    id: NodeId,
    address: SocketAddr,
    rpc: NodeRpc<T, U>,
    context: Arc<Context<Arc<RecommendedCloseNodes>>>,
    /// The fake wire this node is on, so [`FakeNode::shutdown`] can take it
    /// off. `None` for a real node.
    network: Option<Network>,
    /// Everything spawned on the node's behalf that would otherwise outlive
    /// it: aborted when the node is dropped.
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

impl<T, U> Drop for Node<T, U>
where
    T: RpcTransport,
    U: RpcTransport,
{
    fn drop(&mut self) {
        for task in self.tasks.get_mut().unwrap().drain(..) {
            task.abort();
        }
    }
}
pub type RealNode = Node<UdpTransport, TcpTransport>;
pub type FakeNode = Node<NetworkedDebugTransport, NetworkedStreamTransport>;

impl<T, U> Node<T, U>
where
    T: RpcTransport,
    U: RpcTransport,
{
    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn contact(&self) -> Contact {
        Contact {
            id: self.id,
            address: self.address,
        }
    }

    pub fn rpc(&self) -> &NodeRpc<T, U> {
        &self.rpc
    }

    /// Run `future` as this node, so what it logs is tagged with this node's
    /// address. See [`node_scope`](crate::node_scope).
    pub async fn scoped<F: Future>(&self, future: F) -> F::Output {
        node_scope::scope(self.address, future).await
    }

    pub fn routing_snapshot(&self) -> (Vec<Contact>, Vec<(usize, Vec<Contact>)>) {
        let routing = self.context.close_nodes.as_ref();

        let siblings = routing.inner().siblings();
        let buckets = routing.inner().fallback().non_empty_buckets();

        (siblings, buckets)
    }

    /// Whether this node holds a value under `key`.
    pub fn holds(&self, key: &Key) -> bool {
        self.context.values.contains_key(key)
    }

    pub fn datastore_snapshot(&self) -> Vec<(Key, usize)> {
        let mut values: Vec<_> = self
            .context
            .values
            .iter()
            .map(|entry| (*entry.key(), entry.value().len()))
            .collect();

        values.sort_by_key(|(key, _)| *key);
        values
    }
}

impl<T, U> Node<T, U>
where
    T: RpcTransport + Send + Sync + 'static,
    U: RpcTransport + Send + Sync + 'static,
{
    pub fn periodic_task(
        &self,
        _republish_interval: std::time::Duration,
        liveness_check_interval: std::time::Duration,
    ) {
        let rpc = Arc::clone(&self.rpc);
        let liveness_task = tokio::spawn(node_scope::scope(self.address, async move {
            loop {
                tokio::time::sleep(maintenance::jittered(liveness_check_interval)).await;
                maintenance::check_liveness(&rpc).await;
            }
        }));
        self.tasks.lock().unwrap().push(liveness_task);

        // TODO republish data
    }
}

impl RealNode {
    pub async fn bind(bind_address: SocketAddr) -> io::Result<Self> {
        let udp_socket = UdpSocket::bind(bind_address).await?;
        let address = udp_socket.local_addr()?;

        let id = node_id_from_address(address);

        let tcp_listener = TcpListener::bind(address).await?;

        // Everything the node spawns from here on is tagged as it.
        Ok(node_scope::sync_scope(address, || {
            let routing = Arc::new(recommended(id));

            let context = Context::new(id, Arc::clone(&routing));

            let (request_tx, request_rx) = mpsc::channel(64);

            let udp_transport = UdpTransport::with_requests(udp_socket, request_tx);

            let server_task = node_scope::spawn(handle_rpc::serve(
                Arc::clone(&context),
                request_rx,
                udp_transport.socket(),
                tcp_listener,
            ));

            let rpc = Arc::new(Rpc::new(
                Contact { id, address },
                udp_transport,
                TcpTransport,
                Arc::clone(&routing),
            ));

            Self {
                id,
                address,
                rpc,
                context,
                network: None,
                tasks: Mutex::new(vec![server_task]),
            }
        }))
    }
}
impl FakeNode {
    pub fn new(network: &Network) -> Self {
        Self::on(network, network.bind_any())
    }

    /// A node at `address`, or `None` if something on `network` already holds
    /// it. Since a node's id is the hash of its address, choosing addresses is
    /// how a simulation chooses where in the id space its nodes land.
    pub fn at(network: &Network, address: SocketAddr) -> Option<Self> {
        Some(Self::on(network, network.bind(address)?))
    }

    fn on(network: &Network, endpoint: Endpoint) -> Self {
        let address = endpoint.local_addr();

        let id = node_id_from_address(address);

        // Everything the node spawns from here on is tagged as it.
        node_scope::sync_scope(address, || {
            let routing = Arc::new(recommended(id));

            let context = Context::new(id, Arc::clone(&routing));

            let (request_tx, request_rx) = mpsc::channel(64);

            let datagram_transport = NetworkedDebugTransport::with_requests(endpoint, request_tx);

            let shared_endpoint = datagram_transport.socket();

            let server_task = node_scope::spawn(handle_rpc::serve(
                Arc::clone(&context),
                request_rx,
                Arc::clone(&shared_endpoint),
                shared_endpoint,
            ));

            let stream_transport = NetworkedStreamTransport::new(network.clone());

            let rpc = Arc::new(Rpc::new(
                Contact { id, address },
                datagram_transport,
                stream_transport,
                Arc::clone(&routing),
            ));

            Self {
                id,
                address,
                rpc,
                context,
                network: Some(network.clone()),
                tasks: Mutex::new(vec![server_task]),
            }
        })
    }

    /// Take the node off the network, as if its host went down: it stops
    /// answering, its address is freed, and its receive and serve loops wind
    /// down. Its remaining tasks are aborted once the node is dropped.
    ///
    /// Dropping the node alone would not do it. The transport's receive loop
    /// holds the endpoint, so the address would stay bound and keep being
    /// served.
    pub fn shutdown(&self) {
        if let Some(network) = &self.network {
            network.unbind(self.address);
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::CloseNodes;

    #[tokio::test]
    async fn two_nodes_can_ping_each_other() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        assert_eq!(a.id(), node_id_from_address(a.address()));
        assert_eq!(b.id(), node_id_from_address(b.address()));

        assert_ne!(a.id(), b.id());

        assert_eq!(a.rpc().ping(b.address()).await, Some(b.id()));
    }

    #[tokio::test]
    async fn two_nodes_can_ping_and_learn_each_other() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        assert_eq!(a.rpc().ping(b.address()).await, Some(b.id()));

        let contacts = b.rpc().close_nodes().close_nodes(a.id());

        assert!(
            contacts
                .iter()
                .any(|contact| { contact.id == a.id() && contact.address == a.address() })
        );
    }

    #[tokio::test]
    async fn node_can_discover_another_node_through_lookup() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let c = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        // B sends PING to A, so A learns B.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        // C sends PING to B, so B learns C.
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        let found = crate::lookup::lookup_node(a.rpc(), c.id()).await;

        assert!(
            found
                .iter()
                .any(|contact| { contact.id == c.id() && contact.address == c.address() })
        );
    }

    #[tokio::test]
    async fn real_nodes_can_store_and_find_value() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let value = vec![0xAB; 5000];
        let key = crate::hashing::key_for_value(&value);

        assert!(a.rpc().store(&b.contact(), key, value.clone(),).await);

        let result = a.rpc().find_value(&b.contact(), key).await;

        assert_eq!(result, crate::rpc::FindValue::Value(value));
    }

    #[tokio::test]
    async fn node_can_find_value_through_network() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let c = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let value = vec![0xAB; 5000];
        let key = crate::hashing::key_for_value(&value);

        // Make A learn B.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        // Make B learn C.
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        // Preload C with the value.
        assert!(a.rpc().store(&c.contact(), key, value.clone(),).await);
        let found: Option<(Contact, Vec<u8>)> = crate::lookup::lookup_value(a.rpc(), key).await;

        let c_contact = Contact {
            id: c.id(),
            address: c.address(),
        };

        assert_eq!(found, Some((c_contact, value)));
    }
    #[tokio::test]
    async fn fake_nodes_can_ping_each_other() {
        let network = Network::new();

        let a = FakeNode::new(&network);

        let b = FakeNode::new(&network);

        assert_eq!(a.rpc().ping(b.address()).await, Some(b.id()));
    }
    #[tokio::test]
    async fn a_fake_node_can_be_placed_at_a_chosen_address() {
        let network = Network::new();
        let address: SocketAddr = "10.1.2.3:4567".parse().unwrap();

        let a = FakeNode::at(&network, address).expect("the address is free");
        assert_eq!(a.address(), address);
        assert_eq!(a.id(), node_id_from_address(address));
        assert!(FakeNode::at(&network, address).is_none(), "already taken");

        let b = FakeNode::new(&network);
        assert_eq!(b.rpc().ping(address).await, Some(a.id()));
    }

    #[tokio::test]
    async fn a_shut_down_fake_node_stops_answering_and_frees_its_address() {
        let network = Network::new();

        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);
        let b_address = b.address();
        assert_eq!(a.rpc().ping(b_address).await, Some(b.id()));

        b.shutdown();

        assert_eq!(a.rpc().ping(b_address).await, None);
        assert!(network.bind(b_address).is_some());
    }

    #[tokio::test]
    async fn fake_node_can_discover_another_node_through_lookup() {
        let network = Network::new();

        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);
        let c = FakeNode::new(&network);

        // Make A learn B.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        // Make B learn C.
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        let found = crate::lookup::lookup_node(a.rpc(), c.id()).await;

        assert!(
            found
                .iter()
                .any(|contact| { contact.id == c.id() && contact.address == c.address() })
        );
    }
    #[tokio::test]
    async fn fake_nodes_can_store_and_find_value() {
        let network = Network::new();

        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);

        let value = vec![0xAB; 5000];
        let key = crate::hashing::key_for_value(&value);

        assert!(!b.holds(&key));
        assert!(a.rpc().store(&b.contact(), key, value.clone()).await);
        assert!(b.holds(&key));

        let result = a.rpc().find_value(&b.contact(), key).await;

        assert_eq!(result, crate::rpc::FindValue::Value(value));
    }
    #[tokio::test]
    async fn high_level_store_replicates_value() {
        let network = Network::new();

        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);
        let c = FakeNode::new(&network);

        // Make A learn B.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        // Make B learn C.
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        let value = b"hello kademlia".to_vec();
        let expected_key = crate::hashing::key_for_value(&value);

        let key = crate::lookup::store_value(a.rpc(), value.clone()).await;

        assert_eq!(key, expected_key);

        // There are only 3 nodes and k = 10, so all three should
        // be among the k closest and receive the value.
        assert_eq!(
            a.rpc().find_value(&a.contact(), key).await,
            crate::rpc::FindValue::Value(value.clone())
        );

        assert_eq!(
            a.rpc().find_value(&b.contact(), key).await,
            crate::rpc::FindValue::Value(value.clone())
        );

        assert_eq!(
            a.rpc().find_value(&c.contact(), key).await,
            crate::rpc::FindValue::Value(value)
        );
    }
}
