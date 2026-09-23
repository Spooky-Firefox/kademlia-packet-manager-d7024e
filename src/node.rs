use crate::close_nodes::{Contact, NodeId, RecommendedCloseNodes, recommended};
use crate::handle_rpc::{self, Context};
use crate::hashing::node_id_from_address;
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;
use crate::rpc_transport::networked_debug_transport::{
    Network, NetworkedDebugTransport, NetworkedStreamTransport,
};
use crate::rpc_transport::tcp_transport::TcpTransport;
use crate::rpc_transport::udp_transport::UdpTransport;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub type NodeRpc<T, U> = Rpc<T, U, Arc<RecommendedCloseNodes>>;

pub struct Node<T, U>
where
    T: RpcTransport,
    U: RpcTransport,
{
    id: NodeId,
    address: SocketAddr,
    rpc: NodeRpc<T, U>,
    context: Arc<Context<Arc<RecommendedCloseNodes>>>,
    _server_tasks: Vec<JoinHandle<()>>,
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

    pub fn rpc(&self) -> &NodeRpc<T, U> {
        &self.rpc
    }
}

impl RealNode {
    pub async fn bind(bind_address: SocketAddr) -> io::Result<Self> {
        let udp_socket = UdpSocket::bind(bind_address).await?;
        let address = udp_socket.local_addr()?;

        let id = node_id_from_address(address);

        let tcp_listener = TcpListener::bind(address).await?;

        let routing = Arc::new(recommended(id));

        let context = Context::new(id, Arc::clone(&routing));

        let (request_tx, request_rx) = mpsc::channel(64);

        let udp_transport = UdpTransport::with_requests(udp_socket, request_tx);

        let server_task = tokio::spawn(handle_rpc::serve(
            Arc::clone(&context),
            request_rx,
            udp_transport.socket(),
            tcp_listener,
        ));

        let rpc = Rpc::new(
            Contact { id, address },
            udp_transport,
            TcpTransport,
            Arc::clone(&routing),
        );

        Ok(Self {
            id,
            address,
            rpc,
            context,
            _server_tasks: vec![server_task],
        })
    }
}
impl FakeNode {
    pub fn new(network: &Network) -> Self {
        let endpoint = network.bind_any();
        let address = endpoint.local_addr();

        let id = node_id_from_address(address);

        let routing = Arc::new(recommended(id));

        let context = Context::new(id, Arc::clone(&routing));

        let (request_tx, request_rx) = mpsc::channel(64);

        let datagram_transport = NetworkedDebugTransport::with_requests(endpoint, request_tx);

        let shared_endpoint = datagram_transport.socket();

        let server_task = tokio::spawn(handle_rpc::serve(
            Arc::clone(&context),
            request_rx,
            Arc::clone(&shared_endpoint),
            shared_endpoint,
        ));

        let stream_transport = NetworkedStreamTransport::new(network.clone());

        let rpc = Rpc::new(
            Contact { id, address },
            datagram_transport,
            stream_transport,
            Arc::clone(&routing),
        );

        Self {
            id,
            address,
            rpc,
            context,
            _server_tasks: vec![server_task],
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

        assert!(a.rpc().store(b.address(), key, value.clone(),).await);

        let result = a.rpc().find_value(b.address(), key).await;

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
        assert!(a.rpc().store(c.address(), key, value.clone(),).await);

        let found = crate::lookup::lookup_value(a.rpc(), key).await;

        assert_eq!(found, Some(value));
    }
    #[tokio::test]
    async fn fake_nodes_can_ping_each_other() {
        let network = Network::new();

        let a = FakeNode::new(&network);

        let b = FakeNode::new(&network);

        assert_eq!(a.rpc().ping(b.address()).await, Some(b.id()));
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

        assert!(a.rpc().store(b.address(), key, value.clone()).await);

        let result = a.rpc().find_value(b.address(), key).await;

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
            a.rpc().find_value(a.address(), key).await,
            crate::rpc::FindValue::Value(value.clone())
        );

        assert_eq!(
            a.rpc().find_value(b.address(), key).await,
            crate::rpc::FindValue::Value(value.clone())
        );

        assert_eq!(
            a.rpc().find_value(c.address(), key).await,
            crate::rpc::FindValue::Value(value)
        );
    }
}
