use crate::close_nodes::CloseNodes;
use crate::close_nodes::{NodeId, RecommendedCloseNodes, recommended};
use crate::handle_rpc::{self, Context};
use crate::rpc::Rpc;
use crate::rpc_transport::tcp_transport::TcpTransport;
use crate::rpc_transport::udp_transport::UdpTransport;

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinHandle;

use std::io;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;

pub type NodeRpc = Rpc<UdpTransport, TcpTransport, Arc<RecommendedCloseNodes>>;

pub struct Node {
    id: NodeId,
    address: SocketAddr,
    rpc: NodeRpc,
    context: Arc<Context<Arc<RecommendedCloseNodes>>>,
    _server_task: JoinHandle<()>,
}

impl Node {
    pub async fn bind(id: NodeId, bind_address: SocketAddr) -> io::Result<Self> {
        let udp_socket = UdpSocket::bind(bind_address).await?;
        let address = udp_socket.local_addr()?;

        let tcp_listener = TcpListener::bind(address).await?;

        let routing = Arc::new(recommended(id));

        // give incoming handlers access to routingtable
        let context = Context::new(Arc::clone(&routing));
        // create channel for incoming UDP requests to be sent to the RPC handler
        let (request_tx, request_rx) = mpsc::channel(64);

        let udp_transport = UdpTransport::with_requests(udp_socket, request_tx);
        // start the RPC server
        let server_task = tokio::spawn(handle_rpc::serve(
            Arc::clone(&context),
            request_rx,
            udp_transport.socket(),
            tcp_listener,
        ));
        // outgoing RPC
        let rpc = Rpc::new(id, udp_transport, TcpTransport, Arc::clone(&routing));

        Ok(Self {
            id,
            address,
            rpc,
            context,
            _server_task: server_task,
        })
    }
    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn rpc(&self) -> &NodeRpc {
        &self.rpc
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn two_nodes_can_ping_each_other() {
        let a = Node::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = Node::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        assert!(a.rpc().ping(b.address()).await);
    }
    #[tokio::test]
    async fn two_nodes_can_ping_and_learn_each_other() {
        let a = Node::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = Node::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        assert!(a.rpc().ping(b.address()).await);

        let contacts = b.rpc().close_nodes().close_nodes(a.id());

        assert!(
            contacts
                .iter()
                .any(|contact| contact.id == a.id() && contact.address == a.address())
        );
    }
    #[tokio::test]
    async fn node_can_discover_another_node_through_lookup() {
        let a = Node::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = Node::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let c = Node::bind([3u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        // Make A learn B.
        assert!(b.rpc().ping(a.address()).await);

        // Make B learn C.
        assert!(c.rpc().ping(b.address()).await);

        let found = crate::lookup::lookup_node(a.rpc(), c.id()).await;

        assert!(
            found
                .iter()
                .any(|contact| contact.id == c.id() && contact.address == c.address())
        );
    }
    #[tokio::test]
    async fn real_nodes_can_store_and_find_value() {
        let a = Node::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = Node::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let key = [9u8; 20];
        let value = vec![0xAB; 5000];

        assert!(a.rpc().store(b.address(), key, value.clone()).await);

        let result = a.rpc().find_value(b.address(), key).await;

        assert_eq!(result, crate::rpc::FindValue::Value(value));
    }

    #[tokio::test]
    async fn node_can_find_value_through_network() {
        let a = Node::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = Node::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let c = Node::bind([3u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let key = [9u8; 20];
        let value = vec![0xAB; 5000];

        // Make A learn B.
        assert!(b.rpc().ping(a.address()).await);

        // Make B learn C.
        assert!(c.rpc().ping(b.address()).await);

        // Put the value on C.
        assert!(a.rpc().store(c.address(), key, value.clone()).await);

        let found = crate::lookup::lookup_value(a.rpc(), key).await;

        assert_eq!(found, Some(value));
    }
}
