use crate::close_nodes::{Contact, NodeId, RecommendedCloseNodes, recommended};
use crate::handle_rpc::{self, Context};
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;
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
    pub async fn bind(id: NodeId, bind_address: SocketAddr) -> io::Result<Self> {
        let udp_socket = UdpSocket::bind(bind_address).await?;
        let address = udp_socket.local_addr()?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::CloseNodes;

    #[tokio::test]
    async fn two_nodes_can_ping_each_other() {
        let a = RealNode::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        assert_eq!(a.rpc().ping(b.address()).await, Some(b.id()));
    }

    #[tokio::test]
    async fn two_nodes_can_ping_and_learn_each_other() {
        let a = RealNode::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
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
        let a = RealNode::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let c = RealNode::bind([3u8; 20], "127.0.0.1:0".parse().unwrap())
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
        let a = RealNode::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let key = [9u8; 20];
        let value = vec![0xAB; 5000];

        assert!(a.rpc().store(b.address(), key, value.clone(),).await);

        let result = a.rpc().find_value(b.address(), key).await;

        assert_eq!(result, crate::rpc::FindValue::Value(value));
    }

    #[tokio::test]
    async fn node_can_find_value_through_network() {
        let a = RealNode::bind([1u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind([2u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let c = RealNode::bind([3u8; 20], "127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let key = [9u8; 20];
        let value = vec![0xAB; 5000];

        // Make A learn B.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        // Make B learn C.
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        // Preload C with the value.
        assert!(a.rpc().store(c.address(), key, value.clone(),).await);

        let found = crate::lookup::lookup_value(a.rpc(), key).await;

        assert_eq!(found, Some(value));
    }
}
