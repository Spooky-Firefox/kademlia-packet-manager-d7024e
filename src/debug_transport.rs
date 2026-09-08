use dashmap::DashMap;
use std::net::SocketAddr;
use tokio::sync::oneshot;

use crate::rpc_transport_trait::{self, RpcTransport};
struct DebugTransport {
    map: std::sync::Arc<dashmap::DashMap<u64, (SocketAddr, Vec<u8>, oneshot::Sender<Vec<u8>>)>>,
}

impl DebugTransport {
    fn new() -> (
        Self,
        std::sync::Arc<DashMap<u64, (SocketAddr, Vec<u8>, oneshot::Sender<Vec<u8>>)>>,
    ) {
        let map = std::sync::Arc::new(DashMap::new());
        (Self { map: map.clone() }, map)
    }
}

/// The socket address is not used
impl RpcTransport for DebugTransport {
    async fn send_receive(&self, payload: Vec<u8>, address: SocketAddr) -> Vec<u8> {
        let (tx, rx) = oneshot::channel();
        let id = rand::random::<u64>();
        self.map.insert(id, (address, payload, tx));
        rx.await.unwrap()
    }
}
