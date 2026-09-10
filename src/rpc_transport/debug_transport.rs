use dashmap::DashMap;
use std::net::SocketAddr;
use tokio::sync::oneshot;

use crate::rpc_transport::RpcTransport;
struct DebugTransport {
    map: std::sync::Arc<dashmap::DashMap<u64, (SocketAddr, Vec<u8>, oneshot::Sender<Vec<u8>>)>>,
    counter: std::sync::atomic::AtomicU64,
}

impl DebugTransport {
    fn new() -> (
        Self,
        std::sync::Arc<DashMap<u64, (SocketAddr, Vec<u8>, oneshot::Sender<Vec<u8>>)>>,
    ) {
        let map = std::sync::Arc::new(DashMap::new());
        (
            Self {
                map: map.clone(),
                counter: std::sync::atomic::AtomicU64::new(0),
            },
            map,
        )
    }
}

/// The socket address is not used
impl RpcTransport for DebugTransport {
    async fn send_receive(
        &self,
        payload: Vec<u8>,
        address: SocketAddr,
    ) -> std::io::Result<Vec<u8>> {
        let id = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.map.insert(id, (address, payload, tx));
        Ok(rx.await.unwrap())
    }
}
