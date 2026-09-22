//! Where a connection-shaped server gets its connections. The stream-shaped
//! counterpart to [`DataRxTx`](super::data_rx_tx::DataRxTx), implemented by
//! [`tokio::net::TcpListener`] and by the in-process
//! [`Endpoint`](super::networked_debug_transport::Endpoint).
//!
//! Only accepting needs naming: an open connection is already an
//! [`AsyncRead`] + [`AsyncWrite`], and dialling is each transport's
//! [`send_receive`](super::RpcTransport::send_receive).

use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

use std::sync::Arc;

pub trait StreamListener {
    type Stream: AsyncRead + AsyncWrite + Unpin + Send + 'static;

    /// Wait for the next connection, and hand it back with the address that
    /// dialled it — an ephemeral port, not one the caller can be reached at,
    /// which is why it is not
    /// [`from_is_reachable`](crate::handle_rpc::Request::from_is_reachable).
    ///
    /// `impl Future + Send`, not `async fn`, so the accept loop can be spawned.
    fn accept(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<(Self::Stream, SocketAddr)>> + Send;
}
impl<T: StreamListener> StreamListener for Arc<T> {
    type Stream = T::Stream;

    fn accept(
        &self,
    ) -> impl std::future::Future<Output = std::io::Result<(Self::Stream, SocketAddr)>> + Send {
        self.as_ref().accept()
    }
}
