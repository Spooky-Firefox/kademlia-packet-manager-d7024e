//! The datagram channel a
//! [`RetryTransport`](super::retry_transport::RetryTransport) runs over: send a
//! payload to an address, receive the next one into a buffer.
//!
//! Implemented by [`tokio::net::UdpSocket`] for real UDP (in
//! [`udp_transport`](super::udp_transport)) and by
//! [`Endpoint`](super::networked_debug_transport::Endpoint) for the in-process
//! fake network, so the same transport code drives both.

use std::net::SocketAddr;

pub trait DataRxTx {
    /// Send `payload` to `address`. As with UDP, a successful return does not
    /// promise delivery.
    ///
    /// Returns `impl Future + Send` rather than being written `async fn` so the
    /// receive loop this feeds can be `tokio::spawn`ed.
    fn send_packet(
        &self,
        payload: &[u8],
        address: SocketAddr,
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send;

    /// Wait for the next datagram, copy up to `buf.len()` bytes of it into
    /// `buf`, and return how many bytes were written and who sent it. Bytes
    /// past `buf.len()` are discarded, as with `UdpSocket::recv_from`.
    fn receive_packet(
        &self,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<(usize, SocketAddr)>> + Send;
}
