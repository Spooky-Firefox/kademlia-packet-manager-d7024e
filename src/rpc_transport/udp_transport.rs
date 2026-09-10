//! [`RetryTransport`] over a real [`tokio::net::UdpSocket`].

use crate::rpc_transport::data_rx_tx::DataRxTx;
use crate::rpc_transport::retry_transport::RetryTransport;
use std::net::SocketAddr;
use tokio::net::UdpSocket;

/// A [`RetryTransport`] whose datagram channel is a real UDP socket.
pub type UdpTransport = RetryTransport<UdpSocket>;

impl DataRxTx for UdpSocket {
    async fn send_packet(&self, payload: &[u8], address: SocketAddr) -> std::io::Result<()> {
        self.send_to(payload, address).await.map(|_| ())
    }

    async fn receive_packet(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        self.recv_from(buf).await
    }
}
