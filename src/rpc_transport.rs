use std::net::SocketAddr;

pub mod data_rx_tx;
pub mod debug_transport;
pub mod networked_debug_transport;
pub mod retry_transport;
pub mod tcp_transport;
pub mod udp_transport;

pub trait RpcTransport {
    // The request id is the transport's own business: it frames one, matches
    // the reply against it, and only ever hands back a response that carried it.
    //
    // The udp / networked implementations bound their own wait — they resend on
    // silence and return an `io::Error` of kind `TimedOut` once the attempt
    // budget is spent — so a caller only needs an outer `timeout` for a
    // deadline shorter than that.
    async fn send_receive(&self, payload: Vec<u8>, address: SocketAddr)
    -> std::io::Result<Vec<u8>>;
}
