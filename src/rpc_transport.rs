use std::net::SocketAddr;

pub mod debug_transport;
pub mod networked_debug_transport;
pub mod udp_transport;

pub trait RpcTransport {
    // warning, the responsibility to hande a non response is on the user
    //
    // for example use a select! macro to wait for either a response or a timeout
    // as this might otherwise block indefinitely if no response arrives
    async fn send_receive(&self, payload: Vec<u8>, address: SocketAddr) -> Vec<u8>;
}
