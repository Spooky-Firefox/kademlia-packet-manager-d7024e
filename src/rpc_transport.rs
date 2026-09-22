use std::net::SocketAddr;

/// Set on a datagram's id to mark it a *reply*; clear on a *request*.
///
/// A node sends and receives everything over one socket, so its receive loop
/// has to tell an answer to something it asked from a question someone is
/// asking it. Matching the id against
/// [`Pending`](crate::pending::Pending) and calling an unmatched id a request
/// cannot do that: request ids are minted by the sender, every node starts
/// its counter at the same place, and a peer's request id `7` would be
/// delivered to whoever here is waiting on reply `7`. So the distinction is
/// written on the wire instead of inferred — whoever frames a reply sets this
/// bit ([`frame_reply`](crate::handle_rpc::frame_reply)), and whoever mints a
/// request id clears it.
///
/// It costs the top bit of the id space: ids are 63-bit, which is 9.2e18
/// in-flight requests before one repeats.
///
/// Nothing outside [`request_id`], [`reply_id`] and [`is_reply`] should need
/// to touch the bit itself.
pub const REPLY_TAG: u64 = 1 << 63;

/// The id to send a *request* under, with [`REPLY_TAG`] cleared.
///
/// Mint every outgoing request id through here rather than trusting the
/// source to leave the top bit alone: a counter never reaches it, but
/// `rand::random::<u64>()` sets it half the time. Also the way to recover the
/// request's number from a reply's id.
pub fn request_id(id: u64) -> u64 {
    id & !REPLY_TAG
}

/// The id to answer request `id` under, with [`REPLY_TAG`] set.
pub fn reply_id(id: u64) -> u64 {
    id | REPLY_TAG
}

/// Whether a datagram's id marks it a reply rather than a request.
pub fn is_reply(id: u64) -> bool {
    id & REPLY_TAG != 0
}

pub mod data_rx_tx;
pub mod debug_transport;
pub mod networked_debug_transport;
pub mod retry_transport;
pub mod stream_framing;
pub mod stream_listener;
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
