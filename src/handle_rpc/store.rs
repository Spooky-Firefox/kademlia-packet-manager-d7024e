//! STORE: hold a value on behalf of the network.
//!
//! We are asked to store a key that is close to us, so the sender's idea of
//! "close" is worth sanity-checking against our own before spending storage on
//! it — but not worth refusing over, since our routing table and theirs are
//! allowed to disagree while the network churns.

use crate::close_nodes::CloseNodes;
use crate::handle_rpc::Context;
use std::net::SocketAddr;

// TODO: drop this once the stub below has a real body.
#[allow(unused_variables)]
/// Store the key and value in `body`, and acknowledge it.
///
/// Returning `None` leaves the sender to time out, which is the right answer
/// for a request we decline to serve: it has no way to tell "refused" from
/// "unreachable" anyway, and both mean the value did not land here.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    id: u64,
    from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    // NOTE lab spec allows for tcp transport of values, not forcing udp only
    todo!("decode key and value, write them to the store, encode the ack")
}
