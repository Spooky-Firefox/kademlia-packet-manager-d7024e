//! FIND_VALUE: answer with a stored value, or with where to look next.
//!
//! [`find_node`](super::find_node) with a shortcut: if the key is one we
//! hold, the lookup ends here and the value comes back instead of the
//! contacts. The two cases share a request but not a reply, so the encoding
//! has to let the caller tell them apart — that is what
//! [`FindValue`](crate::rpc::FindValue) is on the other end.

use crate::close_nodes::CloseNodes;
use crate::handle_rpc::Context;
use std::net::SocketAddr;

// TODO: drop this once the stub below has a real body.
#[allow(unused_variables)]
/// Answer a FIND_VALUE for the key in `body`.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    // NOTE lab spec allows for tcp transport of values, not forcing udp only
    todo!("decode the key, look it up, encode either the value or the closest contacts")
}
