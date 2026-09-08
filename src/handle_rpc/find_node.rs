//! FIND_NODE: answer with the contacts we know closest to a target.
//!
//! The RPC an iterative lookup is built out of: the caller asks the closest
//! nodes it knows for the ones they know, and repeats until the answers stop
//! getting closer. Our reply is [`CloseNodes::close_nodes`] verbatim — up to
//! [`K`](crate::close_nodes::K) contacts, nearest first — whether or not we
//! are anywhere near the target ourselves.

use crate::close_nodes::CloseNodes;
use crate::handle_rpc::Context;
use std::net::SocketAddr;

// TODO: drop this once the stub below has a real body.
#[allow(unused_variables)]
/// Answer a FIND_NODE for the target id in `body`.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    todo!("decode the target, call context.close_nodes.close_nodes(), encode the contacts")
}
