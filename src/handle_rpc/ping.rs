//! PING: prove the node is alive.
//!
//! The one RPC with no arguments and no lookup behind it, so it is also the
//! liveness probe every other part of the system leans on — bucket refresh,
//! eviction, and a caller deciding whether a contact is worth keeping.

use crate::close_nodes::CloseNodes;
use crate::handle_rpc::Context;
use log::trace;
use std::net::SocketAddr;

/// The reply, which is what [`Rpc::ping`](crate::rpc::Rpc::ping) checks for.
/// Anything else — including a reply to the right id with the wrong body —
/// counts as not alive.
pub const PONG: &[u8] = b"PONG";

/// Answer a PING.
///
/// `body` is expected to be empty; a non-empty one is a peer that knows
/// something we do not, and is answered anyway. Refusing to reply would only
/// make us look dead to a node that is talking to us in good faith.
pub async fn handle<A: CloseNodes>(
    context: &Context<A>,
    id: u64,
    from: SocketAddr,
    body: &[u8],
) -> Option<Vec<u8>> {
    let _ = context;
    if !body.is_empty() {
        trace!(
            "PING {id} from {from} carried {} unexpected bytes",
            body.len()
        );
    }
    // The sender is already learned as a contact by `dispatch`, ahead of
    // every handler — every arriving RPC carries a NodeId now, not just this one.
    Some(PONG.to_vec())
}
