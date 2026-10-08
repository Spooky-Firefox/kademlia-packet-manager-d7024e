//! Structured `metrics` events, one `key=value` line each.
//!
//! Every line starts with `event=NAME` and holds no spaces inside a value, so
//! a script reads it by splitting on whitespace and then on the first `=`. The
//! lines go to `metrics.log` (see [`logging`](crate::logging)), which is what
//! `experiments/analyze.py` and `analyze_metrics.py` read.
//!
//! Lookup and RPC lines carry `op=N`: the operation they ran on behalf of,
//! set with [`with_op`] around the work an experiment measures, or `0` for
//! everything else (joins, republishing, liveness pings). A lookup inside a
//! lookup, and every RPC either sends, run in the same task and so carry the
//! same op.
//!
//! Durations are in microseconds, measured with [`Instant`] in the process
//! that ran the operation, so they include the time spent waiting to be
//! scheduled as well as the time on the wire.

use crate::close_nodes::{Contact, NodeId};

use log::info;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

static NEXT_LOOKUP_ID: AtomicU64 = AtomicU64::new(1);

/// Which RPCs [`rpc_end`] writes, as a [`RpcEvents`] discriminant. Off by
/// default: every liveness ping is an RPC, so a large network would
/// otherwise fill the log with them.
static RPC_EVENTS: AtomicU8 = AtomicU8::new(RpcEvents::Off as u8);

/// Which RPCs get an [`rpc_end`] line.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RpcEvents {
    Off = 0,
    /// Only those made inside a [`with_op`]: what an experiment measures.
    Ops = 1,
    /// Every one, background traffic included.
    All = 2,
}

tokio::task_local! {
    static OP: u64;
}

/// Run `future` as operation `op` (nonzero), so the lookups and RPCs it makes
/// log `op=OP` and can be told apart from background work.
pub async fn with_op<F: Future>(op: u64, future: F) -> F::Output {
    OP.scope(op, future).await
}

/// The operation the running code belongs to, or 0 outside every [`with_op`].
pub fn current_op() -> u64 {
    OP.try_with(|op| *op).unwrap_or(0)
}

pub fn next_lookup_id() -> u64 {
    NEXT_LOOKUP_ID.fetch_add(1, Ordering::Relaxed)
}

/// Choose which RPCs get [`rpc_end`] lines, for the whole process.
pub fn set_rpc_events(which: RpcEvents) {
    RPC_EVENTS.store(which as u8, Ordering::Relaxed);
}

/// Whether an RPC finishing in the running code should be logged.
pub fn rpc_events() -> bool {
    match RPC_EVENTS.load(Ordering::Relaxed) {
        2 => true,
        1 => current_op() != 0,
        _ => false,
    }
}

fn encode_id(id: NodeId) -> String {
    hex::encode(id)
}

pub fn lookup_start(lookup_id: u64, kind: &str, node: NodeId, target: NodeId) {
    info!(
        target: "metrics",
        "event=lookup_start lookup_id={} op={} kind={} node={} target={}",
        lookup_id,
        current_op(),
        kind,
        encode_id(node),
        encode_id(target),
    );
}

pub fn lookup_probe(lookup_id: u64, kind: &str, node: NodeId, contact: Contact) {
    info!(
        target: "metrics",
        "event=lookup_probe lookup_id={} kind={} node={} peer={}",
        lookup_id,
        kind,
        encode_id(node),
        contact.address,
    );
}

/// How a node lookup ended, for [`node_lookup_end`].
pub struct NodeLookupEnd {
    pub lookup_id: u64,
    /// The lookup this one ran inside of (a value lookup starts with a node
    /// lookup), or 0 when it ran on its own.
    pub parent: u64,
    pub node: NodeId,
    pub probes: usize,
    pub result_count: usize,
    pub exact_match: bool,
    /// How many replies, each naming the next node, it took to learn of the
    /// closest contact found: 0 when the routing table already held it.
    pub hops: u32,
    pub duration: Duration,
}

pub fn node_lookup_end(end: NodeLookupEnd) {
    info!(
        target: "metrics",
        "event=lookup_end lookup_id={} op={} kind=node parent={} node={} probes={} result_count={} exact_match={} hops={} duration_us={}",
        end.lookup_id,
        current_op(),
        end.parent,
        encode_id(end.node),
        end.probes,
        end.result_count,
        end.exact_match,
        end.hops,
        end.duration.as_micros(),
    );
}

/// The end of a value lookup. `probes` counts only the FIND_VALUE calls; the
/// node lookup it began with logs its own `lookup_end` with `parent` set to
/// this `lookup_id`.
pub fn value_lookup_end(
    lookup_id: u64,
    node: NodeId,
    probes: usize,
    success: bool,
    duration: Duration,
) {
    info!(
        target: "metrics",
        "event=lookup_end lookup_id={} op={} kind=value node={} probes={} success={} duration_us={}",
        lookup_id,
        current_op(),
        encode_id(node),
        probes,
        success,
        duration.as_micros(),
    );
}

/// One datagram RPC, from the first send to a reply or to giving up, if
/// [`set_rpc_events`] turned these on.
pub fn rpc_end(method: &str, peer: SocketAddr, attempts: usize, success: bool, duration: Duration) {
    if !rpc_events() {
        return;
    }
    info!(
        target: "metrics",
        "event=rpc op={} method={} peer={} attempts={} success={} duration_us={}",
        current_op(),
        method,
        peer,
        attempts,
        success,
        duration.as_micros(),
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_ids_are_unique() {
        let a = next_lookup_id();
        let b = next_lookup_id();
        assert_ne!(a, b);
    }

    #[tokio::test]
    async fn the_op_is_visible_inside_with_op_only() {
        assert_eq!(current_op(), 0);
        assert_eq!(with_op(7, async { current_op() }).await, 7);
        assert_eq!(current_op(), 0);
    }

    /// The only test that touches the process-wide setting, so the steps run
    /// in order here rather than racing each other.
    #[tokio::test]
    async fn rpc_events_follow_the_setting() {
        set_rpc_events(RpcEvents::Ops);
        assert!(!rpc_events(), "background work is not an op");
        assert!(with_op(3, async { rpc_events() }).await);

        set_rpc_events(RpcEvents::All);
        assert!(rpc_events());
        // Logging with no logger installed is a no-op, but runs the formatting.
        rpc_end(
            "PING",
            "127.0.0.1:1".parse().unwrap(),
            2,
            true,
            Duration::from_millis(3),
        );
        set_rpc_events(RpcEvents::Off);
        assert!(!rpc_events());
        assert!(!with_op(3, async { rpc_events() }).await);
        rpc_end(
            "PING",
            "127.0.0.1:1".parse().unwrap(),
            1,
            false,
            Duration::ZERO,
        );
    }
}
