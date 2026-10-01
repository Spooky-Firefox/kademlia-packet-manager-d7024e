//! Structured `metrics` events, one `key=value` line each.
//!
//! Every line starts with `event=NAME` and holds no spaces inside a value, so
//! a script reads it by splitting on whitespace and then on the first `=`. The
//! lines go to `metrics.log` (see [`logging`](crate::logging)), which is what
//! `experiments/analyze.py` and `analyze_metrics.py` read.
//!
//! Durations are in microseconds, measured with [`Instant`] in the process
//! that ran the operation, so they include the time spent waiting to be
//! scheduled as well as the time on the wire.

use crate::close_nodes::{Contact, NodeId};

use log::info;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

static NEXT_LOOKUP_ID: AtomicU64 = AtomicU64::new(1);

/// Whether [`rpc_end`] writes anything. Off by default: every liveness ping
/// is an RPC, so a large network would otherwise fill the log with them.
static RPC_EVENTS: AtomicBool = AtomicBool::new(false);

pub fn next_lookup_id() -> u64 {
    NEXT_LOOKUP_ID.fetch_add(1, Ordering::Relaxed)
}

/// Turn [`rpc_end`] events on or off, for the whole process.
pub fn set_rpc_events(enabled: bool) {
    RPC_EVENTS.store(enabled, Ordering::Relaxed);
}

pub fn rpc_events() -> bool {
    RPC_EVENTS.load(Ordering::Relaxed)
}

fn encode_id(id: NodeId) -> String {
    hex::encode(id)
}

pub fn lookup_start(lookup_id: u64, kind: &str, node: NodeId, target: NodeId) {
    info!(
        target: "metrics",
        "event=lookup_start lookup_id={} kind={} node={} target={}",
        lookup_id,
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
        "event=lookup_end lookup_id={} kind=node parent={} node={} probes={} result_count={} exact_match={} hops={} duration_us={}",
        end.lookup_id,
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
        "event=lookup_end lookup_id={} kind=value node={} probes={} success={} duration_us={}",
        lookup_id,
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
        "event=rpc method={} peer={} attempts={} success={} duration_us={}",
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

    #[test]
    fn rpc_events_can_be_toggled() {
        set_rpc_events(true);
        assert!(rpc_events());
        // Logging with no logger installed is a no-op, but runs the formatting.
        rpc_end(
            "PING",
            "127.0.0.1:1".parse().unwrap(),
            2,
            true,
            Duration::from_millis(3),
        );
        set_rpc_events(false);
        assert!(!rpc_events());
        rpc_end(
            "PING",
            "127.0.0.1:1".parse().unwrap(),
            1,
            false,
            Duration::ZERO,
        );
    }
}
