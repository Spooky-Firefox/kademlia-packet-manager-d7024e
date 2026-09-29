use crate::close_nodes::{Contact, NodeId};

use log::info;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_LOOKUP_ID: AtomicU64 = AtomicU64::new(1);

pub fn next_lookup_id() -> u64 {
    NEXT_LOOKUP_ID.fetch_add(1, Ordering::Relaxed)
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

pub fn node_lookup_end(
    lookup_id: u64,
    node: NodeId,
    probes: usize,
    result_count: usize,
    exact_match: bool,
) {
    info!(
        target: "metrics",
        "event=lookup_end lookup_id={} kind=node node={} probes={} result_count={} exact_match={}",
        lookup_id,
        encode_id(node),
        probes,
        result_count,
        exact_match,
    );
}

pub fn value_lookup_end(lookup_id: u64, node: NodeId, probes: usize, success: bool) {
    info!(
        target: "metrics",
        "event=lookup_end lookup_id={} kind=value node={} probes={} success={}",
        lookup_id,
        encode_id(node),
        probes,
        success,
    );
}
