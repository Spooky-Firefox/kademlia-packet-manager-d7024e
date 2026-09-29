//! Background upkeep of the routing table, one round at a time.
//!
//! Each function here is a single round over an [`Rpc`], like the lookups in
//! [`lookup`](crate::lookup): no timers and no spawning, so a test can run a
//! round and look at the result. [`Node::periodic_task`](crate::node::Node::periodic_task)
//! is what repeats them.

use crate::close_nodes::CloseNodes;
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;
use log::trace;
use std::collections::HashSet;
use std::time::Duration;

/// How often, on average, a node runs [`check_liveness`].
///
/// A round pings every contact, so this trades traffic for how long a dead
/// contact can linger. Failed calls already remove contacts as they happen
/// (see [`Rpc::forget`]); this catches the ones nothing has called lately.
pub const LIVENESS_CHECK_INTERVAL: Duration = Duration::from_secs(60);

/// How often a node republishes the values it holds: the paper's
/// `tReplicate` of one hour.
pub const REPUBLISH_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// How far [`jittered`] may move a period, as a fraction of it either way.
pub const JITTER: f64 = 0.5;

/// `period` scaled by a random factor in `[1 - JITTER, 1 + JITTER)`.
///
/// Nodes started together would otherwise run their upkeep in lockstep, and
/// picking a new factor every round keeps them from drifting back into step.
pub fn jittered(period: Duration) -> Duration {
    period.mul_f64(rand::random_range(1.0 - JITTER..1.0 + JITTER))
}

/// Republish every value this node currently holds to the current K closest
/// nodes.
///
/// The datastore is snapshotted before any network operations are awaited.
/// This avoids holding DashMap guards while a lookup or STORE is in progress.
pub async fn republish_values<T, U, A>(
    rpc: &Rpc<T, U, A>,
    values: Vec<(crate::close_nodes::Key, Vec<u8>)>,
) where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    for (key, value) in values {
        let mut targets = crate::lookup::lookup_node(rpc, key).await;

        targets.push(rpc.my_contact());

        targets.sort_by(|a, b| crate::close_nodes::xor_distance_cmp(a.id, b.id, key));
        targets.dedup_by_key(|contact| contact.id);
        targets.truncate(crate::close_nodes::K);

        for target in targets {
            rpc.store(&target, key, value.clone()).await;
        }
    }
}

/// Ping every contact in the routing table and drop the ones that are gone.
///
/// Gone means no answer at all, or an answer from a different id: whoever
/// holds that address now, it is not the node the entry names. Removal goes
/// through [`Rpc::forget`], so a round never takes the table below its floor —
/// a node whose own network is down would otherwise wipe it here.
///
/// The iterator is walked lazily while pinging, so contacts learned during the
/// round may be checked too, and one contact may be pinged twice.
pub async fn check_liveness<T, U, A>(rpc: &Rpc<T, U, A>)
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    trace!("beginning search for dead contacts");

    let mut dead_contacts = HashSet::new();

    for contact in rpc.close_nodes().contacts_iter() {
        match rpc.ping(contact.address).await {
            Some(id) if id == contact.id => {}
            Some(_) => {
                trace!("{contact:?} answered with another id, adding it to the remove set");
                dead_contacts.insert(contact);
            }
            None => {
                trace!("no response from {contact:?}, adding it to the remove set");
                dead_contacts.insert(contact);
            }
        }
    }

    trace!("removing {} dead contacts", dead_contacts.len());
    for dead_contact in &dead_contacts {
        rpc.forget(dead_contact);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::{Contact, K};
    use crate::node::FakeNode;
    use crate::rpc_transport::networked_debug_transport::Network;
    use std::net::SocketAddr;

    /// Every distinct contact in `node`'s routing table.
    fn contacts(node: &FakeNode) -> HashSet<Contact> {
        node.rpc().close_nodes().contacts_iter().collect()
    }
    fn holds(node: &FakeNode, key: crate::close_nodes::Key) -> bool {
        node.datastore_snapshot()
            .iter()
            .any(|(stored_key, _)| *stored_key == key)
    }
    /// A contact at an address nothing on the network is bound to, so pings
    /// to it go unanswered.
    fn dead_contact() -> Contact {
        Contact {
            id: [0x5a; 32],
            address: SocketAddr::from(([10, 0, 0, 1], 9)),
        }
    }
    #[tokio::test]
    async fn republishing_keeps_value_alive_after_original_holders_leave() {
        let network = Network::new();

        // These nodes are the original replica set.
        let originals: Vec<FakeNode> = (0..K).map(|_| FakeNode::new(&network)).collect();

        // These nodes did not initially store the value. They are the nodes that
        // republishing can move copies to after churn.
        let replacements: Vec<FakeNode> = (0..K).map(|_| FakeNode::new(&network)).collect();

        // A separate node that will try to retrieve the value after every
        // original holder has left.
        let reader = FakeNode::new(&network);

        let value = b"value that must survive churn".to_vec();
        let key = crate::hashing::key_for_value(&value);

        let publisher = &originals[0];

        // Simulate the original replication of the value onto K nodes.
        for holder in &originals {
            assert!(
                publisher
                    .rpc()
                    .store(&holder.contact(), key, value.clone())
                    .await
            );
        }

        assert!(
            originals.iter().all(|node| holds(node, key)),
            "every original replica should initially hold the value"
        );

        // The surviving holder now knows about nodes that can become the new
        // replica set.
        for replacement in &replacements {
            publisher
                .rpc()
                .close_nodes()
                .maybe_add_contact(replacement.contact());
        }

        // Simulate churn: all original holders except one disappear.
        for holder in originals.iter().skip(1) {
            assert!(network.unbind(holder.address()));
        }

        // The last surviving replica performs one republishing round.
        republish_values(publisher.rpc(), vec![(key, value.clone())]).await;

        // Copies should now exist on replacement nodes.
        let republished_to: Vec<Contact> = replacements
            .iter()
            .filter(|node| holds(node, key))
            .map(|node| node.contact())
            .collect();

        assert!(
            republished_to.len() >= K - 1,
            "republishing should place copies on the current close nodes"
        );

        // Now remove the final member of the original replica set.
        assert!(network.unbind(publisher.address()));

        // The reader only knows about nodes that received the republished copy.
        for contact in &republished_to {
            reader.rpc().close_nodes().maybe_add_contact(*contact);
        }

        // The value must still be retrievable even though every original holder
        // is gone.
        let result = crate::lookup::lookup_value(reader.rpc(), key).await;

        assert!(
            matches!(result, Some((_source, found)) if found == value),
            "value should remain retrievable after all original holders leave"
        );
    }
    /// A node that knows `K + 1` live nodes: enough that the floor lets a
    /// round remove one more contact.
    fn node_with_live_contacts(network: &Network) -> (FakeNode, Vec<FakeNode>) {
        let node = FakeNode::new(network);
        let live: Vec<FakeNode> = (0..=K).map(|_| FakeNode::new(network)).collect();
        for peer in &live {
            node.rpc().close_nodes().maybe_add_contact(peer.contact());
        }
        (node, live)
    }

    #[test]
    fn jitter_stays_within_bounds() {
        let period = Duration::from_secs(10);
        for _ in 0..1000 {
            let jittered = jittered(period);
            assert!(jittered >= period.mul_f64(1.0 - JITTER));
            assert!(jittered <= period.mul_f64(1.0 + JITTER));
        }
    }

    #[tokio::test]
    async fn a_dead_contact_is_removed_and_live_ones_are_kept() {
        let network = Network::new();
        let (node, _live) = node_with_live_contacts(&network);
        node.rpc().close_nodes().maybe_add_contact(dead_contact());
        let before = contacts(&node);
        assert!(before.contains(&dead_contact()));
        assert!(before.len() > K);

        check_liveness(node.rpc()).await;

        let mut expected = before;
        expected.remove(&dead_contact());
        assert_eq!(contacts(&node), expected);
    }

    #[tokio::test]
    async fn a_contact_answering_with_another_id_is_removed() {
        let network = Network::new();
        let (node, live) = node_with_live_contacts(&network);
        // The address is alive, but it belongs to a node with another id.
        let stale = Contact {
            id: [0x5a; 32],
            address: live[0].address(),
        };
        node.rpc().close_nodes().maybe_add_contact(stale);
        let before = contacts(&node);
        assert!(before.contains(&stale));

        check_liveness(node.rpc()).await;

        let mut expected = before;
        expected.remove(&stale);
        assert_eq!(contacts(&node), expected);
    }

    #[tokio::test]
    async fn a_small_table_keeps_its_dead_contacts() {
        let network = Network::new();
        let node = FakeNode::new(&network);
        let peer = FakeNode::new(&network);
        node.rpc().close_nodes().maybe_add_contact(peer.contact());
        node.rpc().close_nodes().maybe_add_contact(dead_contact());
        let before = contacts(&node);

        check_liveness(node.rpc()).await;

        assert_eq!(contacts(&node), before);
    }
    #[tokio::test]
    async fn republish_sends_value_to_current_close_nodes() {
        let network = Network::new();

        let publisher = FakeNode::new(&network);
        let peer = FakeNode::new(&network);

        // Make the publisher able to discover the peer.
        publisher
            .rpc()
            .close_nodes()
            .maybe_add_contact(peer.contact());

        let value = b"republished value".to_vec();
        let key = crate::hashing::key_for_value(&value);

        republish_values(publisher.rpc(), vec![(key, value.clone())]).await;

        let result = publisher.rpc().find_value(&peer.contact(), key).await;

        assert_eq!(result, crate::rpc::FindValue::Value(value));
    }
}
