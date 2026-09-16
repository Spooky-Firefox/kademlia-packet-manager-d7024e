//! Joining an existing network.
//!
//! Kademlia has no broadcast, no directory, and no announcement of new
//! arrivals — so a node that knows nobody has no way to find anybody, because
//! finding anybody means asking somebody. [`bootstrap`] breaks that circle
//! with the one fact a joining node is given from outside the protocol: the
//! address of a node already in the network.

use crate::close_nodes::{CloseNodes, Contact};
use crate::lookup;
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapError {
    /// The seed never answered, so there is no networking to join it.
    Unreachable,
    /// The seed answered with our own id.
    SelfSeed,
}

/// Join the network through `seed`, returning the neighbourhood we land in.
///
/// # Why a PING first
///
/// A routing table stores [`Contact`]s, and we arrived holding half of one:
/// `seed` is an address, and only the node living there can say which id goes
/// with it. [`Rpc::ping`](crate::rpc::Rpc::ping) answers with the responder's
/// id for exactly this reason. With both halves the seed becomes the single
/// entry a lookup needs in order to start.
///
/// # Why we look ourselves up
///
/// Searching for an id we already hold reads as a no-op, and is the opposite.
/// A lookup is a walk, not a query: we ask the seed for the nodes it knows
/// nearest us, ask those nodes the same question, and keep going while the
/// answers get closer. [`lookup_node`](crate::lookup::lookup_node) files every
/// contact it meets along the way, so the walk is what populates the table.
///
/// Aiming it at our own id makes it converge on our own neighbourhood, which
/// is the part of the keyspace we most need to know: those are the nodes that
/// will hold values whose keys land near us, and the nodes we answer for.
///
/// The walk also runs in the other direction, which is easy to miss and is
/// half the point. Every request carries our [`NodeId`](crate::close_nodes::NodeId)
/// ahead of its method tag, and the receiving node pairs it with the address
/// the request arrived from — so each node we touch records us while
/// answering. Without that the join would be read-only: a thousand nodes could
/// each bootstrap off the same seed and that seed would still know none of
/// them, answering every query with an empty list while the network failed to
/// form.
///
/// # Errors
///
/// [`Unreachable`](BootstrapError::Unreachable) if the seed never answers:
/// there is no network to join through a node that is not there. It folds
/// together timeout, transport error and garbled reply, none of which a caller
/// could act on differently.
///
/// [`SelfSeed`](BootstrapError::SelfSeed) if the seed answers with our own id,
/// meaning we were pointed at ourselves. That is a configuration mistake
/// rather than a network condition, and it earns its own variant because the
/// alternative is silent: the sibling list declines to store our own id, so we
/// would start the lookup with an empty table and return an empty vector with
/// nothing to explain it.
///
/// An empty-handed success is not an error. The second node on a network
/// legitimately finds only the seed.
pub async fn bootstrap<T, U, A>(
    rpc: &Rpc<T, U, A>,
    seed: SocketAddr,
) -> Result<Vec<Contact>, BootstrapError>
where
    T: RpcTransport,
    U: RpcTransport,
    A: CloseNodes,
{
    let seed_id = rpc.ping(seed).await.ok_or(BootstrapError::Unreachable)?;

    if seed_id == rpc.my_id() {
        return Err(BootstrapError::SelfSeed);
    }

    rpc.close_nodes().maybe_add_contact(Contact {
        id: seed_id,
        address: seed,
    });

    // TODO: refresh the buckets past our nearest neighbour. Kademlia's join
    // follows the self-lookup with a lookup for a random id in each bucket
    // farther out than our closest contact. That is what fills the distant
    // buckets, which the self-lookup never reaches: it converges on our own
    // neighbourhood by design, so it leaves the table dense around us and
    // thin everywhere else, and lookups for far-off keys then take more
    // rounds than they should. Inbound requests fill those buckets over time
    // via `maybe_add_contact`, so this is a refinement rather than a hole —
    // but it starts to matter once the network is large enough that "far
    // off" is most of it.
    Ok(lookup::lookup_node(rpc, rpc.my_id()).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::NodeId;
    use crate::handle_rpc::Method;
    use std::sync::Mutex;

    const MY_ID: NodeId = [1u8; 20];
    const SEED_ID: NodeId = [2u8; 20];

    fn seed_addr() -> SocketAddr {
        "127.0.0.1:8000".parse().unwrap()
    }

    fn me() -> Contact {
        Contact {
            id: MY_ID,
            address: "127.0.0.1:9000".parse().unwrap(),
        }
    }

    /// Routing table remembers what bootstrap taught it
    #[derive(Default)]
    struct RecordingCloseNodes {
        contacts: Mutex<Vec<Contact>>,
    }

    impl CloseNodes for RecordingCloseNodes {
        fn close_nodes(&self, _id: NodeId) -> Vec<Contact> {
            self.contacts.lock().unwrap().clone()
        }

        fn maybe_add_contact(&self, contact: Contact) {
            let mut contacts = self.contacts.lock().unwrap();
            if !contacts.iter().any(|known| known.id == contact.id) {
                contacts.push(contact);
            }
        }
    }

    struct FakeTransport {
        seed_id: NodeId,
        contacts: Vec<Contact>,
        reachable: bool,
    }

    impl FakeTransport {
        fn new(seed_id: NodeId, contacts: Vec<Contact>) -> Self {
            Self {
                seed_id,
                contacts,
                reachable: true,
            }
        }

        fn unreachable() -> Self {
            Self {
                seed_id: SEED_ID,
                contacts: Vec::new(),
                reachable: false,
            }
        }
    }

    impl RpcTransport for FakeTransport {
        async fn send_receive(
            &self,
            payload: Vec<u8>,
            _address: SocketAddr,
        ) -> std::io::Result<Vec<u8>> {
            if !self.reachable {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "no answer",
                ));
            }
            let (method, _body) = Method::split_tag(&payload)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "bad tag"))?;
            match method {
                Method::Ping => Ok(bincode::serialize(&self.seed_id).unwrap()),
                Method::FindNode => Ok(bincode::serialize(&self.contacts).unwrap()),
                other => panic!("bootstrap should not send {other:?}"),
            }
        }
    }

    fn rpc_with(
        transport: FakeTransport,
    ) -> Rpc<FakeTransport, FakeTransport, RecordingCloseNodes> {
        Rpc::new(
            me(),
            transport,
            FakeTransport::unreachable(),
            RecordingCloseNodes::default(),
        )
    }

    #[tokio::test]
    async fn bootstrap_records_the_seed() {
        let rpc = rpc_with(FakeTransport::new(SEED_ID, Vec::new()));

        bootstrap(&rpc, seed_addr()).await.unwrap();

        assert_eq!(
            rpc.close_nodes().close_nodes(MY_ID),
            vec![Contact {
                id: SEED_ID,
                address: seed_addr()
            }]
        );
    }

    #[tokio::test]
    async fn bootstrap_learns_the_seeds_neighbours() {
        let neighbour = Contact {
            id: [3u8; 20],
            address: "127.0.0.1:8001".parse().unwrap(),
        };
        let rpc = rpc_with(FakeTransport::new(SEED_ID, vec![neighbour]));

        let found = bootstrap(&rpc, seed_addr()).await.unwrap();

        assert!(found.contains(&neighbour));
        let mut known = rpc.close_nodes().close_nodes(MY_ID);
        known.sort_by_key(|contact| contact.id);
        assert_eq!(
            known,
            vec![
                Contact {
                    id: SEED_ID,
                    address: seed_addr()
                },
                neighbour
            ]
        )
    }

    #[tokio::test]
    async fn an_unreachable_seed_is_an_error() {
        let rpc = rpc_with(FakeTransport::unreachable());

        assert_eq!(
            bootstrap(&rpc, seed_addr()).await,
            Err(BootstrapError::Unreachable)
        );
        assert!(rpc.close_nodes().close_nodes(MY_ID).is_empty());
    }

    #[tokio::test]
    async fn a_seed_that_is_us_is_an_error() {
        let rpc = rpc_with(FakeTransport::new(MY_ID, Vec::new()));

        assert_eq!(
            bootstrap(&rpc, seed_addr()).await,
            Err(BootstrapError::SelfSeed)
        );
        assert!(rpc.close_nodes().close_nodes(MY_ID).is_empty());
    }
}
