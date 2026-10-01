//! Joining an existing network through a node already in it (the seed).

use crate::close_nodes::{CloseNodes, Contact, NodeId, leading_zeros, xor_distance};
use crate::lookup;
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;
use std::net::SocketAddr;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BootstrapError {
    /// The seed did not answer the PING.
    Unreachable,
    /// The seed answered with our own id.
    SelfSeed,
}
// TODO: add test for this or smth
impl std::fmt::Display for BootstrapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unreachable => f.write_str("seed did not respond"),
            Self::SelfSeed => f.write_str("seed is this node itself"),
        }
    }
}
impl std::error::Error for BootstrapError {}

/// Join the network through `seed`, returning the neighbourhood we land in.
///
/// We only know the seed's address, so we PING it to learn its id and add it
/// to our routing table. Then we look up our own id: the lookup fills our
/// table with the nodes closest to us, and since every request carries our
/// id, each node we query learns about us too. Finally we refresh every bucket
/// farther away than our closest neighbour, so the far side of the keyspace
/// isn't left with a contact or two.
///
/// The returned contacts are from the self-lookup only.
///
/// # Errors
///
/// - [`Unreachable`](BootstrapError::Unreachable) if the PING fails for any
///   reason (timeout, transport error or garbled reply).
/// - [`SelfSeed`](BootstrapError::SelfSeed) if the seed is us. The routing
///   table never stores our own id, so without this check the join would
///   quietly find nobody.
///
/// Finding only the seed is not an error; the second node in a network has
/// nobody else to find.
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

    let contacts = lookup::lookup_node(rpc, rpc.my_id()).await;

    let Some(closest_neighbour) = contacts.first() else {
        return Ok(contacts);
    };
    let cn_distance = xor_distance(closest_neighbour.id, rpc.my_id());
    let cn_leading_zeroes = leading_zeros(cn_distance);

    // The self-lookup only fills the buckets near us. As in the Kademlia
    // paper's join, refresh every bucket farther away than our closest
    // neighbour (lower index = farther) by looking up a random id in it.
    for bucket in 0..cn_leading_zeroes {
        let target = random_id_in_bucket(rpc.my_id(), bucket as usize);
        lookup::lookup_node(rpc, target).await;
    }

    Ok(contacts)
}

/// A random id that shares exactly `bucket` leading bits with `my_id`, so it
/// falls in that bucket. Builds the XOR distance first (`bucket` zero bits,
/// then a one, then random bits) and XORs it onto `my_id`.
// TODO: add test
fn random_id_in_bucket(my_id: NodeId, bucket: usize) -> NodeId {
    let mut distance: NodeId = rand::random();
    distance[..bucket / 8].fill(0);
    distance[bucket / 8] &= 0xFF >> (bucket % 8);
    distance[bucket / 8] |= 0x80 >> (bucket % 8);
    xor_distance(my_id, distance)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::NodeId;
    use crate::handle_rpc::{Method, NODE_ID_LEN};
    use std::sync::Mutex;
    use std::vec;

    const MY_ID: NodeId = [1u8; 32];
    const SEED_ID: NodeId = [2u8; 32];

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

        fn contacts_iter(&self) -> impl std::iter::Iterator<Item = Contact> {
            unimplemented!();
            // so the compiler wont complain
            #[allow(unreachable_code)]
            Vec::new().into_iter()
        }

        fn remove_contact(&self, _contact: &Contact) {
            unimplemented!()
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
            // Skip the sender's NodeId that `Rpc::call` puts before the method tag.
            let rest = payload
                .split_at_checked(NODE_ID_LEN)
                .map(|(_sender, rest)| rest)
                .ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidData, "no node id")
                })?;
            let (method, _body) = Method::split_tag(rest)
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
            id: [3u8; 32],
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
