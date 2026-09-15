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

    rpc.close_nodes().maybe_add_contact(Contact { id: seed_id, address: seed });

    Ok(lookup::lookup_node(rpc, rpc.my_id()).await)
}

#[cfg(test)]
mod tests {
    use heapless::sorted_linked_list::Node;

use super::*;
    use crate::close_nodes::NodeId;
    use crate::handle_rpc::Method;
use crate::rpc;
    use std::sync::Mutex;

    const MY_ID: NodeId = [1u8; 20];
    const SEED_ID: NodeId = [2u8; 20];

    fn seed_addr() -> SocketAddr {
        "127.0.0.1:8000".parse().unwrap()
    }

    fn me() -> Contact {
        Contact { id: MY_ID, address: "127.0.0.1:9000".parse().unwrap() }
    }

    /// Routing table remembers what bootstrap taught it
    #[derive(Default)]
    struct RecordingCloseNodes {
        contacts: Mutex<Vec<Contact>>
    }

    impl CloseNodes for RecordingCloseNodes {
        fn close_nodes(&self, id: NodeId) -> Vec<Contact> {
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
            Self { seed_id, contacts, reachable: true }
        }

        fn unreachable() -> Self {
            Self {seed_id: SEED_ID, contacts: Vec::new(), reachable: false}
        }
    }

    impl RpcTransport for FakeTransport {
        async fn send_receive(&self, 
            payload: Vec<u8>, 
            _address: SocketAddr
        ) -> std::io::Result<Vec<u8>> {
            if !self.reachable {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                     "no answer",
                    ));
            }
            let (method, _body) = Method::split_tag(&payload).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, "bad tag")
            })?;
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
        Rpc::new(me(), transport, FakeTransport::unreachable(), RecordingCloseNodes::default(),)
    }

    #[tokio::test]
    async fn bootstrap_records_the_seed() {
        let rpc = rpc_with(FakeTransport::new(SEED_ID, Vec::new()));

        bootstrap(&rpc, seed_addr()).await.unwrap();

        assert_eq!(
            rpc.close_nodes().close_nodes(MY_ID),
            vec![Contact {id: SEED_ID, address: seed_addr()}]
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
            vec![Contact {id:SEED_ID, address: seed_addr()}, neighbour]
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