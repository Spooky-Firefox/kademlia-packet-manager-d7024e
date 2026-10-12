use crate::close_nodes::{Contact, Key, NodeId, RecommendedCloseNodes, recommended};
use crate::dns_data::dns::Dns;
use crate::dns_data::fake_dns::FakeDns;
use crate::dns_data::{DnsBackend, DnsData};
use crate::handle_rpc::{self, Context};
use crate::hashing::node_id_from_address;
use crate::maintenance;
use crate::rpc::Rpc;
use crate::rpc_transport::RpcTransport;
use crate::rpc_transport::networked_debug_transport::{
    Network, NetworkedDebugTransport, NetworkedStreamTransport,
};
use crate::rpc_transport::tcp_transport::TcpTransport;
use crate::rpc_transport::udp_transport::UdpTransport;
use crate::value::Value;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

pub type NodeRpc<T, U> = Arc<Rpc<T, U, Arc<RecommendedCloseNodes>>>;

pub struct Node<T, U, D>
where
    T: RpcTransport,
    U: RpcTransport,
    D: DnsData,
{
    id: NodeId,
    address: SocketAddr,
    rpc: NodeRpc<T, U>,
    context: Arc<Context<Arc<RecommendedCloseNodes>, Arc<D>>>,
    _server_tasks: Vec<JoinHandle<()>>,
}
pub type RealNode = Node<UdpTransport, TcpTransport, DnsBackend>;
pub type FakeNode = Node<NetworkedDebugTransport, NetworkedStreamTransport, FakeDns>;

impl<T, U, D> Node<T, U, D>
where
    T: RpcTransport,
    U: RpcTransport,
    D: DnsData,
{
    pub fn id(&self) -> NodeId {
        self.id
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub fn contact(&self) -> Contact {
        Contact {
            id: self.id,
            address: self.address,
        }
    }

    pub fn rpc(&self) -> &NodeRpc<T, U> {
        &self.rpc
    }

    /// Where this node looks up domain owners' keys; the STORE handler shares it.
    pub fn dns(&self) -> &Arc<D> {
        &self.context.dns
    }

    pub fn routing_snapshot(&self) -> (Vec<Contact>, Vec<(usize, Vec<Contact>)>) {
        let routing = self.context.close_nodes.as_ref();

        let siblings = routing.inner().siblings();
        let buckets = routing.inner().fallback().non_empty_buckets();

        (siblings, buckets)
    }

    pub fn datastore_snapshot(&self) -> Vec<(Key, Value)> {
        let mut values: Vec<_> = self
            .context
            .values
            .iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        values.sort_by_key(|(key, _)| *key);
        values
    }
}

impl<T, U, D> Node<T, U, D>
where
    T: RpcTransport + Send + Sync + 'static,
    U: RpcTransport + Send + Sync + 'static,
    D: DnsData + 'static,
{
    pub fn periodic_task(
        &self,
        republish_interval: std::time::Duration,
        liveness_check_interval: std::time::Duration,
    ) {
        // TODO deal with the spawn handles
        let rpc = Arc::clone(&self.rpc);
        let _liveness_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(maintenance::jittered(liveness_check_interval)).await;
                maintenance::check_liveness(&rpc).await;
            }
        });

        let rpc = Arc::clone(&self.rpc);
        let context = Arc::clone(&self.context);

        let _republish_task = tokio::spawn(async move {
            loop {
                tokio::time::sleep(maintenance::jittered(republish_interval)).await;

                let values: Vec<(Key, Value)> = context
                    .values
                    .iter()
                    .map(|entry| (*entry.key(), entry.value().clone()))
                    .collect();

                maintenance::republish_values(&rpc, context.dns.as_ref(), values).await;
            }
        });
    }
}

impl RealNode {
    pub async fn bind(bind_address: SocketAddr) -> io::Result<Self> {
        Self::bind_with_dns(bind_address, DnsBackend::Real(Dns::new())).await // create node using real DNS by default
    }

    pub async fn bind_with_dns(bind_address: SocketAddr, dns: DnsBackend) -> io::Result<Self> {
        // create node using passed DNS
        let udp_socket = UdpSocket::bind(bind_address).await?;
        let address = udp_socket.local_addr()?;

        let id = node_id_from_address(address);

        let tcp_listener = TcpListener::bind(address).await?;

        let routing = Arc::new(recommended(id));

        let context = Context::new(id, Arc::clone(&routing), Arc::new(dns));

        let (request_tx, request_rx) = mpsc::channel(64);

        let udp_transport = UdpTransport::with_requests(udp_socket, request_tx);

        let server_task = tokio::spawn(handle_rpc::serve(
            Arc::clone(&context),
            request_rx,
            udp_transport.socket(),
            tcp_listener,
        ));

        let rpc = Arc::new(Rpc::new(
            Contact { id, address },
            udp_transport,
            TcpTransport,
            Arc::clone(&routing),
        ));

        Ok(Self {
            id,
            address,
            rpc,
            context,
            _server_tasks: vec![server_task],
        })
    }
}

impl FakeNode {
    pub fn new(network: &Network) -> Self {
        let endpoint = network.bind_any();
        let address = endpoint.local_addr();

        let id = node_id_from_address(address);

        let routing = Arc::new(recommended(id));

        let context = Context::new(id, Arc::clone(&routing), Arc::new(FakeDns::new()));

        let (request_tx, request_rx) = mpsc::channel(64);

        let datagram_transport = NetworkedDebugTransport::with_requests(endpoint, request_tx);

        let shared_endpoint = datagram_transport.socket();

        let server_task = tokio::spawn(handle_rpc::serve(
            Arc::clone(&context),
            request_rx,
            Arc::clone(&shared_endpoint),
            shared_endpoint,
        ));

        let stream_transport = NetworkedStreamTransport::new(network.clone());

        let rpc = Arc::new(Rpc::new(
            Contact { id, address },
            datagram_transport,
            stream_transport,
            Arc::clone(&routing),
        ));

        Self {
            id,
            address,
            rpc,
            context,
            _server_tasks: vec![server_task],
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::close_nodes::CloseNodes;

    #[tokio::test]
    async fn two_nodes_can_ping_each_other() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        assert_eq!(a.id(), node_id_from_address(a.address()));
        assert_eq!(b.id(), node_id_from_address(b.address()));

        assert_ne!(a.id(), b.id());

        assert_eq!(a.rpc().ping(b.address()).await, Some(b.id()));
    }

    #[tokio::test]
    async fn two_nodes_can_ping_and_learn_each_other() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        assert_eq!(a.rpc().ping(b.address()).await, Some(b.id()));

        let contacts = b.rpc().close_nodes().close_nodes(a.id());

        assert!(
            contacts
                .iter()
                .any(|contact| { contact.id == a.id() && contact.address == a.address() })
        );
    }

    #[tokio::test]
    async fn node_can_discover_another_node_through_lookup() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let c = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        // B sends PING to A, so A learns B.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        // C sends PING to B, so B learns C.
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        let found = crate::lookup::lookup_node(a.rpc(), c.id()).await;

        assert!(
            found
                .iter()
                .any(|contact| { contact.id == c.id() && contact.address == c.address() })
        );
    }

    #[tokio::test]
    async fn real_nodes_can_store_and_find_value() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let value = vec![0xAB; 5000];
        let key = crate::hashing::key_for_value(&value);

        assert!(
            a.rpc()
                .store(&b.contact(), key, value.clone().into(),)
                .await
        );

        let result = a.rpc().find_value(&b.contact(), key).await;

        assert_eq!(result, crate::rpc::FindValue::Value(value.into()));
    }

    #[tokio::test]
    async fn node_can_find_value_through_network() {
        let a = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let b = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let c = RealNode::bind("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();

        let value = vec![0xAB; 5000];
        let key = crate::hashing::key_for_value(&value);

        // Make A learn B.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        // Make B learn C.
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        // Preload C with the value.
        assert!(
            a.rpc()
                .store(&c.contact(), key, value.clone().into(),)
                .await
        );
        let found: Option<(Contact, Vec<u8>)> = crate::lookup::lookup_value(a.rpc(), key).await;

        let c_contact = Contact {
            id: c.id(),
            address: c.address(),
        };

        assert_eq!(found, Some((c_contact, value)));
    }
    #[tokio::test]
    async fn fake_nodes_can_ping_each_other() {
        let network = Network::new();

        let a = FakeNode::new(&network);

        let b = FakeNode::new(&network);

        assert_eq!(a.rpc().ping(b.address()).await, Some(b.id()));
    }
    #[tokio::test]
    async fn fake_node_can_discover_another_node_through_lookup() {
        let network = Network::new();

        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);
        let c = FakeNode::new(&network);

        // Make A learn B.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        // Make B learn C.
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        let found = crate::lookup::lookup_node(a.rpc(), c.id()).await;

        assert!(
            found
                .iter()
                .any(|contact| { contact.id == c.id() && contact.address == c.address() })
        );
    }
    #[tokio::test]
    async fn fake_nodes_can_store_and_find_value() {
        let network = Network::new();

        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);

        let value = vec![0xAB; 5000];
        let key = crate::hashing::key_for_value(&value);

        assert!(a.rpc().store(&b.contact(), key, value.clone().into()).await);

        let result = a.rpc().find_value(&b.contact(), key).await;

        assert_eq!(result, crate::rpc::FindValue::Value(value.into()));
    }
    #[tokio::test]
    async fn high_level_store_replicates_value() {
        let network = Network::new();

        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);
        let c = FakeNode::new(&network);

        // Make A learn B.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));

        // Make B learn C.
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        let value = b"hello kademlia".to_vec();
        let expected_key = crate::hashing::key_for_value(&value);

        let key = crate::lookup::store_value(a.rpc(), value.clone().into()).await;

        assert_eq!(key, expected_key);

        // There are only 3 nodes and k = 10, so all three should
        // be among the k closest and receive the value.
        assert_eq!(
            a.rpc().find_value(&a.contact(), key).await,
            crate::rpc::FindValue::Value(value.clone().into())
        );

        assert_eq!(
            a.rpc().find_value(&b.contact(), key).await,
            crate::rpc::FindValue::Value(value.clone().into())
        );

        assert_eq!(
            a.rpc().find_value(&c.contact(), key).await,
            crate::rpc::FindValue::Value(value.into())
        );
    }

    /// A package's latest pointer, end to end over the fake network: a
    /// forged one is refused by every node, the owner's is stored, and a
    /// reader on another node finds it through its DNS record.

    #[tokio::test]
    async fn latest_pointer_round_trips_and_forgery_is_refused() {
        use crate::dns_data::fake_dns::FAKE_KEYS;
        use crate::package::{
            LATEST_POINTER_TAG, LatestPointer, VERSION_RECORD_TAG, VersionRecord, record_key, sign,
        };

        let network = Network::new();

        let a = FakeNode::new(&network);
        let b = FakeNode::new(&network);
        let c = FakeNode::new(&network);

        // Let the nodes discover each other.
        assert_eq!(b.rpc().ping(a.address()).await, Some(a.id()));
        assert_eq!(c.rpc().ping(b.address()).await, Some(b.id()));

        let domain = FAKE_KEYS[0].domain;
        let owner = FAKE_KEYS[0].signing_key().unwrap();
        let imposter = FAKE_KEYS[1].signing_key().unwrap();

        // Helper: create a signed first publication.
        let publication = |signer: &ed25519_dalek::SigningKey, version: &str| {
            let record = sign(
                VersionRecord {
                    tag: VERSION_RECORD_TAG.to_string(),
                    domain: domain.to_string(),
                    package: "pkg".to_string(),
                    version: version.parse().unwrap(),
                    blob_hash: [7; 32],
                    prev: [0; 32],
                },
                signer,
            )
            .unwrap();

            let pointer = sign(
                LatestPointer {
                    tag: LATEST_POINTER_TAG.to_string(),
                    domain: domain.to_string(),
                    package: "pkg".to_string(),
                    version: record.body.version,
                    version_record_hash: record_key(&record).unwrap(),
                },
                signer,
            )
            .unwrap();

            (pointer, vec![record])
        };

        // Attempt 1: a forged publication.
        let (forged, forged_proof) = publication(&imposter, "9.0.0");

        let (key, accepted) =
            crate::lookup::store_value_with_proof(a.rpc(), Value::Latest(forged), &forged_proof)
                .await;

        // Nobody should accept the impostor's publication.
        assert_eq!(accepted, 0);

        for node in [&a, &b, &c] {
            assert!(node.context.values.get(&key).is_none(), "forgery stored");
        }

        // Attempt 2: a real publication by the domain owner.
        let (genuine, genuine_proof) = publication(&owner, "1.0.0");

        let (genuine_key, accepted) = crate::lookup::store_value_with_proof(
            a.rpc(),
            Value::Latest(genuine.clone()),
            &genuine_proof,
        )
        .await;

        // The key is based on domain + package, not version.
        assert_eq!(genuine_key, key);

        // At least one node must acknowledge the publication.
        assert!(accepted > 0, "no node accepted the publication");

        // Node C should now find the genuine latest pointer.
        let found = crate::lookup::get_latest(c.rpc(), c.dns(), domain, "pkg").await;

        assert_eq!(found, Some(genuine));
    }
}
