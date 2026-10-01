use dashmap::{DashMap, mapref::entry::Entry};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// A pool of in-flight request slots. Each outbound RPC registers its id and
/// expected peer here, and gets back a future that resolves when the recv loop
/// calls [`Pending::deliver`] with a matching response.
///
/// Sharded rather than a single `Mutex<HashMap<..>>`: ids are random, so they
/// spread evenly across shards and concurrent senders rarely touch the same one.

struct PendingSlot {
    expected_peer: SocketAddr,
    tx: oneshot::Sender<Vec<u8>>,
}

#[derive(Default)]
pub struct Pending {
    slots: DashMap<u64, PendingSlot>,
}

impl Pending {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Register `id` as an in-flight request to `expected_peer`.
    /// Returns `None` if that id is already in use.
    pub fn register(
        self: &Arc<Self>,
        id: u64,
        expected_peer: SocketAddr,
    ) -> Option<PendingResponse> {
        let (tx, rx) = oneshot::channel();

        match self.slots.entry(id) {
            Entry::Vacant(entry) => {
                entry.insert(PendingSlot { expected_peer, tx });

                Some(PendingResponse {
                    id,
                    pending: Arc::clone(self),
                    rx,
                })
            }
            Entry::Occupied(_) => None,
        }
    }

    /// Hand a response to whoever is awaiting `id`, but only if it came from the
    /// expected peer. Returns false for an unknown id, wrong peer, duplicate,
    /// or already-cancelled response.
    pub fn deliver(&self, id: u64, from: SocketAddr, payload: Vec<u8>) -> bool {
        match self
            .slots
            .remove_if(&id, |_, slot| slot.expected_peer == from) // a la DashMap 
        {
            Some((_, slot)) => slot.tx.send(payload).is_ok(),
            None => false,
        }
    }
}

/// The awaitable half of a registered request. Dropping it deregisters the id.
#[non_exhaustive]
pub struct PendingResponse {
    id: u64,
    pending: Arc<Pending>,
    rx: oneshot::Receiver<Vec<u8>>,
}

impl Future for PendingResponse {
    /// `None` if the slot was dropped without a response ever arriving.
    type Output = Option<Vec<u8>>;

    // TODO change to err instead of Option
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.rx).poll(cx).map(|res| res.ok())
    }
}

impl Drop for PendingResponse {
    fn drop(&mut self) {
        // No-op if deliver() already removed the slot.
        self.pending.slots.remove(&self.id);
    }
}
