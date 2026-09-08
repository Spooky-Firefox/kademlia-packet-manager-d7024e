use dashmap::DashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::oneshot;

/// A pool of in-flight request slots. Each outbound RPC registers its id here
/// and gets back a future that resolves when the recv loop calls [`Pending::deliver`]
/// with a response carrying that id.
///
/// Sharded rather than a single `Mutex<HashMap<..>>`: ids are random, so they
/// spread evenly across shards and concurrent senders rarely touch the same one.
#[derive(Default)]
pub struct Pending {
    slots: DashMap<u64, oneshot::Sender<Vec<u8>>>,
    counter: std::sync::atomic::AtomicU64,
}

impl Pending {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Hand out the next request id. A counter rather than a random draw: ids
    /// only have to be unique among this node's in-flight requests.
    pub fn next_id(&self) -> u64 {
        self.counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    }

    /// Register `id` as in-flight. Await the returned future to get the response.
    pub fn register(self: &Arc<Self>, id: u64) -> PendingResponse {
        let (tx, rx) = oneshot::channel();
        self.slots.insert(id, tx);
        PendingResponse {
            id,
            pending: Arc::clone(self),
            rx,
        }
    }

    /// Hand a response to whoever is awaiting `id`. Returns false if no one was
    /// waiting: an unsolicited, duplicate, or already-cancelled response.
    pub fn deliver(&self, id: u64, payload: Vec<u8>) -> bool {
        match self.slots.remove(&id) {
            // Err means the caller gave up and dropped its PendingResponse.
            Some((_, tx)) => tx.send(payload).is_ok(),
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
