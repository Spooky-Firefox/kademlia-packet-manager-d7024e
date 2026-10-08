//! Which node the running code belongs to, so a log line can say so.
//!
//! Many nodes share one process in a simulation, and the `log` crate has a
//! single global logger, which cannot tell them apart by itself. Most log call
//! sites (the transports' receive loops, the serve loops) have no node address
//! in scope either. So the address is kept in a tokio task-local instead:
//! [`scope`] sets it around a future, [`current`] reads it back (the logger
//! does this for every line), and [`spawn`] carries it over into a new task,
//! which would otherwise start with no value at all.
//!
//! Code that runs outside every scope, like a bare `tokio::spawn`, simply
//! logs untagged.

use std::future::Future;
use std::net::SocketAddr;
use tokio::task::JoinHandle;

tokio::task_local! {
    static NODE: SocketAddr;
}

/// The address of the node whose code is running, if it is in a scope.
pub fn current() -> Option<SocketAddr> {
    NODE.try_with(|address| *address).ok()
}

/// Run `future` as node `address`.
pub async fn scope<F: Future>(address: SocketAddr, future: F) -> F::Output {
    NODE.scope(address, future).await
}

/// Run `f` as node `address`, for synchronous code that spawns tasks.
pub fn sync_scope<R>(address: SocketAddr, f: impl FnOnce() -> R) -> R {
    NODE.sync_scope(address, f)
}

/// `tokio::spawn`, keeping the spawner's node in the new task.
pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    match current() {
        Some(address) => tokio::spawn(NODE.scope(address, future)),
        None => tokio::spawn(future),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn spawned_tasks_keep_the_node() {
        let address: SocketAddr = "127.0.0.1:4000".parse().unwrap();
        let seen = scope(address, async { spawn(async { current() }).await.unwrap() }).await;
        assert_eq!(seen, Some(address));
    }

    #[tokio::test]
    async fn outside_a_scope_there_is_no_node() {
        assert_eq!(spawn(async { current() }).await.unwrap(), None);
    }
}
