//! A large network under churn, all in one process.
//!
//! Grows a network of `FakeNode`s on the in-process fake wire, then keeps
//! removing random nodes and adding new ones while it stores and looks up
//! values, printing how well lookups hold up.
//!
//! ```text
//! cargo run --release --example churn_sim -- [NODES] [DURATION_SECS] [CHURN_PER_SEC] [LOSS] [LATENCY_MS]
//! ```
//!
//! Defaults: 1000 nodes, 60 s, 5 removals + 5 joins per second, no loss, 1 ms
//! latency.
//!
//! When the churn phase ends, the network is left running and a picker asks
//! which node to open the interactive CLI (the same one the binary has) on,
//! for pinging, `put`/`get` and looking at routing tables by hand. `exit` in
//! the CLI goes back to the picker, so several nodes can be visited in turn.
//!
//! Logs go to `sim.log`, each line tagged with the node that wrote it, so one
//! node's history is `grep 'node=127.0.0.1:49200 ' sim.log`. The level comes
//! from `SIM_LOG` (`error`..`trace`, default `debug`): `debug` is each node's
//! join and removal, and `trace` adds every RPC and datagram, which runs to
//! gigabytes at a thousand nodes. Lookup metrics go to
//! `metrics.log` as they do for the binary, for `analyze_metrics.py`.

use kademlia_packet_manager_d7024e::close_nodes::CloseNodes;
use kademlia_packet_manager_d7024e::node::FakeNode;
use kademlia_packet_manager_d7024e::rpc_transport::networked_debug_transport::{
    Network, NetworkConfig,
};
use kademlia_packet_manager_d7024e::{
    bootstrap, cli, hashing, logging, lookup, maintenance, node_scope,
};
use std::error::Error;
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, BufReader};

/// How many nodes join at once while the network is growing.
const JOIN_BATCH: usize = 50;

/// Liveness rounds run far more often than the binary's minute, so contacts
/// of removed nodes are evicted within the run.
const LIVENESS_CHECK_INTERVAL: Duration = Duration::from_secs(5);

/// A store-and-lookup workload is started this many times per second.
const WORKLOADS_PER_SEC: u64 = 4;

/// How often a progress line is printed.
const REPORT_INTERVAL: Duration = Duration::from_secs(5);

type Nodes = Arc<Mutex<Vec<Arc<FakeNode>>>>;

#[derive(Default)]
struct Stats {
    joined: AtomicU64,
    join_failures: AtomicU64,
    removed: AtomicU64,
    /// A value looked up right after it was stored, from another node.
    fresh_lookups: AtomicU64,
    fresh_found: AtomicU64,
    /// A value stored earlier in the run, which churn may have carried off.
    old_lookups: AtomicU64,
    old_found: AtomicU64,
    /// A node looked up by id from another node.
    node_lookups: AtomicU64,
    node_found: AtomicU64,
}

fn percent(hits: &AtomicU64, total: &AtomicU64) -> String {
    let total = total.load(Ordering::Relaxed);
    if total == 0 {
        return "-".into();
    }
    let hits = hits.load(Ordering::Relaxed);
    format!(
        "{:.1}% ({hits}/{total})",
        100.0 * hits as f64 / total as f64
    )
}

impl Stats {
    fn report(&self, elapsed: Duration, live: usize) {
        println!(
            "[{:>6.1}s] live={live} joined={} (failed {}) removed={} | fresh value {} | old value {} | node {}",
            elapsed.as_secs_f64(),
            self.joined.load(Ordering::Relaxed),
            self.join_failures.load(Ordering::Relaxed),
            self.removed.load(Ordering::Relaxed),
            percent(&self.fresh_found, &self.fresh_lookups),
            percent(&self.old_found, &self.old_lookups),
            percent(&self.node_found, &self.node_lookups),
        );
    }
}

/// Positional argument `index`, or `default` when it is missing.
fn arg<T: FromStr>(index: usize, default: T) -> Result<T, Box<dyn Error>>
where
    T::Err: Error + 'static,
{
    match std::env::args().nth(index) {
        Some(raw) => Ok(raw.parse()?),
        None => Ok(default),
    }
}

/// A random node out of `nodes`, if there is one.
fn pick(nodes: &Nodes) -> Option<Arc<FakeNode>> {
    let nodes = nodes.lock().unwrap();
    if nodes.is_empty() {
        return None;
    }
    Some(Arc::clone(&nodes[rand::random_range(0..nodes.len())]))
}

/// Start a node, bootstrap it through a random live one and, on success, add
/// it to `nodes` with its upkeep running.
async fn join(network: &Network, nodes: &Nodes, stats: &Stats) {
    let node = Arc::new(FakeNode::new(network));
    let Some(seed) = pick(nodes) else {
        return;
    };
    match node
        .scoped(bootstrap::bootstrap(node.rpc(), seed.address()))
        .await
    {
        Ok(contacts) => {
            node_scope::sync_scope(node.address(), || {
                log::debug!(
                    "joined through {}, bootstrap found {} contacts",
                    seed.address(),
                    contacts.len()
                )
            });
            node.periodic_task(maintenance::REPUBLISH_INTERVAL, LIVENESS_CHECK_INTERVAL);
            nodes.lock().unwrap().push(node);
            stats.joined.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) => {
            node_scope::sync_scope(node.address(), || {
                log::warn!("bootstrap through {} failed: {error:?}", seed.address())
            });
            node.shutdown();
            stats.join_failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Take a random node other than the first (the original seed, kept so there
/// is always somebody to join through) off the network.
fn remove_one(nodes: &Nodes, stats: &Stats) {
    let removed = {
        let mut nodes = nodes.lock().unwrap();
        if nodes.len() <= 1 {
            return;
        }
        let index = rand::random_range(1..nodes.len());
        nodes.swap_remove(index)
    };
    node_scope::sync_scope(removed.address(), || {
        log::debug!("removed from the network")
    });
    removed.shutdown();
    stats.removed.fetch_add(1, Ordering::Relaxed);
}

/// Store a fresh value from one node and look it up from another, look up an
/// older value, and look up a node by id.
async fn workload(nodes: Nodes, stats: Arc<Stats>, stored: Arc<Mutex<Vec<Vec<u8>>>>) {
    let (Some(writer), Some(reader)) = (pick(&nodes), pick(&nodes)) else {
        return;
    };

    let value: Vec<u8> = (0..32).map(|_| rand::random::<u8>()).collect();
    let key = writer
        .scoped(lookup::store_value(writer.rpc(), value.clone()))
        .await;
    let found = reader.scoped(lookup::lookup_value(reader.rpc(), key)).await;
    stats.fresh_lookups.fetch_add(1, Ordering::Relaxed);
    if found.is_some_and(|(_, got)| got == value) {
        stats.fresh_found.fetch_add(1, Ordering::Relaxed);
    }

    let old = {
        let mut stored = stored.lock().unwrap();
        stored.push(value);
        stored[rand::random_range(0..stored.len())].clone()
    };
    let old_key = hashing::key_for_value(&old);
    let found = reader
        .scoped(lookup::lookup_value(reader.rpc(), old_key))
        .await;
    stats.old_lookups.fetch_add(1, Ordering::Relaxed);
    if found.is_some_and(|(_, got)| got == old) {
        stats.old_found.fetch_add(1, Ordering::Relaxed);
    }

    let Some(target) = pick(&nodes) else {
        return;
    };
    let found = reader
        .scoped(lookup::lookup_node(reader.rpc(), target.id()))
        .await;
    stats.node_lookups.fetch_add(1, Ordering::Relaxed);
    if found.iter().any(|contact| *contact == target.contact()) {
        stats.node_found.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let node_count: usize = arg(1, 1000)?;
    let duration = Duration::from_secs(arg(2, 60)?);
    let churn_per_sec: f64 = arg(3, 5.0)?;
    let loss: f64 = arg(4, 0.0)?;
    let latency = Duration::from_millis(arg(5, 1)?);

    let level = match std::env::var("SIM_LOG") {
        Ok(level) => level.parse()?,
        Err(_) => log::LevelFilter::Debug,
    };
    // fern appends, and a log mixing runs cannot be read per node.
    let _ = std::fs::remove_file("sim.log");
    logging::setup_sim("sim.log", level)?;

    let network = Network::with_config(NetworkConfig {
        latency,
        loss,
        ..NetworkConfig::default()
    });
    let nodes: Nodes = Arc::default();
    let stats = Arc::new(Stats::default());
    let start = Instant::now();

    // The seed: nobody to bootstrap through, so it starts the network.
    let seed = Arc::new(FakeNode::new(&network));
    seed.periodic_task(maintenance::REPUBLISH_INTERVAL, LIVENESS_CHECK_INTERVAL);
    nodes.lock().unwrap().push(seed);

    println!("growing the network to {node_count} nodes...");
    while nodes.lock().unwrap().len() < node_count {
        let missing = node_count - nodes.lock().unwrap().len();
        let batch = (0..missing.min(JOIN_BATCH)).map(|_| join(&network, &nodes, &stats));
        futures::future::join_all(batch).await;
        if stats.joined.load(Ordering::Relaxed) % 200 < JOIN_BATCH as u64 {
            stats.report(start.elapsed(), nodes.lock().unwrap().len());
        }
    }
    stats.report(start.elapsed(), nodes.lock().unwrap().len());

    println!(
        "churning for {}s at {churn_per_sec} removals and joins per second...",
        duration.as_secs()
    );
    let stored: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let churn_start = Instant::now();
    let tick = Duration::from_millis(1000 / WORKLOADS_PER_SEC);
    let mut ticker = tokio::time::interval(tick);
    let mut next_report = churn_start + REPORT_INTERVAL;
    // Churn owed but not yet done, so fractional rates come out right.
    let mut churn_owed = 0.0;

    while churn_start.elapsed() < duration {
        ticker.tick().await;

        churn_owed += churn_per_sec * tick.as_secs_f64();
        while churn_owed >= 1.0 {
            churn_owed -= 1.0;
            remove_one(&nodes, &stats);
            let (network, nodes, stats) = (network.clone(), Arc::clone(&nodes), Arc::clone(&stats));
            tokio::spawn(async move { join(&network, &nodes, &stats).await });
        }

        tokio::spawn(workload(
            Arc::clone(&nodes),
            Arc::clone(&stats),
            Arc::clone(&stored),
        ));

        if Instant::now() >= next_report {
            next_report += REPORT_INTERVAL;
            stats.report(start.elapsed(), nodes.lock().unwrap().len());
        }
    }

    // Let the last workloads finish before the final count.
    tokio::time::sleep(Duration::from_secs(3)).await;
    println!("done.");
    stats.report(start.elapsed(), nodes.lock().unwrap().len());

    // Churn has stopped, but every node is still up and running its upkeep,
    // so the CLI drives a live network.
    choose_and_run_cli(&nodes).await?;
    Ok(())
}

fn print_picker_help() {
    println!("pick a node to run the CLI on:");
    println!("  list [N]      the first N live nodes (default 20)");
    println!("  INDEX         the node at that index in the list");
    println!("  IP:PORT       the node at that address");
    println!("  random        any live node");
    println!("  help");
    println!("  quit");
}

/// Let the user pick a live node, run the CLI on it until `exit`, and then
/// pick again, until `quit` or EOF.
async fn choose_and_run_cli(nodes: &Nodes) -> std::io::Result<()> {
    // One reader for the picker and every CLI session, so neither buffers
    // away lines meant for the other.
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let live = nodes.lock().unwrap().len();
    println!("\n{live} nodes are live; index 0 is the original seed.");
    print_picker_help();

    loop {
        print!("pick> ");
        std::io::stdout().flush()?;
        let Some(line) = lines.next_line().await? else {
            return Ok(());
        };
        let parts: Vec<&str> = line.split_whitespace().collect();

        let chosen = match parts.as_slice() {
            [] => continue,
            ["quit"] | ["exit"] => return Ok(()),
            ["help"] => {
                print_picker_help();
                continue;
            }
            ["list"] => {
                list_nodes(nodes, 20);
                continue;
            }
            ["list", count] => {
                match count.parse() {
                    Ok(count) => list_nodes(nodes, count),
                    Err(_) => println!("list takes a number"),
                }
                continue;
            }
            ["random"] => pick(nodes),
            [choice] => find_node(nodes, choice),
            _ => None,
        };

        let Some(node) = chosen else {
            println!("no such node; `list` shows them, `help` the commands");
            continue;
        };

        println!(
            "CLI on {} ({}); `exit` returns to the picker",
            node.address(),
            hex::encode(&node.id()[..4])
        );
        // Tagged as the chosen node in sim.log.
        node.scoped(cli::run_with(&*node, &mut lines)).await?;
    }
}

/// The live node at list index `choice`, or at address `choice`.
fn find_node(nodes: &Nodes, choice: &str) -> Option<Arc<FakeNode>> {
    let nodes = nodes.lock().unwrap();
    if let Ok(index) = choice.parse::<usize>() {
        return nodes.get(index).cloned();
    }
    let address: SocketAddr = choice.parse().ok()?;
    nodes.iter().find(|node| node.address() == address).cloned()
}

fn list_nodes(nodes: &Nodes, count: usize) {
    let nodes = nodes.lock().unwrap();
    for (index, node) in nodes.iter().enumerate().take(count) {
        println!(
            "  {index:>4}  {}  {}  {} contacts, {} values",
            node.address(),
            hex::encode(&node.id()[..4]),
            node.rpc().close_nodes().contacts_iter().count(),
            node.datastore_snapshot().len(),
        );
    }
    if nodes.len() > count {
        println!("  ... {} more", nodes.len() - count);
    }
}
