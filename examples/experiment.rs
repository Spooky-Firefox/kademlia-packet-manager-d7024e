//! One run of an experiment: build a network, store values, measure lookups.
//!
//! ```text
//! cargo run --release --example experiment -- --nodes 500 --seed 3 --loss 0.2 --out runs/demo
//! ```
//!
//! Every lookup the run measures is wrapped in
//! [`instrumentation::with_op`], so its `lookup_end` and `rpc` lines carry the
//! same `op=` as the `event=op` line that reports it; background work
//! (republishing, liveness pings) logs `op=0`.
//!
//! Everything goes to `metrics.log` in the `--out` directory as one
//! `event=NAME key=value ...` line per event, for `experiments/analyze.py`.
//! `experiments/run_suite.py` runs many of these, one directory per run.
//!
//! # What a run does
//!
//! 1. **build** — places `--nodes` nodes at addresses drawn from the seed
//!    (random `10.x.y.z:port`), so the seed decides every node id, since an id
//!    is the hash of its address. They join in batches, each through a random
//!    node that has already joined. The wire is lossless during this phase.
//! 2. **refresh** (optional) — each node looks up `--refresh` random ids,
//!    standing in for the bucket refresh that `bootstrap` does not do yet.
//! 3. **settle** — waits `--settle` seconds.
//! 4. **store** — stores `--values` random values from random nodes, still
//!    lossless, then counts which nodes hold each one.
//! 5. **measure** — sets the wire's loss to `--loss`, starts churn if
//!    `--churn` asks for it, and runs `--lookups` value lookups and as many
//!    node lookups, alternating: `--concurrency` at a time, or, with
//!    `--duration-secs`, started at an even pace over that long.
//!
//! # What is and is not repeatable
//!
//! The seed fixes the topology, the join order and seeds, the values, which
//! node runs each lookup and what it looks for, and the sequence of loss
//! draws. It cannot fix how concurrent tasks interleave, so which datagram
//! meets which loss draw, and what a routing table holds when a lookup reads
//! it, vary a little between runs of the same seed. That is why a
//! configuration is run with several seeds.

use kademlia_packet_manager_d7024e::close_nodes::{CloseNodes, K, Key, NodeId, xor_distance_cmp};
use kademlia_packet_manager_d7024e::node::FakeNode;
use kademlia_packet_manager_d7024e::rpc_transport::networked_debug_transport::{
    Network, NetworkConfig,
};
use kademlia_packet_manager_d7024e::rpc_transport::retry_transport::{
    MAX_ATTEMPTS, RESEND_INTERVAL,
};
use kademlia_packet_manager_d7024e::rpc_transport::stream_framing::STREAM_DEADLINE;
use kademlia_packet_manager_d7024e::{
    bootstrap, hashing, instrumentation, logging, lookup, maintenance,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::error::Error;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How many nodes join at once while the network is built.
const JOIN_BATCH: usize = 50;

/// How big each stored value is.
const VALUE_LEN: usize = 64;

/// A `metrics` line, the same target `instrumentation` writes to.
macro_rules! metric {
    ($($arg:tt)*) => { log::info!(target: "metrics", $($arg)*) };
}

#[derive(Clone, Debug)]
struct Config {
    nodes: usize,
    seed: u64,
    loss: f64,
    latency: Duration,
    alpha: usize,
    lookups: usize,
    values: usize,
    /// Nodes replaced (one leaves, one joins) per second while measuring.
    churn: f64,
    concurrency: usize,
    threads: usize,
    liveness: Duration,
    /// How often each node republishes the values it holds.
    republish: Duration,
    settle: Duration,
    refresh: usize,
    /// When set, the measured lookups are spread evenly over this long
    /// instead of run `concurrency` at a time as fast as they finish.
    duration: Option<Duration>,
    rpc_events: bool,
    out: PathBuf,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            nodes: 200,
            seed: 1,
            loss: 0.0,
            latency: Duration::from_millis(5),
            alpha: lookup::DEFAULT_ALPHA,
            lookups: 500,
            values: 100,
            churn: 0.0,
            concurrency: 16,
            threads: 2,
            liveness: maintenance::LIVENESS_CHECK_INTERVAL,
            republish: maintenance::REPUBLISH_INTERVAL,
            settle: Duration::from_secs(2),
            refresh: 0,
            duration: None,
            rpc_events: true,
            out: PathBuf::from("experiment-out"),
        }
    }
}

const USAGE: &str = "\
usage: experiment [--nodes N] [--seed S] [--loss P] [--latency-ms MS] [--alpha A]
                  [--lookups M] [--values V] [--churn PER_SEC] [--concurrency C]
                  [--threads T] [--liveness-secs S] [--republish-secs S] [--settle-secs S]
                  [--refresh R] [--duration-secs S] [--no-rpc-events] [--out DIR]";

fn parse_args() -> Result<Config, Box<dyn Error>> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--no-rpc-events" {
            config.rpc_events = false;
            continue;
        }
        if flag == "--help" || flag == "-h" {
            println!("{USAGE}");
            std::process::exit(0);
        }
        let value = args
            .next()
            .ok_or_else(|| format!("{flag} needs a value\n{USAGE}"))?;
        match flag.as_str() {
            "--nodes" => config.nodes = value.parse()?,
            "--seed" => config.seed = value.parse()?,
            "--loss" => config.loss = value.parse()?,
            "--latency-ms" => config.latency = Duration::from_secs_f64(value.parse::<f64>()? / 1e3),
            "--alpha" => config.alpha = value.parse()?,
            "--lookups" => config.lookups = value.parse()?,
            "--values" => config.values = value.parse()?,
            "--churn" => config.churn = value.parse()?,
            "--concurrency" => config.concurrency = value.parse()?,
            "--threads" => config.threads = value.parse()?,
            "--liveness-secs" => config.liveness = Duration::from_secs_f64(value.parse()?),
            "--republish-secs" => config.republish = Duration::from_secs_f64(value.parse()?),
            "--settle-secs" => config.settle = Duration::from_secs_f64(value.parse()?),
            "--refresh" => config.refresh = value.parse()?,
            "--duration-secs" => config.duration = Some(Duration::from_secs_f64(value.parse()?)),
            "--out" => config.out = PathBuf::from(value),
            _ => return Err(format!("unknown flag {flag}\n{USAGE}").into()),
        }
    }
    if config.nodes < 2 {
        return Err("--nodes must be at least 2".into());
    }
    Ok(config)
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = parse_args()?;
    std::fs::create_dir_all(&config.out)?;
    // metrics.log and sim.log land in the working directory.
    std::env::set_current_dir(&config.out)?;
    // fern appends, and a log mixing two runs would be read as one.
    let _ = std::fs::remove_file("metrics.log");
    let _ = std::fs::remove_file("sim.log");
    logging::setup_sim("sim.log", log::LevelFilter::Warn)?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.threads.max(1))
        .enable_all()
        .build()?;
    runtime.block_on(run(config));
    log::logger().flush();
    // Skip tearing down thousands of nodes one task at a time.
    std::process::exit(0);
}

/// The nodes currently in the network. Index 0 is never removed by churn,
/// so there is always someone to join through.
type Nodes = Arc<Mutex<Vec<Arc<FakeNode>>>>;

struct Experiment {
    config: Config,
    network: Network,
    nodes: Nodes,
    /// Draws addresses for nodes that join during churn. The build and the
    /// workload draw from their own generators up front, so the order churn
    /// happens in cannot change them.
    churn_rng: Mutex<StdRng>,
    start: Instant,
}

impl Experiment {
    fn phase(&self, name: &str) {
        metric!(
            "event=phase name={name} t_ms={} live={}",
            self.start.elapsed().as_millis(),
            self.live()
        );
        println!(
            "[{:>7.1}s] {name} ({} live nodes)",
            self.start.elapsed().as_secs_f64(),
            self.live()
        );
    }

    fn live(&self) -> usize {
        self.nodes.lock().unwrap().len()
    }

    fn snapshot(&self) -> Vec<Arc<FakeNode>> {
        self.nodes.lock().unwrap().clone()
    }

    /// The node at `pick` modulo the number of live nodes, so a pick drawn
    /// before the run still lands on someone when churn has changed the list.
    fn nth(&self, pick: u64) -> Arc<FakeNode> {
        let nodes = self.nodes.lock().unwrap();
        Arc::clone(&nodes[(pick % nodes.len() as u64) as usize])
    }

    /// Start a node at a free address drawn from `rng`.
    fn new_node(&self, rng: &mut StdRng) -> FakeNode {
        loop {
            if let Some(node) = FakeNode::at(&self.network, random_address(rng)) {
                return node;
            }
        }
    }

    /// Bootstrap `node` through `seed` and, if that works, start its upkeep
    /// and add it to the network.
    async fn join(&self, node: FakeNode, seed: Arc<FakeNode>) -> bool {
        let joined = node
            .scoped(bootstrap::bootstrap(node.rpc(), seed.address()))
            .await;
        metric!(
            "event=join node={} seed={} success={} contacts={}",
            node.address(),
            seed.address(),
            joined.is_ok(),
            joined.as_ref().map_or(0, Vec::len),
        );
        if joined.is_err() {
            node.shutdown();
            return false;
        }
        node.periodic_task(self.config.republish, self.config.liveness);
        self.nodes.lock().unwrap().push(Arc::new(node));
        true
    }

    async fn build(&self, rng: &mut StdRng) {
        let first = self.new_node(rng);
        first.periodic_task(self.config.republish, self.config.liveness);
        self.nodes.lock().unwrap().push(Arc::new(first));

        let mut failures = 0;
        while self.live() < self.config.nodes {
            let missing = self.config.nodes - self.live();
            // Draw the whole batch before any of it runs, so the draws do not
            // depend on which join finishes first.
            let batch: Vec<_> = (0..missing.min(JOIN_BATCH))
                .map(|_| {
                    let node = self.new_node(rng);
                    let seed = self.nth(rng.random());
                    (node, seed)
                })
                .collect();
            let joined = futures::future::join_all(
                batch.into_iter().map(|(node, seed)| self.join(node, seed)),
            )
            .await;
            failures += joined.iter().filter(|ok| !**ok).count();
            if failures > self.config.nodes {
                panic!("{failures} joins failed; the network is not forming");
            }
        }
    }

    /// Have every node look up `count` random ids, to fill the far buckets a
    /// self-lookup leaves thin.
    async fn refresh(&self, rng: &mut StdRng) {
        let work: Vec<(Arc<FakeNode>, NodeId)> = self
            .snapshot()
            .into_iter()
            .flat_map(|node| (0..self.config.refresh).map(move |_| Arc::clone(&node)))
            .map(|node| (node, rng.random()))
            .collect();
        run_concurrently(self.config.concurrency, work, |(node, target)| async move {
            node.scoped(lookup::lookup_node(node.rpc(), target)).await;
        })
        .await;
    }

    async fn store(&self, rng: &mut StdRng) -> Vec<(Key, Vec<u8>)> {
        let work: Vec<(Arc<FakeNode>, Vec<u8>)> = (0..self.config.values)
            .map(|_| {
                let value: Vec<u8> = (0..VALUE_LEN).map(|_| rng.random()).collect();
                (self.nth(rng.random()), value)
            })
            .collect();
        let values: Vec<(Key, Vec<u8>)> = work
            .iter()
            .map(|(_, value)| (hashing::key_for_value(value), value.clone()))
            .collect();

        run_concurrently(
            self.config.concurrency,
            work,
            |(origin, value)| async move {
                origin
                    .scoped(lookup::store_value(origin.rpc(), value))
                    .await;
            },
        )
        .await;

        let nodes = self.snapshot();
        for (key, _) in &values {
            let closest = true_closest(&nodes, *key);
            metric!(
                "event=stored key={} holders={} closest_holders={}",
                hex::encode(key),
                nodes.iter().filter(|node| node.holds(key)).count(),
                closest.iter().filter(|node| node.holds(key)).count(),
            );
        }
        values
    }

    /// Replace `churn` nodes per second until `done` says to stop.
    async fn churn(self: Arc<Self>, done: Arc<AtomicUsize>) {
        if self.config.churn <= 0.0 {
            return;
        }
        let period = Duration::from_secs_f64(1.0 / self.config.churn);
        let mut ticker = tokio::time::interval(period);
        ticker.tick().await;
        while done.load(Ordering::Relaxed) == 0 {
            ticker.tick().await;
            let (leaving, node, seed) = {
                let mut rng = self.churn_rng.lock().unwrap();
                let mut nodes = self.nodes.lock().unwrap();
                if nodes.len() < 3 {
                    continue;
                }
                let index = rng.random_range(1..nodes.len());
                let leaving = nodes.swap_remove(index);
                let seed = Arc::clone(&nodes[rng.random_range(0..nodes.len())]);
                drop(nodes);
                (leaving, self.new_node(&mut rng), seed)
            };
            leaving.shutdown();
            metric!("event=churn action=leave node={}", leaving.address());
            let this = Arc::clone(&self);
            tokio::spawn(async move {
                this.join(node, seed).await;
            });
        }
    }

    async fn measure(self: &Arc<Self>, rng: &mut StdRng, values: &[(Key, Vec<u8>)]) {
        // Drawn before anything runs: the same ops in the same order for a
        // seed, whatever churn and scheduling do to the timing.
        let ops: Vec<Op> = (0..self.config.lookups * 2)
            .map(|i| Op {
                id: i as u64 + 1,
                kind: if i % 2 == 0 {
                    OpKind::Value
                } else {
                    OpKind::Node
                },
                origin: rng.random(),
                target: rng.random(),
            })
            .collect();

        let done = Arc::new(AtomicUsize::new(0));
        let churn = tokio::spawn(Arc::clone(self).churn(Arc::clone(&done)));

        let values = Arc::new(values.to_vec());
        let started = Instant::now();
        match self.config.duration {
            None => {
                let this = Arc::clone(self);
                run_concurrently(self.config.concurrency, ops, move |op| {
                    let this = Arc::clone(&this);
                    let values = Arc::clone(&values);
                    async move { this.run_op(op, &values, started).await }
                })
                .await;
            }
            Some(duration) => {
                // Op i starts at i/len of the way through, however long the
                // ones before it are taking.
                let gap = duration.div_f64(ops.len().max(1) as f64);
                let mut running = Vec::with_capacity(ops.len());
                for (i, op) in ops.into_iter().enumerate() {
                    tokio::time::sleep_until((started + gap * i as u32).into()).await;
                    let this = Arc::clone(self);
                    let values = Arc::clone(&values);
                    running.push(tokio::spawn(async move {
                        this.run_op(op, &values, started).await
                    }));
                }
                futures::future::join_all(running).await;
            }
        }

        done.store(1, Ordering::Relaxed);
        churn.abort();
    }

    /// Run one lookup and log how it went; `t_ms` is when it started,
    /// counted from `measure_start`.
    async fn run_op(&self, op: Op, values: &[(Key, Vec<u8>)], measure_start: Instant) {
        let t_ms = measure_start.elapsed().as_millis();
        let origin = self.nth(op.origin);
        match op.kind {
            OpKind::Value => {
                let (key, value) = &values[(op.target % values.len() as u64) as usize];
                let nodes = self.snapshot();
                let holders = nodes.iter().filter(|node| node.holds(key)).count();
                let started = Instant::now();
                let found = instrumentation::with_op(
                    op.id,
                    origin.scoped(lookup::lookup_value(origin.rpc(), *key)),
                )
                .await;
                let took = started.elapsed();
                metric!(
                    "event=op op={} kind=value t_ms={} origin={} key={} success={} correct={} available={} holders={} duration_us={}",
                    op.id,
                    t_ms,
                    origin.address(),
                    hex::encode(key),
                    found.is_some(),
                    found.as_ref().is_some_and(|(_, got)| got == value),
                    holders > 0,
                    holders,
                    took.as_micros(),
                );
            }
            OpKind::Node => {
                let target = self.nth(op.target);
                let nodes = self.snapshot();
                let closest = true_closest(&nodes, target.id());
                let started = Instant::now();
                let found = instrumentation::with_op(
                    op.id,
                    origin.scoped(lookup::lookup_node(origin.rpc(), target.id())),
                )
                .await;
                let took = started.elapsed();
                let recall = closest
                    .iter()
                    .filter(|node| found.iter().any(|c| c.id == node.id()))
                    .count();
                metric!(
                    "event=op op={} kind=node t_ms={} origin={} target={} success={} recall={} of={} duration_us={}",
                    op.id,
                    t_ms,
                    origin.address(),
                    target.address(),
                    found.iter().any(|c| c.id == target.id()),
                    recall,
                    closest.len(),
                    took.as_micros(),
                );
            }
        }
    }
}

#[derive(Clone, Copy)]
enum OpKind {
    Value,
    Node,
}

#[derive(Clone, Copy)]
struct Op {
    /// Logged as `op=` on the op's own line and on every lookup and RPC it
    /// makes, which is how the analysis tells them from background work.
    id: u64,
    kind: OpKind,
    origin: u64,
    target: u64,
}

/// A random address in 10.0.0.0/8 on a non-privileged port.
fn random_address(rng: &mut StdRng) -> SocketAddr {
    let host: u32 = rng.random_range(1..(1 << 24) - 1);
    let ip = Ipv4Addr::from(0x0A00_0000 | host);
    SocketAddr::new(ip.into(), rng.random_range(1024..=u16::MAX))
}

/// The `K` live nodes closest to `target`, the set an ideal lookup returns.
fn true_closest(nodes: &[Arc<FakeNode>], target: NodeId) -> Vec<Arc<FakeNode>> {
    let mut sorted: Vec<_> = nodes.to_vec();
    if sorted.len() > K {
        sorted.select_nth_unstable_by(K, |a, b| xor_distance_cmp(a.id(), b.id(), target));
        sorted.truncate(K);
    }
    sorted
}

/// Run `f` on every item of `work`, `limit` at a time.
async fn run_concurrently<W, F, Fut>(limit: usize, work: Vec<W>, f: F)
where
    F: Fn(W) -> Fut,
    Fut: Future<Output = ()>,
{
    use futures::stream::StreamExt;
    futures::stream::iter(work.into_iter().map(f))
        .buffer_unordered(limit.max(1))
        .collect::<()>()
        .await;
}

async fn run(config: Config) {
    lookup::set_alpha(config.alpha);

    // Separate streams per purpose, each from the seed, so adding draws to
    // one phase does not shift another's.
    let mut build_rng = StdRng::seed_from_u64(config.seed);
    let mut work_rng = StdRng::seed_from_u64(config.seed.wrapping_add(0x5EED_0001));
    let churn_rng = StdRng::seed_from_u64(config.seed.wrapping_add(0x5EED_0002));

    let network = Network::with_config(NetworkConfig {
        latency: config.latency,
        loss: 0.0,
        seed: Some(config.seed.wrapping_add(0x5EED_0003)),
    });

    metric!(
        "event=run_config nodes={} seed={} loss={} latency_ms={} alpha={} k={} lookups={} values={} churn={} concurrency={} threads={} liveness_s={} republish_s={} refresh={} duration_s={} resend_ms={} max_attempts={} stream_deadline_ms={}",
        config.nodes,
        config.seed,
        config.loss,
        config.latency.as_secs_f64() * 1e3,
        lookup::alpha(),
        K,
        config.lookups,
        config.values,
        config.churn,
        config.concurrency,
        config.threads,
        config.liveness.as_secs_f64(),
        config.republish.as_secs_f64(),
        config.refresh,
        config.duration.map_or(0.0, |d| d.as_secs_f64()),
        RESEND_INTERVAL.as_millis(),
        MAX_ATTEMPTS,
        STREAM_DEADLINE.as_millis(),
    );

    let experiment = Arc::new(Experiment {
        config: config.clone(),
        network,
        nodes: Arc::default(),
        churn_rng: Mutex::new(churn_rng),
        start: Instant::now(),
    });

    experiment.phase("build");
    experiment.build(&mut build_rng).await;

    if config.refresh > 0 {
        experiment.phase("refresh");
        experiment.refresh(&mut work_rng).await;
    }

    experiment.phase("settle");
    tokio::time::sleep(config.settle).await;

    let tables: Vec<usize> = experiment
        .snapshot()
        .iter()
        .map(|node| node.rpc().close_nodes().contacts_iter().count())
        .collect();
    metric!(
        "event=routing_tables mean={:.1} min={} max={}",
        tables.iter().sum::<usize>() as f64 / tables.len() as f64,
        tables.iter().min().unwrap(),
        tables.iter().max().unwrap(),
    );

    experiment.phase("store");
    let values = experiment.store(&mut work_rng).await;

    experiment.network.set_loss(config.loss);
    instrumentation::set_rpc_events(config.rpc_events);
    experiment.phase("measure");
    experiment.measure(&mut work_rng, &values).await;
    instrumentation::set_rpc_events(false);
    experiment.phase("done");
}
