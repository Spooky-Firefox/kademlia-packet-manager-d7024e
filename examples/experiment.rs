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
//!    node that has already joined. The wire is lossless during this phase,
//!    and its latency is at most 5 ms (`SETUP_LATENCY`).
//! 2. **refresh** (optional) — each node looks up `--refresh` more random
//!    ids, on top of the bucket refresh `bootstrap` already does.
//! 3. **settle** — waits `--settle` seconds.
//! 4. **store** — stores `--values` random values from random nodes, still
//!    lossless, then counts which nodes hold each one.
//! 5. **measure** — sets the wire's loss to `--loss` and latency to
//!    `--latency-ms`, starts churn if
//!    `--churn` asks for it, and runs `--lookups` value lookups and as many
//!    node lookups, alternating: `--concurrency` at a time, or, with
//!    `--duration-secs`, started at an even pace over that long.
//!
//! # The package workload
//!
//! With `--workload packages`, step 4 instead **publishes** `--packages`
//! packages of `--versions` versions each (`registry::publish_package`, each
//! package from one random node, its versions in order; with
//! `--lossy-publish` under the measure phase's loss and latency), and step 5
//! runs `--lookups` **installs** (`registry::install_package`) from random
//! nodes: half of the latest version, half of a random published one. An
//! install fetches the latest pointer, walks and verifies the whole version
//! chain, then fetches the blob, so its cost grows with the chain.
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
use kademlia_packet_manager_d7024e::dns_data::fake_dns::FAKE_KEYS;
use kademlia_packet_manager_d7024e::instrumentation::RpcEvents;
use kademlia_packet_manager_d7024e::node::FakeNode;
use kademlia_packet_manager_d7024e::package::{Version, latest_key};
use kademlia_packet_manager_d7024e::rpc_transport::networked_debug_transport::{
    Network, NetworkConfig,
};
use kademlia_packet_manager_d7024e::rpc_transport::retry_transport::{
    MAX_ATTEMPTS, RESEND_INTERVAL,
};
use kademlia_packet_manager_d7024e::rpc_transport::stream_framing::STREAM_DEADLINE;
use kademlia_packet_manager_d7024e::{
    bootstrap, hashing, instrumentation, logging, lookup, maintenance, registry,
};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use std::collections::HashSet;
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

/// Where the op ids of publishes start, clear of the measured ops' 1, 2, ...
const PUBLISH_OP_BASE: u64 = 10_000_000;

/// The most latency the network is built and the values stored under. The
/// requested `--latency-ms` only applies once measuring starts, so a latency
/// whose round trip outlasts the RPC retry budget still gets a network to
/// measure, instead of one whose joins all timed out.
const SETUP_LATENCY: Duration = Duration::from_millis(5);

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
    rpc_events: RpcEvents,
    workload: Workload,
    /// Package workload: how many packages, versions of each, bytes per blob.
    packages: usize,
    versions: usize,
    package_bytes: usize,
    /// Publish under the measure phase's loss and latency instead of a clean
    /// wire.
    lossy_publish: bool,
    out: PathBuf,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    /// Store blobs, then measure value and node lookups.
    Lookups,
    /// Publish packages, then measure installs.
    Packages,
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
            rpc_events: RpcEvents::Ops,
            workload: Workload::Lookups,
            packages: 20,
            versions: 3,
            package_bytes: 1024,
            lossy_publish: false,
            out: PathBuf::from("experiment-out"),
        }
    }
}

const USAGE: &str = "\
usage: experiment [--nodes N] [--seed S] [--loss P] [--latency-ms MS] [--alpha A]
                  [--lookups M] [--values V] [--churn PER_SEC] [--concurrency C]
                  [--threads T] [--liveness-secs S] [--republish-secs S] [--settle-secs S]
                  [--refresh R] [--duration-secs S] [--rpc-events off|ops|all] [--out DIR]
                  [--workload lookups|packages] [--packages P] [--versions V]
                  [--package-bytes B] [--lossy-publish true|false]";

fn parse_args() -> Result<Config, Box<dyn Error>> {
    let mut config = Config::default();
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        if flag == "--no-rpc-events" {
            config.rpc_events = RpcEvents::Off;
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
            "--rpc-events" => {
                config.rpc_events = match value.as_str() {
                    "off" => RpcEvents::Off,
                    "ops" => RpcEvents::Ops,
                    "all" => RpcEvents::All,
                    _ => return Err(format!("--rpc-events takes off, ops or all\n{USAGE}").into()),
                }
            }
            "--refresh" => config.refresh = value.parse()?,
            "--duration-secs" => config.duration = Some(Duration::from_secs_f64(value.parse()?)),
            "--out" => config.out = PathBuf::from(value),
            "--workload" => {
                config.workload = match value.as_str() {
                    "lookups" => Workload::Lookups,
                    "packages" => Workload::Packages,
                    _ => {
                        return Err(format!("--workload takes lookups or packages\n{USAGE}").into());
                    }
                }
            }
            "--packages" => config.packages = value.parse()?,
            "--versions" => config.versions = value.parse()?,
            "--package-bytes" => config.package_bytes = value.parse()?,
            "--lossy-publish" => config.lossy_publish = value.parse()?,
            _ => return Err(format!("unknown flag {flag}\n{USAGE}").into()),
        }
    }
    if config.nodes < 2 {
        return Err("--nodes must be at least 2".into());
    }
    if config.workload == Workload::Packages && (config.packages == 0 || config.versions == 0) {
        return Err("--packages and --versions must be at least 1".into());
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

    /// Have every node look up `count` random ids, beyond the bucket refresh
    /// its join already did.
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
                    .scoped(lookup::store_value(origin.rpc(), value.into()))
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

    /// Publish every package's versions in order, packages `concurrency` at a
    /// time, each from one random node. Returns what was published, so an
    /// install can be checked against it; a version whose publish failed is
    /// left out.
    async fn publish(&self, rng: &mut StdRng) -> Vec<Package> {
        let work: Vec<(usize, Arc<FakeNode>, Vec<Vec<u8>>)> = (0..self.config.packages)
            .map(|i| {
                let blobs = (0..self.config.versions)
                    .map(|_| {
                        (0..self.config.package_bytes)
                            .map(|_| rng.random())
                            .collect()
                    })
                    .collect();
                (i, self.nth(rng.random()), blobs)
            })
            .collect();
        let published = Arc::new(Mutex::new(Vec::new()));
        let publish_start = Instant::now();

        run_concurrently(self.config.concurrency, work, |(i, origin, blobs)| {
            let published = Arc::clone(&published);
            async move {
                let owner = &FAKE_KEYS[i % FAKE_KEYS.len()];
                let signer = owner.signing_key().expect("fake keys are valid");
                let mut package = Package {
                    domain: owner.domain,
                    name: format!("pkg{i}"),
                    versions: Vec::new(),
                };
                for (j, blob) in blobs.into_iter().enumerate() {
                    let version = Version {
                        major: j as u32 + 1,
                        minor: 0,
                        patch: 0,
                    };
                    let op = PUBLISH_OP_BASE + (i * self.config.versions + j) as u64;
                    let t_ms = publish_start.elapsed().as_millis();
                    let started = Instant::now();
                    let result = instrumentation::with_op(
                        op,
                        origin.scoped(registry::publish_package(
                            origin.rpc(),
                            origin.dns().as_ref(),
                            package.domain,
                            &package.name,
                            version,
                            blob.clone(),
                            &signer,
                            None,
                            false,
                        )),
                    )
                    .await;
                    let took = started.elapsed();
                    let acks = result.as_ref().map_or((0, 0, 0), |receipt| {
                        (receipt.blob_accepted, receipt.record_accepted, receipt.latest_accepted)
                    });
                    metric!(
                        "event=publish op={op} package={}:{} version={version} index={j} t_ms={t_ms} origin={} success={} error={} blob_acks={} record_acks={} latest_acks={} duration_us={}",
                        package.domain,
                        package.name,
                        origin.address(),
                        result.is_ok(),
                        result.as_ref().err().map_or("none".to_string(), error_kind),
                        acks.0,
                        acks.1,
                        acks.2,
                        took.as_micros(),
                    );
                    if result.is_ok() {
                        package.versions.push((version, blob));
                    }
                }
                published.lock().unwrap().push((i, package));
            }
        })
        .await;

        let mut published = std::mem::take(&mut *published.lock().unwrap());
        published.sort_by_key(|(i, _)| *i);
        let packages: Vec<Package> = published.into_iter().map(|(_, package)| package).collect();

        let nodes = self.snapshot();
        for package in &packages {
            let key = latest_key(package.domain, &package.name);
            let closest = true_closest(&nodes, key);
            metric!(
                "event=package_stored package={}:{} versions={} of={} latest_holders={} closest_holders={}",
                package.domain,
                package.name,
                package.versions.len(),
                self.config.versions,
                nodes.iter().filter(|node| node.holds(&key)).count(),
                closest.iter().filter(|node| node.holds(&key)).count(),
            );
        }
        packages
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

    async fn measure(
        self: &Arc<Self>,
        rng: &mut StdRng,
        values: &[(Key, Vec<u8>)],
        packages: &[Package],
    ) {
        // Drawn before anything runs: the same ops in the same order for a
        // seed, whatever churn and scheduling do to the timing.
        let ops: Vec<Op> = match self.config.workload {
            Workload::Lookups => (0..self.config.lookups * 2)
                .map(|i| Op {
                    id: i as u64 + 1,
                    kind: if i % 2 == 0 {
                        OpKind::Value
                    } else {
                        OpKind::Node
                    },
                    origin: rng.random(),
                    target: rng.random(),
                    extra: 0,
                })
                .collect(),
            Workload::Packages => (0..self.config.lookups)
                .map(|i| Op {
                    id: i as u64 + 1,
                    kind: OpKind::Install,
                    origin: rng.random(),
                    target: rng.random(),
                    extra: rng.random(),
                })
                .collect(),
        };

        let done = Arc::new(AtomicUsize::new(0));
        let churn = tokio::spawn(Arc::clone(self).churn(Arc::clone(&done)));

        let values = Arc::new(values.to_vec());
        let packages = Arc::new(packages.to_vec());
        let started = Instant::now();
        match self.config.duration {
            None => {
                let this = Arc::clone(self);
                run_concurrently(self.config.concurrency, ops, move |op| {
                    let this = Arc::clone(&this);
                    let values = Arc::clone(&values);
                    let packages = Arc::clone(&packages);
                    async move { this.run_op(op, &values, &packages, started).await }
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
                    let packages = Arc::clone(&packages);
                    running.push(tokio::spawn(async move {
                        this.run_op(op, &values, &packages, started).await
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
    async fn run_op(
        &self,
        op: Op,
        values: &[(Key, Vec<u8>)],
        packages: &[Package],
        measure_start: Instant,
    ) {
        let t_ms = measure_start.elapsed().as_millis();
        let origin = self.nth(op.origin);
        match op.kind {
            OpKind::Install => {
                let package = &packages[(op.target % packages.len() as u64) as usize];
                let Some(&(newest, _)) = package.versions.last() else {
                    metric!(
                        "event=op op={} kind=install t_ms={} package={}:{} skipped=unpublished",
                        op.id,
                        t_ms,
                        package.domain,
                        package.name
                    );
                    return;
                };
                // Half the installs ask for the latest version, half for a
                // random published one (which may be the latest too).
                let requested = (op.extra % 2 == 1).then(|| {
                    package.versions[((op.extra / 2) % package.versions.len() as u64) as usize].0
                });
                let expected = requested.unwrap_or(newest);
                let key = latest_key(package.domain, &package.name);
                let holders = self
                    .snapshot()
                    .iter()
                    .filter(|node| node.holds(&key))
                    .count();
                let started = Instant::now();
                let result = instrumentation::with_op(
                    op.id,
                    origin.scoped(registry::install_package(
                        origin.rpc(),
                        origin.dns().as_ref(),
                        package.domain,
                        &package.name,
                        requested,
                    )),
                )
                .await;
                let took = started.elapsed();
                let (got, correct) = match &result {
                    Ok(installed) => {
                        let right_bytes = package
                            .versions
                            .iter()
                            .any(|(v, blob)| *v == installed.version && *blob == installed.blob);
                        (
                            Some(installed.version),
                            right_bytes && installed.version == expected,
                        )
                    }
                    Err(_) => (None, false),
                };
                metric!(
                    "event=op op={} kind=install t_ms={} origin={} package={}:{} chain={} requested={} expected={} got={} success={} correct={} stale={} error={} latest_holders={} duration_us={}",
                    op.id,
                    t_ms,
                    origin.address(),
                    package.domain,
                    package.name,
                    package.versions.len(),
                    requested.map_or("latest".to_string(), |v| v.to_string()),
                    expected,
                    got.map_or("none".to_string(), |v| v.to_string()),
                    result.is_ok(),
                    correct,
                    requested.is_none() && got.is_some_and(|v| v < newest),
                    result.as_ref().err().map_or("none".to_string(), error_kind),
                    holders,
                    took.as_micros(),
                );
            }
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
    Install,
}

/// A package the publish phase put in the network, with every version whose
/// publish succeeded, oldest first.
#[derive(Clone)]
struct Package {
    domain: &'static str,
    name: String,
    versions: Vec<(Version, Vec<u8>)>,
}

/// An error's variant names, outermost first and dot-joined, without the
/// payload: `InvalidHistory(MissingRecord([1, 2, ..]))` is
/// `InvalidHistory.MissingRecord`, one token for a `key=value` line.
fn error_kind(error: impl std::fmt::Debug) -> String {
    format!("{error:?}")
        .split('(')
        .map(|part| part.trim_end_matches(')'))
        .take_while(|part| {
            !part.is_empty() && part.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        })
        .collect::<Vec<_>>()
        .join(".")
}

#[derive(Clone, Copy)]
struct Op {
    /// Logged as `op=` on the op's own line and on every lookup and RPC it
    /// makes, which is how the analysis tells them from background work.
    id: u64,
    kind: OpKind,
    origin: u64,
    target: u64,
    /// Install ops: which version to ask for.
    extra: u64,
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
        latency: config.latency.min(SETUP_LATENCY),
        loss: 0.0,
        seed: Some(config.seed.wrapping_add(0x5EED_0003)),
    });

    metric!(
        "event=run_config nodes={} seed={} loss={} latency_ms={} alpha={} k={} lookups={} values={} churn={} concurrency={} threads={} liveness_s={} republish_s={} refresh={} duration_s={} setup_latency_ms={} resend_ms={} max_attempts={} stream_deadline_ms={} workload={:?} packages={} versions={} package_bytes={} lossy_publish={}",
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
        config.latency.min(SETUP_LATENCY).as_secs_f64() * 1e3,
        RESEND_INTERVAL.as_millis(),
        MAX_ATTEMPTS,
        STREAM_DEADLINE.as_millis(),
        config.workload,
        config.packages,
        config.versions,
        config.package_bytes,
        config.lossy_publish,
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

    // Distinct ids: `contacts_iter` yields a sibling twice, once from the
    // sibling list and once from the bucket that also holds it.
    let tables: Vec<usize> = experiment
        .snapshot()
        .iter()
        .map(|node| {
            let ids: HashSet<NodeId> = node
                .rpc()
                .close_nodes()
                .contacts_iter()
                .map(|contact| contact.id)
                .collect();
            ids.len()
        })
        .collect();
    metric!(
        "event=routing_tables distinct=true mean={:.1} min={} max={}",
        tables.iter().sum::<usize>() as f64 / tables.len() as f64,
        tables.iter().min().unwrap(),
        tables.iter().max().unwrap(),
    );

    let (values, packages) = match config.workload {
        Workload::Lookups => {
            experiment.phase("store");
            (experiment.store(&mut work_rng).await, Vec::new())
        }
        Workload::Packages => {
            if config.lossy_publish {
                experiment.network.set_loss(config.loss);
                experiment.network.set_latency(config.latency);
            }
            // Publishes are measured too, under op ids of their own.
            instrumentation::set_rpc_events(config.rpc_events);
            experiment.phase("publish");
            let packages = experiment.publish(&mut work_rng).await;
            instrumentation::set_rpc_events(RpcEvents::Off);
            (Vec::new(), packages)
        }
    };

    experiment.network.set_loss(config.loss);
    experiment.network.set_latency(config.latency);
    instrumentation::set_rpc_events(config.rpc_events);
    experiment.phase("measure");
    experiment.measure(&mut work_rng, &values, &packages).await;
    instrumentation::set_rpc_events(RpcEvents::Off);
    experiment.phase("done");
}
