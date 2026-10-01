# Kademlia packet manager

A Kademlia node written in Rust for the course D7024E. Nodes find each other
with PING and FIND_NODE over UDP, and store and fetch values with STORE and
FIND_VALUE over TCP. Each node has a small interactive CLI.

## Running a node

You need a stable Rust toolchain that supports edition 2024 (Rust 1.85 or
newer).

```sh
cargo run -- IP:PORT [BOOTSTRAP_IP:PORT]
```

The node binds UDP and TCP on the same `IP:PORT`. Its id is the SHA-256 hash
of that address, so the same address always gives the same id.

With one address the node starts a new network. With two, it joins an existing
network through the second address (the seed): it pings the seed, then looks
up its own id to fill its routing table. If the seed does not answer, or has
the node's own id, the node prints `bootstrap failed: ...` and exits with
code 1.

```sh
cargo run -- 127.0.0.1:8000                   # first node
cargo run -- 127.0.0.1:8001 127.0.0.1:8000    # joins through the first
```

## CLI commands

| Command | What it does |
|---|---|
| `ping IP:PORT` | Pings a node and adds it to the routing table if it answers. |
| `put FILENAME` | Stores the file on the 10 nodes closest to its key and prints the key. |
| `get KEY [FILENAME]` | Fetches a value by its 64-character hex key. Prints it, or saves it to `FILENAME`. |
| `show rt` | Prints the routing table: the sibling list, then every non-empty bucket. |
| `show ds` | Prints the values stored on this node. |
| `help` | Lists the commands. |
| `exit` | Quits. Ctrl-D also works. |

A value's key is the SHA-256 hash of its contents. A node rejects a STORE
whose key does not match its value.

## Running 50 nodes in Docker

```sh
docker compose up -d --build
docker attach kademlia-packet-manager-d7024e-node-20   # detach with Ctrl-P Ctrl-Q
docker compose down
```

`compose.yaml` starts one `seed` node at `172.28.0.10:4000` and 49 `node`
replicas. Each replica waits 0 to 5 seconds, then bootstraps through the seed.
Replica numbers do not match IP addresses, because Docker assigns addresses in
start order.

[docs/docker-network.md](docs/docker-network.md) walks through a full manual
test: store a value, fetch it from another node, stop the node that answered,
and fetch it again.

## Tests and CI

```sh
cargo test
```

The tests run inside one process and need no Docker. Most of them build whole
networks on a simulated network that can add latency and packet loss; a few
use real UDP and TCP sockets on localhost.

CI runs on every push and pull request to `main`. It checks formatting
(`cargo fmt --check`), runs clippy and the tests, and fails if line coverage
drops below 80%. To check coverage locally:

```sh
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov
cargo llvm-cov --all-features --workspace --open
```

## Logs and metrics

Log messages at level `info` and above go to stdout. The level is set in
`src/logging.rs`, and the `RUST_LOG` environment variable is ignored.

Every lookup also writes `key=value` lines to `metrics.log` in the current
directory. The file is appended to and never truncated, so delete it between
runs. `analyze_metrics.py` summarises it:

```sh
python3 analyze_metrics.py metrics.log
python3 analyze_metrics.py metrics.log --network-size 50 --packet-loss 0.1 --seed 1
python3 analyze_metrics.py --summary results.csv
```

The first command prints the number of node lookups, the average number of
RPCs they sent and its variance, and the success rate of value lookups. The
second also appends the run to `results.csv` (`--results` picks another file).
The third averages all saved runs by network size and by packet loss. The
event format is in [docs/architecture.md](docs/architecture.md#metrics).

## Project layout

| Path | Contents |
|---|---|
| `src/main.rs` | Argument parsing, startup and bootstrap |
| `src/cli.rs` | The interactive CLI |
| `src/node.rs` | `RealNode` (real sockets) and `FakeNode` (simulated network) |
| `src/bootstrap.rs` | Joining a network through a seed |
| `src/lookup.rs` | Node lookup, value lookup and `put` |
| `src/rpc.rs` | Sending the four RPCs and removing dead contacts |
| `src/handle_rpc.rs`, `src/handle_rpc/` | Answering RPCs from other nodes |
| `src/close_nodes.rs`, `src/close_nodes/` | The routing table |
| `src/rpc_transport.rs`, `src/rpc_transport/` | UDP, TCP and simulated transports |
| `src/pending.rs` | Matching UDP replies to the requests waiting for them |
| `src/maintenance.rs` | Periodic liveness checks |
| `src/hashing.rs` | Node ids and value keys |
| `src/logging.rs`, `src/instrumentation.rs` | Logging and lookup metrics |
| `analyze_metrics.py` | Metrics analysis |
| `Dockerfile`, `compose.yaml` | The 50-node Docker network |

[docs/architecture.md](docs/architecture.md) explains how these parts fit
together.

## Limitations

- Values are kept in memory only. They are lost when the node stops, and they
  never expire.
- Nodes do not republish values, so a value is gone once every node that
  stored it has stopped.
- `put` prints `stored ...` even when no node accepted the value.
