# Kademlia packet manager

A Kademlia node for D7024E: PING and FIND_NODE over UDP, STORE and FIND_VALUE
over TCP, an S/Kademlia-style routing table, and a small interactive CLI.

## Running a node

```sh
cargo run -- IP:PORT [BOOTSTRAP_IP:PORT]
```

The first address is the one to bind; the node's id is the SHA-256 of it
(`hashing::node_id_from_address`). Without a second address the node starts a
new network. With one, it pings that seed to learn its id and then looks itself
up through it to fill its routing table (`src/bootstrap.rs`). If bootstrap fails
the process exits.

```sh
cargo run -- 127.0.0.1:8000                   # first node
cargo run -- 127.0.0.1:8001 127.0.0.1:8000    # joins through the first
```

Once it is up, the node starts its [periodic tasks](#periodic-maintenance) and
then hands the terminal to the CLI (`src/cli.rs`):

| Command | Does |
|---|---|
| `ping IP:PORT` | Ping a node and, if it answers, add it as a contact |
| `put FILENAME` | Store the file's contents on the `K` nodes closest to its key; prints the key |
| `get KEY [FILENAME]` | Look a key (64 hex characters) up; prints the value, or writes it to `FILENAME` |
| `show rt` | Show the routing table: siblings, then each non-empty bucket |
| `show ds` | Show the values this node stores |
| `help` / `exit` | List the commands / quit |

## Logging and metrics

`logging::setup` (`src/logging.rs`) sends log records to two places, split by
target:

- **Console (stdout)** — everything at `info` and above *except* the `metrics`
  target, as `[LEVEL target] message`.
- **`metrics.log`** — only the `metrics` target, one bare `key=value` line per
  event. The file is opened for appending in the working directory, so runs
  accumulate until you delete it; it is in `.gitignore`.

The level is fixed at `info` in code: `RUST_LOG` is not read, and the
`trace!`/`debug!` calls throughout the code (retries, removed contacts,
liveness rounds) are dropped. To see them, lower the level in `logging::setup`.

### Metric events

`src/instrumentation.rs` is the only writer of the `metrics` target. Every
lookup gets an id from `next_lookup_id` and logs:

| Event | Fields |
|---|---|
| `lookup_start` | `lookup_id kind node target` |
| `lookup_probe` | `lookup_id kind node peer` — one per RPC the lookup sends |
| `lookup_end` (`kind=node`) | `lookup_id node probes result_count exact_match` |
| `lookup_end` (`kind=value`) | `lookup_id node probes success` |

`kind` is `node` or `value`; ids are hex.

### Analysing a run

`analyze_metrics.py` reads the `lookup_end` events of a log and prints the
number of node lookups, their mean probe count and its variance, and the value
lookups' success rate:

```sh
python3 analyze_metrics.py metrics.log
```

Passing all three of `--network-size`, `--packet-loss` and `--seed` also
appends the run as a row of `results.csv` (`--results` to choose another file).
`--summary results.csv` then aggregates the rows: mean probes per network size,
and mean success rate per packet-loss rate.

## Close nodes

Routing lives behind one small trait in `src/close_nodes.rs`:

```rust
pub trait CloseNodes {
    fn close_nodes(&self, id: NodeId) -> Vec<Contact>;
    fn maybe_add_contact(&self, contact: Contact);
    fn remove_contact(&self, contact: &Contact);
    fn contacts_iter(&self) -> impl Iterator<Item = Contact>;
}
```

`close_nodes` answers "who are the `K` nodes nearest this id?" and
`maybe_add_contact` offers a contact the table may keep or ignore.
`remove_contact` drops an entry, but only an exact match — same id *and*
address — so a node that has since moved to a new address is not dropped with
its old one. All of them take `&self`: implementations do their own interior
locking, so the table can be shared across tasks without a mutex around the
whole thing.

`contacts_iter` walks the whole table with deliberately weak promises: it may
repeat a contact, may miss one added after it started, and may hand out one
that has since been removed. What that buys is laziness — `StaticBucket` locks
and copies one bucket only when the iterator reaches it, and holds no lock
between items, so a caller can make network calls while iterating.

Because the trait is this narrow, implementations compose: each one can wrap
another and layer behaviour on top.

### Implementations

- **`DumbBucket`** — a single `Vec<Contact>` that keeps everything and sorts
  the whole thing on every query. No eviction, no structure; it exists as the
  obvious baseline to check the others against.
- **`StaticBucket<BUCKET_SIZE, SEARCH_SHIFT, MAX_BUCKETS>`** — the real routing
  table. Unlike classic Kademlia it never splits buckets: there is a fixed
  array of them, indexed by the number of leading bits a contact shares with
  our own id, so finding the right bucket is an XOR and a leading-zero count
  into a contiguous array. A query starts in the home bucket and widens
  outwards until it has `K` candidates. Full buckets keep what they have —
  long-lived contacts are the ones worth holding.
- **`SiblingList<C, SIBLINGS>`** — the S/Kademlia sibling set, layered over any
  other `CloseNodes`. It pins the `SIBLINGS` nodes closest to us where no
  bucket eviction can reach them, which is what replication needs: the set of
  nodes nearest a key has to be exactly right, not merely pointing the right
  way. It is an overlay, not a partition — every contact also goes to the
  fallback, and queries merge both and let XOR distance decide.
- **`CachedCloseNodes<C>`** — memoises answers per target for a TTL. A
  converging lookup asks about the same target repeatedly while the table
  underneath it barely moves, so the sort is worth caching. `recommended`
  builds it TTL-only (500 ms), so a new contact waits out the TTL, but a
  removal invalidates every cached answer at once: serving a dead contact
  costs a failed RPC every time.

`close_nodes::recommended(my_id)` stacks all three:

```
CachedCloseNodes< SiblingList< StaticBucket > >
```

## Transports

Everything above the wire talks to one trait in `src/rpc_transport.rs`:

```rust
pub trait RpcTransport {
    fn send_receive(&self, payload: Vec<u8>, address: SocketAddr)
        -> impl Future<Output = std::io::Result<Vec<u8>>> + Send;
}
```

Request ids are the transport's own business: it frames one, matches the reply
against it, and only ever hands back a response that carried it. Callers just
send bytes and get bytes.

The future is declared `Send` so code generic over the transport can still
`tokio::spawn` a call — the [liveness task](#periodic-maintenance) does. An
`async fn` in an impl satisfies it as long as it holds nothing that isn't
`Send` across an `.await`.

Every transport gives up on a silent peer by itself and says so with an
`io::Error` of kind `TimedOut`, so callers only need an outer `timeout` for a
shorter deadline.

A node runs two of them at once. `Rpc` holds a `transport` and a
`robust_transport`: PING and FIND_NODE go over the first, STORE and FIND_VALUE
over the second, because a value does not fit in a datagram. Which concrete
transports those are is the node's choice — UDP and TCP for a real node, two
fakes on one in-process network under test.

### Ids on the wire

Both shapes write the same framing: an 8-byte big-endian request id, then the
payload. A reply comes back under that same id with the top bit — `REPLY_TAG` —
set.

The tag is there because a node sends and receives everything on one socket, so
its receive loop has to tell an answer to something it asked from a question
someone is asking it. That is not inferrable from the id: every node starts its
counter in the same place, so a peer's request `7` and our own pending reply `7`
are the same number. So it is written on the wire instead of inferred. Ids are
63-bit as a result, and `request_id` / `reply_id` / `is_reply` are the only
things that touch the bit.

On a connection there is nothing to disambiguate — the only thing that can come
back is the answer to the request just written — so the echoed id is *checked*
rather than matched: a reply under another id is an `InvalidData` error.

### The two seams

Neither shape names UDP or TCP. Each is generic over one small trait, and
everything else is written once:

- **`DataRxTx`** (`data_rx_tx.rs`) — a datagram channel, reduced to
  `send_packet(payload, address)` and `receive_packet(buf)`. Neither promises
  delivery — this is UDP's contract, not a stream's.
- **`StreamListener`** (`stream_listener.rs`) — `accept()`, handing back a
  connection and the address that dialled it. Only accepting needs naming: an
  open connection is already `AsyncRead + AsyncWrite`, and dialling is the
  transport's own `send_receive`.

Both return `impl Future + Send` rather than being written `async fn`, so the
loops built on them can be `tokio::spawn`ed.

### `RetryTransport<T: DataRxTx>`

The datagram half. It frames the request id onto every payload and spawns the
node's one receive loop, which splits each arriving datagram on `REPLY_TAG`: a
tagged one is an answer and goes to whoever is awaiting it through `Pending`,
an untagged one is a question and goes down the channel given to
`with_requests` for [`handle_rpc`](#serving-rpcs) to answer. Built with plain
`new` there is no channel and inbound requests are dropped, which is all a
client-only node needs.

A request that goes quiet is resent every 200 ms, up to 5 attempts, after which
`send_receive` returns an `io::Error` of kind `TimedOut` — so callers only need
an outer `timeout` for a deadline shorter than that.

#### Pending requests

`src/pending.rs` is the bookkeeping between sending an RPC and getting its
answer back, and `RetryTransport` is its only user. There is one receive loop
but many tasks waiting on replies, so something has to route each response to
the right waiter.

`Pending` is a sharded map from request id to a oneshot sender. `send_receive`
calls `next_id()`, `register(id)` to claim a slot, and awaits the returned
`PendingResponse`; the receive loop calls `deliver(id, payload)` and the
matching future wakes up. `deliver` returns `false` when nobody was waiting —
an unsolicited, duplicate, or already-abandoned response — so garbage on the
wire is cheap to drop.

Dropping a `PendingResponse` deregisters its id, which means a timed-out or
cancelled request cleans up after itself rather than leaking a slot. The resend
loop is careful never to drop it between attempts, or the eventual reply would
arrive for a slot nobody holds.

### `dial_and_send`

The connection half, in `stream_framing.rs`, and the client end of one
connection per request. `dial_and_send(connect, payload, deadline)` awaits
`connect` for a connection and runs `stream_send_receive` over it: write the id
and the payload, half-close the write side so the peer sees EOF, then read the
echoed id and the reply until the peer closes its own side. There is no length
framing — the half-close and the final EOF are what delimit the two messages —
and no retry loop, because TCP already does that underneath.

The whole call, connect included, runs under one deadline, `STREAM_DEADLINE`
(2 s), and running out is a `TimedOut` error like the datagram side's. Without
it, a peer that vanished without closing anything would leave the call waiting
on the OS — about two minutes for a connect on Linux, far longer on an
established connection.

Its id is a random `u64` rather than a counter, run through `request_id` so the
tag bit is clear: a random draw would set it half the time.

It takes a future of any `AsyncRead + AsyncWrite`, which is what makes it shared
code the same way `RetryTransport` is: only the thing dialled differs.

None of the bookkeeping above applies here. A connection carries exactly one
request, so it *is* the correlation — there is no map from id to waiter because
there is only ever one waiter, holding the only thing the answer can arrive on.

### The real node's two: `UdpTransport` and `TcpTransport`

`UdpTransport` is `RetryTransport<UdpSocket>` — a two-line `DataRxTx` impl over
`tokio::net::UdpSocket` and nothing else. `TcpTransport::send_receive` hands
`TcpStream::connect` to `dial_and_send`; `TcpListener` gets the
matching two-line `StreamListener` impl so the serve loop can accept on it.

### The fake wire: `Network` and `Endpoint`

`Network` is a wire made of channels, and an `Endpoint` bound to it stands in
for *both* sockets a node answers on: it implements `DataRxTx` and
`StreamListener`, so a datagram sent to its address is delivered to it and a
connection opened to that address is queued for it to accept. One endpoint
carries both because a real node binds its `UdpSocket` and its `TcpListener` to
the same `SocketAddr`.

On top of it sit the fake node's two transports: `NetworkedDebugTransport` is
`RetryTransport<Endpoint>`, and `NetworkedStreamTransport` dials
`Network::connect` into the same `dial_and_send`. Those two impls are the
whole seam — `UdpSocket` and `Endpoint`, `TcpListener` and `Endpoint`, are
interchangeable as far as the transports are concerned, so a test against the
fake network exercises the code a real node runs.

Addresses are real `SocketAddr`s but nothing is bound in the OS and nothing
leaves the process, so tests get UDP semantics — unordered, droppable, silent
when nobody is home — and TCP's, without a port or a real timeout.
`Network::with_config` adds latency and a loss rate to make the fake wire
misbehave on purpose; both apply to datagrams only, since a connection is what a
node reaches for when it will not accept loss.

`connect` mints a *fresh, unbound* address for the dialling end rather than
handing over the caller's, because real TCP shows the accepting side an
ephemeral port. That keeps the fake honest about the rule the next section is
about: a change that started learning a connection's sender fails here the same
way it would fail on a real network, instead of passing because the fake was
generous.

## Serving RPCs

`src/handle_rpc.rs` is the server half — `Rpc` encodes a request and waits for
the answer, this answers other nodes' requests. A request's payload is the
sender's `NodeId`, then a method tag (`PING`, `STORE`, `FIND_NODE`,
`FIND_VALUE`), then the body; `parse_framed` strips the first two and `dispatch`
routes the rest to `ping`, `store`, `find_node` or `find_value`. A handler that
returns `None` — malformed body, unknown method, a request we decline — is
simply not answered, and the sender times out. Handlers take the whole
`Context` (our id, the routing table and the value store) rather than the parts
they use today.

`dispatch` does not read a socket itself; requests arrive from the loop that
already owns the wire.

### One dispatcher, two wire shapes

Both shapes go through the same `dispatch`, and differ on one thing: whether a
request's `from` is somewhere its sender can be reached, and so worth learning
as a `Contact`.

- A datagram transport sends and receives on one bound socket, so a request's
  `from` *is* the address its sender listens on. `dispatch` hands it to
  `maybe_add_contact` before it even looks at the method — which is how a
  routing table fills up from ordinary traffic rather than from FIND_NODE
  replies alone.
- A connection's `from` is the ephemeral local port the OS picked for that one
  `connect`, not the port its sender accepts on. Reply down it, then forget it.

That is `Request::from_is_reachable`, and it is fixed by *which constructor built
the request* — `Request::from_datagram` or `Request::from_connection`, one per
wire shape — rather than being a parameter, so no call site can pass the wrong
one. The names are about the wire's *shape*, not UDP and TCP, because that is
where the rule comes from.

`serve` runs both: it spawns `serve_datagrams` over the transport's request
channel and its socket, and runs `serve_streams` accepting off a
`StreamListener`, each reply going back the way its request came. Both sides are
generic over their channel rather than fixed to `UdpSocket`/`TcpListener`, so a
node can be served entirely in-process against the fake network — the same code
path, not a parallel one.

Every request is handled in its own spawned task, because a handler can block on
work of its own (a STORE that hits disk, a FIND_VALUE that has to ask someone
else) and the sender's timeout is already running.

## Nodes

`src/node.rs` is where all of that gets wired together, so nothing else has to
do it by hand. A `Node` owns its id and address, a `Context`, the `Rpc` it
issues calls through (in an `Arc`, so background tasks can share it), and the
serve task answering calls made to it.

There are two, differing only in what they bind. Both derive the id from the
bound address:

- `RealNode::bind(address)` binds a `UdpSocket` and a `TcpListener` to the
  same address and pairs `UdpTransport` with `TcpTransport`.
- `FakeNode::new(&network)` binds one `Endpoint` on a `Network` and passes
  it to `serve` as both the socket and the listener, pairing
  `NetworkedDebugTransport` with `NetworkedStreamTransport`.

Same `serve`, same handlers, same `Rpc` — which is what makes a test over the
fake network worth anything.

## Dropping dead contacts

Full buckets keep what they have, and the sibling list evicts only by distance,
so a contact that dies would otherwise hold its place forever — in a bucket,
blocking live nodes from it; among the siblings, as a replication target. Two
things remove them, and both go through one method, `Rpc::forget`.

### On a failed call

`Rpc::call` takes the `Contact` it is calling when there is one — FIND_NODE,
FIND_VALUE and STORE take a `&Contact` rather than an address for this reason.
When the call fails with `TimedOut`, `ConnectionRefused` or `ConnectionReset`,
the peer is forgotten. Other errors — a send error on our own socket, a reply
under the wrong id — say nothing about whether the peer is gone, so they don't
count. PING takes a bare address, because bootstrap and the CLI ping one to
*learn* the id behind it, so a failed ping never removes anything.

A timeout is already five unanswered sends over a second, and a contact removed
by mistake comes back on its own: every datagram request a node receives adds
its sender (see [one dispatcher, two wire shapes](#one-dispatcher-two-wire-shapes)).

### The floor

What a failed call can't tell apart is a dead peer from our own network being
down — and in the second case *every* call fails. So `forget` removes nothing
while the table holds `K` or fewer distinct contacts, leaving enough to rejoin
through once the network is back.

## Periodic maintenance

Failed calls only catch the contacts something happens to call.
`src/maintenance.rs` catches the rest, and `Node::periodic_task`, which `main`
starts once bootstrap is done, repeats it in a spawned task.

- **Liveness** — `check_liveness(&rpc)` is one round: walk `contacts_iter()`,
  ping each contact, and forget those that don't answer or answer with another
  id (that address now belongs to a different node). The iterator is walked
  lazily while pinging, so a contact learned mid-round may be checked too, and
  one may be pinged twice. Removal goes through `Rpc::forget`, so the
  [floor](#the-floor) holds here too.
- **Republishing** — not implemented yet. `REPUBLISH_INTERVAL` is one hour, the
  paper's `tReplicate`, and `periodic_task` takes it already.

Rounds are spaced by `sleep(jittered(LIVENESS_CHECK_INTERVAL))`, with the
interval 60 s on average:

- **`sleep` after each round, not an `interval`.** A round spends about a
  second on every dead contact. If a round overran an `interval`, the next one
  would start straight away — exactly when many peers are dead.
- **Jitter.** `jittered` scales the period by a fresh random factor in
  `[0.5, 1.5)` every round, so nodes started together don't ping in lockstep,
  and don't drift back into it either.

Each round is a plain async function over an `Rpc`, with no timer and no spawn,
so the tests in `maintenance.rs` run one round on the fake network and check the
table afterwards.

## Test coverage

CI fails the build if line coverage drops below 80%. To check coverage locally, install [`cargo-llvm-cov`](https://github.com/taiki-e/cargo-llvm-cov) once:

```sh
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov
```

Then run:

```sh
cargo llvm-cov --all-features --workspace
```

For an HTML report you can browse in a browser:

```sh
cargo llvm-cov --all-features --workspace --html
open target/llvm-cov/html/index.html
```
