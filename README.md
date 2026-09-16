testing udp

```sh
echo "this is a test" | nc -u localhost port
```

## Close nodes

Routing lives behind one small trait in `src/close_nodes.rs`:

```rust
pub trait CloseNodes {
    fn close_nodes(&self, id: NodeId) -> Vec<Contact>;
    fn maybe_add_contact(&self, contact: Contact);
}
```

`close_nodes` answers "who are the `K` nodes nearest this id?" and
`maybe_add_contact` offers a contact the table may keep or ignore. Both take
`&self` — implementations do their own interior locking, so the table can be
shared across tasks without a mutex around the whole thing.

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
  underneath it barely moves, so the sort is worth caching.

`close_nodes::recommended(my_id)` stacks all three:

```
CachedCloseNodes< SiblingList< StaticBucket > >
```

## Transports

Everything above the wire talks to one trait in `src/rpc_transport.rs`:

```rust
pub trait RpcTransport {
    async fn send_receive(&self, payload: Vec<u8>, address: SocketAddr)
        -> std::io::Result<Vec<u8>>;
}
```

Request ids are the transport's own business: it frames one, matches the reply
against it, and only ever hands back a response that carried it. Callers just
send bytes and get bytes.

A node sends and receives everything on one socket, so its receive loop has to
tell an answer to something it asked from a question someone is asking it.
That is not inferrable from the id — every node starts its counter in the same
place, so a peer's request `7` and our own pending reply `7` are the same
number. So it is written on the wire: the top bit of the id is `REPLY_TAG`, set
by whoever frames a reply and clear on a request. Ids are 63-bit as a result,
and `request_id` / `reply_id` / `is_reply` are the only things that touch the
bit.

### `DataRxTx`

A datagram channel, reduced to two methods: `send_packet(payload, address)` and
`receive_packet(buf)`. Neither promises delivery — this is UDP's contract, not
a stream's. Both return `impl Future + Send` rather than being written
`async fn`, so the receive loop built on top can be `tokio::spawn`ed.

### `RetryTransport<T: DataRxTx>`

The real work. It prefixes an 8-byte big-endian request id to every payload and
spawns the node's one receive loop, which splits each arriving datagram on
`REPLY_TAG`: a tagged one is an answer and goes to whoever is awaiting it
through [`Pending`](#pending-requests), an untagged one is a question and goes
down the channel given to `with_requests` for
[`handle_rpc`](#serving-rpcs) to answer. Built with plain `new` there is no
channel and inbound requests are dropped, which is all a client-only node
needs.

A request that goes quiet is resent every 200 ms, up to 5 attempts, after which
`send_receive` returns an `io::Error` of kind `TimedOut` — so callers only need
an outer `timeout` for a deadline shorter than that.

Because it is generic over `DataRxTx`, the framing, the matching and the resend
loop are written once and shared by everything below.

### `UdpTransport`

`RetryTransport<UdpSocket>` — a two-line `DataRxTx` impl over
`tokio::net::UdpSocket` and nothing else. This is what a real node runs.

### `NetworkedDebugTransport`

`RetryTransport<Endpoint>`, where `Endpoint` is a fake socket on an in-process
`Network` made of channels. `Endpoint` implements `DataRxTx` — that impl is the
whole seam: `UdpSocket` and `Endpoint` are interchangeable as far as
`RetryTransport` is concerned.

Addresses are real `SocketAddr`s but nothing is bound in the OS and nothing
leaves the process, so tests get UDP semantics — unordered, droppable, silent
when nobody is home — without a port or a real timeout.
`Network::with_config` can add latency and a loss rate to make the fake wire
misbehave on purpose.

Since only the channel is swapped, a test against the fake network exercises
the same transport code `main` runs over real UDP.

### `TcpTransport`

The odd one out: it does not go through `RetryTransport` at all. Each
`send_receive` opens a fresh `TcpStream`, writes the id and payload,
half-closes the write side so the peer sees EOF, then reads the echoed id and
the reply until the peer closes its own side. There is no length framing — EOF
is the only end-of-message signal either side has — and no retry loop, because
TCP already does that underneath.

Its id is a random `u64` rather than a counter, run through `request_id` so the
tag bit is clear: a random draw would set it half the time.

## Serving RPCs

`src/handle_rpc.rs` is the server half — `Rpc` encodes a request and waits for
the answer, this answers other nodes' requests. A request's payload is the
sender's `NodeId`, then a method tag (`PING`, `STORE`, `FIND_NODE`,
`FIND_VALUE`), then the body; `parse_framed` strips the first two and routes
the rest to `ping`, `store`, `find_node` or `find_value`. A handler that
returns `None` — malformed body, unknown method, a request we decline — is
simply not answered, and the sender times out. Handlers take the whole
`Context` (the routing table and the value store) rather than the parts they
use today.

Neither dispatcher reads a socket itself; requests arrive from the transport
that already owns the receive loop.

### Two dispatchers

`DatagramDispatcher` and `StreamDispatcher` parse and route identically. They
differ on one thing: whether `from` is safe to learn as a `Contact`.

- A datagram transport sends and receives on one bound socket, so a request's
  `from` *is* the address its sender listens on. `DatagramDispatcher` hands it
  to `maybe_add_contact` before it even looks at the method — which is how a
  routing table fills up from ordinary traffic rather than from FIND_NODE
  replies alone.
- A connection's `from` is the ephemeral local port the OS picked for that one
  `connect`, not the port its sender accepts on. So `StreamDispatcher` holds no
  `CloseNodes` handle at all and cannot learn a bad contact by accident.

They are named for the transport's *shape* rather than for UDP and TCP on
purpose: the rule follows from the shape, not from the protocol.

`serve` runs both — the datagram dispatcher over the transport's request
channel and its socket, and `serve_tcp` accepting connections, each reply going
back the way its request came. Every request is handled in its own spawned
task, because a handler can block on work of its own (a STORE that hits disk, a
FIND_VALUE that has to ask someone else) and the sender's timeout is already
running.

## Pending requests

`src/pending.rs` is the bookkeeping between sending an RPC and getting its
answer back. The transport has one receive loop, but many tasks are waiting on
replies, so something has to route each response to the right waiter.

`Pending` is a sharded map from request id to a oneshot sender. A sender calls
`next_id()`, `register(id)` to claim a slot, and awaits the returned
`PendingResponse`; the receive loop calls `deliver(id, payload)` and the
matching future wakes up. `deliver` returns `false` when nobody was waiting —
an unsolicited, duplicate, or already-abandoned response — so garbage on the
wire is cheap to drop.

Dropping a `PendingResponse` deregisters its id, which means a timed-out or
cancelled request cleans up after itself rather than leaking a slot.

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
