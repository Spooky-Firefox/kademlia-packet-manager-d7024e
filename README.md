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

### `DataRxTx`

A datagram channel, reduced to two methods: `send_packet(payload, address)` and
`receive_packet(buf)`. Neither promises delivery — this is UDP's contract, not
a stream's. Both return `impl Future + Send` rather than being written
`async fn`, so the receive loop built on top can be `tokio::spawn`ed.

### `RetryTransport<T: DataRxTx>`

The real work. It prefixes an 8-byte big-endian request id to every payload and
spawns a receive loop that reads the id off each arriving datagram and hands
the rest to whoever is awaiting it through [`Pending`](#pending-requests).
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
