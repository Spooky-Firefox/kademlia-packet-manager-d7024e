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

### `stream_send_receive`

The connection half, in `stream_framing.rs`, and the client end of one
connection per request: write the id and the payload, half-close the write side
so the peer sees EOF, then read the echoed id and the reply until the peer
closes its own side. There is no length framing — the half-close and the final
EOF are what delimit the two messages — and no retry loop, because TCP already
does that underneath.

Its id is a random `u64` rather than a counter, run through `request_id` so the
tag bit is clear: a random draw would set it half the time.

It takes any `AsyncRead + AsyncWrite`, which is what makes it shared code the
same way `RetryTransport` is: only the thing dialled differs.

None of the bookkeeping above applies here. A connection carries exactly one
request, so it *is* the correlation — there is no map from id to waiter because
there is only ever one waiter, holding the only thing the answer can arrive on.

### The real node's two: `UdpTransport` and `TcpTransport`

`UdpTransport` is `RetryTransport<UdpSocket>` — a two-line `DataRxTx` impl over
`tokio::net::UdpSocket` and nothing else. `TcpTransport::send_receive` opens a
fresh `TcpStream` and hands it to `stream_send_receive`; `TcpListener` gets the
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
`Network::connect` into the same `stream_send_receive`. Those two impls are the
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
issues calls through, and the serve task answering calls made to it.

There are two, differing only in what they bind:

- `RealNode::bind(id, address)` binds a `UdpSocket` and a `TcpListener` to the
  same address and pairs `UdpTransport` with `TcpTransport`.
- `FakeNode::new(id, &network)` binds one `Endpoint` on a `Network` and passes
  it to `serve` as both the socket and the listener, pairing
  `NetworkedDebugTransport` with `NetworkedStreamTransport`.

Same `serve`, same handlers, same `Rpc` — which is what makes a test over the
fake network worth anything.

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
