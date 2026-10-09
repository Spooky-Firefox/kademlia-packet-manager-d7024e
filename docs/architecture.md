# Architecture

How the node is put together, from the routing table up to the maintenance
loop. The [README](../README.md) covers running it.

Ids and keys are 256 bits (SHA-256). `K` is 10 (`src/close_nodes.rs`) and the
lookup concurrency `ALPHA` is 3 (`src/lookup.rs`).

## Routing table

Routing lives behind one trait in `src/close_nodes.rs`:

```rust
pub trait CloseNodes {
    fn close_nodes(&self, id: NodeId) -> Vec<Contact>;
    fn maybe_add_contact(&self, contact: Contact);
    fn remove_contact(&self, contact: &Contact);
    fn contacts_iter(&self) -> impl Iterator<Item = Contact>;
}
```

`close_nodes` returns the `K` known nodes nearest an id. `maybe_add_contact`
offers a contact, which the table may keep or ignore. `remove_contact` drops
an entry only if both the id and the address match, so a node that has moved
to a new address is not dropped along with its old one. Every method takes
`&self` and each implementation does its own locking, so the table can be
shared between tasks without a mutex around it. There is also a blanket
implementation for `Arc<T>`.

`contacts_iter` walks the whole table and promises little: it may repeat a
contact, miss one added after it started, or return one that has since been
removed. In exchange it is lazy. `StaticBucket` locks and copies one bucket
only when the iterator reaches it and holds no lock between items, so a caller
can make network calls while iterating.

### Implementations

Each implementation can wrap another and add behaviour on top. They live in
`src/close_nodes/`.

- `DumbBucket` (`dumb_bucket.rs`) is a single `Vec<Contact>` that keeps
  everything and sorts it on every query. It is the baseline the others are
  tested against.
- `StaticBucket` (`static_bucket.rs`) is the real routing table. It never
  splits buckets. It has a fixed array of 256, indexed by how many leading
  bits a contact's id shares with ours, so finding a bucket is an XOR and a
  leading-zero count. A query starts in the home bucket and widens outwards
  until it has `K` candidates. A full bucket keeps the contacts it has and
  ignores new ones.
- `SiblingList` (`sibling_list.rs`) is the S/Kademlia sibling set, layered
  over another `CloseNodes`. It keeps the `K` nodes closest to our own id
  where bucket limits cannot push them out, because replication needs the
  nodes nearest a key to be exactly right. Every contact also goes to the
  wrapped table, and queries merge both and sort by XOR distance.
- `CachedCloseNodes` (`cache.rs`) caches answers per target for 500 ms. A
  lookup asks about the same target many times while the table barely
  changes, so the sort is worth caching. A new contact shows up once the
  cached answer expires. A removal clears the whole cache at once, because
  handing out a dead contact costs a failed RPC every time.

`close_nodes::recommended(my_id)` stacks all three:

```
CachedCloseNodes< SiblingList< StaticBucket > >
```

## Transports

Everything above the wire uses one trait in `src/rpc_transport.rs`:

```rust
pub trait RpcTransport {
    fn send_receive(&self, payload: Vec<u8>, address: SocketAddr)
        -> impl Future<Output = std::io::Result<Vec<u8>>> + Send;
}
```

Callers send bytes and get bytes back. The transport handles request ids: it
adds one, matches the reply against it, and only returns a response that
carried it. The future is `Send` so that generic code can `tokio::spawn` it.

Every transport gives up on a silent peer by itself and returns an
`io::Error` of kind `TimedOut`. Callers only need their own `timeout` if they
want a shorter deadline.

`Rpc` (`src/rpc.rs`) holds two transports. PING and FIND_NODE go over
`transport`, and STORE and FIND_VALUE over `robust_transport`, because a value
does not fit in a datagram (the UDP receive buffer is 1024 bytes). A real node
uses UDP and TCP; tests use two fakes on one in-process network.

### Ids on the wire

Both kinds of transport frame a message the same way: an 8-byte big-endian
request id, then the payload. A reply carries the same id with the top bit,
`REPLY_TAG`, set.

The tag exists because a node sends and receives UDP on one socket, so its
receive loop has to tell a reply to its own request apart from a new request
from someone else. The id alone cannot do that, since every node starts its
counter at the same number. Ids are therefore 63 bits, and `request_id`,
`reply_id` and `is_reply` are the only functions that touch the tag bit.

On a TCP connection the only thing that can come back is the answer to the
request just sent, so the echoed id is checked instead of looked up. A reply
under another id is an `InvalidData` error.

### Datagrams and streams

Neither transport names UDP or TCP directly. Each is generic over one small
trait in `src/rpc_transport/`:

- `DataRxTx` (`data_rx_tx.rs`) is a datagram channel with `send_packet` and
  `receive_packet`. Like UDP, it does not promise delivery.
- `StreamListener` (`stream_listener.rs`) has one method, `accept()`, which
  returns a connection and the address that opened it. An open connection is
  already `AsyncRead + AsyncWrite`, and dialling is the transport's own
  `send_receive`, so accepting is the only thing that needs a trait.

Both return `impl Future + Send`, so the loops built on them can be spawned.

### `RetryTransport`

`RetryTransport<T: DataRxTx>` (`retry_transport.rs`) is the datagram side. It
adds the request id to every payload and runs the node's single receive loop.
The loop sorts each datagram by `REPLY_TAG`. A reply goes to the task waiting
for it, through `Pending`. A request goes down the channel passed to
`with_requests`, where the [server](#serving-rpcs) answers it. A transport
built with plain `new` has no channel and drops incoming requests, which is
enough for a node that only makes calls.

A request with no reply is resent every 200 ms, up to 5 attempts. After that
`send_receive` returns `TimedOut`.

`Pending` (`src/pending.rs`) routes replies to waiters. It is a sharded map
from request id to a oneshot sender. `send_receive` calls `next_id()` and
`register(id)` and awaits the returned `PendingResponse`. The receive loop
calls `deliver(id, payload)`, which wakes the matching future, or returns
`false` if nobody is waiting (an unsolicited, duplicate or abandoned reply).
Dropping a `PendingResponse` removes its id, so a request that times out or is
cancelled cleans up after itself. The resend loop keeps the same
`PendingResponse` across attempts, so a late reply still finds its waiter.

### `dial_and_send`

`dial_and_send(connect, payload, deadline)` (`stream_framing.rs`) is the
stream side and opens one connection per request. It writes the id and the
payload, closes its write half so the peer sees EOF, then reads the echoed id
and the reply until the peer closes its side. The two closes mark where each
message ends, so there is no length prefix. There is no retry loop either,
since TCP retransmits by itself.

The whole call, including the connect, runs under `STREAM_DEADLINE` (2 s) and
returns `TimedOut` when it runs out. Without it, a peer that disappeared
without closing anything would leave the call waiting on the OS, which takes
about two minutes for a connect on Linux.

The id is a random `u64` passed through `request_id` to clear the tag bit.
Since a connection carries exactly one request, there is no map of pending
requests.

### Real and simulated transports

A real node uses:

- `UdpTransport` (`udp_transport.rs`): `RetryTransport<UdpSocket>`, with a
  two-line `DataRxTx` impl for `tokio::net::UdpSocket`.
- `TcpTransport` (`tcp_transport.rs`): passes `TcpStream::connect` to
  `dial_and_send`. `TcpListener` gets a two-line `StreamListener` impl.

Tests use a simulated network in `networked_debug_transport.rs`. A `Network`
is a wire made of channels. An `Endpoint` bound to it implements both
`DataRxTx` and `StreamListener`, standing in for both of a real node's
sockets, which share one address. `NetworkedDebugTransport` is
`RetryTransport<Endpoint>`, and `NetworkedStreamTransport` passes
`Network::connect` to the same `dial_and_send`. The tests therefore run the
same code as a real node, with only the socket swapped.

Nothing binds an OS socket, so tests need no ports and no real timeouts.
`Network::with_config` adds latency and a loss rate to datagrams (not to
connections). `connect` gives the dialling side a fresh, unbound address, the
way real TCP shows the accepting side an ephemeral port. That keeps tests
honest about the rule in the next section.

## Serving RPCs

`src/handle_rpc.rs` answers other nodes' requests. A request payload is the
sender's `NodeId`, a method tag (`PING`, `STORE`, `FIND_NODE` or
`FIND_VALUE`), then the body. `parse_framed` strips the first two and
`dispatch` passes the body to the handler in `src/handle_rpc/`. A handler that
returns `None` (a malformed body, an unknown method, a STORE whose key is not
the hash of its value) sends no reply, and the sender times out.

STORE saves the value in an in-memory map. FIND_VALUE answers from that map,
or with the closest contacts it knows. Neither contacts other nodes.

### Learning contacts from requests

UDP and TCP requests go through the same `dispatch` and differ in one way:
whether the sender's address is somewhere it can be reached.

- A UDP node sends and receives on one socket, so the source address of a
  request is the address its sender listens on. `dispatch` passes it to
  `maybe_add_contact` before looking at the method, so a routing table fills
  up from ordinary traffic as well as from FIND_NODE replies.
- A TCP connection comes from an ephemeral port the OS picked for that one
  connection. The node replies on it and does not learn the address.

`Request::from_datagram` and `Request::from_connection` build the two kinds,
and `Request::from_is_reachable` is set by the constructor, so no caller can
pass the wrong value.

`serve` runs both loops: `serve_datagrams` over the transport's request
channel, and `serve_streams` accepting from a `StreamListener`. Each reply goes
back the way its request came. Every request is handled in its own spawned
task.

## Nodes

`src/node.rs` wires everything together. A `Node` owns its id and address, a
`Context` (the routing table and value store), an `Arc<Rpc>` that background
tasks share, and the task that serves incoming requests. Both kinds derive the
id from the bound address.

- `RealNode::bind(address)` binds a `UdpSocket` and a `TcpListener` to the
  same address and pairs `UdpTransport` with `TcpTransport`.
- `FakeNode::new(&network)` binds one `Endpoint` on a `Network`, passes it to
  `serve` as both the socket and the listener, and pairs
  `NetworkedDebugTransport` with `NetworkedStreamTransport`.

Both use the same `serve`, handlers and `Rpc`.

## Dropping dead contacts

Full buckets keep what they have, and the sibling list only evicts by
distance. A dead contact would otherwise keep its place forever, blocking a
live node from its bucket or staying on as a replication target. Two things
remove dead contacts, and both call `Rpc::forget`.

### On a failed call

FIND_NODE, FIND_VALUE and STORE take the `&Contact` they are calling. When the
call fails with `TimedOut`, `ConnectionRefused` or `ConnectionReset`, the
contact is forgotten. Other errors, such as a send error on our own socket or
a reply with the wrong id, say nothing about the peer and are ignored. PING
takes a bare address, because bootstrap and the CLI use it to learn the id at
an address, so a failed ping never removes anything.

How long a call takes to fail depends on the transport. Over UDP (PING,
FIND_NODE) it takes five unanswered sends over one second. Over TCP (STORE,
FIND_VALUE) it is a single attempt: a refused connection fails at once, and a
host that is gone fails after the 2 s deadline.

A contact removed by mistake comes back as soon as it sends us a UDP request
(see [learning contacts from requests](#learning-contacts-from-requests)).

### The floor

A failed call cannot tell a dead peer from our own network being down, and
when our network is down every call fails. So `forget` removes nothing while
the table holds `K` or fewer distinct contacts, which leaves enough to rejoin
through once the network is back.

## Periodic maintenance

Failed calls only catch contacts that something happens to call.
`src/maintenance.rs` checks the rest. After bootstrap, `main` calls
`Node::periodic_task`, which spawns a loop that sleeps, then runs one liveness
round, and repeats.

A round, `check_liveness(&rpc)`, walks `contacts_iter()`, pings each contact
in turn and forgets those that don't answer or answer with a different id
(that address now belongs to another node). Because the iterator is lazy, a
contact learned during the round may be checked too, and one may be pinged
twice. Removal goes through `Rpc::forget`, so [the floor](#the-floor) applies.

The sleep is `jittered(LIVENESS_CHECK_INTERVAL)`: 60 s scaled by a new random
factor in `[0.5, 1.5)` each time. The first round therefore runs 30 to 90 s
after startup. The jitter keeps nodes started together from pinging in
lockstep. The loop sleeps between rounds instead of using a fixed `interval`,
because a round spends about a second on every dead contact, and an overrun
`interval` would start the next round immediately, just when many peers are
dead.

Republishing is not implemented. `REPUBLISH_INTERVAL` (one hour, the paper's
`tReplicate`) is already passed to `periodic_task` but unused.

## Node keys in DNS

A node's ed25519 verifying key can be published in a DNS TXT record on a
domain its owner controls, so anyone can fetch the key that goes with that
name. This is not wired into `Node` yet. Lookup sits behind one trait in
`src/dns_data.rs`:

```rust
pub trait DnsData {
    async fn get_dns_vk(&self, name: impl ToName) -> Option<VerifyingKey>;
}
```

The record text is `d7024ePK=` followed by the 32-byte key in hex, 73
characters in all. `format_vk` writes it and `parse_vk` reads it. The prefix
labels the record for anyone browsing the zone, and it lets the parser skip
the other TXT records on a name, such as SPF. `parse_vk` returns `None` for
anything else: a missing prefix, bad hex, the wrong length, or bytes that are
not a valid curve point. `get_dns_vk` returns the first record on the name
that parses, or `None`.

There are two implementations, in `src/dns_data/`:

- `DNS` (`dns.rs`) queries a `domain::resolv::StubResolver`. It joins a
  record's character strings before parsing, since a long TXT record may be
  split into several.
- `FakeDns` (`fake_dns.rs`) holds `(name, text)` pairs in memory. `new()`
  publishes one record for each of the five `FAKE_KEYS`,
  `node0.d7024e.test` to `node4.d7024e.test`, whose key pairs are hardcoded so
  tests can sign with the private half. Lookups go through the same `parse_vk`
  as `DNS`, and tests can push other records onto `records`. These private
  keys are public in the repository, so they must never go on a real domain.

## Packages

`src/package.rs` holds the data types for the package registry described in
`d7024e/labs/PART2-DESIGN.md`. It defines the records and how they are signed
and keyed. Nothing outside the module's tests uses it yet: no RPC, CLI command
or lookup stores or fetches these records.

A package is named by a domain and a package name, such as `rfin.ch` and
`java-pair`. The domain's owner holds an ed25519 key pair, and every record
about the package is signed with it. Publishing a version puts three kinds of
value in the DHT:

- The package blob, under its hash, as in part 1.
- A `VersionRecord` for each version: `tag`, `domain`, `package`, `version`,
  `blob_hash` (the blob's key) and `prev` (the key of the previous version's
  record, all zeroes for the first). It is stored under `record_key`, the hash
  of the signed record, so like the blob it is immutable and checks itself.
  Following `prev` walks back through every older version.
- One `LatestPointer` per package: `tag`, `domain`, `package`, `version` and
  `version_record_hash`, the key of the newest `VersionRecord`. It is stored
  under `latest_key(domain, package)`, which is `hash("domain:package:latest")`.
  This is the only value that is not stored under its own hash and the only
  one that changes, since publishing a new version replaces it.

`Version` is `major.minor.patch`. It parses from and prints as that string,
and its fields are in that order, so the derived `Ord` compares numerically
(`1.9.0 < 1.11.0`).

Both record types are wrapped in `Signed<T>`, a `body` plus `sig`. `sign`
signs `body_hash(body)`, the SHA-256 of the body's bincode encoding, so the
signature covers every field except itself. `Signed::verify(owner)` checks it
against the owner's public key and returns `false` for a wrong key, a changed
body or bytes that are not a signature at all.

Not implemented yet:

- Finding the owner's public key. The design reads it from a DNS TXT record
  on the domain; `verify` takes the key as an argument.
- Checking that a record is valid beyond its signature, for example that
  `tag` is the right string for the type, that versions only increase along
  the chain, or that a new `LatestPointer` names a newer version than the one
  it replaces.

## Metrics

`src/logging.rs` sends log records with the target `metrics` only to
`metrics.log`, one line per event, and everything else at `info` and above to
stdout. `src/instrumentation.rs` writes all the metric events:

| Event | Fields |
|---|---|
| `lookup_start` | `lookup_id kind node target` |
| `lookup_probe` | `lookup_id kind node peer`, one per RPC the lookup sends |
| `lookup_end` with `kind=node` | `lookup_id node probes result_count exact_match` |
| `lookup_end` with `kind=value` | `lookup_id node probes success` |

`lookup_id` is a decimal counter starting at 1. `kind` is `node` or `value`.
`node`, `target` and `peer` are hex ids.

Lookups nest. A value lookup (`get`) first runs a node lookup, which logs its
own `kind=node` events inside the value lookup's, and the value lookup's
`probes` counts only its FIND_VALUE requests. `put` and bootstrap each run a
node lookup too, so the node-lookup count from `analyze_metrics.py` includes
all of these.
