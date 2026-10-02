# Experiments

Scripts and a harness for the experimental evaluation: lookup scalability vs
N, lookup reliability vs packet loss (both mandatory), and, optionally,
request/response time vs latency and loss, probes and time vs α, and
reliability vs churn, plus heatmaps and 3D surfaces over two parameters at
a time.

| File | What it is |
|---|---|
| `examples/experiment.rs` | One run: builds a network, stores values, measures lookups, logs events. |
| `experiments/run_suite.py` | Runs every configuration × seed, several at a time; resumable. |
| `experiments/analyze.py` | Parses the logs into `summary.csv`, `aggregate.csv`, `plots/*.png`, `report.html`. |
| `experiments/start.sh` | Starts the suite and then the analysis in a detached tmux session. |

## Running it

```sh
experiments/start.sh                     # the full suite (~16-24 h), then analysis
tmux attach -t kademlia-experiments      # watch; Ctrl-b d detaches again
tail -f experiments/results/suite.log    # or follow the log
```

The suite survives a dropped SSH session because it runs inside tmux. If it
is stopped anyway (reboot, `tmux kill-session`), run the same command again.
A run is skipped if its directory has a `DONE` file and its `params.json`
matches the run's current settings exactly. Changing any setting, including
a fixed one like the network size, therefore re-runs the affected runs.

Up to 8 runs go at once, as long as their estimated memory (about 0.26 MB
per node) stays under 7000 MB. `--jobs` and `--mem-budget-mb` change these
limits.

## What the suite runs

Every experiment states its own network size in `experiments/run_suite.py`.
Each configuration is repeated with the listed number of seeds.

| Experiment | Nodes N | Varies | Seeds |
|---|---|---|---|
| `scalability` | 16 … 16384, doubling | N | 10 |
| `scalability_refresh` | 16 … 16384, ×4 steps | N, with bucket refresh after join | 5 |
| `loss` | 5000 | loss 0 … 0.95 | 10 |
| `latency` | 5000 | one-way latency 0 … 600 ms × loss {0, 0.3} | 5 |
| `alpha` | 5000 | α 1 … 10 × loss {0, 0.3} | 8 |
| `churn` | 5000 | churn 0 … 100 nodes/s × liveness {10, 60 s} × republish {off, 60 s}, 10 min | 4 |
| `heat_loss_size` | 250 … 16000 | N × loss 0 … 0.9 | 3 |
| `heat_alpha_size` | 250 … 16000 | N × α 1 … 10 | 3 |
| `heat_latency_loss` | 2000 | latency 0 … 600 ms × loss 0 … 0.8 | 3 |
| `heat_churn_republish` | 2000 | churn 0 … 40/s × republish 15 s … 1 h, 5 min | 3 |
| `heat_churn_liveness` | 2000 | churn 0 … 40/s × liveness 5 … 80 s, 5 min | 3 |

Each report section begins with a "What the numbers mean" box. It turns
the measured and modelled numbers into sentences, for example how long a
churn rate takes to replace the whole network, or at what loss rate lookups
stop succeeding. The heatmap sections also put a model heatmap next to a
measured one where a model exists (RPC success over latency × loss, value
availability over churn × republish).

Other forms:

```sh
python3 experiments/run_suite.py --dry-run             # list runs, estimate time
python3 experiments/run_suite.py --suite quick         # ~3 min smoke test of everything
python3 experiments/run_suite.py --only loss,alpha     # a subset
python3 experiments/run_suite.py --seed-scale 2        # twice the seeds everywhere
experiments/.venv/bin/python experiments/analyze.py    # re-analyze whatever has finished
```

`start.sh` creates `experiments/.venv` with matplotlib and numpy. To do it by
hand: `python3 -m venv experiments/.venv && experiments/.venv/bin/pip install matplotlib numpy`.

A single run, by hand:

```sh
cargo run --release --example experiment -- --nodes 1000 --seed 7 --loss 0.3 --out /tmp/run
```

`--help` lists every flag.

## Experimental setup

**The network.** Every run is one process that simulates the whole network
on an in-process fake wire (`NetworkedDebugTransport`). That wire runs the
same transport code as the real UDP and TCP paths: the same framing, the
same pending-reply matching, the same resend loop. Only the socket is
replaced by a channel. Each run uses a fresh process, so no state carries
over between runs.

**Topology from the seed.** Node addresses are drawn at random from
`10.0.0.0/8` with random ports, using a generator seeded with the run's
seed. A node id is SHA-256 of `ip:port`, so each seed gives a different set
of ids. Nodes join in batches of 50, each bootstrapping through a random
node that has already joined. The values are 64 random bytes from the same
seed, and so are the node that runs each lookup and what it looks for.

**What the seed cannot fix.** Lookups run concurrently on a multi-threaded
runtime. Which datagram meets which loss draw, and what a routing table
holds at the moment a lookup reads it, depend on scheduling. Two runs with
the same seed are therefore close but not identical. Every configuration is
run with 5 to 10 seeds, and the report shows the mean and the variance
across them.

**Phases.** Each run goes through these phases, logged as `event=phase` lines:

1. *build*: lossless wire, latency at most 5 ms; all N nodes join.
2. *refresh* (only in `scalability_refresh`): each node looks up 4 random
   ids. This stands in for the bucket refresh that `bootstrap` does not do
   yet (see the TODO in `src/bootstrap.rs`).
3. *settle*: 2 s.
4. *store*: still lossless. `--values` values are stored from random nodes.
   Then the run counts how many nodes hold each value, and how many of the
   K nodes truly closest to its key do.
5. *measure*: the wire's loss is set to `--loss` and its latency to
   `--latency-ms`, and churn starts if
   requested. Then `--lookups` value lookups and the same number of node
   lookups run, alternating, 16 at a time. In churn runs they are instead
   started at an even pace over `--duration-secs`.

Only lookups the harness starts during *measure* are counted. Each one runs
inside `instrumentation::with_op`, so its `lookup_end` and `rpc` lines carry
the same `op=` as the `event=op` line that reports it. Background work
(republishing, liveness pings, joins under churn) logs `op=0` and is left
out of the lookup statistics. Loss and latency are applied only after the
network is built and the values stored. The loss experiment therefore
measures lookups, not a network that formed badly under loss. And at
latencies whose round trip outlasts the 1 s retry budget, there is still a
network to measure: in an earlier version, those runs failed because no
node could join.

**How things are measured.**

- *Probes*: a lookup counts every FIND_NODE (or FIND_VALUE) it sends,
  including ones that time out. A value lookup first runs a node lookup for
  the key, and that node lookup logs its own probes with `parent=` set to
  the value lookup's id. The analysis adds the two together.
- *Hops*: each contact remembers how many replies, one leading to the next,
  it took to learn of it. Contacts already in the routing table count 0. A
  lookup's hop count is that number for the closest contact it ended with.
  This is the quantity Kademlia's O(log N) bound is about. Probes also
  include the up-to-α parallel queries made each round, and the final
  queries that confirm the K closest.
- *Success*: a value lookup succeeds if it returns a value whose SHA-256 is
  the key. The harness also checks that it is the value that was stored. A
  node lookup succeeds if the target node is among the contacts returned.
  *Recall* is the fraction of the K live nodes truly closest to the target
  that the lookup returned. "Truly closest" is computed by the harness from
  global knowledge of every live node.
- *Availability* (churn): whether any live node still holds the value when
  the lookup starts. Success given availability isolates routing failures
  from data loss.
- *Time*: wall-clock time from the start of a lookup or RPC to its end,
  measured in the process. For RPCs, the time runs from the first send to
  the reply (or to giving up), and the number of sends is logged too. RPC
  events are enabled only during *measure*, and by default only for the
  RPCs measured lookups make (`--rpc-events ops`). `--rpc-events all` adds
  the background traffic, which at 5000 nodes is thousands of liveness
  pings per second.

**Why these measurements can be trusted.** The probe count comes from the
lookup code itself. Success and recall are checked against ground truth the
lookup could not have seen. As a sanity check, the per-RPC numbers can be
compared with a closed-form model. At p = 0.5, the model predicts a 25%
first-attempt success and a 23.7% timeout rate; a test run measured 24.9%
and 23.6%. The analysis plots this comparison for every loss level
(`loss_rpc_fail.png`). Times include scheduling delay inside the process, and
up to 8 runs share the CPU. The simulated latency (5 ms one way by default)
and the 200 ms resend timer are much larger than that delay, so timings stay
meaningful. Even so, read times as approximate and probe counts as exact.

## RPC timeout and retry policy

These settings shape the loss and latency results, so they are logged in
every run's `event=run_config` line:

- **Datagram RPCs** (PING, FIND_NODE) go through `RetryTransport`. A request
  is resent after **200 ms** without a reply, up to **5 sends**, so the call
  gives up after **1 s**. Every resend carries the same request id, so a
  late reply to an earlier send still completes the call. With independent
  loss p per datagram, one send's round trip survives with probability
  (1−p)², and the call succeeds with probability
  `1 − (1 − (1−p)²)^5`.
- **STORE and FIND_VALUE** use the connection transport (TCP on a real
  network), with a **2 s** deadline. The simulated loss applies only to
  datagrams, so these calls are not lost. A value lookup can therefore fail
  only in its node-lookup phase, or because nobody holds the value anymore.
- **Republishing**: every node re-stores each value it holds onto the
  current K closest nodes, on a jittered `--republish-secs` interval. The
  default is the node's own one hour, which never fires inside a run. The
  churn experiment compares that with every 60 s.
- **When a call fails**, the contact is removed from the routing table,
  unless the table has K or fewer contacts left. Every node also pings its
  whole table on a jittered liveness interval (60 s by default; 10 s and 60 s
  in the churn experiment) and drops contacts that don't answer.
- **Lookups**: α = 3 (varied in `alpha`), K = 10. A lookup ends when the K
  closest contacts it knows have all been queried.

## What to expect

- **Scalability.** A lookup queries until its K closest candidates have all
  answered, so it sends at least min(N−1, K) probes. Above that floor, the
  probe count should grow like log N. Hops should stay below log₂N: each
  reply brings up to K contacts from the bucket nearest the target, so one
  hop typically gains more than one bit of shared prefix. Distant buckets
  are filled only by incoming traffic, because `bootstrap` does no bucket
  refresh. That should cost extra hops in large networks; the `refresh`
  variant measures how much. Routing tables should hold about
  `Σ min(K, N/2^(i+1))` contacts.
- **Loss.** Single RPCs follow the model above. Lookups should do much
  better than single RPCs: a lookup needs only some of its ~10–15 probes to
  get through, and only one of the K replicas to be found. FIND_VALUE is not
  lost at all. Lookup time should rise long before success drops, because
  every lost datagram costs a 200 ms wait. At high loss, failed calls also
  remove contacts, so tables shrink and lookups degrade further.
- **Latency.** RTT ≈ 2L. Once 2L > 200 ms, requests are resent before their
  replies can arrive; that is wasted traffic, but the call still succeeds.
  Once 2L > 1 s, no reply can arrive before the call gives up, and every
  datagram RPC fails.
- **α.** Larger α means more probes (speculative queries) but fewer rounds,
  so lower latency, with diminishing returns as α approaches K. Under loss,
  a larger α hides timeouts.
- **Churn.** A node survives t seconds with probability e^(−ct/N). Without
  republishing, a value with h holders is therefore still available with
  probability `1 − (1 − e^(−ct/N))^h`. With republishing every T seconds, a
  value is lost only if all K holders leave within one interval, which
  happens with probability `(1 − e^(−cT/N))^K` per interval. On top of that, lookups lose time and probes on departed contacts until a
  failed call or a liveness round removes them. That is why the experiment
  compares a 10 s liveness interval with a 60 s one.

## Log format

One event per line in `metrics.log` (gzipped to `metrics.log.gz` by the
suite). Each line is `event=NAME` followed by `key=value` pairs; values never
contain spaces.

```text
event=run_config nodes=1000 seed=3 loss=0.3 latency_ms=5 alpha=3 k=10 ... resend_ms=200 max_attempts=5 stream_deadline_ms=2000
event=phase name=measure t_ms=4982 live=1000
event=lookup_start lookup_id=88 op=12 kind=node node=<hex> target=<hex>
event=lookup_probe lookup_id=88 kind=node node=<hex> peer=10.3.4.5:2210
event=lookup_end lookup_id=88 op=12 kind=node parent=0 node=<hex> probes=13 result_count=10 exact_match=true hops=2 duration_us=41730
event=lookup_end lookup_id=87 op=11 kind=value node=<hex> probes=1 success=true duration_us=52011
event=rpc op=12 method=FIND_NODE peer=10.3.4.5:2210 attempts=2 success=true duration_us=210533
event=op op=11 kind=value t_ms=120 origin=10.1.2.3:4000 key=<hex> success=true correct=true available=true holders=10 duration_us=52100
event=op op=12 kind=node t_ms=121 origin=10.1.2.3:4000 target=10.9.8.7:1234 success=true recall=10 of=10 duration_us=41800
event=stored key=<hex> holders=10 closest_holders=9
event=churn action=leave node=10.4.4.4:5555
event=join node=10.5.5.5:6666 seed=10.1.2.3:4000 success=true contacts=10
```

The older `analyze_metrics.py` at the repository root still reads these
files, since it only needs `lookup_end` lines.

## Not covered

- **Replication factor K.** K is a compile-time constant (it sizes the
  routing buckets through const generics), so varying it means rebuilding
  per value. That is possible, but the suite does not do it.
- **Latency jitter and correlated loss.** The fake wire has a fixed latency
  and drops each datagram independently.
