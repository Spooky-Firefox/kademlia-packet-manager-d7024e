# D7024E — Part 2 demo (Docker, fake DNS)

The demo runs on the 50-node Docker network from `compose.yaml`, with every
node using the built-in fake DNS (`--fake-dns`). Fake DNS is a fixed table of
test domains and keys compiled into the program (`src/dns_data/fake_dns.rs`),
so no DNS server is involved.

| Role | Container | What it does |
| --- | --- | --- |
| **Node A** | `kademlia-packet-manager-d7024e-publisher-1` (`172.28.0.11`) | publishes, signing as `node0.d7024e.test` with its built-in test key |
| **Node B** | `kademlia-packet-manager-d7024e-node-20` | an ordinary node that installs packages |

Use three terminals, all in the project directory: terminal 1 for Node A,
terminal 2 for Node B, terminal 3 for `docker` commands.

Detach from a node with **Ctrl-P then Ctrl-Q**. Typing `exit` stops the node.

## Setup

**One-time — turn on fake DNS.** In `compose.yaml`, add `--fake-dns` right
after `exec kademlia` in the `command` of the `seed`, `node` and `publisher`
services, for example:

```yaml
    command: sh -c 'exec kademlia --fake-dns "$$(hostname -i):4000" 172.28.0.10:4000'
```

`--fake-dns` must come before the addresses.

**Terminal 3 — start a clean network:**

```sh
docker compose down
docker compose up -d --build
```

**Terminal 3 — wait for the nodes to join (expect 49):**

```sh
docker compose logs node | grep -c "bootstrap complete"
```

Nodes that start before the seed is ready fail to bootstrap and are restarted
by Docker, so `bootstrap failed` lines in the logs are normal. Once the count
reaches 49, wait about a minute more so routing tables settle before
publishing.

**Terminal 3 — copy the package files into Node A:**

```sh
for f in file1.txt file2.txt file3.txt file5.txt; do docker compose cp "$f" "publisher:/tmp/$f"; done
```

**Terminal 1 — attach to Node A:**

```sh
docker attach kademlia-packet-manager-d7024e-publisher-1
```

**Terminal 2 — attach to Node B:**

```sh
docker attach kademlia-packet-manager-d7024e-node-20
```

**Node A and Node B — check that both use fake DNS:**

```text
show dns node0.d7024e.test
```

Expect `node0.d7024e.test: 5156ad53f8a8149e5e3b563c242f529fc868696a2a3f10d28f38dea5566d399d`,
the built-in test key. Any other answer means the node was started without
`--fake-dns`.

## publishing and installation

**Node A — Publish version 1:**

```text
publish node0.d7024e.test:example:1.0.0 /tmp/file1.txt
```

**Node B — Install latest (should be version 1):**

```text
install node0.d7024e.test:example:latest /tmp/installed-v1.txt
```

**Terminal 3 — show the installed file (expect `1111111111111`):**

```sh
docker compose exec --index 20 node cat /tmp/installed-v1.txt
```

**Node A — Publish version 2:**

```text
publish node0.d7024e.test:example:2.0.0 /tmp/file2.txt
```

**Node B — Install latest, then version 1 explicitly:**

```text
install node0.d7024e.test:example:latest /tmp/installed-v2.txt
install node0.d7024e.test:example:1.0.0 /tmp/installed-v1-again.txt
show node0.d7024e.test:example
show ds
```

**Terminal 3 — show both files (expect `2222…` then `1111…`):**

```sh
docker compose exec --index 20 node cat /tmp/installed-v2.txt /tmp/installed-v1-again.txt
```

## wrong domain owner

**Node A — Attempt to publish under another domain:**

```text
publish node1.d7024e.test:example:1.0.0 /tmp/file1.txt
```

> Node A signs with node0's key, which does not match the key fake DNS gives
> for `node1.d7024e.test`, so the publication is refused.

**Node B — Confirm package was not published:**

```text
show node1.d7024e.test:example
```

## same version, different file

**Node A — Attempt to overwrite 2.0.0 using a different file:**

```text
publish --force node0.d7024e.test:example:2.0.0 /tmp/file3.txt
```

> ```--force``` tells publishing to skip local version check, lets us attempt invalid publications

**Node B — Check that version 2 is unchanged:**

```text
show node0.d7024e.test:example
install node0.d7024e.test:example:2.0.0 /tmp/verified-v2.txt
```

**Terminal 3 — expect `2222…`, not `3333…`:**

```sh
docker compose exec --index 20 node cat /tmp/verified-v2.txt
```

## rollback

**Node A — Publish version 5 legitimately:**

```text
publish node0.d7024e.test:example:5.0.0 /tmp/file5.txt
```

**Node A — Attempt a rollback to version 3:**

```text
publish --force node0.d7024e.test:example:3.0.0 /tmp/file3.txt
```

**Node B — Prove latest is still version 5:**

```text
show node0.d7024e.test:example
install node0.d7024e.test:example:latest /tmp/installed-v5.txt
show ds
```

**Terminal 3 — expect `55555`:**

```sh
docker compose exec --index 20 node cat /tmp/installed-v5.txt
```

> A rejected forced publication may still leave immutable blobs/records in storage; the security requirement is that the **latest pointer is not replaced**.

## Shut down

**Terminal 3:**

```sh
docker compose down
```
