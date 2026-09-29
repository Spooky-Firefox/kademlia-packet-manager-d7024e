# Manual test: a 50-node network in Docker

## Setup

| Piece | What it does |
|---|---|
| `Dockerfile` | Builds the node in a Rust image, then copies only the binary into a slim Debian image. |
| `compose.yaml`  `seed` | One node at the fixed address `172.28.0.10:4000`. It starts a new network. |
| `compose.yaml`  `node` | 49 replicas. Each binds to its own container IP and bootstraps through the seed. |
| Network `kademlia` | Subnet `172.28.0.0/16`. Replicas get addresses from `172.28.1.0/24`, so they never collide with the seed. |

Containers are named `<project>-<service>-<n>`

## P1: init

```sh
docker --version          # tested with 29.x
docker compose version    # tested with 5.x
docker info > /dev/null && echo ok
```


```sh
docker compose up -d --build
```


```sh
docker compose ps --status running | tail -n +2 | wc -l
docker compose logs node | grep -c "bootstrap complete"
docker compose logs node | grep -c "bootstrap failed"
```


```sh
docker attach kademlia-packet-manager-d7024e-node-20
```

```
kademlia> show rt
```

(exit with Ctrl-P then Ctrl-Q)


## p2: create value

Open a second terminal and create a file inside node 20:

```sh
docker compose exec --index 20 node sh -c 'echo "hello kademlia" > /tmp/hello.txt'
```

Back in node 20's CLI:

```
kademlia> put /tmp/hello.txt
```

**Expect:** `stored 15 bytes as <64 hex characters>`. **Copy the full key.**

The value is sent to the `k`nodes with the closest IDs to the key

## P3: Fetch value from different node

Detach from node 20 and attach to a different node:

```sh
docker attach kademlia-packet-manager-d7024e-node-35
```

```
kademlia> get <key>
```

**Expect:**
`
received 15 bytes from 172.28.1.x:4000 (abcd…1234)
hello kademlia
`

`get <key> /tmp/out.txt` if you want to save it to a file.

## P4: See where the copies ended up

Attach to a few nodes and run:

```
kademlia> show ds
```

most nodes print `<empty>`. The ones that hold the value print
`abcd…1234  15 bytes` should be at most 10 of them.

## P5: Failure test: stop a node that holds the value

Note the address in step 5's `received ... from 172.28.1.x` line, and find
which container has that address:

```sh
docker compose ps -q node | xargs docker inspect \
  --format '{{.Name}} {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  | grep '172.28.1.x$'
```

Stop that container:

```sh
docker stop <container-name>
```

Then run the same `get <key>` from another node again.

**Expect:** the value still arrives, this time `from` a different address. The
request may take a moment longer: a stopped node is retried a few times,
200 ms apart, before the lookup gives up on it.


Bring the node back with `docker start <container-name>`.

## P6: shut down

```sh
docker compose down
```