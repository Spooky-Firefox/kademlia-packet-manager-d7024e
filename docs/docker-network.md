# Manual test: a 50-node network in Docker

## Setup

| Piece | What it does |
|---|---|
| `Dockerfile` | Builds the node in a Rust image, then copies only the binary into a slim Debian image as `kademlia`. |
| `compose.yaml`, service `seed` | One node at the fixed address `172.28.0.10:4000`. It starts a new network. |
| `compose.yaml`, service `node` | 49 replicas. Each waits 0 to 5 seconds, binds to its own container IP on port 4000 and bootstraps through the seed. |
| Network `kademlia` | Subnet `172.28.0.0/16`. Replicas get addresses from `172.28.1.0/24`, so they never collide with the seed. |

Containers are named `kademlia-packet-manager-d7024e-<service>-<n>`, for
example `kademlia-packet-manager-d7024e-node-20`. The number is not related to
the container's IP address. Run all commands from the project directory.

## Step 1: start the network

Check that Docker works:

```sh
docker --version          # tested with 29.x
docker compose version    # tested with 5.x
docker info > /dev/null && echo ok
```

Build the image and start all 50 containers:

```sh
docker compose up -d --build
```

After a few seconds, check that they are running and have joined. Expect 49,
49 and 0:

```sh
docker compose ps --status running node | tail -n +2 | wc -l
docker compose logs node | grep -c "bootstrap complete"
docker compose logs node | grep -c "bootstrap failed"
```

Attach to a node and look at its routing table:

```sh
docker attach kademlia-packet-manager-d7024e-node-20
```

```
kademlia> show rt
```

Detach with Ctrl-P then Ctrl-Q. Typing `exit` stops the node instead.

## Step 2: store a value

In a second terminal, create a file inside node 20:

```sh
docker compose exec --index 20 node sh -c 'echo "hello kademlia" > /tmp/hello.txt'
```

Back in node 20's CLI:

```
kademlia> put /tmp/hello.txt
```

Expect `stored 15 bytes as <64 hex characters>`. Copy the full key. The value
is sent to the 10 nodes whose ids are closest to the key.

## Step 3: fetch the value from another node

Detach from node 20 and attach to a different node:

```sh
docker attach kademlia-packet-manager-d7024e-node-35
```

```
kademlia> get <key>
```

Expect:

```
received 15 bytes from 172.28.1.x:4000 (abcd…1234)
hello kademlia
```

The address is the node that sent the value, and `abcd…1234` is its shortened
id. Write the address down for step 5. To save the value to a file instead,
use `get <key> /tmp/out.txt`.

## Step 4: see where the copies are

Attach to a few nodes and run:

```
kademlia> show ds
```

Most nodes print `<empty>`. The ones holding the value print its shortened key
and `15 bytes`. At most 10 nodes hold it.

## Step 5: stop a node that holds the value

Find the container with the address from step 3. Replace `35` with the last
number of your address:

```sh
docker compose ps -q node | xargs docker inspect \
  --format '{{.Name}} {{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' \
  | grep '172.28.1.35$'
```

This prints the container name and its address, for example
`/kademlia-packet-manager-d7024e-node-26 172.28.1.35`. Stop that container:

```sh
docker stop kademlia-packet-manager-d7024e-node-26
```

A stopped container has no IP address, so run the lookup before stopping it.

Run the same `get <key>` from another node. The value should still arrive,
from a different address. It may take a few seconds longer: the lookup waits
up to 1 s for each UDP request and up to 2 s for each TCP request to the
stopped node before giving up on it.

Bring the node back with `docker start <container-name>`.

## Step 6: shut down

```sh
docker compose down
```
