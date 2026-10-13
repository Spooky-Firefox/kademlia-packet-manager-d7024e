# D7024E — Part 2 demo



Start **Node A** (terminal 1):

```powershell
cargo run -- --fake-dns 127.0.0.1:8000
```

Start **Node B** (terminal 2):

```powershell
cargo run -- --fake-dns 127.0.0.1:8001 127.0.0.1:8000
```

## publishing and installation

**Node A — Publish version 1:**

```text
publish node0.d7024e.test:example:1.0.0 file1.txt
```

**Node B — Install latest (should be version 1):**

```text
install node0.d7024e.test:example:latest installed-v1.txt
```


**Node A — Publish version 2:**

```text
publish node0.d7024e.test:example:2.0.0 file2.txt
```

**Node B — Install latest, then version 1 explicitly:**

```text
install node0.d7024e.test:example:latest installed-v2.txt
install node0.d7024e.test:example:1.0.0 installed-v1-again.txt
show node0.d7024e.test:example
show ds
```

## wrong domain owner

**Node A — Attempt to publish under another domain:**

```text
publish node1.d7024e.test:example:1.0.0 file1.txt
```

**Node B — Confirm package was not published:**

```text
show node1.d7024e.test:example
```

## same version, different file

**Node A — Attempt to overwrite 2.0.0 using a different file:**

```text
publish --force node0.d7024e.test:example:2.0.0 file3.txt
```

> ```--force``` tells publishing to skip local version check, lets us attempt invalid publications

**Node B — Check that version 2 is unchanged:**

```text
show node0.d7024e.test:example
install node0.d7024e.test:example:2.0.0 verified-v2.txt
```

## rollback

**Node A — Publish version 5 legitimately:**

```text
publish node0.d7024e.test:example:5.0.0 file5.txt
```

**Node A — Attempt a rollback to version 3:**

```text
publish --force node0.d7024e.test:example:3.0.0 file3.txt
```

**Node B — Prove latest is still version 5:**

```text
show node0.d7024e.test:example
install node0.d7024e.test:example:latest installed-v5.txt
show ds
```

> A rejected forced publication may still leave immutable blobs/records in storage; the security requirement is that the **latest pointer is not replaced**.