testing udp

```sh
echo "this is a test" | nc -u localhost port
```

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
