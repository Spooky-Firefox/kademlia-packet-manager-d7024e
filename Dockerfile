FROM rust:1.98-slim-trixie AS builder
WORKDIR /app 

# build dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release

# build code stuff
COPY src ./src
RUN touch src/main.rs && cargo build --release

FROM debian:trixie-slim
RUN useradd --system kademlia
RUN mkdir /logs && chown kademlia /logs
WORKDIR /logs
COPY --from=builder /app/target/release/kademlia-packet-manager-d7024e /usr/local/bin/kademlia
USER kademlia
EXPOSE 4000/udp 
EXPOSE 4000/tcp