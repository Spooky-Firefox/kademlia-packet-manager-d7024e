FROM rust:1.98-slim-trixie AS builder
WORKDIR /app 

# build dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release

# build code stuff
COPY src ./src
RUN touch src/main.rs && cargo build --release

