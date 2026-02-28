# Dockerfile for building Dolos from source with Redis support
FROM rust:1.93 AS builder

WORKDIR /usr/src/dolos

# Copy the entire project
COPY . .

# Build with all features including Redis
RUN cargo build --release --workspace

# Runtime image
FROM debian:trixie-slim

RUN apt-get update && apt-get install -y ca-certificates redis-tools curl && rm -rf /var/lib/apt/lists/*

# Copy genesis files
COPY --from=builder /usr/src/dolos/examples/redis-mempool/*.json /etc/genesis/

# Copy the built binary
COPY --from=builder /usr/src/dolos/target/release/dolos /bin/dolos
RUN chmod +x /bin/dolos

ENTRYPOINT ["dolos"]
