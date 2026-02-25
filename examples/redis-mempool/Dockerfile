# Dockerfile for building Dolos from source with Redis support
FROM rust:1.82-bookworm AS builder

WORKDIR /usr/src/dolos

# Copy the entire project
COPY . .

# Build with all features including Redis
RUN cargo build --release --workspace --all-features

# Runtime image
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates redis-tools curl && rm -rf /var/lib/apt/lists/*

# Copy genesis files
COPY --from=builder /usr/src/dolos/examples/redis-mempool/*.json /etc/genesis/

# Copy the built binary
COPY --from=builder /usr/src/dolos/target/release/dolos /bin/dolos
RUN chmod +x /bin/dolos

# Create directories for data
RUN mkdir -p /data1 /data2

ENTRYPOINT ["dolos"]
