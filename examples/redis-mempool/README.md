# Redis Mempool Example

This example demonstrates the **RedisMempool** with **watcher election** for multi-node Dolos deployments.

## Overview

Two Dolos instances share a single Redis-backed mempool:
- Only one instance acts as the "watcher" and processes `confirm()` operations
- If the watcher fails, the other instance automatically takes over
- Both instances can receive and track transactions
- Shared mempool state ensures consistency across nodes

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Dolos Node 1  │────▶│   Redis      │◀────│   Dolos Node 2  │
│   (Port 50051)  │     │   (Port 6379)│     │   (Port 50052)  │
└─────────────────┘     └──────────────┘     └─────────────────┘
         │                                            │
         │         Shared Mempool State               │
         │         ├─ Pending transactions            │
         │         ├─ Inflight transactions         │
         │         ├─ Finalized transactions        │
         │         └─ Watcher lock (TTL: 10s)       │
         └────────────────────────────────────────────┘
```

## Quick Start

### 1. Start all services

```bash
cd examples/redis-mempool
docker-compose up -d --build
```

This will:
- Build Dolos from source with Redis support
- Start Redis server
- Start two Dolos instances (dolos1 and dolos2)

### 2. Check which node is the watcher

```bash
./watch-watcher.sh
```

Or manually check Redis:
```bash
docker-compose exec redis redis-cli GET dolos:mempool:watcher:lock
```

### 3. Monitor watcher logs

Watch dolos1:
```bash
docker-compose logs -f dolos1 | grep -E "(watcher|acquired|lost)"
```

Watch dolos2:
```bash
docker-compose logs -f dolos2 | grep -E "(watcher|acquired|lost)"
```

## Testing Failover

### Scenario 1: Watcher Failure

1. Identify which node is the watcher:
   ```bash
   ./watch-watcher.sh
   ```
   Output: `Current watcher: <node-id> (TTL: 8s)`

2. Stop the watcher node (e.g., if dolos1 is watcher):
   ```bash
   docker-compose stop dolos1
   ```

3. Within 10 seconds, dolos2 will acquire the lock:
   ```
   dolos2_1  | INFO dolos_redis::mempool: acquired watcher lock
   ```

4. Verify dolos2 is now the watcher:
   ```bash
   ./watch-watcher.sh
   ```

5. Restart dolos1:
   ```bash
   docker-compose start dolos1
   ```

6. Observe that dolos1 rejoins as a non-watcher:
   ```
   dolos1_1  | DEBUG dolos_redis::mempool: not watcher, skipping confirm
   ```

### Scenario 2: Network Partition

1. Disconnect dolos1 from Redis:
   ```bash
   docker network disconnect dolos-redis-net redis-mempool-dolos1
   ```

2. Within 10 seconds, dolos2 takes over

3. Reconnect dolos1:
   ```bash
   docker network connect dolos-redis-net redis-mempool-dolos1
   ```

## Log Patterns

### When a node acquires the watcher lock:
```
INFO dolos_redis::mempool: acquired watcher lock
```

### When a node renews the watcher lock:
```
DEBUG dolos_redis::mempool: watcher lock renewed
```

### When a node is not the watcher:
```
DEBUG dolos_redis::mempool: not watcher, skipping confirm
```

### When a node loses the watcher lock:
```
DEBUG dolos_redis::mempool: lost watcher lock, skipping confirm
```

## Configuration

Both `dolos1.toml` and `dolos2.toml` share the same Redis configuration:

```toml
[storage.mempool]
backend = "redis"
url = "redis://redis:6379"
key_prefix = "dolos:mempool"  # Shared key namespace
pool_size = 10
max_finalized = 10000
watcher_lock_ttl = 10  # Seconds before lock expires
```

Key differences between nodes:
- `storage.path`: `/data1` vs `/data2` (separate local storage)
- `serve.grpc.listen_address`: `50051` vs `50052` (different gRPC ports)
- `serve.ouroboros.listen_path`: Different Unix socket paths

## Redis Data Structure

The watcher lock key:
- **Key**: `dolos:mempool:watcher:lock`
- **Value**: `<node-id>` (UUID generated at startup)
- **TTL**: 10 seconds (configurable via `watcher_lock_ttl`)

When the TTL expires, any node can acquire the lock.

## Cleanup

Stop and remove all containers:
```bash
docker-compose down -v
```

Remove data directories:
```bash
rm -rf data1 data2
```

## Troubleshooting

### Check Redis connectivity
```bash
docker-compose exec redis redis-cli ping
```

### View Redis keys
```bash
docker-compose exec redis redis-cli KEYS 'dolos:mempool:*'
```

### Check watcher lock TTL
```bash
docker-compose exec redis redis-cli TTL dolos:mempool:watcher:lock
```

### Reset watcher lock (emergency)
```bash
docker-compose exec redis redis-cli DEL dolos:mempool:watcher:lock
```

## Network Details

- **Network**: Cardano Preprod (magic=1)
- **Genesis**: Preprod genesis files included
- **Bootstrap**: Mithril snapshot for fast sync
- **Redis**: Port 6379
- **Dolos 1 gRPC**: Port 50051
- **Dolos 2 gRPC**: Port 50052
