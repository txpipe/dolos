# dolos-redis

Redis-backed mempool implementation for Dolos with leader election for multi-node deployments.

## Overview

`RedisMempool` provides a `MempoolStore` implementation backed by Redis, designed for multi-node Dolos deployments where multiple instances need to share mempool state. Unlike `EphemeralMempool` (in-memory only) or `RedbMempool` (local filesystem), `RedisMempool` enables:

- **Shared mempool state** across multiple Dolos nodes
- **Automatic failover** via leader election
- **Best-effort confirmation semantics** (acceptable for soft-state mempool)

## When to Use RedisMempool

Use `RedisMempool` when you need:

1. **Multiple Dolos nodes** processing transactions (high availability)
2. **Shared mempool state** between nodes (any node can receive/track txs)
3. **Automatic failover** without manual intervention

Don't use `RedisMempool` when:

1. You only run **single Dolos node** (use `RedbMempool` or `EphemeralMempool`)
2. You need **strict exactly-once confirmation** semantics
3. **Redis is unavailable** or adds unacceptable latency

## Architecture

### High-Level Design

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
│   Dolos Node 1  │────▶│              │◀────│   Dolos Node 2  │
│                 │     │    Redis     │     │                 │
│  ┌───────────┐  │     │   (Shared    │     │  ┌───────────┐  │
│  │  Watcher  │  │     │   Mempool)   │     │  │  Non-Watch │  │
│  │  (Active) │  │     │              │     │  │  (Standby) │  │
│  └─────┬─────┘  │     │  ┌────────┐  │     │  └─────┬─────┘  │
│        │        │     │  │ Lock   │  │     │        │        │
│        │confirm │     │  │ (TTL)  │  │     │        │skip    │
│        └────────▶│     │  └────────┘  │     │        └────────▶│
└─────────────────┘     └──────────────┘     └─────────────────┘
```

**Key Concepts:**

- **Watcher Node**: The single node that processes `confirm()` operations (chain updates)
- **Non-Watcher Nodes**: All other nodes; they can receive/track transactions but skip confirmation
- **Watcher Lock**: Redis key with TTL that determines which node is the watcher
- **Automatic Failover**: When the watcher crashes, lock expires and another node takes over

### Redis Data Structures

| Key Pattern | Redis Type | Purpose |
|-------------|-----------|---------|
| `{prefix}:pending` | **List** | FIFO queue of transaction hashes waiting to be propagated |
| `{prefix}:inflight` | **Hash** | O(1) lookup for in-flight transactions by hash |
| `{prefix}:finalized` | **Sorted Set** | Paginated storage of finalized/dropped transactions |
| `{prefix}:seq` | **Counter** | Monotonic sequence number for finalized entries |
| `{prefix}:watcher:lock` | **String (TTL)** | Leader election lock (holds node_id) |
| `{prefix}:payload:{hash}` | **String (TTL)** | Transaction payload storage (24h expiry) |

## Watcher Election Mechanism

### How It Works

1. **Each node has a unique `node_id`** (UUID generated at startup)

2. **Lock Acquisition** (via Redis `SET` with `NX` + `EX`):
   ```rust
   // SET key value NX EX ttl
   // Only succeeds if key doesn't exist (NX)
   // Auto-expires after TTL seconds (EX)
   ```

3. **Lock Renewal**: On each `confirm()` call, the watcher renews the lock TTL

4. **Failover**: If the watcher crashes, the lock expires after TTL (default 10s). Any other node can then acquire the lock.

5. **Best-Effort**: If a node crashes mid-`confirm()`, the next watcher may re-apply some confirmations. This is acceptable because:
   - Mempool is soft-state (peers can re-submit transactions)
   - Confirmation counts may be slightly off
   - Transactions always reach correct final state

### Lock Key

```
Key:    {prefix}:watcher:lock
Value:  <node_id> (UUID)
TTL:    10 seconds (configurable)
```

**Monitoring:**
```bash
# Check which node is watcher
redis-cli GET dolos:mempool:watcher:lock

# Check TTL remaining
redis-cli TTL dolos:mempool:watcher:lock
```

## Design Decisions

### 1. Leader Election Pattern

**Why not use Redis pub/sub or distributed consensus?**

- **Simplicity**: Single lock key is easier to reason about than consensus algorithms
- **Automatic Failover**: TTL ensures lock expires even if node crashes
- **Crash-Safe**: No stuck locks requiring manual intervention
- **Acceptable Semantics**: Best-effort confirmation is sufficient for mempool use case

**Trade-off**: 10-second failover time (configurable via `watcher_lock_ttl`).

### 2. Synchronous API with Async Internals

The `MempoolStore` trait has synchronous methods, but `RedisMempool` uses async Redis internally:

```rust
impl MempoolStore for RedisMempool {
    fn confirm(&self, ...) -> Result<(), MempoolError> {
        let rt = tokio::runtime::Handle::try_current()?;
        rt.block_on(async {
            // async Redis operations
        })
    }
}
```

**Rationale:**
- Trait simplicity (no async trait complexity)
- Redis operations are fast (typically <1ms)
- Trade-off: Potential thread blocking (acceptable for mempool ops)

### 3. CBOR Serialization

Uses `minicbor` for consistency with `RedbMempool`:

- `InflightRecord`: CBOR-encoded, stored in Redis Hash
- `FinalizedEntry`: CBOR-encoded, stored in Sorted Set
- `ChainPoint`: Serialized via `into_bytes()` / `from_bytes()` (40-byte format)
- `TxHash`: Stored as raw `[u8; 32]`

**Custom `FinalizedStage` enum** for serializability (instead of `MempoolTxStage` which lacks CBOR derives).

### 4. Best-Effort Confirmation

We deliberately chose **not** to implement exactly-once semantics:

**Rationale:**
- Mempool is soft-state (can be reconstructed from peer re-submissions)
- Re-applying confirmations is harmless (idempotent-ish for most operations)
- Strict exactly-once would require complex distributed transactions
- 99.9% correctness is sufficient for this use case

**Implications:**
- Confirmation counts may be slightly off after failover
- Transactions are never lost (still in mempool, will re-confirm)
- Final state (Finalized/Dropped) is always correct

### 5. Separate Payload Storage

Transaction payloads are stored separately from the pending queue:

```
Pending List:    [hash1, hash2, hash3, ...]
Payload Key:     {prefix}:payload:{hash1} -> CBOR-encoded payload
```

**Why:**
- Redis Lists can't store large binary data efficiently
- Payload keys have 24h TTL to prevent orphans if tx is dropped
- O(N) pending list operations are acceptable (typically <1000 pending txs)

### 6. Connection Pooling

Uses `deadpool-redis` for connection management:

- **Configurable pool size** (default: 10 connections)
- **Automatic connection reuse**
- **Graceful degradation** if Redis is temporarily unavailable

## Transaction Lifecycle

```
receive(tx)
    ↓
┌─────────────────────────────────────┐
│ 1. Check for duplicates              │
│    - LPOS on pending list            │
│    - HGET on inflight hash           │
│ 2. Add to pending list (RPUSH)       │
│ 3. Store payload separately (SET EX)   │
└─────────────────────────────────────┘
    ↓
mark_inflight([hash])
    ↓
┌─────────────────────────────────────┐
│ 1. Remove each hash from pending     │
│    list (LREM per hash)              │
│ 2. Add to inflight hash (HSET)       │
│    - InflightRecord: stage=Propagated │
│    - Copy payload from separate key    │
└─────────────────────────────────────┘
    ↓
mark_acknowledged([hash])
    ↓
┌─────────────────────────────────────┐
│ 1. HGET record from inflight hash    │
│ 2. Update stage to Acknowledged       │
│ 3. HSET updated record back           │
└─────────────────────────────────────┘
    ↓
confirm(point, seen_txs, unseen_txs)  [WATCHER ONLY]
    ↓
┌─────────────────────────────────────┐
│ 1. Check/renew watcher lock         │
│ 2. For each inflight tx:            │
│    - If in seen_txs: confirm()     │
│    - If in unseen_txs: retry() → pending│
│    - If stale confirmed: re-confirm() │
│    - If droppable: finalize()       │
│ 3. Move finalized to sorted set       │
│ 4. Trim finalized to max size         │
└─────────────────────────────────────┘
```

## Configuration

```toml
[storage.mempool]
backend = "redis"

# Redis connection URL (required)
url = "redis://127.0.0.1:6379"

# Key prefix for namespacing (default: "dolos:mempool")
key_prefix = "dolos:mempool"

# Connection pool size (default: 10)
pool_size = 10

# Max finalized transactions to keep (default: 10000)
max_finalized = 10000

# Watcher lock TTL in seconds (default: 10)
# Shorter = faster failover but more lock contention
# Longer = less contention but slower failover
watcher_lock_ttl = 10
```

**Multiple Nodes:**

All nodes share the same configuration except:
- **Local storage path** must be different per node (`/data1`, `/data2`, etc.)
- **gRPC port** must be different per node (`50051`, `50052`, etc.)
- **key_prefix** must be the same (shared mempool state)

## Limitations

1. **Synchronous Blocking**: All operations block the calling thread (uses `block_on`)
2. **Failover Time**: 10-second default failover (configurable)
3. **O(N) Pending List**: `mark_inflight()` issues one `LREM` per hash
4. **No Cross-Node Broadcast**: Local `broadcast::Sender` only (no Redis pub/sub)
5. **Best-Effort Confirmations**: May re-apply confirmations after failover
6. **Payload Orphans**: Separate payload keys can be orphaned (24h TTL mitigates)

## Usage Example

```rust
use dolos_redis::mempool::RedisMempool;
use dolos_core::config::RedisMempoolConfig;

let config = RedisMempoolConfig {
    url: "redis://127.0.0.1:6379".to_string(),
    key_prefix: "dolos:prod:mempool".to_string(),
    pool_size: 20,
    max_finalized: 50000,
    watcher_lock_ttl: 10,
};

let mempool = RedisMempool::open(&config)?;
```

## Monitoring

### Key Metrics to Watch

1. **Watcher Lock TTL**
   ```bash
   redis-cli TTL dolos:mempool:watcher:lock
   ```
   - Should always be >0 for healthy watcher
   - Approaching 0 indicates potential failover

2. **Pending Queue Length**
   ```bash
   redis-cli LLEN dolos:mempool:pending
   ```
   - Should be relatively stable
   - Growing indefinitely indicates issue

3. **Inflight Count**
   ```bash
   redis-cli HLEN dolos:mempool:inflight
   ```
   - Should correlate with network propagation

4. **Finalized Size**
   ```bash
   redis-cli ZCARD dolos:mempool:finalized
   ```
   - Should stay near `max_finalized` (automatic trimming)

### Log Patterns

**Watcher acquired lock:**
```
INFO dolos_redis::mempool: acquired watcher lock
```

**Non-watcher skipping confirm:**
```
DEBUG dolos_redis::mempool: not watcher, skipping confirm
```

**Lock lost during confirm:**
```
DEBUG dolos_redis::mempool: lost watcher lock, skipping confirm
```

## Failover Testing

```bash
# 1. Start two nodes
# Node 1: will become watcher
# Node 2: will be non-watcher

# 2. Check which node is watcher
docker-compose exec redis redis-cli GET dolos:mempool:watcher:lock

# 3. Stop the watcher node
# Node 2 should acquire lock within watcher_lock_ttl seconds

# 4. Check logs
docker-compose logs -f node2 | grep "acquired watcher lock"

# 5. Restart node 1
# Node 1 should rejoin as non-watcher
```

## Related Documentation

- [RedisMempool Example](../../examples/redis-mempool/): Docker Compose demo
- [EphemeralMempool](../core/src/builtin/mempool.rs): Single-node in-memory
- [RedbMempool](../redb3/src/mempool.rs): Single-node persistent
- [MempoolStore Trait](../core/src/mempool.rs): Common interface

## License

Same as Dolos (Apache-2.0)
