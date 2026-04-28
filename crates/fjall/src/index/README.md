# Fjall Index Store

This module implements the `IndexStore` trait using [Fjall](https://github.com/fjall-rs/fjall), an LSM-tree based embedded database optimized for write-heavy workloads.

## Design Philosophy

The index store is organized into **4 keyspaces** based on workload class. Each keyspace maps to its own physical LSM-tree, which means it gets independent compaction, memtables, and I/O pipelines.

### Why separate keyspaces by workload?

The index has two fundamentally different write patterns:

- **State tags** (UTxO tags) are mutable. Entries are inserted when a UTxO is produced and deleted when it's consumed. This creates high churn with many tombstones that need compaction cleanup.
- **Archive tags** (block tags) are append-only. Entries are written once during block indexing and never deleted. They grow monotonically with chain height.

Mixing these in a single LSM-tree causes problems:

1. **Write amplification** - When L0 SSTs are compacted into deeper levels, a flush triggered by high-volume state tag writes forces rewriting co-located archive tag entries (and vice versa). Separating them means each tree only compacts its own data.

2. **Compaction strategy mismatch** - State tags need aggressive leveled compaction to reclaim space from tombstones. Archive tags could use larger memtables and less aggressive compaction since there are no deletions. Separate keyspaces allow independent tuning.

3. **I/O contention** - A single large keyspace creates a single compaction pipeline that can bottleneck. With separate keyspaces, compaction runs independently per tree, which benefits SSD workloads that handle concurrent I/O well.

4. **Read amplification** - Prefix scans for UTxO lookups no longer traverse SSTs containing unrelated archive data, and vice versa.

### Chain-agnostic storage

The storage layer has **no knowledge of blockchain-specific concepts** like "address", "policy", or "stake". Instead:

1. **Dimension strings are hashed** - Any dimension string (e.g., "address", "policy") is hashed using xxh3 to produce an 8-byte prefix.

2. **Internal prefixes distinguish types** - To prevent collisions between different index types with the same dimension name, internal prefixes are prepended before hashing:
   - `"utxo:"` for state tag dimensions
   - `"block:"` for archive tag dimensions
   - `"exact:"` for exact lookup dimensions

3. **Example**: `hash("utxo:address")` != `hash("block:address")` - no collisions even with same dimension name.

This allows the chain logic layer (dolos-cardano) to define any dimensions it needs without requiring changes to the storage layer.

## Keyspace Layout

| Keyspace | Name | Purpose | Access Pattern | Mutability |
|----------|------|---------|----------------|------------|
| 1 | `index-cursor` | Chain position tracking | Single key read/write | Overwrite |
| 2 | `index-exact` | Hash/number -> slot lookups | Point queries | Append-only |
| 3 | `state-tags` | UTxO tag indexes (address, policy, etc.) | Prefix scans | Insert + delete |
| 4 | `archive-tags` | Block tag indexes (historical lookups) | Prefix scans | Append-only |

All four keyspaces participate in a single atomic write batch per block, so cross-keyspace consistency is guaranteed by Fjall's `OwnedWriteBatch`.

## Key Schemas

### Cursor Keyspace (`index-cursor`)

Single entry storing the current chain position:

```
Key:   [0x00]
Value: bincode-serialized ChainPoint
```

### Exact Keyspace (`index-exact`)

Point lookups for block/transaction identification:

```
Key:   [dim_hash:8][key_data:var]
Value: [slot:8]
```

Where `dim_hash = xxh3("exact:" + dimension)`.

| Dimension | Qualified Name | Key Data |
|-----------|---------------|----------|
| Block Hash | `exact:block_hash` | 32-byte hash |
| Block Number | `exact:block_num` | 8-byte big-endian u64 |
| Tx Hash | `exact:tx_hash` | 32-byte hash |

### State Tags Keyspace (`state-tags`)

Current UTxO set indexes for fast lookups by address, policy, etc:

```
Key:   [dim_hash:8][lookup_key:var][txo_ref:36]
Value: (empty)
```

Where `dim_hash = xxh3("utxo:" + dimension)`.

The `txo_ref` is 36 bytes: 32-byte tx hash + 4-byte big-endian output index.

Entries are inserted when a UTxO is produced and removed when consumed.

### Archive Tags Keyspace (`archive-tags`)

Historical indexes for finding blocks containing specific data:

```
Key:   [dim_hash:8][xxh3(tag_key):8][slot:8]
Value: (empty)
```

Where `dim_hash = xxh3("block:" + dimension)`.

The `xxh3(tag_key)` is an 8-byte hash of the original key data, providing approximate matching with compact keys. The `slot` is 8-byte big-endian for correct lexicographic ordering.

Entries are append-only and never deleted.

## Dimension Hashing

The `hash_dimension()` function in `keys.rs` computes dimension hashes:

```rust
pub fn hash_dimension(prefix: &str, dim: &str) -> [u8; 8] {
    let mut hasher = Xxh3::new();
    hasher.write(prefix.as_bytes());
    hasher.write(b":");
    hasher.write(dim.as_bytes());
    hasher.finish().to_be_bytes()
}
```

Internal prefix constants:
- `dim_prefix::UTXO = "utxo"` - for state tag dimensions
- `dim_prefix::BLOCK = "block"` - for archive tag dimensions
- `dim_prefix::EXACT = "exact"` - for exact lookup dimensions

## Module Structure

```
index/
├── mod.rs              # IndexStore, keyspace management, trait impls
├── exact.rs            # index-exact keyspace (key encoding + operations + queries)
├── state_tags.rs       # state-tags keyspace (key encoding + operations + queries)
├── archive_tags.rs     # archive-tags keyspace (key encoding + operations + SlotIterator)
└── README.md
```

## Encoding Conventions

- **All integers are big-endian** for correct lexicographic ordering in LSM-tree scans
- **Dimension hashes are 8 bytes** (xxh3 truncated to big-endian u64)
- **Variable-length keys** are concatenated directly (no length prefixes needed since we scan by prefix)
- **Empty values** for tag entries - presence of key is sufficient

## Query Patterns

### Point Lookups (Exact Keyspace)

```rust
// Find slot by block hash
let slot = store.slot_by_block_hash(&hash)?;

// Find slot by tx hash
let slot = store.slot_by_tx_hash(&tx_hash)?;

// Find slot by block number
let slot = store.slot_by_block_number(12345)?;
```

### UTxO Queries (State Tags Keyspace)

```rust
// Find all UTxOs for an address (dimension string passed directly)
let utxos = store.utxos_by_tag("address", &addr_bytes)?;

// Find all UTxOs for a policy
let utxos = store.utxos_by_tag("policy", &policy_id)?;
```

### Historical Queries (Archive Tags Keyspace)

```rust
// Find slots where address appeared (within range)
let slots = store.slots_by_tag("address", &addr_bytes, start_slot, end_slot)?;

// Find slots with specific metadata label
let slots = store.slots_by_tag("metadata", &label_bytes, start_slot, end_slot)?;
```

## Performance Considerations

1. **Snapshot reads** - All read operations use MVCC snapshots to avoid blocking concurrent writes.

2. **Batched writes** - The `IndexWriter` accumulates changes and commits atomically across all four keyspaces.

3. **Independent compaction** - State tags and archive tags compact on their own schedules, preventing one workload from stalling the other.

4. **Flush on commit** - Configurable journal flushing prevents unbounded memory growth during bulk imports.

5. **Graceful shutdown** - Call `shutdown()` before dropping to ensure all background work completes.
