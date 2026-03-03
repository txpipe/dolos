# Fjall Index Store

This module implements the `IndexStore` trait using [Fjall](https://github.com/fjall-rs/fjall), an LSM-tree based embedded database optimized for write-heavy workloads.

## Design Philosophy

The index store is organized into **3 keyspaces** based on access patterns, rather than one keyspace per index type. This design:

1. **Reduces file descriptor usage** - LSM-trees create many segment files during compaction. Having 19+ separate keyspaces caused "too many open files" errors during heavy imports.

2. **Isolates access patterns** - Exact-match lookups (point queries) are separated from tag-based lookups (prefix scans), allowing for potential future tuning.

3. **Chain-agnostic storage** - Dimension strings are hashed to 8-byte prefixes, making the storage layer completely independent of blockchain-specific concepts.

## Chain-Agnostic Design

The storage layer has **no knowledge of blockchain-specific concepts** like "address", "policy", or "stake". Instead:

1. **Dimension strings are hashed** - Any dimension string (e.g., "address", "policy") is hashed using xxh3 to produce an 8-byte prefix.

2. **Internal prefixes distinguish types** - To prevent collisions between different index types with the same dimension name, internal prefixes are prepended before hashing:
   - `"utxo:"` for UTxO tag dimensions
   - `"block:"` for block tag dimensions
   - `"exact:"` for exact lookup dimensions

3. **Example**: `hash("utxo:address")` ≠ `hash("block:address")` - no collisions even with same dimension name.

This allows the chain logic layer (dolos-cardano) to define any dimensions it needs without requiring changes to the storage layer.

## Keyspace Layout

| Keyspace | Name | Purpose | Access Pattern |
|----------|------|---------|----------------|
| 1 | `index-cursor` | Chain position tracking | Single key read/write |
| 2 | `index-exact` | Hash/number → slot lookups | Point queries |
| 3 | `index-tags` | UTxO and block tag indexes | Prefix scans |

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

### Tags Keyspace (`index-tags`)

Prefix-scannable indexes for filtering. UTxO tags and block tags are differentiated by their dimension hash prefixes.

#### UTxO Tags

Current UTxO set indexes for fast lookups by address, policy, etc:

```
Key:   [dim_hash:8][lookup_key:var][txo_ref:36]
Value: (empty)
```

Where `dim_hash = xxh3("utxo:" + dimension)`.

The `txo_ref` is 36 bytes: 32-byte tx hash + 4-byte big-endian output index.

#### Block Tags

Historical indexes for finding blocks containing specific data:

```
Key:   [dim_hash:8][xxh3(tag_key):8][slot:8]
Value: (empty)
```

Where `dim_hash = xxh3("block:" + dimension)`.

The `xxh3(tag_key)` is an 8-byte hash of the original key data, providing approximate matching with compact keys. The `slot` is 8-byte big-endian for correct lexicographic ordering.

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
- `dim_prefix::UTXO = "utxo"` - for UTxO tag dimensions
- `dim_prefix::BLOCK = "block"` - for block tag dimensions
- `dim_prefix::EXACT = "exact"` - for exact lookup dimensions

## Module Structure

```
index/
├── mod.rs          # IndexStore implementation, keyspace management
├── exact_keys.rs   # Key encoding for index-exact keyspace
├── tag_keys.rs     # Key encoding for index-tags keyspace
├── history.rs      # Block tag operations (exact lookups + block tags)
├── utxos.rs        # UTxO tag operations
└── README.md       # This file
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

### UTxO Queries (Tags Keyspace)

```rust
// Find all UTxOs for an address (dimension string passed directly)
let utxos = store.utxos_by_tag("address", &addr_bytes)?;

// Find all UTxOs for a policy
let utxos = store.utxos_by_tag("policy", &policy_id)?;
```

### Historical Queries (Tags Keyspace)

```rust
// Find slots where address appeared (within range)
let slots = store.slots_by_tag("address", &addr_bytes, start_slot, end_slot)?;

// Find slots with specific metadata label
let slots = store.slots_by_tag("metadata", &label_bytes, start_slot, end_slot)?;
```

## Performance Considerations

1. **Snapshot reads** - All read operations use MVCC snapshots to avoid blocking concurrent writes.

2. **Batched writes** - The `IndexWriter` accumulates changes and commits atomically.

3. **Flush on commit** - Configurable journal flushing prevents unbounded memory growth during bulk imports.

4. **Graceful shutdown** - Call `shutdown()` before dropping to ensure all background work completes.

## Key Size Comparison

| Key Type | Old Size | New Size | Delta |
|----------|----------|----------|-------|
| UTxO Tag | 2 + var + 36 | 8 + var + 36 | +6 bytes |
| Block Tag | 2 + 8 + 8 = 18 | 8 + 8 + 8 = 24 | +6 bytes |
| Exact | 1 + var | 8 + var | +7 bytes |

The slight increase in key size is a worthwhile tradeoff for chain-agnostic storage.

## Migration Notes

This chain-agnostic design is **not backward compatible** with previous versions. Users must recreate their index databases when upgrading.
