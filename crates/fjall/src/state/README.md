# Fjall State Store

This module implements the `StateStore` trait using [Fjall](https://github.com/fjall-rs/fjall), an LSM-tree based embedded database optimized for write-heavy workloads.

## Design Philosophy

The state store is organized into **3 keyspaces** based on access patterns, rather than one keyspace per entity type. This design:

1. **Reduces file descriptor usage** - LSM-trees create many segment files during compaction. Having 13+ separate keyspaces (one per entity namespace) caused "too many open files" errors during heavy imports.

2. **Simplifies API** - No schema parameter needed at open time since namespaces are hashed dynamically.

3. **Supports extensibility** - New entity types can be added without code changes; namespace strings are hashed to generate prefixes.

## Keyspace Layout

| Keyspace | Name | Purpose | Access Pattern |
|----------|------|---------|----------------|
| 1 | `state-cursor` | Chain position tracking | Single key read/write |
| 2 | `state-utxos` | UTxO set storage | Point lookups by TxoRef |
| 3 | `state-entities` | All entity types | Point lookups and range scans |

## Key Schemas

### Cursor Keyspace (`state-cursor`)

Single entry storing the current chain position:

```
Key:   [0x00]
Value: bincode-serialized ChainPoint
```

### UTxO Keyspace (`state-utxos`)

UTxO set indexed by transaction output reference:

```
Key:   [tx_hash:32][output_index:4]  (36 bytes)
Value: [era:2][cbor:...]
```

- `tx_hash`: 32-byte transaction hash
- `output_index`: 4-byte big-endian u32
- `era`: 2-byte big-endian u16 (Cardano era identifier)
- `cbor`: CBOR-encoded UTxO data

### Entities Keyspace (`state-entities`)

All entity types share a single keyspace with namespace hash prefixes:

```
Key:   [ns_hash:8][entity_key:32]  (40 bytes)
Value: CBOR-encoded entity data
```

- `ns_hash`: 8-byte xxh3 hash of namespace string (e.g., "accounts", "pools")
- `entity_key`: 32-byte entity key (typically a hash or structured identifier)

#### Namespace Hashing

Namespaces are hashed using xxh3 to generate deterministic 8-byte prefixes:

```rust
use xxhash_rust::xxh3::xxh3_64;

fn hash_namespace(ns: &str) -> [u8; 8] {
    xxh3_64(ns.as_bytes()).to_be_bytes()
}
```

This approach:
- Provides deterministic IDs without a hardcoded mapping table
- Allows new entity types to be added without code changes
- Groups entities by namespace for efficient range scans

#### Known Namespaces

| Namespace | Description |
|-----------|-------------|
| `accounts` | Stake account state (controlled amount, delegation, etc.) |
| `pools` | Stake pool state (parameters, pledge, blocks minted) |
| `epochs` | Epoch state (nonces, parameters) |
| `dreps` | DRep state (delegation, activity) |
| `proposals` | Governance proposal state |
| `assets` | Asset mint statistics |
| `datums` | Datum reference counts |
| `eras` | Era summary information |
| `rewards` | Reward log entries |
| `stakes` | Stake log entries |
| `pending_rewards` | Pending reward state |

## Module Structure

```
state/
├── mod.rs          # StateStore implementation, keyspace management
├── entity_keys.rs  # Namespace hashing and entity key encoding
├── entities.rs     # Entity read/write operations
├── utxos.rs        # UTxO set operations
└── README.md       # This file
```

## Encoding Conventions

- **All integers are big-endian** for correct lexicographic ordering in LSM-tree scans
- **Entity values are CBOR-encoded** using the minicbor library
- **Cursor is bincode-encoded** for efficient serialization of ChainPoint

## Query Patterns

### Point Lookups

```rust
// Read entities by keys within a namespace
let values = store.read_entities("accounts", &[&key1, &key2])?;

// Get UTxOs by reference
let utxos = store.get_utxos(vec![txo_ref1, txo_ref2])?;
```

### Range Iteration

```rust
// Iterate entities within a key range (scoped to namespace)
let iter = store.iter_entities("pools", start_key..end_key)?;
for result in iter {
    let (key, value) = result?;
    // Process entity
}
```

### Batched Writes

```rust
let writer = store.start_writer()?;
writer.write_entity("accounts", &key, &value)?;
writer.delete_entity("pools", &key)?;
writer.apply_utxoset(&delta)?;
writer.set_cursor(chain_point)?;
writer.commit()?;
```

## Performance Considerations

1. **Snapshot reads** - All read operations use MVCC snapshots to avoid blocking concurrent writes.

2. **Batched writes** - The `StateWriter` accumulates changes and commits atomically.

3. **Flush on commit** - Configurable journal flushing prevents unbounded memory growth during bulk imports.

4. **Graceful shutdown** - Call `shutdown()` before dropping to ensure all background work completes.

## API Differences from Redb Backend

| Feature | Fjall | Redb |
|---------|-------|------|
| Schema parameter | Not required | Required |
| Entity keyspaces | Unified with hash prefix | Separate per namespace |
| Multimap support | Not supported | Supported |

## Migration Notes

This 3-keyspace design is **not backward compatible** with previous versions that used separate keyspaces per entity type. Users must recreate their state databases when upgrading.

The removal of the schema parameter from `StateStore::open()` is also a breaking API change.
