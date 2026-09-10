# Fjall State Store

This module implements the `StateStore` trait using [Fjall](https://github.com/fjall-rs/fjall), an LSM-tree based embedded database optimized for write-heavy workloads.

## Design Philosophy

The state store is organized into **4 keyspaces** based on access patterns, rather than one keyspace per entity type. This design:

1. **Reduces file descriptor usage** - LSM-trees create many segment files during compaction. Having 13+ separate keyspaces (one per entity namespace) caused "too many open files" errors during heavy imports.

2. **Simplifies API** - No schema parameter needed at open time since namespaces are hashed dynamically.

3. **Supports extensibility** - New entity types can be added without code changes; namespace strings are hashed to generate prefixes.

## Keyspace Layout

| Keyspace | Name | Purpose | Access Pattern |
|----------|------|---------|----------------|
| 1 | `state-cursor` | Chain position tracking | Single key read/write |
| 2 | `state-utxos` | UTxO set storage | Point lookups by TxoRef |
| 3 | `state-entities` | All entity types | Point lookups and range scans |
| 4 | `state-tags` | Live-UTxO tags | Prefix scans by dimension and lookup key |

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

### Tags Keyspace (`state-tags`)

The live-UTxO tags: which UTxOs answer under an address, payment credential, stake credential, policy id, asset id or reference script.

```
Key:   [dim_hash:8][lookup_key:var][txo_ref:36]
Value: (empty)
```

- `dim_hash`: `xxh3("utxo:" + dimension)` — see `keys.rs`, `hash_dimension`
- `lookup_key`: the logical key, stored verbatim so a prefix scan can find it
- `txo_ref`: `[tx_hash:32][index:4]`, the UTxO the tag points at

They are a projection of the UTxO set, so they are written in the same batch as the set (`StateWriter::apply_utxo_tags` / `undo_utxo_tags`) and read through `StateStore::utxos_by_tag`. Before v2 they lived in a separate index database; nothing about the key encoding changed with the move, only which journal, cache and write batch they live under.

Note the asymmetry with the archive's tags, which hash their key: here the lookup key is stored whole, because a live-UTxO query knows the key it is asking about and wants the exact refs back.

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
├── tags.rs         # Live-UTxO tag operations
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

### Full UTxO-Set Iteration

```rust
// Stream the whole UTxO set (snapshot export, live-UTxO index rebuild)
let iter = store.iter_utxos()?;
for result in iter {
    let (txo_ref, era_cbor) = result?;
    // Process UTxO
}
```

Lazy in the same sense `iter_entities` is: construction reads nothing and
entries are decoded one at a time. Callers stream a mainnet-sized set through
it, so buffering would not be a slower implementation but an unusable one —
`tests/memory.rs` asserts both the construction and the iteration bound.

### Batched Writes

```rust
let writer = store.start_writer()?;
writer.write_entity("accounts", &key, &value)?;
writer.delete_entity("pools", &key)?;
writer.apply_utxoset(&delta)?;
writer.apply_utxo_tags(&utxo_index_delta)?;
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

This 4-keyspace design is **not backward compatible** with previous versions that used separate keyspaces per entity type. Users must recreate their state databases when upgrading.

The removal of the schema parameter from `StateStore::open()` is also a breaking API change.
