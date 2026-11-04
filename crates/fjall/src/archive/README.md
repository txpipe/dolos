# Fjall Archive Store

This module implements the `ArchiveStore` trait using [Fjall](https://github.com/fjall-rs/fjall). Block bodies live in flat segment files (`dolos-flatfiles`); everything that points into them lives in the LSM tree described here.

## Keyspace Layout

| Keyspace | Name | Purpose | Access Pattern |
|----------|------|---------|----------------|
| 1 | `archive-blocks` | Slot → packed block locations | Point lookups and range scans by slot |
| 2 | `archive-logs` | All log namespaces | Range scans within a namespace |
| 3 | `archive-tags` | Block tags, append-only | Prefix scans by dimension and key |
| 4 | `index-exact` | Block hash / block number / tx hash → slot | Point lookups |
| 5 | `index-entity` | Entity id → the slot it was first seen at | Batched point lookups |
| 6 | `archive-meta` | The store's compatibility identity | Single key read/write |

`archive-tags` and `index-exact` are projections of the blocks. Keeping them
here lets the history and its lookups commit in the same batch. `index-entity`
is written by the seal of an immutable epoch, with the epoch's sealed logs, and
nothing but a prune removes an entry. The index keyspaces take the preset's
index tuning (L0 threshold 8, 128 MiB memtable) rather than the 4 and 64 MiB of
the others.

## Key Schemas

### Blocks (`archive-blocks`)

```
Key:   [slot:8]
Value: packed 16-byte BlockLocations, newest first
```

Byte-identical to the redb blocks table, the multi-location case included (a Byron epoch-boundary block shares its slot with the first main block of the epoch it opens).

### Logs (`archive-logs`)

```
Key:   [ns_hash:8][slot:8][key:var]
Value: CBOR-encoded entity value
```

The part after the namespace hash is core's `LogKey`: the big-endian slot, then the log's typed key, of whatever length its kind has (decision 0051). Prune and truncate cut at `[ns_hash:8][slot:8]`, so every key at a slot goes the same way whatever follows the slot. An open range ends at the successor of the namespace hash, and a stored key too short to hold its slot is an error from the iterator.

One keyspace rather than one per namespace, mirroring the state store's entities keyspace: per-namespace LSM trees blow the file-descriptor limit during heavy compaction.

### Archive tags (`archive-tags`)

```
Key:   [dim_hash:8][key_hash:8][slot:8]  (24 bytes)
Value: (empty)
```

`key_hash` is `dolos_core::key_hash`, not a hash this crate decides: two backends that decided it differently would exchange records neither could then look up. For every dimension it is `xxh3_64(logical key)` big-endian. The logical key is not recoverable from the store, which is why `TagRecord` carries the stored hash and nothing else.

### Store metadata (`archive-meta`)

```
Key:   "identity"
Value: UTF-8 contract identifier, e.g. "v2-alpha.2"
```

The compatibility identity the store recorded when it was created, which the
node's store opener checks before anything else (see the data layer docs).

### Exact lookups (`index-exact`)

| Kind | Key | Value |
|------|-----|-------|
| Block hash | `[dim_hash:8][hash:32]` | `[slot:8]` |
| Tx hash | `[dim_hash:8][hash:32]` | `[slot:8]` |
| Block number | `[dim_hash:8][num:8]` | `[slot:8]` |

Exact keys are stored verbatim and fixed-width per kind, so these records are lossless — `ExactRecord::new` is the single width-validation site.

### Entity index (`index-entity`)

```
Key:   [kind_hash:8][id:kind width]
Value: [slot:8]
```

The chain declares the entity kinds and core names none. `kind_hash` is the dimension hash of the kind name under the `entity` prefix, and ids are stored verbatim at the kind's width; `check_entity_key` refuses any other width on writes and reads.

## Dimension Hashing

`hash_dimension()` in `keys.rs` computes the dimension hashes every index keyspace uses:

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
- `dim_prefix::BLOCK = "block"` — archive tag dimensions (this store)
- `dim_prefix::EXACT = "exact"` — exact lookup dimensions (this store)
- `dim_prefix::ENTITY = "entity"` — entity index kinds (this store)
- `dim_prefix::UTXO = "utxo"` — live-UTxO tag dimensions (the state store)

`hash("utxo:address") != hash("block:address")`, so a dimension name can be used by more than one index type without a collision.

## Stelae

The `indexes` stele layer is the sorted output of
`ArchiveStore::iter_archive_tags` followed by `iter_exact_records`, and the
input of `ArchiveWriter::append_prehashed`. The `entity-{kind}` layers are
`ArchiveStore::iter_entity_records` per kind, restored through
`ArchiveWriter::write_entity`. See `scan.rs` for the prefix walk the traversals
share.

## Pruning

The slot is the last key component of a tag entry and the value of an exact or entity entry, so `prune_history` sweeps the index keyspaces in resumable chunks rather than range-deleting them by slot. `truncate_front` touches none of them. A rollback removes tag and exact entries through `ArchiveWriter::undo_index`; nothing undoes an entity entry.
