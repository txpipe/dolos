# Fjall Index Store

This module implements the `IndexStore` trait using [Fjall](https://github.com/fjall-rs/fjall). What is left of it is the cursor.

## Where the indexes went

The index store used to hold three keyspaces. Each was a projection of data another store owns, and each now lives beside what it projects, written in the same atomic batch:

| Former keyspace | Now in | Module | Why |
|---|---|---|---|
| `state-tags` (live-UTxO tags) | state store | `../state/tags.rs` | a projection of the UTxO set; mutable, high churn |
| `archive-tags` (block tags) | archive store | `../archive/tags.rs` | a projection of the block history; append-only |
| `index-exact` (hash/number → slot) | archive store | `../archive/exact.rs` | a projection of the block history; append-only |

The keyspaces kept their names, key encodings, dimension hashing and compaction settings; what changed is which database journal and write batch they live under. The `indexes` stele layer — the sorted output of `ArchiveStore::iter_archive_tags` then `iter_exact_records`, and the input of `ArchiveWriter::append_prehashed` — is byte-identical whichever store produced it. See `../archive/mod.rs` for the read and write surface, and `../archive/scan.rs` for the prefix walk both traversals share.

## Keyspace Layout

| Keyspace | Name | Purpose | Access Pattern |
|----------|------|---------|----------------|
| 1 | `index-cursor` | Chain position tracking | Single key read/write |

```
Key:   [0x00]
Value: bincode-serialized ChainPoint
```

The cursor is placed by `IndexWriter::apply(&IndexDelta { cursor })` and read by the bootstrap catch-up and `dolos data check`. Removing it, and the store with it, is the next step of the dissolution.

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
- `dim_prefix::UTXO = "utxo"` - for the live-UTxO tag dimensions (state store)
- `dim_prefix::BLOCK = "block"` - for archive tag dimensions (archive store)
- `dim_prefix::EXACT = "exact"` - for exact lookup dimensions (archive store)

`hash("utxo:address") != hash("block:address")`, so a dimension name can be used by more than one index type without a collision.
