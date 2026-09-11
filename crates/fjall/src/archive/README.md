# Fjall Archive Store

This module implements the `ArchiveStore` trait using [Fjall](https://github.com/fjall-rs/fjall). Block bodies live in flat segment files (`dolos-flatfiles`); everything that points into them lives in the LSM tree described here.

## Keyspace Layout

| Keyspace | Name | Purpose | Access Pattern |
|----------|------|---------|----------------|
| 1 | `archive-blocks` | Slot → packed block locations | Point lookups and range scans by slot |
| 2 | `archive-logs` | All log namespaces | Range scans within a namespace |
| 3 | `archive-tags` | Block tags, append-only | Prefix scans by dimension and key |
| 4 | `index-exact` | Block hash / block number / tx hash → slot | Point lookups |
| 5 | `archive-stake-log` | Stake credential → addresses by first appearance | Prefix scans by stake, from either end |

The last three are projections of the blocks. Keeping them here lets the history
and its lookups commit in the same batch. All three keyspaces use
`l0_threshold = 8` and `memtable_size_mb = 128`.

## Key Schemas

### Blocks (`archive-blocks`)

```
Key:   [slot:8]
Value: packed 16-byte BlockLocations, newest first
```

Byte-identical to the redb blocks table, the multi-location case included (a Byron epoch-boundary block shares its slot with the first main block of the epoch it opens).

### Logs (`archive-logs`)

```
Key:   [ns_hash:8][log_key:40]  (48 bytes)
Value: CBOR-encoded entity value
```

One keyspace rather than one per namespace, mirroring the state store's entities keyspace: per-namespace LSM trees blow the file-descriptor limit during heavy compaction.

### Archive tags (`archive-tags`)

```
Key:   [dim_hash:8][key_hash:8][slot:8]  (24 bytes)
Value: (empty)
```

`key_hash` is `dolos_core::key_hash`, not a hash this crate decides: two backends that decided it differently would exchange records neither could then look up. For every dimension but `metadata` it is `xxh3_64(logical key)` big-endian; `metadata`'s logical key is already a `u64` label and is kept verbatim. The logical key is not recoverable from the store, which is why `TagRecord` carries the stored hash and nothing else.

### Exact lookups (`index-exact`)

| Kind | Key | Value |
|------|-----|-------|
| Block hash | `[dim_hash:8][hash:32]` | `[slot:8]` |
| Tx hash | `[dim_hash:8][hash:32]` | `[slot:8]` |
| Block number | `[dim_hash:8][num:8]` | `[slot:8]` |

Exact keys are stored verbatim and fixed-width per kind, so these records are lossless — `ExactRecord::new` is the single width-validation site.

### Stake address log (`archive-stake-log`)

Three entry shapes, discriminated by a tag byte:

| Entry | Key | Value |
|-------|-----|-------|
| Pair | `[0x00][stake_len:1][stake][address]` | `[slot:8][order:4]` |
| Ordered | `[0x01][stake_len:1][stake][slot:8][order:4][address]` | (empty) |
| Ready marker | `[0xff]` | `[1]` |

Each `(stake, address)` pair is stored once, at its first on-chain appearance;
`order` is the transaction index in the high 16 bits and the output index in
the low 16. The pair entry is the write-path probe and the undo key; the
ordered entry is what a page read walks, from either end. Full address bytes
are stored so pointer addresses round-trip.

Genesis bootstrap writes the ready marker. Until it exists
`addresses_by_stake_log` answers `None`, because a store restored from a stele
or synced before the log existed holds an incomplete log, and callers fall back
to an archive scan.

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
- `dim_prefix::UTXO = "utxo"` — live-UTxO tag dimensions (the state store)

`hash("utxo:address") != hash("block:address")`, so a dimension name can be used by more than one index type without a collision.

## Stelae

The `indexes` stele layer is the sorted output of
`ArchiveStore::iter_archive_tags` followed by `iter_exact_records`, and the
input of `ArchiveWriter::append_prehashed`. See `scan.rs` for the prefix walk
both traversals share.

## Pruning

`prune_history` sweeps `archive-tags` and `index-exact` (see `mod.rs`). It does not touch `archive-stake-log`: its entries are first appearances, so removing one below the cutoff would drop an address the account may still use. `truncate_front` touches none of the three; a rollback removes their entries through `ArchiveWriter::undo_index`.
