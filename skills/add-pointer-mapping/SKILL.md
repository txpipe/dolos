---
name: add-pointer-mapping
description: Add a missing pointer address mapping to hacks.rs by looking up the stake credential in DBSync
---

# Add Pointer Address Mapping

Cardano pointer addresses reference a stake credential by the location of its registration certificate: `(slot, tx_idx, cert_idx)`. Dolos resolves these via a hardcoded lookup table in `crates/cardano/src/hacks.rs` (`pointers::pointer_to_cred`). When a new unmapped pointer is encountered, dolos panics with `"missing pointer mapping"`.

## Step 1: Get the pointer coordinates

The user provides or the panic log shows three values: `slot`, `tx_idx`, `cert_idx`.

## Step 2: Query DBSync for the stake credential

Get the DBSync connection URL from `xtask.toml` (`[dbsync]` section), then run:

```sql
SELECT sa.view,
       substring(encode(sa.hash_raw, 'hex') from 3) AS cred_hash
FROM stake_registration sr
JOIN tx t ON t.id = sr.tx_id
JOIN block b ON b.id = t.block_id
JOIN stake_address sa ON sa.id = sr.addr_id
WHERE b.slot_no = <SLOT> AND t.block_index = <TX_IDX> AND sr.cert_index = <CERT_IDX>;
```

**Interpreting the result:**

- **No rows returned**: The pointer is invalid (no registration at that location). Map it to `None`.
- **`stake1u...` prefix** (key hash): Use `StakeCredential::AddrKeyhash("<cred_hash>")`.
- **`stake1s...` prefix** (script hash): Use `StakeCredential::ScriptHash("<cred_hash>")`.
- The `cred_hash` column strips the 1-byte header (`e0`/`e1`/`f0`/`f1`) from `hash_raw`, yielding the raw 28-byte credential hash.

## Step 3: Determine the network

Use the slot number to figure out which network section to place the entry near:

- **Preview**: small slot numbers, or check if it matches preview DBSync
- **Preprod**: medium slot numbers
- **Mainnet**: large slot numbers (slots > ~4M are post-Byron mainnet)

The entries in hacks.rs are loosely grouped by network with comments.

## Step 4: Add the mapping to hacks.rs

Edit `crates/cardano/src/hacks.rs`, function `pointer_to_cred`. Insert the new arm **before** the catch-all panic at the bottom.

**For a valid credential (has a row in DBSync):**

```rust
(SLOT, TX_IDX, CERT_IDX) => Some(StakeCredential::AddrKeyhash(
    "CRED_HASH"
        .parse()
        .unwrap(),
)),
```

**For an invalid/garbage pointer (no row in DBSync):**

```rust
(SLOT, TX_IDX, CERT_IDX) => None,
```

## Step 5: Verify

```bash
cargo check -p dolos-cardano
```

## Reference: How pointer addresses work

A pointer address encodes `(slot, tx_idx, cert_idx)` as variable-length integers in the address bytes. This triple uniquely identifies a stake registration certificate on-chain. The Cardano node resolves the pointer to the stake credential registered at that location.

Some pointers are intentionally invalid (garbage values like `(12, 12, 12)`) — these appear in on-chain addresses but point to nonexistent registrations. They map to `None`, meaning the address has no stake rights.

## Reference: DBSync tables

| Table | Join | Purpose |
|-------|------|---------|
| `stake_registration` | primary | Certificate registrations, has `cert_index` |
| `tx` | `sr.tx_id = t.id` | Transaction, has `block_index` (= tx_idx within block) |
| `block` | `t.block_id = b.id` | Block, has `slot_no` |
| `stake_address` | `sr.addr_id = sa.id` | Stake address, has `view` (bech32) and `hash_raw` (with header byte) |
