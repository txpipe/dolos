//! Exact-match index operations for the `index-exact` keyspace (chain-agnostic).
//!
//! This module handles key encoding, batch writes (apply/undo), and read queries
//! for the `index-exact` keyspace:
//! - Block hash -> slot
//! - Transaction hash -> slot
//! - Block number -> slot
//!
//! ## Key Format
//!
//! | Type | Key Format | Value |
//! |------|------------|-------|
//! | Block Hash | `[dim_hash:8][hash:32]` | `[slot:8]` |
//! | Tx Hash | `[dim_hash:8][hash:32]` | `[slot:8]` |
//! | Block Number | `[dim_hash:8][num:8]` | `[slot:8]` |
//!
//! The `dim_hash` is computed as `xxh3("exact:" + dimension)`.

use dolos_core::{ArchiveIndexDelta, BlockSlot, IndexDelta};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use crate::keys::{decode_slot, dim_prefix, encode_slot, hash_dimension, DIM_HASH_SIZE, SLOT_SIZE};
use crate::Error;

// ============================================================================
// Internal Dimension Names
// ============================================================================

/// Internal dimension name for block hash lookups
const DIM_BLOCK_HASH: &str = "block_hash";

/// Internal dimension name for block number lookups
const DIM_BLOCK_NUM: &str = "block_num";

/// Internal dimension name for transaction hash lookups
const DIM_TX_HASH: &str = "tx_hash";

// ============================================================================
// Key Encoding
// ============================================================================

/// Build an exact lookup key: `[dim_hash:8][key_data:var]`
fn build_exact_key(dimension: &str, key_data: &[u8]) -> Vec<u8> {
    let dim_hash = hash_dimension(dim_prefix::EXACT, dimension);
    let mut key = Vec::with_capacity(DIM_HASH_SIZE + key_data.len());
    key.extend_from_slice(&dim_hash);
    key.extend_from_slice(key_data);
    key
}

/// Build an exact lookup key for block number: `[dim_hash:8][blocknum:8]`
fn build_exact_key_blocknum(block_number: u64) -> [u8; DIM_HASH_SIZE + 8] {
    let dim_hash = hash_dimension(dim_prefix::EXACT, DIM_BLOCK_NUM);
    let mut key = [0u8; DIM_HASH_SIZE + 8];
    key[..DIM_HASH_SIZE].copy_from_slice(&dim_hash);
    key[DIM_HASH_SIZE..].copy_from_slice(&block_number.to_be_bytes());
    key
}

/// Encode slot as value for exact lookups
fn encode_slot_value(slot: u64) -> [u8; SLOT_SIZE] {
    encode_slot(slot)
}

/// Decode slot from value
fn decode_slot_value(value: &[u8]) -> u64 {
    decode_slot(value)
}

// ============================================================================
// Block Processing
// ============================================================================

/// Apply exact indexes for a single block delta
fn apply_block(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    let slot = block.slot;

    // Exact lookup: block hash -> slot
    if !block.block_hash.is_empty() {
        let key = build_exact_key(DIM_BLOCK_HASH, &block.block_hash);
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    // Exact lookup: block number -> slot
    if let Some(number) = block.block_number {
        let key = build_exact_key_blocknum(number);
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    // Exact lookup: tx hashes -> slot
    for tx_hash in &block.tx_hashes {
        let key = build_exact_key(DIM_TX_HASH, tx_hash.as_slice());
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    Ok(())
}

/// Undo exact indexes for a single block delta (rollback)
fn undo_block(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    if !block.block_hash.is_empty() {
        let key = build_exact_key(DIM_BLOCK_HASH, &block.block_hash);
        batch.remove(exact_keyspace, key);
    }

    if let Some(number) = block.block_number {
        let key = build_exact_key_blocknum(number);
        batch.remove(exact_keyspace, key);
    }

    for tx_hash in &block.tx_hashes {
        let key = build_exact_key(DIM_TX_HASH, tx_hash.as_slice());
        batch.remove(exact_keyspace, key);
    }

    Ok(())
}

/// Apply exact indexes from an IndexDelta
pub fn apply(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    for block in &delta.archive {
        apply_block(batch, exact_keyspace, block)?;
    }
    Ok(())
}

/// Undo exact indexes from an IndexDelta (rollback)
pub fn undo(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    for block in delta.archive.iter().rev() {
        undo_block(batch, exact_keyspace, block)?;
    }
    Ok(())
}

// ============================================================================
// Queries
// ============================================================================

/// Get slot by block hash (exact lookup).
pub fn get_by_block_hash<R: Readable>(
    readable: &R,
    exact_keyspace: &Keyspace,
    block_hash: &[u8],
) -> Result<Option<BlockSlot>, Error> {
    let key = build_exact_key(DIM_BLOCK_HASH, block_hash);
    match readable.get(exact_keyspace, key).map_err(Error::Fjall)? {
        Some(value) => {
            let slot = decode_slot_value(value.as_ref());
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

/// Get slot by block number (exact lookup).
pub fn get_by_block_number<R: Readable>(
    readable: &R,
    exact_keyspace: &Keyspace,
    number: u64,
) -> Result<Option<BlockSlot>, Error> {
    let key = build_exact_key_blocknum(number);
    match readable.get(exact_keyspace, key).map_err(Error::Fjall)? {
        Some(value) => {
            let slot = decode_slot_value(value.as_ref());
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

/// Get slot by tx hash (exact lookup).
pub fn get_by_tx_hash<R: Readable>(
    readable: &R,
    exact_keyspace: &Keyspace,
    tx_hash: &[u8],
) -> Result<Option<BlockSlot>, Error> {
    let key = build_exact_key(DIM_TX_HASH, tx_hash);
    match readable.get(exact_keyspace, key).map_err(Error::Fjall)? {
        Some(value) => {
            let slot = decode_slot_value(value.as_ref());
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_key_block_hash() {
        let block_hash = [0xcd; 32];
        let key = build_exact_key(DIM_BLOCK_HASH, &block_hash);

        // First 8 bytes are dim_hash, rest is key_data
        assert_eq!(key.len(), DIM_HASH_SIZE + 32);
        assert_eq!(&key[DIM_HASH_SIZE..], &block_hash);
    }

    #[test]
    fn test_exact_key_tx_hash() {
        let tx_hash = [0xab; 32];
        let key = build_exact_key(DIM_TX_HASH, &tx_hash);

        assert_eq!(key.len(), DIM_HASH_SIZE + 32);
        assert_eq!(&key[DIM_HASH_SIZE..], &tx_hash);
    }

    #[test]
    fn test_exact_key_blocknum() {
        let block_num = 12345678u64;
        let key = build_exact_key_blocknum(block_num);

        assert_eq!(key.len(), DIM_HASH_SIZE + 8);

        // Verify big-endian encoding of block number
        let decoded = u64::from_be_bytes(key[DIM_HASH_SIZE..].try_into().unwrap());
        assert_eq!(decoded, block_num);
    }

    #[test]
    fn test_slot_value_roundtrip() {
        let slot = 141868807u64;
        let encoded = encode_slot_value(slot);
        let decoded = decode_slot_value(&encoded);
        assert_eq!(slot, decoded);
    }

    #[test]
    fn test_dimension_separation() {
        // Ensure keys from different dimensions don't overlap
        let hash = [0xab; 32];
        let block_key = build_exact_key(DIM_BLOCK_HASH, &hash);
        let tx_key = build_exact_key(DIM_TX_HASH, &hash);

        // First 8 bytes (dim_hash) should be different
        assert_ne!(&block_key[..DIM_HASH_SIZE], &tx_key[..DIM_HASH_SIZE]);
    }

    #[test]
    fn test_any_dimension_works() {
        // Any dimension string should work (chain-agnostic)
        let key = build_exact_key("custom_lookup", &[0x11; 20]);
        assert_eq!(key.len(), DIM_HASH_SIZE + 20);
    }
}
