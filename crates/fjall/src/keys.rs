//! Key encoding utilities for fjall index store.
//!
//! All multi-byte integers are encoded as big-endian to ensure correct
//! lexicographic ordering in the LSM tree.

use dolos_core::TxoRef;
use xxhash_rust::xxh3::xxh3_64;

/// Size of encoded TxoRef: 32-byte tx hash + 4-byte index
pub const TXO_REF_SIZE: usize = 36;

/// Size of encoded slot: 8-byte u64
pub const SLOT_SIZE: usize = 8;

/// Size of encoded hash key: 8-byte u64
pub const HASH_KEY_SIZE: usize = 8;

/// Encode a TxoRef as 36 bytes: tx_hash (32) + index_be (4)
pub fn encode_txo_ref(txo: &TxoRef) -> [u8; TXO_REF_SIZE] {
    let mut result = [0u8; TXO_REF_SIZE];
    result[..32].copy_from_slice(txo.0.as_ref());
    result[32..].copy_from_slice(&txo.1.to_be_bytes());
    result
}

/// Decode a TxoRef from 36 bytes
pub fn decode_txo_ref(bytes: &[u8]) -> TxoRef {
    debug_assert!(bytes.len() >= TXO_REF_SIZE);
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&bytes[..32]);
    let index = u32::from_be_bytes(bytes[32..36].try_into().unwrap());
    TxoRef(hash.into(), index)
}

/// Encode a slot as big-endian u64
pub fn encode_slot(slot: u64) -> [u8; SLOT_SIZE] {
    slot.to_be_bytes()
}

/// Decode a slot from big-endian bytes
pub fn decode_slot(bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() >= SLOT_SIZE);
    u64::from_be_bytes(bytes[..8].try_into().unwrap())
}

/// Encode a u64 as big-endian bytes
pub fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Decode a u64 from big-endian bytes
pub fn decode_u64(bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() >= 8);
    u64::from_be_bytes(bytes[..8].try_into().unwrap())
}

/// Build a composite key by concatenating prefix and suffix
pub fn composite_key(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + suffix.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(suffix);
    key
}

/// Build a composite key for UTxO indexes: lookup_key ++ txo_ref
pub fn utxo_composite_key(lookup_key: &[u8], txo: &TxoRef) -> Vec<u8> {
    let txo_bytes = encode_txo_ref(txo);
    composite_key(lookup_key, &txo_bytes)
}

/// Build a composite key for archive indexes: xxh3_hash ++ slot
pub fn archive_composite_key(hash: u64, slot: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&hash.to_be_bytes());
    key[8..].copy_from_slice(&slot.to_be_bytes());
    key
}

/// Extract slot from the end of a composite key (last 8 bytes)
pub fn decode_slot_from_suffix(key: &[u8]) -> u64 {
    debug_assert!(key.len() >= SLOT_SIZE);
    let start = key.len() - SLOT_SIZE;
    decode_slot(&key[start..])
}

/// Extract TxoRef from the end of a composite key (last 36 bytes)
pub fn decode_txo_ref_from_suffix(key: &[u8]) -> TxoRef {
    debug_assert!(key.len() >= TXO_REF_SIZE);
    let start = key.len() - TXO_REF_SIZE;
    decode_txo_ref(&key[start..])
}

/// Hash variable-length data to a fixed u64 using xxh3
pub fn hash_key(data: &[u8]) -> u64 {
    xxh3_64(data)
}

/// Build prefix for archive index queries (just the hash portion)
pub fn archive_prefix(hash: u64) -> [u8; HASH_KEY_SIZE] {
    hash.to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txo_ref_roundtrip() {
        let txo = TxoRef([0xab; 32].into(), 42);
        let encoded = encode_txo_ref(&txo);
        let decoded = decode_txo_ref(&encoded);
        assert_eq!(txo.0, decoded.0);
        assert_eq!(txo.1, decoded.1);
    }

    #[test]
    fn test_slot_roundtrip() {
        let slot = 141868807u64;
        let encoded = encode_slot(slot);
        let decoded = decode_slot(&encoded);
        assert_eq!(slot, decoded);
    }

    #[test]
    fn test_archive_key_ordering() {
        // Keys with same hash should be ordered by slot
        let hash = hash_key(b"test_address");
        let key1 = archive_composite_key(hash, 100);
        let key2 = archive_composite_key(hash, 200);
        let key3 = archive_composite_key(hash, 50);

        assert!(key1 < key2);
        assert!(key3 < key1);
    }

    #[test]
    fn test_decode_slot_from_suffix() {
        let hash = hash_key(b"test");
        let slot = 12345u64;
        let key = archive_composite_key(hash, slot);
        assert_eq!(decode_slot_from_suffix(&key), slot);
    }

    #[test]
    fn test_utxo_composite_key() {
        let address = b"addr_test1qz...";
        let txo = TxoRef([0xcd; 32].into(), 7);
        let key = utxo_composite_key(address, &txo);

        assert_eq!(key.len(), address.len() + TXO_REF_SIZE);
        assert_eq!(&key[..address.len()], address);

        let decoded_txo = decode_txo_ref_from_suffix(&key);
        assert_eq!(txo.0, decoded_txo.0);
        assert_eq!(txo.1, decoded_txo.1);
    }
}
