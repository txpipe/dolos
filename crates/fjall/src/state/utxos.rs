//! UTxO table operations for fjall state store.
//!
//! UTxOs are stored with composite keys: `txhash[32] ++ idx[4]` (36 bytes)
//! Values are: `era[2] ++ cbor[...]`

use std::collections::HashMap;
use std::sync::Arc;

use dolos_core::{EraCbor, StateError, TxoRef, UtxoEntry, UtxoMap, UtxoSetDelta};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use crate::keys::{
    decode_txo_ref, decode_utxo_value, encode_txo_ref, encode_utxo_value, TXO_REF_SIZE,
};
use crate::Error;

/// Apply UTxO delta to the UTxO keyspace.
///
/// - Inserts produced and recovered UTxOs
/// - Removes consumed and undone UTxOs
pub fn apply_delta(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    delta: &UtxoSetDelta,
) -> Result<(), Error> {
    // Insert produced UTxOs
    for (txo_ref, era_cbor) in &delta.produced_utxo {
        let key = encode_txo_ref(txo_ref);
        let value = encode_utxo_value(era_cbor.0, &era_cbor.1);
        batch.insert(keyspace, key, value);
    }

    // Insert recovered UTxOs (rollback: restore previously consumed)
    for (txo_ref, era_cbor) in &delta.recovered_stxi {
        let key = encode_txo_ref(txo_ref);
        let value = encode_utxo_value(era_cbor.0, &era_cbor.1);
        batch.insert(keyspace, key, value);
    }

    // Remove consumed UTxOs
    for txo_ref in delta.consumed_utxo.keys() {
        let key = encode_txo_ref(txo_ref);
        batch.remove(keyspace, key);
    }

    // Remove undone UTxOs (rollback: remove previously produced)
    for txo_ref in delta.undone_utxo.keys() {
        let key = encode_txo_ref(txo_ref);
        batch.remove(keyspace, key);
    }

    Ok(())
}

/// Batch lookup UTxOs by refs.
///
/// Returns a map of found UTxOs. Missing UTxOs are silently skipped.
///
/// Uses the `Readable` trait to support both direct keyspace access and
/// snapshot-based reads. Snapshot-based reads avoid potential deadlocks with
/// concurrent writes.
pub fn get_utxos<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    refs: &[TxoRef],
) -> Result<UtxoMap, Error> {
    let mut result = HashMap::new();

    for txo_ref in refs {
        let key = encode_txo_ref(txo_ref);

        if let Some(value) = readable.get(keyspace, key).map_err(Error::Fjall)? {
            if let Some((era, cbor)) = decode_utxo_value(&value) {
                result.insert(txo_ref.clone(), Arc::new(EraCbor(era, cbor)));
            }
        }
    }

    Ok(result)
}

/// Lazy streaming iterator over every UTxO in the keyspace.
///
/// Wraps a `fjall::Iter`, an owned iterator holding a `SnapshotNonce` and a
/// boxed cursor. Entries are decoded one at a time in `next()`, so memory stays
/// O(1) regardless of how large the UTxO set is — the same shape as
/// `entities::EntityIterator`.
///
/// This matters: the callers this exists for (snapshot export, live-UTxO index
/// rebuild) run against a mainnet UTxO set of several GB, which buffering would
/// pull into memory whole.
pub struct UtxosIterator {
    inner: fjall::Iter,
}

impl UtxosIterator {
    /// Create a new iterator over all UTxOs.
    ///
    /// Uses the `Readable` trait to support snapshot-based iteration.
    /// Snapshot-based reads avoid potential deadlocks with concurrent writes by
    /// using MVCC (Multi-Version Concurrency Control).
    ///
    /// Returns immediately without reading any data.
    pub fn new<R: Readable>(readable: &R, keyspace: &Keyspace) -> Result<Self, Error> {
        Ok(Self {
            inner: readable.iter(keyspace),
        })
    }
}

impl Iterator for UtxosIterator {
    type Item = Result<UtxoEntry, StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        for guard in self.inner.by_ref() {
            // fjall's Guard::into_inner() gives us both key and value
            match guard.into_inner() {
                Ok((key_bytes, value_bytes)) => {
                    if key_bytes.len() == TXO_REF_SIZE {
                        if let Some((era, cbor)) = decode_utxo_value(&value_bytes) {
                            let txo_ref = decode_txo_ref(&key_bytes);
                            return Some(Ok((txo_ref, Arc::new(EraCbor(era, cbor)))));
                        }
                    }
                    // Skip malformed entries, continue to next
                }
                Err(e) => return Some(Err(Error::Fjall(e).into())),
            }
        }

        None
    }
}
