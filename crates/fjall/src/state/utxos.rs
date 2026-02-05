//! UTxO table operations for fjall state store.
//!
//! UTxOs are stored with composite keys: `txhash[32] ++ idx[4]` (36 bytes)
//! Values are: `era[2] ++ cbor[...]`

use std::collections::HashMap;
use std::sync::Arc;

use dolos_core::{EraCbor, TxoRef, UtxoMap, UtxoSetDelta};
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
/// Uses the `Readable` trait to support both direct keyspace access and snapshot-based
/// reads. Snapshot-based reads avoid potential deadlocks with concurrent writes.
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

/// Iterator over all UTxOs in the keyspace.
///
/// Uses the `Readable` trait to support both direct keyspace access and snapshot-based
/// reads. Snapshot-based reads avoid potential deadlocks with concurrent writes by using
/// MVCC (Multi-Version Concurrency Control).
pub struct UtxosIterator {
    /// Collected UTxOs from scan
    utxos: Vec<(TxoRef, Arc<EraCbor>)>,
    /// Current position
    pos: usize,
}

impl UtxosIterator {
    /// Create a new iterator over all UTxOs.
    ///
    /// Uses the `Readable` trait to support snapshot-based iteration.
    pub fn new<R: Readable>(readable: &R, keyspace: &Keyspace) -> Result<Self, Error> {
        let mut utxos = Vec::new();

        // Scan all entries in the keyspace
        // Using Readable::iter() enables snapshot-based iteration
        for guard in readable.iter(keyspace) {
            // fjall's Guard::into_inner() gives us both key and value
            let (key_bytes, value_bytes) = guard.into_inner().map_err(Error::Fjall)?;

            if key_bytes.len() == TXO_REF_SIZE {
                let txo_ref = decode_txo_ref(&key_bytes);
                if let Some((era, cbor)) = decode_utxo_value(&value_bytes) {
                    utxos.push((txo_ref, Arc::new(EraCbor(era, cbor))));
                }
            }
        }

        Ok(Self { utxos, pos: 0 })
    }
}

impl Iterator for UtxosIterator {
    type Item = Result<(TxoRef, Arc<EraCbor>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.utxos.len() {
            let item = self.utxos[self.pos].clone();
            self.pos += 1;
            Some(Ok(item))
        } else {
            None
        }
    }
}
