//! Datums table operations for fjall state store.
//!
//! Datums use reference counting to track how many UTxOs reference each datum.
//! When the reference count reaches zero, the datum is removed.
//!
//! Key: `datum_hash[32]`
//! Value: `refcount[8] ++ datum_bytes[...]`

use fjall::{Keyspace, OwnedWriteBatch};
use pallas::crypto::hash::Hash;

use crate::keys::{decode_datum_value, encode_datum_value};
use crate::Error;

/// Get datum bytes by hash.
///
/// Returns None if the datum doesn't exist.
pub fn get_datum(keyspace: &Keyspace, datum_hash: &Hash<32>) -> Result<Option<Vec<u8>>, Error> {
    match keyspace
        .get(datum_hash.as_ref())
        .map_err(|e| Error::Fjall(e.into()))?
    {
        Some(value) => {
            if let Some((_refcount, datum_bytes)) = decode_datum_value(&value) {
                Ok(Some(datum_bytes))
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

/// Increment reference count for a datum.
///
/// If the datum doesn't exist, it's inserted with refcount=1.
/// If it exists, the refcount is incremented.
///
/// Note: This requires a read-modify-write pattern. The batch is used for the
/// final write, but we need to read the current value first.
pub fn increment(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    datum_hash: &Hash<32>,
    datum_bytes: &[u8],
) -> Result<(), Error> {
    let key = datum_hash.as_ref();

    // Read current value
    let new_refcount = match keyspace.get(key).map_err(|e| Error::Fjall(e.into()))? {
        Some(value) => {
            if let Some((refcount, _)) = decode_datum_value(&value) {
                refcount.saturating_add(1)
            } else {
                1
            }
        }
        None => 1,
    };

    // Write new value
    let new_value = encode_datum_value(new_refcount, datum_bytes);
    batch.insert(keyspace, key, new_value);

    Ok(())
}

/// Decrement reference count for a datum.
///
/// If the refcount reaches zero, the datum is removed.
/// If the datum doesn't exist, this is a no-op.
///
/// Note: This requires a read-modify-write pattern.
pub fn decrement(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    datum_hash: &Hash<32>,
) -> Result<(), Error> {
    let key = datum_hash.as_ref();

    // Read current value
    if let Some(value) = keyspace.get(key).map_err(|e| Error::Fjall(e.into()))? {
        if let Some((refcount, datum_bytes)) = decode_datum_value(&value) {
            if refcount <= 1 {
                // Remove the datum
                batch.remove(keyspace, key);
            } else {
                // Decrement refcount
                let new_value = encode_datum_value(refcount - 1, &datum_bytes);
                batch.insert(keyspace, key, new_value);
            }
        }
    }

    Ok(())
}
