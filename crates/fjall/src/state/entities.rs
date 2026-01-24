//! Generic entity table operations for fjall state store.
//!
//! Entities are simple key-value pairs stored in namespace-specific keyspaces.
//! Keys are 32-byte EntityKeys, values are arbitrary byte sequences.

use std::ops::Range;

use dolos_core::{EntityKey, EntityValue, StateError};
use fjall::{Keyspace, OwnedWriteBatch};

use crate::Error;

/// Size of entity keys: 32 bytes
pub const ENTITY_KEY_SIZE: usize = 32;

/// Read multiple entities by keys from a namespace keyspace
pub fn read_entities(
    keyspace: &Keyspace,
    keys: &[&EntityKey],
) -> Result<Vec<Option<EntityValue>>, Error> {
    let mut results = Vec::with_capacity(keys.len());

    for key in keys {
        let value = keyspace
            .get(key.as_ref())
            .map_err(|e| Error::Fjall(e.into()))?;
        results.push(value.map(|v| v.as_ref().to_vec()));
    }

    Ok(results)
}

/// Write an entity to a namespace keyspace
pub fn write_entity(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    key: &EntityKey,
    value: &EntityValue,
) {
    batch.insert(keyspace, key.as_ref(), value.as_slice());
}

/// Delete an entity from a namespace keyspace
pub fn delete_entity(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, key: &EntityKey) {
    batch.remove(keyspace, key.as_ref());
}

/// Iterator over entities in a key range.
///
/// This collects all matching entities upfront since fjall's iterators
/// have complex lifetime requirements.
pub struct EntityIterator {
    /// Collected entities from range scan
    entities: Vec<(EntityKey, EntityValue)>,
    /// Current position
    pos: usize,
}

impl EntityIterator {
    /// Create a new entity iterator from a keyspace range scan
    pub fn new(keyspace: &Keyspace, range: Range<EntityKey>) -> Result<Self, Error> {
        let mut entities = Vec::new();

        // Use range scan with start..end keys
        let start = range.start.as_ref();
        let end = range.end.as_ref();

        for guard in keyspace.range(start..end) {
            // fjall's Guard::into_inner() gives us both key and value
            let (key_bytes, value_bytes) = guard.into_inner().map_err(|e| Error::Fjall(e))?;

            // Convert key bytes to EntityKey
            if key_bytes.len() == ENTITY_KEY_SIZE {
                let mut key_array = [0u8; ENTITY_KEY_SIZE];
                key_array.copy_from_slice(&key_bytes);
                let entity_key = EntityKey::from(&key_array);
                let entity_value = value_bytes.to_vec();
                entities.push((entity_key, entity_value));
            }
        }

        Ok(Self { entities, pos: 0 })
    }
}

impl Iterator for EntityIterator {
    type Item = Result<(EntityKey, EntityValue), StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.entities.len() {
            let item = self.entities[self.pos].clone();
            self.pos += 1;
            Some(Ok(item))
        } else {
            None
        }
    }
}

/// Empty iterator for unsupported multimap operations
pub struct EmptyEntityValueIterator;

impl Iterator for EmptyEntityValueIterator {
    type Item = Result<EntityValue, StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
