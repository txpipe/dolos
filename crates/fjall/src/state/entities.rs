//! Generic entity table operations for fjall state store.
//!
//! All entity types share a single keyspace (`state-entities`) with namespace
//! hash prefixes. Keys are 40 bytes: `[ns_hash:8][entity_key:32]`.
//!
//! This design reduces the number of LSM-tree segment files compared to
//! separate keyspaces per entity type.

use std::ops::Range;

use dolos_core::{EntityKey, EntityValue, Namespace, StateError};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use super::entity_keys::{
    build_entity_key, build_range_end, build_range_start, decode_entity_key, PREFIXED_KEY_SIZE,
};
use crate::Error;

/// Read multiple entities by keys from the unified entities keyspace.
///
/// Uses the `Readable` trait to support both direct keyspace access and snapshot-based
/// reads. Snapshot-based reads avoid potential deadlocks with concurrent writes.
pub fn read_entities<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    ns: Namespace,
    keys: &[&EntityKey],
) -> Result<Vec<Option<EntityValue>>, Error> {
    let mut results = Vec::with_capacity(keys.len());

    for key in keys {
        let prefixed_key = build_entity_key(ns, key);
        let value = readable.get(keyspace, prefixed_key).map_err(Error::Fjall)?;
        results.push(value.map(|v| v.as_ref().to_vec()));
    }

    Ok(results)
}

/// Write an entity to the unified entities keyspace
pub fn write_entity(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    ns: Namespace,
    key: &EntityKey,
    value: &EntityValue,
) {
    let prefixed_key = build_entity_key(ns, key);
    batch.insert(keyspace, prefixed_key, value.as_slice());
}

/// Delete an entity from the unified entities keyspace
pub fn delete_entity(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    ns: Namespace,
    key: &EntityKey,
) {
    let prefixed_key = build_entity_key(ns, key);
    batch.remove(keyspace, prefixed_key);
}

/// Iterator over entities in a key range within a namespace.
///
/// This collects all matching entities upfront since fjall's iterators
/// have complex lifetime requirements.
///
/// Uses the `Readable` trait to support both direct keyspace access and snapshot-based
/// reads. Snapshot-based reads avoid potential deadlocks with concurrent writes by using
/// MVCC (Multi-Version Concurrency Control).
pub struct EntityIterator {
    /// Collected entities from range scan
    entities: Vec<(EntityKey, EntityValue)>,
    /// Current position
    pos: usize,
}

impl EntityIterator {
    /// Create a new entity iterator from a keyspace range scan.
    ///
    /// The range is within a single namespace - both start and end keys are
    /// prefixed with the namespace hash before scanning.
    ///
    /// Uses the `Readable` trait to support snapshot-based iteration.
    pub fn new<R: Readable>(
        readable: &R,
        keyspace: &Keyspace,
        ns: Namespace,
        range: Range<EntityKey>,
    ) -> Result<Self, Error> {
        let mut entities = Vec::new();

        // Build prefixed range keys
        let start = build_range_start(ns, &range.start);
        let end = build_range_end(ns, &range.end);

        // Using Readable::range() enables snapshot-based iteration
        for guard in readable.range(keyspace, start.as_slice()..end.as_slice()) {
            // fjall's Guard::into_inner() gives us both key and value
            let (key_bytes, value_bytes) = guard.into_inner().map_err(Error::Fjall)?;

            // Decode entity key from prefixed key
            if key_bytes.len() >= PREFIXED_KEY_SIZE {
                let entity_key = decode_entity_key(&key_bytes);
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
