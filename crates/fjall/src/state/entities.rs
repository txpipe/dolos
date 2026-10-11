//! Generic entity table operations for fjall state store.
//!
//! All entity types share a single keyspace (`state-entities`) with namespace
//! hash prefixes. Keys are `[ns_hash:8][entity_key:var]`.
//!
//! This design reduces the number of LSM-tree segment files compared to
//! separate keyspaces per entity type.

use dolos_core::{EntityKey, EntityValue, KeyRange, Namespace, StateError};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use super::entity_keys::{build_entity_key, build_range, decode_entity_key};
use crate::Error;

/// Read multiple entities by keys from the unified entities keyspace.
///
/// Uses the `Readable` trait to support both direct keyspace access and
/// snapshot-based reads. Snapshot-based reads avoid potential deadlocks with
/// concurrent writes.
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

/// Lazy streaming iterator over entities in a key range within a namespace.
///
/// Wraps a `fjall::Iter` which is an owned iterator holding a `SnapshotNonce`
/// and a `Box<dyn DoubleEndedIterator + Send + 'static>`. Items are decoded
/// one at a time in `next()`, keeping memory usage O(1) regardless of entity
/// count.
///
/// Fuses after the first error: a malformed stored key or a failed read is
/// never skipped, so a consumer cannot take a short scan for a complete one.
pub struct EntityIterator {
    inner: fjall::Iter,
    done: bool,
}

impl EntityIterator {
    /// Create a new lazy entity iterator from a keyspace range scan.
    ///
    /// The range is within a single namespace - both start and end keys are
    /// prefixed with the namespace hash before scanning.
    ///
    /// This returns immediately without reading any data. Items are fetched
    /// lazily as `next()` is called.
    pub fn new<R: Readable>(
        readable: &R,
        keyspace: &Keyspace,
        ns: Namespace,
        range: KeyRange,
    ) -> Result<Self, Error> {
        let inner = readable.range(keyspace, build_range(ns, &range));

        Ok(Self { inner, done: false })
    }
}

impl Iterator for EntityIterator {
    type Item = Result<(EntityKey, EntityValue), StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let item = match self.inner.next()?.into_inner() {
            Ok((key, value)) => decode_entity_key(&key).map(|key| (key, value.to_vec())),
            Err(e) => Err(Error::Fjall(e)),
        };

        self.done = item.is_err();

        Some(item.map_err(StateError::from))
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
