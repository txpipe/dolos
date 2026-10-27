//! The entity index: the `index-entity` keyspace of the archive store
//! (chain-agnostic).
//!
//! Maps an entity's id to the slot it was first seen at, for the kinds the
//! chain declares ([`EntityKind`]). The seal of an immutable epoch writes the
//! entries, in the same batch as the epoch's sealed logs, and nothing undoes
//! them.
//!
//! ## Key Format
//!
//! `[kind_hash:8][id:kind width]` → `[slot:8]`, with
//! `kind_hash = xxh3("entity:" + kind name)`. Ids are stored verbatim, so a
//! lookup answers for exactly the id it was given.

use std::ops::Range;

use dolos_core::{
    check_entity_key, ArchiveError, BlockSlot, EntityKind, EntityRecord, MAX_EXACT_KEY_LEN,
};
use fjall::{Keyspace, OwnedWriteBatch, Readable, Snapshot};

use crate::archive::scan::{DimensionScan, ScanTarget};
use crate::keys::{decode_slot, dim_prefix, encode_slot, hash_dimension, DIM_HASH_SIZE, SLOT_SIZE};
use crate::Error;

/// The widest key this keyspace holds: `[kind_hash:8][id:<=32]`.
const ENTITY_KEY_BUF: usize = DIM_HASH_SIZE + MAX_EXACT_KEY_LEN;

/// An entity index key, built on the stack.
struct EntityKeyBuf {
    buf: [u8; ENTITY_KEY_BUF],
    len: usize,
}

impl EntityKeyBuf {
    fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// The key prefix every entry of `kind` lives under.
fn kind_prefix(kind: EntityKind) -> [u8; DIM_HASH_SIZE] {
    hash_dimension(dim_prefix::ENTITY, kind.name())
}

/// The stored key of an id, refusing one that is not its kind's width.
fn build_key(kind: EntityKind, id: &[u8]) -> Result<EntityKeyBuf, ArchiveError> {
    check_entity_key(kind, id)?;

    let mut buf = [0u8; ENTITY_KEY_BUF];
    buf[..DIM_HASH_SIZE].copy_from_slice(&kind_prefix(kind));
    buf[DIM_HASH_SIZE..DIM_HASH_SIZE + id.len()].copy_from_slice(id);

    Ok(EntityKeyBuf {
        buf,
        len: DIM_HASH_SIZE + id.len(),
    })
}

/// The slot a stored entry resolves to, read from its value: the prune
/// sweep's view of an entry, as for the exact keyspace.
pub fn slot_of_entry(value: &[u8]) -> Result<BlockSlot, Error> {
    if value.len() < SLOT_SIZE {
        return Err(Error::Codec(format!(
            "malformed entity index value: {} bytes, expected {SLOT_SIZE}",
            value.len(),
        )));
    }

    Ok(decode_slot(value))
}

pub fn insert(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    kind: EntityKind,
    id: &[u8],
    slot: BlockSlot,
) -> Result<(), ArchiveError> {
    let key = build_key(kind, id)?;

    batch.insert(keyspace, key.as_slice(), encode_slot(slot));

    Ok(())
}

/// The slot of each id, in the order given.
///
/// Every id is checked before any is read, so a refused batch reads nothing.
pub fn get_many<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    kind: EntityKind,
    ids: &[&[u8]],
) -> Result<Vec<Option<BlockSlot>>, ArchiveError> {
    let keys = ids
        .iter()
        .map(|id| build_key(kind, id))
        .collect::<Result<Vec<_>, _>>()?;

    let mut out = Vec::with_capacity(keys.len());

    for key in keys {
        let value = readable
            .get(keyspace, key.as_slice())
            .map_err(Error::Fjall)?;

        out.push(value.map(|v| slot_of_entry(&v)).transpose()?);
    }

    Ok(out)
}

/// Lazy iterator over entity index records, one kind prefix at a time.
///
/// Driven by the caller's kind list, sorted by name, for the reason the tag
/// and exact iterators are driven by theirs: the stored key holds a hash of
/// the kind name. Within a prefix fjall's key order is id order, so the
/// `(kind name, id)` contract falls out of walking the sorted list. The slot
/// is the value, so every entry of a kind is read; errors are terminal per
/// [`crate::archive::scan`].
pub struct EntityRecordIterator(DimensionScan<EntityScan, std::vec::IntoIter<EntityKind>>);

/// The entity half of the shared prefix walk.
pub struct EntityScan;

impl ScanTarget for EntityScan {
    type Label = EntityKind;
    type Record = EntityRecord;

    fn prefix(kind: EntityKind) -> [u8; DIM_HASH_SIZE] {
        kind_prefix(kind)
    }

    fn decode(
        kind: EntityKind,
        guard: fjall::Guard,
        slots: &Range<BlockSlot>,
    ) -> Result<Option<EntityRecord>, ArchiveError> {
        let (key, value) = guard.into_inner().map_err(Error::Fjall)?;

        let slot = slot_of_entry(&value)?;

        if !slots.contains(&slot) {
            return Ok(None);
        }

        // A stored id of another width is a malformed entry, and
        // `EntityRecord::new` is where that is decided.
        EntityRecord::new(kind, &key[DIM_HASH_SIZE..], slot)
            .map(Some)
            .map_err(|e| Error::Codec(e.to_string()).into())
    }
}

impl EntityRecordIterator {
    /// Create an iterator over every record of `kinds` in the slot range.
    ///
    /// Returns immediately without reading any data.
    pub fn new(
        snapshot: Snapshot,
        keyspace: &Keyspace,
        kinds: &[EntityKind],
        slots: Range<BlockSlot>,
    ) -> Self {
        let mut kinds = kinds.to_vec();
        kinds.sort_unstable();
        kinds.dedup();

        Self(DimensionScan::new(
            snapshot,
            keyspace,
            kinds.into_iter(),
            slots,
        ))
    }
}

impl Iterator for EntityRecordIterator {
    type Item = Result<EntityRecord, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl std::iter::FusedIterator for EntityRecordIterator {}

#[cfg(test)]
mod tests {
    use super::*;

    const TOY: EntityKind = EntityKind::new("toy", 4);
    const OTHER: EntityKind = EntityKind::new("other", 4);

    #[test]
    fn only_the_kind_width_has_a_key() {
        assert!(build_key(TOY, &[0; 4]).is_ok());

        for width in [0, 3, 5, 32] {
            assert!(matches!(
                build_key(TOY, &vec![0; width]),
                Err(ArchiveError::EntityKeyWidth { .. })
            ));
        }
    }

    #[test]
    fn kinds_do_not_share_a_prefix() {
        let toy = build_key(TOY, &[7; 4]).unwrap();
        let other = build_key(OTHER, &[7; 4]).unwrap();

        assert_ne!(
            &toy.as_slice()[..DIM_HASH_SIZE],
            &other.as_slice()[..DIM_HASH_SIZE]
        );
        assert_eq!(&toy.as_slice()[DIM_HASH_SIZE..], &[7; 4]);
    }

    /// Pins the prefix rule: the hash of `entity:` and the kind name, so a
    /// store written today stays readable. A change that looks cosmetic would
    /// orphan every entry.
    #[test]
    fn the_prefix_is_the_entity_hash_of_the_name() {
        assert_eq!(
            kind_prefix(TOY),
            xxhash_rust::xxh3::xxh3_64(b"entity:toy").to_be_bytes()
        );
    }
}
