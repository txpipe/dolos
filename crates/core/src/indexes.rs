//! The chain-agnostic index types.
//!
//! This module defines a chain-agnostic indexing system based on "tags" -
//! associations between entities (blocks, transactions, UTxOs) and dimension
//! keys. Chain-specific code (e.g., Cardano) defines the dimensions and
//! provides extension traits for convenient access.
//!
//! The indexes are projections of the stores that hold them:
//!
//! - the live-UTxO tags project the UTxO set and live in the state store
//!   (`StateStore::utxos_by_tag`, written through
//!   `StateWriter::apply_utxo_tags`);
//! - the archive tags and the exact lookups project the block history and live
//!   in the archive store (`ArchiveStore::slots_by_tag`,
//!   `ArchiveStore::slot_by_*`, written through `ArchiveWriter::apply_index`),
//!   where the `indexes` stele layer is produced from and restored into;
//! - the entity index maps an entity's id to the slot it was first seen at, for
//!   the kinds the chain declares ([`EntityKind`]). It lives in the archive
//!   store too (`ArchiveStore::entity_slots`, written through
//!   `ArchiveWriter::write_entity` by the seal of an immutable epoch) and
//!   travels in one `entity-{kind}` stele layer per kind and epoch.

use std::borrow::Cow;

use thiserror::Error;

use crate::{BlockSlot, TxoRef, UtxoPosition};

/// A dimension name identifying an index table.
///
/// Dimensions are static strings that identify the type of index.
/// Examples: "address", "payment", "stake", "policy", "asset"
///
/// Each dimension corresponds to a separate index table in the storage backend.
pub type TagDimension = &'static str;

/// A tag associating data with a dimension.
///
/// Tags are the fundamental unit of indexing. They associate an entity
/// (block, transaction, or UTxO) with a searchable key within a dimension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag {
    pub dimension: TagDimension,
    pub key: Vec<u8>,
}

impl Tag {
    pub fn new(dimension: TagDimension, key: impl Into<Vec<u8>>) -> Self {
        Self {
            dimension,
            key: key.into(),
        }
    }
}

/// Delta for UTxO filter indexes (current state).
///
/// UTxO filter indexes track the current set of UTxOs matching various tags.
/// They are updated as UTxOs are produced and consumed, through the state
/// writer that applies the UTxO set they project
/// (`StateWriter::apply_utxo_tags`). Each entry carries the UTxO's creation
/// position, which orders it within every tag (`UtxoPosition::tag_order`).
#[derive(Debug, Clone, Default)]
pub struct UtxoIndexDelta {
    /// UTxOs to add to filter indexes: (txo_ref, position, tags)
    pub produced: Vec<(TxoRef, Option<UtxoPosition>, Vec<Tag>)>,
    /// UTxOs to remove from filter indexes: (txo_ref, position, tags)
    pub consumed: Vec<(TxoRef, Option<UtxoPosition>, Vec<Tag>)>,
}

/// Delta for archive indexes (historical).
///
/// Archive indexes track which slots contain data matching various tags.
/// They enable historical queries like "find all blocks with transactions
/// involving this address". One per block; written through
/// `ArchiveWriter::apply_index` in the same batch as the block it projects.
#[derive(Debug, Clone, Default)]
pub struct ArchiveIndexDelta {
    pub slot: BlockSlot,
    pub block_hash: Vec<u8>,
    pub block_number: Option<u64>,
    pub tx_hashes: Vec<Vec<u8>>,
    pub tags: Vec<Tag>,
}

/// What can go wrong building an index record.
#[derive(Debug, Error)]
pub enum IndexError {
    #[error("codec error: {0}")]
    CodecError(String),
}

/// Size of the hashed portion of an archive tag key.
pub const KEY_HASH_SIZE: usize = 8;

/// The stored, fixed-width form of a tag key: `xxh3_64(logical key)` in
/// big-endian byte order, for every dimension.
///
/// Consumers must treat these bytes as opaque and copy them through unchanged.
pub type KeyHash = [u8; KEY_HASH_SIZE];

/// The stored key form of a logical tag key: the rule [`KeyHash`] describes,
/// as code.
///
/// Every backend has to agree on these bytes exactly. A record crossing from
/// one store to another carries the stored form and nothing else — the logical
/// key is not recoverable — so a backend that derived it differently would not
/// fail to restore, it would restore records its own queries then miss.
pub fn key_hash(key: &[u8]) -> KeyHash {
    xxhash_rust::xxh3::xxh3_64(key).to_be_bytes()
}

/// One archive tag entry, carrying the stored key hash instead of the logical
/// key.
///
/// The logical key is not recoverable from the store — only its hash is kept
/// on disk — so this is the most a reader can produce and the least a writer
/// needs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TagRecord {
    /// The logical dimension name. Borrowed when read from a store (the
    /// dimension list drives the traversal), owned when decoded from an
    /// external source.
    pub dimension: Cow<'static, str>,

    /// The stored key hash. See [`KeyHash`].
    pub key_hash: KeyHash,

    pub slot: BlockSlot,
}

impl TagRecord {
    pub fn new(dimension: TagDimension, key_hash: KeyHash, slot: BlockSlot) -> Self {
        Self {
            dimension: Cow::Borrowed(dimension),
            key_hash,
            slot,
        }
    }

    pub fn dimension(&self) -> &str {
        self.dimension.as_ref()
    }
}

/// The kinds of exact-match archive lookup the archive store keeps.
///
/// This is a closed set: the store hashes the kind name into the key, so it
/// cannot be recovered from disk and traversal has to be driven by the known
/// list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExactKind {
    BlockHash,
    BlockNumber,
    TxHash,
}

impl ExactKind {
    /// Every kind, in ascending [`ExactKind::as_str`] order.
    pub const ALL: [ExactKind; 3] = [Self::BlockHash, Self::BlockNumber, Self::TxHash];

    /// The stored dimension name for this kind.
    ///
    /// These strings are hashed into on-disk keys — changing one is a storage
    /// migration, not a rename.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BlockHash => "block_hash",
            Self::BlockNumber => "block_num",
            Self::TxHash => "tx_hash",
        }
    }

    /// The exact byte length of a stored key of this kind.
    ///
    /// Exact keys are fixed-width per kind: a 32-byte hash for
    /// [`ExactKind::BlockHash`] and [`ExactKind::TxHash`], an 8-byte big-endian
    /// number for [`ExactKind::BlockNumber`]. [`ExactRecord::new`] validates
    /// against this, so a wrong-width key from an external source cannot become
    /// a record at all, let alone a permanently unreadable entry.
    pub const fn key_len(&self) -> usize {
        match self {
            Self::BlockHash | Self::TxHash => 32,
            Self::BlockNumber => 8,
        }
    }
}

/// The widest [`ExactKind::key_len`], and so the inline width of
/// [`ExactRecord`]'s key.
///
/// Pinned by `max_exact_key_len_covers_every_kind` rather than derived: a
/// `const fn` maximum over [`ExactKind::ALL`] is not expressible today, and a
/// kind wider than this would silently truncate.
pub const MAX_EXACT_KEY_LEN: usize = 32;

impl std::str::FromStr for ExactKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_str() == s)
            .ok_or_else(|| format!("unknown exact kind: {s}"))
    }
}

impl std::fmt::Display for ExactKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One exact-match archive entry.
///
/// Unlike tags, exact keys are stored verbatim (a 32-byte hash, or an 8-byte
/// big-endian block number), so the record is lossless.
///
/// The key is held inline rather than in a `Vec`: it is fixed-width per kind
/// ([`ExactKind::key_len`]) and never wider than [`MAX_EXACT_KEY_LEN`], so a
/// heap allocation per record would buy nothing and a bulk export makes one
/// record per entry in the store.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExactRecord {
    pub kind: ExactKind,
    /// Zero-padded to [`MAX_EXACT_KEY_LEN`]; [`ExactRecord::key`] trims it.
    ///
    /// Private so [`ExactRecord::new`] is the only way in, which is what makes
    /// the width invariant hold. Ordering is unaffected by the padding: within
    /// a kind every key is the same width, so the tail is identical.
    key: [u8; MAX_EXACT_KEY_LEN],
    pub slot: BlockSlot,
}

impl ExactRecord {
    /// Build a record, checking the key against its kind's width.
    ///
    /// This is the single width-validation site. A wrong-width key can only
    /// come from an external source — the delta path is type-enforced — and
    /// would otherwise land as an entry no lookup could ever reach, which
    /// re-exports as if it were valid.
    pub fn new(kind: ExactKind, key: &[u8], slot: BlockSlot) -> Result<Self, IndexError> {
        let expected = kind.key_len();

        if key.len() != expected {
            return Err(IndexError::CodecError(format!(
                "exact record of kind {kind} has a {}-byte key, expected {expected}",
                key.len(),
            )));
        }

        let mut buf = [0u8; MAX_EXACT_KEY_LEN];
        buf[..expected].copy_from_slice(key);

        Ok(Self {
            kind,
            key: buf,
            slot,
        })
    }

    /// The stored key, trimmed to its kind's width.
    pub fn key(&self) -> &[u8] {
        &self.key[..self.kind.key_len()]
    }
}

/// Prints the key trimmed rather than zero-padded — the padding is an artifact
/// of the inline buffer, and these records are compared in test output.
impl std::fmt::Debug for ExactRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExactRecord")
            .field("kind", &self.kind)
            .field("key", &self.key())
            .field("slot", &self.slot)
            .finish()
    }
}

/// A kind of entity whose first-seen slot the archive's entity index keeps.
///
/// The chain declares its kinds and core names none. An id is fixed-width per
/// kind, at most [`MAX_EXACT_KEY_LEN`] bytes, and stored verbatim, so a
/// lookup answers for exactly the id it was given.
///
/// The fields are private so [`EntityKind::new`] is the only way in, which is
/// what keeps the width within an [`EntityRecord`]'s inline key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityKind {
    name: &'static str,
    key_len: usize,
}

impl EntityKind {
    /// A kind named `name` whose ids are `key_len` bytes.
    ///
    /// Panics on an empty name or a width outside `1..=MAX_EXACT_KEY_LEN`,
    /// which for a `const` declaration is a compile error.
    pub const fn new(name: &'static str, key_len: usize) -> Self {
        assert!(!name.is_empty(), "an entity kind needs a name");
        assert!(
            key_len > 0 && key_len <= MAX_EXACT_KEY_LEN,
            "an entity id is 1 to MAX_EXACT_KEY_LEN bytes"
        );

        Self { name, key_len }
    }

    /// The stored kind name. It is hashed into on-disk keys and names the
    /// kind's stele layer, so renaming one is a storage migration.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// The exact byte length of an id of this kind.
    pub const fn key_len(&self) -> usize {
        self.key_len
    }
}

impl std::fmt::Display for EntityKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name)
    }
}

/// One entity index entry: an id and the slot its entity was first seen at.
///
/// The key is held inline, as [`ExactRecord`]'s is, because a stele export
/// makes one record per entry in the store.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EntityRecord {
    pub kind: EntityKind,
    /// Zero-padded to [`MAX_EXACT_KEY_LEN`]; [`EntityRecord::key`] trims it.
    /// Private so [`EntityRecord::new`] is the only way in.
    key: [u8; MAX_EXACT_KEY_LEN],
    pub slot: BlockSlot,
}

impl EntityRecord {
    /// Build a record, checking the id against its kind's width.
    pub fn new(kind: EntityKind, key: &[u8], slot: BlockSlot) -> Result<Self, IndexError> {
        if key.len() != kind.key_len() {
            return Err(IndexError::CodecError(format!(
                "entity record of kind {kind} has a {}-byte id, expected {}",
                key.len(),
                kind.key_len(),
            )));
        }

        let mut buf = [0u8; MAX_EXACT_KEY_LEN];
        buf[..key.len()].copy_from_slice(key);

        Ok(Self {
            kind,
            key: buf,
            slot,
        })
    }

    /// The stored id, trimmed to its kind's width.
    pub fn key(&self) -> &[u8] {
        &self.key[..self.kind.key_len()]
    }
}

impl std::fmt::Debug for EntityRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EntityRecord")
            .field("kind", &self.kind.name())
            .field("key", &self.key())
            .field("slot", &self.slot)
            .finish()
    }
}

/// Either archive record, as consumed by
/// `ArchiveWriter::append_prehashed`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexRecord {
    Tag(TagRecord),
    Exact(ExactRecord),
}

impl From<TagRecord> for IndexRecord {
    fn from(value: TagRecord) -> Self {
        Self::Tag(value)
    }
}

impl From<ExactRecord> for IndexRecord {
    fn from(value: ExactRecord) -> Self {
        Self::Exact(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_kind_all_is_sorted_by_name() {
        let names: Vec<&str> = ExactKind::ALL.iter().map(|k| k.as_str()).collect();

        let mut sorted = names.clone();
        sorted.sort_unstable();

        assert_eq!(
            names, sorted,
            "ExactKind::ALL must be in ascending name order: the exact record \
             contract sorts by (kind, key) and the traversal walks ALL in order"
        );
    }

    #[test]
    fn exact_kind_ord_matches_name_ord() {
        for pair in ExactKind::ALL.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].as_str() < pair[1].as_str());
        }
    }

    #[test]
    fn exact_kind_roundtrips_through_name() {
        for kind in ExactKind::ALL {
            assert_eq!(kind.as_str().parse::<ExactKind>(), Ok(kind));
        }

        assert!("nope".parse::<ExactKind>().is_err());
    }

    /// Pins the literal strings: they are hashed into on-disk keys, so a
    /// rename that looks cosmetic (e.g. `block_num` -> `block_number`) orphans
    /// every existing entry of that kind. Every other test compares `as_str`
    /// against itself and would survive such a rename — this one exists to
    /// fail it.
    #[test]
    fn exact_kind_names_are_storage_format() {
        assert_eq!(ExactKind::BlockHash.as_str(), "block_hash");
        assert_eq!(ExactKind::BlockNumber.as_str(), "block_num");
        assert_eq!(ExactKind::TxHash.as_str(), "tx_hash");
    }

    #[test]
    fn max_exact_key_len_covers_every_kind() {
        for kind in ExactKind::ALL {
            assert!(
                kind.key_len() <= MAX_EXACT_KEY_LEN,
                "{kind} needs {} bytes, wider than the inline buffer",
                kind.key_len()
            );
        }

        assert!(
            ExactKind::ALL
                .iter()
                .any(|k| k.key_len() == MAX_EXACT_KEY_LEN),
            "MAX_EXACT_KEY_LEN should be the widest kind, not merely an upper bound"
        );
    }

    #[test]
    fn exact_record_rejects_a_wrong_width_key() {
        assert!(ExactRecord::new(ExactKind::BlockHash, &[0u8; 32], 1).is_ok());
        assert!(ExactRecord::new(ExactKind::BlockHash, &[0u8; 8], 1).is_err());
        assert!(ExactRecord::new(ExactKind::BlockNumber, &[0u8; 8], 1).is_ok());
        assert!(ExactRecord::new(ExactKind::BlockNumber, &[0u8; 32], 1).is_err());
        assert!(ExactRecord::new(ExactKind::TxHash, &[], 1).is_err());
    }

    /// The inline key is zero-padded, so ordering could in principle differ
    /// from ordering the trimmed keys. It does not: within a kind every key is
    /// the same width, and the record's ordering is `(kind, key)` before the
    /// padding is ever reached.
    #[test]
    fn exact_record_ord_is_kind_then_key() {
        let low = ExactRecord::new(ExactKind::BlockNumber, &1u64.to_be_bytes(), 9).unwrap();
        let high = ExactRecord::new(ExactKind::BlockNumber, &2u64.to_be_bytes(), 0).unwrap();
        let other_kind = ExactRecord::new(ExactKind::TxHash, &[0u8; 32], 0).unwrap();

        assert!(low < high, "key dominates slot");
        assert!(high < other_kind, "kind dominates key");
        assert_eq!(low.key(), &1u64.to_be_bytes());
    }

    #[test]
    fn entity_record_rejects_a_wrong_width_id() {
        let kind = EntityKind::new("toy", 4);

        assert!(EntityRecord::new(kind, &[0u8; 4], 1).is_ok());
        assert!(EntityRecord::new(kind, &[0u8; 3], 1).is_err());
        assert!(EntityRecord::new(kind, &[0u8; 5], 1).is_err());
        assert!(EntityRecord::new(kind, &[], 1).is_err());
    }

    #[test]
    fn entity_record_ord_is_kind_then_id() {
        let a = EntityKind::new("a", 4);
        let b = EntityKind::new("b", 2);

        let low = EntityRecord::new(a, &[0, 0, 0, 1], 9).unwrap();
        let high = EntityRecord::new(a, &[0, 0, 0, 2], 0).unwrap();
        let other_kind = EntityRecord::new(b, &[0, 0], 0).unwrap();

        assert!(low < high, "id dominates slot");
        assert!(high < other_kind, "kind name dominates id");
        assert_eq!(low.key(), &[0, 0, 0, 1]);
    }

    #[test]
    #[should_panic]
    fn an_entity_kind_wider_than_an_inline_key_is_refused() {
        EntityKind::new("wide", MAX_EXACT_KEY_LEN + 1);
    }

    #[test]
    fn tag_record_ord_is_dimension_then_hash_then_slot() {
        let a = TagRecord::new("address", [0xff; 8], 10);
        let b = TagRecord::new("asset", [0x00; 8], 0);
        let c = TagRecord::new("asset", [0x00; 8], 1);
        let d = TagRecord::new("asset", [0x01; 8], 0);

        assert!(a < b, "dimension dominates");
        assert!(b < c, "slot breaks ties within a key hash");
        assert!(c < d, "key hash dominates slot");
    }
}
