//! Archive tag index operations for the `archive-tags` keyspace of the
//! archive store (chain-agnostic).
//!
//! This module handles key encoding, batch writes (apply/undo), read queries,
//! and the `SlotIterator` for the `archive-tags` keyspace. Block tags are
//! append-only entries that record which blocks contain specific data; they
//! are written in the same batch as the block locations they point at.
//!
//! ## Key Format
//!
//! ```text
//! Key:   [dim_hash:8][key_hash:8][slot:8]
//! Value: (empty)
//! ```
//!
//! The `dim_hash` is computed as `xxh3("block:" + dimension)`. The `key_hash`
//! is [`dolos_core::key_hash`] — this module does not decide it, because two
//! backends that decided it differently would exchange records neither could
//! then look up.

use std::borrow::Cow;
use std::ops::Range;

use dolos_core::{
    key_hash, ArchiveError, ArchiveIndexDelta, BlockSlot, KeyHash, Tag, TagDimension, TagRecord,
};
use fjall::{Iter, Keyspace, OwnedWriteBatch, Readable, Snapshot};

use crate::archive::scan::{DimensionScan, ScanTarget};
use crate::keys::{
    decode_slot, dim_prefix, hash_dimension, DIM_HASH_SIZE, HASH_KEY_SIZE, SLOT_SIZE,
};
use crate::Error;

// ============================================================================
// Key Encoding
// ============================================================================

/// Fixed size of block tag key: dim_hash(8) + key_hash(8) + slot(8) = 24 bytes
const BLOCK_TAG_KEY_SIZE: usize = DIM_HASH_SIZE + HASH_KEY_SIZE + SLOT_SIZE;

/// Build a block tag key from an already-hashed dimension:
/// `[dim_hash:8][key_hash:8][slot:8]`
///
/// The one place this layout is written. Callers that hold the dimension
/// *string* go through [`build_block_tag_key`]; callers restoring a batch of
/// records hash the dimension once per group and come here directly.
fn build_block_tag_key_for_dim(
    dim_hash: [u8; DIM_HASH_SIZE],
    key_hash: KeyHash,
    slot: u64,
) -> [u8; BLOCK_TAG_KEY_SIZE] {
    let mut key = [0u8; BLOCK_TAG_KEY_SIZE];
    key[..DIM_HASH_SIZE].copy_from_slice(&dim_hash);
    key[DIM_HASH_SIZE..DIM_HASH_SIZE + HASH_KEY_SIZE].copy_from_slice(&key_hash);
    key[DIM_HASH_SIZE + HASH_KEY_SIZE..].copy_from_slice(&slot.to_be_bytes());
    key
}

/// Build a block tag key: `[dim_hash:8][key_hash:8][slot:8]`
fn build_block_tag_key(dimension: &str, key_hash: KeyHash, slot: u64) -> [u8; BLOCK_TAG_KEY_SIZE] {
    let dim_hash = hash_dimension(dim_prefix::BLOCK, dimension);
    build_block_tag_key_for_dim(dim_hash, key_hash, slot)
}

/// Decode slot from block tag key (last 8 bytes)
fn decode_block_tag_slot(key: &[u8]) -> u64 {
    debug_assert!(key.len() >= BLOCK_TAG_KEY_SIZE);
    let start = key.len() - SLOT_SIZE;
    decode_slot(&key[start..])
}

/// The slot a stored tag entry belongs to, read from its key.
///
/// The prune sweep's view of an entry: the slot is the key's last component,
/// so nothing but the key is needed. A key too short to carry one is a
/// malformed entry, refused rather than skipped for the same reason the
/// record scan fuses on it.
pub fn slot_of_entry(key: &[u8]) -> Result<BlockSlot, Error> {
    if key.len() < BLOCK_TAG_KEY_SIZE {
        return Err(Error::Codec(format!(
            "malformed archive tag key: {} bytes, expected {BLOCK_TAG_KEY_SIZE}",
            key.len(),
        )));
    }

    Ok(decode_block_tag_slot(key))
}

/// Decode the stored tag key hash from a block tag key (bytes 8..16)
fn decode_block_tag_key_hash(key: &[u8]) -> KeyHash {
    debug_assert!(key.len() >= BLOCK_TAG_KEY_SIZE);
    let mut hash = [0u8; HASH_KEY_SIZE];
    hash.copy_from_slice(&key[DIM_HASH_SIZE..DIM_HASH_SIZE + HASH_KEY_SIZE]);
    hash
}

// ============================================================================
// Operations
// ============================================================================

/// Insert a tag into the archive-tags keyspace.
///
/// The stored key form comes from [`dolos_core::key_hash`], which is also
/// where the `metadata` exception lives — this module knows only that a tag key
/// has a stored form, not how it is derived.
///
/// A key with no valid stored form is dropped rather than stored: `key_hash`
/// declines it precisely because nothing could look it up again.
fn insert_tag(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, tag: &Tag, slot: BlockSlot) {
    let Some(hash) = key_hash(tag.dimension, &tag.key) else {
        return;
    };

    let key = build_block_tag_key(tag.dimension, hash, slot);
    batch.insert(keyspace, key, []);
}

/// Remove a tag from the archive-tags keyspace.
///
/// The exact inverse of [`insert_tag`], including which keys it declines: a tag
/// that was never stored has nothing to remove.
fn remove_tag(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, tag: &Tag, slot: BlockSlot) {
    let Some(hash) = key_hash(tag.dimension, &tag.key) else {
        return;
    };

    let key = build_block_tag_key(tag.dimension, hash, slot);
    batch.remove(keyspace, key);
}

/// Insert a tag entry from a record that already carries its stored key hash.
///
/// This is the write mirror of [`TagRecordIterator`]: the 8 hash bytes go in
/// verbatim, so a record read out of one store lands byte-identical in another.
/// It is deliberately *not* routed through [`insert_tag`] — that function
/// derives the stored form from a logical key, which a restore does not have
/// and cannot recover.
///
/// `dim_hash` is `hash_dimension(dim_prefix::BLOCK, record.dimension())`,
/// passed in rather than computed here: records arrive grouped by dimension,
/// so the caller hashes each dimension once per group instead of once per
/// record.
pub fn insert_prehashed(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    record: &TagRecord,
    dim_hash: [u8; DIM_HASH_SIZE],
) {
    let key = build_block_tag_key_for_dim(dim_hash, record.key_hash, record.slot);
    batch.insert(keyspace, key, []);
}

/// Apply archive tag indexes for a single block delta
fn apply_block(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    for tag in &block.tags {
        insert_tag(batch, keyspace, tag, block.slot);
    }
    Ok(())
}

/// Undo archive tag indexes for a single block delta (rollback)
fn undo_block(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    for tag in &block.tags {
        remove_tag(batch, keyspace, tag, block.slot);
    }
    Ok(())
}

/// Apply archive tag indexes for a batch of block deltas
pub fn apply(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    deltas: &[ArchiveIndexDelta],
) -> Result<(), Error> {
    for block in deltas {
        apply_block(batch, keyspace, block)?;
    }
    Ok(())
}

/// Undo archive tag indexes for a batch of block deltas (rollback)
pub fn undo(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    deltas: &[ArchiveIndexDelta],
) -> Result<(), Error> {
    for block in deltas.iter().rev() {
        undo_block(batch, keyspace, block)?;
    }
    Ok(())
}

// ============================================================================
// SlotIterator
// ============================================================================

/// Slot iterator for block tag queries.
/// Wraps a bounded fjall range iterator and decodes slots on demand.
pub struct SlotIterator {
    inner: Option<Iter>,
}

impl SlotIterator {
    /// Create a slot iterator over the inclusive `[start_slot, end_slot]`
    /// range of one tag. A reversed interval yields nothing. Read errors
    /// surface while iterating, not here.
    ///
    /// The dimension string is passed directly (chain-agnostic), and the key
    /// arrives already in its stored form: the caller derives it with
    /// [`dolos_core::key_hash`], the same function the write path uses, so a
    /// query cannot look under bytes an insert would not have written.
    pub fn new<R: Readable>(
        readable: &R,
        keyspace: &Keyspace,
        dimension: &str,
        key_hash: KeyHash,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self, Error> {
        if start_slot > end_slot {
            return Ok(Self { inner: None });
        }

        let lower = build_block_tag_key(dimension, key_hash, start_slot);
        let upper = build_block_tag_key(dimension, key_hash, end_slot);

        Ok(Self {
            inner: Some(readable.range(keyspace, lower..=upper)),
        })
    }
}

impl Iterator for SlotIterator {
    type Item = Result<BlockSlot, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        let inner = self.inner.as_mut()?;

        loop {
            let guard = inner.next()?;
            match guard.key() {
                Ok(key) if key.len() >= BLOCK_TAG_KEY_SIZE => {
                    return Some(Ok(decode_block_tag_slot(&key)))
                }
                Ok(_) => continue,
                Err(error) => return Some(Err(Error::Fjall(error).into())),
            }
        }
    }
}

impl DoubleEndedIterator for SlotIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        let inner = self.inner.as_mut()?;

        loop {
            let guard = inner.next_back()?;
            match guard.key() {
                Ok(key) if key.len() >= BLOCK_TAG_KEY_SIZE => {
                    return Some(Ok(decode_block_tag_slot(&key)))
                }
                Ok(_) => continue,
                Err(error) => return Some(Err(Error::Fjall(error).into())),
            }
        }
    }
}

/// Lazy iterator over archive tag records, one dimension prefix at a time.
///
/// ## Why it is driven by a dimension list
///
/// The stored key is `[dim_hash:8][tag_hash:8][slot:8]` where
/// `dim_hash = xxh3("block:" + dimension)`. The dimension *string* is nowhere
/// on disk, so a blind scan of the keyspace could not label the records it
/// produced. Traversal therefore takes the known dimension list and seeks one
/// prefix at a time, which supplies the dimension as a side effect.
///
/// ## Ordering
///
/// On-disk order is by `dim_hash`, which is unrelated to the dimension string's
/// order. The contract is `(dimension, key_hash, slot)`, so the dimension list
/// is sorted (and deduplicated) up front and walked in that order; within one
/// prefix, fjall's lexicographic order over `[tag_hash][slot]` is already the
/// rest of the contract, both being big-endian.
///
/// ## Cost
///
/// `slot` is the *last* key component, so a slot range is not a seekable
/// prefix: each dimension is scanned in full and filtered. This is the cost
/// centre of a per-epoch slice. Memory stays O(1) — entries are matched one at
/// a time (tag values are empty, so each entry is just its 24-byte key).
///
/// ## Lifetime
///
/// The iterator holds an MVCC snapshot for as long as it lives, which pins
/// fjall's GC watermark: superseded versions written while it is alive are
/// retained until it drops. Drain or drop it promptly on a live store.
///
/// ## Errors
///
/// Terminal, per the policy in [`crate::archive::scan`] — the one place it is
/// defined.
pub struct TagRecordIterator(DimensionScan<TagScan, std::vec::IntoIter<TagDimension>>);

/// The archive-tag half of the shared prefix walk.
pub struct TagScan;

impl ScanTarget for TagScan {
    type Label = TagDimension;
    type Record = TagRecord;

    fn prefix(dimension: TagDimension) -> [u8; DIM_HASH_SIZE] {
        hash_dimension(dim_prefix::BLOCK, dimension)
    }

    /// Only the key is read: the value of a tag entry is empty, and
    /// [`fjall::Guard::key`] can skip loading a value the tree may have
    /// separated out.
    fn decode(
        dimension: TagDimension,
        guard: fjall::Guard,
        slots: &Range<BlockSlot>,
    ) -> Result<Option<TagRecord>, ArchiveError> {
        let key = guard.key().map_err(Error::Fjall)?;

        if key.len() < BLOCK_TAG_KEY_SIZE {
            return Err(Error::Codec(format!(
                "malformed archive tag key in dimension {dimension}: \
                 {} bytes, expected {BLOCK_TAG_KEY_SIZE}",
                key.len(),
            ))
            .into());
        }

        let slot = decode_block_tag_slot(&key);

        if !slots.contains(&slot) {
            return Ok(None);
        }

        Ok(Some(TagRecord {
            dimension: Cow::Borrowed(dimension),
            key_hash: decode_block_tag_key_hash(&key),
            slot,
        }))
    }
}

impl TagRecordIterator {
    /// Create a new iterator over the tag records of the given dimensions.
    ///
    /// Returns immediately without reading any data.
    pub fn new(
        snapshot: Snapshot,
        keyspace: &Keyspace,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Self {
        // Sorting here rather than trusting the caller makes the ordering
        // contract unconditional; the list is a dozen entries at most.
        let mut dimensions = dimensions.to_vec();
        dimensions.sort_unstable();
        dimensions.dedup();

        Self(DimensionScan::new(
            snapshot,
            keyspace,
            dimensions.into_iter(),
            slots,
        ))
    }
}

impl Iterator for TagRecordIterator {
    type Item = Result<TagRecord, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl std::iter::FusedIterator for TagRecordIterator {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The stored form of a logical key, for tests that start from one.
    /// Production code never unwraps this — see [`insert_tag`].
    fn stored(dimension: &str, key: &[u8]) -> KeyHash {
        key_hash(dimension, key).expect("test keys are valid for their dimension")
    }

    #[test]
    fn test_block_tag_key_roundtrip() {
        let tag_key = b"some_address_bytes";
        let slot = 141868807u64;
        let key = build_block_tag_key("address", stored("address", tag_key), slot);

        assert_eq!(key.len(), BLOCK_TAG_KEY_SIZE);

        let decoded_slot = decode_block_tag_slot(&key);
        assert_eq!(slot, decoded_slot);
    }

    #[test]
    fn test_block_tag_ordering() {
        let tag_key = stored("address", b"test_address");
        let key1 = build_block_tag_key("address", tag_key, 100);
        let key2 = build_block_tag_key("address", tag_key, 200);
        let key3 = build_block_tag_key("address", tag_key, 50);

        // Same prefix, ordered by slot
        assert!(key1 < key2);
        assert!(key3 < key1);
    }

    #[test]
    fn test_utxo_block_separation() {
        // UTxO and block tags with same dimension name should have different prefixes
        let utxo_dim_hash = hash_dimension(dim_prefix::UTXO, "address");
        let block_key = build_block_tag_key("address", stored("address", b"addr"), 0);

        // dim_hash for utxo vs block should be different
        assert_ne!(&utxo_dim_hash, &block_key[..DIM_HASH_SIZE]);
    }

    #[test]
    fn test_any_dimension_works() {
        // Any dimension string should work (chain-agnostic)
        let key = build_block_tag_key(
            "another_dimension",
            stored("another_dimension", b"data"),
            12345,
        );

        // Key should be valid size
        assert_eq!(key.len(), BLOCK_TAG_KEY_SIZE);
    }

    /// The key hash a record carries has to rebuild the exact key it came from,
    /// otherwise an export/restore round trip silently loses entries.
    #[test]
    fn test_prehashed_key_matches_written_key() {
        let slot = 42u64;
        let written =
            build_block_tag_key("address", stored("address", b"some_address_bytes"), slot);

        let record = TagRecord::new("address", decode_block_tag_key_hash(&written), slot);
        let rebuilt = build_block_tag_key(record.dimension(), record.key_hash, record.slot);

        assert_eq!(written, rebuilt);
    }

    /// `metadata` is the one dimension whose stored key bytes are not a hash:
    /// the logical key is already a u64 label and the store keeps it verbatim.
    /// A record must carry those bytes through unchanged rather than
    /// re-deriving them.
    ///
    /// This module no longer decides that — [`dolos_core::key_hash`] does — so
    /// what is pinned here is that the decision reaches the on-disk key.
    #[test]
    fn test_prehashed_key_carries_metadata_label_verbatim() {
        let label = 674u64;
        let slot = 42u64;
        let stored_label = stored("metadata", &label.to_be_bytes());
        let written = build_block_tag_key("metadata", stored_label, slot);

        assert_eq!(u64::from_be_bytes(stored_label), label);
        assert_ne!(
            stored_label,
            xxhash_rust::xxh3::xxh3_64(&label.to_be_bytes()).to_be_bytes(),
            "the metadata label is stored raw, not hashed"
        );

        let record = TagRecord::new("metadata", decode_block_tag_key_hash(&written), slot);
        let rebuilt = build_block_tag_key(record.dimension(), record.key_hash, record.slot);

        assert_eq!(written, rebuilt);
    }

    /// A `metadata` key that is not eight bytes has no stored form: it would
    /// land under bytes no query could reconstruct. Both write paths drop it
    /// rather than store it.
    #[test]
    fn test_malformed_metadata_key_has_no_stored_form() {
        assert!(key_hash("metadata", b"seven!!").is_none());
        assert!(key_hash("metadata", &674u64.to_be_bytes()).is_some());

        // Every other dimension takes a key of any width.
        assert!(key_hash("address", b"seven!!").is_some());
    }
}

#[cfg(test)]
mod slot_iterator_tests {
    use fjall::{Database, KeyspaceCreateOptions, PersistMode};

    use super::*;

    const DIM: &str = "address";
    const TAG: KeyHash = [0x44; 8];
    const ORACLE: [u64; 8] = [0, 1, 2, 10, 11, 42, u64::MAX - 1, u64::MAX];

    struct Fixture {
        _dir: tempfile::TempDir,
        db: Database,
        keyspace: Keyspace,
    }

    impl Fixture {
        fn insert(&self, dimension: &str, key_hash: KeyHash, slots: &[u64]) {
            let mut batch = self.db.batch();
            for slot in slots {
                batch.insert(
                    &self.keyspace,
                    build_block_tag_key(dimension, key_hash, *slot),
                    [],
                );
            }
            batch.commit().unwrap();
        }

        fn iter(&self, key_hash: KeyHash, start: u64, end: u64) -> SlotIterator {
            SlotIterator::new(
                &self.db.snapshot(),
                &self.keyspace,
                DIM,
                key_hash,
                start,
                end,
            )
            .unwrap()
        }

        fn forward(&self, key_hash: KeyHash, start: u64, end: u64) -> Vec<u64> {
            self.iter(key_hash, start, end)
                .collect::<Result<_, _>>()
                .unwrap()
        }

        fn reverse(&self, key_hash: KeyHash, start: u64, end: u64) -> Vec<u64> {
            self.iter(key_hash, start, end)
                .rev()
                .collect::<Result<_, _>>()
                .unwrap()
        }
    }

    /// The oracle tag, inserted twice, between neighbours on every side: the
    /// tag hashes just below and above it at the extreme slots, and the same
    /// tag bytes under another dimension.
    fn fixture() -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::builder(dir.path()).open().unwrap();
        let keyspace = db
            .keyspace("archive-tags", KeyspaceCreateOptions::default)
            .unwrap();
        let fixture = Fixture {
            _dir: dir,
            db,
            keyspace,
        };

        fixture.insert(DIM, TAG, &ORACLE);
        fixture.insert(DIM, TAG, &ORACLE);

        let mut below = TAG;
        below[7] -= 1;
        let mut above = TAG;
        above[7] += 1;
        fixture.insert(DIM, below, &[10, u64::MAX]);
        fixture.insert(DIM, above, &[0, 10]);
        fixture.insert("other-dimension", TAG, &[0, 10, 12, u64::MAX]);

        fixture
    }

    #[test]
    fn full_range_reads_only_the_tag() {
        let fixture = fixture();
        let mut reversed = ORACLE;
        reversed.reverse();

        assert_eq!(fixture.forward(TAG, 0, u64::MAX), ORACLE);
        assert_eq!(fixture.reverse(TAG, 0, u64::MAX), reversed);
    }

    #[test]
    fn bounds_are_inclusive() {
        let fixture = fixture();

        assert_eq!(fixture.forward(TAG, 1, 42), [1, 2, 10, 11, 42]);
        assert_eq!(fixture.reverse(TAG, 1, 42), [42, 11, 10, 2, 1]);
        assert_eq!(fixture.forward(TAG, 3, 41), [10, 11]);
        assert_eq!(fixture.forward(TAG, 10, 10), [10]);
        assert!(fixture.forward(TAG, 12, 41).is_empty());
    }

    #[test]
    fn reversed_interval_is_empty() {
        let fixture = fixture();

        assert!(fixture.forward(TAG, 43, 41).is_empty());
        assert!(fixture.reverse(TAG, 43, 41).is_empty());
        assert!(fixture.forward(TAG, u64::MAX, 0).is_empty());
    }

    #[test]
    fn slot_zero_and_max_are_reachable() {
        let fixture = fixture();

        assert_eq!(fixture.forward(TAG, 0, 0), [0]);
        assert_eq!(fixture.reverse(TAG, 0, 0), [0]);
        assert_eq!(fixture.forward(TAG, u64::MAX, u64::MAX), [u64::MAX]);
        assert_eq!(fixture.reverse(TAG, u64::MAX, u64::MAX), [u64::MAX]);
        assert_eq!(
            fixture.forward(TAG, u64::MAX - 1, u64::MAX),
            [u64::MAX - 1, u64::MAX]
        );
    }

    #[test]
    fn neighbouring_tags_stay_separate() {
        let fixture = fixture();
        let mut above = TAG;
        above[7] += 1;

        assert_eq!(fixture.forward(above, 0, u64::MAX), [0, 10]);
        assert!(fixture.forward([0x66; 8], 0, u64::MAX).is_empty());

        let other = SlotIterator::new(
            &fixture.db.snapshot(),
            &fixture.keyspace,
            "other-dimension",
            TAG,
            1,
            u64::MAX - 1,
        )
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
        assert_eq!(other, [10, 12]);
    }

    #[test]
    fn interleaved_ends_meet_once() {
        let fixture = fixture();
        let mut iter = fixture.iter(TAG, 0, u64::MAX);
        let mut seen = Vec::new();

        while let Some(front) = iter.next() {
            seen.push(front.unwrap());
            match iter.next_back() {
                Some(back) => seen.push(back.unwrap()),
                None => break,
            }
        }

        assert_eq!(seen, [0, u64::MAX, 1, u64::MAX - 1, 2, 42, 10, 11]);
        assert!(iter.next().is_none());
        assert!(iter.next_back().is_none());
    }

    /// Dropped half-read iterators hold no snapshot that stops later writes,
    /// compaction or reads.
    #[test]
    fn early_drop_releases_the_read() {
        let fixture = fixture();

        for _ in 0..2_000 {
            let mut iter = fixture.iter(TAG, 0, u64::MAX);
            assert_eq!(iter.next().unwrap().unwrap(), 0);
            assert_eq!(iter.next_back().unwrap().unwrap(), u64::MAX);
        }

        fixture.insert(DIM, TAG, &[100]);
        fixture.keyspace.major_compact().unwrap();
        fixture.db.persist(PersistMode::SyncAll).unwrap();

        assert!(fixture.forward(TAG, 0, u64::MAX).contains(&100));
    }

    #[test]
    fn snapshot_holds_across_a_write() {
        let fixture = fixture();
        let held = fixture.iter(TAG, 0, u64::MAX);

        fixture.insert(DIM, TAG, &[100]);

        let values = held.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(values, ORACLE);
        assert!(fixture.forward(TAG, 0, u64::MAX).contains(&100));
    }

    /// Successive pages, each from a fresh snapshot and resumed past the last
    /// slot read, cover the tag exactly once in either direction.
    #[test]
    fn pages_resume_without_gaps_or_repeats() {
        let fixture = fixture();
        let mut reversed = ORACLE;
        reversed.reverse();

        for (newest_first, expected) in [(false, ORACLE), (true, reversed)] {
            let (mut start, mut end) = (0, u64::MAX);
            let mut all = Vec::new();

            loop {
                let mut iter = fixture.iter(TAG, start, end);
                let page = (0..3)
                    .map_while(|_| {
                        if newest_first {
                            iter.next_back()
                        } else {
                            iter.next()
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .unwrap();
                let Some(&cursor) = page.last() else { break };
                all.extend(page);

                match (newest_first, cursor) {
                    (false, u64::MAX) | (true, 0) => break,
                    (false, _) => start = cursor + 1,
                    (true, _) => end = cursor - 1,
                }
            }

            assert_eq!(all, expected);
        }
    }
}
