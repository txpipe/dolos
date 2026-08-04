//! In-memory index store backed by ordered maps.
//!
//! Same idea as the state store beside it: the disk backends encode
//! `[dim_hash][key][slot]` triples into one flat keyspace, and an ordered map
//! keyed on the triple itself is that keyspace without the encoding.
//!
//! The payoff is sharpest for archive tags. Their traversal contract is
//! "sorted by `(dimension, key_hash, slot)`, dimension compared as a string",
//! which the disk stores can only reach by walking a caller-supplied dimension
//! list in name order — on disk the leading component is a *hash* of the
//! dimension, whose order is unrelated. Keyed on the triple, that contract is
//! the map's own ordering and holds by construction.
//!
//! ## Stored key form
//!
//! Tags are stored under [`key_hash`], not under their logical key, exactly as
//! the disk backends store them. Keeping the logical key would be a divergence,
//! not an improvement: [`IndexWriter::append_prehashed`] delivers records that
//! carry only the stored form, so a store that indexed anything else could not
//! answer a logical-key query about a restored record.
//!
//! ## Ephemeral by design
//!
//! Nothing persists, and the record iterators materialize their range into an
//! owned buffer rather than streaming it. See the state store's module docs for
//! what that costs and why it is the right trade here.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::Range;
use std::sync::{Arc, Mutex, RwLock};

use crate::indexes::{
    key_hash, ArchiveIndexDelta, ExactKind, ExactRecord, IndexDelta, IndexError, IndexRecord,
    IndexStore, IndexWriter, KeyHash, TagDimension, TagRecord,
};
use crate::{BlockSlot, ChainPoint, TxoRef, UtxoSet};

/// A stored archive tag: the traversal contract's sort key, used as the key.
type ArchiveTag = (Cow<'static, str>, KeyHash, BlockSlot);

/// The store's whole contents, guarded as one unit so a commit is atomic.
#[derive(Default, Clone)]
struct Tables {
    cursor: Option<ChainPoint>,
    /// Live UTxOs per `(dimension, logical key)`. Unlike archive tags these
    /// keep the logical key: the query takes one, the records never leave the
    /// store, and the set is mutable rather than append-only.
    utxo_tags: BTreeMap<(TagDimension, Vec<u8>), HashSet<TxoRef>>,
    archive_tags: BTreeSet<ArchiveTag>,
    exact: BTreeMap<(ExactKind, Vec<u8>), BlockSlot>,
}

/// A single mutation, recorded by a writer and replayed at commit. See the
/// state store's `Op` for why the writer keeps an ordered log.
enum Op {
    SetCursor(ChainPoint),
    InsertUtxoTag(TagDimension, Vec<u8>, TxoRef),
    RemoveUtxoTag(TagDimension, Vec<u8>, TxoRef),
    InsertArchiveTag(ArchiveTag),
    RemoveArchiveTag(ArchiveTag),
    InsertExact(ExactKind, Vec<u8>, BlockSlot),
    RemoveExact(ExactKind, Vec<u8>),
}

fn poisoned() -> IndexError {
    IndexError::DbError("index store lock poisoned".into())
}

/// Index store held entirely in memory.
///
/// Cloning shares the underlying tables — a clone is another handle on the same
/// store, not a copy of it.
#[derive(Clone, Default)]
pub struct MemoryIndexStore {
    tables: Arc<RwLock<Tables>>,
}

impl MemoryIndexStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Present for symmetry with the disk backends. Nothing to drain.
    pub fn shutdown(&self) -> Result<(), IndexError> {
        Ok(())
    }
}

/// Writer that accumulates its mutations privately and merges them under a
/// single write lock at [`IndexWriter::commit`].
pub struct MemoryIndexWriter {
    store: MemoryIndexStore,
    ops: Mutex<Vec<Op>>,
}

impl MemoryIndexWriter {
    fn archive_tags_of(block: &ArchiveIndexDelta) -> impl Iterator<Item = ArchiveTag> + '_ {
        block.tags.iter().filter_map(|tag| {
            key_hash(tag.dimension, &tag.key)
                .map(|hash| (Cow::Borrowed(tag.dimension), hash, block.slot))
        })
    }

    fn exact_keys_of(block: &ArchiveIndexDelta) -> impl Iterator<Item = (ExactKind, Vec<u8>)> + '_ {
        let hash = (!block.block_hash.is_empty())
            .then(|| (ExactKind::BlockHash, block.block_hash.clone()));

        let number = block
            .block_number
            .map(|n| (ExactKind::BlockNumber, n.to_be_bytes().to_vec()));

        let txs = block
            .tx_hashes
            .iter()
            .map(|hash| (ExactKind::TxHash, hash.clone()));

        hash.into_iter().chain(number).chain(txs)
    }
}

impl IndexWriter for MemoryIndexWriter {
    fn apply(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        let mut ops = self.ops.lock().map_err(|_| poisoned())?;

        for (txo, tags) in &delta.utxo.produced {
            for tag in tags {
                ops.push(Op::InsertUtxoTag(
                    tag.dimension,
                    tag.key.clone(),
                    txo.clone(),
                ));
            }
        }

        for (txo, tags) in &delta.utxo.consumed {
            for tag in tags {
                ops.push(Op::RemoveUtxoTag(
                    tag.dimension,
                    tag.key.clone(),
                    txo.clone(),
                ));
            }
        }

        for block in &delta.archive {
            for (kind, key) in Self::exact_keys_of(block) {
                ops.push(Op::InsertExact(kind, key, block.slot));
            }

            for tag in Self::archive_tags_of(block) {
                ops.push(Op::InsertArchiveTag(tag));
            }
        }

        ops.push(Op::SetCursor(delta.cursor.clone()));

        Ok(())
    }

    /// The exact inverse of [`Self::apply`], blocks walked back-to-front. The
    /// cursor is deliberately untouched: an undo reverses index content, and
    /// where the chain now sits is the caller's to declare.
    fn undo(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        let mut ops = self.ops.lock().map_err(|_| poisoned())?;

        for (txo, tags) in &delta.utxo.produced {
            for tag in tags {
                ops.push(Op::RemoveUtxoTag(
                    tag.dimension,
                    tag.key.clone(),
                    txo.clone(),
                ));
            }
        }

        for (txo, tags) in &delta.utxo.consumed {
            for tag in tags {
                ops.push(Op::InsertUtxoTag(
                    tag.dimension,
                    tag.key.clone(),
                    txo.clone(),
                ));
            }
        }

        for block in delta.archive.iter().rev() {
            for (kind, key) in Self::exact_keys_of(block) {
                ops.push(Op::RemoveExact(kind, key));
            }

            for tag in Self::archive_tags_of(block) {
                ops.push(Op::RemoveArchiveTag(tag));
            }
        }

        Ok(())
    }

    fn append_prehashed(&self, records: &[IndexRecord]) -> Result<(), IndexError> {
        let mut ops = self.ops.lock().map_err(|_| poisoned())?;

        for record in records {
            match record {
                IndexRecord::Tag(tag) => ops.push(Op::InsertArchiveTag((
                    tag.dimension.clone(),
                    tag.key_hash,
                    tag.slot,
                ))),
                IndexRecord::Exact(exact) => {
                    // A wrong-width key can only come from an external source —
                    // the delta path is type-enforced — and would land as an
                    // entry no lookup could ever reach.
                    let expected = exact.kind.key_len();
                    if exact.key.len() != expected {
                        return Err(IndexError::CodecError(format!(
                            "exact record of kind {} has a {}-byte key, expected {expected}",
                            exact.kind,
                            exact.key.len(),
                        )));
                    }

                    ops.push(Op::InsertExact(exact.kind, exact.key.clone(), exact.slot));
                }
            }
        }

        Ok(())
    }

    fn commit(self) -> Result<(), IndexError> {
        let ops = self.ops.into_inner().map_err(|_| poisoned())?;

        let mut tables = self.store.tables.write().map_err(|_| poisoned())?;

        for op in ops {
            match op {
                Op::SetCursor(cursor) => tables.cursor = Some(cursor),
                Op::InsertUtxoTag(dimension, key, txo) => {
                    tables
                        .utxo_tags
                        .entry((dimension, key))
                        .or_default()
                        .insert(txo);
                }
                Op::RemoveUtxoTag(dimension, key, txo) => {
                    if let std::collections::btree_map::Entry::Occupied(mut entry) =
                        tables.utxo_tags.entry((dimension, key))
                    {
                        entry.get_mut().remove(&txo);
                        if entry.get().is_empty() {
                            entry.remove();
                        }
                    }
                }
                Op::InsertArchiveTag(tag) => {
                    tables.archive_tags.insert(tag);
                }
                Op::RemoveArchiveTag(tag) => {
                    tables.archive_tags.remove(&tag);
                }
                Op::InsertExact(kind, key, slot) => {
                    tables.exact.insert((kind, key), slot);
                }
                Op::RemoveExact(kind, key) => {
                    tables.exact.remove(&(kind, key));
                }
            }
        }

        Ok(())
    }
}

/// Slot iterator over a materialized range.
pub struct MemorySlotIter(std::vec::IntoIter<BlockSlot>);

impl Iterator for MemorySlotIter {
    type Item = Result<BlockSlot, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Ok)
    }
}

impl DoubleEndedIterator for MemorySlotIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(Ok)
    }
}

/// Archive tag iterator over a materialized range.
pub struct MemoryTagIter(std::vec::IntoIter<TagRecord>);

impl Iterator for MemoryTagIter {
    type Item = Result<TagRecord, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Ok)
    }
}

/// Exact-match record iterator over a materialized range.
pub struct MemoryExactIter(std::vec::IntoIter<ExactRecord>);

impl Iterator for MemoryExactIter {
    type Item = Result<ExactRecord, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Ok)
    }
}

/// Every tag stored under `dimension`, whatever its key hash or slot.
fn dimension_span(dimension: TagDimension) -> std::ops::RangeInclusive<ArchiveTag> {
    let low = (Cow::Borrowed(dimension), [u8::MIN; 8], BlockSlot::MIN);
    let high = (Cow::Borrowed(dimension), [u8::MAX; 8], BlockSlot::MAX);
    low..=high
}

impl IndexStore for MemoryIndexStore {
    type Writer = MemoryIndexWriter;
    type SlotIter = MemorySlotIter;
    type TagIter = MemoryTagIter;
    type ExactIter = MemoryExactIter;

    fn start_writer(&self) -> Result<Self::Writer, IndexError> {
        Ok(MemoryIndexWriter {
            store: self.clone(),
            ops: Mutex::new(Vec::new()),
        })
    }

    fn initialize_schema(&self) -> Result<(), IndexError> {
        Ok(())
    }

    /// Merges this store's contents into `target`, the same way the disk
    /// backends do: a copy adds, it does not replace.
    fn copy(&self, target: &Self) -> Result<(), IndexError> {
        // Cloned and released before touching the target, so copying a store
        // onto itself is a no-op rather than a deadlock.
        let source = self.tables.read().map_err(|_| poisoned())?.clone();

        let mut target = target.tables.write().map_err(|_| poisoned())?;

        if let Some(cursor) = source.cursor {
            target.cursor = Some(cursor);
        }

        for (key, txos) in source.utxo_tags {
            target.utxo_tags.entry(key).or_default().extend(txos);
        }

        target.archive_tags.extend(source.archive_tags);
        target.exact.extend(source.exact);

        Ok(())
    }

    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;
        Ok(tables.cursor.clone())
    }

    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, IndexError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        let found = tables
            .utxo_tags
            .get(&(dimension, key.to_vec()))
            .cloned()
            .unwrap_or_default();

        Ok(found)
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;
        Ok(tables
            .exact
            .get(&(ExactKind::BlockHash, hash.to_vec()))
            .copied())
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, IndexError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;
        Ok(tables
            .exact
            .get(&(ExactKind::BlockNumber, number.to_be_bytes().to_vec()))
            .copied())
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;
        Ok(tables
            .exact
            .get(&(ExactKind::TxHash, hash.to_vec()))
            .copied())
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        let Some(hash) = key_hash(dimension, key) else {
            return Err(IndexError::CodecError(format!(
                "{dimension} key must be 8 bytes, got {}",
                key.len(),
            )));
        };

        if start > end {
            return Ok(MemorySlotIter(Vec::new().into_iter()));
        }

        let tables = self.tables.read().map_err(|_| poisoned())?;

        let low = (Cow::Borrowed(dimension), hash, start);
        let high = (Cow::Borrowed(dimension), hash, end);

        let slots: Vec<BlockSlot> = tables
            .archive_tags
            .range(low..=high)
            .map(|(_, _, slot)| *slot)
            .collect();

        Ok(MemorySlotIter(slots.into_iter()))
    }

    fn iter_archive_tags(
        &self,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Result<Self::TagIter, IndexError> {
        // Sorted and deduplicated here rather than trusted from the caller, so
        // the ordering contract holds whatever order the list arrived in.
        let mut dimensions = dimensions.to_vec();
        dimensions.sort_unstable();
        dimensions.dedup();

        let tables = self.tables.read().map_err(|_| poisoned())?;

        let records: Vec<TagRecord> = dimensions
            .into_iter()
            .flat_map(|dimension| {
                tables
                    .archive_tags
                    .range(dimension_span(dimension))
                    .filter(|(_, _, slot)| slots.contains(slot))
                    .map(move |(_, hash, slot)| TagRecord::new(dimension, *hash, *slot))
            })
            .collect();

        Ok(MemoryTagIter(records.into_iter()))
    }

    fn iter_exact_records(&self, slots: Range<BlockSlot>) -> Result<Self::ExactIter, IndexError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        // `ExactKind`'s ordering is its name ordering, so the map's own order
        // is the `(kind, key)` contract.
        let records: Vec<ExactRecord> = tables
            .exact
            .iter()
            .filter(|(_, slot)| slots.contains(slot))
            .map(|((kind, key), slot)| ExactRecord::new(*kind, key.clone(), *slot))
            .collect();

        Ok(MemoryExactIter(records.into_iter()))
    }
}
