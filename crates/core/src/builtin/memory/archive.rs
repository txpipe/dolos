//! In-memory archive store backed by ordered maps.
//!
//! The persistent archive splits itself in two: block bodies go to flat
//! segment files and an index maps a slot to the locations it holds. That
//! split exists because a body is large and an index row is not — a
//! distinction an in-process map does not have. So this backend keeps the
//! bodies themselves under the slot:
//!
//! - blocks live in one map keyed on the slot, holding every body written to
//!   that slot in arrival order, which is chain order — a Byron epoch-boundary
//!   block and the first main block of the epoch it opens share a slot, and the
//!   main block arrives second;
//! - logs live in one map keyed `(namespace, log_key)`, which makes a
//!   namespaced range scan a plain [`BTreeMap::range`] — the same shape fjall
//!   gets from its `[ns_hash:8][log_key:40]` prefix;
//! - the archive tags live in one set keyed on the traversal contract's own
//!   sort key `(dimension, key_hash, slot)`, and the exact lookups in one map
//!   keyed `(kind, key)`. The disk backend encodes `[dim_hash][key][slot]`
//!   triples into a flat keyspace and can only reach the contract's order by
//!   walking a caller-supplied dimension list in name order; keyed on the
//!   triple, that order is the map's own and holds by construction;
//! - the stake address log lives in two maps: the ordered entries per stake
//!   credential, keyed `(slot, order, address)` so a page read is a walk from
//!   either end, and a membership map from `(stake, address)` to the pair's
//!   first appearance, which is the write-path probe and the undo check.
//!
//! ## Stored key form
//!
//! Tags are stored under [`key_hash`], not under their logical key, exactly as
//! the disk backend stores them. Keeping the logical key would be a
//! divergence, not an improvement: [`ArchiveWriter::append_prehashed`]
//! delivers records that carry only the stored form, so a store that indexed
//! anything else could not answer a logical-key query about a restored record.
//!
//! ## Ephemeral by design
//!
//! Nothing here persists and nothing here is sized for a chain: the whole
//! archive is resident, bodies included, and the block iterator materializes
//! its range before yielding. That is the trade this backend makes — it exists
//! for devnets, tooling and tests, where it is the `in_memory` archive
//! variant, the test harness's store, and the oracle the disk backend's
//! conformance suite is checked against.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::{Bound, Range};
use std::sync::{Arc, Mutex, RwLock};

use pallas::ledger::traverse::MultiEraBlock;

use crate::archive::{ArchiveError, ArchiveStore, ArchiveWriter, LogKey, Skippable};
use crate::indexes::{
    key_hash, ArchiveIndexDelta, ExactKind, ExactRecord, IndexRecord, KeyHash,
    StakeAddressAppearance, TagDimension, TagRecord, MAX_EXACT_KEY_LEN,
};
use crate::{
    BlockBody, BlockSlot, ChainPoint, EntityValue, Namespace, RawBlock, StateSchema, TemporalKey,
};

/// A stored archive tag: the traversal contract's sort key, used as the key.
type ArchiveTag = (Cow<'static, str>, KeyHash, BlockSlot);

/// An exact key, zero-padded to the widest kind — the same inline shape
/// [`ExactRecord`] carries, so a record goes in and comes back out without a
/// heap allocation on either side.
type ExactKey = [u8; MAX_EXACT_KEY_LEN];

/// One ordered entry of the stake address log: `(slot, order, address)`.
type StakeLogEntry = (BlockSlot, u32, Vec<u8>);

/// A `(stake, address)` pair, the membership key of the stake address log.
type StakeLogPair = (Vec<u8>, Vec<u8>);

/// A key's stored form, or `None` unless it is exactly the width its kind
/// requires.
///
/// Refusing rather than padding or truncating to fit: a key adjusted to fit
/// aliases with the key it was adjusted into, so a 33-byte block hash and the
/// 32-byte hash that is its prefix would answer each other's lookups.
/// `ArchiveIndexDelta` holds its hashes as `Vec<u8>`, so nothing upstream
/// enforces the width.
fn exact_key(kind: ExactKind, key: &[u8]) -> Option<ExactKey> {
    if key.len() != kind.key_len() {
        return None;
    }

    let mut buf = [0u8; MAX_EXACT_KEY_LEN];
    buf[..key.len()].copy_from_slice(key);
    Some(buf)
}

/// The store's whole contents, guarded as one unit so a commit is atomic.
#[derive(Default)]
struct Tables {
    /// Every body written to a slot, oldest first. A slot with no bodies left
    /// is removed rather than kept empty, so the first and last keys are
    /// always real chain positions.
    blocks: BTreeMap<BlockSlot, Vec<BlockBody>>,
    logs: BTreeMap<(Namespace, LogKey), EntityValue>,
    archive_tags: BTreeSet<ArchiveTag>,
    /// Keyed on the record's own inline key rather than a `Vec`, so
    /// `iter_exact_records` copies rather than allocates per record.
    exact: BTreeMap<(ExactKind, ExactKey), BlockSlot>,
    /// Stake address log: first appearances ordered `(slot, order, address)`
    /// per stake credential.
    stake_log: BTreeMap<Vec<u8>, BTreeSet<StakeLogEntry>>,
    /// Membership map for the log: each pair's first appearance, which is
    /// also what an undo has to match before it may remove the pair.
    stake_log_pairs: BTreeMap<StakeLogPair, (BlockSlot, u32)>,
    /// Set once the log is complete from genesis; queries answer `None`
    /// until then.
    stake_log_ready: bool,
}

/// A single mutation, recorded by a writer and replayed at commit.
///
/// Keeping the writer's work as an ordered log rather than a merged map is
/// what makes the commit agree with a disk write batch: within one batch, a
/// later operation on a key supersedes an earlier one.
enum Op {
    Apply(ChainPoint, RawBlock),
    WriteLog(Namespace, LogKey, EntityValue),
    Undo(ChainPoint),
    InsertArchiveTag(ArchiveTag),
    RemoveArchiveTag(ArchiveTag),
    InsertExact(ExactKind, ExactKey, BlockSlot),
    RemoveExact(ExactKind, ExactKey),
    InsertStakeAddress(BlockSlot, StakeAddressAppearance),
    RemoveStakeAddress(BlockSlot, StakeAddressAppearance),
}

fn poisoned() -> ArchiveError {
    ArchiveError::InternalError("archive store lock poisoned".into())
}

/// The first log key at `slot`: the bare temporal prefix, zero-padded.
///
/// Both boundary rules are drawn against it — prune keeps rows from here on,
/// truncate drops them — which is the same comparison the disk backends make
/// between a full log key and a bare 8-byte prefix.
fn temporal_bound(slot: BlockSlot) -> LogKey {
    LogKey::from(TemporalKey::from(slot))
}

/// Archive store held entirely in memory.
///
/// Cloning shares the underlying tables, which is what [`ArchiveStore`]'s
/// `Clone` bound means everywhere else: a clone is another handle on the same
/// store, not a copy of it.
#[derive(Clone)]
pub struct MemoryArchiveStore {
    schema: Arc<StateSchema>,
    tables: Arc<RwLock<Tables>>,
}

impl MemoryArchiveStore {
    /// A store over `schema`, which is what a log namespace is checked
    /// against — the archive refuses a namespace it was not opened with, as
    /// the disk backends do.
    pub fn new(schema: StateSchema) -> Self {
        Self {
            schema: Arc::new(schema),
            tables: Arc::new(RwLock::new(Tables::default())),
        }
    }

    /// Present for symmetry with the disk backends, which need it to drain
    /// background work before the process exits. There is nothing to drain
    /// here.
    pub fn shutdown(&self) -> Result<(), ArchiveError> {
        Ok(())
    }

    /// Forget the stake address log's ready marker, so queries answer `None`
    /// and callers take their archive-scan fallback.
    ///
    /// For tests only: the toy domain runs genesis, which marks the log, and
    /// a test of the fallback path needs a store where it is not marked. The
    /// disk backend has no such switch — there the marker is genesis's alone.
    pub fn clear_stake_log_ready(&self) -> Result<(), ArchiveError> {
        let mut tables = self.tables.write().map_err(|_| poisoned())?;
        tables.stake_log_ready = false;
        Ok(())
    }

    fn check_namespace(&self, ns: Namespace) -> Result<(), ArchiveError> {
        if !self.schema.contains_key(ns) {
            return Err(ArchiveError::NamespaceNotFound(ns));
        }

        Ok(())
    }
}

/// Writer that accumulates its mutations privately and merges them under a
/// single write lock at [`ArchiveWriter::commit`].
///
/// The lock is not held for the writer's lifetime: readers keep running while
/// a writer is open, and see nothing of its work until the commit lands — the
/// same visibility a disk transaction gives.
pub struct MemoryArchiveWriter {
    store: MemoryArchiveStore,
    ops: Mutex<Vec<Op>>,
}

impl MemoryArchiveWriter {
    fn push(&self, op: Op) -> Result<(), ArchiveError> {
        self.ops.lock().map_err(|_| poisoned())?.push(op);
        Ok(())
    }

    fn archive_tags_of(block: &ArchiveIndexDelta) -> impl Iterator<Item = ArchiveTag> + '_ {
        block.tags.iter().filter_map(|tag| {
            key_hash(tag.dimension, &tag.key)
                .map(|hash| (Cow::Borrowed(tag.dimension), hash, block.slot))
        })
    }

    /// The exact entries a block delta writes.
    ///
    /// Every key is width-checked here, because the delta path is *not*
    /// type-enforced: `ArchiveIndexDelta` holds its block and transaction
    /// hashes as `Vec<u8>`. A wrong-width hash is a malformed block, and
    /// refusing the batch beats dropping the entry — a block whose hash did not
    /// land is a block no hash lookup can reach, and an index that quietly
    /// omits it looks complete.
    fn exact_keys_of(
        block: &ArchiveIndexDelta,
    ) -> impl Iterator<Item = Result<(ExactKind, ExactKey), ArchiveError>> + '_ {
        let hash = (!block.block_hash.is_empty())
            .then_some((ExactKind::BlockHash, block.block_hash.as_slice()));

        let number = block.block_number.map(|n| (ExactKind::BlockNumber, n));

        let txs = block
            .tx_hashes
            .iter()
            .map(|hash| (ExactKind::TxHash, hash.as_slice()));

        let hashes = hash.into_iter().chain(txs).map(|(kind, key)| {
            exact_key(kind, key).map(|key| (kind, key)).ok_or_else(|| {
                ArchiveError::InternalError(format!(
                    "exact entry of kind {kind} has a {}-byte key, expected {}",
                    key.len(),
                    kind.key_len(),
                ))
            })
        });

        // A block number is type-enforced eight bytes, so it cannot be
        // malformed the way a hash can.
        let number = number.map(|(kind, n)| {
            Ok((
                kind,
                exact_key(kind, &n.to_be_bytes()).expect("a u64 is the block-number key width"),
            ))
        });

        hashes.chain(number)
    }
}

impl ArchiveWriter for MemoryArchiveWriter {
    fn apply(&self, point: &ChainPoint, block: &RawBlock) -> Result<(), ArchiveError> {
        self.push(Op::Apply(point.clone(), block.clone()))
    }

    fn apply_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError> {
        let mut ops = self.ops.lock().map_err(|_| poisoned())?;

        for block in deltas {
            for entry in Self::exact_keys_of(block) {
                let (kind, key) = entry?;
                ops.push(Op::InsertExact(kind, key, block.slot));
            }

            for tag in Self::archive_tags_of(block) {
                ops.push(Op::InsertArchiveTag(tag));
            }

            for appearance in &block.stake_addresses {
                ops.push(Op::InsertStakeAddress(block.slot, appearance.clone()));
            }
        }

        Ok(())
    }

    /// The exact inverse of [`Self::apply_index`], blocks walked
    /// back-to-front.
    fn undo_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError> {
        let mut ops = self.ops.lock().map_err(|_| poisoned())?;

        for block in deltas.iter().rev() {
            for entry in Self::exact_keys_of(block) {
                let (kind, key) = entry?;
                ops.push(Op::RemoveExact(kind, key));
            }

            for tag in Self::archive_tags_of(block) {
                ops.push(Op::RemoveArchiveTag(tag));
            }

            for appearance in block.stake_addresses.iter().rev() {
                ops.push(Op::RemoveStakeAddress(block.slot, appearance.clone()));
            }
        }

        Ok(())
    }

    fn append_prehashed(
        &self,
        records: impl IntoIterator<Item = IndexRecord>,
    ) -> Result<(), ArchiveError> {
        let mut ops = self.ops.lock().map_err(|_| poisoned())?;

        for record in records {
            match record {
                IndexRecord::Tag(tag) => ops.push(Op::InsertArchiveTag((
                    tag.dimension,
                    tag.key_hash,
                    tag.slot,
                ))),
                // The width is already checked: `ExactRecord` cannot be
                // constructed with a key that does not match its kind.
                IndexRecord::Exact(exact) => ops.push(Op::InsertExact(
                    exact.kind,
                    exact_key(exact.kind, exact.key()).expect("a record's key is its kind's width"),
                    exact.slot,
                )),
            }
        }

        Ok(())
    }

    fn write_log(
        &self,
        ns: Namespace,
        key: &LogKey,
        value: &EntityValue,
    ) -> Result<(), ArchiveError> {
        // Resolved here so an unknown namespace fails at the call that names
        // it rather than at commit.
        self.store.check_namespace(ns)?;

        self.push(Op::WriteLog(ns, key.clone(), value.clone()))
    }

    fn undo(&self, point: &ChainPoint) -> Result<(), ArchiveError> {
        self.push(Op::Undo(point.clone()))
    }

    fn commit(self) -> Result<(), ArchiveError> {
        let ops = self.ops.into_inner().map_err(|_| poisoned())?;

        let mut tables = self.store.tables.write().map_err(|_| poisoned())?;

        // Reborrow so the match arms can hold disjoint field borrows.
        let tables = &mut *tables;

        for op in ops {
            match op {
                // An identical body means this block is being written again
                // (a resumed restore rewriting the layer it was in the middle
                // of): the slot already holds it, so nothing changes.
                // Anything else is a second block at the same slot, and it
                // arrived last.
                Op::Apply(point, block) => {
                    let bodies = tables.blocks.entry(point.slot()).or_default();

                    if !bodies.iter().any(|body| body == block.as_ref()) {
                        bodies.push(block.as_ref().clone());
                    }
                }
                Op::WriteLog(ns, key, value) => {
                    tables.logs.insert((ns, key), value);
                }
                // A rollback walks the chain backwards, so at a slot holding
                // more than one block the one to remove is the last to
                // arrive, and the slot survives until its last block is gone.
                Op::Undo(point) => {
                    let slot = point.slot();

                    if let Some(bodies) = tables.blocks.get_mut(&slot) {
                        bodies.pop();

                        if bodies.is_empty() {
                            tables.blocks.remove(&slot);
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
                // Only the first appearance of a pair is kept; the membership
                // map is the probe, and it sees this batch's earlier inserts.
                Op::InsertStakeAddress(slot, app) => {
                    let pair = (app.stake.clone(), app.address.clone());

                    if let std::collections::btree_map::Entry::Vacant(entry) =
                        tables.stake_log_pairs.entry(pair)
                    {
                        entry.insert((slot, app.order));
                        tables.stake_log.entry(app.stake).or_default().insert((
                            slot,
                            app.order,
                            app.address,
                        ));
                    }
                }
                // Removed only when the undone block is the pair's stored
                // first appearance; a pair seen earlier stays untouched.
                Op::RemoveStakeAddress(slot, app) => {
                    let pair = (app.stake.clone(), app.address.clone());

                    let Some(&(first_slot, order)) = tables.stake_log_pairs.get(&pair) else {
                        continue;
                    };

                    if first_slot != slot {
                        continue;
                    }

                    tables.stake_log_pairs.remove(&pair);

                    if let Some(set) = tables.stake_log.get_mut(&app.stake) {
                        set.remove(&(slot, order, app.address));

                        if set.is_empty() {
                            tables.stake_log.remove(&app.stake);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Slot iterator over a materialized range.
pub struct MemorySlotIter(std::vec::IntoIter<BlockSlot>);

impl Iterator for MemorySlotIter {
    type Item = Result<BlockSlot, ArchiveError>;

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
    type Item = Result<TagRecord, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Ok)
    }
}

/// Exact-match record iterator over a materialized range.
pub struct MemoryExactIter(std::vec::IntoIter<ExactRecord>);

impl Iterator for MemoryExactIter {
    type Item = Result<ExactRecord, ArchiveError>;

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

/// Block iterator over a materialized range.
///
/// Unlike the disk backends this is not lazy — see the module docs. It is
/// still a point-in-time view: the buffer is filled under a read lock, so a
/// concurrent writer cannot tear it. Draining from both ends is what keeps a
/// forward and a backward walk from yielding a block twice or dropping one
/// where they meet.
pub struct MemoryBlockIter(VecDeque<(BlockSlot, BlockBody)>);

impl Iterator for MemoryBlockIter {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

impl DoubleEndedIterator for MemoryBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.pop_back()
    }
}

impl Skippable for MemoryBlockIter {
    fn skip_forward(&mut self, n: usize) {
        for _ in 0..n {
            if self.0.pop_front().is_none() {
                break;
            }
        }
    }

    fn skip_backward(&mut self, n: usize) {
        for _ in 0..n {
            if self.0.pop_back().is_none() {
                break;
            }
        }
    }
}

/// Log iterator over a materialized range.
pub struct MemoryLogIter(std::vec::IntoIter<(LogKey, EntityValue)>);

impl Iterator for MemoryLogIter {
    type Item = Result<(LogKey, EntityValue), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Ok)
    }
}

impl ArchiveStore for MemoryArchiveStore {
    type BlockIter<'a> = MemoryBlockIter;
    type Writer = MemoryArchiveWriter;
    type LogIter = MemoryLogIter;
    type EntityValueIter = std::iter::Empty<Result<EntityValue, ArchiveError>>;
    type SlotIter = MemorySlotIter;
    type TagIter = MemoryTagIter;
    type ExactIter = MemoryExactIter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        Ok(MemoryArchiveWriter {
            store: self.clone(),
            ops: Mutex::new(Vec::new()),
        })
    }

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<EntityValue>>, ArchiveError> {
        self.check_namespace(ns)?;

        let tables = self.tables.read().map_err(|_| poisoned())?;

        let values = keys
            .iter()
            .map(|key| tables.logs.get(&(ns, (*key).clone())).cloned())
            .collect();

        Ok(values)
    }

    fn iter_logs(
        &self,
        ns: Namespace,
        range: Range<LogKey>,
    ) -> Result<Self::LogIter, ArchiveError> {
        self.check_namespace(ns)?;

        // An inverted range matches nothing on the disk backends, whose keys
        // are bytes; `BTreeMap::range` panics on one, so it is caught here
        // rather than turned into a difference between backends.
        if range.start > range.end {
            return Ok(MemoryLogIter(Vec::new().into_iter()));
        }

        let tables = self.tables.read().map_err(|_| poisoned())?;

        let rows: Vec<_> = tables
            .logs
            .range((ns, range.start)..(ns, range.end))
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect();

        Ok(MemoryLogIter(rows.into_iter()))
    }

    /// The block the slot resolves to: the last one written to it.
    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        Ok(tables
            .blocks
            .get(slot)
            .and_then(|bodies| bodies.last())
            .cloned())
    }

    /// Every block the archive holds at `slot`, in chain order.
    fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, ArchiveError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        Ok(tables.blocks.get(slot).cloned().unwrap_or_default())
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        if let (Some(from), Some(to)) = (from, to) {
            if from > to {
                return Ok(MemoryBlockIter(VecDeque::new()));
            }
        }

        let start = from.map_or(Bound::Unbounded, Bound::Included);
        let end = to.map_or(Bound::Unbounded, Bound::Excluded);

        let tables = self.tables.read().map_err(|_| poisoned())?;

        let items = tables
            .blocks
            .range((start, end))
            .flat_map(|(slot, bodies)| bodies.iter().map(move |body| (*slot, body.clone())))
            .collect();

        Ok(MemoryBlockIter(items))
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        for point in intersect {
            let ChainPoint::Specific(slot, hash) = point else {
                return Ok(Some(ChainPoint::Origin));
            };

            // A slot can hold more than one block, and an intersect names one
            // of them by hash, so every block recorded there is a candidate.
            for body in self.get_blocks_by_slot(slot)? {
                let decoded =
                    MultiEraBlock::decode(&body).map_err(ArchiveError::BlockDecodingError)?;

                if decoded.hash().eq(hash) {
                    return Ok(Some(ChainPoint::Specific(decoded.slot(), decoded.hash())));
                }
            }
        }

        Ok(None)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        let tip = tables
            .blocks
            .last_key_value()
            .and_then(|(slot, bodies)| bodies.last().map(|body| (*slot, body.clone())));

        Ok(tip)
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError> {
        let mut tables = self.tables.write().map_err(|_| poisoned())?;

        let Some((&first, _)) = tables.blocks.first_key_value() else {
            return Ok(true);
        };

        let Some((&last, _)) = tables.blocks.last_key_value() else {
            return Ok(true);
        };

        let excess = last.saturating_sub(first).saturating_sub(max_slots);

        if excess == 0 {
            return Ok(true);
        }

        let (done, capped) = match max_prune {
            Some(max) => (excess <= max, core::cmp::min(excess, max)),
            None => (true, excess),
        };

        let prune_before = first.saturating_add(capped);
        let cutoff = temporal_bound(prune_before);

        tables.blocks.retain(|slot, _| *slot >= prune_before);
        tables.logs.retain(|(_, key), _| *key >= cutoff);

        // The index entries of a pruned block go with it. The disk backend
        // amortizes this across rounds; here a walk of the maps is the cost
        // of a retain, so every round does it.
        tables
            .archive_tags
            .retain(|(_, _, slot)| *slot >= prune_before);
        tables.exact.retain(|_, slot| *slot >= prune_before);

        // The stake address log is left alone, as on the disk backend: its
        // entries are first appearances, so removing one below the cutoff
        // would drop an address the account may still use.

        Ok(done)
    }

    /// Drop everything the archive holds after `after`.
    ///
    /// The cut is by slot: the block at `after`'s slot survives (including a
    /// second block sharing that slot), while log rows *at* the slot go with
    /// the cut — the boundary the disk backends draw by comparing full log
    /// keys against the bare 8-byte temporal prefix.
    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        let slot = after.slot();
        let cutoff = temporal_bound(slot);

        let mut tables = self.tables.write().map_err(|_| poisoned())?;

        tables.blocks.retain(|s, _| *s <= slot);
        tables.logs.retain(|(_, key), _| *key < cutoff);

        Ok(())
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        // A key that could not have been stored cannot be found, so a
        // wrong-width query is a miss rather than an error.
        let Some(key) = exact_key(ExactKind::BlockHash, hash) else {
            return Ok(None);
        };

        let tables = self.tables.read().map_err(|_| poisoned())?;
        Ok(tables.exact.get(&(ExactKind::BlockHash, key)).copied())
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, ArchiveError> {
        // A block number is type-enforced eight bytes, so this cannot fail.
        let key = exact_key(ExactKind::BlockNumber, &number.to_be_bytes())
            .expect("a u64 is the block-number key width");

        let tables = self.tables.read().map_err(|_| poisoned())?;
        Ok(tables.exact.get(&(ExactKind::BlockNumber, key)).copied())
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        // A key that could not have been stored cannot be found, so a
        // wrong-width query is a miss rather than an error.
        let Some(key) = exact_key(ExactKind::TxHash, hash) else {
            return Ok(None);
        };

        let tables = self.tables.read().map_err(|_| poisoned())?;
        Ok(tables.exact.get(&(ExactKind::TxHash, key)).copied())
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        let Some(hash) = key_hash(dimension, key) else {
            return Err(ArchiveError::InternalError(format!(
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

    fn addresses_by_stake_log(
        &self,
        stake: &[u8],
        offset: usize,
        limit: usize,
        reverse: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, ArchiveError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        if !tables.stake_log_ready {
            return Ok(None);
        }

        let Some(set) = tables.stake_log.get(stake) else {
            return Ok(Some(Vec::new()));
        };

        let pick = |entry: &StakeLogEntry| entry.2.clone();

        let page = if reverse {
            set.iter()
                .rev()
                .skip(offset)
                .take(limit)
                .map(pick)
                .collect()
        } else {
            set.iter().skip(offset).take(limit).map(pick).collect()
        };

        Ok(Some(page))
    }

    fn mark_stake_log_ready(&self) -> Result<(), ArchiveError> {
        let mut tables = self.tables.write().map_err(|_| poisoned())?;
        tables.stake_log_ready = true;
        Ok(())
    }

    fn iter_archive_tags(
        &self,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Result<Self::TagIter, ArchiveError> {
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

    fn iter_exact_records(&self, slots: Range<BlockSlot>) -> Result<Self::ExactIter, ArchiveError> {
        let tables = self.tables.read().map_err(|_| poisoned())?;

        // `ExactKind`'s ordering is its name ordering, so the map's own order
        // is the `(kind, key)` contract.
        let records: Vec<ExactRecord> = tables
            .exact
            .iter()
            .filter(|(_, slot)| slots.contains(slot))
            .map(|((kind, key), slot)| {
                ExactRecord::new(*kind, &key[..kind.key_len()], *slot)
                    .map_err(|e| ArchiveError::InternalError(e.to_string()))
            })
            .collect::<Result<_, _>>()?;

        Ok(MemoryExactIter(records.into_iter()))
    }
}
