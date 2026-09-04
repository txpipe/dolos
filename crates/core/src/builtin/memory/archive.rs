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
//!   gets from its `[ns_hash:8][log_key:40]` prefix.
//!
//! ## Ephemeral by design
//!
//! Nothing here persists and nothing here is sized for a chain: the whole
//! archive is resident, bodies included, and the block iterator materializes
//! its range before yielding. That is the trade this backend makes — it exists
//! for devnets, tooling and tests, where it is the `in_memory` archive
//! variant, the test harness's store, and the oracle the disk backend's
//! conformance suite is checked against.

use std::collections::{BTreeMap, VecDeque};
use std::ops::{Bound, Range};
use std::sync::{Arc, Mutex, RwLock};

use pallas::ledger::traverse::MultiEraBlock;

use crate::archive::{ArchiveError, ArchiveStore, ArchiveWriter, LogKey, Skippable};
use crate::{
    BlockBody, BlockSlot, ChainPoint, EntityValue, Namespace, RawBlock, StateSchema, TemporalKey,
};

/// The store's whole contents, guarded as one unit so a commit is atomic.
#[derive(Default)]
struct Tables {
    /// Every body written to a slot, oldest first. A slot with no bodies left
    /// is removed rather than kept empty, so the first and last keys are
    /// always real chain positions.
    blocks: BTreeMap<BlockSlot, Vec<BlockBody>>,
    logs: BTreeMap<(Namespace, LogKey), EntityValue>,
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
}

impl ArchiveWriter for MemoryArchiveWriter {
    fn apply(&self, point: &ChainPoint, block: &RawBlock) -> Result<(), ArchiveError> {
        self.push(Op::Apply(point.clone(), block.clone()))
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
            }
        }

        Ok(())
    }
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
}
