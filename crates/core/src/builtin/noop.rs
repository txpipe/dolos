//! No-op storage implementations.
//!
//! These implementations silently accept all writes and return empty results
//! for all reads. Useful when you want to disable certain storage backends
//! (e.g., indexes or archive) while still keeping the system functional.

use std::{marker::PhantomData, ops::Range};

use crate::{
    archive::{ArchiveError, ArchiveStore, ArchiveWriter, LogKey},
    indexes::{IndexDelta, IndexError, IndexStore, IndexWriter, TagDimension},
    BlockBody, BlockSlot, ChainPoint, EntityValue, Namespace, RawBlock, UtxoSet,
};

// ============================================================================
// NoOp Index Store
// ============================================================================

/// No-op index writer that accepts all operations but does nothing.
#[derive(Debug, Default)]
pub struct NoOpIndexWriter;

impl IndexWriter for NoOpIndexWriter {
    fn apply(&self, _delta: &IndexDelta) -> Result<(), IndexError> {
        Ok(())
    }

    fn undo(&self, _delta: &IndexDelta) -> Result<(), IndexError> {
        Ok(())
    }

    fn commit(self) -> Result<(), IndexError> {
        Ok(())
    }
}

/// No-op index store that returns empty results for all queries.
#[derive(Debug, Clone, Default)]
pub struct NoOpIndexStore;

impl NoOpIndexStore {
    pub fn new() -> Self {
        Self
    }

    pub fn shutdown(&self) -> Result<(), IndexError> {
        Ok(())
    }
}

/// Empty iterator for slot queries.
pub struct EmptySlotIter;

impl Iterator for EmptySlotIter {
    type Item = Result<BlockSlot, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl DoubleEndedIterator for EmptySlotIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        None
    }
}

impl IndexStore for NoOpIndexStore {
    type Writer = NoOpIndexWriter;
    type SlotIter = EmptySlotIter;

    fn start_writer(&self) -> Result<Self::Writer, IndexError> {
        Ok(NoOpIndexWriter)
    }

    fn initialize_schema(&self) -> Result<(), IndexError> {
        Ok(())
    }

    fn copy(&self, _target: &Self) -> Result<(), IndexError> {
        Ok(())
    }

    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        Ok(None)
    }

    fn utxos_by_tag(&self, _dimension: TagDimension, _key: &[u8]) -> Result<UtxoSet, IndexError> {
        Ok(UtxoSet::default())
    }

    fn slot_by_block_hash(&self, _hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        Ok(None)
    }

    fn slot_by_block_number(&self, _number: u64) -> Result<Option<BlockSlot>, IndexError> {
        Ok(None)
    }

    fn slot_by_tx_hash(&self, _hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        Ok(None)
    }

    fn slots_by_tag(
        &self,
        _dimension: TagDimension,
        _key: &[u8],
        _start: BlockSlot,
        _end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        Ok(EmptySlotIter)
    }
}

// ============================================================================
// NoOp Archive Store
// ============================================================================

/// No-op archive writer that accepts all operations but does nothing.
pub struct NoOpArchiveWriter<E>(PhantomData<E>);

impl<E> std::fmt::Debug for NoOpArchiveWriter<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoOpArchiveWriter").finish()
    }
}

impl<E> Default for NoOpArchiveWriter<E> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> ArchiveWriter for NoOpArchiveWriter<E> {
    type ChainSpecificError = E;

    fn apply(&self, _point: &ChainPoint, _block: &RawBlock) -> Result<(), ArchiveError<E>> {
        Ok(())
    }

    fn write_log(
        &self,
        _ns: Namespace,
        _key: &LogKey,
        _value: &EntityValue,
    ) -> Result<(), ArchiveError<E>> {
        Ok(())
    }

    fn undo(&self, _point: &ChainPoint) -> Result<(), ArchiveError<E>> {
        Ok(())
    }

    fn commit(self) -> Result<(), ArchiveError<E>> {
        Ok(())
    }
}

/// No-op archive store that returns empty results for all queries.
pub struct NoOpArchiveStore<E>(PhantomData<E>);

impl<E> std::fmt::Debug for NoOpArchiveStore<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NoOpArchiveStore").finish()
    }
}

impl<E> Clone for NoOpArchiveStore<E> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<E> Default for NoOpArchiveStore<E> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> NoOpArchiveStore<E> {
    pub fn new() -> Self {
        Self(PhantomData)
    }

    pub fn shutdown(&self) -> Result<(), ArchiveError<E>> {
        Ok(())
    }
}

/// Empty iterator for block queries.
pub struct EmptyBlockIter;

impl Iterator for EmptyBlockIter {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl DoubleEndedIterator for EmptyBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        None
    }
}

impl crate::archive::Skippable for EmptyBlockIter {
    fn skip_forward(&mut self, _n: usize) {}
    fn skip_backward(&mut self, _n: usize) {}
}

/// Empty iterator for log queries.
pub struct EmptyLogIter<E>(PhantomData<E>);

impl<E> Default for EmptyLogIter<E> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> Iterator for EmptyLogIter<E> {
    type Item = Result<(LogKey, EntityValue), ArchiveError<E>>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

/// Empty iterator for entity value queries.
pub struct EmptyEntityValueIter<E>(PhantomData<E>);

impl<E> Default for EmptyEntityValueIter<E> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<E: std::error::Error + Send + Sync + 'static> Iterator for EmptyEntityValueIter<E> {
    type Item = Result<EntityValue, ArchiveError<E>>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl<E: std::error::Error + Send + Sync + 'static> ArchiveStore for NoOpArchiveStore<E> {
    type ChainSpecificError = E;
    type BlockIter<'a> = EmptyBlockIter;
    type Writer = NoOpArchiveWriter<E>;
    type LogIter = EmptyLogIter<E>;
    type EntityValueIter = EmptyEntityValueIter<E>;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError<E>> {
        Ok(NoOpArchiveWriter::default())
    }

    fn read_logs(
        &self,
        _ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<EntityValue>>, ArchiveError<E>> {
        Ok(vec![None; keys.len()])
    }

    fn iter_logs(
        &self,
        _ns: Namespace,
        _range: Range<LogKey>,
    ) -> Result<Self::LogIter, ArchiveError<E>> {
        Ok(EmptyLogIter::default())
    }

    fn get_block_by_slot(&self, _slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError<E>> {
        Ok(None)
    }

    fn get_range<'a>(
        &self,
        _from: Option<BlockSlot>,
        _to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError<E>> {
        Ok(EmptyBlockIter)
    }

    fn find_intersect(
        &self,
        _intersect: &[ChainPoint],
    ) -> Result<Option<ChainPoint>, ArchiveError<E>> {
        Ok(None)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError<E>> {
        Ok(None)
    }

    fn prune_history(
        &self,
        _max_slots: u64,
        _max_prune: Option<u64>,
    ) -> Result<bool, ArchiveError<E>> {
        // Nothing to prune, always "done"
        Ok(true)
    }

    fn truncate_front(&self, _after: &ChainPoint) -> Result<(), ArchiveError<E>> {
        Ok(())
    }
}
