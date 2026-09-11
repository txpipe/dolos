use std::{
    ops::Range,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use dolos_core::{
    archive::Skippable, ArchiveError, ArchiveStore, BlockBody, BlockSlot, ChainPoint, EntityKey,
    EntityValue, LogKey, Namespace, StateError, StateStore, TagDimension, TxoRef, UtxoMap, UtxoSet,
};
use serde::Serialize;

use crate::toy_domain::ToyStores;

#[derive(Clone, Default)]
pub struct WorkCounters {
    pub log_rows: Arc<AtomicU64>,
    pub log_reads: Arc<AtomicU64>,
    pub block_reads: Arc<AtomicU64>,
    pub decoded_bytes: Arc<AtomicU64>,
    pub exact_lookups: Arc<AtomicU64>,
    pub tag_candidates: Arc<AtomicU64>,
    pub state_entities: Arc<AtomicU64>,
    pub utxo_refs: Arc<AtomicU64>,
    pub utxo_reads: Arc<AtomicU64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct WorkSnapshot {
    pub log_rows: u64,
    pub log_reads: u64,
    pub block_reads: u64,
    pub decoded_bytes: u64,
    pub exact_lookups: u64,
    pub tag_candidates: u64,
    pub state_entities: u64,
    pub utxo_refs: u64,
    pub utxo_reads: u64,
}

impl WorkCounters {
    pub fn snapshot(&self) -> WorkSnapshot {
        WorkSnapshot {
            log_rows: self.log_rows.load(Ordering::Relaxed),
            log_reads: self.log_reads.load(Ordering::Relaxed),
            block_reads: self.block_reads.load(Ordering::Relaxed),
            decoded_bytes: self.decoded_bytes.load(Ordering::Relaxed),
            exact_lookups: self.exact_lookups.load(Ordering::Relaxed),
            tag_candidates: self.tag_candidates.load(Ordering::Relaxed),
            state_entities: self.state_entities.load(Ordering::Relaxed),
            utxo_refs: self.utxo_refs.load(Ordering::Relaxed),
            utxo_reads: self.utxo_reads.load(Ordering::Relaxed),
        }
    }

    pub fn reset(&self) {
        self.log_rows.store(0, Ordering::Relaxed);
        self.log_reads.store(0, Ordering::Relaxed);
        self.block_reads.store(0, Ordering::Relaxed);
        self.decoded_bytes.store(0, Ordering::Relaxed);
        self.exact_lookups.store(0, Ordering::Relaxed);
        self.tag_candidates.store(0, Ordering::Relaxed);
        self.state_entities.store(0, Ordering::Relaxed);
        self.utxo_refs.store(0, Ordering::Relaxed);
        self.utxo_reads.store(0, Ordering::Relaxed);
    }

    fn body(&self, body: &[u8]) {
        self.block_reads.fetch_add(1, Ordering::Relaxed);
        self.decoded_bytes
            .fetch_add(body.len() as u64, Ordering::Relaxed);
    }
}

pub struct CountedIter<Inner> {
    inner: Inner,
    count: Arc<AtomicU64>,
}

impl<Inner: Iterator> Iterator for CountedIter<Inner> {
    type Item = Inner::Item;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        self.count.fetch_add(1, Ordering::Relaxed);
        Some(item)
    }
}

impl<Inner: DoubleEndedIterator> DoubleEndedIterator for CountedIter<Inner> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let item = self.inner.next_back()?;
        self.count.fetch_add(1, Ordering::Relaxed);
        Some(item)
    }
}

pub struct CountedBlocks<Inner> {
    inner: Inner,
    counters: WorkCounters,
}

impl<Inner: Iterator<Item = (BlockSlot, BlockBody)>> Iterator for CountedBlocks<Inner> {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.inner.next()?;
        self.counters.body(&item.1);
        Some(item)
    }
}

impl<Inner: DoubleEndedIterator<Item = (BlockSlot, BlockBody)>> DoubleEndedIterator
    for CountedBlocks<Inner>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        let item = self.inner.next_back()?;
        self.counters.body(&item.1);
        Some(item)
    }
}

impl<Inner: Skippable> Skippable for CountedBlocks<Inner> {
    fn skip_forward(&mut self, count: usize) {
        self.inner.skip_forward(count);
    }

    fn skip_backward(&mut self, count: usize) {
        self.inner.skip_backward(count);
    }
}

#[derive(Clone)]
pub struct MeasuredArchive<Inner> {
    inner: Inner,
    pub counters: WorkCounters,
}

impl<Inner: ArchiveStore> ArchiveStore for MeasuredArchive<Inner> {
    type BlockIter<'a> = CountedBlocks<Inner::BlockIter<'a>>;
    type Writer = Inner::Writer;
    type LogIter = CountedIter<Inner::LogIter>;
    type EntityValueIter = Inner::EntityValueIter;
    type SlotIter = CountedIter<Inner::SlotIter>;
    type TagIter = Inner::TagIter;
    type ExactIter = Inner::ExactIter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        self.inner.start_writer()
    }

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<EntityValue>>, ArchiveError> {
        self.counters
            .log_reads
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        self.inner.read_logs(ns, keys)
    }

    fn iter_logs(
        &self,
        ns: Namespace,
        range: Range<LogKey>,
    ) -> Result<Self::LogIter, ArchiveError> {
        Ok(CountedIter {
            inner: self.inner.iter_logs(ns, range)?,
            count: self.counters.log_rows.clone(),
        })
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        let body = self.inner.get_block_by_slot(slot)?;
        if let Some(body) = &body {
            self.counters.body(body);
        }
        Ok(body)
    }

    fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, ArchiveError> {
        let bodies = self.inner.get_blocks_by_slot(slot)?;
        for body in &bodies {
            self.counters.body(body);
        }
        Ok(bodies)
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        Ok(CountedBlocks {
            inner: self.inner.get_range(from, to)?,
            counters: self.counters.clone(),
        })
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        self.inner.find_intersect(intersect)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        let tip = self.inner.get_tip()?;
        if let Some((_, body)) = &tip {
            self.counters.body(body);
        }
        Ok(tip)
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError> {
        self.inner.prune_history(max_slots, max_prune)
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        self.inner.truncate_front(after)
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        self.counters.exact_lookups.fetch_add(1, Ordering::Relaxed);
        self.inner.slot_by_block_hash(hash)
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        self.counters.exact_lookups.fetch_add(1, Ordering::Relaxed);
        self.inner.slot_by_tx_hash(hash)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, ArchiveError> {
        self.counters.exact_lookups.fetch_add(1, Ordering::Relaxed);
        self.inner.slot_by_block_number(number)
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        Ok(CountedIter {
            inner: self.inner.slots_by_tag(dimension, key, start, end)?,
            count: self.counters.tag_candidates.clone(),
        })
    }

    /// One tag candidate per address the page yields: the log stands in for
    /// the tag scan the caller would otherwise run.
    fn addresses_by_stake_log(
        &self,
        stake: &[u8],
        offset: usize,
        limit: usize,
        reverse: bool,
    ) -> Result<Vec<Vec<u8>>, ArchiveError> {
        let page = self
            .inner
            .addresses_by_stake_log(stake, offset, limit, reverse)?;

        self.counters
            .tag_candidates
            .fetch_add(page.len() as u64, Ordering::Relaxed);

        Ok(page)
    }

    fn iter_archive_tags(
        &self,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Result<Self::TagIter, ArchiveError> {
        self.inner.iter_archive_tags(dimensions, slots)
    }

    fn iter_exact_records(&self, slots: Range<BlockSlot>) -> Result<Self::ExactIter, ArchiveError> {
        self.inner.iter_exact_records(slots)
    }
}

#[derive(Clone)]
pub struct MeasuredState<Inner> {
    inner: Inner,
    pub counters: WorkCounters,
}

impl<Inner: StateStore> StateStore for MeasuredState<Inner> {
    type EntityIter = CountedIter<Inner::EntityIter>;
    type EntityValueIter = CountedIter<Inner::EntityValueIter>;
    type UtxoIter = Inner::UtxoIter;
    type Writer = Inner::Writer;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError> {
        self.inner.read_cursor()
    }

    fn start_writer(&self) -> Result<Self::Writer, StateError> {
        self.inner.start_writer()
    }

    fn read_entities(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<EntityValue>>, StateError> {
        self.counters
            .state_entities
            .fetch_add(keys.len() as u64, Ordering::Relaxed);
        self.inner.read_entities(ns, keys)
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: Range<EntityKey>,
    ) -> Result<Self::EntityIter, StateError> {
        Ok(CountedIter {
            inner: self.inner.iter_entities(ns, range)?,
            count: self.counters.state_entities.clone(),
        })
    }

    fn iter_entity_values(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError> {
        Ok(CountedIter {
            inner: self.inner.iter_entity_values(ns, key)?,
            count: self.counters.state_entities.clone(),
        })
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        self.counters
            .utxo_reads
            .fetch_add(refs.len() as u64, Ordering::Relaxed);
        self.inner.get_utxos(refs)
    }

    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, StateError> {
        let refs = self.inner.utxos_by_tag(dimension, key)?;
        self.counters
            .utxo_refs
            .fetch_add(refs.len() as u64, Ordering::Relaxed);
        Ok(refs)
    }

    fn iter_utxos(&self) -> Result<Self::UtxoIter, StateError> {
        self.inner.iter_utxos()
    }
}

#[derive(Clone)]
pub struct MeasuredStores<Inner: ToyStores> {
    _inner: Inner,
    state: MeasuredState<Inner::State>,
    archive: MeasuredArchive<Inner::Archive>,
}

impl<Inner: ToyStores> MeasuredStores<Inner> {
    pub fn new(inner: Inner) -> Self {
        let counters = WorkCounters::default();
        Self {
            state: MeasuredState {
                inner: inner.state().clone(),
                counters: counters.clone(),
            },
            archive: MeasuredArchive {
                inner: inner.archive().clone(),
                counters,
            },
            _inner: inner,
        }
    }
}

impl<Inner: ToyStores> ToyStores for MeasuredStores<Inner> {
    type State = MeasuredState<Inner::State>;
    type Archive = MeasuredArchive<Inner::Archive>;

    fn open() -> Self {
        Self::new(Inner::open())
    }

    fn state(&self) -> &Self::State {
        &self.state
    }

    fn archive(&self) -> &Self::Archive {
        &self.archive
    }
}
