use ::redb::{Database, Range, ReadableDatabase};
use redb::ReadTransaction;
use std::path::Path;
use tracing::{debug, info, warn};

use dolos_core::{
    ArchiveError, BlockBody, BlockSlot, ChainPoint, EraCbor, RawBlock, SlotTags, TxHash, TxOrder,
    TxoRef,
};

use ::redb::Durability;
use pallas::ledger::traverse::MultiEraBlock;
use redb::WriteTransaction;
use std::sync::Arc;

mod indexes;
mod tables;

#[derive(Debug)]
pub struct RedbArchiveError(ArchiveError);

impl From<ArchiveError> for RedbArchiveError {
    fn from(value: ArchiveError) -> Self {
        Self(value)
    }
}

impl From<RedbArchiveError> for ArchiveError {
    fn from(value: RedbArchiveError) -> Self {
        value.0
    }
}

impl From<::redb::DatabaseError> for RedbArchiveError {
    fn from(value: ::redb::DatabaseError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::SetDurabilityError> for RedbArchiveError {
    fn from(value: ::redb::SetDurabilityError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::TableError> for RedbArchiveError {
    fn from(value: ::redb::TableError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::CommitError> for RedbArchiveError {
    fn from(value: ::redb::CommitError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::StorageError> for RedbArchiveError {
    fn from(value: ::redb::StorageError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::TransactionError> for RedbArchiveError {
    fn from(value: ::redb::TransactionError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

const DEFAULT_CACHE_SIZE_MB: usize = 500;

#[derive(Clone)]
pub struct ChainStore {
    db: Arc<Database>,
}

impl ChainStore {
    pub fn open(
        path: impl AsRef<Path>,
        cache_size: Option<usize>,
    ) -> Result<Self, RedbArchiveError> {
        let db = Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "archive db is repairing")
            })
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)?;

        Self::initialize(db)
    }

    pub fn in_memory() -> Result<Self, ArchiveError> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .map_err(RedbArchiveError::from)?;

        Ok(Self::initialize(db)?)
    }

    pub fn initialize(db: Database) -> Result<Self, RedbArchiveError> {
        let mut wx = db.begin_write()?;
        wx.set_durability(Durability::Immediate)?;

        indexes::Indexes::initialize(&wx)?;
        tables::BlocksTable::initialize(&wx)?;

        wx.commit()?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn db_mut(&mut self) -> &mut Database {
        Arc::get_mut(&mut self.db).unwrap()
    }

    pub fn copy(&self, target: &Self) -> Result<(), RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let wx = target.db().begin_write()?;

        indexes::Indexes::copy(&rx, &wx)?;
        tables::BlocksTable::copy(&rx, &wx)?;

        wx.commit()?;

        Ok(())
    }

    pub fn get_range(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<ChainRangeIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = tables::BlocksTable::get_range(&rx, from, to)?;
        Ok(ChainRangeIter(range))
    }

    pub fn start_writer(&self) -> Result<ChainStoreWriter, RedbArchiveError> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Immediate)?;
        wx.set_quick_repair(true);

        Ok(ChainStoreWriter { wx })
    }

    pub fn find_intersect(
        &self,
        intersect: &[ChainPoint],
    ) -> Result<Option<ChainPoint>, RedbArchiveError> {
        let rx = self.db().begin_read()?;

        for point in intersect {
            let ChainPoint::Specific(slot, hash) = point else {
                return Ok(Some(ChainPoint::Origin));
            };

            if let Some(body) = tables::BlocksTable::get_by_slot(&rx, *slot)? {
                let decoded =
                    MultiEraBlock::decode(&body).map_err(ArchiveError::BlockDecodingError)?;

                if decoded.hash().eq(hash) {
                    return Ok(Some(ChainPoint::Specific(decoded.slot(), decoded.hash())));
                }
            }
        }

        Ok(None)
    }

    pub fn get_possible_block_slots_by_address_payment_part(
        &self,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_address_payment_part(&rx, address_payment_part)
    }

    pub fn get_possible_block_slots_by_address_stake_part(
        &self,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_address_stake_part(&rx, address_stake_part)
    }

    pub fn get_possible_block_slots_by_asset(
        &self,
        asset: &[u8],
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_asset(&rx, asset)
    }

    pub fn get_possible_block_slots_by_block_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_block_hash(&rx, block_hash)
    }

    pub fn get_possible_block_slots_by_block_number(
        &self,
        block_number: &u64,
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_block_number(&rx, block_number)
    }

    pub fn get_possible_block_slots_by_datum_hash(
        &self,
        datum_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_datum_hash(&rx, datum_hash)
    }

    pub fn get_possible_block_slots_by_policy(
        &self,
        policy: &[u8],
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_policy(&rx, policy)
    }

    pub fn get_possible_block_slots_by_script_hash(
        &self,
        script_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_script_hash(&rx, script_hash)
    }

    pub fn get_possible_block_slots_by_spent_txo(
        &self,
        spent_txo: &[u8],
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_spent_txo(&rx, spent_txo)
    }

    pub fn get_possible_block_slots_by_tx_hash(
        &self,
        tx_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_tx_hash(&rx, tx_hash)
    }

    pub fn get_possible_blocks_by_address_payment_part(
        &self,
        address_payment_part: &[u8],
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_address_payment_part(address_payment_part)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_address_stake_part(
        &self,
        address_stake_part: &[u8],
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_address_stake_part(address_stake_part)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_asset(
        &self,
        asset: &[u8],
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_asset(asset)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_block_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_block_hash(block_hash)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_block_number(
        &self,
        block_number: &u64,
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_block_number(block_number)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_datum_hash(
        &self,
        datum_hash: &[u8],
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_datum_hash(datum_hash)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_policy(
        &self,
        policy: &[u8],
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_policy(policy)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_script_hash(
        &self,
        script_hash: &[u8],
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_script_hash(script_hash)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_spent_txo(
        &self,
        txo_ref: &[u8],
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_spent_txo(txo_ref)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_tx_hash(
        &self,
        tx_hash: &[u8],
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_tx_hash(tx_hash)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, RedbArchiveError> {
        let possible = self.get_possible_blocks_by_tx_hash(tx_hash)?;

        for raw in possible {
            let block =
                MultiEraBlock::decode(raw.as_slice()).map_err(ArchiveError::BlockDecodingError)?;
            for (idx, tx) in block.txs().iter().enumerate() {
                if tx.hash().to_vec() == tx_hash {
                    return Ok(Some((raw, idx)));
                }
            }
        }

        Ok(None)
    }

    pub fn iter_possible_blocks_with_address(
        &self,
        address: &[u8],
    ) -> Result<ChainSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_address(&rx, address)?;
        Ok(ChainSparseIter(rx, range))
    }

    pub fn iter_possible_blocks_with_asset(
        &self,
        asset: &[u8],
    ) -> Result<ChainSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_asset(&rx, asset)?;
        Ok(ChainSparseIter(rx, range))
    }

    pub fn iter_possible_blocks_with_payment(
        &self,
        payment: &[u8],
    ) -> Result<ChainSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_payment(&rx, payment)?;
        Ok(ChainSparseIter(rx, range))
    }

    pub fn get_block_by_slot(
        &self,
        slot: &BlockSlot,
    ) -> Result<Option<BlockBody>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        tables::BlocksTable::get_by_slot(&rx, *slot)
    }

    pub fn get_block_by_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Option<BlockBody>, RedbArchiveError> {
        let possible: Vec<BlockBody> = self.get_possible_blocks_by_block_hash(block_hash)?;

        for raw in possible {
            let block =
                MultiEraBlock::decode(raw.as_slice()).map_err(ArchiveError::BlockDecodingError)?;

            if block.hash().as_slice() == block_hash {
                return Ok(Some(raw));
            }
        }

        Ok(None)
    }

    pub fn get_block_by_number(
        &self,
        block_number: &u64,
    ) -> Result<Option<BlockBody>, RedbArchiveError> {
        let possible = self.get_possible_blocks_by_block_number(block_number)?;

        for raw in possible {
            let block =
                MultiEraBlock::decode(raw.as_slice()).map_err(ArchiveError::BlockDecodingError)?;

            if block.number() == *block_number {
                return Ok(Some(raw));
            }
        }

        Ok(None)
    }

    pub fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, RedbArchiveError> {
        let mut possible = self.get_possible_block_slots_by_tx_hash(tx_hash)?;
        if possible.len() == 1 {
            Ok(possible.pop())
        } else {
            for slot in possible {
                let block = self.get_block_by_slot(&slot)?;

                if let Some(raw) = block {
                    let block =
                        MultiEraBlock::decode(&raw).map_err(ArchiveError::BlockDecodingError)?;
                    if block.txs().iter().any(|x| x.hash().to_vec() == tx_hash) {
                        return Ok(Some(slot));
                    }
                }
            }
            Ok(None)
        }
    }

    pub fn get_tx_by_spent_txo(
        &self,
        spent_txo: &[u8],
    ) -> Result<Option<TxHash>, RedbArchiveError> {
        let possible: Vec<BlockBody> = self.get_possible_blocks_by_spent_txo(spent_txo)?;

        for raw in possible {
            let block =
                MultiEraBlock::decode(raw.as_slice()).map_err(ArchiveError::BlockDecodingError)?;

            for tx in block.txs().iter() {
                for input in tx.inputs() {
                    let bytes: Vec<u8> = TxoRef::from(&input).into();
                    if bytes.as_slice() == spent_txo {
                        return Ok(Some(tx.hash()));
                    }
                }
            }
        }

        Ok(None)
    }

    pub fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, RedbArchiveError> {
        let possible = self.get_possible_blocks_by_tx_hash(tx_hash)?;

        for raw in possible {
            let block =
                MultiEraBlock::decode(raw.as_slice()).map_err(ArchiveError::BlockDecodingError)?;
            if let Some(tx) = block.txs().iter().find(|x| x.hash().to_vec() == tx_hash) {
                return Ok(Some(EraCbor(block.era().into(), tx.encode())));
            }
        }

        Ok(None)
    }

    pub fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        tables::BlocksTable::get_tip(&rx)
    }

    pub fn prune_history(
        &self,
        max_slots: u64,
        max_prune: Option<u64>,
    ) -> Result<bool, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let start = match tables::BlocksTable::first(&rx)? {
            Some((slot, _)) => slot,
            None => {
                debug!("no start point found on chain, skipping housekeeping");
                return Ok(true);
            }
        };

        let last = match tables::BlocksTable::last(&rx)? {
            Some((slot, _)) => slot,
            None => {
                debug!("no tip found on chain, skipping housekeeping");
                return Ok(true);
            }
        };

        let delta = last.saturating_sub(start);
        let excess = delta.saturating_sub(max_slots);

        debug!(delta, excess, last, start, "chain history delta computed");

        if excess == 0 {
            debug!(delta, max_slots, excess, "no pruning necessary on chain");
            return Ok(true);
        }

        let (done, max_prune) = match max_prune {
            Some(max) => (excess <= max, core::cmp::min(excess, max)),
            None => (true, excess),
        };

        let prune_before = start + max_prune;

        info!(
            cutoff_slot = prune_before,
            start, excess, "pruning archive for excess history"
        );

        let mut wx = self.db().begin_write()?;
        wx.set_quick_repair(true);

        tables::BlocksTable::remove_before(&wx, prune_before)?;

        wx.commit()?;

        Ok(done)
    }

    fn truncate_front(&self, after: BlockSlot) -> Result<(), RedbArchiveError> {
        let mut wx = self.db().begin_write()?;
        wx.set_quick_repair(true);

        tables::BlocksTable::remove_after(&wx, after)?;

        wx.commit()?;

        Ok(())
    }
}

impl From<Database> for ChainStore {
    fn from(value: Database) -> Self {
        Self {
            db: Arc::new(value),
        }
    }
}

pub struct ChainStoreWriter {
    wx: WriteTransaction,
}

impl dolos_core::ArchiveWriter for ChainStoreWriter {
    fn apply(
        &self,
        point: &ChainPoint,
        block: &RawBlock,
        tags: &SlotTags,
    ) -> Result<(), ArchiveError> {
        indexes::Indexes::apply(&self.wx, point, tags)?;
        tables::BlocksTable::apply(&self.wx, point, block)?;

        Ok(())
    }

    fn undo(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), ArchiveError> {
        indexes::Indexes::undo(&self.wx, point, tags)?;
        tables::BlocksTable::undo(&self.wx, point)?;

        Ok(())
    }

    fn commit(self) -> Result<(), ArchiveError> {
        self.wx.commit().map_err(RedbArchiveError::from)?;

        Ok(())
    }
}

impl dolos_core::ArchiveStore for ChainStore {
    type BlockIter<'a> = ChainRangeIter;
    type SparseBlockIter = ChainSparseIter;
    type Writer = ChainStoreWriter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        Ok(Self::start_writer(self)?)
    }

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ArchiveError> {
        Ok(Self::get_block_by_hash(self, block_hash)?)
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        Ok(Self::get_block_by_slot(self, slot)?)
    }

    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ArchiveError> {
        Ok(Self::get_block_by_number(self, number)?)
    }

    fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, ArchiveError> {
        Ok(Self::get_block_with_tx(self, tx_hash)?)
    }

    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, ArchiveError> {
        Ok(Self::get_tx(self, tx_hash)?)
    }

    fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        Ok(Self::get_slot_for_tx(self, tx_hash)?)
    }

    fn get_tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, ArchiveError> {
        Ok(Self::get_tx_by_spent_txo(self, spent_txo)?)
    }

    fn iter_blocks_with_address(
        &self,
        address: &[u8],
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        // TODO: we need to filter the false positives
        let out = self.iter_possible_blocks_with_address(address)?;

        Ok(out)
    }

    fn iter_blocks_with_asset(&self, asset: &[u8]) -> Result<Self::SparseBlockIter, ArchiveError> {
        // TODO: we need to filter the false positives
        let out = self.iter_possible_blocks_with_asset(asset)?;

        Ok(out)
    }

    fn iter_blocks_with_payment(
        &self,
        payment: &[u8],
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        // TODO: we need to filter the false positives
        let out = self.iter_possible_blocks_with_payment(payment)?;

        Ok(out)
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        Ok(Self::get_range(self, from, to)?)
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        Ok(Self::find_intersect(self, intersect)?)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        Ok(Self::get_tip(self)?)
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError> {
        Ok(Self::prune_history(self, max_slots, max_prune)?)
    }

    fn truncate_front(&self, after: BlockSlot) -> Result<(), ArchiveError> {
        Ok(Self::truncate_front(self, after)?)
    }
}

pub struct ChainRangeIter(Range<'static, BlockSlot, BlockBody>);

impl Iterator for ChainRangeIter {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

impl DoubleEndedIterator for ChainRangeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

pub struct ChainSparseIter(ReadTransaction, indexes::SlotKeyIterator);

impl Iterator for ChainSparseIter {
    type Item = Result<(BlockSlot, Option<BlockBody>), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.1.next()?;

        let Ok(slot) = next else {
            return Some(Err(next.err().unwrap().into()));
        };

        let block = tables::BlocksTable::get_by_slot(&self.0, slot);

        let Ok(block) = block else {
            return Some(Err(block.err().unwrap().into()));
        };

        Some(Ok((slot, block)))
    }
}

impl DoubleEndedIterator for ChainSparseIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        let next = self.1.next_back()?;

        let Ok(slot) = next else {
            return Some(Err(next.err().unwrap().into()));
        };

        let block = tables::BlocksTable::get_by_slot(&self.0, slot);

        let Ok(block) = block else {
            return Some(Err(block.err().unwrap().into()));
        };

        Some(Ok((slot, block)))
    }
}
