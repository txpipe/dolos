use ::redb::{Database, Range, ReadableDatabase};
use redb::ReadTransaction;
use std::{collections::HashMap, path::Path};
use tracing::{debug, info, warn};

use dolos_core::{
    ArchiveError, BlockBody, BlockSlot, ChainPoint, EntityValue, EraCbor, LogKey, Namespace,
    RawBlock, SlotTags, StateSchema, TxHash, TxOrder, TxoRef,
};

use ::redb::Durability;
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::{conway::DatumOption, PlutusData},
        traverse::{ComputeHash, MultiEraBlock, OriginalHash},
    },
};
use redb::WriteTransaction;
use redb_extras::buckets::BucketError;
use std::sync::Arc;

use crate::{build_tables, Error, Table};

pub(crate) mod indexes;
pub(crate) mod tables;

#[derive(Debug)]
pub struct RedbArchiveError(ArchiveError);

impl std::fmt::Display for RedbArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for RedbArchiveError {}

impl From<Error> for RedbArchiveError {
    fn from(error: Error) -> Self {
        Self(ArchiveError::InternalError(error.to_string()))
    }
}

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
        Self(ArchiveError::InternalError(value.to_string()))
    }
}

impl From<::redb::SetDurabilityError> for RedbArchiveError {
    fn from(value: ::redb::SetDurabilityError) -> Self {
        Self(ArchiveError::InternalError(value.to_string()))
    }
}

impl From<::redb::TableError> for RedbArchiveError {
    fn from(value: ::redb::TableError) -> Self {
        Self(ArchiveError::InternalError(value.to_string()))
    }
}

impl From<::redb::CommitError> for RedbArchiveError {
    fn from(value: ::redb::CommitError) -> Self {
        Self(ArchiveError::InternalError(value.to_string()))
    }
}

impl From<::redb::StorageError> for RedbArchiveError {
    fn from(value: ::redb::StorageError) -> Self {
        Self(ArchiveError::InternalError(value.to_string()))
    }
}

impl From<::redb::TransactionError> for RedbArchiveError {
    fn from(value: ::redb::TransactionError) -> Self {
        Self(ArchiveError::InternalError(value.to_string()))
    }
}

impl From<BucketError> for RedbArchiveError {
    fn from(value: BucketError) -> Self {
        Self(ArchiveError::InternalError(value.to_string()))
    }
}

const DEFAULT_CACHE_SIZE_MB: usize = 500;

#[derive(Clone)]
pub struct ArchiveStore {
    db: Arc<Database>,
    tables: HashMap<Namespace, Table>,
}

impl ArchiveStore {
    pub fn open(
        schema: StateSchema,
        path: impl AsRef<Path>,
        cache_size: Option<usize>,
    ) -> Result<Self, RedbArchiveError> {
        let db = Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "archive db is repairing")
            })
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)?;

        let tables = build_tables(schema);
        let store = Self {
            db: db.into(),
            tables: HashMap::from_iter(tables),
        };

        store.initialize()?;

        Ok(store)
    }

    pub fn in_memory(schema: StateSchema) -> Result<Self, RedbArchiveError> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())?;

        let tables = build_tables(schema);
        let store = Self {
            db: db.into(),
            tables: HashMap::from_iter(tables),
        };

        store.initialize()?;

        Ok(store)
    }

    pub fn initialize(&self) -> Result<(), RedbArchiveError> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Immediate)?;

        for (_, table) in self.tables.iter() {
            table.initialize(&mut wx)?;
        }

        tables::BlocksTable::initialize(&wx)?;

        wx.commit()?;

        Ok(())
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

        tables::BlocksTable::copy(&rx, &wx)?;

        wx.commit()?;

        Ok(())
    }

    pub fn get_range(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<ArchiveRangeIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = tables::BlocksTable::get_range(&rx, from, to)?;
        Ok(ArchiveRangeIter(range))
    }

    pub fn start_writer(&self) -> Result<ArchiveStoreWriter, RedbArchiveError> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Immediate)?;
        wx.set_quick_repair(true);

        Ok(ArchiveStoreWriter {
            wx,
            tables: self.tables.clone(),
        })
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
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_address_payment_part(
            &rx,
            address_payment_part,
            start_slot,
            end_slot,
        )
    }

    pub fn get_possible_block_slots_by_address_stake_part(
        &self,
        address_stake_part: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_address_stake_part(&rx, address_stake_part, start_slot, end_slot)
    }

    pub fn get_possible_block_slots_by_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_asset(&rx, asset, start_slot, end_slot)
    }

    pub fn get_possible_block_slots_by_datum_hash(
        &self,
        datum_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_datum_hash(&rx, datum_hash, start_slot, end_slot)
    }

    pub fn get_possible_block_slots_by_policy(
        &self,
        policy: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_policy(&rx, policy, start_slot, end_slot)
    }

    pub fn get_possible_block_slots_by_script_hash(
        &self,
        script_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_script_hash(&rx, script_hash, start_slot, end_slot)
    }

    pub fn get_possible_block_slots_by_spent_txo(
        &self,
        spent_txo: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_spent_txo(&rx, spent_txo, start_slot, end_slot)
    }

    pub fn get_possible_block_slots_by_account(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read().map_err(Error::from)?;
        indexes::Indexes::get_by_account(&rx, account, start_slot, end_slot)
    }

    pub fn get_possible_blocks_by_address_payment_part(
        &self,
        address_payment_part: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_address_payment_part(
            address_payment_part,
            start_slot,
            end_slot,
        )?
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
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_address_stake_part(
            address_stake_part,
            start_slot,
            end_slot,
        )?
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
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_asset(asset, start_slot, end_slot)?
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
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_datum_hash(datum_hash, start_slot, end_slot)?
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
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_policy(policy, start_slot, end_slot)?
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
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_script_hash(script_hash, start_slot, end_slot)?
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
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_spent_txo(txo_ref, start_slot, end_slot)?
            .iter()
            .flat_map(|slot| match self.get_block_by_slot(slot) {
                Ok(Some(block)) => Some(Ok(block)),
                Ok(None) => None,
                Err(e) => Some(Err(e)),
            })
            .collect()
    }

    pub fn get_possible_blocks_by_account(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockBody>, RedbArchiveError> {
        self.get_possible_block_slots_by_account(account, start_slot, end_slot)?
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
        let rx = self.db().begin_read()?;
        let Some(slot) = indexes::Indexes::get_by_tx_hash(&rx, tx_hash)? else {
            return Ok(None);
        };

        let Some(raw) = tables::BlocksTable::get_by_slot(&rx, slot)? else {
            return Ok(None);
        };

        let block =
            MultiEraBlock::decode(raw.as_slice()).map_err(ArchiveError::BlockDecodingError)?;
        for (idx, tx) in block.txs().iter().enumerate() {
            if tx.hash().to_vec() == tx_hash {
                return Ok(Some((raw, idx)));
            }
        }

        Ok(None)
    }

    pub fn iter_possible_blocks_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_address(&rx, address, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range))
    }

    pub fn iter_possible_blocks_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_asset(&rx, asset, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range))
    }

    pub fn iter_possible_blocks_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_payment(&rx, payment, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range))
    }

    pub fn iter_possible_blocks_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_stake(&rx, stake, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range))
    }

    pub fn iter_possible_blocks_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_account_certs(&rx, account, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range))
    }

    pub fn iter_possible_blocks_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_metadata(&rx, metadata, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range))
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
        let rx = self.db().begin_read()?;
        match indexes::Indexes::get_by_block_hash(&rx, block_hash)? {
            Some(slot) => tables::BlocksTable::get_by_slot(&rx, slot),
            None => Ok(None),
        }
    }

    pub fn get_block_by_number(
        &self,
        block_number: &u64,
    ) -> Result<Option<BlockBody>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        match indexes::Indexes::get_by_block_number(&rx, block_number)? {
            Some(slot) => tables::BlocksTable::get_by_slot(&rx, slot),
            None => Ok(None),
        }
    }

    pub fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        indexes::Indexes::get_by_tx_hash(&rx, tx_hash)
    }

    pub fn get_tx_by_spent_txo(
        &self,
        spent_txo: &[u8],
    ) -> Result<Option<TxHash>, RedbArchiveError> {
        let (start_slot, end_slot) = self.index_bounds()?;
        let possible: Vec<BlockBody> =
            self.get_possible_blocks_by_spent_txo(spent_txo, start_slot, end_slot)?;

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
        let rx = self.db().begin_read()?;
        let Some(slot) = indexes::Indexes::get_by_tx_hash(&rx, tx_hash)? else {
            return Ok(None);
        };

        let Some(raw) = tables::BlocksTable::get_by_slot(&rx, slot)? else {
            return Ok(None);
        };

        let block =
            MultiEraBlock::decode(raw.as_slice()).map_err(ArchiveError::BlockDecodingError)?;
        if let Some(tx) = block.txs().iter().find(|x| x.hash().to_vec() == tx_hash) {
            return Ok(Some(EraCbor(block.era().into(), tx.encode())));
        }

        Ok(None)
    }

    pub fn get_plutus_data(
        &self,
        datum_hash: &Hash<32>,
    ) -> Result<Option<PlutusData>, RedbArchiveError> {
        let (start_slot, end_slot) = self.index_bounds()?;
        let possible =
            self.get_possible_blocks_by_datum_hash(datum_hash.as_slice(), start_slot, end_slot)?;

        for raw in possible {
            let block =
                MultiEraBlock::decode(raw.as_slice()).map_err(ArchiveError::BlockDecodingError)?;
            for tx in block.txs() {
                // Check witnesses
                if let Some(plutus_data) = tx.find_plutus_data(datum_hash) {
                    // unwarp the KeepRaw wrapper.
                    return Ok(Some(plutus_data.clone().unwrap()));
                }

                // Check inline
                for (_, output) in tx.produces() {
                    if let Some(DatumOption::Data(data)) = output.datum() {
                        if &data.original_hash() == datum_hash {
                            return Ok(Some(data.clone().unwrap().unwrap()));
                        }
                    }
                }

                // Check redeemer data
                for redeemer in tx.redeemers() {
                    // TODO: We should use a KeepRaw structure and original_hash
                    if &redeemer.data().compute_hash() == datum_hash {
                        return Ok(Some(redeemer.data().clone()));
                    }
                }
            }
        }

        Ok(None)
    }

    fn index_bounds(&self) -> Result<(BlockSlot, BlockSlot), RedbArchiveError> {
        let end_slot = self.get_tip()?.map(|(slot, _)| slot).unwrap_or_default();
        Ok((0, end_slot))
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
        let temporal = prune_before.into();
        self.tables.values().try_for_each(|x| {
            x.remove_before(&wx, &temporal)
                .map_err(RedbArchiveError::from)
        })?;

        wx.commit()?;

        Ok(done)
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), RedbArchiveError> {
        let mut wx = self.db().begin_write()?;
        wx.set_quick_repair(true);

        tables::BlocksTable::remove_after(&wx, after.slot())?;

        let temporal = after.into();
        self.tables.values().try_for_each(|x| {
            x.remove_after(&wx, &temporal)
                .map_err(RedbArchiveError::from)
        })?;

        wx.commit()?;

        Ok(())
    }
}

pub struct ArchiveStoreWriter {
    wx: WriteTransaction,
    tables: HashMap<Namespace, Table>,
}

impl dolos_core::ArchiveWriter for ArchiveStoreWriter {
    fn apply(
        &self,
        point: &ChainPoint,
        block: &RawBlock,
        _tags: &SlotTags,
    ) -> Result<(), ArchiveError> {
        tables::BlocksTable::apply(&self.wx, point, block)?;

        Ok(())
    }

    fn undo(&self, point: &ChainPoint, _tags: &SlotTags) -> Result<(), ArchiveError> {
        tables::BlocksTable::undo(&self.wx, point)?;

        Ok(())
    }

    fn commit(self) -> Result<(), ArchiveError> {
        self.wx.commit().map_err(RedbArchiveError::from)?;

        Ok(())
    }

    fn write_log(
        &self,
        ns: Namespace,
        key: &dolos_core::LogKey,
        value: &dolos_core::EntityValue,
    ) -> Result<(), ArchiveError> {
        let table = self
            .tables
            .get(&ns)
            .ok_or(ArchiveError::NamespaceNotFound(ns))?;

        table
            .write(&self.wx, key, value)
            .map_err(RedbArchiveError::from)?;

        Ok(())
    }
}

pub struct LogIter(pub(crate) ::redb::Range<'static, &'static [u8], &'static [u8]>);

impl Iterator for LogIter {
    type Item = Result<(LogKey, EntityValue), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next()?;

        let entry = next
            .map(|(k, v)| (k.value().to_vec(), v.value().to_vec()))
            .map(|(k, v)| (LogKey::from(k), v))
            .map_err(RedbArchiveError::from)
            .map_err(ArchiveError::from);

        Some(entry)
    }
}

pub struct EntityValueIter(pub(crate) ::redb::MultimapValue<'static, &'static [u8]>);

impl Iterator for EntityValueIter {
    type Item = Result<EntityValue, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.0.next()?;

        let entry = next
            .map(|v| v.value().to_vec())
            .map_err(RedbArchiveError::from)
            .map_err(ArchiveError::from);

        Some(entry)
    }
}

impl dolos_core::ArchiveStore for ArchiveStore {
    type BlockIter<'a> = ArchiveRangeIter;
    type Writer = ArchiveStoreWriter;
    type LogIter = LogIter;
    type EntityValueIter = EntityValueIter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        Ok(Self::start_writer(self)?)
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        Ok(Self::get_block_by_slot(self, slot)?)
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

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        Ok(Self::truncate_front(self, after)?)
    }

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&dolos_core::LogKey],
    ) -> Result<Vec<Option<dolos_core::EntityValue>>, ArchiveError> {
        let mut rx = self.db().begin_read().map_err(RedbArchiveError::from)?;

        let table = self
            .tables
            .get(&ns)
            .ok_or(ArchiveError::NamespaceNotFound(ns))?;

        let mut out = vec![];

        for key in keys {
            let value = table
                .read_value(&mut rx, key.as_ref())
                .map_err(RedbArchiveError::from)?;
            out.push(value);
        }

        Ok(out)
    }

    fn iter_logs(
        &self,
        ns: Namespace,
        range: std::ops::Range<dolos_core::LogKey>,
    ) -> Result<Self::LogIter, ArchiveError> {
        let mut rx = self.db().begin_read().map_err(RedbArchiveError::from)?;

        let range = std::ops::Range {
            start: range.start.as_ref(),
            end: range.end.as_ref(),
        };

        let table = self
            .tables
            .get(&ns)
            .ok_or(ArchiveError::NamespaceNotFound(ns))?;

        let values = table
            .range(&mut rx, range)
            .map_err(RedbArchiveError::from)?;

        Ok(LogIter(values))
    }
}

pub struct ArchiveRangeIter(Range<'static, BlockSlot, BlockBody>);

impl Iterator for ArchiveRangeIter {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

impl DoubleEndedIterator for ArchiveRangeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

pub struct ArchiveSparseIter(ReadTransaction, indexes::SlotKeyIterator);

impl Iterator for ArchiveSparseIter {
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

impl DoubleEndedIterator for ArchiveSparseIter {
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
