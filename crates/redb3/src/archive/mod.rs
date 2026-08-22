use ::redb::{Database, ReadableDatabase};
use redb::{
    MultimapTableHandle as _, ReadTransaction, ReadableTableMetadata as _, TableHandle as _,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::Path,
    sync::{Arc, Mutex},
};
use tracing::{debug, info, warn};

use dolos_core::{
    config::RedbArchiveConfig, ArchiveError, BlockBody, BlockSlot, ChainPoint, EntityValue,
    EraCbor, LogKey, Namespace, RawBlock, StateSchema, TemporalKey, TxHash, TxOrder, TxoRef,
};

use ::redb::Durability;
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::{conway::DatumOption, PlutusData},
        traverse::{ComputeHash, MultiEraBlock, OriginalHash},
    },
};
use rand::{rngs::SmallRng, seq::SliceRandom as _, SeedableRng as _};
use redb::WriteTransaction;
use redb_extras::buckets::BucketError;

use crate::{build_tables, Error, Table, TableFootprint};

// The one-time Byron EBB recovery scan (`examples/heal-byron-ebbs.rs`) reuses
// these index definitions and the location packing rather than restating them.
// It is the only thing that ever enables `maintenance`; the `dolos` binary
// does not, so the crate's ordinary surface keeps them private.
#[cfg(not(feature = "maintenance"))]
pub(crate) mod flatfiles;
#[cfg(feature = "maintenance")]
pub mod flatfiles;

pub(crate) mod indexes;

#[cfg(not(feature = "maintenance"))]
pub(crate) mod tables;
#[cfg(feature = "maintenance")]
pub mod tables;

#[cfg(test)]
mod tests;

#[derive(Debug)]
pub struct RedbArchiveError(ArchiveError);

impl std::fmt::Display for RedbArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for RedbArchiveError {}

impl RedbArchiveError {
    pub fn from_io(e: std::io::Error) -> Self {
        Self(ArchiveError::InternalError(e.to_string()))
    }
}

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
    flatfiles: Arc<flatfiles::FlatFileStore>,
    _tempdir: Option<Arc<tempfile::TempDir>>,
}

impl ArchiveStore {
    /// Gracefully shutdown the archive store.
    pub fn shutdown(&self) -> Result<(), RedbArchiveError> {
        Ok(())
    }

    /// Open an archive store.
    ///
    /// `path` is the archive **directory** (e.g. `<storage.path>/archive/`).
    /// The redb index is stored at `<path>/index`.
    /// Segment files are stored in `config.blocks_path` if set, otherwise in
    /// `<path>/`.
    pub fn open(
        schema: StateSchema,
        path: impl AsRef<Path>,
        config: &RedbArchiveConfig,
    ) -> Result<Self, RedbArchiveError> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).map_err(RedbArchiveError::from_io)?;

        let index_path = path.join("index");
        let db = Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "archive db is repairing")
            })
            .set_cache_size(1024 * 1024 * config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(index_path)?;

        let segments_dir = config
            .blocks_path
            .clone()
            .unwrap_or_else(|| path.to_path_buf());

        let flatfiles =
            flatfiles::FlatFileStore::new(segments_dir).map_err(RedbArchiveError::from_io)?;

        let tables = build_tables(schema);
        let store = Self {
            db: db.into(),
            tables: HashMap::from_iter(tables),
            flatfiles: Arc::new(flatfiles),
            _tempdir: None,
        };

        store.initialize()?;

        Ok(store)
    }

    pub fn in_memory(schema: StateSchema) -> Result<Self, RedbArchiveError> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())?;

        let (tempdir, flatfiles) =
            flatfiles::FlatFileStore::for_tempdir().map_err(RedbArchiveError::from_io)?;

        let tables = build_tables(schema);
        let store = Self {
            db: db.into(),
            tables: HashMap::from_iter(tables),
            flatfiles: Arc::new(flatfiles),
            _tempdir: Some(Arc::new(tempdir)),
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

    pub fn flatfiles(&self) -> &Arc<flatfiles::FlatFileStore> {
        &self.flatfiles
    }

    pub fn get_range(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<ArchiveRangeIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = tables::BlocksTable::get_range(&rx, from, to)?;
        Ok(ArchiveRangeIter {
            range,
            front: VecDeque::new(),
            back: VecDeque::new(),
            flatfiles: self.flatfiles.clone(),
        })
    }

    pub fn start_writer(&self) -> Result<ArchiveStoreWriter, RedbArchiveError> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Immediate)?;
        wx.set_quick_repair(true);

        Ok(ArchiveStoreWriter {
            wx,
            tables: self.tables.clone(),
            flatfiles: self.flatfiles.clone(),
            pending_blocks: Mutex::new(Vec::new()),
            pending_logs: Mutex::new(Vec::new()),
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

            // A slot can hold more than one block, and an intersect names one
            // of them by hash, so every block recorded there is a candidate.
            for body in tables::BlocksTable::get_all_by_slot(&rx, &self.flatfiles, *slot)? {
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

        let Some(raw) = tables::BlocksTable::get_by_slot(&rx, &self.flatfiles, slot)? else {
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
        Ok(ArchiveSparseIter(rx, range, self.flatfiles.clone()))
    }

    pub fn iter_possible_blocks_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_asset(&rx, asset, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range, self.flatfiles.clone()))
    }

    pub fn iter_possible_blocks_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_payment(&rx, payment, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range, self.flatfiles.clone()))
    }

    pub fn iter_possible_blocks_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_stake(&rx, stake, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range, self.flatfiles.clone()))
    }

    pub fn iter_possible_blocks_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_account_certs(&rx, account, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range, self.flatfiles.clone()))
    }

    pub fn iter_possible_blocks_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<ArchiveSparseIter, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        let range = indexes::Indexes::iter_by_metadata(&rx, metadata, start_slot, end_slot)?;
        Ok(ArchiveSparseIter(rx, range, self.flatfiles.clone()))
    }

    pub fn get_block_by_slot(
        &self,
        slot: &BlockSlot,
    ) -> Result<Option<BlockBody>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        tables::BlocksTable::get_by_slot(&rx, &self.flatfiles, *slot)
    }

    /// Every block recorded at `slot`, in chain order.
    ///
    /// [`Self::get_block_by_slot`] answers with the block the slot resolves
    /// to; this answers with all of them, which is what a caller holding a
    /// hash needs when the chain put two blocks on one slot.
    pub fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        tables::BlocksTable::get_all_by_slot(&rx, &self.flatfiles, *slot)
    }

    pub fn get_block_by_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Option<BlockBody>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        match indexes::Indexes::get_by_block_hash(&rx, block_hash)? {
            Some(slot) => tables::BlocksTable::get_by_slot(&rx, &self.flatfiles, slot),
            None => Ok(None),
        }
    }

    pub fn get_block_by_number(
        &self,
        block_number: &u64,
    ) -> Result<Option<BlockBody>, RedbArchiveError> {
        let rx = self.db().begin_read()?;
        match indexes::Indexes::get_by_block_number(&rx, block_number)? {
            Some(slot) => tables::BlocksTable::get_by_slot(&rx, &self.flatfiles, slot),
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

        let Some(raw) = tables::BlocksTable::get_by_slot(&rx, &self.flatfiles, slot)? else {
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
        tables::BlocksTable::get_tip(&rx, &self.flatfiles)
    }

    /// Per-table storage statistics for every table in the archive index.
    ///
    /// Enumerated from the database itself rather than from a list of known
    /// definitions, so the derived-log namespaces — which are named by the
    /// state schema at open time and carry the bulk of the file — are covered
    /// alongside the block and index tables without a second registry to keep
    /// in sync.
    pub fn stats(&self) -> Result<Vec<(String, TableFootprint)>, RedbArchiveError> {
        let rx = self.db().begin_read()?;

        let mut out = vec![];

        for handle in rx.list_tables()?.collect::<Vec<_>>() {
            let name = handle.name().to_string();
            let table = rx.open_untyped_table(handle)?;
            out.push((
                name,
                TableFootprint::new(Some(table.len()?), table.stats()?),
            ));
        }

        for handle in rx.list_multimap_tables()?.collect::<Vec<_>>() {
            let name = handle.name().to_string();
            let table = rx.open_untyped_multimap_table(handle)?;
            out.push((
                name,
                TableFootprint::new(Some(table.len()?), table.stats()?),
            ));
        }

        out.sort_by(|(a, _): &(String, _), (b, _)| a.cmp(b));

        Ok(out)
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
        drop(rx);

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

        tables::BlocksTable::remove_before(&wx, &self.flatfiles, prune_before)?;
        let temporal = prune_before.into();
        self.tables.values().try_for_each(|x| {
            x.remove_before(&wx, &temporal)
                .map_err(RedbArchiveError::from)
        })?;

        wx.commit()?;

        Ok(done)
    }

    /// Drop everything the archive holds after `after`.
    ///
    /// The cut is by slot, so at a slot holding more than one block every one
    /// of them survives, including any that follow `after` in the chain. That
    /// is only expressible where two blocks share a slot — a Byron
    /// epoch-boundary block and the first main block of the epoch it opens —
    /// and a rollback cannot reach one: `Domain::rollback` walks the WAL back
    /// to `after`, and the WAL holds `max_rollback` slots behind the tip,
    /// while epoch-boundary blocks stop at the end of Byron. The behaviour
    /// also predates the archive keeping both blocks, which is why it is
    /// recorded here rather than changed: resolving `after` to one block of a
    /// slot means matching its hash, which is `find_intersect`'s job, not this
    /// one's.
    fn truncate_front(&self, after: &ChainPoint) -> Result<(), RedbArchiveError> {
        let mut wx = self.db().begin_write()?;
        wx.set_quick_repair(true);

        tables::BlocksTable::remove_after(&wx, &self.flatfiles, after.slot())?;

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
    flatfiles: Arc<flatfiles::FlatFileStore>,
    pending_blocks: Mutex<Vec<(ChainPoint, RawBlock)>>,
    pending_logs: Mutex<Vec<(Namespace, LogKey, EntityValue)>>,
}

/// Break the ascending order of a batch of log rows before it is inserted.
///
/// Derived-log batches arrive in key order — their writers stream entities out
/// of the state store, which is sorted — and every key in one batch carries a
/// fresh temporal prefix greater than everything already stored. That makes the
/// batch a pure right-edge append, and redb splits a full leaf at half its
/// bytes with no rightmost-split case (`tree_store/btree_base.rs`,
/// `build_split`), so every page left behind converges to half full. The same
/// keys inserted in an arbitrary order converge to the ~69% (`ln 2`)
/// random-insertion asymptote instead.
///
/// Measured on a full preprod replay of the account-epoch log: 47.0% → 61.6%
/// leaf fill, 23.6% fewer leaf pages for byte-identical data.
///
/// This is a property of redb's B-tree, not of any particular log, so it lives
/// with the backend rather than with the callers that happen to produce sorted
/// batches. Only arrival order changes — the rows, their keys, and the table
/// they land in are identical either way, so a stele cut from the result is
/// byte-identical (ADR-004).
fn break_insertion_order(
    tables: &HashMap<Namespace, Table>,
    batch: Vec<(Namespace, LogKey, EntityValue)>,
) -> Vec<(Namespace, LogKey, EntityValue)> {
    // A key written twice in one batch means different things per namespace
    // kind, and the difference decides whether reordering is safe. A value
    // table *replaces*, so only the last write is supposed to survive — and a
    // shuffle, left to itself, would be free to persist the superseded one.
    // A multimap table *adds an element*, so every write is its own row and
    // the order they arrive in does not matter. Collapse the first kind to its
    // last write; leave the second whole.
    let mut superseded = HashSet::new();

    let mut batch: Vec<_> = batch
        .into_iter()
        .rev()
        .filter(|(ns, key, _)| {
            matches!(tables.get(ns), Some(Table::MultiValue(_)))
                || superseded.insert((*ns, key.clone()))
        })
        .collect();

    batch.reverse();

    // The batch seeds its own shuffle from the first key's temporal prefix —
    // the slot every row of one boundary shares — so replaying a boundary lays
    // its pages out the same way twice.
    if let Some((_, key, _)) = batch.first() {
        let seed = TemporalKey::from(key.clone());
        let seed = u64::from_be_bytes(seed.as_ref().try_into().unwrap());

        batch.shuffle(&mut SmallRng::seed_from_u64(seed));
    }

    batch
}

impl dolos_core::ArchiveWriter for ArchiveStoreWriter {
    fn apply(&self, point: &ChainPoint, block: &RawBlock) -> Result<(), ArchiveError> {
        self.pending_blocks
            .lock()
            .unwrap()
            .push((point.clone(), block.clone()));
        Ok(())
    }

    fn undo(&self, point: &ChainPoint) -> Result<(), ArchiveError> {
        tables::BlocksTable::undo(&self.wx, &self.flatfiles, point)?;
        Ok(())
    }

    fn commit(self) -> Result<(), ArchiveError> {
        // 1. Batch-append all pending blocks to flat files (fsync inside).
        // 2. Insert all index entries into redb.
        // 3. Insert the log rows, in an order that does not halve the leaves.
        // 4. Commit redb transaction.
        let pending = self.pending_blocks.into_inner().unwrap();
        if !pending.is_empty() {
            tables::BlocksTable::apply_batch(&self.wx, &self.flatfiles, &pending)?;
        }

        let logs = self.pending_logs.into_inner().unwrap();
        let logs = break_insertion_order(&self.tables, logs);

        for (ns, key, value) in logs {
            let table = self
                .tables
                .get(&ns)
                .ok_or(ArchiveError::NamespaceNotFound(ns))?;

            table
                .write(&self.wx, key, &value)
                .map_err(RedbArchiveError::from)?;
        }

        self.wx.commit().map_err(RedbArchiveError::from)?;
        Ok(())
    }

    fn write_log(
        &self,
        ns: Namespace,
        key: &dolos_core::LogKey,
        value: &dolos_core::EntityValue,
    ) -> Result<(), ArchiveError> {
        // Held back until `commit`, where the whole batch is reordered — see
        // `break_insertion_order`. The namespace is still resolved here so an
        // unknown one fails at the call that names it rather than at commit.
        if !self.tables.contains_key(&ns) {
            return Err(ArchiveError::NamespaceNotFound(ns));
        }

        self.pending_logs
            .lock()
            .unwrap()
            .push((ns, key.clone(), value.clone()));

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

    fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, ArchiveError> {
        Ok(Self::get_blocks_by_slot(self, slot)?)
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

type IndexEntry = (BlockSlot, flatfiles::BlockLocation);

/// Iterator over a range of blocks, reading lazily from flat files.
///
/// A slot can hold more than one block, so an index entry expands to a run of
/// locations and whatever is left of the entry each end is working through is
/// buffered here. When one end's range runs dry it drains the other end's
/// buffer, which is what keeps a forward and a backward walk from yielding a
/// block twice or dropping one where they meet.
pub struct ArchiveRangeIter {
    range: redb::Range<'static, BlockSlot, &'static [u8]>,
    front: VecDeque<IndexEntry>,
    back: VecDeque<IndexEntry>,
    flatfiles: Arc<flatfiles::FlatFileStore>,
}

impl ArchiveRangeIter {
    fn next_entry(&mut self) -> Option<IndexEntry> {
        loop {
            if let Some(entry) = self.front.pop_front() {
                return Some(entry);
            }

            let Some(next) = self.range.next() else {
                return self.back.pop_front();
            };

            let (slot, value) = next.ok()?;
            let slot = slot.value();

            self.front.extend(
                flatfiles::decode_locations(value.value())
                    .rev()
                    .map(|loc| (slot, loc)),
            );
        }
    }

    fn next_entry_back(&mut self) -> Option<IndexEntry> {
        loop {
            if let Some(entry) = self.back.pop_back() {
                return Some(entry);
            }

            let Some(next) = self.range.next_back() else {
                return self.front.pop_back();
            };

            let (slot, value) = next.ok()?;
            let slot = slot.value();

            self.back.extend(
                flatfiles::decode_locations(value.value())
                    .rev()
                    .map(|loc| (slot, loc)),
            );
        }
    }
}

impl Iterator for ArchiveRangeIter {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (slot, loc) = self.next_entry()?;
            match self.flatfiles.read(&loc) {
                Ok(data) => return Some((slot, data)),
                Err(_) => continue, // skip unreadable blocks
            }
        }
    }
}

impl DoubleEndedIterator for ArchiveRangeIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let (slot, loc) = self.next_entry_back()?;
            match self.flatfiles.read(&loc) {
                Ok(data) => return Some((slot, data)),
                Err(_) => continue,
            }
        }
    }
}

impl dolos_core::archive::Skippable for ArchiveRangeIter {
    fn skip_forward(&mut self, n: usize) {
        for _ in 0..n {
            if self.next_entry().is_none() {
                break;
            }
        }
    }

    fn skip_backward(&mut self, n: usize) {
        for _ in 0..n {
            if self.next_entry_back().is_none() {
                break;
            }
        }
    }
}

pub struct ArchiveSparseIter(
    ReadTransaction,
    indexes::SlotKeyIterator,
    Arc<flatfiles::FlatFileStore>,
);

impl Iterator for ArchiveSparseIter {
    type Item = Result<(BlockSlot, Option<BlockBody>), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.1.next()?;

        let Ok(slot) = next else {
            return Some(Err(next.err().unwrap().into()));
        };

        let block = tables::BlocksTable::get_by_slot(&self.0, &self.2, slot);

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

        let block = tables::BlocksTable::get_by_slot(&self.0, &self.2, slot);

        let Ok(block) = block else {
            return Some(Err(block.err().unwrap().into()));
        };

        Some(Ok((slot, block)))
    }
}
