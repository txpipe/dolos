use std::{collections::HashMap, path::Path, sync::Arc};

use dolos_core::{
    BlockSlot, ChainPoint, IndexError, IndexStore as CoreIndexStore,
    IndexWriter as CoreIndexWriter, SlotTags, UtxoSet, UtxoSetDelta,
};
use redb::{
    Database, Durability, ReadTransaction, ReadableDatabase, TableDefinition, TableStats,
    WriteTransaction,
};
use tracing::warn;

use crate::{archive, state, Error};

const DEFAULT_CACHE_SIZE_MB: usize = 500;

/// Key for the single cursor entry in the cursor table.
pub const CURRENT_CURSOR_KEY: u16 = 0;

/// Table storing the index cursor position.
pub const CURSOR_TABLE: TableDefinition<'static, u16, Vec<u8>> =
    TableDefinition::new("index-cursor");

fn map_db_error(error: impl std::fmt::Display) -> IndexError {
    IndexError::DbError(error.to_string())
}

impl From<Error> for IndexError {
    fn from(error: Error) -> Self {
        IndexError::DbError(error.to_string())
    }
}

impl From<archive::RedbArchiveError> for IndexError {
    fn from(error: archive::RedbArchiveError) -> Self {
        IndexError::DbError(error.to_string())
    }
}

#[derive(Clone)]
pub struct IndexStore {
    db: Arc<Database>,
}

impl IndexStore {
    pub fn open(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, Error> {
        let db = Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "index db is repairing")
            })
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)?;

        let store = Self { db: db.into() };

        store.initialize_schema_internal()?;

        Ok(store)
    }

    pub fn in_memory() -> Result<Self, Error> {
        let db =
            Database::builder().create_with_backend(::redb::backends::InMemoryBackend::new())?;

        let store = Self { db: db.into() };

        store.initialize_schema_internal()?;

        Ok(store)
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn count_utxo_by_address(&self, address: &[u8]) -> Result<u64, Error> {
        let rx = self.db.begin_read()?;
        state::utxoset::FilterIndexes::count_within_key(
            &rx,
            state::utxoset::FilterIndexes::BY_ADDRESS,
            address,
        )
    }

    pub fn iter_utxo_by_address(
        &self,
        address: &[u8],
    ) -> Result<state::utxoset::UtxoKeyIterator, Error> {
        let rx = self.db.begin_read()?;
        state::utxoset::FilterIndexes::iter_within_key(
            &rx,
            state::utxoset::FilterIndexes::BY_ADDRESS,
            address,
        )
    }

    pub fn utxo_index_stats(&self) -> Result<HashMap<&'static str, TableStats>, Error> {
        let rx = self.db.begin_read()?;
        state::utxoset::FilterIndexes::stats(&rx)
    }

    fn initialize_schema_internal(&self) -> Result<(), Error> {
        let mut wx = self.db.begin_write()?;
        wx.set_durability(Durability::Immediate)?;

        let _ = wx.open_table(CURSOR_TABLE)?;
        state::utxoset::FilterIndexes::initialize(&wx)?;
        archive::indexes::Indexes::initialize(&wx)?;

        wx.commit()?;

        Ok(())
    }

    fn read_cursor_internal(rx: &ReadTransaction) -> Result<Option<ChainPoint>, Error> {
        let table = rx.open_table(CURSOR_TABLE)?;
        let value = table.get(CURRENT_CURSOR_KEY)?.map(|x| x.value());

        let Some(value) = value else {
            return Ok(None);
        };

        let point = bincode::deserialize(&value).map_err(|_| Error::InvalidCursor)?;

        Ok(Some(point))
    }
}

/// Writer for batched index operations.
///
/// Holds a write transaction that is committed when `commit()` is called,
/// allowing multiple index operations to be batched together.
pub struct IndexStoreWriter {
    wx: WriteTransaction,
}

impl CoreIndexWriter for IndexStoreWriter {
    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), IndexError> {
        state::utxoset::FilterIndexes::apply(&self.wx, delta).map_err(IndexError::from)?;
        Ok(())
    }

    fn apply_archive(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError> {
        archive::indexes::Indexes::apply(&self.wx, point, tags).map_err(IndexError::from)?;
        Ok(())
    }

    fn undo_archive(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError> {
        archive::indexes::Indexes::undo(&self.wx, point, tags).map_err(IndexError::from)?;
        Ok(())
    }

    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), IndexError> {
        let mut table = self.wx.open_table(CURSOR_TABLE).map_err(map_db_error)?;
        let point = bincode::serialize(&cursor).unwrap();
        table
            .insert(CURRENT_CURSOR_KEY, &point)
            .map_err(map_db_error)?;
        Ok(())
    }

    fn commit(self) -> Result<(), IndexError> {
        self.wx.commit().map_err(map_db_error)?;
        Ok(())
    }
}

/// Iterator that yields slot values from the index.
pub struct SlotIter {
    _rx: ReadTransaction,
    range: archive::indexes::SlotKeyIterator,
}

impl Iterator for SlotIter {
    type Item = Result<BlockSlot, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.range.next()?;
        Some(next.map_err(map_db_error))
    }
}

impl DoubleEndedIterator for SlotIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        let next = self.range.next_back()?;
        Some(next.map_err(map_db_error))
    }
}

impl CoreIndexStore for IndexStore {
    type Writer = IndexStoreWriter;
    type SlotIter = SlotIter;

    fn start_writer(&self) -> Result<Self::Writer, IndexError> {
        let mut wx = self.db.begin_write().map_err(map_db_error)?;
        wx.set_durability(Durability::Immediate)
            .map_err(map_db_error)?;
        wx.set_quick_repair(true);
        Ok(IndexStoreWriter { wx })
    }

    fn initialize_schema(&self) -> Result<(), IndexError> {
        self.initialize_schema_internal().map_err(IndexError::from)
    }

    fn copy(&self, target: &Self) -> Result<(), IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let wx = target.db.begin_write().map_err(map_db_error)?;

        // Copy cursor
        if let Some(cursor) = Self::read_cursor_internal(&rx).map_err(IndexError::from)? {
            let mut cursor_table = wx.open_table(CURSOR_TABLE).map_err(map_db_error)?;
            let point = bincode::serialize(&cursor).unwrap();
            cursor_table
                .insert(CURRENT_CURSOR_KEY, &point)
                .map_err(map_db_error)?;
        }

        state::utxoset::FilterIndexes::copy(&rx, &wx).map_err(IndexError::from)?;
        archive::indexes::Indexes::copy(&rx, &wx).map_err(IndexError::from)?;

        wx.commit().map_err(map_db_error)?;

        Ok(())
    }

    fn read_cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        Self::read_cursor_internal(&rx).map_err(IndexError::from)
    }

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_address(&rx, address)?;
        Ok(out)
    }

    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_payment(&rx, payment)?;
        Ok(out)
    }

    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_stake(&rx, stake)?;
        Ok(out)
    }

    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_policy(&rx, policy)?;
        Ok(out)
    }

    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let out = state::utxoset::FilterIndexes::get_by_asset(&rx, asset)?;
        Ok(out)
    }

    fn slot_for_block_hash(&self, block_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        archive::indexes::Indexes::get_by_block_hash(&rx, block_hash).map_err(IndexError::from)
    }

    fn slot_for_block_number(&self, number: &u64) -> Result<Option<BlockSlot>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        archive::indexes::Indexes::get_by_block_number(&rx, number).map_err(IndexError::from)
    }

    fn slot_for_tx_hash(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        archive::indexes::Indexes::get_by_tx_hash(&rx, tx_hash).map_err(IndexError::from)
    }

    fn slots_for_datum_hash(
        &self,
        datum_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        archive::indexes::Indexes::get_by_datum_hash(&rx, datum_hash, start_slot, end_slot)
            .map_err(IndexError::from)
    }

    fn slots_for_spent_txo(
        &self,
        spent_txo: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        archive::indexes::Indexes::get_by_spent_txo(&rx, spent_txo, start_slot, end_slot)
            .map_err(IndexError::from)
    }

    fn slots_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let range = archive::indexes::Indexes::iter_by_address(&rx, address, start_slot, end_slot)?;
        Ok(SlotIter { _rx: rx, range })
    }

    fn slots_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let range = archive::indexes::Indexes::iter_by_asset(&rx, asset, start_slot, end_slot)?;
        Ok(SlotIter { _rx: rx, range })
    }

    fn slots_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let range = archive::indexes::Indexes::iter_by_payment(&rx, payment, start_slot, end_slot)?;
        Ok(SlotIter { _rx: rx, range })
    }

    fn slots_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let range = archive::indexes::Indexes::iter_by_stake(&rx, stake, start_slot, end_slot)?;
        Ok(SlotIter { _rx: rx, range })
    }

    fn slots_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let range =
            archive::indexes::Indexes::iter_by_account_certs(&rx, account, start_slot, end_slot)?;
        Ok(SlotIter { _rx: rx, range })
    }

    fn slots_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        let range =
            archive::indexes::Indexes::iter_by_metadata(&rx, metadata, start_slot, end_slot)?;
        Ok(SlotIter { _rx: rx, range })
    }
}
