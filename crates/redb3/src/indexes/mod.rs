//! Index store implementation using Redb v3.
//!
//! This module provides the `IndexStore` implementation for Redb, supporting
//! both UTxO filter indexes (for current state queries) and archive indexes
//! (for historical queries).

use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};

use dolos_core::{
    config::RedbIndexConfig, ArchiveIndexDelta, BlockSlot, ChainPoint, IndexDelta, IndexError,
    IndexStore as CoreIndexStore, IndexWriter as CoreIndexWriter, Tag, TagDimension, TxoRef,
    UtxoSet,
};
use redb::{
    Database, Durability, MultimapTableDefinition, ReadTransaction, ReadableDatabase,
    ReadableTableMetadata as _, TableDefinition, TableStats, WriteTransaction,
};
use tracing::warn;

use crate::{archive, Error};

// ============================================================================
// UTxO Filter Indexes
// ============================================================================

type UtxosKey = (&'static [u8; 32], u32);

pub struct UtxoKeyIterator(redb::MultimapValue<'static, UtxosKey>);

impl Iterator for UtxoKeyIterator {
    type Item = Result<TxoRef, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.0.next()?;

        let out = item
            .map(|item| {
                let (hash, idx) = item.value();
                TxoRef((*hash).into(), idx)
            })
            .map_err(Error::from);

        Some(out)
    }
}

/// UTxO filter dimension constants (must match dolos-cardano dimensions).
pub mod utxo_dimensions {
    pub const ADDRESS: &str = "address";
    pub const PAYMENT: &str = "payment";
    pub const STAKE: &str = "stake";
    pub const POLICY: &str = "policy";
    pub const ASSET: &str = "asset";
}

/// Archive index dimension constants (must match dolos-cardano dimensions).
pub mod archive_dimensions {
    pub const ADDRESS: &str = "address";
    pub const PAYMENT: &str = "payment";
    pub const STAKE: &str = "stake";
    pub const POLICY: &str = "policy";
    pub const ASSET: &str = "asset";
    pub const DATUM: &str = "datum";
    pub const SPENT_TXO: &str = "spent_txo";
    pub const ACCOUNT_CERTS: &str = "account_certs";
    pub const METADATA: &str = "metadata";
    pub const SCRIPT: &str = "script";
}

pub struct FilterIndexes;

impl FilterIndexes {
    pub const BY_ADDRESS: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("byaddress");

    pub const BY_PAYMENT: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("bypayment");

    pub const BY_STAKE: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("bystake");

    pub const BY_POLICY: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("bypolicy");

    pub const BY_ASSET: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("byasset");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_multimap_table(Self::BY_ADDRESS)?;
        wx.open_multimap_table(Self::BY_PAYMENT)?;
        wx.open_multimap_table(Self::BY_STAKE)?;
        wx.open_multimap_table(Self::BY_POLICY)?;
        wx.open_multimap_table(Self::BY_ASSET)?;

        Ok(())
    }

    /// Get the table definition for a given dimension.
    fn table_for_dimension(
        dimension: TagDimension,
    ) -> Option<MultimapTableDefinition<'static, &'static [u8], UtxosKey>> {
        match dimension {
            utxo_dimensions::ADDRESS => Some(Self::BY_ADDRESS),
            utxo_dimensions::PAYMENT => Some(Self::BY_PAYMENT),
            utxo_dimensions::STAKE => Some(Self::BY_STAKE),
            utxo_dimensions::POLICY => Some(Self::BY_POLICY),
            utxo_dimensions::ASSET => Some(Self::BY_ASSET),
            _ => None,
        }
    }

    fn get_by_key(
        rx: &ReadTransaction,
        table_def: MultimapTableDefinition<&[u8], UtxosKey>,
        key: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        let table = rx.open_multimap_table(table_def)?;

        let mut out = HashSet::new();

        for item in table.get(key)? {
            let item = item?;
            let (hash, idx) = item.value();
            out.insert(TxoRef((*hash).into(), idx));
        }

        Ok(out)
    }

    pub fn count_within_key(
        rx: &ReadTransaction,
        table_def: MultimapTableDefinition<&[u8], UtxosKey>,
        key: &[u8],
    ) -> Result<u64, Error> {
        let table = rx.open_multimap_table(table_def)?;

        let count = table.get(key)?.len();

        Ok(count)
    }

    pub fn iter_within_key(
        rx: &ReadTransaction,
        table_def: MultimapTableDefinition<&[u8], UtxosKey>,
        key: &[u8],
    ) -> Result<UtxoKeyIterator, Error> {
        let table = rx.open_multimap_table(table_def)?;

        let inner = table.get(key)?;

        Ok(UtxoKeyIterator(inner))
    }

    pub fn get_by_address(
        rx: &ReadTransaction,
        exact_address: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_ADDRESS, exact_address)
    }

    pub fn get_by_payment(
        rx: &ReadTransaction,
        payment_part: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_PAYMENT, payment_part)
    }

    pub fn get_by_stake(rx: &ReadTransaction, stake_part: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_STAKE, stake_part)
    }

    pub fn get_by_policy(rx: &ReadTransaction, policy: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_POLICY, policy)
    }

    pub fn get_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_ASSET, asset)
    }

    /// Get UTxOs by tag dimension and key.
    pub fn get_by_tag(
        rx: &ReadTransaction,
        dimension: TagDimension,
        key: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        let table_def = Self::table_for_dimension(dimension)
            .ok_or_else(|| Error::InvalidDimension(dimension.to_string()))?;
        Self::get_by_key(rx, table_def, key)
    }

    /// Apply UTxO filter changes from an IndexDelta.
    ///
    /// Inserts produced UTxOs and removes consumed UTxOs from filter indexes.
    pub fn apply(wx: &WriteTransaction, delta: &IndexDelta) -> Result<(), Error> {
        let mut address_table = wx.open_multimap_table(Self::BY_ADDRESS)?;
        let mut payment_table = wx.open_multimap_table(Self::BY_PAYMENT)?;
        let mut stake_table = wx.open_multimap_table(Self::BY_STAKE)?;
        let mut policy_table = wx.open_multimap_table(Self::BY_POLICY)?;
        let mut asset_table = wx.open_multimap_table(Self::BY_ASSET)?;

        // Insert produced UTxOs
        for (txo_ref, tags) in &delta.utxo.produced {
            let v: (&[u8; 32], u32) = (&txo_ref.0, txo_ref.1);

            for tag in tags {
                match tag.dimension {
                    utxo_dimensions::ADDRESS => {
                        address_table.insert(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::PAYMENT => {
                        payment_table.insert(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::STAKE => {
                        stake_table.insert(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::POLICY => {
                        policy_table.insert(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::ASSET => {
                        asset_table.insert(tag.key.as_slice(), v)?;
                    }
                    _ => {} // Ignore unknown dimensions
                }
            }
        }

        // Remove consumed UTxOs
        for (txo_ref, tags) in &delta.utxo.consumed {
            let v: (&[u8; 32], u32) = (&txo_ref.0, txo_ref.1);

            for tag in tags {
                match tag.dimension {
                    utxo_dimensions::ADDRESS => {
                        address_table.remove(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::PAYMENT => {
                        payment_table.remove(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::STAKE => {
                        stake_table.remove(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::POLICY => {
                        policy_table.remove(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::ASSET => {
                        asset_table.remove(tag.key.as_slice(), v)?;
                    }
                    _ => {} // Ignore unknown dimensions
                }
            }
        }

        Ok(())
    }

    /// Undo UTxO filter changes from an IndexDelta (for rollback).
    ///
    /// Removes produced UTxOs and restores consumed UTxOs to filter indexes.
    pub fn undo(wx: &WriteTransaction, delta: &IndexDelta) -> Result<(), Error> {
        let mut address_table = wx.open_multimap_table(Self::BY_ADDRESS)?;
        let mut payment_table = wx.open_multimap_table(Self::BY_PAYMENT)?;
        let mut stake_table = wx.open_multimap_table(Self::BY_STAKE)?;
        let mut policy_table = wx.open_multimap_table(Self::BY_POLICY)?;
        let mut asset_table = wx.open_multimap_table(Self::BY_ASSET)?;

        // Remove produced UTxOs (undo insertion)
        for (txo_ref, tags) in &delta.utxo.produced {
            let v: (&[u8; 32], u32) = (&txo_ref.0, txo_ref.1);

            for tag in tags {
                match tag.dimension {
                    utxo_dimensions::ADDRESS => {
                        address_table.remove(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::PAYMENT => {
                        payment_table.remove(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::STAKE => {
                        stake_table.remove(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::POLICY => {
                        policy_table.remove(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::ASSET => {
                        asset_table.remove(tag.key.as_slice(), v)?;
                    }
                    _ => {}
                }
            }
        }

        // Restore consumed UTxOs (undo removal)
        for (txo_ref, tags) in &delta.utxo.consumed {
            let v: (&[u8; 32], u32) = (&txo_ref.0, txo_ref.1);

            for tag in tags {
                match tag.dimension {
                    utxo_dimensions::ADDRESS => {
                        address_table.insert(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::PAYMENT => {
                        payment_table.insert(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::STAKE => {
                        stake_table.insert(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::POLICY => {
                        policy_table.insert(tag.key.as_slice(), v)?;
                    }
                    utxo_dimensions::ASSET => {
                        asset_table.insert(tag.key.as_slice(), v)?;
                    }
                    _ => {}
                }
            }
        }

        Ok(())
    }

    fn copy_table<K: ::redb::Key, V: ::redb::Key + ::redb::Value>(
        rx: &ReadTransaction,
        wx: &WriteTransaction,
        def: MultimapTableDefinition<K, V>,
    ) -> Result<(), Error> {
        let source = rx.open_multimap_table(def)?;
        let mut target = wx.open_multimap_table(def)?;

        let all = source.range::<K::SelfType<'static>>(..)?;

        for entry in all {
            let (key, values) = entry?;
            for value in values {
                let value = value?;
                target.insert(key.value(), value.value())?;
            }
        }

        Ok(())
    }

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        Self::copy_table(rx, wx, Self::BY_ADDRESS)?;
        Self::copy_table(rx, wx, Self::BY_PAYMENT)?;
        Self::copy_table(rx, wx, Self::BY_STAKE)?;
        Self::copy_table(rx, wx, Self::BY_POLICY)?;
        Self::copy_table(rx, wx, Self::BY_ASSET)?;

        Ok(())
    }

    pub fn stats(rx: &ReadTransaction) -> Result<HashMap<&'static str, redb::TableStats>, Error> {
        let address = rx.open_multimap_table(Self::BY_ADDRESS)?;
        let payment = rx.open_multimap_table(Self::BY_PAYMENT)?;
        let stake = rx.open_multimap_table(Self::BY_STAKE)?;
        let policy = rx.open_multimap_table(Self::BY_POLICY)?;
        let asset = rx.open_multimap_table(Self::BY_ASSET)?;

        Ok(HashMap::from_iter([
            ("address", address.stats()?),
            ("payment", payment.stats()?),
            ("stake", stake.stats()?),
            ("policy", policy.stats()?),
            ("asset", asset.stats()?),
        ]))
    }
}

// ============================================================================
// Archive Index Helpers
// ============================================================================

/// Apply archive index changes from an ArchiveIndexDelta.
fn apply_archive_delta(wx: &WriteTransaction, block: &ArchiveIndexDelta) -> Result<(), Error> {
    let slot = block.slot;

    // Block hash index
    if !block.block_hash.is_empty() {
        let mut table = wx.open_table(archive::indexes::BlockHashIndexTable::DEF)?;
        table.insert(block.block_hash.as_slice(), slot)?;
    }

    // Block number index
    if let Some(number) = block.block_number {
        let mut table = wx.open_table(archive::indexes::BlockNumberIndexTable::DEF)?;
        table.insert(number, slot)?;
    }

    // Transaction hash index
    if !block.tx_hashes.is_empty() {
        let mut table = wx.open_table(archive::indexes::TxHashIndexTable::DEF)?;
        for tx_hash in &block.tx_hashes {
            table.insert(tx_hash.as_slice(), slot)?;
        }
    }

    // Tag-based indexes
    for tag in &block.tags {
        insert_archive_tag(wx, tag, slot)?;
    }

    Ok(())
}

/// Undo archive index changes from an ArchiveIndexDelta.
fn undo_archive_delta(wx: &WriteTransaction, block: &ArchiveIndexDelta) -> Result<(), Error> {
    let slot = block.slot;

    // Block hash index
    if !block.block_hash.is_empty() {
        let mut table = wx.open_table(archive::indexes::BlockHashIndexTable::DEF)?;
        table.remove(block.block_hash.as_slice())?;
    }

    // Block number index
    if let Some(number) = block.block_number {
        let mut table = wx.open_table(archive::indexes::BlockNumberIndexTable::DEF)?;
        table.remove(number)?;
    }

    // Transaction hash index
    if !block.tx_hashes.is_empty() {
        let mut table = wx.open_table(archive::indexes::TxHashIndexTable::DEF)?;
        for tx_hash in &block.tx_hashes {
            table.remove(tx_hash.as_slice())?;
        }
    }

    // Tag-based indexes
    for tag in &block.tags {
        remove_archive_tag(wx, tag, slot)?;
    }

    Ok(())
}

/// Insert a single archive tag.
fn insert_archive_tag(wx: &WriteTransaction, tag: &Tag, slot: BlockSlot) -> Result<(), Error> {
    use archive::indexes::*;
    use xxhash_rust::xxh3::xxh3_64;

    let key_builder = archive::indexes::key_builder();
    let bucketed_key = if tag.dimension == archive_dimensions::METADATA {
        let metadata = u64::from_be_bytes(tag.key.as_slice().try_into().map_err(|_| {
            Error::ArchiveError("metadata key must be 8 bytes".to_string())
        })?);
        key_builder.bucketed_key(metadata, slot)
    } else {
        let key = xxh3_64(&tag.key);
        key_builder.bucketed_key(key, slot)
    };

    match tag.dimension {
        archive_dimensions::ADDRESS => {
            let mut table = wx.open_multimap_table(AddressApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        archive_dimensions::PAYMENT => {
            let mut table = wx.open_multimap_table(AddressPaymentPartApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        archive_dimensions::STAKE => {
            let mut table = wx.open_multimap_table(AddressStakePartApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        archive_dimensions::POLICY => {
            let mut table = wx.open_multimap_table(PolicyApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        archive_dimensions::ASSET => {
            let mut table = wx.open_multimap_table(AssetApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        archive_dimensions::DATUM => {
            let mut table = wx.open_multimap_table(DatumHashApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        archive_dimensions::SPENT_TXO => {
            let mut table = wx.open_multimap_table(SpentTxoApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        archive_dimensions::ACCOUNT_CERTS => {
            let mut table = wx.open_multimap_table(AccountCertsApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        archive_dimensions::METADATA => {
            let mut table = wx.open_multimap_table(MetadataApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        archive_dimensions::SCRIPT => {
            let mut table = wx.open_multimap_table(ScriptHashApproxIndexTable::DEF)?;
            table.insert(bucketed_key, slot)?;
        }
        _ => {} // Ignore unknown dimensions
    }

    Ok(())
}

/// Remove a single archive tag.
fn remove_archive_tag(wx: &WriteTransaction, tag: &Tag, slot: BlockSlot) -> Result<(), Error> {
    use archive::indexes::*;
    use xxhash_rust::xxh3::xxh3_64;

    let key_builder = archive::indexes::key_builder();
    let bucketed_key = if tag.dimension == archive_dimensions::METADATA {
        let metadata = u64::from_be_bytes(tag.key.as_slice().try_into().map_err(|_| {
            Error::ArchiveError("metadata key must be 8 bytes".to_string())
        })?);
        key_builder.bucketed_key(metadata, slot)
    } else {
        let key = xxh3_64(&tag.key);
        key_builder.bucketed_key(key, slot)
    };

    match tag.dimension {
        archive_dimensions::ADDRESS => {
            let mut table = wx.open_multimap_table(AddressApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        archive_dimensions::PAYMENT => {
            let mut table = wx.open_multimap_table(AddressPaymentPartApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        archive_dimensions::STAKE => {
            let mut table = wx.open_multimap_table(AddressStakePartApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        archive_dimensions::POLICY => {
            let mut table = wx.open_multimap_table(PolicyApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        archive_dimensions::ASSET => {
            let mut table = wx.open_multimap_table(AssetApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        archive_dimensions::DATUM => {
            let mut table = wx.open_multimap_table(DatumHashApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        archive_dimensions::SPENT_TXO => {
            let mut table = wx.open_multimap_table(SpentTxoApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        archive_dimensions::ACCOUNT_CERTS => {
            let mut table = wx.open_multimap_table(AccountCertsApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        archive_dimensions::METADATA => {
            let mut table = wx.open_multimap_table(MetadataApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        archive_dimensions::SCRIPT => {
            let mut table = wx.open_multimap_table(ScriptHashApproxIndexTable::DEF)?;
            table.remove(bucketed_key, slot)?;
        }
        _ => {} // Ignore unknown dimensions
    }

    Ok(())
}

// ============================================================================
// IndexStore Implementation
// ============================================================================

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
    /// Gracefully shutdown the index store.
    ///
    /// For Redb, this is a no-op since Redb handles cleanup automatically
    /// during drop without blocking issues.
    pub fn shutdown(&self) -> Result<(), Error> {
        Ok(())
    }

    pub fn open(path: impl AsRef<Path>, config: &RedbIndexConfig) -> Result<Self, Error> {
        let db = Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "index db is repairing")
            })
            .set_cache_size(1024 * 1024 * config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB))
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
        FilterIndexes::count_within_key(&rx, FilterIndexes::BY_ADDRESS, address)
    }

    pub fn iter_utxo_by_address(&self, address: &[u8]) -> Result<UtxoKeyIterator, Error> {
        let rx = self.db.begin_read()?;
        FilterIndexes::iter_within_key(&rx, FilterIndexes::BY_ADDRESS, address)
    }

    pub fn utxo_index_stats(&self) -> Result<HashMap<&'static str, TableStats>, Error> {
        let rx = self.db.begin_read()?;
        FilterIndexes::stats(&rx)
    }

    fn initialize_schema_internal(&self) -> Result<(), Error> {
        let mut wx = self.db.begin_write()?;
        wx.set_durability(Durability::Immediate)?;

        let _ = wx.open_table(CURSOR_TABLE)?;
        FilterIndexes::initialize(&wx)?;
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

    fn set_cursor_internal(wx: &WriteTransaction, cursor: &ChainPoint) -> Result<(), Error> {
        let mut table = wx.open_table(CURSOR_TABLE)?;
        let point = bincode::serialize(cursor).unwrap();
        table.insert(CURRENT_CURSOR_KEY, &point)?;
        Ok(())
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
    fn apply(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        // Apply UTxO filter changes
        FilterIndexes::apply(&self.wx, delta).map_err(IndexError::from)?;

        // Apply archive index changes
        for block in &delta.archive {
            apply_archive_delta(&self.wx, block).map_err(IndexError::from)?;
        }

        // Set cursor
        IndexStore::set_cursor_internal(&self.wx, &delta.cursor).map_err(IndexError::from)?;

        Ok(())
    }

    fn undo(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        // Undo UTxO filter changes
        FilterIndexes::undo(&self.wx, delta).map_err(IndexError::from)?;

        // Undo archive index changes (in reverse order)
        for block in delta.archive.iter().rev() {
            undo_archive_delta(&self.wx, block).map_err(IndexError::from)?;
        }

        // Note: cursor should be set by the caller after undo

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
            Self::set_cursor_internal(&wx, &cursor).map_err(IndexError::from)?;
        }

        FilterIndexes::copy(&rx, &wx).map_err(IndexError::from)?;
        archive::indexes::Indexes::copy(&rx, &wx).map_err(IndexError::from)?;

        wx.commit().map_err(map_db_error)?;

        Ok(())
    }

    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        Self::read_cursor_internal(&rx).map_err(IndexError::from)
    }

    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        FilterIndexes::get_by_tag(&rx, dimension, key).map_err(IndexError::from)
    }

    fn slot_by_block_hash(&self, block_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        archive::indexes::Indexes::get_by_block_hash(&rx, block_hash).map_err(IndexError::from)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        archive::indexes::Indexes::get_by_block_number(&rx, &number).map_err(IndexError::from)
    }

    fn slot_by_tx_hash(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;
        archive::indexes::Indexes::get_by_tx_hash(&rx, tx_hash).map_err(IndexError::from)
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        let rx = self.db.begin_read().map_err(map_db_error)?;

        let range = match dimension {
            archive_dimensions::ADDRESS => {
                archive::indexes::Indexes::iter_by_address(&rx, key, start, end)?
            }
            archive_dimensions::PAYMENT => {
                archive::indexes::Indexes::iter_by_payment(&rx, key, start, end)?
            }
            archive_dimensions::STAKE => {
                archive::indexes::Indexes::iter_by_stake(&rx, key, start, end)?
            }
            archive_dimensions::ASSET => {
                archive::indexes::Indexes::iter_by_asset(&rx, key, start, end)?
            }
            archive_dimensions::POLICY => {
                archive::indexes::Indexes::iter_by_policy(&rx, key, start, end)?
            }
            archive_dimensions::DATUM => {
                archive::indexes::Indexes::iter_by_datum(&rx, key, start, end)?
            }
            archive_dimensions::SPENT_TXO => {
                archive::indexes::Indexes::iter_by_spent_txo(&rx, key, start, end)?
            }
            archive_dimensions::ACCOUNT_CERTS => {
                archive::indexes::Indexes::iter_by_account_certs(&rx, key, start, end)?
            }
            archive_dimensions::METADATA => {
                // Metadata is keyed by u64, need to parse the bytes
                let metadata = u64::from_be_bytes(key.try_into().map_err(|_| {
                    IndexError::CodecError("metadata key must be 8 bytes".to_string())
                })?);
                archive::indexes::Indexes::iter_by_metadata(&rx, &metadata, start, end)?
            }
            archive_dimensions::SCRIPT => {
                archive::indexes::Indexes::iter_by_script(&rx, key, start, end)?
            }
            _ => {
                return Err(IndexError::DimensionNotFound(dimension.to_string()));
            }
        };

        Ok(SlotIter { _rx: rx, range })
    }
}
