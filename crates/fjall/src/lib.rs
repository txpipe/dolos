//! Fjall-based index store implementation for Dolos.
//!
//! This crate provides an implementation of the `IndexStore` trait using fjall,
//! an LSM-tree based embedded database. This is optimized for write-heavy workloads
//! with many keys, which is ideal for blockchain index data.
//!
//! ## Key Design
//!
//! - **UTxO filters**: Use composite keys `lookup_key ++ txo_ref` with prefix scanning
//! - **Archive exact lookups**: Direct key-value (hash -> slot)
//! - **Archive approx lookups**: Use composite keys `xxh3(data) ++ slot` with prefix scanning
//!
//! All multi-byte integers are big-endian encoded for correct lexicographic ordering.

use std::path::Path;
use std::sync::{Arc, Mutex};

use dolos_core::{
    BlockSlot, ChainPoint, IndexError, IndexStore as CoreIndexStore,
    IndexWriter as CoreIndexWriter, SlotTags, UtxoSet, UtxoSetDelta,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode};

pub mod archive;
pub mod keys;
pub mod utxo;

// Re-export the iterator type
pub use archive::SlotIterator as SlotIter;

/// Default cache size in MB
const DEFAULT_CACHE_SIZE_MB: usize = 500;

/// Keyspace names
mod keyspace_names {
    pub const CURSOR: &str = "cursor";
    // UTxO filters
    pub const UTXO_ADDRESS: &str = "utxo-address";
    pub const UTXO_PAYMENT: &str = "utxo-payment";
    pub const UTXO_STAKE: &str = "utxo-stake";
    pub const UTXO_POLICY: &str = "utxo-policy";
    pub const UTXO_ASSET: &str = "utxo-asset";
    // Archive exact
    pub const ARCHIVE_BLOCKHASH: &str = "archive-blockhash";
    pub const ARCHIVE_BLOCKNUM: &str = "archive-blocknum";
    pub const ARCHIVE_TXHASH: &str = "archive-txhash";
    // Archive approx
    pub const ARCHIVE_ADDRESS: &str = "archive-address";
    pub const ARCHIVE_PAYMENT: &str = "archive-payment";
    pub const ARCHIVE_STAKE: &str = "archive-stake";
    pub const ARCHIVE_ASSET: &str = "archive-asset";
    pub const ARCHIVE_DATUM: &str = "archive-datum";
    pub const ARCHIVE_SPENTTXO: &str = "archive-spenttxo";
    pub const ARCHIVE_ACCOUNT: &str = "archive-account";
    pub const ARCHIVE_METADATA: &str = "archive-metadata";
}

/// Error type for fjall index store operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("lock poisoned")]
    LockPoisoned,
}

impl From<Error> for IndexError {
    fn from(error: Error) -> Self {
        IndexError::DbError(error.to_string())
    }
}

/// Key for the cursor entry
const CURSOR_KEY: &[u8] = &[0u8];

/// Fjall-based index store implementation
#[derive(Clone)]
pub struct IndexStore {
    db: Arc<Database>,
    // Cursor
    cursor: Keyspace,
    // UTxO filter keyspaces
    utxo_address: Keyspace,
    utxo_payment: Keyspace,
    utxo_stake: Keyspace,
    utxo_policy: Keyspace,
    utxo_asset: Keyspace,
    // Archive exact keyspaces
    archive_blockhash: Keyspace,
    archive_blocknum: Keyspace,
    archive_txhash: Keyspace,
    // Archive approx keyspaces
    archive_address: Keyspace,
    archive_payment: Keyspace,
    archive_stake: Keyspace,
    archive_asset: Keyspace,
    archive_datum: Keyspace,
    archive_spenttxo: Keyspace,
    archive_account: Keyspace,
    archive_metadata: Keyspace,
}

impl IndexStore {
    /// Open or create an index store at the given path
    pub fn open(path: impl AsRef<Path>, cache_size_mb: Option<usize>) -> Result<Self, Error> {
        let cache_size = cache_size_mb.unwrap_or(DEFAULT_CACHE_SIZE_MB);
        let cache_bytes = (cache_size * 1024 * 1024) as u64;

        let db = Database::builder(path.as_ref())
            .cache_size(cache_bytes)
            .open()?;

        Self::from_database(db)
    }

    /// Create an index store from an existing database
    fn from_database(db: Database) -> Result<Self, Error> {
        // Helper closure to create default keyspace options
        let opts = || KeyspaceCreateOptions::default();

        // Create or open all keyspaces
        let cursor = db.keyspace(keyspace_names::CURSOR, opts)?;

        let utxo_address = db.keyspace(keyspace_names::UTXO_ADDRESS, opts)?;
        let utxo_payment = db.keyspace(keyspace_names::UTXO_PAYMENT, opts)?;
        let utxo_stake = db.keyspace(keyspace_names::UTXO_STAKE, opts)?;
        let utxo_policy = db.keyspace(keyspace_names::UTXO_POLICY, opts)?;
        let utxo_asset = db.keyspace(keyspace_names::UTXO_ASSET, opts)?;

        let archive_blockhash = db.keyspace(keyspace_names::ARCHIVE_BLOCKHASH, opts)?;
        let archive_blocknum = db.keyspace(keyspace_names::ARCHIVE_BLOCKNUM, opts)?;
        let archive_txhash = db.keyspace(keyspace_names::ARCHIVE_TXHASH, opts)?;

        let archive_address = db.keyspace(keyspace_names::ARCHIVE_ADDRESS, opts)?;
        let archive_payment = db.keyspace(keyspace_names::ARCHIVE_PAYMENT, opts)?;
        let archive_stake = db.keyspace(keyspace_names::ARCHIVE_STAKE, opts)?;
        let archive_asset = db.keyspace(keyspace_names::ARCHIVE_ASSET, opts)?;
        let archive_datum = db.keyspace(keyspace_names::ARCHIVE_DATUM, opts)?;
        let archive_spenttxo = db.keyspace(keyspace_names::ARCHIVE_SPENTTXO, opts)?;
        let archive_account = db.keyspace(keyspace_names::ARCHIVE_ACCOUNT, opts)?;
        let archive_metadata = db.keyspace(keyspace_names::ARCHIVE_METADATA, opts)?;

        Ok(Self {
            db: Arc::new(db),
            cursor,
            utxo_address,
            utxo_payment,
            utxo_stake,
            utxo_policy,
            utxo_asset,
            archive_blockhash,
            archive_blocknum,
            archive_txhash,
            archive_address,
            archive_payment,
            archive_stake,
            archive_asset,
            archive_datum,
            archive_spenttxo,
            archive_account,
            archive_metadata,
        })
    }

    /// Get UTxO keyspaces reference
    fn utxo_keyspaces(&self) -> utxo::UtxoKeyspaces<'_> {
        utxo::UtxoKeyspaces {
            address: &self.utxo_address,
            payment: &self.utxo_payment,
            stake: &self.utxo_stake,
            policy: &self.utxo_policy,
            asset: &self.utxo_asset,
        }
    }

    /// Get archive keyspaces reference
    fn archive_keyspaces(&self) -> archive::ArchiveKeyspaces<'_> {
        archive::ArchiveKeyspaces {
            blockhash: &self.archive_blockhash,
            blocknum: &self.archive_blocknum,
            txhash: &self.archive_txhash,
            address: &self.archive_address,
            payment: &self.archive_payment,
            stake: &self.archive_stake,
            asset: &self.archive_asset,
            datum: &self.archive_datum,
            spenttxo: &self.archive_spenttxo,
            account: &self.archive_account,
            metadata: &self.archive_metadata,
        }
    }
}

/// Writer for batched index operations.
///
/// Uses interior mutability via Mutex because the `IndexWriter` trait
/// requires `&self` for all methods, but fjall's `OwnedWriteBatch` needs
/// `&mut self` for insert/remove operations.
pub struct IndexStoreWriter {
    batch: Mutex<OwnedWriteBatch>,
    store: IndexStore,
}

impl CoreIndexWriter for IndexStoreWriter {
    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), IndexError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;
        utxo::apply(&mut batch, &self.store.utxo_keyspaces(), delta).map_err(IndexError::from)
    }

    fn apply_archive(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;
        archive::apply(&mut batch, &self.store.archive_keyspaces(), point, tags)
            .map_err(IndexError::from)
    }

    fn undo_archive(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;
        archive::undo(&mut batch, &self.store.archive_keyspaces(), point, tags)
            .map_err(IndexError::from)
    }

    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), IndexError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;
        let cursor_bytes = bincode::serialize(&cursor).map_err(|e| Error::Codec(e.to_string()))?;
        batch.insert(&self.store.cursor, CURSOR_KEY, cursor_bytes);
        Ok(())
    }

    fn commit(self) -> Result<(), IndexError> {
        let batch = self
            .batch
            .into_inner()
            .map_err(|_| Error::LockPoisoned)?
            .durability(Some(PersistMode::Buffer));

        batch.commit().map_err(|e| Error::Fjall(e))?;
        Ok(())
    }
}

impl CoreIndexStore for IndexStore {
    type Writer = IndexStoreWriter;
    type SlotIter = SlotIter;

    fn start_writer(&self) -> Result<Self::Writer, IndexError> {
        let batch = self.db.batch();
        Ok(IndexStoreWriter {
            batch: Mutex::new(batch),
            store: self.clone(),
        })
    }

    fn initialize_schema(&self) -> Result<(), IndexError> {
        // Keyspaces are created on open, nothing to do here
        Ok(())
    }

    fn copy(&self, _target: &Self) -> Result<(), IndexError> {
        todo!("copy not implemented for fjall index store")
    }

    fn read_cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        match self.cursor.get(CURSOR_KEY).map_err(Error::from)? {
            Some(value) => {
                let point: ChainPoint =
                    bincode::deserialize(&value).map_err(|e| Error::Codec(e.to_string()))?;
                Ok(Some(point))
            }
            None => Ok(None),
        }
    }

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, IndexError> {
        utxo::get_by_key(&self.utxo_address, address).map_err(IndexError::from)
    }

    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, IndexError> {
        utxo::get_by_key(&self.utxo_payment, payment).map_err(IndexError::from)
    }

    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, IndexError> {
        utxo::get_by_key(&self.utxo_stake, stake).map_err(IndexError::from)
    }

    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, IndexError> {
        utxo::get_by_key(&self.utxo_policy, policy).map_err(IndexError::from)
    }

    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, IndexError> {
        utxo::get_by_key(&self.utxo_asset, asset).map_err(IndexError::from)
    }

    fn slot_for_block_hash(&self, block_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        archive::get_by_block_hash(&self.archive_blockhash, block_hash).map_err(IndexError::from)
    }

    fn slot_for_block_number(&self, number: &u64) -> Result<Option<BlockSlot>, IndexError> {
        archive::get_by_block_number(&self.archive_blocknum, *number).map_err(IndexError::from)
    }

    fn slot_for_tx_hash(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        archive::get_by_tx_hash(&self.archive_txhash, tx_hash).map_err(IndexError::from)
    }

    fn slots_for_datum_hash(
        &self,
        datum_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, IndexError> {
        archive::get_slots_by_key(&self.archive_datum, datum_hash, start_slot, end_slot)
            .map_err(IndexError::from)
    }

    fn slots_for_spent_txo(
        &self,
        spent_txo: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, IndexError> {
        archive::get_slots_by_key(&self.archive_spenttxo, spent_txo, start_slot, end_slot)
            .map_err(IndexError::from)
    }

    fn slots_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        SlotIter::new(&self.archive_address, address, start_slot, end_slot)
            .map_err(IndexError::from)
    }

    fn slots_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        SlotIter::new(&self.archive_asset, asset, start_slot, end_slot).map_err(IndexError::from)
    }

    fn slots_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        SlotIter::new(&self.archive_payment, payment, start_slot, end_slot)
            .map_err(IndexError::from)
    }

    fn slots_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        SlotIter::new(&self.archive_stake, stake, start_slot, end_slot).map_err(IndexError::from)
    }

    fn slots_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        SlotIter::new(&self.archive_account, account, start_slot, end_slot)
            .map_err(IndexError::from)
    }

    fn slots_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        SlotIter::from_hash(&self.archive_metadata, *metadata, start_slot, end_slot)
            .map_err(IndexError::from)
    }
}
