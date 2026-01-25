//! Fjall-based index store implementation for Dolos.
//!
//! This module provides an implementation of the `IndexStore` trait using fjall,
//! an LSM-tree based embedded database. This is optimized for write-heavy workloads
//! with many keys, which is ideal for blockchain index data.
//!
//! ## Key Design
//!
//! - **UTxO filters**: Use composite keys `lookup_key ++ txo_ref` with prefix scanning
//! - **History exact lookups**: Direct key-value (hash -> slot)
//! - **History approx lookups**: Use composite keys `xxh3(data) ++ slot` with prefix scanning
//!
//! All multi-byte integers are big-endian encoded for correct lexicographic ordering.

use std::path::Path;
use std::sync::{Arc, Mutex};

use dolos_core::{
    BlockSlot, ChainPoint, IndexDelta, IndexError, IndexStore as CoreIndexStore,
    IndexWriter as CoreIndexWriter, TagDimension, UtxoSet,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode, Readable};

pub mod history;
pub mod utxos;

use history::HistoryKeyspaces;
use utxos::UtxoKeyspaces;

use crate::Error;

// Re-export the iterator type
pub use history::SlotIterator as SlotIter;

/// Default cache size in MB
const DEFAULT_CACHE_SIZE_MB: usize = 500;

/// Default max journal size in MB (2 GiB)
const DEFAULT_MAX_JOURNAL_SIZE_MB: usize = 2048;

/// Default flush on commit setting
const DEFAULT_FLUSH_ON_COMMIT: bool = true;

/// Keyspace names for index store
mod keyspace_names {
    pub const CURSOR: &str = "index-cursor";
    // UTxO filters
    pub const UTXO_ADDRESS: &str = "index-utxo-address";
    pub const UTXO_PAYMENT: &str = "index-utxo-payment";
    pub const UTXO_STAKE: &str = "index-utxo-stake";
    pub const UTXO_POLICY: &str = "index-utxo-policy";
    pub const UTXO_ASSET: &str = "index-utxo-asset";
    // History exact
    pub const HISTORY_BLOCKHASH: &str = "index-history-blockhash";
    pub const HISTORY_BLOCKNUM: &str = "index-history-blocknum";
    pub const HISTORY_TXHASH: &str = "index-history-txhash";
    // History approx
    pub const HISTORY_ADDRESS: &str = "index-history-address";
    pub const HISTORY_PAYMENT: &str = "index-history-payment";
    pub const HISTORY_STAKE: &str = "index-history-stake";
    pub const HISTORY_ASSET: &str = "index-history-asset";
    pub const HISTORY_POLICY: &str = "index-history-policy";
    pub const HISTORY_DATUM: &str = "index-history-datum";
    pub const HISTORY_SPENTTXO: &str = "index-history-spenttxo";
    pub const HISTORY_ACCOUNT: &str = "index-history-account";
    pub const HISTORY_METADATA: &str = "index-history-metadata";
    pub const HISTORY_SCRIPT: &str = "index-history-script";
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
    // History exact keyspaces
    history_blockhash: Keyspace,
    history_blocknum: Keyspace,
    history_txhash: Keyspace,
    // History approx keyspaces
    history_address: Keyspace,
    history_payment: Keyspace,
    history_stake: Keyspace,
    history_asset: Keyspace,
    history_policy: Keyspace,
    history_datum: Keyspace,
    history_spenttxo: Keyspace,
    history_account: Keyspace,
    history_metadata: Keyspace,
    history_script: Keyspace,
    // Configuration
    flush_on_commit: bool,
}

impl IndexStore {
    /// Open or create an index store at the given path
    pub fn open(
        path: impl AsRef<Path>,
        cache_size_mb: Option<usize>,
        max_journal_size_mb: Option<usize>,
        flush_on_commit: Option<bool>,
    ) -> Result<Self, Error> {
        let cache_size = cache_size_mb.unwrap_or(DEFAULT_CACHE_SIZE_MB);
        let cache_bytes = (cache_size * 1024 * 1024) as u64;

        let max_journal = max_journal_size_mb.unwrap_or(DEFAULT_MAX_JOURNAL_SIZE_MB);
        let max_journal_bytes = (max_journal as u64) * 1024 * 1024;

        let flush = flush_on_commit.unwrap_or(DEFAULT_FLUSH_ON_COMMIT);

        let db = Database::builder(path.as_ref())
            .cache_size(cache_bytes)
            .max_journaling_size(max_journal_bytes)
            .open()?;

        Self::from_database(db, flush)
    }

    /// Create an index store from an existing database
    fn from_database(db: Database, flush_on_commit: bool) -> Result<Self, Error> {
        // Helper closure to create default keyspace options
        let opts = || KeyspaceCreateOptions::default();

        // Create or open all keyspaces
        let cursor = db.keyspace(keyspace_names::CURSOR, opts)?;

        let utxo_address = db.keyspace(keyspace_names::UTXO_ADDRESS, opts)?;
        let utxo_payment = db.keyspace(keyspace_names::UTXO_PAYMENT, opts)?;
        let utxo_stake = db.keyspace(keyspace_names::UTXO_STAKE, opts)?;
        let utxo_policy = db.keyspace(keyspace_names::UTXO_POLICY, opts)?;
        let utxo_asset = db.keyspace(keyspace_names::UTXO_ASSET, opts)?;

        let history_blockhash = db.keyspace(keyspace_names::HISTORY_BLOCKHASH, opts)?;
        let history_blocknum = db.keyspace(keyspace_names::HISTORY_BLOCKNUM, opts)?;
        let history_txhash = db.keyspace(keyspace_names::HISTORY_TXHASH, opts)?;

        let history_address = db.keyspace(keyspace_names::HISTORY_ADDRESS, opts)?;
        let history_payment = db.keyspace(keyspace_names::HISTORY_PAYMENT, opts)?;
        let history_stake = db.keyspace(keyspace_names::HISTORY_STAKE, opts)?;
        let history_asset = db.keyspace(keyspace_names::HISTORY_ASSET, opts)?;
        let history_policy = db.keyspace(keyspace_names::HISTORY_POLICY, opts)?;
        let history_datum = db.keyspace(keyspace_names::HISTORY_DATUM, opts)?;
        let history_spenttxo = db.keyspace(keyspace_names::HISTORY_SPENTTXO, opts)?;
        let history_account = db.keyspace(keyspace_names::HISTORY_ACCOUNT, opts)?;
        let history_metadata = db.keyspace(keyspace_names::HISTORY_METADATA, opts)?;
        let history_script = db.keyspace(keyspace_names::HISTORY_SCRIPT, opts)?;

        Ok(Self {
            db: Arc::new(db),
            cursor,
            utxo_address,
            utxo_payment,
            utxo_stake,
            utxo_policy,
            utxo_asset,
            history_blockhash,
            history_blocknum,
            history_txhash,
            history_address,
            history_payment,
            history_stake,
            history_asset,
            history_policy,
            history_datum,
            history_spenttxo,
            history_account,
            history_metadata,
            history_script,
            flush_on_commit,
        })
    }

    /// Get UTxO keyspaces reference
    fn utxo_keyspaces(&self) -> UtxoKeyspaces<'_> {
        UtxoKeyspaces {
            address: &self.utxo_address,
            payment: &self.utxo_payment,
            stake: &self.utxo_stake,
            policy: &self.utxo_policy,
            asset: &self.utxo_asset,
        }
    }

    /// Get history keyspaces reference
    fn history_keyspaces(&self) -> HistoryKeyspaces<'_> {
        HistoryKeyspaces {
            blockhash: &self.history_blockhash,
            blocknum: &self.history_blocknum,
            txhash: &self.history_txhash,
            address: &self.history_address,
            payment: &self.history_payment,
            stake: &self.history_stake,
            asset: &self.history_asset,
            policy: &self.history_policy,
            datum: &self.history_datum,
            spenttxo: &self.history_spenttxo,
            account: &self.history_account,
            metadata: &self.history_metadata,
            script: &self.history_script,
        }
    }

    /// Get UTxO keyspace for a given dimension
    fn utxo_keyspace_for_dimension(&self, dimension: TagDimension) -> Option<&Keyspace> {
        self.utxo_keyspaces().keyspace_for_dimension(dimension)
    }

    /// Get history keyspace for a given dimension
    fn history_keyspace_for_dimension(&self, dimension: TagDimension) -> Option<&Keyspace> {
        self.history_keyspaces().keyspace_for_dimension(dimension)
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &Database {
        &self.db
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
    fn apply(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        // Apply UTxO filter changes
        utxos::apply(&mut batch, &self.store.utxo_keyspaces(), delta).map_err(IndexError::from)?;

        // Apply history index changes
        history::apply(&mut batch, &self.store.history_keyspaces(), delta)
            .map_err(IndexError::from)?;

        // Set cursor
        let cursor_bytes =
            bincode::serialize(&delta.cursor).map_err(|e| Error::Codec(e.to_string()))?;
        batch.insert(&self.store.cursor, CURSOR_KEY, cursor_bytes);

        Ok(())
    }

    fn undo(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        // Undo UTxO filter changes
        utxos::undo(&mut batch, &self.store.utxo_keyspaces(), delta).map_err(IndexError::from)?;

        // Undo history index changes
        history::undo(&mut batch, &self.store.history_keyspaces(), delta)
            .map_err(IndexError::from)?;

        Ok(())
    }

    fn commit(self) -> Result<(), IndexError> {
        let batch = self
            .batch
            .into_inner()
            .map_err(|_| Error::LockPoisoned)?
            .durability(Some(PersistMode::Buffer));

        batch.commit().map_err(Error::Fjall)?;

        // Flush journal if configured to prevent accumulation
        if self.store.flush_on_commit {
            self.store
                .db
                .persist(PersistMode::Buffer)
                .map_err(Error::Fjall)?;
        }

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

    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        match snapshot
            .get(&self.cursor, CURSOR_KEY)
            .map_err(Error::from)?
        {
            Some(value) => {
                let point: ChainPoint =
                    bincode::deserialize(&value).map_err(|e| Error::Codec(e.to_string()))?;
                Ok(Some(point))
            }
            None => Ok(None),
        }
    }

    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, IndexError> {
        let keyspace = self
            .utxo_keyspace_for_dimension(dimension)
            .ok_or_else(|| IndexError::DimensionNotFound(dimension.to_string()))?;
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        utxos::get_by_key(&snapshot, keyspace, key).map_err(IndexError::from)
    }

    fn slot_by_block_hash(&self, block_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        history::get_by_block_hash(&snapshot, &self.history_blockhash, block_hash)
            .map_err(IndexError::from)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        history::get_by_block_number(&snapshot, &self.history_blocknum, number)
            .map_err(IndexError::from)
    }

    fn slot_by_tx_hash(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        history::get_by_tx_hash(&snapshot, &self.history_txhash, tx_hash).map_err(IndexError::from)
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();

        // For metadata, key is already the u64 encoded as bytes
        if dimension == history::dimensions::METADATA {
            let metadata =
                u64::from_be_bytes(key.try_into().map_err(|_| {
                    IndexError::CodecError("metadata key must be 8 bytes".to_string())
                })?);
            return SlotIter::from_hash(&snapshot, &self.history_metadata, metadata, start, end)
                .map_err(IndexError::from);
        }

        let keyspace = self
            .history_keyspace_for_dimension(dimension)
            .ok_or_else(|| IndexError::DimensionNotFound(dimension.to_string()))?;
        SlotIter::new(&snapshot, keyspace, key, start, end).map_err(IndexError::from)
    }
}
