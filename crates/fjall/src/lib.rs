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
    BlockSlot, ChainPoint, IndexDelta, IndexError, IndexStore as CoreIndexStore,
    IndexWriter as CoreIndexWriter, TagDimension, UtxoSet,
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
    pub const ARCHIVE_POLICY: &str = "archive-policy";
    pub const ARCHIVE_DATUM: &str = "archive-datum";
    pub const ARCHIVE_SPENTTXO: &str = "archive-spenttxo";
    pub const ARCHIVE_ACCOUNT: &str = "archive-account";
    pub const ARCHIVE_METADATA: &str = "archive-metadata";
    pub const ARCHIVE_SCRIPT: &str = "archive-script";
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

/// Error type for fjall index store operations
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("fjall error: {0}")]
    Fjall(#[from] fjall::Error),

    #[error("codec error: {0}")]
    Codec(String),

    #[error("lock poisoned")]
    LockPoisoned,

    #[error("invalid dimension: {0}")]
    InvalidDimension(String),
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
    archive_policy: Keyspace,
    archive_datum: Keyspace,
    archive_spenttxo: Keyspace,
    archive_account: Keyspace,
    archive_metadata: Keyspace,
    archive_script: Keyspace,
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
        let archive_policy = db.keyspace(keyspace_names::ARCHIVE_POLICY, opts)?;
        let archive_datum = db.keyspace(keyspace_names::ARCHIVE_DATUM, opts)?;
        let archive_spenttxo = db.keyspace(keyspace_names::ARCHIVE_SPENTTXO, opts)?;
        let archive_account = db.keyspace(keyspace_names::ARCHIVE_ACCOUNT, opts)?;
        let archive_metadata = db.keyspace(keyspace_names::ARCHIVE_METADATA, opts)?;
        let archive_script = db.keyspace(keyspace_names::ARCHIVE_SCRIPT, opts)?;

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
            archive_policy,
            archive_datum,
            archive_spenttxo,
            archive_account,
            archive_metadata,
            archive_script,
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
            policy: &self.archive_policy,
            datum: &self.archive_datum,
            spenttxo: &self.archive_spenttxo,
            account: &self.archive_account,
            metadata: &self.archive_metadata,
            script: &self.archive_script,
        }
    }

    /// Get UTxO keyspace for a given dimension
    fn utxo_keyspace_for_dimension(&self, dimension: TagDimension) -> Option<&Keyspace> {
        match dimension {
            utxo_dimensions::ADDRESS => Some(&self.utxo_address),
            utxo_dimensions::PAYMENT => Some(&self.utxo_payment),
            utxo_dimensions::STAKE => Some(&self.utxo_stake),
            utxo_dimensions::POLICY => Some(&self.utxo_policy),
            utxo_dimensions::ASSET => Some(&self.utxo_asset),
            _ => None,
        }
    }

    /// Get archive keyspace for a given dimension
    fn archive_keyspace_for_dimension(&self, dimension: TagDimension) -> Option<&Keyspace> {
        match dimension {
            archive_dimensions::ADDRESS => Some(&self.archive_address),
            archive_dimensions::PAYMENT => Some(&self.archive_payment),
            archive_dimensions::STAKE => Some(&self.archive_stake),
            archive_dimensions::ASSET => Some(&self.archive_asset),
            archive_dimensions::POLICY => Some(&self.archive_policy),
            archive_dimensions::DATUM => Some(&self.archive_datum),
            archive_dimensions::SPENT_TXO => Some(&self.archive_spenttxo),
            archive_dimensions::ACCOUNT_CERTS => Some(&self.archive_account),
            archive_dimensions::METADATA => Some(&self.archive_metadata),
            archive_dimensions::SCRIPT => Some(&self.archive_script),
            _ => None,
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
    fn apply(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        // Apply UTxO filter changes
        utxo::apply(&mut batch, &self.store.utxo_keyspaces(), delta).map_err(IndexError::from)?;

        // Apply archive index changes
        archive::apply(&mut batch, &self.store.archive_keyspaces(), delta)
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
        utxo::undo(&mut batch, &self.store.utxo_keyspaces(), delta).map_err(IndexError::from)?;

        // Undo archive index changes
        archive::undo(&mut batch, &self.store.archive_keyspaces(), delta)
            .map_err(IndexError::from)?;

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

    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        match self.cursor.get(CURSOR_KEY).map_err(Error::from)? {
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
        utxo::get_by_key(keyspace, key).map_err(IndexError::from)
    }

    fn slot_by_block_hash(&self, block_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        archive::get_by_block_hash(&self.archive_blockhash, block_hash).map_err(IndexError::from)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, IndexError> {
        archive::get_by_block_number(&self.archive_blocknum, number).map_err(IndexError::from)
    }

    fn slot_by_tx_hash(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        archive::get_by_tx_hash(&self.archive_txhash, tx_hash).map_err(IndexError::from)
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        // For metadata, key is already the u64 encoded as bytes
        if dimension == archive_dimensions::METADATA {
            let metadata =
                u64::from_be_bytes(key.try_into().map_err(|_| {
                    IndexError::CodecError("metadata key must be 8 bytes".to_string())
                })?);
            return SlotIter::from_hash(&self.archive_metadata, metadata, start, end)
                .map_err(IndexError::from);
        }

        let keyspace = self
            .archive_keyspace_for_dimension(dimension)
            .ok_or_else(|| IndexError::DimensionNotFound(dimension.to_string()))?;
        SlotIter::new(keyspace, key, start, end).map_err(IndexError::from)
    }
}
