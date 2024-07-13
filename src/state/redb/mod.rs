use ::redb::Database;
use itertools::Itertools as _;
use redb::TableHandle as _;
use std::{
    collections::HashSet,
    hash::{Hash as _, Hasher as _},
    path::Path,
};

use tracing::warn;

use crate::ledger::*;

mod tables;
mod v1;
mod v2;

const DEFAULT_CACHE_SIZE_MB: usize = 500;

fn compute_schema_hash(db: &Database) -> Result<Option<u64>, LedgerError> {
    let mut hasher = std::hash::DefaultHasher::new();

    let mut names = db
        .begin_read()
        .map_err(|e| LedgerError::StorageError(e.into()))?
        .list_tables()
        .map_err(|e| LedgerError::StorageError(e.into()))?
        .map(|t| t.name().to_owned())
        .collect_vec();

    if names.is_empty() {
        // this db hasn't been initialized, we can't compute hash
        return Ok(None);
    }

    // sort to make sure we don't depend on some redb implementation regarding order
    // of the tables.
    names.sort();

    names.into_iter().for_each(|n| n.hash(&mut hasher));

    let hash = hasher.finish();

    Ok(Some(hash))
}

fn open_db(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Database, LedgerError> {
    let db = Database::builder()
        .set_repair_callback(|x| warn!(progress = x.progress() * 100f64, "ledger db is repairing"))
        .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
        //.create_with_backend(redb::backends::InMemoryBackend::new())?;
        .create(path)
        .map_err(|x| LedgerError::StorageError(x.into()))?;

    Ok(db)
}

#[derive(Clone)]
pub enum LedgerStore {
    SchemaV1(v1::LedgerStore),
    SchemaV2(v2::LedgerStore),
}

impl LedgerStore {
    pub fn open(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, LedgerError> {
        let db = open_db(path, cache_size)?;
        let hash = compute_schema_hash(&db)?;

        let schema = match hash {
            // use stable schema if no hash
            None => v1::LedgerStore::from(db).into(),
            // v1 hash
            Some(13844724490616556453) => v1::LedgerStore::from(db).into(),
            Some(x) => panic!("can't recognize db hash {}", x),
        };

        Ok(schema)
    }

    pub fn cursor(&self) -> Result<Option<ChainPoint>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.cursor().map_err(LedgerError::StorageError),
            LedgerStore::SchemaV2(x) => x.cursor().map_err(LedgerError::StorageError),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            LedgerStore::SchemaV1(x) => x.is_empty(),
            LedgerStore::SchemaV2(x) => x.is_empty(),
        }
    }

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<PParamsBody>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.get_pparams(until).map_err(LedgerError::StorageError),
            LedgerStore::SchemaV2(x) => x.get_pparams(until).map_err(LedgerError::StorageError),
        }
    }

    pub fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.get_utxos(refs).map_err(LedgerError::StorageError),
            LedgerStore::SchemaV2(x) => x.get_utxos(refs).map_err(LedgerError::StorageError),
        }
    }

    pub fn get_utxo_by_address_set(&self, _address: &[u8]) -> Result<HashSet<TxoRef>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(_) => Err(LedgerError::QueryNotSupported),
            LedgerStore::SchemaV2(x) => x.get_utxos_by_address(),
        }
    }

    pub fn apply(&mut self, deltas: &[LedgerDelta]) -> Result<(), LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.apply(deltas).map_err(LedgerError::StorageError),
            LedgerStore::SchemaV2(x) => x.apply(deltas).map_err(LedgerError::StorageError),
        }
    }

    pub fn finalize(&mut self, until: BlockSlot) -> Result<(), LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.finalize(until).map_err(LedgerError::StorageError),
            LedgerStore::SchemaV2(x) => x.finalize(until).map_err(LedgerError::StorageError),
        }
    }
}

impl From<v1::LedgerStore> for LedgerStore {
    fn from(value: v1::LedgerStore) -> Self {
        Self::SchemaV1(value)
    }
}

impl From<v2::LedgerStore> for LedgerStore {
    fn from(value: v2::LedgerStore) -> Self {
        Self::SchemaV2(value)
    }
}
