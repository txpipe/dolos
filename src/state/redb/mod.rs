mod v1;

use ::redb::Database;
use std::{collections::HashSet, path::Path, sync::Arc};
use tracing::warn;

use crate::ledger::*;

const DEFAULT_CACHE_SIZE_MB: usize = 500;

#[derive(Clone)]
pub enum LedgerStore {
    SchemaV1(v1::LedgerStore),
}

impl LedgerStore {
    pub fn open(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, LedgerError> {
        let inner = Database::builder()
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "ledger db is repairing")
            })
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            //.create_with_backend(redb::backends::InMemoryBackend::new())?;
            .create(path)
            .map_err(|x| LedgerError::StorageError(x.into()))?;

        Ok(Self::SchemaV1(v1::LedgerStore(Arc::new(inner))))
    }

    pub fn cursor(&self) -> Result<Option<ChainPoint>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.cursor().map_err(LedgerError::StorageError),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            LedgerStore::SchemaV1(x) => x.is_empty(),
        }
    }

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<PParamsBody>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.get_pparams(until).map_err(LedgerError::StorageError),
        }
    }

    pub fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.get_utxos(refs).map_err(LedgerError::StorageError),
        }
    }

    pub fn get_utxo_by_address_set(&self, _address: &[u8]) -> Result<HashSet<TxoRef>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(_) => Err(LedgerError::QueryNotSupported),
        }
    }

    pub fn apply(&mut self, deltas: &[LedgerDelta]) -> Result<(), LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.apply(deltas).map_err(LedgerError::StorageError),
        }
    }

    pub fn finalize(&mut self, until: BlockSlot) -> Result<(), LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => x.finalize(until).map_err(LedgerError::StorageError),
        }
    }
}

impl From<LedgerStore> for super::LedgerStore {
    fn from(value: LedgerStore) -> Self {
        super::LedgerStore::Redb(value)
    }
}
