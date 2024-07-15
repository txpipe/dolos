use ::redb::Database;
use itertools::Itertools;
use log::info;
use redb::{MultimapTableHandle as _, TableHandle as _};
use std::path::Path;

use tracing::{debug, warn};

use crate::ledger::*;

mod tables;
mod v1;
mod v2;

const DEFAULT_CACHE_SIZE_MB: usize = 500;

fn compute_schema_hash(db: &Database) -> Result<Option<String>, LedgerError> {
    let mut hasher = pallas::crypto::hash::Hasher::<160>::new();

    let rx = db
        .begin_read()
        .map_err(|e| LedgerError::StorageError(e.into()))?;

    let names_1 = rx
        .list_tables()
        .map_err(|e| LedgerError::StorageError(e.into()))?
        .map(|t| t.name().to_owned());

    let names_2 = rx
        .list_multimap_tables()
        .map_err(|e| LedgerError::StorageError(e.into()))?
        .map(|t| t.name().to_owned());

    let mut names = names_1.chain(names_2).collect_vec();

    debug!(tables = ?names, "tables names used to compute hash");

    if names.is_empty() {
        // this db hasn't been initialized, we can't compute hash
        return Ok(None);
    }

    // sort to make sure we don't depend on some redb implementation regarding order
    // of the tables.
    names.sort();

    names.into_iter().for_each(|n| hasher.input(n.as_bytes()));

    let hash = hasher.finalize();

    Ok(Some(hash.to_string()))
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

impl From<::redb::Error> for LedgerError {
    fn from(value: ::redb::Error) -> Self {
        LedgerError::StorageError(value)
    }
}

const V1_HASH: &str = "067c3397778523b67202fa0ea720ef4d2c091e30";
const V2_HASH: &str = "eff59f15f18250d950120494c8bcb9b13575057a";

#[derive(Clone)]
pub enum LedgerStore {
    SchemaV1(v1::LedgerStore),
    SchemaV2(v2::LedgerStore),
}

impl LedgerStore {
    pub fn open(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, LedgerError> {
        let db = open_db(path, cache_size)?;
        let hash = compute_schema_hash(&db)?;

        let schema = match hash.as_deref() {
            // use stable schema if no hash
            None => {
                info!("no state db schema, initializing as v2");
                v2::LedgerStore::initialize(db)?.into()
            }
            Some(V1_HASH) => {
                info!("detected state db schema v1");
                v1::LedgerStore::from(db).into()
            }
            Some(V2_HASH) => {
                info!("detected state db schema v2");
                v2::LedgerStore::from(db).into()
            }
            Some(x) => panic!("can't recognize db hash {}", x),
        };

        Ok(schema)
    }

    pub fn cursor(&self) -> Result<Option<ChainPoint>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.cursor()?),
            LedgerStore::SchemaV2(x) => Ok(x.cursor()?),
        }
    }

    pub fn is_empty(&self) -> Result<bool, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.is_empty()?),
            LedgerStore::SchemaV2(x) => Ok(x.is_empty()?),
        }
    }

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<PParamsBody>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.get_pparams(until)?),
            LedgerStore::SchemaV2(x) => Ok(x.get_pparams(until)?),
        }
    }

    pub fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.get_utxos(refs)?),
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos(refs)?),
        }
    }

    pub fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV1(_) => Err(LedgerError::QueryNotSupported),
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_address(address)?),
        }
    }

    pub fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV1(_) => Err(LedgerError::QueryNotSupported),
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_payment(payment)?),
        }
    }

    pub fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV1(_) => Err(LedgerError::QueryNotSupported),
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_stake(stake)?),
        }
    }

    pub fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV1(_) => Err(LedgerError::QueryNotSupported),
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_policy(policy)?),
        }
    }

    pub fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV1(_) => Err(LedgerError::QueryNotSupported),
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_policy(asset)?),
        }
    }

    pub fn apply(&mut self, deltas: &[LedgerDelta]) -> Result<(), LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.apply(deltas)?),
            LedgerStore::SchemaV2(x) => Ok(x.apply(deltas)?),
        }
    }

    pub fn finalize(&mut self, until: BlockSlot) -> Result<(), LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.finalize(until)?),
            LedgerStore::SchemaV2(x) => Ok(x.finalize(until)?),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_hash_computation() {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .unwrap();

        let hash = compute_schema_hash(&db).unwrap();
        assert!(hash.is_none());

        let store = v1::LedgerStore::initialize(db).unwrap();

        let hash = compute_schema_hash(&store.0).unwrap();
        assert_eq!(hash.unwrap(), V1_HASH);

        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .unwrap();

        let store = v2::LedgerStore::initialize(db).unwrap();

        let hash = compute_schema_hash(&store.0).unwrap();
        assert_eq!(hash.unwrap(), V2_HASH);
    }
}
