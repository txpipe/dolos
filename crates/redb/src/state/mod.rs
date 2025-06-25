use ::redb::{Database, MultimapTableHandle as _, TableHandle as _};
use redb::TableStats;
use std::{collections::HashMap, path::Path};
use tracing::{debug, info, warn};

use dolos_core::{
    BlockSlot, ChainPoint, EraCbor, LedgerDelta, StateError, TxoRef, UtxoMap, UtxoSet,
};

use crate::state::tables::UtxoKeyIterator;

mod tables;
pub mod v1;
pub mod v2;
pub mod v2light;

#[derive(Debug)]
pub struct RedbStateError(StateError);

impl From<StateError> for RedbStateError {
    fn from(value: StateError) -> Self {
        Self(value)
    }
}

impl From<RedbStateError> for StateError {
    fn from(value: RedbStateError) -> Self {
        value.0
    }
}

impl From<::redb::DatabaseError> for RedbStateError {
    fn from(value: ::redb::DatabaseError) -> Self {
        Self(StateError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::TableError> for RedbStateError {
    fn from(value: ::redb::TableError) -> Self {
        Self(StateError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::CommitError> for RedbStateError {
    fn from(value: ::redb::CommitError) -> Self {
        Self(StateError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::StorageError> for RedbStateError {
    fn from(value: ::redb::StorageError) -> Self {
        Self(StateError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::TransactionError> for RedbStateError {
    fn from(value: ::redb::TransactionError) -> Self {
        Self(StateError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

const DEFAULT_CACHE_SIZE_MB: usize = 500;

fn compute_schema_hash(db: &Database) -> Result<Option<String>, RedbStateError> {
    let mut hasher = pallas::crypto::hash::Hasher::<160>::new();

    let rx = db.begin_read()?;

    let names_1 = rx.list_tables()?.map(|t| t.name().to_owned());

    let names_2 = rx.list_multimap_tables()?.map(|t| t.name().to_owned());

    let mut names: Vec<_> = names_1.chain(names_2).collect();

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

fn open_db(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Database, RedbStateError> {
    let db = Database::builder()
        .set_repair_callback(|x| warn!(progress = x.progress() * 100f64, "state db is repairing"))
        .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
        .create(path)?;

    Ok(db)
}

const V1_HASH: &str = "067c3397778523b67202fa0ea720ef4d2c091e30";
const V2_HASH: &str = "eff59f15f18250d950120494c8bcb9b13575057a";
const V2_LIGHT_HASH: &str = "788921eb9af899359a257c49f4f8092c99886076";

#[derive(Clone)]
pub enum LedgerStore {
    SchemaV1(v1::LedgerStore),
    SchemaV2(v2::LedgerStore),
    SchemaV2Light(v2light::LedgerStore),
}

impl LedgerStore {
    pub fn open(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, RedbStateError> {
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
                v2::LedgerStore::new(db).into()
            }
            Some(V2_LIGHT_HASH) => {
                info!("detected state db schema v2-light");
                v2light::LedgerStore::new(db).into()
            }
            Some(x) => panic!("can't recognize db hash {}", x),
        };

        Ok(schema)
    }

    pub fn open_v2_light(
        path: impl AsRef<Path>,
        cache_size: Option<usize>,
    ) -> Result<Self, RedbStateError> {
        let db = open_db(path, cache_size)?;
        let hash = compute_schema_hash(&db)?;

        let schema = match hash.as_deref() {
            None => {
                info!("no state db schema, initializing as v2-light");
                v2light::LedgerStore::initialize(db)?.into()
            }
            Some(V2_LIGHT_HASH) => {
                info!("detected state db schema v2-light");
                v2light::LedgerStore::new(db).into()
            }
            _ => return Err(RedbStateError(StateError::InvalidStoreVersion)),
        };

        Ok(schema)
    }

    pub fn in_memory_v1() -> Result<Self, StateError> {
        let store = v1::LedgerStore::in_memory()?;

        Ok(store.into())
    }

    pub fn in_memory_v2() -> Result<Self, StateError> {
        let store = v2::LedgerStore::in_memory()?;

        Ok(store.into())
    }

    pub fn in_memory_v2_light() -> Result<Self, RedbStateError> {
        let store = v2light::LedgerStore::in_memory()?;

        Ok(store.into())
    }

    pub fn db(&self) -> &Database {
        match self {
            LedgerStore::SchemaV1(x) => x.db(),
            LedgerStore::SchemaV2(x) => x.db(),
            LedgerStore::SchemaV2Light(x) => x.db(),
        }
    }

    pub fn db_mut(&mut self) -> Option<&mut Database> {
        match self {
            LedgerStore::SchemaV1(x) => x.db_mut(),
            LedgerStore::SchemaV2(x) => x.db_mut(),
            LedgerStore::SchemaV2Light(x) => x.db_mut(),
        }
    }

    pub fn start(&self) -> Result<Option<ChainPoint>, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.start()?),
            _ => Err(RedbStateError(StateError::InvalidStoreVersion)),
        }
    }

    pub fn cursor(&self) -> Result<Option<ChainPoint>, RedbStateError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.cursor()?),
            LedgerStore::SchemaV2(x) => Ok(x.cursor()?),
            LedgerStore::SchemaV2Light(x) => Ok(x.cursor()?),
        }
    }

    pub fn is_empty(&self) -> Result<bool, RedbStateError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.is_empty()?),
            LedgerStore::SchemaV2(x) => Ok(x.is_empty()?),
            LedgerStore::SchemaV2Light(x) => Ok(x.is_empty()?),
        }
    }

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<EraCbor>, RedbStateError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.get_pparams(until)?),
            LedgerStore::SchemaV2(x) => Ok(x.get_pparams(until)?),
            LedgerStore::SchemaV2Light(x) => Ok(x.get_pparams(until)?),
        }
    }

    pub fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, RedbStateError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.get_utxos(refs)?),
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos(refs)?),
            LedgerStore::SchemaV2Light(x) => Ok(x.get_utxos(refs)?),
        }
    }

    pub fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_address(address)?),
            _ => Err(RedbStateError(StateError::QueryNotSupported)),
        }
    }

    pub fn count_utxos_by_address(&self, address: &[u8]) -> Result<u64, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.count_utxos_by_address(address)?),
            _ => Err(RedbStateError(StateError::QueryNotSupported)),
        }
    }

    pub fn iter_utxos_by_address(&self, address: &[u8]) -> Result<UtxoKeyIterator, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.iter_utxos_by_address(address)?),
            _ => Err(RedbStateError(StateError::QueryNotSupported)),
        }
    }

    pub fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_payment(payment)?),
            _ => Err(RedbStateError(StateError::QueryNotSupported)),
        }
    }

    pub fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_stake(stake)?),
            _ => Err(RedbStateError(StateError::QueryNotSupported)),
        }
    }

    pub fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_policy(policy)?),
            _ => Err(RedbStateError(StateError::QueryNotSupported)),
        }
    }

    pub fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_asset(asset)?),
            _ => Err(RedbStateError(StateError::QueryNotSupported)),
        }
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), RedbStateError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.apply(deltas)?),
            LedgerStore::SchemaV2(x) => Ok(x.apply(deltas)?),
            LedgerStore::SchemaV2Light(x) => Ok(x.apply(deltas)?),
        }
    }

    pub fn prune_history(
        &self,
        max_slots: u64,
        max_prune: Option<u64>,
    ) -> Result<bool, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.prune_history(max_slots, max_prune)?),
            LedgerStore::SchemaV2Light(x) => Ok(x.prune_history(max_slots, max_prune)?),
            _ => Err(RedbStateError(StateError::InvalidStoreVersion)),
        }
    }

    /// Upgrades a light store to a full store by indexing data
    pub fn upgrade(self) -> Result<Self, RedbStateError> {
        match self {
            LedgerStore::SchemaV2Light(x) => {
                let db = x.upgrade()?;
                Ok(LedgerStore::SchemaV2(v2::LedgerStore::new(db)))
            }
            _ => Err(RedbStateError(StateError::InvalidStoreVersion)),
        }
    }

    pub fn copy(&self, target: &Self) -> Result<(), RedbStateError> {
        match (self, target) {
            (LedgerStore::SchemaV2(x), LedgerStore::SchemaV2(target)) => Ok(x.copy(target)?),
            (LedgerStore::SchemaV2Light(x), LedgerStore::SchemaV2Light(target)) => {
                Ok(x.copy(target)?)
            }
            _ => Err(RedbStateError(StateError::InvalidStoreVersion)),
        }
    }

    pub fn stats(&self) -> Result<HashMap<&str, TableStats>, RedbStateError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.stats()?),
            _ => Err(RedbStateError(StateError::InvalidStoreVersion)),
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

impl From<v2light::LedgerStore> for LedgerStore {
    fn from(value: v2light::LedgerStore) -> Self {
        Self::SchemaV2Light(value)
    }
}

#[cfg(test)]
mod tests;
