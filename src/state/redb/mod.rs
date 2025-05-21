use ::redb::{Database, MultimapTableHandle as _, TableHandle as _};
use itertools::Itertools;
use log::info;
use std::path::Path;

use tracing::{debug, warn};

use super::*;

mod tables;
pub mod v1;
pub mod v2;
pub mod v2light;

const DEFAULT_CACHE_SIZE_MB: usize = 500;

fn compute_schema_hash(db: &Database) -> Result<Option<String>, LedgerError> {
    let mut hasher = pallas::crypto::hash::Hasher::<160>::new();

    let rx = db.begin_read()?;

    let names_1 = rx.list_tables()?.map(|t| t.name().to_owned());

    let names_2 = rx.list_multimap_tables()?.map(|t| t.name().to_owned());

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
    ) -> Result<Self, LedgerError> {
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
            _ => return Err(LedgerError::InvalidStoreVersion),
        };

        Ok(schema)
    }

    pub fn in_memory_v1() -> Result<Self, LedgerError> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .unwrap();

        let store = v1::LedgerStore::initialize(db)?;
        Ok(store.into())
    }

    pub fn in_memory_v2() -> Result<Self, LedgerError> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .unwrap();

        let store = v2::LedgerStore::initialize(db)?;
        Ok(store.into())
    }

    pub fn in_memory_v2_light() -> Result<Self, LedgerError> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .unwrap();

        let store = v2light::LedgerStore::initialize(db)?;
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

    pub fn cursor(&self) -> Result<Option<ChainPoint>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.cursor()?),
            LedgerStore::SchemaV2(x) => Ok(x.cursor()?),
            LedgerStore::SchemaV2Light(x) => Ok(x.cursor()?),
        }
    }

    pub fn is_empty(&self) -> Result<bool, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.is_empty()?),
            LedgerStore::SchemaV2(x) => Ok(x.is_empty()?),
            LedgerStore::SchemaV2Light(x) => Ok(x.is_empty()?),
        }
    }

    pub fn get_pparams(&self, until: BlockSlot) -> Result<Vec<EraCbor>, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.get_pparams(until)?),
            LedgerStore::SchemaV2(x) => Ok(x.get_pparams(until)?),
            LedgerStore::SchemaV2Light(x) => Ok(x.get_pparams(until)?),
        }
    }

    pub fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.get_utxos(refs)?),
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos(refs)?),
            LedgerStore::SchemaV2Light(x) => Ok(x.get_utxos(refs)?),
        }
    }

    pub fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_address(address)?),
            _ => Err(LedgerError::QueryNotSupported),
        }
    }

    pub fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_payment(payment)?),
            _ => Err(LedgerError::QueryNotSupported),
        }
    }

    pub fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_stake(stake)?),
            _ => Err(LedgerError::QueryNotSupported),
        }
    }

    pub fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_policy(policy)?),
            _ => Err(LedgerError::QueryNotSupported),
        }
    }

    pub fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, LedgerError> {
        match self {
            LedgerStore::SchemaV2(x) => Ok(x.get_utxos_by_asset(asset)?),
            _ => Err(LedgerError::QueryNotSupported),
        }
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.apply(deltas)?),
            LedgerStore::SchemaV2(x) => Ok(x.apply(deltas)?),
            LedgerStore::SchemaV2Light(x) => Ok(x.apply(deltas)?),
        }
    }

    pub fn finalize(&self, until: BlockSlot) -> Result<(), LedgerError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.finalize(until)?),
            LedgerStore::SchemaV2(x) => Ok(x.finalize(until)?),
            LedgerStore::SchemaV2Light(x) => Ok(x.finalize(until)?),
        }
    }

    /// Upgrades a light store to a full store by indexing data
    pub fn upgrade(self) -> Result<Self, LedgerError> {
        match self {
            LedgerStore::SchemaV2Light(x) => {
                let db = x.upgrade()?;
                Ok(LedgerStore::SchemaV2(v2::LedgerStore::new(db)))
            }
            _ => Err(LedgerError::InvalidStoreVersion),
        }
    }

    pub fn copy(&self, target: &Self) -> Result<(), LedgerError> {
        match (self, target) {
            (LedgerStore::SchemaV2(x), LedgerStore::SchemaV2(target)) => Ok(x.copy(target)?),
            (LedgerStore::SchemaV2Light(x), LedgerStore::SchemaV2Light(target)) => {
                Ok(x.copy(target)?)
            }
            _ => Err(LedgerError::InvalidStoreVersion),
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
mod tests {
    use super::*;

    #[test]
    fn schema_hash_computation() {
        let store = LedgerStore::in_memory_v1().unwrap();
        let hash = compute_schema_hash(store.db()).unwrap();
        assert_eq!(hash.unwrap(), V1_HASH);

        let store = LedgerStore::in_memory_v2().unwrap();
        let hash = compute_schema_hash(store.db()).unwrap();
        assert_eq!(hash.unwrap(), V2_HASH);

        let store = LedgerStore::in_memory_v2_light().unwrap();
        let hash = compute_schema_hash(store.db()).unwrap();
        assert_eq!(hash.unwrap(), V2_LIGHT_HASH);
    }

    #[test]
    fn empty_until_cursor() {
        let store = LedgerStore::in_memory_v2().unwrap();

        assert!(store.is_empty().unwrap());

        let delta = LedgerDelta {
            new_position: Some(ChainPoint(
                1,
                pallas::crypto::hash::Hash::new(b"01010101010101010101010101010101".to_owned()),
            )),
            ..Default::default()
        };

        store.apply(&[delta]).unwrap();
        assert!(!store.is_empty().unwrap());
    }
}
