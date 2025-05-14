use ::redb::{Database, MultimapTableHandle as _, Range, TableHandle as _};
use itertools::Itertools;
use log::info;
use std::path::Path;

use tracing::{debug, warn};

mod indexes;
mod tables;
mod v1;

use super::*;

const DEFAULT_CACHE_SIZE_MB: usize = 500;

fn compute_schema_hash(db: &Database) -> Result<Option<String>, ChainError> {
    let mut hasher = pallas::crypto::hash::Hasher::<160>::new();

    let rx = db
        .begin_read()
        .map_err(|e| ChainError::StorageError(e.into()))?;

    let names_1 = rx
        .list_tables()
        .map_err(|e| ChainError::StorageError(e.into()))?
        .map(|t| t.name().to_owned());

    let names_2 = rx
        .list_multimap_tables()
        .map_err(|e| ChainError::StorageError(e.into()))?
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

fn open_db(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Database, ChainError> {
    let db = Database::builder()
        .set_repair_callback(|x| warn!(progress = x.progress() * 100f64, "ledger db is repairing"))
        .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
        .create(path)
        .map_err(|x| ChainError::StorageError(x.into()))?;

    Ok(db)
}

impl From<::redb::Error> for ChainError {
    fn from(value: ::redb::Error) -> Self {
        ChainError::StorageError(value)
    }
}

const V1_HASH: &str = "b9e1e00f2427e24139929a60fbaf925948c11069";

#[derive(Clone)]
pub enum ChainStore {
    SchemaV1(v1::ChainStore),
}

impl ChainStore {
    pub fn open(
        path: impl AsRef<Path>,
        cache_size: Option<usize>,
        max_slots: Option<u64>,
    ) -> Result<Self, ChainError> {
        let db = open_db(path, cache_size)?;
        let hash = compute_schema_hash(&db)?;

        let schema = match hash.as_deref() {
            // use stable schema if no hash
            None => {
                info!("no state db schema, initializing as v1");
                v1::ChainStore::initialize(db, max_slots)?.into()
            }
            Some(V1_HASH) => {
                info!("detected state db schema v1");
                v1::ChainStore::from((db, max_slots)).into()
            }
            Some(x) => panic!("can't recognize db hash {}", x),
        };

        Ok(schema)
    }

    pub fn in_memory_v1() -> Result<Self, ChainError> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .unwrap();

        let store = v1::ChainStore::initialize(db, None)?;
        Ok(store.into())
    }

    pub fn db(&self) -> &Database {
        match self {
            ChainStore::SchemaV1(x) => x.db(),
        }
    }

    pub fn db_mut(&mut self) -> Option<&mut Database> {
        match self {
            ChainStore::SchemaV1(x) => x.db_mut(),
        }
    }

    pub fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ChainError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_tip(),
        }
    }

    pub fn get_range(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<ChainIter, ChainError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_range(from, to),
        }
    }

    pub fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ChainError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_block_by_hash(block_hash),
        }
    }

    pub fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ChainError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_block_by_slot(slot),
        }
    }

    pub fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ChainError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_block_by_number(number),
        }
    }

    pub fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<Vec<u8>>, ChainError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_tx(tx_hash),
        }
    }

    pub fn get_tx_with_block_data(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, Vec<u8>)>, ChainError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_tx_with_block_data(tx_hash),
        }
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), ChainError> {
        match self {
            ChainStore::SchemaV1(x) => Ok(x.apply(deltas)?),
        }
    }

    pub fn prune_history(
        &mut self,
        max_slots: u64,
        max_prune: Option<u64>,
    ) -> Result<(), ChainError> {
        match self {
            ChainStore::SchemaV1(x) => x.prune_history(max_slots, max_prune),
        }
    }

    pub fn housekeeping(&mut self) -> Result<(), ChainError> {
        match self {
            ChainStore::SchemaV1(x) => x.housekeeping(),
        }
    }

    pub fn finalize(&self, until: BlockSlot) -> Result<(), ChainError> {
        match self {
            ChainStore::SchemaV1(x) => Ok(x.finalize(until)?),
        }
    }
}

impl From<v1::ChainStore> for ChainStore {
    fn from(value: v1::ChainStore) -> Self {
        Self::SchemaV1(value)
    }
}

pub struct ChainIter<'a>(Range<'a, BlockSlot, BlockBody>);
impl Iterator for ChainIter<'_> {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

impl DoubleEndedIterator for ChainIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_hash_computation() {
        let store = ChainStore::in_memory_v1().unwrap();
        let hash = compute_schema_hash(store.db()).unwrap();
        assert_eq!(hash.unwrap(), V1_HASH);
    }
}
