use ::redb::{Database, MultimapTableHandle as _, Range, TableHandle as _};
use std::path::Path;
use tracing::{debug, info, warn};

use dolos_core::{ArchiveError, BlockBody, BlockSlot, EraCbor, LedgerDelta, TxOrder, TxoRef};

mod indexes;
mod tables;
mod v1;

#[derive(Debug)]
pub struct RedbArchiveError(ArchiveError);

impl From<ArchiveError> for RedbArchiveError {
    fn from(value: ArchiveError) -> Self {
        Self(value)
    }
}

impl From<RedbArchiveError> for ArchiveError {
    fn from(value: RedbArchiveError) -> Self {
        value.0
    }
}

impl From<::redb::DatabaseError> for RedbArchiveError {
    fn from(value: ::redb::DatabaseError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::TableError> for RedbArchiveError {
    fn from(value: ::redb::TableError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::CommitError> for RedbArchiveError {
    fn from(value: ::redb::CommitError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::StorageError> for RedbArchiveError {
    fn from(value: ::redb::StorageError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

impl From<::redb::TransactionError> for RedbArchiveError {
    fn from(value: ::redb::TransactionError) -> Self {
        Self(ArchiveError::InternalError(Box::new(::redb::Error::from(
            value,
        ))))
    }
}

const DEFAULT_CACHE_SIZE_MB: usize = 500;

fn compute_schema_hash(db: &Database) -> Result<Option<String>, RedbArchiveError> {
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

fn open_db(
    path: impl AsRef<Path>,
    cache_size: Option<usize>,
) -> Result<Database, RedbArchiveError> {
    let db = Database::builder()
        .set_repair_callback(|x| warn!(progress = x.progress() * 100f64, "ledger db is repairing"))
        .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
        .create(path)?;

    Ok(db)
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
    ) -> Result<Self, RedbArchiveError> {
        let db = open_db(path, cache_size)?;
        let hash = compute_schema_hash(&db)?;

        let schema = match hash.as_deref() {
            // use stable schema if no hash
            None => {
                info!("no state db schema, initializing as v1");
                v1::ChainStore::initialize(db)?.into()
            }
            Some(V1_HASH) => {
                info!("detected state db schema v1");
                v1::ChainStore::from(db).into()
            }
            Some(x) => panic!("can't recognize db hash {}", x),
        };

        Ok(schema)
    }

    pub fn in_memory_v1() -> Result<Self, ArchiveError> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .map_err(RedbArchiveError::from)?;

        let store = v1::ChainStore::initialize(db)?;
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

    pub fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, RedbArchiveError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_tip(),
        }
    }

    pub fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<ChainIter<'a>, RedbArchiveError> {
        let out = match self {
            ChainStore::SchemaV1(x) => x.get_range(from, to)?,
        };

        Ok(out)
    }

    pub fn get_block_by_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Option<BlockBody>, RedbArchiveError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_block_by_hash(block_hash),
        }
    }

    pub fn get_block_by_slot(
        &self,
        slot: &BlockSlot,
    ) -> Result<Option<BlockBody>, RedbArchiveError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_block_by_slot(slot),
        }
    }

    pub fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, RedbArchiveError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_block_by_number(number),
        }
    }

    pub fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, RedbArchiveError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_block_with_tx(tx_hash),
        }
    }

    pub fn get_utxo_by_address(
        &self,
        address: &[u8],
    ) -> Result<Vec<(TxoRef, EraCbor)>, RedbArchiveError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_utxo_by_address(address),
        }
    }

    pub fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, RedbArchiveError> {
        match self {
            ChainStore::SchemaV1(x) => x.get_tx(tx_hash),
        }
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), RedbArchiveError> {
        match self {
            ChainStore::SchemaV1(x) => Ok(x.apply(deltas)?),
        }
    }

    pub fn prune_history(
        &self,
        max_slots: u64,
        max_prune: Option<u64>,
    ) -> Result<(), RedbArchiveError> {
        match self {
            ChainStore::SchemaV1(x) => x.prune_history(max_slots, max_prune),
        }
    }
}

impl From<v1::ChainStore> for ChainStore {
    fn from(value: v1::ChainStore) -> Self {
        Self::SchemaV1(value)
    }
}

pub struct ChainIter<'a>(Range<'a, BlockSlot, BlockBody>);

impl<'a> Iterator for ChainIter<'a> {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

impl<'a> DoubleEndedIterator for ChainIter<'a> {
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
