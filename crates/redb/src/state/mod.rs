use ::redb::{Database, MultimapTableHandle as _, TableHandle as _};
use std::path::Path;
use tracing::{debug, info, warn};

use dolos_core::{
    BlockSlot, ChainPoint, EraCbor, LedgerDelta, StateError, TxoRef, UtxoMap, UtxoSet,
};

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
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .map_err(RedbStateError::from)?;

        let store = v1::LedgerStore::initialize(db)?;
        Ok(store.into())
    }

    pub fn in_memory_v2() -> Result<Self, StateError> {
        let db = ::redb::Database::builder()
            .create_with_backend(::redb::backends::InMemoryBackend::new())
            .map_err(RedbStateError::from)?;

        let store = v2::LedgerStore::initialize(db)?;
        Ok(store.into())
    }

    pub fn in_memory_v2_light() -> Result<Self, RedbStateError> {
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

    pub fn finalize(&self, until: BlockSlot) -> Result<(), RedbStateError> {
        match self {
            LedgerStore::SchemaV1(x) => Ok(x.finalize(until)?),
            LedgerStore::SchemaV2(x) => Ok(x.finalize(until)?),
            LedgerStore::SchemaV2Light(x) => Ok(x.finalize(until)?),
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
    use std::str::FromStr;

    use super::*;
    use dolos_core::testing::*;
    use pallas::ledger::addresses::Address;

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

        let delta = fake_genesis_delta(1_000_000_000);
        store.apply(&[delta]).unwrap();

        assert!(!store.is_empty().unwrap());
    }

    fn get_test_address_utxos(store: &LedgerStore, address: FakeAddress) -> UtxoMap {
        let bobs = store.get_utxo_by_address(&address.to_bytes()).unwrap();
        store.get_utxos(bobs.into_iter().collect()).unwrap()
    }

    #[test]
    fn test_apply_genesis() {
        let store = LedgerStore::in_memory_v2().unwrap();

        let genesis = fake_genesis_delta(1_000_000_000);
        store.apply(&[genesis]).unwrap();

        // TODO: the store is not persisting the cursor unless it's a specific point. We
        // need to fix this in the next breaking change version.
        //assert_eq!(store.cursor().unwrap(), Some(ChainPoint::Origin));

        let bobs = get_test_address_utxos(&store, FakeAddress::Bob);
        assert_eq!(bobs.len(), 1);
        assert_utxo_map_address_and_value(&bobs, FakeAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, FakeAddress::Carol);
        assert_eq!(carols.len(), 1);
        assert_utxo_map_address_and_value(&carols, FakeAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_apply_forward_block() {
        let store = LedgerStore::in_memory_v2().unwrap();

        let genesis = fake_genesis_delta(1_000_000_000);
        store.apply(&[genesis]).unwrap();

        let bobs = get_test_address_utxos(&store, FakeAddress::Bob);
        let delta = make_move_utxo_delta(bobs, 1, 1, FakeAddress::Carol);
        store.apply(&[delta.clone()]).unwrap();

        assert_eq!(
            store.cursor().unwrap(),
            Some(ChainPoint::Specific(1, slot_to_hash(1)))
        );

        let bobs = get_test_address_utxos(&store, FakeAddress::Bob);
        assert!(bobs.is_empty());
        assert_utxo_map_address_and_value(&bobs, FakeAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, FakeAddress::Carol);
        assert_eq!(carols.len(), 2);
        assert_utxo_map_address_and_value(&carols, FakeAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_apply_undo_block() {
        let store = LedgerStore::in_memory_v2().unwrap();

        let genesis = fake_genesis_delta(1_000_000_000);
        store.apply(&[genesis]).unwrap();

        let bobs = get_test_address_utxos(&store, FakeAddress::Bob);
        let forward = make_move_utxo_delta(bobs, 1, 1, FakeAddress::Carol);
        store.apply(&[forward.clone()]).unwrap();

        let undo = revert_delta(forward);
        store.apply(&[undo]).unwrap();

        // TODO: the store is not persisting the origin cursor, instead it's keeping it
        // empty. We should fix this in the next breaking change version.
        assert_eq!(store.cursor().unwrap(), None);

        let bobs = get_test_address_utxos(&store, FakeAddress::Bob);
        assert_eq!(bobs.len(), 1);
        assert_utxo_map_address_and_value(&bobs, FakeAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, FakeAddress::Carol);
        assert_eq!(carols.len(), 1);
        assert_utxo_map_address_and_value(&carols, FakeAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_apply_in_batch() {
        let mut batch = Vec::new();

        // first we do a step-by-step apply to use as reference. We keep the deltas in a
        // vector to apply them in batch later.
        let store = LedgerStore::in_memory_v2().unwrap();

        let genesis = fake_genesis_delta(1_000_000_000);
        store.apply(&[genesis.clone()]).unwrap();
        batch.push(genesis);

        let bobs = get_test_address_utxos(&store, FakeAddress::Bob);
        let forward = make_move_utxo_delta(bobs, 1, 1, FakeAddress::Carol);
        store.apply(&[forward.clone()]).unwrap();
        batch.push(forward.clone());

        let undo = revert_delta(forward);
        store.apply(&[undo.clone()]).unwrap();
        batch.push(undo);

        // now we apply the batch in one go.
        let store = LedgerStore::in_memory_v2().unwrap();
        store.apply(&batch).unwrap();

        let bobs = get_test_address_utxos(&store, FakeAddress::Bob);
        assert_eq!(bobs.len(), 1);
        assert_utxo_map_address_and_value(&bobs, FakeAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, FakeAddress::Carol);
        assert_eq!(carols.len(), 1);
        assert_utxo_map_address_and_value(&carols, FakeAddress::Carol, 1_000_000_000);
    }

    fn get_addresses_test_vectors() -> Vec<Address> {
        vec![
            // a Shelley address with both payment and stake parts
            Address::from_str(
                "addr1q9dhugez3ka82k2kgh7r2lg0j7aztr8uell46kydfwu3vk6n8w2cdu8mn2ha278q6q25a9rc6gmpfeekavuargcd32vsvxhl7e",
            ).unwrap(),
            // a Shelley address with only payment part
            Address::from_str(
                "addr1vpu5vlrf4xkxv2qpwngf6cjhtw542ayty80v8dyr49rf5eg0yu80w",
            )
            .unwrap(),
            // a Shelley stake address
            Address::from_str(
                "stake178phkx6acpnf78fuvxn0mkew3l0fd058hzquvz7w36x4gtcccycj5",
            )
            .unwrap(),
            // a Shelley script address
            Address::from_str(
                "addr1w9jx45flh83z6wuqypyash54mszwmdj8r64fydafxtfc6jgrw4rm3",
            )
            .unwrap(),
            // a Byron address
            Address::from_str("37btjrVyb4KDXBNC4haBVPCrro8AQPHwvCMp3RFhhSVWwfFmZ6wwzSK6JK1hY6wHNmtrpTf1kdbva8TCneM2YsiXT7mrzT21EacHnPpz5YyUdj64na").unwrap(),
        ]
    }

    #[test]
    fn test_query_by_address() {
        let store = LedgerStore::in_memory_v2().unwrap();

        let addresses: Vec<_> = get_addresses_test_vectors()
            .into_iter()
            .enumerate()
            .collect();

        let initial_utxos = addresses
            .iter()
            .map(|(ordinal, address)| {
                fake_genesis_utxo(
                    address.to_string(),
                    *ordinal,
                    1_000_000_000 * (*ordinal as u64),
                )
            })
            .collect();

        let delta = LedgerDelta {
            new_position: Some(ChainPoint::Origin),
            produced_utxo: initial_utxos,
            ..Default::default()
        };

        store.apply(&[delta]).unwrap();

        let assertion = |utxos: UtxoSet, address: &Address, ordinal: usize| {
            let utxos = store.get_utxos(utxos.into_iter().collect()).unwrap();

            assert_eq!(utxos.len(), 1);

            assert_utxo_map_address_and_value(
                &utxos,
                address.to_vec(),
                1_000_000_000 * (ordinal as u64),
            );
        };

        for (ordinal, address) in addresses {
            match address.clone() {
                Address::Byron(x) => {
                    println!("address: {:?}", address);
                    let utxos = store.get_utxo_by_address(&x.to_vec()).unwrap();
                    assertion(utxos, &address, ordinal);
                }
                Address::Shelley(x) => {
                    println!("address: {:?}", address);
                    let utxos = store.get_utxo_by_address(&x.to_vec()).unwrap();
                    assertion(utxos, &address, ordinal);

                    println!("address: {:?}", address);
                    let utxos = store.get_utxo_by_stake(&x.delegation().to_vec()).unwrap();
                    assertion(utxos, &address, ordinal);

                    println!("address: {:?}", address);
                    let utxos = store.get_utxo_by_payment(&x.payment().to_vec()).unwrap();
                    assertion(utxos, &address, ordinal);
                }
                Address::Stake(x) => {
                    println!("address: {:?}", address);
                    let utxos = store.get_utxo_by_stake(&x.to_vec()).unwrap();
                    assertion(utxos, &address, ordinal);
                }
            };
        }
    }
}
