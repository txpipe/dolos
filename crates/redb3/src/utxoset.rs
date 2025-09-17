use ::redb::Durability;
use dolos_core::{EraCbor, TxoRef, UtxoMap, UtxoSetDelta};
use pallas::ledger::{addresses::ShelleyDelegationPart, traverse::MultiEraOutput};
use redb::{
    MultimapTableDefinition, Range, ReadTransaction, ReadableTable as _,
    ReadableTableMetadata as _, TableDefinition, TableStats, WriteTransaction,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::Error;

use super::StateStore;

type UtxosKey = (&'static [u8; 32], u32);
type UtxosValue = (u16, &'static [u8]);

pub struct UtxosIterator(Range<'static, UtxosKey, UtxosValue>);

impl Iterator for UtxosIterator {
    type Item = Result<(TxoRef, EraCbor), ::redb::StorageError>;

    fn next(&mut self) -> Option<Self::Item> {
        let x = self.0.next()?;

        let x = x.map(|(k, v)| {
            let (hash, idx) = k.value();
            let k = TxoRef((*hash).into(), idx);

            let (era, cbor) = v.value();
            let cbor = cbor.to_owned();
            let v = EraCbor(era, cbor);

            (k, v)
        });

        Some(x)
    }
}

pub struct UtxosTable;

impl UtxosTable {
    pub const DEF: TableDefinition<'static, UtxosKey, UtxosValue> = TableDefinition::new("utxos");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;

        Ok(())
    }

    pub fn iter(rx: &ReadTransaction) -> Result<UtxosIterator, Error> {
        let table = rx.open_table(UtxosTable::DEF)?;
        let range = table.range::<UtxosKey>(..)?;
        Ok(UtxosIterator(range))
    }

    pub fn get_sparse(rx: &ReadTransaction, refs: Vec<TxoRef>) -> Result<UtxoMap, Error> {
        let table = rx.open_table(Self::DEF)?;
        let mut out = HashMap::new();

        for key in refs {
            if let Some(body) = table.get(&(&key.0 as &[u8; 32], key.1))? {
                let (era, cbor) = body.value();
                let cbor = cbor.to_owned();
                let value = Arc::new(EraCbor(era, cbor));

                out.insert(key, value);
            }
        }

        Ok(out)
    }

    pub fn apply(wx: &WriteTransaction, delta: &UtxoSetDelta) -> Result<(), Error> {
        let mut table = wx.open_table(Self::DEF)?;

        for (k, v) in delta.produced_utxo.iter() {
            let k: (&[u8; 32], u32) = (&k.0, k.1);
            let v: (u16, &[u8]) = (v.0, &v.1);
            table.insert(k, v)?;
        }

        for (k, _) in delta.undone_utxo.iter() {
            let k: (&[u8; 32], u32) = (&k.0, k.1);
            table.remove(k)?;
        }

        for (k, _) in delta.consumed_utxo.iter() {
            let k: (&[u8; 32], u32) = (&k.0, k.1);
            table.remove(k)?;
        }

        Ok(())
    }

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        let source = rx.open_table(Self::DEF)?;
        let mut target = wx.open_table(Self::DEF)?;

        for entry in source.iter()? {
            let (k, v) = entry?;
            target.insert(k.value(), v.value())?;
        }

        Ok(())
    }

    pub fn stats(rx: &ReadTransaction) -> Result<redb::TableStats, Error> {
        let table = rx.open_table(Self::DEF)?;
        let stats = table.stats()?;

        Ok(stats)
    }
}

pub struct UtxoKeyIterator(redb::MultimapValue<'static, UtxosKey>);

impl Iterator for UtxoKeyIterator {
    type Item = Result<TxoRef, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.0.next()?;

        let out = item
            .map(|item| {
                let (hash, idx) = item.value();
                TxoRef((*hash).into(), idx)
            })
            .map_err(Error::from);

        Some(out)
    }
}

pub struct FilterIndexes;

struct SplitAddressResult(Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>);

impl FilterIndexes {
    pub const BY_ADDRESS: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("byaddress");

    pub const BY_PAYMENT: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("bypayment");

    pub const BY_STAKE: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("bystake");

    pub const BY_POLICY: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("bypolicy");

    pub const BY_ASSET: MultimapTableDefinition<'static, &'static [u8], UtxosKey> =
        MultimapTableDefinition::new("byasset");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_multimap_table(Self::BY_ADDRESS)?;
        wx.open_multimap_table(Self::BY_PAYMENT)?;
        wx.open_multimap_table(Self::BY_STAKE)?;
        wx.open_multimap_table(Self::BY_POLICY)?;
        wx.open_multimap_table(Self::BY_ASSET)?;

        Ok(())
    }

    fn get_by_key(
        rx: &ReadTransaction,
        table_def: MultimapTableDefinition<&[u8], UtxosKey>,
        key: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        let table = rx.open_multimap_table(table_def)?;

        let mut out = HashSet::new();

        for item in table.get(key)? {
            let item = item?;
            let (hash, idx) = item.value();
            out.insert(TxoRef((*hash).into(), idx));
        }

        Ok(out)
    }

    pub fn count_within_key(
        rx: &ReadTransaction,
        table_def: MultimapTableDefinition<&[u8], UtxosKey>,
        key: &[u8],
    ) -> Result<u64, Error> {
        let table = rx.open_multimap_table(table_def)?;

        let count = table.get(key)?.len();

        Ok(count)
    }

    pub fn iter_within_key(
        rx: &ReadTransaction,
        table_def: MultimapTableDefinition<&[u8], UtxosKey>,
        key: &[u8],
    ) -> Result<UtxoKeyIterator, Error> {
        let table = rx.open_multimap_table(table_def)?;

        let inner = table.get(key)?;

        Ok(UtxoKeyIterator(inner))
    }

    pub fn get_by_address(
        rx: &ReadTransaction,
        exact_address: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_ADDRESS, exact_address)
    }

    pub fn get_by_payment(
        rx: &ReadTransaction,
        payment_part: &[u8],
    ) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_PAYMENT, payment_part)
    }

    pub fn get_by_stake(rx: &ReadTransaction, stake_part: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_STAKE, stake_part)
    }

    pub fn get_by_policy(rx: &ReadTransaction, policy: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_POLICY, policy)
    }

    pub fn get_by_asset(rx: &ReadTransaction, asset: &[u8]) -> Result<HashSet<TxoRef>, Error> {
        Self::get_by_key(rx, Self::BY_ASSET, asset)
    }

    fn split_address(utxo: &MultiEraOutput) -> Result<SplitAddressResult, Error> {
        use pallas::ledger::addresses::Address;

        match utxo.address() {
            Ok(address) => match &address {
                Address::Shelley(x) => {
                    let a = x.to_vec();
                    let b = x.payment().to_vec();

                    let c = match x.delegation() {
                        ShelleyDelegationPart::Key(..) => Some(x.delegation().to_vec()),
                        ShelleyDelegationPart::Script(..) => Some(x.delegation().to_vec()),
                        ShelleyDelegationPart::Pointer(..) => Some(x.delegation().to_vec()),
                        ShelleyDelegationPart::Null => None,
                    };

                    Ok(SplitAddressResult(Some(a), Some(b), c))
                }
                Address::Stake(x) => {
                    let a = x.to_vec();
                    let c = x.to_vec();
                    Ok(SplitAddressResult(Some(a), None, Some(c)))
                }
                Address::Byron(x) => {
                    let a = x.to_vec();
                    Ok(SplitAddressResult(Some(a), None, None))
                }
            },
            Err(err) => Err(Error::from(err)),
        }
    }

    pub fn apply(wx: &WriteTransaction, delta: &UtxoSetDelta) -> Result<(), Error> {
        let mut address_table = wx.open_multimap_table(Self::BY_ADDRESS)?;
        let mut payment_table = wx.open_multimap_table(Self::BY_PAYMENT)?;
        let mut stake_table = wx.open_multimap_table(Self::BY_STAKE)?;
        let mut policy_table = wx.open_multimap_table(Self::BY_POLICY)?;
        let mut asset_table = wx.open_multimap_table(Self::BY_ASSET)?;

        let trackable = delta
            .produced_utxo
            .iter()
            .chain(delta.recovered_stxi.iter());

        for (utxo, body) in trackable {
            let v: (&[u8; 32], u32) = (&utxo.0, utxo.1);

            // TODO: decoding here is very inefficient
            let body = MultiEraOutput::try_from(body.as_ref()).unwrap();
            let SplitAddressResult(addr, pay, stake) = Self::split_address(&body)?;

            if let Some(k) = addr {
                address_table.insert(k.as_slice(), v)?;
            }

            if let Some(k) = pay {
                payment_table.insert(k.as_slice(), v)?;
            }

            if let Some(k) = stake {
                stake_table.insert(k.as_slice(), v)?;
            }

            let value = body.value();
            let assets = value.assets();

            for batch in assets {
                policy_table.insert(batch.policy().as_slice(), v)?;

                for asset in batch.assets() {
                    let mut subject = asset.policy().to_vec();
                    subject.extend(asset.name());

                    asset_table.insert(subject.as_slice(), v)?;
                }
            }
        }

        let forgettable = delta.consumed_utxo.iter().chain(delta.undone_utxo.iter());

        for (stxi, body) in forgettable {
            let v: (&[u8; 32], u32) = (&stxi.0, stxi.1);

            // TODO: decoding here is very inefficient
            let body = MultiEraOutput::try_from(body.as_ref()).unwrap();

            let SplitAddressResult(addr, pay, stake) = Self::split_address(&body)?;

            if let Some(k) = addr {
                address_table.remove(k.as_slice(), v)?;
            }

            if let Some(k) = pay {
                payment_table.remove(k.as_slice(), v)?;
            }

            if let Some(k) = stake {
                stake_table.remove(k.as_slice(), v)?;
            }

            let value = body.value();
            let assets = value.assets();

            for batch in assets {
                policy_table.remove(batch.policy().as_slice(), v)?;

                for asset in batch.assets() {
                    let mut subject = asset.policy().to_vec();
                    subject.extend(asset.name());

                    asset_table.remove(subject.as_slice(), v)?;
                }
            }
        }

        Ok(())
    }

    fn copy_table<K: ::redb::Key, V: ::redb::Key + ::redb::Value>(
        rx: &ReadTransaction,
        wx: &WriteTransaction,
        def: MultimapTableDefinition<K, V>,
    ) -> Result<(), Error> {
        let source = rx.open_multimap_table(def)?;
        let mut target = wx.open_multimap_table(def)?;

        let all = source.range::<K::SelfType<'static>>(..)?;

        for entry in all {
            let (key, values) = entry?;
            for value in values {
                let value = value?;
                target.insert(key.value(), value.value())?;
            }
        }

        Ok(())
    }

    pub fn copy(rx: &ReadTransaction, wx: &WriteTransaction) -> Result<(), Error> {
        Self::copy_table(rx, wx, Self::BY_ADDRESS)?;
        Self::copy_table(rx, wx, Self::BY_PAYMENT)?;
        Self::copy_table(rx, wx, Self::BY_STAKE)?;
        Self::copy_table(rx, wx, Self::BY_POLICY)?;
        Self::copy_table(rx, wx, Self::BY_ASSET)?;

        Ok(())
    }

    pub fn stats(rx: &ReadTransaction) -> Result<HashMap<&'static str, redb::TableStats>, Error> {
        let address = rx.open_multimap_table(Self::BY_ADDRESS)?;
        let payment = rx.open_multimap_table(Self::BY_PAYMENT)?;
        let stake = rx.open_multimap_table(Self::BY_STAKE)?;
        let policy = rx.open_multimap_table(Self::BY_POLICY)?;
        let asset = rx.open_multimap_table(Self::BY_ASSET)?;

        Ok(HashMap::from_iter([
            ("address", address.stats()?),
            ("payment", payment.stats()?),
            ("stake", stake.stats()?),
            ("policy", policy.stats()?),
            ("asset", asset.stats()?),
        ]))
    }
}

impl StateStore {
    pub fn count_utxo_by_address(&self, address: &[u8]) -> Result<u64, Error> {
        let rx = self.db().begin_read()?;
        FilterIndexes::count_within_key(&rx, FilterIndexes::BY_ADDRESS, address)
    }

    pub fn iter_utxo_by_address(&self, address: &[u8]) -> Result<UtxoKeyIterator, Error> {
        let rx = self.db().begin_read()?;
        FilterIndexes::iter_within_key(&rx, FilterIndexes::BY_ADDRESS, address)
    }

    pub fn apply_utxoset(&self, deltas: &[UtxoSetDelta]) -> Result<(), Error> {
        let mut wx = self.db().begin_write()?;
        wx.set_durability(Durability::Eventual);
        wx.set_quick_repair(true);

        for delta in deltas {
            UtxosTable::apply(&wx, delta)?;
            FilterIndexes::apply(&wx, delta)?;
        }

        wx.commit()?;

        Ok(())
    }

    pub fn utxoset_stats(&self) -> Result<HashMap<&str, TableStats>, Error> {
        let rx = self.db().begin_read()?;

        let utxos = UtxosTable::stats(&rx)?;
        let filters = FilterIndexes::stats(&rx)?;

        let all_tables = [("utxos", utxos)].into_iter().chain(filters);

        Ok(HashMap::from_iter(all_tables))
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::FromStr as _, sync::Arc};

    use dolos_core::{
        ChainPoint, StateSchema, StateStore as _, TxoRef, UtxoMap, UtxoSet, UtxoSetDelta,
    };
    use dolos_testing::*;
    use pallas::ledger::addresses::{Address, ShelleyDelegationPart};

    use crate::StateStore;

    fn get_test_address_utxos(store: &StateStore, address: TestAddress) -> UtxoMap {
        let bobs = store.get_utxo_by_address(&address.to_bytes()).unwrap();
        store.get_utxos(bobs.into_iter().collect()).unwrap()
    }

    #[test]
    fn test_apply_genesis() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();

        let genesis = fake_genesis_delta(1_000_000_000);
        store.apply_utxoset(&[genesis]).unwrap();

        // TODO: the store is not persisting the cursor unless it's a specific point. We
        // need to fix this in the next breaking change version.
        //assert_eq!(store.cursor().unwrap(), Some(ChainPoint::Origin));

        let bobs = get_test_address_utxos(&store, TestAddress::Bob);
        assert_eq!(bobs.len(), 1);
        assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, TestAddress::Carol);
        assert_eq!(carols.len(), 1);
        assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_apply_forward_block() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();

        let genesis = fake_genesis_delta(1_000_000_000);
        store.apply_utxoset(&[genesis]).unwrap();

        let bobs = get_test_address_utxos(&store, TestAddress::Bob);
        let delta = make_move_utxo_delta(bobs, 1, 1, TestAddress::Carol);
        store.apply_utxoset(std::slice::from_ref(&delta)).unwrap();

        assert_eq!(
            store.read_cursor().unwrap(),
            Some(ChainPoint::Specific(1, slot_to_hash(1)))
        );

        let bobs = get_test_address_utxos(&store, TestAddress::Bob);
        assert!(bobs.is_empty());
        assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, TestAddress::Carol);
        assert_eq!(carols.len(), 2);
        assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_apply_undo_block() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();

        let genesis = fake_genesis_delta(1_000_000_000);
        store.apply_utxoset(&[genesis]).unwrap();

        let bobs = get_test_address_utxos(&store, TestAddress::Bob);
        let forward = make_move_utxo_delta(bobs, 1, 1, TestAddress::Carol);
        store.apply_utxoset(std::slice::from_ref(&forward)).unwrap();

        let undo = revert_delta(forward);
        store.apply_utxoset(std::slice::from_ref(&undo)).unwrap();

        // TODO: the store is not persisting the origin cursor, instead it's keeping it
        // empty. We should fix this in the next breaking change version.
        assert_eq!(store.read_cursor().unwrap(), None);

        let bobs = get_test_address_utxos(&store, TestAddress::Bob);
        assert_eq!(bobs.len(), 1);
        assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, TestAddress::Carol);
        assert_eq!(carols.len(), 1);
        assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_apply_in_batch() {
        let mut batch = Vec::new();

        // first we do a step-by-step apply to use as reference. We keep the deltas in a
        // vector to apply them in batch later.
        let store = StateStore::in_memory(StateSchema::default()).unwrap();

        let genesis = fake_genesis_delta(1_000_000_000);
        store.apply_utxoset(std::slice::from_ref(&genesis)).unwrap();
        batch.push(genesis);

        let bobs = get_test_address_utxos(&store, TestAddress::Bob);
        let forward = make_move_utxo_delta(bobs, 1, 1, TestAddress::Carol);
        store.apply_utxoset(std::slice::from_ref(&forward)).unwrap();
        batch.push(forward.clone());

        let undo = revert_delta(forward);
        store.apply_utxoset(std::slice::from_ref(&undo)).unwrap();
        batch.push(undo);

        // now we apply the batch in one go.
        let store = StateStore::in_memory(StateSchema::default()).unwrap();
        store.apply_utxoset(&batch).unwrap();

        let bobs = get_test_address_utxos(&store, TestAddress::Bob);
        assert_eq!(bobs.len(), 1);
        assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, TestAddress::Carol);
        assert_eq!(carols.len(), 1);
        assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_query_by_address() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();

        let addresses: Vec<_> = TestAddress::everyone().into_iter().enumerate().collect();

        let initial_utxos = addresses
            .iter()
            .map(|(ordinal, address)| {
                let (k, v) =
                    fake_genesis_utxo(address.clone(), *ordinal, 1_000_000_000 * (*ordinal as u64));
                (k, Arc::new(v))
            })
            .collect();

        let delta = UtxoSetDelta {
            new_position: Some(ChainPoint::Origin),
            produced_utxo: initial_utxos,
            ..Default::default()
        };

        store.apply_utxoset(&[delta]).unwrap();

        let assertion = |utxos: UtxoSet, address: &Address, ordinal: usize| {
            let utxos = store.get_utxos(utxos.into_iter().collect()).unwrap();

            assert_eq!(utxos.len(), 1);

            assert_utxo_map_address_and_value(
                &utxos,
                address.to_vec(),
                1_000_000_000 * (ordinal as u64),
            );
        };

        for (ordinal, test_address) in addresses {
            let address = Address::from_str(test_address.as_str()).unwrap();

            match address.clone() {
                Address::Byron(x) => {
                    let utxos = store.get_utxo_by_address(&x.to_vec()).unwrap();
                    assertion(utxos, &address, ordinal);
                }
                Address::Shelley(x) => {
                    let utxos = store.get_utxo_by_address(&x.to_vec()).unwrap();
                    assertion(utxos, &address, ordinal);

                    let utxos = store.get_utxo_by_payment(&x.payment().to_vec()).unwrap();
                    assertion(utxos, &address, ordinal);

                    match x.delegation() {
                        ShelleyDelegationPart::Key(..) | ShelleyDelegationPart::Script(..) => {
                            let utxos = store.get_utxo_by_stake(&x.delegation().to_vec()).unwrap();
                            assertion(utxos, &address, ordinal);
                        }
                        _ => {
                            let utxos = store.get_utxo_by_stake(&x.delegation().to_vec()).unwrap();
                            assert!(utxos.is_empty());
                        }
                    }
                }
                Address::Stake(x) => {
                    let utxos = store.get_utxo_by_stake(&x.to_vec()).unwrap();
                    assertion(utxos, &address, ordinal);
                }
            };
        }
    }

    #[test]
    fn test_count_utxos_by_address() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();

        let utxo_generator = |x: &TestAddress| utxo_with_random_amount(x, 1_000_000..1_500_000);

        let delta = make_custom_utxo_delta(0, TestAddress::everyone(), 10..11, utxo_generator);

        store.apply_utxoset(std::slice::from_ref(&delta)).unwrap();

        for address in TestAddress::everyone().iter() {
            let expected = delta
                .produced_utxo
                .values()
                .map(|x| get_utxo_address_and_value(x))
                .filter(|(addr, _)| addr == address.to_bytes().as_slice())
                .count();

            let count = store
                .count_utxo_by_address(address.to_bytes().as_slice())
                .unwrap();

            assert_eq!(expected as u64, count);
        }
    }

    #[test]
    fn test_iter_within_key() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();

        let utxo_generator = |x: &TestAddress| utxo_with_random_amount(x, 1_000_000..1_500_000);

        let delta = make_custom_utxo_delta(0, TestAddress::everyone(), 10..11, utxo_generator);

        store.apply_utxoset(std::slice::from_ref(&delta)).unwrap();

        for address in TestAddress::everyone().iter() {
            let mut expected: HashSet<TxoRef> = delta
                .produced_utxo
                .iter()
                .map(|(k, v)| (k, get_utxo_address_and_value(v)))
                .filter_map(|(k, (addr, _))| {
                    if addr == address.to_bytes().as_slice() {
                        Some(k.clone())
                    } else {
                        None
                    }
                })
                .collect();

            let iterator = store
                .iter_utxo_by_address(address.to_bytes().as_slice())
                .unwrap();

            for key in iterator {
                let key = key.unwrap();
                assert!(expected.remove(&key));
            }

            assert!(expected.is_empty());
        }
    }
}
