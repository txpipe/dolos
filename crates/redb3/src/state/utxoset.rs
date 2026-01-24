use dolos_core::{EraCbor, TxoRef, UtxoMap, UtxoSetDelta};
use pallas::crypto::hash::Hash;
use redb::{
    Range, ReadTransaction, ReadableDatabase, ReadableTable as _, ReadableTableMetadata as _,
    TableDefinition, TableStats, WriteTransaction,
};
use std::{collections::HashMap, sync::Arc};

use crate::Error;

use super::StateStore;

type UtxosKey = (&'static [u8; 32], u32);
type UtxosValue = (u16, &'static [u8]);
type DatumKey = &'static [u8; 32];
type DatumValue = (u64, &'static [u8]);

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

    #[allow(unused)]
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

        for (k, v) in delta.recovered_stxi.iter() {
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

        for (datum_hash, datum_value) in delta.witness_datums_add.iter() {
            DatumsTable::increment(wx, datum_hash, datum_value)?;
        }

        for datum_hash in delta.witness_datums_remove.iter() {
            DatumsTable::decrement(wx, datum_hash)?;
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

pub struct DatumsTable;

impl DatumsTable {
    pub const DEF: TableDefinition<'static, DatumKey, DatumValue> = TableDefinition::new("datums");

    pub fn initialize(wx: &WriteTransaction) -> Result<(), Error> {
        wx.open_table(Self::DEF)?;
        Ok(())
    }

    pub fn get(rx: &ReadTransaction, datum_hash: &Hash<32>) -> Result<Option<Vec<u8>>, Error> {
        let table = rx.open_table(Self::DEF)?;
        Ok(table.get(&**datum_hash)?.map(|v| v.value().1.to_vec()))
    }

    pub fn increment(
        wx: &WriteTransaction,
        datum_hash: &Hash<32>,
        datum_value: &[u8],
    ) -> Result<u64, Error> {
        let mut table = wx.open_table(Self::DEF)?;

        let current_count = table
            .get(&**datum_hash)?
            .map(|entry| entry.value().0)
            .unwrap_or(0);

        let new_count = current_count + 1;
        table.insert(&**datum_hash, (new_count, datum_value))?;
        Ok(new_count)
    }

    pub fn decrement(wx: &WriteTransaction, datum_hash: &Hash<32>) -> Result<u64, Error> {
        let mut table = wx.open_table(Self::DEF)?;

        let entry_data: Option<(u64, Vec<u8>)> = table.get(&**datum_hash)?.map(|entry| {
            let (count, bytes) = entry.value();
            (count, bytes.to_vec())
        });

        let Some((count, bytes)) = entry_data else {
            return Ok(0);
        };

        if count <= 1 {
            table.remove(&**datum_hash)?;
            return Ok(0);
        }

        let new_count = count - 1;
        table.insert(&**datum_hash, (new_count, bytes.as_slice()))?;
        Ok(new_count)
    }

    pub fn stats(rx: &ReadTransaction) -> Result<TableStats, Error> {
        let table = rx.open_table(Self::DEF)?;
        Ok(table.stats()?)
    }
}

impl StateStore {
    pub fn utxoset_stats(&self) -> Result<HashMap<&str, TableStats>, Error> {
        let rx = self.db().begin_read()?;

        let utxos = UtxosTable::stats(&rx)?;
        let datums = DatumsTable::stats(&rx)?;
        Ok(HashMap::from_iter([("utxos", utxos), ("datums", datums)]))
    }

    pub fn get_datum(&self, datum_hash: &Hash<32>) -> Result<Option<Vec<u8>>, Error> {
        let rx = self.db().begin_read()?;
        DatumsTable::get(&rx, datum_hash)
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, str::FromStr as _, sync::Arc};

    use dolos_core::{
        ChainPoint, IndexDelta, IndexStore as _, IndexWriter as _, StateSchema, StateStore as _,
        StateWriter as _, Tag, TxoRef, UtxoIndexDelta, UtxoMap, UtxoSet, UtxoSetDelta,
    };
    use dolos_testing::*;
    use pallas::ledger::{
        addresses::{Address, ShelleyDelegationPart},
        traverse::MultiEraOutput,
    };

    use crate::state::StateStore;

    // Define dimension constants locally for tests (matching dolos_cardano::indexes::dimensions)
    mod dimensions {
        pub const ADDRESS: &str = "address";
        pub const PAYMENT: &str = "payment";
        pub const STAKE: &str = "stake";
        pub const POLICY: &str = "policy";
        pub const ASSET: &str = "asset";
    }

    fn build_indexes(_store: &StateStore) -> crate::indexes::IndexStore {
        crate::indexes::IndexStore::in_memory().unwrap()
    }

    fn get_test_address_utxos(
        store: &StateStore,
        indexes: &crate::indexes::IndexStore,
        address: TestAddress,
    ) -> UtxoMap {
        let bobs = indexes
            .utxos_by_tag(dimensions::ADDRESS, &address.to_bytes())
            .unwrap();
        store.get_utxos(bobs.into_iter().collect()).unwrap()
    }

    /// Build an IndexDelta from a UtxoSetDelta for testing.
    /// This is a simplified version that extracts address tags from UTxO outputs.
    /// Handles both forward (produced/consumed) and rollback (recovered/undone) cases.
    fn build_index_delta_from_utxo_delta(
        cursor: ChainPoint,
        utxo_delta: &UtxoSetDelta,
    ) -> IndexDelta {
        let mut produced = Vec::new();
        let mut consumed = Vec::new();

        // Handle forward operations: produced_utxo -> add to index, consumed_utxo -> remove from index
        for (txo_ref, era_cbor) in utxo_delta.produced_utxo.iter() {
            if let Ok(output) = MultiEraOutput::try_from(era_cbor.as_ref()) {
                let tags = extract_utxo_tags(&output);
                produced.push((txo_ref.clone(), tags));
            }
        }

        for (txo_ref, era_cbor) in utxo_delta.consumed_utxo.iter() {
            if let Ok(output) = MultiEraOutput::try_from(era_cbor.as_ref()) {
                let tags = extract_utxo_tags(&output);
                consumed.push((txo_ref.clone(), tags));
            }
        }

        // Handle rollback operations: recovered_stxi -> restore to index (add), undone_utxo -> remove from index
        // recovered_stxi: UTxOs that were previously consumed, now being restored
        for (txo_ref, era_cbor) in utxo_delta.recovered_stxi.iter() {
            if let Ok(output) = MultiEraOutput::try_from(era_cbor.as_ref()) {
                let tags = extract_utxo_tags(&output);
                produced.push((txo_ref.clone(), tags));
            }
        }

        // undone_utxo: UTxOs that were previously produced, now being removed
        for (txo_ref, era_cbor) in utxo_delta.undone_utxo.iter() {
            if let Ok(output) = MultiEraOutput::try_from(era_cbor.as_ref()) {
                let tags = extract_utxo_tags(&output);
                consumed.push((txo_ref.clone(), tags));
            }
        }

        IndexDelta {
            cursor,
            utxo: UtxoIndexDelta { produced, consumed },
            archive: Vec::new(),
        }
    }

    fn extract_utxo_tags(output: &MultiEraOutput) -> Vec<Tag> {
        let mut tags = Vec::new();

        if let Ok(addr) = output.address() {
            match addr {
                Address::Shelley(x) => {
                    tags.push(Tag::new(dimensions::ADDRESS, x.to_vec()));
                    tags.push(Tag::new(dimensions::PAYMENT, x.payment().to_vec()));
                    // Extract stake address if present
                    match x.delegation() {
                        ShelleyDelegationPart::Key(..) | ShelleyDelegationPart::Script(..) => {
                            tags.push(Tag::new(dimensions::STAKE, x.delegation().to_vec()));
                        }
                        _ => {}
                    }
                }
                Address::Stake(x) => {
                    tags.push(Tag::new(dimensions::ADDRESS, x.to_vec()));
                    tags.push(Tag::new(dimensions::STAKE, x.to_vec()));
                }
                Address::Byron(x) => {
                    tags.push(Tag::new(dimensions::ADDRESS, x.to_vec()));
                }
            }
        }

        // Asset tags
        for ma in output.value().assets() {
            tags.push(Tag::new(dimensions::POLICY, ma.policy().to_vec()));
            for asset in ma.assets() {
                let mut subject = asset.policy().to_vec();
                subject.extend(asset.name());
                tags.push(Tag::new(dimensions::ASSET, subject));
            }
        }

        tags
    }

    macro_rules! apply_utxoset {
        ($store:expr, $indexes:expr, $deltas:expr) => {
            let writer = $store.start_writer().unwrap();
            let index_writer = $indexes.start_writer().unwrap();
            for delta in $deltas.iter() {
                writer.apply_utxoset(&delta).unwrap();
                // Build index delta from UTxO delta
                let cursor = $store.read_cursor().unwrap().unwrap_or(ChainPoint::Origin);
                let index_delta = build_index_delta_from_utxo_delta(cursor, &delta);
                index_writer.apply(&index_delta).unwrap();
            }
            writer.commit().unwrap();
            index_writer.commit().unwrap();
        };
    }

    #[test]
    fn test_apply_genesis() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();
        let indexes = build_indexes(&store);

        let genesis = fake_genesis_delta(1_000_000_000);
        apply_utxoset!(store, &indexes, [&genesis]);

        // TODO: the store is not persisting the cursor unless it's a specific point. We
        // need to fix this in the next breaking change version.
        //assert_eq!(store.cursor().unwrap(), Some(ChainPoint::Origin));

        let bobs = get_test_address_utxos(&store, &indexes, TestAddress::Bob);
        assert_eq!(bobs.len(), 1);
        assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, &indexes, TestAddress::Carol);
        assert_eq!(carols.len(), 1);
        assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_apply_forward_block() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();
        let indexes = build_indexes(&store);

        let genesis = fake_genesis_delta(1_000_000_000);
        apply_utxoset!(store, &indexes, [genesis]);

        let bobs = get_test_address_utxos(&store, &indexes, TestAddress::Bob);
        let delta = make_move_utxo_delta(bobs, 1, TestAddress::Carol);
        apply_utxoset!(store, &indexes, [&delta]);

        let bobs = get_test_address_utxos(&store, &indexes, TestAddress::Bob);
        assert!(bobs.is_empty());
        assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, &indexes, TestAddress::Carol);
        assert_eq!(carols.len(), 2);
        assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_apply_undo_block() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();
        let indexes = build_indexes(&store);

        let genesis = fake_genesis_delta(1_000_000_000);
        apply_utxoset!(store, &indexes, [&genesis]);

        let bobs = get_test_address_utxos(&store, &indexes, TestAddress::Bob);
        let forward = make_move_utxo_delta(bobs, 1, TestAddress::Carol);
        apply_utxoset!(store, &indexes, [&forward]);

        let undo = revert_delta(forward);
        apply_utxoset!(store, &indexes, [&undo]);

        // TODO: the store is not persisting the origin cursor, instead it's keeping it
        // empty. We should fix this in the next breaking change version.
        assert_eq!(store.read_cursor().unwrap(), None);

        let bobs = get_test_address_utxos(&store, &indexes, TestAddress::Bob);
        assert_eq!(bobs.len(), 1);
        assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, &indexes, TestAddress::Carol);
        assert_eq!(carols.len(), 1);
        assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_apply_in_batch() {
        let mut batch = Vec::new();

        // first we do a step-by-step apply to use as reference. We keep the deltas in a
        // vector to apply them in batch later.
        let store = StateStore::in_memory(StateSchema::default()).unwrap();
        let indexes = build_indexes(&store);

        let genesis = fake_genesis_delta(1_000_000_000);
        apply_utxoset!(store, &indexes, [&genesis]);
        batch.push(genesis);

        let bobs = get_test_address_utxos(&store, &indexes, TestAddress::Bob);
        let forward = make_move_utxo_delta(bobs, 1, TestAddress::Carol);
        apply_utxoset!(store, &indexes, [&forward]);
        batch.push(forward.clone());

        let undo = revert_delta(forward);
        apply_utxoset!(store, &indexes, [&undo]);
        batch.push(undo);

        // now we apply the batch in one go.
        let store = StateStore::in_memory(StateSchema::default()).unwrap();
        let indexes = build_indexes(&store);
        apply_utxoset!(store, &indexes, batch);

        let bobs = get_test_address_utxos(&store, &indexes, TestAddress::Bob);
        assert_eq!(bobs.len(), 1);
        assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

        let carols = get_test_address_utxos(&store, &indexes, TestAddress::Carol);
        assert_eq!(carols.len(), 1);
        assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
    }

    #[test]
    fn test_query_by_address() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();
        let indexes = build_indexes(&store);

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
            produced_utxo: initial_utxos,
            ..Default::default()
        };

        apply_utxoset!(store, &indexes, [&delta]);

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
                    let utxos = indexes
                        .utxos_by_tag(dimensions::ADDRESS, &x.to_vec())
                        .unwrap();
                    assertion(utxos, &address, ordinal);
                }
                Address::Shelley(x) => {
                    let utxos = indexes
                        .utxos_by_tag(dimensions::ADDRESS, &x.to_vec())
                        .unwrap();
                    assertion(utxos, &address, ordinal);

                    let utxos = indexes
                        .utxos_by_tag(dimensions::PAYMENT, &x.payment().to_vec())
                        .unwrap();
                    assertion(utxos, &address, ordinal);

                    match x.delegation() {
                        ShelleyDelegationPart::Key(..) | ShelleyDelegationPart::Script(..) => {
                            let utxos = indexes
                                .utxos_by_tag(dimensions::STAKE, &x.delegation().to_vec())
                                .unwrap();
                            assertion(utxos, &address, ordinal);
                        }
                        _ => {
                            let utxos = indexes
                                .utxos_by_tag(dimensions::STAKE, &x.delegation().to_vec())
                                .unwrap();
                            assert!(utxos.is_empty());
                        }
                    }
                }
                Address::Stake(x) => {
                    let utxos = indexes
                        .utxos_by_tag(dimensions::STAKE, &x.to_vec())
                        .unwrap();
                    assertion(utxos, &address, ordinal);
                }
            };
        }
    }

    #[test]
    fn test_count_utxos_by_address() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();
        let indexes = build_indexes(&store);

        let utxo_generator = |x: &TestAddress| utxo_with_random_amount(x, 1_000_000..1_500_000);

        let delta = make_custom_utxo_delta(TestAddress::everyone(), 10..11, utxo_generator);

        apply_utxoset!(store, &indexes, [&delta]);

        for address in TestAddress::everyone().iter() {
            let expected = delta
                .produced_utxo
                .values()
                .map(|x| get_utxo_address_and_value(x))
                .filter(|(addr, _)| addr == address.to_bytes().as_slice())
                .count();

            let count = indexes
                .count_utxo_by_address(address.to_bytes().as_slice())
                .unwrap();

            assert_eq!(expected as u64, count);
        }
    }

    #[test]
    fn test_iter_within_key() {
        let store = StateStore::in_memory(StateSchema::default()).unwrap();
        let indexes = build_indexes(&store);

        let utxo_generator = |x: &TestAddress| utxo_with_random_amount(x, 1_000_000..1_500_000);

        let delta = make_custom_utxo_delta(TestAddress::everyone(), 10..11, utxo_generator);

        apply_utxoset!(store, &indexes, [&delta]);

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

            let iterator = indexes
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
