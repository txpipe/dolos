use pallas::ledger::addresses::{Address, ShelleyDelegationPart};
use std::{collections::HashSet, str::FromStr as _};

use dolos_testing::*;

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
fn cursor_is_persisted() {
    let store = LedgerStore::in_memory_v2().unwrap();

    let delta = fake_genesis_delta(1_000_000_000);
    store.apply(&[delta]).unwrap();

    for i in 1..10 {
        let delta = forward_delta_from_slot(i);
        store.apply(&[delta]).unwrap();

        assert_eq!(
            store.cursor().unwrap(),
            Some(ChainPoint::Specific(i, slot_to_hash(i)))
        );
    }

    for i in 0..5 {
        let undo_slot = 10 - i;
        let delta = undo_delta_from_slot(undo_slot);
        store.apply(&[delta]).unwrap();

        let cursor_slot = undo_slot - 1;

        assert_eq!(
            store.cursor().unwrap(),
            Some(ChainPoint::Specific(cursor_slot, slot_to_hash(cursor_slot)))
        );
    }
}

#[test]
fn empty_until_cursor() {
    let store = LedgerStore::in_memory_v2().unwrap();

    assert!(store.is_empty().unwrap());

    let delta = fake_genesis_delta(1_000_000_000);
    store.apply(&[delta]).unwrap();

    let delta = forward_delta_from_slot(1);
    store.apply(&[delta]).unwrap();

    assert!(!store.is_empty().unwrap());
}

fn get_test_address_utxos(store: &LedgerStore, address: TestAddress) -> UtxoMap {
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

    let bobs = get_test_address_utxos(&store, TestAddress::Bob);
    assert_eq!(bobs.len(), 1);
    assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

    let carols = get_test_address_utxos(&store, TestAddress::Carol);
    assert_eq!(carols.len(), 1);
    assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
}

#[test]
fn test_apply_forward_block() {
    let store = LedgerStore::in_memory_v2().unwrap();

    let genesis = fake_genesis_delta(1_000_000_000);
    store.apply(&[genesis]).unwrap();

    let bobs = get_test_address_utxos(&store, TestAddress::Bob);
    let delta = make_move_utxo_delta(bobs, 1, 1, TestAddress::Carol);
    store.apply(std::slice::from_ref(&delta)).unwrap();

    assert_eq!(
        store.cursor().unwrap(),
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
    let store = LedgerStore::in_memory_v2().unwrap();

    let genesis = fake_genesis_delta(1_000_000_000);
    store.apply(&[genesis]).unwrap();

    let bobs = get_test_address_utxos(&store, TestAddress::Bob);
    let forward = make_move_utxo_delta(bobs, 1, 1, TestAddress::Carol);
    store.apply(std::slice::from_ref(&forward)).unwrap();

    let undo = revert_delta(forward);
    store.apply(std::slice::from_ref(&undo)).unwrap();

    // TODO: the store is not persisting the origin cursor, instead it's keeping it
    // empty. We should fix this in the next breaking change version.
    assert_eq!(store.cursor().unwrap(), None);

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
    let store = LedgerStore::in_memory_v2().unwrap();

    let genesis = fake_genesis_delta(1_000_000_000);
    store.apply(std::slice::from_ref(&genesis)).unwrap();
    batch.push(genesis);

    let bobs = get_test_address_utxos(&store, TestAddress::Bob);
    let forward = make_move_utxo_delta(bobs, 1, 1, TestAddress::Carol);
    store.apply(std::slice::from_ref(&forward)).unwrap();
    batch.push(forward.clone());

    let undo = revert_delta(forward);
    store.apply(std::slice::from_ref(&undo)).unwrap();
    batch.push(undo);

    // now we apply the batch in one go.
    let store = LedgerStore::in_memory_v2().unwrap();
    store.apply(&batch).unwrap();

    let bobs = get_test_address_utxos(&store, TestAddress::Bob);
    assert_eq!(bobs.len(), 1);
    assert_utxo_map_address_and_value(&bobs, TestAddress::Bob, 1_000_000_000);

    let carols = get_test_address_utxos(&store, TestAddress::Carol);
    assert_eq!(carols.len(), 1);
    assert_utxo_map_address_and_value(&carols, TestAddress::Carol, 1_000_000_000);
}

#[test]
fn test_query_by_address() {
    let store = LedgerStore::in_memory_v2().unwrap();

    let addresses: Vec<_> = TestAddress::everyone().into_iter().enumerate().collect();

    let initial_utxos = addresses
        .iter()
        .map(|(ordinal, address)| {
            fake_genesis_utxo(address.clone(), *ordinal, 1_000_000_000 * (*ordinal as u64))
        })
        .collect();

    let delta = UtxoSetDelta {
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
    let store = LedgerStore::in_memory_v2().unwrap();

    let utxo_generator = |x: &TestAddress| utxo_with_random_amount(x, 1_000_000..1_500_000);

    let delta = make_custom_utxo_delta(0, TestAddress::everyone(), 10..11, utxo_generator);

    store.apply(std::slice::from_ref(&delta)).unwrap();

    for address in TestAddress::everyone().iter() {
        let expected = delta
            .produced_utxo
            .values()
            .map(get_utxo_address_and_value)
            .filter(|(addr, _)| addr == address.to_bytes().as_slice())
            .count();

        let count = store
            .count_utxos_by_address(address.to_bytes().as_slice())
            .unwrap();

        assert_eq!(expected as u64, count);
    }
}

#[test]
fn test_iter_within_key() {
    let store = LedgerStore::in_memory_v2().unwrap();

    let utxo_generator = |x: &TestAddress| utxo_with_random_amount(x, 1_000_000..1_500_000);

    let delta = make_custom_utxo_delta(0, TestAddress::everyone(), 10..11, utxo_generator);

    store.apply(std::slice::from_ref(&delta)).unwrap();

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
            .iter_utxos_by_address(address.to_bytes().as_slice())
            .unwrap();

        for key in iterator {
            let key = key.unwrap();
            assert!(expected.remove(&key));
        }

        assert!(expected.is_empty());
    }
}
