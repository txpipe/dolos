use std::sync::Arc;

use dolos_cardano::{active_era_index, era_table, load_era_table, EraBoundary, EraKey, EraState};
use dolos_core::{Domain as _, StateStore as _, StateWriter as _};
use dolos_testing::{era_fixtures::PublicNetwork, toy_domain::ToyDomain};

fn protocols(table: &[EraState]) -> Vec<u16> {
    table.iter().map(|era| era.protocol).collect()
}

#[test]
fn public_networks_list_every_era_contiguously_from_slot_zero() {
    for network in PublicNetwork::all() {
        let table = era_table(&network.eras, &network.genesis).unwrap();

        assert_eq!(protocols(&table), [0, 2, 3, 4, 5, 7, 9]);
        assert_eq!(table[0].start.slot, 0);
        assert_eq!(table[0].start.epoch, 0);

        for pair in table.windows(2) {
            let end = pair[0].end.as_ref().unwrap();

            assert_eq!(end.slot, pair[1].start.slot);
            assert_eq!(end.epoch, pair[1].start.epoch);
            assert_eq!(end.timestamp, pair[1].start.timestamp);
        }

        assert!(table.last().unwrap().end.is_none());
    }
}

#[test]
fn padded_rows_start_at_the_system_start() {
    let table = era_table(&[], &dolos_cardano::include::mainnet::load()).unwrap();

    assert_eq!(table[0].start.timestamp, 1506203091);
    assert_eq!(table[0].end.as_ref().unwrap().slot, 4492800);
    assert_eq!(
        table[0].end.as_ref().unwrap().timestamp,
        1506203091 + 89856000
    );

    let preview = dolos_cardano::include::preview::load();
    let table = era_table(&[], &preview).unwrap();

    assert_eq!(protocols(&table), [0, 2, 3, 4, 5]);
    assert_eq!(table[0].epoch_length, 4320);
    assert_eq!(table[0].slot_length, 20);
    assert_eq!(table[1].epoch_length, 86400);
    assert_eq!(table[1].slot_length, 1);
    assert!(table
        .iter()
        .all(|era| { era.start.timestamp == 1666656000 && era.end.as_ref().unwrap().slot == 0 }));
}

#[test]
fn a_custom_magic_or_unusable_genesis_has_no_known_history() {
    let mut genesis = dolos_cardano::include::preview::load();
    genesis.shelley.network_magic = Some(42);
    assert!(era_table(&[], &genesis).is_none());

    let mut genesis = dolos_cardano::include::preview::load();
    genesis.shelley.network_magic = None;
    assert!(era_table(&[], &genesis).is_none());

    let mut genesis = dolos_cardano::include::preview::load();
    genesis.shelley.system_start = Some("not a date".into());
    assert!(era_table(&[], &genesis).is_none());

    let mut genesis = dolos_cardano::include::preview::load();
    genesis.shelley.epoch_length = None;
    assert!(era_table(&[], &genesis).is_none());
}

#[test]
fn loading_serves_the_table_with_the_last_row_open() {
    for network in PublicNetwork::all() {
        let domain = ToyDomain::new_with_genesis(network.genesis.clone(), None, None);
        network.seed(&domain);

        let loaded = load_era_table(domain.state(), &network.genesis).unwrap();

        assert_eq!(protocols(&loaded), [0, 2, 3, 4, 5, 7, 9]);
        assert!(loaded.last().unwrap().end.is_none());
    }
}

#[test]
fn loading_opens_a_padded_last_row() {
    // a preview store fresh from genesis holds protocol 6 only
    let genesis = Arc::new(dolos_cardano::include::preview::load());
    let domain = ToyDomain::new_with_genesis(genesis.clone(), None, None);

    let loaded = load_era_table(domain.state(), &genesis).unwrap();

    assert_eq!(protocols(&loaded), [0, 2, 3, 4, 5]);
    assert!(loaded[3].end.is_some());
    assert!(loaded[4].end.is_none());
}

#[test]
fn loading_a_custom_network_returns_what_state_holds() {
    let mut genesis = dolos_cardano::include::preview::load();
    genesis.shelley.network_magic = Some(42);
    let genesis = Arc::new(genesis);
    let domain = ToyDomain::new_with_genesis(genesis.clone(), None, None);

    let boundary = |slot| EraBoundary {
        epoch: 1,
        slot,
        timestamp: 1666656000 + slot,
    };
    let writer = domain.state().start_writer().unwrap();
    writer
        .write_entity_typed(
            &EraKey::from(7),
            &EraState {
                start: boundary(86400),
                end: None,
                epoch_length: 86400,
                slot_length: 1,
                // an old record without its protocol; the key has it
                protocol: 0,
            },
        )
        .unwrap();
    writer.commit().unwrap();

    let loaded = load_era_table(domain.state(), &genesis).unwrap();

    assert_eq!(protocols(&loaded), [6, 7]);
    assert_eq!(loaded[0].start.slot, 0);
    assert_eq!(loaded[1].start.slot, 86400);
    assert!(loaded[1].end.is_none());
}

#[test]
fn loading_an_empty_store_returns_nothing() {
    let genesis = dolos_cardano::include::mainnet::load();
    let state = dolos_testing::toy_domain::empty_state_store();

    assert!(load_era_table(&state, &genesis).unwrap().is_empty());
}

#[test]
fn the_active_era_covers_the_bumps_inside_it() {
    let network = PublicNetwork::mainnet();
    let table = era_table(&network.eras, &network.genesis).unwrap();
    let name = |protocol| active_era_index(&table, protocol).map(|index| table[index].protocol);

    for (active, row) in [
        (0, 0),
        (1, 0),
        (2, 2),
        (4, 4),
        (5, 5),
        (6, 5),
        (7, 7),
        (8, 7),
        (9, 9),
        (10, 9),
        (11, 9),
    ] {
        assert_eq!(name(active), Some(row), "active protocol {active}");
    }

    assert_eq!(active_era_index(&[], 10), None);
}
