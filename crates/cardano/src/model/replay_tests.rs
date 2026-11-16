//! Catch-up applies a WAL entry's deltas a second time, decoded from the WAL
//! and restored by `ChainLogic::prepare_replay`. Every delta variant must
//! then give the entity it was first applied to the same result as its first
//! apply did.

use std::{cell::RefCell, collections::BTreeSet, sync::Arc};

use dolos_core::{
    builtin::MemoryStateStore, ChainLogic as _, Domain as _, Entity as _, EntityDelta as _,
    EntityValue, Genesis, LogValue, Namespace, StateStore as _, StateWriter as _,
};
use dolos_testing::{blocks::make_conway_block, toy_domain::ToyDomain};
use pallas::ledger::primitives::Epoch;
use proptest::test_runner::{Config, TestCaseError, TestRunner};

use super::testing::ReplayCase;
use super::*;
use crate::CardanoLogic;

const ERA_START: u64 = 100_000;

fn all_cases(genesis: Arc<Genesis>) -> Vec<ReplayCase> {
    [
        account_addresses::prop_tests::replay_cases(),
        accounts::prop_tests::replay_cases(),
        asset_mints::prop_tests::replay_cases(),
        datums::tests::replay_cases(),
        drep_observations::tests::replay_cases(),
        dreps::prop_tests::replay_cases(),
        epochs::prop_tests::replay_cases(genesis),
        first_mints::prop_tests::replay_cases(),
        gov::prop_tests::replay_cases(),
        metadata_labels::tests::replay_cases(),
        pending::prop_tests::replay_cases(),
        pools::prop_tests::replay_cases(),
        proposals::prop_tests::replay_cases(),
        scripts::tests::replay_cases(),
        stats::prop_tests::replay_cases(),
    ]
    .into_iter()
    .flatten()
    .collect()
}

/// The WAL codec writes a variant as its index, then its payload.
fn variant_index(delta: &CardanoDelta) -> u32 {
    let encoded = bincode::serialize(delta).unwrap();
    u32::from_le_bytes(encoded[..4].try_into().unwrap())
}

/// The indices of the variants the WAL can hold, read off the codec itself:
/// an index alone decodes to a retired variant, fails on the missing payload
/// of a live one, and is refused past the last variant.
fn live_variant_indices() -> BTreeSet<u32> {
    let mut live = BTreeSet::new();
    for index in 0u32.. {
        match bincode::deserialize::<CardanoDelta>(&index.to_le_bytes()) {
            Ok(
                CardanoDelta::RetiredMintStatsUpdate(_)
                | CardanoDelta::RetiredMetadataTxUpdate(_)
                | CardanoDelta::RetiredDRepAnchorUpdate(_)
                | CardanoDelta::RetiredDRepPowerUpdate(_)
                | CardanoDelta::RetiredScriptFirstSeen(_),
            ) => {}
            Err(error) if error.to_string().contains("variant index") => break,
            _ => {
                live.insert(index);
            }
        }
    }
    live
}

/// The epoch of the block that produced the delta. Only a minting delta
/// depends on it.
fn block_epoch(delta: &CardanoDelta) -> Epoch {
    match delta {
        CardanoDelta::MintedBlocksInc(delta) => delta.epoch,
        _ => 0,
    }
}

/// A state whose single era puts `block` in `epoch`, and which has
/// initialized that epoch.
fn state_with_block_in(epoch: Epoch) -> (MemoryStateStore, Vec<u8>) {
    let state = MemoryStateStore::new();
    let writer = state.start_writer().unwrap();
    writer
        .write_entity_typed(
            &EraKey::from(9),
            &EraState {
                start: EraBoundary {
                    epoch,
                    slot: ERA_START,
                    timestamp: 0,
                },
                end: None,
                epoch_length: 100,
                slot_length: 1,
                protocol: 9,
            },
        )
        .unwrap();
    writer
        .write_entity_typed(
            &(),
            &EpochState {
                number: epoch,
                ..Default::default()
            },
        )
        .unwrap();
    writer.commit().unwrap();
    let (_, block) = make_conway_block(ERA_START + 7);
    (state, block.to_vec())
}

fn stored(entity: &Option<CardanoEntity>) -> Option<(Namespace, EntityValue)> {
    entity.as_ref().map(CardanoEntity::encode_entity)
}

fn replay_matches_first_apply(
    genesis: &Arc<Genesis>,
    entity: Option<CardanoEntity>,
    delta: CardanoDelta,
) -> Result<(), TestCaseError> {
    let (state, block) = state_with_block_in(block_epoch(&delta));

    let mut first = entity.clone();
    let mut applied = delta;
    applied.apply(&mut first);

    // the WAL row holds the deltas as their first apply left them
    let row = bincode::serialize(&LogValue {
        block: block.clone(),
        delta: vec![applied],
        inputs: Default::default(),
    })
    .unwrap();
    let mut row: LogValue<CardanoDelta> = bincode::deserialize(&row).unwrap();

    CardanoLogic::prepare_replay(&state, genesis.clone(), &block, &mut row.delta)
        .map_err(|error| TestCaseError::fail(format!("prepare_replay failed: {error}")))?;

    let mut replayed = entity;
    row.delta[0].apply(&mut replayed);

    proptest::prop_assert_eq!(stored(&replayed), stored(&first));
    Ok(())
}

#[test]
fn every_delta_variant_replays_from_the_wal() {
    let genesis = ToyDomain::new(None, None).genesis();
    let covered = RefCell::new(BTreeSet::new());

    for case in all_cases(genesis.clone()) {
        let mut runner = TestRunner::new(Config {
            cases: 32,
            failure_persistence: None,
            ..Config::default()
        });
        runner
            .run(&case, |(entity, delta)| {
                let index = variant_index(&delta);
                covered.borrow_mut().insert(index);
                replay_matches_first_apply(&genesis, entity, delta)
                    .map_err(|error| TestCaseError::fail(format!("variant {index}: {error}")))
            })
            .unwrap();
    }

    let missing: Vec<_> = live_variant_indices()
        .difference(&covered.borrow())
        .copied()
        .collect();
    assert!(
        missing.is_empty(),
        "delta variants {missing:?} have no replay case"
    );
}
