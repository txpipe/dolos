use dolos_core::{
    BrokenInvariant, ChainError, Domain, EntityKey, Genesis, StateStore as _, StateWriter as _,
};
use tracing::debug;

use crate::{
    mutable_slots, sweep::Pots, EpochState, EraBoundary, EraSummary, Nonces, PParamsSet,
    EPOCH_KEY_MARK,
};

fn get_utxo_amount(genesis: &Genesis) -> u64 {
    let byron_utxo = pallas::ledger::configs::byron::genesis_utxos(&genesis.byron)
        .iter()
        .fold(0, |acc, (_, _, amount)| acc + amount);

    let shelley_utxo = pallas::ledger::configs::shelley::shelley_utxos(&genesis.shelley)
        .iter()
        .fold(0, |acc, (_, _, amount)| acc + amount);

    byron_utxo + shelley_utxo
}

const SHELLEY_PROTOCOL: u16 = 2;

fn bootstrap_pots(
    protocol: u16,
    genesis: &Genesis,
    pparams: &PParamsSet,
) -> Result<Pots, ChainError> {
    let initial_utxos = get_utxo_amount(genesis);

    // for any era before shelley, we don't have the concept of reserves or
    // treasury, so we just return the initial utxos
    if protocol < SHELLEY_PROTOCOL {
        return Ok(Pots {
            reserves: 0,
            treasury: 0,
            utxos: initial_utxos,
        });
    }

    let max_supply = genesis.shelley.max_lovelace_supply.unwrap_or_default();
    crate::sweep::compute_genesis_pots(max_supply, initial_utxos, pparams)
}

fn bootrap_epoch<D: Domain>(domain: &D) -> Result<EpochState, ChainError> {
    let genesis = domain.genesis();

    let mut pparams = crate::forks::from_byron_genesis(&genesis.byron);
    let mut nonces = None;

    if let Some(force_protocol) = genesis.force_protocol {
        pparams = crate::forks::evolve_pparams(&pparams, genesis, force_protocol as u16)?;

        // TODO: why do we set nonces only if there's a force protocol?
        nonces = Some(Nonces::bootstrap(genesis.shelley_hash));
    }

    let protocol = pparams.protocol_major().unwrap_or_default();

    let pots = bootstrap_pots(protocol, genesis, &pparams)?;

    let epoch = EpochState {
        pparams,
        number: 0,
        reserves: pots.reserves,
        treasury: pots.treasury,
        utxos: pots.utxos,
        active_stake: 0,
        deposits: 0,
        gathered_fees: 0,
        gathered_deposits: 0,
        decayed_deposits: 0,
        rewards_to_distribute: None,
        rewards_to_treasury: None,
        largest_stable_slot: genesis.shelley.epoch_length.unwrap() as u64 - mutable_slots(genesis),
        nonces,
    };

    let writer = domain.state().start_writer()?;
    writer.write_entity_typed(&EntityKey::from(EPOCH_KEY_MARK), &epoch)?;
    writer.commit()?;

    Ok(epoch)
}

fn bootstrap_eras<D: Domain>(domain: &D, epoch: &EpochState) -> Result<(), ChainError> {
    let system_start = epoch.pparams.system_start().unwrap_or_default();
    let epoch_length = epoch.pparams.epoch_length().unwrap_or_default();
    let slot_length = epoch.pparams.slot_length().unwrap_or_default();
    let protocol_major = epoch.pparams.protocol_major().unwrap_or_default();

    let era = EraSummary {
        start: EraBoundary {
            epoch: 0,
            slot: 0,
            timestamp: system_start,
        },
        end: None,
        epoch_length,
        slot_length,
    };

    let key = protocol_major.to_be_bytes();

    let writer = domain.state().start_writer()?;
    writer.write_entity_typed(&EntityKey::from(&key), &era)?;
    writer.commit()?;

    Ok(())
}

pub fn bootstrap_utxos<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let delta = crate::utxoset::compute_origin_delta(domain.genesis());

    let writer = domain.state().start_writer()?;
    writer.apply_utxoset(&delta)?;
    writer.commit()?;

    Ok(())
}

pub fn execute<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let epoch = bootrap_epoch(domain)?;

    bootstrap_eras(domain, &epoch)?;

    bootstrap_utxos(domain)?;

    Ok(())
}
