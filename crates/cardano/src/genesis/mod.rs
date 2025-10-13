use dolos_core::{ChainError, Domain, EntityKey, Genesis, StateStore as _, StateWriter as _};

use crate::{
    mutable_slots, pallas_ratio, pots::Pots, ratio, EpochState, EraBoundary, EraSummary, Nonces,
    PParamsSet, EPOCH_KEY_MARK,
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

fn bootstrap_pots(protocol: u16, genesis: &Genesis) -> Result<Pots, ChainError> {
    let utxos = get_utxo_amount(genesis);

    // for any era before shelley, we don't have the concept of reserves or
    // treasury, so we just return the initial utxos
    if protocol < SHELLEY_PROTOCOL {
        return Ok(Pots {
            utxos,
            reserves: 0,
            treasury: 0,
            fees: 0,
            deposits: 0,
            rewards: 0,
        });
    }

    let max_supply = genesis
        .shelley
        .max_lovelace_supply
        .ok_or(ChainError::GenesisFieldMissing(
            "max_lovelace_supply".to_string(),
        ))?;

    let reserves = max_supply.saturating_sub(utxos);

    Ok(Pots {
        reserves,
        utxos,
        treasury: 0,
        fees: 0,
        deposits: 0,
        rewards: 0,
    })
}

pub fn bootstrap_epoch<D: Domain>(
    state: &D::State,
    genesis: &Genesis,
) -> Result<EpochState, ChainError> {
    let mut pparams = crate::forks::from_byron_genesis(&genesis.byron);
    let mut nonces = None;

    if let Some(force_protocol) = genesis.force_protocol {
        pparams = crate::forks::force_pparams_version(&pparams, genesis, 0, force_protocol as u16)?;

        // TODO: why do we set nonces only if there's a force protocol?
        nonces = Some(Nonces::bootstrap(genesis.shelley_hash));
    }

    let protocol = pparams.protocol_major().unwrap_or_default();

    let pots = bootstrap_pots(protocol, genesis)?;

    let epoch = EpochState {
        pparams,
        initial_pots: pots,
        largest_stable_slot: genesis.shelley.epoch_length.unwrap() as u64 - mutable_slots(genesis),
        nonces,
        number: 0,

        // computed throughout the epoch during _roll_
        pparams_update: PParamsSet::default(),
        produced_utxos: 0,
        consumed_utxos: 0,
        gathered_fees: 0,
        gathered_deposits: 0,
        decayed_deposits: 0,
        pot_delta: None,
        final_pots: None,
        blocks_minted: 0,
    };

    let writer = state.start_writer()?;
    writer.write_entity_typed(&EntityKey::from(EPOCH_KEY_MARK), &epoch)?;
    writer.commit()?;

    Ok(epoch)
}

pub fn bootstrap_eras<D: Domain>(state: &D::State, epoch: &EpochState) -> Result<(), ChainError> {
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

    let writer = state.start_writer()?;
    writer.write_entity_typed(&EntityKey::from(&key), &era)?;
    writer.commit()?;

    Ok(())
}

pub fn bootstrap_utxos<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<(), ChainError> {
    let delta = crate::utxoset::compute_origin_delta(genesis);

    let writer = state.start_writer()?;
    writer.apply_utxoset(&delta)?;
    writer.commit()?;

    Ok(())
}

pub fn execute<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<(), ChainError> {
    let epoch = bootstrap_epoch::<D>(state, genesis)?;

    bootstrap_eras::<D>(state, &epoch)?;

    bootstrap_utxos::<D>(state, genesis)?;

    Ok(())
}
