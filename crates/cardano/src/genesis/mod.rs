use dolos_core::{ChainError, Domain, EntityKey, Genesis, StateStore as _, StateWriter as _};

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
    let utxos = get_utxo_amount(genesis);

    // for any era before shelley, we don't have the concept of reserves or
    // treasury, so we just return the initial utxos
    if protocol < SHELLEY_PROTOCOL {
        return Ok(Pots {
            reserves: 0,
            treasury: 0,
            utxos,
        });
    }

    let max_supply = genesis
        .shelley
        .max_lovelace_supply
        .ok_or(ChainError::GenesisFieldMissing(
            "max_lovelace_supply".to_string(),
        ))?;

    let initial_reserves = max_supply.saturating_sub(utxos);

    let tau = pparams.ensure_tau()?;
    let rho = pparams.ensure_rho()?;
    let eta = num_rational::BigRational::from_integer(num_bigint::BigInt::from(1));

    let pots = crate::pots::compute_pot_delta(initial_reserves, 0, &rho, &tau, eta);

    Ok(Pots {
        reserves: initial_reserves - pots.incentives + pots.available_rewards,
        treasury: pots.treasury_tax,
        utxos,
    })
}

fn bootrap_epoch<D: Domain>(state: &D::State, genesis: &Genesis) -> Result<EpochState, ChainError> {
    let mut pparams = crate::forks::from_byron_genesis(&genesis.byron);
    let mut nonces = None;

    if let Some(force_protocol) = genesis.force_protocol {
        pparams = crate::forks::force_pparams_version(&pparams, genesis, 0, force_protocol as u16)?;

        // TODO: why do we set nonces only if there's a force protocol?
        nonces = Some(Nonces::bootstrap(genesis.shelley_hash));
    }

    let protocol = pparams.protocol_major().unwrap_or_default();

    let pots = bootstrap_pots(protocol, genesis, &pparams)?;

    let epoch = EpochState {
        pparams,
        pparams_update: PParamsSet::default(),
        number: 0,
        reserves: pots.reserves,
        treasury: pots.treasury,
        utxos: pots.utxos,
        deposits: 0,
        gathered_fees: 0,
        gathered_deposits: 0,
        decayed_deposits: 0,
        blocks_minted: 0,
        effective_rewards: None,
        unspendable_rewards: None,
        treasury_tax: None,
        largest_stable_slot: genesis.shelley.epoch_length.unwrap() as u64 - mutable_slots(genesis),
        nonces,
    };

    let writer = state.start_writer()?;
    writer.write_entity_typed(&EntityKey::from(EPOCH_KEY_MARK), &epoch)?;
    writer.commit()?;

    Ok(epoch)
}

fn bootstrap_eras<D: Domain>(state: &D::State, epoch: &EpochState) -> Result<(), ChainError> {
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
    let epoch = bootrap_epoch::<D>(state, genesis)?;

    bootstrap_eras::<D>(state, &epoch)?;

    bootstrap_utxos::<D>(state, genesis)?;

    Ok(())
}
