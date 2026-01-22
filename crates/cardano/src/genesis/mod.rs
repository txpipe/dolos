use dolos_core::{
    config::CardanoConfig, ChainError, Domain, EntityKey, Genesis, StateStore as _,
    StateWriter as _,
};

use crate::{
    pots::Pots, utils::nonce_stability_window, EpochState, EpochValue, EraBoundary, EraSummary,
    Lovelace, Nonces, PParamsSet, RollingStats, CURRENT_EPOCH_KEY,
};

mod staking;

fn get_utxo_amount(genesis: &Genesis) -> Lovelace {
    let byron_utxo = pallas::ledger::configs::byron::genesis_utxos(&genesis.byron)
        .iter()
        .fold(0, |acc, (_, _, amount)| acc + amount);

    let shelley_utxo = pallas::ledger::configs::shelley::shelley_utxos(&genesis.shelley)
        .iter()
        .fold(0, |acc, (_, _, amount)| acc + amount);

    byron_utxo + shelley_utxo
}

fn bootstrap_pots(pparams: &PParamsSet, genesis: &Genesis) -> Result<Pots, ChainError> {
    let utxos = get_utxo_amount(genesis);

    let max_supply = genesis
        .shelley
        .max_lovelace_supply
        .ok_or(ChainError::GenesisFieldMissing(
            "max_lovelace_supply".to_string(),
        ))?;

    let reserves = max_supply - utxos;

    Ok(Pots {
        reserves,
        utxos,
        deposit_per_pool: pparams.pool_deposit_or_default(),
        deposit_per_account: pparams.key_deposit_or_default(),
        ..Default::default()
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

    let pots = bootstrap_pots(&pparams, genesis)?;
    let protocol = pparams.ensure_protocol_major()?;

    let pparams = EpochValue::with_genesis(pparams);

    let epoch = EpochState {
        pparams,
        initial_pots: pots,
        largest_stable_slot: genesis.shelley.epoch_length.unwrap() as u64
            - nonce_stability_window(protocol as u16, genesis),
        nonces,
        previous_nonce_tail: None,
        number: 0,
        rolling: EpochValue::with_live(0, RollingStats::default()),
        end: None,
    };

    let writer = state.start_writer()?;
    writer.write_entity_typed(&EntityKey::from(CURRENT_EPOCH_KEY), &epoch)?;
    writer.commit()?;

    Ok(epoch)
}

pub fn bootstrap_eras<D: Domain>(state: &D::State, epoch: &EpochState) -> Result<(), ChainError> {
    let pparams = epoch.pparams.unwrap_live();

    let system_start = pparams.ensure_system_start()?;
    let epoch_length = pparams.ensure_epoch_length()?;
    let slot_length = pparams.ensure_slot_length()?;
    let protocol_major = pparams.protocol_major_or_default();

    let era = EraSummary {
        start: EraBoundary {
            epoch: 0,
            slot: 0,
            timestamp: system_start,
        },
        end: None,
        epoch_length,
        slot_length,
        protocol: protocol_major,
    };

    let key = protocol_major.to_be_bytes();

    let writer = state.start_writer()?;
    writer.write_entity_typed(&EntityKey::from(&key), &era)?;
    writer.commit()?;

    Ok(())
}

pub fn bootstrap_utxos<D: Domain>(
    state: &D::State,
    genesis: &Genesis,
    config: &CardanoConfig,
) -> Result<(), ChainError> {
    let writer = state.start_writer()?;

    let delta = crate::utxoset::compute_origin_delta(genesis);
    writer.apply_utxoset(&delta, true)?;

    let delta = crate::utxoset::build_custom_utxos_delta(config)?;
    writer.apply_utxoset(&delta, true)?;

    writer.commit()?;

    Ok(())
}

pub fn execute<D: Domain>(
    state: &D::State,
    genesis: &Genesis,
    config: &CardanoConfig,
) -> Result<(), ChainError> {
    let epoch = bootstrap_epoch::<D>(state, genesis)?;

    bootstrap_eras::<D>(state, &epoch)?;

    bootstrap_utxos::<D>(state, genesis, config)?;

    staking::bootstrap::<D>(state, genesis)?;

    Ok(())
}
