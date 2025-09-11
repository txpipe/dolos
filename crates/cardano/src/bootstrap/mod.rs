use dolos_core::{
    BrokenInvariant, ChainError, Domain, EntityKey, Genesis, State3Store, StateStore as _,
};
use tracing::debug;

use crate::{EpochState, EraBoundary, EraSummary, PParamsSet, EPOCH_KEY_MARK};

fn force_hardforks(
    pparams: &mut PParamsSet,
    force_protocol: u16,
    genesis: &Genesis,
) -> Result<(), BrokenInvariant> {
    while pparams.protocol_major().unwrap_or_default() < force_protocol {
        let previous = pparams.protocol_major();

        dbg!(&pparams);

        *pparams = crate::forks::bump_pparams_version(&pparams, genesis);

        dbg!(&pparams);

        // if the protocol major is not set, something went wrong and we might be
        // stuck in a loop. We return an error to avoid infinite loops.
        let Some(previous) = previous else {
            return Err(BrokenInvariant::InvalidGenesisConfig.into());
        };

        // if the protocol major didn't increase, something went wrong and we might be
        // stuck in a loop. We return an error to avoid infinite loops.
        if pparams.protocol_major().unwrap_or_default() <= previous {
            return Err(BrokenInvariant::InvalidGenesisConfig.into());
        }

        debug!(protocol = pparams.protocol_major(), "forced hardfork");
    }

    Ok(())
}

fn bootrap_epoch<D: Domain>(domain: &D) -> Result<EpochState, ChainError> {
    let genesis = domain.genesis();

    let mut pparams = crate::forks::from_byron_genesis(&genesis.byron);

    if let Some(force_protocol) = genesis.force_protocol {
        force_hardforks(&mut pparams, force_protocol as u16, genesis)?;
    }

    let epoch = EpochState {
        pparams,
        number: 0,
        reserves: genesis.shelley.max_lovelace_supply.unwrap_or_default(),
        ..Default::default()
    };

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_MARK), &epoch)?;

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

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(&key), &era)?;

    Ok(())
}

pub fn bootstrap_utxos<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let delta = crate::utxoset::compute_origin_delta(domain.genesis());

    domain.state().apply(&[delta])?;

    Ok(())
}

pub fn execute<D: Domain>(domain: &D) -> Result<(), ChainError> {
    let epoch = bootrap_epoch(domain)?;

    dbg!("pparams bootstrapped");

    bootstrap_eras(domain, &epoch)?;

    dbg!("eras bootstrapped");

    bootstrap_utxos(domain)?;

    dbg!("utxos bootstrapped");

    Ok(())
}
