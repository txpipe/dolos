use dolos_core::{
    BrokenInvariant, ChainError, Domain, EntityKey, Genesis, State3Store, StateStore as _,
};
use tracing::debug;

use crate::{EraBoundary, EraSummary, PParamsState, EPOCH_KEY_MARK};

fn force_hardforks(
    pparams: &mut PParamsState,
    force_protocol: usize,
    genesis: &Genesis,
) -> Result<(), BrokenInvariant> {
    while pparams.protocol_major() < force_protocol as u16 {
        let previous = pparams.protocol_major();

        *pparams = crate::forks::bump_pparams_version(&pparams, genesis);

        // if the protocol major didn't increase, something went wrong and we might be
        // stuck in a loop. We return an error to avoid infinite loops.
        if pparams.protocol_major() <= previous as u16 {
            return Err(BrokenInvariant::InvalidGenesisConfig.into());
        }

        debug!(protocol = pparams.protocol_major(), "forced hardfork");
    }

    Ok(())
}

fn bootrap_pparams<D: Domain>(domain: &D) -> Result<PParamsState, ChainError> {
    let genesis = domain.genesis();

    let mut pparams = crate::forks::from_byron_genesis(&genesis.byron);

    if let Some(force_protocol) = genesis.force_protocol {
        force_hardforks(&mut pparams, force_protocol, genesis)?;
    }

    domain
        .state3()
        .write_entity_typed(&EntityKey::from(EPOCH_KEY_MARK), &pparams)?;

    Ok(pparams)
}

fn bootstrap_eras<D: Domain>(domain: &D, pparams: &PParamsState) -> Result<(), ChainError> {
    let era = EraSummary {
        start: EraBoundary {
            epoch: 0,
            slot: 0,
            timestamp: pparams.system_start,
        },
        end: None,
        epoch_length: pparams.epoch_length,
        slot_length: pparams.slot_length,
    };

    let key = pparams.protocol_major().to_be_bytes();

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
    let pparams = bootrap_pparams(domain)?;

    dbg!("pparams bootstrapped");

    bootstrap_eras(domain, &pparams)?;

    dbg!("eras bootstrapped");

    bootstrap_utxos(domain)?;

    dbg!("utxos bootstrapped");

    Ok(())
}
