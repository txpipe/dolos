use dolos_core::{ChainError, Domain, EntityKey, State3Store, StateStore as _};
use tracing::debug;

use crate::{EraBoundary, EraSummary, PParamsState, EPOCH_KEY_MARK};

fn bootrap_pparams<D: Domain>(domain: &D) -> Result<PParamsState, ChainError> {
    let genesis = domain.genesis();

    let mut pparams = crate::forks::from_byron_genesis(&genesis.byron);

    if let Some(force_protocol) = genesis.force_protocol {
        for next_protocol in 1..=force_protocol {
            pparams = crate::forks::migrate_pparams(&pparams, genesis, next_protocol);

            debug!(protocol = next_protocol, "forced hardfork");
        }
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

    bootstrap_eras(domain, &pparams)?;

    bootstrap_utxos(domain)?;

    Ok(())
}
