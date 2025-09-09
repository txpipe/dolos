use dolos_core::{ArchiveStore, ChainError, Domain, DomainError};
use pallas::{
    crypto::{
        hash::Hash,
        nonce::{generate_epoch_nonce, generate_rolling_nonce},
    },
    ledger::traverse::MultiEraBlock,
};

use crate::{
    mutable_slots,
    pparams::ChainSummary,
    utils::{epoch_first_slot, get_first_shelley_slot_and_epoch},
};

/// Calculate rolling nonce for last inmutable block before epoch boundary using archive.
/// 
/// Note: This will iterate from the beggining of Shelley, will be heavy for high epochs.
pub fn calculate_nc_from_archive<D: Domain>(
    first_shelley_slot: u64,
    epoch: u64,
    domain: &D,
    summary: &ChainSummary,
) -> Result<Hash<32>, DomainError> {
    let mut eta = domain.genesis().shelley_hash;

    let epoch_first_slot = epoch_first_slot(epoch, summary);

    let mutable_slots = mutable_slots(domain.genesis());
    if mutable_slots > epoch_first_slot {
        return Err(ChainError::InvalidParameters.into());
    }

    let eta_slot = epoch_first_slot - mutable_slots;
    if eta_slot < first_shelley_slot {
        return Err(ChainError::InvalidParameters.into());
    }

    for (_, raw) in domain
        .archive()
        .get_range(Some(first_shelley_slot), Some(eta_slot - 1))?
    {
        let block = MultiEraBlock::decode(&raw).map_err(ChainError::from)?;
        eta = generate_rolling_nonce(
            eta,
            &block
                .header()
                .nonce_vrf_output()
                .map_err(ChainError::from)?
                .to_vec(),
        );
    }

    Ok(eta)
}

/// Get rolling nonce from last inmutable block before epoch boundary using state.
///
/// Note: Only valid for latest epoch.
pub fn get_nc_from_state<D: Domain>(
    domain: &D,
    summary: &ChainSummary,
) -> Result<Hash<32>, DomainError> {
    match get_nc
    Ok(eta)
}

/// Get the previous block hash of the first block from the previous epoch.
pub fn get_nh<D: Domain>(
    epoch: u64,
    domain: &D,
    summary: &ChainSummary,
) -> Result<Option<Hash<32>>, DomainError> {
    let slot = epoch_first_slot(epoch - 1, summary);
    let (_, raw) = domain
        .archive()
        .get_range(None, Some(slot - 1))?
        .next_back()
        .unwrap();

    let block = MultiEraBlock::decode(&raw).map_err(ChainError::from)?;
    Ok(block.header().previous_hash())
}

fn compute_nonce_from_archive<D: Domain>(
    epoch: u64,
    domain: &D,
    summary: &ChainSummary,
    use_archive: bool,
) -> Result<Hash<32>, DomainError> {
    let (first_shelley_slot, first_shelley_epoch) = get_first_shelley_slot_and_epoch(&summary)
        .ok_or(DomainError::from(ChainError::InvalidParameters))?;

    if first_shelley_epoch == epoch {
        return Ok(domain.genesis().shelley_hash);
    }

    let nc = if use_archive {
        get_nc_from_archive(first_shelley_slot, epoch, domain, &summary)?
    } else {
        get_nc_from_state(first_shelley_slot, epoch, domain, &summary)?
    };
    match get_nh(epoch, domain, &summary)? {
        Some(nh) => {
            dbg!(nh);
            Ok(generate_epoch_nonce(nc, nh, None))
        }
        None => Ok(nc),
    }
}
