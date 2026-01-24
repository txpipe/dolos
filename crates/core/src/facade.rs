//! High-level facade functions for domain operations.
//!
//! This module provides the main entry points for interacting with the domain,
//! including block import, chain synchronization, and transaction handling.

use tracing::{error, info, warn};

use crate::{
    executor::execute_work_unit, ArchiveStore, BlockSlot, ChainLogic, ChainPoint, Domain,
    DomainError, MempoolAwareUtxoStore, MempoolStore, MempoolTx, RawBlock, StateStore, TxHash,
    WalStore,
};

/// Drain all pending work from the chain logic.
///
/// This function repeatedly pops work units from the chain logic and executes
/// them until no more work is available.
async fn drain_pending_work<D: Domain>(
    chain: &mut D::Chain,
    domain: &D,
) -> Result<(), DomainError> {
    while let Some(mut work) = chain.pop_work::<D>(domain) {
        execute_work_unit(domain, work.as_mut())?;
    }

    Ok(())
}

/// Import a batch of blocks during bulk import operations.
///
/// This function processes multiple blocks efficiently for initial sync
/// or catch-up scenarios. Tip notifications are suppressed during bulk import.
pub async fn import_blocks<D: Domain>(
    domain: &D,
    mut raw: Vec<RawBlock>,
) -> Result<BlockSlot, DomainError> {
    let mut last = 0;
    let mut chain = domain.write_chain().await;

    for block in raw.drain(..) {
        if !chain.can_receive_block() {
            drain_pending_work(&mut *chain, domain).await?;
        }

        last = chain.receive_block(block)?;
    }

    // one last drain to ensure we're up to date
    drain_pending_work(&mut *chain, domain).await?;

    Ok(last)
}

/// Process a single block during live synchronization.
///
/// This function handles a single block received during live sync,
/// executing any resulting work units and notifying tip subscribers.
pub async fn roll_forward<D: Domain>(
    domain: &D,
    block: RawBlock,
) -> Result<BlockSlot, DomainError> {
    let mut chain = domain.write_chain().await;

    // we drain first in case there's previous work that needs to be applied (eg: initialization).
    drain_pending_work(&mut *chain, domain).await?;

    let last = chain.receive_block(block)?;

    drain_pending_work(&mut *chain, domain).await?;

    Ok(last)
}

/// Validate a transaction against the current ledger state.
pub fn validate_tx<D: Domain>(
    domain: &D,
    chain: &D::Chain,
    cbor: &[u8],
) -> Result<MempoolTx, DomainError> {
    let tip = domain.state().read_cursor()?;

    let utxos =
        MempoolAwareUtxoStore::<'_, D>::new(domain.state(), domain.indexes(), domain.mempool());

    let tx = chain.validate_tx(cbor, &utxos, tip, &domain.genesis())?;

    Ok(tx)
}

/// Validate and receive a transaction into the mempool.
pub fn receive_tx<D: Domain>(
    domain: &D,
    chain: &D::Chain,
    cbor: &[u8],
) -> Result<TxHash, DomainError> {
    let tx = validate_tx(domain, chain, cbor)?;

    let hash = tx.hash;

    domain.mempool().receive(tx)?;

    Ok(hash)
}

fn ensure_wal_in_sync_with_state<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let wal = domain.wal().find_tip()?.map(|(point, _)| point);
    let state = domain.state().read_cursor()?;

    match (wal, state) {
        (Some(wal), Some(state)) => {
            if wal != state {
                warn!(%wal, %state, "wal is out of sync");
                info!("resetting wal to match state");
                domain.wal().reset_to(&state)?;
            }
        }
        (None, Some(state)) => {
            warn!(%state, "missing wal, resetting to match state");
            info!("resetting wal to match state");
            domain.wal().reset_to(&state)?;
        }
        (Some(_), None) => {
            // weird case, strictly speaking, this should clear the wal rather than reset to
            // origin
            warn!("missing wal, resetting to origin");
            info!("resetting wal to origin");
            domain.wal().reset_to(&ChainPoint::Origin)?;
        }
        (None, None) => (),
    }

    Ok(())
}

fn check_archive_in_sync_with_state<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let archive = domain.archive().get_tip()?.map(|(slot, _)| slot);
    let state = domain.state().read_cursor()?.map(|x| x.slot());

    match (archive, state) {
        (Some(archive), Some(state)) => {
            if archive != state {
                error!(%archive, %state, "archive is out of sync");
            }
        }
        (None, Some(_)) => {
            warn!("archive is missing");
        }
        (Some(_), None) => {
            error!("found archive but no state");
        }
        (None, None) => (),
    }

    Ok(())
}

/// Check domain integrity.
///
/// Ensures WAL and archive are in sync with the state store.
pub fn check_integrity<D: Domain>(domain: &D) -> Result<(), DomainError> {
    ensure_wal_in_sync_with_state(domain)?;
    check_archive_in_sync_with_state(domain)?;
    check_indexes_in_sync_with_state(domain)?;

    Ok(())
}

/// Bootstrap the domain.
///
/// Performs integrity checks and drains any pending initialization work.
pub async fn bootstrap<D: Domain>(domain: &D) -> Result<(), DomainError>
where
{
    check_integrity(domain)?;

    // TODO: we should probably catch up stores here
    // catch_up_stores(domain)?;

    // drain any work that might have been defined by the initialization
    let mut chain = domain.write_chain().await;
    drain_pending_work(&mut *chain, domain).await?;

    Ok(())
}
