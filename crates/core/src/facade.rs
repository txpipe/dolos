use tracing::{error, info, warn};

use crate::{
    batch::WorkBatch, ArchiveStore, Block as _, BlockSlot, ChainLogic, ChainPoint, Domain,
    DomainError, IndexStore, MempoolAwareUtxoStore, MempoolStore, MempoolTx, RawBlock, StateStore,
    TipEvent, TxHash, WalStore, WorkUnit,
};

/// Process a batch of blocks during bulk import operations, skipping the WAL
/// and doesn't notify tip
fn execute_batch<D: Domain>(
    chain: &D::Chain,
    domain: &D,
    batch: &mut WorkBatch<D::Chain>,
    with_wal: bool,
) -> Result<BlockSlot, DomainError> {
    batch.load_utxos(domain)?;

    batch.decode_utxos(chain)?;

    // Chain-specific logic
    chain.compute_delta::<D>(domain.state(), domain.genesis(), batch)?;

    if with_wal {
        batch.commit_wal(domain)?;
    }

    batch.load_entities(domain)?;

    batch.apply_entities()?;

    batch.commit_state(domain)?;

    batch.commit_archive(domain)?;

    batch.commit_indexes(domain)?;

    Ok(batch.last_slot())
}

fn notify_work<D: Domain>(domain: &D, work: &WorkUnit<D::Chain>) {
    let WorkUnit::Blocks(batch) = work else {
        return;
    };

    for block in batch.iter_blocks() {
        let point = block.point();
        let raw = block.raw();
        domain.notify_tip(TipEvent::Apply(point.clone(), raw));
        info!(%point, "roll forward");
    }
}

fn execute_work<D: Domain>(
    chain: &mut D::Chain,
    domain: &D,
    work: &mut WorkUnit<D::Chain>,
    live: bool,
) -> Result<(), DomainError> {
    match work {
        WorkUnit::Genesis => {
            chain.apply_genesis::<D>(domain.state(), domain.indexes(), domain.genesis())?;
            domain.wal().reset_to(&ChainPoint::Origin)?;
        }
        WorkUnit::EWrap(slot) => {
            chain.apply_ewrap::<D>(domain.state(), domain.archive(), domain.genesis(), *slot)?;
        }
        WorkUnit::EStart(slot) => {
            chain.apply_estart::<D>(domain.state(), domain.archive(), domain.genesis(), *slot)?;
        }
        WorkUnit::Rupd(slot) => {
            chain.apply_rupd::<D>(domain.state(), domain.archive(), domain.genesis(), *slot)?;
        }
        WorkUnit::Blocks(batch) => {
            execute_batch(chain, domain, batch, live)?;
        }
        WorkUnit::ForcedStop => {
            return Err(DomainError::StopEpochReached);
        }
    };

    if live {
        notify_work(domain, work);
    }

    Ok(())
}

async fn drain_pending_work<D: Domain>(
    chain: &mut D::Chain,
    domain: &D,
    live: bool,
) -> Result<(), DomainError> {
    while let Some(mut work) = chain.pop_work() {
        execute_work(chain, domain, &mut work, live)?;
    }

    Ok(())
}

pub async fn import_blocks<D: Domain>(
    domain: &D,
    mut raw: Vec<RawBlock>,
) -> Result<BlockSlot, DomainError> {
    let mut last = 0;
    let mut chain = domain.write_chain().await;

    for block in raw.drain(..) {
        if !chain.can_receive_block() {
            drain_pending_work(&mut *chain, domain, false).await?;
        }

        last = chain.receive_block(block)?;
    }

    // one last drain to ensure we're up to date
    drain_pending_work(&mut *chain, domain, false).await?;

    Ok(last)
}

pub async fn roll_forward<D: Domain>(
    domain: &D,
    block: RawBlock,
) -> Result<BlockSlot, DomainError> {
    let mut chain = domain.write_chain().await;

    // we drain first in case there's previous work that needs to be applied (eg: initialization).
    drain_pending_work(&mut *chain, domain, true).await?;

    let last = chain.receive_block(block)?;

    drain_pending_work(&mut *chain, domain, true).await?;

    Ok(last)
}

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

fn check_indexes_in_sync_with_state<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let indexes = domain.indexes().read_cursor()?.map(|x| x.slot());
    let state = domain.state().read_cursor()?.map(|x| x.slot());

    match (indexes, state) {
        (Some(indexes), Some(state)) => {
            if indexes != state {
                warn!(%indexes, %state, "indexes are out of sync");
            }
        }
        (None, Some(_)) => {
            warn!("index cursor is missing");
        }
        (Some(_), None) => {
            warn!("found index cursor but no state");
        }
        (None, None) => (),
    }

    Ok(())
}

pub fn check_integrity<D: Domain>(domain: &D) -> Result<(), DomainError> {
    ensure_wal_in_sync_with_state(domain)?;
    check_archive_in_sync_with_state(domain)?;
    check_indexes_in_sync_with_state(domain)?;

    Ok(())
}

pub async fn bootstrap<D: Domain>(domain: &D) -> Result<(), DomainError>
where
{
    check_integrity(domain)?;

    // TODO: we should probably catch up stores here
    // catch_up_stores(domain)?;

    // drain any work that might have been defined by the initialization
    let mut chain = domain.write_chain().await;
    drain_pending_work(&mut *chain, domain, false).await?;

    Ok(())
}
