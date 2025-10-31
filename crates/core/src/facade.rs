use tracing::{error, info, warn};

use crate::{
    batch::WorkBatch, ArchiveStore, Block as _, BlockSlot, ChainError, ChainLogic, ChainPoint,
    Domain, DomainError, RawBlock, StateStore, TipEvent, WalStore, WorkKind, WorkUnit,
};

/// Process a batch of blocks during bulk import operations, skipping the WAL
/// and doesn't notify tip
fn execute_batch<D: Domain>(
    domain: &D,
    batch: &mut WorkBatch<D::Chain>,
    with_wal: bool,
) -> Result<BlockSlot, DomainError> {
    batch.load_utxos(domain)?;

    batch.decode_utxos(domain.chain())?;

    // Chain-specific logic
    domain.chain().compute_delta::<D>(domain.state(), batch)?;

    if with_wal {
        batch.commit_wal(domain)?;
    }

    batch.load_entities(domain)?;

    batch.apply_entities()?;

    batch.commit_state(domain)?;

    batch.commit_archive(domain)?;

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
    domain: &D,
    work: &mut WorkUnit<D::Chain>,
    with_wal: bool,
) -> Result<(), DomainError> {
    match work {
        WorkUnit::Genesis => {
            domain
                .chain()
                .apply_genesis::<D>(domain.state(), domain.genesis())?;

            domain.wal().reset_to(&ChainPoint::Origin)?;
        }

        WorkUnit::EWrap(slot) => {
            domain.chain().apply_ewrap::<D>(
                domain.state(),
                domain.archive(),
                domain.genesis(),
                *slot,
            )?;
        }
        WorkUnit::EStart(slot) => {
            domain.chain().apply_estart::<D>(
                domain.state(),
                domain.archive(),
                domain.genesis(),
                *slot,
            )?;
        }
        WorkUnit::Rupd(slot) => {
            domain.chain().apply_rupd::<D>(
                domain.state(),
                domain.archive(),
                domain.genesis(),
                *slot,
            )?;
        }
        WorkUnit::Blocks(batch) => {
            execute_batch(domain, batch, with_wal)?;
        }
    }

    Ok(())
}

fn drain_pending_work<D: Domain>(domain: &D) -> Result<(), DomainError> {
    while !domain.chain().can_receive_block() {
        if let Some(mut work) = domain.chain().pop_work() {
            execute_work(domain, &mut work, false)?;
        }
    }

    Ok(())
}

pub fn import_blocks<D: Domain>(
    domain: &D,
    mut raw: Vec<RawBlock>,
) -> Result<BlockSlot, DomainError> {
    let mut last = 0;

    for block in raw.drain(..) {
        drain_pending_work(domain)?;
        last = domain.chain().receive_block(block)?;
    }

    Ok(last)
}

pub fn roll_forward<D: Domain>(domain: &D, block: RawBlock) -> Result<BlockSlot, DomainError> {
    let last = domain.chain().receive_block(block)?;

    while let Some(mut work) = domain.chain().pop_work() {
        execute_work(domain, &mut work, true)?;
        notify_work(domain, &work);
    }

    Ok(last)
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

pub fn check_integrity<D: Domain>(domain: &D) -> Result<(), DomainError> {
    ensure_wal_in_sync_with_state(domain)?;
    check_archive_in_sync_with_state(domain)?;

    Ok(())
}

pub fn bootstrap<D: Domain>(domain: &D) -> Result<(), DomainError>
where
{
    check_integrity(domain)?;

    // TODO: we should probably catch up stores here
    // catch_up_stores(domain)?;

    // drain any work that might have been defined by the initialization
    drain_pending_work(domain)?;

    Ok(())
}
