use tracing::{error, info, warn};

use crate::{
    batch::WorkBatch, ArchiveStore, Block as _, BlockSlot, ChainLogic, ChainPoint, Domain,
    DomainError, RawBlock, StateStore, TipEvent, WalStore, WorkKind, WorkUnit,
};

/// Process a batch of blocks during bulk import operations, skipping the WAL
/// and doesn't notify tip
fn import_batch<D: Domain>(
    domain: &D,
    mut batch: WorkBatch<D::Chain>,
) -> Result<BlockSlot, DomainError> {
    batch.load_utxos(domain)?;

    batch.decode_utxos(domain.chain())?;

    // Chain-specific logic
    domain
        .chain()
        .compute_delta::<D>(domain.state(), &mut batch)?;

    // since we're are importing finalized blocks, we don't care about the potential
    // for undos. This allows us to just drop the mutated delta without having to
    // persist it in the WAL.

    batch.load_entities(domain)?;

    batch.apply_entities()?;

    batch.commit_state(domain)?;

    batch.commit_archive(domain)?;

    Ok(batch.last_slot())
}

/// Process a batch of blocks during live operations, saving to WAL and
/// notifying tip
fn roll_batch<D: Domain>(
    domain: &D,
    mut batch: WorkBatch<D::Chain>,
) -> Result<BlockSlot, DomainError> {
    batch.load_utxos(domain)?;

    batch.decode_utxos(domain.chain())?;

    // Chain-specific logic
    domain
        .chain()
        .compute_delta::<D>(domain.state(), &mut batch)?;

    batch.commit_wal(domain)?;

    batch.load_entities(domain)?;

    batch.apply_entities()?;

    batch.commit_state(domain)?;

    batch.commit_archive(domain)?;

    for block in batch.iter_blocks() {
        let point = block.point();
        let raw = block.raw();
        domain.notify_tip(TipEvent::Apply(point.clone(), raw));
        info!(%point, "roll forward");
    }

    Ok(batch.last_slot())
}

/// Execute isolated work, such as genesis or sweeps.
///
/// Don't use this to execute blocks, those should be executed in a batch. This
/// method can be used for both import and roll operations.
fn execute_isolated_work<D: Domain>(
    domain: &D,
    work: WorkUnit<D::Chain>,
) -> Result<(), DomainError> {
    match work {
        WorkUnit::Genesis => {
            domain
                .chain()
                .apply_genesis::<D>(domain.state(), domain.genesis())?;

            domain.wal().reset_to(&ChainPoint::Origin)?;
        }

        WorkUnit::Sweep(slot) => {
            domain.chain().apply_sweep::<D>(
                domain.state(),
                domain.archive(),
                domain.genesis(),
                slot,
            )?;
        }
        WorkUnit::Block(_) => {
            unreachable!("isolated work can't be a block, only genesis or sweeps are allowed");
        }
    }

    Ok(())
}

fn drain_pending_work<D: Domain>(domain: &D) -> Result<(), DomainError> {
    while let Some(work) = domain.chain().pop_work() {
        execute_isolated_work(domain, work)?;
    }

    Ok(())
}

fn gather_batched_work<D: Domain>(domain: &D) -> Result<WorkBatch<D::Chain>, DomainError> {
    let mut batch = WorkBatch::default();

    let chain = domain.chain();

    while let Some(work) = chain.peek_work() {
        if !matches!(work, WorkKind::Block) {
            break;
        }

        match chain.pop_work().unwrap() {
            WorkUnit::Block(block) => {
                batch.add_work(block);
            }
            _ => unreachable!("can't pop work that isn't a block"),
        }
    }

    Ok(batch)
}

pub fn import_blocks<D: Domain>(
    domain: &D,
    mut raw: Vec<RawBlock>,
) -> Result<BlockSlot, DomainError> {
    for block in raw.drain(..) {
        domain.chain().receive_block(block)?;
    }

    let mut last = 0;

    while let Some(work) = domain.chain().peek_work() {
        if matches!(work, WorkKind::Block) {
            let batch = gather_batched_work(domain)?;
            last = import_batch(domain, batch)?;
        } else {
            let work = domain.chain().pop_work().unwrap();
            execute_isolated_work(domain, work)?;
        }
    }

    Ok(last)
}

pub fn roll_forward<D: Domain>(domain: &D, block: RawBlock) -> Result<BlockSlot, DomainError> {
    domain.chain().receive_block(block)?;

    let mut last = 0;

    while let Some(work) = domain.chain().peek_work() {
        if matches!(work, WorkKind::Block) {
            let batch = gather_batched_work(domain)?;
            last = roll_batch(domain, batch)?;
        } else {
            let work = domain.chain().pop_work().unwrap();
            execute_isolated_work(domain, work)?;
        }
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
