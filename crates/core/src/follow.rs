use tracing::info;

use crate::{
    batch::WorkBatch, Block, BlockSlot, ChainLogic, ChainPoint, Domain, DomainError, RawBlock,
    TipEvent,
};

pub fn roll_forward<D, C>(domain: &D, block: &RawBlock) -> Result<(), DomainError>
where
    C: ChainLogic,
    D: Domain<Chain = C, Entity = C::Entity, EntityDelta = C::Delta>,
{
    let mut batch = WorkBatch::from_raw_batch(vec![block.clone()]);

    batch.decode_blocks(domain.chain())?;

    batch.load_utxos(domain)?;

    batch.decode_utxos(domain.chain())?;

    batch.compute_delta(domain)?;

    batch.commit_wal(domain)?;

    batch.load_entities(domain)?;

    batch.apply_entities()?;

    batch.commit_utxo_set(domain)?;

    batch.commit_state(domain)?;

    batch.commit_archive(domain)?;

    for (point, block) in batch.iter_raw() {
        domain.notify_tip(TipEvent::Apply(point.clone(), block.clone()));
    }

    Ok(())
}

pub fn rollback<D, B>(domain: &D, point: &ChainPoint) -> Result<BlockSlot, DomainError>
where
    D: Domain,
    B: Block,
    D::Chain: ChainLogic<Block = B, Delta = D::EntityDelta>,
{
    //super::state::undo_batch::<D>(domain.state3(), &delta)?;
    //todo!()

    Ok(point.slot())
}
