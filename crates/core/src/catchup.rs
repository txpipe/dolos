use crate::{
    batch::WorkBatch, BlockSlot, ChainLogic, Domain, DomainError, RawBlock, StateStore as _,
};

fn import_contiguous_batch<D, C>(domain: &D, batch: &mut WorkBatch<C>) -> Result<(), DomainError>
where
    C: ChainLogic,
    D: Domain<Chain = C, Entity = C::Entity>,
{
    batch.load_utxos(domain)?;

    batch.decode_utxos(domain.chain())?;

    batch.compute_delta(domain)?;

    // since we're are importing finalized blocks, we don't care about the potential
    // for undos. This allows us to just drop the mutated delta without having to
    // persist it in the WAL.

    batch.load_entities(domain)?;

    batch.apply_entities()?;

    batch.commit_utxo_set(domain)?;

    batch.commit_state(domain)?;

    batch.commit_archive(domain)?;

    Ok(())
}

fn import_decoded_batch<D, C>(
    domain: &D,
    batch: WorkBatch<D::Chain>,
) -> Result<BlockSlot, DomainError>
where
    C: ChainLogic,
    D: Domain<Chain = C, Entity = C::Entity>,
{
    let (mut before, after) = batch.split_by_sweep(domain.chain());

    if let Some((next_sweep, after)) = after {
        import_contiguous_batch(domain, &mut before)?;
        domain.chain().execute_sweep(domain, next_sweep)?;
        import_decoded_batch(domain, after)
    } else {
        import_contiguous_batch(domain, &mut before)?;
        Ok(before.last_slot())
    }
}

pub fn import_batch<D, C>(domain: &D, batch: Vec<RawBlock>) -> Result<BlockSlot, DomainError>
where
    C: ChainLogic,
    D: Domain<Chain = C, Entity = C::Entity>,
{
    let mut batch = WorkBatch::from_raw_batch(batch);

    batch.decode_blocks(domain.chain())?;

    let last = import_decoded_batch(domain, batch)?;

    Ok(last)
}

pub fn apply_origin<D>(domain: &D) -> Result<(), DomainError>
where
    D: Domain,
{
    let delta = domain.chain().compute_origin_utxo_delta(domain.genesis())?;

    domain.state().apply(&[delta])?;

    Ok(())
}
