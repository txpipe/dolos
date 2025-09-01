use std::{
    collections::{HashMap, HashSet},
    marker::PhantomData,
    ops::{Range, RangeInclusive},
    sync::Arc,
};

use itertools::Itertools;

use crate::{
    Block, BlockBody, BlockSlot, ChainLogic, Domain, DomainError, EraCbor, RawUtxoMap, StateDelta,
    StateStore as _, TxoRef, UtxoSetDelta,
};

struct DecodedBatch<B: Block> {
    batch: Vec<B>,
}

impl<B: Block> DecodedBatch<B> {
    fn new(mut batch: Vec<B>) -> Self {
        assert!(batch.len() > 0);
        batch.sort_by_key(|x| x.slot());
        Self { batch }
    }

    fn range(&self) -> RangeInclusive<BlockSlot> {
        let start = self.batch.first().unwrap().slot();
        let end = self.batch.last().unwrap().slot();
        start..=end
    }

    fn split_at(mut self, slot: BlockSlot) -> (Self, Self) {
        let mut before = Vec::new();
        let mut after = Vec::new();

        for block in self.batch.drain(..) {
            if block.slot() <= slot {
                before.push(block);
            } else {
                after.push(block);
            }
        }

        (Self { batch: before }, Self { batch: after })
    }
}

impl<B: Block> std::ops::Deref for DecodedBatch<B> {
    type Target = Vec<B>;

    fn deref(&self) -> &Self::Target {
        &self.batch
    }
}

pub fn decode_batch<D, B>(domain: &D, batch: &RawBlockBatch) -> Result<DecodedBatch<B>, DomainError>
where
    D: Domain,
    B: Block,
    D::Chain: ChainLogic<Block = B>,
{
    // TODO: paralelize in chunks

    let blocks: Vec<_> = batch
        .iter()
        .map(|block| domain.chain().decode_block(block.clone()))
        .collect::<Result<_, _>>()?;

    Ok(DecodedBatch::new(blocks))
}

pub fn resolve_batch<D, B>(domain: &D, batch: &DecodedBatch<B>) -> Result<RawUtxoMap, DomainError>
where
    D: Domain,
    B: Block,
    D::Chain: ChainLogic<Block = B>,
{
    // TODO: paralelize in chunks

    let mut loaded_deps = HashMap::new();

    let all_refs: Vec<_> = batch
        .iter()
        .flat_map(|x| x.depends_on(&mut loaded_deps))
        .unique()
        .collect();

    let inputs: HashMap<_, _> = domain.state().get_utxos(all_refs)?.into_iter().collect();

    loaded_deps.extend(inputs);

    Ok(loaded_deps)
}

pub fn decode_utxos_for_batch<D, U>(
    domain: &D,
    batch: &RawUtxoMap,
) -> Result<HashMap<TxoRef, U>, DomainError>
where
    D: Domain,
    D::Chain: ChainLogic<Utxo = U>,
    U: Sized,
{
    let utxos = batch
        .iter()
        .map(|(k, v)| {
            domain
                .chain()
                .decode_utxo(v.clone())
                .map(|x| (k.clone(), x))
        })
        .collect::<Result<_, _>>()?;

    Ok(utxos)
}

use rayon::prelude::*;

pub fn compute_batch_deltas<D, B, U>(
    domain: &D,
    batch: &[B],
    inputs: &HashMap<TxoRef, U>,
) -> Result<StateDelta<D::EntityDelta>, DomainError>
where
    D: Domain,
    D::Chain: ChainLogic<Block = B, Utxo = U>,
{
    let deltas: Vec<_> = batch
        .iter()
        .map(|block| domain.chain().compute_block_delta(block, &inputs))
        .collect::<Result<_, _>>()?;

    let folded = deltas
        .into_iter()
        .fold(StateDelta::default(), |mut acc, x| {
            acc += x;
            acc
        });

    Ok(folded)
}

pub fn update_utxo_set_batch<D, B, U>(
    domain: &D,
    batch: &[B],
    inputs: &RawUtxoMap,
) -> Result<(), DomainError>
where
    D: Domain,
    D::Chain: ChainLogic<Block = B, Utxo = U>,
{
    let deltas: Vec<_> = batch
        .iter()
        .map(|b| domain.chain().compute_block_utxo_delta(&b, &inputs))
        .collect::<Result<_, _>>()?;

    domain.state().apply(&deltas)?;

    Ok(())
}

pub type RawBlock = Arc<BlockBody>;
pub type RawBlockBatch = Vec<RawBlock>;

fn import_contiguous_batch<D, B>(
    domain: &D,
    batch: DecodedBatch<B>,
) -> Result<BlockSlot, DomainError>
where
    D: Domain,
    D::Chain: ChainLogic<Block = B>,
    B: Block,
{
    let inputs = resolve_batch(domain, &batch)?;

    // since we don't yet have a folding implemented for utxo set deltas, we'll just
    // update the utxo set directly for each block.
    update_utxo_set_batch(domain, &batch, &inputs)?;

    let inputs = decode_utxos_for_batch(domain, &inputs)?;

    let mut state_delta = compute_batch_deltas(domain, &batch, &inputs)?;

    super::state::apply_batch::<D>(domain.state3(), &mut state_delta)?;

    // since we're are importing finalized blocks, we don't care about the potential
    // for undos. This allows us to just drop the mutated delta and forget about it.

    Ok(state_delta.new_cursor)
}

pub fn import_decoded_batch<D, B>(
    domain: &D,
    batch: DecodedBatch<B>,
) -> Result<BlockSlot, DomainError>
where
    D: Domain,
    B: Block,
    D::Chain: ChainLogic<Block = B>,
{
    assert!(batch.len() > 0);

    let range = batch.range();
    let next_sweep = domain.chain().next_sweep(*range.start());

    if !range.contains(&next_sweep) {
        import_contiguous_batch(domain, batch)?;
    } else {
        let (before, after) = batch.split_at(next_sweep);

        import_contiguous_batch(domain, before)?;

        domain.chain().execute_sweep(domain, next_sweep)?;

        import_decoded_batch(domain, after)?;
    }

    Ok(next_sweep)
}

pub fn import_batch<D>(domain: &D, batch: RawBlockBatch) -> Result<BlockSlot, DomainError>
where
    D: Domain,
{
    assert!(batch.len() > 0);

    let batch = decode_batch(domain, &batch)?;

    import_decoded_batch(domain, batch)
}

pub fn apply_origin<D>(domain: &D) -> Result<(), DomainError>
where
    D: Domain,
{
    let delta = domain.chain().compute_origin_utxo_delta(domain.genesis())?;

    domain.state().apply(&[delta])?;

    Ok(())
}

pub fn apply_block<D, B>(domain: &D, block: RawBlock) -> Result<BlockSlot, DomainError>
where
    D: Domain,
    B: Block,
    D::Chain: ChainLogic<Block = B>,
{
    let block = domain.chain().decode_block(block.clone())?;

    let batch = DecodedBatch::new(vec![block]);

    let inputs = resolve_batch(domain, &batch)?;

    // since we don't yet have a folding implemented for utxo set deltas, we'll just
    // update the utxo set directly for each block.
    update_utxo_set_batch(domain, &batch, &inputs)?;

    let inputs = decode_utxos_for_batch(domain, &inputs)?;

    let mut state_delta = compute_batch_deltas(domain, &batch, &inputs)?;

    super::state::apply_batch::<D>(domain.state3(), &mut state_delta)?;

    // TODO: save updated deltas for potential undo during a rollback

    Ok(state_delta.new_cursor)
}
