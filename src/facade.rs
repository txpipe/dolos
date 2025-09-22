use std::{collections::HashMap, sync::Arc};

use dolos_core::{
    batch::WorkBatch, ArchiveStore as _, BlockSlot, ChainLogic, ChainPoint, Domain, DomainError,
    EntityDelta as _, RawBlock, StateStore, StateWriter as _, TipEvent, WalStore,
};
use tracing::info;

pub trait DomainExt: Domain
where
    Self::Chain: ChainLogic<Delta = Self::EntityDelta, Entity = Self::Entity>,
{
    fn import_contiguous_batch(
        &self,
        batch: &mut WorkBatch<Self::Chain>,
    ) -> Result<(), DomainError> {
        batch.load_utxos(self)?;

        batch.decode_utxos(self.chain())?;

        batch.compute_delta(self)?;

        // since we're are importing finalized blocks, we don't care about the potential
        // for undos. This allows us to just drop the mutated delta without having to
        // persist it in the WAL.

        batch.load_entities(self)?;

        batch.apply_entities()?;

        batch.commit_state(self)?;

        batch.commit_archive(self)?;

        Ok(())
    }

    fn import_decoded_batch(&self, batch: WorkBatch<Self::Chain>) -> Result<BlockSlot, DomainError>
    where
        Self::Chain: ChainLogic<Delta = Self::EntityDelta, Entity = Self::Entity>,
    {
        let (mut before, after) = batch.split_by_sweep(self)?;

        if let Some((next_sweep, after)) = after {
            self.import_contiguous_batch(&mut before)?;
            self.chain().execute_sweep(self, next_sweep)?;
            self.import_decoded_batch(after)
        } else {
            self.import_contiguous_batch(&mut before)?;
            Ok(before.last_slot())
        }
    }

    fn import_batch(&self, batch: Vec<RawBlock>) -> Result<BlockSlot, DomainError> {
        let mut batch = WorkBatch::from_raw_batch(batch);

        batch.decode_blocks(self.chain())?;

        let last = self.import_decoded_batch(batch)?;

        Ok(last)
    }

    fn roll_forward(&self, block: &RawBlock) -> Result<(), DomainError> {
        let mut batch = WorkBatch::from_raw_batch(vec![block.clone()]);

        batch.decode_blocks(self.chain())?;

        batch.load_utxos(self)?;

        batch.decode_utxos(self.chain())?;

        batch.compute_delta(self)?;

        batch.commit_wal(self)?;

        batch.load_entities(self)?;

        batch.apply_entities()?;

        batch.commit_state(self)?;

        batch.commit_archive(self)?;

        for (point, block) in batch.iter_raw() {
            self.notify_tip(TipEvent::Apply(point.clone(), block.clone()));
            info!(%point, "roll forward");
        }

        Ok(())
    }

    fn rollback(&self, to: &ChainPoint) -> Result<(), DomainError> {
        let undo_blocks = self.wal().iter_logs(Some(to.clone()), None)?;

        let writer = self.state().start_writer()?;

        for (point, mut log) in undo_blocks.rev() {
            if point == *to {
                writer.set_cursor(point.clone())?;
                break;
            }

            let entities = log
                .delta
                .iter()
                .map(|delta| delta.key())
                .collect::<Vec<_>>();

            let mut entities =
                dolos_core::state::load_entity_chunk::<Self>(entities.as_slice(), self.state())?;

            for (key, entity) in entities.iter_mut() {
                for delta in log.delta.iter_mut() {
                    if delta.key() == *key {
                        delta.undo(entity);
                    }
                }
            }

            let block = Arc::new(log.block);

            let blockd = dolos_cardano::owned::OwnedMultiEraBlock::decode(block.clone())
                .map_err(dolos_core::ChainError::from)?;
            let blockd = blockd.view();

            let inputs: HashMap<_, _> = log
                .inputs
                .iter()
                .map(|(k, v)| {
                    let out = (
                        k.clone(),
                        dolos_cardano::owned::OwnedMultiEraOutput::decode(v.clone())
                            .map_err(dolos_core::ChainError::from)?,
                    );

                    Result::<_, dolos_core::ChainError>::Ok(out)
                })
                .collect::<Result<_, _>>()?;

            let utxo_undo = dolos_cardano::utxoset::compute_undo_delta(blockd, &inputs)
                .map_err(dolos_core::ChainError::from)?;

            writer.apply_utxoset(&utxo_undo)?;

            // TODO: we should differ notifications until the we commit the writers
            self.notify_tip(TipEvent::Undo(point.clone(), block));
            info!(%point, "block undone");
        }

        writer.commit()?;

        self.archive().truncate_front(to.slot())?;

        self.wal().truncate_front(to)?;

        Ok(())
    }
}

impl<D> DomainExt for D
where
    D: Domain,
    D::Chain: ChainLogic<Delta = D::EntityDelta, Entity = D::Entity>,
{
}
