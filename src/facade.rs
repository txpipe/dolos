use std::{collections::HashMap, sync::Arc};

use dolos_cardano::indexes::index_delta_from_utxo_delta;
use dolos_core::{
    ArchiveStore as _, ChainLogic, ChainPoint, Domain, DomainError, EntityDelta as _,
    IndexStore as _, IndexWriter as _, StateStore, StateWriter as _, TipEvent, WalStore,
};
use tracing::info;

pub trait DomainExt: Domain
where
    Self::Chain: ChainLogic<Delta = Self::EntityDelta, Entity = Self::Entity>,
{
    fn rollback(&self, to: &ChainPoint) -> Result<(), DomainError> {
        let undo_blocks = self.wal().iter_logs(Some(to.clone()), None)?;

        let writer = self.state().start_writer()?;
        let index_writer = self.indexes().start_writer()?;

        for (point, mut log) in undo_blocks.rev() {
            if point == *to {
                // Final cursor update - build an empty delta with just the cursor
                let empty_delta = dolos_core::IndexDelta {
                    cursor: point.clone(),
                    ..Default::default()
                };
                index_writer.apply(&empty_delta)?;
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

            // Apply UTxO undo to state
            writer.apply_utxoset(&utxo_undo)?;

            // Build and apply index delta for the undo
            let index_delta = index_delta_from_utxo_delta(point.clone(), &utxo_undo);
            index_writer.undo(&index_delta)?;

            // TODO: we should differ notifications until the we commit the writers
            self.notify_tip(TipEvent::Undo(point.clone(), block));
            info!(%point, "block undone");
        }

        writer.commit()?;
        index_writer.commit()?;

        self.archive().truncate_front(to)?;

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
