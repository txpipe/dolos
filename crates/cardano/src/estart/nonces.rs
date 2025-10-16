use dolos_core::{BlockSlot, ChainError, EntityDelta, NsKey};
use serde::{Deserialize, Serialize};

use crate::{
    utils::nonce_stability_window, EpochState, FixedNamespace as _, Nonces, CURRENT_EPOCH_KEY,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NonceTransition {
    next_nonce: Option<Nonces>,
    next_slot: BlockSlot,
}

impl NonceTransition {}

impl EntityDelta for NonceTransition {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing epoch");

        entity.previous_nonce_tail = entity.nonces.as_ref().and_then(|n| n.tail);
        entity.nonces = self.next_nonce.clone();
        entity.largest_stable_slot = self.next_slot;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        todo!()
    }
}

fn next_largest_stable_slot(ctx: &super::WorkContext) -> BlockSlot {
    let stability_window = nonce_stability_window(ctx.active_protocol.into(), &ctx.genesis);
    let epoch_finish_slot = ctx.active_era.epoch_start(ctx.starting_epoch_no() + 1);

    let largest_stable_slot = epoch_finish_slot - stability_window;

    largest_stable_slot
}

fn initial_nonces(ctx: &super::WorkContext) -> Option<Nonces> {
    let Some(era_transition) = ctx.ended_state.era_transition() else {
        return None;
    };

    if era_transition.entering_shelley() {
        Some(Nonces::bootstrap(ctx.genesis.shelley_hash))
    } else {
        None
    }
}

fn next_nonce(ctx: &super::WorkContext) -> Option<Nonces> {
    let Some(current) = &ctx.ended_state.nonces else {
        return initial_nonces(ctx);
    };

    let tail = ctx.ended_state.previous_nonce_tail;

    Some(current.sweep(tail, None))
}

#[derive(Default)]
pub struct BoundaryVisitor;

impl super::BoundaryVisitor for BoundaryVisitor {
    fn flush(&mut self, ctx: &mut super::WorkContext) -> Result<(), ChainError> {
        let next_slot = next_largest_stable_slot(ctx);
        let next_nonce = next_nonce(ctx);

        ctx.deltas.add_for_entity(NonceTransition {
            next_nonce,
            next_slot,
        });

        Ok(())
    }
}
