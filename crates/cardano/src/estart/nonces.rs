use dolos_core::{BlockSlot, ChainError};

use crate::{sub, utils::nonce_stability_window, NonceTransition, Nonces};

fn next_largest_stable_slot(ctx: &super::WorkContext) -> BlockSlot {
    let stability_window = nonce_stability_window(ctx.active_protocol.into(), &ctx.genesis);
    let epoch_finish_slot = ctx.chain_summary.epoch_start(ctx.starting_epoch_no() + 1);

    sub!(epoch_finish_slot, stability_window)
}

fn initial_nonces(ctx: &super::WorkContext) -> Option<Nonces> {
    let era_transition = ctx.ended_state.pparams.era_transition()?;

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
