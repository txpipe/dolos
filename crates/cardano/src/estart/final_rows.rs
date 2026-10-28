//! The rows ESTART moves out of state, to the final-row log
//! (`crate::final_rows`): entities no ledger rule reads again.
//!
//! A kind joins by giving its visit a rule and calling
//! [`super::WorkContext::move_to_log`]; the commit writes the log rows and the
//! state deletes for every kind alike.

use dolos_core::ChainError;

use crate::{
    estart::{PoolId, ProposalId},
    PoolState, ProposalState,
};

#[derive(Default)]
pub struct BoundaryVisitor;

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_pool(
        &mut self,
        ctx: &mut super::WorkContext,
        id: &PoolId,
        pool: &PoolState,
    ) -> Result<(), ChainError> {
        if pool.leaves_state() {
            ctx.move_to_log(id, pool);
        }

        Ok(())
    }

    fn visit_proposal(
        &mut self,
        ctx: &mut super::WorkContext,
        id: &ProposalId,
        proposal: &ProposalState,
    ) -> Result<(), ChainError> {
        if proposal.leaves_state_at(ctx.starting_epoch_no()) {
            ctx.move_to_log(id, proposal);
        }

        Ok(())
    }
}
