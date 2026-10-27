//! The rows ESTART moves out of state, to the final-row log
//! (`crate::final_rows`): entities no ledger rule reads again.
//!
//! A kind joins by giving its visit a rule and calling
//! [`super::WorkContext::move_to_log`]; the commit writes the log rows and the
//! state deletes for every kind alike.

use dolos_core::{BlockSlot, ChainError};

use crate::{
    estart::{DRepId, ProposalId},
    DRepState, ProposalState,
};

#[derive(Default)]
pub struct BoundaryVisitor {
    /// The DReps that leave state unless a live proposal predates their
    /// unregistration. The DReps are visited before the proposals, so the
    /// rule is decided in `flush`.
    dreps: Vec<(DRepId, DRepState)>,

    /// The earliest submission among the proposals the next boundary can
    /// still ratify.
    oldest_live: Option<BlockSlot>,
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_drep(
        &mut self,
        ctx: &mut super::WorkContext,
        id: &DRepId,
        drep: &DRepState,
    ) -> Result<(), ChainError> {
        if drep.leaves_state_at(ctx.final_row_slot(), None) {
            self.dreps.push((id.clone(), drep.clone()));
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

        if proposal.is_unresolved_at_close(ctx.starting_epoch_no()) {
            self.oldest_live = Some(
                self.oldest_live
                    .map_or(proposal.slot, |oldest| oldest.min(proposal.slot)),
            );
        }

        Ok(())
    }

    fn flush(&mut self, ctx: &mut super::WorkContext) -> Result<(), ChainError> {
        let closing_start = ctx.final_row_slot();

        for (id, drep) in std::mem::take(&mut self.dreps) {
            if drep.leaves_state_at(closing_start, self.oldest_live) {
                ctx.move_to_log(&id, &drep);
            }
        }

        Ok(())
    }
}
