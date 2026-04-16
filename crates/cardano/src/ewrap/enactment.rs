use dolos_core::{ChainError, EntityKey};

use crate::{
    ewrap::{BoundaryWork, ProposalId},
    AccountState, CardanoDelta, CardanoEntity, PParamValue, PParamsSet, PParamsUpdate,
    ProposalAction, ProposalState, TreasuryWithdrawal,
};

#[derive(Default)]
pub struct BoundaryVisitor {
    deltas: Vec<CardanoDelta>,
    logs: Vec<(EntityKey, CardanoEntity)>,
}

impl BoundaryVisitor {
    fn change(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.push(delta.into());
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_enacting_proposal(
        &mut self,
        _: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
        _: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        tracing::debug!(proposal=%id, "visiting enacted proposal");

        // Apply proposal on ending state
        match &proposal.action {
            ProposalAction::HardFork(version) => {
                let value = PParamValue::ProtocolVersion(*version);
                let pparams = PParamsSet::default().with(value);
                self.change(PParamsUpdate::new(pparams));
            }
            ProposalAction::ParamChange(pparams) => {
                self.change(PParamsUpdate::new(pparams.clone()));
            }
            ProposalAction::TreasuryWithdrawal(withdrawals) => {
                for (credential, amount) in withdrawals {
                    self.change(TreasuryWithdrawal::new(credential.clone(), *amount));
                }
            }
            x => {
                dbg!(x);
                tracing::error!(proposal=%id, "don't know how to enact proposal action: {:?}", x);
            }
        }

        Ok(())
    }

    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        for (key, log) in self.logs.drain(..) {
            ctx.logs.push((key, log));
        }

        Ok(())
    }
}
