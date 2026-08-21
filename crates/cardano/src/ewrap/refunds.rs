use dolos_core::ChainError;

use crate::{
    ewrap::{BoundaryWork, ProposalId},
    AccountState, CardanoDelta, PoolDepositRefund, PoolHash, PoolState, ProposalDepositRefund,
    ProposalState,
};

#[derive(Default)]
pub struct BoundaryVisitor {
    deltas: Vec<CardanoDelta>,
}

impl BoundaryVisitor {
    fn change(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.push(delta.into());
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_dropping_proposal(
        &mut self,
        _: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        tracing::debug!(proposal=%id, "visiting dropped proposal");

        if let Some(deposit) = proposal.deposit {
            if let Some(account) = account {
                if account.is_registered() {
                    self.change(ProposalDepositRefund::new(
                        deposit,
                        account.credential.clone(),
                    ));
                }
            }
        }

        Ok(())
    }

    fn visit_enacting_proposal(
        &mut self,
        _: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &ProposalState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        tracing::debug!(proposal=%id, "visiting enacting proposal");

        if let Some(deposit) = proposal.deposit {
            if let Some(account) = account {
                if account.is_registered() {
                    self.change(ProposalDepositRefund::new(
                        deposit,
                        account.credential.clone(),
                    ));
                }
            }
        }

        Ok(())
    }

    fn visit_retiring_pool(
        &mut self,
        ctx: &mut super::BoundaryWork,
        // The refund's record now travels on the account's merged row, keyed
        // and indexed by `load_pool_data`; this visitor is left with the delta.
        _pool_id: PoolHash,
        _: &PoolState,
        account: Option<&AccountState>,
    ) -> Result<(), ChainError> {
        let deposit = ctx
            .ending_state()
            .pparams
            .unwrap_live()
            .ensure_pool_deposit()?;

        if let Some(account) = account {
            if account.is_registered() {
                self.change(PoolDepositRefund::new(deposit, account.credential.clone()));
            }
        }

        Ok(())
    }

    fn flush(&mut self, ctx: &mut BoundaryWork) -> Result<(), ChainError> {
        for delta in self.deltas.drain(..) {
            ctx.add_delta(delta);
        }

        Ok(())
    }
}
