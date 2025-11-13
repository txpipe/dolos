use dolos_core::{ChainError, NsKey};
use pallas::{codec::minicbor, ledger::primitives::StakeCredential};
use serde::{Deserialize, Serialize};

use crate::{
    ewrap::{BoundaryWork, ProposalId},
    AccountState, CardanoDelta, FixedNamespace as _, PoolHash, PoolState, ProposalState,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDepositRefund {
    pool_deposit: u64,
    account: StakeCredential,
}

impl PoolDepositRefund {
    pub fn new(pool_deposit: u64, account: StakeCredential) -> Self {
        Self {
            pool_deposit,
            account,
        }
    }
}

impl dolos_core::EntityDelta for PoolDepositRefund {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.account).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        let stake = entity.stake.scheduled_or_default();

        stake.rewards_sum += self.pool_deposit;
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // todo!()
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalDepositRefund {
    proposal_deposit: u64,
    account: StakeCredential,
}

impl ProposalDepositRefund {
    pub fn new(proposal_deposit: u64, account: StakeCredential) -> Self {
        Self {
            proposal_deposit,
            account,
        }
    }
}

impl dolos_core::EntityDelta for ProposalDepositRefund {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.account).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        tracing::error!(cred=?self.account, deposit=%self.proposal_deposit, "applying proposal deposit refund");

        let stake = entity.stake.scheduled_or_default();

        stake.rewards_sum += self.proposal_deposit;
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // todo!()
        // Placeholder undo logic. Ensure this does not panic.
    }
}

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
        tracing::error!(proposal=%id, "visiting dropped proposal");

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
        _: PoolHash,
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
