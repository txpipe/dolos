use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::{codec::minicbor, ledger::primitives::StakeCredential};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    add,
    ewrap::{BoundaryWork, ProposalId},
    sub, AccountState, CardanoDelta, CardanoEntity, EpochState, FixedNamespace as _, PParamValue,
    PParamsSet, ProposalAction, ProposalState, CURRENT_EPOCH_KEY,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PParamsUpdate {
    to_update: PParamsSet,
}

impl PParamsUpdate {
    pub fn new(to_update: PParamsSet) -> Self {
        Self { to_update }
    }
}

impl dolos_core::EntityDelta for PParamsUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.as_mut().expect("epoch state missing");

        debug!(value = ?self.to_update, "applying pparam update");

        let next = entity.pparams.scheduled_or_default();

        next.merge(self.to_update.clone());
    }

    fn undo(&self, _entity: &mut Option<EpochState>) {
        // todo!()
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreasuryWithdrawal {
    account: StakeCredential,
    amount: u64,
}

impl TreasuryWithdrawal {
    pub fn new(account: StakeCredential, amount: u64) -> Self {
        Self { account, amount }
    }
}

impl dolos_core::EntityDelta for TreasuryWithdrawal {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.account).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        debug!(account=?self.account, amount=%self.amount, "applying treasury withdrawal");

        let stake = entity.stake.unwrap_live_mut();
        stake.rewards_sum = add!(stake.rewards_sum, self.amount);
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        debug!(account=?self.account, amount=%self.amount, "undoing treasury withdrawal");

        let stake = entity.stake.unwrap_live_mut();
        stake.rewards_sum = sub!(stake.rewards_sum, self.amount);
    }
}

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
