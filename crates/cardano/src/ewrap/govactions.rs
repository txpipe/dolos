use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::ledger::primitives::{
    conway::{GovAction, ProtocolParamUpdate},
    ExUnitPrices, RationalNumber,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    ewrap::{BoundaryWork, ProposalId},
    hacks, CardanoDelta, CardanoEntity, FixedNamespace as _, PParamValue, Proposal,
};

fn should_enact_proposal(ctx: &mut BoundaryWork, proposal: &Proposal) -> bool {
    if let Some(epoch) = match ctx.genesis.shelley.network_magic {
        Some(764824073) => {
            hacks::proposals::mainnet::enactment_epoch_by_proposal_id(&proposal.id_as_string())
        }
        Some(1) => {
            hacks::proposals::preprod::enactment_epoch_by_proposal_id(&proposal.id_as_string())
        }
        Some(2) => {
            hacks::proposals::preview::enactment_epoch_by_proposal_id(&proposal.id_as_string())
        }
        _ => None,
    } {
        epoch == ctx.starting_epoch_no()
    } else {
        false
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalEnactment {
    id: ProposalId,
    epoch: u64,
}

impl ProposalEnactment {
    pub fn new(id: ProposalId, epoch: u64) -> Self {
        Self { id, epoch }
    }
}

impl dolos_core::EntityDelta for ProposalEnactment {
    type Entity = Proposal;

    fn key(&self) -> NsKey {
        NsKey::from((Proposal::NS, self.id.clone()))
    }

    fn apply(&mut self, entity: &mut Option<Proposal>) {
        let Some(proposal) = entity else {
            return;
        };

        debug!(proposal=%self.id, "enacting proposal");
        proposal.enacted_epoch = Some(self.epoch);
        proposal.ratified_epoch = Some(self.epoch - 1);
    }

    fn undo(&self, entity: &mut Option<Proposal>) {
        let Some(entity) = entity else {
            return;
        };

        debug!(proposal=%self.id, "undoing enact proposal");
        entity.enacted_epoch = None;
        entity.ratified_epoch = None;
    }
}

pub type PParamsUpdate = crate::roll::epochs::PParamsUpdate;

#[derive(Default)]
pub struct BoundaryVisitor {
    deltas: Vec<CardanoDelta>,
    logs: Vec<(EntityKey, CardanoEntity)>,
}

macro_rules! pparams_update {
    ($update:expr, $getter:ident, $self:expr, $variant:ident) => {
        let value = $update.$getter.clone();
        if let Some(value) = value {
            let value = value.try_into().expect("pparam value doesn't fit");
            $self.change(PParamsUpdate::new(PParamValue::$variant(value)));
        }
    };
}

macro_rules! check_all_updates {
    ($update:expr, $self:expr, $($getter:ident => $variant:ident),*) => {
        $(
            pparams_update!($update, $getter, $self, $variant);
        )*
    };
}

impl BoundaryVisitor {
    fn change(&mut self, delta: impl Into<CardanoDelta>) {
        self.deltas.push(delta.into());
    }

    fn visit_param_change(&mut self, update: &ProtocolParamUpdate) -> Result<(), ChainError> {
        check_all_updates! {
            update,
            self,

            minfee_a => MinFeeA,
            minfee_b => MinFeeB,
            max_block_body_size => MaxBlockBodySize,
            max_transaction_size => MaxTransactionSize,
            max_block_header_size => MaxBlockHeaderSize,
            key_deposit => KeyDeposit,
            pool_deposit => PoolDeposit,
            desired_number_of_stake_pools => DesiredNumberOfStakePools,
            ada_per_utxo_byte => MinUtxoValue,
            min_pool_cost => MinPoolCost,
            expansion_rate => ExpansionRate,
            treasury_growth_rate => TreasuryGrowthRate,
            maximum_epoch => MaximumEpoch,
            pool_pledge_influence => PoolPledgeInfluence,
            ada_per_utxo_byte => AdaPerUtxoByte,
            max_value_size => MaxValueSize,
            collateral_percentage => CollateralPercentage,
            max_collateral_inputs => MaxCollateralInputs,
            pool_voting_thresholds => PoolVotingThresholds,
            drep_voting_thresholds => DrepVotingThresholds,
            min_committee_size => MinCommitteeSize,
            committee_term_limit => CommitteeTermLimit,
            governance_action_validity_period => GovernanceActionValidityPeriod,
            governance_action_deposit => GovernanceActionDeposit,
            drep_deposit => DrepDeposit,
            drep_inactivity_period => DrepInactivityPeriod
        };

        // TODO: these are special cases where we don't have automatic type mappings. We
        // should fix this at the Pallas level.

        if let Some(updated) = update.max_tx_ex_units {
            let value = PParamValue::MaxTxExUnits(pallas::ledger::primitives::ExUnits {
                mem: updated.mem,
                steps: updated.steps,
            });

            self.change(PParamsUpdate::new(value));
        }
        if let Some(updated) = update.max_block_ex_units {
            let value = PParamValue::MaxBlockExUnits(pallas::ledger::primitives::ExUnits {
                mem: updated.mem,
                steps: updated.steps,
            });

            self.change(PParamsUpdate::new(value));
        }

        if let Some(updated) = update.minfee_refscript_cost_per_byte.as_ref() {
            let value = PParamValue::MinFeeRefScriptCostPerByte(RationalNumber {
                numerator: updated.numerator,
                denominator: updated.denominator,
            });

            self.change(PParamsUpdate::new(value));
        }

        if let Some(updated) = update.execution_costs.as_ref() {
            let value = PParamValue::ExecutionCosts(ExUnitPrices {
                mem_price: updated.mem_price.clone(),
                step_price: updated.step_price.clone(),
            });

            self.change(PParamsUpdate::new(value));
        }

        if let Some(updated) = update.cost_models_for_script_languages.as_ref() {
            if let Some(v1) = updated.plutus_v1.as_ref() {
                let value = PParamValue::CostModelsPlutusV1(v1.clone());
                self.change(PParamsUpdate::new(value));
            }

            if let Some(v2) = updated.plutus_v2.as_ref() {
                let value = PParamValue::CostModelsPlutusV2(v2.clone());
                self.change(PParamsUpdate::new(value));
            }

            if let Some(v3) = updated.plutus_v3.as_ref() {
                let value = PParamValue::CostModelsPlutusV3(v3.clone());
                self.change(PParamsUpdate::new(value));
            }

            if !updated.unknown.is_empty() {
                let value = PParamValue::CostModelsUnknown(updated.unknown.clone());
                self.change(PParamsUpdate::new(value));
            }
        }

        Ok(())
    }
}

impl super::BoundaryVisitor for BoundaryVisitor {
    fn visit_proposal(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &ProposalId,
        proposal: &Proposal,
    ) -> Result<(), ChainError> {
        if !should_enact_proposal(ctx, proposal) {
            return Ok(());
        }

        self.deltas
            .push(ProposalEnactment::new(id.clone(), ctx.starting_epoch_no()).into());

        // Apply proposal on ending state
        match &proposal.proposal.gov_action {
            GovAction::HardForkInitiation(_, version) => {
                let value = PParamValue::ProtocolVersion(*version);
                self.change(PParamsUpdate::new(value));
            }
            GovAction::ParameterChange(_, update, _) => {
                // Special cases that must be converted by hand:
                self.visit_param_change(update)?;
            }
            GovAction::TreasuryWithdrawals(_, _) => {
                // TODO: Track of this withdrawal from treasury, updating reward
                // account as well
            }
            _ => {}
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
