use dolos_core::{ChainError, EntityKey, NsKey};
use pallas::ledger::primitives::{
    conway::{DRep, GovAction},
    ExUnitPrices, RationalNumber,
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    sweep::{hacks, AccountId, BoundaryWork, PoolId, ProposalId},
    AccountState, CardanoDelta, CardanoEntity, FixedNamespace as _, PParamValue, PoolState,
    Proposal,
};

fn should_enact_proposal(ctx: &mut BoundaryWork, proposal: &Proposal) -> bool {
    if let Some(epoch) = match ctx.network_magic {
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
        epoch as u64 == ctx.starting_epoch_no()
    } else {
        false
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountTransition {
    account: AccountId,

    // undo
    prev_pool: Option<Vec<u8>>,
    prev_drep: Option<DRep>,
    prev_stake: Option<u64>,
}

impl AccountTransition {
    pub fn new(account: AccountId) -> Self {
        Self {
            account,
            prev_pool: None,
            prev_drep: None,
            prev_stake: None,
        }
    }
}

impl dolos_core::EntityDelta for AccountTransition {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.account.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            return;
        };

        // undo info
        self.prev_pool = entity.latest_pool.clone();
        self.prev_drep = entity.latest_drep.clone();
        self.prev_stake = Some(entity.active_stake);

        // apply changes
        entity.active_pool = entity.latest_pool.clone();
        entity.active_drep = entity.latest_drep.clone();
        entity.active_stake = entity.wait_stake;
        entity.wait_stake = entity.live_stake();
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let Some(entity) = entity else {
            return;
        };

        entity.latest_drep = entity.active_drep.clone();
        entity.latest_pool = entity.active_pool.clone();
        entity.wait_stake = entity.active_stake;

        entity.active_pool = self.prev_pool.clone();
        entity.active_drep = self.prev_drep.clone();
        entity.active_stake = self.prev_stake.unwrap_or(0);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolTransition {
    pool: PoolId,
    ending_stake: u64,

    // undo
    prev_stake: Option<u64>,
    prev_blocks_minted: Option<u32>,
}

impl PoolTransition {
    pub fn new(pool: PoolId, ending_stake: u64) -> Self {
        Self {
            pool,
            ending_stake,
            prev_stake: None,
            prev_blocks_minted: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolTransition {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.pool.clone()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        let Some(entity) = entity else {
            return;
        };

        // undo info
        self.prev_stake = Some(entity.active_stake);
        self.prev_blocks_minted = Some(entity.blocks_minted_epoch);

        // order matters
        entity.active_stake = entity.wait_stake;
        entity.wait_stake = self.ending_stake;

        entity.blocks_minted_epoch = 0;
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        let Some(entity) = entity else {
            return;
        };

        entity.wait_stake = entity.active_stake;
        entity.active_stake = self.prev_stake.unwrap_or(0);

        entity.blocks_minted_epoch = self.prev_blocks_minted.unwrap_or(0);
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

macro_rules! handle_update {
    ($update:expr, $getter:ident, $ctx:expr, $variant:ident) => {
        if let Some(updated) = $update.$getter.as_ref() {
            #[allow(irrefutable_let_patterns)]
            if let Ok(converted) = updated.clone().try_into() {
                debug!(
                    variant = stringify!($variant),
                    value =? converted,
                    "applying new pparam value on ending state"
                );
                $ctx.ending_state
                    .pparams
                    .set(PParamValue::$variant(converted))
            }
        }
    };
}

macro_rules! check_all {
    ($update:expr, $deltas:expr, $($getter:ident => $variant:ident),*) => {
        $(
            handle_update!($update, $getter, $deltas, $variant);
        )*
    };
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
    fn visit_pool(
        &mut self,
        ctx: &mut BoundaryWork,
        id: &PoolId,
        _: &PoolState,
    ) -> Result<(), ChainError> {
        let ending_stake = ctx.active_snapshot.get_pool_stake(id);

        self.change(PoolTransition::new(id.clone(), ending_stake));

        Ok(())
    }

    fn visit_account(
        &mut self,
        _: &mut BoundaryWork,
        id: &AccountId,
        _: &AccountState,
    ) -> Result<(), ChainError> {
        self.change(AccountTransition::new(id.clone()));

        Ok(())
    }

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
                debug!(
                    version =? version,
                    "applying proposed hardfork on ending state"
                );
                ctx.ending_state
                    .pparams
                    .set(PParamValue::ProtocolVersion(*version));
            }
            GovAction::ParameterChange(_, update, _) => {
                check_all! {
                    update,
                    ctx,

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

                // Special cases that must be converted by hand:
                if let Some(updated) = update.max_tx_ex_units {
                    debug!(
                        variant = "max_tx_ex_units",
                        value =? updated,
                        "applying new pparam value on ending state"
                    );
                    ctx.ending_state.pparams.set(PParamValue::MaxTxExUnits(
                        pallas::ledger::primitives::ExUnits {
                            mem: updated.mem,
                            steps: updated.steps,
                        },
                    ))
                }
                if let Some(updated) = update.max_block_ex_units {
                    debug!(
                        variant = "max_block_ex_units",
                        value =? updated,
                        "applying new pparam value on ending state"
                    );
                    ctx.ending_state.pparams.set(PParamValue::MaxBlockExUnits(
                        pallas::ledger::primitives::ExUnits {
                            mem: updated.mem,
                            steps: updated.steps,
                        },
                    ))
                }
                if let Some(updated) = update.minfee_refscript_cost_per_byte.as_ref() {
                    debug!(
                        variant = "minfee_refscript_cost_per_byte",
                        value =? updated,
                        "applying new pparam value on ending state"
                    );
                    ctx.ending_state
                        .pparams
                        .set(PParamValue::MinFeeRefScriptCostPerByte(RationalNumber {
                            numerator: updated.numerator,
                            denominator: updated.denominator,
                        }))
                }
                if let Some(updated) = update.execution_costs.as_ref() {
                    debug!(
                        variant = "execution_costs",
                        value =? updated,
                        "applying new pparam value on ending state"
                    );
                    ctx.ending_state
                        .pparams
                        .set(PParamValue::ExecutionCosts(ExUnitPrices {
                            mem_price: updated.mem_price.clone(),
                            step_price: updated.step_price.clone(),
                        }));
                }

                if let Some(updated) = update.cost_models_for_script_languages.as_ref() {
                    debug!(
                        variant = "cost_models",
                        value =? updated,
                        "applying new pparam value on ending state"
                    );

                    if let Some(v1) = updated.plutus_v1.as_ref() {
                        ctx.ending_state
                            .pparams
                            .set(PParamValue::CostModelsPlutusV1(v1.clone()));
                    }
                    if let Some(v2) = updated.plutus_v2.as_ref() {
                        ctx.ending_state
                            .pparams
                            .set(PParamValue::CostModelsPlutusV2(v2.clone()));
                    }
                    if let Some(v3) = updated.plutus_v3.as_ref() {
                        ctx.ending_state
                            .pparams
                            .set(PParamValue::CostModelsPlutusV3(v3.clone()));
                    }
                    if !updated.unknown.is_empty() {
                        ctx.ending_state
                            .pparams
                            .set(PParamValue::CostModelsUnknonwn(updated.unknown.clone()));
                    }
                }
            }
            GovAction::TreasuryWithdrawals(_, _) => {
                // TODO: Track of this withdrawal from treasury, updating reward account as well
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
