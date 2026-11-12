use std::collections::{BTreeMap, HashMap};

use dolos_core::{ChainError, Genesis, TxOrder, TxoRef};
use pallas::{
    codec::utils::Bytes,
    ledger::{
        primitives::{
            conway::{GovAction, GovActionId, ProtocolParamUpdate},
            Epoch, ExUnitPrices, RationalNumber,
        },
        traverse::{MultiEraBlock, MultiEraTx, MultiEraUpdate},
    },
};

use super::WorkDeltas;
use crate::{
    owned::OwnedMultiEraOutput, pallas_extras, roll::BlockVisitor, GovPurpose, NewProposalV2,
    PParamValue, PParamsSet, ProposalAction, VoteCast,
};

macro_rules! map_conway_pparam {
    ($update:expr, $getter:ident, $set:expr, $variant:ident) => {
        let value = $update.$getter.clone();
        if let Some(value) = value {
            let value = value.try_into().expect("pparam value doesn't fit");
            $set.set(PParamValue::$variant(value));
        }
    };
}

macro_rules! check_conway_pparams {
    ($update:expr, $set:expr, $($getter:ident => $variant:ident),*) => {
        $(
            map_conway_pparam!($update, $getter, $set, $variant);
        )*
    };
}

fn parse_treasury_withdrawals(withdrawals: &BTreeMap<Bytes, u64>) -> ProposalAction {
    let mut items = vec![];

    for (credential, amount) in withdrawals {
        let credential = pallas_extras::parse_reward_account(credential)
            .expect("reward account should be valid");
        let amount = *amount;
        items.push((credential, amount));
    }

    ProposalAction::TreasuryWithdrawal(items)
}

fn conway_to_pparamset(update: &ProtocolParamUpdate) -> PParamsSet {
    let mut set = PParamsSet::default();

    check_conway_pparams! {
        update,
        set,

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

        set.set(value);
    }

    if let Some(updated) = update.max_block_ex_units {
        let value = PParamValue::MaxBlockExUnits(pallas::ledger::primitives::ExUnits {
            mem: updated.mem,
            steps: updated.steps,
        });

        set.set(value);
    }

    if let Some(updated) = update.minfee_refscript_cost_per_byte.as_ref() {
        let value = PParamValue::MinFeeRefScriptCostPerByte(RationalNumber {
            numerator: updated.numerator,
            denominator: updated.denominator,
        });

        set.set(value);
    }

    if let Some(updated) = update.execution_costs.as_ref() {
        let value = PParamValue::ExecutionCosts(ExUnitPrices {
            mem_price: updated.mem_price.clone(),
            step_price: updated.step_price.clone(),
        });

        set.set(value);
    }

    if let Some(updated) = update.cost_models_for_script_languages.as_ref() {
        if let Some(v1) = updated.plutus_v1.as_ref() {
            let value = PParamValue::CostModelsPlutusV1(v1.clone());
            set.set(value);
        }

        if let Some(v2) = updated.plutus_v2.as_ref() {
            let value = PParamValue::CostModelsPlutusV2(v2.clone());
            set.set(value);
        }

        if let Some(v3) = updated.plutus_v3.as_ref() {
            let value = PParamValue::CostModelsPlutusV3(v3.clone());
            set.set(value);
        }

        if !updated.unknown.is_empty() {
            let value = PParamValue::CostModelsUnknown(updated.unknown.clone());
            set.set(value);
        }
    }

    set
}

macro_rules! map_pre_conway_pparam {
    ($update:expr, $getter:ident, $set:expr, $variant:ident) => {
        let value = $update.$getter().clone();
        if let Some(value) = value.first().cloned() {
            let value = value.try_into().expect("pparam value doesn't fit");
            $set.set(PParamValue::$variant(value));
        }
    };
}

macro_rules! check_pre_conway_pparams {
    ($update:expr, $set:expr, $($getter:ident => $variant:ident),*) => {
        $(
            map_pre_conway_pparam!($update, $getter, $set, $variant);
        )*
    };
}

fn pre_conway_to_pparamset(update: &MultiEraUpdate) -> PParamsSet {
    let mut set = PParamsSet::default();

    check_pre_conway_pparams! {
        update,
        set,

        all_proposed_minfee_a => MinFeeA,
        all_proposed_minfee_b => MinFeeB,
        all_proposed_max_block_body_size => MaxBlockBodySize,
        all_proposed_max_transaction_size => MaxTransactionSize,
        all_proposed_max_block_header_size => MaxBlockHeaderSize,
        all_proposed_key_deposit => KeyDeposit,
        all_proposed_pool_deposit => PoolDeposit,
        all_proposed_desired_number_of_stake_pools => DesiredNumberOfStakePools,
        all_proposed_protocol_version => ProtocolVersion,
        all_proposed_ada_per_utxo_byte => MinUtxoValue,
        all_proposed_min_pool_cost => MinPoolCost,
        all_proposed_expansion_rate => ExpansionRate,
        all_proposed_treasury_growth_rate => TreasuryGrowthRate,
        all_proposed_maximum_epoch => MaximumEpoch,
        all_proposed_pool_pledge_influence => PoolPledgeInfluence,
        all_proposed_decentralization_constant => DecentralizationConstant,
        all_proposed_extra_entropy => ExtraEntropy,
        all_proposed_ada_per_utxo_byte => AdaPerUtxoByte,
        all_proposed_execution_costs => ExecutionCosts,
        all_proposed_max_tx_ex_units => MaxTxExUnits,
        all_proposed_max_block_ex_units => MaxBlockExUnits,
        all_proposed_max_value_size => MaxValueSize,
        all_proposed_collateral_percentage => CollateralPercentage,
        all_proposed_max_collateral_inputs => MaxCollateralInputs,
        all_proposed_pool_voting_thresholds => PoolVotingThresholds,
        all_proposed_drep_voting_thresholds => DrepVotingThresholds,
        all_proposed_min_committee_size => MinCommitteeSize,
        all_proposed_committee_term_limit => CommitteeTermLimit,
        all_proposed_governance_action_validity_period => GovernanceActionValidityPeriod,
        all_proposed_governance_action_deposit => GovernanceActionDeposit,
        all_proposed_drep_deposit => DrepDeposit,
        all_proposed_drep_inactivity_period => DrepInactivityPeriod,
        all_proposed_minfee_refscript_cost_per_byte => MinFeeRefScriptCostPerByte
    };

    if let Some((major, minor, _)) = update.byron_proposed_block_version() {
        set.set(PParamValue::ProtocolVersion((major.into(), minor.into())));
    }

    if let Some(cm) = update.alonzo_first_proposed_cost_models_for_script_languages() {
        if let Some(v1) = cm.get(&pallas::ledger::primitives::alonzo::Language::PlutusV1) {
            set.set(PParamValue::CostModelsPlutusV1(v1.clone()));
        }
    }

    if let Some(cm) = update.babbage_first_proposed_cost_models_for_script_languages() {
        if let Some(v1) = cm.plutus_v1 {
            set.set(PParamValue::CostModelsPlutusV1(v1));
        }
        if let Some(v2) = cm.plutus_v2 {
            set.set(PParamValue::CostModelsPlutusV2(v2));
        }
    }

    if let Some(cm) = update.conway_first_proposed_cost_models_for_script_languages() {
        if let Some(v1) = cm.plutus_v1 {
            set.set(PParamValue::CostModelsPlutusV1(v1));
        }

        if let Some(v2) = cm.plutus_v2 {
            set.set(PParamValue::CostModelsPlutusV2(v2));
        }

        if let Some(v3) = cm.plutus_v3 {
            set.set(PParamValue::CostModelsPlutusV3(v3));
        }

        if !cm.unknown.is_empty() {
            set.set(PParamValue::CostModelsUnknown(cm.unknown));
        }
    }

    set
}

/// Maps a Conway governance action to its dolos representation plus the
/// lineage data the action declares: the parent (previous governance action
/// id of the same purpose) and the purpose tree it belongs to.
/// TreasuryWithdrawals and Info have no lineage.
fn parse_gov_action(
    action: &GovAction,
) -> (ProposalAction, Option<GovActionId>, Option<GovPurpose>) {
    match action {
        GovAction::ParameterChange(parent, update, _) => (
            ProposalAction::ParamChange(conway_to_pparamset(update)),
            parent.clone(),
            Some(GovPurpose::PParamUpdate),
        ),
        GovAction::HardForkInitiation(parent, version) => (
            ProposalAction::HardFork(*version),
            parent.clone(),
            Some(GovPurpose::HardFork),
        ),
        GovAction::TreasuryWithdrawals(withdrawals, _) => {
            (parse_treasury_withdrawals(withdrawals), None, None)
        }
        GovAction::NoConfidence(parent) => (
            ProposalAction::NoConfidence,
            parent.clone(),
            Some(GovPurpose::Committee),
        ),
        GovAction::UpdateCommittee(parent, to_remove, to_add, threshold) => (
            ProposalAction::UpdateCommittee {
                to_remove: to_remove.iter().cloned().collect(),
                to_add: to_add
                    .iter()
                    .map(|(cred, epoch)| (cred.clone(), *epoch))
                    .collect(),
                threshold: threshold.clone(),
            },
            parent.clone(),
            Some(GovPurpose::Committee),
        ),
        GovAction::NewConstitution(parent, constitution) => (
            ProposalAction::NewConstitution {
                anchor: constitution.anchor.clone(),
                guardrail_script: constitution.guardrail_script,
            },
            parent.clone(),
            Some(GovPurpose::Constitution),
        ),
        GovAction::Information => (ProposalAction::Info, None, None),
    }
}

#[derive(Clone, Default)]
pub struct ProposalVisitor {
    validity_period: Option<u64>,
    current_epoch: Option<Epoch>,
    epoch_start: Option<u64>,
    network_magic: Option<u32>,
    protocol: Option<u16>,
    pending_votes: Vec<VoteCast>,

    /// Transactions of the block visited so far, invalid ones included, so
    /// the current one's index matches the crawl's `order`.
    txs_seen: TxOrder,
}

impl BlockVisitor for ProposalVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        genesis: &Genesis,
        pparams: &PParamsSet,
        epoch: Epoch,
        epoch_start: u64,
        protocol: u16,
    ) -> Result<(), ChainError> {
        self.validity_period = pparams.governance_action_validity_period();
        self.current_epoch = Some(epoch);
        self.epoch_start = Some(epoch_start);
        self.network_magic = Some(genesis.network_magic());
        self.protocol = Some(protocol);

        Ok(())
    }

    fn visit_tx(
        &mut self,
        _: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        _: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        let order = self.txs_seen;
        self.txs_seen += 1;

        let MultiEraTx::Conway(conway_tx) = tx else {
            return Ok(());
        };

        // Phase-2-invalid transactions contribute nothing to governance
        // state: CERTS / GOV only run for valid transactions.
        if !tx.is_valid() {
            return Ok(());
        }

        let Some(voting_procedures) = &conway_tx.transaction_body.voting_procedures else {
            return Ok(());
        };

        for (voter, votes) in voting_procedures.iter() {
            for (gov_action_id, procedure) in votes.iter() {
                self.pending_votes.push(VoteCast::new(
                    gov_action_id.transaction_id,
                    gov_action_id.action_index,
                    voter.clone(),
                    procedure.vote.clone(),
                    block.slot(),
                    order,
                    self.epoch_start.expect("value set in root"),
                ));
            }
        }

        Ok(())
    }

    fn visit_update(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: Option<&MultiEraTx>,
        update: &MultiEraUpdate,
    ) -> Result<(), ChainError> {
        let action = pre_conway_to_pparamset(update);

        deltas.add_for_entity(NewProposalV2::new(
            block.slot(),
            tx.map(|tx| tx.hash()).unwrap_or_else(|| block.hash()),
            0,
            ProposalAction::ParamChange(action),
            None,
            None,
            self.validity_period,
            self.current_epoch.expect("value set in root"),
            self.network_magic.expect("value set in root"),
            self.protocol.expect("value set in root"),
            // pre-Conway updates carry no Conway lineage or anchor
            None,
            None,
            None,
        ));

        Ok(())
    }

    fn visit_proposal(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        proposal: &pallas::ledger::traverse::MultiEraProposal,
        idx: usize,
    ) -> Result<(), ChainError> {
        let Some(proposal) = proposal.as_conway() else {
            return Ok(());
        };

        let (action, parent, purpose) = parse_gov_action(&proposal.gov_action);

        let reward_account = pallas_extras::parse_reward_account(&proposal.reward_account)
            .ok_or(ChainError::InvalidProposalParams)?;

        deltas.add_for_entity(NewProposalV2::new(
            block.slot(),
            tx.hash(),
            idx as u32,
            action,
            Some(proposal.deposit),
            Some(reward_account),
            self.validity_period,
            self.current_epoch.expect("value set in root"),
            self.network_magic.expect("value set in root"),
            self.protocol.expect("value set in root"),
            parent,
            purpose,
            Some(proposal.anchor.clone()),
        ));

        Ok(())
    }

    fn flush(&mut self, deltas: &mut WorkDeltas) -> Result<(), ChainError> {
        // Votes buffered during `visit_tx` are emitted at block flush so a
        // vote targeting a proposal submitted in the same block (legal in
        // Conway, even within the same tx) lands *after* that proposal's
        // `NewProposalV2` in the per-entity delta ordering.
        for vote in self.pending_votes.drain(..) {
            deltas.add_for_entity(vote);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use pallas::ledger::primitives::conway::{
        CostModels, DRepVotingThresholds, ExUnitPrices as ConwayExUnitPrices, ExUnits,
        PoolVotingThresholds,
    };

    use super::*;

    fn half() -> RationalNumber {
        RationalNumber {
            numerator: 1,
            denominator: 2,
        }
    }

    fn no_languages() -> CostModels {
        CostModels {
            plutus_v1: None,
            plutus_v2: None,
            plutus_v3: None,
            unknown: BTreeMap::new(),
        }
    }

    fn every_language() -> CostModels {
        CostModels {
            plutus_v1: Some(vec![1]),
            plutus_v2: Some(vec![2]),
            plutus_v3: Some(vec![3]),
            unknown: BTreeMap::from([(7, vec![7])]),
        }
    }

    fn no_fields() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            minfee_a: None,
            minfee_b: None,
            max_block_body_size: None,
            max_transaction_size: None,
            max_block_header_size: None,
            key_deposit: None,
            pool_deposit: None,
            maximum_epoch: None,
            desired_number_of_stake_pools: None,
            pool_pledge_influence: None,
            expansion_rate: None,
            treasury_growth_rate: None,
            min_pool_cost: None,
            ada_per_utxo_byte: None,
            cost_models_for_script_languages: None,
            execution_costs: None,
            max_tx_ex_units: None,
            max_block_ex_units: None,
            max_value_size: None,
            collateral_percentage: None,
            max_collateral_inputs: None,
            pool_voting_thresholds: None,
            drep_voting_thresholds: None,
            min_committee_size: None,
            committee_term_limit: None,
            governance_action_validity_period: None,
            governance_action_deposit: None,
            drep_deposit: None,
            drep_inactivity_period: None,
            minfee_refscript_cost_per_byte: None,
        }
    }

    fn every_field() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            minfee_a: Some(1),
            minfee_b: Some(1),
            max_block_body_size: Some(1),
            max_transaction_size: Some(1),
            max_block_header_size: Some(1),
            key_deposit: Some(1),
            pool_deposit: Some(1),
            maximum_epoch: Some(1),
            desired_number_of_stake_pools: Some(1),
            pool_pledge_influence: Some(half()),
            expansion_rate: Some(half()),
            treasury_growth_rate: Some(half()),
            min_pool_cost: Some(1),
            ada_per_utxo_byte: Some(1),
            cost_models_for_script_languages: Some(every_language()),
            execution_costs: Some(ConwayExUnitPrices {
                mem_price: half(),
                step_price: half(),
            }),
            max_tx_ex_units: Some(ExUnits { mem: 1, steps: 1 }),
            max_block_ex_units: Some(ExUnits { mem: 1, steps: 1 }),
            max_value_size: Some(1),
            collateral_percentage: Some(1),
            max_collateral_inputs: Some(1),
            pool_voting_thresholds: Some(PoolVotingThresholds {
                motion_no_confidence: half(),
                committee_normal: half(),
                committee_no_confidence: half(),
                hard_fork_initiation: half(),
                security_voting_threshold: half(),
            }),
            drep_voting_thresholds: Some(DRepVotingThresholds {
                motion_no_confidence: half(),
                committee_normal: half(),
                committee_no_confidence: half(),
                update_constitution: half(),
                hard_fork_initiation: half(),
                pp_network_group: half(),
                pp_economic_group: half(),
                pp_technical_group: half(),
                pp_governance_group: half(),
                treasury_withdrawal: half(),
            }),
            min_committee_size: Some(1),
            committee_term_limit: Some(1),
            governance_action_validity_period: Some(1),
            governance_action_deposit: Some(1),
            drep_deposit: Some(1),
            drep_inactivity_period: Some(1),
            minfee_refscript_cost_per_byte: Some(half()),
        }
    }

    /// Splits `$full` into one value per field, each with only that field
    /// set over `$none`. The destructure has no `..`, so a field added to
    /// the struct fails the build until it is listed here.
    macro_rules! each_field_alone {
        ($ty:ident, $full:expr, $none:expr, [$($field:ident),* $(,)?]) => {{
            let $ty { $($field),* } = $full;
            vec![$((stringify!($field), $ty { $field, ..$none })),*]
        }};
    }

    /// DATA-MODEL-WORKAROUND(dolos#1375): ratification reads an empty
    /// stored set as a named but empty cost-model map. That holds only
    /// while every other Conway update field leaves a value behind.
    #[test]
    fn every_conway_update_field_alone_leaves_a_value() {
        let singles = each_field_alone!(
            ProtocolParamUpdate,
            every_field(),
            no_fields(),
            [
                minfee_a,
                minfee_b,
                max_block_body_size,
                max_transaction_size,
                max_block_header_size,
                key_deposit,
                pool_deposit,
                maximum_epoch,
                desired_number_of_stake_pools,
                pool_pledge_influence,
                expansion_rate,
                treasury_growth_rate,
                min_pool_cost,
                ada_per_utxo_byte,
                cost_models_for_script_languages,
                execution_costs,
                max_tx_ex_units,
                max_block_ex_units,
                max_value_size,
                collateral_percentage,
                max_collateral_inputs,
                pool_voting_thresholds,
                drep_voting_thresholds,
                min_committee_size,
                committee_term_limit,
                governance_action_validity_period,
                governance_action_deposit,
                drep_deposit,
                drep_inactivity_period,
                minfee_refscript_cost_per_byte,
            ]
        );

        for (field, update) in singles {
            assert!(
                !conway_to_pparamset(&update).is_empty(),
                "`{field}` alone maps to no value"
            );
        }
    }

    /// DATA-MODEL-WORKAROUND(dolos#1375): each cost-model language leaves
    /// a value, so a named cost-model map leaves none only when it is empty.
    #[test]
    fn only_an_empty_cost_model_map_leaves_no_value() {
        let languages = each_field_alone!(
            CostModels,
            every_language(),
            no_languages(),
            [plutus_v1, plutus_v2, plutus_v3, unknown]
        );

        for (language, cost_models) in languages {
            let update = ProtocolParamUpdate {
                cost_models_for_script_languages: Some(cost_models),
                ..no_fields()
            };

            assert!(
                !conway_to_pparamset(&update).is_empty(),
                "`{language}` alone maps to no value"
            );
        }

        let empty = ProtocolParamUpdate {
            cost_models_for_script_languages: Some(no_languages()),
            ..no_fields()
        };

        assert!(conway_to_pparamset(&empty).is_empty());
    }
}
