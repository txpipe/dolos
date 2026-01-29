use std::collections::BTreeMap;

use dolos_core::{BlockSlot, ChainError, Genesis, NsKey};
use pallas::{
    codec::utils::Bytes,
    crypto::hash::Hash,
    ledger::{
        primitives::{
            conway::{GovAction, ProtocolParamUpdate},
            Epoch, ExUnitPrices, RationalNumber, StakeCredential,
        },
        traverse::{MultiEraBlock, MultiEraTx, MultiEraUpdate},
    },
};
use serde::{Deserialize, Serialize};

use super::WorkDeltas;
use crate::{
    hacks::{self, proposals::ProposalOutcome},
    model::FixedNamespace as _,
    pallas_extras,
    roll::BlockVisitor,
    Lovelace, PParamValue, PParamsSet, ProposalAction, ProposalState,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewProposal {
    slot: BlockSlot,
    tx: Hash<32>,
    idx: u32,
    action: ProposalAction,
    deposit: Option<Lovelace>,
    reward_account: Option<StakeCredential>,
    validity_period: Option<u64>,
    current_epoch: Epoch,
    network_magic: u32,
    protocol: u16,
}

impl dolos_core::EntityDelta for NewProposal {
    type Entity = ProposalState;

    fn key(&self) -> NsKey {
        NsKey::from((
            ProposalState::NS,
            ProposalState::build_entity_key(self.tx, self.idx),
        ))
    }

    fn apply(&mut self, entity: &mut Option<ProposalState>) {
        let id = ProposalState::id(self.tx, self.idx);

        let outcome = hacks::proposals::outcome(self.network_magic, self.protocol, &id);

        let max_epoch = self.validity_period.map(|x| self.current_epoch + x);

        let ratified_epoch = match &outcome {
            ProposalOutcome::Ratified(epoch) => Some(*epoch),
            ProposalOutcome::RatifiedCurrentEpoch => Some(self.current_epoch),
            _ => None,
        };

        let canceled_epoch = match &outcome {
            ProposalOutcome::Canceled(epoch) => Some(*epoch),
            _ => None,
        };

        let state = ProposalState {
            slot: self.slot,
            tx: self.tx,
            idx: self.idx,
            action: self.action.clone(),
            reward_account: self.reward_account.clone(),
            deposit: self.deposit,
            max_epoch,
            ratified_epoch,
            canceled_epoch,
        };

        let _ = entity.insert(state);
    }

    fn undo(&self, entity: &mut Option<ProposalState>) {
        entity.take();
    }
}

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

#[derive(Clone, Default)]
pub struct ProposalVisitor {
    validity_period: Option<u64>,
    current_epoch: Option<Epoch>,
    network_magic: Option<u32>,
    protocol: Option<u16>,
}

impl BlockVisitor for ProposalVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        genesis: &Genesis,
        pparams: &PParamsSet,
        epoch: Epoch,
        protocol: u16,
    ) -> Result<(), ChainError> {
        self.validity_period = pparams.governance_action_validity_period();
        self.current_epoch = Some(epoch);
        self.network_magic = Some(genesis.network_magic());
        self.protocol = Some(protocol);

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

        deltas.add_for_entity(NewProposal {
            slot: block.slot(),
            tx: tx.map(|tx| tx.hash()).unwrap_or_else(|| block.hash()),
            idx: 0,
            action: ProposalAction::ParamChange(action),
            deposit: None,
            reward_account: None,
            validity_period: self.validity_period,
            current_epoch: self.current_epoch.expect("value set in root"),
            network_magic: self.network_magic.expect("value set in root"),
            protocol: self.protocol.expect("value set in root"),
        });

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

        let action = match &proposal.gov_action {
            GovAction::ParameterChange(_, x, _) => {
                ProposalAction::ParamChange(conway_to_pparamset(x))
            }
            GovAction::HardForkInitiation(_, version) => ProposalAction::HardFork(*version),
            GovAction::TreasuryWithdrawals(x, _) => parse_treasury_withdrawals(x),
            GovAction::Information => ProposalAction::Other,
            GovAction::NoConfidence(..) => ProposalAction::Other,
            GovAction::UpdateCommittee(..) => ProposalAction::Other,
            GovAction::NewConstitution(..) => ProposalAction::Other,
        };

        let reward_account = pallas_extras::parse_reward_account(&proposal.reward_account)
            .ok_or(ChainError::InvalidProposalParams)?;

        deltas.add_for_entity(NewProposal {
            slot: block.slot(),
            tx: tx.hash(),
            idx: idx as u32,
            reward_account: Some(reward_account),
            deposit: Some(proposal.deposit),
            action,
            validity_period: self.validity_period,
            current_epoch: self.current_epoch.expect("value set in root"),
            network_magic: self.network_magic.expect("value set in root"),
            protocol: self.protocol.expect("value set in root"),
        });

        Ok(())
    }
}
