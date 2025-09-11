use crate::roll::BlockVisitor;
use crate::{CardanoLogic, EpochState, FixedNamespace as _, PParamValue, EPOCH_KEY_MARK};
use dolos_core::batch::WorkDeltas;
use dolos_core::{ChainError, NsKey};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx, MultiEraUpdate};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PParamsUpdate {
    to_update: PParamValue,

    // undo
    prev_value: Option<PParamValue>,
}

impl dolos_core::EntityDelta for PParamsUpdate {
    type Entity = EpochState;

    fn key(&self) -> Cow<'_, NsKey> {
        Cow::Owned(NsKey::from((EpochState::NS, EPOCH_KEY_MARK)))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.get_or_insert_default();
        entity.pparams.set(self.to_update.clone());
    }

    fn undo(&mut self, entity: &mut Option<EpochState>) {
        if let Some(entity) = entity {
            if let Some(prev_value) = &self.prev_value {
                entity.pparams.set(prev_value.clone());
            }
        }
    }
}

macro_rules! pparams_update {
    ($update:expr, $getter:ident, $deltas:expr, $variant:ident) => {
        $update.$getter().iter().for_each(|value| {
            $deltas.add_for_entity(PParamsUpdate {
                to_update: PParamValue::$variant(value.clone().into()),
                prev_value: None,
            });
        });
    };
}

macro_rules! check_all_proposed {
    ($update:expr, $deltas:expr, $($getter:ident => $variant:ident),*) => {
        $(
            pparams_update!($update, $getter, $deltas, $variant);
        )*
    };
}

pub struct PParamsStateVisitor;

impl<'a> BlockVisitor for PParamsStateVisitor {
    fn visit_update(
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        update: &MultiEraUpdate,
    ) -> Result<(), ChainError> {
        check_all_proposed! {
            update,
            deltas,

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

        // TODO: cost model updates

        Ok(())
    }
}
