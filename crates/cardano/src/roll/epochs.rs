use dolos_core::{batch::WorkDeltas, ChainError, NsKey};
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{MultiEraBlock, MultiEraCert, MultiEraTx, MultiEraUpdate},
};
use serde::{Deserialize, Serialize};

use crate::{
    model::{EpochState, FixedNamespace as _, EPOCH_KEY_MARK},
    pallas_extras,
    roll::BlockVisitor,
    CardanoLogic, Nonces, PParamValue,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EpochStatsUpdate {
    block_fees: u64,
    utxo_consumed: u64,
    utxo_produced: u64,
    stake_registration_count: u64,
    stake_deregistration_count: u64,
    pool_registration_count: u64,
}

impl dolos_core::EntityDelta for EpochStatsUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, EPOCH_KEY_MARK))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.get_or_insert_default();

        entity.gathered_fees += self.block_fees;

        entity.utxos += self.utxo_produced;
        entity.utxos = entity.utxos.saturating_sub(self.utxo_consumed);

        entity.gathered_deposits += self.stake_registration_count
            * entity.pparams.key_deposit_or_default()
            + self.pool_registration_count * entity.pparams.pool_deposit_or_default();
        entity.decayed_deposits +=
            self.stake_deregistration_count * entity.pparams.pool_deposit_or_default();
    }

    fn undo(&self, entity: &mut Option<EpochState>) {
        let entity = entity.get_or_insert_default();

        entity.gathered_fees -= self.block_fees;

        entity.utxos -= self.utxo_produced;
        entity.utxos += self.utxo_consumed;

        entity.gathered_deposits -= self.stake_registration_count
            * entity.pparams.key_deposit_or_default()
            + self.pool_registration_count * entity.pparams.pool_deposit_or_default();
        entity.decayed_deposits -=
            self.stake_deregistration_count * entity.pparams.pool_deposit_or_default();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoncesUpdate {
    slot: u64,
    tail: Option<Hash<32>>,
    nonce_vrf_output: Vec<u8>,

    previous: Option<Nonces>,
}

impl dolos_core::EntityDelta for NoncesUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, EPOCH_KEY_MARK))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.get_or_insert_default();
        if let Some(nonces) = entity.nonces.as_ref() {
            self.previous = Some(nonces.clone());
            entity.nonces = Some(nonces.roll(
                self.slot < entity.largest_stable_slot,
                &self.nonce_vrf_output,
                self.tail,
            ));
        }
    }

    fn undo(&self, entity: &mut Option<EpochState>) {
        let entity = entity.get_or_insert_default();
        entity.nonces = self.previous.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PParamsUpdate {
    to_update: PParamValue,

    // undo
    prev_value: Option<PParamValue>,
}

impl dolos_core::EntityDelta for PParamsUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, EPOCH_KEY_MARK))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.get_or_insert_default();
        entity.pparams.set(self.to_update.clone());
    }

    fn undo(&self, entity: &mut Option<EpochState>) {
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

#[derive(Clone, Default)]
pub struct EpochStateVisitor {
    stats_delta: Option<EpochStatsUpdate>,
    nonces_delta: Option<NoncesUpdate>,
}

impl BlockVisitor for EpochStateVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
    ) -> Result<(), ChainError> {
        self.stats_delta = Some(EpochStatsUpdate::default());

        // we only track nonces for Shelley and later
        if block.era() >= pallas::ledger::traverse::Era::Shelley {
            self.nonces_delta = Some(NoncesUpdate {
                slot: block.header().slot(),
                tail: block.header().previous_hash(),
                nonce_vrf_output: block.header().nonce_vrf_output()?,
                previous: None,
            });
        }

        Ok(())
    }

    fn visit_tx(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        tx: &MultiEraTx,
    ) -> Result<(), ChainError> {
        self.stats_delta.as_mut().unwrap().block_fees += tx.fee().unwrap_or_default();

        Ok(())
    }

    fn visit_input(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &pallas::ledger::traverse::MultiEraInput,
        resolved: &pallas::ledger::traverse::MultiEraOutput,
    ) -> Result<(), ChainError> {
        let amount = resolved.value().coin();
        self.stats_delta.as_mut().unwrap().utxo_consumed += amount;

        Ok(())
    }

    fn visit_output(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &pallas::ledger::traverse::MultiEraOutput,
    ) -> Result<(), ChainError> {
        let amount = output.value().coin();
        self.stats_delta.as_mut().unwrap().utxo_produced += amount;

        Ok(())
    }

    fn visit_cert(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        if pallas_extras::cert_as_stake_registration(cert).is_some() {
            self.stats_delta.as_mut().unwrap().stake_registration_count += 1;
        }

        if pallas_extras::cert_as_stake_deregistration(cert).is_some() {
            self.stats_delta
                .as_mut()
                .unwrap()
                .stake_deregistration_count += 1;
        }

        if pallas_extras::cert_to_pool_state(cert).is_some() {
            self.stats_delta.as_mut().unwrap().pool_registration_count += 1;
        }

        // TODO: decayed deposits

        Ok(())
    }

    fn visit_update(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: Option<&MultiEraTx>,
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

    fn flush(&mut self, deltas: &mut WorkDeltas<CardanoLogic>) -> Result<(), ChainError> {
        if let Some(delta) = self.stats_delta.take() {
            deltas.add_for_entity(delta);
        }

        if let Some(delta) = self.nonces_delta.take() {
            deltas.add_for_entity(delta);
        }

        Ok(())
    }
}
