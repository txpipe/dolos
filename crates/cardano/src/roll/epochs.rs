use std::collections::{HashMap, HashSet};

use dolos_core::{batch::WorkDeltas, BrokenInvariant, ChainError, NsKey, TxoRef};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::Epoch,
        traverse::{MultiEraBlock, MultiEraCert, MultiEraTx, MultiEraUpdate},
    },
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    model::{EpochState, FixedNamespace as _},
    owned::OwnedMultiEraOutput,
    pallas_extras,
    roll::BlockVisitor,
    CardanoLogic, Lovelace, Nonces, PParamValue, PParamsSet, PoolHash, CURRENT_EPOCH_KEY,
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EpochStatsUpdate {
    block_fees: u64,

    // we need to use a delta approach instead of simple increments because the total size of moved
    // lovelace can be higher than u64, causing overflows
    utxo_delta: i64,

    new_accounts: u64,
    removed_accounts: u64,
    withdrawals: u64,
    registered_pools: HashSet<PoolHash>,
    drep_deposits: Lovelace,
    proposal_deposits: Lovelace,
    drep_refunds: Lovelace,
    treasury_donations: Lovelace,
}

impl dolos_core::EntityDelta for EpochStatsUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let Some(entity) = entity else { return };

        let stats = entity.rolling.live_mut_unchecked().get_or_insert_default();

        stats.blocks_minted += 1;

        if self.utxo_delta > 0 {
            stats.produced_utxos += self.utxo_delta.unsigned_abs();
        } else {
            stats.consumed_utxos += self.utxo_delta.unsigned_abs();
        }

        stats.gathered_fees += self.block_fees;
        stats.new_accounts += self.new_accounts;
        stats.removed_accounts += self.removed_accounts;
        stats.withdrawals += self.withdrawals;
        stats.proposal_deposits += self.proposal_deposits;
        stats.drep_deposits += self.drep_deposits;
        stats.drep_refunds += self.drep_refunds;
        stats.treasury_donations += self.treasury_donations;

        stats.registered_pools = stats
            .registered_pools
            .union(&self.registered_pools)
            .cloned()
            .collect();
    }

    fn undo(&self, entity: &mut Option<EpochState>) {
        let Some(entity) = entity else { return };

        let stats = entity
            .rolling
            .live_mut_unchecked()
            .as_mut()
            .expect("data to undo");

        stats.blocks_minted -= 1;

        if self.utxo_delta > 0 {
            stats.produced_utxos -= self.utxo_delta.unsigned_abs();
        } else {
            stats.consumed_utxos -= self.utxo_delta.unsigned_abs();
        }

        stats.gathered_fees -= self.block_fees;
        stats.new_accounts -= self.new_accounts;
        stats.removed_accounts -= self.removed_accounts;
        stats.withdrawals -= self.withdrawals;

        stats.registered_pools = stats
            .registered_pools
            .difference(&self.registered_pools)
            .cloned()
            .collect();
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
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let Some(entity) = entity else { return };
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
        let Some(entity) = entity else { return };
        entity.nonces = self.previous.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PParamsUpdate {
    to_update: PParamValue,

    // undo
    prev_value: Option<PParamValue>,
}

impl PParamsUpdate {
    pub fn new(to_update: PParamValue) -> Self {
        Self {
            to_update,
            prev_value: None,
        }
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

        // undo data
        self.prev_value = next.get(self.to_update.kind()).cloned();

        next.set(self.to_update.clone());
    }

    fn undo(&self, _entity: &mut Option<EpochState>) {
        // todo!()
        // Placeholder undo logic. Ensure this does not panic.
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

// HACK: There are txs that don't have an explicit value for total collateral
// and Alonzo txs don't even have the total collateral field. This is why we
// need to compute it by looking at collateral inputs and collateral return.
// Pallas hides this from us by providing the "consumes" / "produces" facade.
// Note: This can be called for transactions from any era (Alonzo, Babbage, Conway)
// when total_collateral is not set.
fn compute_collateral_value(
    tx: &MultiEraTx,
    utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
) -> Result<Lovelace, ChainError> {
    debug_assert!(!tx.is_valid());

    let mut total = 0;

    for input in tx.consumes() {
        let utxo = utxos
            .get(&TxoRef::from(&input))
            .ok_or(ChainError::BrokenInvariant(BrokenInvariant::MissingUtxo(
                TxoRef::from(&input),
            )))?;
        utxo.with_dependent(|_, utxo| {
            total += utxo.value().coin();
        });
    }

    for (_, output) in tx.produces() {
        total -= output.value().coin();
    }

    Ok(total)
}

fn define_tx_fees(
    tx: &MultiEraTx,
    utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
) -> Result<Lovelace, ChainError> {
    if tx.is_valid() {
        Ok(tx.fee().unwrap_or_default())
    } else if let Some(collateral) = tx.total_collateral() {
        tracing::debug!(tx=%tx.hash(), collateral, "total collateral consumed");
        Ok(collateral)
    } else {
        let fee = compute_collateral_value(tx, utxos)?;
        tracing::debug!(tx=%tx.hash(), fee, "alonzo-style collateral computed");
        Ok(fee)
    }
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
        _: &PParamsSet,
        _: Epoch,
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
        utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        let fees = define_tx_fees(tx, utxos)?;

        self.stats_delta.as_mut().unwrap().block_fees += fees;

        if let Some(donation) = pallas_extras::tx_treasury_donation(tx) {
            self.stats_delta.as_mut().unwrap().treasury_donations += donation;
        }

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
        self.stats_delta.as_mut().unwrap().utxo_delta -= amount as i64;

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
        self.stats_delta.as_mut().unwrap().utxo_delta += amount as i64;

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
            self.stats_delta.as_mut().unwrap().new_accounts += 1;
        }

        if pallas_extras::cert_as_stake_deregistration(cert).is_some() {
            self.stats_delta.as_mut().unwrap().removed_accounts += 1;
        }

        if let Some(cert) = pallas_extras::cert_as_pool_registration(cert) {
            self.stats_delta
                .as_mut()
                .unwrap()
                .registered_pools
                .insert(cert.operator);
        }

        if let Some(cert) = pallas_extras::cert_as_drep_registration(cert) {
            tracing::debug!(cert=?cert.cred, "drep registration");
            self.stats_delta.as_mut().unwrap().drep_deposits += cert.deposit;
        }

        if let Some(cert) = pallas_extras::cert_as_drep_unregistration(cert) {
            tracing::debug!(cert=?cert.cred, "drep un-registration");
            self.stats_delta.as_mut().unwrap().drep_refunds += cert.deposit;
        }

        Ok(())
    }

    fn visit_proposal(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        proposal: &pallas::ledger::traverse::MultiEraProposal,
        _: usize,
    ) -> Result<(), ChainError> {
        tracing::warn!(proposal=?proposal.gov_action(), deposit=proposal.deposit(), "proposal deposit");

        self.stats_delta.as_mut().unwrap().proposal_deposits += proposal.deposit();

        Ok(())
    }

    fn visit_withdrawal(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &[u8],
        amount: u64,
    ) -> Result<(), ChainError> {
        self.stats_delta.as_mut().unwrap().withdrawals += amount;
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

        if let Some((major, minor, _)) = update.byron_proposed_block_version() {
            deltas.add_for_entity(PParamsUpdate {
                to_update: PParamValue::ProtocolVersion((major.into(), minor.into())),
                prev_value: None,
            });
        }

        if let Some(cm) = update.alonzo_first_proposed_cost_models_for_script_languages() {
            if let Some(v1) = cm.get(&pallas::ledger::primitives::alonzo::Language::PlutusV1) {
                deltas.add_for_entity(PParamsUpdate {
                    to_update: PParamValue::CostModelsPlutusV1(v1.clone()),
                    prev_value: None,
                });
            }
        }

        if let Some(cm) = update.babbage_first_proposed_cost_models_for_script_languages() {
            if let Some(v1) = cm.plutus_v1 {
                deltas.add_for_entity(PParamsUpdate {
                    to_update: PParamValue::CostModelsPlutusV1(v1),
                    prev_value: None,
                });
            }
            if let Some(v2) = cm.plutus_v2 {
                deltas.add_for_entity(PParamsUpdate {
                    to_update: PParamValue::CostModelsPlutusV2(v2),
                    prev_value: None,
                });
            }
        }

        if let Some(cm) = update.conway_first_proposed_cost_models_for_script_languages() {
            if let Some(v1) = cm.plutus_v1 {
                deltas.add_for_entity(PParamsUpdate {
                    to_update: PParamValue::CostModelsPlutusV1(v1),
                    prev_value: None,
                });
            }

            if let Some(v2) = cm.plutus_v2 {
                deltas.add_for_entity(PParamsUpdate {
                    to_update: PParamValue::CostModelsPlutusV2(v2),
                    prev_value: None,
                });
            }

            if let Some(v3) = cm.plutus_v3 {
                deltas.add_for_entity(PParamsUpdate {
                    to_update: PParamValue::CostModelsPlutusV3(v3),
                    prev_value: None,
                });
            }

            if !cm.unknown.is_empty() {
                deltas.add_for_entity(PParamsUpdate {
                    to_update: PParamValue::CostModelsUnknown(cm.unknown),
                    prev_value: None,
                });
            }
        }

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
