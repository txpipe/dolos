use std::collections::{HashMap, HashSet};

use dolos_core::{BrokenInvariant, ChainError, Genesis, NsKey, TxoRef};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::{
            alonzo::{
                InstantaneousRewardSource, InstantaneousRewardTarget, MoveInstantaneousReward,
            },
            Epoch,
        },
        traverse::{fees::compute_byron_fee, MultiEraBlock, MultiEraCert, MultiEraTx},
    },
};
use serde::{Deserialize, Serialize};

use super::WorkDeltas;
use crate::{
    model::{EpochState, FixedNamespace as _},
    owned::OwnedMultiEraOutput,
    pallas_extras,
    roll::BlockVisitor,
    Lovelace, Nonces, PParamsSet, PoolHash, CURRENT_EPOCH_KEY,
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
    reserve_mirs: Lovelace,
}

impl dolos_core::EntityDelta for EpochStatsUpdate {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, CURRENT_EPOCH_KEY))
    }

    fn apply(&mut self, entity: &mut Option<EpochState>) {
        let entity = entity.as_mut().expect("existing epoch");

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
        stats.reserve_mirs += self.reserve_mirs;

        stats.registered_pools = stats
            .registered_pools
            .union(&self.registered_pools)
            .cloned()
            .collect();
    }

    fn undo(&self, _entity: &mut Option<EpochState>) {
        // TODO: implement undo
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
    if let Some(byron) = tx.as_byron() {
        let fee = compute_byron_fee(byron, None);
        Ok(fee)
    } else if tx.is_valid() {
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
        _: &mut WorkDeltas,
        block: &MultiEraBlock,
        _: &Genesis,
        _: &PParamsSet,
        _: Epoch,
        _: u16,
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
        _: &mut WorkDeltas,
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
        _: &mut WorkDeltas,
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
        _: &mut WorkDeltas,
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
        _: &mut WorkDeltas,
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

        if let Some(cert) = pallas_extras::cert_as_mir_certificate(cert) {
            let MoveInstantaneousReward { source, target, .. } = cert;

            if source == InstantaneousRewardSource::Reserves {
                if let InstantaneousRewardTarget::StakeCredentials(creds) = target {
                    for (_, amount) in creds {
                        let amount = amount.max(0) as u64;
                        self.stats_delta.as_mut().unwrap().reserve_mirs += amount;
                    }
                }
            }

            // TODO: track rewards from treasury (unless there's none in mainnet)
        }

        Ok(())
    }

    fn visit_proposal(
        &mut self,
        _: &mut WorkDeltas,
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
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &[u8],
        amount: u64,
    ) -> Result<(), ChainError> {
        self.stats_delta.as_mut().unwrap().withdrawals += amount;
        Ok(())
    }

    fn flush(&mut self, deltas: &mut WorkDeltas) -> Result<(), ChainError> {
        if let Some(delta) = self.stats_delta.take() {
            deltas.add_for_entity(delta);
        }

        if let Some(delta) = self.nonces_delta.take() {
            deltas.add_for_entity(delta);
        }

        Ok(())
    }
}
