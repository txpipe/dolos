use std::collections::HashMap;

use dolos_core::{BrokenInvariant, ChainError, Genesis, TxOrder, TxoRef};
use pallas::ledger::{
    primitives::{
        alonzo::{InstantaneousRewardSource, InstantaneousRewardTarget, MoveInstantaneousReward},
        conway::RationalNumber,
        Epoch,
    },
    traverse::{fees::compute_byron_fee, MultiEraBlock, MultiEraCert, MultiEraTx},
};

use super::WorkDeltas;
use crate::{
    owned::OwnedMultiEraOutput, pallas_extras, roll::BlockVisitor, EpochStatsUpdate, Lovelace,
    NoncesUpdate, PParamsSet,
};

// HACK: There are txs that don't have an explicit value for total collateral
// and Alonzo txs don't even have the total collateral field. This is why we
// need to compute it by looking at collateral inputs and collateral return.
// Pallas hides this from us by providing the "consumes" / "produces" facade.
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

fn is_overlay_slot(first_slot: u64, d: &RationalNumber, slot: u64) -> bool {
    let s = slot.saturating_sub(first_slot) as u128;
    let numer = d.numerator as u128;
    let denom = d.denominator as u128;

    if denom == 0 {
        return false;
    }

    let step = |x: u128| (x.saturating_mul(numer).saturating_add(denom - 1)) / denom;

    step(s) < step(s + 1)
}

impl BlockVisitor for EpochStateVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas,
        block: &MultiEraBlock,
        _: &Genesis,
        pparams: &PParamsSet,
        epoch: Epoch,
        epoch_start: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        self.stats_delta = Some(EpochStatsUpdate {
            epoch,
            ..Default::default()
        });
        if let Some(stats) = self.stats_delta.as_mut() {
            let is_overlay = match pparams.ensure_d().ok() {
                Some(d) => is_overlay_slot(epoch_start, &d, block.header().slot()),
                None => false,
            };

            if !is_overlay {
                stats.non_overlay_blocks_minted += 1;
            }
        }
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
        _: &TxOrder,
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

            match (source, target) {
                (
                    InstantaneousRewardSource::Reserves,
                    InstantaneousRewardTarget::StakeCredentials(creds),
                ) => {
                    for (cred, amount) in creds {
                        if amount < 0 {
                            tracing::warn!(
                                source = "reserves",
                                credential = ?cred,
                                amount = amount,
                                "NEGATIVE MIR amount detected - clamping to 0"
                            );
                        }
                        let amount = amount.max(0) as u64;
                        self.stats_delta.as_mut().unwrap().reserve_mirs += amount;
                    }
                }
                (
                    InstantaneousRewardSource::Treasury,
                    InstantaneousRewardTarget::StakeCredentials(creds),
                ) => {
                    for (cred, amount) in creds {
                        if amount < 0 {
                            tracing::warn!(
                                source = "treasury",
                                credential = ?cred,
                                amount = amount,
                                "NEGATIVE MIR amount detected - clamping to 0"
                            );
                        }
                        let amount_u64 = amount.max(0) as u64;
                        tracing::debug!(
                            source = "treasury",
                            credential = ?cred,
                            amount = amount,
                            amount_u64 = amount_u64,
                            "processing treasury MIR"
                        );
                        self.stats_delta.as_mut().unwrap().treasury_mirs += amount_u64;
                    }
                }
                _ => {}
            }
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
        tracing::debug!(proposal=?proposal.gov_action(), deposit=proposal.deposit(), "proposal deposit");

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
