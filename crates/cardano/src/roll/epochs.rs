use std::collections::HashMap;

use dolos_core::{BrokenInvariant, ChainError, Genesis, TxOrder, TxoRef};
use pallas::ledger::{
    primitives::{
        alonzo::{InstantaneousRewardSource, InstantaneousRewardTarget, MoveInstantaneousReward},
        conway::RationalNumber,
        Epoch,
    },
    traverse::{MultiEraBlock, MultiEraCert, MultiEraTx},
};

use super::WorkDeltas;
use crate::{
    owned::OwnedMultiEraOutput, pallas_extras, roll::BlockVisitor, EpochStatsUpdate, Lovelace,
    NoncesUpdate, PParamsSet,
};

/// Lovelace consumed minus lovelace produced, over the inputs and outputs the
/// tx's validity selects (`consumes` / `produces`).
fn compute_consumed_minus_produced(
    tx: &MultiEraTx,
    utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
) -> Result<Lovelace, ChainError> {
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

// HACK: There are txs that don't have an explicit value for total collateral
// and Alonzo txs don't even have the total collateral field. This is why we
// need to compute it by looking at collateral inputs and collateral return.
// Pallas hides this from us by providing the "consumes" / "produces" facade.
fn compute_collateral_value(
    tx: &MultiEraTx,
    utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
) -> Result<Lovelace, ChainError> {
    debug_assert!(!tx.is_valid());

    compute_consumed_minus_produced(tx, utxos)
}

fn define_tx_fees(
    tx: &MultiEraTx,
    utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
) -> Result<Lovelace, ChainError> {
    // Byron txs carry no fee field: the fee is what the inputs leave over,
    // which can exceed the policy minimum.
    if matches!(tx, MultiEraTx::Byron(_)) {
        compute_consumed_minus_produced(tx, utxos)
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
            block_slot: block.header().slot(),
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
            self.nonces_delta = Some(NoncesUpdate::new(
                block.header().slot(),
                block.header().previous_hash(),
                block.header().nonce_vrf_output()?,
            ));
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
        self.stats_delta.as_mut().unwrap().tx_count += 1;

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
        _: &TxOrder,
        _: u32,
        output: &pallas::ledger::traverse::MultiEraOutput,
    ) -> Result<(), ChainError> {
        let amount = output.value().coin();
        let stats = self.stats_delta.as_mut().unwrap();
        stats.utxo_delta += amount as i64;
        // Gross, unsigned sum of all produced outputs (db-sync `epoch.out_sum`).
        // The driver iterates `tx.produces()`. So this sum includes the regular
        // outputs of valid transactions and the collateral-return outputs of
        // phase-2 failures.
        stats.output += amount;

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use pallas::codec::minicbor;
    use pallas::codec::utils::{CborWrap, EmptyMap, KeepRaw, MaybeIndefArray, Nullable, TagWrap};
    use pallas::crypto::hash::Hash;
    use pallas::ledger::primitives::{alonzo, byron};
    use pallas::ledger::traverse::{Era, MultiEraOutput};

    use super::*;

    fn byron_output(amount: u64) -> byron::TxOut {
        byron::TxOut {
            address: byron::Address {
                payload: TagWrap::new(vec![0x83, 0x58, 0x1c].into()),
                crc: 0,
            },
            amount,
        }
    }

    fn alonzo_output(amount: u64) -> alonzo::TransactionOutput {
        alonzo::TransactionOutput {
            address: vec![0x61; 29].into(),
            amount: alonzo::Value::Coin(amount),
            datum_hash: None,
        }
    }

    fn owned(output: MultiEraOutput<'_>) -> OwnedMultiEraOutput {
        OwnedMultiEraOutput::decode(Arc::new(output.into())).unwrap()
    }

    fn alonzo_tx_cbor(body: alonzo::TransactionBody, success: bool) -> Vec<u8> {
        let witness_set = alonzo::WitnessSet {
            vkeywitness: None,
            native_script: None,
            bootstrap_witness: None,
            plutus_script: None,
            plutus_data: None,
            redeemer: None,
        };

        let tx = alonzo::Tx {
            transaction_body: KeepRaw::from(body),
            transaction_witness_set: KeepRaw::from(witness_set),
            success,
            auxiliary_data: Nullable::Null,
        };

        minicbor::to_vec(tx).unwrap()
    }

    fn alonzo_body(
        inputs: Vec<alonzo::TransactionInput>,
        outputs: Vec<alonzo::TransactionOutput>,
        fee: u64,
        collateral: Option<Vec<alonzo::TransactionInput>>,
    ) -> alonzo::TransactionBody {
        alonzo::TransactionBody {
            inputs,
            outputs,
            fee,
            ttl: None,
            certificates: None,
            withdrawals: None,
            update: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: None,
            script_data_hash: None,
            collateral,
            required_signers: None,
            network_id: None,
        }
    }

    #[test]
    fn byron_fee_is_inputs_minus_outputs() {
        let spent = [(Hash::new([1; 32]), 0), (Hash::new([2; 32]), 3)];

        let tx = byron::Tx {
            inputs: MaybeIndefArray::Def(
                spent
                    .iter()
                    .map(|(hash, index)| byron::TxIn::Variant0(CborWrap((*hash, *index))))
                    .collect(),
            ),
            outputs: MaybeIndefArray::Def(vec![byron_output(4_000_000), byron_output(500_000)]),
            attributes: EmptyMap,
        };

        let payload = byron::TxPayload {
            transaction: KeepRaw::from(tx),
            witness: KeepRaw::from(MaybeIndefArray::Def(vec![])),
        };

        let cbor = minicbor::to_vec(&payload).unwrap();
        let tx = MultiEraTx::decode_for_era(Era::Byron, &cbor).unwrap();

        let funding = [byron_output(3_000_000), byron_output(1_700_000)];
        let utxos: HashMap<_, _> = spent
            .iter()
            .zip(funding.iter())
            .map(|((hash, index), out)| {
                (
                    TxoRef(*hash, *index),
                    owned(MultiEraOutput::from_byron(out)),
                )
            })
            .collect();

        // 4_700_000 in, 4_500_000 out: the 200_000 fee paid is not the policy
        // minimum for this tx's size.
        let paid = 200_000;
        assert_ne!(tx.fee_or_compute(), paid);

        assert_eq!(define_tx_fees(&tx, &utxos).unwrap(), paid);
    }

    #[test]
    fn valid_shelley_fee_is_the_declared_fee() {
        let input = alonzo::TransactionInput {
            transaction_id: Hash::new([3; 32]),
            index: 0,
        };

        let body = alonzo_body(
            vec![input.clone()],
            vec![alonzo_output(1_000_000)],
            170_000,
            None,
        );
        let cbor = alonzo_tx_cbor(body, true);
        let tx = MultiEraTx::decode_for_era(Era::Shelley, &cbor).unwrap();

        // the input carries more than outputs plus fee; the declared fee wins
        let utxos = HashMap::from([(
            TxoRef(input.transaction_id, input.index as u32),
            owned(MultiEraOutput::from_alonzo_compatible(
                &alonzo_output(5_000_000),
                Era::Shelley,
            )),
        )]);

        assert_eq!(define_tx_fees(&tx, &utxos).unwrap(), 170_000);
    }

    #[test]
    fn invalid_alonzo_fee_is_the_collateral() {
        let input = alonzo::TransactionInput {
            transaction_id: Hash::new([4; 32]),
            index: 0,
        };
        let collateral = alonzo::TransactionInput {
            transaction_id: Hash::new([5; 32]),
            index: 1,
        };

        let body = alonzo_body(
            vec![input.clone()],
            vec![alonzo_output(1_000_000)],
            170_000,
            Some(vec![collateral.clone()]),
        );
        let cbor = alonzo_tx_cbor(body, false);
        let tx = MultiEraTx::decode_for_era(Era::Alonzo, &cbor).unwrap();

        let utxos = HashMap::from([
            (
                TxoRef(input.transaction_id, input.index as u32),
                owned(MultiEraOutput::from_alonzo_compatible(
                    &alonzo_output(5_000_000),
                    Era::Alonzo,
                )),
            ),
            (
                TxoRef(collateral.transaction_id, collateral.index as u32),
                owned(MultiEraOutput::from_alonzo_compatible(
                    &alonzo_output(2_500_000),
                    Era::Alonzo,
                )),
            ),
        ]);

        assert_eq!(define_tx_fees(&tx, &utxos).unwrap(), 2_500_000);
    }
}
