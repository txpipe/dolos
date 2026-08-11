use std::collections::BTreeMap;

use dolos_core::{ChainError, Genesis, TxOrder};

use super::WorkDeltas;
use pallas::codec::utils::Bytes;
use pallas::ledger::primitives::alonzo::{
    InstantaneousRewardSource, InstantaneousRewardTarget, MoveInstantaneousReward,
};
use pallas::ledger::primitives::{Epoch, StakeCredential};
use pallas::ledger::{
    addresses::Address,
    traverse::{MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraTx},
};
use serde::{Deserialize, Serialize};

use crate::{
    pallas_extras, roll::BlockVisitor, AccountActivityRecord, AccountAssetActivityRecord,
    ControlledAmountDec, ControlledAmountInc, EnqueueMir, PParamsSet, StakeDelegation,
    StakeDeregistration, StakeRegistration, VoteDelegation, WithdrawalInc,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackSeenAddresses {
    cred: pallas::ledger::primitives::StakeCredential,
    full_address: Vec<u8>,
    full_address_new: Option<bool>,
}

impl TrackSeenAddresses {
    pub fn new(cred: pallas::ledger::primitives::StakeCredential, full_address: Address) -> Self {
        Self {
            cred,
            full_address: full_address.to_vec(),
            full_address_new: None,
        }
    }
}

#[derive(Default, Clone)]
struct TxActivity {
    received_lovelace: u128,
    received_assets: BTreeMap<Bytes, u128>,
    sent_lovelace: u128,
    sent_assets: BTreeMap<Bytes, u128>,
}

impl TxActivity {
    fn add_output(&mut self, output: &MultiEraOutput, sent: bool) {
        let value = output.value();

        let (lovelace, assets) = if sent {
            (&mut self.sent_lovelace, &mut self.sent_assets)
        } else {
            (&mut self.received_lovelace, &mut self.received_assets)
        };

        *lovelace += value.coin() as u128;

        for ma in value.assets() {
            for asset in ma.assets() {
                let mut unit = ma.policy().to_vec();
                unit.extend(asset.name());
                let amount = asset.output_coin().unwrap_or_default() as u128;
                *assets.entry(Bytes::from(unit)).or_insert(0) += amount;
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct AccountVisitor {
    deposit: Option<u64>,
    epoch: Option<Epoch>,
    /// Protocol version for determining MIR accumulation behavior.
    /// Pre-Alonzo (< 5): MIRs overwrite previous values.
    /// Alonzo+ (>= 5): MIRs accumulate.
    protocol_version: Option<u16>,
    tx_activity: BTreeMap<StakeCredential, TxActivity>,
}

impl AccountVisitor {
    fn flush_tx_activity(&mut self, deltas: &mut WorkDeltas) {
        for (cred, activity) in std::mem::take(&mut self.tx_activity) {
            deltas.add_for_entity(AccountActivityRecord::new(
                cred.clone(),
                activity.received_lovelace,
                activity.sent_lovelace,
            ));

            let assets = AccountAssetActivityRecord::new(
                cred,
                activity.received_assets.into_iter().collect(),
                activity.sent_assets.into_iter().collect(),
            );

            // lovelace-only txs skip the asset entity
            if !assets.is_empty() {
                deltas.add_for_entity(assets);
            }
        }
    }
}

impl BlockVisitor for AccountVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &Genesis,
        pparams: &PParamsSet,
        epoch: Epoch,
        _: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        self.deposit = pparams.ensure_key_deposit().ok();
        self.epoch = Some(epoch);
        self.protocol_version = pparams.protocol_major();
        Ok(())
    }

    fn visit_tx(
        &mut self,
        deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &std::collections::HashMap<dolos_core::TxoRef, crate::owned::OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        self.flush_tx_activity(deltas);

        Ok(())
    }

    fn visit_input(
        &mut self,
        deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        let address = resolved.address().unwrap();

        let Some((cred, is_pointer)) = pallas_extras::address_as_stake_cred(&address) else {
            return Ok(());
        };

        // pointer addresses are excluded to match the account endpoints
        if !is_pointer {
            self.tx_activity
                .entry(cred.clone())
                .or_default()
                .add_output(resolved, true);
        }

        deltas.add_for_entity(ControlledAmountDec::new(
            cred,
            is_pointer,
            resolved.value().coin(),
        ));

        Ok(())
    }

    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        let address = output.address().expect("valid address");
        let epoch = self.epoch.expect("value set in root");

        let Some((cred, is_pointer)) = pallas_extras::address_as_stake_cred(&address) else {
            return Ok(());
        };

        if !is_pointer {
            self.tx_activity
                .entry(cred.clone())
                .or_default()
                .add_output(output, false);
        }

        deltas.add_for_entity(ControlledAmountInc::new(
            cred.clone(),
            is_pointer,
            output.value().coin(),
            epoch,
        ));

        Ok(())
    }

    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        order: &TxOrder,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        let epoch = self.epoch.expect("value set in root");

        if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
            let deposit = self.deposit.expect("value set in root");
            let epoch = self.epoch.expect("value set in root");
            deltas.add_for_entity(StakeRegistration::new(cred, block.slot(), epoch, deposit));
        }

        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            deltas.add_for_entity(StakeDelegation::new(cert.delegator, cert.pool, epoch));
        }

        if let Some(cred) = pallas_extras::cert_as_stake_deregistration(cert) {
            deltas.add_for_entity(StakeDeregistration::new(cred, block.slot(), epoch));
        }

        if let Some(cert) = pallas_extras::cert_as_vote_delegation(cert) {
            deltas.add_for_entity(VoteDelegation::new(
                cert.delegator,
                cert.drep,
                block.slot(),
                *order,
                epoch,
            ));
        }

        if let Some(cert) = pallas_extras::cert_as_mir_certificate(cert) {
            let MoveInstantaneousReward { source, target, .. } = cert;

            if let InstantaneousRewardTarget::StakeCredentials(creds) = target {
                // Pre-Alonzo (protocol < 5): MIRs overwrite previous values (Map.union
                // semantics) Alonzo+ (protocol >= 5): MIRs accumulate
                // (Map.unionWith (<>) semantics) TODO: move this logic out of
                // the visitor and into a module more ledger-related.
                let overwrite = self.protocol_version.unwrap_or(0) < 5;

                for (cred, amount) in creds {
                    let amount = amount.max(0) as u64;
                    // Store pending MIR to be applied at EWRAP (not immediately)
                    // This ensures MIRs are only applied to accounts that are
                    // registered at epoch boundary, matching the Cardano ledger.
                    match source {
                        InstantaneousRewardSource::Reserves => {
                            deltas
                                .add_for_entity(EnqueueMir::from_reserves(cred, amount, overwrite));
                        }
                        InstantaneousRewardSource::Treasury => {
                            deltas
                                .add_for_entity(EnqueueMir::from_treasury(cred, amount, overwrite));
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn visit_withdrawal(
        &mut self,
        deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        account: &[u8],
        amount: u64,
    ) -> Result<(), ChainError> {
        let address = Address::from_bytes(account)?;

        let Some((cred, _)) = pallas_extras::address_as_stake_cred(&address) else {
            return Ok(());
        };

        deltas.add_for_entity(WithdrawalInc::new(cred, amount));

        Ok(())
    }

    fn flush(&mut self, deltas: &mut WorkDeltas) -> Result<(), ChainError> {
        self.flush_tx_activity(deltas);

        Ok(())
    }
}
