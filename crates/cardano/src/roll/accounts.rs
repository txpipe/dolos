use std::collections::HashSet;

use dolos_core::{ChainError, Genesis, TxOrder};

use super::WorkDeltas;
use pallas::ledger::primitives::alonzo::{
    InstantaneousRewardSource, InstantaneousRewardTarget, MoveInstantaneousReward,
};
use pallas::ledger::primitives::{Epoch, StakeCredential};
use pallas::ledger::{
    addresses::Address,
    traverse::{
        MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraProposal, MultiEraTx,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    pallas_extras, roll::BlockVisitor, AccountAddressSet, AccountSeen, ControlledAmountDec,
    ControlledAmountInc, EnqueueMir, PParamsSet, StageAccountAddress, StakeDelegation,
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
pub struct AccountVisitor {
    deposit: Option<u64>,
    epoch: Option<Epoch>,
    /// Protocol version for determining MIR accumulation behavior.
    /// Pre-Alonzo (< 5): MIRs overwrite previous values.
    /// Alonzo+ (>= 5): MIRs accumulate.
    protocol_version: Option<u16>,

    /// Account addresses already staged by this block: a repeat changes no
    /// staged row but would still cost a WAL delta.
    staged: HashSet<(StakeCredential, Vec<u8>)>,
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
        block: &MultiEraBlock,
        _: &MultiEraTx,
        order: &TxOrder,
        index: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        let address = output.address().expect("valid address");
        let epoch = self.epoch.expect("value set in root");

        if let Some(account) = pallas_extras::address_account(&address) {
            let bytes = address.to_vec();

            if self.staged.insert((account.clone(), bytes.clone())) {
                deltas.add_for_entity(StageAccountAddress::new(AccountAddressSet {
                    account,
                    address: bytes,
                    epoch,
                    slot: block.slot(),
                    tx_index: *order as u32,
                    output_index: index,
                }));
            }
        }

        let Some((cred, is_pointer)) = pallas_extras::address_as_stake_cred(&address) else {
            return Ok(());
        };

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

        if let Some(cert) = pallas_extras::cert_as_pool_registration(cert) {
            let cred = pallas_extras::parse_reward_account(&cert.reward_account)
                .ok_or(ChainError::InvalidPoolParams)?;
            deltas.add_for_entity(AccountSeen::new(cred, epoch));

            for owner in cert.pool_owners {
                deltas.add_for_entity(AccountSeen::new(StakeCredential::AddrKeyhash(owner), epoch));
            }
        }

        if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
            let deposit = self
                .deposit
                .ok_or(ChainError::StartingPParamMissing("key_deposit"))?;
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

    fn visit_proposal(
        &mut self,
        deltas: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        proposal: &MultiEraProposal,
        _: usize,
    ) -> Result<(), ChainError> {
        let Some(proposal) = proposal.as_conway() else {
            return Ok(());
        };
        let cred = pallas_extras::parse_reward_account(&proposal.reward_account)
            .ok_or(ChainError::InvalidProposalParams)?;
        let epoch = self.epoch.expect("value set in root");
        deltas.add_for_entity(AccountSeen::new(cred, epoch));
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
}

#[cfg(test)]
mod tests {
    use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};

    use super::*;

    /// A chain that opens on Byron's pparams carries no key deposit, so a
    /// stake registration reached before the Shelley fork is a typed error
    /// rather than a panic.
    #[test]
    fn stake_registration_without_key_deposit_is_a_typed_error() {
        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            ..Default::default()
        });
        let block = MultiEraBlock::decode(&blocks[0]).unwrap();

        let genesis = crate::include::devnet::load();
        let pparams = crate::forks::from_byron_genesis(&genesis.byron);

        let mut visitor = AccountVisitor::default();
        let mut deltas = WorkDeltas::default();

        visitor
            .visit_root(&mut deltas, &block, &genesis, &pparams, 0, 0, 0)
            .unwrap();

        let txs = block.txs();
        let cert = txs[0].certs().into_iter().next().unwrap();
        assert!(pallas_extras::cert_as_stake_registration(&cert).is_some());

        let err = visitor
            .visit_cert(&mut deltas, &block, &txs[0], &0, &cert)
            .unwrap_err();

        assert!(matches!(
            err,
            ChainError::StartingPParamMissing("key_deposit")
        ));
    }
}
