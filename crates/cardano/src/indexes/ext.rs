//! Cardano-specific store extension traits.
//!
//! This module provides `CardanoStateIndexExt`, which adds the live-UTxO
//! lookups to any `StateStore`, and `CardanoArchiveIndexExt`, which adds the
//! history lookups to any `ArchiveStore`.

use dolos_core::{keys, ArchiveError, ArchiveStore, BlockSlot, StateError, StateStore, UtxoSet};
use pallas::ledger::{
    primitives::{Epoch, StakeCredential},
    traverse::MultiEraBlock,
};

use super::dimensions::{archive, utxo};
use crate::{pallas_extras, CertPosition, PoolHash, ProposalKey};

/// What a pool certificate did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolCert {
    /// A registration, or an update of a registered pool's parameters.
    Registration,
    /// A retirement taking effect at the start of the epoch it names.
    Retirement(Epoch),
}

/// The certificates of `operator` in the valid transactions of `block`, in
/// chain order.
pub fn pool_certs_in(block: &MultiEraBlock, operator: &PoolHash) -> Vec<(CertPosition, PoolCert)> {
    let mut out = Vec::new();

    for (tx_index, tx) in block.txs().iter().enumerate() {
        if !tx.is_valid() {
            continue;
        }

        for (cert_index, cert) in tx.certs().iter().enumerate() {
            let at = (block.slot(), tx_index, cert_index);

            if let Some(reg) = pallas_extras::cert_as_pool_registration(cert) {
                if reg.operator == *operator {
                    out.push((at, PoolCert::Registration));
                }
            }

            if let Some(retire) = pallas_extras::cert_as_pool_retirement(cert) {
                if retire.operator == *operator {
                    out.push((at, PoolCert::Retirement(retire.epoch)));
                }
            }
        }
    }

    out
}

/// Extension trait providing Cardano-specific live-UTxO queries.
///
/// This trait is automatically implemented for all types implementing
/// `StateStore`. It provides convenient methods that map Cardano concepts to
/// generic tag lookups over the UTxO set.
///
/// # Example
///
/// ```ignore
/// use dolos_cardano::indexes::CardanoStateIndexExt;
///
/// // domain.state() returns a StateStore implementation
/// let utxos = domain.state().utxos_by_address(&address_bytes)?;
/// let utxos = domain.state().utxos_by_payment(&payment_cred)?;
/// ```
pub trait CardanoStateIndexExt: StateStore {
    /// Get UTxOs by full address.
    fn utxos_by_address(&self, address: &[u8]) -> Result<UtxoSet, StateError> {
        self.utxos_by_tag(utxo::ADDRESS, address)
    }

    /// Get UTxOs by payment credential.
    fn utxos_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, StateError> {
        self.utxos_by_tag(utxo::PAYMENT, payment)
    }

    /// Get UTxOs by stake credential.
    fn utxos_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, StateError> {
        self.utxos_by_tag(utxo::STAKE, stake)
    }

    /// Get UTxOs by native asset policy ID.
    fn utxos_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, StateError> {
        self.utxos_by_tag(utxo::POLICY, policy)
    }

    /// Get UTxOs by native asset subject (policy + name).
    fn utxos_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, StateError> {
        self.utxos_by_tag(utxo::ASSET, asset)
    }

    /// Get UTxOs that carry a script as their reference script.
    fn utxos_by_script_ref(&self, script_hash: &[u8]) -> Result<UtxoSet, StateError> {
        self.utxos_by_tag(utxo::SCRIPT_REF, script_hash)
    }
}

impl<T: StateStore> CardanoStateIndexExt for T {}

/// Extension trait providing Cardano-specific archive index queries.
///
/// This trait is automatically implemented for all types implementing
/// `ArchiveStore`. It provides convenient methods that map Cardano concepts
/// to generic tag lookups.
///
/// # Example
///
/// ```ignore
/// use dolos_cardano::indexes::CardanoArchiveIndexExt;
///
/// // domain.archive() returns an ArchiveStore implementation
/// let slots = domain.archive().slots_by_address(&address_bytes, start, end)?;
/// ```
pub trait CardanoArchiveIndexExt: ArchiveStore {
    // ============ Archive Slot Queries ============

    /// Iterate over slots of blocks containing transactions involving an
    /// address.
    fn slots_by_address(
        &self,
        address: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::ADDRESS, address, start, end)
    }

    /// Iterate over slots of blocks containing transactions involving a payment
    /// credential.
    fn slots_by_payment(
        &self,
        payment: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::PAYMENT, payment, start, end)
    }

    /// Iterate over slots of blocks containing transactions involving a stake
    /// credential.
    fn slots_by_stake(
        &self,
        stake: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::STAKE, stake, start, end)
    }

    /// Iterate over slots of blocks containing transactions involving an asset.
    fn slots_by_asset(
        &self,
        asset: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::ASSET, asset, start, end)
    }

    /// Iterate over slots of blocks that contain transactions for assets of a
    /// policy.
    fn slots_by_policy(
        &self,
        policy: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::POLICY, policy, start, end)
    }

    /// Iterate over slots of blocks containing a datum hash.
    fn slots_by_datum(
        &self,
        datum: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::DATUM, datum, start, end)
    }

    /// Iterate over slots of blocks that spent a specific UTxO.
    fn slots_by_spent_txo(
        &self,
        txo: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::SPENT_TXO, txo, start, end)
    }

    /// Iterate over slots of blocks containing certificates for an account.
    fn slots_by_account_certs(
        &self,
        account: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::ACCOUNT_CERTS, account, start, end)
    }

    /// Iterate over slots of blocks containing certificates for a pool.
    fn slots_by_pool_certs(
        &self,
        pool: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::POOL_CERTS, pool, start, end)
    }

    /// Where `operator` first registered, in a valid transaction. The
    /// `pool_certs` tag is written for phase-2-invalid transactions too, so
    /// each tagged block is decoded until one holds a valid registration.
    fn pool_first_registration(
        &self,
        operator: &PoolHash,
    ) -> Result<Option<CertPosition>, ArchiveError> {
        for slot in self.slots_by_pool_certs(operator.as_ref(), 0, BlockSlot::MAX)? {
            let Some(body) = self.get_block_by_slot(&slot?)? else {
                continue;
            };

            let block = MultiEraBlock::decode(&body)?;

            let first = pool_certs_in(&block, operator)
                .into_iter()
                .find(|(_, cert)| *cert == PoolCert::Registration);

            if let Some((at, _)) = first {
                return Ok(Some(at));
            }
        }

        Ok(None)
    }

    /// The newest certificate of `operator` in a valid transaction of a block
    /// before `slot`: what the pool was at the end of the previous block.
    fn pool_cert_before(
        &self,
        operator: &PoolHash,
        slot: BlockSlot,
    ) -> Result<Option<PoolCert>, ArchiveError> {
        let Some(end) = slot.checked_sub(1) else {
            return Ok(None);
        };

        for slot in self.slots_by_pool_certs(operator.as_ref(), 0, end)?.rev() {
            let Some(body) = self.get_block_by_slot(&slot?)? else {
                continue;
            };

            let block = MultiEraBlock::decode(&body)?;

            if let Some((_, cert)) = pool_certs_in(&block, operator).pop() {
                return Ok(Some(cert));
            }
        }

        Ok(None)
    }

    /// Iterate over slots of blocks containing certificates for a DRep.
    ///
    /// The key is the CIP-129 DRep id bytes: the key-or-script header, then
    /// the credential hash.
    fn slots_by_drep_certs(
        &self,
        drep: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::DREP_CERTS, drep, start, end)
    }

    fn slots_by_committee_certs(
        &self,
        cold: &StakeCredential,
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(
            archive::COMMITTEE_CERTS,
            &crate::pallas_extras::committee_cold_id_bytes(cold),
            start,
            end,
        )
    }

    /// Iterate over slots of blocks containing withdrawals for an account.
    fn slots_by_account_withdrawals(
        &self,
        account: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::ACCOUNT_WITHDRAWALS, account, start, end)
    }

    /// Iterate over slots of blocks containing transactions with a specific
    /// metadata label.
    fn slots_by_metadata(
        &self,
        label: u64,
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::METADATA, &label.to_be_bytes(), start, end)
    }

    /// Iterate over slots of blocks that a pool minted.
    ///
    /// The key is the pool operator hash. Byron blocks are never listed.
    fn slots_by_pool_blocks(
        &self,
        pool: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::POOL_BLOCKS, pool, start, end)
    }

    /// Iterate over slots of blocks containing governance votes cast by a
    /// voter.
    ///
    /// The key is `pallas_extras::voter_id_bytes` of the voter. Blocks whose
    /// only votes of the voter sit in phase-2-invalid transactions are listed
    /// too.
    fn slots_by_voter_votes(
        &self,
        voter: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::VOTER_VOTES, voter, start, end)
    }

    /// Iterate over slots of blocks containing governance votes on an action.
    ///
    /// Blocks whose only votes on the action sit in phase-2-invalid
    /// transactions are listed too.
    fn slots_by_action_votes(
        &self,
        action: &ProposalKey,
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(archive::ACTION_VOTES, &keys::encode(action), start, end)
    }

    fn slots_by_committee_votes(
        &self,
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.slots_by_tag(
            archive::COMMITTEE_VOTES,
            archive::COMMITTEE_VOTES_KEY,
            start,
            end,
        )
    }

    // ============ Bulk Export ============

    /// Iterate every archive tag record in `slots`, across every Cardano
    /// archive dimension.
    ///
    /// [`ArchiveStore::iter_archive_tags`] takes the dimension list because
    /// the storage layer is chain-agnostic and cannot know it — stores keep a
    /// hash of the dimension name, not the name. That makes `archive::ALL` the
    /// caller's to supply, and every export call site a place the list can
    /// drift out of.
    ///
    /// This is that list, once. `slots` is half-open, as it is on the method
    /// underneath.
    fn iter_all_archive_tags(
        &self,
        slots: std::ops::Range<BlockSlot>,
    ) -> Result<Self::TagIter, ArchiveError> {
        self.iter_archive_tags(&archive::ALL, slots)
    }
}

impl<T: ArchiveStore> CardanoArchiveIndexExt for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::{builtin::MemoryArchiveStore, ArchiveIndexDelta, ArchiveWriter, Tag};
    use pallas::crypto::hash::Hash;

    fn store_with_tags(tags: Vec<(BlockSlot, Vec<Tag>)>) -> MemoryArchiveStore {
        let store = MemoryArchiveStore::new(crate::model::build_log_schema());
        let deltas: Vec<_> = tags
            .into_iter()
            .map(|(slot, tags)| ArchiveIndexDelta {
                slot,
                block_hash: vec![slot as u8; 32],
                block_number: Some(slot),
                tx_hashes: vec![],
                tags,
            })
            .collect();
        let writer = store.start_writer().unwrap();
        writer.apply_index(&deltas).unwrap();
        writer.commit().unwrap();
        store
    }

    #[test]
    fn committee_certificate_lookup_uses_cold_cip129_bytes_and_slot_bounds() {
        let hash = Hash::new([0x99; 28]);
        let key_id = [&[0x12u8][..], hash.as_slice()].concat();
        let script_id = [&[0x13u8][..], hash.as_slice()].concat();
        let store = store_with_tags(vec![
            (90, vec![Tag::new(archive::COMMITTEE_CERTS, key_id.clone())]),
            (100, vec![Tag::new(archive::COMMITTEE_CERTS, key_id)]),
            (110, vec![Tag::new(archive::COMMITTEE_CERTS, script_id)]),
        ]);

        assert_eq!(
            store
                .slots_by_committee_certs(&StakeCredential::AddrKeyhash(hash), 100, 110)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            [100]
        );
        assert_eq!(
            store
                .slots_by_committee_certs(&StakeCredential::ScriptHash(hash), 100, 110)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            [110]
        );
        assert!(store
            .slots_by_committee_certs(&StakeCredential::ScriptHash(hash), 100, 109)
            .unwrap()
            .next()
            .is_none());
    }

    #[test]
    fn committee_vote_lookup_uses_fixed_key_and_slot_bounds() {
        let store = store_with_tags(vec![
            (
                90,
                vec![Tag::new(archive::COMMITTEE_VOTES, b"committee".to_vec())],
            ),
            (
                100,
                vec![Tag::new(archive::COMMITTEE_VOTES, b"committee".to_vec())],
            ),
            (
                105,
                vec![Tag::new(archive::COMMITTEE_VOTES, b"other".to_vec())],
            ),
            (
                110,
                vec![Tag::new(archive::COMMITTEE_VOTES, b"committee".to_vec())],
            ),
        ]);

        assert_eq!(
            store
                .slots_by_committee_votes(100, 110)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap(),
            [100, 110]
        );
        assert!(store
            .slots_by_committee_votes(101, 109)
            .unwrap()
            .next()
            .is_none());
    }
}
