//! Cardano-specific store extension traits.
//!
//! This module provides `CardanoStateIndexExt`, which adds the live-UTxO
//! lookups to any `StateStore`, and `CardanoArchiveIndexExt`, which adds the
//! history lookups to any `ArchiveStore`.

use dolos_core::{ArchiveError, ArchiveStore, BlockSlot, StateError, StateStore, UtxoSet};

use super::dimensions::{archive, utxo};

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
