//! Cardano-specific index store extension trait.
//!
//! This module provides `CardanoIndexExt`, an extension trait that adds
//! convenient Cardano-specific methods to any `IndexStore` implementation.

use dolos_core::{BlockSlot, IndexError, IndexStore, UtxoSet};

use super::dimensions::{archive, utxo};

/// Extension trait providing Cardano-specific index queries.
///
/// This trait is automatically implemented for all types implementing `IndexStore`.
/// It provides convenient methods that map Cardano concepts to generic tag lookups.
///
/// # Example
///
/// ```ignore
/// use dolos_cardano::indexes::CardanoIndexExt;
///
/// // domain.indexes() returns an IndexStore implementation
/// let utxos = domain.indexes().utxos_by_address(&address_bytes)?;
/// let utxos = domain.indexes().utxos_by_payment(&payment_cred)?;
/// ```
pub trait CardanoIndexExt: IndexStore {
    // ============ UTxO Filter Queries ============

    /// Get UTxOs by full address.
    fn utxos_by_address(&self, address: &[u8]) -> Result<UtxoSet, IndexError> {
        self.utxos_by_tag(utxo::ADDRESS, address)
    }

    /// Get UTxOs by payment credential.
    fn utxos_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, IndexError> {
        self.utxos_by_tag(utxo::PAYMENT, payment)
    }

    /// Get UTxOs by stake credential.
    fn utxos_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, IndexError> {
        self.utxos_by_tag(utxo::STAKE, stake)
    }

    /// Get UTxOs by native asset policy ID.
    fn utxos_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, IndexError> {
        self.utxos_by_tag(utxo::POLICY, policy)
    }

    /// Get UTxOs by native asset subject (policy + name).
    fn utxos_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, IndexError> {
        self.utxos_by_tag(utxo::ASSET, asset)
    }

    // ============ Archive Slot Queries ============

    /// Iterate over slots of blocks containing transactions involving an address.
    fn slots_by_address(
        &self,
        address: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        self.slots_by_tag(archive::ADDRESS, address, start, end)
    }

    /// Iterate over slots of blocks containing transactions involving a payment credential.
    fn slots_by_payment(
        &self,
        payment: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        self.slots_by_tag(archive::PAYMENT, payment, start, end)
    }

    /// Iterate over slots of blocks containing transactions involving a stake credential.
    fn slots_by_stake(
        &self,
        stake: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        self.slots_by_tag(archive::STAKE, stake, start, end)
    }

    /// Iterate over slots of blocks containing transactions involving an asset.
    fn slots_by_asset(
        &self,
        asset: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        self.slots_by_tag(archive::ASSET, asset, start, end)
    }

    /// Iterate over slots of blocks containing a datum hash.
    fn slots_by_datum(
        &self,
        datum: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        self.slots_by_tag(archive::DATUM, datum, start, end)
    }

    /// Iterate over slots of blocks that spent a specific UTxO.
    fn slots_by_spent_txo(
        &self,
        txo: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        self.slots_by_tag(archive::SPENT_TXO, txo, start, end)
    }

    /// Iterate over slots of blocks containing certificates for an account.
    fn slots_by_account_certs(
        &self,
        account: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        self.slots_by_tag(archive::ACCOUNT_CERTS, account, start, end)
    }

    /// Iterate over slots of blocks containing transactions with a specific metadata label.
    fn slots_by_metadata(
        &self,
        label: u64,
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        self.slots_by_tag(archive::METADATA, &label.to_be_bytes(), start, end)
    }
}

// Blanket implementation for all IndexStore implementations
impl<T: IndexStore> CardanoIndexExt for T {}
