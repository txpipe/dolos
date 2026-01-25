//! Cardano-specific query helpers extension trait.
//!
//! This module provides `CardanoQueryExt`, an extension trait that adds
//! Cardano-specific query methods to any `Domain` implementation.

use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::conway::{DatumOption, PlutusData},
        traverse::{ComputeHash, MultiEraBlock, OriginalHash},
    },
};

use dolos_core::{
    ArchiveStore, BlockSlot, ChainError, DomainError, EntityKey, QueryHelpers, SparseBlockIter,
    StateStore as _, TxHash, TxoRef,
};

use crate::model::{DatumState, DATUM_NS};

use super::dimensions::archive;
use super::CardanoIndexExt;

/// Extension trait providing Cardano-specific query helpers.
///
/// This trait extends `QueryHelpers` with methods specific to Cardano,
/// such as datum resolution and UTxO spending lookups.
///
/// # Example
///
/// ```ignore
/// use dolos_cardano::indexes::CardanoQueryExt;
///
/// // Iterate over blocks containing an address
/// for result in domain.blocks_by_address(&addr, start, end)? {
///     let (slot, block) = result?;
///     // process block...
/// }
///
/// // Find Plutus data by hash
/// if let Some(datum) = domain.plutus_data(&datum_hash)? {
///     // use datum...
/// }
/// ```
pub trait CardanoQueryExt: QueryHelpers {
    /// Iterate over blocks containing transactions involving an address.
    fn blocks_by_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        self.blocks_by_tag(archive::ADDRESS, address, start_slot, end_slot)
    }

    /// Iterate over blocks containing transactions involving a payment credential.
    fn blocks_by_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        self.blocks_by_tag(archive::PAYMENT, payment, start_slot, end_slot)
    }

    /// Iterate over blocks containing transactions involving a stake credential.
    fn blocks_by_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        self.blocks_by_tag(archive::STAKE, stake, start_slot, end_slot)
    }

    /// Iterate over blocks containing transactions involving an asset.
    fn blocks_by_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        self.blocks_by_tag(archive::ASSET, asset, start_slot, end_slot)
    }

    /// Iterate over blocks containing certificates for an account.
    fn blocks_by_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        self.blocks_by_tag(archive::ACCOUNT_CERTS, account, start_slot, end_slot)
    }

    /// Iterate over blocks containing transactions with a specific metadata label.
    fn blocks_by_metadata(
        &self,
        label: u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        self.blocks_by_tag(
            archive::METADATA,
            &label.to_be_bytes(),
            start_slot,
            end_slot,
        )
    }

    /// Get Plutus data by its datum hash.
    ///
    /// Searches blocks indexed by the datum hash for matching Plutus data.
    /// Checks witness datums, inline datums, and redeemer data.
    fn plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, DomainError> {
        let end_slot = self
            .archive()
            .get_tip()?
            .map(|(slot, _)| slot)
            .unwrap_or_default();

        let slots: Vec<BlockSlot> = self
            .indexes()
            .slots_by_datum(datum_hash.as_slice(), 0, end_slot)?
            .collect::<Result<Vec<_>, _>>()?;

        for slot in slots {
            let Some(raw) = self.archive().get_block_by_slot(&slot)? else {
                continue;
            };

            let block = MultiEraBlock::decode(raw.as_slice())
                .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;

            for tx in block.txs() {
                // Check witness datums
                if let Some(plutus_data) = tx.find_plutus_data(datum_hash) {
                    return Ok(Some(plutus_data.clone().unwrap()));
                }

                // Check inline datums in outputs
                for (_, output) in tx.produces() {
                    if let Some(DatumOption::Data(data)) = output.datum() {
                        if &data.original_hash() == datum_hash {
                            return Ok(Some(data.clone().unwrap().unwrap()));
                        }
                    }
                }

                // Check redeemer data
                for redeemer in tx.redeemers() {
                    if &redeemer.data().compute_hash() == datum_hash {
                        return Ok(Some(redeemer.data().clone()));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Get witness datum bytes by hash from the state store.
    ///
    /// Returns the raw CBOR datum bytes if the datum exists in the state.
    /// Only witness datums (those referenced by `DatumOption::Hash`) are stored;
    /// inline datums are not tracked.
    fn get_datum(&self, datum_hash: &Hash<32>) -> Result<Option<Vec<u8>>, DomainError> {
        let key = EntityKey::from(*datum_hash);
        let datum_state: Option<DatumState> = self.state().read_entity_typed(DATUM_NS, &key)?;
        Ok(datum_state.map(|s| s.bytes))
    }

    /// Get the transaction hash that spent a given UTxO.
    ///
    /// Searches blocks indexed by the spent UTxO reference for the spending transaction.
    fn tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, DomainError> {
        let end_slot = self
            .archive()
            .get_tip()?
            .map(|(slot, _)| slot)
            .unwrap_or_default();

        let slots: Vec<BlockSlot> = self
            .indexes()
            .slots_by_spent_txo(spent_txo, 0, end_slot)?
            .collect::<Result<Vec<_>, _>>()?;

        for slot in slots {
            let Some(raw) = self.archive().get_block_by_slot(&slot)? else {
                continue;
            };

            let block = MultiEraBlock::decode(raw.as_slice())
                .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;

            for tx in block.txs().iter() {
                for input in tx.inputs() {
                    let bytes: Vec<u8> = TxoRef::from(&input).into();
                    if bytes.as_slice() == spent_txo {
                        return Ok(Some(tx.hash()));
                    }
                }
            }
        }

        Ok(None)
    }
}

// Blanket implementation for all QueryHelpers (which includes all Domain types)
impl<D: QueryHelpers> CardanoQueryExt for D {}
