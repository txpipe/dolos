//! Cardano-specific index delta builder.
//!
//! This module provides `CardanoIndexDeltaBuilder` for constructing `IndexDelta`
//! structures from Cardano block data.

use dolos_core::{
    ArchiveIndexDelta, BlockSlot, ChainPoint, EraCbor, IndexDelta, Tag, TxoRef, UtxoIndexDelta,
    UtxoSetDelta,
};
use pallas::{
    codec::minicbor,
    ledger::{
        addresses::Address,
        primitives::conway::DatumOption,
        traverse::{MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraValue},
    },
};

use super::dimensions::{archive, utxo};
use crate::pallas_extras;

/// Builder for constructing `IndexDelta` from Cardano block data.
///
/// This builder accumulates index tags as blocks are processed and produces
/// a complete `IndexDelta` that can be applied to an `IndexStore`.
///
/// # Example
///
/// ```ignore
/// let mut builder = CardanoIndexDeltaBuilder::new(cursor_point);
///
/// // Start processing a block
/// builder.start_block(slot, block_hash, Some(block_number));
///
/// // Add tags as block is traversed
/// builder.add_tx_hash(tx_hash);
/// builder.add_address(&address);
/// builder.add_produced_utxo(txo_ref, &output);
///
/// // Build the final delta
/// let delta = builder.build();
/// ```
pub struct CardanoIndexDeltaBuilder {
    delta: IndexDelta,
}

impl CardanoIndexDeltaBuilder {
    /// Create a new builder with the given cursor position.
    pub fn new(cursor: ChainPoint) -> Self {
        Self {
            delta: IndexDelta {
                cursor,
                ..Default::default()
            },
        }
    }

    // ============ UTxO Operations ============

    /// Add a produced UTxO to the delta.
    ///
    /// Extracts tags from the output (address, assets) and adds them to the UTxO
    /// filter delta for insertion.
    pub fn add_produced_utxo(&mut self, txo_ref: TxoRef, output: &MultiEraOutput) {
        let tags = Self::extract_utxo_tags(output);
        self.delta.utxo.produced.push((txo_ref, tags));
    }

    /// Add a consumed UTxO to the delta.
    ///
    /// Extracts tags from the output (address, assets) and adds them to the UTxO
    /// filter delta for removal.
    pub fn add_consumed_utxo(&mut self, txo_ref: TxoRef, output: &MultiEraOutput) {
        let tags = Self::extract_utxo_tags(output);
        self.delta.utxo.consumed.push((txo_ref, tags));
    }

    /// Extract UTxO filter tags from an output.
    fn extract_utxo_tags(output: &MultiEraOutput) -> Vec<Tag> {
        let mut tags = Vec::new();

        // Address tags
        if let Ok(addr) = output.address() {
            match addr {
                Address::Shelley(x) => {
                    tags.push(Tag::new(utxo::ADDRESS, x.to_vec()));
                    tags.push(Tag::new(utxo::PAYMENT, x.payment().to_vec()));
                    if let Some(stake) = pallas_extras::shelley_address_to_stake_address(&x) {
                        tags.push(Tag::new(utxo::STAKE, stake.to_vec()));
                    }
                }
                Address::Stake(x) => {
                    tags.push(Tag::new(utxo::ADDRESS, x.to_vec()));
                    tags.push(Tag::new(utxo::STAKE, x.to_vec()));
                }
                Address::Byron(x) => {
                    tags.push(Tag::new(utxo::ADDRESS, x.to_vec()));
                }
            }
        }

        // Asset tags
        for ma in output.value().assets() {
            tags.push(Tag::new(utxo::POLICY, ma.policy().to_vec()));
            for asset in ma.assets() {
                let mut subject = asset.policy().to_vec();
                subject.extend(asset.name());
                tags.push(Tag::new(utxo::ASSET, subject));
            }
        }

        tags
    }

    // ============ Archive Block Operations ============

    /// Start a new block in the archive delta.
    ///
    /// Must be called before adding block-level tags.
    pub fn start_block(&mut self, slot: BlockSlot, block_hash: Vec<u8>, number: Option<u64>) {
        self.delta.archive.push(ArchiveIndexDelta {
            slot,
            block_hash,
            block_number: number,
            tx_hashes: Vec::new(),
            tags: Vec::new(),
        });
    }

    /// Get mutable reference to the current block delta.
    fn current_block(&mut self) -> &mut ArchiveIndexDelta {
        self.delta
            .archive
            .last_mut()
            .expect("must call start_block before adding tags")
    }

    /// Add a transaction hash to the current block.
    pub fn add_tx_hash(&mut self, hash: Vec<u8>) {
        self.current_block().tx_hashes.push(hash);
    }

    /// Add address tags to the current block.
    pub fn add_address(&mut self, addr: &Address) {
        let block = self.current_block();
        match addr {
            Address::Shelley(x) => {
                block.tags.push(Tag::new(archive::ADDRESS, x.to_vec()));
                block
                    .tags
                    .push(Tag::new(archive::PAYMENT, x.payment().to_vec()));
                if let Some(stake) = pallas_extras::shelley_address_to_stake_address(x) {
                    block.tags.push(Tag::new(archive::STAKE, stake.to_vec()));
                }
            }
            Address::Stake(x) => {
                block.tags.push(Tag::new(archive::ADDRESS, x.to_vec()));
                block.tags.push(Tag::new(archive::STAKE, x.to_vec()));
            }
            Address::Byron(x) => {
                block.tags.push(Tag::new(archive::ADDRESS, x.to_vec()));
            }
        }
    }

    /// Add asset tags to the current block.
    pub fn add_assets(&mut self, assets: &MultiEraValue) {
        let block = self.current_block();
        for ma in assets.assets() {
            block
                .tags
                .push(Tag::new(archive::POLICY, ma.policy().to_vec()));
            for asset in ma.assets() {
                let mut subject = asset.policy().to_vec();
                subject.extend(asset.name());
                block.tags.push(Tag::new(archive::ASSET, subject));
            }
        }
    }

    /// Add a datum tag to the current block.
    pub fn add_datum(&mut self, datum: &DatumOption) {
        let hash = match datum {
            DatumOption::Hash(hash) => hash.to_vec(),
            DatumOption::Data(data) => {
                use pallas::ledger::traverse::OriginalHash;
                data.original_hash().to_vec()
            }
        };
        self.current_block()
            .tags
            .push(Tag::new(archive::DATUM, hash));
    }

    /// Add a datum hash directly to the current block.
    pub fn add_datum_hash(&mut self, hash: Vec<u8>) {
        self.current_block()
            .tags
            .push(Tag::new(archive::DATUM, hash));
    }

    /// Add a spent TxO reference to the current block.
    pub fn add_spent_input(&mut self, input: &MultiEraInput) {
        let txo_ref: TxoRef = input.into();
        let bytes: Vec<u8> = txo_ref.into();
        self.current_block()
            .tags
            .push(Tag::new(archive::SPENT_TXO, bytes));
    }

    /// Add a script hash to the current block.
    pub fn add_script_hash(&mut self, hash: Vec<u8>) {
        self.current_block()
            .tags
            .push(Tag::new(archive::SCRIPT, hash));
    }

    /// Add certificate tags to the current block.
    pub fn add_cert(&mut self, cert: &MultiEraCert) {
        if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
            let bytes = minicbor::to_vec(&cred).unwrap();
            self.current_block()
                .tags
                .push(Tag::new(archive::ACCOUNT_CERTS, bytes));
        }

        if let Some(cred) = pallas_extras::cert_as_stake_deregistration(cert) {
            let bytes = minicbor::to_vec(&cred).unwrap();
            self.current_block()
                .tags
                .push(Tag::new(archive::ACCOUNT_CERTS, bytes));
        }

        if let Some(deleg) = pallas_extras::cert_as_stake_delegation(cert) {
            let bytes = minicbor::to_vec(&deleg.delegator).unwrap();
            self.current_block()
                .tags
                .push(Tag::new(archive::ACCOUNT_CERTS, bytes));
        }
    }

    /// Add a metadata label to the current block.
    pub fn add_metadata_label(&mut self, label: u64) {
        self.current_block()
            .tags
            .push(Tag::new(archive::METADATA, label.to_be_bytes().to_vec()));
    }

    /// Build the final `IndexDelta`.
    pub fn build(self) -> IndexDelta {
        self.delta
    }

    /// Get a reference to the UTxO delta (for inspection/testing).
    pub fn utxo_delta(&self) -> &UtxoIndexDelta {
        &self.delta.utxo
    }

    /// Get a reference to the archive deltas (for inspection/testing).
    pub fn archive_deltas(&self) -> &[ArchiveIndexDelta] {
        &self.delta.archive
    }

    // ============ Batch UTxO Operations (for genesis/import) ============

    /// Add produced UTxOs from a UtxoSetDelta.
    ///
    /// This is used for genesis bootstrap and bulk imports where UTxOs are
    /// provided as raw CBOR rather than parsed block outputs.
    pub fn add_produced_utxos_from_delta(&mut self, utxo_delta: &UtxoSetDelta) {
        for (txo_ref, era_cbor) in utxo_delta.produced_utxo.iter() {
            if let Some(tags) = Self::extract_tags_from_era_cbor(era_cbor) {
                self.delta.utxo.produced.push((txo_ref.clone(), tags));
            }
        }
    }

    /// Add consumed UTxOs from a UtxoSetDelta.
    ///
    /// This is used for bulk operations where UTxOs are provided as raw CBOR.
    pub fn add_consumed_utxos_from_delta(&mut self, utxo_delta: &UtxoSetDelta) {
        for (txo_ref, era_cbor) in utxo_delta.consumed_utxo.iter() {
            if let Some(tags) = Self::extract_tags_from_era_cbor(era_cbor) {
                self.delta.utxo.consumed.push((txo_ref.clone(), tags));
            }
        }
    }

    /// Extract UTxO filter tags from raw EraCbor.
    fn extract_tags_from_era_cbor(era_cbor: &EraCbor) -> Option<Vec<Tag>> {
        let output = MultiEraOutput::try_from(era_cbor).ok()?;
        Some(Self::extract_utxo_tags(&output))
    }
}

/// Build an `IndexDelta` from a `UtxoSetDelta` (for genesis/bulk import).
///
/// This creates an `IndexDelta` containing only UTxO filter changes,
/// with no archive index entries. Useful for genesis bootstrap.
pub fn index_delta_from_utxo_delta(cursor: ChainPoint, utxo_delta: &UtxoSetDelta) -> IndexDelta {
    let mut builder = CardanoIndexDeltaBuilder::new(cursor);
    builder.add_produced_utxos_from_delta(utxo_delta);
    builder.add_consumed_utxos_from_delta(utxo_delta);
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::ChainPoint;
    use pallas::crypto::hash::Hash;
    use pallas::ledger::addresses::{
        Network, ShelleyAddress, ShelleyDelegationPart, ShelleyPaymentPart,
    };

    fn test_shelley_address() -> Address {
        Address::Shelley(ShelleyAddress::new(
            Network::Testnet,
            ShelleyPaymentPart::Key([1; 28].as_slice().into()),
            ShelleyDelegationPart::Key([2; 28].as_slice().into()),
        ))
    }

    #[test]
    fn test_builder_basic() {
        let cursor = ChainPoint::Specific(100, Hash::new([0; 32]));
        let mut builder = CardanoIndexDeltaBuilder::new(cursor.clone());

        builder.start_block(100, vec![0; 32], Some(50));
        builder.add_tx_hash(vec![1; 32]);
        builder.add_address(&test_shelley_address());

        let delta = builder.build();

        assert_eq!(delta.cursor, cursor);
        assert_eq!(delta.archive.len(), 1);
        assert_eq!(delta.archive[0].slot, 100);
        assert_eq!(delta.archive[0].block_number, Some(50));
        assert_eq!(delta.archive[0].tx_hashes.len(), 1);
        // Shelley address produces 3 tags: full, payment, stake
        assert_eq!(delta.archive[0].tags.len(), 3);
    }
}
