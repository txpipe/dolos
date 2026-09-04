//! Cardano-specific index delta builder.
//!
//! This module provides `CardanoIndexDeltaBuilder` for constructing
//! `IndexDelta` structures from Cardano block data.

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
    /// Extracts tags from the output (address, assets) and adds them to the
    /// UTxO filter delta for insertion.
    pub fn add_produced_utxo(&mut self, txo_ref: TxoRef, output: &MultiEraOutput) {
        let tags = Self::extract_utxo_tags(output);
        self.delta.utxo.produced.push((txo_ref, tags));
    }

    /// Add a consumed UTxO to the delta.
    ///
    /// Extracts tags from the output (address, assets) and adds them to the
    /// UTxO filter delta for removal.
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

        // Reference script tag
        if let Some(script_ref) = output.script_ref() {
            tags.push(Tag::new(
                utxo::SCRIPT_REF,
                pallas_extras::script_ref_hash(&script_ref).to_vec(),
            ));
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

    /// Add the hash of a script that a redeemer executed to the current
    /// block.
    pub fn add_redeemer_script(&mut self, hash: Vec<u8>) {
        self.current_block()
            .tags
            .push(Tag::new(archive::SCRIPT_REDEEMERS, hash));
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

        if let Some(cert) = pallas_extras::cert_as_pool_registration(cert) {
            self.current_block()
                .tags
                .push(Tag::new(archive::POOL_CERTS, cert.operator.to_vec()));
        }

        if let Some(cert) = pallas_extras::cert_as_pool_retirement(cert) {
            self.current_block()
                .tags
                .push(Tag::new(archive::POOL_CERTS, cert.operator.to_vec()));
        }
    }

    /// Add withdrawal tags to the current block.
    pub fn add_withdrawal(&mut self, account: &[u8]) {
        self.current_block()
            .tags
            .push(Tag::new(archive::ACCOUNT_WITHDRAWALS, account.to_vec()));
    }

    /// Add a metadata label to the current block.
    pub fn add_metadata_label(&mut self, label: u64) {
        self.current_block()
            .tags
            .push(Tag::new(archive::METADATA, label.to_be_bytes().to_vec()));
    }

    /// Index all archive entries for a single block.
    ///
    /// Calls `start_block`, then iterates all transactions adding
    /// tx hashes, metadata, inputs (with resolved UTxO lookups),
    /// outputs (with script refs), witness scripts/datums, certs, and
    /// redeemers.
    pub fn index_block(
        &mut self,
        block: &pallas::ledger::traverse::MultiEraBlock<'_>,
        resolved_inputs: &std::collections::HashMap<TxoRef, crate::OwnedMultiEraOutput>,
    ) {
        use pallas::ledger::traverse::{ComputeHash as _, OriginalHash as _};

        self.start_block(block.slot(), block.hash().to_vec(), Some(block.number()));

        for tx in block.txs() {
            self.add_tx_hash(tx.hash().to_vec());

            for (label, _) in tx.metadata().collect::<Vec<_>>() {
                self.add_metadata_label(label);
            }

            for input in tx.inputs() {
                self.add_spent_input(&input);

                let txo_ref: TxoRef = (&input).into();
                if let Some(resolved) = resolved_inputs.get(&txo_ref) {
                    resolved.with_dependent(|_, output| {
                        if let Ok(addr) = output.address() {
                            self.add_address(&addr);
                        }
                        self.add_assets(&output.value());
                        if let Some(datum) = output.datum() {
                            self.add_datum(&datum);
                        }
                    });
                }
            }

            for (_, output) in tx.produces() {
                if let Ok(addr) = output.address() {
                    self.add_address(&addr);
                }
                self.add_assets(&output.value());
                if let Some(datum) = output.datum() {
                    self.add_datum(&datum);
                }

                if let Some(script_ref) = output.script_ref() {
                    self.add_script_hash(pallas_extras::script_ref_hash(&script_ref).to_vec());
                }
            }

            for script in tx.native_scripts() {
                self.add_script_hash(script.original_hash().to_vec());
            }
            for script in tx.plutus_v1_scripts() {
                self.add_script_hash(script.compute_hash().to_vec());
            }
            for script in tx.plutus_v2_scripts() {
                self.add_script_hash(script.compute_hash().to_vec());
            }
            for script in tx.plutus_v3_scripts() {
                self.add_script_hash(script.compute_hash().to_vec());
            }

            for datum in tx.plutus_data() {
                self.add_datum_hash(datum.original_hash().to_vec());
            }

            for cert in tx.certs() {
                self.add_cert(&cert);
            }

            for (account, _) in tx.withdrawals().collect::<Vec<_>>() {
                self.add_withdrawal(account);
            }

            for redeemer in tx.redeemers() {
                self.add_datum_hash(redeemer.data().compute_hash().to_vec());
            }

            // tags are candidates, not response rows: phase-2-failed txs get
            // tagged like every other dimension does, and the Blockfrost
            // rule (db-sync stores no redeemers for failed txs) stays a
            // query-time filter. resolution is best effort — a spend
            // redeemer only tags when its declared input is in
            // `resolved_inputs`.
            for redeemer in tx.redeemers() {
                let resolved =
                    crate::pallas_extras::redeemer_script_hash(&tx, &redeemer, &mut |input| {
                        let txo_ref: TxoRef = input.into();
                        let address = resolved_inputs.get(&txo_ref).and_then(|resolved| {
                            resolved.with_dependent(|_, output| output.address().ok())
                        });
                        Ok::<_, std::convert::Infallible>(address)
                    });

                let Ok(Some(hash)) = resolved else {
                    continue;
                };

                self.add_redeemer_script(hash.to_vec());
            }
        }
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
    use std::collections::BTreeSet;

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

    /// An output that carries a reference script gets a `script_ref` tag whose
    /// key is the script's on-chain hash.
    #[test]
    fn reference_script_output_gets_script_ref_tag() {
        use pallas::codec::minicbor;
        use pallas::codec::utils::{CborWrap, KeepRaw};
        use pallas::crypto::hash::Hasher;
        use pallas::ledger::primitives::conway::{PostAlonzoTransactionOutput, ScriptRef, Value};
        use pallas::ledger::traverse::{Era, MultiEraOutput};

        let script = pallas::ledger::primitives::alonzo::NativeScript::InvalidHereafter(500_000);

        let output = PostAlonzoTransactionOutput {
            address: test_shelley_address().to_vec().into(),
            value: Value::Coin(1_000_000),
            datum_option: None,
            script_ref: Some(CborWrap(ScriptRef::NativeScript(KeepRaw::from(
                script.clone(),
            )))),
        };

        let cbor = minicbor::to_vec(&output).unwrap();
        let output = MultiEraOutput::decode(Era::Conway, &cbor).unwrap();

        let tags = CardanoIndexDeltaBuilder::extract_utxo_tags(&output);

        // Native scripts hash their CBOR behind a leading 0x00 language tag.
        let script_cbor = minicbor::to_vec(&script).unwrap();
        let expected = Hasher::<224>::hash_tagged(&script_cbor, 0);

        let tag = tags
            .iter()
            .find(|tag| tag.dimension == utxo::SCRIPT_REF)
            .expect("output with a reference script must produce a script_ref tag");

        assert_eq!(tag.key, expected.to_vec());
    }

    /// Drive every tag-producing method on the builder once.
    ///
    /// This is the producer side of the registry: what a block turns into.
    /// Kept as one function so the completeness check below and any future
    /// caller see the same set.
    fn tag_every_dimension(builder: &mut CardanoIndexDeltaBuilder) {
        use pallas::ledger::primitives::{
            alonzo::TransactionInput,
            conway::{Certificate, Value},
            StakeCredential,
        };
        use pallas::ledger::traverse::{MultiEraCert, MultiEraInput, MultiEraValue};
        use std::borrow::Cow;
        use std::collections::BTreeMap;

        builder.start_block(100, vec![0; 32], Some(50));
        builder.add_tx_hash(vec![1; 32]);

        // ADDRESS, PAYMENT, STAKE
        builder.add_address(&test_shelley_address());

        // POLICY, ASSET
        let mut names = BTreeMap::new();
        names.insert(
            vec![0xaa; 4].into(),
            1u64.try_into().expect("1 is a positive coin"),
        );
        let mut multiasset = BTreeMap::new();
        multiasset.insert(Hash::new([0x11; 28]), names);
        let value = Value::Multiasset(0, multiasset);
        builder.add_assets(&MultiEraValue::Conway(Cow::Owned(value)));

        // DATUM
        builder.add_datum_hash(vec![0x22; 32]);

        // SPENT_TXO
        let input = TransactionInput {
            transaction_id: Hash::new([0x33; 32]),
            index: 0,
        };
        builder.add_spent_input(&MultiEraInput::from_alonzo_compatible(&input));

        // SCRIPT
        builder.add_script_hash(vec![0x44; 28]);

        // SCRIPT_REDEEMERS
        builder.add_redeemer_script(vec![0x88; 28]);

        // ACCOUNT_CERTS
        let registration =
            Certificate::StakeRegistration(StakeCredential::AddrKeyhash(Hash::new([0x55; 28])));
        builder.add_cert(&MultiEraCert::Conway(Box::new(Cow::Owned(registration))));

        // POOL_CERTS
        let retirement = Certificate::PoolRetirement(Hash::new([0x66; 28]), 42);
        builder.add_cert(&MultiEraCert::Conway(Box::new(Cow::Owned(retirement))));

        // ACCOUNT_WITHDRAWALS
        builder.add_withdrawal(&[0x77; 29]);

        // METADATA
        builder.add_metadata_label(674);
    }

    /// The dimension registry has to hold every dimension this builder emits.
    ///
    /// `archive::ALL` drives every bulk traversal of archive tags — index
    /// stores keep a hash of the dimension name, not the name, so the set is
    /// not discoverable from disk. A dimension produced here and missing there
    /// does not fail: it silently stops being exported, and a snapshot built
    /// from the traversal is quietly incomplete.
    ///
    /// The reverse direction is checked too. Without it the test could stop
    /// covering a dimension — by a builder method losing its call above — and
    /// still pass, which is the same blind spot one step removed.
    #[test]
    fn every_produced_dimension_is_registered() {
        let mut builder =
            CardanoIndexDeltaBuilder::new(ChainPoint::Specific(100, Hash::new([0; 32])));
        tag_every_dimension(&mut builder);

        let delta = builder.build();

        let produced: BTreeSet<&str> = delta
            .archive
            .iter()
            .flat_map(|block| block.tags.iter())
            .map(|tag| tag.dimension)
            .collect();

        let registered: BTreeSet<&str> = archive::ALL.iter().copied().collect();

        assert!(
            produced.is_subset(&registered),
            "these dimensions are produced but not in archive::ALL, so they \
             would silently stop being exported: {:?}",
            &produced - &registered,
        );

        assert_eq!(
            produced,
            registered,
            "every registered dimension must be exercised here, otherwise this \
             test stops covering the ones it misses: {:?} are unexercised",
            &registered - &produced,
        );
    }
}
