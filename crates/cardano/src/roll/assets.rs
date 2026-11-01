//! Asset mints.
//!
//! Stages every mint and burn of a valid tx in `asset-mints`, and each
//! asset's first mint ever in the `first-mints` row of its epoch. The witness
//! that tells a first mint from a re-mint is the archive's entity index, which
//! the seal writes for every sealed epoch, plus the staged rows of the
//! unsealed epochs (decision 0064). As for scripts, which of the batch's
//! assets an earlier block minted is read once per batch, ahead of the crawl,
//! and kept current by hand from block to block.

use std::{collections::HashSet, ops::RangeInclusive};

use dolos_core::{ArchiveStore, ChainError, Genesis, StateStore, TxOrder};
use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::Epoch,
        traverse::{MultiEraBlock, MultiEraPolicyAssets, MultiEraTx},
    },
};
use tracing::trace;

use super::WorkDeltas;
use crate::cip25::{cip25_metadata_for_tx, cip25_mint_names_asset};
use crate::cip68::has_cip25_metadata;
use crate::roll::BlockVisitor;
use crate::{
    indexes::entities, periods::GroupKey, AssetKey, FirstMintSet, PParamsSet, PolicyKey,
    StageAssetMint, StageFirstMint,
};

/// The assets `block`'s valid txs mint or burn with a non-zero quantity, by
/// policy and key: the mints that can be an asset's first.
pub fn block_minted_assets(block: &MultiEraBlock<'_>) -> Vec<(Hash<28>, AssetKey)> {
    let mut out = Vec::new();

    for tx in block.txs() {
        if !tx.is_valid() {
            continue;
        }

        for mint in tx.mints() {
            for asset in mint.assets() {
                if asset.mint_coin().unwrap_or_default() != 0 {
                    let key = AssetKey::of_parts(mint.policy().as_slice(), asset.name());
                    out.push((*mint.policy(), key));
                }
            }
        }
    }

    out
}

/// What the roll knows about first mints while it crawls a batch.
#[derive(Debug, Default)]
pub struct MintWitnessContext {
    /// The assets the batch mints that a block before it minted, plus the ones
    /// the batch has minted so far.
    known: HashSet<AssetKey>,
}

impl MintWitnessContext {
    /// Read the witness for a batch starting at `first_slot` whose blocks
    /// mint `minted`, while `unsealed` are the epochs whose staged rows are
    /// still in state.
    ///
    /// An asset was minted before when the entity index holds a slot for it
    /// below the batch, or a staged `first-mints` row of an unsealed epoch
    /// names it. The seal writes the index entry before it deletes the staged
    /// row, so a sealed first mint is always in one of the two. The slot bound
    /// keeps a re-roll over an archive that already indexes the batch (`data
    /// rebuild`) from reading the batch's own entries.
    pub fn load<S: StateStore, A: ArchiveStore>(
        state: &S,
        archive: &A,
        minted: impl IntoIterator<Item = (Hash<28>, AssetKey)>,
        first_slot: u64,
        unsealed: RangeInclusive<Epoch>,
    ) -> Result<Self, ChainError> {
        let assets: Vec<(Hash<28>, AssetKey)> = minted
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();

        let mut known = HashSet::new();

        let keys: Vec<&[u8]> = assets.iter().map(|(_, key)| key.0.as_slice()).collect();
        let slots = archive.entity_slots(entities::ASSET, &keys)?;

        let mut rest = Vec::new();

        for (asset, slot) in assets.iter().zip(slots) {
            match slot {
                Some(slot) if slot < first_slot => {
                    known.insert(asset.1);
                }
                _ => rest.push(*asset),
            }
        }

        if rest.is_empty() {
            return Ok(Self { known });
        }

        let keys: Vec<GroupKey<PolicyKey, AssetKey>> = unsealed
            .flat_map(|epoch| {
                rest.iter().map(move |(policy, key)| GroupKey {
                    group: PolicyKey(*policy),
                    epoch,
                    member: *key,
                })
            })
            .collect();

        let rows = state.read_entities_typed::<FirstMintSet>(&keys.iter().collect::<Vec<_>>())?;

        for (key, row) in keys.iter().zip(rows) {
            if row.is_some() {
                known.insert(key.member);
            }
        }

        Ok(Self { known })
    }
}

/// Emits the deltas of a block's mints: every mint and burn, and the first
/// mint of each asset no earlier block minted.
#[derive(Default)]
pub struct AssetMintVisitor {
    epoch: Epoch,

    /// The first-mint witness, carried from block to block of the batch.
    witness: MintWitnessContext,
}

impl AssetMintVisitor {
    pub fn new(witness: MintWitnessContext) -> Self {
        Self { epoch: 0, witness }
    }

    /// The witness after this block, for the next one of the batch.
    pub fn take_witness(&mut self) -> MintWitnessContext {
        std::mem::take(&mut self.witness)
    }
}

impl BlockVisitor for AssetMintVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas,
        _: &MultiEraBlock,
        _: &Genesis,
        _: &PParamsSet,
        epoch: Epoch,
        _: u64,
        _: u16,
    ) -> Result<(), ChainError> {
        self.epoch = epoch;
        Ok(())
    }

    fn visit_mint(
        &mut self,
        deltas: &mut WorkDeltas,
        block: &MultiEraBlock,
        tx: &MultiEraTx,
        order: &TxOrder,
        mint: &MultiEraPolicyAssets,
    ) -> Result<(), ChainError> {
        let policy = mint.policy();
        let has_metadata = has_cip25_metadata(tx);
        let cip25_metadata = if has_metadata {
            cip25_metadata_for_tx(tx)
        } else {
            None
        };

        for asset in mint.assets() {
            trace!(%policy, asset = %hex::encode(asset.name()), "detected mint");

            let amount = asset.mint_coin().unwrap_or_default();

            // a zero-quantity entry mints nothing: no mint row, no first mint
            if amount == 0 {
                continue;
            }

            let key = AssetKey::of_parts(policy.as_slice(), asset.name());
            let names_asset =
                cip25_mint_names_asset(cip25_metadata.as_ref(), policy, asset.name(), amount);

            deltas.add_for_entity(StageAssetMint::mint(
                key,
                self.epoch,
                (block.slot(), *order as u32),
                tx.hash(),
                amount,
                names_asset,
            ));

            if self.witness.known.insert(key) {
                deltas.add_for_entity(StageFirstMint::new(FirstMintSet {
                    policy: *policy,
                    name: asset.name().to_vec(),
                    epoch: self.epoch,
                    slot: block.slot(),
                    tx_index: *order as u32,
                }));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use dolos_core::{
        builtin::{MemoryArchiveStore, MemoryStateStore},
        indexes::{ArchiveIndexDelta, Tag},
        ArchiveStore as _, ArchiveWriter as _, FixedNamespace as _, StateWriter as _,
    };
    use pallas::{
        codec::utils::{Bytes, NonZeroInt, Set},
        ledger::primitives::conway::TransactionBody,
    };

    use super::*;
    use crate::{
        indexes::archive_dimensions,
        roll::scripts::tests::{commit, undo},
    };

    const P: [u8; 28] = [0x33; 28];

    fn key(name: &str) -> AssetKey {
        AssetKey::of_parts(&P, name.as_bytes())
    }

    /// A block at `slot` whose one valid tx mints `amount` of each name
    /// under [`P`].
    fn block(slot: u64, mints: &[(&str, i64)]) -> Vec<u8> {
        let mut assets = BTreeMap::new();

        for (name, amount) in mints {
            assets.insert(
                Bytes::from(name.as_bytes().to_vec()),
                NonZeroInt::try_from(*amount).unwrap(),
            );
        }

        let body = TransactionBody {
            inputs: Set::from(vec![]),
            outputs: vec![],
            fee: 170_000,
            ttl: None,
            certificates: None,
            withdrawals: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: Some(BTreeMap::from([(Hash::from(P), assets)])),
            script_data_hash: None,
            collateral: None,
            required_signers: None,
            network_id: None,
            collateral_return: None,
            total_collateral: None,
            reference_inputs: None,
            voting_procedures: None,
            proposal_procedures: None,
            treasury_value: None,
            donation: None,
        };

        dolos_testing::blocks::make_conway_block_with_tx(slot, body, None, true)
            .1
            .to_vec()
    }

    /// The staged first mints, as `(epoch, name, slot)`.
    fn staged(state: &MemoryStateStore) -> Vec<(Epoch, Vec<u8>, u64)> {
        let mut out: Vec<_> = state
            .iter_entities_typed::<FirstMintSet>(None)
            .unwrap()
            .map(|row| {
                let (_, row) = row.unwrap();
                (row.epoch, row.name, row.slot)
            })
            .collect();

        out.sort();
        out
    }

    /// A state and archive pair, the stores the roll reads and writes.
    struct Stores {
        state: MemoryStateStore,
        archive: MemoryArchiveStore,
    }

    impl Stores {
        fn new() -> Self {
            Self {
                state: MemoryStateStore::new(),
                archive: MemoryArchiveStore::new(crate::model::build_log_schema()),
            }
        }

        /// Seal the staged rows of `epochs` as the seal does: index each
        /// asset at its row's slot in the archive, then delete the row.
        fn seal(&self, epochs: RangeInclusive<Epoch>) {
            let rows: Vec<_> = self
                .state
                .iter_entities_typed::<FirstMintSet>(None)
                .unwrap()
                .map(Result::unwrap)
                .filter(|(key, _)| epochs.contains(&key.epoch))
                .collect();

            let writer = self.archive.start_writer().unwrap();

            for (key, row) in &rows {
                writer
                    .write_entity(entities::ASSET, key.member.0.as_slice(), row.slot)
                    .unwrap();
            }

            writer.commit().unwrap();

            let writer = self.state.start_writer().unwrap();

            for (key, _) in &rows {
                writer
                    .delete_entity(FirstMintSet::NS, &dolos_core::EntityKey::of(key))
                    .unwrap();
            }

            writer.commit().unwrap();
        }

        /// Run `blocks` through one batch's visitors, each block `(epoch,
        /// raw)`, and commit them, the way `compute_delta` does. Every epoch
        /// up to the batch's is taken as unsealed. Returns each block's
        /// applied deltas.
        fn roll(&self, blocks: Vec<(Epoch, Vec<u8>)>) -> Vec<WorkDeltas> {
            let decoded: Vec<_> = blocks
                .iter()
                .map(|(epoch, raw)| (*epoch, MultiEraBlock::decode(raw).unwrap()))
                .collect();

            let first_slot = decoded.iter().map(|(_, b)| b.slot()).min().unwrap();
            let last_epoch = decoded.iter().map(|(e, _)| *e).max().unwrap();
            let minted = decoded.iter().flat_map(|(_, b)| block_minted_assets(b));

            let mut witness = MintWitnessContext::load(
                &self.state,
                &self.archive,
                minted,
                first_slot,
                0..=last_epoch,
            )
            .unwrap();

            let mut applied = vec![];

            for (epoch, block) in decoded {
                let mut visitor = AssetMintVisitor::new(std::mem::take(&mut witness));
                let mut deltas = WorkDeltas::default();
                let genesis = crate::load_test_genesis("preview");

                visitor
                    .visit_root(
                        &mut deltas,
                        &block,
                        &genesis,
                        &Default::default(),
                        epoch,
                        0,
                        0,
                    )
                    .unwrap();

                for (order, tx) in block.txs().iter().enumerate() {
                    for mint in tx.mints() {
                        visitor
                            .visit_mint(&mut deltas, &block, tx, &order, &mint)
                            .unwrap();
                    }
                }

                witness = visitor.take_witness();
                applied.push(commit(&self.state, deltas));
            }

            applied
        }
    }

    #[test]
    fn a_batch_stages_each_asset_once_where_first_minted() {
        let stores = Stores::new();

        // one batch: the second block re-mints A and burns B, the third
        // mints nothing new
        stores.roll(vec![
            (4, block(10, &[("A", 5), ("B", 1)])),
            (4, block(11, &[("A", 1), ("B", -1), ("C", 1)])),
            (4, block(12, &[("C", 2)])),
        ]);

        // the next batch, in the next epoch, knows them from the staged rows
        stores.roll(vec![(5, block(20, &[("A", 1), ("D", 1)]))]);

        assert_eq!(
            staged(&stores.state),
            vec![
                (4, b"A".to_vec(), 10),
                (4, b"B".to_vec(), 10),
                (4, b"C".to_vec(), 11),
                (5, b"D".to_vec(), 20),
            ]
        );
    }

    /// The witness across epochs: an asset sealed into the entity index, an
    /// asset staged in an unsealed epoch and a new one, then a re-roll of the
    /// batch that first minted the indexed asset.
    #[test]
    fn the_witness_reads_the_index_below_the_batch_and_the_staged_rows() {
        let stores = Stores::new();

        // A first minted at slot 10, in epoch 1, and sealed
        stores.roll(vec![(1, block(10, &[("A", 1)]))]);
        stores.seal(1..=1);
        assert!(staged(&stores.state).is_empty());

        // B staged in epoch 6
        stores.roll(vec![(6, block(50, &[("B", 1)]))]);

        let minted = [
            (P.into(), key("A")),
            (P.into(), key("B")),
            (P.into(), key("C")),
        ];

        let load = |first_slot, unsealed| {
            MintWitnessContext::load(&stores.state, &stores.archive, minted, first_slot, unsealed)
                .unwrap()
                .known
        };

        assert_eq!(load(60, 6..=7), HashSet::from([key("A"), key("B")]));

        // a staged row outside the unsealed epochs is not read
        assert_eq!(load(60, 7..=7), HashSet::from([key("A")]));

        // the index entry at the batch's own first slot is not below it
        assert!(load(10, 7..=7).is_empty());

        // across the boundary: the next epoch's batch stages C alone
        stores.roll(vec![(7, block(60, &[("A", 1), ("B", 1), ("C", 1)]))]);

        assert_eq!(
            staged(&stores.state),
            vec![(6, b"B".to_vec(), 50), (7, b"C".to_vec(), 60)]
        );
    }

    /// The mainnet pattern that bounded the tag witness at the epoch's start:
    /// an `asset` tag from a quantity-0 output before the asset's first mint,
    /// in the same epoch. The witness reads the index and the staged rows, not
    /// the tag, so the mint is still the first.
    #[test]
    fn an_asset_tag_before_the_first_mint_is_no_witness() {
        let stores = Stores::new();

        let writer = stores.archive.start_writer().unwrap();
        writer
            .apply_index(&[ArchiveIndexDelta {
                slot: 5,
                block_hash: vec![0x05; 32],
                block_number: None,
                tx_hashes: vec![],
                tags: vec![Tag::new(
                    archive_dimensions::ASSET,
                    [P.as_slice(), b"A"].concat(),
                )],
            }])
            .unwrap();
        writer.commit().unwrap();

        stores.roll(vec![(4, block(10, &[("A", 1)]))]);

        assert_eq!(staged(&stores.state), vec![(4, b"A".to_vec(), 10)]);
    }

    /// A batch never reads its own index entries: re-rolling it over an
    /// archive that already indexes every sealed first mint, as `data
    /// rebuild` does, stages the rows the first roll staged.
    #[test]
    fn a_re_roll_over_a_full_archive_stages_the_same_rows() {
        let batches = vec![
            vec![
                (4, block(10, &[("A", 1), ("B", 1)])),
                (4, block(11, &[("A", 1)])),
            ],
            vec![(5, block(20, &[("B", -1), ("C", 1)]))],
        ];

        let first = Stores::new();

        for batch in batches.clone() {
            first.roll(batch);
        }

        let rows = staged(&first.state);

        // the replay's state starts empty, its archive indexes every asset
        first.seal(4..=5);

        let replay = Stores {
            state: MemoryStateStore::new(),
            archive: first.archive.clone(),
        };

        for batch in batches {
            replay.roll(batch);
        }

        assert_eq!(staged(&replay.state), rows);
        assert_eq!(
            staged(&replay.state),
            vec![
                (4, b"A".to_vec(), 10),
                (4, b"B".to_vec(), 10),
                (5, b"C".to_vec(), 20),
            ]
        );
    }

    /// Undoing a block's deltas leaves no row of it, and the rows of the
    /// block before it stay.
    #[test]
    fn undoing_a_block_removes_its_rows() {
        let stores = Stores::new();

        stores.roll(vec![(4, block(10, &[("A", 1)]))]);

        let before = staged(&stores.state);

        let applied = stores.roll(vec![(4, block(11, &[("A", 1), ("B", 1)]))]);

        assert_eq!(staged(&stores.state).len(), 2);

        for deltas in applied {
            undo(&stores.state, deltas);
        }

        assert_eq!(staged(&stores.state), before);
    }
}
