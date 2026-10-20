//! The number of transactions for each metadata label.
//!
//! Each valid transaction that has a label in its auxiliary data adds 1 to the
//! count of that label. The `tx_metadata` table of db-sync gives the same
//! count. This table has one row for each transaction and label. It has no
//! rows for phase-2-invalid transactions.

use std::collections::{BTreeMap, HashMap};

use dolos_core::{ChainError, TxoRef};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};

use super::{BlockVisitor, WorkDeltas};
use crate::owned::OwnedMultiEraOutput;
use crate::MetadataLabelTxInc;

/// This visitor counts the valid transactions of a block for each metadata
/// label. When the roll calls `flush`, the visitor makes one
/// [`MetadataLabelTxInc`] for each label.
#[derive(Default)]
pub struct MetadataLabelVisitor {
    counts: BTreeMap<u64, u64>,
}

impl BlockVisitor for MetadataLabelVisitor {
    fn visit_tx(
        &mut self,
        _deltas: &mut WorkDeltas,
        _block: &MultiEraBlock,
        tx: &MultiEraTx,
        _utxos: &HashMap<TxoRef, OwnedMultiEraOutput>,
    ) -> Result<(), ChainError> {
        if !tx.is_valid() {
            return Ok(());
        }

        // The metadata is a map. As a result, a transaction cannot contain a
        // label more than one time.
        let metadata = tx.metadata();

        if let Some(metadata) = metadata.as_alonzo() {
            for label in metadata.keys() {
                *self.counts.entry(*label).or_default() += 1;
            }
        }

        Ok(())
    }

    fn flush(&mut self, deltas: &mut WorkDeltas) -> Result<(), ChainError> {
        for (label, count) in std::mem::take(&mut self.counts) {
            deltas.add_for_entity(MetadataLabelTxInc::new(label, count));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use pallas::ledger::primitives::alonzo;

    use super::*;
    use crate::{CardanoDelta, FixedNamespace as _, MetadataLabelState, OwnedMultiEraBlock};

    /// Makes a block with one transaction that has `labels` in its metadata.
    fn block(labels: &[u64], valid: bool) -> OwnedMultiEraBlock {
        let metadata = labels
            .iter()
            .map(|label| (*label, alonzo::Metadatum::Text("x".to_string())))
            .collect();

        let (_, raw) =
            dolos_testing::blocks::make_conway_block_with_metadata(1_000, metadata, valid);

        OwnedMultiEraBlock::decode(raw).unwrap()
    }

    fn visit(visitor: &mut MetadataLabelVisitor, block: &OwnedMultiEraBlock) {
        let block = block.view();
        let mut deltas = WorkDeltas::default();

        for tx in block.txs() {
            visitor
                .visit_tx(&mut deltas, block, &tx, &HashMap::new())
                .unwrap();
        }

        assert!(deltas.entities.is_empty(), "visit_tx must not add deltas");
    }

    fn flush(visitor: &mut MetadataLabelVisitor) -> BTreeMap<u64, u64> {
        let mut deltas = WorkDeltas::default();
        visitor.flush(&mut deltas).unwrap();

        deltas
            .entities
            .into_iter()
            .flat_map(|(key, group)| {
                assert_eq!(key.ns(), MetadataLabelState::NS);
                let label = key.key().decode::<u64>().unwrap();
                assert_eq!(group.len(), 1);
                assert!(matches!(
                    &group[0],
                    CardanoDelta::MetadataLabelTxInc(inc) if inc.label == label
                ));
                group
            })
            .map(|delta| match delta {
                CardanoDelta::MetadataLabelTxInc(inc) => (inc.label, inc.count),
                other => panic!("the delta is not a MetadataLabelTxInc: {other:?}"),
            })
            .collect()
    }

    #[test]
    fn pipeline_rollback_removes_only_valid_transaction_contributions() {
        use dolos_core::{
            sync::SyncExt as _, Domain as _, EntityDelta as _, StateStore as _, WalStore as _,
        };
        use dolos_testing::{
            synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
            toy_domain::ToyDomain,
            MIN_UTXO_AMOUNT,
        };

        let labels = [0, 674, u64::MAX];
        let (blocks, _, config) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 2,
            plain_txs: true,
            seed_amount: MIN_UTXO_AMOUNT + 7,
            ada_only_by_block: vec![true; 3],
            invalid_txs_by_block: vec![vec![0], vec![], vec![]],
            metadata_entries: labels
                .iter()
                .map(|label| (*label, alonzo::Metadatum::Text("x".to_string())))
                .collect(),
            ..Default::default()
        });
        let genesis = std::sync::Arc::new(crate::include::preview::load());
        let domain = ToyDomain::new_with_genesis_and_config(genesis, config, None, None);
        let mut points = Vec::new();

        let assert_count = |count| {
            let rows: BTreeMap<_, _> = domain
                .state()
                .iter_entities_typed::<MetadataLabelState>(None)
                .unwrap()
                .map(|row| {
                    let (label, state) = row.unwrap();
                    (label, state.tx_count)
                })
                .collect();
            let expected = if count == 0 {
                BTreeMap::new()
            } else {
                labels.into_iter().map(|label| (label, count)).collect()
            };
            assert_eq!(rows, expected);
        };

        for (index, raw) in blocks.into_iter().enumerate() {
            domain.roll_forward(raw.clone()).unwrap();
            let block = OwnedMultiEraBlock::decode(raw).unwrap();
            let point = dolos_core::ChainPoint::Specific(block.view().slot(), block.view().hash());
            let wal = domain.wal().read_entry(&point).unwrap().unwrap();
            let increments = wal
                .delta
                .iter()
                .filter(|delta| delta.key().ns() == MetadataLabelState::NS)
                .count();
            assert_eq!(increments, if index == 0 { 0 } else { labels.len() });
            points.push(point);
            assert_count(index as u64);
        }

        domain.rollback(&points[1]).unwrap();
        assert_count(1);
        domain.rollback(&points[0]).unwrap();
        assert_count(0);
    }

    #[test]
    fn counts_each_label_once_per_valid_transaction() {
        let mut visitor = MetadataLabelVisitor::default();

        visit(&mut visitor, &block(&[5, 674], true));
        visit(&mut visitor, &block(&[5], true));
        visit(&mut visitor, &block(&[9], false));

        assert_eq!(flush(&mut visitor), BTreeMap::from([(5, 2), (674, 1)]));
        assert!(
            flush(&mut visitor).is_empty(),
            "flush must remove the counts"
        );
    }
}
