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
    use dolos_core::NsKey;
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
            .flat_map(|(NsKey(ns, _), group)| {
                assert_eq!(ns, MetadataLabelState::NS);
                group
            })
            .map(|delta| match delta {
                CardanoDelta::MetadataLabelTxInc(inc) => (inc.label, inc.count),
                other => panic!("the delta is not a MetadataLabelTxInc: {other:?}"),
            })
            .collect()
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
