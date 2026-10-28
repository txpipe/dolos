use dolos_core::{BlockBody, BlockHash, BlockSlot, RawBlock, RawUtxoMap, TxoRef, UtxoValue};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use self_cell::self_cell;
use std::sync::Arc;

self_cell!(
    pub struct OwnedMultiEraBlock {
        owner: Arc<BlockBody>,

        #[covariant]
        dependent: MultiEraBlock,
    }
);

impl OwnedMultiEraBlock {
    pub fn decode(buf: Arc<BlockBody>) -> Result<Self, pallas::ledger::traverse::Error> {
        Self::try_new(buf, |x| MultiEraBlock::decode(x))
    }

    pub fn view(&self) -> &MultiEraBlock<'_> {
        self.borrow_dependent()
    }
}

impl dolos_core::Block for OwnedMultiEraBlock {
    fn depends_on(&self, loaded: &mut RawUtxoMap) -> Vec<TxoRef> {
        crate::utxoset::compute_block_dependencies(self.view(), loaded)
    }

    fn slot(&self) -> BlockSlot {
        self.view().slot()
    }

    fn hash(&self) -> BlockHash {
        self.view().hash()
    }

    fn raw(&self) -> RawBlock {
        self.borrow_owner().clone()
    }
}

self_cell!(
    pub struct OwnedMultiEraOutput {
        owner: Arc<UtxoValue>,

        #[not_covariant]
        dependent: MultiEraOutput,
    }
);

impl OwnedMultiEraOutput {
    pub fn decode(buf: Arc<UtxoValue>) -> Result<Self, pallas::ledger::traverse::Error> {
        Self::try_new(buf, |x| {
            let era = pallas::ledger::traverse::Era::try_from(x.era())?;

            let dec = MultiEraOutput::decode(era, x.cbor())
                .map_err(|x| pallas::ledger::traverse::Error::InvalidCbor(x.to_string()))?;

            Ok(dec)
        })
    }
}
