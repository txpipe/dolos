use dolos_core::{BlockBody, BlockSlot, EraCbor, RawUtxoMap, TxoRef};
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
}

self_cell!(
    pub struct OwnedMultiEraOutput {
        owner: Arc<EraCbor>,

        #[not_covariant]
        dependent: MultiEraOutput,
    }
);

impl OwnedMultiEraOutput {
    pub fn decode(buf: Arc<EraCbor>) -> Result<Self, pallas::ledger::traverse::Error> {
        Self::try_new(buf, |x| {
            let EraCbor(era, cbor) = x.as_ref();

            let era = pallas::ledger::traverse::Era::try_from(*era)?;

            let dec = MultiEraOutput::decode(era, &cbor)
                .map_err(|x| pallas::ledger::traverse::Error::InvalidCbor(x.to_string()))?;

            Ok(dec)
        })
    }
}
