use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{
    batch::{WorkBatch, WorkBlock},
    *,
};

use crate::{
    owned::{OwnedMultiEraBlock, OwnedMultiEraOutput},
    pparams::ChainSummary,
};

pub mod pallas_extras;

pub mod model;
pub mod nonce;
pub mod owned;
pub mod pparams;
pub mod roll;
pub mod sweep;
pub mod utils;
pub mod utxoset;

#[cfg(feature = "include-genesis")]
pub mod include;

pub use model::*;
pub use utils::{mutable_slots, slot_epoch, slot_time, slot_time_within_era};

pub type Block<'a> = MultiEraBlock<'a>;

pub type UtxoBody<'a> = MultiEraOutput<'a>;

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct TrackConfig {
    pub account_state: bool,
    pub asset_state: bool,
    pub pool_state: bool,
    pub epoch_state: bool,
    pub drep_state: bool,
    pub tx_logs: bool,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Config {
    #[serde(default)]
    pub track: TrackConfig,
}

#[derive(Clone)]
pub struct CardanoLogic {
    config: Config,
    summary: Arc<ChainSummary>,
}

impl CardanoLogic {
    pub fn new(config: Config) -> Self {
        Self {
            config,
            summary: Default::default(),
        }
    }
}

impl dolos_core::ChainLogic for CardanoLogic {
    type Block = OwnedMultiEraBlock;
    type Utxo = OwnedMultiEraOutput;
    type Delta = CardanoDelta;
    type Entity = CardanoEntity;

    fn decode_block(&self, block: Arc<BlockBody>) -> Result<Self::Block, ChainError> {
        let out = OwnedMultiEraBlock::decode(block)?;

        Ok(out)
    }

    fn decode_utxo(&self, utxo: Arc<EraCbor>) -> Result<Self::Utxo, ChainError> {
        let out = OwnedMultiEraOutput::decode(utxo)?;

        Ok(out)
    }

    fn execute_sweep<D: Domain>(&self, domain: &D, at: BlockSlot) -> Result<(), ChainError> {
        sweep::sweep(domain, at)?;

        Ok(())
    }

    fn next_sweep(&self, after: BlockSlot) -> BlockSlot {
        // TODO: implement era folding so that it's available for epoch boundaries
        //pallas_extras::next_epoch_boundary(&self.summary, after)
        BlockSlot::MAX
    }

    fn mutable_slots(domain: &impl Domain) -> BlockSlot {
        utils::mutable_slots(domain.genesis())
    }

    fn compute_origin_utxo_delta(&self, genesis: &Genesis) -> Result<UtxoSetDelta, ChainError> {
        let delta = utxoset::compute_origin_delta(genesis);

        Ok(delta)
    }

    fn compute_block_utxo_delta(
        &self,
        block: &Self::Block,
        deps: &RawUtxoMap,
    ) -> Result<UtxoSetDelta, ChainError> {
        let delta = utxoset::compute_apply_delta(block.view(), deps)?;

        Ok(delta)
    }

    fn compute_origin_delta(&self, genesis: &Genesis) -> Result<WorkBatch<Self>, ChainError> {
        todo!()
    }

    fn compute_delta(
        &self,
        block: &mut WorkBlock<Self>,
        deps: &HashMap<TxoRef, Self::Utxo>,
    ) -> Result<(), ChainError> {
        let mut builder = roll::DeltaBuilder::new(self.config.track.clone(), block);

        builder.crawl(deps)?;

        Ok(())
    }
}
