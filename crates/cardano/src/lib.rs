use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{batch::WorkBlock, *};

use crate::owned::{OwnedMultiEraBlock, OwnedMultiEraOutput};

pub mod pallas_extras;

pub mod bootstrap;
pub mod eras;
pub mod forks;
pub mod model;
pub mod owned;
pub mod roll;
pub mod sweep;
pub mod utils;
pub mod utxoset;

#[cfg(feature = "include-genesis")]
pub mod include;

pub use eras::*;
pub use model::*;
pub use utils::mutable_slots;

pub type Block<'a> = MultiEraBlock<'a>;

pub type UtxoBody<'a> = MultiEraOutput<'a>;

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct TrackConfig {
    pub account_state: bool,
    pub asset_state: bool,
    pub pool_state: bool,
    pub epoch_state: bool,
    pub drep_state: bool,
    pub pparams_state: bool,
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
}

impl CardanoLogic {
    pub fn new(config: Config) -> Self {
        Self { config }
    }
}

impl dolos_core::ChainLogic for CardanoLogic {
    type Block = OwnedMultiEraBlock;
    type Utxo = OwnedMultiEraOutput;
    type Delta = CardanoDelta;
    type Entity = CardanoEntity;

    fn bootstrap<D: Domain>(&self, domain: &D) -> Result<(), ChainError> {
        bootstrap::execute(domain)?;

        Ok(())
    }

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

    fn next_sweep<D: Domain>(&self, domain: &D, after: BlockSlot) -> Result<BlockSlot, ChainError> {
        let summary = eras::load_era_summary(domain)?;

        let next_sweep = pallas_extras::next_epoch_boundary(&summary, after);

        Ok(next_sweep)
    }

    fn mutable_slots(domain: &impl Domain) -> BlockSlot {
        utils::mutable_slots(domain.genesis())
    }

    fn compute_block_utxo_delta(
        &self,
        block: &Self::Block,
        deps: &RawUtxoMap,
    ) -> Result<UtxoSetDelta, ChainError> {
        let delta = utxoset::compute_apply_delta(block.view(), deps)?;

        Ok(delta)
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

pub fn load_current_epoch<D: Domain>(domain: &D) -> Result<EpochState, ChainError> {
    let epoch = domain
        .state3()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_MARK))?
        .ok_or(ChainError::PParamsNotFound)?;

    Ok(epoch)
}

pub fn load_current_pparams<D: Domain>(domain: &D) -> Result<PParamsSet, ChainError> {
    let epoch = load_current_epoch(domain)?;

    Ok(epoch.pparams)
}

#[cfg(test)]
pub fn load_test_genesis(env: &str) -> Genesis {
    use std::path::{Path, PathBuf};

    fn load_json<T>(path: &Path) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        let file = std::fs::File::open(path).unwrap();
        serde_json::from_reader(file).unwrap()
    }

    let test_data = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
        .join("test_data")
        .join(env);

    Genesis {
        byron: load_json(&test_data.join("genesis/byron.json")),
        shelley: load_json(&test_data.join("genesis/shelley.json")),
        alonzo: load_json(&test_data.join("genesis/alonzo.json")),
        conway: load_json(&test_data.join("genesis/conway.json")),
        force_protocol: None,
    }
}
