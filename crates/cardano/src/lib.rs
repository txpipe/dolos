use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// re-export pallas for version compatibility downstream
pub use pallas;

use dolos_core::{batch::WorkBatch, *};

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

#[derive(Serialize, Deserialize, Clone)]
pub struct TrackConfig {
    pub account_state: bool,
    pub asset_state: bool,
    pub pool_state: bool,
    pub epoch_state: bool,
    pub drep_state: bool,
    pub tx_logs: bool,
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct LogConfig {
    pub rewards: bool,
    pub pool_stakes: bool,
    pub epoch_state: bool,
}

impl Default for TrackConfig {
    fn default() -> Self {
        Self {
            account_state: true,
            asset_state: true,
            pool_state: true,
            epoch_state: true,
            drep_state: true,
            tx_logs: true,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Default)]
pub struct Config {
    #[serde(default)]
    pub track: TrackConfig,
    pub log: LogConfig,
    pub stop_epoch: Option<u32>,
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
        sweep::sweep(domain, at, &self.config)
    }

    fn next_sweep<D: Domain>(&self, domain: &D, after: BlockSlot) -> Result<BlockSlot, ChainError> {
        let summary = eras::load_era_summary(domain)?;

        let next_sweep = pallas_extras::next_epoch_boundary(&summary, after);

        Ok(next_sweep)
    }

    fn mutable_slots(domain: &impl Domain) -> BlockSlot {
        utils::mutable_slots(domain.genesis())
    }

    fn compute_delta<D: Domain>(
        &self,
        domain: &D,
        batch: &mut WorkBatch<Self>,
    ) -> Result<(), ChainError> {
        let (_, active_era) = eras::load_active_era(domain)?;
        let (epoch, _) = active_era.slot_epoch(batch.first_slot());
        let active_params = load_effective_pparams(domain, epoch)?;

        for block in batch.blocks.iter_mut() {
            let mut builder = roll::DeltaBuilder::new(
                self.config.track.clone(),
                &active_params,
                block,
                &batch.utxos_decoded,
            );

            builder.crawl()?;

            // TODO: we treat the UTxO set differently due to tech-debt. We should migrate
            // this into the entity system.
            let blockd = block.unwrap_decoded();
            let blockd = blockd.view();
            let utxos = utxoset::compute_apply_delta(blockd, &batch.utxos_decoded)?;
            block.utxo_delta = Some(utxos);
        }

        Ok(())
    }
}

pub fn load_effective_pparams<D: Domain>(
    domain: &D,
    caller_epoch: Epoch,
) -> Result<PParamsSet, ChainError> {
    // the effective pparams are usually the ones for the previous epoch (aka: `set`
    // epoch) except for the initial epoch, where we use the mark epoch since
    // there's nothing before
    let epoch = match caller_epoch {
        0 => load_mark_epoch(domain)?,
        _ => load_set_epoch(domain)?.ok_or(BrokenInvariant::InvalidEpochState)?,
    };

    if epoch.number != 0 && epoch.number != (caller_epoch - 1) {
        return Err(ChainError::from(BrokenInvariant::InvalidEpochState));
    }

    Ok(epoch.pparams)
}

pub fn load_go_epoch<D: Domain>(domain: &D) -> Result<Option<EpochState>, ChainError> {
    let epoch = domain
        .state()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_GO))?;

    Ok(epoch)
}

pub fn load_set_epoch<D: Domain>(domain: &D) -> Result<Option<EpochState>, ChainError> {
    let epoch = domain
        .state()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_SET))?;

    Ok(epoch)
}

pub fn load_mark_epoch<D: Domain>(domain: &D) -> Result<EpochState, ChainError> {
    let epoch = domain
        .state()
        .read_entity_typed::<EpochState>(EpochState::NS, &EntityKey::from(EPOCH_KEY_MARK))?
        .ok_or(ChainError::from(BrokenInvariant::BadBootstrap))?;

    Ok(epoch)
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

    Genesis::from_file_paths(
        &test_data.join("genesis/byron.json"),
        &test_data.join("genesis/shelley.json"),
        &test_data.join("genesis/alonzo.json"),
        &test_data.join("genesis/conway.json"),
        None,
    )
    .unwrap()
}
