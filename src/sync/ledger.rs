use gasket::framework::*;
use pallas::ledger::configs::{byron, shelley};
use pallas::ledger::traverse::MultiEraBlock;
use tracing::info;

use crate::prelude::*;

pub type UpstreamPort = gasket::messaging::InputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "ledger", unit = "RollEvent", worker = "Worker")]
pub struct Stage {
    ledger: crate::ledger::store::LedgerStore,
    byron: byron::GenesisFile,
    shelley: shelley::GenesisFile,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(
        ledger: crate::ledger::store::LedgerStore,
        byron: byron::GenesisFile,
        shelley: shelley::GenesisFile,
    ) -> Self {
        Self {
            ledger,
            byron,
            shelley,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }
}

pub struct Worker;

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<RollEvent>, WorkerError> {
        let msg = stage.upstream.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &RollEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            RollEvent::Apply(slot, _, cbor) => {
                info!(slot, "applying block");

                let block = MultiEraBlock::decode(cbor).or_panic()?;

                crate::ledger::import_block_batch(
                    &[block],
                    &mut stage.ledger,
                    &stage.byron,
                    &stage.shelley,
                )
                .or_panic()?;
            }
            RollEvent::Undo(slot, _, cbor) => {
                info!(slot, "undoing block");

                let block = MultiEraBlock::decode(cbor).or_panic()?;
                let context =
                    crate::ledger::load_slice_for_block(&block, &stage.ledger, &[]).or_panic()?;

                let delta = crate::ledger::compute_undo_delta(&block, context).or_panic()?;
                stage.ledger.apply(&[delta]).or_panic()?;
            }
            RollEvent::Origin => {
                info!("applying origin");

                let delta = crate::ledger::compute_origin_delta(&stage.byron);
                stage.ledger.apply(&[delta]).or_panic()?;
            }
        };

        Ok(())
    }
}
