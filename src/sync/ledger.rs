use gasket::framework::*;
use pallas::ledger::configs::byron::GenesisFile;
use tracing::info;

use crate::prelude::*;
use crate::storage::applydb::ApplyDB;

pub type UpstreamPort = gasket::messaging::tokio::InputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "ledger", unit = "RollEvent", worker = "Worker")]
pub struct Stage {
    ledger: ApplyDB,
    genesis: GenesisFile,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(ledger: ApplyDB, genesis: GenesisFile) -> Self {
        Self {
            ledger,
            genesis,
            upstream: Default::default(),
            // downstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }
}

impl Stage {}

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
                stage.ledger.apply_block(cbor).or_panic()?;
            }
            RollEvent::Undo(slot, _, cbor) => {
                info!(slot, "undoing block");
                stage.ledger.undo_block(cbor).or_panic()?;
            }
            RollEvent::Origin => {
                info!("applying origin");
                stage.ledger.apply_origin(&stage.genesis).or_panic()?;
            }
        };

        Ok(())
    }
}
