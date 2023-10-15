use gasket::framework::*;
use pallas::storage::rolldb::chain;
use tracing::info;

use crate::prelude::*;

pub type UpstreamPort = gasket::messaging::tokio::InputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "chain", unit = "RollEvent", worker = "Worker")]
pub struct Stage {
    chain: chain::Store,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(chain: chain::Store) -> Self {
        Self {
            chain,
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
            RollEvent::Apply(slot, hash, cbor) => {
                info!(slot, "extending chain");

                stage
                    .chain
                    .roll_forward(*slot, *hash, cbor.clone())
                    .or_panic()?;
            }
            RollEvent::Undo(slot, _, _) => {
                info!(slot, "rolling back chain");
                stage.chain.roll_back(*slot).or_panic()?;
            }
            RollEvent::Origin => {
                info!("rolling back to origin");
                stage.chain.roll_back_origin().or_panic()?;
            }
        };

        Ok(())
    }
}
