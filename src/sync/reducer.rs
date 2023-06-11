use gasket::framework::*;

use crate::prelude::*;
use crate::storage::rolldb::RollDB;

pub type UpstreamPort = gasket::messaging::tokio::InputPort<UpstreamEvent>;

#[derive(Stage)]
#[stage(name = "reducer", unit = "UpstreamEvent", worker = "Worker")]
pub struct Stage {
    rolldb: RollDB,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(rolldb: RollDB) -> Self {
        Self {
            rolldb,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }
}

pub struct Worker;

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self)
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<UpstreamEvent>, WorkerError> {
        let msg = stage.upstream.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(
        &mut self,
        unit: &UpstreamEvent,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        match unit {
            UpstreamEvent::RollForward(slot, hash, body) => {
                stage
                    .rolldb
                    .roll_forward(*slot, hash.clone(), body.clone())
                    .or_panic()?;
            }
            UpstreamEvent::Rollback(point) => match point {
                pallas::network::miniprotocols::Point::Specific(slot, _) => {
                    stage.rolldb.roll_back(*slot).or_panic()?;
                }
                pallas::network::miniprotocols::Point::Origin => {
                    //todo!();
                }
            },
        }

        // TODO: don't do this while doing full sync
        stage.rolldb.compact().or_panic()?;

        Ok(())
    }
}
