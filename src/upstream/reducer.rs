use std::path::{Path, PathBuf};

use gasket::framework::*;

use crate::prelude::*;
use crate::rolldb::RollDB;

pub type UpstreamPort = gasket::messaging::tokio::InputPort<UpstreamEvent>;

#[derive(Stage)]
#[stage(name = "reducer", unit = "UpstreamEvent", worker = "Worker")]
pub struct Stage {
    path: PathBuf,

    k_param: u64,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(path: &Path, k_param: u64) -> Self {
        Self {
            path: PathBuf::from(path),
            k_param,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }
}

pub struct Worker {
    db: RollDB,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {
            db: RollDB::open(&stage.path).or_panic()?,
        })
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
                self.db
                    .roll_forward(*slot, hash.clone(), body.clone())
                    .or_panic()?;
            }
            UpstreamEvent::Rollback(point) => match point {
                pallas::network::miniprotocols::Point::Specific(slot, _) => {
                    self.db.roll_back(*slot).or_panic()?;
                }
                pallas::network::miniprotocols::Point::Origin => {
                    //todo!();
                }
            },
        }

        // TODO: don't do this while doing full sync
        self.db.compact(stage.k_param).or_panic()?;

        Ok(())
    }
}
