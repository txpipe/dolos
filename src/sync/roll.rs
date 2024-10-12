use gasket::framework::*;
use tracing::info;

use crate::{
    prelude::*,
    wal::{self, redb::WalStore, WalWriter},
};

pub type Cursor = (BlockSlot, BlockHash);
pub type UpstreamPort = gasket::messaging::InputPort<PullEvent>;
pub type DownstreamPort = gasket::messaging::OutputPort<RollEvent>;

const HOUSEKEEPING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

pub enum WorkUnit {
    PullEvent(PullEvent),
    Housekeeping,
}

#[derive(Stage)]
#[stage(name = "roll", unit = "WorkUnit", worker = "Worker")]
pub struct Stage {
    store: WalStore,

    pub upstream: UpstreamPort,
    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    roll_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(store: WalStore) -> Self {
        Self {
            store,
            upstream: Default::default(),
            downstream: Default::default(),
            block_count: Default::default(),
            roll_count: Default::default(),
        }
    }

    async fn process_pull_event(&mut self, unit: &PullEvent) -> Result<(), WorkerError> {
        match unit {
            PullEvent::RollForward(block) => {
                let block = wal::RawBlock {
                    slot: block.slot,
                    hash: block.hash,
                    era: block.era,
                    body: block.body.clone(),
                };

                info!(block.slot, %block.hash, "extending wal");

                self.store.roll_forward(std::iter::once(block)).or_panic()?;
            }
            PullEvent::Rollback(point) => {
                let point = match point {
                    pallas::network::miniprotocols::Point::Origin => wal::ChainPoint::Origin,
                    pallas::network::miniprotocols::Point::Specific(s, h) => {
                        wal::ChainPoint::Specific(*s, h.as_slice().into())
                    }
                };

                info!(?point, "rolling back wal");

                self.store.roll_back(&point).or_panic()?;
            }
        }

        self.downstream
            .send(RollEvent::TipChanged.into())
            .await
            .or_panic()?;

        Ok(())
    }
}

pub struct Worker {
    housekeeping_timer: tokio::time::Interval,
}

impl Worker {}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Worker {
            // TODO: make this interval user-configurable
            housekeeping_timer: tokio::time::interval(HOUSEKEEPING_INTERVAL),
        })
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<WorkUnit>, WorkerError> {
        tokio::select! {
            msg = stage.upstream.recv() => {
                let msg = msg.or_panic()?;
                Ok(WorkSchedule::Unit(WorkUnit::PullEvent(msg.payload)))
            }
            _ = self.housekeeping_timer.tick() => {
                Ok(WorkSchedule::Unit(WorkUnit::Housekeeping))
            }
        }
    }

    async fn execute(&mut self, unit: &WorkUnit, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            WorkUnit::PullEvent(pull) => stage.process_pull_event(pull).await?,
            WorkUnit::Housekeeping => stage.store.housekeeping().or_panic()?,
        }

        Ok(())
    }
}
