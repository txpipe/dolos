use gasket::framework::*;
use tracing::info;

use crate::adapters::WalAdapter;
use crate::prelude::*;

pub type Cursor = (BlockSlot, BlockHash);
pub type UpstreamPort = gasket::messaging::InputPort<PullEvent>;
pub type DownstreamPort = gasket::messaging::OutputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "roll", unit = "PullEvent", worker = "Worker")]
pub struct Stage {
    store: WalAdapter,

    pub upstream: UpstreamPort,
    pub downstream: DownstreamPort,

    housekeeping_interval: std::time::Duration,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    roll_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(store: WalAdapter, housekeeping_interval: std::time::Duration) -> Self {
        Self {
            store,
            upstream: Default::default(),
            downstream: Default::default(),
            block_count: Default::default(),
            roll_count: Default::default(),
            housekeeping_interval,
        }
    }

    async fn process_pull_event(&mut self, unit: &PullEvent) -> Result<(), WorkerError> {
        match unit {
            PullEvent::RollForward(block) => {
                let block = RawBlock {
                    slot: block.slot,
                    hash: block.hash,
                    era: block.era,
                    body: block.body.clone(),
                };

                info!(block.slot, %block.hash, "extending wal");

                self.store.roll_forward(std::iter::once(block)).or_panic()?;
            }
            PullEvent::Rollback(point) => {
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
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Worker {
            housekeeping_timer: tokio::time::interval(stage.housekeeping_interval),
        })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<PullEvent>, WorkerError> {
        let msg = stage.upstream.recv().await.or_panic()?;
        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &PullEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        stage.process_pull_event(unit).await?;

        Ok(())
    }
}
