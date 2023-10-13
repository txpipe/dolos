use gasket::framework::*;
use pallas::storage::rolldb::wal;
use tracing::info;

use crate::prelude::*;

pub type Cursor = (BlockSlot, BlockHash);
pub type UpstreamPort = gasket::messaging::tokio::InputPort<PullEvent>;
pub type DownstreamPort = gasket::messaging::tokio::OutputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "roll", unit = "PullEvent", worker = "Worker")]
pub struct Stage {
    store: wal::Store,

    cursor: Option<Cursor>,

    pub upstream: UpstreamPort,
    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    roll_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(store: wal::Store, cursor: Option<Cursor>) -> Self {
        Self {
            store,
            cursor,
            upstream: Default::default(),
            downstream: Default::default(),
            block_count: Default::default(),
            roll_count: Default::default(),
        }
    }
}

pub struct Worker {
    last_seq: Option<u64>,
}

impl Worker {
    fn update_store(&self, unit: &PullEvent, store: &mut wal::Store) -> Result<(), WorkerError> {
        match unit {
            PullEvent::RollForward(slot, hash, body) => {
                store.roll_forward(*slot, *hash, body.clone()).or_panic()?;
            }
            PullEvent::Rollback(point) => match point {
                pallas::network::miniprotocols::Point::Specific(slot, _) => {
                    store.roll_back(*slot).or_panic()?;
                }
                pallas::network::miniprotocols::Point::Origin => {
                    store.roll_back_origin().or_panic()?;
                }
            },
        }

        Ok(())
    }

    /// Catch-up output with current persisted state
    ///
    /// Reads from Wal using the latest known cursor and outputs the
    /// corresponding downstream events
    async fn catchup_dowstream(&mut self, stage: &mut Stage) -> Result<(), WorkerError> {
        let iter = stage.store.crawl_after(self.last_seq);

        for wal in iter {
            let (seq, wal) = wal.or_panic()?;
            info!(seq, "processing wal entry");

            let evt = match wal {
                wal::Log::Apply(slot, hash, body) => RollEvent::Apply(slot, hash, body),
                wal::Log::Undo(slot, hash, body) => RollEvent::Undo(slot, hash, body),
                wal::Log::Origin => RollEvent::Origin,
                wal::Log::Mark(..) => continue,
            };

            stage.downstream.send(evt.into()).await.or_panic()?;
            self.last_seq = Some(seq);
        }

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let last_seq = match stage.cursor {
            Some(cursor) => {
                let last_seq = stage.store.find_wal_seq(Some(cursor)).or_panic()?;
                Some(last_seq)
            }
            None => None,
        };

        Ok(Self { last_seq })
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<PullEvent>, WorkerError> {
        let msg = stage.upstream.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &PullEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        self.update_store(unit, &mut stage.store)?;

        self.catchup_dowstream(stage).await?;

        // TODO: don't do this while doing full sync
        stage.store.prune_wal().or_panic()?;

        Ok(())
    }
}
