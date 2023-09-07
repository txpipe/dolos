use gasket::framework::*;

use crate::prelude::*;
use crate::storage::rolldb::RollDB;

pub type Cursor = (BlockSlot, BlockHash);
pub type UpstreamPort = gasket::messaging::tokio::InputPort<PullEvent>;
pub type DownstreamPort = gasket::messaging::tokio::OutputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "reducer", unit = "PullEvent", worker = "Worker")]
pub struct Stage {
    rolldb: RollDB,

    cursor: Option<Cursor>,

    pub upstream: UpstreamPort,
    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    roll_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(rolldb: RollDB, cursor: Option<Cursor>) -> Self {
        Self {
            rolldb,
            cursor,
            upstream: Default::default(),
            downstream: Default::default(),
            block_count: Default::default(),
            roll_count: Default::default(),
        }
    }

    /// Catch-up output with current persisted state
    ///
    /// Reads from Wal using the latest known cursor and outputs the corresponding downstream events
    async fn catchup(&mut self) -> Result<(), WorkerError> {
        let iter = self.rolldb.crawl_wal_from_cursor(self.cursor).or_panic()?;

        for wal in iter {
            let (_, wal) = wal.or_panic()?;

            let cbor = self.rolldb.get_block(*wal.hash()).or_panic()?.unwrap();

            let evt = match wal.action() {
                crate::storage::rolldb::wal::WalAction::Apply => {
                    RollEvent::Apply(wal.slot(), *wal.hash(), cbor)
                }
                crate::storage::rolldb::wal::WalAction::Undo => {
                    RollEvent::Undo(wal.slot(), *wal.hash(), cbor)
                }
                crate::storage::rolldb::wal::WalAction::Mark => {
                    // TODO: do we really need mark events?
                    // for now we bail
                    continue;
                }
            };

            self.downstream.send(evt.into()).await.or_panic()?;
            self.cursor = Some((wal.slot(), *wal.hash()));
        }

        Ok(())
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
    ) -> Result<WorkSchedule<PullEvent>, WorkerError> {
        let msg = stage.upstream.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    async fn execute(&mut self, unit: &PullEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            PullEvent::RollForward(slot, hash, body) => {
                stage
                    .rolldb
                    .roll_forward(*slot, *hash, body.clone())
                    .or_panic()?;
            }
            PullEvent::Rollback(point) => match point {
                pallas::network::miniprotocols::Point::Specific(slot, _) => {
                    stage.rolldb.roll_back(*slot).or_panic()?;
                }
                pallas::network::miniprotocols::Point::Origin => {
                    //todo!();
                }
            },
        }

        // TODO: if we have a wel seq in memory, we should avoid scanning for a particular slot/hash
        stage.catchup().await?;

        // TODO: don't do this while doing full sync
        stage.rolldb.prune().or_panic()?;

        Ok(())
    }
}
