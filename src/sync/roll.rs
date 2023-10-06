use gasket::framework::*;
use tracing::info;

use crate::prelude::*;
use crate::storage::rolldb::RollDB;

pub type Cursor = (BlockSlot, BlockHash);
pub type UpstreamPort = gasket::messaging::tokio::InputPort<PullEvent>;
pub type DownstreamPort = gasket::messaging::tokio::OutputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "roll", unit = "PullEvent", worker = "Worker")]
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
}

pub struct Worker {
    last_seq: Option<u64>,
}

impl Worker {
    /// Catch-up output with current persisted state
    ///
    /// Reads from Wal using the latest known cursor and outputs the
    /// corresponding downstream events
    async fn catchup(&mut self, stage: &mut Stage) -> Result<(), WorkerError> {
        let iter = stage.rolldb.crawl_wal_after(self.last_seq);

        for wal in iter {
            let (seq, wal) = wal.or_panic()?;
            info!(seq, "processing wal entry");

            let evt = match wal.action() {
                crate::storage::rolldb::wal::WalAction::Apply => {
                    let cbor = stage.rolldb.get_block(*wal.hash()).or_panic()?.unwrap();
                    RollEvent::Apply(wal.slot(), *wal.hash(), cbor)
                }
                crate::storage::rolldb::wal::WalAction::Undo => {
                    let cbor = stage.rolldb.get_block(*wal.hash()).or_panic()?.unwrap();
                    RollEvent::Undo(wal.slot(), *wal.hash(), cbor)
                }
                crate::storage::rolldb::wal::WalAction::Origin => RollEvent::Origin,
                crate::storage::rolldb::wal::WalAction::Mark => {
                    // TODO: do we really need mark events?
                    // for now we bail
                    continue;
                }
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
                let last_seq = stage.rolldb.find_wal_seq(Some(cursor)).or_panic()?;
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
                    stage.rolldb.roll_back_origin().or_panic()?;
                }
            },
        }

        // TODO: if we have a wel seq in memory, we should avoid scanning for a
        // particular slot/hash
        self.catchup(stage).await?;

        // TODO: don't do this while doing full sync
        stage.rolldb.prune_wal().or_panic()?;

        Ok(())
    }
}
