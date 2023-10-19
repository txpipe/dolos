use gasket::framework::*;
use pallas::storage::rolldb::wal;
use tracing::{debug, trace};

use crate::prelude::*;

pub type Cursor = (BlockSlot, BlockHash);
pub type UpstreamPort = gasket::messaging::tokio::InputPort<PullEvent>;
pub type DownstreamPort = gasket::messaging::tokio::OutputPort<RollEvent>;

/// Catch-up output with current persisted state
///
/// Reads from Wal using the latest known cursor and outputs the
/// corresponding downstream events
async fn catchup_downstream(
    store: &wal::Store,
    mut last_seq: Option<u64>,
    port: &mut DownstreamPort,
) -> Result<Option<u64>, WorkerError> {
    let iter = store.crawl_after(last_seq);

    for wal in iter {
        let (seq, wal) = wal.or_panic()?;
        trace!(seq, "processing wal entry");

        let evt = match wal {
            wal::Log::Apply(slot, hash, body) => RollEvent::Apply(slot, hash, body),
            wal::Log::Undo(slot, hash, body) => RollEvent::Undo(slot, hash, body),
            wal::Log::Origin => RollEvent::Origin,
            wal::Log::Mark(..) => continue,
        };

        port.send(evt.into()).await.or_panic()?;
        last_seq = Some(seq);
    }

    Ok(last_seq)
}

fn update_store(unit: &PullEvent, store: &mut wal::Store) -> Result<(), WorkerError> {
    match unit {
        PullEvent::RollForward(slot, hash, body) => {
            store.roll_forward(*slot, *hash, body.clone()).or_panic()?;
            debug!(slot, %hash, "wal extended");
        }
        PullEvent::Rollback(point) => match point {
            pallas::network::miniprotocols::Point::Specific(slot, _) => {
                store.roll_back(*slot).or_panic()?;
                debug!(slot, "wal rollback");
            }
            pallas::network::miniprotocols::Point::Origin => {
                store.roll_back_origin().or_panic()?;
                debug!("wal rollback to origin");
            }
        },
    }

    Ok(())
}

#[derive(Stage)]
#[stage(name = "roll", unit = "PullEvent", worker = "Worker")]
pub struct Stage {
    store: wal::Store,

    cursor_chain: Option<Cursor>,
    cursor_ledger: Option<Cursor>,

    pub upstream: UpstreamPort,

    pub downstream_chain: DownstreamPort,
    pub downstream_ledger: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    roll_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(
        store: wal::Store,
        cursor_chain: Option<Cursor>,
        cursor_ledger: Option<Cursor>,
    ) -> Self {
        Self {
            store,
            cursor_chain,
            cursor_ledger,
            upstream: Default::default(),
            downstream_chain: Default::default(),
            downstream_ledger: Default::default(),
            block_count: Default::default(),
            roll_count: Default::default(),
        }
    }
}

pub struct Worker {
    last_seq_chain: Option<u64>,
    last_seq_ledger: Option<u64>,
}

impl Worker {}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let last_seq_chain = stage.store.find_wal_seq(stage.cursor_chain).or_panic()?;
        let last_seq_ledger = stage.store.find_wal_seq(stage.cursor_ledger).or_panic()?;

        Ok(Self {
            last_seq_chain,
            last_seq_ledger,
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
        update_store(unit, &mut stage.store)?;

        self.last_seq_chain = catchup_downstream(
            &stage.store,
            self.last_seq_chain,
            &mut stage.downstream_chain,
        )
        .await
        .or_panic()?;

        self.last_seq_ledger = catchup_downstream(
            &stage.store,
            self.last_seq_ledger,
            &mut stage.downstream_ledger,
        )
        .await
        .or_panic()?;

        // TODO: don't do this while doing full sync
        stage.store.prune_wal().or_panic()?;

        Ok(())
    }
}
