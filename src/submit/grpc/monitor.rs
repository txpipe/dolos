// starts following the chaindb from the point

use gasket::framework::*;
use pallas::{
    ledger::traverse::MultiEraBlock,
    storage::rolldb::wal::{self, Log},
};
use tracing::info;

use super::{BlockHeight, TxHash};

pub type MempoolSender = gasket::messaging::tokio::OutputPort<BlockMonitorMessage>;

#[derive(Clone, Debug)]
pub enum BlockMonitorMessage {
    NewBlock(BlockHeight, Vec<TxHash>),
    Rollback(BlockHeight),
}

#[derive(Stage)]
#[stage(name = "monitor", unit = "Vec<BlockMonitorMessage>", worker = "Worker")]
pub struct Stage {
    wal: wal::Store,
    last_wal_seq: u64,

    pub downstream_mempool: MempoolSender,
    // #[metric]
    // received_txs: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(wal: wal::Store, last_wal_seq: u64) -> Self {
        Self {
            wal,
            last_wal_seq,
            downstream_mempool: Default::default(),
        }
    }
}

pub struct Worker;

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self)
    }

    /// Wait until WAL tip changes
    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<Vec<BlockMonitorMessage>>, WorkerError> {
        stage.wal.tip_change.notified().await;

        let mut updates = vec![];

        for entry in stage.wal.crawl_after(Some(stage.last_wal_seq)) {
            let (seq, log) = entry.or_restart()?;

            match log {
                Log::Apply(_, _, body) => {
                    let block = MultiEraBlock::decode(&body).or_panic()?;

                    let height = block.number();
                    let txs = block.txs().iter().map(|x| x.hash()).collect::<Vec<_>>();

                    updates.push(BlockMonitorMessage::NewBlock(height, txs))
                }
                Log::Mark(_, _, body) => {
                    let block = MultiEraBlock::decode(&body).or_panic()?;

                    let height = block.number();

                    updates.push(BlockMonitorMessage::Rollback(height))
                }
                Log::Undo(_, _, _) => (),
                Log::Origin => (),
            }

            stage.last_wal_seq = seq;
        }

        Ok(WorkSchedule::Unit(updates))
    }

    /// Send a ChainUpdate message to the mempool stage
    async fn execute(
        &mut self,
        unit: &Vec<BlockMonitorMessage>,
        stage: &mut Stage,
    ) -> Result<(), WorkerError> {
        for update in unit.clone() {
            info!("sending chain update to mempool: {:?}", update);
            stage
                .downstream_mempool
                .send(update.into())
                .await
                .or_panic()?;
        }

        Ok(())
    }
}
