// starts following the chaindb from the point

use std::time::Duration;

use gasket::framework::*;
use pallas::{
    ledger::traverse::MultiEraBlock,
    storage::rolldb::wal::{self, Log},
};
use tracing::debug;

use super::{BlockHeight, TxHash};

pub type MempoolSender = gasket::messaging::tokio::OutputPort<BlockMonitorMessage>;

#[derive(Clone, Debug)]
pub enum BlockMonitorMessage {
    NewBlock(BlockHeight, Vec<TxHash>),
    Rollback(BlockHeight),
}

#[derive(Stage)]
#[stage(name = "monitor", unit = "()", worker = "Worker")]
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
    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        tokio::select! {
            _ = stage.wal.tip_change.notified() => {
                debug!("tip changed");
                Ok(WorkSchedule::Unit(()))
            }
            _ = tokio::time::sleep(Duration::from_secs(20)) => {
                Ok(WorkSchedule::Idle)
            }
        }
    }

    async fn execute(&mut self, _: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        for entry in stage.wal.crawl_after(Some(stage.last_wal_seq)) {
            let (seq, log) = entry.or_restart()?;

            match log {
                Log::Apply(_, _, body) => {
                    let block = MultiEraBlock::decode(&body).or_panic()?;

                    let height = block.number();
                    let txs = block.txs().iter().map(|x| x.hash()).collect::<Vec<_>>();

                    debug!("sending chain update to mempool");

                    stage
                        .downstream_mempool
                        .send(BlockMonitorMessage::NewBlock(height, txs).into())
                        .await
                        .or_panic()?;
                }
                Log::Mark(_, _, body) => {
                    let block = MultiEraBlock::decode(&body).or_panic()?;
                    let height = block.number();

                    debug!("sending chain update to mempool");

                    stage
                        .downstream_mempool
                        .send(BlockMonitorMessage::Rollback(height).into())
                        .await
                        .or_panic()?;
                }
                Log::Undo(_, _, _) => (),
                Log::Origin => (),
            }

            stage.last_wal_seq = seq;
        }

        Ok(())
    }
}
