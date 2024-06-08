// starts following the chaindb from the point

use std::time::Duration;

use gasket::framework::*;
use pallas::ledger::traverse::MultiEraBlock;
use tracing::debug;

use super::{BlockSlot, TxHash};

use crate::wal::{self, WalReader};

pub type MempoolSender = gasket::messaging::OutputPort<BlockMonitorMessage>;

#[derive(Clone, Debug)]
pub enum BlockMonitorMessage {
    NewBlock(BlockSlot, Vec<TxHash>),
    Rollback(BlockSlot),
}

#[derive(Stage)]
#[stage(name = "monitor", unit = "()", worker = "Worker")]
pub struct Stage {
    wal: wal::redb::WalStore,

    pub downstream_mempool: MempoolSender,
    // #[metric]
    // received_txs: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(wal: wal::redb::WalStore) -> Self {
        Self {
            wal,
            downstream_mempool: Default::default(),
        }
    }
}

pub struct Worker(Option<wal::LogSeq>);

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let cursor = stage.wal.find_tip().or_panic()?.map(|(seq, _)| seq);

        Ok(Self(cursor))
    }

    /// Wait until WAL tip changes
    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<()>, WorkerError> {
        tokio::select! {
            _ = stage.wal.tip_change() => {
                debug!("tip changed");
                Ok(WorkSchedule::Unit(()))
            }
            _ = tokio::time::sleep(Duration::from_secs(20)) => {
                Ok(WorkSchedule::Idle)
            }
        }
    }

    async fn execute(&mut self, _: &(), stage: &mut Stage) -> Result<(), WorkerError> {
        let iter = stage.wal.crawl_from(self.0).or_panic()?.skip(1);

        for (seq, log) in iter {
            match log {
                wal::LogValue::Apply(wal::RawBlock { slot, body, .. }) => {
                    let block = MultiEraBlock::decode(&body).or_panic()?;

                    let txs = block.txs().iter().map(|x| x.hash()).collect::<Vec<_>>();

                    debug!("sending chain update to mempool");

                    stage
                        .downstream_mempool
                        .send(BlockMonitorMessage::NewBlock(slot, txs).into())
                        .await
                        .or_panic()?;
                }
                wal::LogValue::Mark(wal::ChainPoint::Specific(slot, _)) => {
                    debug!("sending chain update to mempool");

                    stage
                        .downstream_mempool
                        .send(BlockMonitorMessage::Rollback(slot).into())
                        .await
                        .or_panic()?;
                }
                wal::LogValue::Undo(..) => {
                    // TODO: this should re-instate txs
                }
                _ => (),
            }

            self.0 = Some(seq);
        }

        Ok(())
    }
}
