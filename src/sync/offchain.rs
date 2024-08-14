use gasket::framework::*;
use pallas::ledger::configs::{byron, shelley};
use pallas::ledger::traverse::MultiEraBlock;
use tracing::{debug, info};

use crate::wal::{self, LogValue, WalReader as _};
use crate::{ledger, prelude::*};

pub type UpstreamPort = gasket::messaging::InputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "offchain", unit = "RollEvent", worker = "Worker")]
pub struct Stage {
    wal: crate::wal::redb::WalStore,
    runtime: crate::balius::Runtime,

    pub upstream: UpstreamPort,
}

impl Stage {
    pub fn new(wal: crate::wal::redb::WalStore, runtime: crate::balius::Runtime) -> Self {
        Self {
            wal,
            runtime,
            upstream: Default::default(),
        }
    }

    fn process_origin(&mut self) -> Result<(), WorkerError> {
        info!("applying origin");

        //TODO

        Ok(())
    }

    fn process_undo(&mut self, block: &wal::RawBlock) -> Result<(), WorkerError> {
        let wal::RawBlock { slot, body, .. } = block;

        info!(slot, "undoing block");

        let block = MultiEraBlock::decode(body).or_panic()?;

        self.runtime.undo_block(&block).or_panic()?;

        Ok(())
    }

    fn process_apply(&mut self, block: &wal::RawBlock) -> Result<(), WorkerError> {
        let wal::RawBlock { slot, body, .. } = block;

        info!(slot, "applying block");

        let block = MultiEraBlock::decode(body).or_panic()?;

        self.runtime.apply_block(&block).or_panic()?;

        Ok(())
    }

    fn process_wal(&mut self, log: wal::LogValue) -> Result<(), WorkerError> {
        match log {
            LogValue::Mark(wal::ChainPoint::Origin) => self.process_origin(),
            LogValue::Apply(x) => self.process_apply(&x),
            LogValue::Undo(x) => self.process_undo(&x),
            // we can skip marks since we know they have been already applied
            LogValue::Mark(..) => Ok(()),
        }
    }
}

pub struct Worker(wal::LogSeq);

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let cursor = stage.runtime.cursor().or_panic()?;

        info!(?cursor, "cursor found");

        let point = match cursor {
            Some(crate::balius::ChainPoint(s, h)) => wal::ChainPoint::Specific(s, h),
            None => wal::ChainPoint::Origin,
        };

        let seq = stage.wal.assert_point(&point).or_panic()?;

        info!(seq, "wal sequence found");

        Ok(Self(seq))
    }

    async fn schedule(
        &mut self,
        stage: &mut Stage,
    ) -> Result<WorkSchedule<RollEvent>, WorkerError> {
        let msg = stage.upstream.recv().await.or_panic()?;

        Ok(WorkSchedule::Unit(msg.payload))
    }

    /// Catch-up ledger with latest state of WAL
    ///
    /// Reads from WAL using the latest known cursor and applies the
    /// corresponding downstream changes to the ledger
    async fn execute(&mut self, _: &RollEvent, stage: &mut Stage) -> Result<(), WorkerError> {
        let iter = stage.wal.crawl_from(Some(self.0)).or_panic()?.skip(1);

        // TODO: analyze scenario where we're too far behind and this for loop takes
        // longer that the allocated policy timeout.

        for (seq, log) in iter {
            debug!(seq, "processing wal entry");
            stage.process_wal(log)?;
            self.0 = seq;
        }

        Ok(())
    }
}
