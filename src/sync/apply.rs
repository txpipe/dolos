use gasket::framework::*;
use pallas::ledger::configs::{byron, shelley};
use pallas::ledger::traverse::MultiEraBlock;
use tracing::{debug, info};

use crate::wal::{self, LogValue, WalReader as _};
use crate::{ledger, prelude::*};

pub type UpstreamPort = gasket::messaging::InputPort<RollEvent>;

#[derive(Stage)]
#[stage(name = "ledger", unit = "RollEvent", worker = "Worker")]
pub struct Stage {
    wal: crate::wal::redb::WalStore,
    ledger: crate::state::LedgerStore,
    byron: byron::GenesisFile,
    shelley: shelley::GenesisFile,
    mempool: crate::mempool::Mempool, // Add this line

    max_ledger_history: Option<u64>,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(
        wal: crate::wal::redb::WalStore,
        ledger: crate::state::LedgerStore,
        mempool: crate::mempool::Mempool,
        byron: byron::GenesisFile,
        shelley: shelley::GenesisFile,
        max_ledger_history: Option<u64>,
    ) -> Self {
        Self {
            wal,
            ledger,
            mempool,
            byron,
            shelley,
            max_ledger_history,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }

    fn process_origin(&mut self) -> Result<(), WorkerError> {
        info!("applying origin");

        let delta = crate::ledger::compute_origin_delta(&self.byron);
        self.ledger.apply(&[delta]).or_panic()?;

        Ok(())
    }

    fn process_undo(&mut self, block: &wal::RawBlock) -> Result<(), WorkerError> {
        let wal::RawBlock { slot, body, .. } = block;

        info!(slot, "undoing block");

        let block = MultiEraBlock::decode(body).or_panic()?;
        let context = crate::state::load_slice_for_block(&block, &self.ledger, &[]).or_panic()?;

        let delta = crate::ledger::compute_undo_delta(&block, context).or_panic()?;
        self.ledger.apply(&[delta]).or_panic()?;

        self.mempool.undo_block(&block);

        Ok(())
    }

    fn process_apply(&mut self, block: &wal::RawBlock) -> Result<(), WorkerError> {
        let wal::RawBlock { slot, body, .. } = block;

        info!(slot, "applying block");

        let block = MultiEraBlock::decode(body).or_panic()?;

        crate::state::apply_block_batch(
            [&block],
            &mut self.ledger,
            &self.byron,
            &self.shelley,
            self.max_ledger_history,
        )
        .or_panic()?;

        self.mempool.apply_block(&block);

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
        let cursor = stage.ledger.cursor().or_panic()?;

        info!(?cursor, "cursor found");

        let point = match cursor {
            Some(ledger::ChainPoint(s, h)) => wal::ChainPoint::Specific(s, h),
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
