use std::sync::Arc;

use gasket::framework::*;
use pallas::ledger::traverse::MultiEraBlock;
use tracing::{debug, info};

use crate::prelude::*;
use crate::wal::{self, LogValue, WalReader as _};

use dolos_core::Genesis;

pub type UpstreamPort = gasket::messaging::InputPort<RollEvent>;

pub enum WorkUnit {
    ApplyEvent,
    Housekeeping,
}

#[derive(Stage)]
#[stage(name = "apply", unit = "WorkUnit", worker = "Worker")]
pub struct Stage {
    wal: crate::wal::redb::WalStore,
    ledger: crate::state::LedgerStore,
    chain: crate::chain::ChainStore,
    genesis: Arc<Genesis>,
    mempool: crate::mempool::Mempool, // Add this line

    max_ledger_history: Option<u64>,
    housekeeping_interval: std::time::Duration,

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
        chain: crate::chain::ChainStore,
        mempool: crate::mempool::Mempool,
        genesis: Arc<Genesis>,
        max_ledger_history: Option<u64>,
        housekeeping_interval: std::time::Duration,
    ) -> Self {
        Self {
            wal,
            ledger,
            chain,
            mempool,
            genesis,
            max_ledger_history,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
            housekeeping_interval,
        }
    }

    fn process_origin(&self) -> Result<(), WorkerError> {
        info!("applying origin");

        let deltas = vec![dolos_cardano::compute_origin_delta(&self.genesis)];
        self.ledger.apply(&deltas).or_panic()?;
        self.chain.apply(&deltas).or_panic()?;

        Ok(())
    }

    fn process_undo(&self, block: &wal::RawBlock) -> Result<(), WorkerError> {
        let wal::RawBlock { slot, body, .. } = block;

        info!(slot, "undoing block");

        let block = MultiEraBlock::decode(body).or_panic()?;
        let context = crate::state::load_slice_for_block(&block, &self.ledger, &[]).or_panic()?;

        let deltas = vec![dolos_cardano::compute_undo_delta(&block, context).or_panic()?];
        self.ledger.apply(&deltas).or_panic()?;
        self.chain.apply(&deltas).or_panic()?;

        self.mempool.undo_block(&block);

        Ok(())
    }

    fn process_apply(&self, block: &wal::RawBlock) -> Result<(), WorkerError> {
        let wal::RawBlock { slot, body, .. } = block;

        info!(slot, "applying block");

        let block = MultiEraBlock::decode(body).or_panic()?;

        let deltas =
            crate::state::calculate_block_batch_deltas([&block], &self.ledger).or_panic()?;

        self.chain.apply(&deltas).or_panic()?;
        crate::state::apply_delta_batch(
            deltas,
            &self.ledger,
            &self.genesis,
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

pub struct Worker {
    logseq: wal::LogSeq,
    housekeeping_timer: tokio::time::Interval,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let cursor = stage.ledger.cursor().or_panic()?;

        if cursor.is_none() {
            info!("cursor not found, applying origin");
            stage.process_origin()?;
        } else {
            info!(?cursor, "cursor found");
        }

        let point = match cursor {
            Some(ChainPoint(s, h)) => wal::ChainPoint::Specific(s, h),
            None => wal::ChainPoint::Origin,
        };

        let seq = stage.wal.assert_point(&point).or_panic()?;

        info!(seq, "wal sequence found");

        Ok(Self {
            logseq: seq,
            housekeeping_timer: tokio::time::interval(stage.housekeeping_interval),
        })
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<WorkUnit>, WorkerError> {
        {
            tokio::select! {
                msg = stage.upstream.recv() => {
                    let _ = msg.or_panic()?;
                    Ok(WorkSchedule::Unit(WorkUnit::ApplyEvent))
                }
                _ = self.housekeeping_timer.tick() => {
                    Ok(WorkSchedule::Unit(WorkUnit::Housekeeping))
                }
            }
        }
    }

    /// Catch-up ledger with latest state of WAL
    ///
    /// Reads from WAL using the latest known cursor and applies the
    /// corresponding downstream changes to the ledger
    async fn execute(&mut self, unit: &WorkUnit, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            WorkUnit::ApplyEvent => {
                let iter = stage.wal.crawl_from(Some(self.logseq)).or_panic()?.skip(1);

                // TODO: analyze scenario where we're too far behind and this for loop takes
                // longer that the allocated policy timeout.

                for (seq, log) in iter {
                    debug!(seq, "processing wal entry");
                    stage.process_wal(log)?;
                    self.logseq = seq;
                }
            }
            WorkUnit::Housekeeping => stage.chain.housekeeping().or_panic()?,
        }

        Ok(())
    }
}
