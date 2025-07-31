use gasket::framework::*;
use tracing::{debug, info};

use crate::{adapters::DomainAdapter, prelude::*};

pub type UpstreamPort = gasket::messaging::InputPort<RollEvent>;

pub enum WorkUnit {
    ApplyEvent,
    Housekeeping,
}

#[derive(Stage)]
#[stage(name = "apply", unit = "WorkUnit", worker = "Worker")]
pub struct Stage {
    domain: DomainAdapter,

    housekeeping_interval: std::time::Duration,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(domain: DomainAdapter, housekeeping_interval: std::time::Duration) -> Self {
        Self {
            domain,
            housekeeping_interval,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }

    fn process_origin(&self) -> Result<(), WorkerError> {
        info!("applying origin");

        self.domain.apply_origin().or_panic()?;

        Ok(())
    }

    fn process_undo(&self, block: RawBlock) -> Result<(), WorkerError> {
        info!(slot = &block.slot, "undoing block");

        self.domain.undo_blocks(&[block]).or_panic()?;

        Ok(())
    }

    fn process_apply(&self, block: RawBlock) -> Result<(), WorkerError> {
        info!(slot = &block.slot, "applying block");

        self.domain.apply_blocks(&[block]).or_panic()?;

        Ok(())
    }

    fn process_wal(&mut self, log: LogValue) -> Result<(), WorkerError> {
        match log {
            LogValue::Mark(ChainPoint::Origin) => self.process_origin(),
            LogValue::Apply(x) => self.process_apply(x),
            LogValue::Undo(x) => self.process_undo(x),
            // we can skip marks since we know they have been already applied
            LogValue::Mark(..) => Ok(()),
        }
    }
}

pub struct Worker {
    logseq: LogSeq,
    housekeeping_timer: tokio::time::Interval,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        let cursor = stage.domain.state().cursor().or_panic()?;

        if cursor.is_none() {
            info!("cursor not found, applying origin");
            stage.process_origin()?;
        } else {
            info!(?cursor, "cursor found");
        }

        let point = cursor.unwrap_or(ChainPoint::Origin);

        let seq = stage.domain.wal().assert_point(&point).or_panic()?;

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
                let iter = stage
                    .domain
                    .wal()
                    .crawl_from(Some(self.logseq))
                    .or_panic()?
                    .skip(1);

                // TODO: analyze scenario where we're too far behind and this for loop takes
                // longer that the allocated policy timeout.

                for (seq, log) in iter {
                    debug!(seq, "processing wal entry");
                    stage.process_wal(log)?;
                    self.logseq = seq;
                }
            }
            WorkUnit::Housekeeping => {
                stage.domain.housekeeping().or_panic()?;
            }
        }

        Ok(())
    }
}
