use std::sync::Arc;

use dolos_core::SyncExt;
use gasket::{framework::*, messaging::Message};
use tokio::sync::watch;
use tracing::{debug, info};

use super::{SyncLimit, SyncProgress};
use crate::{adapters::DomainAdapter, prelude::*};

pub type UpstreamPort = gasket::messaging::InputPort<PullEvent>;

pub enum WorkUnit {
    PullEvent(PullEvent),
    Housekeeping,
    /// The pull stage fixed the target, which the stores may already hold.
    TargetFixed,
}

impl From<Message<PullEvent>> for WorkUnit {
    fn from(value: Message<PullEvent>) -> Self {
        WorkUnit::PullEvent(value.payload)
    }
}

impl From<WorkUnit> for WorkSchedule<WorkUnit> {
    fn from(value: WorkUnit) -> Self {
        WorkSchedule::Unit(value)
    }
}

#[derive(Stage)]
#[stage(name = "apply", unit = "WorkUnit", worker = "Worker")]
pub struct Stage {
    domain: DomainAdapter,
    limit: SyncLimit,
    progress: Arc<SyncProgress>,

    housekeeping_interval: std::time::Duration,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(
        domain: DomainAdapter,
        limit: SyncLimit,
        progress: Arc<SyncProgress>,
        housekeeping_interval: std::time::Duration,
    ) -> Self {
        Self {
            domain,
            limit,
            progress,
            housekeeping_interval,
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }

    fn on_roll_forward(&self, block: RawBlock) -> Result<(), WorkerError> {
        debug!("handling roll forward");

        match self.domain.roll_forward(block) {
            Err(DomainError::StopEpochReached) => {
                info!("stop epoch reached");
                self.progress.mark_stop_epoch_reached();
                Ok(())
            }
            other => other.map(|_| ()).or_panic(),
        }
    }

    fn on_rollback(&self, point: &ChainPoint) -> Result<(), WorkerError> {
        debug!(slot = &point.slot(), "handling rollback");

        self.domain.rollback(point).or_panic()?;

        Ok(())
    }

    /// Mark the target caught up once the WAL and the state have both
    /// committed it.
    fn check_target(&self) -> Result<(), WorkerError> {
        if self.progress.caught_up() {
            return Ok(());
        }

        let Some(target) = self.progress.target() else {
            return Ok(());
        };

        let wal_tip = self
            .domain
            .wal()
            .find_tip()
            .or_panic()?
            .map(|(point, _)| point);

        let state = self.domain.state().read_cursor().or_panic()?;

        if super::committed(&target, wal_tip.as_ref(), state.as_ref()) {
            info!(point = %target, "catch-up target committed");
            self.progress.mark_caught_up();
        }

        Ok(())
    }
}

pub struct Worker {
    interval: tokio::time::Interval,
    target: watch::Receiver<Option<ChainPoint>>,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        // Seen as changed, so a target fixed before this subscription is
        // still checked.
        let mut target = stage.progress.watch_target();
        target.mark_changed();

        Ok(Self {
            interval: tokio::time::interval(stage.housekeeping_interval),
            target,
        })
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<WorkUnit>, WorkerError> {
        if stage.progress.succeeded(stage.limit) {
            return Ok(WorkSchedule::Done);
        }

        tokio::select! {
            msg = stage.upstream.recv() => {
                let msg = msg.or_panic()?;
                let unit = WorkUnit::from(msg);
                Ok(unit.into())
            }
            _ = self.interval.tick() => {
                Ok(WorkSchedule::Unit(WorkUnit::Housekeeping))
            }
            Ok(()) = self.target.changed() => {
                Ok(WorkSchedule::Unit(WorkUnit::TargetFixed))
            }
        }
    }

    async fn execute(&mut self, unit: &WorkUnit, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            WorkUnit::PullEvent(evt) => match evt {
                PullEvent::RollForward(x) => stage.on_roll_forward(x.clone())?,
                PullEvent::Rollback(x) => stage.on_rollback(x)?,
            },
            WorkUnit::Housekeeping => {
                stage.domain.housekeeping().or_panic()?;
            }
            WorkUnit::TargetFixed => (),
        }

        if !stage.progress.stop_epoch_reached() {
            stage.check_target()?;
        }

        Ok(())
    }
}
