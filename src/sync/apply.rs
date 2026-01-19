use gasket::{framework::*, messaging::Message};
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::debug;

use crate::{adapters::DomainAdapter, facade::DomainExt as _, prelude::*};

type WorkerError = gasket::framework::WorkerError;

static STOP_EPOCH_REACHED: AtomicBool = AtomicBool::new(false);
static QUOTA_REACHED: AtomicBool = AtomicBool::new(false);

pub fn set_stop_epoch_reached(value: bool) {
    STOP_EPOCH_REACHED.store(value, Ordering::SeqCst);
}
pub fn is_stop_epoch_reached() -> bool {
    STOP_EPOCH_REACHED.load(Ordering::SeqCst)
}

pub fn set_quota_reached(value: bool) {
    QUOTA_REACHED.store(value, Ordering::SeqCst);
}

pub fn is_quota_reached() -> bool {
    QUOTA_REACHED.load(Ordering::SeqCst)
}

pub type UpstreamPort = gasket::messaging::InputPort<PullEvent>;

pub enum WorkUnit {
    PullEvent(PullEvent),
    Housekeeping,
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

    async fn on_roll_forward(&self, block: RawBlock) -> Result<(), WorkerError> {
        debug!("handling roll forward");
        match dolos_core::facade::roll_forward(&self.domain, block).await {
            Ok(_) => Ok(()),
            Err(dolos_core::DomainError::StopEpochReached) => {
                // indicate stop epoch was reached
                set_stop_epoch_reached(true);
                // Return a special error to indicate stop epoch was reached
                Err(WorkerError::Panic)
            }
            Err(e) => {
                // Convert other domain errors to worker errors
                tracing::error!("Domain error: {:?}", e);
                Err(WorkerError::Panic)
            }
        }
    }

    fn on_rollback(&self, point: &ChainPoint) -> Result<(), WorkerError> {
        debug!(slot = &point.slot(), "handling rollback");

        self.domain.rollback(point).or_panic()?;

        Ok(())
    }
}

pub struct Worker {
    interval: tokio::time::Interval,
}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Self {
            interval: tokio::time::interval(stage.housekeeping_interval),
        })
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<WorkUnit>, WorkerError> {
        tokio::select! {
            msg = stage.upstream.recv() => {
                let msg = msg.or_panic()?;
                let unit = WorkUnit::from(msg);
                Ok(unit.into())
            }
            _ = self.interval.tick() => {
                Ok(WorkSchedule::Unit(WorkUnit::Housekeeping))
            }
        }
    }

    async fn execute(&mut self, unit: &WorkUnit, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            WorkUnit::PullEvent(evt) => match evt {
                PullEvent::RollForward(x) => stage.on_roll_forward(x.clone()).await?,
                PullEvent::Rollback(x) => stage.on_rollback(x)?,
            },
            WorkUnit::Housekeeping => {
                stage.domain.housekeeping().or_panic()?;
            }
        }

        Ok(())
    }
}
