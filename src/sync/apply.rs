use std::time::{Duration, Instant};

use dolos_core::SyncExt;
use gasket::{framework::*, messaging::Message};
use tracing::{debug, warn};

use crate::{adapters::DomainAdapter, prelude::*};

pub type UpstreamPort = gasket::messaging::InputPort<PullEvent>;

/// How long the pipeline may go without hearing anything from upstream before
/// each housekeeping tick starts warning about it.
///
/// Well past any plausible inter-block gap on a Cardano network, where the mean
/// is ~20s. The point is to separate "behind but advancing" — a node catching
/// up floods this stage with events — from "not receiving anything at all",
/// which is the shape of an unreachable or silent peer and looks identical to a
/// healthy node from the outside.
const UPSTREAM_SILENCE_THRESHOLD: Duration = Duration::from_secs(300);

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

    /// The peer this pipeline pulls from, named in the silence warning so an
    /// operator can tell a misconfigured address from a peer that went quiet.
    /// `None` for the emulator pipeline, which has no upstream to lose.
    peer_address: Option<String>,

    last_upstream_event: Instant,

    pub upstream: UpstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    wal_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(
        domain: DomainAdapter,
        housekeeping_interval: std::time::Duration,
        peer_address: Option<String>,
    ) -> Self {
        Self {
            domain,
            housekeeping_interval,
            peer_address,
            last_upstream_event: Instant::now(),
            upstream: Default::default(),
            block_count: Default::default(),
            wal_count: Default::default(),
        }
    }

    /// Warn, on every tick, while nothing has arrived from upstream for longer
    /// than [`UPSTREAM_SILENCE_THRESHOLD`]. Repeating it is deliberate: a
    /// single line at the moment the peer went quiet scrolls out of a log
    /// that then looks entirely healthy.
    fn warn_if_upstream_silent(&self) {
        let Some(peer_address) = self.peer_address.as_ref() else {
            return;
        };

        let silence = self.last_upstream_event.elapsed();

        if silence >= UPSTREAM_SILENCE_THRESHOLD {
            warn!(
                peer_address,
                silence_sec = silence.as_secs(),
                "nothing received from upstream peer; the chain we serve is not advancing"
            );
        }
    }

    fn on_roll_forward(&self, block: RawBlock) -> Result<(), WorkerError> {
        debug!("handling roll forward");

        self.domain.roll_forward(block).or_panic()?;

        Ok(())
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
            WorkUnit::PullEvent(evt) => {
                stage.last_upstream_event = Instant::now();

                match evt {
                    PullEvent::RollForward(x) => stage.on_roll_forward(x.clone())?,
                    PullEvent::Rollback(x) => stage.on_rollback(x)?,
                }
            }
            WorkUnit::Housekeeping => {
                stage.warn_if_upstream_silent();
                stage.domain.housekeeping().or_panic()?;
            }
        }

        Ok(())
    }
}
