use gasket::framework::*;
use tracing::{debug, info, warn};

use crate::{
    prelude::*,
    wal::{self, redb::WalStore, ChainPoint, WalWriter},
};

pub type Cursor = (BlockSlot, BlockHash);
pub type UpstreamPort = gasket::messaging::InputPort<PullEvent>;
pub type DownstreamPort = gasket::messaging::OutputPort<RollEvent>;

pub enum WorkUnit {
    PullEvent(PullEvent),
    Housekeeping,
}

#[derive(Stage)]
#[stage(name = "roll", unit = "WorkUnit", worker = "Worker")]
pub struct Stage {
    store: WalStore,

    max_history_slots: Option<u64>,

    pub upstream: UpstreamPort,
    pub downstream: DownstreamPort,

    #[metric]
    block_count: gasket::metrics::Counter,

    #[metric]
    roll_count: gasket::metrics::Counter,
}

impl Stage {
    pub fn new(store: WalStore) -> Self {
        Self {
            store,
            upstream: Default::default(),
            downstream: Default::default(),
            block_count: Default::default(),
            roll_count: Default::default(),
            max_history_slots: Some(500_000),
        }
    }

    async fn process_pull_event(&mut self, unit: &PullEvent) -> Result<(), WorkerError> {
        match unit {
            PullEvent::RollForward(block) => {
                let block = wal::RawBlock {
                    slot: block.slot,
                    hash: block.hash,
                    era: block.era,
                    body: block.body.clone(),
                };

                info!(block.slot, %block.hash, "extending wal");

                self.store.roll_forward(std::iter::once(block)).or_panic()?;
            }
            PullEvent::Rollback(point) => {
                let point = match point {
                    pallas::network::miniprotocols::Point::Origin => wal::ChainPoint::Origin,
                    pallas::network::miniprotocols::Point::Specific(s, h) => {
                        wal::ChainPoint::Specific(*s, h.as_slice().into())
                    }
                };

                info!(?point, "rolling back wal");

                self.store.roll_back(&point).or_panic()?;
            }
        }

        self.downstream
            .send(RollEvent::TipChanged.into())
            .await
            .or_panic()?;

        Ok(())
    }

    async fn housekeeping(&mut self) -> Result<(), WorkerError> {
        let max_slots = match self.max_history_slots {
            Some(s) => s,
            None => {
                debug!("wal pruning is disabled");
                return Ok(());
            }
        };

        use crate::wal::WalReader;

        let start_slot = match self.store.find_start().or_panic()? {
            Some((_, ChainPoint::Origin)) => 0,
            Some((_, ChainPoint::Specific(slot, _))) => slot,
            _ => {
                debug!("no start point found, skipping housekeeping");
                return Ok(());
            }
        };

        let last_slot = match self.store.find_tip().or_panic()? {
            Some((_, ChainPoint::Specific(slot, _))) => slot,
            _ => {
                debug!("no tip found, skipping housekeeping");
                return Ok(());
            }
        };

        let delta = last_slot - start_slot - max_slots;

        debug!(delta, last_slot, start_slot, "wal history delta computed");

        if delta <= max_slots {
            debug!(delta, max_slots, "no pruning necessary");
            return Ok(());
        }

        let max_prune = core::cmp::min(delta, 1000);

        let prune_before = start_slot + max_prune;

        info!(cutoff_slot = prune_before, "pruning wal for excess history");

        self.store.remove_before(prune_before).or_panic()?;

        Ok(())
    }
}

pub struct Worker {
    housekeeping_timer: tokio::time::Interval,
}

impl Worker {}

#[async_trait::async_trait(?Send)]
impl gasket::framework::Worker<Stage> for Worker {
    async fn bootstrap(_stage: &Stage) -> Result<Self, WorkerError> {
        Ok(Worker {
            // TODO: make this interval user-configurable
            housekeeping_timer: tokio::time::interval(std::time::Duration::from_secs(6)),
        })
    }

    async fn schedule(&mut self, stage: &mut Stage) -> Result<WorkSchedule<WorkUnit>, WorkerError> {
        tokio::select! {
            msg = stage.upstream.recv() => {
                let msg = msg.or_panic()?;
                Ok(WorkSchedule::Unit(WorkUnit::PullEvent(msg.payload)))
            }
            _ = self.housekeeping_timer.tick() => {
                Ok(WorkSchedule::Unit(WorkUnit::Housekeeping))
            }
        }
    }

    async fn execute(&mut self, unit: &WorkUnit, stage: &mut Stage) -> Result<(), WorkerError> {
        match unit {
            WorkUnit::PullEvent(pull) => stage.process_pull_event(pull).await?,
            WorkUnit::Housekeeping => stage.housekeeping().await?,
        }

        Ok(())
    }
}
