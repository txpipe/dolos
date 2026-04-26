//! Ewrap work unit — global (non-account) work and the boundary close.
//!
//! Runs after the per-account `AShardWorkUnit` series. Performs
//! pool/drep/proposal classification, enactment, MIR processing, deposit
//! refunds, then assembles the final `EndStats` (combining prepare-time
//! fields with the shard-populated accumulators) and emits a single
//! `EpochWrapUp` delta. `EpochWrapUp::apply` overwrites `entity.end` with
//! the final stats, rotates rolling/pparams snapshots forward, and clears
//! `ashard_progress`. The completed `EpochState` is also written to archive
//! at commit time.

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::CardanoLogic;

use super::BoundaryWork;

pub struct EwrapWorkUnit {
    slot: BlockSlot,
    #[allow(dead_code)]
    config: CardanoConfig,
    genesis: Arc<Genesis>,

    boundary: Option<BoundaryWork>,
}

impl EwrapWorkUnit {
    pub fn new(slot: BlockSlot, config: CardanoConfig, genesis: Arc<Genesis>) -> Self {
        Self {
            slot,
            config,
            genesis,
            boundary: None,
        }
    }

    pub fn boundary(&self) -> Option<&BoundaryWork> {
        self.boundary.as_ref()
    }
}

impl<D> WorkUnit<D> for EwrapWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "ewrap"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!(slot = self.slot, "loading ewrap context");

        let boundary = BoundaryWork::load_ewrap::<D>(domain.state(), self.genesis.clone())?;

        info!(epoch = boundary.ending_state().number, "ewrap");

        self.boundary = Some(boundary);

        debug!("ewrap context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        debug!("ewrap compute (deltas computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        let boundary = self
            .boundary
            .as_mut()
            .ok_or_else(|| DomainError::Internal("ewrap boundary not loaded".into()))?;

        boundary.commit_ewrap::<D>(domain.state(), domain.archive())?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }
}

