//! Ewrap (Epoch Wrap) work units — three-phase pipeline.
//!
//! EWRAP at an epoch boundary is partitioned into three distinct work units
//! scheduled sequentially by the chain logic:
//!
//! 1. `EwrapWorkUnit` — global (non-account) work: pool/drep/proposal
//!    classification, enactment, MIR processing, deposit refunds, and
//!    emitting the `EpochEndInit` delta that populates `EpochState.end` with
//!    the prepare-time globals + zeroed reward accumulators. The structural
//!    `EpochState.end` slot is opened (as `Some(EndStats::default())`) by
//!    `EpochTransition` during ESTART, so this phase only writes content.
//! 2. `EwrapShardWorkUnit { shard_index }` — emitted `total_shards` times.
//!    Each shard covers a first-byte prefix range of the account key space,
//!    range-loads pending rewards, iterates accounts in range, applies
//!    rewards + drops visitors, and emits `EpochEndAccumulate` with the
//!    shard's reward contribution.
//! 3. `EwrapFinalizeWorkUnit` — reads the accumulated `EpochState.end`,
//!    emits `EpochWrapUp` (transitions rolling/pparams, clears
//!    `ewrap_progress`), writes the completed epoch state to archive.

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::CardanoLogic;

use super::{shard::shard_key_range, BoundaryWork};

// ---------------------------------------------------------------------------
// EwrapWorkUnit
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// EwrapShardWorkUnit
// ---------------------------------------------------------------------------

pub struct EwrapShardWorkUnit {
    slot: BlockSlot,
    config: CardanoConfig,
    genesis: Arc<Genesis>,
    shard_index: u32,

    boundary: Option<BoundaryWork>,
}

impl EwrapShardWorkUnit {
    pub fn new(
        slot: BlockSlot,
        config: CardanoConfig,
        genesis: Arc<Genesis>,
        shard_index: u32,
    ) -> Self {
        Self {
            slot,
            config,
            genesis,
            shard_index,
            boundary: None,
        }
    }

    pub fn shard_index(&self) -> u32 {
        self.shard_index
    }

    pub fn boundary(&self) -> Option<&BoundaryWork> {
        self.boundary.as_ref()
    }
}

impl<D> WorkUnit<D> for EwrapShardWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "ewrap_shard"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        let total_shards = self.config.ewrap_total_shards();
        let range = shard_key_range(self.shard_index, total_shards);

        debug!(
            slot = self.slot,
            shard = self.shard_index,
            total = total_shards,
            "loading ewrap shard context"
        );

        let boundary = BoundaryWork::load_shard::<D>(
            domain.state(),
            self.genesis.clone(),
            self.shard_index,
            range,
        )?;

        info!(
            epoch = boundary.ending_state().number,
            shard = self.shard_index,
            "ewrap shard"
        );

        self.boundary = Some(boundary);

        debug!("ewrap shard context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        debug!("ewrap shard compute (deltas computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        let total_shards = self.config.ewrap_total_shards();
        let range = shard_key_range(self.shard_index, total_shards);

        let boundary = self
            .boundary
            .as_mut()
            .ok_or_else(|| DomainError::Internal("ewrap shard boundary not loaded".into()))?;

        boundary.commit_shard::<D>(domain.state(), domain.archive(), range)?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// EwrapFinalizeWorkUnit
// ---------------------------------------------------------------------------

pub struct EwrapFinalizeWorkUnit {
    slot: BlockSlot,
    #[allow(dead_code)]
    config: CardanoConfig,
    genesis: Arc<Genesis>,

    boundary: Option<BoundaryWork>,
}

impl EwrapFinalizeWorkUnit {
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

impl<D> WorkUnit<D> for EwrapFinalizeWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "ewrap_finalize"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!(slot = self.slot, "loading ewrap finalize context");

        let boundary = BoundaryWork::load_finalize::<D>(domain.state(), self.genesis.clone())?;

        info!(epoch = boundary.ending_state().number, "ewrap finalize");

        self.boundary = Some(boundary);

        debug!("ewrap finalize context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        debug!("ewrap finalize compute (deltas computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        let boundary = self
            .boundary
            .as_mut()
            .ok_or_else(|| DomainError::Internal("ewrap finalize boundary not loaded".into()))?;

        boundary.commit_finalize::<D>(domain.state(), domain.archive())?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }
}
