//! AShard work unit — per-account leg of the epoch-boundary pipeline.
//!
//! Emitted `total_shards` times in sequence. Each shard covers a first-byte
//! prefix range of the account key space, range-loads pending rewards,
//! iterates accounts in range, applies rewards + drops visitors, and emits
//! `EpochEndAccumulate` to populate the reward accumulators on
//! `EpochState.end` (the slot is opened by `EpochTransition` during ESTART).
//!
//! Followed by the `EwrapWorkUnit` global phase which closes the boundary.

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::{ewrap::BoundaryWork, CardanoLogic};

use super::shard::shard_key_range;

pub struct AShardWorkUnit {
    slot: BlockSlot,
    config: CardanoConfig,
    genesis: Arc<Genesis>,
    shard_index: u32,

    boundary: Option<BoundaryWork>,
}

impl AShardWorkUnit {
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

impl<D> WorkUnit<D> for AShardWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "ashard"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        let total_shards = self.config.account_shards();
        let range = shard_key_range(self.shard_index, total_shards);

        debug!(
            slot = self.slot,
            shard = self.shard_index,
            total = total_shards,
            "loading ashard context"
        );

        let boundary = BoundaryWork::load_ashard::<D>(
            domain.state(),
            self.genesis.clone(),
            self.shard_index,
            range,
        )?;

        info!(
            epoch = boundary.ending_state().number,
            shard = self.shard_index,
            "ashard"
        );

        self.boundary = Some(boundary);

        debug!("ashard context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        debug!("ashard compute (deltas computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        let total_shards = self.config.account_shards();
        let range = shard_key_range(self.shard_index, total_shards);

        let boundary = self
            .boundary
            .as_mut()
            .ok_or_else(|| DomainError::Internal("ashard boundary not loaded".into()))?;

        boundary.commit_ashard::<D>(domain.state(), domain.archive(), range)?;
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
        Ok(())
    }
}
