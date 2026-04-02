//! Genesis work unit implementation.
//!
//! The genesis work unit bootstraps the chain state from genesis configuration.

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, ChainPoint, Domain, DomainError, WalStore as _, WorkUnit};
use tracing::{debug, info};

use crate::{CardanoError, CardanoGenesis, CardanoLogic};

/// Work unit for bootstrapping the chain from genesis.
pub struct GenesisWorkUnit {
    config: CardanoConfig,
    genesis: Arc<CardanoGenesis>,
}

impl GenesisWorkUnit {
    /// Create a new genesis work unit.
    pub fn new(config: CardanoConfig, genesis: Arc<CardanoGenesis>) -> Self {
        Self { config, genesis }
    }
}

impl<D> WorkUnit<D> for GenesisWorkUnit
where
    D: Domain<Chain = CardanoLogic, ChainSpecificError = CardanoError, Genesis = CardanoGenesis>,
{
    fn name(&self) -> &'static str {
        "genesis"
    }

    fn load(&mut self, _domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        // Genesis doesn't load existing state - it creates initial state
        debug!("genesis work unit: no loading required");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError<D::ChainSpecificError>> {
        // Genesis is mostly I/O-bound, minimal compute
        debug!("genesis work unit: no computation required");
        Ok(())
    }

    fn commit_wal(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        // Reset WAL to origin for genesis
        domain.wal().reset_to(&ChainPoint::Origin)?;
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        info!("bootstrapping chain from genesis");

        // Execute the genesis bootstrap
        super::execute::<D>(
            domain.state(),
            domain.indexes(),
            &self.genesis,
            &self.config,
        )?;

        debug!("genesis bootstrap complete");
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        // Genesis doesn't write to archive
        Ok(())
    }
}
