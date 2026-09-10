//! Supported headless construction and bulk-replay lifecycle.
//!
//! Applications embedding Dolos should use this module instead of copying the
//! executable's domain assembly. Configuration-file precedence, progress UI,
//! signal handling, source acquisition, publication, and housekeeping remain
//! application policy; this module owns the stable engine seams beneath them.

use std::fmt;
use std::sync::Arc;

use dolos_core::config::{ChainConfig, RootConfig};
use dolos_core::{
    recover_bulk_checkpoint, BootstrapExt as _, BulkRecovery, BulkRecoveryError, ChainLogic as _,
    ChainPoint, Domain, DomainError, Genesis, ImportExt as _, RawBlock, StateStore as _, TipEvent,
};

use crate::adapters::{DomainAdapter, TipSubscription};
use crate::storage;

/// The concrete store set used by a Cardano [`DomainAdapter`].
pub type Stores = storage::Stores<dolos_cardano::CardanoDelta>;

/// A failure while assembling and bootstrapping a Dolos domain.
#[derive(Debug, thiserror::Error)]
pub enum DomainBuildError {
    #[error("opening configured Dolos stores: {0}")]
    Storage(#[source] crate::prelude::Error),

    #[error("initializing Cardano ledger logic: {0}")]
    Chain(#[source] dolos_core::ChainError),

    #[error("bootstrapping the Dolos domain: {0}")]
    Bootstrap(#[source] DomainError),
}

/// Assemble a [`DomainAdapter`] from explicit configuration and genesis.
///
/// The builder never loads files or environment variables. An embedding
/// application decides how configuration and genesis are obtained, then hands
/// the resolved values here. `stop_epoch` overrides only the cloned chain
/// configuration used by this domain; the caller's [`RootConfig`] is not
/// mutated.
pub struct DomainBuilder<'a> {
    config: &'a RootConfig,
    genesis: Arc<Genesis>,
    stop_epoch: Option<u64>,
}

impl<'a> DomainBuilder<'a> {
    /// Start a domain build with no stopping-epoch override.
    pub fn new(config: &'a RootConfig, genesis: Arc<Genesis>) -> Self {
        Self {
            config,
            genesis,
            stop_epoch: None,
        }
    }

    /// Override the configured Cardano stopping epoch for this domain.
    pub fn stop_epoch(mut self, stop_epoch: Option<u64>) -> Self {
        self.stop_epoch = stop_epoch;
        self
    }

    /// Open the configured stores without constructing or bootstrapping a
    /// domain.
    ///
    /// Publishers use this phase to recover and publish a boundary already on
    /// disk before building a domain that can advance beyond it.
    pub fn open_stores(&self) -> Result<Stores, DomainBuildError> {
        storage::open_data_stores(self.config).map_err(DomainBuildError::Storage)
    }

    /// Open stores and construct the domain.
    ///
    /// This is the normal node path. It performs the existing bootstrap and
    /// consistency checks but does not apply bulk-replay recovery first; use
    /// [`BulkReplaySession::open`] for an importer whose state may be ahead of
    /// WAL by design.
    pub fn build(&self) -> Result<DomainAdapter, DomainBuildError> {
        let stores = self.open_stores()?;
        self.build_with_stores(stores)
    }

    /// Construct a domain from stores the caller opened explicitly.
    ///
    /// No checkpoint recovery is hidden here. In particular, construction
    /// never treats a pending publish boundary as permission to import or
    /// prune past it.
    pub fn build_with_stores(&self, stores: Stores) -> Result<DomainAdapter, DomainBuildError> {
        let ChainConfig::Cardano(mut chain_config) = self.config.chain.clone();

        if let Some(stop_epoch) = self.stop_epoch {
            chain_config.stop_epoch = Some(stop_epoch);
        }

        let chain = dolos_cardano::CardanoLogic::initialize::<DomainAdapter>(
            chain_config,
            &stores.state,
            &self.genesis,
        )
        .map_err(DomainBuildError::Chain)?;

        let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);

        let domain = DomainAdapter {
            storage_config: Arc::new(self.config.storage.clone()),
            sync_config: Arc::new(self.config.sync.clone()),
            genesis: self.genesis.clone(),
            chain: Arc::new(std::sync::RwLock::new(chain)),
            wal: stores.wal,
            state: stores.state,
            archive: stores.archive,
            mempool: stores.mempool,
            tip_broadcast,
        };

        domain.bootstrap().map_err(DomainBuildError::Bootstrap)?;

        Ok(domain)
    }
}

/// The durable result of importing one batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplayProgress {
    /// The batch committed normally at this state-store position.
    Committed { position: ChainPoint },
    /// The configured stopping epoch fired after its anchoring block committed.
    Boundary { position: ChainPoint },
}

impl ReplayProgress {
    /// The committed state cursor reported by this import.
    pub fn position(&self) -> &ChainPoint {
        match self {
            Self::Committed { position } | Self::Boundary { position } => position,
        }
    }

    /// Whether the configured stopping boundary was reached.
    pub fn is_boundary(&self) -> bool {
        matches!(self, Self::Boundary { .. })
    }
}

/// A failure in the bulk-replay lifecycle.
#[derive(Debug, thiserror::Error)]
pub enum BulkReplayError {
    #[error(transparent)]
    Build(#[from] DomainBuildError),

    #[error("recovering the bulk-replay checkpoint: {0}")]
    Recovery(#[source] BulkRecoveryError),

    #[error("importing a bulk-replay batch: {0}")]
    Import(#[source] DomainError),

    #[error("reading the committed bulk-replay position: {0}")]
    Position(#[source] dolos_core::StateError),

    #[error("bulk replay completed without a state cursor")]
    MissingPosition,

    #[error("shutting down the bulk-replay domain: {0}")]
    Shutdown(#[source] DomainError),

    #[error("checkpoint recovery failed ({recovery}) and shutdown also failed ({shutdown})")]
    RecoveryAndShutdown {
        recovery: BulkRecoveryError,
        shutdown: DomainError,
    },
}

/// A bulk-import domain with explicit recovery, progress, and close semantics.
///
/// Opening first reconciles only the expected import shape (state ahead of
/// WAL), then bootstraps a domain with the requested stopping epoch. Importing
/// uses [`dolos_core::ImportExt`], so it does not enter the live sync/WAL-tip
/// notification pipeline. [`close`](Self::close) checkpoints WAL and drains
/// the configured stores; it never runs housekeeping.
#[derive(Clone)]
pub struct BulkReplaySession {
    domain: DomainAdapter,
    initial_recovery: BulkRecovery,
}

impl BulkReplaySession {
    /// Recover an importer checkpoint and construct a replay domain.
    pub fn open(
        config: &RootConfig,
        genesis: Arc<Genesis>,
        stop_epoch: Option<u64>,
    ) -> Result<Self, BulkReplayError> {
        let builder = DomainBuilder::new(config, genesis).stop_epoch(stop_epoch);
        let stores = builder.open_stores()?;
        let initial_recovery = recover_bulk_checkpoint(&stores.state, &stores.wal)
            .map_err(BulkReplayError::Recovery)?;
        let domain = builder.build_with_stores(stores)?;

        Ok(Self {
            domain,
            initial_recovery,
        })
    }

    /// What opening the session found and, if necessary, repaired.
    pub fn initial_recovery(&self) -> &BulkRecovery {
        &self.initial_recovery
    }

    /// The underlying domain for read-only consumers and existing facades.
    pub fn domain(&self) -> &DomainAdapter {
        &self.domain
    }

    /// Read the last position committed across the state-store boundary.
    pub fn committed_position(&self) -> Result<Option<ChainPoint>, BulkReplayError> {
        self.domain
            .state()
            .read_cursor()
            .map_err(BulkReplayError::Position)
    }

    /// Import one trusted immutable batch and report the committed position or
    /// configured epoch boundary.
    pub fn import_blocks(&self, blocks: Vec<RawBlock>) -> Result<ReplayProgress, BulkReplayError> {
        let boundary = match self.domain.import_blocks(blocks) {
            Ok(_last_received) => false,
            Err(DomainError::StopEpochReached) => true,
            Err(error) => return Err(BulkReplayError::Import(error)),
        };

        let position = self
            .committed_position()?
            .ok_or(BulkReplayError::MissingPosition)?;

        if boundary {
            Ok(ReplayProgress::Boundary { position })
        } else {
            Ok(ReplayProgress::Committed { position })
        }
    }

    /// Checkpoint WAL and explicitly flush/shut down every store.
    ///
    /// Shutdown is attempted even if checkpoint recovery fails, and a caller
    /// should call this on both operation success and failure.
    /// [`run`](Self::run) packages that rule for a complete replay
    /// operation.
    pub fn close(&self) -> Result<BulkRecovery, BulkReplayError> {
        let recovery = recover_bulk_checkpoint(self.domain.state(), self.domain.wal());
        let shutdown = self.domain.shutdown();

        match (recovery, shutdown) {
            (Ok(recovery), Ok(())) => Ok(recovery),
            (Err(recovery), Ok(())) => Err(BulkReplayError::Recovery(recovery)),
            (Ok(_), Err(shutdown)) => Err(BulkReplayError::Shutdown(shutdown)),
            (Err(recovery), Err(shutdown)) => {
                Err(BulkReplayError::RecoveryAndShutdown { recovery, shutdown })
            }
        }
    }

    /// Run an operation and close the session whether it succeeds or fails.
    pub fn run<T, E>(
        self,
        operation: impl FnOnce(&Self) -> Result<T, E>,
    ) -> Result<T, BulkReplayRunError<E>> {
        let result = operation(&self);
        let close = self.close();

        match (result, close) {
            (Ok(value), Ok(_)) => Ok(value),
            (Err(operation), Ok(_)) => Err(BulkReplayRunError::Operation(operation)),
            (Ok(_), Err(close)) => Err(BulkReplayRunError::Close(close)),
            (Err(operation), Err(close)) => {
                Err(BulkReplayRunError::OperationAndClose { operation, close })
            }
        }
    }
}

/// A replay operation's error, preserving a close failure if both happened.
#[derive(Debug)]
pub enum BulkReplayRunError<E> {
    Operation(E),
    Close(BulkReplayError),
    OperationAndClose {
        operation: E,
        close: BulkReplayError,
    },
}

impl<E: fmt::Display> fmt::Display for BulkReplayRunError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(error) => write!(formatter, "bulk replay failed: {error}"),
            Self::Close(error) => write!(formatter, "closing bulk replay failed: {error}"),
            Self::OperationAndClose { operation, close } => write!(
                formatter,
                "bulk replay failed ({operation}) and closing it also failed ({close})"
            ),
        }
    }
}

impl<E> std::error::Error for BulkReplayRunError<E>
where
    E: std::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation(error)
            | Self::OperationAndClose {
                operation: error, ..
            } => Some(error),
            Self::Close(error) => Some(error),
        }
    }
}

impl Domain for BulkReplaySession {
    type Entity = dolos_cardano::CardanoEntity;
    type EntityDelta = dolos_cardano::CardanoDelta;
    type Chain = dolos_cardano::CardanoLogic;
    type WorkUnit = dolos_cardano::CardanoWorkUnit;
    type Wal = crate::adapters::WalAdapter;
    type State = crate::adapters::StateStoreBackend;
    type Archive = crate::adapters::ArchiveStoreBackend;
    type Mempool = crate::adapters::MempoolBackend;
    type TipSubscription = TipSubscription;

    fn storage_config(&self) -> &dolos_core::config::StorageConfig {
        self.domain.storage_config()
    }

    fn sync_config(&self) -> &dolos_core::config::SyncConfig {
        self.domain.sync_config()
    }

    fn genesis(&self) -> Arc<Genesis> {
        self.domain.genesis()
    }

    fn read_chain(&self) -> std::sync::RwLockReadGuard<'_, Self::Chain> {
        self.domain.read_chain()
    }

    fn write_chain(&self) -> std::sync::RwLockWriteGuard<'_, Self::Chain> {
        self.domain.write_chain()
    }

    fn wal(&self) -> &Self::Wal {
        self.domain.wal()
    }

    fn state(&self) -> &Self::State {
        self.domain.state()
    }

    fn archive(&self) -> &Self::Archive {
        self.domain.archive()
    }

    fn mempool(&self) -> &Self::Mempool {
        self.domain.mempool()
    }

    fn watch_tip(&self, from: Option<ChainPoint>) -> Result<Self::TipSubscription, DomainError> {
        self.domain.watch_tip(from)
    }

    fn notify_tip(&self, tip: TipEvent) {
        self.domain.notify_tip(tip)
    }
}
