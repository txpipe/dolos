//! Headless node construction and exclusive bulk replay.
//!
//! Hosts provide resolved configuration, trusted blocks and stopping policy.
//! Opening a replay workspace does not advance the ledger. Inspect or export
//! its snapshot before explicitly starting replay. Finalization persists the
//! committed position and releases resources without pruning history.

mod checkpoint;

use std::fmt;
use std::sync::Arc;

use dolos_core::config::{ChainConfig, RootConfig};
pub use dolos_core::ReplayProgress;
use dolos_core::{
    BootstrapExt as _, ChainLogic as _, ChainPoint, Domain as _, DomainError, Genesis,
    ImportExt as _, RawBlock, StateStore as _,
};
use dolos_snapshot::source::{SnapshotSource, StoreSnapshot};

use crate::adapters::{ArchiveStoreBackend, DomainAdapter, StateStoreBackend, WalAdapter};
use crate::storage;

type Stores = storage::Stores<dolos_cardano::CardanoDelta>;
type Cause = Box<dyn std::error::Error + Send + Sync + 'static>;

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

    /// Construct a normal node domain, including initialization and integrity
    /// checks.
    pub fn build(&self) -> Result<DomainAdapter, DomainBuildError> {
        let stores = storage::open_data_stores(self.config).map_err(DomainBuildError::Storage)?;
        self.build_with_stores(&stores)
    }

    fn build_with_stores(&self, stores: &Stores) -> Result<DomainAdapter, DomainBuildError> {
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
            wal: stores.wal.clone(),
            state: stores.state.clone(),
            archive: stores.archive.clone(),
            mempool: stores.mempool.clone(),
            tip_broadcast,
        };

        domain.bootstrap().map_err(DomainBuildError::Bootstrap)?;

        Ok(domain)
    }
}

/// An operation failed. Storage-specific details remain in the error source
/// chain rather than becoming part of the replay protocol.
#[derive(Debug, thiserror::Error)]
#[error("{operation}: {source}")]
pub struct BulkReplayError {
    operation: &'static str,
    #[source]
    source: Cause,
}

impl BulkReplayError {
    fn new(operation: &'static str, source: impl Into<Cause>) -> Self {
        Self {
            operation,
            source: source.into(),
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("{primary}; finalization also failed: {cleanup}")]
struct WithCleanup {
    #[source]
    primary: Cause,
    cleanup: Cause,
}

fn complete<T>(
    result: Result<T, BulkReplayError>,
    cleanup: Result<(), BulkReplayError>,
) -> Result<T, BulkReplayError> {
    match (result, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) | (Ok(_), Err(error)) => Err(error),
        (Err(primary), Err(cleanup)) => Err(BulkReplayError::new(
            "finalizing replay",
            WithCleanup {
                primary: Box::new(primary),
                cleanup: Box::new(cleanup),
            },
        )),
    }
}

fn flush(
    wal: &WalAdapter,
    state: &StateStoreBackend,
    archive: &ArchiveStoreBackend,
) -> Result<(), BulkReplayError> {
    let wal = wal
        .shutdown()
        .map_err(|error| BulkReplayError::new("finishing replay", error));
    let state = state
        .shutdown()
        .map_err(|error| BulkReplayError::new("finishing replay", error));
    let archive = archive
        .shutdown()
        .map_err(|error| BulkReplayError::new("finishing replay", error));
    complete(complete(wal, state), archive)
}

/// An exclusively owned replay dataset, opened without advancing chain state.
///
/// Use the borrowed profile view to inspect or publish pending data, then
/// consume the workspace with `start` to advance it or `finish` to release it.
/// Neither this handle nor its profile view exposes writable stores.
#[must_use = "finish the workspace or consume it by starting replay"]
pub struct ReplayWorkspace<'a> {
    config: &'a RootConfig,
    genesis: Arc<Genesis>,
    stores: Stores,
}

impl<'a> ReplayWorkspace<'a> {
    /// Open the configured dataset without running ledger initialization,
    /// replaying blocks, or pruning history.
    pub fn open(config: &'a RootConfig, genesis: Arc<Genesis>) -> Result<Self, BulkReplayError> {
        let stores = storage::open_data_stores(config)
            .map_err(|error| BulkReplayError::new("opening replay workspace", error))?;
        Ok(Self {
            config,
            genesis,
            stores,
        })
    }

    /// Borrow read-only Dolos profile operations, not storage handles.
    pub fn snapshot(&self) -> impl SnapshotSource + '_ {
        StoreSnapshot::new(&self.stores.archive, &self.stores.state)
    }

    /// Begin processing, including any pending ledger initialization.
    ///
    /// Call only after publishing any pending boundary. The optional stopping
    /// epoch overrides the configured value; `None` retains that configuration.
    pub fn start(self, stop_epoch: Option<u64>) -> Result<BulkReplaySession, BulkReplayError> {
        let result = checkpoint::reconcile(&self.stores.state, &self.stores.wal)
            .map_err(|error| BulkReplayError::new("preparing replay", error))
            .and_then(|()| {
                DomainBuilder::new(self.config, self.genesis.clone())
                    .stop_epoch(stop_epoch)
                    .build_with_stores(&self.stores)
                    .map_err(|error| BulkReplayError::new("starting replay", error))
            });
        match result {
            Ok(domain) => Ok(BulkReplaySession {
                domain,
                boundary: None,
                failed: false,
            }),
            Err(error) => complete(Err(error), self.finish()),
        }
    }

    /// Release the inspection workspace without advancing or pruning it.
    pub fn finish(self) -> Result<(), BulkReplayError> {
        flush(&self.stores.wal, &self.stores.state, &self.stores.archive)
    }
}

/// An exclusive session for trusted immutable blocks.
///
/// Session ownership cannot be duplicated:
/// ```compile_fail
/// fn duplicate(session: dolos::engine::BulkReplaySession) {
///     let other = session.clone();
/// }
/// ```
///
/// A replay session does not expose the live-node domain interface:
/// ```compile_fail
/// fn live_domain<D: dolos::core::Domain>() {}
/// live_domain::<dolos::engine::BulkReplaySession>();
/// ```
///
/// A profile view prevents advancement while it is in use:
/// ```compile_fail
/// use dolos_snapshot::source::SnapshotSource;
/// fn inspect(session: &mut dolos::engine::BulkReplaySession) {
///     let snapshot = session.snapshot();
///     session.import_blocks(vec![]).unwrap();
///     snapshot.committed_position().unwrap();
/// }
/// ```
///
/// Finishing consumes the handle:
/// ```compile_fail
/// fn finish(mut session: dolos::engine::BulkReplaySession) {
///     session.finish().unwrap();
///     session.import_blocks(vec![]).unwrap();
/// }
/// ```
///
/// The session is neither cloneable nor a `Domain`: processing requires a
/// mutable borrow, and finishing consumes it. After a boundary, further import
/// calls report that same boundary without accepting more input. After an
/// execution failure, finish and reopen rather than continuing a damaged
/// session. An empty batch is rejected before execution and does not invalidate
/// the session.
///
/// `run` finalizes on ordinary success and error returns. Dropping a session,
/// panicking or killing the process is not a substitute for successful
/// finalization; interrupted ledger transitions may require explicit repair.
#[must_use = "finish the session or use run to finalize an operation"]
pub struct BulkReplaySession {
    domain: DomainAdapter,
    boundary: Option<ChainPoint>,
    failed: bool,
}

impl BulkReplaySession {
    /// Open and immediately start replay when no pending export needs
    /// inspection.
    pub fn open(
        config: &RootConfig,
        genesis: Arc<Genesis>,
        stop_epoch: Option<u64>,
    ) -> Result<Self, BulkReplayError> {
        ReplayWorkspace::open(config, genesis)?.start(stop_epoch)
    }

    /// Read the last committed input position.
    pub fn committed_position(&self) -> Result<Option<ChainPoint>, BulkReplayError> {
        self.domain
            .state()
            .read_cursor()
            .map_err(|error| BulkReplayError::new("reading committed position", error))
    }

    /// Borrow profile operations over the currently committed dataset.
    pub fn snapshot(&self) -> impl SnapshotSource + '_ {
        StoreSnapshot::new(self.domain.archive(), self.domain.state())
    }

    /// Process a nonempty batch and report its committed position or boundary.
    ///
    /// A boundary can stop partway through a batch. Resume the source from the
    /// returned position, not from the last block submitted.
    pub fn import_blocks(
        &mut self,
        blocks: Vec<RawBlock>,
    ) -> Result<ReplayProgress, BulkReplayError> {
        if self.failed {
            return Err(BulkReplayError::new(
                "importing blocks",
                "session failed; finish and reopen it",
            ));
        }
        if let Some(position) = &self.boundary {
            return Ok(ReplayProgress::Boundary {
                position: position.clone(),
            });
        }
        if blocks.is_empty() {
            return Err(BulkReplayError::new(
                "importing blocks",
                "batch must not be empty",
            ));
        }
        let boundary = match self.domain.import_blocks(blocks) {
            Ok(_) => false,
            Err(DomainError::StopEpochReached) => true,
            Err(error) => {
                self.failed = true;
                return Err(BulkReplayError::new("importing blocks", error));
            }
        };
        let position = match self.committed_position() {
            Ok(Some(position)) => position,
            result => {
                self.failed = true;
                return Err(result.err().unwrap_or_else(|| {
                    BulkReplayError::new("importing blocks", "no committed position")
                }));
            }
        };
        if boundary {
            self.boundary = Some(position.clone());
            Ok(ReplayProgress::Boundary { position })
        } else {
            Ok(ReplayProgress::Committed { position })
        }
    }

    /// Explicitly apply the configured history retention policy.
    ///
    /// The host must first publish any data it intends to preserve. Import and
    /// finalization never call this operation automatically.
    pub fn prune_history(&mut self) -> Result<u64, BulkReplayError> {
        if self.failed || self.boundary.is_some() {
            return Err(BulkReplayError::new(
                "pruning history",
                "finish the session before starting the next replay round",
            ));
        }
        self.domain
            .drain_housekeeping(None)
            .map_err(|error| BulkReplayError::new("pruning history", error))
    }

    /// Persist completed work and release resources without advancing or
    /// pruning.
    ///
    /// Successful finalization makes the committed position usable by a later
    /// replay session or normal node startup. Resource finalization is
    /// attempted even when the dataset cannot be made resumable.
    pub fn finish(self) -> Result<(), BulkReplayError> {
        let result = checkpoint::reconcile(self.domain.state(), self.domain.wal())
            .map_err(|error| BulkReplayError::new("finishing replay", error));
        let cleanup = flush(
            self.domain.wal(),
            self.domain.state(),
            self.domain.archive(),
        );
        complete(result, cleanup)
    }

    /// Run an operation and finalize on both successful and failed returns.
    pub fn run<T, E>(
        mut self,
        operation: impl FnOnce(&mut Self) -> Result<T, E>,
    ) -> Result<T, BulkReplayRunError<E>> {
        let result = operation(&mut self);
        let finish = self.finish();
        match (result, finish) {
            (Ok(value), Ok(())) => Ok(value),
            (Err(operation), Ok(())) => Err(BulkReplayRunError::Operation(operation)),
            (Ok(_), Err(finish)) => Err(BulkReplayRunError::Finish(finish)),
            (Err(operation), Err(finish)) => {
                Err(BulkReplayRunError::OperationAndFinish { operation, finish })
            }
        }
    }
}

/// Preserve both failures when an operation and its finalization fail.
#[derive(Debug)]
pub enum BulkReplayRunError<E> {
    Operation(E),
    Finish(BulkReplayError),
    OperationAndFinish {
        operation: E,
        finish: BulkReplayError,
    },
}

impl<E: fmt::Display> fmt::Display for BulkReplayRunError<E> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Operation(error) => write!(formatter, "bulk replay failed: {error}"),
            Self::Finish(error) => write!(formatter, "finishing bulk replay failed: {error}"),
            Self::OperationAndFinish { operation, finish } => write!(
                formatter,
                "bulk replay failed ({operation}) and finalization also failed ({finish})"
            ),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for BulkReplayRunError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Operation(error)
            | Self::OperationAndFinish {
                operation: error, ..
            } => Some(error),
            Self::Finish(error) => Some(error),
        }
    }
}

#[cfg(feature = "mithril")]
impl dolos_snapshot::backfill::Workspace for ReplayWorkspace<'_> {
    type Session = BulkReplaySession;

    fn snapshot(&self) -> impl SnapshotSource + '_ {
        self.snapshot()
    }

    fn start(self, target: u64) -> Result<Self::Session, dolos_snapshot::backfill::Error> {
        self.start(Some(target))
            .map_err(dolos_snapshot::backfill::Error::caller)
    }

    fn finish(self) -> Result<(), dolos_snapshot::backfill::Error> {
        self.finish()
            .map_err(dolos_snapshot::backfill::Error::caller)
    }
}

#[cfg(feature = "mithril")]
impl dolos_snapshot::backfill::Session for BulkReplaySession {
    fn committed_position(&self) -> Result<Option<ChainPoint>, dolos_snapshot::backfill::Error> {
        self.committed_position()
            .map_err(dolos_snapshot::backfill::Error::caller)
    }

    fn import_blocks(
        &mut self,
        blocks: Vec<RawBlock>,
    ) -> Result<ReplayProgress, dolos_snapshot::backfill::Error> {
        self.import_blocks(blocks)
            .map_err(dolos_snapshot::backfill::Error::caller)
    }

    fn prune_history(&mut self) -> Result<u64, dolos_snapshot::backfill::Error> {
        self.prune_history()
            .map_err(dolos_snapshot::backfill::Error::caller)
    }

    fn finish(self) -> Result<(), dolos_snapshot::backfill::Error> {
        self.finish()
            .map_err(dolos_snapshot::backfill::Error::caller)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finalization_preserves_both_errors() {
        let result = complete::<()>(
            Err(BulkReplayError::new("operation", "first failure")),
            Err(BulkReplayError::new("finish", "second failure")),
        );
        let error = result.unwrap_err().to_string();
        assert!(error.contains("first failure"));
        assert!(error.contains("second failure"));
    }
}
