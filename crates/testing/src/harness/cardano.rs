use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use dolos_cardano::{CardanoDelta, CardanoLogic, CardanoWorkUnit};
use dolos_core::{
    config::{CardanoConfig, FjallStateConfig, StorageConfig},
    BootstrapExt, ChainLogic, Domain, DomainError, Genesis, MempoolError, MempoolEvent,
    MempoolStore, MempoolTx, MempoolTxStage, StateStore as CoreStateStore, TipEvent,
    TipSubscription as CoreTipSubscription, *,
};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub struct Config {
    /// Writable state directory (fjall) — caller is responsible for setup.
    pub state_dir: PathBuf,
    /// Path to Mithril immutable DB directory.
    pub immutable_dir: PathBuf,
    /// Pre-loaded genesis data.
    pub genesis: Genesis,
    /// Cardano chain config (stop_epoch, track, etc).
    pub chain: CardanoConfig,
    /// Fjall state store configuration.
    pub fjall_config: FjallStateConfig,
}

// ---------------------------------------------------------------------------
// Stub Mempool (same pattern as ToyDomain)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct Mempool {}

pub struct MempoolStream;

impl futures_core::Stream for MempoolStream {
    type Item = Result<MempoolEvent, MempoolError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Pending
    }
}

impl MempoolStore for Mempool {
    type Stream = MempoolStream;

    fn receive(&self, _tx: MempoolTx) -> Result<(), MempoolError> {
        Ok(())
    }

    fn has_pending(&self) -> bool {
        false
    }

    fn peek_pending(&self, _limit: usize) -> Vec<MempoolTx> {
        vec![]
    }

    fn pending(&self) -> Vec<(TxHash, EraCbor)> {
        vec![]
    }

    fn mark_inflight(&self, _hashes: &[TxHash]) {}

    fn mark_acknowledged(&self, _hashes: &[TxHash]) {}

    fn get_inflight(&self, _tx_hash: &TxHash) -> Option<MempoolTx> {
        None
    }

    fn apply(&self, _seen_txs: &[TxHash], _unseen_txs: &[TxHash]) {}

    fn finalize(&self, _threshold: u32) {}

    fn check_stage(&self, _tx_hash: &TxHash) -> MempoolTxStage {
        MempoolTxStage::Unknown
    }

    fn subscribe(&self) -> Self::Stream {
        MempoolStream
    }
}

// ---------------------------------------------------------------------------
// Stub TipSubscription
// ---------------------------------------------------------------------------

pub struct StubTipSubscription;

impl CoreTipSubscription for StubTipSubscription {
    async fn next_tip(&mut self) -> TipEvent {
        std::future::pending().await
    }
}

// ---------------------------------------------------------------------------
// HarnessDomain
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct HarnessDomain {
    wal: dolos_redb3::wal::RedbWalStore<CardanoDelta>,
    chain: Arc<RwLock<CardanoLogic>>,
    state: dolos_fjall::StateStore,
    archive: dolos_core::builtin::NoOpArchiveStore,
    indexes: dolos_core::builtin::NoOpIndexStore,
    mempool: Mempool,
    storage_config: StorageConfig,
    genesis: Arc<Genesis>,
}

impl Domain for HarnessDomain {
    type Entity = dolos_cardano::CardanoEntity;
    type EntityDelta = CardanoDelta;
    type Wal = dolos_redb3::wal::RedbWalStore<CardanoDelta>;
    type Archive = dolos_core::builtin::NoOpArchiveStore;
    type State = dolos_fjall::StateStore;
    type Chain = CardanoLogic;
    type WorkUnit = CardanoWorkUnit;
    type TipSubscription = StubTipSubscription;
    type Indexes = dolos_core::builtin::NoOpIndexStore;
    type Mempool = Mempool;

    fn storage_config(&self) -> &StorageConfig {
        &self.storage_config
    }

    fn genesis(&self) -> Arc<Genesis> {
        self.genesis.clone()
    }

    fn read_chain(&self) -> std::sync::RwLockReadGuard<'_, Self::Chain> {
        self.chain.read().expect("chain lock poisoned")
    }

    fn write_chain(&self) -> std::sync::RwLockWriteGuard<'_, Self::Chain> {
        self.chain.write().expect("chain lock poisoned")
    }

    fn wal(&self) -> &Self::Wal {
        &self.wal
    }

    fn state(&self) -> &Self::State {
        &self.state
    }

    fn archive(&self) -> &Self::Archive {
        &self.archive
    }

    fn indexes(&self) -> &Self::Indexes {
        &self.indexes
    }

    fn mempool(&self) -> &Self::Mempool {
        &self.mempool
    }

    fn watch_tip(&self, _from: Option<ChainPoint>) -> Result<Self::TipSubscription, DomainError> {
        Ok(StubTipSubscription)
    }

    fn notify_tip(&self, _tip: TipEvent) {
        // no-op: we don't need tip notifications in the harness
    }
}

// ---------------------------------------------------------------------------
// LedgerHarness
// ---------------------------------------------------------------------------

pub struct LedgerHarness {
    domain: HarnessDomain,
    immutable_path: PathBuf,
}

impl LedgerHarness {
    pub fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        // 1. Open fjall StateStore from provided dir with custom config
        let state = dolos_fjall::StateStore::open(&config.state_dir, &config.fjall_config)?;

        // 3. Initialize chain logic
        let genesis = Arc::new(config.genesis);
        let chain = CardanoLogic::initialize::<HarnessDomain>(config.chain, &state, &genesis)?;

        // 4. Assemble domain
        let domain = HarnessDomain {
            wal: dolos_redb3::wal::RedbWalStore::memory()?,
            chain: Arc::new(RwLock::new(chain)),
            state,
            archive: dolos_core::builtin::NoOpArchiveStore,
            indexes: dolos_core::builtin::NoOpIndexStore,
            mempool: Mempool {},
            storage_config: StorageConfig::default(),
            genesis,
        };

        // 5. Bootstrap (integrity check + drain pending work)
        domain.bootstrap()?;

        Ok(Self {
            domain,
            immutable_path: config.immutable_dir,
        })
    }

    /// Process blocks from the immutable DB, calling `on_work` after each work
    /// unit is executed. Uses the import lifecycle (skips WAL, no tip events).
    pub fn run<F>(
        &self,
        chunk_size: usize,
        mut on_work: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(&HarnessDomain, &CardanoWorkUnit),
    {
        use pallas::network::miniprotocols::Point;

        let cursor = self
            .domain
            .state
            .read_cursor()?
            .map(|c| c.try_into().unwrap())
            .unwrap_or(Point::Origin);

        let mut iter = pallas::storage::hardano::immutable::read_blocks_from_point(
            &self.immutable_path,
            cursor.clone(),
        )?;

        // Skip first block when resuming (cursor points at last processed block)
        if cursor != Point::Origin {
            iter.next();
        }

        for chunk in itertools::Itertools::chunks(iter, chunk_size).into_iter() {
            let raw_blocks: Vec<_> = chunk.collect::<Result<Vec<_>, _>>()?;
            let raw_blocks: Vec<_> = raw_blocks.into_iter().map(Arc::new).collect();

            // Feed blocks and process work units one at a time for callback
            for block in raw_blocks {
                {
                    let mut chain = self.domain.write_chain();
                    if !chain.can_receive_block()
                        && self.drain_with_callback(&mut on_work)?
                    {
                        return Ok(());
                    }
                    chain.receive_block(block)?;
                }
                if self.drain_with_callback(&mut on_work)? {
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn drain_with_callback<F>(&self, on_work: &mut F) -> Result<bool, Box<dyn std::error::Error>>
    where
        F: FnMut(&HarnessDomain, &CardanoWorkUnit),
    {
        loop {
            let work = {
                let mut chain = self.domain.write_chain();
                chain.pop_work(&self.domain)
            };

            let Some(mut work) = work else { break };

            // Use import-style execution (no WAL, no tip notify)
            use dolos_core::WorkUnit;
            WorkUnit::<HarnessDomain>::load(&mut work, &self.domain)?;
            WorkUnit::<HarnessDomain>::compute(&mut work)?;

            match WorkUnit::<HarnessDomain>::commit_state(&mut work, &self.domain) {
                Ok(()) => {}
                Err(DomainError::StopEpochReached) => return Ok(true),
                Err(e) => return Err(e.into()),
            }

            WorkUnit::<HarnessDomain>::commit_archive(&mut work, &self.domain)?;
            WorkUnit::<HarnessDomain>::commit_indexes(&mut work, &self.domain)?;

            on_work(&self.domain, &work);
        }

        Ok(false)
    }

    pub fn state(&self) -> &dolos_fjall::StateStore {
        &self.domain.state
    }

    pub fn domain(&self) -> &HarnessDomain {
        &self.domain
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;

    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let dest_path = dst.join(entry.file_name());

        if file_type.is_dir() {
            copy_dir_recursive(&entry.path(), &dest_path)?;
        } else {
            std::fs::copy(entry.path(), &dest_path)?;
        }
    }

    Ok(())
}
