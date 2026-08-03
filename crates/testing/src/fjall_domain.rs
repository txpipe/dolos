//! A `Domain` implementation backed by the live storage set.
//!
//! [`ToyDomain`](crate::toy_domain::ToyDomain) binds its state and index
//! stores to redb3, which is exactly the pair that returns `Unsupported` for
//! the export/restore seam (`StateStore::iter_utxos`,
//! `IndexStore::iter_archive_tags`, `IndexWriter::append_prehashed`). This
//! sibling binds them to fjall — the live backends — so domain-level tests
//! can reach those APIs through the `Domain` trait instead of through a
//! concrete store.
//!
//! It is a sibling rather than a parameterization on purpose: the
//! fault-injection suites are written against the redb-backed `ToyDomain`
//! and stay there, and the two backends differ in construction (redb offers
//! in-memory stores, fjall needs a filesystem path — held here as a temp
//! directory that lives as long as the domain).

use crate::toy_domain::{Mempool, TipSubscription};
use dolos_cardano::indexes::index_delta_from_utxo_delta;
use dolos_core::{
    config::{CardanoConfig, FjallIndexConfig, FjallStateConfig, StorageConfig, SyncConfig},
    sync::execute_work_unit,
    BootstrapExt, LogKey, TemporalKey, *,
};
use std::sync::Arc;
use std::sync::RwLock;

#[derive(Clone)]
pub struct FjallToyDomain {
    wal: dolos_redb3::wal::RedbWalStore<dolos_cardano::CardanoDelta>,
    chain: Arc<RwLock<dolos_cardano::CardanoLogic>>,
    state: dolos_fjall::StateStore,
    archive: dolos_redb3::archive::ArchiveStore,
    indexes: dolos_fjall::IndexStore,
    mempool: Mempool,
    storage_config: StorageConfig,
    sync_config: SyncConfig,
    genesis: Arc<dolos_core::Genesis>,
    tip_broadcast: tokio::sync::broadcast::Sender<TipEvent>,
    /// Keeps the fjall databases' directory alive for the domain's lifetime.
    _dir: Arc<tempfile::TempDir>,
}

impl FjallToyDomain {
    /// Create a new domain over fjall state/index stores held in a temp
    /// directory, seeded through the same genesis flow `ToyDomain` uses.
    pub fn new(initial_delta: Option<UtxoSetDelta>) -> Self {
        let genesis = Arc::new(dolos_cardano::include::devnet::load());
        Self::new_with_genesis(genesis, initial_delta)
    }

    pub fn new_with_genesis(
        genesis: Arc<dolos_core::Genesis>,
        initial_delta: Option<UtxoSetDelta>,
    ) -> Self {
        let config = CardanoConfig::default();

        let dir = tempfile::tempdir().expect("failed to create temp dir for fjall stores");

        let state =
            dolos_fjall::StateStore::open(dir.path().join("state"), &FjallStateConfig::default())
                .unwrap();

        let indexes =
            dolos_fjall::IndexStore::open(dir.path().join("index"), &FjallIndexConfig::default())
                .unwrap();

        let archive =
            dolos_redb3::archive::ArchiveStore::in_memory(dolos_cardano::model::build_schema())
                .unwrap();

        let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);

        let chain =
            dolos_cardano::CardanoLogic::initialize::<Self>(config.clone(), &state, &genesis)
                .unwrap();

        // Create the domain first (genesis work unit needs it for execution)
        let domain = Self {
            state,
            wal: dolos_redb3::wal::RedbWalStore::memory().unwrap(),
            chain: Arc::new(RwLock::new(chain)),
            archive,
            indexes,
            mempool: Mempool::default(),
            storage_config: StorageConfig::default(),
            sync_config: SyncConfig::default(),
            genesis: genesis.clone(),
            tip_broadcast,
            _dir: Arc::new(dir),
        };

        // Apply genesis state using the work unit pattern.
        // Note: We're bypassing the normal pop_work flow here, so we need to
        // manually trigger the cache refresh that would normally happen.
        let mut genesis_work = dolos_cardano::CardanoWorkUnit::Genesis(Box::new(
            dolos_cardano::genesis::GenesisWorkUnit::new(config, genesis),
        ));
        execute_work_unit(&domain, &mut genesis_work).unwrap();

        // Manually refresh the chain cache after genesis since we bypassed
        // pop_work. In normal operation, the cache refresh happens
        // automatically via the needs_cache_refresh flag in
        // CardanoLogic::pop_work.
        {
            let mut chain = domain.chain.write().expect("chain lock poisoned");
            chain.refresh_cache::<Self>(&domain.state).unwrap();
        }

        domain.bootstrap().unwrap();

        // Ensure the current epoch state is available as an archive log entry.
        let chain = dolos_cardano::eras::load_era_summary::<Self>(&domain.state).unwrap();
        let epoch = dolos_cardano::load_epoch::<Self>(&domain.state).unwrap();
        let epoch_start = chain.epoch_start(epoch.number);
        let log_key = LogKey::from(TemporalKey::from(epoch_start));
        let writer = domain.archive.start_writer().unwrap();
        writer.write_log_typed(&log_key, &epoch).unwrap();
        writer.commit().unwrap();

        if let Some(delta) = initial_delta {
            let writer = domain.state.start_writer().unwrap();
            let index_writer = domain.indexes.start_writer().unwrap();
            writer.apply_utxoset(&delta).unwrap();

            // Build index delta from UTxO delta using Cardano-specific helper
            let cursor = domain
                .state
                .read_cursor()
                .unwrap()
                .unwrap_or(ChainPoint::Origin);
            let index_delta = index_delta_from_utxo_delta(cursor, &delta);
            index_writer.apply(&index_delta).unwrap();

            writer.commit().unwrap();
            index_writer.commit().unwrap();
        }

        domain
    }
}

impl dolos_core::Domain for FjallToyDomain {
    type Entity = dolos_cardano::CardanoEntity;
    type EntityDelta = dolos_cardano::CardanoDelta;
    type Wal = dolos_redb3::wal::RedbWalStore<dolos_cardano::CardanoDelta>;
    type Archive = dolos_redb3::archive::ArchiveStore;
    type State = dolos_fjall::StateStore;
    type Chain = dolos_cardano::CardanoLogic;
    type WorkUnit = dolos_cardano::CardanoWorkUnit;
    type TipSubscription = TipSubscription;
    type Indexes = dolos_fjall::IndexStore;
    type Mempool = Mempool;

    fn storage_config(&self) -> &StorageConfig {
        &self.storage_config
    }

    fn sync_config(&self) -> &SyncConfig {
        &self.sync_config
    }

    fn genesis(&self) -> Arc<dolos_core::Genesis> {
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

    fn watch_tip(&self, from: Option<ChainPoint>) -> Result<Self::TipSubscription, DomainError> {
        let receiver = self.tip_broadcast.subscribe();

        let replay = self
            .wal()
            .iter_blocks(from.clone(), None)?
            .filter(|(point, _)| match from.as_ref() {
                Some(from) => from != point,
                None => true,
            })
            .collect::<Vec<_>>();

        Ok(TipSubscription::new(replay, receiver))
    }

    fn notify_tip(&self, tip: TipEvent) {
        if self.tip_broadcast.receiver_count() > 0 {
            self.tip_broadcast.send(tip).unwrap();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_genesis_delta;
    use dolos_cardano::indexes::archive_dimensions;
    use dolos_core::{ArchiveIndexDelta, IndexRecord, Tag, TagRecord};

    /// Exercise the export/restore seam strictly through the `Domain` trait:
    /// past the constructor, no concrete store type appears.
    fn export_seam_roundtrip<D: Domain>(domain: &D) {
        // State side: the UTxO set is iterable.
        let utxos = domain
            .state()
            .iter_utxos()
            .expect("iter_utxos should be supported on the live backend")
            .collect::<Result<Vec<_>, _>>()
            .expect("utxo iteration failed");

        assert!(!utxos.is_empty(), "seeded UTxOs should be iterable");

        // Index side: seed one archive delta the way the sync pipeline would.
        let slot = 1234;
        let dimension = archive_dimensions::ADDRESS;
        let key = vec![0xAB; 29];

        let delta = IndexDelta {
            cursor: ChainPoint::Slot(slot),
            utxo: Default::default(),
            archive: vec![ArchiveIndexDelta {
                slot,
                block_hash: vec![0x01; 32],
                block_number: Some(1),
                tx_hashes: vec![vec![0x02; 32]],
                tags: vec![Tag::new(dimension, key.clone())],
            }],
        };

        let writer = domain.indexes().start_writer().unwrap();
        writer.apply(&delta).unwrap();
        writer.commit().unwrap();

        // Export: record iteration yields the seeded tag.
        let records = domain
            .indexes()
            .iter_archive_tags(&[dimension], 0..10_000)
            .expect("iter_archive_tags should be supported on the live backend")
            .collect::<Result<Vec<TagRecord>, _>>()
            .expect("tag iteration failed")
            .into_iter()
            .map(IndexRecord::from)
            .collect::<Vec<_>>();

        assert!(!records.is_empty(), "the seeded tag should be exported");

        // Restore: pre-hashed append accepts exactly what iteration yielded.
        let writer = domain.indexes().start_writer().unwrap();
        writer
            .append_prehashed(&records)
            .expect("append_prehashed should be supported on the live backend");
        writer.commit().unwrap();

        // The store still answers the logical-key query after the rewrite.
        let slots = domain
            .indexes()
            .slots_by_tag(dimension, &key, 0, 9_999)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(slots, vec![slot]);
    }

    #[test]
    fn fjall_domain_reaches_export_seam_through_domain_trait() {
        let domain = FjallToyDomain::new(Some(fake_genesis_delta(1_000_000)));
        export_seam_roundtrip(&domain);
    }
}
