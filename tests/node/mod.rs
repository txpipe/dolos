//! A live Dolos node behind a real `dolos.toml`, for the tests that drive the
//! binary.
//!
//! Shared by the restore suites because they need the same thing: a node whose
//! stores were opened by `open_data_stores`, so the backend enums stand in for
//! the store traits exactly as they do in production, and a configuration file
//! a separate process can be pointed at.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it. They are not.
#![allow(dead_code)]

use std::process::Command;
use std::sync::Arc;

use dolos::adapters::DomainAdapter;
use dolos::engine::DomainBuilder;
use dolos_cardano::CardanoLogic;
use dolos_core::{
    config::{CardanoConfig, ChainConfig, CustomUtxo, GenesisConfig, RootConfig},
    import::ImportExt as _,
    ArchiveStore as _, BootstrapExt as _, ChainLogic as _, ChainPoint, Domain as _, Genesis,
    QueryLimiter, RawBlock, StateStore as _, WalStore as _, WorkUnit,
};
use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
use pallas::ledger::traverse::MultiEraBlock;

/// A path as a TOML string *value*, quotes and escapes included.
///
/// Interpolated rather than quoted by hand, because a Windows temp path is
/// `C:\Users\RUNNER~1\...` and `\U` inside a TOML basic string opens an
/// eight-digit unicode escape — so a hand-quoted path parses on Unix and is a
/// syntax error on Windows. `toml::Value`'s own `Display` is the escaping rule,
/// and using it means this fixture cannot be wrong about it.
pub fn toml_string(path: &std::path::Path) -> String {
    toml::Value::String(path.display().to_string()).to_string()
}

/// A node directory holding preview genesis, a `dolos.toml` and, once
/// [`sync`] has run, a populated set of live stores.
pub struct Node {
    pub root: tempfile::TempDir,
    pub config: RootConfig,
}

impl Node {
    pub fn new() -> Self {
        Self::with_tables("")
    }

    /// [`Self::new`] with `tables` appended to its `dolos.toml`, such as
    /// store types or `[mempool]`.
    pub fn with_tables(tables: &str) -> Self {
        let root = tempfile::tempdir().unwrap();
        dolos_cardano::include::preview::save(root.path()).unwrap();

        // Without `tables`, storage backends are left at their defaults, which
        // is the point: this test is meant to run against whatever a real node
        // runs against.
        let toml = format!(
            r#"
            compatibility = "v2-alpha.1"

            [chain]
            protocol = "cardano"
            network = "preview"

            [upstream]
            peer_address = "unused.example:3001"

            [storage]
            path = {data}

            {tables}
            "#,
            data = toml_string(&root.path().join("data")),
        );

        std::fs::write(root.path().join("dolos.toml"), &toml).unwrap();

        // Parsed as written: `load` would apply the developer's `DOLOS_`
        // variables, which could point these stores anywhere.
        Self {
            root,
            config: toml::from_str(&toml).unwrap(),
        }
    }

    pub fn config_path(&self) -> std::path::PathBuf {
        self.root.path().join("dolos.toml")
    }

    /// A custom network of `magic` over the preview genesis in the node's
    /// directory.
    pub fn custom_chain(&self, magic: u32, custom_utxos: Vec<CustomUtxo>) -> ChainConfig {
        ChainConfig::custom(CardanoConfig {
            magic,
            is_testnet: true,
            genesis: GenesisConfig::in_dir(self.root.path()),
            custom_utxos,
        })
    }

    /// Replace the `[chain]` of the node's `dolos.toml`.
    pub fn write_chain(&self, chain: &ChainConfig) {
        let config_path = self.config_path();
        let mut document: toml::Value =
            toml::from_str(&std::fs::read_to_string(&config_path).unwrap()).unwrap();
        document["chain"] = toml::Value::try_from(chain).unwrap();
        std::fs::write(&config_path, toml::to_string(&document).unwrap()).unwrap();
    }

    /// Apply synthetic blocks through the real domain, then close every store
    /// so the publishing process can open them.
    pub fn sync(&self) {
        // Inside epoch zero, so the ledger stays epoch-coherent and the
        // `strict` assertions in `dolos-cardano` hold — see the note on the
        // export crate's own harness fixture.
        self.sync_chain(
            Arc::new(dolos_cardano::include::preview::load()),
            GenesisConfig::in_dir(self.root.path()),
            SyntheticBlockConfig {
                block_count: 3,
                txs_per_block: 2,
                slot: 100,
                ..Default::default()
            },
        );
    }

    /// [`Self::sync`] over `genesis`, which the files `files` names hold, and
    /// the synthetic blocks `blocks` configures.
    pub fn sync_chain(
        &self,
        genesis: Arc<dolos_core::Genesis>,
        files: GenesisConfig,
        blocks: SyntheticBlockConfig,
    ) {
        dolos::storage::ensure_storage_path(&self.config).unwrap();

        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&self.config).unwrap();

        let (blocks, _, chain_config) = build_synthetic_blocks(blocks);

        // The synthetic chain's initial UTxOs are seeded through the chain
        // config's `custom_utxos`, so they are part of this chain's genesis.
        // Persist them into `dolos.toml`: a separate process replaying from
        // origin (`doctor rebuild-state`) runs genesis from the file and must
        // seed the same set. Only a custom network carries them, so the file
        // becomes the genesis under a custom chain.
        self.write_chain(&ChainConfig::custom(CardanoConfig {
            magic: genesis.network_magic(),
            is_testnet: true,
            genesis: files,
            custom_utxos: chain_config.custom_utxos.clone(),
        }));

        let chain = dolos_cardano::CardanoLogic::initialize::<DomainAdapter>(
            chain_config,
            &stores.state,
            &genesis,
        )
        .unwrap();

        let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);
        let query_limiter = QueryLimiter::new(self.config.serve.max_concurrent_queries);
        let cache = dolos::storage::open_cache_store(&self.config, query_limiter.clone()).unwrap();

        let domain = DomainAdapter {
            compatibility: self.config.compatibility(),
            storage_config: Arc::new(self.config.storage.clone()),
            history_config: self.config.history,
            mempool_config: self.config.mempool.clone(),
            genesis,
            chain: Arc::new(std::sync::RwLock::new(chain)),
            wal: stores.wal,
            state: stores.state,
            archive: stores.archive,
            mempool: stores.mempool,
            cache,
            tip_broadcast,
            query_limiter,
        };

        domain.bootstrap().unwrap();
        domain.import_blocks(blocks).unwrap();

        assert!(
            domain.state().read_cursor().unwrap().is_some(),
            "the node did not advance past origin"
        );

        domain.shutdown().unwrap();
        drop(domain);
    }

    /// Put the synced stores on the first block of epoch 1, as a
    /// `stop_epoch` sync leaves them, so a stele can be cut from them.
    ///
    /// Appends two empty blocks chained onto the archive's tip — the last block
    /// of epoch 0 and the first of epoch 1 — and moves the state cursor onto
    /// the second. The ledger is not run over them; they are what the
    /// archive shows about where the state stands.
    ///
    /// Returns the first block's parent, the evidence a plan reports.
    pub fn stand_at_boundary(&self) -> dolos_core::ChainPoint {
        use dolos_cardano::pallas::ledger::traverse::MultiEraBlock;
        use dolos_core::{ArchiveStore as _, ArchiveWriter as _, StateWriter as _};
        use dolos_testing::blocks::make_conway_block_with_prev;

        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&self.config).unwrap();

        let summary = dolos_cardano::eras::load_chain_summary_from_state(&stores.state).unwrap();
        let start = summary.epoch_start(1);

        let (_, tip) = stores.archive.get_tip().unwrap().unwrap();
        let tip = MultiEraBlock::decode(&tip).unwrap();

        let (last, last_body) =
            make_conway_block_with_prev(start - 1, Some(tip.hash()), tip.number() + 1);
        let (first, first_body) = make_conway_block_with_prev(start, last.hash(), tip.number() + 2);

        let archive = stores.archive.start_writer().unwrap();
        archive.apply(&last, &last_body).unwrap();
        archive.apply(&first, &first_body).unwrap();
        archive.commit().unwrap();

        let state = stores.state.start_writer().unwrap();
        state.set_cursor(first).unwrap();
        state.commit().unwrap();

        drop(stores);

        last
    }

    /// Move the stores one block past the boundary [`Self::stand_at_boundary`]
    /// put them on: an empty block a few slots later in epoch 1, chained onto
    /// the cursor's, with the cursor on it. Where a node that kept syncing
    /// past `stop_epoch` stands.
    pub fn step_past_boundary(&self) {
        use dolos_cardano::pallas::ledger::traverse::MultiEraBlock;
        use dolos_core::{
            ArchiveStore as _, ArchiveWriter as _, StateStore as _, StateWriter as _,
        };
        use dolos_testing::blocks::make_conway_block_with_prev;

        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&self.config).unwrap();

        let cursor = stores.state.read_cursor().unwrap().unwrap();
        let body = stores
            .archive
            .get_block_by_slot(&cursor.slot())
            .unwrap()
            .unwrap();
        let number = MultiEraBlock::decode(&body).unwrap().number();

        let (next, next_body) =
            make_conway_block_with_prev(cursor.slot() + 5, cursor.hash(), number + 1);

        let archive = stores.archive.start_writer().unwrap();
        archive.apply(&next, &next_body).unwrap();
        archive.commit().unwrap();

        let state = stores.state.start_writer().unwrap();
        state.set_cursor(next).unwrap();
        state.commit().unwrap();

        drop(stores);
    }

    /// Write a directory stele as a restore fixture through the retained
    /// headless profile encoder.
    pub fn write_stele(&self, output: &std::path::Path) {
        use dolos_snapshot::facade::{Selection, SnapshotSource as _, StoreSnapshot};

        let stores =
            dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&self.config).unwrap();
        let genesis = dolos_cardano::include::preview::load();
        let retained = dolos_snapshot::planning::retained_epochs(&[]).unwrap();
        let snapshot = StoreSnapshot::new(&stores.archive, &stores.state);
        let plan = snapshot
            .selected_plan(
                u64::from(genesis.network_magic()),
                retained,
                Selection::default(),
            )
            .unwrap();

        snapshot
            .publish_directory(output, &plan, &dolos_snapshot::progress::Observer::silent())
            .unwrap();
    }

    /// `dolos bootstrap stelae --force --source …`, the operator's own restore
    /// command.
    ///
    /// `--force` is what clears the storage directory first, so this is the
    /// wipe and the restore in the one step an operator actually runs.
    pub fn bootstrap_stelae(&self, source: &str) -> std::process::Output {
        let mut command = self.command();

        command.args(["bootstrap", "stelae", "--force", "--source", source]);

        command.output().unwrap()
    }

    /// The same, naming which stele in a repository to read.
    pub fn bootstrap_stelae_at(&self, source: &str, point: &str) -> std::process::Output {
        let mut command = self.command();

        command.args([
            "bootstrap",
            "stelae",
            "--force",
            "--source",
            source,
            "--point",
            point,
        ]);

        command.output().unwrap()
    }

    /// `dolos bootstrap <flags>` with no subcommand and no terminal.
    ///
    /// With no subcommand, bootstrap asks which method to use — and, for a
    /// stele, where the stele is. With no stdin the first prompt cannot be
    /// answered, which is the cheapest stand-in for the cases that matter: a
    /// typo, a cancel, a machine with no terminal.
    pub fn bootstrap_headless(&self, flags: &[&str]) -> std::process::Output {
        let mut command = self.command();

        command
            .arg("bootstrap")
            .args(flags)
            .stdin(std::process::Stdio::null());

        command.output().unwrap()
    }

    /// `dolos <args>` against the node's `dolos.toml`.
    pub fn run(&self, args: &[&str]) -> std::process::Output {
        self.command().args(args).output().unwrap()
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_dolos"));

        command.arg("--config").arg(self.config_path());

        command
    }
}

pub fn assert_ok(output: &std::process::Output) -> String {
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();

    assert!(
        output.status.success(),
        "the command failed\nstdout:\n{stdout}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );

    stdout
}

/// A node over `tables` on the synthetic chain's network, and that chain's
/// blocks, for the tests that sync it live through [`start`].
pub fn fixture(tables: &str) -> (Node, Arc<Genesis>, Vec<RawBlock>) {
    let mut node = Node::with_tables(tables);
    let (blocks, _, chain) = build_synthetic_blocks(SyntheticBlockConfig {
        block_count: 4,
        slot: 100,
        ..Default::default()
    });
    node.config.chain = node.custom_chain(2, chain.custom_utxos);
    node.write_chain(&node.config.chain);

    let genesis = Arc::new(dolos_cardano::include::preview::load());

    (node, genesis, blocks)
}

pub fn start(node: &Node, genesis: &Arc<Genesis>) -> DomainAdapter {
    DomainBuilder::new(&node.config, genesis.clone())
        .build()
        .unwrap()
}

pub fn point(block: &RawBlock) -> ChainPoint {
    let block = MultiEraBlock::decode(block).unwrap();
    ChainPoint::Specific(block.slot(), block.hash())
}

/// Where in a work unit's lifecycle a crash comes.
#[derive(Clone, Copy, Debug)]
pub enum CrashAfter {
    /// After compute, before any commit.
    Compute,
    /// After the WAL commit.
    Wal,
    /// After the WAL and state commits.
    State,
}

/// Feed `blocks` through the work-unit lifecycle, stopping each block's unit
/// where `crash` says. Genesis, which a fresh node's first block brings, runs
/// whole.
pub fn feed_until_crash(domain: &DomainAdapter, blocks: &[RawBlock], crash: CrashAfter) {
    let mut chain = domain.write_chain();

    for block in blocks {
        if !chain.can_receive_block() {
            drain_until_crash(&mut chain, domain, crash);
        }
        chain.receive_block(block.clone()).unwrap();
    }

    drain_until_crash(&mut chain, domain, crash);
}

fn drain_until_crash(chain: &mut CardanoLogic, domain: &DomainAdapter, crash: CrashAfter) {
    while let Some(mut work) = chain.pop_work::<DomainAdapter>(domain) {
        if WorkUnit::<DomainAdapter>::name(&work) == "genesis" {
            dolos_core::sync::execute_work_unit(domain, &mut work).unwrap();
            continue;
        }

        WorkUnit::<DomainAdapter>::initialize(&mut work, domain).unwrap();

        let shards = WorkUnit::<DomainAdapter>::start_shard(&work)
            ..WorkUnit::<DomainAdapter>::total_shards(&work);

        for shard in shards {
            WorkUnit::<DomainAdapter>::load(&mut work, domain, shard).unwrap();
            WorkUnit::<DomainAdapter>::compute(&mut work, shard).unwrap();

            if matches!(crash, CrashAfter::Compute) {
                continue;
            }

            WorkUnit::<DomainAdapter>::commit_wal(&mut work, domain, shard).unwrap();

            if matches!(crash, CrashAfter::State) {
                WorkUnit::<DomainAdapter>::commit_state(&mut work, domain, shard).unwrap();
            }
        }
    }
}

/// The WAL tip, state cursor and archive tip, read without recovery.
pub fn store_positions(node: &Node) -> (Option<ChainPoint>, Option<ChainPoint>, Option<u64>) {
    let stores =
        dolos::storage::open_data_stores::<dolos_cardano::CardanoDelta>(&node.config).unwrap();

    let wal = stores.wal.find_tip().unwrap().map(|(point, _)| point);
    let state = stores.state.read_cursor().unwrap();
    let archive = stores.archive.get_tip().unwrap().map(|(slot, _)| slot);

    stores.state.shutdown().unwrap();
    stores.archive.shutdown().unwrap();

    (wal, state, archive)
}
