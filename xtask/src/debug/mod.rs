//! Maintainer tooling over a node's stores, outside the `dolos` CLI.
//!
//! Each command opens the dataset of the `dolos.toml` it is given as it is on
//! disk: it creates, bootstraps and catches up nothing, and refuses a store it
//! opens that is missing or that another process holds, naming its path. Stop
//! the node first. `find-seq` and `copy-wal` open every store with the
//! library's `open_existing_stores`. `update-entity` and `clear-state` open
//! the state alone with `open_existing_state`, so that a writable open cannot
//! migrate or wipe a WAL from another version. The library's logs go to
//! stderr as the node's do, at its `[logging]` section.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use dolos::storage::{
    open_existing_state, open_existing_stores, Access, StateStoreBackend, Stores,
};
use dolos_cardano::CardanoDelta;
use dolos_core::config::{LoggingConfig, RootConfig};
use tracing_subscriber::{
    filter::Targets, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

pub mod clear_state;
pub mod copy_wal;
pub mod find_seq;
pub mod update_entity;

#[derive(clap::Subcommand)]
pub enum Cmd {
    /// Find the WAL entry of a block
    FindSeq(find_seq::Args),

    /// Copy a range of slots from the WAL into a new WAL file
    CopyWal(copy_wal::Args),

    /// Replace an entity in the state
    UpdateEntity(update_entity::Args),

    /// Delete every entity of a state namespace
    ClearState(clear_state::Args),
}

/// The node configuration a command runs on.
#[derive(clap::Args)]
pub struct ConfigArgs {
    /// The node's `dolos.toml`
    #[arg(long, value_name = "PATH")]
    pub config: PathBuf,
}

impl ConfigArgs {
    /// The configuration, with its logging set up.
    fn load(&self) -> Result<RootConfig> {
        let config = load_config(&self.config)?;
        setup_logging(&config.logging)?;
        Ok(config)
    }
}

pub fn run(command: Cmd) -> Result<()> {
    match command {
        Cmd::FindSeq(args) => find_seq::run(&args),
        Cmd::CopyWal(args) => copy_wal::run(&args),
        Cmd::UpdateEntity(args) => update_entity::run(&args),
        Cmd::ClearState(args) => clear_state::run(&args),
    }
}

/// The configuration at `path`, read as the node reads it, `DOLOS_`
/// variables included.
pub fn load_config(path: &Path) -> Result<RootConfig> {
    dolos_core::config::load_checked(Some(path), |_| Ok(()))
        .with_context(|| format!("loading {}", path.display()))
}

/// Dolos's own targets at `logging.level`, then its `filter` directives, on
/// stderr, which leaves stdout to the command's result.
fn setup_logging(logging: &LoggingConfig) -> Result<()> {
    let mut filter = Targets::new()
        .with_target("dolos", logging.level)
        .with_target("gasket", logging.level);

    if let Some(directives) = &logging.filter {
        let extra: Targets = directives
            .parse()
            .with_context(|| format!("logging.filter = \"{directives}\""))?;

        if let Some(default) = extra.default_level() {
            filter = filter.with_default(default);
        }

        for (target, level) in extra.iter() {
            filter = filter.with_target(target, level);
        }
    }

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(filter)
        .try_init()
        .context("setting up logging")?;

    // fjall logs through the `log` crate
    tracing_log::LogTracer::init().ok();

    Ok(())
}

fn open(config: &RootConfig, access: Access) -> Result<Stores<CardanoDelta>> {
    open_existing_stores::<CardanoDelta>(config, access).context("opening the node's stores")
}

fn open_state(config: &RootConfig, access: Access) -> Result<StateStoreBackend> {
    open_existing_state(config, access).context("opening the node's state store")
}
