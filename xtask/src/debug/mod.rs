//! Maintainer tooling over a node's stores, outside the `dolos` CLI.
//!
//! Each command opens the dataset of the `dolos.toml` it is given as it is on
//! disk, with the library's `open_existing_stores`: it creates, bootstraps and
//! catches up nothing, and refuses a store that is missing or that another
//! process holds, naming its path. Stop the node first. The commands that
//! write touch the state only, and open nothing else, so that a writable open
//! cannot migrate or wipe a WAL from another version.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use dolos::storage::{
    open_existing_state, open_existing_stores, Access, StateStoreBackend, Stores,
};
use dolos_cardano::CardanoDelta;
use dolos_core::config::RootConfig;

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
    fn load(&self) -> Result<RootConfig> {
        load_config(&self.config)
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

fn open(config: &RootConfig, access: Access) -> Result<Stores<CardanoDelta>> {
    open_existing_stores::<CardanoDelta>(config, access).context("opening the node's stores")
}

fn open_state(config: &RootConfig, access: Access) -> Result<StateStoreBackend> {
    open_existing_state(config, access).context("opening the node's state store")
}
