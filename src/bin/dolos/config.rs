//! `dolos config`: commands over the configuration file alone.
//!
//! The file is loaded, validated and its network resolved before any command
//! runs, so `check` only reports what was resolved. No store is opened.

use std::fmt;
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use dolos_core::config::{FjallPreset, RootConfig, StoreType};
use serde::Serialize;

use crate::output::FormatArgs;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// validates the configuration and prints the network and stores it
    /// resolves to, without opening any of them
    Check(CheckArgs),
}

#[derive(Debug, clap::Args)]
pub struct CheckArgs {
    #[command(flatten)]
    format: FormatArgs,
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

/// What `config check` resolved the configuration to.
#[derive(Debug, Serialize)]
struct Checked {
    valid: PathBuf,
    network: Network,
    state: Store,
    /// `None` while the WAL is kept in memory.
    wal: Option<PathBuf>,
    /// `None` without an archive (`history.mode = "none"`).
    archive: Option<Store>,
    /// Where block segments live, when apart from the archive.
    blocks: Option<PathBuf>,
    /// `None` while the cache is off.
    cache: Option<PathBuf>,
    /// `None` while the mempool is kept in memory.
    mempool: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct Network {
    name: String,
    magic: u64,
    testnet: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Store {
    Fjall { preset: &'static str, path: PathBuf },
    Ephemeral,
}

impl Store {
    fn new(kind: StoreType, path: Option<PathBuf>) -> Self {
        match (kind, path) {
            (StoreType::Fjall { preset }, Some(path)) => {
                let preset = match preset {
                    FjallPreset::Standard => "standard",
                };
                Self::Fjall { preset, path }
            }
            _ => Self::Ephemeral,
        }
    }
}

impl fmt::Display for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fjall { preset, path } => write!(f, "fjall ({preset}) at {}", path.display()),
            Self::Ephemeral => f.write_str("ephemeral"),
        }
    }
}

impl fmt::Display for Checked {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let network = &self.network;
        let kind = if network.testnet {
            "testnet"
        } else {
            "mainnet"
        };

        writeln!(f, "valid: {}", self.valid.display())?;
        writeln!(
            f,
            "network: {} (magic {}, {kind})",
            network.name, network.magic
        )?;
        writeln!(f, "state: {}", self.state)?;

        match &self.wal {
            Some(path) => writeln!(f, "wal: {}", path.display())?,
            None => writeln!(f, "wal: in memory")?,
        }

        match &self.archive {
            Some(archive) => {
                writeln!(f, "archive: {archive}")?;

                if let Some(blocks) = &self.blocks {
                    writeln!(f, "blocks: {}", blocks.display())?;
                }
            }
            None => writeln!(f, "archive: none (history.mode = \"none\")")?,
        }

        match &self.cache {
            Some(path) => writeln!(f, "cache: {}", path.display())?,
            None => writeln!(f, "cache: off")?,
        }

        match &self.mempool {
            Some(path) => write!(f, "mempool: {}", path.display()),
            None => write!(f, "mempool: in memory"),
        }
    }
}

fn check(config: &RootConfig, config_file: &Path, args: &CheckArgs) -> miette::Result<()> {
    let network = crate::common::resolve_network(config)?;
    let name = network
        .known()
        .map_or(config.chain.network.as_str(), |known| known.name);
    let storage = &config.storage;
    let keeps_archive = config.keeps_archive();

    let checked = Checked {
        valid: config_file.to_path_buf(),
        network: Network {
            name: name.to_string(),
            magic: network.magic(),
            testnet: network.is_testnet(),
        },
        state: Store::new(storage.state.kind, storage.state_path()),
        wal: config.wal_path(),
        archive: keeps_archive.then(|| Store::new(storage.archive.kind, storage.archive_path())),
        blocks: storage
            .archive
            .blocks_path
            .clone()
            .filter(|_| keeps_archive),
        cache: storage.cache_path(),
        mempool: config.mempool_path(),
    };

    crate::output::emit(&checked, &args.format)
}

pub fn run(config: &RootConfig, config_file: &Path, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::Check(x) => check(config, config_file, x),
    }
}
