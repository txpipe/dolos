//! `dolos config`: commands over the configuration file alone.
//!
//! `check` loads, validates and resolves the file as every command does, and
//! only reports what was resolved. `write-genesis` reads the file only when it
//! is not given a network. No store is opened.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use dolos_cardano::networks::{KnownNetwork, ResolvedNetwork};
use dolos_core::config::{ChainConfig, FjallPreset, GenesisConfig, RootConfig, StoreType};
use miette::{Context as _, IntoDiagnostic as _};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// validates the configuration and prints the network and stores it
    /// resolves to, without opening any of them
    Check,

    /// writes a known network's genesis files, without opening any store
    WriteGenesis(WriteGenesisArgs),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, clap::Args)]
pub struct WriteGenesisArgs {
    /// A known network; without it, the configuration's network, and no
    /// configuration is read when it is given
    #[arg(long, value_name = "NAME", value_parser = parse_network)]
    network: Option<&'static KnownNetwork>,

    /// The directory the files are written to, created if missing
    #[arg(long, value_name = "PATH", default_value = "./genesis")]
    dir: PathBuf,

    /// Overwrite files that already exist
    #[arg(long)]
    force: bool,
}

const CUSTOM_GENESIS: &str = "custom genesis is never generated; provide the files";

fn parse_network(name: &str) -> Result<&'static KnownNetwork, String> {
    if name.eq_ignore_ascii_case(ChainConfig::CUSTOM) {
        return Err(CUSTOM_GENESIS.into());
    }

    crate::common::parse_known_network(name)
}

fn store(kind: StoreType, path: Option<PathBuf>) -> String {
    match (kind, path) {
        (StoreType::Fjall { preset }, Some(path)) => {
            let preset = match preset {
                FjallPreset::Standard => "standard",
            };
            format!("fjall ({preset}) at {}", path.display())
        }
        _ => "ephemeral".into(),
    }
}

fn check(config: &RootConfig, config_file: &Path) -> miette::Result<()> {
    let network = crate::common::resolve_network(config)?;
    let name = network
        .known()
        .map_or(config.chain.network.as_str(), |known| known.name);
    let kind = if network.is_testnet() {
        "testnet"
    } else {
        "mainnet"
    };
    let storage = &config.storage;

    println!("valid: {}", config_file.display());
    println!("network: {name} (magic {}, {kind})", network.magic());
    println!("state: {}", store(storage.state.kind, storage.state_path()));

    match config.wal_path() {
        Some(path) => println!("wal: {}", path.display()),
        None => println!("wal: in memory"),
    }

    if config.keeps_archive() {
        println!(
            "archive: {}",
            store(storage.archive.kind, storage.archive_path())
        );

        if let Some(blocks) = &storage.archive.blocks_path {
            println!("blocks: {}", blocks.display());
        }
    } else {
        println!("archive: none (history.mode = \"none\")");
    }

    match storage.cache_path() {
        Some(path) => println!("cache: {}", path.display()),
        None => println!("cache: off"),
    }

    match config.mempool_path() {
        Some(path) => println!("mempool: {}", path.display()),
        None => println!("mempool: in memory"),
    }

    Ok(())
}

/// The configuration's network, which must be a known one. Its genesis files
/// are not read: a custom network's are the ones that may be missing.
fn configured_network(config_file: &Path) -> miette::Result<&'static KnownNetwork> {
    let config = crate::common::load_config(Some(config_file))
        .map_err(miette::Report::from_err)
        .context("loading configuration")?;

    match crate::common::resolve_network(&config)? {
        ResolvedNetwork::Known(network) if config.chain.cardano.is_none() => Ok(network),
        _ => Err(miette::miette!(CUSTOM_GENESIS)),
    }
}

fn write_genesis(config_file: &Path, args: &WriteGenesisArgs) -> miette::Result<()> {
    let network = match args.network {
        Some(network) => network,
        None => configured_network(config_file)?,
    };

    let files = GenesisConfig::in_dir(&args.dir);
    let paths = [
        &files.byron_path,
        &files.shelley_path,
        &files.alonzo_path,
        &files.conway_path,
    ];

    let existing: Vec<String> = paths
        .iter()
        .filter(|path| path.exists())
        .map(|path| path.display().to_string())
        .collect();

    if !existing.is_empty() && !args.force {
        return Err(miette::miette!(
            help = "pass --force to overwrite them",
            "genesis files already exist: {}",
            existing.join(", ")
        ));
    }

    network
        .save_genesis_to(&files)
        .into_diagnostic()
        .context("writing genesis")?;

    for path in paths {
        println!("{}", path.display());
    }

    Ok(())
}

pub fn run(config_file: &Path, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::Check => check(
            &crate::common::load_and_check_config(config_file)?,
            config_file,
        ),
        Command::WriteGenesis(x) => write_genesis(config_file, x),
    }
}
