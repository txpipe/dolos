//! `dolos config`: commands over the configuration file alone.
//!
//! The file is loaded, validated and its network resolved before any command
//! runs, so `check` only reports what was resolved. No store is opened.

use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};
use dolos_core::config::{FjallPreset, RootConfig, StoreType};

#[derive(Debug, Subcommand)]
pub enum Command {
    /// validates the configuration and prints the network and stores it
    /// resolves to, without opening any of them
    Check,
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
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

pub fn run(config: &RootConfig, config_file: &Path, args: &Args) -> miette::Result<()> {
    match args.command {
        Command::Check => check(config, config_file),
    }
}
