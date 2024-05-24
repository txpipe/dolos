use dolos::wal::redb::WalStore;
use miette::{Context as _, IntoDiagnostic};
use pallas::ledger::configs::alonzo::GenesisFile as AlonzoFile;
use pallas::ledger::configs::byron::GenesisFile as ByronFile;
use pallas::ledger::configs::shelley::GenesisFile as ShelleyFile;
use std::path::Path;
use tracing::Level;
use tracing_subscriber::{filter::Targets, prelude::*};

use dolos::{ledger::store::LedgerStore, prelude::*};

use crate::{GenesisConfig, LoggingConfig};

fn define_rolldb_path(config: &crate::Config) -> &Path {
    config
        .storage
        .path
        .as_deref()
        .unwrap_or_else(|| Path::new("./data"))
}

pub type Stores = (WalStore, LedgerStore);

pub fn open_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    let rolldb_path = define_rolldb_path(config);

    std::fs::create_dir_all(rolldb_path).map_err(Error::storage)?;

    let wal = WalStore::open(rolldb_path.join("wal")).map_err(Error::storage)?;
    let ledger = LedgerStore::open(rolldb_path.join("ledger")).map_err(Error::storage)?;

    Ok((wal, ledger))
}

#[allow(dead_code)]
pub fn destroy_data_stores(config: &crate::Config) -> Result<(), Error> {
    let rolldb_path = define_rolldb_path(config);

    std::fs::remove_file(rolldb_path.join("wal")).map_err(Error::storage)?;
    std::fs::remove_file(rolldb_path.join("ledger")).map_err(Error::storage)?;

    Ok(())
}

pub fn setup_tracing(config: &LoggingConfig) -> miette::Result<()> {
    let level = config.max_level.unwrap_or(Level::INFO);

    let mut filter = Targets::new()
        .with_target("dolos", level)
        .with_target("gasket", level);

    if config.include_pallas {
        filter = filter.with_target("pallas", level);
    }

    if config.include_grpc {
        filter = filter.with_target("tonic", level);
    }

    tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(level)
        .finish()
        .with(filter)
        .init();

    Ok(())
}

pub type GenesisFiles = (ByronFile, ShelleyFile, AlonzoFile);

pub fn open_genesis_files(config: &GenesisConfig) -> miette::Result<GenesisFiles> {
    let byron_genesis = pallas::ledger::configs::byron::from_file(&config.byron_path)
        .into_diagnostic()
        .context("loading byron genesis config")?;

    let shelley_genesis = pallas::ledger::configs::shelley::from_file(&config.shelley_path)
        .into_diagnostic()
        .context("loading shelley genesis config")?;

    let alonzo_genesis = pallas::ledger::configs::alonzo::from_file(&config.alonzo_path)
        .into_diagnostic()
        .context("loading alonzo genesis config")?;

    Ok((byron_genesis, shelley_genesis, alonzo_genesis))
}
