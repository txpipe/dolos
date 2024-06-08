use std::time::Duration;

use dolos::wal::redb::WalStore;
use miette::{Context as _, IntoDiagnostic};
use pallas::ledger::configs::alonzo::GenesisFile as AlonzoFile;
use pallas::ledger::configs::byron::GenesisFile as ByronFile;
use pallas::ledger::configs::shelley::GenesisFile as ShelleyFile;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use tracing_subscriber::{filter::Targets, prelude::*};

use dolos::{ledger::store::LedgerStore, prelude::*};

use crate::{GenesisConfig, LoggingConfig};

pub type Stores = (WalStore, LedgerStore);

pub fn open_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    let root = &config.storage.path;

    std::fs::create_dir_all(root).map_err(Error::storage)?;

    let wal = WalStore::open(root.join("wal")).map_err(Error::storage)?;
    let ledger = LedgerStore::open(root.join("ledger")).map_err(Error::storage)?;

    Ok((wal, ledger))
}

pub fn data_stores_exist(config: &crate::Config) -> bool {
    let root = &config.storage.path;

    root.join("wal").is_file() || root.join("ledger").is_file()
}

pub fn destroy_data_stores(config: &crate::Config) -> Result<(), Error> {
    let root = &config.storage.path;

    if root.join("wal").is_file() {
        std::fs::remove_file(root.join("wal")).map_err(Error::storage)?;
    }

    if root.join("ledger").is_file() {
        std::fs::remove_file(root.join("ledger")).map_err(Error::storage)?;
    }

    Ok(())
}

pub fn setup_tracing(config: &LoggingConfig) -> miette::Result<()> {
    let level = config.max_level;

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

pub fn hook_exit_token() -> CancellationToken {
    let cancel = CancellationToken::new();

    let cancel2 = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        warn!("exit signal detected");
        debug!("notifying exit");
        cancel2.cancel();
    });

    cancel
}

pub async fn run_pipeline(pipeline: gasket::daemon::Daemon, exit: CancellationToken) {
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5000)) => {
                if pipeline.should_stop() {
                    break;
                }
            }
            _ = exit.cancelled() => {
                debug!("exit requested");
                break;
            }
        }
    }

    debug!("shutting down pipeline");
    pipeline.teardown();
}

pub fn spawn_pipeline(pipeline: gasket::daemon::Daemon, exit: CancellationToken) -> JoinHandle<()> {
    tokio::spawn(run_pipeline(pipeline, exit))
}
