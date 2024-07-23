use dolos::{state, wal};
use miette::{Context as _, IntoDiagnostic};
use pallas::ledger::configs::alonzo::GenesisFile as AlonzoFile;
use pallas::ledger::configs::byron::GenesisFile as ByronFile;
use pallas::ledger::configs::shelley::GenesisFile as ShelleyFile;
use std::{path::PathBuf, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use tracing_subscriber::{filter::Targets, prelude::*};

use dolos::prelude::*;

use crate::{GenesisConfig, LoggingConfig};

pub type Stores = (wal::redb::WalStore, state::LedgerStore);

pub fn open_wal(config: &crate::Config) -> Result<wal::redb::WalStore, Error> {
    let root = &config.storage.path;

    std::fs::create_dir_all(root).map_err(Error::storage)?;

    let wal = wal::redb::WalStore::open(root.join("wal"), config.storage.wal_cache)
        .map_err(Error::storage)?;

    Ok(wal)
}

pub fn define_ledger_path(config: &crate::Config) -> Result<PathBuf, Error> {
    let root = &config.storage.path;
    std::fs::create_dir_all(root).map_err(Error::storage)?;

    let ledger = root.join("ledger");

    Ok(ledger)
}

pub fn open_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    let root = &config.storage.path;

    std::fs::create_dir_all(root).map_err(Error::storage)?;

    let wal = wal::redb::WalStore::open(root.join("wal"), config.storage.wal_cache)
        .map_err(Error::storage)?;

    let ledger = state::redb::LedgerStore::open(root.join("ledger"), config.storage.ledger_cache)
        .map_err(Error::storage)?
        .into();

    Ok((wal, ledger))
}

pub fn setup_tracing(config: &LoggingConfig) -> miette::Result<()> {
    let level = config.max_level;

    let mut filter = Targets::new()
        .with_target("dolos", level)
        .with_target("gasket", level);

    if config.include_tokio {
        filter = filter
            .with_target("tokio", level)
            .with_target("runtime", level);
    }

    if config.include_pallas {
        filter = filter.with_target("pallas", level);
    }

    if config.include_grpc {
        filter = filter.with_target("tonic", level);
    }

    #[cfg(not(feature = "debug"))]
    {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(filter)
            .init();
    }

    #[cfg(feature = "debug")]
    {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
            .with(console_subscriber::spawn())
            .with(filter)
            .init();
    }

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

#[inline]
#[cfg(unix)]
async fn wait_for_exit_signal() {
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            warn!("SIGINT detected");
        }
        _ = sigterm.recv() => {
            warn!("SIGTERM detected");
        }
    };
}

#[inline]
#[cfg(windows)]
async fn wait_for_exit_signal() {
    tokio::signal::ctrl_c().await.unwrap()
}

pub fn hook_exit_token() -> CancellationToken {
    let cancel = CancellationToken::new();

    let cancel2 = cancel.clone();
    tokio::spawn(async move {
        wait_for_exit_signal().await;
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
