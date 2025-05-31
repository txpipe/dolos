use dolos::{chain, ledger::pparams::Genesis, state, wal};
use miette::{Context as _, IntoDiagnostic};
use std::{fs, path::PathBuf, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{filter::Targets, prelude::*};

use dolos::prelude::*;

use crate::{GenesisConfig, LoggingConfig};

pub type Stores = (wal::redb::WalStore, state::LedgerStore, chain::ChainStore);

pub fn ensure_storage_path(config: &crate::Config) -> Result<PathBuf, Error> {
    let root = config.storage.path.as_ref().ok_or(Error::config(
        "can't define storage path for ephemeral config",
    ))?;

    std::fs::create_dir_all(root)?;

    Ok(root.to_path_buf())
}

pub fn open_wal_store(config: &crate::Config) -> Result<wal::redb::WalStore, Error> {
    let root = ensure_storage_path(config)?;

    let wal = wal::redb::WalStore::open(
        root.join("wal"),
        config.storage.wal_cache,
        config.storage.max_wal_history,
    )?;

    Ok(wal)
}

pub fn open_chain_store(config: &crate::Config) -> Result<chain::ChainStore, Error> {
    let root = ensure_storage_path(config)?;

    let chain = chain::redb::ChainStore::open(
        root.join("chain"),
        config.storage.chain_cache,
        config.storage.max_chain_history,
    )?;

    Ok(chain.into())
}

pub fn open_ledger_store(config: &crate::Config) -> Result<state::LedgerStore, Error> {
    let root = ensure_storage_path(config)?;

    let ledger = state::redb::LedgerStore::open(root.join("ledger"), config.storage.ledger_cache)?;

    Ok(ledger.into())
}

pub fn open_persistent_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    if config.storage.version == StorageVersion::V0 {
        error!("Storage should be removed and init procedure run again.");
        return Err(Error::StorageError("Invalid store version".to_string()));
    }

    let wal = open_wal_store(config)?;
    let ledger = open_ledger_store(config)?;
    let chain = open_chain_store(config)?;

    Ok((wal, ledger, chain))
}

pub fn create_ephemeral_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    let mut wal = wal::redb::WalStore::memory(config.storage.max_wal_history)?;

    wal.initialize_from_origin()?;

    let ledger = state::LedgerStore::Redb(state::redb::LedgerStore::in_memory_v2()?);

    let chain = chain::ChainStore::Redb(chain::redb::ChainStore::in_memory_v1()?);

    Ok((wal, ledger, chain))
}

pub fn setup_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    if config.storage.is_ephemeral() {
        create_ephemeral_data_stores(config)
    } else {
        open_persistent_data_stores(config)
    }
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

    if config.include_trp {
        filter = filter.with_target("jsonrpsee-server", level);
    }

    if config.include_minibf {
        filter = filter.with_target("tower_http", level);
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

pub fn open_genesis_files(config: &GenesisConfig) -> miette::Result<Genesis> {
    let byron_genesis = pallas::ledger::configs::byron::from_file(&config.byron_path)
        .into_diagnostic()
        .context("loading byron genesis config")?;

    let shelley_genesis = pallas::ledger::configs::shelley::from_file(&config.shelley_path)
        .into_diagnostic()
        .context("loading shelley genesis config")?;

    let alonzo_genesis = pallas::ledger::configs::alonzo::from_file(&config.alonzo_path)
        .into_diagnostic()
        .context("loading alonzo genesis config")?;

    let conway_genesis = pallas::ledger::configs::conway::from_file(&config.conway_path)
        .into_diagnostic()
        .context("loading conway genesis config")?;

    Ok(Genesis {
        byron: byron_genesis,
        shelley: shelley_genesis,
        alonzo: alonzo_genesis,
        conway: conway_genesis,
        force_protocol: config.force_protocol,
    })
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

pub fn cleanup_data(config: &crate::Config) -> Result<(), std::io::Error> {
    let Some(root) = &config.storage.path else {
        return Ok(());
    };

    if root.is_dir() {
        for entry_result in fs::read_dir(root)? {
            let entry = entry_result?;
            let entry_path = entry.path();
            if entry_path.is_file() {
                fs::remove_file(&entry_path)?;
            }
        }
        fs::remove_dir(root)?; // Remove the now-empty directory
    } else {
        info!("Path is not a directory, ignoring.");
    }
    Ok(())
}
