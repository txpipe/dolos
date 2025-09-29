use miette::{Context as _, IntoDiagnostic};
use std::sync::Arc;
use std::{fs, path::PathBuf, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use tracing_subscriber::{filter::Targets, prelude::*};

use dolos::adapters::{ChainConfig, DomainAdapter, WalAdapter};
use dolos::core::Genesis;
use dolos::prelude::*;

use crate::{GenesisConfig, LoggingConfig};

pub struct Stores {
    pub wal: WalAdapter,
    pub state: dolos_redb3::state::StateStore,
    pub archive: dolos_redb3::archive::ArchiveStore,
}

pub fn ensure_storage_path(config: &crate::Config) -> Result<PathBuf, Error> {
    let root = config.storage.path.as_ref().ok_or(Error::config(
        "can't define storage path for ephemeral config",
    ))?;

    std::fs::create_dir_all(root)?;

    Ok(root.to_path_buf())
}

pub fn open_wal_store(config: &crate::Config) -> Result<WalAdapter, Error> {
    let root = ensure_storage_path(config)?;

    let wal = dolos_redb::wal::RedbWalStore::open(root.join("wal"), config.storage.wal_cache)?;

    Ok(wal)
}

pub fn open_archive_store(
    config: &crate::Config,
) -> Result<dolos_redb3::archive::ArchiveStore, Error> {
    let root = ensure_storage_path(config)?;
    let schema = dolos_cardano::model::build_schema();

    let archive = dolos_redb3::archive::ArchiveStore::open(
        schema,
        root.join("chain"),
        config.storage.chain_cache,
    )
    .map_err(ArchiveError::from)?;

    Ok(archive)
}

pub fn open_state_store(config: &crate::Config) -> Result<dolos_redb3::state::StateStore, Error> {
    let root = ensure_storage_path(config)?;
    let schema = dolos_cardano::model::build_schema();

    let state3 = dolos_redb3::state::StateStore::open(
        schema,
        root.join("state"),
        config.storage.ledger_cache,
    )
    .map_err(StateError::from)?;

    Ok(state3)
}

pub fn open_persistent_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    if config.storage.version == StorageVersion::V0 {
        error!("Storage should be removed and init procedure run again.");
        return Err(Error::StorageError("Invalid store version".to_string()));
    }

    let wal = open_wal_store(config)?;

    let state = open_state_store(config)?;

    let archive = open_archive_store(config)?;

    Ok(Stores {
        wal,
        state,
        archive,
    })
}

pub fn create_ephemeral_data_stores() -> Result<Stores, Error> {
    let wal = dolos_redb::wal::RedbWalStore::memory()?;

    let schema = dolos_cardano::model::build_schema();
    let state =
        dolos_redb3::state::StateStore::in_memory(schema.clone()).map_err(StateError::from)?;

    let archive =
        dolos_redb3::archive::ArchiveStore::in_memory(schema).map_err(ArchiveError::from)?;

    Ok(Stores {
        wal,
        archive,
        state,
    })
}

pub fn setup_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    if config.storage.is_ephemeral() {
        create_ephemeral_data_stores()
    } else {
        open_persistent_data_stores(config)
    }
}

pub fn setup_domain(config: &crate::Config) -> miette::Result<DomainAdapter> {
    let stores = setup_data_stores(config)?;
    let genesis = Arc::new(open_genesis_files(&config.genesis)?);
    let mempool = dolos::mempool::Mempool::new();
    let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);
    let chain = config.chain.clone();

    let ChainConfig::Cardano(chain_config) = chain;

    let chain =
        dolos_cardano::CardanoLogic::initialize::<DomainAdapter>(chain_config, &stores.state)
            .into_diagnostic()?;

    let domain = DomainAdapter {
        storage_config: Arc::new(config.storage.clone()),
        genesis,
        chain,
        wal: stores.wal,
        state: stores.state,
        archive: stores.archive,
        mempool,
        tip_broadcast,
    };

    // this will make sure the domain is correctly initialized and in a valid state.
    dolos_core::facade::bootstrap(&domain).map_err(|x| miette::miette!("{:?}", x))?;

    Ok(domain)
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
    Genesis::from_file_paths(
        &config.byron_path,
        &config.shelley_path,
        &config.alonzo_path,
        &config.conway_path,
        config.force_protocol,
    )
    .into_diagnostic()
    .context("loading genesis files")
}

#[inline]
#[cfg(unix)]
async fn wait_for_exit_signal() {
    let mut sigterm =
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::warn!("SIGINT detected");
        }
        _ = sigterm.recv() => {
            tracing::warn!("SIGTERM detected");
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
            _ = tokio::time::sleep(Duration::from_secs(5)) => {
                if pipeline.should_stop() {
                    debug!("pipeline should stop");

                    // trigger cancel so that stages stop early
                    exit.cancel();
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
