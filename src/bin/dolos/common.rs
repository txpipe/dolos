use dolos_core::config::{
    ArchiveStoreConfig, ChainConfig, GenesisConfig, IndexStoreConfig, LoggingConfig, RootConfig,
    StateStoreConfig, StorageVersion, WalStoreConfig,
};
use dolos_core::BootstrapExt;
use miette::{Context as _, IntoDiagnostic};
use std::sync::Arc;
use std::{fs, path::PathBuf, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};
use tracing_subscriber::{filter::Targets, prelude::*};

use dolos::adapters::{DomainAdapter, WalAdapter};
use dolos::core::Genesis;
use dolos::prelude::*;
use dolos::storage::{IndexStoreBackend, StateStoreBackend};

pub struct Stores {
    pub wal: WalAdapter,
    pub state: StateStoreBackend,
    pub archive: dolos_redb3::archive::ArchiveStore,
    pub indexes: IndexStoreBackend,
}

pub fn ensure_storage_path(config: &RootConfig) -> Result<PathBuf, Error> {
    let root = config.storage.path.as_ref().ok_or(Error::config(
        "can't define storage path for ephemeral config",
    ))?;

    std::fs::create_dir_all(root)?;

    Ok(root.to_path_buf())
}

pub fn open_wal_store(config: &RootConfig) -> Result<WalAdapter, Error> {
    let root = ensure_storage_path(config)?;

    match &config.storage.wal {
        WalStoreConfig::Redb { cache, .. } => {
            let wal = dolos_redb3::wal::RedbWalStore::open(root.join("wal"), *cache)?;
            Ok(wal)
        }
    }
}

pub fn open_archive_store(
    config: &RootConfig,
) -> Result<dolos_redb3::archive::ArchiveStore, Error> {
    let root = ensure_storage_path(config)?;
    let schema = dolos_cardano::model::build_schema();

    match &config.storage.archive {
        ArchiveStoreConfig::Redb { cache, .. } => {
            let archive =
                dolos_redb3::archive::ArchiveStore::open(schema, root.join("chain"), *cache)
                    .map_err(ArchiveError::from)?;
            Ok(archive)
        }
    }
}

pub fn open_index_store(config: &RootConfig) -> Result<IndexStoreBackend, Error> {
    let root = ensure_storage_path(config)?;

    match &config.storage.index {
        IndexStoreConfig::Redb { cache } => {
            let store = dolos_redb3::indexes::IndexStore::open(root.join("index"), *cache)
                .map_err(IndexError::from)?;
            Ok(IndexStoreBackend::Redb(store))
        }
        IndexStoreConfig::Fjall {
            cache,
            max_journal_size,
            flush_on_commit,
        } => {
            let store = dolos_fjall::IndexStore::open(
                root.join("index"),
                *cache,
                *max_journal_size,
                *flush_on_commit,
            )
            .map_err(IndexError::from)?;
            Ok(IndexStoreBackend::Fjall(store))
        }
    }
}

pub fn open_state_store(config: &RootConfig) -> Result<StateStoreBackend, Error> {
    let root = ensure_storage_path(config)?;

    match &config.storage.state {
        StateStoreConfig::Redb { cache, .. } => {
            let schema = dolos_cardano::model::build_schema();
            let store = dolos_redb3::state::StateStore::open(schema, root.join("state"), *cache)
                .map_err(StateError::from)?;
            Ok(StateStoreBackend::Redb(store))
        }
        StateStoreConfig::Fjall {
            cache,
            max_journal_size,
            flush_on_commit,
            ..
        } => {
            // Fjall uses a unified entities keyspace with namespace hash prefixes,
            // so it doesn't need the schema to pre-create keyspaces
            let store = dolos_fjall::StateStore::open(
                root.join("state"),
                *cache,
                *max_journal_size,
                *flush_on_commit,
            )
            .map_err(StateError::from)?;
            Ok(StateStoreBackend::Fjall(store))
        }
    }
}

pub fn open_persistent_data_stores(config: &RootConfig) -> Result<Stores, Error> {
    match config.storage.version {
        StorageVersion::V0 => {
            error!("Storage version V0 is no longer supported. Please remove storage and run init procedure again.");
            return Err(Error::StorageError("Invalid store version V0".to_string()));
        }
        StorageVersion::V1 | StorageVersion::V2 => {
            error!("Storage version {:?} uses old config format. Please update your config to V3 format with nested storage sections.", config.storage.version);
            return Err(Error::StorageError(format!(
                "Storage version {:?} requires config migration to V3",
                config.storage.version
            )));
        }
        StorageVersion::V3 => {
            // Current version, proceed normally
        }
    }

    let wal = open_wal_store(config)?;

    let state = open_state_store(config)?;

    let archive = open_archive_store(config)?;

    let indexes = open_index_store(config)?;

    Ok(Stores {
        wal,
        state,
        archive,
        indexes,
    })
}

pub fn create_ephemeral_data_stores(config: &RootConfig) -> Result<Stores, Error> {
    // Fjall does not support in-memory mode
    if config.storage.state.is_fjall() {
        return Err(Error::config(
            "fjall backend does not support ephemeral storage for state store",
        ));
    }
    if config.storage.index.is_fjall() {
        return Err(Error::config(
            "fjall backend does not support ephemeral storage for index store",
        ));
    }
    // Note: archive only has Redb variant currently, so no need to check

    let wal = dolos_redb3::wal::RedbWalStore::memory()?;

    let schema = dolos_cardano::model::build_schema();
    let state =
        dolos_redb3::state::StateStore::in_memory(schema.clone()).map_err(StateError::from)?;

    let archive =
        dolos_redb3::archive::ArchiveStore::in_memory(schema).map_err(ArchiveError::from)?;

    let indexes = dolos_redb3::indexes::IndexStore::in_memory().map_err(IndexError::from)?;

    Ok(Stores {
        wal,
        archive,
        state: StateStoreBackend::Redb(state),
        indexes: IndexStoreBackend::Redb(indexes),
    })
}

pub fn setup_data_stores(config: &RootConfig) -> Result<Stores, Error> {
    if config.storage.is_ephemeral() {
        create_ephemeral_data_stores(config)
    } else {
        open_persistent_data_stores(config)
    }
}

pub fn load_config(
    explicit_file: &Option<std::path::PathBuf>,
) -> Result<RootConfig, ::config::ConfigError> {
    let mut s = ::config::Config::builder();

    // our base config will always be in /etc/dolos
    s = s.add_source(::config::File::with_name("/etc/dolos/daemon.toml").required(false));

    // but we can override it by having a file in the working dir
    s = s.add_source(::config::File::with_name("dolos.toml").required(false));

    // if an explicit file was passed, then we load it as mandatory
    if let Some(explicit) = explicit_file.as_ref().and_then(|x| x.to_str()) {
        s = s.add_source(::config::File::with_name(explicit).required(true));
    }

    // finally, we use env vars to make some last-step overrides
    s = s.add_source(::config::Environment::with_prefix("DOLOS").separator("_"));

    s.build()?.try_deserialize()
}

pub fn setup_domain(config: &RootConfig) -> miette::Result<DomainAdapter> {
    let stores = setup_data_stores(config)?;
    let genesis = Arc::new(open_genesis_files(&config.genesis)?);
    let mempool = dolos::mempool::Mempool::new();
    let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);
    let chain = config.chain.clone();

    let ChainConfig::Cardano(chain_config) = chain;

    let chain = dolos_cardano::CardanoLogic::initialize::<DomainAdapter>(
        chain_config,
        &stores.state,
        &genesis,
    )
    .into_diagnostic()?;

    let domain = DomainAdapter {
        storage_config: Arc::new(config.storage.clone()),
        genesis,
        chain: Arc::new(std::sync::RwLock::new(chain)),
        wal: stores.wal,
        state: stores.state,
        archive: stores.archive,
        indexes: stores.indexes,
        mempool,
        tip_broadcast,
    };

    // this will make sure the domain is correctly initialized and in a valid state.
    domain.bootstrap().map_err(|x| miette::miette!("{:?}", x))?;

    Ok(domain)
}

pub fn setup_tracing(config: &LoggingConfig) -> miette::Result<()> {
    let level = config.max_level;

    let mut filter = Targets::new()
        .with_target("dolos", level)
        .with_target("gasket", level)
        // Include fjall and lsm_tree for storage backend debugging
        .with_target("fjall", level)
        .with_target("lsm_tree", level);

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

    // Initialize the log-to-tracing bridge AFTER the tracing subscriber is set up.
    // This allows crates using the `log` crate (like fjall) to have their messages
    // forwarded to the tracing subscriber.
    tracing_log::LogTracer::init().ok();

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

pub fn cleanup_data(config: &RootConfig) -> Result<(), std::io::Error> {
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
