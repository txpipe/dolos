use dolos_core::config::{ChainConfig, GenesisConfig, LoggingConfig, RootConfig};
use dolos_core::BootstrapExt;
use miette::{Context as _, IntoDiagnostic};
use std::sync::Arc;
use std::{fs, path::PathBuf, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info};
use tracing_subscriber::{filter::Targets, prelude::*};

use dolos::adapters::DomainAdapter;
use dolos::core::Genesis;
use dolos::prelude::*;
use dolos::storage;

pub type Stores = storage::Stores<dolos_cardano::CardanoDelta>;

/// Ensure the storage root directory exists.
pub fn ensure_storage_path(config: &RootConfig) -> Result<PathBuf, Error> {
    storage::ensure_storage_path(config)
}

pub fn open_wal_store(
    config: &RootConfig,
) -> Result<storage::WalStoreBackend<dolos_cardano::CardanoDelta>, Error> {
    storage::open_wal_store(config)
}

pub fn open_archive_store(config: &RootConfig) -> Result<storage::ArchiveStoreBackend, Error> {
    storage::open_archive_store(config)
}

pub fn open_index_store(config: &RootConfig) -> Result<storage::IndexStoreBackend, Error> {
    storage::open_index_store(config)
}

pub fn open_state_store(config: &RootConfig) -> Result<storage::StateStoreBackend, Error> {
    storage::open_state_store(config)
}

pub fn open_data_stores(config: &RootConfig) -> Result<Stores, Error> {
    storage::open_data_stores(config)
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
    let stores = open_data_stores(config)?;
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

pub fn setup_tracing_error_only() -> miette::Result<()> {
    let filter = Targets::new().with_default(tracing::Level::ERROR);

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter)
        .init();

    tracing_log::LogTracer::init().ok();

    Ok(())
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
    let root = &config.storage.path;

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
