use miette::{Context as _, IntoDiagnostic};
use std::sync::Arc;
use std::{fs, path::PathBuf, time::Duration};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use tracing_subscriber::{filter::Targets, prelude::*};

use once_cell::sync::OnceCell;
use opentelemetry::{global, KeyValue};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{Protocol, WithExportConfig};
use opentelemetry_sdk::{logs as sdklog, Resource};

use dolos::adapters::{ArchiveAdapter, DomainAdapter, StateAdapter, WalAdapter};
use dolos::core::Genesis;
use dolos::prelude::*;

use crate::{Config, GenesisConfig};

pub struct Stores {
    pub wal: WalAdapter,
    pub state: StateAdapter,
    pub archive: ArchiveAdapter,

    pub state3: dolos_redb3::StateStore,
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

    Ok(WalAdapter::Redb(wal))
}

pub fn open_chain_store(config: &crate::Config) -> Result<ArchiveAdapter, Error> {
    let root = ensure_storage_path(config)?;

    let chain =
        dolos_redb::archive::ChainStore::open(root.join("chain"), config.storage.chain_cache)
            .map_err(ArchiveError::from)?;

    Ok(chain.into())
}

pub fn open_ledger_store(config: &crate::Config) -> Result<StateAdapter, Error> {
    let root = ensure_storage_path(config)?;

    let ledger =
        dolos_redb::state::LedgerStore::open(root.join("ledger"), config.storage.ledger_cache)
            .map_err(StateError::from)?;

    Ok(ledger.into())
}

pub fn open_state3_store(config: &crate::Config) -> Result<dolos_redb3::StateStore, Error> {
    let root = ensure_storage_path(config)?;
    let schema = dolos_cardano::model::build_schema();

    let state3 =
        dolos_redb3::StateStore::open(schema, root.join("state"), config.storage.ledger_cache)
            .map_err(State3Error::from)?;

    Ok(state3)
}

pub fn open_persistent_data_stores(config: &crate::Config) -> Result<Stores, Error> {
    if config.storage.version == StorageVersion::V0 {
        error!("Storage should be removed and init procedure run again.");
        return Err(Error::StorageError("Invalid store version".to_string()));
    }

    let wal = open_wal_store(config)?;

    let ledger = open_ledger_store(config)?;

    let state3 = open_state3_store(config)?;

    let chain = open_chain_store(config)?;

    Ok(Stores {
        wal,
        state: ledger,
        archive: chain,
        state3,
    })
}

pub fn create_ephemeral_data_stores() -> Result<Stores, Error> {
    let wal = dolos_redb::wal::RedbWalStore::memory()?;

    wal.initialize_from_origin().map_err(WalError::from)?;

    let ledger = dolos_redb::state::LedgerStore::in_memory_v2()?;

    let state3 = dolos_redb3::StateStore::in_memory(dolos_cardano::model::build_schema())
        .map_err(State3Error::from)?;

    let chain = dolos_redb::archive::ChainStore::in_memory_v1()?;

    Ok(Stores {
        wal: wal.into(),
        state: ledger.into(),
        archive: chain.into(),
        state3,
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
    let mempool = dolos::mempool::Mempool::new(genesis.clone(), stores.state.clone());

    let domain = DomainAdapter {
        storage_config: Arc::new(config.storage.clone()),
        genesis,
        wal: stores.wal,
        state: stores.state,
        archive: stores.archive,
        mempool,

        state3: stores.state3,
    };

    Ok(domain)
}

static LOGGER_PROVIDER: OnceCell<sdklog::SdkLoggerProvider> = OnceCell::new();
static METRICS_PROVIDER: OnceCell<opentelemetry_sdk::metrics::SdkMeterProvider> = OnceCell::new();

pub fn setup_tracing(config: &Config) -> miette::Result<()> {
    let fmt_layer = tracing_subscriber::fmt::layer();

    let level = config.logging.max_level;
    let mut filter = Targets::new()
        .with_target("dolos", level)
        .with_target("gasket", level);

    if config.logging.include_tokio {
        filter = filter
            .with_target("tokio", level)
            .with_target("runtime", level);
    }

    if config.logging.include_pallas {
        filter = filter.with_target("pallas", level);
    }

    if config.logging.include_grpc {
        filter = filter.with_target("tonic", level);
    }

    if config.logging.include_trp {
        filter = filter.with_target("jsonrpsee-server", level);
    }

    if config.logging.include_minibf {
        filter = filter.with_target("tower_http", level);
    }

    if let Some(telemetry_config) = config.telemetry.as_ref() {
        let resource = Resource::builder_empty()
            .with_service_name(telemetry_config.service_name.clone())
            .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
            .build();

        let exporter = opentelemetry_otlp::LogExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .with_endpoint(&telemetry_config.logs_endpoint)
            .with_timeout(Duration::from_secs(3))
            .build()
            .unwrap();

        let provider = sdklog::SdkLoggerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource.clone())
            .build();

        // keep provider alive for the whole program
        LOGGER_PROVIDER.set(provider.clone()).ok();

        let otel_layer = OpenTelemetryTracingBridge::new(&provider);

        let exporter = opentelemetry_otlp::MetricExporter::builder()
            .with_http()
            .with_protocol(Protocol::HttpJson)
            .with_timeout(Duration::from_secs(3))
            .with_endpoint(&telemetry_config.metrics_endpoint)
            .build()
            .expect("Failed to create metric exporter");

        let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
            .with_periodic_exporter(exporter)
            .with_resource(resource)
            .build();

        METRICS_PROVIDER.set(provider.clone()).ok();
        global::set_meter_provider(provider);

        #[cfg(not(feature = "debug"))]
        {
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(otel_layer)
                .with(filter)
                .init();
        }

        #[cfg(feature = "debug")]
        {
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(console_subscriber::spawn())
                .with(otel_layer)
                .with(filter)
                .init();
        }
    } else {
        // No telemetry, just use fmt layer
        #[cfg(not(feature = "debug"))]
        {
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(filter)
                .init();
        }

        #[cfg(feature = "debug")]
        {
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(console_subscriber::spawn())
                .with(filter)
                .init();
        }
    }

    Ok(())
}

pub fn shutdown_tracing() {
    if let Some(p) = LOGGER_PROVIDER.get() {
        let _ = p.shutdown();
    }
    if let Some(p) = METRICS_PROVIDER.get() {
        let _ = p.shutdown();
    }
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
