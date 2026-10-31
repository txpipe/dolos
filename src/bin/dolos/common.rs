use dolos_cardano::networks::{KnownNetwork, ResolvedNetwork};
use dolos_core::config::{
    ConfigError, LoggingConfig, RootConfig, TelemetryConfig, DEFAULT_CONFIG_FILE,
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use miette::{Context as _, IntoDiagnostic};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use std::sync::Arc;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};
use tracing_subscriber::{filter::Targets, prelude::*};

use dolos::adapters::DomainAdapter;
use dolos::core::Genesis;
use dolos::engine::{DomainBuildError, DomainBuilder};
use dolos::prelude::*;
use dolos::storage;
use dolos::sync::SyncLimit;

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

pub fn open_state_store(config: &RootConfig) -> Result<storage::StateStoreBackend, Error> {
    storage::open_state_store(config)
}

pub fn open_data_stores(config: &RootConfig) -> Result<Stores, Error> {
    storage::open_data_stores(config)
}

/// The dataset's stores as they are on disk, creating, bootstrapping and
/// catching up nothing; see [`storage::open_existing_stores`].
#[allow(
    dead_code,
    reason = "the first commands ported to it land with the sync and data groups"
)]
pub fn open_existing_stores(
    config: &RootConfig,
    access: storage::Access,
) -> Result<Stores, storage::OpenError> {
    storage::open_existing_stores(config, access)
}

/// The configuration file a command reads and `init` writes: `--config`, else
/// `./dolos.toml`.
pub fn config_file(explicit: Option<&Path>) -> &Path {
    explicit.unwrap_or(Path::new(DEFAULT_CONFIG_FILE))
}

/// Load the configuration, resolve its network against the catalog and parse
/// its log filter.
pub fn load_config(explicit_file: Option<&Path>) -> Result<RootConfig, ConfigError> {
    dolos_core::config::load_checked(explicit_file, |config| {
        ResolvedNetwork::resolve(&config.chain)
            .map_err(|error| ConfigError::invalid(["chain.network"], error.to_string()))?;

        log_filter(&config.logging).map(|_| ())
    })
}

/// Parses `--known-network` / `--network` against the catalog.
pub fn parse_known_network(name: &str) -> Result<&'static KnownNetwork, String> {
    KnownNetwork::by_name(name).ok_or_else(|| format!("unknown network {name}"))
}

/// The configuration a command runs on. A custom network's genesis is checked
/// before any command that could touch a store, since several of them clear
/// data before they need it.
pub fn load_and_check_config(config_file: &Path) -> miette::Result<RootConfig> {
    load_config(Some(config_file))
        .map_err(miette::Report::from_err)
        .and_then(|config| {
            check_network(&config)?;
            Ok(config)
        })
        .context("loading configuration")
}

pub fn resolve_network(config: &RootConfig) -> miette::Result<ResolvedNetwork<'_>> {
    ResolvedNetwork::resolve(&config.chain).into_diagnostic()
}

/// Reads a custom network's genesis files and checks them against its magic,
/// so that a command stops before it touches a store. A known network's
/// genesis is built in.
pub fn check_network(config: &RootConfig) -> miette::Result<()> {
    if resolve_network(config)?.known().is_none() {
        open_genesis(config)?;
    }

    Ok(())
}

/// The network's genesis: embedded for a known network, read from the
/// configured files for a custom one.
pub fn open_genesis(config: &RootConfig) -> miette::Result<Genesis> {
    resolve_network(config)?
        .genesis()
        .into_diagnostic()
        .context("loading genesis")
}

/// When `daemon` and `sync` stop.
#[derive(Debug, Clone, Default, clap::Args)]
pub struct StopArgs {
    /// Stop once the ledger reaches this epoch, on its first block: the only
    /// state a Stelae snapshot of that epoch is cut from
    #[arg(long, value_name = "N")]
    pub stop_epoch: Option<u64>,

    /// Stop pulling once the upstream tip is reached
    #[arg(long, conflicts_with = "max_blocks")]
    pub until_tip: bool,

    /// Stop pulling after this many blocks
    #[arg(long, value_name = "N")]
    pub max_blocks: Option<u64>,
}

impl StopArgs {
    pub fn sync_limit(&self) -> SyncLimit {
        match (self.until_tip, self.max_blocks) {
            (true, _) => SyncLimit::UntilTip,
            (false, Some(blocks)) => SyncLimit::MaxBlocks(blocks),
            (false, None) => SyncLimit::NoLimit,
        }
    }
}

pub fn setup_domain(config: &RootConfig) -> miette::Result<DomainAdapter> {
    setup_domain_with_stop_epoch(config, None)
}

/// The same domain [`setup_domain`] assembles, stopping once the ledger
/// reaches `stop_epoch`.
pub fn setup_domain_with_stop_epoch(
    config: &RootConfig,
    stop_epoch: Option<u64>,
) -> miette::Result<DomainAdapter> {
    let genesis = Arc::new(open_genesis(config)?);

    DomainBuilder::new(config, genesis)
        .stop_epoch(stop_epoch)
        .build()
        .map_err(render_domain_build_error)
}

/// Keep actionable executable diagnostics at the UI boundary while the
/// library returns typed construction errors.
fn render_domain_build_error(error: DomainBuildError) -> miette::Report {
    match error {
        DomainBuildError::Storage(error @ Error::InUse { .. }) => {
            miette::Report::new(storage::OpenError::from(error))
        }
        DomainBuildError::Storage(Error::WalError(WalError::IncompatibleVersion {
            found,
            expected,
        })) => miette::miette!(
            help = format!(
                "WAL was created by a newer dolos version (v{found}) than this binary supports (v{expected}); upgrade dolos or run `dolos bootstrap --force` to wipe storage and re-bootstrap",
            ),
            "incompatible WAL version: found v{found}, expected v{expected}",
        ),
        DomainBuildError::Bootstrap(dolos_core::DomainError::InconsistentState {
            ref wal,
            ref state,
        }) => {
            let msg = match (wal, state) {
                (Some(w), Some(s)) => format!(
                    "state (slot {}) is ahead of WAL (slot {})",
                    s.slot(),
                    w.slot()
                ),
                (None, Some(s)) => format!("WAL is empty but state exists at slot {}", s.slot()),
                (Some(w), None) => format!("WAL at slot {} but state has no cursor", w.slot()),
                (None, None) => "WAL and state are both missing".into(),
            };
            let help: &str = match (wal, state) {
                (_, Some(_)) => {
                    "run `dolos doctor reset-wal` to rebuild the WAL from the current state"
                }
                _ => "storage may be corrupted; consider re-bootstrapping with `dolos bootstrap`",
            };
            miette::miette!(help = help, "{msg}")
        }
        other => miette::miette!("{other}"),
    }
}

pub fn setup_tracing_error_only() -> miette::Result<()> {
    let filter = Targets::new().with_default(tracing::Level::ERROR);

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(filter)
        .init();

    tracing_log::LogTracer::init().ok();

    Ok(())
}

/// Warnings and errors only, on stderr, apart from a command's own output.
pub fn setup_tracing_warnings() -> miette::Result<()> {
    let filter = Targets::new().with_default(tracing::Level::WARN);

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(filter)
        .init();

    tracing_log::LogTracer::init().ok();

    Ok(())
}

/// Dolos's own targets at `level`, then the `filter` directives over them.
fn log_filter(config: &LoggingConfig) -> Result<Targets, ConfigError> {
    let mut filter = Targets::new()
        .with_target("dolos", config.level)
        .with_target("gasket", config.level);

    if let Some(directives) = &config.filter {
        let extra: Targets = directives.parse().map_err(|error| {
            ConfigError::invalid(
                ["logging.filter"],
                format!("`logging.filter = \"{directives}\"`: {error}"),
            )
        })?;

        if let Some(default) = extra.default_level() {
            filter = filter.with_default(default);
        }

        for (target, level) in extra.iter() {
            filter = filter.with_target(target, level);
        }
    }

    Ok(filter)
}

/// Logs go to stderr, which leaves stdout to a command's result.
pub fn setup_tracing(
    config: &LoggingConfig,
    telemetry: Option<&TelemetryConfig>,
) -> miette::Result<()> {
    let filter = log_filter(config).into_diagnostic()?;

    let otel_layer = if let Some(telemetry) = telemetry {
        let exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&telemetry.otlp_endpoint)
            .build()
            .into_diagnostic()
            .context("building OTLP span exporter")?;

        let tracer = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(
                opentelemetry_sdk::Resource::builder()
                    .with_service_name(telemetry.service_name.clone())
                    .build(),
            )
            .build();

        opentelemetry::global::set_tracer_provider(tracer.clone());

        let layer = tracing_opentelemetry::layer().with_tracer(tracer.tracer("dolos"));
        Some(layer)
    } else {
        None
    };

    #[cfg(not(feature = "debug"))]
    {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(otel_layer)
            .with(filter)
            .init();
    }

    #[cfg(feature = "debug")]
    {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(console_subscriber::spawn())
            .with(otel_layer)
            .with(filter)
            .init();
    }

    // Initialize the log-to-tracing bridge AFTER the tracing subscriber is set up.
    // This allows crates using the `log` crate (like fjall) to have their messages
    // forwarded to the tracing subscriber.
    tracing_log::LogTracer::init().ok();

    Ok(())
}

#[inline]
#[cfg(unix)]
pub(crate) async fn wait_for_exit_signal() {
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
pub(crate) async fn wait_for_exit_signal() {
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

/// Drains the serving drivers, cancelling the rest once any one of them fails.
///
/// Returns the first failure instead of only logging it: the exit status is
/// what a supervisor reads. Draining continues past that failure because
/// cancellation is what winds the healthy drivers down and nothing else
/// awaits them.
pub async fn monitor_drivers(
    mut drivers: FuturesUnordered<JoinHandle<Result<(), ServeError>>>,
    exit: CancellationToken,
) -> Result<(), ServeError> {
    let mut first_failure = None;

    while let Some(result) = drivers.next().await {
        let failure = match result {
            Ok(Ok(())) => continue,
            Ok(Err(e)) => {
                error!(error = %e, "driver failed");
                e
            }
            Err(e) => {
                error!(error = %e, "driver task failed");
                ServeError::Internal(Box::new(e))
            }
        };

        warn!("cancelling remaining drivers");
        exit.cancel();

        first_failure.get_or_insert(failure);
    }

    first_failure.map_or(Ok(()), Err)
}

/// Serializes the tests that set process environment variables.
#[cfg(test)]
pub static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use futures_util::stream::FuturesUnordered;
    use std::io;
    use tokio::task::JoinHandle;

    use super::*;

    #[test]
    fn the_log_filter_adds_targets_over_the_level() {
        let config = LoggingConfig {
            level: tracing::Level::WARN,
            filter: Some("dolos=debug,fjall=info".into()),
        };

        let filter = log_filter(&config).unwrap();

        assert!(filter.would_enable("dolos", &tracing::Level::DEBUG));
        assert!(filter.would_enable("gasket", &tracing::Level::WARN));
        assert!(!filter.would_enable("gasket", &tracing::Level::INFO));
        assert!(filter.would_enable("fjall", &tracing::Level::INFO));
        assert!(!filter.would_enable("tonic", &tracing::Level::ERROR));

        let config = LoggingConfig {
            filter: Some("dolos=loud".into()),
            ..Default::default()
        };
        assert!(log_filter(&config).is_err());
    }

    #[tokio::test]
    async fn monitor_drivers_observes_a_failure_a_healthy_driver_would_hide() {
        let exit = CancellationToken::new();
        let drivers: FuturesUnordered<JoinHandle<Result<(), ServeError>>> = FuturesUnordered::new();

        // `FuturesUnordered` links at the head, so the ordered drain this
        // replaced reached the last push first.
        drivers.push(tokio::spawn(async {
            Err(ServeError::BindError(io::Error::new(
                io::ErrorKind::AddrInUse,
                "socket already exists",
            )))
        }));

        let exit_for_healthy_driver = exit.clone();
        drivers.push(tokio::spawn(async move {
            exit_for_healthy_driver.cancelled().await;
            Ok(())
        }));

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            monitor_drivers(drivers, exit.clone()),
        )
        .await
        .expect("driver monitor should observe the bind failure promptly");

        assert!(exit.is_cancelled());

        assert!(
            matches!(result, Err(ServeError::BindError(_))),
            "the bind failure has to reach the caller, not just the log",
        );
    }

    #[tokio::test]
    async fn monitor_drivers_propagates_a_panicking_driver() {
        let exit = CancellationToken::new();
        let drivers: FuturesUnordered<JoinHandle<Result<(), ServeError>>> = FuturesUnordered::new();

        drivers.push(tokio::spawn(async { panic!("driver panicked") }));

        let result = monitor_drivers(drivers, exit.clone()).await;

        assert!(exit.is_cancelled());

        assert!(
            result.is_err(),
            "a panicking driver must not report success",
        );
    }

    #[tokio::test]
    async fn monitor_drivers_reports_a_clean_shutdown_as_success() {
        let exit = CancellationToken::new();
        let drivers: FuturesUnordered<JoinHandle<Result<(), ServeError>>> = FuturesUnordered::new();

        for _ in 0..3 {
            let exit_for_driver = exit.clone();
            drivers.push(tokio::spawn(async move {
                exit_for_driver.cancelled().await;
                Ok(())
            }));
        }

        exit.cancel();

        let result = tokio::time::timeout(
            Duration::from_secs(1),
            monitor_drivers(drivers, exit.clone()),
        )
        .await
        .expect("cancelled drivers should finish promptly");

        assert!(result.is_ok(), "a signalled shutdown is not a failure");
    }
}
