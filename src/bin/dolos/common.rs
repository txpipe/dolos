use dolos_core::config::{ChainConfig, GenesisConfig, LoggingConfig, RootConfig, TelemetryConfig};
use dolos_core::BootstrapExt;
use futures_util::{stream::FuturesUnordered, StreamExt};
use miette::{Context as _, IntoDiagnostic};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
use std::sync::Arc;
use std::{path::PathBuf, time::Duration};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
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
    setup_domain_with_stop_epoch(config, None)
}

/// The same domain [`setup_domain`] assembles, with `chain.stop_epoch` forced.
///
/// For callers that replay to a chosen epoch boundary over the live stores —
/// `snapshot backfill` — the way `doctor rebuild-state` forces it on the
/// domain it hand-builds. A `Some` here overrides whatever the configuration
/// says; `None` leaves it alone.
pub fn setup_domain_with_stop_epoch(
    config: &RootConfig,
    stop_epoch: Option<u64>,
) -> miette::Result<DomainAdapter> {
    let stores = open_data_stores(config).map_err(|e| match e {
        Error::WalError(WalError::IncompatibleVersion { found, expected }) => miette::miette!(
            help = format!(
                "WAL was created by a newer dolos version (v{found}) than this binary supports (v{expected}); upgrade dolos or run `dolos bootstrap --force` to wipe storage and re-bootstrap",
            ),
            "incompatible WAL version: found v{found}, expected v{expected}",
        ),
        other => miette::miette!("{other}"),
    })?;
    let genesis = Arc::new(open_genesis_files(&config.genesis)?);
    let mempool = stores.mempool.clone();
    let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);
    let chain = config.chain.clone();

    let ChainConfig::Cardano(mut chain_config) = chain;

    if stop_epoch.is_some() {
        chain_config.stop_epoch = stop_epoch;
    }

    let chain = dolos_cardano::CardanoLogic::initialize::<DomainAdapter>(
        chain_config,
        &stores.state,
        &genesis,
    )
    .into_diagnostic()?;

    let domain = DomainAdapter {
        storage_config: Arc::new(config.storage.clone()),
        sync_config: Arc::new(config.sync.clone()),
        genesis,
        chain: Arc::new(std::sync::RwLock::new(chain)),
        wal: stores.wal,
        state: stores.state,
        archive: stores.archive,
        mempool,
        tip_broadcast,
    };

    // this will make sure the domain is correctly initialized and in a valid state.
    domain.bootstrap().map_err(|e| match e {
        dolos_core::DomainError::InconsistentState { ref wal, ref state } => {
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
        other => miette::miette!("{other:?}"),
    })?;

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

pub fn setup_tracing(config: &LoggingConfig, telemetry: &TelemetryConfig) -> miette::Result<()> {
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
        filter = filter.with_target("minibf", level);
    }

    if config.include_minikupo {
        filter = filter.with_target("minikupo", level);
    }

    if config.include_fjall {
        filter = filter
            .with_target("fjall", level)
            .with_target("lsm_tree", level);
    }

    if config.include_otlp {
        filter = filter.with_target("opentelemetry", level);
    }

    let otel_layer = if telemetry.enabled {
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
            .with(tracing_subscriber::fmt::layer())
            .with(otel_layer)
            .with(filter)
            .init();
    }

    #[cfg(feature = "debug")]
    {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer())
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

/// Clear the storage an accepted upgrade replaces.
///
/// The wipe is [`storage::clear_storage`]'s recursive removal of
/// `storage.path`, and it only runs when that is the whole of the old data:
/// a store configured outside the root would survive it, and a config
/// recorded at the new version over a surviving store is the mismatch the
/// version gate exists to refuse. So a store outside the root, or a symlink
/// anywhere under it, aborts before anything is deleted and names what to
/// remove by hand. A root that does not exist is nothing to clear.
pub fn cleanup_data(config: &RootConfig) -> miette::Result<()> {
    let root = &config.storage.path;

    let outside = storage::stores_outside_root(&config.storage).into_diagnostic()?;

    if !outside.is_empty() {
        let listed = outside
            .iter()
            .map(|path| format!("  {}", path.display()))
            .collect::<Vec<_>>()
            .join("\n");

        miette::bail!(
            "the configuration keeps stores outside the storage root {}, so clearing the \
             root would leave them behind:\n{listed}\nRemove the storage root and each of \
             those directories by hand, then run `dolos init` again",
            root.display(),
        );
    }

    let links = storage::symlinks_under(root).into_diagnostic()?;

    if !links.is_empty() {
        let listed = links
            .iter()
            .map(|path| format!("  {}", path.display()))
            .collect::<Vec<_>>()
            .join("\n");

        miette::bail!(
            "the storage root {} holds symbolic links, and clearing it would not reach \
             what they point at:\n{listed}\nRemove the storage root and the linked \
             directories by hand, then run `dolos init` again",
            root.display(),
        );
    }

    if !root.exists() {
        info!(path = %root.display(), "no storage to clear");
        return Ok(());
    }

    storage::clear_storage(root).into_diagnostic()
}

#[cfg(test)]
mod tests {
    use dolos_core::config::StelaeConfig;
    use dolos_snapshot::{node::registry_auth, registry::Auth};
    use futures_util::stream::FuturesUnordered;
    use std::io;
    use tokio::task::JoinHandle;

    use super::*;

    /// The environment reaches `[stelae.registry]` by the same route as every
    /// other setting, and *this* is the assertion that says so.
    ///
    /// It stays here, beside [`load_config`], while the credential policy it
    /// feeds is [`dolos_snapshot::node::registry_auth`]'s: what is being pinned
    /// is this binary's configuration sources, not what the profile crate makes
    /// of them. It is the whole of Dolos's registry-credential environment
    /// story — a publisher exports `DOLOS_STELAE_REGISTRY_USER` and
    /// `DOLOS_STELAE_REGISTRY_PASSWORD` and nothing in this binary reads them —
    /// so it is worth pinning rather than trusting. The source is built exactly
    /// as [`load_config`] builds it; only the file layers are left off, because
    /// those would make the test depend on the working directory.
    ///
    /// What would break it is a rename of either field or a change to the
    /// prefix or separator, and all three are silent failures at run time: the
    /// override would simply stop applying, and a publisher would authenticate
    /// as the read-only user.
    #[test]
    fn the_dolos_environment_prefix_reaches_the_registry_section() {
        #[derive(serde::Deserialize)]
        struct Root {
            stelae: StelaeConfig,
        }

        // Process-wide, so this test owns these three names for its duration.
        // Nothing else in this binary reads them.
        static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = ENV.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        let names = [
            "DOLOS_STELAE_REGISTRY_USER",
            "DOLOS_STELAE_REGISTRY_PASSWORD",
            "DOLOS_STELAE_REGISTRY_TOKEN",
        ];

        let previous: Vec<Option<String>> = names.iter().map(|n| std::env::var(n).ok()).collect();

        std::env::set_var("DOLOS_STELAE_REGISTRY_USER", "publisher");
        std::env::set_var("DOLOS_STELAE_REGISTRY_PASSWORD", "full-access");
        std::env::remove_var("DOLOS_STELAE_REGISTRY_TOKEN");

        let built: Root = ::config::Config::builder()
            .add_source(::config::Environment::with_prefix("DOLOS").separator("_"))
            .build()
            .expect("the environment source builds")
            .try_deserialize()
            .expect("DOLOS_STELAE_REGISTRY_* deserializes into [stelae.registry]");

        for (name, value) in names.iter().zip(previous) {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }

        assert_eq!(
            registry_auth(&built.stelae).unwrap(),
            Auth::Basic {
                user: "publisher".to_owned(),
                password: "full-access".to_owned(),
            },
            "the DOLOS_ prefix no longer reaches [stelae.registry]",
        );
    }

    /// A configuration over `root` with every store on its disk backend at
    /// its default path, so the root is all the data there is.
    fn config_over(root: &std::path::Path) -> RootConfig {
        let toml = format!(
            r#"
            [upstream]
            peer_address = "unused.example:3001"

            [storage]
            version = "v4"
            path = {path}

            [storage.mempool]
            backend = "redb"

            [genesis]
            byron_path = "byron.json"
            shelley_path = "shelley.json"
            alonzo_path = "alonzo.json"
            conway_path = "conway.json"

            [chain]
            type = "cardano"
            magic = 2
            is_testnet = true
            "#,
            path = toml::Value::String(root.display().to_string()),
        );

        toml::from_str(&toml).unwrap()
    }

    /// The directory-based stores with files nested inside them, a block
    /// segment, and a restore's progress file: what a wipe has to take.
    fn old_store_under(root: &std::path::Path) -> Vec<PathBuf> {
        let files = vec![
            root.join("wal").join("journal"),
            root.join("state").join("index").join("000001.sst"),
            root.join("archive").join("index").join("000001.sst"),
            root.join("archive").join("000000.segment"),
            dolos_snapshot::restore::progress_path_in(root),
        ];

        for file in &files {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"a store built by v1.6").unwrap();
        }

        files
    }

    /// The stores are directories, not files under the root, and the wipe
    /// takes them whole — the restore progress file with them — and leaves
    /// an empty root for the bootstrap that follows.
    #[test]
    fn cleanup_clears_the_nested_stores_and_the_restore_progress() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let files = old_store_under(&root);

        cleanup_data(&config_over(&root)).unwrap();

        assert!(!files.iter().any(|file| file.exists()));
        assert!(root.is_dir());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
    }

    /// A root that is not there is nothing to clear, not a failure.
    #[test]
    fn cleanup_of_a_missing_root_is_nothing_to_do() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("never-created");

        cleanup_data(&config_over(&root)).unwrap();

        assert!(!root.exists(), "a root that was not there is not created");
    }

    /// A store configured outside the root would survive a wipe of the root,
    /// so the cleanup refuses before deleting anything and names it.
    #[test]
    fn cleanup_refuses_a_store_outside_the_root_before_deleting_anything() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let blocks = dir.path().join("blocks");
        let files = old_store_under(&root);

        std::fs::create_dir_all(&blocks).unwrap();
        std::fs::write(blocks.join("000000.segment"), b"a store built by v1.6").unwrap();

        let mut config = config_over(&root);
        let dolos_core::config::ArchiveStoreConfig::Fjall(archive) = &mut config.storage.archive
        else {
            panic!("the default archive backend is fjall");
        };
        archive.blocks_path = Some(blocks.clone());

        let error =
            cleanup_data(&config).expect_err("a store outside the root refuses the cleanup");

        let message = error.to_string();
        assert!(message.contains(&blocks.display().to_string()), "{message}");
        assert!(message.contains("by hand"), "{message}");
        assert!(files.iter().all(|file| file.is_file()));
        assert!(blocks.join("000000.segment").is_file());
    }

    /// A symlink under the root points at data the wipe would not reach, so
    /// the cleanup refuses before deleting anything and names the link.
    #[cfg(unix)]
    #[test]
    fn cleanup_refuses_a_root_with_symlinks_before_deleting_anything() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        let elsewhere = dir.path().join("elsewhere");
        let files = old_store_under(&root);

        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("000000.segment"), b"a store built by v1.6").unwrap();
        std::os::unix::fs::symlink(&elsewhere, root.join("blocks")).unwrap();

        let error = cleanup_data(&config_over(&root))
            .expect_err("a symlink under the root refuses the cleanup");

        let message = error.to_string();
        assert!(
            message.contains(&root.join("blocks").display().to_string()),
            "{message}"
        );
        assert!(message.contains("by hand"), "{message}");
        assert!(files.iter().all(|file| file.is_file()));
        assert!(root.join("blocks").is_symlink());
        assert!(elsewhere.join("000000.segment").is_file());
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
