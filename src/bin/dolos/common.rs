use dolos_core::config::{
    ChainConfig, GenesisConfig, LoggingConfig, RootConfig, StelaeConfig, StorageConfig,
    TelemetryConfig,
};
use dolos_core::BootstrapExt;
use dolos_snapshot::registry::Auth;
use miette::{Context as _, IntoDiagnostic};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig as _;
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

/// Who this node authenticates to a stele registry as.
///
/// A pure function of `[stelae.registry]`, and deliberately nothing more.
/// **Dolos reads no environment variable of its own here**, because it does not
/// have to: `load_config` layers `config::Environment` with the `DOLOS` prefix
/// over every setting, so `DOLOS_STELAE_REGISTRY_USER` and
/// `DOLOS_STELAE_REGISTRY_PASSWORD` already override what the file says, by the
/// same mechanism and with the same precedence as every other field. A second,
/// hand-rolled set of variables would be a second answer to a question the
/// configuration has already answered — and this binary has no other.
///
/// So the two sources the operator sees are the two the configuration has: a
/// consumer's published user in `dolos.toml`, and a publisher's real
/// credentials exported into the environment and never written down.
///
/// Two refusals, because both are operator mistakes worth a sentence rather
/// than a precedence rule:
///
/// - **a token and a user together** — two identities, and which was meant is
///   not something to guess at. On a registry whose credentials carry different
///   capabilities, guessing is the difference between a publish and a 403
///   nobody can explain.
/// - **a password with no user** — a secret that arrived with nobody to be. It
///   is a typo or half an export, and the half that arrived cannot be sent on
///   its own.
pub fn stele_registry_auth(config: &StelaeConfig) -> miette::Result<Auth> {
    let Some(registry) = &config.registry else {
        return Ok(Auth::Anonymous);
    };

    if registry.token.is_some() && registry.user.is_some() {
        miette::bail!(
            "[stelae.registry] sets both `token` and `user`; a registry client authenticates as \
             one identity and which one was meant is not something to guess at — drop the one \
             you did not mean, or unset DOLOS_STELAE_REGISTRY_TOKEN / DOLOS_STELAE_REGISTRY_USER"
        );
    }

    if let Some(token) = &registry.token {
        return Ok(Auth::Bearer(token.clone()));
    }

    match &registry.user {
        Some(user) => Ok(Auth::Basic {
            user: user.clone(),
            // A user and no password means the official registry's, which is
            // compiled in beside the rest of the hardcoded defaults rather than
            // written into every generated `dolos.toml`.
            password: registry
                .password
                .clone()
                .unwrap_or_else(|| crate::init::OFFICIAL_REGISTRY_PASSWORD.to_owned()),
        }),
        // A password with nobody to be. Anonymous would be the quiet answer and
        // the wrong one: the operator supplied a secret and it would go unused.
        None if registry.password.is_some() => miette::bail!(
            "[stelae.registry] sets `password` with no `user`; basic registry credentials are a \
             pair"
        ),
        None => Ok(Auth::Anonymous),
    }
}

/// The directory a registry transfer stages in when the operator names none.
///
/// A child of `storage.path` rather than a sibling of it: on a host where the
/// data lives on a dedicated mount, a sibling is on the *parent* filesystem —
/// the small root volume this default exists to keep bytes off.
pub const STELE_SCRATCH_DIR: &str = "scratch";

/// Where this node stages the layers of a registry transfer.
///
/// An explicit `--scratch-dir` is taken literally, relative paths included:
/// it is resolved against the working directory like every other path this
/// binary takes from a command line.
pub fn stele_scratch_dir(config: &StorageConfig, chosen: Option<&std::path::Path>) -> PathBuf {
    match chosen {
        Some(dir) => dir.to_path_buf(),
        None => config.path.join(STELE_SCRATCH_DIR),
    }
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
        indexes: stores.indexes,
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

/// Attempts a transient-prone external gets before its failure is fatal.
///
/// Bounded on purpose. The preprod G1 backfill measured why the retry exists —
/// four container exits between 06:39Z and 08:53Z on 2026-08-23, each one
/// paying a store restore, a window re-download and the in-flight epoch's
/// re-replay for what a half-minute wait would have absorbed — and the same
/// measurement is why it is not open-ended: a misconfigured aggregator or a
/// repository the credentials cannot read has to keep failing, and keep
/// failing while whoever launched the run is still watching.
const RETRY_ATTEMPTS: u32 = 4;

/// The first wait between attempts; each later one doubles it. Three waits of
/// 5s, 10s and 20s put the ceiling at 35 seconds of patience.
const RETRY_BASE_DELAY: Duration = Duration::from_secs(5);

/// Sleep, unless a shutdown is requested first. `false` means it was.
///
/// Sliced rather than slept in one call so a signal that arrives during a
/// backoff is honoured at the next slice instead of at the end of the wait —
/// the driver's whole shutdown budget is a container's SIGTERM grace period,
/// which a 20-second sleep would eat.
fn sleep_unless_aborted(delay: Duration, abort: &dyn Fn() -> bool) -> bool {
    const SLICE: Duration = Duration::from_millis(250);

    let mut left = delay;

    while !left.is_zero() {
        if abort() {
            return false;
        }

        let slice = left.min(SLICE);
        std::thread::sleep(slice);
        left -= slice;
    }

    !abort()
}

/// Run `op`, retrying a failure with exponential backoff, then let it be fatal.
///
/// The last attempt's error is returned untouched, so every caller keeps the
/// diagnostic it had before the retry was wrapped around it — the retry moves
/// where the fatal path is reached, never what it says. Nothing here decides a
/// failure is transient: the classification the alternative would need does not
/// exist at these seams (an aggregator's errors arrive as opaque strings), and
/// guessing it wrong reinstates exactly the fatal exits this is here to absorb.
/// What bounds the patience is [`RETRY_ATTEMPTS`], not a judgement about the
/// error.
///
/// `abort` is polled between and during the waits, so a shutdown ends the run
/// on the failure in hand rather than after the remaining backoff. Callers with
/// no shutdown to observe pass `&|| false`.
///
/// Only for operations that are safe to simply run again: reads, and downloads
/// whose destination is rewritten from the same arguments.
pub fn retry_transient<T, E, F>(what: &str, abort: &dyn Fn() -> bool, op: F) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    retry_bounded(what, RETRY_ATTEMPTS, RETRY_BASE_DELAY, abort, op)
}

/// [`retry_transient`] with its two constants spelled out, so the tests can
/// exercise the loop without waiting out a real backoff.
fn retry_bounded<T, E, F>(
    what: &str,
    attempts: u32,
    base_delay: Duration,
    abort: &dyn Fn() -> bool,
    mut op: F,
) -> Result<T, E>
where
    F: FnMut() -> Result<T, E>,
    E: std::fmt::Display,
{
    let mut delay = base_delay;

    for attempt in 1..attempts {
        match op() {
            Ok(value) => return Ok(value),
            Err(err) => {
                if abort() {
                    return Err(err);
                }

                tracing::warn!(
                    what,
                    attempt,
                    remaining = attempts - attempt,
                    backoff_secs = delay.as_secs(),
                    error = %err,
                    "transient failure; retrying",
                );

                if !sleep_unless_aborted(delay, abort) {
                    return Err(err);
                }

                delay = delay.saturating_mul(2);
            }
        }
    }

    op()
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

#[cfg(test)]
mod tests {
    use dolos_core::config::StelaeRegistryConfig;

    use super::*;
    use crate::init::OFFICIAL_REGISTRY_PASSWORD;

    /// The retry loop, with the real backoff replaced by none of it.
    fn retried<T, E: std::fmt::Display>(
        abort: &dyn Fn() -> bool,
        op: impl FnMut() -> Result<T, E>,
    ) -> Result<T, E> {
        super::retry_bounded("a test", super::RETRY_ATTEMPTS, Duration::ZERO, abort, op)
    }

    #[test]
    fn a_call_that_succeeds_is_made_once() {
        let mut calls = 0;

        let result: Result<u8, String> = retried(&|| false, || {
            calls += 1;
            Ok(7)
        });

        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls, 1, "a success must not be retried");
    }

    #[test]
    fn a_transient_failure_is_absorbed() {
        let mut calls = 0;

        let result: Result<u8, String> = retried(&|| false, || {
            calls += 1;

            if calls < 3 {
                Err("the aggregator hung up".to_owned())
            } else {
                Ok(7)
            }
        });

        assert_eq!(result.unwrap(), 7);
        assert_eq!(calls, 3, "the loop stops at the first success");
    }

    #[test]
    fn patience_is_bounded_and_the_last_error_is_the_one_raised() {
        let mut calls = 0;

        let result: Result<u8, String> = retried(&|| false, || {
            calls += 1;
            Err(format!("attempt {calls} failed"))
        });

        assert_eq!(
            calls, RETRY_ATTEMPTS as usize,
            "a persistent failure must still reach the fatal path",
        );

        assert_eq!(
            result.unwrap_err(),
            format!("attempt {RETRY_ATTEMPTS} failed"),
            "the caller keeps the diagnostic the final attempt produced",
        );
    }

    #[test]
    fn a_shutdown_ends_the_run_on_the_failure_in_hand() {
        let mut calls = 0;

        let result: Result<u8, String> = retried(&|| true, || {
            calls += 1;
            Err("interrupted".to_owned())
        });

        assert_eq!(calls, 1, "a requested shutdown is not waited out");
        assert_eq!(result.unwrap_err(), "interrupted");
    }

    #[test]
    fn a_shutdown_during_a_backoff_cuts_the_wait_short() {
        assert!(!sleep_unless_aborted(Duration::from_secs(60), &|| true));
        assert!(sleep_unless_aborted(Duration::ZERO, &|| false));
    }

    fn storage(path: &str) -> StorageConfig {
        let document = format!(
            "version = \"v3\"\npath = {}\n",
            toml::Value::String(path.to_owned()),
        );

        toml::from_str(&document).expect("a storage section with a path")
    }

    #[test]
    fn staging_defaults_inside_the_storage_path_and_takes_a_named_one_literally() {
        let config = storage("/var/lib/dolos/data");

        assert_eq!(
            stele_scratch_dir(&config, None),
            PathBuf::from("/var/lib/dolos/data/scratch"),
        );

        for named in ["/mnt/big/staging", "staging", "../staging"] {
            assert_eq!(
                stele_scratch_dir(&config, Some(std::path::Path::new(named))),
                PathBuf::from(named),
                "{named}",
            );
        }
    }

    fn config(registry: Option<StelaeRegistryConfig>) -> StelaeConfig {
        StelaeConfig { registry }
    }

    fn basic(user: &str, password: Option<&str>) -> StelaeRegistryConfig {
        StelaeRegistryConfig {
            user: Some(user.to_owned()),
            password: password.map(str::to_owned),
            token: None,
        }
    }

    /// The three shapes `[stelae.registry]` can name, and the one it names by
    /// saying nothing.
    #[test]
    fn the_section_names_a_user_a_token_or_nobody() {
        assert_eq!(stele_registry_auth(&config(None)).unwrap(), Auth::Anonymous);

        assert_eq!(
            stele_registry_auth(&config(Some(basic("dolos-reader", Some("published"))))).unwrap(),
            Auth::Basic {
                user: "dolos-reader".to_owned(),
                password: "published".to_owned(),
            }
        );

        let bearer = StelaeRegistryConfig {
            token: Some("ghp_x".to_owned()),
            ..Default::default()
        };

        assert_eq!(
            stele_registry_auth(&config(Some(bearer))).unwrap(),
            Auth::Bearer("ghp_x".to_owned())
        );
    }

    /// A user with no password is the shape `dolos init` seeds: the file says
    /// who, the binary says with what.
    #[test]
    fn a_seeded_user_takes_the_compiled_in_password() {
        assert_eq!(
            stele_registry_auth(&config(Some(basic("dolos-reader", None)))).unwrap(),
            Auth::Basic {
                user: "dolos-reader".to_owned(),
                password: OFFICIAL_REGISTRY_PASSWORD.to_owned(),
            }
        );
    }

    #[test]
    fn two_identities_at_once_are_refused() {
        let both = StelaeRegistryConfig {
            user: Some("dolos-reader".to_owned()),
            password: None,
            token: Some("ghp_x".to_owned()),
        };

        let message = stele_registry_auth(&config(Some(both)))
            .unwrap_err()
            .to_string();

        assert!(message.contains("token"), "{message}");
        assert!(message.contains("user"), "{message}");
    }

    /// A password with nobody to be. Anonymous would be the quiet answer and
    /// the wrong one: the operator supplied a secret and it would go unused.
    #[test]
    fn a_password_with_no_user_is_refused() {
        let orphan = StelaeRegistryConfig {
            password: Some("full-access".to_owned()),
            ..Default::default()
        };

        let message = stele_registry_auth(&config(Some(orphan)))
            .unwrap_err()
            .to_string();

        assert!(message.contains("password"), "{message}");
        assert!(message.contains("user"), "{message}");
    }

    /// The environment reaches this section by the same route as every other
    /// setting, and *this* is the assertion that says so.
    ///
    /// It is the whole of Dolos's registry-credential environment story — a
    /// publisher exports `DOLOS_STELAE_REGISTRY_USER` and
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
            stele_registry_auth(&built.stelae).unwrap(),
            Auth::Basic {
                user: "publisher".to_owned(),
                password: "full-access".to_owned(),
            },
            "the DOLOS_ prefix no longer reaches [stelae.registry]",
        );
    }
}
