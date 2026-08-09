use dolos_core::config::{
    ChainConfig, GenesisConfig, LoggingConfig, RootConfig, StelaeConfig, TelemetryConfig,
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

/// Environment variable holding a bearer token for a stele registry.
///
/// These three are **this program's**, not the protocol's. `stelae` takes
/// credentials as a value and never sources them: a library that read an
/// environment variable would be choosing its host's credential policy, and
/// naming the variable would freeze that choice into a published API. So the
/// names live here, in the binary whose deployment they describe, and so does
/// the precedence between them.
pub const STELE_REGISTRY_TOKEN_ENV: &str = "STELAE_REGISTRY_TOKEN";

/// The user half of a Basic credential pair for a stele registry.
pub const STELE_REGISTRY_USER_ENV: &str = "STELAE_REGISTRY_USER";

/// The password half of a Basic credential pair for a stele registry.
pub const STELE_REGISTRY_PASSWORD_ENV: &str = "STELAE_REGISTRY_PASSWORD";

/// Who this node authenticates to a stele registry as.
///
/// Two sources, one rule. A publish takes its full-access pair from
/// [`STELE_REGISTRY_USER_ENV`] / [`STELE_REGISTRY_PASSWORD_ENV`] (or a token
/// from [`STELE_REGISTRY_TOKEN_ENV`]); a restore takes the published read-only
/// user from `[stelae.registry]` in `dolos.toml`. Those are two *sources*, not
/// two rules:
///
/// - **the environment wins.** A publisher's credentials are a secret and never
///   enter a configuration file, so the environment is the only place they can
///   come from — and a node that already carries the read-only user must not
///   have to be edited before it can publish.
/// - **what is configured is the fallback**, which is what lets a node created
///   by `dolos init` pull from the official registry with nothing exported.
/// - **neither is anonymous**, which is what a genuinely public repository
///   wants and what a credentialed one answers with a 401.
///
/// Two refusals, because both are operator mistakes worth a sentence rather
/// than a precedence rule: a token and a pair set together, and half a pair. An
/// operator who exported both meant one of them, and a client that guessed
/// would authenticate as an identity nobody chose — which on a registry whose
/// credentials carry different capabilities is the difference between a publish
/// and a 403 nobody can explain.
pub fn stele_registry_auth(config: &StelaeConfig) -> miette::Result<Auth> {
    let read = |name: &str| match std::env::var(name) {
        // An empty value is unset, so a stale `export STELAE_REGISTRY_TOKEN=`
        // in a shell profile leaves a node anonymous rather than
        // authenticating it as the empty token.
        Ok(value) if !value.is_empty() => Some(value),
        _ => None,
    };

    let token = read(STELE_REGISTRY_TOKEN_ENV);
    let user = read(STELE_REGISTRY_USER_ENV);
    let password = read(STELE_REGISTRY_PASSWORD_ENV);

    if token.is_some() && (user.is_some() || password.is_some()) {
        miette::bail!(
            "{STELE_REGISTRY_TOKEN_ENV} and \
             {STELE_REGISTRY_USER_ENV}/{STELE_REGISTRY_PASSWORD_ENV} are both set; registry \
             credentials come from one of the two and which one was meant is not something to \
             guess at — unset the one you did not mean"
        );
    }

    let half = |set: &str, missing: &str| {
        miette::miette!("{set} is set without {missing}; basic registry credentials are a pair")
    };

    match (user, password) {
        (Some(user), Some(password)) => return Ok(Auth::Basic { user, password }),
        (Some(_), None) => return Err(half(STELE_REGISTRY_USER_ENV, STELE_REGISTRY_PASSWORD_ENV)),
        (None, Some(_)) => return Err(half(STELE_REGISTRY_PASSWORD_ENV, STELE_REGISTRY_USER_ENV)),
        (None, None) => {}
    }

    if let Some(token) = token {
        return Ok(Auth::Bearer(token));
    }

    Ok(match &config.registry {
        Some(credentials) => Auth::Basic {
            user: credentials.user.clone(),
            // Through the accessor, not the field: a config that names a user
            // and no password means the official registry's, which is compiled
            // in rather than written into every generated `dolos.toml`.
            password: credentials.password().to_owned(),
        },
        None => Auth::Anonymous,
    })
}

pub fn setup_domain(config: &RootConfig) -> miette::Result<DomainAdapter> {
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

    let ChainConfig::Cardano(chain_config) = chain;

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

#[cfg(test)]
mod tests {
    use dolos_core::config::{StelaeRegistryConfig, OFFICIAL_REGISTRY_PASSWORD};

    use super::*;

    /// The environment is process-wide, so these run one at a time.
    static ENV: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Run `body` with exactly these registry variables set, and put the
    /// process environment back afterwards.
    ///
    /// Restoring is not politeness: `cargo test` runs every test in this binary
    /// in one process, and a leaked `STELAE_REGISTRY_TOKEN` would be read by
    /// whatever ran next.
    fn with_env<T>(
        token: Option<&str>,
        user: Option<&str>,
        password: Option<&str>,
        body: impl FnOnce() -> T,
    ) -> T {
        let _guard = ENV.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

        let names = [
            STELE_REGISTRY_TOKEN_ENV,
            STELE_REGISTRY_USER_ENV,
            STELE_REGISTRY_PASSWORD_ENV,
        ];

        let previous: Vec<(&str, Option<String>)> = names
            .into_iter()
            .map(|name| (name, std::env::var(name).ok()))
            .collect();

        let apply = |values: [Option<&str>; 3]| {
            for (name, value) in names.into_iter().zip(values) {
                match value {
                    Some(value) => std::env::set_var(name, value),
                    None => std::env::remove_var(name),
                }
            }
        };

        apply([token, user, password]);

        let outcome = body();

        for (name, value) in previous {
            match value {
                Some(value) => std::env::set_var(name, value),
                None => std::env::remove_var(name),
            }
        }

        outcome
    }

    /// A `[stelae]` section carrying `registry`, or carrying nothing.
    fn config(registry: Option<StelaeRegistryConfig>) -> StelaeConfig {
        StelaeConfig { registry }
    }

    fn reader() -> StelaeRegistryConfig {
        StelaeRegistryConfig {
            user: "dolos-reader".to_owned(),
            password: Some("published".to_owned()),
        }
    }

    /// The restore path: what `dolos init` seeded is what a node authenticates
    /// with when nothing is exported.
    #[test]
    fn a_configured_user_is_used_when_the_environment_is_silent() {
        with_env(None, None, None, || {
            assert_eq!(
                stele_registry_auth(&config(Some(reader()))).unwrap(),
                Auth::Basic {
                    user: "dolos-reader".to_owned(),
                    password: "published".to_owned(),
                }
            );

            // A user with no password is the seeded shape: the file says who,
            // the binary says with what.
            let seeded = StelaeRegistryConfig {
                user: "dolos-reader".to_owned(),
                password: None,
            };

            assert_eq!(
                stele_registry_auth(&config(Some(seeded))).unwrap(),
                Auth::Basic {
                    user: "dolos-reader".to_owned(),
                    password: OFFICIAL_REGISTRY_PASSWORD.to_owned(),
                }
            );

            // And a node that configured nothing stays anonymous rather than
            // inventing an identity.
            assert_eq!(stele_registry_auth(&config(None)).unwrap(), Auth::Anonymous);
        });
    }

    /// The publish path, and the precedence that makes it work on a node that
    /// already carries the read-only user.
    #[test]
    fn the_environment_overrides_what_is_configured() {
        with_env(None, Some("publisher"), Some("full-access"), || {
            assert_eq!(
                stele_registry_auth(&config(Some(reader()))).unwrap(),
                Auth::Basic {
                    user: "publisher".to_owned(),
                    password: "full-access".to_owned(),
                },
                "a publisher must not have to edit dolos.toml before it can publish",
            );
        });

        // A bearer token overrides it too: the environment is the source, and
        // which shape it names is the environment's business.
        with_env(Some("ghp_x"), None, None, || {
            assert_eq!(
                stele_registry_auth(&config(Some(reader()))).unwrap(),
                Auth::Bearer("ghp_x".to_owned())
            );
        });

        // An empty value is unset. A stale `export STELAE_REGISTRY_TOKEN=` in a
        // shell profile should not authenticate as the empty token.
        with_env(Some(""), Some(""), Some(""), || {
            assert_eq!(stele_registry_auth(&config(None)).unwrap(), Auth::Anonymous);
        });
    }

    /// Two kinds of credential at once is a refusal, and the message names
    /// every variable involved so an operator knows which to unset.
    ///
    /// A configured user is not a tie-breaker for it: the operator's mistake is
    /// in the environment, and what is in `dolos.toml` cannot resolve it.
    #[test]
    fn a_token_and_a_pair_together_are_refused() {
        with_env(
            Some("ghp_x"),
            Some("publisher"),
            Some("full-access"),
            || {
                for configured in [None, Some(reader())] {
                    let err = stele_registry_auth(&config(configured)).unwrap_err();
                    let message = err.to_string();

                    assert!(message.contains(STELE_REGISTRY_TOKEN_ENV), "{message}");
                    assert!(message.contains(STELE_REGISTRY_USER_ENV), "{message}");
                    assert!(message.contains(STELE_REGISTRY_PASSWORD_ENV), "{message}");
                }
            },
        );

        // Either half of the pair is enough to make it ambiguous. That one of
        // them is incomplete is not a reason to silently prefer the other.
        for (user, password) in [(Some("publisher"), None), (None, Some("full-access"))] {
            with_env(Some("ghp_x"), user, password, || {
                assert!(stele_registry_auth(&config(None)).is_err());
            });
        }
    }

    /// Half a pair is a typo or a secret that never reached the process, and
    /// sending the half that arrived would authenticate as somebody the
    /// operator did not name.
    #[test]
    fn half_a_pair_is_refused() {
        for (user, password, set, missing) in [
            (
                Some("publisher"),
                None,
                STELE_REGISTRY_USER_ENV,
                STELE_REGISTRY_PASSWORD_ENV,
            ),
            (
                None,
                Some("full-access"),
                STELE_REGISTRY_PASSWORD_ENV,
                STELE_REGISTRY_USER_ENV,
            ),
        ] {
            with_env(None, user, password, || {
                let message = stele_registry_auth(&config(Some(reader())))
                    .unwrap_err()
                    .to_string();

                assert!(message.contains(set), "{message}");
                assert!(message.contains(missing), "{message}");
            });
        }
    }
}
