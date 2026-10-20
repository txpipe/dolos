//! The configuration fixtures under `tests/fixtures/config/`: the reference
//! examples parse to their values, each validation rule refuses its fixture,
//! and `compatibility` and older files get their diagnostics.
//!
//! Every test that loads is `#[serial]`: one of them sets `DOLOS_` variables,
//! which every load reads.

use std::path::{Path, PathBuf};
use std::process::Command;

use dolos_cardano::networks::ResolvedNetwork;
use dolos_core::config::{
    load, ArchiveStoreConfig, CacheConfig, ChainConfig, ConfigError, CustomUtxo, FjallPreset,
    HistoryConfig, HistoryMode, HistoryWindow, LoggingConfig, MempoolConfig, Protocol, RetryConfig,
    RootConfig, StateStoreConfig, StoreType, SyncConfig, TelemetryConfig, UpstreamConfig,
    MIGRATION_GUIDE_URL,
};
use serial_test::serial;

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/config")
        .join(name)
}

/// What the binary does before it opens a store: load, resolve the network
/// and read its genesis.
fn check(path: &Path) -> Result<RootConfig, String> {
    let config = load(Some(path)).map_err(|error| error.to_string())?;

    ResolvedNetwork::resolve(&config.chain)
        .and_then(|network| network.genesis())
        .map_err(|error| error.to_string())?;

    Ok(config)
}

fn rejection(path: &Path) -> String {
    match check(path) {
        Ok(_) => panic!("{} was accepted", path.display()),
        Err(message) => message,
    }
}

/// `name` copied beside a `genesis/` directory holding the devnet genesis with
/// `magic` as its network magic.
fn beside_genesis(name: &str, magic: u32) -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let genesis = dir.path().join("genesis");
    std::fs::create_dir(&genesis).unwrap();
    dolos_cardano::include::devnet::save(&genesis).unwrap();

    let shelley = genesis.join("shelley.json");
    let patched = std::fs::read_to_string(&shelley).unwrap().replace(
        "\"networkMagic\": 1,",
        &format!("\"networkMagic\": {magic},"),
    );
    std::fs::write(&shelley, patched).unwrap();

    let path = dir.path().join("dolos.toml");
    std::fs::copy(fixture(name), &path).unwrap();

    (dir, path)
}

fn rolling_month() -> HistoryConfig {
    HistoryConfig::rolling(HistoryWindow::slots(2_592_000))
}

#[test]
#[serial]
fn the_ordinary_example_parses() {
    let config = check(&fixture("ordinary.toml")).unwrap();

    let mut expected = RootConfig::new(ChainConfig::known("mainnet"));
    expected.history = rolling_month();
    expected.serve.minibf = Some(dolos_core::config::MinibfConfig::new(
        "[::]:3000".parse().unwrap(),
    ));

    assert_eq!(config, expected);
    assert_eq!(config.compatibility().as_str(), "v2-alpha.1");
}

#[test]
#[serial]
fn the_full_reference_parses() {
    let config = check(&fixture("full.toml")).unwrap();

    assert_eq!(config.chain, ChainConfig::known("mainnet"));
    assert_eq!(
        config.upstream,
        Some(UpstreamConfig::peer(
            "backbone.mainnet.cardanofoundation.org:3001"
        ))
    );
    assert_eq!(
        config.sync,
        SyncConfig {
            wal_path: Some("wal".into()),
            pull_batch_size: 100,
            retry: RetryConfig::default(),
        }
    );

    assert_eq!(config.storage.path, PathBuf::from("data"));
    assert_eq!(
        config.storage.state,
        StateStoreConfig {
            kind: StoreType::Fjall {
                preset: FjallPreset::Standard
            },
            path: Some("state".into()),
        }
    );
    assert_eq!(
        config.storage.archive,
        ArchiveStoreConfig {
            kind: StoreType::Fjall {
                preset: FjallPreset::Standard
            },
            path: Some("archive".into()),
            blocks_path: Some("blocks".into()),
        }
    );
    assert_eq!(
        config.storage.cache,
        CacheConfig {
            path: Some("cache".into()),
            admission: [("totals".to_string(), 100_000)].into(),
            ..CacheConfig::default()
        }
    );

    assert_eq!(config.history, rolling_month());
    assert_eq!(
        config.mempool,
        MempoolConfig {
            persistent: true,
            path: Some("mempool".into()),
        }
    );

    let serve = &config.serve;
    assert_eq!(serve.max_concurrent_queries, 32);

    let grpc = serve.grpc.as_ref().unwrap();
    assert_eq!(grpc.listen_address, "[::]:50051");
    assert_eq!(grpc.tls_client_ca_root, Some("./client-ca.pem".into()));
    assert!(grpc.permissive_cors());
    assert!(!grpc.http2_adaptive_window());
    assert_eq!(grpc.http2_initial_stream_window_size, Some(8_388_608));
    assert_eq!(grpc.http2_initial_connection_window_size, Some(16_777_216));
    assert_eq!(grpc.http2_max_frame_size, Some(16_384));
    assert_eq!(grpc.http2_max_concurrent_streams, Some(256));

    let minibf = serve.minibf.as_ref().unwrap();
    assert_eq!(minibf.listen_address, "[::]:3000".parse().unwrap());
    assert!(minibf.permissive_cors());
    assert_eq!(
        minibf.public_url.as_deref(),
        Some("https://api.example.com")
    );
    assert_eq!(
        minibf.token_registry_url.as_deref(),
        Some("https://tokens.example.com")
    );
    assert_eq!(minibf.max_scan_items(), 3000);
    assert_eq!(
        minibf.ipfs_gateways(),
        ["https://ipfs.io", "https://gateway.pinata.cloud"]
    );

    let minikupo = serve.minikupo.as_ref().unwrap();
    assert_eq!(minikupo.listen_address, "[::]:1442".parse().unwrap());
    assert!(minikupo.permissive_cors());

    let ouroboros = serve.ouroboros.as_ref().unwrap();
    assert_eq!(ouroboros.listen_path, PathBuf::from("./dolos.socket"));

    let trp = serve.trp.as_ref().unwrap();
    assert_eq!(trp.listen_address, "[::]:8164".parse().unwrap());
    assert!(trp.permissive_cors());
    assert_eq!(trp.max_optimize_rounds(), 10);
    assert_eq!(trp.extra_fees_lovelace, Some(200_000));

    assert_eq!(config.relay.unwrap().listen_address, "[::]:30031");
    assert_eq!(
        config.logging,
        LoggingConfig {
            level: tracing::Level::INFO,
            filter: Some("dolos=debug,fjall=warn".into()),
        }
    );
    assert_eq!(
        config.telemetry,
        Some(TelemetryConfig {
            otlp_endpoint: "http://localhost:4317".into(),
            service_name: "dolos-mainnet".into(),
        })
    );
}

#[test]
#[serial]
fn the_custom_example_parses_with_genesis_beside_it() {
    let (dir, path) = beside_genesis("custom.toml", 42);

    let config = check(&path).unwrap();

    let cardano = config.chain.cardano.as_ref().unwrap();
    assert_eq!(config.chain.protocol, Protocol::Cardano);
    assert!(config.chain.is_custom());
    assert_eq!((cardano.magic, cardano.is_testnet), (42, true));
    assert_eq!(
        cardano.genesis.shelley_path,
        dir.path().join("./genesis/shelley.json")
    );
    assert_eq!(cardano.genesis.force_protocol, Some(9));
    assert_eq!(
        cardano.custom_utxos,
        [CustomUtxo {
            ref_: dolos_core::TxoRef([1; 32].into(), 0),
            era: Some(7),
            cbor: vec![],
        }]
    );
    assert_eq!(
        config.upstream,
        Some(UpstreamConfig::peer("127.0.0.1:3001"))
    );

    let network = ResolvedNetwork::resolve(&config.chain).unwrap();
    assert_eq!(network.magic(), 42);
    assert_eq!(network.default_peer(), None);
}

#[test]
#[serial]
fn the_ephemeral_example_parses() {
    let config = check(&fixture("ephemeral.toml")).unwrap();

    assert_eq!(config.storage.state, StateStoreConfig::ephemeral());
    assert_eq!(config.storage.archive, ArchiveStoreConfig::ephemeral());
    assert_eq!(config.storage.state_path(), None);
    assert_eq!(config.storage.archive_path(), None);
    assert_eq!(config.wal_path(), None);
}

#[test]
#[serial]
fn omitted_sections_take_their_defaults() {
    let config = check(&fixture("minimal.toml")).unwrap();

    assert_eq!(config, RootConfig::new(ChainConfig::known("preprod")));

    assert_eq!(config.upstream, None);
    assert_eq!(config.sync.pull_batch_size, 100);
    assert_eq!(
        config.sync.retry,
        RetryConfig {
            max_retries: 20,
            backoff_unit_seconds: 2,
            backoff_factor: 2,
            max_backoff_seconds: 60,
            dismissible: false,
        }
    );
    assert_eq!(config.storage.path, PathBuf::from("data"));
    for kind in [config.storage.state.kind, config.storage.archive.kind] {
        assert_eq!(
            kind,
            StoreType::Fjall {
                preset: FjallPreset::Standard
            }
        );
    }
    assert!(config.storage.cache.enabled);
    assert_eq!(config.storage.cache.memory_mb, 512);
    assert_eq!(config.storage.cache.disk_mb, 4096);
    assert_eq!(config.history.mode, HistoryMode::Full);
    assert!(!config.mempool.persistent);
    assert_eq!(config.serve.max_concurrent_queries, 32);
    assert!(config.serve.grpc.is_none() && config.serve.minibf.is_none());
    assert!(config.relay.is_none());
    assert_eq!(config.logging.level, tracing::Level::INFO);
    assert!(config.telemetry.is_none());

    let root = PathBuf::from("data");
    assert_eq!(config.wal_path(), Some(root.join("wal")));
    assert_eq!(config.storage.state_path(), Some(root.join("state")));
    assert_eq!(config.storage.archive_path(), Some(root.join("archive")));
    assert_eq!(config.storage.cache_path(), Some(root.join("cache")));
    assert_eq!(config.mempool_path(), None);

    let network = ResolvedNetwork::resolve(&config.chain).unwrap();
    assert_eq!(network.magic(), 1);
    assert_eq!(
        network.default_peer(),
        Some(dolos_cardano::networks::PREPROD.default_peer)
    );
}

#[test]
#[serial]
fn each_rule_refuses_its_fixture() {
    for (name, message) in [
        ("custom-without-cardano", "requires `[chain.cardano]`"),
        (
            "cardano-on-a-known-network",
            "`[chain.cardano]` is only for `network = \"custom\"`",
        ),
        (
            "unknown-network",
            "unknown network `sanchonet`; known networks are `mainnet`, `preprod`, `preview`",
        ),
        ("custom-without-upstream", "has no default peer"),
        (
            "custom-peer-without-address",
            "`upstream.peer_address` is required",
        ),
        (
            "emulator-on-a-known-network",
            "`upstream.type = \"emulator\"` is only valid with `network = \"custom\"`",
        ),
        (
            "window-without-rolling",
            "`history.window` is only valid with `mode = \"rolling\"`",
        ),
        ("rolling-without-window", "requires a `window`"),
        (
            "no-concurrent-queries",
            "`serve.max_concurrent_queries` must be between 1 and 64, found 0",
        ),
        (
            "too-many-concurrent-queries",
            "`serve.max_concurrent_queries` must be between 1 and 64, found 65",
        ),
        (
            "ephemeral-store-with-a-path",
            "an ephemeral store takes no other field; remove `path`",
        ),
        ("unknown-field", "unknown field `version`"),
        ("moved-section", "[mithril] moved to"),
    ] {
        let found = rejection(&fixture(&format!("reject/{name}.toml")));
        assert!(found.contains(message), "{name}: {found}");
    }
}

#[test]
#[serial]
fn a_custom_magic_must_match_its_genesis() {
    let (_dir, path) = beside_genesis("reject/magic-mismatch.toml", 42);

    let found = rejection(&path);

    assert!(
        found.contains("network magic 43 does not match the Shelley genesis networkMagic 42"),
        "{found}"
    );
}

#[test]
#[serial]
fn compatibility_is_checked_first() {
    let missing = load(Some(&fixture("compatibility/missing.toml"))).unwrap_err();
    assert!(matches!(missing, ConfigError::MissingCompatibility));
    let message = missing.to_string();
    assert!(message.contains("`compatibility`"), "{message}");
    assert!(message.contains("\"v2-alpha.1\""), "{message}");

    let malformed = load(Some(&fixture("compatibility/malformed.toml"))).unwrap_err();
    assert!(matches!(malformed, ConfigError::MalformedCompatibility(_)));
    assert!(malformed.to_string().contains("found 2"), "{malformed}");

    let unsupported = load(Some(&fixture("compatibility/unsupported.toml"))).unwrap_err();
    assert!(matches!(
        unsupported,
        ConfigError::UnsupportedCompatibility(_)
    ));
    let message = unsupported.to_string();
    assert!(message.contains("\"v2-alpha.0\""), "{message}");
    assert!(message.contains("supports \"v2-alpha.1\""), "{message}");

    let supported = load(Some(&fixture("ordinary.toml"))).unwrap();
    assert_eq!(supported.compatibility().as_str(), "v2-alpha.1");
}

#[test]
#[serial]
fn older_files_point_to_the_migration_guide() {
    for name in [
        "legacy/v1.6-sync-preview.toml",
        "legacy/v2.0.0-alpha.0-sync-mainnet.toml",
    ] {
        let error = load(Some(&fixture(name))).unwrap_err();

        assert!(matches!(error, ConfigError::LegacyShape), "{name}: {error}");
        assert!(error.to_string().contains(MIGRATION_GUIDE_URL), "{name}");
    }
}

/// `data summary` opens every store, so a refused file shows that none was.
fn run_dolos(cwd: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dolos"))
        .current_dir(cwd)
        .args(args)
        .env_remove("DOLOS_UPSTREAM__PEER_ADDRESS")
        .output()
        .unwrap()
}

#[test]
fn an_older_file_opens_no_store() {
    for name in [
        "legacy/v1.6-sync-preview.toml",
        "legacy/v2.0.0-alpha.0-sync-mainnet.toml",
    ] {
        let dir = tempfile::tempdir().unwrap();
        std::fs::copy(fixture(name), dir.path().join("dolos.toml")).unwrap();

        let output = run_dolos(dir.path(), &["data", "summary"]);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(!output.status.success(), "{name}");
        assert!(stderr.contains(MIGRATION_GUIDE_URL), "{name}: {stderr}");
        assert_eq!(
            std::fs::read_dir(dir.path()).unwrap().count(),
            1,
            "{name}: only the config file is there"
        );
    }
}

#[test]
fn only_the_selected_file_is_read() {
    let dir = tempfile::tempdir().unwrap();

    let output = run_dolos(dir.path(), &["data", "summary"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(
        stderr.contains("no configuration file at dolos.toml"),
        "{stderr}"
    );

    let elsewhere = dir.path().join("elsewhere.toml");
    let output = run_dolos(
        dir.path(),
        &["--config", elsewhere.to_str().unwrap(), "data", "summary"],
    );
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("no configuration file at"), "{stderr}");
    assert!(stderr.contains("elsewhere.toml"), "{stderr}");
}

#[test]
#[serial]
fn nested_environment_variables_override_the_file() {
    let overrides = [
        ("DOLOS_UPSTREAM__PEER_ADDRESS", "relay.example:3001"),
        ("DOLOS_SERVE__MINIBF__LISTEN_ADDRESS", "127.0.0.1:4000"),
        ("DOLOS_SERVE__MAX_CONCURRENT_QUERIES", "8"),
        ("DOLOS_MITHRIL_AGGREGATOR", "https://aggregator.example"),
        ("DOLOS_COMPATIBILITY", "v2-alpha.0"),
    ];

    for (name, value) in overrides {
        std::env::set_var(name, value);
    }

    let loaded = load(Some(&fixture("minimal.toml")));

    for (name, _) in overrides {
        std::env::remove_var(name);
    }

    let config = loaded.unwrap();

    assert_eq!(
        config.upstream,
        Some(UpstreamConfig::peer("relay.example:3001"))
    );
    assert_eq!(
        config.serve.minibf.as_ref().unwrap().listen_address,
        "127.0.0.1:4000".parse().unwrap()
    );
    assert_eq!(config.serve.max_concurrent_queries, 8);
    assert_eq!(config.compatibility().as_str(), "v2-alpha.1");
}
