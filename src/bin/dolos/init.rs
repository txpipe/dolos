use clap::Parser;
use dolos_cardano::networks::{self, KnownNetwork, Peer};
use dolos_core::config::{
    ChainConfig, GrpcConfig, HistoryConfig, HistoryWindow, MinibfConfig, MinikupoConfig,
    PeerConfig, RelayConfig, RootConfig, TrpConfig, UpstreamConfig,
};
use inquire::{Confirm, MultiSelect, Select, Text};
use miette::{Context as _, IntoDiagnostic};
use std::{fmt::Display, path::Path};

use crate::feedback::Feedback;

/// Parses `--known-network` / `--network` against the catalog.
pub fn parse_known_network(name: &str) -> Result<&'static KnownNetwork, String> {
    KnownNetwork::by_name(name).ok_or_else(|| format!("unknown network {name}"))
}

fn chain_config(network: &KnownNetwork) -> ChainConfig {
    ChainConfig::known(network.name)
}

#[derive(Debug, Clone)]
pub enum HistoryPrunningOptions {
    Keep1Day,
    Keep1Week,
    Keep1Month,
    KeepEverything,
    Custom(u64),
}

impl HistoryPrunningOptions {
    const VARIANTS: &'static [Self] = &[
        Self::Keep1Day,
        Self::Keep1Week,
        Self::Keep1Month,
        Self::KeepEverything,
    ];
}

impl Display for HistoryPrunningOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Keep1Day => f.write_str("1 day"),
            Self::Keep1Week => f.write_str("1 week"),
            Self::Keep1Month => f.write_str("1 month"),
            Self::KeepEverything => f.write_str("keep everything"),
            Self::Custom(x) => write!(f, "{x} slots"),
        }
    }
}

impl From<HistoryPrunningOptions> for HistoryConfig {
    fn from(value: HistoryPrunningOptions) -> Self {
        let slots = match value {
            HistoryPrunningOptions::KeepEverything => return HistoryConfig::default(),
            HistoryPrunningOptions::Keep1Day => 24 * 60 * 60,
            HistoryPrunningOptions::Keep1Week => 7 * 24 * 60 * 60,
            HistoryPrunningOptions::Keep1Month => 30 * 24 * 60 * 60,
            HistoryPrunningOptions::Custom(x) => x,
        };

        HistoryConfig::rolling(HistoryWindow::slots(slots))
    }
}

impl From<Option<u64>> for HistoryPrunningOptions {
    fn from(value: Option<u64>) -> Self {
        match value {
            None => Self::KeepEverything,
            Some(x) => Self::Custom(x),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AvailableApi {
    UtxoRpc,
    Minibf,
    Minikupo,
    Trp,
    #[cfg(unix)]
    Ouroboros,
}

impl Display for AvailableApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UtxoRpc => f.write_str("UTxO RPC (gRPC): Performant API for UTxO blockchains"),
            Self::Minibf => f.write_str("Mini-Blockfrost (HTTP): Blockfrost-compatible API"),
            Self::Minikupo => f.write_str("Mini-Kupo (HTTP): Kupo-compatible API"),
            Self::Trp => f.write_str("TRP (JSON-RPC): Tx3 transaction resolver protocol"),
            #[cfg(unix)]
            Self::Ouroboros => {
                f.write_str("Ouroboros (unix socket): node-to-client compatible API")
            }
        }
    }
}

impl AvailableApi {
    #[cfg(unix)]
    const VARIANTS: &'static [Self] = &[
        Self::UtxoRpc,
        Self::Minibf,
        Self::Minikupo,
        Self::Trp,
        Self::Ouroboros,
    ];

    #[cfg(windows)]
    const VARIANTS: &'static [Self] = &[Self::UtxoRpc, Self::Minibf, Self::Minikupo, Self::Trp];
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RemotePeerChoice {
    Preset(Peer),
    Other,
}

impl Display for RemotePeerChoice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemotePeerChoice::Preset(preset) => {
                write!(f, "{} ({})", preset.name, preset.address)
            }
            RemotePeerChoice::Other => f.write_str("Other"),
        }
    }
}

#[derive(Debug, Parser)]
pub struct Args {
    /// Use one of the well-known networks
    #[arg(long, value_parser = parse_known_network)]
    known_network: Option<&'static KnownNetwork>,

    /// Remote peer to use as source
    #[arg(long)]
    remote_peer: Option<String>,

    /// How much history of the chain to keep on disk
    #[arg(long)]
    max_chain_history: Option<u64>,

    /// Serve clients via gRPC
    #[arg(long)]
    serve_grpc: Option<bool>,

    /// Serve clients minibf via HTTP
    #[arg(long)]
    serve_minibf: Option<bool>,

    /// Serve clients via a MiniKupo-compatible HTTP endpoint
    #[arg(long)]
    serve_minikupo: Option<bool>,

    /// Serve clients TRP
    #[arg(long)]
    serve_trp: Option<bool>,

    /// Serve clients via Ouroboros
    #[arg(long)]
    serve_ouroboros: Option<bool>,

    /// Enable relay operations
    #[arg(long)]
    enable_relay: Option<bool>,
}

struct ConfigEditor(RootConfig);

impl Default for ConfigEditor {
    fn default() -> Self {
        let editor = Self(RootConfig::new(chain_config(&networks::MAINNET)));

        let editor = editor
            .apply_serve_grpc(Some(true))
            .apply_serve_minibf(Some(true))
            .apply_serve_minikupo(Some(true))
            .apply_serve_trp(Some(true));

        #[cfg(unix)]
        let editor = editor.apply_serve_ouroboros(Some(true));

        editor
    }
}

impl ConfigEditor {
    /// Switching networks drops the peer chosen for the previous one. Names
    /// resolve without regard to case, so `Preview` is not a switch to
    /// `preview`.
    fn apply_known_network(mut self, network: Option<&'static KnownNetwork>) -> Self {
        if let Some(network) = network {
            let current = KnownNetwork::by_name(&self.0.chain.network);

            if !current.is_some_and(|current| std::ptr::eq(current, network)) {
                self.0.upstream = None;
            }

            self.0.chain = chain_config(network);
        }

        self
    }

    /// `[upstream]` is written only for a peer other than the network's
    /// default, which the node uses without it.
    fn apply_remote_peer(mut self, value: Option<&String>) -> Self {
        if let Some(remote_peer) = value {
            let default = KnownNetwork::by_name(&self.0.chain.network).map(|x| x.default_peer);

            self.0.upstream =
                (default != Some(remote_peer.as_str())).then(|| UpstreamConfig::peer(remote_peer));
        }

        self
    }

    fn apply_history_pruning(mut self, value: HistoryPrunningOptions) -> Self {
        self.0.history = value.into();

        self
    }

    fn apply_serve_grpc(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                self.0.serve.grpc = GrpcConfig::new("[::]:50051".into(), None).into();
            } else {
                self.0.serve.grpc = None;
            }
        }

        self
    }

    fn apply_serve_minibf(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                self.0.serve.minibf = MinibfConfig::new("[::]:3000".parse().unwrap()).into();
            } else {
                self.0.serve.minibf = None;
            }
        }

        self
    }

    fn apply_serve_minikupo(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                self.0.serve.minikupo = MinikupoConfig::new("[::]:1442".parse().unwrap()).into();
            } else {
                self.0.serve.minikupo = None;
            }
        }

        self
    }

    fn apply_serve_trp(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                self.0.serve.trp = TrpConfig::new("[::]:8164".parse().unwrap(), None).into();
            } else {
                self.0.serve.trp = None;
            }
        }

        self
    }

    #[cfg(unix)]
    fn apply_serve_ouroboros(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                use dolos_core::config::OuroborosConfig;

                self.0.serve.ouroboros = OuroborosConfig {
                    listen_path: "dolos.socket".into(),
                }
                .into();
            } else {
                self.0.serve.ouroboros = None;
            }
        }

        self
    }

    #[cfg(windows)]
    fn apply_serve_ouroboros(self, _: Option<bool>) -> Self {
        // skip for windows
        self
    }

    fn apply_enable_relay(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                self.0.relay = RelayConfig {
                    listen_address: "[::]:30031".into(),
                }
                .into();
            } else {
                self.0.relay = None;
            }
        }

        self
    }

    fn fill_values_from_args(self, args: &Args) -> Self {
        self.apply_known_network(args.known_network)
            .apply_remote_peer(args.remote_peer.as_ref())
            .apply_history_pruning(args.max_chain_history.into())
            .apply_serve_grpc(args.serve_grpc)
            .apply_serve_minibf(args.serve_minibf)
            .apply_serve_minikupo(args.serve_minikupo)
            .apply_serve_trp(args.serve_trp)
            .apply_serve_ouroboros(args.serve_ouroboros)
            .apply_enable_relay(args.enable_relay)
    }

    fn prompt_known_network(self) -> miette::Result<Self> {
        let options = KnownNetwork::all().to_vec();

        let selected = KnownNetwork::by_name(&self.0.chain.network);

        let starting_cursor = selected
            .and_then(|x| options.iter().position(|y| std::ptr::eq(*y, x)))
            .unwrap_or_default();

        let value = Select::new("Which network are you connecting to?", options)
            .with_starting_cursor(starting_cursor)
            .prompt()
            .into_diagnostic()
            .context("asking for network")?;

        Ok(self.apply_known_network(Some(value)))
    }

    fn prompt_remote_peer(self) -> miette::Result<Self> {
        let network = KnownNetwork::by_name(&self.0.chain.network);

        let current = match &self.0.upstream {
            Some(UpstreamConfig::Peer(PeerConfig {
                peer_address: Some(address),
            })) => address.as_str(),
            _ => network.map(|x| x.default_peer).unwrap_or_default(),
        };

        let Some(network) = network else {
            let value = Text::new("Which remote peer (relay) do you want to use?")
                .with_default(current)
                .prompt()
                .into_diagnostic()
                .context("asking for remote peer")?;

            return Ok(self.apply_remote_peer(Some(&value)));
        };

        let mut options: Vec<_> = network
            .peers
            .iter()
            .copied()
            .map(RemotePeerChoice::Preset)
            .collect();

        options.push(RemotePeerChoice::Other);

        let starting_cursor = options
            .iter()
            .position(
                |x| matches!(x, RemotePeerChoice::Preset(preset) if preset.address == current),
            )
            .unwrap_or(options.len() - 1);

        let selected = Select::new("Which remote peer (relay) do you want to use?", options)
            .with_starting_cursor(starting_cursor)
            .prompt()
            .into_diagnostic()
            .context("asking for remote peer")?;

        let value = match selected {
            RemotePeerChoice::Preset(preset) => preset.address.to_string(),
            RemotePeerChoice::Other => Text::new("Custom remote peer (relay address host:port):")
                .with_default(current)
                .with_help_message("Format: host:port")
                .prompt()
                .into_diagnostic()
                .context("asking for custom remote peer")?,
        };

        Ok(self.apply_remote_peer(Some(&value)))
    }

    fn is_api_enabled(&self, api: AvailableApi) -> bool {
        match api {
            AvailableApi::UtxoRpc => self.0.serve.grpc.is_some(),
            AvailableApi::Minibf => self.0.serve.minibf.is_some(),
            AvailableApi::Minikupo => self.0.serve.minikupo.is_some(),
            AvailableApi::Trp => self.0.serve.trp.is_some(),
            #[cfg(unix)]
            AvailableApi::Ouroboros => self.0.serve.ouroboros.is_some(),
        }
    }

    fn default_api_indexes(&self) -> Vec<usize> {
        AvailableApi::VARIANTS
            .iter()
            .copied()
            .enumerate()
            .filter_map(|(idx, api)| self.is_api_enabled(api).then_some(idx))
            .collect()
    }

    fn prompt_serve_apis(self) -> miette::Result<Self> {
        let value = MultiSelect::new(
            "Which APIs do you want to enable?",
            AvailableApi::VARIANTS.to_vec(),
        )
        .with_default(&self.default_api_indexes())
        .without_filtering()
        .prompt()
        .into_diagnostic()
        .context("asking for APIs to serve")?;

        let config = self
            .apply_serve_grpc(Some(value.contains(&AvailableApi::UtxoRpc)))
            .apply_serve_minibf(Some(value.contains(&AvailableApi::Minibf)))
            .apply_serve_minikupo(Some(value.contains(&AvailableApi::Minikupo)))
            .apply_serve_trp(Some(value.contains(&AvailableApi::Trp)));

        #[cfg(unix)]
        let config = config.apply_serve_ouroboros(Some(value.contains(&AvailableApi::Ouroboros)));

        Ok(config)
    }

    fn prompt_enable_relay(self) -> miette::Result<Self> {
        let value = Confirm::new("Do you want to act as a relay for other nodes?")
            .with_default(self.0.relay.is_some())
            .prompt()
            .into_diagnostic()
            .context("asking for relay enabled")?;

        Ok(self.apply_enable_relay(Some(value)))
    }

    fn prompt_history_pruning(self) -> miette::Result<Self> {
        let value = Select::new(
            "How much history of the chain do you want to keep on disk?",
            HistoryPrunningOptions::VARIANTS.to_vec(),
        )
        .prompt()
        .into_diagnostic()
        .context("asking for history pruning")?;

        Ok(self.apply_history_pruning(value))
    }

    fn confirm_values(mut self) -> miette::Result<ConfigEditor> {
        self = self
            .prompt_known_network()?
            .prompt_remote_peer()?
            .prompt_history_pruning()?
            .prompt_serve_apis()?
            .prompt_enable_relay()?;

        Ok(self)
    }

    fn save(self, path: &Path) -> miette::Result<()> {
        self.0
            .validate()
            .into_diagnostic()
            .context("validating the generated config")?;

        let config = toml::to_string_pretty(&self.0)
            .into_diagnostic()
            .context("serializing config toml")?;

        std::fs::write(path, config)
            .into_diagnostic()
            .context("saving config file")?;

        Ok(())
    }
}

/// Refuses a config file that is present but did not load, leaving it
/// untouched: the load error says what to change, and init never guesses at
/// the storage such a file names.
fn refuse_unreadable(
    config: miette::Result<RootConfig>,
    config_file: &Path,
) -> miette::Result<Option<RootConfig>> {
    match config {
        Ok(config) => Ok(Some(config)),
        Err(report) if config_file.exists() => Err(report.wrap_err(format!(
            "leaving the existing `{}` untouched",
            config_file.display()
        ))),
        Err(_) => Ok(None),
    }
}

/// Writes `config_file` from the configuration already there, `args`, and
/// what `confirm` settles on, and loads it back from the same file.
fn write_config(
    config: miette::Result<RootConfig>,
    config_file: &Path,
    args: &Args,
    confirm: impl FnOnce(ConfigEditor) -> miette::Result<ConfigEditor>,
) -> miette::Result<RootConfig> {
    let editor = refuse_unreadable(config, config_file)?
        .map(ConfigEditor)
        .unwrap_or_default()
        .fill_values_from_args(args);

    confirm(editor)?.save(config_file)?;

    println!("config saved to {}", config_file.display());

    crate::common::load_config(Some(config_file))
        .into_diagnostic()
        .context("loading configuration")
}

/// The command that starts the node on `config_file`.
fn daemon_command(config_file: &Path) -> String {
    if config_file == crate::common::config_file(None) {
        "dolos daemon".into()
    } else {
        format!("dolos --config {} daemon", config_file.display())
    }
}

pub fn run(
    config: miette::Result<RootConfig>,
    config_file: &Path,
    args: &Args,
    feedback: &Feedback,
) -> miette::Result<()> {
    crate::banner::print_init_banner();

    let config = write_config(config, config_file, args, ConfigEditor::confirm_values)?;

    if !config
        .upstream
        .as_ref()
        .is_some_and(UpstreamConfig::is_emulator)
    {
        super::bootstrap::run(&config, &super::bootstrap::Args::default(), feedback)?;
    }

    println!("\nDolos is ready!");
    println!("- run `{}` to start the node", daemon_command(config_file));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::config::MIGRATION_GUIDE_URL;
    use std::path::PathBuf;

    fn init_args(flags: &[&str]) -> Args {
        Args::parse_from(std::iter::once("init").chain(flags.iter().copied()))
    }

    /// `init` over `config_file` with nothing asked beyond its arguments.
    fn write_unprompted(config_file: &Path, args: &Args) -> miette::Result<RootConfig> {
        let config = dolos_core::config::load(Some(config_file)).map_err(miette::Report::from_err);

        write_config(config, config_file, args, Ok)
    }

    /// The setup a fresh init writes works: the saved config loads, the
    /// stores open, and the first block appended lands in a segment file as a
    /// zstd frame with nothing having been configured for it.
    #[test]
    fn a_fresh_init_appends_compressed_frames() {
        use dolos_core::archive::{ArchiveStore as _, ArchiveWriter as _};
        use dolos_core::{ChainPoint, RawBlock};

        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let config_file = dir.path().join("dolos.toml");

        let mut editor = ConfigEditor::default();
        editor.0.storage.path = data.clone();
        editor.save(&config_file).unwrap();

        // Parsed as written: `load` would apply the developer's `DOLOS_`
        // variables, which could point these stores anywhere.
        let config: dolos_core::config::RootConfig =
            toml::from_str(&std::fs::read_to_string(&config_file).unwrap()).unwrap();
        assert_eq!(config.storage.path, data);

        let stores = crate::common::open_data_stores(&config).unwrap();

        let body: RawBlock = std::sync::Arc::new("a block body ".repeat(64).into_bytes());
        let point = ChainPoint::Specific(7, pallas::crypto::hash::Hash::new([7u8; 32]));

        let writer = stores.archive.start_writer().unwrap();
        writer.apply(&point, &body).unwrap();
        writer.commit().unwrap();

        assert_eq!(
            stores.archive.get_block_by_slot(&7).unwrap().unwrap(),
            *body
        );

        let segments: Vec<PathBuf> = std::fs::read_dir(data.join("archive"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| path.extension().is_some_and(|ext| ext == "segment"))
            .collect();
        assert_eq!(segments.len(), 1, "one segment holds the first block");

        let frame = std::fs::read(&segments[0]).unwrap();
        assert_eq!(&frame[..4], &[0x28, 0xB5, 0x2F, 0xFD], "a zstd frame");
        assert!(
            frame.len() < body.len(),
            "the frame is compressed: {} bytes for a {} byte body",
            frame.len(),
            body.len()
        );

        stores.archive.shutdown().unwrap();
    }

    /// Bootstrap sources and credentials are inputs of the bootstrap
    /// commands, so a generated config carries none of them — and still loads.
    #[test]
    fn a_fresh_config_carries_no_bootstrap_section() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dolos.toml");

        ConfigEditor::default()
            .apply_known_network(Some(&networks::PREPROD))
            .save(&path)
            .unwrap();

        let written = std::fs::read_to_string(&path).unwrap();

        for section in ["[mithril", "[snapshot", "[stelae"] {
            assert!(!written.contains(section), "{written}");
        }

        dolos_core::config::load(Some(&path)).expect("the generated config loads");
    }

    /// A config an older `init` wrote carries `[mithril]`; init reports where
    /// it moved and leaves the file as it was.
    #[test]
    fn an_older_init_config_is_reported_and_left_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dolos.toml");

        ConfigEditor::default()
            .apply_known_network(Some(&networks::PREPROD))
            .save(&path)
            .unwrap();

        let mut written = std::fs::read_to_string(&path).unwrap();
        written.push_str(
            "\n[mithril]\naggregator = \"https://aggregator.example\"\ngenesis_key = \"5b31\"\n",
        );
        std::fs::write(&path, &written).unwrap();

        let config = dolos_core::config::load(Some(&path)).map_err(miette::Report::from_err);

        let report = refuse_unreadable(config, &path).err().unwrap();
        let message = format!("{report:?}");

        assert!(message.contains("[mithril] moved to"), "{message}");
        assert!(message.contains("untouched"), "{message}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), written);
    }

    #[test]
    fn a_missing_config_is_a_fresh_init() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dolos.toml");

        let config = dolos_core::config::load(Some(&path)).map_err(miette::Report::from_err);

        assert!(refuse_unreadable(config, &path).unwrap().is_none());
    }

    /// The selected file is the only one written, and init reloads it as the
    /// configuration it was written from.
    #[test]
    fn init_writes_and_reloads_the_selected_file() {
        let _guard = crate::common::ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("other.toml");
        let args = init_args(&[
            "--known-network",
            "preview",
            "--remote-peer",
            "relay.example:3001",
        ]);

        let reloaded = write_unprompted(&path, &args).unwrap();

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(files, ["other.toml"]);

        let expected = ConfigEditor::default().fill_values_from_args(&args).0;
        assert_eq!(expected.chain, ChainConfig::known("preview"));
        assert_eq!(reloaded, expected);
        assert_eq!(dolos_core::config::load(Some(&path)).unwrap(), expected);
    }

    /// Without `--config`, init writes `dolos.toml` and nothing else.
    #[test]
    fn without_a_path_init_writes_the_default_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(crate::common::config_file(None));

        let reloaded =
            write_unprompted(&path, &init_args(&["--known-network", "preprod"])).unwrap();
        assert_eq!(reloaded.chain, ChainConfig::known("preprod"));

        let files: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(files, ["dolos.toml"]);
    }

    /// What init returns is read back from the file: a fresh init under a
    /// `DOLOS_` override carries the override, which the file does not hold.
    #[test]
    fn init_returns_the_config_read_back_from_the_file() {
        let _guard = crate::common::ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dolos.toml");
        let args = init_args(&["--known-network", "preview"]);

        std::env::set_var("DOLOS_SERVE__MAX_CONCURRENT_QUERIES", "7");
        let reloaded = write_config(Err(miette::miette!("no file yet")), &path, &args, Ok);
        std::env::remove_var("DOLOS_SERVE__MAX_CONCURRENT_QUERIES");

        assert_eq!(reloaded.unwrap().serve.max_concurrent_queries, 7);

        let written = std::fs::read_to_string(&path).unwrap();
        assert!(!written.contains("max_concurrent_queries = 7"), "{written}");
    }

    /// The hint names the file init wrote.
    #[test]
    fn the_daemon_hint_names_a_selected_file() {
        assert_eq!(daemon_command(Path::new("dolos.toml")), "dolos daemon");
        assert_eq!(
            daemon_command(Path::new("conf/other.toml")),
            "dolos --config conf/other.toml daemon"
        );
    }

    fn with_peer(chain: ChainConfig) -> ConfigEditor {
        let mut config = RootConfig::new(chain);
        config.upstream = Some(UpstreamConfig::peer("my-relay:3001"));
        ConfigEditor(config)
    }

    /// A re-run on the same network keeps a custom peer, whatever the case
    /// of the network's name; a switch, from a known or a custom network,
    /// drops it.
    #[test]
    fn only_a_network_switch_drops_the_peer() {
        let peer = Some(UpstreamConfig::peer("my-relay:3001"));

        for network in ["preview", "Preview"] {
            let editor = with_peer(ChainConfig::known(network))
                .apply_known_network(Some(&networks::PREVIEW));
            assert_eq!(editor.0.upstream, peer, "{network}");
            assert_eq!(editor.0.chain, ChainConfig::known("preview"));
        }

        let editor =
            with_peer(ChainConfig::known("preview")).apply_known_network(Some(&networks::PREPROD));
        assert_eq!(editor.0.upstream, None);
        assert_eq!(editor.0.chain, ChainConfig::known("preprod"));

        let custom = ChainConfig::custom(dolos_core::config::CardanoConfig {
            magic: 42,
            is_testnet: true,
            genesis: dolos_core::config::GenesisConfig::in_dir(Path::new("genesis")),
            custom_utxos: vec![],
        });
        let editor = with_peer(custom).apply_known_network(Some(&networks::PREVIEW));
        assert_eq!(editor.0.upstream, None);
        assert_eq!(editor.0.chain, ChainConfig::known("preview"));

        let editor = with_peer(ChainConfig::known("preview")).apply_known_network(None);
        assert_eq!(editor.0.upstream, peer);
    }

    /// A known network carries its genesis, peer, rollback window and store
    /// tuning, so the written file names none of them.
    #[test]
    fn a_known_network_config_writes_no_derived_fields() {
        for network in KnownNetwork::all() {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("dolos.toml");

            write_unprompted(&path, &init_args(&["--known-network", network.name])).unwrap();

            let written = std::fs::read_to_string(&path).unwrap();
            assert!(
                written.starts_with("compatibility = \"v2-alpha.1\""),
                "{written}"
            );

            for absent in [
                "upstream",
                "genesis",
                "rollback",
                "wal",
                "[sync",
                "preset",
                "bootstrap",
            ] {
                assert!(
                    !written.contains(absent),
                    "{}: {absent} in {written}",
                    network.name
                );
            }

            let files = std::fs::read_dir(dir.path()).unwrap().count();
            assert_eq!(files, 1, "{}: only the config is written", network.name);
        }
    }

    /// `[upstream]` names a peer only when it is not the network's default.
    #[test]
    fn upstream_is_written_only_for_another_peer() {
        let peer = |remote_peer: &str| {
            let args = init_args(&["--known-network", "preview", "--remote-peer", remote_peer]);
            ConfigEditor::default()
                .fill_values_from_args(&args)
                .0
                .upstream
        };

        assert_eq!(peer(networks::PREVIEW.default_peer), None);
        assert_eq!(
            peer("relay.example:3001"),
            Some(UpstreamConfig::peer("relay.example:3001"))
        );
    }

    /// An alpha.0 file at the selected path is reported with the schema's
    /// diagnostic and kept byte for byte.
    #[test]
    fn an_alpha_config_is_reported_and_left_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("other.toml");
        std::fs::copy(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/config/legacy/v2.0.0-alpha.0-sync-mainnet.toml"),
            &path,
        )
        .unwrap();
        let before = std::fs::read(&path).unwrap();

        let report = write_unprompted(&path, &init_args(&["--known-network", "preview"]))
            .err()
            .unwrap();
        let message = format!("{report:?}");

        assert!(message.contains(MIGRATION_GUIDE_URL), "{message}");
        assert!(message.contains("untouched"), "{message}");
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn history_choices_map_onto_history_modes() {
        assert_eq!(
            HistoryConfig::from(HistoryPrunningOptions::KeepEverything),
            HistoryConfig::default()
        );
        assert_eq!(
            HistoryConfig::from(HistoryPrunningOptions::Keep1Day).max_slots(),
            Some(86_400)
        );

        let editor =
            ConfigEditor::default().apply_history_pruning(HistoryPrunningOptions::Custom(5));
        editor.0.validate().unwrap();
        assert_eq!(editor.0.history.max_slots(), Some(5));
    }
}
