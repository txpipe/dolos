use clap::Parser;
use dolos_cardano::{include, mutable_slots};
use dolos_core::{
    config::{
        CardanoConfig, ChainConfig, GenesisConfig, GrpcConfig, MinibfConfig, MinikupoConfig,
        MithrilConfig, PeerConfig, RelayConfig, RootConfig, StelaeConfig, StelaeRegistryConfig,
        StorageConfig, StorageVersion, TrpConfig, UpstreamConfig,
    },
    Genesis,
};
use inquire::{Confirm, MultiSelect, Select, Text};
use miette::{miette, Context as _, IntoDiagnostic};
use std::{
    fmt::Display,
    path::{Path, PathBuf},
    str::FromStr,
};

use dolos::storage::{CURRENT_STORAGE_VERSION, MIGRATION_GUIDE_URL};

use crate::{common::cleanup_data, feedback::Feedback};

/// The config file `dolos init` writes, and the one whose failure to parse
/// tells us an older setup is present.
const CONFIG_FILE: &str = "dolos.toml";

/// What `dolos init` may do about the storage of a setup it found on disk.
///
/// The distinction that matters is whether init knows where the old store
/// lives. A config that parses names its path, so the delete-and-bootstrap
/// step can be offered. A config that did not parse — which is what a v1.6
/// TOML naming a removed backend variant looks like from here — leaves the
/// editor filled from defaults, and the default path is a guess: deleting on
/// a guess would aim at a directory the operator never named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageUpgrade {
    /// The config already reads at the version this binary carries.
    NotNeeded,
    /// An older version, at a path init knows: offer to delete and bootstrap.
    Offer,
    /// An existing config init cannot read, so the store's path is unknown.
    Unreadable,
}

fn storage_upgrade(version: &StorageVersion, unparseable_existing: bool) -> StorageUpgrade {
    if unparseable_existing {
        StorageUpgrade::Unreadable
    } else if *version != CURRENT_STORAGE_VERSION {
        StorageUpgrade::Offer
    } else {
        StorageUpgrade::NotNeeded
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
#[allow(clippy::enum_variant_names)]
pub enum KnownNetwork {
    CardanoMainnet,
    CardanoPreProd,
    CardanoPreview,
}

impl KnownNetwork {
    const VARIANTS: &'static [KnownNetwork] = &[
        KnownNetwork::CardanoMainnet,
        KnownNetwork::CardanoPreProd,
        KnownNetwork::CardanoPreview,
    ];

    pub fn from_magic(magic: u64) -> Option<Self> {
        match magic {
            764824073 => Some(KnownNetwork::CardanoMainnet),
            1 => Some(KnownNetwork::CardanoPreProd),
            2 => Some(KnownNetwork::CardanoPreview),
            _ => None,
        }
    }

    pub fn magic(&self) -> u64 {
        match self {
            KnownNetwork::CardanoMainnet => 764824073,
            KnownNetwork::CardanoPreProd => 1,
            KnownNetwork::CardanoPreview => 2,
        }
    }

    pub fn is_testnet(&self) -> bool {
        !matches!(self, KnownNetwork::CardanoMainnet)
    }

    pub fn cardano_foundation_peer_address(&self) -> &'static str {
        match self {
            KnownNetwork::CardanoMainnet => "backbone.mainnet.cardanofoundation.org:3001",
            KnownNetwork::CardanoPreProd => "preprod-node.world.dev.cardano.org:30000",
            KnownNetwork::CardanoPreview => "preview-node.world.dev.cardano.org:30002",
        }
    }

    pub fn demeter_peer_address(&self) -> &'static str {
        match self {
            KnownNetwork::CardanoMainnet => "relay.cnode-m1.demeter.run:3000",
            KnownNetwork::CardanoPreProd => "relay.cnode-m1.demeter.run:3001",
            KnownNetwork::CardanoPreview => "relay.cnode-m1.demeter.run:3002",
        }
    }

    pub fn remote_peer_options(&self) -> Vec<RemotePeerPreset> {
        vec![
            RemotePeerPreset {
                name: "Demeter Relay",
                address: self.demeter_peer_address(),
            },
            RemotePeerPreset {
                name: "CF Relay",
                address: self.cardano_foundation_peer_address(),
            },
        ]
    }

    pub fn load_included_genesis(&self) -> Genesis {
        match self {
            KnownNetwork::CardanoMainnet => include::mainnet::load(),
            KnownNetwork::CardanoPreProd => include::preprod::load(),
            KnownNetwork::CardanoPreview => include::preview::load(),
        }
    }

    pub fn save_included_genesis(&self, root: &Path) -> miette::Result<()> {
        let result = match self {
            KnownNetwork::CardanoMainnet => include::mainnet::save(root),
            KnownNetwork::CardanoPreProd => include::preprod::save(root),
            KnownNetwork::CardanoPreview => include::preview::save(root),
        };

        result.into_diagnostic().context("saving genesis")
    }
}

impl FromStr for KnownNetwork {
    type Err = miette::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mainnet" => Ok(KnownNetwork::CardanoMainnet),
            "preprod" => Ok(KnownNetwork::CardanoPreProd),
            "preview" => Ok(KnownNetwork::CardanoPreview),
            x => Err(miette!("unknown network {x}")),
        }
    }
}

impl Display for KnownNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KnownNetwork::CardanoMainnet => f.write_str("Cardano Mainnet"),
            KnownNetwork::CardanoPreProd => f.write_str("Cardano PreProd"),
            KnownNetwork::CardanoPreview => f.write_str("Cardano Preview"),
            // KnownNetwork::CardanoSanchonet => f.write_str("Cardano SanchoNet"),
        }
    }
}

impl From<&KnownNetwork> for PeerConfig {
    fn from(value: &KnownNetwork) -> Self {
        PeerConfig {
            peer_address: value.demeter_peer_address().into(),
        }
    }
}

impl From<&KnownNetwork> for ChainConfig {
    fn from(value: &KnownNetwork) -> Self {
        ChainConfig::Cardano(CardanoConfig {
            magic: value.magic(),
            is_testnet: value.is_testnet(),
            ..Default::default()
        })
    }
}

impl From<&KnownNetwork> for UpstreamConfig {
    fn from(value: &KnownNetwork) -> Self {
        UpstreamConfig::Peer(value.into())
    }
}

impl From<&KnownNetwork> for GenesisConfig {
    fn from(value: &KnownNetwork) -> Self {
        match value {
            KnownNetwork::CardanoPreview => GenesisConfig {
                force_protocol: Some(6), // Preview network starts at Alonzo
                ..Default::default()
            },
            KnownNetwork::CardanoPreProd => GenesisConfig {
                ..Default::default()
            },
            // KnownNetwork::CardanoSanchonet => todo!(),
            _ => GenesisConfig::default(),
        }
    }
}

impl From<&KnownNetwork> for MithrilConfig {
    fn from(value: &KnownNetwork) -> Self {
        match value {
            KnownNetwork::CardanoMainnet => MithrilConfig {
                aggregator: "https://aggregator.release-mainnet.api.mithril.network/aggregator".into(),
                genesis_key: "5b3139312c36362c3134302c3138352c3133382c31312c3233372c3230372c3235302c3134342c32372c322c3138382c33302c31322c38312c3135352c3230342c31302c3137392c37352c32332c3133382c3139362c3231372c352c31342c32302c35372c37392c33392c3137365d".into(),
                ancillary_key: Some("5b32332c37312c39362c3133332c34372c3235332c3232362c3133362c3233352c35372c3136342c3130362c3138362c322c32312c32392c3132302c3136332c38392c3132312c3137372c3133382c3230382c3133382c3231342c39392c35382c32322c302c35382c332c36395d".into()),
            },
            KnownNetwork::CardanoPreProd => MithrilConfig {
                aggregator: "https://aggregator.release-preprod.api.mithril.network/aggregator".into(),
                genesis_key: "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d".into(),
                ancillary_key: Some("5b3138392c3139322c3231362c3135302c3131342c3231362c3233372c3231302c34352c31382c32312c3139362c3230382c3234362c3134362c322c3235322c3234332c3235312c3139372c32382c3135372c3230342c3134352c33302c31342c3232382c3136382c3132392c38332c3133362c33365d".into()),
            },
            KnownNetwork::CardanoPreview => MithrilConfig {
                aggregator: "https://aggregator.pre-release-preview.api.mithril.network/aggregator".into(),
                genesis_key: "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d".into(),
                ancillary_key: Some("5b3138392c3139322c3231362c3135302c3131342c3231362c3233372c3231302c34352c31382c32312c3139362c3230382c3234362c3134362c322c3235322c3234332c3235312c3139372c32382c3135372c3230342c3134352c33302c31342c3232382c3136382c3132392c38332c3133362c33365d".into()),
            },
        }
    }
}

/// The official stele registry's published read-only user.
///
/// A hardcoded default of the same kind as the relay addresses and the Mithril
/// aggregators above, and here for the same reason: this module is where a
/// generated `dolos.toml` gets everything it points at. Written into the file,
/// so a config says which identity it reads the registry as.
///
/// Issued by the official registry at `oci.stelae.store`.
const OFFICIAL_REGISTRY_USER: &str = "stelae";

// The password that goes with `OFFICIAL_REGISTRY_USER` is
// `dolos_snapshot::node::OFFICIAL_REGISTRY_PASSWORD`, which is where rotating
// the pair is documented.

/// `[stelae]` as a generated config carries it: the official registry's user,
/// and no password.
///
/// Empty only while [`OFFICIAL_REGISTRY_USER`] is, which is why it is a
/// function rather than a `Default` impl on the config type — it asks for the
/// official registry specifically, and would give the honest answer if there
/// were not one.
fn official_stelae() -> StelaeConfig {
    StelaeConfig {
        registry: (!OFFICIAL_REGISTRY_USER.is_empty()).then(|| StelaeRegistryConfig {
            user: Some(OFFICIAL_REGISTRY_USER.to_owned()),
            ..Default::default()
        }),
    }
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

impl From<HistoryPrunningOptions> for Option<u64> {
    fn from(value: HistoryPrunningOptions) -> Self {
        match value {
            HistoryPrunningOptions::KeepEverything => None,
            HistoryPrunningOptions::Keep1Day => Some(24 * 60 * 60),
            HistoryPrunningOptions::Keep1Week => Some(7 * 24 * 60 * 60),
            HistoryPrunningOptions::Keep1Month => Some(30 * 24 * 60 * 60),
            HistoryPrunningOptions::Custom(x) => Some(x),
        }
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
pub struct RemotePeerPreset {
    name: &'static str,
    address: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum RemotePeerChoice {
    Preset(RemotePeerPreset),
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
    #[arg(long)]
    known_network: Option<KnownNetwork>,

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

type IncludeGenesisFiles = Option<KnownNetwork>;

struct ConfigEditor(RootConfig, IncludeGenesisFiles);

impl Default for ConfigEditor {
    fn default() -> Self {
        let editor = Self(
            RootConfig {
                upstream: From::from(&KnownNetwork::CardanoMainnet),
                mithril: Some(From::from(&KnownNetwork::CardanoMainnet)),
                snapshot: Default::default(),
                // Seeded, so a node created here restores from the official
                // stele registry without an operator having to find a
                // credential first. Only on a *fresh* config: an existing
                // `dolos.toml` keeps whatever it carries, because a section
                // an operator removed and one that predates the field look
                // the same from here, and overwriting would undo the first.
                stelae: official_stelae(),
                storage: StorageConfig {
                    version: CURRENT_STORAGE_VERSION,
                    ..Default::default()
                },
                genesis: Default::default(),
                sync: Default::default(),
                submit: Default::default(),
                serve: Default::default(),
                relay: Default::default(),
                retries: Default::default(),
                logging: Default::default(),
                telemetry: Default::default(),
                chain: ChainConfig::from(&KnownNetwork::CardanoMainnet),
            },
            None,
        );

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
    fn apply_known_network(mut self, network: Option<&KnownNetwork>) -> Self {
        if let Some(network) = network {
            self.0.genesis = network.into();
            self.0.upstream = network.into();
            self.0.chain = network.into();
            self.0.mithril = Some(network.into());
            self.1 = Some(network.clone());

            // Add max rollback window for network from Genesis.
            if self.0.sync.max_rollback.is_none() {
                let genesis = network.load_included_genesis();
                self.0.sync.max_rollback = Some(mutable_slots(&genesis));
            }
        }

        self
    }

    fn apply_remote_peer(mut self, value: Option<&String>) -> Self {
        if let Some(remote_peer) = value {
            let config = self.0.upstream.as_peer_mut();

            if let Some(config) = config {
                remote_peer.clone_into(&mut config.peer_address)
            }
        }

        self
    }

    fn apply_history_pruning(mut self, value: HistoryPrunningOptions) -> Self {
        self.0.sync.max_history = value.into();

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
        self.apply_known_network(args.known_network.as_ref())
            .apply_remote_peer(args.remote_peer.as_ref())
            .apply_history_pruning(args.max_chain_history.into())
            .apply_serve_grpc(args.serve_grpc)
            .apply_serve_minibf(args.serve_minibf)
            .apply_serve_minikupo(args.serve_minikupo)
            .apply_serve_trp(args.serve_trp)
            .apply_serve_ouroboros(args.serve_ouroboros)
            .apply_enable_relay(args.enable_relay)
    }

    fn prompt_storage_upgrade(self, unparseable_existing: bool) -> miette::Result<Self> {
        match storage_upgrade(&self.0.storage.version, unparseable_existing) {
            StorageUpgrade::NotNeeded => Ok(self),
            StorageUpgrade::Unreadable => Err(miette!(
                "found a `{CONFIG_FILE}` this dolos cannot read, so the storage it configures \
                 cannot be located. Remove the data directory your existing `{CONFIG_FILE}` \
                 names, then delete the file and run `dolos init` again — see the migration \
                 guide at {MIGRATION_GUIDE_URL}"
            )),
            StorageUpgrade::Offer => {
                let delete = Confirm::new("Your storage is incompatible with the current version. Do you want to delete data and bootstrap?")
                    .with_default(true)
                    .prompt()
                    .into_diagnostic()
                    .context("asking for storage version upgrade")?;

                self.apply_storage_upgrade(delete)
            }
        }
    }

    /// Carry out the operator's answer to the delete-and-bootstrap offer.
    ///
    /// Declining is declining the upgrade: init writes nothing and touches no
    /// data, so the setup stays exactly as it was found and the daemon keeps
    /// refusing it. Recording the new version without performing the migration
    /// would be the tool producing the config/data mismatch the version gate
    /// puts on the operator alone — which is also why the version is assigned
    /// only after the cleanup has succeeded: a cleanup that refuses or fails
    /// ends the init with the editor, and the config on disk, as they were.
    fn apply_storage_upgrade(mut self, delete: bool) -> miette::Result<Self> {
        if !delete {
            return Err(miette!(
                "storage upgrade declined, leaving the existing setup untouched. Dolos will \
                 keep refusing to start against it until the data is re-bootstrapped — see \
                 the migration guide at {MIGRATION_GUIDE_URL}"
            ));
        }

        cleanup_data(&self.0).context("cleaning up data")?;

        self.0.storage.version = CURRENT_STORAGE_VERSION;

        Ok(self)
    }

    fn prompt_known_network(self) -> miette::Result<Self> {
        let options = KnownNetwork::VARIANTS.to_vec();

        let selected = KnownNetwork::from_magic(self.0.chain.magic());

        let starting_cursor = selected
            .and_then(|x| options.iter().position(|y| y.eq(&x)))
            .unwrap_or_default();

        let value = Select::new("Which network are you connecting to?", options)
            .with_starting_cursor(starting_cursor)
            .prompt()
            .into_diagnostic()
            .context("asking for network")?;

        Ok(self.apply_known_network(Some(&value)))
    }

    fn prompt_include_genesis(mut self) -> miette::Result<Self> {
        if let Some(network) = self.1 {
            let value = Confirm::new("Do you want us to provide the genesis files?")
                .with_default(true)
                .prompt()
                .into_diagnostic()
                .context("asking for including genesis")?;

            self.1 = value.then_some(network);
        }

        Ok(self)
    }

    fn prompt_remote_peer(self) -> miette::Result<Self> {
        let current = self.0.upstream.peer_address().unwrap_or_default();

        let Some(network) = KnownNetwork::from_magic(self.0.chain.magic()) else {
            let value = Text::new("Which remote peer (relay) do you want to use?")
                .with_default(current)
                .prompt()
                .into_diagnostic()
                .context("asking for remote peer")?;

            return Ok(self.apply_remote_peer(Some(&value)));
        };

        let mut options: Vec<_> = network
            .remote_peer_options()
            .into_iter()
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

    fn confirm_values(mut self, storage_upgrade: bool) -> miette::Result<ConfigEditor> {
        self = self
            .prompt_storage_upgrade(storage_upgrade)?
            .prompt_known_network()?
            .prompt_include_genesis()?
            .prompt_remote_peer()?
            .prompt_history_pruning()?
            .prompt_serve_apis()?
            .prompt_enable_relay()?;

        Ok(self)
    }

    fn include_genesis_files(self) -> miette::Result<Self> {
        if let Some(network) = &self.1 {
            network.save_included_genesis(&PathBuf::from("./"))?;
        }

        Ok(self)
    }

    fn save(self, path: &Path) -> miette::Result<()> {
        let config = toml::to_string_pretty(&self.0)
            .into_diagnostic()
            .context("serializing config toml")?;

        std::fs::write(path, config)
            .into_diagnostic()
            .context("saving config file")?;

        Ok(())
    }
}

/// Whether a config file is present that this dolos could not read.
///
/// A load that failed with the file on disk is an older setup, not a missing
/// one: a v1.6 `dolos.toml` naming a removed backend, or carrying the retired
/// `block_compression` table, does not parse here. `storage_upgrade` records
/// what follows from that.
fn existing_config_is_unreadable(config: &miette::Result<RootConfig>, config_file: &Path) -> bool {
    config.is_err() && config_file.exists()
}

pub fn run(
    config: miette::Result<RootConfig>,
    args: &Args,
    feedback: &Feedback,
) -> miette::Result<()> {
    crate::banner::print_init_banner();

    let unparseable_existing = existing_config_is_unreadable(&config, Path::new(CONFIG_FILE));

    config
        .map(|x| ConfigEditor(x, None))
        .unwrap_or_default()
        .fill_values_from_args(args)
        .confirm_values(unparseable_existing)?
        .include_genesis_files()?
        .save(&PathBuf::from(CONFIG_FILE))?;

    println!("config saved to {CONFIG_FILE}");

    let config = crate::common::load_config(&None)
        .into_diagnostic()
        .context("parsing configuration")?;

    if let UpstreamConfig::Peer(_) = &config.upstream {
        super::bootstrap::run(&config, &super::bootstrap::Args::default(), feedback)?;
    }

    println!("\nDolos is ready!");
    println!("- run `dolos daemon` to start the node");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_snapshot::restore::progress_path_in;

    /// A fresh config is seeded at the version this binary reads, so the
    /// upgrade prompt does not fire on a node init just created.
    #[test]
    fn a_fresh_config_is_seeded_at_the_current_storage_version() {
        let editor = ConfigEditor::default();

        assert_eq!(editor.0.storage.version, CURRENT_STORAGE_VERSION);
        assert_eq!(
            storage_upgrade(&editor.0.storage.version, false),
            StorageUpgrade::NotNeeded
        );
    }

    /// An older version at a path init can read is the case the offer exists
    /// for; a config that did not parse is not, whatever version the defaults
    /// filled in for it say.
    #[test]
    fn an_unreadable_config_is_never_the_deletable_case() {
        assert_eq!(
            storage_upgrade(&StorageVersion::V3, false),
            StorageUpgrade::Offer
        );
        assert_eq!(
            storage_upgrade(&CURRENT_STORAGE_VERSION, true),
            StorageUpgrade::Unreadable
        );
        assert_eq!(
            storage_upgrade(&StorageVersion::V3, true),
            StorageUpgrade::Unreadable
        );
    }

    /// An editor pointed at `path` as its storage root, at the version a v1.6
    /// setup carries.
    fn editor_over(path: &Path) -> ConfigEditor {
        let mut editor = ConfigEditor::default();
        editor.0.storage.path = path.to_path_buf();
        editor.0.storage.version = StorageVersion::V3;
        editor
    }

    /// Lay out what an older node leaves under `root`: the directory-based
    /// stores with their files nested inside, a block segment, and the
    /// progress file of a restore that never finished. Returns every file
    /// written, so a test can say whether all of it is still there.
    fn old_store_under(root: &Path) -> Vec<PathBuf> {
        let files = vec![
            root.join("wal").join("journal"),
            root.join("state").join("index").join("000001.sst"),
            root.join("archive").join("index").join("000001.sst"),
            root.join("archive").join("000000.segment"),
            progress_path_in(root),
        ];

        for file in &files {
            std::fs::create_dir_all(file.parent().unwrap()).unwrap();
            std::fs::write(file, b"a store built by v1.6").unwrap();
        }

        files
    }

    fn all_present(files: &[PathBuf]) -> bool {
        files.iter().all(|file| file.is_file())
    }

    /// Declining the offer costs the operator nothing: no data removed, and no
    /// version recorded that the data does not back.
    #[test]
    fn declining_the_upgrade_touches_neither_data_nor_version() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let files = old_store_under(&data);

        let error = editor_over(&data)
            .apply_storage_upgrade(false)
            .err()
            .expect("declining aborts the init");

        assert!(
            error.to_string().contains(MIGRATION_GUIDE_URL),
            "the abort must point at the migration guide: {error}"
        );
        assert!(
            all_present(&files),
            "declining must leave the store in place"
        );
    }

    /// Accepting is the only path that deletes. The stores are directories
    /// with files nested inside them, not flat files under the root, and the
    /// wipe has to take all of it — the restore progress file included — and
    /// hand back an empty root for the bootstrap that follows. The version is
    /// recorded only once that has happened.
    #[test]
    fn accepting_the_upgrade_clears_the_nested_stores_and_records_the_version() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let files = old_store_under(&data);

        let editor = editor_over(&data).apply_storage_upgrade(true).unwrap();

        assert!(
            !files.iter().any(|file| file.exists()),
            "accepting must clear every store under the root"
        );
        assert!(data.is_dir(), "the root comes back for the bootstrap");
        assert_eq!(std::fs::read_dir(&data).unwrap().count(), 0);
        assert_eq!(editor.0.storage.version, CURRENT_STORAGE_VERSION);
    }

    /// A store the configuration keeps outside the root would survive a wipe
    /// of the root, and a config recorded at the new version over a surviving
    /// store is the mismatch the gate exists to refuse. So the accept path
    /// stops before deleting anything, names the directory to remove by hand,
    /// and — because the error carries the editor away with it — leaves no
    /// upgraded config to save.
    #[test]
    fn a_store_outside_the_root_aborts_before_anything_is_deleted() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let elsewhere = dir.path().join("elsewhere").join("state");
        let files = old_store_under(&data);

        std::fs::create_dir_all(&elsewhere).unwrap();
        std::fs::write(elsewhere.join("journal"), b"a store built by v1.6").unwrap();

        let mut editor = editor_over(&data);
        let dolos_core::config::StateStoreConfig::Fjall(state) = &mut editor.0.storage.state else {
            panic!("the default state backend is fjall");
        };
        state.path = Some(elsewhere.clone());

        let error = editor
            .apply_storage_upgrade(true)
            .err()
            .expect("a store outside the root aborts the init");

        // The CLI prints the whole chain; `Display` alone is the outer context.
        let message = error
            .chain()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(": ");
        assert!(
            message.contains(&elsewhere.display().to_string()),
            "the abort must name the store to remove by hand: {message}"
        );
        assert!(
            message.contains("by hand"),
            "the abort must ask for manual removal: {message}"
        );
        assert!(all_present(&files), "nothing under the root may be deleted");
        assert!(
            elsewhere.join("journal").is_file(),
            "nothing outside the root may be deleted"
        );
    }

    /// A cleanup that fails partway is an init that fails, not one that
    /// records a version over whatever survived.
    #[test]
    fn a_failed_cleanup_records_no_version() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");

        // A root that is not a directory cannot be cleared as one.
        std::fs::write(&data, b"not a directory").unwrap();

        let error = editor_over(&data)
            .apply_storage_upgrade(true)
            .err()
            .expect("a failed cleanup aborts the init");

        assert!(
            error.to_string().contains("cleaning up data"),
            "the abort must say what failed: {error}"
        );
        assert_eq!(std::fs::read(&data).unwrap(), b"not a directory");
    }

    /// A config init cannot read never reaches the deletion: the store's path
    /// is whatever that config named, and the defaults filled in for it point
    /// somewhere else.
    #[test]
    fn an_unreadable_config_aborts_without_deleting_anything() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let files = old_store_under(&data);

        let error = editor_over(&data)
            .prompt_storage_upgrade(true)
            .err()
            .expect("an unreadable config aborts the init");

        assert!(
            error.to_string().contains(MIGRATION_GUIDE_URL),
            "the abort must point at the migration guide: {error}"
        );
        assert!(
            all_present(&files),
            "an unreadable config must leave every directory alone"
        );
    }

    /// A v1.6 `dolos.toml` over `root`. With `block_compression` the table is
    /// one this release retired, so the file does not parse; without it the
    /// same file is a readable `v3` configuration.
    fn v1_6_config(root: &Path, block_compression: bool) -> String {
        let table = if block_compression {
            "\n[storage.archive.block_compression]\nprofile = \"per-block\"\n"
        } else {
            ""
        };

        format!(
            r#"
[upstream]
peer_address = "unused.example:3001"

[storage]
version = "v3"
path = {path}

[storage.archive]
backend = "fjall"
{table}
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
        )
    }

    /// The whole of what init does with an existing v1.6 setup, driven by the
    /// actual config bytes rather than an editor built for the test.
    ///
    /// A file carrying the retired `block_compression` table does not load,
    /// so init sees an existing config it cannot read: it aborts with the
    /// manual instructions, and neither the file nor the data changes. The
    /// same file without the table loads as `v3` and gets the offer: declining
    /// changes nothing either, and accepting clears the nested stores and
    /// records `v4`.
    #[test]
    fn a_v1_6_config_is_refused_or_upgraded_without_ever_being_guessed_at() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let config_file = dir.path().join(CONFIG_FILE);

        let unreadable = v1_6_config(&data, true);
        std::fs::write(&config_file, &unreadable).unwrap();
        let files = old_store_under(&data);

        let loaded = toml::from_str::<RootConfig>(&unreadable).into_diagnostic();
        let message = loaded
            .as_ref()
            .err()
            .expect("the retired table fails the load")
            .to_string();
        assert!(
            message.contains("block_compression"),
            "the load failure must name the retired table: {message}"
        );

        assert!(existing_config_is_unreadable(&loaded, &config_file));
        assert!(
            !existing_config_is_unreadable(&loaded, &dir.path().join("absent.toml")),
            "a load failure with no file on disk is a fresh init"
        );

        // What `run` builds for that case: an editor filled from defaults,
        // whose storage path is not the one the file names.
        let editor = ConfigEditor::default();
        assert_ne!(editor.0.storage.path, data);

        let error = editor
            .prompt_storage_upgrade(true)
            .err()
            .expect("an unreadable config aborts the init");

        assert!(error.to_string().contains(MIGRATION_GUIDE_URL));
        assert_eq!(std::fs::read_to_string(&config_file).unwrap(), unreadable);
        assert!(all_present(&files), "the abort must delete nothing");

        let readable = v1_6_config(&data, false);
        std::fs::write(&config_file, &readable).unwrap();

        let config: RootConfig = toml::from_str(&readable).unwrap();
        assert_eq!(config.storage.version, StorageVersion::V3);
        assert_eq!(
            storage_upgrade(&config.storage.version, false),
            StorageUpgrade::Offer
        );

        ConfigEditor(config, None)
            .apply_storage_upgrade(false)
            .err()
            .expect("declining aborts the init");

        assert_eq!(std::fs::read_to_string(&config_file).unwrap(), readable);
        assert!(all_present(&files), "declining must delete nothing");

        let config: RootConfig = toml::from_str(&readable).unwrap();
        let upgraded = ConfigEditor(config, None)
            .apply_storage_upgrade(true)
            .unwrap();

        assert!(!files.iter().any(|file| file.exists()));
        assert_eq!(std::fs::read_dir(&data).unwrap().count(), 0);
        assert_eq!(upgraded.0.storage.version, CURRENT_STORAGE_VERSION);
    }

    /// The setup a fresh init writes is a working `v4` one: the saved config
    /// reads back at the current version, the stores open through the same
    /// gate every command uses, and the first block appended lands in a
    /// segment file as a zstd frame with nothing having been configured for
    /// it.
    #[test]
    fn a_fresh_v4_init_appends_compressed_frames() {
        use dolos_core::archive::{ArchiveStore as _, ArchiveWriter as _};
        use dolos_core::{ChainPoint, RawBlock};

        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let config_file = dir.path().join(CONFIG_FILE);

        let mut editor = ConfigEditor::default();
        editor.0.storage.path = data.clone();
        editor.save(&config_file).unwrap();

        let config: RootConfig =
            toml::from_str(&std::fs::read_to_string(&config_file).unwrap()).unwrap();
        assert_eq!(config.storage.version, CURRENT_STORAGE_VERSION);

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

    /// A freshly initialized node carries the stelae registry section the
    /// official registry needs, and no password.
    ///
    /// The section appears once [`OFFICIAL_REGISTRY_USER`] names one; until
    /// then there is no user to write and the file says nothing. Either way the
    /// generated config has to parse and to round-trip to exactly what
    /// [`official_stelae`] is, which is what will still hold on the day the
    /// constants are filled in.
    #[test]
    fn a_fresh_config_seeds_the_official_registry_and_no_password() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dolos.toml");

        ConfigEditor::default().save(&path).unwrap();

        let written = std::fs::read_to_string(&path).unwrap();
        println!("{written}");

        // The password is compiled in, so a generated config never carries one
        // however the constants are set.
        assert!(!written.contains("password"), "{written}");

        let parsed: RootConfig = toml::from_str(&written).expect("the generated config parses");
        assert_eq!(parsed.stelae, official_stelae());

        if let Some(registry) = &parsed.stelae.registry {
            assert_eq!(registry.user.as_deref(), Some(OFFICIAL_REGISTRY_USER));
            assert_eq!(registry.password, None);
        }
    }

    /// A password an operator wrote is kept, so a private registry is
    /// configurable in the file the same command generates.
    ///
    /// Only the round trip is asserted here. What a section with *no* password
    /// authenticates with is [`dolos_snapshot::node::registry_auth`]'s
    /// question, and is answered by its tests against
    /// [`dolos_snapshot::node::OFFICIAL_REGISTRY_PASSWORD`].
    #[test]
    fn a_configured_password_survives_the_round_trip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dolos.toml");

        let mut editor = ConfigEditor::default();
        editor.0.stelae.registry = Some(StelaeRegistryConfig {
            user: Some("dolos".to_owned()),
            password: Some("a-private-registry".to_owned()),
            token: None,
        });

        editor.save(&path).unwrap();

        let written = std::fs::read_to_string(&path).unwrap();
        assert!(written.contains("[stelae.registry]"), "{written}");

        let parsed: RootConfig = toml::from_str(&written).unwrap();
        let registry = parsed.stelae.registry.expect("the section round-trips");

        assert_eq!(registry.user.as_deref(), Some("dolos"));
        assert_eq!(registry.password.as_deref(), Some("a-private-registry"));
    }
}
