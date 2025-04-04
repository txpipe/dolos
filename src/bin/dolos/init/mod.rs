use clap::Parser;
use dolos::model::StorageVersion;
use include::network_mutable_slots;
use inquire::{Confirm, Select, Text};
use miette::{miette, Context as _, IntoDiagnostic};
use std::{
    fmt::Display,
    path::{Path, PathBuf},
    str::FromStr,
};

use crate::{common::cleanup_data, feedback::Feedback};

mod include;

#[derive(Debug, Clone)]
#[non_exhaustive]
#[allow(clippy::enum_variant_names)]
pub enum KnownNetwork {
    CardanoMainnet,
    CardanoPreProd,
    CardanoPreview,
    // CardanoSanchonet,
}

impl KnownNetwork {
    const VARIANTS: &'static [KnownNetwork] = &[
        KnownNetwork::CardanoMainnet,
        KnownNetwork::CardanoPreProd,
        KnownNetwork::CardanoPreview,
        // KnownNetwork::CardanoSanchonet,
    ];
}

impl FromStr for KnownNetwork {
    type Err = miette::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "mainnet" => Ok(KnownNetwork::CardanoMainnet),
            "preprod" => Ok(KnownNetwork::CardanoPreProd),
            "preview" => Ok(KnownNetwork::CardanoPreview),
            // "sanchonet" => Ok(KnownNetwork::CardanoSanchonet),
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

impl From<&KnownNetwork> for dolos::model::UpstreamConfig {
    fn from(value: &KnownNetwork) -> Self {
        match value {
            KnownNetwork::CardanoMainnet => dolos::model::UpstreamConfig {
                peer_address: "backbone.mainnet.cardanofoundation.org:3001".into(),
                network_magic: 764824073,
                is_testnet: false,
            },
            KnownNetwork::CardanoPreProd => dolos::model::UpstreamConfig {
                peer_address: "preprod-node.world.dev.cardano.org:30000".into(),
                network_magic: 1,
                is_testnet: true,
            },
            KnownNetwork::CardanoPreview => dolos::model::UpstreamConfig {
                peer_address: "preview-node.world.dev.cardano.org:30002".into(),
                network_magic: 2,
                is_testnet: true,
            },
            // KnownNetwork::CardanoSanchonet => todo!(),
        }
    }
}

impl From<&KnownNetwork> for crate::GenesisConfig {
    fn from(value: &KnownNetwork) -> Self {
        match value {
            KnownNetwork::CardanoPreview => crate::GenesisConfig {
                force_protocol: Some(6), // Preview network starts at Alonzo
                ..Default::default()
            },
            // KnownNetwork::CardanoSanchonet => todo!(),
            _ => crate::GenesisConfig::default(),
        }
    }
}

impl From<&KnownNetwork> for crate::MithrilConfig {
    fn from(value: &KnownNetwork) -> Self {
        match value {
            KnownNetwork::CardanoMainnet => crate::MithrilConfig {
                aggregator: "https://aggregator.release-mainnet.api.mithril.network/aggregator".into(),
                genesis_key: "5b3139312c36362c3134302c3138352c3133382c31312c3233372c3230372c3235302c3134342c32372c322c3138382c33302c31322c38312c3135352c3230342c31302c3137392c37352c32332c3133382c3139362c3231372c352c31342c32302c35372c37392c33392c3137365d".into(),
            },
            KnownNetwork::CardanoPreProd => crate::MithrilConfig {
                aggregator: "https://aggregator.release-preprod.api.mithril.network/aggregator".into(),
                genesis_key: "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d".into()
            },
            KnownNetwork::CardanoPreview => crate::MithrilConfig {
                aggregator: "https://aggregator.pre-release-preview.api.mithril.network/aggregator".into(),
                genesis_key: "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d".into(),
            },
            // KnownNetwork::CardanoSanchonet => crate::MithrilConfig {
            //     aggregator: todo!(),
            //     genesis_key: todo!(),
            // },
        }
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
            Self::Custom(x) => write!(f, "{} slots", x),
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

#[derive(Debug, Parser)]
pub struct Args {
    /// Use one of the well-known networks
    #[arg(long)]
    known_network: Option<KnownNetwork>,

    /// Remote peer to use as source
    #[arg(long)]
    remote_peer: Option<String>,

    /// How much history of the chain to keep in disk
    #[arg(long)]
    max_chain_history: Option<u64>,

    /// Serve clients via gRPC
    #[arg(long)]
    serve_grpc: Option<bool>,

    /// Serve clients minibf via HTTP
    #[arg(long)]
    serve_minibf: Option<bool>,

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

struct ConfigEditor(crate::Config, IncludeGenesisFiles);

impl Default for ConfigEditor {
    fn default() -> Self {
        Self(
            crate::Config {
                upstream: From::from(&KnownNetwork::CardanoMainnet),
                mithril: Some(From::from(&KnownNetwork::CardanoMainnet)),
                snapshot: Default::default(),
                storage: dolos::model::StorageConfig {
                    version: dolos::model::StorageVersion::V1,
                    ..Default::default()
                },
                genesis: Default::default(),
                sync: Default::default(),
                submit: Default::default(),
                serve: Default::default(),
                relay: Default::default(),
                retries: Default::default(),
                logging: Default::default(),
            },
            None,
        )
    }
}

impl ConfigEditor {
    fn apply_known_network(mut self, network: Option<&KnownNetwork>) -> Self {
        if let Some(network) = network {
            self.0.genesis = network.into();
            self.0.upstream = network.into();
            self.0.mithril = Some(network.into());
            self.1 = Some(network.clone());

            // Add max wall history for network from Genesis.
            self.0.storage.max_wal_history = Some(network_mutable_slots(network));
        }
        self
    }

    fn apply_remote_peer(mut self, value: Option<&String>) -> Self {
        if let Some(remote_peer) = value {
            remote_peer.clone_into(&mut self.0.upstream.peer_address)
        }

        self
    }

    fn apply_history_pruning(mut self, value: HistoryPrunningOptions) -> Self {
        self.0.storage.max_chain_history = value.into();

        self
    }

    fn apply_serve_grpc(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                self.0.serve.grpc = dolos::serve::grpc::Config {
                    listen_address: "[::]:50051".into(),
                    tls_client_ca_root: None,
                    permissive_cors: Some(true),
                }
                .into();
            } else {
                self.0.serve.grpc = None;
            }
        }

        self
    }

    fn apply_serve_minibf(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                self.0.serve.minibf = dolos::serve::minibf::Config {
                    listen_address: "[::]:3000".parse().unwrap(),
                }
                .into();
            } else {
                self.0.serve.minibf = None;
            }
        }

        self
    }

    fn apply_serve_trp(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                self.0.serve.trp = dolos::serve::trp::Config {
                    listen_address: "[::]:8000".parse().unwrap(),
                    max_optimize_rounds: 10,
                }
                .into();
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
                self.0.serve.ouroboros = dolos::serve::o7s::Config {
                    listen_path: "dolos.socket".into(),
                    magic: self.0.upstream.network_magic,
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
                self.0.relay = dolos::relay::Config {
                    listen_address: "[::]:30031".into(),
                    magic: self.0.upstream.network_magic,
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
            .apply_serve_trp(args.serve_trp)
            .apply_serve_ouroboros(args.serve_ouroboros)
            .apply_enable_relay(args.enable_relay)
    }

    fn prompt_storage_upgrade(mut self) -> miette::Result<Self> {
        if self.0.storage.version == StorageVersion::V0 {
            self.0.storage.version = StorageVersion::V1;
            let delete = Confirm::new("Your storage is incompatible with current version. Do you want to delete data and bootstrap?")
                .with_default(true)
                .prompt()
                .into_diagnostic()
                .context("asking for storage version upgrade")?;

            if delete {
                cleanup_data(&self.0)
                    .into_diagnostic()
                    .context("cleaning up data")?;
            }
        }

        Ok(self)
    }

    fn prompt_known_network(self) -> miette::Result<Self> {
        let value = Select::new(
            "Which network are you connecting to?",
            KnownNetwork::VARIANTS.to_vec(),
        )
        .prompt()
        .into_diagnostic()
        .context("asking for network")?;

        Ok(self.apply_known_network(Some(&value)))
    }

    fn prompt_include_genesis(mut self) -> miette::Result<Self> {
        if let Some(network) = self.1 {
            let value = Confirm::new("Do you want to use included genesis files?")
                .with_default(true)
                .prompt()
                .into_diagnostic()
                .context("asking for including genesis")?;

            self.1 = value.then_some(network);
        }

        Ok(self)
    }

    fn prompt_remote_peer(self) -> miette::Result<Self> {
        let value = Text::new("Which remote peer (relay) do you want to use?")
            .with_default(&self.0.upstream.peer_address)
            .prompt()
            .into_diagnostic()
            .context("asking for remote peer")?;

        Ok(self.apply_remote_peer(Some(&value)))
    }

    fn prompt_serve_grpc(self) -> miette::Result<Self> {
        let value = Confirm::new("Do you want to serve clients via gRPC?")
            .with_default(self.0.serve.grpc.is_some())
            .prompt()
            .into_diagnostic()
            .context("asking for serve grpc")?;

        Ok(self.apply_serve_grpc(Some(value)))
    }

    fn prompt_serve_minibf(self) -> miette::Result<Self> {
        let value =
            Confirm::new("Do you want to serve clients via a Blockfrost-like HTTP endpoint?")
                .with_default(self.0.serve.minibf.is_some())
                .prompt()
                .into_diagnostic()
                .context("asking for serve http")?;

        Ok(self.apply_serve_minibf(Some(value)))
    }

    fn prompt_serve_trp(self) -> miette::Result<Self> {
        let value = Confirm::new("Do you want to serve clients a TRP endpoint?")
            .with_default(self.0.serve.trp.is_some())
            .prompt()
            .into_diagnostic()
            .context("asking for serve trp")?;

        Ok(self.apply_serve_trp(Some(value)))
    }

    #[cfg(unix)]
    fn prompt_serve_ouroboros(self) -> miette::Result<Self> {
        let value = Confirm::new("Do you want to serve clients via Ouroboros (aka: node socket)?")
            .with_default(self.0.serve.ouroboros.is_some())
            .prompt()
            .into_diagnostic()
            .context("asking for serve ouroboros")?;

        Ok(self.apply_serve_ouroboros(Some(value)))
    }

    #[cfg(windows)]
    fn prompt_serve_ouroboros(self) -> miette::Result<Self> {
        // skip for windows
        Ok(self)
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
            "How much history of the chain do you want to keep in disk?",
            HistoryPrunningOptions::VARIANTS.to_vec(),
        )
        .prompt()
        .into_diagnostic()
        .context("asking for history pruning")?;

        Ok(self.apply_history_pruning(value))
    }

    fn confirm_values(mut self) -> miette::Result<ConfigEditor> {
        self = self
            .prompt_storage_upgrade()?
            .prompt_known_network()?
            .prompt_include_genesis()?
            .prompt_remote_peer()?
            .prompt_history_pruning()?
            .prompt_serve_grpc()?
            .prompt_serve_minibf()?
            .prompt_serve_trp()?
            .prompt_serve_ouroboros()?
            .prompt_enable_relay()?;

        Ok(self)
    }

    fn include_genesis_files(self) -> miette::Result<Self> {
        if let Some(network) = &self.1 {
            include::save_genesis_configs(&PathBuf::from("./"), network)?;
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

pub fn run(
    config: miette::Result<super::Config>,
    args: &Args,
    feedback: &Feedback,
) -> miette::Result<()> {
    config
        .map(|x| ConfigEditor(x, None))
        .unwrap_or_default()
        .fill_values_from_args(args)
        .confirm_values()?
        .include_genesis_files()?
        .save(&PathBuf::from("dolos.toml"))?;

    println!("config saved to dolos.toml");

    let config = super::Config::new(&None)
        .into_diagnostic()
        .context("parsing configuration")?;

    super::bootstrap::run(&config, &super::bootstrap::Args::default(), feedback)?;

    println!("\nDolos is ready!");
    println!("- run `dolos daemon` to start the node");

    Ok(())
}
