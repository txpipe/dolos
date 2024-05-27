use clap::Parser;
use inquire::{Confirm, Select, Text};
use miette::{miette, Context as _, IntoDiagnostic};
use std::{
    fmt::Display,
    path::{Path, PathBuf},
    str::FromStr,
};

mod include;

#[derive(Debug, Clone)]
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
                peer_address: "relays-new.cardano-mainnet.iohk.io:3001".into(),
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

#[derive(Debug, Parser)]
pub struct Args {
    /// Use one of the well-known networks
    #[arg(long)]
    known_network: Option<KnownNetwork>,

    /// Remote peer to use as source
    #[arg(long)]
    remote_peer: Option<String>,

    /// Serve clients via gRPC
    #[arg(long)]
    serve_grpc: Option<bool>,

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
                storage: Default::default(),
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
            self.0.genesis = Default::default();
            self.0.upstream = network.into();
            self.0.mithril = Some(network.into());
            self.1 = Some(network.clone());
        }

        self
    }

    fn apply_remote_peer(mut self, value: Option<&String>) -> Self {
        if let Some(remote_peer) = value {
            self.0.upstream.peer_address = remote_peer.to_owned();
        }

        self
    }

    fn apply_serve_grpc(mut self, value: Option<bool>) -> Self {
        if let Some(value) = value {
            if value {
                self.0.serve.grpc = dolos::serve::grpc::Config {
                    listen_address: "[::]:50051".into(),
                    tls_client_ca_root: None,
                }
                .into();
            } else {
                self.0.serve.grpc = None;
            }
        }

        self
    }

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
            .apply_serve_grpc(args.serve_grpc.clone())
            .apply_serve_ouroboros(args.serve_ouroboros.clone())
            .apply_enable_relay(args.enable_relay.clone())
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
            let value = Confirm::new("Do you to use included genesis files?")
                .with_default(true)
                .prompt()
                .into_diagnostic()
                .context("asking for including genesis")?;

            self.1 = value.then(|| network);
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

    fn prompt_serve_ouroboros(self) -> miette::Result<Self> {
        let value = Confirm::new("Do you want to serve clients via Ouroboros (aka: node socket)?")
            .with_default(self.0.serve.ouroboros.is_some())
            .prompt()
            .into_diagnostic()
            .context("asking for serve ouroboros")?;

        Ok(self.apply_serve_ouroboros(Some(value)))
    }

    fn prompt_enable_relay(self) -> miette::Result<Self> {
        let value = Confirm::new("Do you want to act as a relay for other nodes?")
            .with_default(self.0.relay.is_some())
            .prompt()
            .into_diagnostic()
            .context("asking for relay enabled")?;

        Ok(self.apply_enable_relay(Some(value)))
    }

    fn confirm_values(mut self) -> miette::Result<ConfigEditor> {
        self = self
            .prompt_known_network()?
            .prompt_include_genesis()?
            .prompt_remote_peer()?
            .prompt_serve_grpc()?
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

pub fn run(config: miette::Result<super::Config>, args: &Args) -> miette::Result<()> {
    config
        .map(|x| ConfigEditor(x, None))
        .unwrap_or_default()
        .fill_values_from_args(args)
        .confirm_values()?
        .include_genesis_files()?
        .save(&PathBuf::from("dolos.toml"))?;

    Ok(())
}
