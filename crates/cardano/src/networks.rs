//! The Cardano networks Dolos knows by name, and what it defaults for each.

use std::{fmt::Display, path::Path};

use dolos_core::{
    config::{CardanoConfig, ChainConfig, CustomUtxo},
    ChainError, Genesis,
};

use crate::include;

/// A relay a known network can sync from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peer {
    pub name: &'static str,
    pub address: &'static str,
}

/// The Mithril aggregator that certifies a known network's snapshots.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Mithril {
    pub aggregator: &'static str,
    pub genesis_vkey: &'static str,
    pub ancillary_vkey: Option<&'static str>,
}

/// The official stele repository for a known network and the read-only user
/// it is read as. The user's password is
/// `dolos_snapshot::node::OFFICIAL_REGISTRY_PASSWORD`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stelae {
    pub registry: &'static str,
    pub user: &'static str,
}

#[derive(Debug)]
pub struct KnownNetwork {
    pub name: &'static str,
    pub label: &'static str,
    pub magic: u32,
    pub is_testnet: bool,
    pub default_peer: &'static str,
    /// Relays offered by `dolos init`, the default one first.
    pub peers: &'static [Peer],
    pub mithril: Mithril,
    pub stelae: Stelae,
    load_genesis: fn() -> Genesis,
    save_genesis: fn(&Path) -> std::io::Result<()>,
}

const OFFICIAL_STELAE_USER: &str = "stelae";

const DEMETER_MAINNET: &str = "relay.cnode-m1.demeter.run:3000";
const DEMETER_PREPROD: &str = "relay.cnode-m1.demeter.run:3001";
const DEMETER_PREVIEW: &str = "relay.cnode-m1.demeter.run:3002";

const TESTNET_MITHRIL_GENESIS_VKEY: &str = "5b3132372c37332c3132342c3136312c362c3133372c3133312c3231332c3230372c3131372c3139382c38352c3137362c3139392c3136322c3234312c36382c3132332c3131392c3134352c31332c3233322c3234332c34392c3232392c322c3234392c3230352c3230352c33392c3233352c34345d";
const TESTNET_MITHRIL_ANCILLARY_VKEY: &str = "5b3138392c3139322c3231362c3135302c3131342c3231362c3233372c3231302c34352c31382c32312c3139362c3230382c3234362c3134362c322c3235322c3234332c3235312c3139372c32382c3135372c3230342c3134352c33302c31342c3232382c3136382c3132392c38332c3133362c33365d";

pub static MAINNET: KnownNetwork = KnownNetwork {
    name: "mainnet",
    label: "Cardano Mainnet",
    magic: 764824073,
    is_testnet: false,
    default_peer: DEMETER_MAINNET,
    peers: &[
        Peer {
            name: "Demeter Relay",
            address: DEMETER_MAINNET,
        },
        Peer {
            name: "CF Relay",
            address: "backbone.mainnet.cardanofoundation.org:3001",
        },
    ],
    mithril: Mithril {
        aggregator: "https://aggregator.release-mainnet.api.mithril.network/aggregator",
        genesis_vkey: "5b3139312c36362c3134302c3138352c3133382c31312c3233372c3230372c3235302c3134342c32372c322c3138382c33302c31322c38312c3135352c3230342c31302c3137392c37352c32332c3133382c3139362c3231372c352c31342c32302c35372c37392c33392c3137365d",
        ancillary_vkey: Some("5b32332c37312c39362c3133332c34372c3235332c3232362c3133362c3233352c35372c3136342c3130362c3138362c322c32312c32392c3132302c3136332c38392c3132312c3137372c3133382c3230382c3133382c3231342c39392c35382c32322c302c35382c332c36395d"),
    },
    stelae: Stelae {
        registry: "oci://oci.stelae.store/cardano/mainnet",
        user: OFFICIAL_STELAE_USER,
    },
    load_genesis: include::mainnet::load,
    save_genesis: include::mainnet::save,
};

pub static PREPROD: KnownNetwork = KnownNetwork {
    name: "preprod",
    label: "Cardano PreProd",
    magic: 1,
    is_testnet: true,
    default_peer: DEMETER_PREPROD,
    peers: &[
        Peer {
            name: "Demeter Relay",
            address: DEMETER_PREPROD,
        },
        Peer {
            name: "CF Relay",
            address: "preprod-node.world.dev.cardano.org:30000",
        },
    ],
    mithril: Mithril {
        aggregator: "https://aggregator.release-preprod.api.mithril.network/aggregator",
        genesis_vkey: TESTNET_MITHRIL_GENESIS_VKEY,
        ancillary_vkey: Some(TESTNET_MITHRIL_ANCILLARY_VKEY),
    },
    stelae: Stelae {
        registry: "oci://oci.stelae.store/cardano/preprod",
        user: OFFICIAL_STELAE_USER,
    },
    load_genesis: include::preprod::load,
    save_genesis: include::preprod::save,
};

pub static PREVIEW: KnownNetwork = KnownNetwork {
    name: "preview",
    label: "Cardano Preview",
    magic: 2,
    is_testnet: true,
    default_peer: DEMETER_PREVIEW,
    peers: &[
        Peer {
            name: "Demeter Relay",
            address: DEMETER_PREVIEW,
        },
        Peer {
            name: "CF Relay",
            address: "preview-node.world.dev.cardano.org:30002",
        },
    ],
    mithril: Mithril {
        aggregator: "https://aggregator.pre-release-preview.api.mithril.network/aggregator",
        genesis_vkey: TESTNET_MITHRIL_GENESIS_VKEY,
        ancillary_vkey: Some(TESTNET_MITHRIL_ANCILLARY_VKEY),
    },
    stelae: Stelae {
        registry: "oci://oci.stelae.store/cardano/preview",
        user: OFFICIAL_STELAE_USER,
    },
    load_genesis: include::preview::load,
    save_genesis: include::preview::save,
};

static ALL: [&KnownNetwork; 3] = [&MAINNET, &PREPROD, &PREVIEW];

impl KnownNetwork {
    pub fn all() -> &'static [&'static KnownNetwork] {
        &ALL
    }

    /// Case-insensitive, as an operator types it.
    pub fn by_name(name: &str) -> Option<&'static KnownNetwork> {
        ALL.iter()
            .copied()
            .find(|network| network.name.eq_ignore_ascii_case(name))
    }

    pub fn by_magic(magic: u64) -> Option<&'static KnownNetwork> {
        ALL.iter()
            .copied()
            .find(|network| u64::from(network.magic) == magic)
    }

    /// The embedded genesis.
    pub fn genesis(&self) -> Genesis {
        (self.load_genesis)()
    }

    /// Writes the embedded genesis files into `root`.
    pub fn save_genesis(&self, root: &Path) -> std::io::Result<()> {
        (self.save_genesis)(root)
    }
}

impl Display for KnownNetwork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.label)
    }
}

/// Checks a custom network's magic against its Shelley genesis.
pub fn validate_custom(magic: u32, genesis: &Genesis) -> Result<(), ChainError> {
    let genesis_magic = genesis
        .shelley
        .network_magic
        .ok_or_else(|| ChainError::GenesisFieldMissing("networkMagic".to_string()))?;

    if magic != genesis_magic {
        return Err(ChainError::NetworkMagicMismatch {
            magic,
            genesis: genesis_magic,
        });
    }

    Ok(())
}

/// A configured `[chain]`: a network from the catalog, or a custom one.
#[derive(Debug, Clone, Copy)]
pub enum ResolvedNetwork<'a> {
    Known(&'static KnownNetwork),
    Custom(&'a CardanoConfig),
}

impl<'a> ResolvedNetwork<'a> {
    /// Looks `chain.network` up in the catalog; an unknown name lists it.
    pub fn resolve(chain: &'a ChainConfig) -> Result<Self, ChainError> {
        if let Some(cardano) = chain.cardano.as_ref().filter(|_| chain.is_custom()) {
            return Ok(Self::Custom(cardano));
        }

        KnownNetwork::by_name(&chain.network)
            .map(Self::Known)
            .ok_or_else(|| ChainError::UnknownNetwork {
                name: chain.network.clone(),
                known: KnownNetwork::all()
                    .iter()
                    .map(|network| format!("`{}`", network.name))
                    .collect::<Vec<_>>()
                    .join(", "),
            })
    }

    pub fn known(&self) -> Option<&'static KnownNetwork> {
        match self {
            Self::Known(network) => Some(network),
            Self::Custom(_) => None,
        }
    }

    pub fn magic(&self) -> u64 {
        match self {
            Self::Known(network) => network.magic.into(),
            Self::Custom(cardano) => cardano.magic.into(),
        }
    }

    pub fn is_testnet(&self) -> bool {
        match self {
            Self::Known(network) => network.is_testnet,
            Self::Custom(cardano) => cardano.is_testnet,
        }
    }

    /// The peer an `[upstream]` without `peer_address` syncs from.
    pub fn default_peer(&self) -> Option<&'static str> {
        self.known().map(|network| network.default_peer)
    }

    pub fn custom_utxos(&self) -> &'a [CustomUtxo] {
        match self {
            Self::Known(_) => &[],
            Self::Custom(cardano) => &cardano.custom_utxos,
        }
    }

    /// The embedded genesis, or a custom network's files checked against its
    /// magic.
    pub fn genesis(&self) -> Result<Genesis, ChainError> {
        match self {
            Self::Known(network) => Ok(network.genesis()),
            Self::Custom(cardano) => {
                let files = &cardano.genesis;
                let genesis = Genesis::from_file_paths(
                    &files.byron_path,
                    &files.shelley_path,
                    &files.alonzo_path,
                    &files.conway_path,
                    files.force_protocol,
                )
                .map_err(ChainError::GenesisFiles)?;

                validate_custom(cardano.magic, &genesis)?;

                Ok(genesis)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalog_magic_matches_embedded_genesis() {
        for network in KnownNetwork::all() {
            let genesis = network.genesis();

            assert_eq!(
                genesis.shelley.network_magic,
                Some(network.magic),
                "{}",
                network.name
            );
            validate_custom(network.magic, &genesis).unwrap();
        }
    }

    #[test]
    fn lookups_resolve_every_network() {
        for network in KnownNetwork::all() {
            assert!(std::ptr::eq(
                KnownNetwork::by_name(network.name).unwrap(),
                *network
            ));
            assert!(std::ptr::eq(
                KnownNetwork::by_magic(network.magic.into()).unwrap(),
                *network
            ));
            assert_eq!(network.peers[0].address, network.default_peer);
        }

        assert!(std::ptr::eq(
            KnownNetwork::by_name("PreProd").unwrap(),
            &PREPROD
        ));
        assert!(KnownNetwork::by_name("devnet").is_none());
        assert!(KnownNetwork::by_magic(42).is_none());
    }

    #[test]
    fn validate_custom_rejects_a_mismatched_magic() {
        let genesis = PREVIEW.genesis();

        let err = validate_custom(1, &genesis).unwrap_err();

        assert!(matches!(
            err,
            ChainError::NetworkMagicMismatch {
                magic: 1,
                genesis: 2
            }
        ));
        assert_eq!(
            err.to_string(),
            "network magic 1 does not match the Shelley genesis networkMagic 2"
        );
    }

    #[test]
    fn preview_genesis_starts_at_protocol_six() {
        assert_eq!(PREVIEW.genesis().starting_protocol().unwrap(), Some(6));
        assert_eq!(MAINNET.genesis().starting_protocol().unwrap(), None);
        assert_eq!(PREPROD.genesis().starting_protocol().unwrap(), None);
        assert_eq!(
            include::devnet::load().starting_protocol().unwrap(),
            Some(9)
        );
    }

    #[test]
    fn explicit_force_protocol_wins() {
        let mut genesis = PREVIEW.genesis();
        genesis.force_protocol = Some(4);

        assert_eq!(genesis.starting_protocol().unwrap(), Some(4));
    }

    #[test]
    fn shelley_major_above_two_is_a_fast_forwarded_start() {
        let mut genesis = PREVIEW.genesis();

        genesis.shelley.protocol_params.protocol_version.major = 2;
        assert_eq!(genesis.starting_protocol().unwrap(), None);

        genesis.shelley.protocol_params.protocol_version.major = 3;
        assert_eq!(genesis.starting_protocol().unwrap(), Some(3));
    }

    #[test]
    fn starting_protocol_out_of_range_is_a_typed_error() {
        let mut genesis = PREVIEW.genesis();
        genesis.shelley.protocol_params.protocol_version.major = 65536;

        assert!(matches!(
            genesis.starting_protocol(),
            Err(ChainError::UnsupportedStartingProtocol(65536))
        ));
    }
}
