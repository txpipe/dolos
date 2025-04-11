use std::path::Path;

use dolos::ledger::{mutable_slots, pparams::Genesis};
use miette::{Context, IntoDiagnostic};

mod devnet;
mod mainnet;
mod preprod;
mod preview;

fn save_one(root: &Path, name: &str, contents: &[u8]) -> miette::Result<()> {
    std::fs::write(root.join(name), contents)
        .into_diagnostic()
        .context("saving genesis file")
}

pub fn save_genesis_configs(root: &Path, network: &super::KnownNetwork) -> miette::Result<()> {
    match network {
        super::KnownNetwork::CardanoMainnet => {
            save_one(root, "byron.json", mainnet::BYRON)?;
            save_one(root, "shelley.json", mainnet::SHELLEY)?;
            save_one(root, "alonzo.json", mainnet::ALONZO)?;
            save_one(root, "conway.json", mainnet::CONWAY)?;
        }
        super::KnownNetwork::CardanoPreProd => {
            save_one(root, "byron.json", preprod::BYRON)?;
            save_one(root, "shelley.json", preprod::SHELLEY)?;
            save_one(root, "alonzo.json", preprod::ALONZO)?;
            save_one(root, "conway.json", preprod::CONWAY)?;
        }
        super::KnownNetwork::CardanoPreview => {
            save_one(root, "byron.json", preview::BYRON)?;
            save_one(root, "shelley.json", preview::SHELLEY)?;
            save_one(root, "alonzo.json", preview::ALONZO)?;
            save_one(root, "conway.json", preview::CONWAY)?;
        }
        super::KnownNetwork::Devnet => {
            save_one(root, "byron.json", devnet::BYRON)?;
            save_one(root, "shelley.json", devnet::SHELLEY)?;
            save_one(root, "alonzo.json", devnet::ALONZO)?;
            save_one(root, "conway.json", devnet::CONWAY)?;
            save_one(root, "skey", devnet::SKEY)?;
            save_one(root, "vkey", devnet::VKEY)?;
        }
    }

    Ok(())
}

pub fn network_mutable_slots(network: &super::KnownNetwork) -> u64 {
    let genesis = match network {
        super::KnownNetwork::CardanoMainnet => Genesis {
            alonzo: serde_json::from_slice(mainnet::ALONZO).unwrap(),
            conway: serde_json::from_slice(mainnet::CONWAY).unwrap(),
            byron: serde_json::from_slice(mainnet::BYRON).unwrap(),
            shelley: serde_json::from_slice(mainnet::SHELLEY).unwrap(),
            force_protocol: None,
        },
        super::KnownNetwork::CardanoPreProd => Genesis {
            alonzo: serde_json::from_slice(preprod::ALONZO).unwrap(),
            conway: serde_json::from_slice(preprod::CONWAY).unwrap(),
            byron: serde_json::from_slice(preprod::BYRON).unwrap(),
            shelley: serde_json::from_slice(preprod::SHELLEY).unwrap(),
            force_protocol: None,
        },
        super::KnownNetwork::CardanoPreview => Genesis {
            alonzo: serde_json::from_slice(preview::ALONZO).unwrap(),
            conway: serde_json::from_slice(preview::CONWAY).unwrap(),
            byron: serde_json::from_slice(preview::BYRON).unwrap(),
            shelley: serde_json::from_slice(preview::SHELLEY).unwrap(),
            force_protocol: Some(6),
        },
        super::KnownNetwork::Devnet => Genesis {
            alonzo: serde_json::from_slice(preview::ALONZO).unwrap(),
            conway: serde_json::from_slice(preview::CONWAY).unwrap(),
            byron: serde_json::from_slice(preview::BYRON).unwrap(),
            shelley: serde_json::from_slice(preview::SHELLEY).unwrap(),
            force_protocol: Some(10),
        },
    };
    mutable_slots(&genesis)
}
