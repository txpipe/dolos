use std::path::Path;

use miette::{Context, IntoDiagnostic};

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
        }
        super::KnownNetwork::CardanoPreProd => {
            save_one(root, "byron.json", preprod::BYRON)?;
            save_one(root, "shelley.json", preprod::SHELLEY)?;
            save_one(root, "alonzo.json", preprod::ALONZO)?;
        }
        super::KnownNetwork::CardanoPreview => {
            save_one(root, "byron.json", preview::BYRON)?;
            save_one(root, "shelley.json", preview::SHELLEY)?;
            save_one(root, "alonzo.json", preview::ALONZO)?;
        }
    }

    Ok(())
}
