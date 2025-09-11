use std::collections::HashMap;

use pallas::ledger::validate::utils::{ConwayProtParams, MultiEraProtocolParameters};

use dolos_cardano::pparams;
use dolos_core::{ChainPoint, Domain, Genesis, StateStore as _};

use crate::{Config, Error};

pub fn network_id_from_genesis(genesis: &Genesis) -> Option<tx3_cardano::Network> {
    match genesis.shelley.network_id.as_ref() {
        Some(network) => match network.as_str() {
            "Mainnet" => Some(tx3_cardano::Network::Mainnet),
            "Testnet" => Some(tx3_cardano::Network::Testnet),
            _ => None,
        },
        None => None,
    }
}

fn map_cost_models(pparams: &ConwayProtParams) -> HashMap<u8, tx3_cardano::CostModel> {
    let original = pparams.cost_models_for_script_languages.clone();

    let present: Vec<(u8, tx3_cardano::CostModel)> = [
        original.plutus_v1.map(|x| (0, x)),
        original.plutus_v2.map(|x| (1, x)),
        original.plutus_v3.map(|x| (2, x)),
    ]
    .into_iter()
    .flatten()
    .collect();

    HashMap::from_iter(present)
}

fn build_pparams<D: Domain>(
    genesis: &Genesis,
    ledger: &D::State,
) -> Result<tx3_cardano::PParams, Error> {
    let network = network_id_from_genesis(genesis).unwrap();

    let tip = ledger.cursor()?;

    let updates = ledger.get_pparams(tip.as_ref().map(|p| p.slot()).unwrap_or_default())?;

    let updates: Vec<_> = updates
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<_, _>>()?;

    let summary = pparams::fold_with_hacks(genesis, &updates, tip.as_ref().unwrap().slot());
    let era = summary.era_for_slot(tip.as_ref().unwrap().slot());

    let out = match &era.pparams {
        MultiEraProtocolParameters::Conway(pparams) => tx3_cardano::PParams {
            network,
            cost_models: map_cost_models(pparams),
            min_fee_coefficient: pparams.minfee_a as u64,
            min_fee_constant: pparams.minfee_b as u64,
            coins_per_utxo_byte: pparams.ada_per_utxo_byte,
        },
        MultiEraProtocolParameters::Byron(_) => {
            return Err(Error::UnsupportedEra {
                era: "Byron".to_string(),
            })
        }
        MultiEraProtocolParameters::Shelley(_) => {
            return Err(Error::UnsupportedEra {
                era: "Shelley".to_string(),
            })
        }
        MultiEraProtocolParameters::Alonzo(_) => {
            return Err(Error::UnsupportedEra {
                era: "Alonzo".to_string(),
            })
        }
        MultiEraProtocolParameters::Babbage(_) => {
            return Err(Error::UnsupportedEra {
                era: "Babbage".to_string(),
            })
        }
        _ => {
            return Err(Error::UnsupportedEra {
                era: "Unknown".to_string(),
            })
        }
    };

    Ok(out)
}
pub fn load_compiler<D: Domain>(
    genesis: &Genesis,
    ledger: &D::State,
    config: &Config,
) -> Result<tx3_cardano::Compiler, Error> {
    let pparams = build_pparams::<D>(genesis, ledger)?;

    let cursor = ledger.cursor()?.ok_or(Error::TipNotResolved)?;

    let tip = match cursor {
        ChainPoint::Specific(slot, hash) => tx3_cardano::ChainPoint {
            slot,
            hash: hash.to_vec(),
        },
        ChainPoint::Origin => return Err(Error::TipNotResolved),
    };

    let compiler = tx3_cardano::Compiler::new(
        pparams,
        tx3_cardano::Config {
            extra_fees: config.extra_fees,
        },
        tip,
    );

    Ok(compiler)
}
