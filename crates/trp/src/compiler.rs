use pallas::ledger::primitives::conway::CostModels;
use std::collections::HashMap;

use dolos_core::{Domain, Genesis};

use crate::{Facade, Config, Error};

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

fn map_cost_models(original: CostModels) -> HashMap<u8, tx3_cardano::CostModel> {
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

pub fn load_compiler<D: Domain>(
    domain: &Facade<D>,
    config: &Config,
) -> Result<tx3_cardano::Compiler, Error> {
    let network = network_id_from_genesis(domain.genesis()).unwrap();

    let pparams = domain.get_pparams()?;

    let costs = pparams
        .cost_models_for_script_languages()
        .ok_or(Error::PParamsNotAvailable)?;

    let chain_tip = domain.get_chain_tip()?;

    let slot_config = domain.get_slot_config()?;

    let compiler = tx3_cardano::Compiler::new(
        tx3_cardano::Config {
            extra_fees: config.extra_fees,
        },
        network,
        chain_tip,
        pparams,
        map_cost_models(costs),
        slot_config,
    );

    Ok(compiler)
}
