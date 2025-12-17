use pallas::ledger::primitives::conway::CostModels;
use std::collections::HashMap;

use dolos_core::{config::TrpConfig, Domain, Genesis, StateStore};

use crate::Error;

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

fn build_pparams<D: Domain>(domain: &D) -> Result<tx3_cardano::PParams, Error> {
    let network = network_id_from_genesis(&domain.genesis()).unwrap();

    let pparams = dolos_cardano::load_effective_pparams::<D>(domain.state())
        .map_err(|_| Error::PParamsNotAvailable)?;

    let costs = pparams.cost_models_for_script_languages();

    let out = tx3_cardano::PParams {
        network,
        cost_models: map_cost_models(costs),
        min_fee_coefficient: pparams.min_fee_a_or_default() as u64,
        min_fee_constant: pparams.min_fee_b_or_default() as u64,
        coins_per_utxo_byte: pparams.ada_per_utxo_byte_or_default() as u64,
    };

    Ok(out)
}

pub fn find_cursor<D: Domain>(domain: &D) -> Result<tx3_cardano::ChainPoint, Error> {
    let cursor = domain
        .state()
        .read_cursor()
        .map_err(|e| Error::InternalError(e.to_string()))?
        .unwrap_or(dolos_core::ChainPoint::Origin);

    let (_, era) = dolos_cardano::load_active_era::<D>(domain.state())?;

    Ok(tx3_cardano::ChainPoint {
        slot: cursor.slot(),
        hash: cursor.hash().map(|h| h.to_vec()).unwrap_or_default(),
        timestamp: era.slot_time(cursor.slot()) as u128,
    })
}

pub fn load_compiler<D: Domain>(
    domain: &D,
    config: &TrpConfig,
) -> Result<tx3_cardano::Compiler, Error> {
    let pparams = build_pparams::<D>(domain)?;

    let cursor = find_cursor(domain)?;

    let compiler = tx3_cardano::Compiler::new(
        pparams,
        tx3_cardano::Config {
            extra_fees: config.extra_fees,
        },
        cursor,
    );

    Ok(compiler)
}
