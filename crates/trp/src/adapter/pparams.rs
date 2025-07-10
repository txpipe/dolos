use std::collections::HashMap;

use pallas::ledger::{traverse::MultiEraUpdate, validate::utils::ConwayProtParams};

use dolos_cardano::pparams;
use dolos_core::{Domain, Genesis, StateStore as _};

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

pub fn resolve<D: Domain>(
    genesis: &Genesis,
    ledger: &D::State,
) -> Result<tx3_cardano::PParams, tx3_cardano::Error> {
    let network = network_id_from_genesis(genesis).unwrap();

    let tip = ledger
        .cursor()
        .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

    let updates = ledger
        .get_pparams(tip.as_ref().map(|p| p.slot()).unwrap_or_default())
        .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

    let updates: Vec<_> = updates
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<MultiEraUpdate>, pallas::codec::minicbor::decode::Error>>()
        .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

    let summary = pparams::fold_with_hacks(genesis, &updates, tip.as_ref().unwrap().slot());
    let era = summary.era_for_slot(tip.as_ref().unwrap().slot());

    let out = match &era.pparams {
        pallas::ledger::validate::utils::MultiEraProtocolParameters::Conway(pparams) => {
            tx3_cardano::PParams {
                network,
                cost_models: map_cost_models(pparams),
                min_fee_coefficient: pparams.minfee_a as u64,
                min_fee_constant: pparams.minfee_b as u64,
                coins_per_utxo_byte: pparams.ada_per_utxo_byte,
            }
        }
        _ => todo!("unsupported era for pparams"),
    };

    Ok(out)
}
