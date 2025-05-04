use std::collections::HashMap;

use pallas::ledger::{primitives::NetworkId, traverse::MultiEraUpdate};

use crate::{ledger::pparams, state::LedgerStore};

pub fn network_id_from_genesis(genesis: &pparams::Genesis) -> Option<NetworkId> {
    match genesis.shelley.network_id.as_ref() {
        Some(network) => match network.as_str() {
            "Mainnet" => Some(NetworkId::Mainnet),
            "Testnet" => Some(NetworkId::Testnet),
            _ => None,
        },
        None => None,
    }
}

pub fn resolve(
    genesis: &pparams::Genesis,
    ledger: &LedgerStore,
) -> Result<tx3_cardano::PParams, tx3_cardano::Error> {
    let network = network_id_from_genesis(genesis).unwrap();

    let tip = ledger
        .cursor()
        .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

    let updates = ledger
        .get_pparams(tip.as_ref().map(|p| p.0).unwrap_or_default())
        .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

    let updates: Vec<_> = updates
        .into_iter()
        .map(TryInto::try_into)
        .collect::<Result<Vec<MultiEraUpdate>, pallas::codec::minicbor::decode::Error>>()
        .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

    let summary = pparams::fold_with_hacks(genesis, &updates, tip.as_ref().unwrap().0);
    let era = summary.era_for_slot(tip.as_ref().unwrap().0);

    let out = match &era.pparams {
        pallas::ledger::validate::utils::MultiEraProtocolParameters::Conway(pparams) => {
            tx3_cardano::PParams {
                network,
                cost_models: HashMap::from([
                    (
                        1,
                        pparams
                            .cost_models_for_script_languages
                            .plutus_v1
                            .clone()
                            .unwrap(),
                    ),
                    (
                        2,
                        pparams
                            .cost_models_for_script_languages
                            .plutus_v2
                            .clone()
                            .unwrap(),
                    ),
                    (
                        3,
                        pparams
                            .cost_models_for_script_languages
                            .plutus_v3
                            .clone()
                            .unwrap(),
                    ),
                ]),
                min_fee_coefficient: pparams.minfee_a as u64,
                min_fee_constant: pparams.minfee_b as u64,
                coins_per_utxo_byte: pparams.ada_per_utxo_byte,
            }
        }
        _ => todo!("unsupported era for pparams"),
    };

    Ok(out)
}
