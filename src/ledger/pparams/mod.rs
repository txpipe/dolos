use pallas::{
    applying::utils::{
        AlonzoProtParams, BabbageProtParams, ByronProtParams, ConwayProtParams,
        MultiEraProtocolParameters, ShelleyProtParams,
    },
    ledger::{
        configs::{alonzo, byron, shelley},
        primitives::alonzo::Language,
        traverse::MultiEraUpdate,
    },
};
use tracing::{trace, warn};

pub struct Genesis<'a> {
    pub byron: &'a byron::GenesisFile,
    pub shelley: &'a shelley::GenesisFile,
    pub alonzo: &'a alonzo::GenesisFile,
}

fn bootstrap_byron_pparams(byron: &byron::GenesisFile) -> ByronProtParams {
    ByronProtParams {
        block_version: (0, 0, 0),
        summand: byron.block_version_data.tx_fee_policy.summand,
        multiplier: byron.block_version_data.tx_fee_policy.multiplier,
        max_tx_size: byron.block_version_data.max_tx_size,
        script_version: byron.block_version_data.script_version,
        slot_duration: byron.block_version_data.slot_duration,
        max_block_size: byron.block_version_data.max_block_size,
        max_header_size: byron.block_version_data.max_header_size,
        max_proposal_size: byron.block_version_data.max_proposal_size,
        mpc_thd: byron.block_version_data.mpc_thd,
        heavy_del_thd: byron.block_version_data.heavy_del_thd,
        update_vote_thd: byron.block_version_data.update_vote_thd,
        update_proposal_thd: byron.block_version_data.update_proposal_thd,
        update_implicit: byron.block_version_data.update_implicit,
        soft_fork_rule: byron.block_version_data.softfork_rule.clone().into(),
        unlock_stake_epoch: byron.block_version_data.unlock_stake_epoch,
    }
}

fn bootstrap_shelley_pparams(shelley: &shelley::GenesisFile) -> ShelleyProtParams {
    ShelleyProtParams {
        protocol_version: shelley.protocol_params.protocol_version.clone().into(),
        max_block_body_size: shelley.protocol_params.max_block_body_size,
        max_transaction_size: shelley.protocol_params.max_tx_size,
        max_block_header_size: shelley.protocol_params.max_block_header_size,
        key_deposit: shelley.protocol_params.key_deposit,
        min_utxo_value: shelley.protocol_params.min_utxo_value,
        minfee_a: shelley.protocol_params.min_fee_a,
        minfee_b: shelley.protocol_params.min_fee_b,
        pool_deposit: shelley.protocol_params.pool_deposit,
        desired_number_of_stake_pools: shelley.protocol_params.n_opt,
        min_pool_cost: shelley.protocol_params.min_pool_cost,
        expansion_rate: shelley.protocol_params.rho.clone(),
        treasury_growth_rate: shelley.protocol_params.tau.clone(),
        maximum_epoch: shelley.protocol_params.e_max,
        pool_pledge_influence: shelley.protocol_params.a0.clone(),
        decentralization_constant: shelley.protocol_params.decentralisation_param.clone(),
        extra_entropy: shelley.protocol_params.extra_entropy.clone().into(),
    }
}

fn bootstrap_alonzo_pparams(
    previous: ShelleyProtParams,
    genesis: &alonzo::GenesisFile,
) -> AlonzoProtParams {
    AlonzoProtParams {
        minfee_a: previous.minfee_a,
        minfee_b: previous.minfee_b,
        max_block_body_size: previous.max_block_body_size,
        max_transaction_size: previous.max_transaction_size,
        max_block_header_size: previous.max_block_header_size,
        key_deposit: previous.key_deposit,
        pool_deposit: previous.pool_deposit,
        protocol_version: previous.protocol_version,
        min_pool_cost: previous.min_pool_cost,
        desired_number_of_stake_pools: previous.desired_number_of_stake_pools,
        expansion_rate: previous.expansion_rate.clone(),
        treasury_growth_rate: previous.treasury_growth_rate.clone(),
        maximum_epoch: previous.maximum_epoch,
        pool_pledge_influence: previous.pool_pledge_influence,
        decentralization_constant: previous.decentralization_constant,
        extra_entropy: previous.extra_entropy,
        // new from genesis
        ada_per_utxo_byte: genesis.lovelace_per_utxo_word,
        cost_models_for_script_languages: genesis.cost_models.clone().into(),
        execution_costs: genesis.execution_prices.clone().into(),
        max_tx_ex_units: genesis.max_tx_ex_units.clone().into(),
        max_block_ex_units: genesis.max_block_ex_units.clone().into(),
        max_value_size: genesis.max_value_size,
        collateral_percentage: genesis.collateral_percentage,
        max_collateral_inputs: genesis.max_collateral_inputs,
    }
}

fn bootstrap_babbage_pparams(previous: AlonzoProtParams) -> BabbageProtParams {
    BabbageProtParams {
        minfee_a: previous.minfee_a,
        minfee_b: previous.minfee_b,
        max_block_body_size: previous.max_block_body_size,
        max_transaction_size: previous.max_transaction_size,
        max_block_header_size: previous.max_block_header_size,
        key_deposit: previous.key_deposit,
        pool_deposit: previous.pool_deposit,
        protocol_version: previous.protocol_version,
        min_pool_cost: previous.min_pool_cost,
        desired_number_of_stake_pools: previous.desired_number_of_stake_pools,
        ada_per_utxo_byte: previous.ada_per_utxo_byte,
        execution_costs: previous.execution_costs,
        max_tx_ex_units: previous.max_tx_ex_units,
        max_block_ex_units: previous.max_block_ex_units,
        max_value_size: previous.max_value_size,
        collateral_percentage: previous.collateral_percentage,
        max_collateral_inputs: previous.max_collateral_inputs,
        expansion_rate: previous.expansion_rate,
        treasury_growth_rate: previous.treasury_growth_rate,
        maximum_epoch: previous.maximum_epoch,
        pool_pledge_influence: previous.pool_pledge_influence,
        decentralization_constant: previous.decentralization_constant,
        extra_entropy: previous.extra_entropy,
        cost_models_for_script_languages: pallas::ledger::primitives::babbage::CostMdls {
            plutus_v1: previous
                .cost_models_for_script_languages
                .iter()
                .filter(|(k, _)| k == &Language::PlutusV1)
                .map(|(_, v)| v.clone())
                .next(),
            plutus_v2: None,
        },
    }
}

fn bootstrap_conway_pparams(previous: BabbageProtParams) -> ConwayProtParams {
    ConwayProtParams {
        minfee_a: previous.minfee_a,
        minfee_b: previous.minfee_b,
        max_block_body_size: previous.max_block_body_size,
        max_transaction_size: previous.max_transaction_size,
        max_block_header_size: previous.max_block_header_size,
        key_deposit: previous.key_deposit,
        pool_deposit: previous.pool_deposit,
        protocol_version: previous.protocol_version,
        min_pool_cost: previous.min_pool_cost,
        desired_number_of_stake_pools: previous.desired_number_of_stake_pools,
        ada_per_utxo_byte: previous.ada_per_utxo_byte,
        execution_costs: previous.execution_costs,
        max_tx_ex_units: previous.max_tx_ex_units,
        max_block_ex_units: previous.max_block_ex_units,
        max_value_size: previous.max_value_size,
        collateral_percentage: previous.collateral_percentage,
        max_collateral_inputs: previous.max_collateral_inputs,
        expansion_rate: previous.expansion_rate,
        treasury_growth_rate: previous.treasury_growth_rate,
        maximum_epoch: previous.maximum_epoch,
        pool_pledge_influence: previous.pool_pledge_influence,
        cost_models_for_script_languages: pallas::ledger::primitives::conway::CostMdls {
            plutus_v1: previous.cost_models_for_script_languages.plutus_v1,
            plutus_v2: previous.cost_models_for_script_languages.plutus_v2,
            plutus_v3: None,
        },
        // TODO: load these values from genesis config
        pool_voting_thresholds: pallas::ledger::primitives::conway::PoolVotingThresholds {
            motion_no_confidence: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            committee_normal: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            committee_no_confidence: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            hard_fork_initiation: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            security_voting_threshold: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
        },
        drep_voting_thresholds: pallas::ledger::primitives::conway::DRepVotingThresholds {
            motion_no_confidence: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            committee_normal: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            committee_no_confidence: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            update_constitution: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            hard_fork_initiation: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            pp_network_group: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            pp_economic_group: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            pp_technical_group: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            pp_governance_group: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            treasury_withdrawal: pallas::ledger::primitives::conway::RationalNumber {
                numerator: 0,
                denominator: 1,
            },
        },
        min_committee_size: Default::default(),
        committee_term_limit: Default::default(),
        governance_action_validity_period: Default::default(),
        governance_action_deposit: Default::default(),
        drep_deposit: Default::default(),
        drep_inactivity_period: Default::default(),
        minfee_refscript_cost_per_byte: pallas::ledger::primitives::conway::RationalNumber {
            numerator: 0,
            denominator: 1,
        },
    }
}

fn apply_param_update(
    current: MultiEraProtocolParameters,
    update: &MultiEraUpdate,
) -> MultiEraProtocolParameters {
    match current {
        MultiEraProtocolParameters::Byron(mut pparams) => {
            if let Some(new) = update.byron_proposed_block_version() {
                warn!(?new, "found new block version");
                pparams.block_version = new;
            }

            if let Some(pallas::ledger::primitives::byron::TxFeePol::Variant0(new)) =
                update.byron_proposed_fee_policy()
            {
                warn!("found new byron fee policy update proposal");
                let (summand, multiplier) = new.unwrap();
                pparams.summand = summand as u64;
                pparams.multiplier = multiplier as u64;
            }

            if let Some(new) = update.byron_proposed_max_tx_size() {
                warn!("found new byron max tx size update proposal");
                pparams.max_tx_size = new;
            }

            MultiEraProtocolParameters::Byron(pparams)
        }
        MultiEraProtocolParameters::Shelley(mut pparams) => {
            if let Some(new) = update.first_proposed_protocol_version() {
                warn!(?new, "found new protocol version");
                pparams.protocol_version = new;
            }

            if let Some(x) = update.first_proposed_minfee_a() {
                warn!(x, "found new minfee a update proposal");
                pparams.minfee_a = x;
            }

            if let Some(x) = update.first_proposed_minfee_b() {
                warn!(x, "found new minfee b update proposal");
                pparams.minfee_b = x;
            }

            if let Some(x) = update.first_proposed_max_transaction_size() {
                warn!(x, "found new max tx size update proposal");
                pparams.max_transaction_size = x;
            }

            // TODO: where's the min utxo value in the network primitives for shelley? do we
            // have them wrong in Pallas?

            MultiEraProtocolParameters::Shelley(pparams)
        }
        MultiEraProtocolParameters::Alonzo(mut pparams) => {
            if let Some(new) = update.first_proposed_protocol_version() {
                warn!(?new, "found new protocol version");
                pparams.protocol_version = new;
            }

            MultiEraProtocolParameters::Alonzo(pparams)
        }
        MultiEraProtocolParameters::Babbage(mut pparams) => {
            if let Some(new) = update.first_proposed_protocol_version() {
                warn!(?new, "found new protocol version");
                pparams.protocol_version = new;
            }

            MultiEraProtocolParameters::Babbage(pparams)
        }
        MultiEraProtocolParameters::Conway(mut pparams) => {
            if let Some(new) = update.first_proposed_protocol_version() {
                warn!(?new, "found new protocol version");
                pparams.protocol_version = new;
            }

            MultiEraProtocolParameters::Conway(pparams)
        }
        _ => unimplemented!(),
    }
}

fn advance_hardfork(
    current: MultiEraProtocolParameters,
    genesis: &Genesis,
    next_protocol: usize,
) -> MultiEraProtocolParameters {
    match current {
        // Source: https://github.com/cardano-foundation/CIPs/blob/master/CIP-0059/feature-table.md
        // NOTE: part of the confusion here is that there are two versioning schemes that can be
        // easily conflated:
        // - The protocol version, negotiated in the networking layer
        // - The protocol version broadcast in the block header
        // Generally, these refer to the latter; the update proposals jump from 2 to 5, because the
        // node team decided it would be helpful to have these in sync.

        // Protocol starts at version 0;
        // There was one intra-era "hard fork" in byron (even though they weren't called that yet)
        MultiEraProtocolParameters::Byron(current) if next_protocol == 1 => {
            MultiEraProtocolParameters::Byron(current)
        }
        // Protocol version 2 transitions from Byron to Shelley
        MultiEraProtocolParameters::Byron(_) if next_protocol == 2 => {
            MultiEraProtocolParameters::Shelley(bootstrap_shelley_pparams(genesis.shelley))
        }
        // Two intra-era hard forks, named Allegra (3) and Mary (4); we don't have separate types
        // for these eras
        MultiEraProtocolParameters::Shelley(current) if next_protocol < 5 => {
            MultiEraProtocolParameters::Shelley(current)
        }
        // Protocol version 5 transitions from Shelley (Mary, technically) to Alonzo
        MultiEraProtocolParameters::Shelley(current) if next_protocol == 5 => {
            MultiEraProtocolParameters::Alonzo(bootstrap_alonzo_pparams(current, genesis.alonzo))
        }
        // One intra-era hard-fork in alonzo at protocol version 6
        MultiEraProtocolParameters::Alonzo(current) if next_protocol == 6 => {
            MultiEraProtocolParameters::Alonzo(current)
        }
        // Protocol version 7 transitions from Alonzo to Babbage
        MultiEraProtocolParameters::Alonzo(current) if next_protocol == 7 => {
            MultiEraProtocolParameters::Babbage(bootstrap_babbage_pparams(current))
        }
        // One intra-era hard-fork in babbage at protocol version 8
        MultiEraProtocolParameters::Babbage(current) if next_protocol == 8 => {
            MultiEraProtocolParameters::Babbage(current)
        }
        // Protocol version 9 will transition from Babbage to Conway; not yet implemented
        MultiEraProtocolParameters::Babbage(current) if next_protocol == 9 => {
            MultiEraProtocolParameters::Conway(bootstrap_conway_pparams(current))
        }
        _ => unimplemented!("don't know how to handle hardfork"),
    }
}

pub fn fold_pparams(
    genesis: &Genesis,
    updates: &[MultiEraUpdate],
    for_epoch: u64,
) -> MultiEraProtocolParameters {
    let mut pparams = match &updates[0] {
        MultiEraUpdate::Byron(_, _) => {
            MultiEraProtocolParameters::Byron(bootstrap_byron_pparams(genesis.byron))
        }
        // Preview beggins directly on Shelley.
        _ => MultiEraProtocolParameters::Shelley(bootstrap_shelley_pparams(genesis.shelley)),
    };
    let mut last_protocol = 0;

    for epoch in 0..for_epoch {
        for next_protocol in last_protocol + 1..=pparams.protocol_version() {
            warn!(next_protocol, "advancing hardfork");
            pparams = advance_hardfork(pparams, genesis, next_protocol);
            last_protocol = next_protocol;
        }

        for update in updates.iter().filter(|e| e.epoch() == epoch) {
            trace!(epoch, "Applying update");
            pparams = apply_param_update(pparams, update);
        }
    }

    pparams
}

#[cfg(test)]
mod tests {
    use std::{io::Read, path::Path};

    use itertools::Itertools;
    use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};

    use super::*;

    fn load_json<T, P: AsRef<Path>>(path: P) -> T
    where
        T: serde::de::DeserializeOwned,
    {
        let file = std::fs::File::open(path).unwrap();
        serde_json::from_reader(file).unwrap()
    }

    fn test_env_fold(env: &str) {
        let test_data = format!("src/ledger/pparams/test_data/{env}");

        // Load each genesis file
        let genesis = Genesis {
            byron: &load_json(format!("{test_data}/genesis/byron_genesis.json")),
            shelley: &load_json(format!("{test_data}/genesis/shelley_genesis.json")),
            alonzo: &load_json(format!("{test_data}/genesis/alonzo_genesis.json")),
        };

        // Then load each mainnet example update proposal as buffers
        let files: Vec<_> = std::fs::read_dir(format!("{test_data}/update_proposal_blocks/"))
            .unwrap()
            .map(|x| std::fs::File::open(x.unwrap().path()).unwrap())
            .map(|mut x| {
                let mut buf = vec![];
                x.read_to_end(&mut buf).unwrap();
                buf
            })
            .collect();

        // Decode those buffers as blocks, and sort them by slot, so we can process them
        // in order
        let blocks: Vec<_> = files
            .iter()
            .map(|x| MultiEraBlock::decode(&x).unwrap())
            .sorted_by_key(|b| b.slot())
            .collect();

        let block_data: Vec<_> = blocks.iter().map(|b| (b.update(), b.txs())).collect();

        let update_pairs: Vec<_> = block_data
            .iter()
            .map(|(b, txs)| (b, txs.iter().filter_map(MultiEraTx::update)))
            .collect();

        let chained_updates: Vec<_> = update_pairs
            .into_iter()
            .flat_map(|(b, txs)| {
                let b = b.iter().cloned();
                txs.chain(b)
            })
            .collect();

        // Now, for each epoch we've recorded protocol parameters for,
        // test if we get the right value when folding
        for file in std::fs::read_dir(format!("{test_data}/expected_params/")).unwrap() {
            let filename = file.unwrap().path();
            println!("Comparing to {:?}", filename);
            let epoch = filename
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap();
            // TODO: implement serialize/deserialize, and get full protocol param json files
            let expected = load_json::<usize, _>(filename);
            let actual = fold_pparams(&genesis, &chained_updates, epoch);
            assert_eq!(expected, actual.protocol_version())

            //assert_eq!(expected, actual)
        }
    }

    #[test]
    fn test_mainnet_fold() {
        test_env_fold("mainnet")
    }
}
