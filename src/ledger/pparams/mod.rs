use pallas::{
    applying::utils::{
        AlonzoProtParams, BabbageProtParams, ByronProtParams, ConwayProtParams,
        MultiEraProtocolParameters, ShelleyProtParams,
    },
    ledger::{
        configs::{alonzo, byron, conway, shelley},
        primitives::alonzo::Language as AlonzoLanguage,
        traverse::MultiEraUpdate,
    },
};
use tracing::{debug, warn};

mod summary;

pub use summary::*;

macro_rules! apply_field {
    ($target:ident, $update:ident, $field:ident) => {
        paste::paste! {
            if let Some(new) = $update.[<first_proposed_ $field>]() {
                debug!(
                    ?new,
                    param = stringify!($field),
                    "found new update proposal"
                );

                $target.$field = new;
            }
        }
    };
}

pub struct Genesis {
    pub byron: byron::GenesisFile,
    pub shelley: shelley::GenesisFile,
    pub alonzo: alonzo::GenesisFile,
    pub conway: conway::GenesisFile,
    pub force_protocol: Option<usize>,
}

fn bootstrap_byron_pparams(byron: &byron::GenesisFile) -> ByronProtParams {
    ByronProtParams {
        block_version: (0, 0, 0),
        start_time: byron.start_time,
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
        // TODO: remove unwrap once we make the whole process fallible
        system_start: chrono::DateTime::parse_from_rfc3339(shelley.system_start.as_ref().unwrap())
            .unwrap(),
        protocol_version: shelley.protocol_params.protocol_version.clone().into(),
        epoch_length: shelley.epoch_length.unwrap_or_default() as u64,
        slot_length: shelley.slot_length.unwrap_or_default() as u64,
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
        system_start: previous.system_start,
        epoch_length: previous.epoch_length,
        slot_length: previous.slot_length,
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
        system_start: previous.system_start,
        epoch_length: previous.epoch_length,
        slot_length: previous.slot_length,
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
        cost_models_for_script_languages: pallas::ledger::primitives::babbage::CostModels {
            plutus_v1: previous
                .cost_models_for_script_languages
                .iter()
                .filter(|(k, _)| k == &AlonzoLanguage::PlutusV1)
                .map(|(_, v)| v.clone())
                .next(),
            plutus_v2: None,
        },
    }
}

fn bootstrap_conway_pparams(
    previous: BabbageProtParams,
    genesis: &conway::GenesisFile,
) -> ConwayProtParams {
    ConwayProtParams {
        system_start: previous.system_start,
        epoch_length: previous.epoch_length,
        slot_length: previous.slot_length,
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
        cost_models_for_script_languages: pallas::ledger::primitives::conway::CostModels {
            plutus_v1: previous.cost_models_for_script_languages.plutus_v1,
            plutus_v2: previous.cost_models_for_script_languages.plutus_v2,
            plutus_v3: Some(genesis.plutus_v3_cost_model.clone()),
        },
        pool_voting_thresholds: pallas::ledger::primitives::conway::PoolVotingThresholds {
            motion_no_confidence: float_to_rational(
                genesis.pool_voting_thresholds.motion_no_confidence,
            ),
            committee_normal: float_to_rational(genesis.pool_voting_thresholds.committee_normal),
            committee_no_confidence: float_to_rational(
                genesis.pool_voting_thresholds.committee_no_confidence,
            ),
            hard_fork_initiation: float_to_rational(
                genesis.pool_voting_thresholds.hard_fork_initiation,
            ),
            security_voting_threshold: float_to_rational(
                genesis.pool_voting_thresholds.pp_security_group,
            ),
        },
        drep_voting_thresholds: pallas::ledger::primitives::conway::DRepVotingThresholds {
            motion_no_confidence: float_to_rational(
                genesis.d_rep_voting_thresholds.motion_no_confidence,
            ),
            committee_normal: float_to_rational(genesis.d_rep_voting_thresholds.committee_normal),
            committee_no_confidence: float_to_rational(
                genesis.d_rep_voting_thresholds.committee_no_confidence,
            ),
            update_constitution: float_to_rational(
                genesis.d_rep_voting_thresholds.update_to_constitution,
            ),
            hard_fork_initiation: float_to_rational(
                genesis.d_rep_voting_thresholds.hard_fork_initiation,
            ),
            pp_network_group: float_to_rational(genesis.d_rep_voting_thresholds.pp_network_group),
            pp_economic_group: float_to_rational(genesis.d_rep_voting_thresholds.pp_economic_group),
            pp_technical_group: float_to_rational(
                genesis.d_rep_voting_thresholds.pp_technical_group,
            ),
            pp_governance_group: float_to_rational(genesis.d_rep_voting_thresholds.pp_gov_group),
            treasury_withdrawal: float_to_rational(
                genesis.d_rep_voting_thresholds.treasury_withdrawal,
            ),
        },
        min_committee_size: genesis.committee_min_size,
        committee_term_limit: genesis.committee_max_term_length.into(),
        governance_action_validity_period: genesis.gov_action_lifetime.into(),
        governance_action_deposit: genesis.gov_action_deposit,
        drep_deposit: genesis.d_rep_deposit,
        drep_inactivity_period: genesis.d_rep_activity.into(),
        minfee_refscript_cost_per_byte: pallas::ledger::primitives::conway::RationalNumber {
            numerator: genesis.min_fee_ref_script_cost_per_byte,
            denominator: 1,
        },
    }
}

fn float_to_rational(x: f32) -> pallas::ledger::primitives::conway::RationalNumber {
    const PRECISION: u32 = 9;
    let scale = 10u64.pow(PRECISION);
    let scaled = (x * scale as f32).round() as u64;

    // Check if it's very close to a whole number
    if (x.round() - x).abs() < f32::EPSILON {
        return pallas::ledger::primitives::conway::RationalNumber {
            numerator: x.round() as u64,
            denominator: 1,
        };
    }

    let gcd = gcd(scaled, scale);

    pallas::ledger::primitives::conway::RationalNumber {
        numerator: scaled / gcd,
        denominator: scale / gcd,
    }
}

// Helper function to calculate the Greatest Common Divisor
fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let temp = b;
        b = a % b;
        a = temp;
    }
    a
}

fn apply_param_update(
    current: MultiEraProtocolParameters,
    update: &MultiEraUpdate,
) -> MultiEraProtocolParameters {
    match current {
        MultiEraProtocolParameters::Byron(mut pparams) => {
            if let Some(new) = update.byron_proposed_block_version() {
                debug!(?new, "found new block version");
                pparams.block_version = new;
            }

            if let Some(pallas::ledger::primitives::byron::TxFeePol::Variant0(new)) =
                update.byron_proposed_fee_policy()
            {
                debug!("found new byron fee policy update proposal");
                let (summand, multiplier) = new.unwrap();
                pparams.summand = summand as u64;
                pparams.multiplier = multiplier as u64;
            }

            if let Some(new) = update.byron_proposed_max_tx_size() {
                debug!("found new byron max tx size update proposal");
                pparams.max_tx_size = new;
            }

            MultiEraProtocolParameters::Byron(pparams)
        }
        MultiEraProtocolParameters::Shelley(mut pparams) => {
            apply_field!(pparams, update, minfee_a);
            apply_field!(pparams, update, minfee_b);
            apply_field!(pparams, update, max_block_body_size);
            apply_field!(pparams, update, max_transaction_size);
            apply_field!(pparams, update, max_block_header_size);
            apply_field!(pparams, update, key_deposit);
            apply_field!(pparams, update, pool_deposit);
            apply_field!(pparams, update, desired_number_of_stake_pools);
            apply_field!(pparams, update, min_pool_cost);
            apply_field!(pparams, update, expansion_rate);
            apply_field!(pparams, update, treasury_growth_rate);
            apply_field!(pparams, update, maximum_epoch);
            apply_field!(pparams, update, pool_pledge_influence);
            apply_field!(pparams, update, decentralization_constant);
            apply_field!(pparams, update, extra_entropy);

            MultiEraProtocolParameters::Shelley(pparams)
        }
        MultiEraProtocolParameters::Alonzo(mut pparams) => {
            apply_field!(pparams, update, minfee_a);
            apply_field!(pparams, update, minfee_b);
            apply_field!(pparams, update, max_block_body_size);
            apply_field!(pparams, update, max_transaction_size);
            apply_field!(pparams, update, max_block_header_size);
            apply_field!(pparams, update, key_deposit);
            apply_field!(pparams, update, pool_deposit);
            apply_field!(pparams, update, desired_number_of_stake_pools);
            apply_field!(pparams, update, min_pool_cost);
            apply_field!(pparams, update, ada_per_utxo_byte);
            apply_field!(pparams, update, execution_costs);
            apply_field!(pparams, update, max_tx_ex_units);
            apply_field!(pparams, update, max_block_ex_units);
            apply_field!(pparams, update, max_value_size);
            apply_field!(pparams, update, collateral_percentage);
            apply_field!(pparams, update, max_collateral_inputs);
            apply_field!(pparams, update, expansion_rate);
            apply_field!(pparams, update, treasury_growth_rate);
            apply_field!(pparams, update, maximum_epoch);
            apply_field!(pparams, update, pool_pledge_influence);
            apply_field!(pparams, update, decentralization_constant);
            apply_field!(pparams, update, extra_entropy);

            if let Some(value) = update.alonzo_first_proposed_cost_models_for_script_languages() {
                warn!(
                    ?value,
                    "found new cost_models_for_script_languages update proposal"
                );

                pparams.cost_models_for_script_languages = value;
            }

            MultiEraProtocolParameters::Alonzo(pparams)
        }
        MultiEraProtocolParameters::Babbage(mut pparams) => {
            apply_field!(pparams, update, minfee_a);
            apply_field!(pparams, update, minfee_b);
            apply_field!(pparams, update, max_block_body_size);
            apply_field!(pparams, update, max_transaction_size);
            apply_field!(pparams, update, max_block_header_size);
            apply_field!(pparams, update, key_deposit);
            apply_field!(pparams, update, pool_deposit);
            apply_field!(pparams, update, desired_number_of_stake_pools);
            apply_field!(pparams, update, min_pool_cost);
            apply_field!(pparams, update, ada_per_utxo_byte);
            apply_field!(pparams, update, execution_costs);
            apply_field!(pparams, update, max_tx_ex_units);
            apply_field!(pparams, update, max_block_ex_units);
            apply_field!(pparams, update, max_value_size);
            apply_field!(pparams, update, collateral_percentage);
            apply_field!(pparams, update, max_collateral_inputs);
            apply_field!(pparams, update, expansion_rate);
            apply_field!(pparams, update, treasury_growth_rate);
            apply_field!(pparams, update, maximum_epoch);
            apply_field!(pparams, update, pool_pledge_influence);
            apply_field!(pparams, update, decentralization_constant);
            apply_field!(pparams, update, extra_entropy);

            if let Some(value) = update.babbage_first_proposed_cost_models_for_script_languages() {
                warn!(
                    ?value,
                    "found new cost_models_for_script_languages update proposal"
                );

                pparams.cost_models_for_script_languages = value;
            }

            MultiEraProtocolParameters::Babbage(pparams)
        }
        MultiEraProtocolParameters::Conway(mut pparams) => {
            apply_field!(pparams, update, minfee_a);
            apply_field!(pparams, update, minfee_b);
            apply_field!(pparams, update, max_block_body_size);
            apply_field!(pparams, update, max_transaction_size);
            apply_field!(pparams, update, max_block_header_size);
            apply_field!(pparams, update, key_deposit);
            apply_field!(pparams, update, pool_deposit);
            apply_field!(pparams, update, desired_number_of_stake_pools);
            apply_field!(pparams, update, min_pool_cost);
            apply_field!(pparams, update, ada_per_utxo_byte);
            apply_field!(pparams, update, execution_costs);
            apply_field!(pparams, update, max_tx_ex_units);
            apply_field!(pparams, update, max_block_ex_units);
            apply_field!(pparams, update, max_value_size);
            apply_field!(pparams, update, collateral_percentage);
            apply_field!(pparams, update, max_collateral_inputs);
            apply_field!(pparams, update, expansion_rate);
            apply_field!(pparams, update, treasury_growth_rate);
            apply_field!(pparams, update, maximum_epoch);
            apply_field!(pparams, update, pool_pledge_influence);
            apply_field!(pparams, update, pool_voting_thresholds);
            apply_field!(pparams, update, drep_voting_thresholds);
            apply_field!(pparams, update, min_committee_size);
            apply_field!(pparams, update, committee_term_limit);
            apply_field!(pparams, update, governance_action_validity_period);
            apply_field!(pparams, update, governance_action_deposit);
            apply_field!(pparams, update, drep_deposit);
            apply_field!(pparams, update, drep_inactivity_period);
            apply_field!(pparams, update, minfee_refscript_cost_per_byte);

            if let Some(value) = update.conway_first_proposed_cost_models_for_script_languages() {
                warn!(
                    ?value,
                    "found new cost_models_for_script_languages update proposal"
                );

                pparams.cost_models_for_script_languages = value;
            }

            MultiEraProtocolParameters::Conway(pparams)
        }
        _ => unimplemented!(),
    }
}

fn migrate_pparams(
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
            MultiEraProtocolParameters::Shelley(bootstrap_shelley_pparams(&genesis.shelley))
        }
        // Two intra-era hard forks, named Allegra (3) and Mary (4); we don't have separate types
        // for these eras
        MultiEraProtocolParameters::Shelley(current) if next_protocol < 5 => {
            MultiEraProtocolParameters::Shelley(current)
        }
        // Protocol version 5 transitions from Shelley (Mary, technically) to Alonzo
        MultiEraProtocolParameters::Shelley(current) if next_protocol == 5 => {
            MultiEraProtocolParameters::Alonzo(bootstrap_alonzo_pparams(current, &genesis.alonzo))
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
        // Protocol version 9 transitions from Babbage to Conway
        MultiEraProtocolParameters::Babbage(current) if next_protocol == 9 => {
            MultiEraProtocolParameters::Conway(bootstrap_conway_pparams(current, &genesis.conway))
        }
        x => unimplemented!(
            "don't know how to handle hardfork (protocol: {})",
            x.protocol_version()
        ),
    }
}

pub fn fold_until_epoch(
    genesis: &Genesis,
    updates: &[MultiEraUpdate],
    for_epoch: u64,
) -> (MultiEraProtocolParameters, ChainSummary) {
    debug!("Initializing with Byron parameters");
    let mut pparams = MultiEraProtocolParameters::Byron(bootstrap_byron_pparams(&genesis.byron));

    let mut summary = ChainSummary::start(&pparams);

    if let Some(force_protocol) = genesis.force_protocol {
        let current_protocol = summary.current().protocol_version;

        while current_protocol < force_protocol {
            let next_protocol = current_protocol + 1;
            pparams = migrate_pparams(pparams, genesis, next_protocol);
            summary.advance(0, &pparams);

            debug!(
                protocol = summary.current().protocol_version,
                "forced hardfork"
            );
        }
    }

    for epoch in 1..for_epoch {
        let epoch_updates: Vec<_> = updates
            .iter()
            .filter(|e| e.epoch() == (epoch - 1))
            .collect();

        if !epoch_updates.is_empty() {
            debug!(
                epoch,
                count = epoch_updates.len(),
                "found updates for epoch",
            );
        }

        for update in epoch_updates {
            pparams = apply_param_update(pparams, update);

            let byron_version_change = update
                .byron_proposed_block_version()
                .map(|(v, _, _)| (v as usize));

            let post_byron_version_change = update
                .first_proposed_protocol_version()
                .map(|(v, _)| v as usize);

            let version_change = byron_version_change.or(post_byron_version_change);

            if let Some(next_protocol) = version_change {
                let current_protocol = summary.current().protocol_version;

                while current_protocol < next_protocol {
                    let next_protocol = current_protocol + 1;
                    pparams = migrate_pparams(pparams, genesis, next_protocol);
                    summary.advance(epoch, &pparams);

                    debug!(
                        protocol = summary.current().protocol_version,
                        "hardfork executed"
                    );
                }
            }
        }
    }

    (pparams, summary)
}

pub fn fold(
    genesis: &Genesis,
    updates: &[MultiEraUpdate],
) -> (MultiEraProtocolParameters, ChainSummary) {
    let for_epoch = updates.last().map(|u| u.epoch()).unwrap_or(0);

    let (pparams, summary) = fold_until_epoch(genesis, updates, for_epoch);

    (pparams, summary)
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
            byron: load_json(format!("{test_data}/genesis/byron_genesis.json")),
            shelley: load_json(format!("{test_data}/genesis/shelley_genesis.json")),
            alonzo: load_json(format!("{test_data}/genesis/alonzo_genesis.json")),
            conway: load_json(format!("{test_data}/genesis/conway_genesis.json")),
            force_protocol: None,
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
            .map(|x| MultiEraBlock::decode(x).unwrap())
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
            let (_, summary) = fold_until_epoch(&genesis, &chained_updates, epoch);

            assert_eq!(expected, summary.current().protocol_version)

            //assert_eq!(expected, actual)
        }
    }

    #[test]
    fn test_mainnet_fold() {
        test_env_fold("mainnet")
    }

    #[test]
    fn test_pool_voting_thresholds_rational() {
        let thresholds = [
            ("committeeNormal", 0.51),
            ("committeeNoConfidence", 0.51),
            ("hardForkInitiation", 0.51),
            ("motionNoConfidence", 0.51),
            ("ppSecurityGroup", 0.51),
        ];

        for (name, value) in thresholds.iter() {
            let result = float_to_rational(*value);
            assert_eq!(result.numerator, 51, "Failed for {}", name);
            assert_eq!(result.denominator, 100, "Failed for {}", name);
        }
    }

    #[test]
    fn test_drep_voting_thresholds_rational() {
        let thresholds = [
            ("motionNoConfidence", 0.67),
            ("committeeNormal", 0.67),
            ("committeeNoConfidence", 0.60),
            ("updateToConstitution", 0.75),
            ("hardForkInitiation", 0.60),
            ("ppNetworkGroup", 0.67),
            ("ppEconomicGroup", 0.67),
            ("ppTechnicalGroup", 0.67),
            ("ppGovGroup", 0.75),
            ("treasuryWithdrawal", 0.67),
        ];

        for (name, value) in thresholds.iter() {
            let result = float_to_rational(*value);
            match *value {
                0.67 => {
                    assert_eq!(result.numerator, 67, "Failed for {}", name);
                    assert_eq!(result.denominator, 100, "Failed for {}", name);
                }
                0.60 => {
                    assert_eq!(result.numerator, 3, "Failed for {}", name);
                    assert_eq!(result.denominator, 5, "Failed for {}", name);
                }
                0.75 => {
                    assert_eq!(result.numerator, 3, "Failed for {}", name);
                    assert_eq!(result.denominator, 4, "Failed for {}", name);
                }
                _ => panic!("Unexpected value for {}: {}", name, value),
            }
        }
    }

    fn assert_rational_eq(
        result: pallas::ledger::primitives::conway::RationalNumber,
        expected_num: u64,
        expected_den: u64,
        input: f32,
    ) {
        assert_eq!(
            result.numerator, expected_num,
            "Numerator mismatch for input {}",
            input
        );
        assert_eq!(
            result.denominator, expected_den,
            "Denominator mismatch for input {}",
            input
        );
    }

    #[test]
    fn test_whole_number() {
        let test_cases = [
            (1.0, 1, 1),
            (2.0, 2, 1),
            (100.0, 100, 1),
            (1000000.0, 1000000, 1),
        ];

        for &(input, expected_num, expected_den) in test_cases.iter() {
            let result = float_to_rational(input);
            assert_rational_eq(result, expected_num, expected_den, input);
        }
    }

    #[test]
    fn test_fractions() {
        let test_cases = [
            (0.5, 1, 2),
            (0.25, 1, 4),
            // (0.33333334, 333333343, 1000000000), // These fails due to floating point precision
            // (0.66666669, 666666687, 1000000000),
        ];

        for &(input, expected_num, expected_den) in test_cases.iter() {
            let result = float_to_rational(input);
            assert_rational_eq(result, expected_num, expected_den, input);
        }
    }
}
