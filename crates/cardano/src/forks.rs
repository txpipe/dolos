use crate::{utils::float_to_rational, PParamsState};
use dolos_core::Genesis;
use pallas::{
    crypto::hash::Hash,
    ledger::{
        configs::{alonzo, byron, conway, shelley},
        primitives::{
            conway::{CostModels, DRepVotingThresholds, PoolVotingThresholds},
            CostModel, ExUnits, Nonce, NonceVariant,
        },
    },
};

fn from_config_nonce(config: &shelley::ExtraEntropy) -> Nonce {
    Nonce {
        variant: match config.tag {
            shelley::NonceVariant::NeutralNonce => NonceVariant::NeutralNonce,
            shelley::NonceVariant::Nonce => NonceVariant::Nonce,
        },
        hash: {
            let bytes = hex::decode(config.hash.as_ref().unwrap()).unwrap();
            Some(Hash::from(bytes.as_slice()))
        },
    }
}

fn from_config_exunits(config: &alonzo::ExUnits) -> ExUnits {
    ExUnits {
        mem: config.ex_units_mem,
        steps: config.ex_units_steps,
    }
}

fn from_alonzo_cost_models_map(
    config: &alonzo::CostModelPerLanguage,
    language: &alonzo::Language,
) -> Option<CostModel> {
    config
        .iter()
        .filter(|(k, _)| *k == language)
        .map(|(_, v)| CostModel::from(v.clone()))
        .next()
}

fn from_conway_pool_voting_thresholds(
    config: &conway::PoolVotingThresholds,
) -> PoolVotingThresholds {
    PoolVotingThresholds {
        motion_no_confidence: float_to_rational(config.motion_no_confidence),
        committee_normal: float_to_rational(config.committee_normal),
        committee_no_confidence: float_to_rational(config.committee_no_confidence),
        hard_fork_initiation: float_to_rational(config.hard_fork_initiation),
        security_voting_threshold: float_to_rational(config.pp_security_group),
    }
}

fn from_conway_drep_voting_thresholds(
    config: &conway::DRepVotingThresholds,
) -> DRepVotingThresholds {
    pallas::ledger::primitives::conway::DRepVotingThresholds {
        motion_no_confidence: float_to_rational(config.motion_no_confidence),
        committee_normal: float_to_rational(config.committee_normal),
        committee_no_confidence: float_to_rational(config.committee_no_confidence),
        update_constitution: float_to_rational(config.update_to_constitution),
        hard_fork_initiation: float_to_rational(config.hard_fork_initiation),
        pp_network_group: float_to_rational(config.pp_network_group),
        pp_economic_group: float_to_rational(config.pp_economic_group),
        pp_technical_group: float_to_rational(config.pp_technical_group),
        pp_governance_group: float_to_rational(config.pp_gov_group),
        treasury_withdrawal: float_to_rational(config.treasury_withdrawal),
    }
}

pub fn from_byron_genesis(byron: &byron::GenesisFile) -> PParamsState {
    PParamsState {
        protocol_version: (0, 0),
        system_start: byron.start_time,
        slot_length: byron.block_version_data.slot_duration,
        minfee_a: byron.block_version_data.tx_fee_policy.multiplier,
        minfee_b: byron.block_version_data.tx_fee_policy.summand,
        max_block_body_size: byron.block_version_data.max_block_size,
        max_transaction_size: byron.block_version_data.max_tx_size,
        max_block_header_size: byron.block_version_data.max_header_size,
        ..Default::default()
    }
}

pub fn from_shelley_genesis(shelley: &shelley::GenesisFile) -> PParamsState {
    let system_start = chrono::DateTime::parse_from_rfc3339(shelley.system_start.as_ref().unwrap())
        .expect("invalid system start value");

    PParamsState {
        system_start: system_start.timestamp() as u64,
        protocol_version: shelley.protocol_params.protocol_version.clone().into(),
        epoch_length: shelley.epoch_length.unwrap_or_default() as u64,
        slot_length: shelley.slot_length.unwrap_or_default() as u64,
        max_block_body_size: shelley.protocol_params.max_block_body_size as u64,
        max_transaction_size: shelley.protocol_params.max_tx_size as u64,
        max_block_header_size: shelley.protocol_params.max_block_header_size as u64,
        key_deposit: shelley.protocol_params.key_deposit,
        min_utxo_value: shelley.protocol_params.min_utxo_value,
        minfee_a: shelley.protocol_params.min_fee_a as u64,
        minfee_b: shelley.protocol_params.min_fee_b as u64,
        pool_deposit: shelley.protocol_params.pool_deposit,
        desired_number_of_stake_pools: shelley.protocol_params.n_opt,
        min_pool_cost: shelley.protocol_params.min_pool_cost,
        expansion_rate: Some(shelley.protocol_params.rho.clone()),
        treasury_growth_rate: Some(shelley.protocol_params.tau.clone()),
        maximum_epoch: shelley.protocol_params.e_max,
        pool_pledge_influence: Some(shelley.protocol_params.a0.clone()),
        decentralization_constant: Some(shelley.protocol_params.decentralisation_param.clone()),
        extra_entropy: Some(from_config_nonce(&shelley.protocol_params.extra_entropy)),
        ..Default::default()
    }
}

pub fn into_alonzo(previous: &PParamsState, genesis: &alonzo::GenesisFile) -> PParamsState {
    PParamsState {
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
        pool_pledge_influence: previous.pool_pledge_influence.clone(),
        decentralization_constant: previous.decentralization_constant.clone(),
        extra_entropy: previous.extra_entropy.clone(),
        // new from genesis
        ada_per_utxo_byte: genesis.lovelace_per_utxo_word,
        cost_models_for_script_languages: Some(CostModels {
            plutus_v1: from_alonzo_cost_models_map(
                &genesis.cost_models,
                &alonzo::Language::PlutusV1,
            ),
            plutus_v2: None,
            plutus_v3: None,
            unknown: Default::default(),
        }),
        execution_costs: Some(genesis.execution_prices.clone().into()),
        max_tx_ex_units: Some(from_config_exunits(&genesis.max_tx_ex_units)),
        max_block_ex_units: Some(from_config_exunits(&genesis.max_block_ex_units)),
        max_value_size: genesis.max_value_size,
        collateral_percentage: genesis.collateral_percentage,
        max_collateral_inputs: genesis.max_collateral_inputs,
        ..previous.clone()
    }
}

pub fn into_babbage(previous: &PParamsState, genesis: &alonzo::GenesisFile) -> PParamsState {
    PParamsState {
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
        execution_costs: previous.execution_costs.clone(),
        max_tx_ex_units: previous.max_tx_ex_units,
        max_block_ex_units: previous.max_block_ex_units,
        max_value_size: previous.max_value_size,
        collateral_percentage: previous.collateral_percentage,
        max_collateral_inputs: previous.max_collateral_inputs,
        expansion_rate: previous.expansion_rate.clone(),
        treasury_growth_rate: previous.treasury_growth_rate.clone(),
        maximum_epoch: previous.maximum_epoch,
        pool_pledge_influence: previous.pool_pledge_influence.clone(),
        decentralization_constant: previous.decentralization_constant.clone(),
        extra_entropy: previous.extra_entropy.clone(),
        cost_models_for_script_languages: Some(CostModels {
            plutus_v1: previous
                .cost_models_for_script_languages
                .as_ref()
                .and_then(|x| x.plutus_v1.clone()),
            plutus_v2: from_alonzo_cost_models_map(
                &genesis.cost_models,
                &alonzo::Language::PlutusV2,
            ),
            plutus_v3: None,
            unknown: Default::default(),
        }),
        ..previous.clone()
    }
}

pub fn into_conway(previous: &PParamsState, genesis: &conway::GenesisFile) -> PParamsState {
    PParamsState {
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
        // In the hardfork, the value got translated from words to bytes
        // Since the transformation from words to bytes is hardcoded, the transformation here is
        // also hardcoded
        ada_per_utxo_byte: previous.ada_per_utxo_byte / 8,
        execution_costs: previous.execution_costs.clone(),
        max_tx_ex_units: previous.max_tx_ex_units,
        max_block_ex_units: previous.max_block_ex_units,
        max_value_size: previous.max_value_size,
        collateral_percentage: previous.collateral_percentage,
        max_collateral_inputs: previous.max_collateral_inputs,
        expansion_rate: previous.expansion_rate.clone(),
        treasury_growth_rate: previous.treasury_growth_rate.clone(),
        maximum_epoch: previous.maximum_epoch,
        pool_pledge_influence: previous.pool_pledge_influence.clone(),
        cost_models_for_script_languages: Some(CostModels {
            plutus_v1: previous
                .cost_models_for_script_languages
                .as_ref()
                .and_then(|x| x.plutus_v1.clone()),
            plutus_v2: previous
                .cost_models_for_script_languages
                .as_ref()
                .and_then(|x| x.plutus_v2.clone()),
            plutus_v3: Some(genesis.plutus_v3_cost_model.clone()),
            unknown: Default::default(),
        }),
        pool_voting_thresholds: Some(from_conway_pool_voting_thresholds(
            &genesis.pool_voting_thresholds,
        )),
        drep_voting_thresholds: Some(from_conway_drep_voting_thresholds(
            &genesis.d_rep_voting_thresholds,
        )),
        min_committee_size: genesis.committee_min_size,
        committee_term_limit: genesis.committee_max_term_length.into(),
        governance_action_validity_period: genesis.gov_action_lifetime.into(),
        governance_action_deposit: genesis.gov_action_deposit,
        drep_deposit: genesis.d_rep_deposit,
        drep_inactivity_period: genesis.d_rep_activity.into(),
        minfee_refscript_cost_per_byte: Some(pallas::ledger::primitives::conway::RationalNumber {
            numerator: genesis.min_fee_ref_script_cost_per_byte,
            denominator: 1,
        }),
        ..previous.clone()
    }
}

pub fn migrate_pparams(
    current: &PParamsState,
    genesis: &Genesis,
    next_protocol: usize,
) -> PParamsState {
    let current_protocol = current.protocol_major();
    // Source: https://github.com/cardano-foundation/CIPs/blob/master/CIP-0059/feature-table.md
    // NOTE: part of the confusion here is that there are two versioning schemes
    // that can be easily conflated:
    // - The protocol version, negotiated in the networking layer
    // - The protocol version broadcast in the block header
    // Generally, these refer to the latter; the update proposals jump from 2 to 5,
    // because the node team decided it would be helpful to have these in sync.

    match (current_protocol, next_protocol) {
        // Protocol starts at version 0;
        // There was one intra-era "hard fork" in byron (even though they weren't called that yet)
        (0, 1) => from_byron_genesis(&genesis.byron),
        // Protocol version 2 transitions from Byron to Shelley
        (1, 2) => from_shelley_genesis(&genesis.shelley),
        // Two intra-era hard forks, named Allegra (3) and Mary (4); we don't have separate types
        // for these eras
        (2, 3) => current.clone(),
        (3, 4) => current.clone(),
        // Protocol version 5 transitions from Shelley (Mary, technically) to Alonzo
        (4, 5) => into_alonzo(current, &genesis.alonzo),
        // One intra-era hard-fork in alonzo at protocol version 6
        (5, 6) => current.clone(),
        // Protocol version 7 transitions from Alonzo to Babbage
        (6, 7) => into_babbage(current, &genesis.alonzo),
        // One intra-era hard-fork in babbage at protocol version 8
        (7, 8) => current.clone(),
        // Protocol version 9 transitions from Babbage to Conway
        (8, 9) => into_conway(current, &genesis.conway),
        // One intra-era hard-fork in conway at protocol version 10
        (9, 10) => current.clone(),
        (from, to) => {
            unimplemented!("don't know how to hardfork from version {from} to {to}",)
        }
    }
}
