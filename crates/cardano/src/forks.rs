use crate::{utils::float_to_rational, PParamValue, PParamsSet};
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

pub type Val = PParamValue;

fn from_config_nonce(config: &shelley::ExtraEntropy) -> Nonce {
    Nonce {
        variant: match config.tag {
            shelley::NonceVariant::NeutralNonce => NonceVariant::NeutralNonce,
            shelley::NonceVariant::Nonce => NonceVariant::Nonce,
        },
        hash: config.hash.as_ref().map(|x| {
            let bytes = hex::decode(x).unwrap();
            Hash::from(bytes.as_slice())
        }),
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

pub fn from_byron_genesis(byron: &byron::GenesisFile) -> PParamsSet {
    let version = &byron.block_version_data;

    PParamsSet::new()
        .with(Val::ProtocolVersion((0, 0)))
        .with(Val::SystemStart(byron.start_time))
        .with(Val::SlotLength(version.slot_duration))
        .with(Val::MinFeeA(version.tx_fee_policy.multiplier))
        .with(Val::MinFeeB(version.tx_fee_policy.summand))
        .with(Val::MaxBlockBodySize(version.max_block_size))
        .with(Val::MaxTransactionSize(version.max_tx_size))
        .with(Val::MaxBlockHeaderSize(version.max_header_size))
}

pub fn from_shelley_genesis(shelley: &shelley::GenesisFile) -> PParamsSet {
    let system_start = chrono::DateTime::parse_from_rfc3339(shelley.system_start.as_ref().unwrap())
        .expect("invalid system start value");

    let epoch_length = shelley.epoch_length.unwrap_or_default();
    let slot_length = shelley.slot_length.unwrap_or_default();
    let shelley = &shelley.protocol_params;
    let version = &shelley.protocol_version;

    PParamsSet::new()
        .with(Val::SystemStart(system_start.timestamp() as u64))
        .with(Val::ProtocolVersion(version.clone().into()))
        .with(Val::EpochLength(epoch_length as u64))
        .with(Val::SlotLength(slot_length as u64))
        .with(Val::MaxBlockBodySize(shelley.max_block_body_size as u64))
        .with(Val::MaxTransactionSize(shelley.max_tx_size as u64))
        .with(Val::MaxBlockHeaderSize(
            shelley.max_block_header_size as u64,
        ))
        .with(Val::KeyDeposit(shelley.key_deposit))
        .with(Val::MinUtxoValue(shelley.min_utxo_value))
        .with(Val::MinFeeA(shelley.min_fee_a as u64))
        .with(Val::MinFeeB(shelley.min_fee_b as u64))
        .with(Val::PoolDeposit(shelley.pool_deposit))
        .with(Val::DesiredNumberOfStakePools(shelley.n_opt))
        .with(Val::MinPoolCost(shelley.min_pool_cost))
        .with(Val::ExpansionRate(shelley.rho.clone()))
        .with(Val::TreasuryGrowthRate(shelley.tau.clone()))
        .with(Val::MaximumEpoch(shelley.e_max))
        .with(Val::PoolPledgeInfluence(shelley.a0.clone()))
        .with(Val::DecentralizationConstant(
            shelley.decentralisation_param.clone(),
        ))
        .with(Val::ExtraEntropy(from_config_nonce(&shelley.extra_entropy)))
}

pub fn into_alonzo(previous: &PParamsSet, genesis: &alonzo::GenesisFile) -> PParamsSet {
    let cost_models = CostModels {
        plutus_v1: from_alonzo_cost_models_map(&genesis.cost_models, &alonzo::Language::PlutusV1),
        plutus_v2: None,
        plutus_v3: None,
        unknown: Default::default(),
    };

    previous
        .clone()
        .with(Val::AdaPerUtxoByte(genesis.lovelace_per_utxo_word))
        .with(Val::CostModelsForScriptLanguages(cost_models))
        .with(Val::ExecutionCosts(genesis.execution_prices.clone().into()))
        .with(Val::MaxTxExUnits(from_config_exunits(
            &genesis.max_tx_ex_units,
        )))
        .with(Val::MaxBlockExUnits(from_config_exunits(
            &genesis.max_block_ex_units,
        )))
        .with(Val::MaxValueSize(genesis.max_value_size))
        .with(Val::CollateralPercentage(genesis.collateral_percentage))
        .with(Val::MaxCollateralInputs(genesis.max_collateral_inputs))
}

pub fn into_babbage(previous: &PParamsSet, genesis: &alonzo::GenesisFile) -> PParamsSet {
    let cost_models = previous
        .cost_models_for_script_languages()
        .unwrap_or_else(|| CostModels {
            plutus_v1: None,
            plutus_v2: None,
            plutus_v3: None,
            unknown: Default::default(),
        });

    let cost_models = CostModels {
        plutus_v2: from_alonzo_cost_models_map(&genesis.cost_models, &alonzo::Language::PlutusV2),
        ..cost_models
    };

    previous
        .clone()
        .with(Val::CostModelsForScriptLanguages(cost_models))
}

pub fn into_conway(previous: &PParamsSet, genesis: &conway::GenesisFile) -> PParamsSet {
    let cost_models = previous
        .cost_models_for_script_languages()
        .unwrap_or_else(|| CostModels {
            plutus_v1: None,
            plutus_v2: None,
            plutus_v3: None,
            unknown: Default::default(),
        });

    let cost_models = CostModels {
        plutus_v3: Some(genesis.plutus_v3_cost_model.clone()),
        ..cost_models
    };

    // In the hardfork, the value got translated from words to bytes
    // Since the transformation from words to bytes is hardcoded, the transformation
    // here is also hardcoded
    let ada_per_utxo_byte = previous.ada_per_utxo_byte().unwrap_or_default() / 8;

    previous
        .clone()
        .with(Val::AdaPerUtxoByte(ada_per_utxo_byte))
        .with(Val::CostModelsForScriptLanguages(cost_models))
        .with(Val::PoolVotingThresholds(
            from_conway_pool_voting_thresholds(&genesis.pool_voting_thresholds),
        ))
        .with(Val::DrepVotingThresholds(
            from_conway_drep_voting_thresholds(&genesis.d_rep_voting_thresholds),
        ))
        .with(Val::MinCommitteeSize(genesis.committee_min_size))
        .with(Val::CommitteeTermLimit(
            genesis.committee_max_term_length.into(),
        ))
        .with(Val::GovernanceActionValidityPeriod(
            genesis.gov_action_lifetime.into(),
        ))
        .with(Val::GovernanceActionDeposit(genesis.gov_action_deposit))
        .with(Val::DrepDeposit(genesis.d_rep_deposit))
        .with(Val::DrepInactivityPeriod(genesis.d_rep_activity.into()))
        .with(Val::MinFeeRefScriptCostPerByte(
            pallas::ledger::primitives::conway::RationalNumber {
                numerator: genesis.min_fee_ref_script_cost_per_byte,
                denominator: 1,
            },
        ))
}

/// Increments the protocol version by 1 without changing any other fields
pub fn intra_era_hardfork(current: &PParamsSet) -> PParamsSet {
    let version = current.protocol_major().unwrap_or_default();

    current
        .clone()
        .with(PParamValue::ProtocolVersion((version as u64 + 1, 0)))
}

// Source: https://github.com/cardano-foundation/CIPs/blob/master/CIP-0059/feature-table.md
// NOTE: part of the confusion here is that there are two versioning schemes
// that can be easily conflated:
// - The protocol version, negotiated in the networking layer
// - The protocol version broadcast in the block header
// Generally, these refer to the latter; the update proposals jump from 2 to 5,
// because the node team decided it would be helpful to have these in sync.
pub fn bump_pparams_version(current: &PParamsSet, genesis: &Genesis) -> PParamsSet {
    let current_protocol = current.protocol_major().unwrap_or_default();

    match current_protocol {
        // Protocol starts at version 0;
        // There was one intra-era "hard fork" in byron (even though they weren't called that yet)
        0 => intra_era_hardfork(current),
        // Protocol version 2 transitions from Byron to Shelley
        1 => from_shelley_genesis(&genesis.shelley),
        // Two intra-era hard forks, named Allegra (3) and Mary (4); we don't have separate types
        // for these eras
        2 => intra_era_hardfork(current),
        3 => intra_era_hardfork(current),
        // Protocol version 5 transitions from Shelley (Mary, technically) to Alonzo
        4 => into_alonzo(current, &genesis.alonzo),
        // One intra-era hard-fork in alonzo at protocol version 6
        5 => intra_era_hardfork(current),
        // Protocol version 7 transitions from Alonzo to Babbage
        6 => into_babbage(current, &genesis.alonzo),
        // One intra-era hard-fork in babbage at protocol version 8
        7 => intra_era_hardfork(current),
        // Protocol version 9 transitions from Babbage to Conway
        8 => into_conway(current, &genesis.conway),
        // One intra-era hard-fork in conway at protocol version 10
        9 => intra_era_hardfork(current),
        from => {
            unimplemented!("don't know how to bump from version {from}",)
        }
    }
}
