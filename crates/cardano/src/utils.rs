use chrono::DateTime;
use dolos_core::*;
use pallas::ledger::validate::utils::{ConwayProtParams, MultiEraProtocolParameters};

use crate::PParamsSet;

/// Computes the amount of mutable slots in chain.
///
/// Reads the relevant genesis config values and uses the security window
/// guarantee formula from consensus to calculate the latest slot that can be
/// considered immutable.
pub fn mutable_slots(genesis: &Genesis) -> u64 {
    ((3.0 * genesis.byron.protocol_consts.k as f32) / (genesis.shelley.active_slots_coeff.unwrap()))
        as u64
}

/// Computes the amount of mutable slots in chain.
///
/// Reads the relevant genesis config values and uses the security window
/// guarantee formula from consensus to calculate the latest slot that can be
/// considered immutable. Same as `mutable_slots`, added for the code to be similar in naming
/// convention to other implementations.
pub fn stability_window(genesis: &Genesis) -> u64 {
    mutable_slots(genesis)
}

/// Computes the amount of slots to substract to get the eta_h value for nonce calculation.
///
/// Similar to `mutable_slots` but with 4 instead of 3 as the constant. See the following issue for
/// refference: https://github.com/IntersectMBO/cardano-ledger/issues/1914
pub fn randomness_stability_window(genesis: &Genesis) -> u64 {
    ((4.0 * genesis.byron.protocol_consts.k as f32) / (genesis.shelley.active_slots_coeff.unwrap()))
        as u64
}

/// Get the window of slots used to calculate eta_h for epoch nonce calculation.
///
/// This is supposed be `randomness_stability_window` but due to a bug in the code it is dependant
/// on the protocol. See https://github.com/IntersectMBO/cardano-ledger/issues/1914.
pub fn nonce_stability_window(protocol: u16, genesis: &Genesis) -> u64 {
    if protocol >= 9 {
        randomness_stability_window(genesis)
    } else {
        stability_window(genesis)
    }
}

/// Computes the latest immutable slot
///
/// Takes the latest known tip, reads the relevant genesis config values and
/// uses the security window guarantee formula from consensus to calculate the
/// latest slot that can be considered immutable. This is used mainly to define
/// which slots can be finalized in the ledger store (aka: compaction).
pub fn lastest_immutable_slot(tip: BlockSlot, genesis: &Genesis) -> BlockSlot {
    tip.saturating_sub(mutable_slots(genesis))
}

pub fn float_to_rational(x: f32) -> pallas::ledger::primitives::conway::RationalNumber {
    let ratio: num_rational::Ratio<i64> = num_rational::Ratio::approximate_float(x).unwrap();

    pallas::ledger::primitives::conway::RationalNumber {
        numerator: *ratio.numer() as u64,
        denominator: *ratio.denom() as u64,
    }
}

// Helper function to calculate the Greatest Common Divisor
pub fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let temp = b;
        b = a % b;
        a = temp;
    }
    a
}

pub fn load_genesis(path: &std::path::Path) -> Genesis {
    Genesis::from_file_paths(
        path.join("byron.json"),
        path.join("shelley.json"),
        path.join("alonzo.json"),
        path.join("conway.json"),
        None,
    )
    .unwrap()
}

pub fn pparams_to_pallas(pparams: &PParamsSet) -> MultiEraProtocolParameters {
    MultiEraProtocolParameters::Conway(ConwayProtParams {
        system_start: DateTime::from_timestamp(pparams.system_start_or_default() as i64, 0)
            .unwrap_or_default()
            .into(),
        epoch_length: pparams.epoch_length_or_default(),
        slot_length: pparams.slot_length_or_default(),
        minfee_a: pparams.min_fee_a_or_default() as u32,
        minfee_b: pparams.min_fee_b_or_default() as u32,
        max_block_body_size: pparams.max_block_body_size_or_default() as u32,
        max_transaction_size: pparams.max_transaction_size_or_default() as u32,
        max_block_header_size: pparams.max_block_header_size_or_default() as u32,
        key_deposit: pparams.key_deposit_or_default(),
        pool_deposit: pparams.pool_deposit_or_default(),
        desired_number_of_stake_pools: pparams.desired_number_of_stake_pools_or_default(),
        protocol_version: pparams.protocol_version_or_default(),
        min_pool_cost: pparams.min_pool_cost_or_default(),
        ada_per_utxo_byte: pparams.ada_per_utxo_byte_or_default(),
        cost_models_for_script_languages: pparams.cost_models_for_script_languages_or_default(),
        execution_costs: pparams.execution_costs_or_default(),
        max_tx_ex_units: pparams.max_tx_ex_units_or_default(),
        max_block_ex_units: pparams.max_block_ex_units_or_default(),
        max_value_size: pparams.max_value_size_or_default(),
        collateral_percentage: pparams.collateral_percentage_or_default(),
        max_collateral_inputs: pparams.max_collateral_inputs_or_default(),
        expansion_rate: pparams.expansion_rate_or_default(),
        treasury_growth_rate: pparams.treasury_growth_rate_or_default(),
        maximum_epoch: pparams.maximum_epoch_or_default(),
        pool_pledge_influence: pparams.pool_pledge_influence_or_default(),
        pool_voting_thresholds: pparams.pool_voting_thresholds_or_default(),
        drep_voting_thresholds: pparams.drep_voting_thresholds_or_default(),
        min_committee_size: pparams.min_committee_size_or_default(),
        committee_term_limit: pparams.committee_term_limit_or_default(),
        governance_action_validity_period: pparams.governance_action_validity_period_or_default(),
        governance_action_deposit: pparams.governance_action_deposit_or_default(),
        drep_deposit: pparams.drep_deposit_or_default(),
        drep_inactivity_period: pparams.drep_inactivity_period_or_default(),
        minfee_refscript_cost_per_byte: pparams.min_fee_ref_script_cost_per_byte_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lastest_immutable_slot() {
        let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("test_data")
            .join("mainnet")
            .join("genesis");

        let genesis = load_genesis(&path);

        let tip: BlockSlot = 1_000_000;

        let result = lastest_immutable_slot(tip, &genesis);

        // slot delta in hours
        let delta_in_hours = tip.saturating_sub(result) / (60 * 60);

        // the well-known volatility window for mainnet is 36 hours.
        assert_eq!(delta_in_hours, 36);
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn assert_rational_eq(
            result: pallas::ledger::primitives::conway::RationalNumber,
            expected_num: u64,
            expected_den: u64,
            input: f32,
        ) {
            assert_eq!(
                result.numerator, expected_num,
                "Numerator mismatch for input {input}",
            );
            assert_eq!(
                result.denominator, expected_den,
                "Denominator mismatch for input {input}",
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
                // (0.33333334, 333333343, 1000000000), // These fails due to floating point
                // precision (0.66666669, 666666687, 1000000000),
            ];

            for &(input, expected_num, expected_den) in test_cases.iter() {
                let result = float_to_rational(input);
                assert_rational_eq(result, expected_num, expected_den, input);
            }
        }
    }
}
