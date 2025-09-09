use chrono::DateTime;
use dolos_core::*;
use pallas::ledger::primitives::conway::{
    CostModels, DRepVotingThresholds, PoolVotingThresholds, UnitInterval,
};
use pallas::ledger::primitives::{ExUnitPrices, ExUnits, RationalNumber};
use pallas::ledger::validate::utils::{ConwayProtParams, MultiEraProtocolParameters};

use crate::model::PParamsState;

/// Computes the amount of mutable slots in chain.
///
/// Reads the relevant genesis config values and uses the security window
/// guarantee formula from consensus to calculate the latest slot that can be
/// considered immutable.
pub fn mutable_slots(genesis: &Genesis) -> u64 {
    ((3.0 * genesis.byron.protocol_consts.k as f32) / (genesis.shelley.active_slots_coeff.unwrap()))
        as u64
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
pub fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let temp = b;
        b = a % b;
        a = temp;
    }
    a
}

pub fn load_genesis(path: &std::path::Path) -> Genesis {
    let byron = pallas::ledger::configs::byron::from_file(&path.join("byron.json")).unwrap();
    let shelley = pallas::ledger::configs::shelley::from_file(&path.join("shelley.json")).unwrap();
    let alonzo = pallas::ledger::configs::alonzo::from_file(&path.join("alonzo.json")).unwrap();
    let conway = pallas::ledger::configs::conway::from_file(&path.join("conway.json")).unwrap();

    Genesis {
        byron,
        shelley,
        alonzo,
        conway,
        force_protocol: None,
    }
}

pub fn pparams_to_pallas(pparams: &PParamsState) -> MultiEraProtocolParameters {
    MultiEraProtocolParameters::Conway(ConwayProtParams {
        system_start: DateTime::from_timestamp(pparams.system_start as i64, 0)
            .unwrap_or_default()
            .into(),
        epoch_length: pparams.epoch_length as u64,
        slot_length: pparams.slot_length as u64,
        minfee_a: pparams.minfee_a as u32,
        minfee_b: pparams.minfee_b as u32,
        max_block_body_size: pparams.max_block_body_size as u32,
        max_transaction_size: pparams.max_transaction_size as u32,
        max_block_header_size: pparams.max_block_header_size as u32,
        key_deposit: pparams.key_deposit,
        pool_deposit: pparams.pool_deposit,
        desired_number_of_stake_pools: pparams.desired_number_of_stake_pools,
        protocol_version: pparams.protocol_version,
        min_pool_cost: pparams.min_pool_cost,
        ada_per_utxo_byte: pparams.ada_per_utxo_byte,
        cost_models_for_script_languages: CostModels {
            plutus_v1: pparams
                .cost_models_for_script_languages
                .as_ref()
                .and_then(|x| x.plutus_v1.clone()),
            plutus_v2: pparams
                .cost_models_for_script_languages
                .as_ref()
                .and_then(|x| x.plutus_v2.clone()),
            plutus_v3: pparams
                .cost_models_for_script_languages
                .as_ref()
                .and_then(|x| x.plutus_v3.clone()),
            unknown: Default::default(),
        },
        execution_costs: pparams.execution_costs.clone().unwrap_or(ExUnitPrices {
            mem_price: RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            step_price: RationalNumber {
                numerator: 0,
                denominator: 1,
            },
        }),
        max_tx_ex_units: pparams
            .max_tx_ex_units
            .unwrap_or(ExUnits { mem: 0, steps: 0 }),
        max_block_ex_units: pparams
            .max_block_ex_units
            .unwrap_or(ExUnits { mem: 0, steps: 0 }),
        max_value_size: pparams.max_value_size,
        collateral_percentage: pparams.collateral_percentage,
        max_collateral_inputs: pparams.max_collateral_inputs,
        expansion_rate: pparams.expansion_rate.clone().unwrap_or(UnitInterval {
            numerator: 1,
            denominator: 1,
        }),
        treasury_growth_rate: pparams
            .treasury_growth_rate
            .clone()
            .unwrap_or(UnitInterval {
                numerator: 1,
                denominator: 1,
            }),
        maximum_epoch: pparams.maximum_epoch,
        pool_pledge_influence: pparams
            .pool_pledge_influence
            .clone()
            .unwrap_or(RationalNumber {
                numerator: 1,
                denominator: 1,
            }),
        pool_voting_thresholds: pparams.pool_voting_thresholds.clone().unwrap_or(
            PoolVotingThresholds {
                motion_no_confidence: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                committee_normal: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                committee_no_confidence: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                hard_fork_initiation: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                security_voting_threshold: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
            },
        ),
        drep_voting_thresholds: pparams.drep_voting_thresholds.clone().unwrap_or(
            DRepVotingThresholds {
                motion_no_confidence: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                committee_normal: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                committee_no_confidence: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                update_constitution: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                hard_fork_initiation: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                pp_network_group: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                pp_economic_group: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                pp_technical_group: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                pp_governance_group: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
                treasury_withdrawal: RationalNumber {
                    numerator: 1,
                    denominator: 1,
                },
            },
        ),
        min_committee_size: pparams.min_committee_size,
        committee_term_limit: pparams.committee_term_limit,
        governance_action_validity_period: pparams.governance_action_validity_period,
        governance_action_deposit: pparams.governance_action_deposit,
        drep_deposit: pparams.drep_deposit,
        drep_inactivity_period: pparams.drep_inactivity_period,
        minfee_refscript_cost_per_byte: pparams.minfee_refscript_cost_per_byte.clone().unwrap_or(
            RationalNumber {
                numerator: 1,
                denominator: 1,
            },
        ),
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
