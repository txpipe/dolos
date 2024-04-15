use gasket::framework::{AsWorkError, WorkerError};
use pallas::{
    applying::utils::{
        AlonzoProtParams, BabbageProtParams, ByronProtParams, MultiEraProtocolParameters,
        ShelleyProtParams,
    },
    ledger::{
        configs::{byron, shelley},
        traverse::{Era, MultiEraUpdate},
    },
};
use tracing::warn;

use super::PParamsBody;

//mod test_data;

pub struct Genesis<'a> {
    pub byron: &'a byron::GenesisFile,
    pub shelley: &'a shelley::GenesisFile,
}

fn pparams_from_byron_genesis(
    byron: &byron::GenesisFile,
) -> Result<MultiEraProtocolParameters, WorkerError> {
    let out = MultiEraProtocolParameters::Byron(ByronProtParams {
        summand: byron
            .block_version_data
            .tx_fee_policy
            .summand
            .parse()
            .or_panic()?,
        multiplier: byron
            .block_version_data
            .tx_fee_policy
            .multiplier
            .parse()
            .or_panic()?,
        max_tx_size: byron.block_version_data.max_tx_size.parse().or_panic()?,
        script_version: byron.block_version_data.script_version as u16,
        slot_duration: byron.block_version_data.slot_duration.parse().or_panic()?,
        max_block_size: byron.block_version_data.max_block_size.parse().or_panic()?,
        max_header_size: byron
            .block_version_data
            .max_header_size
            .parse()
            .or_panic()?,
        max_proposal_size: byron
            .block_version_data
            .max_proposal_size
            .parse()
            .or_panic()?,
        mpc_thd: byron.block_version_data.mpc_thd.parse().or_panic()?,
        heavy_del_thd: byron.block_version_data.heavy_del_thd.parse().or_panic()?,
        update_vote_thd: byron
            .block_version_data
            .update_vote_thd
            .parse()
            .or_panic()?,
        update_proposal_thd: byron
            .block_version_data
            .update_proposal_thd
            .parse()
            .or_panic()?,
        update_implicit: byron
            .block_version_data
            .update_implicit
            .parse()
            .or_panic()?,
        soft_fork_rule: (
            byron
                .block_version_data
                .softfork_rule
                .init_thd
                .parse()
                .or_panic()?,
            byron
                .block_version_data
                .softfork_rule
                .min_thd
                .parse()
                .or_panic()?,
            byron
                .block_version_data
                .softfork_rule
                .thd_decrement
                .parse()
                .or_panic()?,
        ),
        unlock_stake_epoch: byron
            .block_version_data
            .unlock_stake_epoch
            .parse()
            .or_panic()?,
    });

    Ok(out)
}

fn pparams_from_shelley_genesis(
    shelley: &shelley::GenesisFile,
) -> Result<MultiEraProtocolParameters, WorkerError> {
    let out = MultiEraProtocolParameters::Shelley(ShelleyProtParams {
        minfee_a: shelley.protocol_params.min_fee_a as u32,
        minfee_b: shelley.protocol_params.min_fee_b as u32,
        max_block_body_size: shelley.protocol_params.max_block_body_size.unwrap(),
        max_transaction_size: shelley.protocol_params.max_tx_size as u32,
        max_block_header_size: shelley.protocol_params.max_block_header_size.unwrap(),
        key_deposit: shelley.protocol_params.key_deposit.unwrap() as u64,
        pool_deposit: shelley.protocol_params.pool_deposit.unwrap(),
        maximum_epoch: Default::default(),
        desired_number_of_stake_pools: Default::default(),
        pool_pledge_influence: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        expansion_rate: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        treasury_growth_rate: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        decentralization_constant: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        extra_entropy: pallas::ledger::primitives::conway::Nonce {
            variant: pallas::ledger::primitives::alonzo::NonceVariant::NeutralNonce,
            hash: Default::default(),
        },
        protocol_version: (
            shelley
                .protocol_params
                .protocol_version
                .as_ref()
                .unwrap()
                .major
                .unwrap() as u64,
            shelley
                .protocol_params
                .protocol_version
                .as_ref()
                .unwrap()
                .minor
                .unwrap() as u64,
        ),
        min_utxo_value: shelley.protocol_params.min_u_tx_o_value,
    });

    Ok(out)
}

fn pparams_from_alonzo_genesis() -> Result<MultiEraProtocolParameters, WorkerError> {
    let out = MultiEraProtocolParameters::Alonzo(AlonzoProtParams {
        minfee_a: Default::default(),
        minfee_b: Default::default(),
        max_block_body_size: Default::default(),
        max_transaction_size: Default::default(),
        max_block_header_size: Default::default(),
        key_deposit: Default::default(),
        pool_deposit: Default::default(),
        maximum_epoch: Default::default(),
        desired_number_of_stake_pools: Default::default(),
        pool_pledge_influence: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        expansion_rate: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        treasury_growth_rate: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        decentralization_constant: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        extra_entropy: pallas::ledger::primitives::conway::Nonce {
            variant: pallas::ledger::primitives::conway::NonceVariant::NeutralNonce,
            hash: Default::default(),
        },
        protocol_version: Default::default(),
        min_pool_cost: Default::default(),
        ada_per_utxo_byte: Default::default(),
        cost_models_for_script_languages: pallas::codec::utils::KeyValuePairs::Def(vec![]),
        execution_costs: pallas::ledger::primitives::alonzo::ExUnitPrices {
            mem_price: pallas::ledger::primitives::alonzo::RationalNumber {
                numerator: Default::default(),
                denominator: Default::default(),
            },
            step_price: pallas::ledger::primitives::alonzo::RationalNumber {
                numerator: Default::default(),
                denominator: Default::default(),
            },
        },
        max_tx_ex_units: pallas::ledger::primitives::conway::ExUnits {
            mem: Default::default(),
            steps: Default::default(),
        },
        max_block_ex_units: pallas::ledger::primitives::conway::ExUnits {
            mem: Default::default(),
            steps: Default::default(),
        },
        max_value_size: Default::default(),
        collateral_percentage: Default::default(),
        max_collateral_inputs: Default::default(),
    });

    Ok(out)
}

fn pparams_from_babbage_genesis() -> Result<MultiEraProtocolParameters, WorkerError> {
    let out = MultiEraProtocolParameters::Babbage(BabbageProtParams {
        minfee_a: Default::default(),
        minfee_b: Default::default(),
        max_block_body_size: Default::default(),
        max_transaction_size: Default::default(),
        max_block_header_size: Default::default(),
        key_deposit: Default::default(),
        pool_deposit: Default::default(),
        maximum_epoch: Default::default(),
        desired_number_of_stake_pools: Default::default(),
        pool_pledge_influence: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        expansion_rate: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        treasury_growth_rate: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        decentralization_constant: pallas::ledger::primitives::alonzo::RationalNumber {
            numerator: Default::default(),
            denominator: Default::default(),
        },
        extra_entropy: pallas::ledger::primitives::conway::Nonce {
            variant: pallas::ledger::primitives::conway::NonceVariant::NeutralNonce,
            hash: Default::default(),
        },
        protocol_version: Default::default(),
        min_pool_cost: Default::default(),
        ada_per_utxo_byte: Default::default(),
        cost_models_for_script_languages: pallas::ledger::primitives::babbage::CostMdls {
            plutus_v1: Default::default(),
            plutus_v2: Default::default(),
        },
        execution_costs: pallas::ledger::primitives::alonzo::ExUnitPrices {
            mem_price: pallas::ledger::primitives::alonzo::RationalNumber {
                numerator: Default::default(),
                denominator: Default::default(),
            },
            step_price: pallas::ledger::primitives::alonzo::RationalNumber {
                numerator: Default::default(),
                denominator: Default::default(),
            },
        },
        max_tx_ex_units: pallas::ledger::primitives::conway::ExUnits {
            mem: Default::default(),
            steps: Default::default(),
        },
        max_block_ex_units: pallas::ledger::primitives::conway::ExUnits {
            mem: Default::default(),
            steps: Default::default(),
        },
        max_value_size: Default::default(),
        collateral_percentage: Default::default(),
        max_collateral_inputs: Default::default(),
    });

    Ok(out)
}

fn apply_era_hardfork(
    genesis: &Genesis,
    new_protocol: u64,
) -> Result<MultiEraProtocolParameters, WorkerError> {
    match new_protocol {
        1 => pparams_from_byron_genesis(genesis.byron),
        2..=4 => pparams_from_shelley_genesis(genesis.shelley),
        5 => pparams_from_alonzo_genesis(),
        6 => pparams_from_babbage_genesis(),
        x => {
            unimplemented!("don't know how to handle hardfork for protocol {x}");
        }
    }
}

fn apply_param_update(
    genesis: &Genesis,
    era: Era,
    current: MultiEraProtocolParameters,
    update: MultiEraUpdate,
) -> Result<MultiEraProtocolParameters, WorkerError> {
    match current {
        MultiEraProtocolParameters::Byron(mut pparams) => {
            assert_eq!(u16::from(era), 1, "pparam update doesn't match era");

            if let Some((major, _, _)) = update.byron_proposed_block_version() {
                warn!(major, "found new byron protocol update proposal");
                return apply_era_hardfork(genesis, major as u64);
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

            Ok(MultiEraProtocolParameters::Byron(pparams))
        }
        MultiEraProtocolParameters::Shelley(mut pparams) => {
            match u16::from(era) {
                2..=4 => (),
                _ => panic!("pparam update doesn't match era"),
            }

            if let Some((major, _)) = update.first_proposed_protocol_version() {
                warn!(
                    major,
                    "found new [shelley, mary, allegra] protocol update proposal"
                );
                return apply_era_hardfork(genesis, major);
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

            Ok(MultiEraProtocolParameters::Shelley(pparams))
        }
        MultiEraProtocolParameters::Alonzo(mut pparams) => {
            assert_eq!(u16::from(era), 5, "pparam update doesn't match era");

            if let Some((major, _)) = update.first_proposed_protocol_version() {
                warn!(major, "found new alonzo protocol update proposal");
                return apply_era_hardfork(genesis, major);
            }

            Ok(MultiEraProtocolParameters::Alonzo(pparams))
        }
        MultiEraProtocolParameters::Babbage(mut pparams) => {
            assert_eq!(u16::from(era), 6, "pparam update doesn't match era");

            if let Some((major, _)) = update.first_proposed_protocol_version() {
                warn!(major, "found new babbage protocol update proposal");
                return apply_era_hardfork(genesis, major);
            }

            Ok(MultiEraProtocolParameters::Babbage(pparams))
        }
        _ => unimplemented!(),
    }
}

// TODO: perform proper protocol parameters update for the Alonzo era.
pub fn fold_pparams(
    genesis: Genesis,
    updates: &[PParamsBody],
) -> Result<MultiEraProtocolParameters, WorkerError> {
    let mut prot_params = apply_era_hardfork(&genesis, 1)?;

    for PParamsBody(era, cbor) in updates {
        let era = Era::try_from(*era).or_panic()?;
        let update = MultiEraUpdate::decode_for_era(era, &cbor).or_panic()?;
        prot_params = apply_param_update(&genesis, era, prot_params, update)?;
    }

    Ok(prot_params)
}
