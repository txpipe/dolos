use pallas::{
    ledger::validate::utils::MultiEraProtocolParameters,
    ledger::{
        configs::{alonzo, byron, conway, shelley},
        traverse::MultiEraUpdate,
    },
};
use tracing::debug;

use dolos_core::Genesis;

use crate::PParamsState;

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

            if let Some(new) = update.byron_proposed_block_version() {
                debug!("found new byron block version update proposal");
                pparams.block_version = new;
            }

            MultiEraProtocolParameters::Byron(pparams)
        }
        MultiEraProtocolParameters::Shelley(mut pparams) => {
            apply_field!(pparams, update, protocol_version);
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
            apply_field!(pparams, update, protocol_version);
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
                debug!(
                    ?value,
                    "found new cost_models_for_script_languages update proposal"
                );

                pparams.cost_models_for_script_languages = value;
            }

            MultiEraProtocolParameters::Alonzo(pparams)
        }
        MultiEraProtocolParameters::Babbage(mut pparams) => {
            apply_field!(pparams, update, protocol_version);
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
                debug!(
                    ?value,
                    "found new cost_models_for_script_languages update proposal"
                );

                pparams.cost_models_for_script_languages = value;
            }

            MultiEraProtocolParameters::Babbage(pparams)
        }
        MultiEraProtocolParameters::Conway(mut pparams) => {
            apply_field!(pparams, update, protocol_version);
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
                debug!(
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
