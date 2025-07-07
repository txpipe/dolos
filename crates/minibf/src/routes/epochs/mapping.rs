use std::collections::HashMap;

use crate::{mapping::IntoModel, routes::epochs::cost_models::get_named_cost_model};
use axum::http::StatusCode;
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;
use dolos_core::Genesis;
use pallas::ledger::{
    primitives::{RationalNumber, conway::CostModels},
    validate::utils::{
        AlonzoProtParams, BabbageProtParams, ByronProtParams, ConwayProtParams,
        MultiEraProtocolParameters, ShelleyProtParams,
    },
};

fn rational_to_f64<const DECIMALS: u8>(val: &RationalNumber) -> f64 {
    let res = val.numerator as f64 / val.denominator as f64;
    let multiplier = 10_f64.powi(DECIMALS as i32);

    (res * multiplier).round() / multiplier
}

fn cost_models_to_key_value(cost_models: &CostModels) -> Vec<(&'static str, &[i64])> {
    let maybe = vec![
        ("plutus_v1", cost_models.plutus_v1.as_ref()),
        ("plutus_v2", cost_models.plutus_v2.as_ref()),
        ("plutus_v3", cost_models.plutus_v3.as_ref()),
    ];

    maybe
        .into_iter()
        .filter_map(|(k, v)| v.map(|v| (k, v.as_slice())))
        .collect()
}

fn map_cost_models_raw(cost_models: &CostModels) -> HashMap<String, serde_json::Value> {
    cost_models_to_key_value(cost_models)
        .into_iter()
        .map(|(k, v)| (k.to_string(), serde_json::to_value(v).unwrap()))
        .collect()
}

fn map_cost_models_named(cost_models: &CostModels) -> HashMap<String, serde_json::Value> {
    cost_models_to_key_value(cost_models)
        .into_iter()
        .map(|(k, v)| (k.to_string(), get_named_cost_model(1, v)))
        .collect()
}

pub struct ParametersModelBuilder<'a> {
    pub epoch: u64,
    pub params: MultiEraProtocolParameters,
    pub genesis: &'a Genesis,
}

impl<'a> ParametersModelBuilder<'a> {
    fn map_conway_params(&self, params: &ConwayProtParams) -> EpochParamContent {
        let Self { genesis, epoch, .. } = self;

        EpochParamContent {
            epoch: *epoch as i32,
            a0: rational_to_f64::<3>(&genesis.shelley.protocol_params.a0),
            e_max: genesis.shelley.protocol_params.e_max as i32,
            max_tx_size: params.max_transaction_size as i32,
            max_block_size: params.max_block_body_size as i32,
            max_block_header_size: params.max_block_header_size as i32,
            min_fee_a: params.minfee_a as i32,
            min_fee_b: params.minfee_b as i32,
            min_utxo: genesis.shelley.protocol_params.min_utxo_value.to_string(),
            coins_per_utxo_size: Some(params.ada_per_utxo_byte.to_string()),
            coins_per_utxo_word: Some(params.ada_per_utxo_byte.to_string()),
            key_deposit: params.key_deposit.to_string(),
            pool_deposit: params.pool_deposit.to_string(),
            n_opt: params.desired_number_of_stake_pools as i32,
            rho: rational_to_f64::<3>(&params.expansion_rate),
            tau: rational_to_f64::<3>(&params.treasury_growth_rate),
            min_pool_cost: params.min_pool_cost.to_string(),
            protocol_major_ver: params.protocol_version.0 as i32,
            protocol_minor_ver: params.protocol_version.1 as i32,
            max_val_size: Some(params.max_value_size.to_string()),
            collateral_percent: Some(params.collateral_percentage as i32),
            max_collateral_inputs: Some(params.max_collateral_inputs as i32),
            price_mem: Some(rational_to_f64::<3>(&params.execution_costs.mem_price)),
            price_step: Some(rational_to_f64::<9>(&params.execution_costs.step_price)),
            max_tx_ex_mem: Some(params.max_tx_ex_units.mem.to_string()),
            max_tx_ex_steps: Some(params.max_tx_ex_units.steps.to_string()),
            max_block_ex_mem: Some(params.max_block_ex_units.mem.to_string()),
            max_block_ex_steps: Some(params.max_block_ex_units.steps.to_string()),
            min_fee_ref_script_cost_per_byte: Some(rational_to_f64::<3>(
                &params.minfee_refscript_cost_per_byte,
            )),
            drep_deposit: Some(params.drep_deposit.to_string()),
            drep_activity: Some(params.drep_inactivity_period.to_string()),
            cost_models_raw: Some(Some(map_cost_models_raw(
                &params.cost_models_for_script_languages,
            ))),
            cost_models: Some(map_cost_models_named(
                &params.cost_models_for_script_languages,
            )),
            decentralisation_param: rational_to_f64::<3>(
                &genesis.shelley.protocol_params.decentralisation_param,
            ),

            pvt_motion_no_confidence: Some(rational_to_f64::<3>(
                &params.pool_voting_thresholds.motion_no_confidence,
            )),
            pvt_committee_normal: Some(rational_to_f64::<3>(
                &params.pool_voting_thresholds.committee_normal,
            )),
            pvt_committee_no_confidence: Some(rational_to_f64::<3>(
                &params.pool_voting_thresholds.committee_no_confidence,
            )),
            pvt_hard_fork_initiation: Some(rational_to_f64::<3>(
                &params.pool_voting_thresholds.hard_fork_initiation,
            )),
            dvt_motion_no_confidence: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.motion_no_confidence,
            )),
            dvt_committee_normal: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.committee_normal,
            )),
            dvt_committee_no_confidence: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.committee_no_confidence,
            )),
            dvt_update_to_constitution: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.update_constitution,
            )),
            dvt_hard_fork_initiation: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.hard_fork_initiation,
            )),
            dvt_p_p_network_group: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.pp_network_group,
            )),
            dvt_p_p_economic_group: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.pp_economic_group,
            )),
            dvt_p_p_technical_group: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.pp_technical_group,
            )),
            dvt_p_p_gov_group: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.pp_governance_group,
            )),
            dvt_treasury_withdrawal: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.treasury_withdrawal,
            )),
            committee_min_size: Some(params.min_committee_size.to_string()),
            committee_max_term_length: Some(params.committee_term_limit.to_string()),
            gov_action_lifetime: Some(params.governance_action_validity_period.to_string()),
            gov_action_deposit: Some(params.governance_action_deposit.to_string()),
            pvtpp_security_group: Some(rational_to_f64::<3>(
                &params.pool_voting_thresholds.security_voting_threshold,
            )),
            pvt_p_p_security_group: Some(rational_to_f64::<3>(
                &params.drep_voting_thresholds.pp_technical_group,
            )),
            // TODO: confirm mapping
            nonce: String::default(),
            extra_entropy: None,
        }
    }

    fn map_babbage_params(&self, params: &BabbageProtParams) -> EpochParamContent {
        let Self { genesis, epoch, .. } = self;

        EpochParamContent {
            epoch: *epoch as i32,
            a0: rational_to_f64::<3>(&genesis.shelley.protocol_params.a0),
            e_max: genesis.shelley.protocol_params.e_max as i32,
            max_tx_size: params.max_transaction_size as i32,
            max_block_size: params.max_block_body_size as i32,
            max_block_header_size: params.max_block_header_size as i32,
            min_fee_a: params.minfee_a as i32,
            min_fee_b: params.minfee_b as i32,
            min_utxo: genesis.shelley.protocol_params.min_utxo_value.to_string(),
            coins_per_utxo_size: Some(params.ada_per_utxo_byte.to_string()),
            coins_per_utxo_word: Some(params.ada_per_utxo_byte.to_string()),
            key_deposit: params.key_deposit.to_string(),
            pool_deposit: params.pool_deposit.to_string(),
            n_opt: params.desired_number_of_stake_pools as i32,
            rho: rational_to_f64::<3>(&params.expansion_rate),
            tau: rational_to_f64::<3>(&params.treasury_growth_rate),
            min_pool_cost: params.min_pool_cost.to_string(),
            protocol_major_ver: params.protocol_version.0 as i32,
            protocol_minor_ver: params.protocol_version.1 as i32,
            max_val_size: Some(params.max_value_size.to_string()),
            collateral_percent: Some(params.collateral_percentage as i32),
            max_collateral_inputs: Some(params.max_collateral_inputs as i32),
            price_mem: Some(rational_to_f64::<3>(&params.execution_costs.mem_price)),
            price_step: Some(rational_to_f64::<9>(&params.execution_costs.step_price)),
            max_tx_ex_mem: Some(params.max_tx_ex_units.mem.to_string()),
            max_tx_ex_steps: Some(params.max_tx_ex_units.steps.to_string()),
            max_block_ex_mem: Some(params.max_block_ex_units.mem.to_string()),
            max_block_ex_steps: Some(params.max_block_ex_units.steps.to_string()),
            decentralisation_param: rational_to_f64::<3>(
                &genesis.shelley.protocol_params.decentralisation_param,
            ),
            ..Default::default()
        }
    }

    fn map_alonzo_params(&self, params: &AlonzoProtParams) -> EpochParamContent {
        let Self { genesis, epoch, .. } = self;

        EpochParamContent {
            epoch: *epoch as i32,
            a0: rational_to_f64::<3>(&genesis.shelley.protocol_params.a0),
            e_max: genesis.shelley.protocol_params.e_max as i32,
            max_tx_size: params.max_transaction_size as i32,
            max_block_size: params.max_block_body_size as i32,
            max_block_header_size: params.max_block_header_size as i32,
            min_fee_a: params.minfee_a as i32,
            min_fee_b: params.minfee_b as i32,
            min_utxo: genesis.shelley.protocol_params.min_utxo_value.to_string(),
            coins_per_utxo_size: Some(params.ada_per_utxo_byte.to_string()),
            coins_per_utxo_word: Some(params.ada_per_utxo_byte.to_string()),
            key_deposit: params.key_deposit.to_string(),
            pool_deposit: params.pool_deposit.to_string(),
            n_opt: params.desired_number_of_stake_pools as i32,
            rho: rational_to_f64::<3>(&params.expansion_rate),
            tau: rational_to_f64::<3>(&params.treasury_growth_rate),
            min_pool_cost: params.min_pool_cost.to_string(),
            protocol_major_ver: params.protocol_version.0 as i32,
            protocol_minor_ver: params.protocol_version.1 as i32,
            max_val_size: Some(params.max_value_size.to_string()),
            collateral_percent: Some(params.collateral_percentage as i32),
            max_collateral_inputs: Some(params.max_collateral_inputs as i32),
            price_mem: Some(rational_to_f64::<3>(&params.execution_costs.mem_price)),
            price_step: Some(rational_to_f64::<9>(&params.execution_costs.step_price)),
            max_tx_ex_mem: Some(params.max_tx_ex_units.mem.to_string()),
            max_tx_ex_steps: Some(params.max_tx_ex_units.steps.to_string()),
            max_block_ex_mem: Some(params.max_block_ex_units.mem.to_string()),
            max_block_ex_steps: Some(params.max_block_ex_units.steps.to_string()),
            decentralisation_param: rational_to_f64::<3>(
                &genesis.shelley.protocol_params.decentralisation_param,
            ),
            ..Default::default()
        }
    }

    fn map_shelley_params(&self, params: &ShelleyProtParams) -> EpochParamContent {
        let Self { genesis, epoch, .. } = self;

        EpochParamContent {
            epoch: *epoch as i32,
            a0: rational_to_f64::<3>(&genesis.shelley.protocol_params.a0),
            e_max: genesis.shelley.protocol_params.e_max as i32,
            max_tx_size: params.max_transaction_size as i32,
            max_block_size: params.max_block_body_size as i32,
            max_block_header_size: params.max_block_header_size as i32,
            min_fee_a: params.minfee_a as i32,
            min_fee_b: params.minfee_b as i32,
            min_utxo: genesis.shelley.protocol_params.min_utxo_value.to_string(),
            key_deposit: params.key_deposit.to_string(),
            pool_deposit: params.pool_deposit.to_string(),
            n_opt: params.desired_number_of_stake_pools as i32,
            rho: rational_to_f64::<3>(&params.expansion_rate),
            tau: rational_to_f64::<3>(&params.treasury_growth_rate),
            min_pool_cost: params.min_pool_cost.to_string(),
            protocol_major_ver: params.protocol_version.0 as i32,
            protocol_minor_ver: params.protocol_version.1 as i32,
            decentralisation_param: rational_to_f64::<3>(
                &genesis.shelley.protocol_params.decentralisation_param,
            ),
            ..Default::default()
        }
    }
}

impl<'a> IntoModel<EpochParamContent> for ParametersModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<EpochParamContent, axum::http::StatusCode> {
        let out = match &self.params {
            MultiEraProtocolParameters::Conway(x) => self.map_conway_params(&x),
            MultiEraProtocolParameters::Babbage(x) => self.map_babbage_params(&x),
            MultiEraProtocolParameters::Alonzo(x) => self.map_alonzo_params(&x),
            MultiEraProtocolParameters::Shelley(x) => self.map_shelley_params(&x),
            // TODO: define mapping for byron params
            _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        };

        Ok(out)
    }
}
