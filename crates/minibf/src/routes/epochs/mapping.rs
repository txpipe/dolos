use std::collections::HashMap;

use crate::{
    mapping::{rational_to_f64, IntoModel},
    routes::epochs::cost_models::get_named_cost_model,
};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;
use dolos_cardano::PParamsState;
use dolos_core::Genesis;
use pallas::ledger::primitives::conway::CostModels;

fn cost_models_to_key_value(cost_models: &CostModels) -> Vec<(&'static str, &[i64])> {
    let maybe = vec![
        ("PlutusV1", cost_models.plutus_v1.as_ref()),
        ("PlutusV2", cost_models.plutus_v2.as_ref()),
        ("PlutusV3", cost_models.plutus_v3.as_ref()),
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
        .map(|(k, v)| {
            (
                k.to_string(),
                get_named_cost_model(
                    match k {
                        "PlutusV1" => 1,
                        "PlutusV2" => 2,
                        "PlutusV3" => 3,
                        _ => unreachable!(),
                    },
                    v,
                ),
            )
        })
        .collect()
}

pub struct ParametersModelBuilder<'a> {
    pub epoch: u64,
    pub params: PParamsState,
    pub genesis: &'a Genesis,
}

impl<'a> IntoModel<EpochParamContent> for ParametersModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<EpochParamContent, axum::http::StatusCode> {
        let Self {
            genesis,
            epoch,
            params,
        } = self;

        let out = EpochParamContent {
            epoch: epoch as i32,
            a0: rational_to_f64::<3>(&genesis.shelley.protocol_params.a0),
            e_max: genesis.shelley.protocol_params.e_max as i32,
            max_tx_size: params.max_transaction_size as i32,
            max_block_size: params.max_block_body_size as i32,
            max_block_header_size: params.max_block_header_size as i32,
            min_fee_a: params.minfee_a as i32,
            min_fee_b: params.minfee_b as i32,
            min_utxo: params.ada_per_utxo_byte.to_string(),
            coins_per_utxo_size: Some(params.ada_per_utxo_byte.to_string()),
            coins_per_utxo_word: Some(params.ada_per_utxo_byte.to_string()),
            key_deposit: params.key_deposit.to_string(),
            pool_deposit: params.pool_deposit.to_string(),
            n_opt: params.desired_number_of_stake_pools as i32,
            rho: params
                .expansion_rate
                .map(|x| rational_to_f64::<3>(&x))
                .unwrap_or_default(),
            tau: params
                .treasury_growth_rate
                .map(|x| rational_to_f64::<3>(&x))
                .unwrap_or_default(),
            min_pool_cost: params.min_pool_cost.to_string(),
            protocol_major_ver: params.protocol_version.0 as i32,
            protocol_minor_ver: params.protocol_version.1 as i32,
            max_val_size: Some(params.max_value_size.to_string()),
            collateral_percent: Some(params.collateral_percentage as i32),
            max_collateral_inputs: Some(params.max_collateral_inputs as i32),
            price_mem: params
                .execution_costs
                .as_ref()
                .map(|x| rational_to_f64::<4>(&x.mem_price)),
            price_step: params
                .execution_costs
                .as_ref()
                .map(|x| rational_to_f64::<9>(&x.step_price)),
            max_tx_ex_mem: params.max_tx_ex_units.as_ref().map(|x| x.mem.to_string()),
            max_tx_ex_steps: params.max_tx_ex_units.as_ref().map(|x| x.steps.to_string()),
            max_block_ex_mem: params
                .max_block_ex_units
                .as_ref()
                .map(|x| x.mem.to_string()),
            max_block_ex_steps: params
                .max_block_ex_units
                .as_ref()
                .map(|x| x.steps.to_string()),
            min_fee_ref_script_cost_per_byte: params
                .minfee_refscript_cost_per_byte
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x)),
            drep_deposit: Some(params.drep_deposit.to_string()),
            drep_activity: Some(params.drep_inactivity_period.to_string()),
            cost_models_raw: Some(
                params
                    .cost_models_for_script_languages
                    .as_ref()
                    .map(|x| map_cost_models_raw(&x)),
            ),
            cost_models: params
                .cost_models_for_script_languages
                .as_ref()
                .map(|x| map_cost_models_named(&x)),
            pvt_motion_no_confidence: params
                .pool_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.motion_no_confidence)),
            pvt_committee_normal: params
                .pool_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.committee_normal)),
            pvt_committee_no_confidence: params
                .pool_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.committee_no_confidence)),
            pvt_hard_fork_initiation: params
                .pool_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.hard_fork_initiation)),
            dvt_motion_no_confidence: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.motion_no_confidence)),
            dvt_committee_normal: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.committee_normal)),
            dvt_committee_no_confidence: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.committee_no_confidence)),
            dvt_update_to_constitution: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.update_constitution)),
            dvt_hard_fork_initiation: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.hard_fork_initiation)),
            dvt_p_p_network_group: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.pp_network_group)),
            dvt_p_p_economic_group: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.pp_economic_group)),
            dvt_p_p_technical_group: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.pp_technical_group)),
            dvt_p_p_gov_group: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.pp_governance_group)),
            dvt_treasury_withdrawal: params
                .drep_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.treasury_withdrawal)),
            committee_min_size: Some(params.min_committee_size.to_string()),
            committee_max_term_length: Some(params.committee_term_limit.to_string()),
            gov_action_lifetime: Some(params.governance_action_validity_period.to_string()),
            gov_action_deposit: Some(params.governance_action_deposit.to_string()),
            pvtpp_security_group: params
                .pool_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.security_voting_threshold)),
            pvt_p_p_security_group: params
                .pool_voting_thresholds
                .as_ref()
                .map(|x| rational_to_f64::<3>(&x.security_voting_threshold)),
            // TODO: confirm mapping
            nonce: String::default(),
            extra_entropy: None,
            ..Default::default()
        };

        Ok(out)
    }
}
