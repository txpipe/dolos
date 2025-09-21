use std::collections::HashMap;

use crate::{
    mapping::{rational_to_f64, IntoModel},
    routes::epochs::cost_models::get_named_cost_model,
};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;
use dolos_cardano::PParamsSet;
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
    pub params: PParamsSet,
    pub genesis: &'a Genesis,
    pub nonce: Option<String>,
}

impl<'a> IntoModel<EpochParamContent> for ParametersModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<EpochParamContent, axum::http::StatusCode> {
        let Self {
            genesis,
            epoch,
            params,
            nonce,
        } = self;

        let out = EpochParamContent {
            epoch: epoch as i32,
            a0: rational_to_f64::<3>(&genesis.shelley.protocol_params.a0),
            e_max: genesis.shelley.protocol_params.e_max as i32,
            max_tx_size: params.max_transaction_size_or_default() as i32,
            max_block_size: params.max_block_body_size_or_default() as i32,
            max_block_header_size: params.max_block_header_size_or_default() as i32,
            min_fee_a: params.min_fee_a_or_default() as i32,
            min_fee_b: params.min_fee_b_or_default() as i32,
            min_utxo: params.ada_per_utxo_byte_or_default().to_string(),
            coins_per_utxo_size: params.ada_per_utxo_byte().map(|x| x.to_string()),
            coins_per_utxo_word: params.ada_per_utxo_byte().map(|x| x.to_string()),
            key_deposit: params.key_deposit_or_default().to_string(),
            pool_deposit: params.pool_deposit_or_default().to_string(),
            n_opt: params.desired_number_of_stake_pools_or_default() as i32,
            rho: params
                .rho()
                .map(|x| rational_to_f64::<3>(&x))
                .unwrap_or_default(),
            tau: params
                .tau()
                .map(|x| rational_to_f64::<3>(&x))
                .unwrap_or_default(),
            min_pool_cost: params.min_pool_cost_or_default().to_string(),
            protocol_major_ver: params.protocol_major().unwrap_or_default() as i32,
            protocol_minor_ver: params.protocol_version_or_default().1 as i32,
            max_val_size: params.max_value_size().map(|x| x.to_string()),
            collateral_percent: params.collateral_percentage().map(|x| x as i32),
            max_collateral_inputs: params.max_collateral_inputs().map(|x| x as i32),
            price_mem: params
                .execution_costs()
                .map(|x| rational_to_f64::<4>(&x.mem_price)),
            price_step: params
                .execution_costs()
                .map(|x| rational_to_f64::<9>(&x.step_price)),
            max_tx_ex_mem: params.max_tx_ex_units().map(|x| x.mem.to_string()),
            max_tx_ex_steps: params.max_tx_ex_units().map(|x| x.steps.to_string()),
            max_block_ex_mem: params.max_block_ex_units().map(|x| x.mem.to_string()),
            max_block_ex_steps: params.max_block_ex_units().map(|x| x.steps.to_string()),
            min_fee_ref_script_cost_per_byte: params
                .min_fee_ref_script_cost_per_byte()
                .map(|x| rational_to_f64::<3>(&x)),
            drep_deposit: params.drep_deposit().map(|x| x.to_string()),
            drep_activity: params.drep_inactivity_period().map(|x| x.to_string()),
            cost_models_raw: params
                .cost_models_for_script_languages()
                .map(|x| Some(map_cost_models_raw(&x))),
            cost_models: params
                .cost_models_for_script_languages()
                .map(|x| map_cost_models_named(&x)),
            pvt_motion_no_confidence: params
                .pool_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.motion_no_confidence)),
            pvt_committee_normal: params
                .pool_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.committee_normal)),
            pvt_committee_no_confidence: params
                .pool_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.committee_no_confidence)),
            pvt_hard_fork_initiation: params
                .pool_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.hard_fork_initiation)),
            dvt_motion_no_confidence: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.motion_no_confidence)),
            dvt_committee_normal: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.committee_normal)),
            dvt_committee_no_confidence: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.committee_no_confidence)),
            dvt_update_to_constitution: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.update_constitution)),
            dvt_hard_fork_initiation: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.hard_fork_initiation)),
            dvt_p_p_network_group: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.pp_network_group)),
            dvt_p_p_economic_group: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.pp_economic_group)),
            dvt_p_p_technical_group: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.pp_technical_group)),
            dvt_p_p_gov_group: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.pp_governance_group)),
            dvt_treasury_withdrawal: params
                .drep_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.treasury_withdrawal)),
            committee_min_size: params.min_committee_size().map(|x| x.to_string()),
            committee_max_term_length: params.committee_term_limit().map(|x| x.to_string()),
            gov_action_lifetime: params
                .governance_action_validity_period()
                .map(|x| x.to_string()),
            gov_action_deposit: params.governance_action_deposit().map(|x| x.to_string()),
            pvtpp_security_group: params
                .pool_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.security_voting_threshold)),
            pvt_p_p_security_group: params
                .pool_voting_thresholds()
                .map(|x| rational_to_f64::<3>(&x.security_voting_threshold)),
            nonce: nonce.unwrap_or_default(),
            extra_entropy: None,
            ..Default::default()
        };

        Ok(out)
    }
}
