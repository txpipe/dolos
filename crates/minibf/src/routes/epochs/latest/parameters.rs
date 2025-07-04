use axum::{Json, extract::State, http::StatusCode};
use itertools::Itertools as _;

use dolos_cardano::pparams;
use dolos_core::{Domain, Genesis, StateStore as _};
use pallas::ledger::{primitives::RationalNumber, validate::utils::MultiEraProtocolParameters};

use crate::{Facade, routes::epochs::cost_models::get_named_cost_model};

use super::{CostModels, CostModelsRaw, ProtocolParams};

fn rational_number_to_f64(val: RationalNumber) -> f64 {
    val.numerator as f64 / val.denominator as f64
}

fn map_pparams(
    epoch: u64,
    params: MultiEraProtocolParameters,
    genesis: &Genesis,
) -> ProtocolParams {
    match params {
        MultiEraProtocolParameters::Conway(params) => ProtocolParams {
            epoch,
            a0: rational_number_to_f64(genesis.shelley.protocol_params.a0.clone()),
            e_max: genesis.shelley.protocol_params.e_max,
            max_tx_size: params.max_transaction_size.into(),
            max_block_size: params.max_block_body_size.into(),
            max_block_header_size: params.max_block_header_size.into(),
            min_fee_a: params.minfee_a.into(),
            min_fee_b: params.minfee_b.into(),
            coins_per_utxo_size: Some(params.ada_per_utxo_byte.to_string()),
            coins_per_utxo_word: Some(params.ada_per_utxo_byte.to_string()),
            key_deposit: params.key_deposit.to_string(),
            pool_deposit: params.pool_deposit.to_string(),
            n_opt: params.desired_number_of_stake_pools.into(),
            rho: rational_number_to_f64(params.expansion_rate),
            tau: rational_number_to_f64(params.treasury_growth_rate),
            min_pool_cost: params.min_pool_cost.to_string(),
            protocol_major_ver: params.protocol_version.0,
            protocol_minor_ver: params.protocol_version.1,
            max_val_size: Some(params.max_value_size.to_string()),
            collateral_percent: Some(params.collateral_percentage.into()),
            max_collateral_inputs: Some(params.max_collateral_inputs.into()),
            price_mem: Some(rational_number_to_f64(params.execution_costs.mem_price)),
            price_step: Some(rational_number_to_f64(params.execution_costs.step_price)),
            max_tx_ex_mem: Some(params.max_tx_ex_units.mem.to_string()),
            max_tx_ex_steps: Some(params.max_tx_ex_units.steps.to_string()),
            max_block_ex_mem: Some(params.max_block_ex_units.mem.to_string()),
            max_block_ex_steps: Some(params.max_block_ex_units.steps.to_string()),
            min_fee_ref_script_cost_per_byte: Some(rational_number_to_f64(
                params.minfee_refscript_cost_per_byte,
            )),
            drep_deposit: Some(params.drep_deposit.to_string()),
            drep_activity: Some(params.drep_inactivity_period.to_string()),
            cost_models_raw: CostModelsRaw {
                plutus_v1: params.cost_models_for_script_languages.plutus_v1.clone(),
                plutus_v2: params.cost_models_for_script_languages.plutus_v2.clone(),
                plutus_v3: params.cost_models_for_script_languages.plutus_v3.clone(),
            }
            .into(),
            cost_models: CostModels {
                plutus_v1: params
                    .cost_models_for_script_languages
                    .plutus_v1
                    .as_ref()
                    .map(|v1| get_named_cost_model(1, v1)),
                plutus_v2: params
                    .cost_models_for_script_languages
                    .plutus_v2
                    .as_ref()
                    .map(|v2| get_named_cost_model(2, v2)),
                plutus_v3: params
                    .cost_models_for_script_languages
                    .plutus_v3
                    .as_ref()
                    .map(|v3| get_named_cost_model(3, v3)),
            }
            .into(),
            ..Default::default()
        },
        _ => todo!(),
    }
}

pub async fn route<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<ProtocolParams>, StatusCode> {
    let tip = domain
        .state()
        .cursor()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let updates = domain
        .state()
        .get_pparams(tip.as_ref().map(|p| p.slot()).unwrap_or_default())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let updates: Vec<_> = updates
        .into_iter()
        .map(TryInto::try_into)
        .try_collect()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let summary =
        pparams::fold_with_hacks(domain.genesis(), &updates, tip.as_ref().unwrap().slot());
    let era = summary.era_for_slot(tip.as_ref().unwrap().slot());

    let pparams = map_pparams(era.start.epoch, era.pparams.clone(), domain.genesis());

    Ok(Json(pparams))
}
