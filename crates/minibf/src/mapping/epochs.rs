use crate::mapping::{
    cost_models::{map_cost_models_named, map_cost_models_raw},
    protocol_params::protocol_params_model,
    rational_to_f64, IntoModel, Rounded,
};
use blockfrost_openapi::models::{
    epoch_content::EpochContent, epoch_param_content::EpochParamContent,
};
use dolos_cardano::{model::EpochState, PParamsSet};
use dolos_core::{cbor, Genesis};
use pallas::ledger::primitives::Epoch;

pub struct ParametersModelBuilder<'a> {
    pub epoch: Epoch,
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

        let out = protocol_params_model!(
            params,
            Rounded,
            EpochParamContent {
                epoch: epoch as i32,
                a0: rational_to_f64::<3>(&genesis.shelley.protocol_params.a0),
                e_max: genesis.shelley.protocol_params.e_max as i32,
                max_tx_size: params.max_transaction_size_or_default() as i32,
                max_block_size: params.max_block_body_size_or_default() as i32,
                max_block_header_size: params.max_block_header_size_or_default() as i32,
                min_fee_a: params.min_fee_a_or_default() as i32,
                min_fee_b: params.min_fee_b_or_default() as i32,
                min_utxo: params
                    .ada_per_utxo_byte()
                    .unwrap_or(genesis.shelley.protocol_params.min_utxo_value)
                    .to_string(),
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
                cost_models_raw: map_cost_models_raw(&params.cost_models_for_script_languages()),
                cost_models: map_cost_models_named(&params.cost_models_for_script_languages()),
                nonce: nonce.unwrap_or_default(),
                decentralisation_param: rational_to_f64::<3>(
                    &params.decentralization_constant_or_default(),
                ),
            }
        );

        Ok(out)
    }
}

pub struct EpochContentModelBuilder {
    pub state: EpochState,
    pub start_time: u64,
    pub end_time: u64,
    pub first_block_time: u64,
    pub last_block_time: u64,
    pub tx_count: u64,
    pub output: cbor::U128,
    pub active_stake: Option<u64>,
}

impl IntoModel<EpochContent> for EpochContentModelBuilder {
    type SortKey = Epoch;

    fn sort_key(&self) -> Option<Self::SortKey> {
        Some(self.state.number)
    }

    fn into_model(self) -> Result<EpochContent, axum::http::StatusCode> {
        let Self {
            state,
            start_time,
            end_time,
            first_block_time,
            last_block_time,
            tx_count,
            output,
            active_stake,
        } = self;

        let rolling = state.rolling.live().cloned().unwrap_or_default();

        let out = EpochContent {
            epoch: state.number as i32,
            start_time: start_time as i32,
            end_time: end_time as i32,
            first_block_time: first_block_time as i32,
            last_block_time: last_block_time as i32,
            block_count: rolling.blocks_minted as i32,
            tx_count: tx_count as i32,
            output: output.to_string(),
            fees: rolling.gathered_fees.to_string(),
            active_stake: active_stake.map(|x| x.to_string()),
        };

        Ok(out)
    }
}
