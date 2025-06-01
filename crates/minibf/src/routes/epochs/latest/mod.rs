use serde::{Deserialize, Serialize};

pub mod parameters;

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ProtocolParams {
    pub epoch: u64,
    pub min_fee_a: u64,
    pub min_fee_b: u64,
    pub max_block_size: u64,
    pub max_tx_size: u64,
    pub max_block_header_size: u64,
    pub key_deposit: String,
    pub pool_deposit: String,
    pub e_max: u64,
    pub n_opt: u64,
    pub a0: f64,
    pub rho: f64,
    pub tau: f64,
    pub decentralisation_param: f64,
    pub extra_entropy: Option<String>,
    pub protocol_major_ver: u64,
    pub protocol_minor_ver: u64,
    pub min_utxo: String,
    pub min_pool_cost: String,
    pub nonce: String,
    pub cost_models: Option<CostModels>,
    pub cost_models_raw: Option<CostModelsRaw>,
    pub price_mem: Option<f64>,
    pub price_step: Option<f64>,
    pub max_tx_ex_mem: Option<String>,
    pub max_tx_ex_steps: Option<String>,
    pub max_block_ex_mem: Option<String>,
    pub max_block_ex_steps: Option<String>,
    pub max_val_size: Option<String>,
    pub collateral_percent: Option<u64>,
    pub max_collateral_inputs: Option<u64>,
    pub coins_per_utxo_size: Option<String>,
    pub coins_per_utxo_word: Option<String>,
    pub pvt_motion_no_confidence: Option<f64>,
    pub pvt_committee_normal: Option<f64>,
    pub pvt_committee_no_confidence: Option<f64>,
    pub pvt_hard_fork_initiation: Option<f64>,
    pub dvt_motion_no_confidence: Option<f64>,
    pub dvt_committee_normal: Option<f64>,
    pub dvt_committee_no_confidence: Option<f64>,
    pub dvt_update_to_constitution: Option<f64>,
    pub dvt_hard_fork_initiation: Option<f64>,
    pub dvt_p_p_network_group: Option<f64>,
    pub dvt_p_p_economic_group: Option<f64>,
    pub dvt_p_p_technical_group: Option<f64>,
    pub dvt_p_p_gov_group: Option<f64>,
    pub dvt_treasury_withdrawal: Option<f64>,
    pub committee_min_size: Option<String>,
    pub committee_max_term_length: Option<String>,
    pub gov_action_lifetime: Option<String>,
    pub gov_action_deposit: Option<String>,
    pub drep_deposit: Option<String>,
    pub drep_activity: Option<String>,
    pub pvtpp_security_group: Option<f64>,
    pub pvt_p_p_security_group: Option<f64>,
    pub min_fee_ref_script_cost_per_byte: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CostModelsRaw {
    pub plutus_v1: Option<Vec<i64>>,
    pub plutus_v2: Option<Vec<i64>>,
    pub plutus_v3: Option<Vec<i64>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct CostModels {
    pub plutus_v1: Option<serde_json::Value>,
    pub plutus_v2: Option<serde_json::Value>,
    pub plutus_v3: Option<serde_json::Value>,
}
