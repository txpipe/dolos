use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};

use crate::{ledger::pparams, serve::minibf::SharedState};
use itertools::Itertools as _;

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
    #[deprecated]
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
    #[deprecated]
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
    #[deprecated]
    pub pvtpp_security_group: Option<f64>,
    pub pvt_p_p_security_group: Option<f64>,
    pub min_fee_ref_script_cost_per_byte: Option<f64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CostModelsRaw {
    pub plutus_v1: Option<Vec<i64>>,
    pub plutus_v2: Option<Vec<i64>>,
    pub plutus_v3: Option<Vec<i64>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CostModels {
    pub plutus_v1: Option<CostParametersV1>,
    pub plutus_v2: Option<CostParametersV2>,
    pub plutus_v3: Option<CostParametersV3>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CostParametersV1 {
    #[serde(rename = "addInteger-cpu-arguments-intercept")]
    pub add_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "addInteger-cpu-arguments-slope")]
    pub add_integer_cpu_arguments_slope: i64,
    #[serde(rename = "addInteger-memory-arguments-intercept")]
    pub add_integer_memory_arguments_intercept: i64,
    #[serde(rename = "addInteger-memory-arguments-slope")]
    pub add_integer_memory_arguments_slope: i64,
    #[serde(rename = "appendByteString-cpu-arguments-intercept")]
    pub append_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "appendByteString-cpu-arguments-slope")]
    pub append_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "appendByteString-memory-arguments-intercept")]
    pub append_byte_string_memory_arguments_intercept: i64,
    #[serde(rename = "appendByteString-memory-arguments-slope")]
    pub append_byte_string_memory_arguments_slope: i64,
    #[serde(rename = "appendString-cpu-arguments-intercept")]
    pub append_string_cpu_arguments_intercept: i64,
    #[serde(rename = "appendString-cpu-arguments-slope")]
    pub append_string_cpu_arguments_slope: i64,
    #[serde(rename = "appendString-memory-arguments-intercept")]
    pub append_string_memory_arguments_intercept: i64,
    #[serde(rename = "appendString-memory-arguments-slope")]
    pub append_string_memory_arguments_slope: i64,
    #[serde(rename = "bData-cpu-arguments")]
    pub b_data_cpu_arguments: i64,
    #[serde(rename = "bData-memory-arguments")]
    pub b_data_memory_arguments: i64,
    #[serde(rename = "blake2b_256-cpu-arguments-intercept")]
    pub blake2b_256_cpu_arguments_intercept: i64,
    #[serde(rename = "blake2b_256-cpu-arguments-slope")]
    pub blake2b_256_cpu_arguments_slope: i64,
    #[serde(rename = "blake2b_256-memory-arguments")]
    pub blake2b_256_memory_arguments: i64,
    #[serde(rename = "cekApplyCost-exBudgetCPU")]
    pub cek_apply_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekApplyCost-exBudgetMemory")]
    pub cek_apply_cost_ex_budget_memory: i64,
    #[serde(rename = "cekBuiltinCost-exBudgetCPU")]
    pub cek_builtin_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekBuiltinCost-exBudgetMemory")]
    pub cek_builtin_cost_ex_budget_memory: i64,
    #[serde(rename = "cekConstCost-exBudgetCPU")]
    pub cek_const_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekConstCost-exBudgetMemory")]
    pub cek_const_cost_ex_budget_memory: i64,
    #[serde(rename = "cekDelayCost-exBudgetCPU")]
    pub cek_delay_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekDelayCost-exBudgetMemory")]
    pub cek_delay_cost_ex_budget_memory: i64,
    #[serde(rename = "cekForceCost-exBudgetCPU")]
    pub cek_force_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekForceCost-exBudgetMemory")]
    pub cek_force_cost_ex_budget_memory: i64,
    #[serde(rename = "cekLamCost-exBudgetCPU")]
    pub cek_lam_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekLamCost-exBudgetMemory")]
    pub cek_lam_cost_ex_budget_memory: i64,
    #[serde(rename = "cekStartupCost-exBudgetCPU")]
    pub cek_startup_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekStartupCost-exBudgetMemory")]
    pub cek_startup_cost_ex_budget_memory: i64,
    #[serde(rename = "cekVarCost-exBudgetCPU")]
    pub cek_var_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekVarCost-exBudgetMemory")]
    pub cek_var_cost_ex_budget_memory: i64,
    #[serde(rename = "chooseData-cpu-arguments")]
    pub choose_data_cpu_arguments: i64,
    #[serde(rename = "chooseData-memory-arguments")]
    pub choose_data_memory_arguments: i64,
    #[serde(rename = "chooseList-cpu-arguments")]
    pub choose_list_cpu_arguments: i64,
    #[serde(rename = "chooseList-memory-arguments")]
    pub choose_list_memory_arguments: i64,
    #[serde(rename = "chooseUnit-cpu-arguments")]
    pub choose_unit_cpu_arguments: i64,
    #[serde(rename = "chooseUnit-memory-arguments")]
    pub choose_unit_memory_arguments: i64,
    #[serde(rename = "consByteString-cpu-arguments-intercept")]
    pub cons_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "consByteString-cpu-arguments-slope")]
    pub cons_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "consByteString-memory-arguments-intercept")]
    pub cons_byte_string_memory_arguments_intercept: i64,
    #[serde(rename = "consByteString-memory-arguments-slope")]
    pub cons_byte_string_memory_arguments_slope: i64,
    #[serde(rename = "constrData-cpu-arguments")]
    pub constr_data_cpu_arguments: i64,
    #[serde(rename = "constrData-memory-arguments")]
    pub constr_data_memory_arguments: i64,
    #[serde(rename = "decodeUtf8-cpu-arguments-intercept")]
    pub decode_utf8_cpu_arguments_intercept: i64,
    #[serde(rename = "decodeUtf8-cpu-arguments-slope")]
    pub decode_utf8_cpu_arguments_slope: i64,
    #[serde(rename = "decodeUtf8-memory-arguments-intercept")]
    pub decode_utf8_memory_arguments_intercept: i64,
    #[serde(rename = "decodeUtf8-memory-arguments-slope")]
    pub decode_utf8_memory_arguments_slope: i64,
    #[serde(rename = "divideInteger-cpu-arguments-constant")]
    pub divide_integer_cpu_arguments_constant: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-intercept")]
    pub divide_integer_cpu_arguments_model_arguments_intercept: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-slope")]
    pub divide_integer_cpu_arguments_model_arguments_slope: i64,
    #[serde(rename = "divideInteger-memory-arguments-intercept")]
    pub divide_integer_memory_arguments_intercept: i64,
    #[serde(rename = "divideInteger-memory-arguments-minimum")]
    pub divide_integer_memory_arguments_minimum: i64,
    #[serde(rename = "divideInteger-memory-arguments-slope")]
    pub divide_integer_memory_arguments_slope: i64,
    #[serde(rename = "encodeUtf8-cpu-arguments-intercept")]
    pub encode_utf8_cpu_arguments_intercept: i64,
    #[serde(rename = "encodeUtf8-cpu-arguments-slope")]
    pub encode_utf8_cpu_arguments_slope: i64,
    #[serde(rename = "encodeUtf8-memory-arguments-intercept")]
    pub encode_utf8_memory_arguments_intercept: i64,
    #[serde(rename = "encodeUtf8-memory-arguments-slope")]
    pub encode_utf8_memory_arguments_slope: i64,
    #[serde(rename = "equalsByteString-cpu-arguments-constant")]
    pub equals_byte_string_cpu_arguments_constant: i64,
    #[serde(rename = "equalsByteString-cpu-arguments-intercept")]
    pub equals_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsByteString-cpu-arguments-slope")]
    pub equals_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "equalsByteString-memory-arguments")]
    pub equals_byte_string_memory_arguments: i64,
    #[serde(rename = "equalsData-cpu-arguments-intercept")]
    pub equals_data_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsData-cpu-arguments-slope")]
    pub equals_data_cpu_arguments_slope: i64,
    #[serde(rename = "equalsData-memory-arguments")]
    pub equals_data_memory_arguments: i64,
    #[serde(rename = "equalsInteger-cpu-arguments-intercept")]
    pub equals_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsInteger-cpu-arguments-slope")]
    pub equals_integer_cpu_arguments_slope: i64,
    #[serde(rename = "equalsInteger-memory-arguments")]
    pub equals_integer_memory_arguments: i64,
    #[serde(rename = "equalsString-cpu-arguments-constant")]
    pub equals_string_cpu_arguments_constant: i64,
    #[serde(rename = "equalsString-cpu-arguments-intercept")]
    pub equals_string_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsString-cpu-arguments-slope")]
    pub equals_string_cpu_arguments_slope: i64,
    #[serde(rename = "equalsString-memory-arguments")]
    pub equals_string_memory_arguments: i64,
    #[serde(rename = "fstPair-cpu-arguments")]
    pub fst_pair_cpu_arguments: i64,
    #[serde(rename = "fstPair-memory-arguments")]
    pub fst_pair_memory_arguments: i64,
    #[serde(rename = "headList-cpu-arguments")]
    pub head_list_cpu_arguments: i64,
    #[serde(rename = "headList-memory-arguments")]
    pub head_list_memory_arguments: i64,
    #[serde(rename = "iData-cpu-arguments")]
    pub i_data_cpu_arguments: i64,
    #[serde(rename = "iData-memory-arguments")]
    pub i_data_memory_arguments: i64,
    #[serde(rename = "ifThenElse-cpu-arguments")]
    pub if_then_else_cpu_arguments: i64,
    #[serde(rename = "ifThenElse-memory-arguments")]
    pub if_then_else_memory_arguments: i64,
    #[serde(rename = "indexByteString-cpu-arguments")]
    pub index_byte_string_cpu_arguments: i64,
    #[serde(rename = "indexByteString-memory-arguments")]
    pub index_byte_string_memory_arguments: i64,
    #[serde(rename = "lengthOfByteString-cpu-arguments")]
    pub length_of_byte_string_cpu_arguments: i64,
    #[serde(rename = "lengthOfByteString-memory-arguments")]
    pub length_of_byte_string_memory_arguments: i64,
    #[serde(rename = "lessThanByteString-cpu-arguments-intercept")]
    pub less_than_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanByteString-cpu-arguments-slope")]
    pub less_than_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanByteString-memory-arguments")]
    pub less_than_byte_string_memory_arguments: i64,
    #[serde(rename = "lessThanEqualsByteString-cpu-arguments-intercept")]
    pub less_than_equals_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanEqualsByteString-cpu-arguments-slope")]
    pub less_than_equals_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanEqualsByteString-memory-arguments")]
    pub less_than_equals_byte_string_memory_arguments: i64,
    #[serde(rename = "lessThanEqualsInteger-cpu-arguments-intercept")]
    pub less_than_equals_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanEqualsInteger-cpu-arguments-slope")]
    pub less_than_equals_integer_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanEqualsInteger-memory-arguments")]
    pub less_than_equals_integer_memory_arguments: i64,
    #[serde(rename = "lessThanInteger-cpu-arguments-intercept")]
    pub less_than_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanInteger-cpu-arguments-slope")]
    pub less_than_integer_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanInteger-memory-arguments")]
    pub less_than_integer_memory_arguments: i64,
    #[serde(rename = "listData-cpu-arguments")]
    pub list_data_cpu_arguments: i64,
    #[serde(rename = "listData-memory-arguments")]
    pub list_data_memory_arguments: i64,
    #[serde(rename = "mapData-cpu-arguments")]
    pub map_data_cpu_arguments: i64,
    #[serde(rename = "mapData-memory-arguments")]
    pub map_data_memory_arguments: i64,
    #[serde(rename = "mkCons-cpu-arguments")]
    pub mk_cons_cpu_arguments: i64,
    #[serde(rename = "mkCons-memory-arguments")]
    pub mk_cons_memory_arguments: i64,
    #[serde(rename = "mkNilData-cpu-arguments")]
    pub mk_nil_data_cpu_arguments: i64,
    #[serde(rename = "mkNilData-memory-arguments")]
    pub mk_nil_data_memory_arguments: i64,
    #[serde(rename = "mkNilPairData-cpu-arguments")]
    pub mk_nil_pair_data_cpu_arguments: i64,
    #[serde(rename = "mkNilPairData-memory-arguments")]
    pub mk_nil_pair_data_memory_arguments: i64,
    #[serde(rename = "mkPairData-cpu-arguments")]
    pub mk_pair_data_cpu_arguments: i64,
    #[serde(rename = "mkPairData-memory-arguments")]
    pub mk_pair_data_memory_arguments: i64,
    #[serde(rename = "modInteger-cpu-arguments-constant")]
    pub mod_integer_cpu_arguments_constant: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-intercept")]
    pub mod_integer_cpu_arguments_model_arguments_intercept: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-slope")]
    pub mod_integer_cpu_arguments_model_arguments_slope: i64,
    #[serde(rename = "modInteger-memory-arguments-intercept")]
    pub mod_integer_memory_arguments_intercept: i64,
    #[serde(rename = "modInteger-memory-arguments-minimum")]
    pub mod_integer_memory_arguments_minimum: i64,
    #[serde(rename = "modInteger-memory-arguments-slope")]
    pub mod_integer_memory_arguments_slope: i64,
    #[serde(rename = "multiplyInteger-cpu-arguments-intercept")]
    pub multiply_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "multiplyInteger-cpu-arguments-slope")]
    pub multiply_integer_cpu_arguments_slope: i64,
    #[serde(rename = "multiplyInteger-memory-arguments-intercept")]
    pub multiply_integer_memory_arguments_intercept: i64,
    #[serde(rename = "multiplyInteger-memory-arguments-slope")]
    pub multiply_integer_memory_arguments_slope: i64,
    #[serde(rename = "nullList-cpu-arguments")]
    pub null_list_cpu_arguments: i64,
    #[serde(rename = "nullList-memory-arguments")]
    pub null_list_memory_arguments: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-constant")]
    pub quotient_integer_cpu_arguments_constant: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-intercept")]
    pub quotient_integer_cpu_arguments_model_arguments_intercept: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-slope")]
    pub quotient_integer_cpu_arguments_model_arguments_slope: i64,
    #[serde(rename = "quotientInteger-memory-arguments-intercept")]
    pub quotient_integer_memory_arguments_intercept: i64,
    #[serde(rename = "quotientInteger-memory-arguments-minimum")]
    pub quotient_integer_memory_arguments_minimum: i64,
    #[serde(rename = "quotientInteger-memory-arguments-slope")]
    pub quotient_integer_memory_arguments_slope: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-constant")]
    pub remainder_integer_cpu_arguments_constant: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-intercept")]
    pub remainder_integer_cpu_arguments_model_arguments_intercept: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-slope")]
    pub remainder_integer_cpu_arguments_model_arguments_slope: i64,
    #[serde(rename = "remainderInteger-memory-arguments-intercept")]
    pub remainder_integer_memory_arguments_intercept: i64,
    #[serde(rename = "remainderInteger-memory-arguments-minimum")]
    pub remainder_integer_memory_arguments_minimum: i64,
    #[serde(rename = "remainderInteger-memory-arguments-slope")]
    pub remainder_integer_memory_arguments_slope: i64,
    #[serde(rename = "sha2_256-cpu-arguments-intercept")]
    pub sha2_256_cpu_arguments_intercept: i64,
    #[serde(rename = "sha2_256-cpu-arguments-slope")]
    pub sha2_256_cpu_arguments_slope: i64,
    #[serde(rename = "sha2_256-memory-arguments")]
    pub sha2_256_memory_arguments: i64,
    #[serde(rename = "sha3_256-cpu-arguments-intercept")]
    pub sha3_256_cpu_arguments_intercept: i64,
    #[serde(rename = "sha3_256-cpu-arguments-slope")]
    pub sha3_256_cpu_arguments_slope: i64,
    #[serde(rename = "sha3_256-memory-arguments")]
    pub sha3_256_memory_arguments: i64,
    #[serde(rename = "sliceByteString-cpu-arguments-intercept")]
    pub slice_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "sliceByteString-cpu-arguments-slope")]
    pub slice_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "sliceByteString-memory-arguments-intercept")]
    pub slice_byte_string_memory_arguments_intercept: i64,
    #[serde(rename = "sliceByteString-memory-arguments-slope")]
    pub slice_byte_string_memory_arguments_slope: i64,
    #[serde(rename = "sndPair-cpu-arguments")]
    pub snd_pair_cpu_arguments: i64,
    #[serde(rename = "sndPair-memory-arguments")]
    pub snd_pair_memory_arguments: i64,
    #[serde(rename = "subtractInteger-cpu-arguments-intercept")]
    pub subtract_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "subtractInteger-cpu-arguments-slope")]
    pub subtract_integer_cpu_arguments_slope: i64,
    #[serde(rename = "subtractInteger-memory-arguments-intercept")]
    pub subtract_integer_memory_arguments_intercept: i64,
    #[serde(rename = "subtractInteger-memory-arguments-slope")]
    pub subtract_integer_memory_arguments_slope: i64,
    #[serde(rename = "tailList-cpu-arguments")]
    pub tail_list_cpu_arguments: i64,
    #[serde(rename = "tailList-memory-arguments")]
    pub tail_list_memory_arguments: i64,
    #[serde(rename = "trace-cpu-arguments")]
    pub trace_cpu_arguments: i64,
    #[serde(rename = "trace-memory-arguments")]
    pub trace_memory_arguments: i64,
    #[serde(rename = "unBData-cpu-arguments")]
    pub un_b_data_cpu_arguments: i64,
    #[serde(rename = "unBData-memory-arguments")]
    pub un_b_data_memory_arguments: i64,
    #[serde(rename = "unConstrData-cpu-arguments")]
    pub un_constr_data_cpu_arguments: i64,
    #[serde(rename = "unConstrData-memory-arguments")]
    pub un_constr_data_memory_arguments: i64,
    #[serde(rename = "unIData-cpu-arguments")]
    pub un_i_data_cpu_arguments: i64,
    #[serde(rename = "unIData-memory-arguments")]
    pub un_i_data_memory_arguments: i64,
    #[serde(rename = "unListData-cpu-arguments")]
    pub un_list_data_cpu_arguments: i64,
    #[serde(rename = "unListData-memory-arguments")]
    pub un_list_data_memory_arguments: i64,
    #[serde(rename = "unMapData-cpu-arguments")]
    pub un_map_data_cpu_arguments: i64,
    #[serde(rename = "unMapData-memory-arguments")]
    pub un_map_data_memory_arguments: i64,
    #[serde(rename = "verifyEd25519Signature-cpu-arguments-intercept")]
    pub verify_ed25519_signature_cpu_arguments_intercept: i64,
    #[serde(rename = "verifyEd25519Signature-cpu-arguments-slope")]
    pub verify_ed25519_signature_cpu_arguments_slope: i64,
    #[serde(rename = "verifyEd25519Signature-memory-arguments")]
    pub verify_ed25519_signature_memory_arguments: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CostParametersV2 {
    #[serde(rename = "addInteger-cpu-arguments-intercept")]
    pub add_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "addInteger-cpu-arguments-slope")]
    pub add_integer_cpu_arguments_slope: i64,
    #[serde(rename = "addInteger-memory-arguments-intercept")]
    pub add_integer_memory_arguments_intercept: i64,
    #[serde(rename = "addInteger-memory-arguments-slope")]
    pub add_integer_memory_arguments_slope: i64,
    #[serde(rename = "appendByteString-cpu-arguments-intercept")]
    pub append_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "appendByteString-cpu-arguments-slope")]
    pub append_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "appendByteString-memory-arguments-intercept")]
    pub append_byte_string_memory_arguments_intercept: i64,
    #[serde(rename = "appendByteString-memory-arguments-slope")]
    pub append_byte_string_memory_arguments_slope: i64,
    #[serde(rename = "appendString-cpu-arguments-intercept")]
    pub append_string_cpu_arguments_intercept: i64,
    #[serde(rename = "appendString-cpu-arguments-slope")]
    pub append_string_cpu_arguments_slope: i64,
    #[serde(rename = "appendString-memory-arguments-intercept")]
    pub append_string_memory_arguments_intercept: i64,
    #[serde(rename = "appendString-memory-arguments-slope")]
    pub append_string_memory_arguments_slope: i64,
    #[serde(rename = "bData-cpu-arguments")]
    pub b_data_cpu_arguments: i64,
    #[serde(rename = "bData-memory-arguments")]
    pub b_data_memory_arguments: i64,
    #[serde(rename = "blake2b_256-cpu-arguments-intercept")]
    pub blake2b_256_cpu_arguments_intercept: i64,
    #[serde(rename = "blake2b_256-cpu-arguments-slope")]
    pub blake2b_256_cpu_arguments_slope: i64,
    #[serde(rename = "blake2b_256-memory-arguments")]
    pub blake2b_256_memory_arguments: i64,
    #[serde(rename = "cekApplyCost-exBudgetCPU")]
    pub cek_apply_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekApplyCost-exBudgetMemory")]
    pub cek_apply_cost_ex_budget_memory: i64,
    #[serde(rename = "cekBuiltinCost-exBudgetCPU")]
    pub cek_builtin_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekBuiltinCost-exBudgetMemory")]
    pub cek_builtin_cost_ex_budget_memory: i64,
    #[serde(rename = "cekConstCost-exBudgetCPU")]
    pub cek_const_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekConstCost-exBudgetMemory")]
    pub cek_const_cost_ex_budget_memory: i64,
    #[serde(rename = "cekDelayCost-exBudgetCPU")]
    pub cek_delay_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekDelayCost-exBudgetMemory")]
    pub cek_delay_cost_ex_budget_memory: i64,
    #[serde(rename = "cekForceCost-exBudgetCPU")]
    pub cek_force_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekForceCost-exBudgetMemory")]
    pub cek_force_cost_ex_budget_memory: i64,
    #[serde(rename = "cekLamCost-exBudgetCPU")]
    pub cek_lam_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekLamCost-exBudgetMemory")]
    pub cek_lam_cost_ex_budget_memory: i64,
    #[serde(rename = "cekStartupCost-exBudgetCPU")]
    pub cek_startup_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekStartupCost-exBudgetMemory")]
    pub cek_startup_cost_ex_budget_memory: i64,
    #[serde(rename = "cekVarCost-exBudgetCPU")]
    pub cek_var_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekVarCost-exBudgetMemory")]
    pub cek_var_cost_ex_budget_memory: i64,
    #[serde(rename = "chooseData-cpu-arguments")]
    pub choose_data_cpu_arguments: i64,
    #[serde(rename = "chooseData-memory-arguments")]
    pub choose_data_memory_arguments: i64,
    #[serde(rename = "chooseList-cpu-arguments")]
    pub choose_list_cpu_arguments: i64,
    #[serde(rename = "chooseList-memory-arguments")]
    pub choose_list_memory_arguments: i64,
    #[serde(rename = "chooseUnit-cpu-arguments")]
    pub choose_unit_cpu_arguments: i64,
    #[serde(rename = "chooseUnit-memory-arguments")]
    pub choose_unit_memory_arguments: i64,
    #[serde(rename = "consByteString-cpu-arguments-intercept")]
    pub cons_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "consByteString-cpu-arguments-slope")]
    pub cons_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "consByteString-memory-arguments-intercept")]
    pub cons_byte_string_memory_arguments_intercept: i64,
    #[serde(rename = "consByteString-memory-arguments-slope")]
    pub cons_byte_string_memory_arguments_slope: i64,
    #[serde(rename = "constrData-cpu-arguments")]
    pub constr_data_cpu_arguments: i64,
    #[serde(rename = "constrData-memory-arguments")]
    pub constr_data_memory_arguments: i64,
    #[serde(rename = "decodeUtf8-cpu-arguments-intercept")]
    pub decode_utf8_cpu_arguments_intercept: i64,
    #[serde(rename = "decodeUtf8-cpu-arguments-slope")]
    pub decode_utf8_cpu_arguments_slope: i64,
    #[serde(rename = "decodeUtf8-memory-arguments-intercept")]
    pub decode_utf8_memory_arguments_intercept: i64,
    #[serde(rename = "decodeUtf8-memory-arguments-slope")]
    pub decode_utf8_memory_arguments_slope: i64,
    #[serde(rename = "divideInteger-cpu-arguments-constant")]
    pub divide_integer_cpu_arguments_constant: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-intercept")]
    pub divide_integer_cpu_arguments_model_arguments_intercept: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-slope")]
    pub divide_integer_cpu_arguments_model_arguments_slope: i64,
    #[serde(rename = "divideInteger-memory-arguments-intercept")]
    pub divide_integer_memory_arguments_intercept: i64,
    #[serde(rename = "divideInteger-memory-arguments-minimum")]
    pub divide_integer_memory_arguments_minimum: i64,
    #[serde(rename = "divideInteger-memory-arguments-slope")]
    pub divide_integer_memory_arguments_slope: i64,
    #[serde(rename = "encodeUtf8-cpu-arguments-intercept")]
    pub encode_utf8_cpu_arguments_intercept: i64,
    #[serde(rename = "encodeUtf8-cpu-arguments-slope")]
    pub encode_utf8_cpu_arguments_slope: i64,
    #[serde(rename = "encodeUtf8-memory-arguments-intercept")]
    pub encode_utf8_memory_arguments_intercept: i64,
    #[serde(rename = "encodeUtf8-memory-arguments-slope")]
    pub encode_utf8_memory_arguments_slope: i64,
    #[serde(rename = "equalsByteString-cpu-arguments-constant")]
    pub equals_byte_string_cpu_arguments_constant: i64,
    #[serde(rename = "equalsByteString-cpu-arguments-intercept")]
    pub equals_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsByteString-cpu-arguments-slope")]
    pub equals_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "equalsByteString-memory-arguments")]
    pub equals_byte_string_memory_arguments: i64,
    #[serde(rename = "equalsData-cpu-arguments-intercept")]
    pub equals_data_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsData-cpu-arguments-slope")]
    pub equals_data_cpu_arguments_slope: i64,
    #[serde(rename = "equalsData-memory-arguments")]
    pub equals_data_memory_arguments: i64,
    #[serde(rename = "equalsInteger-cpu-arguments-intercept")]
    pub equals_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsInteger-cpu-arguments-slope")]
    pub equals_integer_cpu_arguments_slope: i64,
    #[serde(rename = "equalsInteger-memory-arguments")]
    pub equals_integer_memory_arguments: i64,
    #[serde(rename = "equalsString-cpu-arguments-constant")]
    pub equals_string_cpu_arguments_constant: i64,
    #[serde(rename = "equalsString-cpu-arguments-intercept")]
    pub equals_string_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsString-cpu-arguments-slope")]
    pub equals_string_cpu_arguments_slope: i64,
    #[serde(rename = "equalsString-memory-arguments")]
    pub equals_string_memory_arguments: i64,
    #[serde(rename = "fstPair-cpu-arguments")]
    pub fst_pair_cpu_arguments: i64,
    #[serde(rename = "fstPair-memory-arguments")]
    pub fst_pair_memory_arguments: i64,
    #[serde(rename = "headList-cpu-arguments")]
    pub head_list_cpu_arguments: i64,
    #[serde(rename = "headList-memory-arguments")]
    pub head_list_memory_arguments: i64,
    #[serde(rename = "iData-cpu-arguments")]
    pub i_data_cpu_arguments: i64,
    #[serde(rename = "iData-memory-arguments")]
    pub i_data_memory_arguments: i64,
    #[serde(rename = "ifThenElse-cpu-arguments")]
    pub if_then_else_cpu_arguments: i64,
    #[serde(rename = "ifThenElse-memory-arguments")]
    pub if_then_else_memory_arguments: i64,
    #[serde(rename = "indexByteString-cpu-arguments")]
    pub index_byte_string_cpu_arguments: i64,
    #[serde(rename = "indexByteString-memory-arguments")]
    pub index_byte_string_memory_arguments: i64,
    #[serde(rename = "lengthOfByteString-cpu-arguments")]
    pub length_of_byte_string_cpu_arguments: i64,
    #[serde(rename = "lengthOfByteString-memory-arguments")]
    pub length_of_byte_string_memory_arguments: i64,
    #[serde(rename = "lessThanByteString-cpu-arguments-intercept")]
    pub less_than_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanByteString-cpu-arguments-slope")]
    pub less_than_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanByteString-memory-arguments")]
    pub less_than_byte_string_memory_arguments: i64,
    #[serde(rename = "lessThanEqualsByteString-cpu-arguments-intercept")]
    pub less_than_equals_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanEqualsByteString-cpu-arguments-slope")]
    pub less_than_equals_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanEqualsByteString-memory-arguments")]
    pub less_than_equals_byte_string_memory_arguments: i64,
    #[serde(rename = "lessThanEqualsInteger-cpu-arguments-intercept")]
    pub less_than_equals_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanEqualsInteger-cpu-arguments-slope")]
    pub less_than_equals_integer_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanEqualsInteger-memory-arguments")]
    pub less_than_equals_integer_memory_arguments: i64,
    #[serde(rename = "lessThanInteger-cpu-arguments-intercept")]
    pub less_than_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanInteger-cpu-arguments-slope")]
    pub less_than_integer_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanInteger-memory-arguments")]
    pub less_than_integer_memory_arguments: i64,
    #[serde(rename = "listData-cpu-arguments")]
    pub list_data_cpu_arguments: i64,
    #[serde(rename = "listData-memory-arguments")]
    pub list_data_memory_arguments: i64,
    #[serde(rename = "mapData-cpu-arguments")]
    pub map_data_cpu_arguments: i64,
    #[serde(rename = "mapData-memory-arguments")]
    pub map_data_memory_arguments: i64,
    #[serde(rename = "mkCons-cpu-arguments")]
    pub mk_cons_cpu_arguments: i64,
    #[serde(rename = "mkCons-memory-arguments")]
    pub mk_cons_memory_arguments: i64,
    #[serde(rename = "mkNilData-cpu-arguments")]
    pub mk_nil_data_cpu_arguments: i64,
    #[serde(rename = "mkNilData-memory-arguments")]
    pub mk_nil_data_memory_arguments: i64,
    #[serde(rename = "mkNilPairData-cpu-arguments")]
    pub mk_nil_pair_data_cpu_arguments: i64,
    #[serde(rename = "mkNilPairData-memory-arguments")]
    pub mk_nil_pair_data_memory_arguments: i64,
    #[serde(rename = "mkPairData-cpu-arguments")]
    pub mk_pair_data_cpu_arguments: i64,
    #[serde(rename = "mkPairData-memory-arguments")]
    pub mk_pair_data_memory_arguments: i64,
    #[serde(rename = "modInteger-cpu-arguments-constant")]
    pub mod_integer_cpu_arguments_constant: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-intercept")]
    pub mod_integer_cpu_arguments_model_arguments_intercept: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-slope")]
    pub mod_integer_cpu_arguments_model_arguments_slope: i64,
    #[serde(rename = "modInteger-memory-arguments-intercept")]
    pub mod_integer_memory_arguments_intercept: i64,
    #[serde(rename = "modInteger-memory-arguments-minimum")]
    pub mod_integer_memory_arguments_minimum: i64,
    #[serde(rename = "modInteger-memory-arguments-slope")]
    pub mod_integer_memory_arguments_slope: i64,
    #[serde(rename = "multiplyInteger-cpu-arguments-intercept")]
    pub multiply_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "multiplyInteger-cpu-arguments-slope")]
    pub multiply_integer_cpu_arguments_slope: i64,
    #[serde(rename = "multiplyInteger-memory-arguments-intercept")]
    pub multiply_integer_memory_arguments_intercept: i64,
    #[serde(rename = "multiplyInteger-memory-arguments-slope")]
    pub multiply_integer_memory_arguments_slope: i64,
    #[serde(rename = "nullList-cpu-arguments")]
    pub null_list_cpu_arguments: i64,
    #[serde(rename = "nullList-memory-arguments")]
    pub null_list_memory_arguments: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-constant")]
    pub quotient_integer_cpu_arguments_constant: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-intercept")]
    pub quotient_integer_cpu_arguments_model_arguments_intercept: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-slope")]
    pub quotient_integer_cpu_arguments_model_arguments_slope: i64,
    #[serde(rename = "quotientInteger-memory-arguments-intercept")]
    pub quotient_integer_memory_arguments_intercept: i64,
    #[serde(rename = "quotientInteger-memory-arguments-minimum")]
    pub quotient_integer_memory_arguments_minimum: i64,
    #[serde(rename = "quotientInteger-memory-arguments-slope")]
    pub quotient_integer_memory_arguments_slope: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-constant")]
    pub remainder_integer_cpu_arguments_constant: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-intercept")]
    pub remainder_integer_cpu_arguments_model_arguments_intercept: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-slope")]
    pub remainder_integer_cpu_arguments_model_arguments_slope: i64,
    #[serde(rename = "remainderInteger-memory-arguments-intercept")]
    pub remainder_integer_memory_arguments_intercept: i64,
    #[serde(rename = "remainderInteger-memory-arguments-minimum")]
    pub remainder_integer_memory_arguments_minimum: i64,
    #[serde(rename = "remainderInteger-memory-arguments-slope")]
    pub remainder_integer_memory_arguments_slope: i64,
    #[serde(rename = "serialiseData-cpu-arguments-intercept")]
    pub serialise_data_cpu_arguments_intercept: i64,
    #[serde(rename = "serialiseData-cpu-arguments-slope")]
    pub serialise_data_cpu_arguments_slope: i64,
    #[serde(rename = "serialiseData-memory-arguments-intercept")]
    pub serialise_data_memory_arguments_intercept: i64,
    #[serde(rename = "serialiseData-memory-arguments-slope")]
    pub serialise_data_memory_arguments_slope: i64,
    #[serde(rename = "sha2_256-cpu-arguments-intercept")]
    pub sha2_256_cpu_arguments_intercept: i64,
    #[serde(rename = "sha2_256-cpu-arguments-slope")]
    pub sha2_256_cpu_arguments_slope: i64,
    #[serde(rename = "sha2_256-memory-arguments")]
    pub sha2_256_memory_arguments: i64,
    #[serde(rename = "sha3_256-cpu-arguments-intercept")]
    pub sha3_256_cpu_arguments_intercept: i64,
    #[serde(rename = "sha3_256-cpu-arguments-slope")]
    pub sha3_256_cpu_arguments_slope: i64,
    #[serde(rename = "sha3_256-memory-arguments")]
    pub sha3_256_memory_arguments: i64,
    #[serde(rename = "sliceByteString-cpu-arguments-intercept")]
    pub slice_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "sliceByteString-cpu-arguments-slope")]
    pub slice_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "sliceByteString-memory-arguments-intercept")]
    pub slice_byte_string_memory_arguments_intercept: i64,
    #[serde(rename = "sliceByteString-memory-arguments-slope")]
    pub slice_byte_string_memory_arguments_slope: i64,
    #[serde(rename = "sndPair-cpu-arguments")]
    pub snd_pair_cpu_arguments: i64,
    #[serde(rename = "sndPair-memory-arguments")]
    pub snd_pair_memory_arguments: i64,
    #[serde(rename = "subtractInteger-cpu-arguments-intercept")]
    pub subtract_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "subtractInteger-cpu-arguments-slope")]
    pub subtract_integer_cpu_arguments_slope: i64,
    #[serde(rename = "subtractInteger-memory-arguments-intercept")]
    pub subtract_integer_memory_arguments_intercept: i64,
    #[serde(rename = "subtractInteger-memory-arguments-slope")]
    pub subtract_integer_memory_arguments_slope: i64,
    #[serde(rename = "tailList-cpu-arguments")]
    pub tail_list_cpu_arguments: i64,
    #[serde(rename = "tailList-memory-arguments")]
    pub tail_list_memory_arguments: i64,
    #[serde(rename = "trace-cpu-arguments")]
    pub trace_cpu_arguments: i64,
    #[serde(rename = "trace-memory-arguments")]
    pub trace_memory_arguments: i64,
    #[serde(rename = "unBData-cpu-arguments")]
    pub un_b_data_cpu_arguments: i64,
    #[serde(rename = "unBData-memory-arguments")]
    pub un_b_data_memory_arguments: i64,
    #[serde(rename = "unConstrData-cpu-arguments")]
    pub un_constr_data_cpu_arguments: i64,
    #[serde(rename = "unConstrData-memory-arguments")]
    pub un_constr_data_memory_arguments: i64,
    #[serde(rename = "unIData-cpu-arguments")]
    pub un_i_data_cpu_arguments: i64,
    #[serde(rename = "unIData-memory-arguments")]
    pub un_i_data_memory_arguments: i64,
    #[serde(rename = "unListData-cpu-arguments")]
    pub un_list_data_cpu_arguments: i64,
    #[serde(rename = "unListData-memory-arguments")]
    pub un_list_data_memory_arguments: i64,
    #[serde(rename = "unMapData-cpu-arguments")]
    pub un_map_data_cpu_arguments: i64,
    #[serde(rename = "unMapData-memory-arguments")]
    pub un_map_data_memory_arguments: i64,
    #[serde(rename = "verifyEcdsaSecp256k1Signature-cpu-arguments")]
    pub verify_ecdsa_secp256k1_signature_cpu_arguments: i64,
    #[serde(rename = "verifyEcdsaSecp256k1Signature-memory-arguments")]
    pub verify_ecdsa_secp256k1_signature_memory_arguments: i64,
    #[serde(rename = "verifyEd25519Signature-cpu-arguments-intercept")]
    pub verify_ed25519_signature_cpu_arguments_intercept: i64,
    #[serde(rename = "verifyEd25519Signature-cpu-arguments-slope")]
    pub verify_ed25519_signature_cpu_arguments_slope: i64,
    #[serde(rename = "verifyEd25519Signature-memory-arguments")]
    pub verify_ed25519_signature_memory_arguments: i64,
    #[serde(rename = "verifySchnorrSecp256k1Signature-cpu-arguments-intercept")]
    pub verify_schnorr_secp256k1_signature_cpu_arguments_intercept: i64,
    #[serde(rename = "verifySchnorrSecp256k1Signature-cpu-arguments-slope")]
    pub verify_schnorr_secp256k1_signature_cpu_arguments_slope: i64,
    #[serde(rename = "verifySchnorrSecp256k1Signature-memory-arguments")]
    pub verify_schnorr_secp256k1_signature_memory_arguments: i64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CostParametersV3 {
    #[serde(rename = "addInteger-cpu-arguments-intercept")]
    pub add_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "addInteger-cpu-arguments-slope")]
    pub add_integer_cpu_arguments_slope: i64,
    #[serde(rename = "addInteger-memory-arguments-intercept")]
    pub add_integer_memory_arguments_intercept: i64,
    #[serde(rename = "addInteger-memory-arguments-slope")]
    pub add_integer_memory_arguments_slope: i64,
    #[serde(rename = "appendByteString-cpu-arguments-intercept")]
    pub append_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "appendByteString-cpu-arguments-slope")]
    pub append_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "appendByteString-memory-arguments-intercept")]
    pub append_byte_string_memory_arguments_intercept: i64,
    #[serde(rename = "appendByteString-memory-arguments-slope")]
    pub append_byte_string_memory_arguments_slope: i64,
    #[serde(rename = "appendString-cpu-arguments-intercept")]
    pub append_string_cpu_arguments_intercept: i64,
    #[serde(rename = "appendString-cpu-arguments-slope")]
    pub append_string_cpu_arguments_slope: i64,
    #[serde(rename = "appendString-memory-arguments-intercept")]
    pub append_string_memory_arguments_intercept: i64,
    #[serde(rename = "appendString-memory-arguments-slope")]
    pub append_string_memory_arguments_slope: i64,
    #[serde(rename = "bData-cpu-arguments")]
    pub b_data_cpu_arguments: i64,
    #[serde(rename = "bData-memory-arguments")]
    pub b_data_memory_arguments: i64,
    #[serde(rename = "blake2b_256-cpu-arguments-intercept")]
    pub blake2b_256_cpu_arguments_intercept: i64,
    #[serde(rename = "blake2b_256-cpu-arguments-slope")]
    pub blake2b_256_cpu_arguments_slope: i64,
    #[serde(rename = "blake2b_256-memory-arguments")]
    pub blake2b_256_memory_arguments: i64,
    #[serde(rename = "cekApplyCost-exBudgetCPU")]
    pub cek_apply_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekApplyCost-exBudgetMemory")]
    pub cek_apply_cost_ex_budget_memory: i64,
    #[serde(rename = "cekBuiltinCost-exBudgetCPU")]
    pub cek_builtin_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekBuiltinCost-exBudgetMemory")]
    pub cek_builtin_cost_ex_budget_memory: i64,
    #[serde(rename = "cekConstCost-exBudgetCPU")]
    pub cek_const_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekConstCost-exBudgetMemory")]
    pub cek_const_cost_ex_budget_memory: i64,
    #[serde(rename = "cekDelayCost-exBudgetCPU")]
    pub cek_delay_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekDelayCost-exBudgetMemory")]
    pub cek_delay_cost_ex_budget_memory: i64,
    #[serde(rename = "cekForceCost-exBudgetCPU")]
    pub cek_force_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekForceCost-exBudgetMemory")]
    pub cek_force_cost_ex_budget_memory: i64,
    #[serde(rename = "cekLamCost-exBudgetCPU")]
    pub cek_lam_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekLamCost-exBudgetMemory")]
    pub cek_lam_cost_ex_budget_memory: i64,
    #[serde(rename = "cekStartupCost-exBudgetCPU")]
    pub cek_startup_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekStartupCost-exBudgetMemory")]
    pub cek_startup_cost_ex_budget_memory: i64,
    #[serde(rename = "cekVarCost-exBudgetCPU")]
    pub cek_var_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekVarCost-exBudgetMemory")]
    pub cek_var_cost_ex_budget_memory: i64,
    #[serde(rename = "chooseData-cpu-arguments")]
    pub choose_data_cpu_arguments: i64,
    #[serde(rename = "chooseData-memory-arguments")]
    pub choose_data_memory_arguments: i64,
    #[serde(rename = "chooseList-cpu-arguments")]
    pub choose_list_cpu_arguments: i64,
    #[serde(rename = "chooseList-memory-arguments")]
    pub choose_list_memory_arguments: i64,
    #[serde(rename = "chooseUnit-cpu-arguments")]
    pub choose_unit_cpu_arguments: i64,
    #[serde(rename = "chooseUnit-memory-arguments")]
    pub choose_unit_memory_arguments: i64,
    #[serde(rename = "consByteString-cpu-arguments-intercept")]
    pub cons_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "consByteString-cpu-arguments-slope")]
    pub cons_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "consByteString-memory-arguments-intercept")]
    pub cons_byte_string_memory_arguments_intercept: i64,
    #[serde(rename = "consByteString-memory-arguments-slope")]
    pub cons_byte_string_memory_arguments_slope: i64,
    #[serde(rename = "constrData-cpu-arguments")]
    pub constr_data_cpu_arguments: i64,
    #[serde(rename = "constrData-memory-arguments")]
    pub constr_data_memory_arguments: i64,
    #[serde(rename = "decodeUtf8-cpu-arguments-intercept")]
    pub decode_utf8_cpu_arguments_intercept: i64,
    #[serde(rename = "decodeUtf8-cpu-arguments-slope")]
    pub decode_utf8_cpu_arguments_slope: i64,
    #[serde(rename = "decodeUtf8-memory-arguments-intercept")]
    pub decode_utf8_memory_arguments_intercept: i64,
    #[serde(rename = "decodeUtf8-memory-arguments-slope")]
    pub decode_utf8_memory_arguments_slope: i64,
    #[serde(rename = "divideInteger-cpu-arguments-constant")]
    pub divide_integer_cpu_arguments_constant: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-c00")]
    pub divide_integer_cpu_arguments_model_arguments_c00: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-c01")]
    pub divide_integer_cpu_arguments_model_arguments_c01: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-c02")]
    pub divide_integer_cpu_arguments_model_arguments_c02: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-c10")]
    pub divide_integer_cpu_arguments_model_arguments_c10: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-c11")]
    pub divide_integer_cpu_arguments_model_arguments_c11: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-c20")]
    pub divide_integer_cpu_arguments_model_arguments_c20: i64,
    #[serde(rename = "divideInteger-cpu-arguments-model-arguments-minimum")]
    pub divide_integer_cpu_arguments_model_arguments_minimum: i64,
    #[serde(rename = "divideInteger-memory-arguments-intercept")]
    pub divide_integer_memory_arguments_intercept: i64,
    #[serde(rename = "divideInteger-memory-arguments-minimum")]
    pub divide_integer_memory_arguments_minimum: i64,
    #[serde(rename = "divideInteger-memory-arguments-slope")]
    pub divide_integer_memory_arguments_slope: i64,
    #[serde(rename = "encodeUtf8-cpu-arguments-intercept")]
    pub encode_utf8_cpu_arguments_intercept: i64,
    #[serde(rename = "encodeUtf8-cpu-arguments-slope")]
    pub encode_utf8_cpu_arguments_slope: i64,
    #[serde(rename = "encodeUtf8-memory-arguments-intercept")]
    pub encode_utf8_memory_arguments_intercept: i64,
    #[serde(rename = "encodeUtf8-memory-arguments-slope")]
    pub encode_utf8_memory_arguments_slope: i64,
    #[serde(rename = "equalsByteString-cpu-arguments-constant")]
    pub equals_byte_string_cpu_arguments_constant: i64,
    #[serde(rename = "equalsByteString-cpu-arguments-intercept")]
    pub equals_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsByteString-cpu-arguments-slope")]
    pub equals_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "equalsByteString-memory-arguments")]
    pub equals_byte_string_memory_arguments: i64,
    #[serde(rename = "equalsData-cpu-arguments-intercept")]
    pub equals_data_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsData-cpu-arguments-slope")]
    pub equals_data_cpu_arguments_slope: i64,
    #[serde(rename = "equalsData-memory-arguments")]
    pub equals_data_memory_arguments: i64,
    #[serde(rename = "equalsInteger-cpu-arguments-intercept")]
    pub equals_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsInteger-cpu-arguments-slope")]
    pub equals_integer_cpu_arguments_slope: i64,
    #[serde(rename = "equalsInteger-memory-arguments")]
    pub equals_integer_memory_arguments: i64,
    #[serde(rename = "equalsString-cpu-arguments-constant")]
    pub equals_string_cpu_arguments_constant: i64,
    #[serde(rename = "equalsString-cpu-arguments-intercept")]
    pub equals_string_cpu_arguments_intercept: i64,
    #[serde(rename = "equalsString-cpu-arguments-slope")]
    pub equals_string_cpu_arguments_slope: i64,
    #[serde(rename = "equalsString-memory-arguments")]
    pub equals_string_memory_arguments: i64,
    #[serde(rename = "fstPair-cpu-arguments")]
    pub fst_pair_cpu_arguments: i64,
    #[serde(rename = "fstPair-memory-arguments")]
    pub fst_pair_memory_arguments: i64,
    #[serde(rename = "headList-cpu-arguments")]
    pub head_list_cpu_arguments: i64,
    #[serde(rename = "headList-memory-arguments")]
    pub head_list_memory_arguments: i64,
    #[serde(rename = "iData-cpu-arguments")]
    pub i_data_cpu_arguments: i64,
    #[serde(rename = "iData-memory-arguments")]
    pub i_data_memory_arguments: i64,
    #[serde(rename = "ifThenElse-cpu-arguments")]
    pub if_then_else_cpu_arguments: i64,
    #[serde(rename = "ifThenElse-memory-arguments")]
    pub if_then_else_memory_arguments: i64,
    #[serde(rename = "indexByteString-cpu-arguments")]
    pub index_byte_string_cpu_arguments: i64,
    #[serde(rename = "indexByteString-memory-arguments")]
    pub index_byte_string_memory_arguments: i64,
    #[serde(rename = "lengthOfByteString-cpu-arguments")]
    pub length_of_byte_string_cpu_arguments: i64,
    #[serde(rename = "lengthOfByteString-memory-arguments")]
    pub length_of_byte_string_memory_arguments: i64,
    #[serde(rename = "lessThanByteString-cpu-arguments-intercept")]
    pub less_than_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanByteString-cpu-arguments-slope")]
    pub less_than_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanByteString-memory-arguments")]
    pub less_than_byte_string_memory_arguments: i64,
    #[serde(rename = "lessThanEqualsByteString-cpu-arguments-intercept")]
    pub less_than_equals_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanEqualsByteString-cpu-arguments-slope")]
    pub less_than_equals_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanEqualsByteString-memory-arguments")]
    pub less_than_equals_byte_string_memory_arguments: i64,
    #[serde(rename = "lessThanEqualsInteger-cpu-arguments-intercept")]
    pub less_than_equals_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanEqualsInteger-cpu-arguments-slope")]
    pub less_than_equals_integer_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanEqualsInteger-memory-arguments")]
    pub less_than_equals_integer_memory_arguments: i64,
    #[serde(rename = "lessThanInteger-cpu-arguments-intercept")]
    pub less_than_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "lessThanInteger-cpu-arguments-slope")]
    pub less_than_integer_cpu_arguments_slope: i64,
    #[serde(rename = "lessThanInteger-memory-arguments")]
    pub less_than_integer_memory_arguments: i64,
    #[serde(rename = "listData-cpu-arguments")]
    pub list_data_cpu_arguments: i64,
    #[serde(rename = "listData-memory-arguments")]
    pub list_data_memory_arguments: i64,
    #[serde(rename = "mapData-cpu-arguments")]
    pub map_data_cpu_arguments: i64,
    #[serde(rename = "mapData-memory-arguments")]
    pub map_data_memory_arguments: i64,
    #[serde(rename = "mkCons-cpu-arguments")]
    pub mk_cons_cpu_arguments: i64,
    #[serde(rename = "mkCons-memory-arguments")]
    pub mk_cons_memory_arguments: i64,
    #[serde(rename = "mkNilData-cpu-arguments")]
    pub mk_nil_data_cpu_arguments: i64,
    #[serde(rename = "mkNilData-memory-arguments")]
    pub mk_nil_data_memory_arguments: i64,
    #[serde(rename = "mkNilPairData-cpu-arguments")]
    pub mk_nil_pair_data_cpu_arguments: i64,
    #[serde(rename = "mkNilPairData-memory-arguments")]
    pub mk_nil_pair_data_memory_arguments: i64,
    #[serde(rename = "mkPairData-cpu-arguments")]
    pub mk_pair_data_cpu_arguments: i64,
    #[serde(rename = "mkPairData-memory-arguments")]
    pub mk_pair_data_memory_arguments: i64,
    #[serde(rename = "modInteger-cpu-arguments-constant")]
    pub mod_integer_cpu_arguments_constant: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-c00")]
    pub mod_integer_cpu_arguments_model_arguments_c00: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-c01")]
    pub mod_integer_cpu_arguments_model_arguments_c01: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-c02")]
    pub mod_integer_cpu_arguments_model_arguments_c02: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-c10")]
    pub mod_integer_cpu_arguments_model_arguments_c10: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-c11")]
    pub mod_integer_cpu_arguments_model_arguments_c11: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-c20")]
    pub mod_integer_cpu_arguments_model_arguments_c20: i64,
    #[serde(rename = "modInteger-cpu-arguments-model-arguments-minimum")]
    pub mod_integer_cpu_arguments_model_arguments_minimum: i64,
    #[serde(rename = "modInteger-memory-arguments-intercept")]
    pub mod_integer_memory_arguments_intercept: i64,
    #[serde(rename = "modInteger-memory-arguments-minimum")]
    pub mod_integer_memory_arguments_minimum: i64,
    #[serde(rename = "modInteger-memory-arguments-slope")]
    pub mod_integer_memory_arguments_slope: i64,
    #[serde(rename = "multiplyInteger-cpu-arguments-intercept")]
    pub multiply_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "multiplyInteger-cpu-arguments-slope")]
    pub multiply_integer_cpu_arguments_slope: i64,
    #[serde(rename = "multiplyInteger-memory-arguments-intercept")]
    pub multiply_integer_memory_arguments_intercept: i64,
    #[serde(rename = "multiplyInteger-memory-arguments-slope")]
    pub multiply_integer_memory_arguments_slope: i64,
    #[serde(rename = "nullList-cpu-arguments")]
    pub null_list_cpu_arguments: i64,
    #[serde(rename = "nullList-memory-arguments")]
    pub null_list_memory_arguments: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-constant")]
    pub quotient_integer_cpu_arguments_constant: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-c00")]
    pub quotient_integer_cpu_arguments_model_arguments_c00: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-c01")]
    pub quotient_integer_cpu_arguments_model_arguments_c01: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-c02")]
    pub quotient_integer_cpu_arguments_model_arguments_c02: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-c10")]
    pub quotient_integer_cpu_arguments_model_arguments_c10: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-c11")]
    pub quotient_integer_cpu_arguments_model_arguments_c11: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-c20")]
    pub quotient_integer_cpu_arguments_model_arguments_c20: i64,
    #[serde(rename = "quotientInteger-cpu-arguments-model-arguments-minimum")]
    pub quotient_integer_cpu_arguments_model_arguments_minimum: i64,
    #[serde(rename = "quotientInteger-memory-arguments-intercept")]
    pub quotient_integer_memory_arguments_intercept: i64,
    #[serde(rename = "quotientInteger-memory-arguments-minimum")]
    pub quotient_integer_memory_arguments_minimum: i64,
    #[serde(rename = "quotientInteger-memory-arguments-slope")]
    pub quotient_integer_memory_arguments_slope: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-constant")]
    pub remainder_integer_cpu_arguments_constant: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-c00")]
    pub remainder_integer_cpu_arguments_model_arguments_c00: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-c01")]
    pub remainder_integer_cpu_arguments_model_arguments_c01: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-c02")]
    pub remainder_integer_cpu_arguments_model_arguments_c02: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-c10")]
    pub remainder_integer_cpu_arguments_model_arguments_c10: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-c11")]
    pub remainder_integer_cpu_arguments_model_arguments_c11: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-c20")]
    pub remainder_integer_cpu_arguments_model_arguments_c20: i64,
    #[serde(rename = "remainderInteger-cpu-arguments-model-arguments-minimum")]
    pub remainder_integer_cpu_arguments_model_arguments_minimum: i64,
    #[serde(rename = "remainderInteger-memory-arguments-intercept")]
    pub remainder_integer_memory_arguments_intercept: i64,
    #[serde(rename = "remainderInteger-memory-arguments-minimum")]
    pub remainder_integer_memory_arguments_minimum: i64,
    #[serde(rename = "remainderInteger-memory-arguments-slope")]
    pub remainder_integer_memory_arguments_slope: i64,
    #[serde(rename = "serialiseData-cpu-arguments-intercept")]
    pub serialise_data_cpu_arguments_intercept: i64,
    #[serde(rename = "serialiseData-cpu-arguments-slope")]
    pub serialise_data_cpu_arguments_slope: i64,
    #[serde(rename = "serialiseData-memory-arguments-intercept")]
    pub serialise_data_memory_arguments_intercept: i64,
    #[serde(rename = "serialiseData-memory-arguments-slope")]
    pub serialise_data_memory_arguments_slope: i64,
    #[serde(rename = "sha2_256-cpu-arguments-intercept")]
    pub sha2_256_cpu_arguments_intercept: i64,
    #[serde(rename = "sha2_256-cpu-arguments-slope")]
    pub sha2_256_cpu_arguments_slope: i64,
    #[serde(rename = "sha2_256-memory-arguments")]
    pub sha2_256_memory_arguments: i64,
    #[serde(rename = "sha3_256-cpu-arguments-intercept")]
    pub sha3_256_cpu_arguments_intercept: i64,
    #[serde(rename = "sha3_256-cpu-arguments-slope")]
    pub sha3_256_cpu_arguments_slope: i64,
    #[serde(rename = "sha3_256-memory-arguments")]
    pub sha3_256_memory_arguments: i64,
    #[serde(rename = "sliceByteString-cpu-arguments-intercept")]
    pub slice_byte_string_cpu_arguments_intercept: i64,
    #[serde(rename = "sliceByteString-cpu-arguments-slope")]
    pub slice_byte_string_cpu_arguments_slope: i64,
    #[serde(rename = "sliceByteString-memory-arguments-intercept")]
    pub slice_byte_string_memory_arguments_intercept: i64,
    #[serde(rename = "sliceByteString-memory-arguments-slope")]
    pub slice_byte_string_memory_arguments_slope: i64,
    #[serde(rename = "sndPair-cpu-arguments")]
    pub snd_pair_cpu_arguments: i64,
    #[serde(rename = "sndPair-memory-arguments")]
    pub snd_pair_memory_arguments: i64,
    #[serde(rename = "subtractInteger-cpu-arguments-intercept")]
    pub subtract_integer_cpu_arguments_intercept: i64,
    #[serde(rename = "subtractInteger-cpu-arguments-slope")]
    pub subtract_integer_cpu_arguments_slope: i64,
    #[serde(rename = "subtractInteger-memory-arguments-intercept")]
    pub subtract_integer_memory_arguments_intercept: i64,
    #[serde(rename = "subtractInteger-memory-arguments-slope")]
    pub subtract_integer_memory_arguments_slope: i64,
    #[serde(rename = "tailList-cpu-arguments")]
    pub tail_list_cpu_arguments: i64,
    #[serde(rename = "tailList-memory-arguments")]
    pub tail_list_memory_arguments: i64,
    #[serde(rename = "trace-cpu-arguments")]
    pub trace_cpu_arguments: i64,
    #[serde(rename = "trace-memory-arguments")]
    pub trace_memory_arguments: i64,
    #[serde(rename = "unBData-cpu-arguments")]
    pub un_b_data_cpu_arguments: i64,
    #[serde(rename = "unBData-memory-arguments")]
    pub un_b_data_memory_arguments: i64,
    #[serde(rename = "unConstrData-cpu-arguments")]
    pub un_constr_data_cpu_arguments: i64,
    #[serde(rename = "unConstrData-memory-arguments")]
    pub un_constr_data_memory_arguments: i64,
    #[serde(rename = "unIData-cpu-arguments")]
    pub un_i_data_cpu_arguments: i64,
    #[serde(rename = "unIData-memory-arguments")]
    pub un_i_data_memory_arguments: i64,
    #[serde(rename = "unListData-cpu-arguments")]
    pub un_list_data_cpu_arguments: i64,
    #[serde(rename = "unListData-memory-arguments")]
    pub un_list_data_memory_arguments: i64,
    #[serde(rename = "unMapData-cpu-arguments")]
    pub un_map_data_cpu_arguments: i64,
    #[serde(rename = "unMapData-memory-arguments")]
    pub un_map_data_memory_arguments: i64,
    #[serde(rename = "verifyEcdsaSecp256k1Signature-cpu-arguments")]
    pub verify_ecdsa_secp256k1_signature_cpu_arguments: i64,
    #[serde(rename = "verifyEcdsaSecp256k1Signature-memory-arguments")]
    pub verify_ecdsa_secp256k1_signature_memory_arguments: i64,
    #[serde(rename = "verifyEd25519Signature-cpu-arguments-intercept")]
    pub verify_ed25519_signature_cpu_arguments_intercept: i64,
    #[serde(rename = "verifyEd25519Signature-cpu-arguments-slope")]
    pub verify_ed25519_signature_cpu_arguments_slope: i64,
    #[serde(rename = "verifyEd25519Signature-memory-arguments")]
    pub verify_ed25519_signature_memory_arguments: i64,
    #[serde(rename = "verifySchnorrSecp256k1Signature-cpu-arguments-intercept")]
    pub verify_schnorr_secp256k1_signature_cpu_arguments_intercept: i64,
    #[serde(rename = "verifySchnorrSecp256k1Signature-cpu-arguments-slope")]
    pub verify_schnorr_secp256k1_signature_cpu_arguments_slope: i64,
    #[serde(rename = "verifySchnorrSecp256k1Signature-memory-arguments")]
    pub verify_schnorr_secp256k1_signature_memory_arguments: i64,
    #[serde(rename = "cekConstrCost-exBudgetCPU")]
    pub cek_constr_cost_ex_budget_cpu: i64,
    #[serde(rename = "cekConstrCost-exBudgetMemory")]
    pub cek_constr_cost_ex_budget_memory: i64,
}

impl From<&Vec<i64>> for CostParametersV1 {
    fn from(values: &Vec<i64>) -> Self {
        Self {
            add_integer_cpu_arguments_intercept: values[0],
            add_integer_cpu_arguments_slope: values[1],
            add_integer_memory_arguments_intercept: values[2],
            add_integer_memory_arguments_slope: values[3],
            append_byte_string_cpu_arguments_intercept: values[4],
            append_byte_string_cpu_arguments_slope: values[5],
            append_byte_string_memory_arguments_intercept: values[6],
            append_byte_string_memory_arguments_slope: values[7],
            append_string_cpu_arguments_intercept: values[8],
            append_string_cpu_arguments_slope: values[9],
            append_string_memory_arguments_intercept: values[10],
            append_string_memory_arguments_slope: values[11],
            b_data_cpu_arguments: values[12],
            b_data_memory_arguments: values[13],
            blake2b_256_cpu_arguments_intercept: values[14],
            blake2b_256_cpu_arguments_slope: values[15],
            blake2b_256_memory_arguments: values[16],
            cek_apply_cost_ex_budget_cpu: values[17],
            cek_apply_cost_ex_budget_memory: values[18],
            cek_builtin_cost_ex_budget_cpu: values[19],
            cek_builtin_cost_ex_budget_memory: values[20],
            cek_const_cost_ex_budget_cpu: values[21],
            cek_const_cost_ex_budget_memory: values[22],
            cek_delay_cost_ex_budget_cpu: values[23],
            cek_delay_cost_ex_budget_memory: values[24],
            cek_force_cost_ex_budget_cpu: values[25],
            cek_force_cost_ex_budget_memory: values[26],
            cek_lam_cost_ex_budget_cpu: values[27],
            cek_lam_cost_ex_budget_memory: values[28],
            cek_startup_cost_ex_budget_cpu: values[29],
            cek_startup_cost_ex_budget_memory: values[30],
            cek_var_cost_ex_budget_cpu: values[31],
            cek_var_cost_ex_budget_memory: values[32],
            choose_data_cpu_arguments: values[33],
            choose_data_memory_arguments: values[34],
            choose_list_cpu_arguments: values[35],
            choose_list_memory_arguments: values[36],
            choose_unit_cpu_arguments: values[37],
            choose_unit_memory_arguments: values[38],
            cons_byte_string_cpu_arguments_intercept: values[39],
            cons_byte_string_cpu_arguments_slope: values[40],
            cons_byte_string_memory_arguments_intercept: values[41],
            cons_byte_string_memory_arguments_slope: values[42],
            constr_data_cpu_arguments: values[43],
            constr_data_memory_arguments: values[44],
            decode_utf8_cpu_arguments_intercept: values[45],
            decode_utf8_cpu_arguments_slope: values[46],
            decode_utf8_memory_arguments_intercept: values[47],
            decode_utf8_memory_arguments_slope: values[48],
            divide_integer_cpu_arguments_constant: values[49],
            divide_integer_cpu_arguments_model_arguments_intercept: values[50],
            divide_integer_cpu_arguments_model_arguments_slope: values[51],
            divide_integer_memory_arguments_intercept: values[52],
            divide_integer_memory_arguments_minimum: values[53],
            divide_integer_memory_arguments_slope: values[54],
            encode_utf8_cpu_arguments_intercept: values[55],
            encode_utf8_cpu_arguments_slope: values[56],
            encode_utf8_memory_arguments_intercept: values[57],
            encode_utf8_memory_arguments_slope: values[58],
            equals_byte_string_cpu_arguments_constant: values[59],
            equals_byte_string_cpu_arguments_intercept: values[60],
            equals_byte_string_cpu_arguments_slope: values[61],
            equals_byte_string_memory_arguments: values[62],
            equals_data_cpu_arguments_intercept: values[63],
            equals_data_cpu_arguments_slope: values[64],
            equals_data_memory_arguments: values[65],
            equals_integer_cpu_arguments_intercept: values[66],
            equals_integer_cpu_arguments_slope: values[67],
            equals_integer_memory_arguments: values[68],
            equals_string_cpu_arguments_constant: values[69],
            equals_string_cpu_arguments_intercept: values[70],
            equals_string_cpu_arguments_slope: values[71],
            equals_string_memory_arguments: values[72],
            fst_pair_cpu_arguments: values[73],
            fst_pair_memory_arguments: values[74],
            head_list_cpu_arguments: values[75],
            head_list_memory_arguments: values[76],
            i_data_cpu_arguments: values[77],
            i_data_memory_arguments: values[78],
            if_then_else_cpu_arguments: values[79],
            if_then_else_memory_arguments: values[80],
            index_byte_string_cpu_arguments: values[81],
            index_byte_string_memory_arguments: values[82],
            length_of_byte_string_cpu_arguments: values[83],
            length_of_byte_string_memory_arguments: values[84],
            less_than_byte_string_cpu_arguments_intercept: values[85],
            less_than_byte_string_cpu_arguments_slope: values[86],
            less_than_byte_string_memory_arguments: values[87],
            less_than_equals_byte_string_cpu_arguments_intercept: values[88],
            less_than_equals_byte_string_cpu_arguments_slope: values[89],
            less_than_equals_byte_string_memory_arguments: values[90],
            less_than_equals_integer_cpu_arguments_intercept: values[91],
            less_than_equals_integer_cpu_arguments_slope: values[92],
            less_than_equals_integer_memory_arguments: values[93],
            less_than_integer_cpu_arguments_intercept: values[94],
            less_than_integer_cpu_arguments_slope: values[95],
            less_than_integer_memory_arguments: values[96],
            list_data_cpu_arguments: values[97],
            list_data_memory_arguments: values[98],
            map_data_cpu_arguments: values[99],
            map_data_memory_arguments: values[100],
            mk_cons_cpu_arguments: values[101],
            mk_cons_memory_arguments: values[102],
            mk_nil_data_cpu_arguments: values[103],
            mk_nil_data_memory_arguments: values[104],
            mk_nil_pair_data_cpu_arguments: values[105],
            mk_nil_pair_data_memory_arguments: values[106],
            mk_pair_data_cpu_arguments: values[107],
            mk_pair_data_memory_arguments: values[108],
            mod_integer_cpu_arguments_constant: values[109],
            mod_integer_cpu_arguments_model_arguments_intercept: values[110],
            mod_integer_cpu_arguments_model_arguments_slope: values[111],
            mod_integer_memory_arguments_intercept: values[112],
            mod_integer_memory_arguments_minimum: values[113],
            mod_integer_memory_arguments_slope: values[114],
            multiply_integer_cpu_arguments_intercept: values[115],
            multiply_integer_cpu_arguments_slope: values[116],
            multiply_integer_memory_arguments_intercept: values[117],
            multiply_integer_memory_arguments_slope: values[118],
            null_list_cpu_arguments: values[119],
            null_list_memory_arguments: values[120],
            quotient_integer_cpu_arguments_constant: values[121],
            quotient_integer_cpu_arguments_model_arguments_intercept: values[122],
            quotient_integer_cpu_arguments_model_arguments_slope: values[123],
            quotient_integer_memory_arguments_intercept: values[124],
            quotient_integer_memory_arguments_minimum: values[125],
            quotient_integer_memory_arguments_slope: values[126],
            remainder_integer_cpu_arguments_constant: values[127],
            remainder_integer_cpu_arguments_model_arguments_intercept: values[128],
            remainder_integer_cpu_arguments_model_arguments_slope: values[129],
            remainder_integer_memory_arguments_intercept: values[130],
            remainder_integer_memory_arguments_minimum: values[131],
            remainder_integer_memory_arguments_slope: values[132],
            sha2_256_cpu_arguments_intercept: values[133],
            sha2_256_cpu_arguments_slope: values[134],
            sha2_256_memory_arguments: values[135],
            sha3_256_cpu_arguments_intercept: values[136],
            sha3_256_cpu_arguments_slope: values[137],
            sha3_256_memory_arguments: values[138],
            slice_byte_string_cpu_arguments_intercept: values[139],
            slice_byte_string_cpu_arguments_slope: values[140],
            slice_byte_string_memory_arguments_intercept: values[141],
            slice_byte_string_memory_arguments_slope: values[142],
            snd_pair_cpu_arguments: values[143],
            snd_pair_memory_arguments: values[144],
            subtract_integer_cpu_arguments_intercept: values[145],
            subtract_integer_cpu_arguments_slope: values[146],
            subtract_integer_memory_arguments_intercept: values[147],
            subtract_integer_memory_arguments_slope: values[148],
            tail_list_cpu_arguments: values[149],
            tail_list_memory_arguments: values[150],
            trace_cpu_arguments: values[151],
            trace_memory_arguments: values[152],
            un_b_data_cpu_arguments: values[153],
            un_b_data_memory_arguments: values[154],
            un_constr_data_cpu_arguments: values[155],
            un_constr_data_memory_arguments: values[156],
            un_i_data_cpu_arguments: values[157],
            un_i_data_memory_arguments: values[158],
            un_list_data_cpu_arguments: values[159],
            un_list_data_memory_arguments: values[160],
            un_map_data_cpu_arguments: values[161],
            un_map_data_memory_arguments: values[162],
            verify_ed25519_signature_cpu_arguments_intercept: values[163],
            verify_ed25519_signature_cpu_arguments_slope: values[164],
            verify_ed25519_signature_memory_arguments: values[165],
        }
    }
}

impl From<&Vec<i64>> for CostParametersV2 {
    fn from(values: &Vec<i64>) -> Self {
        Self {
            add_integer_cpu_arguments_intercept: values[0],
            add_integer_cpu_arguments_slope: values[1],
            add_integer_memory_arguments_intercept: values[2],
            add_integer_memory_arguments_slope: values[3],
            append_byte_string_cpu_arguments_intercept: values[4],
            append_byte_string_cpu_arguments_slope: values[5],
            append_byte_string_memory_arguments_intercept: values[6],
            append_byte_string_memory_arguments_slope: values[7],
            append_string_cpu_arguments_intercept: values[8],
            append_string_cpu_arguments_slope: values[9],
            append_string_memory_arguments_intercept: values[10],
            append_string_memory_arguments_slope: values[11],
            b_data_cpu_arguments: values[12],
            b_data_memory_arguments: values[13],
            blake2b_256_cpu_arguments_intercept: values[14],
            blake2b_256_cpu_arguments_slope: values[15],
            blake2b_256_memory_arguments: values[16],
            cek_apply_cost_ex_budget_cpu: values[17],
            cek_apply_cost_ex_budget_memory: values[18],
            cek_builtin_cost_ex_budget_cpu: values[19],
            cek_builtin_cost_ex_budget_memory: values[20],
            cek_const_cost_ex_budget_cpu: values[21],
            cek_const_cost_ex_budget_memory: values[22],
            cek_delay_cost_ex_budget_cpu: values[23],
            cek_delay_cost_ex_budget_memory: values[24],
            cek_force_cost_ex_budget_cpu: values[25],
            cek_force_cost_ex_budget_memory: values[26],
            cek_lam_cost_ex_budget_cpu: values[27],
            cek_lam_cost_ex_budget_memory: values[28],
            cek_startup_cost_ex_budget_cpu: values[29],
            cek_startup_cost_ex_budget_memory: values[30],
            cek_var_cost_ex_budget_cpu: values[31],
            cek_var_cost_ex_budget_memory: values[32],
            choose_data_cpu_arguments: values[33],
            choose_data_memory_arguments: values[34],
            choose_list_cpu_arguments: values[35],
            choose_list_memory_arguments: values[36],
            choose_unit_cpu_arguments: values[37],
            choose_unit_memory_arguments: values[38],
            cons_byte_string_cpu_arguments_intercept: values[39],
            cons_byte_string_cpu_arguments_slope: values[40],
            cons_byte_string_memory_arguments_intercept: values[41],
            cons_byte_string_memory_arguments_slope: values[42],
            constr_data_cpu_arguments: values[43],
            constr_data_memory_arguments: values[44],
            decode_utf8_cpu_arguments_intercept: values[45],
            decode_utf8_cpu_arguments_slope: values[46],
            decode_utf8_memory_arguments_intercept: values[47],
            decode_utf8_memory_arguments_slope: values[48],
            divide_integer_cpu_arguments_constant: values[49],
            divide_integer_cpu_arguments_model_arguments_intercept: values[50],
            divide_integer_cpu_arguments_model_arguments_slope: values[51],
            divide_integer_memory_arguments_intercept: values[52],
            divide_integer_memory_arguments_minimum: values[53],
            divide_integer_memory_arguments_slope: values[54],
            encode_utf8_cpu_arguments_intercept: values[55],
            encode_utf8_cpu_arguments_slope: values[56],
            encode_utf8_memory_arguments_intercept: values[57],
            encode_utf8_memory_arguments_slope: values[58],
            equals_byte_string_cpu_arguments_constant: values[59],
            equals_byte_string_cpu_arguments_intercept: values[60],
            equals_byte_string_cpu_arguments_slope: values[61],
            equals_byte_string_memory_arguments: values[62],
            equals_data_cpu_arguments_intercept: values[63],
            equals_data_cpu_arguments_slope: values[64],
            equals_data_memory_arguments: values[65],
            equals_integer_cpu_arguments_intercept: values[66],
            equals_integer_cpu_arguments_slope: values[67],
            equals_integer_memory_arguments: values[68],
            equals_string_cpu_arguments_constant: values[69],
            equals_string_cpu_arguments_intercept: values[70],
            equals_string_cpu_arguments_slope: values[71],
            equals_string_memory_arguments: values[72],
            fst_pair_cpu_arguments: values[73],
            fst_pair_memory_arguments: values[74],
            head_list_cpu_arguments: values[75],
            head_list_memory_arguments: values[76],
            i_data_cpu_arguments: values[77],
            i_data_memory_arguments: values[78],
            if_then_else_cpu_arguments: values[79],
            if_then_else_memory_arguments: values[80],
            index_byte_string_cpu_arguments: values[81],
            index_byte_string_memory_arguments: values[82],
            length_of_byte_string_cpu_arguments: values[83],
            length_of_byte_string_memory_arguments: values[84],
            less_than_byte_string_cpu_arguments_intercept: values[85],
            less_than_byte_string_cpu_arguments_slope: values[86],
            less_than_byte_string_memory_arguments: values[87],
            less_than_equals_byte_string_cpu_arguments_intercept: values[88],
            less_than_equals_byte_string_cpu_arguments_slope: values[89],
            less_than_equals_byte_string_memory_arguments: values[90],
            less_than_equals_integer_cpu_arguments_intercept: values[91],
            less_than_equals_integer_cpu_arguments_slope: values[92],
            less_than_equals_integer_memory_arguments: values[93],
            less_than_integer_cpu_arguments_intercept: values[94],
            less_than_integer_cpu_arguments_slope: values[95],
            less_than_integer_memory_arguments: values[96],
            list_data_cpu_arguments: values[97],
            list_data_memory_arguments: values[98],
            map_data_cpu_arguments: values[99],
            map_data_memory_arguments: values[100],
            mk_cons_cpu_arguments: values[101],
            mk_cons_memory_arguments: values[102],
            mk_nil_data_cpu_arguments: values[103],
            mk_nil_data_memory_arguments: values[104],
            mk_nil_pair_data_cpu_arguments: values[105],
            mk_nil_pair_data_memory_arguments: values[106],
            mk_pair_data_cpu_arguments: values[107],
            mk_pair_data_memory_arguments: values[108],
            mod_integer_cpu_arguments_constant: values[109],
            mod_integer_cpu_arguments_model_arguments_intercept: values[110],
            mod_integer_cpu_arguments_model_arguments_slope: values[111],
            mod_integer_memory_arguments_intercept: values[112],
            mod_integer_memory_arguments_minimum: values[113],
            mod_integer_memory_arguments_slope: values[114],
            multiply_integer_cpu_arguments_intercept: values[115],
            multiply_integer_cpu_arguments_slope: values[116],
            multiply_integer_memory_arguments_intercept: values[117],
            multiply_integer_memory_arguments_slope: values[118],
            null_list_cpu_arguments: values[119],
            null_list_memory_arguments: values[120],
            quotient_integer_cpu_arguments_constant: values[121],
            quotient_integer_cpu_arguments_model_arguments_intercept: values[122],
            quotient_integer_cpu_arguments_model_arguments_slope: values[123],
            quotient_integer_memory_arguments_intercept: values[124],
            quotient_integer_memory_arguments_minimum: values[125],
            quotient_integer_memory_arguments_slope: values[126],
            remainder_integer_cpu_arguments_constant: values[127],
            remainder_integer_cpu_arguments_model_arguments_intercept: values[128],
            remainder_integer_cpu_arguments_model_arguments_slope: values[129],
            remainder_integer_memory_arguments_intercept: values[130],
            remainder_integer_memory_arguments_minimum: values[131],
            remainder_integer_memory_arguments_slope: values[132],
            serialise_data_cpu_arguments_intercept: values[133],
            serialise_data_cpu_arguments_slope: values[134],
            serialise_data_memory_arguments_intercept: values[135],
            serialise_data_memory_arguments_slope: values[136],
            sha2_256_cpu_arguments_intercept: values[137],
            sha2_256_cpu_arguments_slope: values[138],
            sha2_256_memory_arguments: values[139],
            sha3_256_cpu_arguments_intercept: values[140],
            sha3_256_cpu_arguments_slope: values[141],
            sha3_256_memory_arguments: values[142],
            slice_byte_string_cpu_arguments_intercept: values[143],
            slice_byte_string_cpu_arguments_slope: values[144],
            slice_byte_string_memory_arguments_intercept: values[145],
            slice_byte_string_memory_arguments_slope: values[146],
            snd_pair_cpu_arguments: values[147],
            snd_pair_memory_arguments: values[148],
            subtract_integer_cpu_arguments_intercept: values[149],
            subtract_integer_cpu_arguments_slope: values[150],
            subtract_integer_memory_arguments_intercept: values[151],
            subtract_integer_memory_arguments_slope: values[152],
            tail_list_cpu_arguments: values[153],
            tail_list_memory_arguments: values[154],
            trace_cpu_arguments: values[155],
            trace_memory_arguments: values[156],
            un_b_data_cpu_arguments: values[157],
            un_b_data_memory_arguments: values[158],
            un_constr_data_cpu_arguments: values[159],
            un_constr_data_memory_arguments: values[160],
            un_i_data_cpu_arguments: values[161],
            un_i_data_memory_arguments: values[162],
            un_list_data_cpu_arguments: values[163],
            un_list_data_memory_arguments: values[164],
            un_map_data_cpu_arguments: values[165],
            un_map_data_memory_arguments: values[166],
            verify_ecdsa_secp256k1_signature_cpu_arguments: values[167],
            verify_ecdsa_secp256k1_signature_memory_arguments: values[168],
            verify_ed25519_signature_cpu_arguments_intercept: values[169],
            verify_ed25519_signature_cpu_arguments_slope: values[170],
            verify_ed25519_signature_memory_arguments: values[171],
            verify_schnorr_secp256k1_signature_cpu_arguments_intercept: values[172],
            verify_schnorr_secp256k1_signature_cpu_arguments_slope: values[173],
            verify_schnorr_secp256k1_signature_memory_arguments: values[174],
        }
    }
}

impl From<&Vec<i64>> for CostParametersV3 {
    fn from(values: &Vec<i64>) -> Self {
        Self {
            add_integer_cpu_arguments_intercept: values[0],
            add_integer_cpu_arguments_slope: values[1],
            add_integer_memory_arguments_intercept: values[2],
            add_integer_memory_arguments_slope: values[3],
            append_byte_string_cpu_arguments_intercept: values[4],
            append_byte_string_cpu_arguments_slope: values[5],
            append_byte_string_memory_arguments_intercept: values[6],
            append_byte_string_memory_arguments_slope: values[7],
            append_string_cpu_arguments_intercept: values[8],
            append_string_cpu_arguments_slope: values[9],
            append_string_memory_arguments_intercept: values[10],
            append_string_memory_arguments_slope: values[11],
            b_data_cpu_arguments: values[12],
            b_data_memory_arguments: values[13],
            blake2b_256_cpu_arguments_intercept: values[14],
            blake2b_256_cpu_arguments_slope: values[15],
            blake2b_256_memory_arguments: values[16],
            cek_apply_cost_ex_budget_cpu: values[17],
            cek_apply_cost_ex_budget_memory: values[18],
            cek_builtin_cost_ex_budget_cpu: values[19],
            cek_builtin_cost_ex_budget_memory: values[20],
            cek_const_cost_ex_budget_cpu: values[21],
            cek_const_cost_ex_budget_memory: values[22],
            cek_delay_cost_ex_budget_cpu: values[23],
            cek_delay_cost_ex_budget_memory: values[24],
            cek_force_cost_ex_budget_cpu: values[25],
            cek_force_cost_ex_budget_memory: values[26],
            cek_lam_cost_ex_budget_cpu: values[27],
            cek_lam_cost_ex_budget_memory: values[28],
            cek_startup_cost_ex_budget_cpu: values[29],
            cek_startup_cost_ex_budget_memory: values[30],
            cek_var_cost_ex_budget_cpu: values[31],
            cek_var_cost_ex_budget_memory: values[32],
            choose_data_cpu_arguments: values[33],
            choose_data_memory_arguments: values[34],
            choose_list_cpu_arguments: values[35],
            choose_list_memory_arguments: values[36],
            choose_unit_cpu_arguments: values[37],
            choose_unit_memory_arguments: values[38],
            cons_byte_string_cpu_arguments_intercept: values[39],
            cons_byte_string_cpu_arguments_slope: values[40],
            cons_byte_string_memory_arguments_intercept: values[41],
            cons_byte_string_memory_arguments_slope: values[42],
            constr_data_cpu_arguments: values[43],
            constr_data_memory_arguments: values[44],
            decode_utf8_cpu_arguments_intercept: values[45],
            decode_utf8_cpu_arguments_slope: values[46],
            decode_utf8_memory_arguments_intercept: values[47],
            decode_utf8_memory_arguments_slope: values[48],
            divide_integer_cpu_arguments_constant: values[49],
            divide_integer_cpu_arguments_model_arguments_c00: values[50],
            divide_integer_cpu_arguments_model_arguments_c01: values[51],
            divide_integer_cpu_arguments_model_arguments_c02: values[52],
            divide_integer_cpu_arguments_model_arguments_c10: values[53],
            divide_integer_cpu_arguments_model_arguments_c11: values[54],
            divide_integer_cpu_arguments_model_arguments_c20: values[55],
            divide_integer_cpu_arguments_model_arguments_minimum: values[56],
            divide_integer_memory_arguments_intercept: values[57],
            divide_integer_memory_arguments_minimum: values[58],
            divide_integer_memory_arguments_slope: values[59],
            encode_utf8_cpu_arguments_intercept: values[60],
            encode_utf8_cpu_arguments_slope: values[61],
            encode_utf8_memory_arguments_intercept: values[62],
            encode_utf8_memory_arguments_slope: values[63],
            equals_byte_string_cpu_arguments_constant: values[64],
            equals_byte_string_cpu_arguments_intercept: values[65],
            equals_byte_string_cpu_arguments_slope: values[66],
            equals_byte_string_memory_arguments: values[67],
            equals_data_cpu_arguments_intercept: values[68],
            equals_data_cpu_arguments_slope: values[69],
            equals_data_memory_arguments: values[70],
            equals_integer_cpu_arguments_intercept: values[71],
            equals_integer_cpu_arguments_slope: values[72],
            equals_integer_memory_arguments: values[73],
            equals_string_cpu_arguments_constant: values[74],
            equals_string_cpu_arguments_intercept: values[75],
            equals_string_cpu_arguments_slope: values[76],
            equals_string_memory_arguments: values[77],
            fst_pair_cpu_arguments: values[78],
            fst_pair_memory_arguments: values[79],
            head_list_cpu_arguments: values[80],
            head_list_memory_arguments: values[81],
            i_data_cpu_arguments: values[82],
            i_data_memory_arguments: values[83],
            if_then_else_cpu_arguments: values[84],
            if_then_else_memory_arguments: values[85],
            index_byte_string_cpu_arguments: values[86],
            index_byte_string_memory_arguments: values[87],
            length_of_byte_string_cpu_arguments: values[88],
            length_of_byte_string_memory_arguments: values[89],
            less_than_byte_string_cpu_arguments_intercept: values[90],
            less_than_byte_string_cpu_arguments_slope: values[91],
            less_than_byte_string_memory_arguments: values[92],
            less_than_equals_byte_string_cpu_arguments_intercept: values[93],
            less_than_equals_byte_string_cpu_arguments_slope: values[94],
            less_than_equals_byte_string_memory_arguments: values[95],
            less_than_equals_integer_cpu_arguments_intercept: values[96],
            less_than_equals_integer_cpu_arguments_slope: values[97],
            less_than_equals_integer_memory_arguments: values[98],
            less_than_integer_cpu_arguments_intercept: values[99],
            less_than_integer_cpu_arguments_slope: values[100],
            less_than_integer_memory_arguments: values[101],
            list_data_cpu_arguments: values[102],
            list_data_memory_arguments: values[103],
            map_data_cpu_arguments: values[104],
            map_data_memory_arguments: values[105],
            mk_cons_cpu_arguments: values[106],
            mk_cons_memory_arguments: values[107],
            mk_nil_data_cpu_arguments: values[108],
            mk_nil_data_memory_arguments: values[109],
            mk_nil_pair_data_cpu_arguments: values[110],
            mk_nil_pair_data_memory_arguments: values[111],
            mk_pair_data_cpu_arguments: values[112],
            mk_pair_data_memory_arguments: values[113],
            mod_integer_cpu_arguments_constant: values[114],
            mod_integer_cpu_arguments_model_arguments_c00: values[115],
            mod_integer_cpu_arguments_model_arguments_c01: values[116],
            mod_integer_cpu_arguments_model_arguments_c02: values[117],
            mod_integer_cpu_arguments_model_arguments_c10: values[118],
            mod_integer_cpu_arguments_model_arguments_c11: values[119],
            mod_integer_cpu_arguments_model_arguments_c20: values[120],
            mod_integer_cpu_arguments_model_arguments_minimum: values[121],
            mod_integer_memory_arguments_intercept: values[122],
            mod_integer_memory_arguments_minimum: values[123],
            mod_integer_memory_arguments_slope: values[124],
            multiply_integer_cpu_arguments_intercept: values[125],
            multiply_integer_cpu_arguments_slope: values[126],
            multiply_integer_memory_arguments_intercept: values[127],
            multiply_integer_memory_arguments_slope: values[128],
            null_list_cpu_arguments: values[129],
            null_list_memory_arguments: values[130],
            quotient_integer_cpu_arguments_constant: values[131],
            quotient_integer_cpu_arguments_model_arguments_c00: values[132],
            quotient_integer_cpu_arguments_model_arguments_c01: values[133],
            quotient_integer_cpu_arguments_model_arguments_c02: values[134],
            quotient_integer_cpu_arguments_model_arguments_c10: values[135],
            quotient_integer_cpu_arguments_model_arguments_c11: values[136],
            quotient_integer_cpu_arguments_model_arguments_c20: values[137],
            quotient_integer_cpu_arguments_model_arguments_minimum: values[138],
            quotient_integer_memory_arguments_intercept: values[139],
            quotient_integer_memory_arguments_minimum: values[140],
            quotient_integer_memory_arguments_slope: values[141],
            remainder_integer_cpu_arguments_constant: values[142],
            remainder_integer_cpu_arguments_model_arguments_c00: values[143],
            remainder_integer_cpu_arguments_model_arguments_c01: values[144],
            remainder_integer_cpu_arguments_model_arguments_c02: values[145],
            remainder_integer_cpu_arguments_model_arguments_c10: values[146],
            remainder_integer_cpu_arguments_model_arguments_c11: values[147],
            remainder_integer_cpu_arguments_model_arguments_c20: values[148],
            remainder_integer_cpu_arguments_model_arguments_minimum: values[149],
            remainder_integer_memory_arguments_intercept: values[150],
            remainder_integer_memory_arguments_minimum: values[151],
            remainder_integer_memory_arguments_slope: values[152],
            serialise_data_cpu_arguments_intercept: values[153],
            serialise_data_cpu_arguments_slope: values[154],
            serialise_data_memory_arguments_intercept: values[155],
            serialise_data_memory_arguments_slope: values[156],
            sha2_256_cpu_arguments_intercept: values[157],
            sha2_256_cpu_arguments_slope: values[158],
            sha2_256_memory_arguments: values[159],
            sha3_256_cpu_arguments_intercept: values[160],
            sha3_256_cpu_arguments_slope: values[161],
            sha3_256_memory_arguments: values[162],
            slice_byte_string_cpu_arguments_intercept: values[163],
            slice_byte_string_cpu_arguments_slope: values[164],
            slice_byte_string_memory_arguments_intercept: values[165],
            slice_byte_string_memory_arguments_slope: values[166],
            snd_pair_cpu_arguments: values[167],
            snd_pair_memory_arguments: values[168],
            subtract_integer_cpu_arguments_intercept: values[169],
            subtract_integer_cpu_arguments_slope: values[170],
            subtract_integer_memory_arguments_intercept: values[171],
            subtract_integer_memory_arguments_slope: values[172],
            tail_list_cpu_arguments: values[173],
            tail_list_memory_arguments: values[174],
            trace_cpu_arguments: values[175],
            trace_memory_arguments: values[176],
            un_b_data_cpu_arguments: values[177],
            un_b_data_memory_arguments: values[178],
            un_constr_data_cpu_arguments: values[179],
            un_constr_data_memory_arguments: values[180],
            un_i_data_cpu_arguments: values[181],
            un_i_data_memory_arguments: values[182],
            un_list_data_cpu_arguments: values[183],
            un_list_data_memory_arguments: values[184],
            un_map_data_cpu_arguments: values[185],
            un_map_data_memory_arguments: values[186],
            verify_ecdsa_secp256k1_signature_cpu_arguments: values[187],
            verify_ecdsa_secp256k1_signature_memory_arguments: values[188],
            verify_ed25519_signature_cpu_arguments_intercept: values[189],
            verify_ed25519_signature_cpu_arguments_slope: values[190],
            verify_ed25519_signature_memory_arguments: values[191],
            verify_schnorr_secp256k1_signature_cpu_arguments_intercept: values[192],
            verify_schnorr_secp256k1_signature_cpu_arguments_slope: values[193],
            verify_schnorr_secp256k1_signature_memory_arguments: values[194],
            cek_constr_cost_ex_budget_cpu: values[195],
            cek_constr_cost_ex_budget_memory: values[196],
        }
    }
}

pub async fn route(State(state): State<SharedState>) -> Result<Json<ProtocolParams>, StatusCode> {
    let tip = state
        .ledger
        .cursor()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let updates = state
        .ledger
        .get_pparams(tip.as_ref().map(|p| p.0).unwrap_or_default())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let updates: Vec<_> = updates
        .into_iter()
        .map(TryInto::try_into)
        .try_collect()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let summary = pparams::fold_with_hacks(&state.genesis, &updates, tip.as_ref().unwrap().0);
    let era = summary.era_for_slot(tip.as_ref().unwrap().0);
    let mapper = pallas::interop::utxorpc::Mapper::new(state.ledger.clone());
    let mapped = mapper.map_pparams(era.pparams.clone());

    let pparams = ProtocolParams {
        epoch: era.start.epoch,
        min_fee_a: mapped.min_fee_coefficient,
        min_fee_b: mapped.min_fee_constant,
        max_block_size: mapped.max_block_body_size,
        max_tx_size: mapped.max_tx_size,
        max_block_header_size: mapped.max_block_header_size,
        key_deposit: mapped.stake_key_deposit.to_string(),
        pool_deposit: mapped.pool_deposit.to_string(),
        n_opt: mapped.desired_number_of_pools,
        protocol_major_ver: mapped.protocol_version.clone().unwrap().major as u64,
        protocol_minor_ver: mapped.protocol_version.clone().unwrap().minor as u64,
        min_pool_cost: mapped.min_pool_cost.to_string(),
        cost_models: mapped.cost_models.clone().map(|cost_models| CostModels {
            plutus_v1: cost_models
                .plutus_v1
                .map(|v1| CostParametersV1::from(&v1.values)),
            plutus_v2: cost_models
                .plutus_v2
                .map(|v2| CostParametersV2::from(&v2.values)),
            plutus_v3: cost_models
                .plutus_v3
                .map(|v3| CostParametersV3::from(&v3.values)),
        }),
        cost_models_raw: mapped.cost_models.clone().map(|cost_models| CostModelsRaw {
            plutus_v1: cost_models.plutus_v1.map(|v1| v1.values),
            plutus_v2: cost_models.plutus_v2.map(|v2| v2.values),
            plutus_v3: cost_models.plutus_v3.map(|v3| v3.values),
        }),
        price_mem: match &mapped.prices {
            Some(x) => x
                .memory
                .as_ref()
                .map(|x| x.numerator as f64 / x.denominator as f64),
            None => None,
        },
        price_step: match &mapped.prices {
            Some(x) => x
                .steps
                .as_ref()
                .map(|x| x.numerator as f64 / x.denominator as f64),
            None => None,
        },
        coins_per_utxo_size: Some(mapped.coins_per_utxo_byte.to_string()),
        max_tx_ex_mem: mapped
            .max_execution_units_per_transaction
            .clone()
            .map(|units| units.memory.to_string()),
        max_tx_ex_steps: mapped
            .max_execution_units_per_transaction
            .clone()
            .map(|units| units.steps.to_string()),
        max_block_ex_mem: mapped
            .max_execution_units_per_block
            .clone()
            .map(|units| units.memory.to_string()),
        max_block_ex_steps: mapped
            .max_execution_units_per_block
            .clone()
            .map(|units| units.steps.to_string()),
        max_val_size: Some(mapped.max_value_size.to_string()),
        collateral_percent: Some(mapped.collateral_percentage),
        max_collateral_inputs: Some(mapped.max_collateral_inputs),
        min_fee_ref_script_cost_per_byte: mapped
            .min_fee_script_ref_cost_per_byte
            .as_ref()
            .map(|x| x.numerator as f64 / x.denominator as f64),
        e_max: mapped.pool_retirement_epoch_bound,
        a0: mapped.desired_number_of_pools as f64,
        rho: mapped
            .monetary_expansion
            .as_ref()
            .map(|x| x.numerator as f64 / x.denominator as f64)
            .unwrap_or_default(),
        tau: mapped
            .treasury_expansion
            .as_ref()
            .map(|x| x.numerator as f64 / x.denominator as f64)
            .unwrap_or_default(),
        ..Default::default()
    };

    Ok(Json(pparams))
}
