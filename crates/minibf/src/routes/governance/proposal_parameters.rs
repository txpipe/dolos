use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::proposal_parameters::ProposalParameters;
use dolos_cardano::model::{ProposalAction, ProposalKey, ProposalState};
use dolos_core::{Domain, StateStore as _};
use pallas::crypto::hash::Hash;

use crate::{
    error::Error,
    mapping::{
        governance::proposals::ProposalParametersBuilder, parse_gov_action_id, sorted_json,
        IntoModel,
    },
    Facade,
};

use super::is_gov_action;

/// The parameter change proposed by `tx` at action index `idx`.
///
/// Blockfrost joins the proposal against db-sync's `param_proposal` table, so
/// a proposal of any other kind has no row to return and is a 404 — unlike the
/// withdrawal listing beside it, which answers with an empty array.
fn read_parameters<D: Domain>(
    domain: &Facade<D>,
    tx: Hash<32>,
    idx: u32,
) -> Result<ProposalParameters, Error> {
    let key = ProposalKey::new(tx, idx);

    let state = domain
        .state()
        .read_entity_typed::<ProposalState>(&key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // A pre-Conway update proposal is a parameter change too, but db-sync
    // keeps those out of the governance table this endpoint reads.
    if !is_gov_action(&state) {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let ProposalAction::ParamChange(params) = state.action else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let model = ProposalParametersBuilder { tx, idx, params };

    Ok(model.into_model()?)
}

/// `GET /governance/proposals/{tx_hash}/{cert_index}/parameters`.
pub async fn proposal_parameters<D>(
    Path((tx_hash, cert_index)): Path<(String, String)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<serde_json::Value>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let cert_index = cert_index
        .parse::<u32>()
        .map_err(|_| Error::InvalidCertIndex)?;

    // Blockfrost matches the hash as text against db-sync, so a malformed one
    // finds nothing rather than failing the request.
    let tx = tx_hash
        .parse::<Hash<32>>()
        .map_err(|_| StatusCode::NOT_FOUND)?;

    Ok(sorted_json(read_parameters(&domain, tx, cert_index)?)?)
}

/// `GET /governance/proposals/{gov_action_id}/parameters`: the same change,
/// addressed by CIP-129 id instead of by tx hash and action index.
pub async fn proposal_parameters_by_gov_action<D>(
    Path(gov_action_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<serde_json::Value>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let (tx, idx) = parse_gov_action_id(&gov_action_id).map_err(|_| Error::InvalidGovActionId)?;

    Ok(sorted_json(read_parameters(&domain, tx, idx)?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::{bech32, bech32_gov_action},
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use itertools::Itertools;
    use pallas::ledger::primitives::{
        conway::{
            CostModels, DRepVotingThresholds, ExUnitPrices, GovAction, PoolVotingThresholds,
            ProtocolParamUpdate,
        },
        ExUnits, RationalNumber,
    };

    fn ratio(numerator: u64, denominator: u64) -> RationalNumber {
        RationalNumber {
            numerator,
            denominator,
        }
    }

    /// The scalar half of preview's `608037b7…e09b#0`, the widest parameter
    /// change on that network: every non-threshold field it sets, with the
    /// value Blockfrost returns for it.
    fn wide_update() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            minfee_a: Some(999),
            minfee_b: Some(9_999_999),
            max_block_body_size: Some(122_879),
            max_transaction_size: Some(32_768),
            max_block_header_size: Some(5_000),
            key_deposit: Some(5_000_000),
            pool_deposit: Some(250_000_000),
            maximum_epoch: Some(0),
            desired_number_of_stake_pools: Some(2_000),
            pool_pledge_influence: Some(ratio(1, 10)),
            expansion_rate: Some(ratio(5, 1_000)),
            treasury_growth_rate: Some(ratio(3, 10)),
            min_pool_cost: Some(500_000_000),
            ada_per_utxo_byte: Some(6_500),
            cost_models_for_script_languages: None,
            execution_costs: Some(ExUnitPrices {
                mem_price: ratio(2, 10),
                step_price: ratio(2, 10_000),
            }),
            max_tx_ex_units: Some(ExUnits {
                mem: 40_000_000,
                steps: 14_900_000_000,
            }),
            max_block_ex_units: Some(ExUnits {
                mem: 120_000_000,
                steps: 40_000_000_000,
            }),
            max_value_size: Some(12_287),
            collateral_percentage: Some(200),
            max_collateral_inputs: Some(999),
            pool_voting_thresholds: None,
            drep_voting_thresholds: None,
            min_committee_size: Some(10),
            committee_term_limit: Some(293),
            governance_action_validity_period: Some(1),
            governance_action_deposit: Some(1_000_000),
            drep_deposit: Some(99_999_000_000),
            drep_inactivity_period: Some(13),
            minfee_refscript_cost_per_byte: Some(ratio(999, 1)),
        }
    }

    fn empty_update() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            minfee_a: None,
            minfee_b: None,
            max_block_body_size: None,
            max_transaction_size: None,
            max_block_header_size: None,
            key_deposit: None,
            pool_deposit: None,
            maximum_epoch: None,
            desired_number_of_stake_pools: None,
            pool_pledge_influence: None,
            expansion_rate: None,
            treasury_growth_rate: None,
            min_pool_cost: None,
            ada_per_utxo_byte: None,
            cost_models_for_script_languages: None,
            execution_costs: None,
            max_tx_ex_units: None,
            max_block_ex_units: None,
            max_value_size: None,
            collateral_percentage: None,
            max_collateral_inputs: None,
            pool_voting_thresholds: None,
            drep_voting_thresholds: None,
            min_committee_size: None,
            committee_term_limit: None,
            governance_action_validity_period: None,
            governance_action_deposit: None,
            drep_deposit: None,
            drep_inactivity_period: None,
            minfee_refscript_cost_per_byte: None,
        }
    }

    /// The thresholds preview's `aa1fe93e…0f6b#0` (pool) and
    /// `1fb793f6…425b#0` (DRep) set, in one update so a single fixture pins
    /// both groups.
    fn thresholds_update() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            pool_voting_thresholds: Some(PoolVotingThresholds {
                motion_no_confidence: ratio(52, 100),
                committee_normal: ratio(52, 100),
                committee_no_confidence: ratio(52, 100),
                hard_fork_initiation: ratio(52, 100),
                security_voting_threshold: ratio(52, 100),
            }),
            drep_voting_thresholds: Some(DRepVotingThresholds {
                motion_no_confidence: ratio(68, 100),
                committee_normal: ratio(68, 100),
                committee_no_confidence: ratio(61, 100),
                update_constitution: ratio(76, 100),
                hard_fork_initiation: ratio(61, 100),
                pp_network_group: ratio(68, 100),
                pp_economic_group: ratio(68, 100),
                pp_technical_group: ratio(68, 100),
                pp_governance_group: ratio(76, 100),
                treasury_withdrawal: ratio(68, 100),
            }),
            ..empty_update()
        }
    }

    fn cost_models_update() -> ProtocolParamUpdate {
        ProtocolParamUpdate {
            cost_models_for_script_languages: Some(CostModels {
                plutus_v1: Some(vec![1, -2, 3]),
                plutus_v2: Some(vec![4, 5, -6]),
                plutus_v3: Some(vec![100_788, 420, 1, 1]),
                unknown: Default::default(),
            }),
            ..empty_update()
        }
    }

    /// One tx proposing four parameter changes and, at index 4, an info
    /// action — so the same fixture covers a proposal that names no
    /// parameters at all.
    fn parameters_app() -> TestApp {
        let change = |update| GovAction::ParameterChange(None, Box::new(update), None);

        TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![
                change(wide_update()),
                change(thresholds_update()),
                change(cost_models_update()),
                change(empty_update()),
                GovAction::Information,
            ]]],
            ..Default::default()
        })
    }

    async fn get_parameters(app: &TestApp, path: &str) -> ProposalParameters {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse proposal parameters")
    }

    /// The fields the response actually sets, as `(name, json)` pairs. Keeps
    /// the assertions to what a proposal names instead of spelling out the
    /// fifty-odd nulls around it.
    fn set_fields(model: &ProposalParameters) -> Vec<(String, serde_json::Value)> {
        let value = serde_json::to_value(&*model.parameters).expect("failed to serialize");
        let object = value.as_object().expect("parameters is an object").clone();

        object
            .into_iter()
            .filter(|(_, v)| !v.is_null())
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect()
    }

    #[tokio::test]
    async fn governance_proposal_parameters_happy_path() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/0/parameters")).await;

        assert_eq!(model.tx_hash, tx);
        assert_eq!(model.cert_index, 0);
        let parsed: Hash<32> = tx.parse().expect("failed to parse tx hash");
        assert_eq!(model.id, bech32_gov_action(&parsed, 0).unwrap());

        // the value Blockfrost returns for every field this change names
        let expected = serde_json::json!({
            "min_fee_a": 999,
            "min_fee_b": 9_999_999,
            "max_block_size": 122_879,
            "max_tx_size": 32_768,
            "max_block_header_size": 5_000,
            "key_deposit": "5000000",
            "pool_deposit": "250000000",
            "e_max": 0,
            "n_opt": 2_000,
            "a0": 0.1,
            "rho": 0.005,
            "tau": 0.3,
            "min_utxo": "6500",
            "min_pool_cost": "500000000",
            "price_mem": 0.2,
            "price_step": 0.0002,
            "max_tx_ex_mem": "40000000",
            "max_tx_ex_steps": "14900000000",
            "max_block_ex_mem": "120000000",
            "max_block_ex_steps": "40000000000",
            "max_val_size": "12287",
            "collateral_percent": 200,
            "max_collateral_inputs": 999,
            "coins_per_utxo_size": "6500",
            "coins_per_utxo_word": "6500",
            "committee_min_size": "10",
            "committee_max_term_length": "293",
            "gov_action_lifetime": "1",
            "gov_action_deposit": "1000000",
            "drep_deposit": "99999000000",
            "drep_activity": "13",
            "min_fee_ref_script_cost_per_byte": 999.0,
        });

        let expected: Vec<(String, serde_json::Value)> = expected
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect();

        assert_eq!(set_fields(&model), expected);
    }

    /// The two names Blockfrost renders one db-sync column under have to move
    /// together, in both pairs.
    #[tokio::test]
    async fn governance_proposal_parameters_duplicate_names_agree() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);

        let wide = get_parameters(&app, &format!("/governance/proposals/{tx}/0/parameters")).await;
        assert_eq!(
            wide.parameters.coins_per_utxo_size,
            wide.parameters.coins_per_utxo_word
        );
        assert_eq!(wide.parameters.coins_per_utxo_size.as_deref(), Some("6500"));

        let thresholds =
            get_parameters(&app, &format!("/governance/proposals/{tx}/1/parameters")).await;
        assert_eq!(
            thresholds.parameters.pvtpp_security_group,
            thresholds.parameters.pvt_p_p_security_group
        );
        assert_eq!(thresholds.parameters.pvtpp_security_group, Some(0.52));
    }

    #[tokio::test]
    async fn governance_proposal_parameters_thresholds() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/1/parameters")).await;

        let expected = serde_json::json!({
            "pvt_motion_no_confidence": 0.52,
            "pvt_committee_normal": 0.52,
            "pvt_committee_no_confidence": 0.52,
            "pvt_hard_fork_initiation": 0.52,
            "pvtpp_security_group": 0.52,
            "pvt_p_p_security_group": 0.52,
            "dvt_motion_no_confidence": 0.68,
            "dvt_committee_normal": 0.68,
            "dvt_committee_no_confidence": 0.61,
            "dvt_update_to_constitution": 0.76,
            "dvt_hard_fork_initiation": 0.61,
            "dvt_p_p_network_group": 0.68,
            "dvt_p_p_economic_group": 0.68,
            "dvt_p_p_technical_group": 0.68,
            "dvt_p_p_gov_group": 0.76,
            "dvt_treasury_withdrawal": 0.68,
        });

        let expected: Vec<(String, serde_json::Value)> = expected
            .as_object()
            .unwrap()
            .clone()
            .into_iter()
            .sorted_by(|a, b| a.0.cmp(&b.0))
            .collect();

        assert_eq!(set_fields(&model), expected);
    }

    /// Cost models come back as the raw operation-cost vectors, not the named
    /// map `/epochs/{n}/parameters` builds from the same data.
    #[tokio::test]
    async fn governance_proposal_parameters_cost_models_stay_raw() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/2/parameters")).await;

        let cost_models = model.parameters.cost_models.expect("cost models are set");
        assert_eq!(cost_models.len(), 3);
        assert_eq!(cost_models["PlutusV1"], serde_json::json!([1, -2, 3]));
        assert_eq!(cost_models["PlutusV2"], serde_json::json!([4, 5, -6]));
        assert_eq!(
            cost_models["PlutusV3"],
            serde_json::json!([100_788, 420, 1, 1])
        );
    }

    #[tokio::test]
    async fn governance_proposal_parameters_distinguish_absent_and_empty_cost_models() {
        let empty_models = CostModels {
            plutus_v1: None,
            plutus_v2: None,
            plutus_v3: None,
            unknown: Default::default(),
        };
        let updates = [
            empty_update(),
            ProtocolParamUpdate {
                cost_models_for_script_languages: Some(empty_models.clone()),
                ..empty_update()
            },
            ProtocolParamUpdate {
                max_block_body_size: Some(90_015),
                cost_models_for_script_languages: Some(empty_models.clone()),
                ..empty_update()
            },
            ProtocolParamUpdate {
                cost_models_for_script_languages: Some(CostModels {
                    plutus_v1: Some(vec![]),
                    ..empty_models
                }),
                ..empty_update()
            },
        ];
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![updates
                .into_iter()
                .map(|update| GovAction::ParameterChange(None, Box::new(update), None))
                .collect()]],
            ..Default::default()
        });
        let tx: Hash<32> = tx_hash_of_block(&app, 0).parse().unwrap();
        let expected = [
            serde_json::Value::Null,
            serde_json::json!({}),
            serde_json::json!({}),
            serde_json::json!({"PlutusV1": []}),
        ];

        for (idx, cost_models) in expected.into_iter().enumerate() {
            let id = bech32_gov_action(&tx, idx as u32).unwrap();
            let paths = [
                format!("/governance/proposals/{tx}/{idx}/parameters"),
                format!("/governance/proposals/{id}/parameters"),
            ];
            let mut responses = vec![];
            for path in paths {
                let (status, bytes) = app.get_bytes(&path).await;
                assert_eq!(status, StatusCode::OK, "{path}");
                let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(
                    value["parameters"].get("cost_models"),
                    Some(&cost_models),
                    "{path}"
                );
                if idx == 2 {
                    assert_eq!(value["parameters"]["max_block_size"], 90_015);
                }
                responses.push(bytes);
            }
            assert_eq!(responses[0], responses[1]);
        }
    }

    #[tokio::test]
    async fn governance_proposal_parameters_have_stable_bytes() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);
        let id = bech32_gov_action(&tx.parse().unwrap(), 2).unwrap();
        let paths = [
            format!("/governance/proposals/{tx}/2/parameters"),
            format!("/governance/proposals/{id}/parameters"),
        ];
        let (status, expected) = app.get_bytes(&paths[0]).await;
        assert_eq!(status, StatusCode::OK);

        let body = std::str::from_utf8(&expected).unwrap();
        assert!(body.find("\"PlutusV1\"").unwrap() < body.find("\"PlutusV2\"").unwrap());
        assert!(body.find("\"PlutusV2\"").unwrap() < body.find("\"PlutusV3\"").unwrap());

        for path in paths {
            for _ in 0..16 {
                let (status, actual) = app.get_bytes(&path).await;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(actual, expected, "response changed for {path}");
            }
        }
    }

    /// Ratios keep the precision the chain gave them.
    ///
    /// `/epochs/{n}/parameters` rounds the same parameters, and Blockfrost
    /// rounds there too — but this endpoint reads db-sync's `param_proposal`
    /// columns straight, so a third that cannot be written down exactly has
    /// to come back long. Preview's `4869ef5d…ff1a#0` sets tau to 1/6 and
    /// Blockfrost answers 0.16666666666666666.
    #[tokio::test]
    async fn governance_proposal_parameters_keep_full_precision() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![GovAction::ParameterChange(
                None,
                Box::new(ProtocolParamUpdate {
                    treasury_growth_rate: Some(ratio(1, 6)),
                    ..empty_update()
                }),
                None,
            )]]],
            ..Default::default()
        });

        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/0/parameters")).await;

        assert_eq!(model.parameters.tau, Some(1.0 / 6.0));

        // and it survives serialization as the long form, not as 0.167
        let value = serde_json::to_value(&*model.parameters).unwrap();
        assert_eq!(value["tau"].to_string(), "0.16666666666666666");
    }

    /// A change that names nothing still answers — with every field null.
    #[tokio::test]
    async fn governance_proposal_parameters_empty_change() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);
        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/3/parameters")).await;

        assert!(set_fields(&model).is_empty(), "{:?}", set_fields(&model));

        // `epoch` is serialized as an explicit null rather than dropped
        let value = serde_json::to_value(&*model.parameters).unwrap();
        assert_eq!(value["epoch"], serde_json::Value::Null);
        assert!(value.as_object().unwrap().contains_key("epoch"));
    }

    /// The model types fees, sizes and counts as `i32`, while a proposal can
    /// set them anywhere in the chain's range: a value past `i32::MAX` is a
    /// 500, not a parameter wrapped negative. Index 0 names a field the
    /// proposal model maps itself, index 1 one the shared mapping maps, and
    /// index 2 sits exactly on the limit to show it is the limit that fails.
    #[tokio::test]
    async fn governance_proposal_parameters_past_i32_are_500() {
        let change = |update| GovAction::ParameterChange(None, Box::new(update), None);

        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 1,
            txs_per_block: 1,
            gov_actions_by_block: vec![vec![vec![
                change(ProtocolParamUpdate {
                    minfee_b: Some(3_000_000_000),
                    ..empty_update()
                }),
                change(ProtocolParamUpdate {
                    collateral_percentage: Some(3_000_000_000),
                    ..empty_update()
                }),
                change(ProtocolParamUpdate {
                    minfee_b: Some(i32::MAX as u64),
                    ..empty_update()
                }),
            ]]],
            ..Default::default()
        });

        let tx = tx_hash_of_block(&app, 0);

        for idx in 0..2 {
            let path = format!("/governance/proposals/{tx}/{idx}/parameters");
            assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
        }

        let model = get_parameters(&app, &format!("/governance/proposals/{tx}/2/parameters")).await;
        assert_eq!(model.parameters.min_fee_b, Some(i32::MAX));
    }

    /// Unlike the withdrawal listing beside it, this one 404s rather than
    /// answering empty: Blockfrost inner-joins the proposal against
    /// `param_proposal`, so a proposal of another kind has no row.
    #[tokio::test]
    async fn governance_proposal_parameters_not_found() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);

        // index 4 of the same tx is the info action
        let path = format!("/governance/proposals/{tx}/4/parameters");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        // an index the tx never proposed at
        let path = format!("/governance/proposals/{tx}/9/parameters");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        let missing = hex::encode([0u8; 32]);
        let path = format!("/governance/proposals/{missing}/0/parameters");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        let path = "/governance/proposals/not-a-tx-hash/0/parameters";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;

        // a well-formed id for a proposal nobody made
        let id = bech32_gov_action(&Hash::from([0u8; 32]), 0).unwrap();
        let path = format!("/governance/proposals/{id}/parameters");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_proposal_parameters_by_gov_action_id() {
        let app = parameters_app();
        let tx: Hash<32> = tx_hash_of_block(&app, 0)
            .parse()
            .expect("failed to parse tx hash");

        let by_index =
            get_parameters(&app, &format!("/governance/proposals/{tx}/0/parameters")).await;

        let id = bech32_gov_action(&tx, 0).unwrap();
        let by_id = get_parameters(&app, &format!("/governance/proposals/{id}/parameters")).await;
        assert_eq!(by_id, by_index);

        // the bare-hash form explorers write for index 0 names the same
        // proposal as the one-byte form Blockfrost writes
        let minimal = bech32(bech32::Hrp::parse("gov_action").unwrap(), tx.as_slice()).unwrap();
        let by_minimal =
            get_parameters(&app, &format!("/governance/proposals/{minimal}/parameters")).await;
        assert_eq!(by_minimal, by_index);

        // an id past index 0 still resolves to its own action
        let id = bech32_gov_action(&tx, 2).unwrap();
        let by_id = get_parameters(&app, &format!("/governance/proposals/{id}/parameters")).await;
        assert_eq!(by_id.cert_index, 2);
        assert!(by_id.parameters.cost_models.is_some());
    }

    #[tokio::test]
    async fn governance_proposal_parameters_bad_request() {
        let app = parameters_app();
        let tx = tx_hash_of_block(&app, 0);

        // the cert index is the only path part that has to be a number
        let path = format!("/governance/proposals/{tx}/x/parameters");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;

        for id in ["not-bech32", &missing_drep()] {
            let path = format!("/governance/proposals/{id}/parameters");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn governance_proposal_parameters_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = format!(
            "/governance/proposals/{}/0/parameters",
            hex::encode([1u8; 32])
        );
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
