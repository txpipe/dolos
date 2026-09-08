//! The `governance_description` JSON of a proposal.
//!
//! db-sync stores the cardano-ledger Aeson encoding of the submitted
//! `GovAction` and Blockfrost copies it verbatim, so every tag, field name
//! and number rendering here follows the ledger JSON instances.

use std::collections::HashMap;

use axum::http::StatusCode;
use pallas::ledger::{
    addresses::{Address, Network, StakePayload},
    primitives::{
        conway::{GovAction, GovActionId, ProtocolParamUpdate},
        RationalNumber, StakeCredential,
    },
};
use serde_json::{json, Value};

fn rational_json(x: &RationalNumber) -> Value {
    json!({ "numerator": x.numerator, "denominator": x.denominator })
}

fn credential_json(cred: &StakeCredential) -> Value {
    match cred {
        StakeCredential::AddrKeyhash(x) => json!({ "keyHash": hex::encode(x) }),
        StakeCredential::ScriptHash(x) => json!({ "scriptHash": hex::encode(x) }),
    }
}

/// Credentials used as JSON map keys follow the ledger `ToJSONKey` text form.
fn credential_key(cred: &StakeCredential) -> String {
    match cred {
        StakeCredential::AddrKeyhash(x) => format!("keyHash-{}", hex::encode(x)),
        StakeCredential::ScriptHash(x) => format!("scriptHash-{}", hex::encode(x)),
    }
}

fn gov_action_id_json(id: &GovActionId) -> Value {
    json!({
        "txId": hex::encode(id.transaction_id),
        "govActionIx": id.action_index,
    })
}

/// Render a ratio like the ledger pparams JSON: a plain number when the
/// fraction has a terminating decimal form, else a numerator/denominator
/// object (cf. cardano-api `toRationalJSON`).
fn ledger_ratio_json(x: &RationalNumber) -> Value {
    fn gcd(a: u64, b: u64) -> u64 {
        if b == 0 {
            a
        } else {
            gcd(b, a % b)
        }
    }

    if x.denominator == 0 {
        return rational_json(x);
    }

    let mut d = x.denominator / gcd(x.numerator, x.denominator);

    while d.is_multiple_of(2) {
        d /= 2;
    }

    while d.is_multiple_of(5) {
        d /= 5;
    }

    if d != 1 {
        return rational_json(x);
    }

    serde_json::Number::from_f64(x.numerator as f64 / x.denominator as f64)
        .map(Value::Number)
        .unwrap_or_else(|| rational_json(x))
}

fn reward_account_json(account: &[u8]) -> Result<Value, StatusCode> {
    let address = Address::from_bytes(account).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let Address::Stake(address) = address else {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    };

    let network = match address.network() {
        Network::Mainnet => "Mainnet",
        _ => "Testnet",
    };

    let credential = match address.payload() {
        StakePayload::Stake(x) => json!({ "keyHash": hex::encode(x) }),
        StakePayload::Script(x) => json!({ "scriptHash": hex::encode(x) }),
    };

    Ok(json!({ "network": network, "credential": credential }))
}

/// Render the changed params with the ledger conway `PParamsUpdate` JSON
/// names (cf. the `pparams-update.json` golden file in cardano-ledger).
fn pparams_update_json(update: &ProtocolParamUpdate) -> Value {
    let mut out = serde_json::Map::new();

    if let Some(x) = update.minfee_a {
        out.insert("txFeePerByte".into(), json!(x));
    }
    if let Some(x) = update.minfee_b {
        out.insert("txFeeFixed".into(), json!(x));
    }
    if let Some(x) = update.max_block_body_size {
        out.insert("maxBlockBodySize".into(), json!(x));
    }
    if let Some(x) = update.max_transaction_size {
        out.insert("maxTxSize".into(), json!(x));
    }
    if let Some(x) = update.max_block_header_size {
        out.insert("maxBlockHeaderSize".into(), json!(x));
    }
    if let Some(x) = update.key_deposit {
        out.insert("stakeAddressDeposit".into(), json!(x));
    }
    if let Some(x) = update.pool_deposit {
        out.insert("stakePoolDeposit".into(), json!(x));
    }
    if let Some(x) = update.maximum_epoch {
        out.insert("poolRetireMaxEpoch".into(), json!(x));
    }
    if let Some(x) = update.desired_number_of_stake_pools {
        out.insert("stakePoolTargetNum".into(), json!(x));
    }
    if let Some(x) = &update.pool_pledge_influence {
        out.insert("poolPledgeInfluence".into(), ledger_ratio_json(x));
    }
    if let Some(x) = &update.expansion_rate {
        out.insert("monetaryExpansion".into(), ledger_ratio_json(x));
    }
    if let Some(x) = &update.treasury_growth_rate {
        out.insert("treasuryCut".into(), ledger_ratio_json(x));
    }
    if let Some(x) = update.min_pool_cost {
        out.insert("minPoolCost".into(), json!(x));
    }
    if let Some(x) = update.ada_per_utxo_byte {
        out.insert("utxoCostPerByte".into(), json!(x));
    }
    if let Some(x) = &update.cost_models_for_script_languages {
        let mut cost_models = serde_json::Map::new();

        if let Some(v1) = &x.plutus_v1 {
            cost_models.insert("PlutusV1".into(), json!(v1));
        }
        if let Some(v2) = &x.plutus_v2 {
            cost_models.insert("PlutusV2".into(), json!(v2));
        }
        if let Some(v3) = &x.plutus_v3 {
            cost_models.insert("PlutusV3".into(), json!(v3));
        }

        out.insert("costModels".into(), Value::Object(cost_models));
    }
    if let Some(x) = &update.execution_costs {
        out.insert(
            "executionUnitPrices".into(),
            json!({
                "priceMemory": ledger_ratio_json(&x.mem_price),
                "priceSteps": ledger_ratio_json(&x.step_price),
            }),
        );
    }
    if let Some(x) = &update.max_tx_ex_units {
        out.insert(
            "maxTxExecutionUnits".into(),
            json!({ "memory": x.mem, "steps": x.steps }),
        );
    }
    if let Some(x) = &update.max_block_ex_units {
        out.insert(
            "maxBlockExecutionUnits".into(),
            json!({ "memory": x.mem, "steps": x.steps }),
        );
    }
    if let Some(x) = update.max_value_size {
        out.insert("maxValueSize".into(), json!(x));
    }
    if let Some(x) = update.collateral_percentage {
        out.insert("collateralPercentage".into(), json!(x));
    }
    if let Some(x) = update.max_collateral_inputs {
        out.insert("maxCollateralInputs".into(), json!(x));
    }
    if let Some(x) = &update.pool_voting_thresholds {
        out.insert(
            "poolVotingThresholds".into(),
            json!({
                "motionNoConfidence": ledger_ratio_json(&x.motion_no_confidence),
                "committeeNormal": ledger_ratio_json(&x.committee_normal),
                "committeeNoConfidence": ledger_ratio_json(&x.committee_no_confidence),
                "hardForkInitiation": ledger_ratio_json(&x.hard_fork_initiation),
                "ppSecurityGroup": ledger_ratio_json(&x.security_voting_threshold),
            }),
        );
    }
    if let Some(x) = &update.drep_voting_thresholds {
        out.insert(
            "dRepVotingThresholds".into(),
            json!({
                "motionNoConfidence": ledger_ratio_json(&x.motion_no_confidence),
                "committeeNormal": ledger_ratio_json(&x.committee_normal),
                "committeeNoConfidence": ledger_ratio_json(&x.committee_no_confidence),
                "updateToConstitution": ledger_ratio_json(&x.update_constitution),
                "hardForkInitiation": ledger_ratio_json(&x.hard_fork_initiation),
                "ppNetworkGroup": ledger_ratio_json(&x.pp_network_group),
                "ppEconomicGroup": ledger_ratio_json(&x.pp_economic_group),
                "ppTechnicalGroup": ledger_ratio_json(&x.pp_technical_group),
                "ppGovGroup": ledger_ratio_json(&x.pp_governance_group),
                "treasuryWithdrawal": ledger_ratio_json(&x.treasury_withdrawal),
            }),
        );
    }
    if let Some(x) = update.min_committee_size {
        out.insert("committeeMinSize".into(), json!(x));
    }
    if let Some(x) = update.committee_term_limit {
        out.insert("committeeMaxTermLength".into(), json!(x));
    }
    if let Some(x) = update.governance_action_validity_period {
        out.insert("govActionLifetime".into(), json!(x));
    }
    if let Some(x) = update.governance_action_deposit {
        out.insert("govActionDeposit".into(), json!(x));
    }
    if let Some(x) = update.drep_deposit {
        out.insert("dRepDeposit".into(), json!(x));
    }
    if let Some(x) = update.drep_inactivity_period {
        out.insert("dRepActivity".into(), json!(x));
    }
    if let Some(x) = &update.minfee_refscript_cost_per_byte {
        out.insert("minFeeRefScriptCostPerByte".into(), ledger_ratio_json(x));
    }

    Value::Object(out)
}

/// Build the `governance_description` object from the submitted action.
/// db-sync stores the cardano-ledger Aeson encoding of the `GovAction`, so
/// tags and field names follow the ledger JSON instances.
pub(super) fn description_json(action: &GovAction) -> Result<HashMap<String, Value>, StatusCode> {
    fn parent_json(parent: &Option<GovActionId>) -> Value {
        parent
            .as_ref()
            .map(gov_action_id_json)
            .unwrap_or(Value::Null)
    }

    let (tag, contents) = match action {
        GovAction::ParameterChange(parent, update, policy) => (
            "ParameterChange",
            Some(json!([
                parent_json(parent),
                pparams_update_json(update),
                policy.as_ref().map(hex::encode),
            ])),
        ),
        GovAction::HardForkInitiation(parent, (major, minor)) => (
            "HardForkInitiation",
            Some(json!([parent_json(parent), { "major": major, "minor": minor }])),
        ),
        GovAction::TreasuryWithdrawals(withdrawals, policy) => {
            let withdrawals = withdrawals
                .iter()
                .map(|(account, coin)| Ok(json!([reward_account_json(account)?, coin])))
                .collect::<Result<Vec<_>, StatusCode>>()?;

            (
                "TreasuryWithdrawals",
                Some(json!([withdrawals, policy.as_ref().map(hex::encode)])),
            )
        }
        GovAction::NoConfidence(parent) => ("NoConfidence", Some(parent_json(parent))),
        GovAction::UpdateCommittee(parent, to_remove, to_add, threshold) => {
            let removed: Vec<Value> = to_remove.iter().map(credential_json).collect();

            let added: serde_json::Map<String, Value> = to_add
                .iter()
                .map(|(cred, epoch)| (credential_key(cred), json!(epoch)))
                .collect();

            (
                "UpdateCommittee",
                Some(json!([
                    parent_json(parent),
                    removed,
                    added,
                    rational_json(threshold),
                ])),
            )
        }
        GovAction::NewConstitution(parent, constitution) => (
            "NewConstitution",
            Some(json!([
                parent_json(parent),
                {
                    "anchor": {
                        "url": constitution.anchor.url,
                        "dataHash": hex::encode(constitution.anchor.content_hash),
                    },
                    "script": constitution.guardrail_script.as_ref().map(hex::encode),
                },
            ])),
        ),
        GovAction::Information => ("InfoAction", None),
    };

    let mut out = HashMap::from([("tag".to_string(), json!(tag))]);

    if let Some(contents) = contents {
        out.insert("contents".to_string(), contents);
    }

    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ledger_ratio_renders_terminating_fractions_as_numbers() {
        let half = RationalNumber {
            numerator: 1,
            denominator: 2,
        };
        assert_eq!(ledger_ratio_json(&half), serde_json::json!(0.5));

        let repeating = RationalNumber {
            numerator: 7,
            denominator: 19,
        };
        assert_eq!(
            ledger_ratio_json(&repeating),
            serde_json::json!({ "numerator": 7, "denominator": 19 })
        );
    }

    #[test]
    fn description_uses_ledger_pparams_update_names() {
        let update = ProtocolParamUpdate {
            minfee_a: None,
            minfee_b: None,
            max_block_body_size: None,
            max_transaction_size: None,
            max_block_header_size: None,
            key_deposit: None,
            pool_deposit: None,
            maximum_epoch: None,
            desired_number_of_stake_pools: Some(600),
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
        };

        let action = GovAction::ParameterChange(
            Some(GovActionId {
                transaction_id: [0x1fu8; 32].into(),
                action_index: 0,
            }),
            Box::new(update),
            Some([0xfau8; 28].into()),
        );

        let description = description_json(&action).expect("failed to build description");

        assert_eq!(description["tag"], serde_json::json!("ParameterChange"));
        assert_eq!(
            description["contents"],
            serde_json::json!([
                { "txId": hex::encode([0x1fu8; 32]), "govActionIx": 0 },
                { "stakePoolTargetNum": 600 },
                hex::encode([0xfau8; 28]),
            ])
        );
    }
}
