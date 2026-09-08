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

/// Decimals with this many significant digits or fewer round-trip through
/// f64 exactly, so the number form reproduces the ledger's digits.
const MAX_FAITHFUL_DIGITS: u32 = 15;

/// Render a bounded ratio like the ledger JSON does: a plain number when
/// the fraction terminates, else a numerator/denominator object in reduced
/// form — the ledger's `Rational` normalizes on construction (cf. the
/// `BoundedRatio` `ToJSON` instance in cardano-ledger `BaseTypes`).
///
/// The ledger prints the exact decimal up to 19 digits. A JSON number here
/// goes through f64, so the number form stops at [`MAX_FAITHFUL_DIGITS`]
/// and anything longer renders as the fraction object — never as rounded
/// digits. No plausible on-chain ratio reaches that.
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

    let g = gcd(x.numerator, x.denominator);
    let n = x.numerator / g;
    let d = x.denominator / g;

    // `decimals` over-counts a denominator holding both 2s and 5s, which
    // only makes the faithfulness cut stricter.
    let mut remainder = d;
    let mut decimals = 0u32;
    while remainder.is_multiple_of(2) || remainder.is_multiple_of(5) {
        remainder /= if remainder.is_multiple_of(2) { 2 } else { 5 };
        decimals += 1;
    }

    if remainder != 1 || decimals > MAX_FAITHFUL_DIGITS {
        return json!({ "numerator": n, "denominator": d });
    }

    // `scaled` holds every significant digit of the terminating decimal:
    // n / d == scaled / scale. u128 holds the worst case (u64 times 10^15).
    let scale = 10u128.pow(decimals);
    let scaled = n as u128 * scale / d as u128;

    if scaled >= 10u128.pow(MAX_FAITHFUL_DIGITS) {
        return json!({ "numerator": n, "denominator": d });
    }

    serde_json::Number::from_f64(scaled as f64 / scale as f64)
        .map(Value::Number)
        .unwrap_or_else(|| json!({ "numerator": n, "denominator": d }))
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

        // The ledger keeps cost models of languages it does not know under
        // an "Unknown" object keyed by the numeric language tag (cf. the
        // conway `pparams-update.json` golden).
        if !x.unknown.is_empty() {
            let unknown: serde_json::Map<String, Value> = x
                .unknown
                .iter()
                .map(|(language, model)| (language.to_string(), json!(model)))
                .collect();

            cost_models.insert("Unknown".into(), Value::Object(unknown));
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
                    ledger_ratio_json(threshold),
                ])),
            )
        }
        GovAction::NewConstitution(parent, constitution) => {
            let mut body = serde_json::Map::new();

            body.insert(
                "anchor".into(),
                json!({
                    "url": constitution.anchor.url,
                    "dataHash": hex::encode(constitution.anchor.content_hash),
                }),
            );

            // The ledger omits the key entirely when there is no guardrail
            // script, never emitting a null.
            if let Some(script) = &constitution.guardrail_script {
                body.insert("script".into(), json!(hex::encode(script)));
            }

            ("NewConstitution", Some(json!([parent_json(parent), body])))
        }
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
    use std::collections::BTreeMap;

    use pallas::codec::utils::Set;
    use pallas::ledger::primitives::conway::{Anchor, Constitution, CostModels};

    use super::*;

    fn empty_pparams_update() -> ProtocolParamUpdate {
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

        // terminating, but past the faithful-digit cut
        let past_cap = RationalNumber {
            numerator: 1,
            denominator: 2u64.pow(20),
        };
        assert_eq!(
            ledger_ratio_json(&past_cap),
            serde_json::json!({ "numerator": 1, "denominator": 2u64.pow(20) })
        );

        // past the faithful-digit cut the reduced fraction form beats
        // digits that could round through f64
        let past_faithful = RationalNumber {
            numerator: 9_007_199_254_740_993,
            denominator: 10_000_000_000_000_000,
        };
        assert_eq!(
            ledger_ratio_json(&past_faithful),
            serde_json::json!({
                "numerator": 9_007_199_254_740_993u64,
                "denominator": 10_000_000_000_000_000u64,
            })
        );

        // the fraction fallback reduces, like the ledger's Rational does
        let unreduced = RationalNumber {
            numerator: 2,
            denominator: 6,
        };
        assert_eq!(
            ledger_ratio_json(&unreduced),
            serde_json::json!({ "numerator": 1, "denominator": 3 })
        );
    }

    #[test]
    fn description_matches_ledger_optional_encodings() {
        // the committee threshold follows the bounded-ratio number form
        let committee = GovAction::UpdateCommittee(
            None,
            Set::from(vec![]),
            BTreeMap::from([(StakeCredential::AddrKeyhash([3u8; 28].into()), 700u64)]),
            RationalNumber {
                numerator: 3,
                denominator: 5,
            },
        );
        let description = description_json(&committee).expect("failed to build description");

        let mut added = serde_json::Map::new();
        added.insert(format!("keyHash-{}", hex::encode([3u8; 28])), json!(700));

        assert_eq!(
            description["contents"],
            serde_json::json!([Value::Null, [], added, 0.6])
        );

        // a constitution without a guardrail omits the script key entirely
        let constitution = GovAction::NewConstitution(
            None,
            Constitution {
                anchor: Anchor {
                    url: "https://example.com".into(),
                    content_hash: [9u8; 32].into(),
                },
                guardrail_script: None,
            },
        );
        let description = description_json(&constitution).expect("failed to build description");
        assert_eq!(
            description["contents"],
            serde_json::json!([
                Value::Null,
                {
                    "anchor": {
                        "url": "https://example.com",
                        "dataHash": hex::encode([9u8; 32]),
                    },
                },
            ])
        );
    }

    #[test]
    fn cost_models_keep_unknown_languages() {
        let cost_models = CostModels {
            plutus_v1: None,
            plutus_v2: None,
            plutus_v3: Some(vec![1, 2, 3]),
            unknown: BTreeMap::from([(10u64, vec![7, 7])]),
        };

        let update = ProtocolParamUpdate {
            cost_models_for_script_languages: Some(cost_models),
            ..empty_pparams_update()
        };

        let rendered = pparams_update_json(&update);
        assert_eq!(
            rendered["costModels"],
            serde_json::json!({
                "PlutusV3": [1, 2, 3],
                "Unknown": { "10": [7, 7] },
            })
        );
    }

    #[test]
    fn description_uses_ledger_pparams_update_names() {
        let update = ProtocolParamUpdate {
            desired_number_of_stake_pools: Some(600),
            ..empty_pparams_update()
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
