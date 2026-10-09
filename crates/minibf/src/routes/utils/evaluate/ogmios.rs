//! Renders an evaluation outcome in the JSON of Ogmios v5 (JSON-WSP) or Ogmios
//! v6 (JSON-RPC), the two formats that Blockfrost serves.

use dolos_cardano::validate::{RedeemerFailure, RedeemerReport};
use dolos_core::TxoRef;
use pallas::ledger::primitives::conway::{Language, RedeemerTag};
use serde_json::{json, Map, Value as Json};

use super::Outcome;

const SCRIPT_FAILURES: &str = "Some scripts of the transactions terminated with error(s).";

/// The Ogmios version whose format the response uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Version {
    V5,
    V6,
}

pub(super) fn render(outcome: &Outcome, version: Version, id: &str) -> Json {
    match version {
        Version::V5 => render_v5(outcome, id),
        Version::V6 => render_v6(outcome, id),
    }
}

fn render_v5(outcome: &Outcome, id: &str) -> Json {
    let result = match outcome {
        Outcome::InvalidPayload => {
            return fault_v5(
                "Invalid request: failed to decode payload from base64 or base16.".into(),
                id,
            )
        }
        Outcome::Deserialisation(error) => {
            return fault_v5(
                format!(
                    "Invalid request: Deserialisation failure while decoding serialised \
                     transaction. CBOR failed with error: {error}"
                ),
                id,
            )
        }
        Outcome::IncompatibleEra(era) => failure_v5(json!({ "IncompatibleEra": capitalize(era) })),
        Outcome::UnsupportedEra(era) => failure_v5(json!({
            "UnsupportedEra": { "reason": unsupported_era_reason(era) }
        })),
        Outcome::NodeTipTooOld(era) => failure_v5(json!({
            "NotEnoughSynced": {
                "minimumRequiredEra": "Alonzo",
                "currentNodeEra": capitalize(era),
            }
        })),
        Outcome::AdditionalUtxoOverlap(refs) => failure_v5(json!({
            "AdditionalUtxoOverlap": refs
                .iter()
                .map(|TxoRef(hash, index)| json!({ "txId": hash.to_string(), "index": index }))
                .collect::<Vec<_>>()
        })),
        Outcome::Evaluated(reports) => {
            if reports.iter().all(|report| report.result.is_ok()) {
                let budgets = reports
                    .iter()
                    .filter_map(|report| {
                        let units = report.result.as_ref().ok()?;
                        let budget = json!({ "memory": units.mem, "steps": units.steps });
                        Some((pointer_v5(report), budget))
                    })
                    .collect::<Map<_, _>>();

                json!({ "EvaluationResult": budgets })
            } else {
                // Blockfrost names a failed redeemer with its v6 purpose, for
                // example `withdraw:0`, also in the v5 format.
                let failures = reports
                    .iter()
                    .filter_map(|report| {
                        let failure = report.result.as_ref().err()?;
                        Some((pointer_v6(report), failure_entry_v5(report, failure)))
                    })
                    .collect::<Map<_, _>>();

                failure_v5(json!({ "ScriptFailures": failures }))
            }
        }
    };

    json!({
        "type": "jsonwsp/response",
        "version": "1.0",
        "servicename": "ogmios",
        "methodname": "EvaluateTx",
        "result": result,
        "reflection": { "id": id },
    })
}

fn fault_v5(message: String, id: &str) -> Json {
    json!({
        "type": "jsonwsp/fault",
        "version": "1.0",
        "servicename": "ogmios",
        "fault": { "code": "client", "string": message },
        "reflection": { "id": id },
    })
}

fn failure_v5(failure: Json) -> Json {
    json!({ "EvaluationFailure": failure })
}

/// The v5 failure entry of one redeemer. A failed script gets the Ogmios v5
/// `validatorFailed` entry, with the error and the traces.
fn failure_entry_v5(report: &RedeemerReport, failure: &RedeemerFailure) -> Json {
    match failure {
        RedeemerFailure::Extraneous => json!({ "extraRedeemers": [pointer_v6(report)] }),
        RedeemerFailure::MissingScript => {
            // Blockfrost keeps the v5 purpose only in this list, for example
            // `withdrawal:0`.
            json!({ "missingRequiredScripts": { "missing": [pointer_v5(report)] } })
        }
        RedeemerFailure::MissingDatum(hash) => {
            json!({ "missingRequiredDatums": { "missing": [hash] } })
        }
        RedeemerFailure::MissingCostModel(language) => {
            json!({ "noCostModelForLanguage": language_name(language) })
        }
        RedeemerFailure::UnknownInput(txo_ref) => {
            json!({ "CannotCreateEvaluationContext": { "reason": unknown_input_reason(txo_ref) } })
        }
        RedeemerFailure::Context(reason) => {
            json!({ "CannotCreateEvaluationContext": { "reason": reason } })
        }
        RedeemerFailure::Validation { error, traces } => {
            json!({ "validatorFailed": { "error": error, "traces": traces } })
        }
    }
}

fn render_v6(outcome: &Outcome, id: &str) -> Json {
    let response = match outcome {
        Outcome::InvalidPayload => Err(json!({
            "code": -32600,
            "message": "Invalid request: failed to decode payload from base64 or base16.",
        })),
        Outcome::Deserialisation(error) => Err(json!({
            "code": -32602,
            "message": "Invalid transaction; the transaction is not well-formed in any era. \
                        The field 'data' holds the error of the Conway decoder.",
            "data": { "conway": error },
        })),
        Outcome::IncompatibleEra(era) => Err(json!({
            "code": 3000,
            "message": "Trying to evaluate a transaction from an old era (prior to Alonzo).",
            "data": { "incompatibleEra": era },
        })),
        Outcome::UnsupportedEra(era) => Err(json!({
            "code": 3001,
            "message": unsupported_era_reason(era),
            "data": { "unsupportedEra": era },
        })),
        Outcome::NodeTipTooOld(era) => Err(json!({
            "code": 3003,
            "message": "The ledger is not yet in an era where scripts are enabled (Alonzo and \
                        beyond).",
            "data": { "currentNodeEra": era, "minimumRequiredEra": "alonzo" },
        })),
        Outcome::AdditionalUtxoOverlap(refs) => Err(json!({
            "code": 3002,
            "message": "Some user-provided additional UTxO entries overlap with those that \
                        exist in the ledger.",
            "data": {
                "overlappingOutputReferences": refs
                    .iter()
                    .map(|TxoRef(hash, index)| {
                        json!({ "transaction": { "id": hash.to_string() }, "index": index })
                    })
                    .collect::<Vec<_>>()
            },
        })),
        Outcome::Evaluated(reports) => {
            if reports.iter().all(|report| report.result.is_ok()) {
                Ok(reports
                    .iter()
                    .filter_map(|report| {
                        let units = report.result.as_ref().ok()?;
                        Some(json!({
                            "validator": validator_v6(report),
                            "budget": { "memory": units.mem, "cpu": units.steps },
                        }))
                    })
                    .collect::<Json>())
            } else {
                let failures = reports
                    .iter()
                    .filter_map(|report| {
                        let failure = report.result.as_ref().err()?;
                        Some(json!({
                            "validator": validator_v6(report),
                            "error": failure_entry_v6(report, failure),
                        }))
                    })
                    .collect::<Vec<_>>();

                Err(json!({ "code": 3010, "message": SCRIPT_FAILURES, "data": failures }))
            }
        }
    };

    let (field, body) = match response {
        Ok(result) => ("result", result),
        Err(error) => ("error", error),
    };

    json!({
        "jsonrpc": "2.0",
        "method": "evaluateTransaction",
        field: body,
        "id": id,
    })
}

fn failure_entry_v6(report: &RedeemerReport, failure: &RedeemerFailure) -> Json {
    match failure {
        RedeemerFailure::Extraneous => json!({
            "code": 3110,
            "message": "Extraneous (non-required) redeemers found in the transaction.",
            "data": { "extraneousRedeemers": [validator_v6(report)] },
        }),
        RedeemerFailure::MissingScript => json!({
            "code": 3011,
            "message": "An associated script witness is missing.",
            "data": { "missingScripts": [validator_v6(report)] },
        }),
        RedeemerFailure::MissingDatum(hash) => json!({
            "code": 3111,
            "message": "Some Plutus scripts are missing their associated datums.",
            "data": { "missingDatums": [hash] },
        }),
        RedeemerFailure::MissingCostModel(language) => json!({
            "code": 3115,
            "message": "The transaction uses a Plutus version that has no cost model yet.",
            "data": { "missingCostModels": [language_name(language)] },
        }),
        RedeemerFailure::UnknownInput(txo_ref) => context_error_v6(unknown_input_reason(txo_ref)),
        RedeemerFailure::Context(reason) => context_error_v6(reason.clone()),
        RedeemerFailure::Validation { error, traces } => json!({
            "code": 3012,
            "message": "Some of the scripts failed to evaluate to a positive outcome.",
            "data": { "validationError": error, "traces": traces },
        }),
    }
}

fn context_error_v6(reason: String) -> Json {
    json!({
        "code": 3004,
        "message": "Unable to create the evaluation context from the given transaction.",
        "data": { "reason": reason },
    })
}

fn unknown_input_reason(txo_ref: &TxoRef) -> String {
    format!("Unknown transaction input (missing from UTxO set): {txo_ref}")
}

fn unsupported_era_reason(era: &str) -> String {
    format!(
        "Couldn't evaluate the transaction in the current network era: the transaction is in \
         the {} format. Make sure its format is compatible with Conway.",
        capitalize(era)
    )
}

/// The v5 name of a redeemer, for example `spend:0` or `withdrawal:1`.
fn pointer_v5(report: &RedeemerReport) -> String {
    let purpose = match report.tag {
        RedeemerTag::Spend => "spend",
        RedeemerTag::Mint => "mint",
        RedeemerTag::Cert => "certificate",
        RedeemerTag::Reward => "withdrawal",
        RedeemerTag::Vote => "vote",
        RedeemerTag::Propose => "propose",
    };

    format!("{purpose}:{}", report.index)
}

/// The v6 name of a redeemer, for example `spend:0` or `withdraw:1`.
fn pointer_v6(report: &RedeemerReport) -> String {
    format!("{}:{}", purpose_v6(report.tag), report.index)
}

fn purpose_v6(tag: RedeemerTag) -> &'static str {
    match tag {
        RedeemerTag::Spend => "spend",
        RedeemerTag::Mint => "mint",
        RedeemerTag::Cert => "publish",
        RedeemerTag::Reward => "withdraw",
        RedeemerTag::Vote => "vote",
        RedeemerTag::Propose => "propose",
    }
}

fn validator_v6(report: &RedeemerReport) -> Json {
    json!({ "index": report.index, "purpose": purpose_v6(report.tag) })
}

fn language_name(language: &Language) -> &'static str {
    match language {
        Language::PlutusV1 => "plutus:v1",
        Language::PlutusV2 => "plutus:v2",
        Language::PlutusV3 => "plutus:v3",
    }
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();

    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use pallas::{crypto::hash::Hash, ledger::primitives::conway::ExUnits};

    use super::*;

    fn report(
        tag: RedeemerTag,
        index: u32,
        result: Result<ExUnits, RedeemerFailure>,
    ) -> RedeemerReport {
        RedeemerReport { tag, index, result }
    }

    fn units(mem: u64, steps: u64) -> Result<ExUnits, RedeemerFailure> {
        Ok(ExUnits { mem, steps })
    }

    fn v5_result(outcome: &Outcome) -> Json {
        let rendered = render(outcome, Version::V5, "id");
        assert_eq!(rendered["type"], "jsonwsp/response");
        assert_eq!(rendered["methodname"], "EvaluateTx");
        assert_eq!(rendered["reflection"], json!({ "id": "id" }));
        rendered["result"].clone()
    }

    fn v6_error(outcome: &Outcome) -> Json {
        let rendered = render(outcome, Version::V6, "id");
        assert_eq!(rendered["jsonrpc"], "2.0");
        assert_eq!(rendered["method"], "evaluateTransaction");
        assert_eq!(rendered["id"], "id");
        assert!(rendered.get("result").is_none());
        rendered["error"].clone()
    }

    #[test]
    fn renders_budgets() {
        let outcome = Outcome::Evaluated(vec![
            report(RedeemerTag::Spend, 0, units(1, 2)),
            report(RedeemerTag::Mint, 1, units(3, 4)),
            report(RedeemerTag::Cert, 2, units(5, 6)),
            report(RedeemerTag::Reward, 3, units(7, 8)),
        ]);

        assert_eq!(
            v5_result(&outcome),
            json!({ "EvaluationResult": {
                "spend:0": { "memory": 1, "steps": 2 },
                "mint:1": { "memory": 3, "steps": 4 },
                "certificate:2": { "memory": 5, "steps": 6 },
                "withdrawal:3": { "memory": 7, "steps": 8 },
            }})
        );

        assert_eq!(
            render(&outcome, Version::V6, "id"),
            json!({
                "jsonrpc": "2.0",
                "method": "evaluateTransaction",
                "result": [
                    { "validator": { "index": 0, "purpose": "spend" }, "budget": { "memory": 1, "cpu": 2 } },
                    { "validator": { "index": 1, "purpose": "mint" }, "budget": { "memory": 3, "cpu": 4 } },
                    { "validator": { "index": 2, "purpose": "publish" }, "budget": { "memory": 5, "cpu": 6 } },
                    { "validator": { "index": 3, "purpose": "withdraw" }, "budget": { "memory": 7, "cpu": 8 } },
                ],
                "id": "id",
            })
        );
    }

    #[test]
    fn renders_an_empty_budget_map() {
        let outcome = Outcome::Evaluated(vec![]);

        assert_eq!(v5_result(&outcome), json!({ "EvaluationResult": {} }));
        assert_eq!(render(&outcome, Version::V6, "id")["result"], json!([]));
    }

    #[test]
    fn renders_only_the_failed_redeemers() {
        let unknown = TxoRef(Hash::from([0xab; 32]), 1);

        let outcome = Outcome::Evaluated(vec![
            report(RedeemerTag::Spend, 0, Err(RedeemerFailure::Extraneous)),
            report(RedeemerTag::Spend, 1, Err(RedeemerFailure::MissingScript)),
            report(
                RedeemerTag::Spend,
                2,
                Err(RedeemerFailure::MissingDatum("cd".into())),
            ),
            report(
                RedeemerTag::Mint,
                0,
                Err(RedeemerFailure::UnknownInput(unknown)),
            ),
            report(
                RedeemerTag::Mint,
                1,
                Err(RedeemerFailure::Context("no context".into())),
            ),
            report(
                RedeemerTag::Reward,
                0,
                Err(RedeemerFailure::Validation {
                    error: "boom".into(),
                    traces: vec!["trace".into()],
                }),
            ),
            report(RedeemerTag::Vote, 0, units(1, 1)),
            report(
                RedeemerTag::Propose,
                0,
                Err(RedeemerFailure::MissingCostModel(Language::PlutusV3)),
            ),
        ]);

        let missing_input = format!(
            "Unknown transaction input (missing from UTxO set): {}#1",
            "ab".repeat(32)
        );

        assert_eq!(
            v5_result(&outcome),
            json!({ "EvaluationFailure": { "ScriptFailures": {
                "spend:0": { "extraRedeemers": ["spend:0"] },
                "spend:1": { "missingRequiredScripts": { "missing": ["spend:1"] } },
                "spend:2": { "missingRequiredDatums": { "missing": ["cd"] } },
                "mint:0": { "CannotCreateEvaluationContext": { "reason": missing_input } },
                "mint:1": { "CannotCreateEvaluationContext": { "reason": "no context" } },
                "withdraw:0": { "validatorFailed": { "error": "boom", "traces": ["trace"] } },
                "propose:0": { "noCostModelForLanguage": "plutus:v3" },
            }}})
        );

        let error = v6_error(&outcome);
        assert_eq!(error["code"], 3010);

        let codes = error["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| (entry["validator"].clone(), entry["error"]["code"].clone()))
            .collect::<Vec<_>>();

        assert_eq!(
            codes,
            vec![
                (json!({ "index": 0, "purpose": "spend" }), json!(3110)),
                (json!({ "index": 1, "purpose": "spend" }), json!(3011)),
                (json!({ "index": 2, "purpose": "spend" }), json!(3111)),
                (json!({ "index": 0, "purpose": "mint" }), json!(3004)),
                (json!({ "index": 1, "purpose": "mint" }), json!(3004)),
                (json!({ "index": 0, "purpose": "withdraw" }), json!(3012)),
                (json!({ "index": 0, "purpose": "propose" }), json!(3115)),
            ]
        );

        assert_eq!(
            error["data"][3]["error"]["data"],
            json!({ "reason": missing_input })
        );
        assert_eq!(
            error["data"][5]["error"]["data"],
            json!({ "validationError": "boom", "traces": ["trace"] })
        );
    }

    #[test]
    fn names_failures_like_blockfrost() {
        let outcome = Outcome::Evaluated(vec![
            report(RedeemerTag::Cert, 0, Err(RedeemerFailure::Extraneous)),
            report(RedeemerTag::Cert, 1, Err(RedeemerFailure::MissingScript)),
            report(RedeemerTag::Reward, 0, Err(RedeemerFailure::Extraneous)),
            report(RedeemerTag::Reward, 1, Err(RedeemerFailure::MissingScript)),
        ]);

        assert_eq!(
            v5_result(&outcome),
            json!({ "EvaluationFailure": { "ScriptFailures": {
                "publish:0": { "extraRedeemers": ["publish:0"] },
                "publish:1": { "missingRequiredScripts": { "missing": ["certificate:1"] } },
                "withdraw:0": { "extraRedeemers": ["withdraw:0"] },
                "withdraw:1": { "missingRequiredScripts": { "missing": ["withdrawal:1"] } },
            }}})
        );
    }

    #[test]
    fn renders_era_failures() {
        assert_eq!(
            v5_result(&Outcome::IncompatibleEra("mary")),
            json!({ "EvaluationFailure": { "IncompatibleEra": "Mary" } })
        );
        assert_eq!(
            v6_error(&Outcome::IncompatibleEra("mary"))["data"],
            json!({ "incompatibleEra": "mary" })
        );

        let reason = &v5_result(&Outcome::UnsupportedEra("babbage"))["EvaluationFailure"]
            ["UnsupportedEra"]["reason"];
        assert!(reason.as_str().unwrap().contains("Babbage"));
        assert_eq!(
            v6_error(&Outcome::UnsupportedEra("babbage"))["data"],
            json!({ "unsupportedEra": "babbage" })
        );

        assert_eq!(
            v5_result(&Outcome::NodeTipTooOld("mary")),
            json!({ "EvaluationFailure": { "NotEnoughSynced": {
                "minimumRequiredEra": "Alonzo",
                "currentNodeEra": "Mary",
            }}})
        );
        assert_eq!(
            v6_error(&Outcome::NodeTipTooOld("mary"))["data"],
            json!({ "currentNodeEra": "mary", "minimumRequiredEra": "alonzo" })
        );
    }

    #[test]
    fn renders_overlapping_utxos() {
        let outcome = Outcome::AdditionalUtxoOverlap(vec![TxoRef(Hash::from([0x01; 32]), 2)]);

        assert_eq!(
            v5_result(&outcome),
            json!({ "EvaluationFailure": { "AdditionalUtxoOverlap": [
                { "txId": "01".repeat(32), "index": 2 }
            ]}})
        );

        assert_eq!(
            v6_error(&outcome)["data"],
            json!({ "overlappingOutputReferences": [
                { "transaction": { "id": "01".repeat(32) }, "index": 2 }
            ]})
        );
    }

    #[test]
    fn renders_faults() {
        assert_eq!(
            render(&Outcome::InvalidPayload, Version::V5, "id"),
            json!({
                "type": "jsonwsp/fault",
                "version": "1.0",
                "servicename": "ogmios",
                "fault": {
                    "code": "client",
                    "string": "Invalid request: failed to decode payload from base64 or base16.",
                },
                "reflection": { "id": "id" },
            })
        );
        assert_eq!(v6_error(&Outcome::InvalidPayload)["code"], -32600);

        let outcome = Outcome::Deserialisation("bad tag".into());
        let fault = render(&outcome, Version::V5, "id");
        assert_eq!(fault["type"], "jsonwsp/fault");
        assert_eq!(
            fault["fault"]["string"],
            "Invalid request: Deserialisation failure while decoding serialised transaction. \
             CBOR failed with error: bad tag"
        );

        let error = v6_error(&outcome);
        assert_eq!(error["code"], -32602);
        assert_eq!(error["data"], json!({ "conway": "bad tag" }));
    }
}
