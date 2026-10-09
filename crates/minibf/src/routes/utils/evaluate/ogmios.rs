//! Calls Ogmios v6 `evaluateTransaction` and converts its response to the
//! Ogmios v5 format that Blockfrost serves by default.

use std::{sync::LazyLock, time::Duration};

use serde_json::{json, Map, Value as Json};

/// The longest time that one Ogmios request can take.
const TIMEOUT: Duration = Duration::from_secs(30);

const INVALID_PAYLOAD: &str = "Invalid request: failed to decode payload from base64 or base16.";

// Blockfrost answers every Ogmios deserialisation failure with this text.
const DESERIALISATION_FAILURE: &str = "Invalid request: Deserialisation failure while decoding \
                                       serialised transaction. CBOR failed with error: \
                                       DeserialiseFailure 0 \"expected tag\".";

const ERAS: [&str; 7] = [
    "byron", "shelley", "allegra", "mary", "alonzo", "babbage", "conway",
];

static CLIENT: LazyLock<Result<reqwest::Client, reqwest::Error>> =
    LazyLock::new(|| reqwest::Client::builder().timeout(TIMEOUT).build());

/// Sends a transaction in base16 and its additional UTxO entries to Ogmios.
/// Returns the JSON-RPC response object, or why there is none.
pub(super) async fn evaluate(
    url: &str,
    cbor: &str,
    additional_utxo: Vec<Json>,
) -> Result<Json, String> {
    let mut params = json!({ "transaction": { "cbor": cbor } });
    if !additional_utxo.is_empty() {
        params["additionalUtxo"] = Json::Array(additional_utxo);
    }

    let request = json!({
        "jsonrpc": "2.0",
        "method": "evaluateTransaction",
        "params": params,
        "id": uuid::Uuid::new_v4().to_string(),
    });

    let client = CLIENT
        .as_ref()
        .map_err(|error| format!("no HTTP client: {error}"))?;

    let response: Json = client
        .post(url)
        .json(&request)
        .send()
        .await
        .map_err(|error| format!("request failed: {error}"))?
        .json()
        .await
        .map_err(|error| format!("response is not JSON: {error}"))?;

    if response.get("jsonrpc").is_none() {
        return Err(format!("response is not JSON-RPC: {response}"));
    }

    Ok(response)
}

/// Converts an Ogmios v6 JSON-RPC response to the Ogmios v5 JSON-WSP format.
pub(super) fn to_v5(response: &Json) -> Json {
    let id = &response["id"];

    if let Some(results) = response["result"].as_array() {
        return respond(id, json!({ "EvaluationResult": budgets(results) }));
    }

    let error = &response["error"];
    let data = &error["data"];
    let code = error["code"].as_i64().unwrap_or_default();

    match code {
        -32600 => fault(id, INVALID_PAYLOAD.into()),
        -32602 => fault(id, DESERIALISATION_FAILURE.into()),
        3000 if data["incompatibleEra"].is_string() => respond(
            id,
            failure(json!({ "IncompatibleEra": capitalize(&data["incompatibleEra"]) })),
        ),
        3002 => respond(
            id,
            failure(
                json!({ "AdditionalUtxoOverlap": references(&data["overlappingOutputReferences"]) }),
            ),
        ),
        3003 => respond(
            id,
            failure(json!({
                "NotEnoughSynced": {
                    "minimumRequiredEra": capitalize(&data["minimumRequiredEra"]),
                    "currentNodeEra": capitalize(&data["currentNodeEra"]),
                }
            })),
        ),
        3010 if data.is_array() => respond(
            id,
            failure(json!({ "ScriptFailures": script_failures(data) })),
        ),
        _ => match error_name(code) {
            Some(name) => {
                let reason = data["reason"]
                    .as_str()
                    .or_else(|| error["message"].as_str())
                    .unwrap_or_default();

                respond(
                    id,
                    failure(json!({ name: { "reason": capitalize_eras(reason) } })),
                )
            }
            None => fault(id, error["message"].as_str().unwrap_or_default().into()),
        },
    }
}

fn respond(id: &Json, result: Json) -> Json {
    json!({
        "type": "jsonwsp/response",
        "version": "1.0",
        "servicename": "ogmios",
        "methodname": "EvaluateTx",
        "result": result,
        "reflection": { "id": id },
    })
}

fn fault(id: &Json, message: String) -> Json {
    json!({
        "type": "jsonwsp/fault",
        "version": "1.0",
        "servicename": "ogmios",
        "fault": { "code": "client", "string": message },
        "reflection": { "id": id },
    })
}

fn failure(failure: Json) -> Json {
    json!({ "EvaluationFailure": failure })
}

/// The v5 budget of each validator, under its v5 name.
fn budgets(results: &[Json]) -> Map<String, Json> {
    results
        .iter()
        .map(|result| {
            let budget = &result["budget"];
            (
                pointer(&result["validator"], v5_purpose),
                json!({ "memory": budget["memory"], "steps": budget["cpu"] }),
            )
        })
        .collect()
}

// Blockfrost names a failed validator with its v6 purpose, for example
// `withdraw:0`. Only the `missingRequiredScripts` list uses the v5 purpose,
// for example `withdrawal:0`.
fn script_failures(failures: &Json) -> Map<String, Json> {
    let mut converted = Map::new();

    for item in failures.as_array().into_iter().flatten() {
        let key = pointer(&item["validator"], v6_purpose);
        let error = &item["error"];
        let data = &error["data"];
        let code = error["code"].as_i64().unwrap_or_default();

        let (name, value) = match code {
            3004 => (
                "CannotCreateEvaluationContext",
                json!({ "reason": data["reason"] }),
            ),
            3011 => (
                "missingRequiredScripts",
                json!({ "missing": pointers(&data["missingScripts"], v5_purpose) }),
            ),
            3012 => (
                "validatorFailed",
                json!({ "error": data["validationError"], "traces": data["traces"] }),
            ),
            3013 => (
                "unknownInputReferencedByRedeemer",
                reference(&data["unsuitableOutputReference"]),
            ),
            3110 => (
                "extraRedeemers",
                json!(pointers(&data["extraneousRedeemers"], v6_purpose)),
            ),
            3111 => (
                "missingRequiredDatums",
                json!({ "missing": data["missingDatums"] }),
            ),
            3115 => (
                "noCostModelForLanguage",
                data["missingCostModels"][0].clone(),
            ),
            3117 => (
                "UnknownOutputReference",
                json!(references(&data["unknownOutputReferences"])),
            ),
            _ => {
                tracing::warn!(code, %key, "dropped an Ogmios script failure without a v5 form");
                continue;
            }
        };

        let entry = converted.entry(key).or_insert_with(|| json!({}));
        if let Some(entry) = entry.as_object_mut() {
            entry.insert(name.into(), value);
        }
    }

    converted
}

/// The name of an Ogmios evaluation error that Blockfrost reports as a v5
/// `EvaluationFailure` with a reason.
fn error_name(code: i64) -> Option<&'static str> {
    match code {
        3000 => Some("IncompatibleEra"),
        3001 => Some("UnsupportedEra"),
        3002 => Some("OverlappingAdditionalUtxo"),
        3003 => Some("NodeTipTooOld"),
        3004 => Some("CannotCreateEvaluationContext"),
        3005 => Some("EraMismatch"),
        3010 => Some("ScriptExecutionFailure"),
        _ => None,
    }
}

fn v6_purpose(purpose: &str) -> &str {
    purpose
}

fn v5_purpose(purpose: &str) -> &str {
    match purpose {
        "publish" => "certificate",
        "withdraw" => "withdrawal",
        other => other,
    }
}

/// The `purpose:index` name of a v6 validator, with `name_purpose` applied to
/// the purpose.
fn pointer(validator: &Json, name_purpose: fn(&str) -> &str) -> String {
    let purpose = validator["purpose"].as_str().unwrap_or_default();
    format!("{}:{}", name_purpose(purpose), validator["index"])
}

fn pointers(validators: &Json, name_purpose: fn(&str) -> &str) -> Vec<String> {
    validators
        .as_array()
        .into_iter()
        .flatten()
        .map(|validator| pointer(validator, name_purpose))
        .collect()
}

/// Converts a v6 output reference to the v5 `{txId, index}` form.
fn reference(reference: &Json) -> Json {
    json!({ "txId": reference["transaction"]["id"], "index": reference["index"] })
}

fn references(references: &Json) -> Vec<Json> {
    references
        .as_array()
        .into_iter()
        .flatten()
        .map(reference)
        .collect()
}

fn capitalize(era: &Json) -> Json {
    let era = era.as_str().unwrap_or_default();
    let mut chars = era.chars();

    match chars.next() {
        Some(first) => Json::String(first.to_uppercase().chain(chars).collect()),
        None => Json::String(String::new()),
    }
}

/// Capitalizes the era names in a reason. Blockfrost also joins the words with
/// single spaces.
fn capitalize_eras(reason: &str) -> String {
    reason
        .split_whitespace()
        .map(|word| {
            if ERAS.contains(&word.to_lowercase().as_str()) {
                capitalize(&Json::String(word.into()))
                    .as_str()
                    .unwrap_or(word)
                    .to_string()
            } else {
                word.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Live responses from 2026-10-09 for the same transactions: the Ogmios v6
    /// response that Blockfrost passes through for `version=6`, and the
    /// Blockfrost v5 answer.
    const PAIRS: &str = include_str!("testdata/ogmios_pairs.json");

    fn without_id(mut response: Json) -> Json {
        response["reflection"]["id"] = Json::Null;
        response
    }

    #[test]
    fn converts_live_ogmios_responses_like_blockfrost() {
        let pairs: Vec<Json> = serde_json::from_str(PAIRS).unwrap();
        assert_eq!(pairs.len(), 15);

        for pair in &pairs {
            let name = pair["name"].as_str().unwrap();
            let converted = to_v5(&pair["v6"]);

            assert_eq!(converted["reflection"]["id"], pair["v6"]["id"], "{name}");

            let expected = match name {
                // Blockfrost drops validator failures and answers an empty
                // map. Dolos keeps them.
                "hello_validation_failure" => json!({
                    "EvaluationFailure": { "ScriptFailures": { "spend:0": { "validatorFailed": {
                        "error": pair["v6"]["error"]["data"][0]["error"]["data"]["validationError"],
                        "traces": [],
                    }}}}
                }),
                _ => pair["v5"]["result"].clone(),
            };

            let mut expected_response = pair["v5"].clone();
            if expected_response["type"] == "jsonwsp/response" {
                expected_response["result"] = expected;
            }

            assert_eq!(
                without_id(converted),
                without_id(expected_response),
                "{name}"
            );
        }
    }

    fn v6_error(code: i64, data: Json) -> Json {
        json!({
            "jsonrpc": "2.0",
            "method": "evaluateTransaction",
            "error": { "code": code, "message": "message", "data": data },
            "id": "id",
        })
    }

    fn nested(failures: Vec<(Json, i64, Json)>) -> Json {
        let data = failures
            .into_iter()
            .map(|(validator, code, data)| {
                json!({ "validator": validator, "error": { "code": code, "message": "m", "data": data } })
            })
            .collect::<Vec<_>>();

        v6_error(3010, json!(data))
    }

    #[test]
    fn converts_each_nested_failure() {
        let spend = |index: u64| json!({ "purpose": "spend", "index": index });
        let reference = json!({ "transaction": { "id": "ab".repeat(32) }, "index": 1 });

        let response = nested(vec![
            (
                spend(0),
                3013,
                json!({ "unsuitableOutputReference": reference }),
            ),
            (
                spend(1),
                3111,
                json!({ "missingDatums": ["cd".repeat(32)] }),
            ),
            (
                spend(2),
                3115,
                json!({ "missingCostModels": ["plutus:v3"] }),
            ),
            (
                spend(3),
                3117,
                json!({ "unknownOutputReferences": [reference] }),
            ),
            (
                json!({ "purpose": "propose", "index": 0 }),
                3011,
                json!({ "missingScripts": [{ "purpose": "propose", "index": 0 }] }),
            ),
            (spend(4), 3999, json!({})),
        ]);

        let txo = json!({ "txId": "ab".repeat(32), "index": 1 });

        assert_eq!(
            to_v5(&response)["result"],
            json!({ "EvaluationFailure": { "ScriptFailures": {
                "spend:0": { "unknownInputReferencedByRedeemer": txo },
                "spend:1": { "missingRequiredDatums": { "missing": ["cd".repeat(32)] } },
                "spend:2": { "noCostModelForLanguage": "plutus:v3" },
                "spend:3": { "UnknownOutputReference": [txo] },
                "propose:0": { "missingRequiredScripts": { "missing": ["propose:0"] } },
            }}})
        );
    }

    #[test]
    fn converts_top_level_failures() {
        let reference = json!({ "transaction": { "id": "01".repeat(32) }, "index": 2 });

        let cases = [
            (
                v6_error(3002, json!({ "overlappingOutputReferences": [reference] })),
                json!({ "AdditionalUtxoOverlap": [{ "txId": "01".repeat(32), "index": 2 }] }),
            ),
            (
                v6_error(
                    3003,
                    json!({ "currentNodeEra": "mary", "minimumRequiredEra": "alonzo" }),
                ),
                json!({ "NotEnoughSynced": { "minimumRequiredEra": "Alonzo", "currentNodeEra": "Mary" } }),
            ),
            (
                v6_error(3004, json!({ "reason": "an alonzo   output" })),
                json!({ "CannotCreateEvaluationContext": { "reason": "an Alonzo output" } }),
            ),
            (
                v6_error(3001, json!({ "unsupportedEra": "babbage" })),
                json!({ "UnsupportedEra": { "reason": "message" } }),
            ),
        ];

        for (response, expected) in cases {
            assert_eq!(to_v5(&response)["result"]["EvaluationFailure"], expected);
        }
    }

    #[test]
    fn converts_other_errors_to_faults() {
        let response = json!({
            "jsonrpc": "2.0",
            "error": { "code": -32603, "message": "internal" },
            "id": "id",
        });

        assert_eq!(
            to_v5(&response),
            json!({
                "type": "jsonwsp/fault",
                "version": "1.0",
                "servicename": "ogmios",
                "fault": { "code": "client", "string": "internal" },
                "reflection": { "id": "id" },
            })
        );
    }
}
