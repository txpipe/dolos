//! Converts the `additionalUtxoSet` field from Ogmios v5 `[TxIn, TxOut]` pairs
//! to the Ogmios v6 `additionalUtxo` entries.

use serde::Deserialize;
use serde_json::{json, Map, Number, Value as Json};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxIn {
    tx_id: String,
    index: u64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxOut {
    address: String,
    value: TxOutValue,
    #[serde(alias = "datum_hash")]
    datum_hash: Option<Json>,
    datum: Option<Json>,
    script: Option<Map<String, Json>>,
}

#[derive(Deserialize)]
struct TxOutValue {
    coins: Number,
    #[serde(default)]
    assets: Map<String, Json>,
}

/// Converts Ogmios v5 `[TxIn, TxOut]` pairs to Ogmios v6 UTxO entries. Returns
/// a message that names the first malformed entry. Ogmios checks the content
/// of each field.
pub(super) fn to_v6(entries: &[Json]) -> Result<Vec<Json>, String> {
    entries
        .iter()
        .enumerate()
        .map(|(position, entry)| {
            entry_to_v6(entry).map_err(|error| format!("additionalUtxoSet[{position}]: {error}"))
        })
        .collect()
}

fn entry_to_v6(entry: &Json) -> Result<Json, String> {
    let Some([txin, txout]) = entry.as_array().map(Vec::as_slice) else {
        return Err("expected a [TxIn, TxOut] pair".into());
    };

    let txin = TxIn::deserialize(txin).map_err(|error| format!("TxIn: {error}"))?;
    let txout = TxOut::deserialize(txout).map_err(|error| format!("TxOut: {error}"))?;

    let mut utxo = json!({
        "transaction": { "id": txin.tx_id },
        "index": txin.index,
        "address": txout.address,
        "value": value_to_v6(&txout.value),
    });

    if let Some(datum_hash) = txout.datum_hash {
        utxo["datumHash"] = datum_hash;
    }

    if let Some(datum) = txout.datum {
        utxo["datum"] = datum;
    }

    if let Some(script) = &txout.script {
        utxo["script"] = script_to_v6(script)?;
    }

    Ok(utxo)
}

/// Ogmios v5 names an asset `{policy}.{name}`, or `{policy}` when the name is
/// empty. Ogmios v6 nests the names under the policy.
fn value_to_v6(value: &TxOutValue) -> Json {
    let mut converted = json!({ "ada": { "lovelace": value.coins } });

    for (unit, quantity) in &value.assets {
        let (policy, name) = unit.split_once('.').unwrap_or((unit, ""));
        converted[policy][name] = quantity.clone();
    }

    converted
}

fn script_to_v6(script: &Map<String, Json>) -> Result<Json, String> {
    let mut entries = script.iter();

    let (Some((language, body)), None) = (entries.next(), entries.next()) else {
        return Err("script must have exactly one language key".into());
    };

    match language.as_str() {
        "native" => Ok(json!({ "language": "native", "json": native_to_v6(body)? })),
        "plutus:v1" | "plutus:v2" | "plutus:v3" => {
            let cbor = body
                .as_str()
                .ok_or_else(|| format!("{language} script must be a string"))?;
            Ok(json!({ "language": language, "cbor": cbor }))
        }
        other => Err(format!("script language {other} is not known")),
    }
}

/// Converts a native script from the Ogmios v5 JSON form to the v6 clauses.
fn native_to_v6(script: &Json) -> Result<Json, String> {
    match script {
        Json::String(key_hash) => Ok(json!({ "clause": "signature", "from": key_hash })),
        Json::Object(clause) if clause.len() == 1 => {
            let (name, body) = clause.iter().next().expect("one entry");

            let scripts = |body: &Json| -> Result<Vec<Json>, String> {
                body.as_array()
                    .ok_or_else(|| format!("native script clause {name} must be a list"))?
                    .iter()
                    .map(native_to_v6)
                    .collect()
            };

            match name.as_str() {
                "all" | "any" => Ok(json!({ "clause": name, "from": scripts(body)? })),
                "expiresAt" => Ok(json!({ "clause": "before", "slot": body })),
                "startsAt" => Ok(json!({ "clause": "after", "slot": body })),
                required => match required.parse::<u64>() {
                    Ok(required) => Ok(json!({
                        "clause": "some",
                        "atLeast": required,
                        "from": scripts(body)?,
                    })),
                    Err(_) => Err(format!("native script clause {required} is not known")),
                },
            }
        }
        _ => Err("native script must be a key hash or an object with one clause".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: &str = "e16c2dc8ae937e8d3790c7fd7168d7b994621ba14ca11415f39fed72";
    const KEY_HASH: &str = "3c07030e36bfffe67e2e2ec09e5293d384637cd2f004356ef320f3fe";

    fn convert(txout: Json) -> Result<Json, String> {
        let entries = [json!([{ "txId": "ff".repeat(32), "index": 3 }, txout])];
        to_v6(&entries).map(|mut converted| converted.remove(0))
    }

    #[test]
    fn converts_the_reference_and_the_address() {
        let utxo = convert(json!({ "address": "addr1x", "value": { "coins": 7 } })).unwrap();

        assert_eq!(
            utxo,
            json!({
                "transaction": { "id": "ff".repeat(32) },
                "index": 3,
                "address": "addr1x",
                "value": { "ada": { "lovelace": 7 } },
            })
        );
    }

    #[test]
    fn nests_the_assets_under_their_policy() {
        let utxo = convert(json!({
            "address": "addr1x",
            "value": {
                "coins": 0,
                "assets": {
                    POLICY: 5,
                    format!("{POLICY}.4d494e"): 18_446_744_073_709_551_615_u64,
                },
            },
        }))
        .unwrap();

        assert_eq!(
            utxo["value"],
            json!({
                "ada": { "lovelace": 0 },
                POLICY: { "": 5, "4d494e": 18_446_744_073_709_551_615_u64 },
            })
        );
    }

    #[test]
    fn keeps_the_datum_fields() {
        let hashed = convert(json!({
            "address": "addr1x",
            "value": { "coins": 1 },
            "datumHash": "aa".repeat(32),
        }))
        .unwrap();
        assert_eq!(hashed["datumHash"], "aa".repeat(32));
        assert!(hashed.get("datum").is_none());

        let inline =
            convert(json!({ "address": "addr1x", "value": { "coins": 1 }, "datum": "d87980" }))
                .unwrap();
        assert_eq!(inline["datum"], "d87980");
        assert!(inline.get("datumHash").is_none());
    }

    #[test]
    fn converts_each_plutus_language() {
        for language in ["plutus:v1", "plutus:v2", "plutus:v3"] {
            let utxo = convert(json!({
                "address": "addr1x",
                "value": { "coins": 1 },
                "script": { language: "4d01000033222220051200120011" },
            }))
            .unwrap();

            assert_eq!(
                utxo["script"],
                json!({ "language": language, "cbor": "4d01000033222220051200120011" })
            );
        }
    }

    #[test]
    fn converts_each_native_clause() {
        let signature = json!({ "clause": "signature", "from": KEY_HASH });

        let cases = [
            (json!(KEY_HASH), signature.clone()),
            (
                json!({ "all": [KEY_HASH] }),
                json!({ "clause": "all", "from": [signature] }),
            ),
            (
                json!({ "any": [KEY_HASH] }),
                json!({ "clause": "any", "from": [signature] }),
            ),
            (
                json!({ "2": [KEY_HASH, { "startsAt": 5 }] }),
                json!({
                    "clause": "some",
                    "atLeast": 2,
                    "from": [signature, { "clause": "after", "slot": 5 }],
                }),
            ),
            (
                json!({ "startsAt": 10 }),
                json!({ "clause": "after", "slot": 10 }),
            ),
            (
                json!({ "expiresAt": 20 }),
                json!({ "clause": "before", "slot": 20 }),
            ),
        ];

        for (native, expected) in cases {
            let utxo = convert(json!({
                "address": "addr1x",
                "value": { "coins": 1 },
                "script": { "native": native.clone() },
            }))
            .unwrap();

            assert_eq!(
                utxo["script"],
                json!({ "language": "native", "json": expected }),
                "{native}"
            );
        }
    }

    #[test]
    fn rejects_malformed_entries() {
        let txin = json!({ "txId": "ff".repeat(32), "index": 0 });
        let value = json!({ "coins": 1 });

        let cases = [
            json!([]),
            json!([txin]),
            json!([{ "txId": 1, "index": 0 }, { "address": "addr1x", "value": value }]),
            json!([{ "txId": "ff", "index": -1 }, { "address": "addr1x", "value": value }]),
            json!([txin, { "address": "addr1x" }]),
            json!([txin, { "address": "addr1x", "value": { "coins": "1" } }]),
            json!([txin, { "address": "addr1x", "value": value, "script": { "plutus:v9": "00" } }]),
            json!([txin, { "address": "addr1x", "value": value, "script": { "plutus:v2": 1 } }]),
            json!([txin, { "address": "addr1x", "value": value, "script": { "native": { "sometimes": [] } } }]),
        ];

        for entry in cases {
            let error = to_v6(std::slice::from_ref(&entry)).expect_err(&entry.to_string());
            assert!(error.starts_with("additionalUtxoSet[0]: "), "{error}");
        }
    }
}
