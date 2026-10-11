//! The `additionalUtxoSet` field: Ogmios v5 `[TxIn, TxOut]` pairs.

use std::collections::BTreeMap;

use dolos_core::{EraCbor, TxoRef};
use pallas::{
    codec::{
        minicbor,
        utils::{CborWrap, KeepRaw, PositiveCoin},
    },
    crypto::hash::Hash,
    ledger::{
        addresses::Address,
        primitives::{
            conway::{
                DatumOption, NativeScript, PlutusData, PlutusScript, PostAlonzoTransactionOutput,
                ScriptRef, TransactionOutput, Value,
            },
            PolicyId,
        },
        traverse::Era,
    },
};
use serde::Deserialize;
use serde_json::{Map, Value as Json};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxIn {
    tx_id: String,
    index: u32,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxOut {
    address: String,
    value: TxOutValue,
    #[serde(alias = "datum_hash")]
    datum_hash: Option<String>,
    datum: Option<String>,
    script: Option<Map<String, Json>>,
}

#[derive(Deserialize)]
struct TxOutValue {
    coins: u64,
    #[serde(default)]
    assets: BTreeMap<String, u64>,
}

/// Parses Ogmios v5 `[TxIn, TxOut]` pairs into UTxO entries in the Conway
/// output format. Returns a message that names the first malformed entry.
pub(super) fn parse(entries: &[Json]) -> Result<Vec<(TxoRef, EraCbor)>, String> {
    entries
        .iter()
        .enumerate()
        .map(|(position, entry)| {
            parse_entry(entry).map_err(|error| format!("additionalUtxoSet[{position}]: {error}"))
        })
        .collect()
}

fn parse_entry(entry: &Json) -> Result<(TxoRef, EraCbor), String> {
    let Some([txin, txout]) = entry.as_array().map(Vec::as_slice) else {
        return Err("expected a [TxIn, TxOut] pair".into());
    };

    let txin = TxIn::deserialize(txin).map_err(|error| format!("TxIn: {error}"))?;
    let txout = TxOut::deserialize(txout).map_err(|error| format!("TxOut: {error}"))?;

    let tx_hash = parse_hash(&txin.tx_id, "txId")?;

    Ok((TxoRef(tx_hash, txin.index), encode_output(&txout)?))
}

fn encode_output(txout: &TxOut) -> Result<EraCbor, String> {
    let address: Address = txout
        .address
        .parse()
        .map_err(|_| format!("address {} is not valid", txout.address))?;

    // The inline datum keeps the bytes of the request, so its hash does not
    // change.
    let datum_bytes = txout
        .datum
        .as_deref()
        .map(|datum| decode_hex(datum, "datum"))
        .transpose()?;

    let datum_option = match (&txout.datum_hash, &datum_bytes) {
        (Some(_), Some(_)) => return Err("use datumHash or datum, not both".into()),
        (Some(hash), None) => Some(DatumOption::Hash(parse_hash(hash, "datumHash")?)),
        (None, Some(bytes)) => {
            let data = minicbor::decode::<KeepRaw<PlutusData>>(bytes)
                .map_err(|_| "datum is not valid Plutus data".to_string())?;
            Some(DatumOption::Data(CborWrap(data)))
        }
        (None, None) => None,
    };

    let script_ref = txout.script.as_ref().map(parse_script).transpose()?;

    let output = PostAlonzoTransactionOutput {
        address: address.to_vec().into(),
        value: parse_value(&txout.value)?,
        datum_option: datum_option.map(KeepRaw::from),
        script_ref: script_ref.map(CborWrap),
    };

    let cbor = minicbor::to_vec(TransactionOutput::PostAlonzo(KeepRaw::from(output)))
        .map_err(|error| format!("cannot encode the output: {error}"))?;

    Ok(EraCbor(Era::Conway.into(), cbor))
}

/// Ogmios v5 names an asset `{policy}.{name}`, or `{policy}` when the name is
/// empty. The name is hexadecimal.
fn parse_value(value: &TxOutValue) -> Result<Value, String> {
    if value.assets.is_empty() {
        return Ok(Value::Coin(value.coins));
    }

    let mut assets: BTreeMap<PolicyId, BTreeMap<_, PositiveCoin>> = BTreeMap::new();

    for (unit, quantity) in &value.assets {
        let (policy, name) = unit.split_once('.').unwrap_or((unit, ""));
        let policy = parse_hash(policy, "asset policy")?;
        let name = decode_hex(name, "asset name")?;

        let quantity = PositiveCoin::try_from(*quantity)
            .map_err(|_| format!("asset {unit} must have a positive quantity"))?;

        assets
            .entry(policy)
            .or_default()
            .insert(name.into(), quantity);
    }

    Ok(Value::Multiasset(value.coins, assets))
}

fn parse_script(script: &Map<String, Json>) -> Result<ScriptRef<'static>, String> {
    let mut entries = script.iter();

    let (Some((language, body)), None) = (entries.next(), entries.next()) else {
        return Err("script must have exactly one language key".into());
    };

    let plutus = |body: &Json| -> Result<Vec<u8>, String> {
        let body = body
            .as_str()
            .ok_or_else(|| format!("{language} script must be a hexadecimal string"))?;
        decode_hex(body, "script")
    };

    match language.as_str() {
        "native" => Ok(ScriptRef::NativeScript(KeepRaw::from(parse_native(body)?))),
        "plutus:v1" => Ok(ScriptRef::PlutusV1Script(PlutusScript(
            plutus(body)?.into(),
        ))),
        "plutus:v2" => Ok(ScriptRef::PlutusV2Script(PlutusScript(
            plutus(body)?.into(),
        ))),
        "plutus:v3" => Ok(ScriptRef::PlutusV3Script(PlutusScript(
            plutus(body)?.into(),
        ))),
        other => Err(format!("script language {other} is not known")),
    }
}

/// Parses the Ogmios v5 JSON form of a native script.
fn parse_native(script: &Json) -> Result<NativeScript, String> {
    match script {
        Json::String(key_hash) => Ok(NativeScript::ScriptPubkey(parse_hash(
            key_hash,
            "native script key hash",
        )?)),
        Json::Object(clause) if clause.len() == 1 => {
            let (name, body) = clause.iter().next().expect("one entry");

            let scripts = |body: &Json| -> Result<Vec<NativeScript>, String> {
                body.as_array()
                    .ok_or_else(|| format!("native script clause {name} must be a list"))?
                    .iter()
                    .map(parse_native)
                    .collect()
            };

            let slot = |body: &Json| {
                body.as_u64()
                    .ok_or_else(|| format!("native script clause {name} must be a slot number"))
            };

            match name.as_str() {
                "all" => Ok(NativeScript::ScriptAll(scripts(body)?)),
                "any" => Ok(NativeScript::ScriptAny(scripts(body)?)),
                "startsAt" => Ok(NativeScript::InvalidBefore(slot(body)?)),
                "expiresAt" => Ok(NativeScript::InvalidHereafter(slot(body)?)),
                required => match required.parse::<i64>() {
                    Ok(required) => Ok(NativeScript::ScriptNOfK(required, scripts(body)?)),
                    Err(_) => Err(format!("native script clause {required} is not known")),
                },
            }
        }
        _ => Err("native script must be a key hash or an object with one clause".into()),
    }
}

fn parse_hash<const BYTES: usize>(text: &str, field: &str) -> Result<Hash<BYTES>, String> {
    text.parse()
        .map_err(|_| format!("{field} must be {} hexadecimal characters", BYTES * 2))
}

fn decode_hex(text: &str, field: &str) -> Result<Vec<u8>, String> {
    hex::decode(text).map_err(|_| format!("{field} must be hexadecimal"))
}

#[cfg(test)]
mod tests {
    use pallas::{
        codec::utils::Bytes,
        ledger::{
            primitives::conway::{DatumOption, NativeScript, ScriptRef},
            traverse::{ComputeHash as _, MultiEraOutput, OriginalHash as _},
        },
    };
    use serde_json::json;

    use super::*;

    const SCRIPT_ADDRESS: &str = "addr_test1wrzqlyffcf5yq3htqge9h9k29zv6d7ny0rqam6d4c5eqdfgg0h7yw";
    const POLICY: &str = "e16c2dc8ae937e8d3790c7fd7168d7b994621ba14ca11415f39fed72";
    const KEY_HASH: &str = "3c07030e36bfffe67e2e2ec09e5293d384637cd2f004356ef320f3fe";

    fn parse_one(txout: Json) -> Result<(TxoRef, EraCbor), String> {
        let entries = [json!([{ "txId": "ff".repeat(32), "index": 3 }, txout])];
        parse(&entries).map(|mut parsed| parsed.remove(0))
    }

    fn with_output<T>(txout: Json, check: impl FnOnce(&MultiEraOutput) -> T) -> T {
        let (_, utxo) = parse_one(txout).expect("valid entry");
        let output = MultiEraOutput::try_from(&utxo).expect("decodable output");
        check(&output)
    }

    #[test]
    fn parses_the_reference() {
        let (txo_ref, utxo) =
            parse_one(json!({ "address": SCRIPT_ADDRESS, "value": { "coins": 1 } })).unwrap();

        assert_eq!(txo_ref, TxoRef(Hash::from([0xff; 32]), 3));
        assert_eq!(utxo.era(), u16::from(Era::Conway));
    }

    #[test]
    fn parses_coins_and_assets() {
        let value = with_output(
            json!({
                "address": SCRIPT_ADDRESS,
                "value": {
                    "coins": 2_000_000,
                    "assets": { POLICY: 7, format!("{POLICY}.4d494e"): 9 }
                }
            }),
            |output| output.value().into_conway(),
        );

        let policy: Hash<28> = POLICY.parse().unwrap();
        let mut names = BTreeMap::new();
        names.insert(Bytes::from(vec![]), PositiveCoin::try_from(7).unwrap());
        names.insert(
            Bytes::from(b"MIN".to_vec()),
            PositiveCoin::try_from(9).unwrap(),
        );

        assert_eq!(
            value,
            Value::Multiasset(2_000_000, BTreeMap::from([(policy, names)]))
        );
    }

    #[test]
    fn parses_a_datum_hash() {
        let datum = with_output(
            json!({
                "address": SCRIPT_ADDRESS,
                "value": { "coins": 1 },
                "datumHash": "aa".repeat(32),
            }),
            |output| output.datum().map(|datum| minicbor::to_vec(datum).unwrap()),
        );

        let expected = DatumOption::Hash(Hash::from([0xaa; 32]));
        assert_eq!(datum, Some(minicbor::to_vec(expected).unwrap()));
    }

    #[test]
    fn keeps_the_inline_datum_bytes() {
        // An indefinite list encodes the same data as `d87980`, with other
        // bytes and thus another hash.
        let bytes = hex::decode("d8799fff").unwrap();

        let hash = with_output(
            json!({
                "address": SCRIPT_ADDRESS,
                "value": { "coins": 1 },
                "datum": "d8799fff",
            }),
            |output| match output.datum() {
                Some(DatumOption::Data(data)) => data.0.original_hash(),
                other => panic!("unexpected datum {other:?}"),
            },
        );

        assert_eq!(hash, pallas::crypto::hash::Hasher::<256>::hash(&bytes));
    }

    #[test]
    fn parses_each_plutus_language() {
        let flat = "4e4d01000033222220051200120011";

        for (language, version) in [("plutus:v1", 1), ("plutus:v2", 2), ("plutus:v3", 3)] {
            let script = with_output(
                json!({
                    "address": SCRIPT_ADDRESS,
                    "value": { "coins": 1 },
                    "script": { language: flat },
                }),
                |output| {
                    output
                        .script_ref()
                        .map(|script| minicbor::to_vec(script).unwrap())
                },
            );

            let bytes = Bytes::from(hex::decode(flat).unwrap());
            let expected = match version {
                1 => ScriptRef::PlutusV1Script(PlutusScript(bytes)),
                2 => ScriptRef::PlutusV2Script(PlutusScript(bytes)),
                _ => ScriptRef::PlutusV3Script(PlutusScript(bytes)),
            };

            assert_eq!(
                script,
                Some(minicbor::to_vec(expected).unwrap()),
                "{language}"
            );
        }
    }

    #[test]
    fn parses_each_native_clause() {
        let key: Hash<28> = KEY_HASH.parse().unwrap();

        let cases = [
            (json!(KEY_HASH), NativeScript::ScriptPubkey(key)),
            (
                json!({ "all": [KEY_HASH] }),
                NativeScript::ScriptAll(vec![NativeScript::ScriptPubkey(key)]),
            ),
            (
                json!({ "any": [KEY_HASH] }),
                NativeScript::ScriptAny(vec![NativeScript::ScriptPubkey(key)]),
            ),
            (
                json!({ "2": [KEY_HASH, { "startsAt": 5 }] }),
                NativeScript::ScriptNOfK(
                    2,
                    vec![
                        NativeScript::ScriptPubkey(key),
                        NativeScript::InvalidBefore(5),
                    ],
                ),
            ),
            (json!({ "startsAt": 10 }), NativeScript::InvalidBefore(10)),
            (
                json!({ "expiresAt": 20 }),
                NativeScript::InvalidHereafter(20),
            ),
        ];

        for (body, expected) in cases {
            let hash = with_output(
                json!({
                    "address": SCRIPT_ADDRESS,
                    "value": { "coins": 1 },
                    "script": { "native": body },
                }),
                |output| match output.script_ref() {
                    Some(ScriptRef::NativeScript(script)) => script.original_hash(),
                    other => panic!("unexpected script {other:?}"),
                },
            );

            assert_eq!(hash, expected.compute_hash(), "{body}");
        }
    }

    #[test]
    fn rejects_malformed_entries() {
        let address = json!(SCRIPT_ADDRESS);
        let cases = [
            json!([]),
            json!([{ "txId": "ff".repeat(32), "index": 0 }]),
            json!([{ "txId": "ff", "index": 0 }, { "address": address, "value": { "coins": 1 } }]),
            json!([{ "txId": "ff".repeat(32), "index": -1 }, { "address": address, "value": { "coins": 1 } }]),
            json!([{ "txId": "ff".repeat(32), "index": 0 }, { "address": "addr1nope", "value": { "coins": 1 } }]),
            json!([{ "txId": "ff".repeat(32), "index": 0 }, { "address": address }]),
            json!([{ "txId": "ff".repeat(32), "index": 0 }, { "address": address, "value": { "coins": 1, "assets": { POLICY: 0 } } }]),
            json!([{ "txId": "ff".repeat(32), "index": 0 }, { "address": address, "value": { "coins": 1 }, "datum": "zz" }]),
            json!([{ "txId": "ff".repeat(32), "index": 0 }, { "address": address, "value": { "coins": 1 }, "datum": "d87980", "datumHash": "aa".repeat(32) }]),
            json!([{ "txId": "ff".repeat(32), "index": 0 }, { "address": address, "value": { "coins": 1 }, "script": { "plutus:v9": "00" } }]),
            json!([{ "txId": "ff".repeat(32), "index": 0 }, { "address": address, "value": { "coins": 1 }, "script": { "native": { "sometimes": [] } } }]),
        ];

        for entry in cases {
            let error = parse(std::slice::from_ref(&entry)).expect_err(&entry.to_string());
            assert!(error.starts_with("additionalUtxoSet[0]: "), "{error}");
        }
    }
}
