use tx3_lang::ir::{Expression, StructExpr};

use dolos_core::{EraCbor, TxoRef};
use pallas::{
    codec::utils::KeyValuePairs,
    ledger::{
        primitives::{conway::DatumOption, BigInt, Constr, PlutusData},
        traverse::{Era, MultiEraAsset, MultiEraOutput, MultiEraPolicyAssets, MultiEraValue},
    },
};

use crate::Error;

fn map_custom_asset(asset: &MultiEraAsset) -> tx3_lang::CanonicalAssets {
    let policy = asset.policy().as_slice();
    let asset_name = asset.name();
    let amount = asset.any_coin();

    tx3_lang::CanonicalAssets::from_defined_asset(policy, asset_name, amount)
}

fn map_policy_assets(assets: &MultiEraPolicyAssets) -> tx3_lang::CanonicalAssets {
    let all = tx3_lang::CanonicalAssets::empty();

    let all = assets
        .assets()
        .iter()
        .map(map_custom_asset)
        .fold(all, |acc, x| acc + x);

    all
}

fn map_assets(value: &MultiEraValue<'_>) -> tx3_lang::CanonicalAssets {
    let naked = tx3_lang::CanonicalAssets::from_naked_amount(value.coin() as i128);

    let all = value
        .assets()
        .iter()
        .map(map_policy_assets)
        .fold(naked, |acc, x| acc + x);

    all
}

fn map_big_int(x: &BigInt) -> Expression {
    match x {
        BigInt::Int(x) => Expression::Number((*x).into()),
        BigInt::BigUInt(bounded_bytes) => {
            // Convert bytes to big-endian integer
            let mut result = 0i128;
            for &byte in bounded_bytes.iter() {
                result = (result << 8) | (byte as i128);
            }
            Expression::Number(result)
        }
        BigInt::BigNInt(bounded_bytes) => {
            // Convert bytes to big-endian integer and negate
            let mut result = 0i128;
            for &byte in bounded_bytes.iter() {
                result = (result << 8) | (byte as i128);
            }
            Expression::Number(-result)
        }
    }
}

fn map_constr(x: &Constr<PlutusData>) -> Expression {
    Expression::Struct(StructExpr {
        constructor: x.constructor_value().unwrap_or_default() as usize,
        fields: x.fields.iter().map(map_datum).collect(),
    })
}

fn map_array(x: &[PlutusData]) -> Expression {
    Expression::List(x.iter().map(map_datum).collect())
}

fn map_map(x: &KeyValuePairs<PlutusData, PlutusData>) -> Expression {
    Expression::List(
        x.iter()
            .map(|(k, v)| Expression::List(vec![map_datum(k), map_datum(v)]))
            .collect(),
    )
}

fn map_datum(datum: &PlutusData) -> Expression {
    match datum {
        PlutusData::Constr(x) => map_constr(x),
        PlutusData::Map(x) => map_map(x),
        PlutusData::BigInt(x) => map_big_int(x),
        PlutusData::BoundedBytes(x) => Expression::Bytes(x.to_vec()),
        PlutusData::Array(x) => map_array(x),
    }
}

pub fn from_tx3_utxoref(r#ref: tx3_lang::UtxoRef) -> TxoRef {
    let txid = dolos_cardano::pallas::crypto::hash::Hash::from(r#ref.txid.as_slice());

    TxoRef(txid, r#ref.index)
}

pub fn into_tx3_utxoref(txoref: TxoRef) -> tx3_lang::UtxoRef {
    tx3_lang::UtxoRef {
        txid: txoref.0.to_vec(),
        index: txoref.1,
    }
}

pub fn into_tx3_utxo(txoref: TxoRef, utxo: EraCbor) -> Result<tx3_lang::Utxo, Error> {
    let r#ref = into_tx3_utxoref(txoref);

    let EraCbor(era, cbor) = utxo;

    let era = Era::try_from(era)?;

    let parsed = MultiEraOutput::decode(era, &cbor)?;

    let address = parsed.address()?.to_vec();

    let assets = map_assets(&parsed.value());

    let datum = match parsed.datum() {
        Some(DatumOption::Data(x)) => Some(map_datum(&x.0)),
        _ => None,
    };

    Ok(tx3_lang::Utxo {
        r#ref,
        address,
        datum,
        assets,
        script: None,
    })
}
