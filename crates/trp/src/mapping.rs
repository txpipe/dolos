use std::sync::Arc;

use tx3_tir::model::assets::CanonicalAssets;
use tx3_tir::model::v1beta0 as tir;

use dolos_core::{EraCbor, TxoRef};
use pallas::{
    codec::utils::KeyValuePairs,
    ledger::{
        primitives::{conway::DatumOption, BigInt, Constr, PlutusData},
        traverse::{Era, MultiEraAsset, MultiEraOutput, MultiEraPolicyAssets, MultiEraValue},
    },
};

fn map_custom_asset(asset: &MultiEraAsset) -> CanonicalAssets {
    let policy = asset.policy().as_slice();
    let asset_name = asset.name();
    let amount = asset.any_coin();

    CanonicalAssets::from_defined_asset(policy, asset_name, amount)
}

fn map_policy_assets(assets: &MultiEraPolicyAssets) -> CanonicalAssets {
    let all = CanonicalAssets::empty();

    let all = assets
        .assets()
        .iter()
        .map(map_custom_asset)
        .fold(all, |acc, x| acc + x);

    all
}

fn map_assets(value: &MultiEraValue<'_>) -> CanonicalAssets {
    let naked = CanonicalAssets::from_naked_amount(value.coin() as i128);

    let all = value
        .assets()
        .iter()
        .map(map_policy_assets)
        .fold(naked, |acc, x| acc + x);

    all
}

fn map_big_int(x: &BigInt) -> tir::Expression {
    match x {
        BigInt::Int(x) => tir::Expression::Number((*x).into()),
        BigInt::BigUInt(bounded_bytes) => {
            // Convert bytes to big-endian integer
            let mut result = 0i128;
            for &byte in bounded_bytes.iter() {
                result = (result << 8) | (byte as i128);
            }
            tir::Expression::Number(result)
        }
        BigInt::BigNInt(bounded_bytes) => {
            // Convert bytes to big-endian integer and negate
            let mut result = 0i128;
            for &byte in bounded_bytes.iter() {
                result = (result << 8) | (byte as i128);
            }
            tir::Expression::Number(-result)
        }
    }
}

fn map_constr(x: &Constr<PlutusData>) -> tir::Expression {
    tir::Expression::Struct(tir::StructExpr {
        constructor: x.constructor_value().unwrap_or_default() as usize,
        fields: x.fields.iter().map(map_datum).collect(),
    })
}

fn map_array(x: &[PlutusData]) -> tir::Expression {
    tir::Expression::List(x.iter().map(map_datum).collect())
}

fn map_map(x: &KeyValuePairs<PlutusData, PlutusData>) -> tir::Expression {
    tir::Expression::List(
        x.iter()
            .map(|(k, v)| tir::Expression::List(vec![map_datum(k), map_datum(v)]))
            .collect(),
    )
}

fn map_datum(datum: &PlutusData) -> tir::Expression {
    match datum {
        PlutusData::Constr(x) => map_constr(x),
        PlutusData::Map(x) => map_map(x),
        PlutusData::BigInt(x) => map_big_int(x),
        PlutusData::BoundedBytes(x) => tir::Expression::Bytes(x.to_vec()),
        PlutusData::Array(x) => map_array(x),
    }
}

pub fn from_tx3_utxoref(r#ref: tir::UtxoRef) -> TxoRef {
    let txid = dolos_cardano::pallas::crypto::hash::Hash::from(r#ref.txid.as_slice());

    TxoRef(txid, r#ref.index)
}

pub fn into_tx3_utxoref(txoref: TxoRef) -> tir::UtxoRef {
    tir::UtxoRef {
        txid: txoref.0.to_vec(),
        index: txoref.1,
    }
}

pub fn into_tx3_utxo(txoref: TxoRef, utxo: Arc<EraCbor>) -> Result<tir::Utxo, tx3_resolver::Error> {
    let r#ref = into_tx3_utxoref(txoref);

    let EraCbor(era, cbor) = utxo.as_ref();

    let era = Era::try_from(*era).map_err(|e| tx3_resolver::Error::StoreError(e.to_string()))?;

    let parsed = MultiEraOutput::decode(era, cbor)
        .map_err(|e| tx3_resolver::Error::StoreError(e.to_string()))?;

    let address = parsed
        .address()
        .map_err(|e| tx3_resolver::Error::StoreError(e.to_string()))?
        .to_vec();

    let assets = map_assets(&parsed.value());

    let datum = match parsed.datum() {
        Some(DatumOption::Data(x)) => Some(map_datum(&x.0)),
        _ => None,
    };

    Ok(tir::Utxo {
        r#ref,
        address,
        datum,
        assets,
        script: None,
    })
}
