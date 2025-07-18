use tx3_cardano::pallas::{
    codec::utils::KeyValuePairs,
    ledger::{
        primitives::{conway::DatumOption, BigInt, Constr, PlutusData},
        traverse::{MultiEraAsset, MultiEraOutput, MultiEraPolicyAssets, MultiEraValue},
    },
};

use tx3_lang::ir::{Expression, StructExpr};

use dolos_core::TxoRef;

fn map_custom_asset(asset: &MultiEraAsset) -> tx3_lang::ir::AssetExpr {
    let policy = asset.policy().to_vec();
    let asset_name = asset.name().to_vec();
    let amount = asset.any_coin();

    tx3_lang::ir::AssetExpr {
        policy: Expression::Bytes(policy),
        asset_name: Expression::Bytes(asset_name),
        amount: Expression::Number(amount),
    }
}

fn map_policy_assets(assets: &MultiEraPolicyAssets) -> Vec<tx3_lang::ir::AssetExpr> {
    assets.assets().iter().map(map_custom_asset).collect()
}

fn map_assets(value: &MultiEraValue<'_>) -> Vec<tx3_lang::ir::AssetExpr> {
    let coin = tx3_lang::ir::AssetExpr {
        policy: Expression::None,
        asset_name: Expression::None,
        amount: Expression::Number(value.coin() as i128),
    };

    let assets = value.assets();

    let policy_assets = assets.iter().flat_map(|x| map_policy_assets(x));

    let mut out = vec![coin];
    out.extend(policy_assets);

    out
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
    Expression::Map(
        x.iter()
            .map(|(k, v)| (map_datum(k), map_datum(v)))
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

pub fn into_tx3_utxo(
    txoref: &TxoRef,
    parsed: &MultiEraOutput<'_>,
) -> Result<tx3_lang::Utxo, tx3_cardano::Error> {
    let r#ref = tx3_lang::UtxoRef {
        txid: txoref.0.to_vec(),
        index: txoref.1,
    };

    let address = parsed
        .address()
        .map_err(tx3_cardano::Error::InvalidAddress)?
        .to_vec();

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
