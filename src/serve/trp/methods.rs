use base64::{engine::general_purpose::STANDARD, Engine};
use jsonrpsee::types::{ErrorCode, ErrorObject, ErrorObjectOwned, Params};
use serde::Deserialize;
use std::sync::Arc;
use tx3_lang::ProtoTx;

use super::Context;

#[derive(Debug, Default)]
pub struct TxEval {
    pub payload: Vec<u8>,
    pub fee: u64,
    pub ex_units: u64,
}

#[derive(Deserialize)]
enum IrEncoding {
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "hex")]
    Hex,
}

#[derive(Deserialize)]
struct IrEnvelope {
    #[allow(dead_code)]
    pub version: String,
    pub bytecode: String,
    pub encoding: IrEncoding,
}

#[derive(Deserialize)]
struct TrpResolveParams {
    pub tir: IrEnvelope,
    pub args: serde_json::Value,
}

pub fn decode_params(params: Params<'_>) -> Result<ProtoTx, ErrorObjectOwned> {
    let params: TrpResolveParams = params.parse()?;

    let tx = match params.tir.encoding {
        IrEncoding::Base64 => STANDARD.decode(params.tir.bytecode).map_err(|x| {
            ErrorObject::owned(
                ErrorCode::InvalidParams.code(),
                "Failed to decode IR using Base64 encoding",
                Some(x.to_string()),
            )
        })?,
        IrEncoding::Hex => hex::decode(params.tir.bytecode).map_err(|x| {
            ErrorObject::owned(
                ErrorCode::InvalidParams.code(),
                "Failed to decode IR using hex encoding",
                Some(x.to_string()),
            )
        })?,
    };

    let mut tx = tx3_lang::ProtoTx::from_ir_bytes(&tx).map_err(|x| {
        ErrorObject::owned(
            ErrorCode::InvalidParams.code(),
            "Failed to decode IR bytes",
            Some(x.to_string()),
        )
    })?;

    let Some(arguments) = params.args.as_object() else {
        return Err(ErrorObject::owned(
            ErrorCode::InvalidParams.code(),
            "Failed to parse arguments as object.",
            None as Option<String>,
        ));
    };

    for (key, val) in arguments.iter() {
        match val {
            serde_json::Value::String(x) => tx.set_arg(key, x.as_str().into()),
            serde_json::Value::Number(x) => tx.set_arg(
                key,
                match x.as_i64() {
                    Some(y) => y.into(),
                    None => {
                        return Err(ErrorObject::owned(
                            ErrorCode::InvalidParams.code(),
                            "Argument cannot be cast as i64",
                            Some(serde_json::json!({ "key": key, "value": val })),
                        ))
                    }
                },
            ),
            _ => {
                return Err(ErrorObject::owned(
                    ErrorCode::InvalidParams.code(),
                    "Invalid argument",
                    Some(serde_json::json!({ "key": key, "value": val })),
                ))
            }
        }
    }

    Ok(tx)
}

pub async fn trp_resolve(
    params: Params<'_>,
    context: Arc<Context>,
) -> Result<serde_json::Value, ErrorObjectOwned> {
    tracing::info!(method = "trp.resolve", "Received TRP request.");
    let tx = match decode_params(params) {
        Ok(tx) => tx,
        Err(err) => {
            tracing::warn!(err = ?err, "Failed to decode params.");
            return Err(err);
        }
    };

    let resolved = tx3_cardano::resolve_tx::<Context>(
        tx,
        (*context).clone(),
        context.config.max_optimize_rounds.into(),
    )
    .await
    .map_err(|err| {
        ErrorObject::owned(
            ErrorCode::InternalError.code(),
            "Failed to resolve",
            Some(err.to_string()),
        )
    });

    let resolved = match resolved {
        Ok(resolved) => resolved,
        Err(err) => {
            tracing::warn!(err = ?err, "Failed to resolve tx.");
            return Err(err);
        }
    };

    Ok(serde_json::json!({ "tx": hex::encode(resolved.payload) }))
}

pub fn health(context: &Context) -> bool {
    context.ledger.cursor().is_ok()
}
