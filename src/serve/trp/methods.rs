use base64::{engine::general_purpose::STANDARD, Engine};
use jsonrpsee::types::{ErrorCode, ErrorObject, ErrorObjectOwned, Params};
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc};
use tx3_lang::ProtoTx;

use super::Context;

#[derive(Deserialize, Debug)]
enum IrEncoding {
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "hex")]
    Hex,
}

#[derive(Deserialize, Debug)]
struct IrEnvelope {
    #[allow(dead_code)]
    pub version: String,
    pub bytecode: String,
    pub encoding: IrEncoding,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum BytesEncoding {
    Base64,
    Hex,
}

#[derive(Debug, Deserialize)]
struct BytesPayload {
    content: String,
    encoding: BytesEncoding,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", content = "value")]
enum ParamsArgValue {
    #[serde(rename = "string")]
    String(String),

    #[serde(rename = "number")]
    Number(i64),

    #[serde(rename = "boolean")]
    Boolean(bool),

    #[serde(rename = "null")]
    Null,

    #[serde(rename = "bytes")]
    Bytes(BytesPayload),
}

#[derive(Deserialize, Debug)]
struct TrpResolveParams {
    pub tir: IrEnvelope,
    pub args: HashMap<String, ParamsArgValue>,
}

fn handle_param_args(
    tx: &mut ProtoTx,
    key: &str,
    val: &ParamsArgValue,
) -> Result<(), ErrorObjectOwned> {
    match val {
        ParamsArgValue::String(s) => {
            tx.set_arg(key, s.as_str().into());
        }
        ParamsArgValue::Number(n) => {
            tx.set_arg(key, (*n).into());
        }
        ParamsArgValue::Boolean(b) => {
            tx.set_arg(key, (*b).into());
        }
        ParamsArgValue::Bytes(payload) => {
            let decoded = match payload.encoding {
                BytesEncoding::Base64 => base64::engine::general_purpose::STANDARD
                    .decode(&payload.content)
                    .map_err(|e| {
                        ErrorObject::owned(
                            ErrorCode::InvalidParams.code(),
                            format!("Invalid base64 for key '{}'", key),
                            Some(e.to_string()),
                        )
                    })?,
                BytesEncoding::Hex => hex::decode(&payload.content).map_err(|e| {
                    ErrorObject::owned(
                        ErrorCode::InvalidParams.code(),
                        format!("Invalid hex for key '{}'", key),
                        Some(e.to_string()),
                    )
                })?,
            };
            tx.set_arg(key, decoded.into());
        }
        ParamsArgValue::Null => {
            return Err(ErrorObject::owned(
                ErrorCode::InvalidParams.code(),
                format!("Null is not a valid argument for key '{}'", key),
                None::<()>,
            ));
        }
    }

    Ok(())
}

pub fn decode_params(params: Params<'_>) -> Result<ProtoTx, ErrorObjectOwned> {
    let params: TrpResolveParams = params.parse()?;

    if params.tir.version != tx3_lang::ir::IR_VERSION {
        return Err(ErrorObject::owned(
            ErrorCode::InvalidParams.code(),
            format!(
                "Unsupported IR version, expected {}",
                tx3_lang::ir::IR_VERSION
            ),
            Some(params.tir.version),
        ));
    }

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

    for (key, val) in params.args.iter() {
        handle_param_args(&mut tx, key, val)?;
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
