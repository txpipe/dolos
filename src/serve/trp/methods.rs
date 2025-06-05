use base64::{engine::general_purpose::STANDARD, Engine};
use jsonrpsee::types::{ErrorCode, ErrorObject, ErrorObjectOwned, Params};
use serde::Deserialize;
use std::sync::Arc;
use tx3_lang::ProtoTx;

use dolos_core::{Domain, StateStore as _};

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

#[derive(Deserialize, Debug)]
struct TrpResolveParams {
    pub tir: IrEnvelope,
    pub args: serde_json::Value,
}

fn handle_param_args(tx: &mut ProtoTx, args: &serde_json::Value) -> Result<(), ErrorObjectOwned> {
    let Some(arguments) = args.as_object() else {
        return Err(ErrorObject::owned(
            ErrorCode::InvalidParams.code(),
            "Failed to parse arguments as object.",
            None as Option<String>,
        ));
    };

    for (key, val) in arguments.iter() {
        match val {
            serde_json::Value::Bool(v) => tx.set_arg(key, (*v).into()),
            serde_json::Value::Number(v) => tx.set_arg(
                key,
                match v.as_i64() {
                    Some(i) => i.into(),
                    None => {
                        return Err(ErrorObject::owned(
                            ErrorCode::InvalidParams.code(),
                            "Argument cannot be cast as i64",
                            Some(serde_json::json!({ "key": key, "value": val })),
                        ))
                    }
                },
            ),
            serde_json::Value::String(v) => {
                let arg = if let Some(hex_str) = v.strip_prefix("0x") {
                    hex::decode(hex_str)
                        .map_err(|_| {
                            ErrorObject::owned(
                                ErrorCode::InvalidParams.code(),
                                "Invalid hex string",
                                Some(serde_json::json!({ "key": key, "value": val })),
                            )
                        })?
                        .into()
                } else {
                    v.as_str().into()
                };

                tx.set_arg(key, arg);
            }
            serde_json::Value::Object(v) => {
                let obj = serde_json::Value::Object(v.clone());
                let Ok(v) = serde_json::from_value::<BytesPayload>(obj) else {
                    return Err(ErrorObject::owned(
                        ErrorCode::InvalidParams.code(),
                        "Invalid object type",
                        Some(serde_json::json!({ "key": key, "value": val })),
                    ));
                };

                let decoded = match v.encoding {
                    BytesEncoding::Base64 => base64::engine::general_purpose::STANDARD
                        .decode(&v.content)
                        .map_err(|_| {
                            ErrorObject::owned(
                                ErrorCode::InvalidParams.code(),
                                "Invalid base64 content",
                                Some(serde_json::json!({ "key": key, "value": val })),
                            )
                        })?,
                    BytesEncoding::Hex => hex::decode(&v.content).map_err(|_| {
                        ErrorObject::owned(
                            ErrorCode::InvalidParams.code(),
                            "Invalid hex content",
                            Some(serde_json::json!({ "key": key, "value": val })),
                        )
                    })?,
                };
                tx.set_arg(key, decoded.into());
            }
            _ => {
                return Err(ErrorObject::owned(
                    ErrorCode::InvalidParams.code(),
                    "Invalid argument",
                    Some(serde_json::json!({ "key": key, "value": val })),
                ))
            }
        }
    }

    Ok(())
}

fn decode_params(params: Params<'_>) -> Result<ProtoTx, ErrorObjectOwned> {
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

    handle_param_args(&mut tx, &params.args)?;

    Ok(tx)
}

pub async fn trp_resolve<D: Domain>(
    params: Params<'_>,
    context: Arc<Context<D>>,
) -> Result<serde_json::Value, ErrorObjectOwned> {
    tracing::info!(method = "trp.resolve", "Received TRP request.");
    let tx = match decode_params(params) {
        Ok(tx) => tx,
        Err(err) => {
            tracing::warn!(err = ?err, "Failed to decode params.");
            return Err(err);
        }
    };

    let resolved = tx3_cardano::resolve_tx::<Context<D>>(
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

pub fn health<D: Domain>(context: &Context<D>) -> bool {
    context.domain.state().cursor().is_ok()
}
