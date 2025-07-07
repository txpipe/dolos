use base64::{Engine, engine::general_purpose::STANDARD};
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
                        ));
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
                ));
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
                "Unsupported IR version, expected {}. Make sure you have the latest version of the tx3 toolchain",
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

#[cfg(test)]
mod tests {
    use dolos_testing::TestAddress::{Alice, Bob};
    use dolos_testing::toy_domain::ToyDomain;
    use serde_json::json;

    use crate::{Config, metrics::Metrics};

    use super::*;

    #[tokio::test]
    async fn test_resolve_happy_path() {
        let protocol = tx3_lang::Protocol::from_string(
            "party Sender;
            party Receiver;
            tx swap(quantity: Int) {
              input source {
                from: Sender,
                min_amount: Ada(quantity) + fees,
              }

              output {
                to: Receiver,
                amount: Ada(quantity),
              }

              output {
                to: Sender,
                amount: source - Ada(quantity) - fees,
              }
            }"
            .to_string(),
        )
        .load()
        .unwrap();

        let tx = protocol
            .new_tx("swap")
            .unwrap()
            .with_arg("quantity", 100.into())
            .with_arg("sender", Alice.to_bytes().into())
            .with_arg("receiver", Bob.to_bytes().into())
            .apply()
            .unwrap();

        let ir = tx.apply().unwrap().ir_bytes();

        let req = json!({
            "tir": {
                "version": "v1alpha6",
                "bytecode": hex::encode(ir),
                "encoding": "hex"
            },
            "args": {}
        })
        .to_string();

        let params = Params::new(Some(req.as_str()));

        let delta = dolos_testing::make_custom_utxo_delta(
            1,
            dolos_testing::TestAddress::everyone(),
            2..4,
            |x: &dolos_testing::TestAddress| {
                dolos_testing::utxo_with_random_amount(x, 4_000_000..5_000_000)
            },
        );

        let domain = ToyDomain::new(Some(delta));

        let context = Arc::new(Context {
            domain,
            config: Arc::new(Config {
                max_optimize_rounds: 3,

                // next are dummy, not used
                listen_address: "[::]:1234".parse().unwrap(),
                permissive_cors: None,
            }),
            metrics: Metrics::default(),
        });

        let resolved = trp_resolve(params, context.clone()).await.unwrap();

        let tx = hex::decode(resolved["tx"].as_str().unwrap()).unwrap();

        let _ = pallas::ledger::traverse::MultiEraTx::decode(&tx).unwrap();
    }
}
