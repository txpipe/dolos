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

    let params = tx.find_params();

    for (key, ty) in params {
        let Some(arg) = arguments.get(&key) else {
            return Err(ErrorObject::owned(
                ErrorCode::InvalidParams.code(),
                format!("Missing argument for parameter {key} of type {ty:?}"),
                Some(serde_json::json!({ "key": key, "type": ty })),
            ));
        };

        let arg = tx3_sdk::trp::args::from_json(arg.clone(), &ty).map_err(|e| {
            ErrorObject::owned(
                ErrorCode::InvalidParams.code(),
                format!("Failed to parse argument {key} of type {ty:?}"),
                Some(serde_json::json!({ "error": e.to_string(), "value": arg })),
            )
        })?;

        tx.set_arg(&key, arg);
    }

    Ok(())
}

fn decode_params(params: Params<'_>) -> Result<ProtoTx, ErrorObjectOwned> {
    let params: TrpResolveParams = params.parse()?;

    if params.tir.version != tx3_lang::ir::IR_VERSION {
        return Err(ErrorObject::owned(
            ErrorCode::ServerError(-32000).code(),
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
    use dolos_testing::toy_domain::ToyDomain;
    use dolos_testing::TestAddress::{Alice, Bob};
    use serde_json::json;

    use crate::{metrics::Metrics, Config};

    use super::*;

    fn setup_test_context() -> Arc<Context<ToyDomain>> {
        let delta = dolos_testing::make_custom_utxo_delta(
            1,
            dolos_testing::TestAddress::everyone(),
            2..4,
            |x: &dolos_testing::TestAddress| {
                dolos_testing::utxo_with_random_amount(x, 4_000_000..5_000_000)
            },
        );

        let domain = ToyDomain::new(Some(delta));

        Arc::new(Context {
            domain,
            config: Arc::new(Config {
                max_optimize_rounds: 3,

                // next are dummy, not used
                listen_address: "[::]:1234".parse().unwrap(),
                permissive_cors: None,
            }),
            metrics: Metrics::default(),
        })
    }

    const SUBJECT_PROTOCOL: &str = r#"
        party Sender;
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
        }
    "#;

    async fn attempt_resolve(
        args: &serde_json::Value,
    ) -> Result<serde_json::Value, ErrorObjectOwned> {
        let protocol = tx3_lang::Protocol::from_string(SUBJECT_PROTOCOL.to_owned())
            .load()
            .unwrap();

        let tx = protocol.new_tx("swap").unwrap();

        let ir = tx.apply().unwrap().ir_bytes();

        let req = json!({
            "tir": {
                "version": "v1alpha6",
                "bytecode": hex::encode(ir),
                "encoding": "hex"
            },
            "args": args
        })
        .to_string();

        let params = Params::new(Some(req.as_str()));

        let context = setup_test_context();

        trp_resolve(params, context.clone()).await
    }

    #[tokio::test]
    async fn test_resolve_happy_path() {
        let args = json!({
            "quantity": 100,
            "sender": Alice.as_str(),
            "receiver": Bob.as_str(),
        });

        let resolved = attempt_resolve(&args).await.unwrap();

        let tx = hex::decode(resolved["tx"].as_str().unwrap()).unwrap();

        let _ = pallas::ledger::traverse::MultiEraTx::decode(&tx).unwrap();
    }

    #[tokio::test]
    async fn test_resolve_missing_args() {
        let args = json!({});

        let resolved = attempt_resolve(&args).await;

        let err = resolved.unwrap_err();

        dbg!(&err);

        assert_eq!(err.code(), ErrorCode::InvalidParams.code());
    }

    #[tokio::test]
    async fn test_resolve_invalid_args() {
        let args = json!({
            "quantity": "abc",
            "sender": "Alice",
            "receiver": "Bob",
        });

        let resolved = attempt_resolve(&args).await;

        let err = resolved.unwrap_err();

        dbg!(&err);

        assert_eq!(err.code(), ErrorCode::InvalidParams.code());
    }
}
