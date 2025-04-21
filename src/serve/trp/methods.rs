use base64::{engine::general_purpose::STANDARD, Engine};
use jsonrpsee::types::{ErrorCode, ErrorObject, ErrorObjectOwned, Params};
use pallas::ledger::{
    primitives::NetworkId,
    traverse::{MultiEraOutput, MultiEraUpdate},
};
use serde::Deserialize;
use std::sync::Arc;
use tx3_lang::ProtoTx;

use crate::ledger::pparams;

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
    let resolved = match tx3_cardano::resolve_tx::<Context>(
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
    }) {
        Ok(resolved) => resolved,
        Err(err) => {
            tracing::warn!(err = ?err, "Failed to resolve tx.");
            return Err(err);
        }
    };

    Ok(serde_json::json!({ "tx": hex::encode(resolved.payload) }))
}

impl tx3_cardano::Ledger for Context {
    async fn get_pparams(&self) -> Result<tx3_cardano::PParams, tx3_cardano::Error> {
        let tip = self
            .ledger
            .cursor()
            .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

        let updates = self
            .ledger
            .get_pparams(tip.as_ref().map(|p| p.0).unwrap_or_default())
            .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

        let updates: Vec<_> = updates
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<MultiEraUpdate>, pallas::codec::minicbor::decode::Error>>()
            .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

        let summary = pparams::fold_with_hacks(&self.genesis, &updates, tip.as_ref().unwrap().0);
        let era = summary.era_for_slot(tip.as_ref().unwrap().0);
        let mapper = pallas::interop::utxorpc::Mapper::new(self.ledger.clone());
        let params = mapper.map_pparams(era.pparams.clone());

        Ok(tx3_cardano::PParams {
            network: match self.genesis.shelley.network_id.as_ref() {
                Some(network) => match network.as_str() {
                    "Mainnet" => Some(NetworkId::Mainnet),
                    "Testnet" => Some(NetworkId::Testnet),
                    _ => None,
                },
                None => None,
            }
            .unwrap(),
            min_fee_coefficient: params.min_fee_coefficient,
            min_fee_constant: params.min_fee_constant,
            coins_per_utxo_byte: params.coins_per_utxo_byte,
        })
    }

    async fn resolve_input(
        &self,
        query: &tx3_lang::ir::InputQuery,
    ) -> Result<tx3_lang::UtxoSet, tx3_cardano::Error> {
        let address = match &query.address {
            Some(tx3_lang::ir::Expression::Address(address)) => address.clone(),
            Some(tx3_lang::ir::Expression::String(address)) => {
                pallas::ledger::addresses::Address::from_bech32(address)
                    .map_err(tx3_cardano::Error::InvalidAddress)?
                    .to_vec()
            }
            _ => return Err(tx3_cardano::Error::MissingAddress),
        };

        let refs = self
            .ledger
            .get_utxo_by_address(&address)
            .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

        let utxos = self
            .ledger
            .get_utxos(refs.into_iter().collect())
            .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

        utxos
            .into_iter()
            .filter_map(|(txoref, eracbor)| {
                let parsed = match MultiEraOutput::try_from(&eracbor) {
                    Ok(parsed) => parsed,
                    Err(err) => {
                        return Some(Err(tx3_cardano::Error::LedgerInternalError(
                            err.to_string(),
                        )))
                    }
                };

                let coin = parsed.value().coin() as i128;
                if let Some(tx3_lang::ir::Expression::Number(amount)) = &query.min_amount {
                    if coin < *amount {
                        println!("coin: {coin}");
                        println!("amount: {amount}");
                        return None;
                    }
                };

                let address = match parsed.address() {
                    Ok(address) => address.to_vec(),
                    Err(err) => return Some(Err(tx3_cardano::Error::InvalidAddress(err))),
                };

                let mut assets = vec![tx3_lang::ir::AssetExpr {
                    policy: tx3_lang::ir::Expression::None,
                    asset_name: tx3_lang::ir::Expression::None,
                    amount: tx3_lang::ir::Expression::Number(coin),
                }];
                assets.extend(parsed.value().assets().into_iter().flat_map(|x| {
                    let policy = x.policy().to_vec();
                    x.assets()
                        .into_iter()
                        .map(|y| {
                            let asset_name = y.name().to_vec();
                            let amount = y.output_coin();
                            tx3_lang::ir::AssetExpr {
                                policy: tx3_lang::ir::Expression::Bytes(policy.clone()),
                                asset_name: tx3_lang::ir::Expression::Bytes(asset_name),
                                amount: match amount {
                                    Some(amount) => tx3_lang::ir::Expression::Number(amount.into()),
                                    None => tx3_lang::ir::Expression::None,
                                },
                            }
                        })
                        .collect::<Vec<_>>()
                }));

                Some(Ok(tx3_lang::Utxo {
                    r#ref: tx3_lang::UtxoRef {
                        txid: txoref.0.to_vec(),
                        index: txoref.1,
                    },
                    address,
                    datum: match parsed.datum() {
                        Some(pallas::ledger::primitives::conway::DatumOption::Data(x)) => {
                            Some(tx3_lang::ir::Expression::Bytes(x.raw_cbor().to_vec()))
                        }
                        _ => None,
                    },
                    assets,
                    script: None,
                }))
            })
            .collect()
    }
}

pub fn health(context: &Context) -> bool {
    context.ledger.cursor().is_ok()
}
