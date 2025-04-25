use base64::{engine::general_purpose::STANDARD, Engine};
use itertools::Itertools;
use jsonrpsee::types::{ErrorCode, ErrorObject, ErrorObjectOwned, Params};
use pallas::ledger::{
    primitives::NetworkId,
    traverse::{MultiEraOutput, MultiEraUpdate},
};
use serde::Deserialize;
use std::sync::Arc;
use tx3_lang::{ir::Expression, ProtoTx};

use crate::ledger::{pparams, EraCbor, TxoRef};

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
        let mut result: Option<tx3_lang::UtxoSet> = None;

        let address = match &query.address {
            Some(Expression::Address(address)) => Some(address.clone()),
            Some(Expression::String(address)) => Some(
                pallas::ledger::addresses::Address::from_bech32(address)
                    .map_err(tx3_cardano::Error::InvalidAddress)?
                    .to_vec(),
            ),
            _ => None,
        };

        if let Some(address) = &address {
            let refs = self
                .ledger
                .get_utxo_by_address(address)
                .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;
            let set = self
                .ledger
                .get_utxos(refs.into_iter().collect())
                .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?
                .into_iter()
                .map(|(txoref, eracbor)| into_tx3_utxo(&txoref, &eracbor))
                .collect::<Result<tx3_lang::UtxoSet, _>>()?;
            match result {
                Some(set) => result = Some(set.intersection(&set).cloned().collect()),
                None => result = Some(set),
            }
        }

        let mut min_lovelace_filter = None;

        if let Some(Expression::Assets(min_amount)) = &query.min_amount {
            for expr in min_amount {
                let policy = match &expr.policy {
                    Expression::Bytes(policy) => Some(policy),
                    _ => None,
                };
                let asset_name = match &expr.asset_name {
                    Expression::Bytes(asset_name) => Some(asset_name),
                    _ => None,
                };
                let amount = match expr.amount {
                    Expression::Number(amount) => amount,
                    _ => return Err(tx3_cardano::Error::MissingAmount),
                };

                match (policy, asset_name, amount) {
                    // Minimum lovelace in UTxO. Will be applied last.
                    (None, None, amount) => min_lovelace_filter = Some(amount),
                    (Some(policy), None, amount) => {
                        let refs = self.ledger.get_utxo_by_policy(policy).map_err(|err| {
                            tx3_cardano::Error::LedgerInternalError(err.to_string())
                        })?;
                        let utxos = self
                            .ledger
                            .get_utxos(refs.into_iter().collect())
                            .map_err(|err| {
                                tx3_cardano::Error::LedgerInternalError(err.to_string())
                            })?
                            .iter()
                            .map(|(txoref, eracbor)| into_tx3_utxo(txoref, eracbor))
                            .filter_ok(|utxo| {
                                let assetexpr =
                                    utxo.assets.iter().find(|asset| match &asset.policy {
                                        Expression::Bytes(x) => x == policy,
                                        _ => false,
                                    });
                                match assetexpr {
                                    Some(assetexpr) => match assetexpr.amount {
                                        Expression::Number(y) => y > amount,
                                        _ => false,
                                    },
                                    _ => false,
                                }
                            })
                            .collect::<Result<tx3_lang::UtxoSet, _>>()?;
                        match result {
                            Some(set) => result = Some(set.intersection(&utxos).cloned().collect()),
                            None => result = Some(utxos),
                        }
                    }
                    (Some(policy), Some(asset_name), amount) => {
                        let mut asset = Vec::new();
                        asset.extend_from_slice(policy.as_slice());
                        asset.extend_from_slice(asset_name.as_slice());

                        let refs = self.ledger.get_utxo_by_asset(&asset).map_err(|err| {
                            tx3_cardano::Error::LedgerInternalError(err.to_string())
                        })?;
                        let utxos = self
                            .ledger
                            .get_utxos(refs.into_iter().collect())
                            .map_err(|err| {
                                tx3_cardano::Error::LedgerInternalError(err.to_string())
                            })?
                            .iter()
                            .map(|(txoref, eracbor)| into_tx3_utxo(txoref, eracbor))
                            .filter_ok(|utxo| {
                                let assetexpr = utxo.assets.iter().find(|asset| {
                                    match (&asset.policy, &asset.asset_name) {
                                        (Expression::Bytes(x), Expression::Bytes(y)) => {
                                            x == policy && y == asset_name
                                        }
                                        _ => false,
                                    }
                                });
                                match assetexpr {
                                    Some(assetexpr) => match assetexpr.amount {
                                        Expression::Number(y) => y > amount,
                                        _ => false,
                                    },
                                    _ => false,
                                }
                            })
                            .collect::<Result<tx3_lang::UtxoSet, _>>()?;

                        match result {
                            Some(set) => result = Some(set.intersection(&utxos).cloned().collect()),
                            None => result = Some(utxos),
                        }
                    }
                    (_, Some(_), _) => {
                        return Err(tx3_cardano::Error::InvalidAssetExpression(
                            "policy must be defined when asset name is not None".to_string(),
                        ))
                    }
                }
            }
        }

        // TODO: think about the position of this block
        // ref have precedence
        if let Some(Expression::UtxoRefs(refs)) = &query.r#ref {
            let utxos = self
                .ledger
                .get_utxos(
                    refs.iter()
                        .map(|r#ref| TxoRef {
                            0: pallas::ledger::primitives::Hash::<32>::from(r#ref.txid.as_slice()),
                            1: r#ref.index,
                        })
                        .collect(),
                )
                .unwrap()
                .iter()
                .map(|(txoref, eracbor)| into_tx3_utxo(txoref, eracbor))
                .collect::<Result<tx3_lang::UtxoSet, _>>()?;
            if !utxos.is_empty() {
                return Ok(utxos);
            }
        }

        let Some(mut result) = result else {
            return Ok(tx3_lang::UtxoSet::new());
        };

        // Finally, filter by lovelace is needed
        if let Some(min_lovelace_filter) = min_lovelace_filter {
            result.retain(|utxo| {
                let assetexpr = utxo
                    .assets
                    .iter()
                    .find(|asset| matches!(&asset.policy, Expression::None));
                match assetexpr {
                    Some(assetexpr) => match assetexpr.amount {
                        Expression::Number(y) => y > min_lovelace_filter,
                        _ => false,
                    },
                    _ => false,
                }
            });
        }

        Ok(result)
    }
}

pub fn health(context: &Context) -> bool {
    context.ledger.cursor().is_ok()
}

pub fn into_tx3_utxo(
    txoref: &TxoRef,
    eracbor: &EraCbor,
) -> Result<tx3_lang::Utxo, tx3_cardano::Error> {
    let parsed = MultiEraOutput::try_from(eracbor)
        .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;

    let coin = parsed.value().coin() as i128;
    let address = parsed
        .address()
        .map_err(tx3_cardano::Error::InvalidAddress)?
        .to_vec();

    let mut assets = vec![tx3_lang::ir::AssetExpr {
        policy: Expression::None,
        asset_name: Expression::None,
        amount: Expression::Number(coin),
    }];

    assets.extend(parsed.value().assets().into_iter().flat_map(|x| {
        let policy = x.policy().to_vec();
        x.assets()
            .into_iter()
            .map(|y| {
                let asset_name = y.name().to_vec();
                let amount = y.any_coin();
                tx3_lang::ir::AssetExpr {
                    policy: Expression::Bytes(policy.clone()),
                    asset_name: Expression::Bytes(asset_name),
                    amount: Expression::Number(amount),
                }
            })
            .collect::<Vec<_>>()
    }));

    Ok(tx3_lang::Utxo {
        r#ref: tx3_lang::UtxoRef {
            txid: txoref.0.to_vec(),
            index: txoref.1,
        },
        address,
        datum: match parsed.datum() {
            Some(pallas::ledger::primitives::conway::DatumOption::Data(x)) => {
                Some(Expression::Bytes(x.raw_cbor().to_vec()))
            }
            _ => None,
        },
        assets,
        script: None,
    })
}
