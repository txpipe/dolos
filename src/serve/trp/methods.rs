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
            serde_json::Value::Number(x) => tx.set_arg(key, x.as_i64().unwrap().into()),
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
    let tx = decode_params(params)?;
    let resolved = tx3_cardano::resolve_tx::<Context>(tx, (*context).clone(), 0)
        .await
        .map_err(|err| {
            ErrorObject::owned(
                ErrorCode::InvalidParams.code(),
                "Invalid argument",
                Some(err.to_string()),
            )
        })?;

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
            .map(|(txoref, eracbor)| {
                let parsed = MultiEraOutput::try_from(&eracbor)
                    .map_err(|err| tx3_cardano::Error::LedgerInternalError(err.to_string()))?;
                Ok(tx3_lang::Utxo {
                    r#ref: tx3_lang::UtxoRef {
                        txid: txoref.0.to_vec(),
                        index: txoref.1,
                    },
                    address: parsed
                        .address()
                        .map(|x| x.to_vec())
                        .map_err(tx3_cardano::Error::InvalidAddress)?,
                    datum: None,
                    assets: vec![tx3_lang::ir::AssetExpr {
                        policy: vec![],
                        asset_name: tx3_lang::ir::Expression::Bytes(vec![]),
                        amount: tx3_lang::ir::Expression::Number(parsed.value().coin() as i128),
                    }],
                    script: None,
                })
            })
            .collect()
    }
}
