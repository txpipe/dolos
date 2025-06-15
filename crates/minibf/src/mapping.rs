use axum::{Json, http::StatusCode};
use blockfrost_openapi::models::{
    tx_content::TxContent, tx_content_cbor::TxContentCbor,
    tx_content_output_amount_inner::TxContentOutputAmountInner, tx_content_utxo::TxContentUtxo,
    tx_content_utxo_inputs_inner::TxContentUtxoInputsInner,
    tx_content_utxo_outputs_inner::TxContentUtxoOutputsInner,
};
use dolos_cardano::pparams::ChainSummary;
use dolos_core::{BlockBody, EraCbor, TxHash, TxOrder, TxoIdx};
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    ledger::{
        addresses::Address,
        primitives::conway::{DatumOption, ScriptRef},
        traverse::{
            ComputeHash, MultiEraBlock, MultiEraInput, MultiEraOutput, MultiEraTx, MultiEraValue,
            OriginalHash,
        },
    },
};
use std::collections::HashMap;

macro_rules! try_into_or_500 {
    ($expr:expr) => {
        $expr
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    };
}

pub trait IntoModel<T>
where
    T: serde::Serialize,
    Self: Sized,
{
    fn into_model(self) -> Result<T, StatusCode>;

    fn into_response(self) -> Result<Json<T>, StatusCode> {
        let tx = self.into_model()?;

        Ok(Json(tx))
    }
}

/// Resolve epoch, epoch slot and block time using Genesis values.
pub fn slot_time(slot: u64, summary: &ChainSummary) -> (u64, u64, u64) {
    let era = summary.era_for_slot(slot);

    let era_slot = slot - era.start.slot;
    let era_epoch = era_slot / era.pparams.epoch_length();
    let epoch_slot = era_slot % era.pparams.epoch_length();
    let epoch = era.start.epoch + era_epoch;
    let time = era.start.timestamp.timestamp() as u64
        + (slot - era.start.slot) * era.pparams.slot_length();

    (epoch, epoch_slot, time)
}

pub fn aggregate_amount<'a>(
    txouts: impl Iterator<Item = &'a MultiEraOutput<'a>>,
) -> Vec<TxContentOutputAmountInner> {
    let mut lovelace = 0;
    let mut by_asset: HashMap<String, u64> = HashMap::new();

    for txout in txouts {
        let value = txout.value();

        // Add lovelace amount
        lovelace += value.coin();

        // Add other assets
        for ma in value.assets() {
            for asset in ma.assets() {
                let unit = format!("{}{}", ma.policy(), hex::encode(asset.name()));
                let amount = asset.output_coin().unwrap_or_default();
                *by_asset.entry(unit).or_insert(0) += amount;
            }
        }
    }

    let lovelace = TxContentOutputAmountInner {
        unit: "lovelace".to_string(),
        quantity: lovelace.to_string(),
    };

    let mut assets: Vec<_> = by_asset
        .into_iter()
        .map(|(unit, quantity)| TxContentOutputAmountInner {
            unit,
            quantity: quantity.to_string(),
        })
        .collect();

    assets.sort_by_key(|a| a.unit.clone());

    std::iter::once(lovelace).chain(assets).collect()
}

impl IntoModel<Vec<TxContentOutputAmountInner>> for MultiEraValue<'_> {
    fn into_model(self) -> Result<Vec<TxContentOutputAmountInner>, StatusCode> {
        let mut out = vec![];

        out.push(TxContentOutputAmountInner {
            unit: "lovelace".to_string(),
            quantity: self.coin().to_string(),
        });

        for ma in self.assets() {
            for asset in ma.assets() {
                let unit = format!("{}{}", ma.policy(), hex::encode(asset.name()));
                let amount = asset.output_coin().unwrap_or_default();

                out.push(TxContentOutputAmountInner {
                    unit,
                    quantity: amount.to_string(),
                });
            }
        }

        Ok(out)
    }
}

impl IntoModel<String> for Result<Address, pallas::ledger::addresses::Error> {
    fn into_model(self) -> Result<String, StatusCode> {
        self.map(|a| a.to_string())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl<'a> IntoModel<String> for ScriptRef<'a> {
    fn into_model(self) -> Result<String, StatusCode> {
        let out = match self {
            ScriptRef::NativeScript(x) => x.original_hash(),
            ScriptRef::PlutusV1Script(x) => x.compute_hash(),
            ScriptRef::PlutusV2Script(x) => x.compute_hash(),
            ScriptRef::PlutusV3Script(x) => x.compute_hash(),
        }
        .to_string();

        Ok(out)
    }
}

pub struct UtxoOutputModelBuilder<'a> {
    tx_hash: TxHash,
    txo_idx: TxoIdx,
    output: MultiEraOutput<'a>,
    is_collateral: bool,
}

impl<'a> UtxoOutputModelBuilder<'a> {
    pub fn from_output(tx_hash: TxHash, txo_idx: TxoIdx, output: MultiEraOutput<'a>) -> Self {
        Self {
            tx_hash,
            txo_idx,
            output,
            is_collateral: false,
        }
    }

    pub fn from_collateral_return(
        tx_hash: TxHash,
        output_count: usize,
        collateral_idx: TxoIdx,
        output: MultiEraOutput<'a>,
    ) -> Self {
        Self {
            tx_hash,
            txo_idx: (output_count + collateral_idx as usize) as u32,
            output,
            is_collateral: true,
        }
    }
}

impl<'a> IntoModel<TxContentUtxoOutputsInner> for UtxoOutputModelBuilder<'a> {
    fn into_model(self) -> Result<TxContentUtxoOutputsInner, StatusCode> {
        let out = TxContentUtxoOutputsInner {
            address: self.output.address().into_model()?,
            amount: self.output.value().into_model()?,
            output_index: try_into_or_500!(self.txo_idx),
            // TODO: searching for this value is an expensive query. Judging by the official BF
            // endpoint, this is not always populated. Research in which conditions this is set.
            consumed_by_tx: Some(None),
            data_hash: self.output.datum().map(|x| match x {
                DatumOption::Hash(x) => x.to_string(),
                DatumOption::Data(x) => x.original_hash().to_string(),
            }),
            inline_datum: self
                .output
                .datum()
                .and_then(|x| match x {
                    DatumOption::Hash(_) => None,
                    DatumOption::Data(x) => Some(minicbor::to_vec(&x.0).unwrap()),
                })
                .map(hex::encode),
            collateral: self.is_collateral,
            reference_script_hash: self
                .output
                .script_ref()
                .map(|h| h.into_model())
                .transpose()?,
        };

        Ok(out)
    }
}

pub struct UtxoInputModelBuilder<'a> {
    input: MultiEraInput<'a>,
    as_output: Option<MultiEraOutput<'a>>,
    is_reference: bool,
    is_collateral: bool,
}

impl<'a> IntoModel<TxContentUtxoInputsInner> for UtxoInputModelBuilder<'a> {
    fn into_model(self) -> Result<TxContentUtxoInputsInner, StatusCode> {
        let mut out = TxContentUtxoInputsInner {
            tx_hash: self.input.hash().to_string(),
            output_index: try_into_or_500!(self.input.index()),
            collateral: self.is_collateral,
            reference: Some(self.is_reference),
            ..Default::default()
        };

        if let Some(o) = self.as_output {
            out = TxContentUtxoInputsInner {
                address: o.address().into_model()?,
                amount: o.value().into_model()?,
                reference_script_hash: o.script_ref().map(|h| h.into_model()).transpose()?,
                data_hash: o.datum().map(|x| match x {
                    DatumOption::Hash(x) => x.to_string(),
                    DatumOption::Data(x) => x.original_hash().to_string(),
                }),
                inline_datum: o
                    .datum()
                    .and_then(|x| match x {
                        DatumOption::Hash(_) => None,
                        DatumOption::Data(x) => Some(minicbor::to_vec(&x.0).unwrap()),
                    })
                    .map(hex::encode),
                ..out
            }
        }

        Ok(out)
    }
}

pub struct TxModelBuilder<'a> {
    chain: Option<ChainSummary>,
    block: MultiEraBlock<'a>,
    order: TxOrder,
    deps: HashMap<TxHash, MultiEraTx<'a>>,
}

impl<'a> TxModelBuilder<'a> {
    pub fn new(block: &'a [u8], order: TxOrder) -> Result<Self, StatusCode> {
        let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self {
            block,
            order,
            chain: None,
            deps: HashMap::new(),
        })
    }

    pub fn with_chain(self, chain: ChainSummary) -> Self {
        Self {
            chain: Some(chain),
            ..self
        }
    }

    fn tx(&self) -> Result<MultiEraTx<'_>, StatusCode> {
        let tx = self
            .block
            .txs()
            .get(self.order)
            .ok_or(StatusCode::NOT_FOUND)?
            .clone();

        Ok(tx)
    }

    fn chain_or_500(&self) -> Result<&ChainSummary, StatusCode> {
        self.chain.as_ref().ok_or(StatusCode::INTERNAL_SERVER_ERROR)
    }

    pub fn required_deps(&self) -> Result<Vec<TxHash>, StatusCode> {
        let tx = self.tx()?;

        let mut deps = vec![];

        for i in tx.inputs() {
            deps.push(*i.hash());
        }

        for i in tx.collateral() {
            deps.push(*i.hash());
        }

        for i in tx.reference_inputs() {
            deps.push(*i.hash());
        }

        let unique = deps.into_iter().unique().collect();

        Ok(unique)
    }

    pub fn load_dep(&mut self, key: TxHash, cbor: &'a EraCbor) -> Result<(), StatusCode> {
        let era = try_into_or_500!(cbor.0);

        let tx = MultiEraTx::decode_for_era(era, &cbor.1)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        self.deps.insert(key, tx);

        Ok(())
    }

    fn into_input_builder<'b, 'c>(
        &'b self,
        input: MultiEraInput<'c>,
        is_collateral: bool,
        is_reference: bool,
    ) -> UtxoInputModelBuilder<'c>
    where
        'b: 'c,
    {
        let tx = self.deps.get(input.hash());
        let as_output = tx.and_then(|tx| tx.output_at(input.index() as usize));

        UtxoInputModelBuilder {
            input,
            as_output,
            is_reference,
            is_collateral,
        }
    }
}

impl<'a> IntoModel<TxContentUtxo> for TxModelBuilder<'a> {
    fn into_model(self) -> Result<TxContentUtxo, StatusCode> {
        let tx = self.tx()?;

        let mut reference: Vec<_> = tx
            .reference_inputs()
            .into_iter()
            .map(|i| self.into_input_builder(i, false, true))
            .map(|b| b.into_model())
            .try_collect()?;

        reference.sort_by_key(|i| (i.tx_hash.clone(), i.output_index));

        let mut inputs: Vec<_> = tx
            .inputs()
            .into_iter()
            .map(|i| self.into_input_builder(i, false, false))
            .map(|b| b.into_model())
            .try_collect()?;

        inputs.sort_by_key(|i| (i.tx_hash.clone(), i.output_index));

        let mut collateral_inputs: Vec<_> = tx
            .collateral()
            .into_iter()
            .map(|i| self.into_input_builder(i, true, false))
            .map(|b| b.into_model())
            .try_collect()?;

        collateral_inputs.sort_by_key(|i| (i.tx_hash.clone(), i.output_index));

        let all_inputs = reference
            .into_iter()
            .chain(inputs)
            .chain(collateral_inputs)
            .collect();

        let outputs: Vec<_> = tx
            .outputs()
            .into_iter()
            .enumerate()
            .map(|(i, o)| UtxoOutputModelBuilder::from_output(tx.hash(), i as u32, o).into_model())
            .try_collect()?;

        let collateral_outputs: Vec<_> = tx
            .collateral_return()
            .into_iter()
            .enumerate()
            .map(|(i, o)| {
                UtxoOutputModelBuilder::from_collateral_return(
                    tx.hash(),
                    outputs.len(),
                    i as u32,
                    o,
                )
                .into_model()
            })
            .try_collect()?;

        let all_outputs = outputs.into_iter().chain(collateral_outputs).collect();

        Ok(TxContentUtxo {
            hash: tx.hash().to_string(),
            inputs: all_inputs,
            outputs: all_outputs,
        })
    }
}

impl IntoModel<TxContent> for TxModelBuilder<'_> {
    fn into_model(self) -> Result<TxContent, StatusCode> {
        let tx = self.tx()?;
        let block = &self.block;
        let order = self.order;
        let txin = tx.inputs();
        let txouts = tx.outputs();
        let chain = self.chain_or_500()?;

        let (_, _, block_time) = slot_time(block.slot(), &chain);

        let tx = TxContent {
            hash: tx.hash().to_string(),
            block: block.hash().to_string(),
            block_height: try_into_or_500!(block.number()),
            slot: try_into_or_500!(block.slot()),
            index: try_into_or_500!(order),
            output_amount: aggregate_amount(txouts.iter()),
            fees: tx.fee().map(|f| f.to_string()).unwrap_or_default(),
            size: try_into_or_500!(tx.size()),
            invalid_before: tx.validity_start().map(|v| v.to_string()),
            invalid_hereafter: tx.ttl().map(|v| v.to_string()),
            utxo_count: try_into_or_500!(txin.len() + txouts.len()),
            redeemer_count: try_into_or_500!(tx.redeemers().len()),
            valid_contract: tx.is_valid(),
            block_time: try_into_or_500!(block_time),
            withdrawal_count: 0,         // TODO
            mir_cert_count: 0,           // TODO
            delegation_count: 0,         // TODO
            stake_cert_count: 0,         // TODO
            pool_update_count: 0,        // TODO
            pool_retire_count: 0,        // TODO
            asset_mint_or_burn_count: 0, // TODO
            deposit: "0".to_string(),    // TODO
        };

        Ok(tx)
    }
}

impl IntoModel<TxContentCbor> for TxModelBuilder<'_> {
    fn into_model(self) -> Result<TxContentCbor, StatusCode> {
        let tx = self.tx()?;

        let cbor = tx.encode();
        let cbor = hex::encode(&cbor);

        let tx = TxContentCbor { cbor };

        Ok(tx)
    }
}
