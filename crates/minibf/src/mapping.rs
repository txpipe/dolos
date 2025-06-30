use axum::{Json, http::StatusCode};
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::{
        addresses::Address,
        primitives::{
            alonzo::{self, Certificate},
            conway::{DatumOption, ScriptRef},
        },
        traverse::{
            ComputeHash, MultiEraBlock, MultiEraHeader, MultiEraInput, MultiEraOutput, MultiEraTx,
            MultiEraValue, OriginalHash,
        },
    },
};
use std::collections::HashMap;

use blockfrost_openapi::models::{
    address_utxo_content_inner::AddressUtxoContentInner, block_content::BlockContent,
    tx_content::TxContent, tx_content_cbor::TxContentCbor,
    tx_content_metadata_cbor_inner::TxContentMetadataCborInner,
    tx_content_metadata_inner::TxContentMetadataInner,
    tx_content_metadata_inner_json_metadata::TxContentMetadataInnerJsonMetadata,
    tx_content_output_amount_inner::TxContentOutputAmountInner, tx_content_utxo::TxContentUtxo,
    tx_content_utxo_inputs_inner::TxContentUtxoInputsInner,
    tx_content_utxo_outputs_inner::TxContentUtxoOutputsInner,
    tx_content_withdrawals_inner::TxContentWithdrawalsInner,
};

use dolos_cardano::pparams::ChainSummary;
use dolos_core::{EraCbor, TxHash, TxOrder, TxoIdx};

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
    type SortKey: Ord + Clone + Default + Sized;

    fn sort_key(&self) -> Option<Self::SortKey> {
        None
    }

    fn into_model(self) -> Result<T, StatusCode>;

    fn into_model_with_sort_key(self) -> Result<(Self::SortKey, T), StatusCode> {
        let sort_key = self.sort_key().unwrap_or_default();
        let model = self.into_model()?;
        Ok((sort_key, model))
    }

    fn into_response(self) -> Result<Json<T>, StatusCode> {
        let tx = self.into_model()?;

        Ok(Json(tx))
    }
}

/// Resolve epoch, epoch slot and block time using Genesis values and return
/// them as BF expects them as i32.
pub fn slot_time(slot: u64, summary: &ChainSummary) -> (i32, i32, i32) {
    let era = summary.era_for_slot(slot);

    let era_slot = slot - era.start.slot;
    let era_epoch = era_slot / era.pparams.epoch_length();
    let epoch_slot = era_slot % era.pparams.epoch_length();
    let epoch = era.start.epoch + era_epoch;
    let time = era.start.timestamp.timestamp() as u64
        + (slot - era.start.slot) * era.pparams.slot_length();

    (epoch as i32, epoch_slot as i32, time as i32)
}

#[allow(unused)]
pub fn aggregate_assets<'a>(
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

pub fn list_assets<'a>(
    txouts: impl Iterator<Item = &'a MultiEraOutput<'a>>,
) -> Vec<TxContentOutputAmountInner> {
    let mut lovelace = 0;
    let mut assets: Vec<TxContentOutputAmountInner> = vec![];

    for txout in txouts {
        let value = txout.value();

        // Add lovelace amount
        lovelace += value.coin();

        // Add other assets
        for ma in value.assets() {
            for asset in ma.assets() {
                let unit = format!("{}{}", ma.policy(), hex::encode(asset.name()));
                let amount = asset.output_coin().unwrap_or_default();
                assets.push(TxContentOutputAmountInner {
                    unit,
                    quantity: amount.to_string(),
                });
            }
        }
    }

    let lovelace = TxContentOutputAmountInner {
        unit: "lovelace".to_string(),
        quantity: lovelace.to_string(),
    };

    assets.sort_by_key(|a| a.unit.clone());

    std::iter::once(lovelace).chain(assets).collect()
}

impl IntoModel<Vec<TxContentOutputAmountInner>> for MultiEraValue<'_> {
    type SortKey = ();

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
    type SortKey = ();

    fn into_model(self) -> Result<String, StatusCode> {
        self.map(|a| a.to_string())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}

impl<'a> IntoModel<String> for ScriptRef<'a> {
    type SortKey = ();

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
    txo_idx: TxoIdx,
    output: MultiEraOutput<'a>,
    is_collateral: bool,
    block: Option<MultiEraBlock<'a>>,
    tx_order: Option<TxOrder>,
}

impl<'a> UtxoOutputModelBuilder<'a> {
    pub fn from_output(txo_idx: TxoIdx, output: MultiEraOutput<'a>) -> Self {
        Self {
            txo_idx,
            output,
            is_collateral: false,
            block: None,
            tx_order: None,
        }
    }

    pub fn from_collateral(
        output_count: usize,
        collateral_idx: TxoIdx,
        output: MultiEraOutput<'a>,
    ) -> Self {
        Self {
            txo_idx: (output_count + collateral_idx as usize) as u32,
            output,
            is_collateral: true,
            block: None,
            tx_order: None,
        }
    }

    pub fn with_block_data(self, block: MultiEraBlock<'a>, tx_order: TxOrder) -> Self {
        Self {
            block: Some(block),
            tx_order: Some(tx_order),
            ..self
        }
    }

    pub fn find_tx(&self) -> Option<MultiEraTx<'_>> {
        let txs = self.block.as_ref()?.txs();
        let order = self.tx_order?;

        txs.get(order).cloned()
    }
}

impl<'a> IntoModel<TxContentUtxoOutputsInner> for UtxoOutputModelBuilder<'a> {
    type SortKey = u64;

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

impl<'a> IntoModel<AddressUtxoContentInner> for UtxoOutputModelBuilder<'a> {
    type SortKey = (u64, usize, u32);

    fn sort_key(&self) -> Option<Self::SortKey> {
        match (self.block.as_ref(), self.tx_order.as_ref()) {
            (Some(block), Some(txorder)) => Some((block.slot(), *txorder, self.txo_idx)),
            _ => None,
        }
    }

    fn into_model(self) -> Result<AddressUtxoContentInner, StatusCode> {
        let out = AddressUtxoContentInner {
            address: self.output.address().into_model()?,
            tx_hash: self
                .find_tx()
                .map(|tx| tx.hash().to_string())
                .unwrap_or_default(),
            block: self
                .block
                .as_ref()
                .map(|b| b.hash().to_string())
                .unwrap_or_default(),
            output_index: try_into_or_500!(self.txo_idx),
            amount: self.output.value().into_model()?,
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
            reference_script_hash: self
                .output
                .script_ref()
                .map(|h| h.into_model())
                .transpose()?,

            // DEPRECATED
            tx_index: try_into_or_500!(self.txo_idx),
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
    type SortKey = ();

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

    fn new_input_builder<'b, 'c>(
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
    type SortKey = ();

    fn into_model(self) -> Result<TxContentUtxo, StatusCode> {
        let tx = self.tx()?;

        let mut reference: Vec<_> = tx
            .reference_inputs()
            .into_iter()
            .map(|i| self.new_input_builder(i, false, true))
            .map(|b| b.into_model())
            .try_collect()?;

        reference.sort_by_key(|i| (i.tx_hash.clone(), i.output_index));

        let mut inputs: Vec<_> = tx
            .inputs()
            .into_iter()
            .map(|i| self.new_input_builder(i, false, false))
            .map(|b| b.into_model())
            .try_collect()?;

        inputs.sort_by_key(|i| (i.tx_hash.clone(), i.output_index));

        let mut collateral_inputs: Vec<_> = tx
            .collateral()
            .into_iter()
            .map(|i| self.new_input_builder(i, true, false))
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
            .map(|(i, o)| UtxoOutputModelBuilder::from_output(i as u32, o))
            .map(|b| b.into_model())
            .try_collect()?;

        let collateral_outputs: Vec<_> = tx
            .collateral_return()
            .into_iter()
            .enumerate()
            .map(|(i, o)| UtxoOutputModelBuilder::from_collateral(outputs.len(), i as u32, o))
            .map(|b| b.into_model())
            .try_collect()?;

        let all_outputs = outputs.into_iter().chain(collateral_outputs).collect();

        Ok(TxContentUtxo {
            hash: tx.hash().to_string(),
            inputs: all_inputs,
            outputs: all_outputs,
        })
    }
}

macro_rules! count_certs {
    ($tx:expr, "alonzo", $cert:ident) => {
        $tx.certs()
            .iter()
            .map(|x| x.as_alonzo())
            .flatten()
            .filter(|x| matches!(x, Certificate::$cert { .. }))
            .count() as i32
    };
}

impl IntoModel<TxContent> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<TxContent, StatusCode> {
        let tx = self.tx()?;
        let block = &self.block;
        let order = self.order;
        let txin = tx.inputs();
        let txouts = tx.outputs();
        let chain = self.chain_or_500()?;

        let (_, _, block_time) = slot_time(block.slot(), chain);

        let tx = TxContent {
            hash: tx.hash().to_string(),
            block: block.hash().to_string(),
            block_height: try_into_or_500!(block.number()),
            slot: try_into_or_500!(block.slot()),
            index: try_into_or_500!(order),
            output_amount: list_assets(txouts.iter()),
            fees: tx.fee().map(|f| f.to_string()).unwrap_or_default(),
            size: try_into_or_500!(tx.size()),
            invalid_before: tx.validity_start().map(|v| v.to_string()),
            invalid_hereafter: tx.ttl().map(|v| v.to_string()),
            utxo_count: try_into_or_500!(txin.len() + txouts.len()),
            redeemer_count: try_into_or_500!(tx.redeemers().len()),
            valid_contract: tx.is_valid(),
            block_time: try_into_or_500!(block_time),
            withdrawal_count: tx.withdrawals().collect::<Vec<_>>().len() as i32,
            mir_cert_count: count_certs!(tx, "alonzo", MoveInstantaneousRewardsCert),
            delegation_count: count_certs!(tx, "alonzo", StakeDelegation),
            stake_cert_count: count_certs!(tx, "alonzo", StakeRegistration),
            pool_update_count: count_certs!(tx, "alonzo", PoolRegistration),
            pool_retire_count: count_certs!(tx, "alonzo", PoolRetirement),
            asset_mint_or_burn_count: tx.mints().iter().flat_map(|x| x.assets()).count() as i32,
            // TODO: need to understand exactly what this means in terms of the transaction
            deposit: "0".to_string(),
        };

        Ok(tx)
    }
}

impl IntoModel<TxContentCbor> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentCbor, StatusCode> {
        let tx = self.tx()?;

        let cbor = tx.encode();
        let cbor = hex::encode(&cbor);

        let tx = TxContentCbor { cbor };

        Ok(tx)
    }
}

impl IntoModel<String> for alonzo::Metadatum {
    type SortKey = ();

    fn into_model(self) -> Result<String, StatusCode> {
        let out = match self {
            alonzo::Metadatum::Int(x) => x.to_string(),
            alonzo::Metadatum::Bytes(x) => hex::encode(x.as_slice()),
            alonzo::Metadatum::Text(x) => x.to_string(),
            alonzo::Metadatum::Array(_) => "array".to_string(),
            alonzo::Metadatum::Map(_) => "map".to_string(),
        };

        Ok(out)
    }
}

impl IntoModel<serde_json::Value> for alonzo::Metadatum {
    type SortKey = ();

    fn into_model(self) -> Result<serde_json::Value, StatusCode> {
        let out = match self {
            alonzo::Metadatum::Int(x) => serde_json::Value::String(x.to_string()),
            alonzo::Metadatum::Text(x) => serde_json::Value::String(x.to_string()),
            alonzo::Metadatum::Bytes(x) => {
                let hex_str = hex::encode(x.as_slice());

                serde_json::Value::String(hex_str)
            }
            alonzo::Metadatum::Array(x) => {
                let items: Vec<_> = x.into_iter().map(|x| x.into_model()).try_collect()?;

                serde_json::Value::Array(items)
            }
            alonzo::Metadatum::Map(x) => {
                let items: serde_json::Map<String, serde_json::Value> = x
                    .iter()
                    .map(|(k, v)| Ok((k.clone().into_model()?, v.clone().into_model()?)))
                    .try_collect()
                    .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

                serde_json::Value::Object(items)
            }
        };

        Ok(out)
    }
}

impl IntoModel<TxContentMetadataInnerJsonMetadata> for alonzo::Metadatum {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentMetadataInnerJsonMetadata, StatusCode> {
        let out = match self {
            alonzo::Metadatum::Int(x) => TxContentMetadataInnerJsonMetadata::String(x.to_string()),
            alonzo::Metadatum::Bytes(x) => {
                TxContentMetadataInnerJsonMetadata::String(hex::encode(x.as_slice()))
            }
            alonzo::Metadatum::Text(x) => TxContentMetadataInnerJsonMetadata::String(x.to_string()),
            alonzo::Metadatum::Array(x) => {
                let items: Vec<_> = x.into_iter().map(|x| x.into_model()).try_collect()?;

                TxContentMetadataInnerJsonMetadata::Object(HashMap::from_iter([(
                    "array".to_string(),
                    serde_json::Value::Array(items),
                )]))
            }
            alonzo::Metadatum::Map(x) => {
                let items: HashMap<String, serde_json::Value> = x
                    .iter()
                    .map(|(k, v)| Ok((k.clone().into_model()?, v.clone().into_model()?)))
                    .try_collect()
                    .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

                TxContentMetadataInnerJsonMetadata::Object(items)
            }
        };

        Ok(out)
    }
}

impl IntoModel<Vec<TxContentMetadataInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentMetadataInner>, StatusCode> {
        let tx = self.tx()?;
        let metadata = tx.metadata();

        let entries: Vec<_> = metadata.collect();

        let items = entries
            .into_iter()
            .map(|(label, metadatum)| {
                Ok(TxContentMetadataInner {
                    label: label.to_string(),
                    json_metadata: Box::new(metadatum.clone().into_model()?),
                })
            })
            .try_collect()
            .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(items)
    }
}

impl IntoModel<Vec<TxContentMetadataCborInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentMetadataCborInner>, StatusCode> {
        let tx = self.tx()?;
        let metadata = tx.metadata();

        let entries: Vec<_> = metadata.collect();

        let items = entries
            .into_iter()
            .map(|(label, metadatum)| {
                Ok(TxContentMetadataCborInner {
                    label: label.to_string(),
                    metadata: Some(hex::encode(minicbor::to_vec(metadatum).unwrap())),
                    ..Default::default()
                })
            })
            .try_collect()
            .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(items)
    }
}

impl IntoModel<Vec<TxContentWithdrawalsInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentWithdrawalsInner>, StatusCode> {
        let tx = self.tx()?;
        let withdrawals = tx.withdrawals();
        let withdrawals: Vec<_> = withdrawals.collect();

        let items = withdrawals
            .into_iter()
            .map(|(address, amount)| {
                let address =
                    Address::from_bytes(address).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                Ok(TxContentWithdrawalsInner {
                    address: address.to_string(),
                    amount: amount.to_string(),
                })
            })
            .try_collect()
            .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(items)
    }
}

pub struct BlockModelBuilder<'a> {
    block: MultiEraBlock<'a>,
    chain: Option<&'a ChainSummary>,
    previous: Option<MultiEraBlock<'a>>,
    next: Option<MultiEraBlock<'a>>,
    tip: Option<MultiEraBlock<'a>>,
}

impl<'a> BlockModelBuilder<'a> {
    pub fn new(block: &'a [u8]) -> Result<Self, StatusCode> {
        let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self {
            block,
            previous: None,
            next: None,
            tip: None,
            chain: None,
        })
    }

    pub fn with_chain(self, chain: &'a ChainSummary) -> Self {
        Self {
            chain: Some(chain),
            ..self
        }
    }

    pub fn with_previous(self, previous: &'a [u8]) -> Result<Self, StatusCode> {
        let previous =
            MultiEraBlock::decode(previous).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self {
            previous: Some(previous),
            ..self
        })
    }

    pub fn with_next(self, next: &'a [u8]) -> Result<Self, StatusCode> {
        let next = MultiEraBlock::decode(next).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self {
            next: Some(next),
            ..self
        })
    }

    pub fn with_tip(self, tip: &'a [u8]) -> Result<Self, StatusCode> {
        let tip = MultiEraBlock::decode(tip).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Self {
            tip: Some(tip),
            ..self
        })
    }

    pub fn previous_hash(&self) -> Option<Hash<32>> {
        self.block.header().previous_hash()
    }

    pub fn next_number(&self) -> u64 {
        self.block.number() + 1
    }

    fn format_block_vrf(&self) -> Result<Option<String>, StatusCode> {
        let header = self.block.header();

        let Some(key) = header.vrf_vkey() else {
            return Ok(None);
        };

        let hrp = bech32::Hrp::parse("vrf_vk").map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let out = bech32::encode::<bech32::Bech32>(hrp, key)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Some(out))
    }

    fn format_slot_leader(&self) -> Result<Option<String>, StatusCode> {
        let header = self.block.header();

        let Some(key) = header.issuer_vkey() else {
            return Ok(None);
        };

        let hrp = bech32::Hrp::parse("pool").map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let out = bech32::encode::<bech32::Bech32>(hrp, key)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(Some(out))
    }

    fn format_ops_cert_data(&self) -> (Option<String>, Option<String>) {
        let header = self.block.header();

        match header {
            MultiEraHeader::ShelleyCompatible(x) => (
                Some(hex::encode(
                    x.header_body.operational_cert_hot_vkey.as_slice(),
                )),
                Some(x.header_body.operational_cert_sequence_number.to_string()),
            ),
            MultiEraHeader::BabbageCompatible(x) => (
                Some(hex::encode(
                    x.header_body
                        .operational_cert
                        .operational_cert_hot_vkey
                        .as_slice(),
                )),
                Some(
                    x.header_body
                        .operational_cert
                        .operational_cert_sequence_number
                        .to_string(),
                ),
            ),
            _ => (None, None),
        }
    }

    fn compute_total_fees(&self) -> String {
        let txs = self.block.txs();

        txs.iter()
            .map(|tx| tx.fee().unwrap_or(0))
            .sum::<u64>()
            .to_string()
    }

    fn compute_total_output(&self) -> String {
        let txs = self.block.txs();

        txs.iter()
            .map(|tx| tx.outputs().iter().map(|o| o.value().coin()).sum::<u64>())
            .sum::<u64>()
            .to_string()
    }
}

impl<'a> IntoModel<BlockContent> for BlockModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<BlockContent, StatusCode> {
        let block = &self.block;

        let (epoch, epoch_slot, block_time) = self
            .chain
            .as_ref()
            .map(|c| slot_time(block.slot(), &c))
            .map(|(a, b, c)| (Some(a), Some(b), Some(c)))
            .unwrap_or_default();

        let confirmations = self
            .tip
            .as_ref()
            .map(|x| x.number() - block.number())
            .map(|x| x as i32)
            .unwrap_or_default();

        let block_vrf = self.format_block_vrf()?;

        let slot_leader = self.format_slot_leader()?.unwrap_or_default();

        let next_block = self.next.as_ref().map(|x| x.hash().to_string());

        let previous_block = self.previous.as_ref().map(|x| x.hash().to_string());

        let (op_cert, op_cert_counter) = self.format_ops_cert_data();

        let output = self.compute_total_output();

        let fees = self.compute_total_fees();

        let out = BlockContent {
            hash: block.hash().to_string(),
            next_block,
            previous_block,
            epoch: epoch,
            epoch_slot: epoch_slot,
            time: block_time.unwrap_or_default(),
            slot: Some(block.slot() as i32),
            height: Some(block.number() as i32),
            tx_count: block.txs().len() as i32,
            size: block.size() as i32,
            confirmations,
            slot_leader,
            block_vrf,
            op_cert,
            op_cert_counter,
            output: Some(output),
            fees: Some(fees),
        };

        Ok(out)
    }
}

// HACK: This is the mapping to return the tx hashes for a block. For some
// reason, the openspi type BlockContentAddressesInnerTransactionsInner is being
// serialized as an object instead of a the expected strings. As a workaround,
// we return a Vec<String> instead.
impl<'a> IntoModel<Vec<String>> for BlockModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<String>, StatusCode> {
        let block = &self.block;

        let txs = block
            .txs()
            .iter()
            .map(|tx| tx.hash().to_string())
            //.sorted()
            //.map(|tx| BlockContentAddressesInnerTransactionsInner { tx_hash: tx })
            .collect();

        Ok(txs)
    }
}
