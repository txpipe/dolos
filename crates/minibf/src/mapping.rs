use axum::{http::StatusCode, Json};
use itertools::Itertools;
use pallas::{
    codec::{minicbor, utils::Bytes},
    crypto::hash::Hash,
    ledger::{
        addresses::{Address, Network, StakeAddress, StakePayload},
        primitives::{
            alonzo::{self, Certificate as AlonzoCert},
            conway::{Certificate as ConwayCert, DatumOption, ScriptRef},
            StakeCredential,
        },
        traverse::{
            ComputeHash, MultiEraBlock, MultiEraCert, MultiEraHeader, MultiEraInput,
            MultiEraOutput, MultiEraTx, MultiEraValue, OriginalHash,
        },
    },
};
use std::collections::HashMap;

use blockfrost_openapi::models::{
    address_utxo_content_inner::AddressUtxoContentInner,
    block_content::BlockContent,
    tx_content::TxContent,
    tx_content_cbor::TxContentCbor,
    tx_content_delegations_inner::TxContentDelegationsInner,
    tx_content_metadata_cbor_inner::TxContentMetadataCborInner,
    tx_content_metadata_inner::TxContentMetadataInner,
    tx_content_metadata_inner_json_metadata::TxContentMetadataInnerJsonMetadata,
    tx_content_mirs_inner::{Pot, TxContentMirsInner},
    tx_content_output_amount_inner::TxContentOutputAmountInner,
    tx_content_pool_certs_inner::TxContentPoolCertsInner,
    tx_content_pool_certs_inner_metadata::TxContentPoolCertsInnerMetadata,
    tx_content_pool_certs_inner_relays_inner::TxContentPoolCertsInnerRelaysInner,
    tx_content_pool_retires_inner::TxContentPoolRetiresInner,
    tx_content_utxo::TxContentUtxo,
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

pub fn round_f64<const DECIMALS: u8>(val: f64) -> f64 {
    let multiplier = 10_f64.powi(DECIMALS as i32);
    (val * multiplier).round() / multiplier
}

pub fn rational_to_f64<const DECIMALS: u8>(val: &alonzo::RationalNumber) -> f64 {
    let res = val.numerator as f64 / val.denominator as f64;
    round_f64::<DECIMALS>(res)
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
    network: Option<Network>,
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
            network: None,
            deps: HashMap::new(),
        })
    }

    pub fn with_chain(self, chain: ChainSummary) -> Self {
        Self {
            chain: Some(chain),
            ..self
        }
    }

    pub fn with_network(self, network: Network) -> Self {
        Self {
            network: Some(network),
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

macro_rules! match_certs {
    ($tx:expr, "alonzo", $cert:ident) => {
        $tx.certs()
            .iter()
            .enumerate()
            .filter_map(|(index, x)| x.as_alonzo().map(|x| (index, x)))
            .filter(|(_, x)| matches!(x, AlonzoCert::$cert { .. }))
    };

    ($tx:expr, "conway", $cert:ident) => {
        $tx.certs()
            .iter()
            .enumerate()
            .filter_map(|(index, x)| x.as_conway().map(|x| (index, x)))
            .filter(|(_, x)| matches!(x, ConwayCert::$cert { .. }))
    };
}

macro_rules! count_certs {
    ($tx:expr, "alonzo", $cert:ident) => {{
        let alonzo = match_certs!($tx, "alonzo", $cert).count();
        alonzo
    }};

    ($tx:expr, "conway", $cert:ident) => {{
        let conway = match_certs!($tx, "conway", $cert).count();
        conway
    }};

    ($tx:expr, $cert:ident) => {{
        let alonzo = match_certs!($tx, "alonzo", $cert).count();
        let conway = match_certs!($tx, "conway", $cert).count();

        alonzo + conway
    }};
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

        let block_time = dolos_cardano::slot_time(block.slot(), chain);

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
            mir_cert_count: count_certs!(tx, "alonzo", MoveInstantaneousRewardsCert) as i32,
            delegation_count: count_certs!(tx, StakeDelegation) as i32,
            stake_cert_count: count_certs!(tx, StakeRegistration) as i32,
            pool_update_count: count_certs!(tx, PoolRegistration) as i32,
            pool_retire_count: count_certs!(tx, PoolRetirement) as i32,
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

fn stake_cred_to_address(cred: &StakeCredential, network: Network) -> StakeAddress {
    match cred {
        StakeCredential::AddrKeyhash(key) => StakeAddress::new(network, StakePayload::Stake(*key)),
        StakeCredential::ScriptHash(key) => StakeAddress::new(network, StakePayload::Script(*key)),
    }
}

fn build_delegation_inner(
    index: usize,
    cred: &StakeCredential,
    pool: &Hash<28>,
    network: Network,
    active_epoch: i32,
) -> Result<TxContentDelegationsInner, StatusCode> {
    let pool_hrp = bech32::Hrp::parse("pool").map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let pool_id = bech32::encode::<bech32::Bech32>(pool_hrp, pool.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let address = stake_cred_to_address(cred, network);

    let address = address
        .to_bech32()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(TxContentDelegationsInner {
        index: index as i32,
        address,
        pool_id,
        active_epoch,
        // DEPRECATED
        cert_index: index as i32,
    })
}

impl IntoModel<Vec<TxContentDelegationsInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentDelegationsInner>, StatusCode> {
        let tx = self.tx()?;

        let certs = tx.certs();

        let network = self.network.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        // TODO: we're hardcoding a ledger rule here by saying that the active epoch is
        // the epoch number + 1. Although this is correct, the mapping layer
        // shouldn't be the one defining this.
        let active_epoch = self
            .chain
            .as_ref()
            .map(|c| dolos_cardano::slot_epoch(self.block.slot(), c))
            .map(|(a, _)| (a + 1) as i32)
            .unwrap_or_default();

        let items =
            certs
                .into_iter()
                .enumerate()
                .filter_map(|(index, cert)| match cert {
                    MultiEraCert::AlonzoCompatible(cert) => {
                        match &**cert {
                            AlonzoCert::StakeDelegation(cred, pool) => Some(
                                build_delegation_inner(index, cred, pool, network, active_epoch),
                            ),
                            _ => None,
                        }
                    }
                    MultiEraCert::Conway(cert) => {
                        match &**cert {
                            ConwayCert::StakeDelegation(cred, pool) => Some(
                                build_delegation_inner(index, cred, pool, network, active_epoch),
                            ),
                            _ => None,
                        }
                    }
                    _ => None,
                })
                .try_collect()?;

        Ok(items)
    }
}

fn build_mir_inners(
    index: usize,
    mir: &alonzo::Certificate,
    network: Network,
) -> Result<Vec<TxContentMirsInner>, StatusCode> {
    let AlonzoCert::MoveInstantaneousRewardsCert(mir) = mir else {
        return Ok(vec![]);
    };

    let pot = match mir.source {
        alonzo::InstantaneousRewardSource::Reserves => Pot::Reserve,
        alonzo::InstantaneousRewardSource::Treasury => Pot::Treasury,
    };

    let targets = match &mir.target {
        alonzo::InstantaneousRewardTarget::StakeCredentials(creds) => creds.iter().collect(),
        _ => vec![],
    };

    let items = targets
        .into_iter()
        .map(|(cred, amount)| {
            let address = stake_cred_to_address(cred, network);

            let address = address
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            Ok::<_, StatusCode>(TxContentMirsInner {
                pot,
                cert_index: index as i32,
                address,
                amount: amount.to_string(),
            })
        })
        .try_collect()?;

    Ok(items)
}

impl IntoModel<Vec<TxContentMirsInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentMirsInner>, StatusCode> {
        let tx = self.tx()?;

        let network = self.network.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let items = match_certs!(tx, "alonzo", MoveInstantaneousRewardsCert)
            .map(|(index, cert)| build_mir_inners(index, cert, network))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(items)
    }
}

impl IntoModel<Vec<TxContentPoolRetiresInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentPoolRetiresInner>, StatusCode> {
        let tx = self.tx()?;

        let alonzo = match_certs!(tx, "alonzo", PoolRetirement)
            .filter_map(|(index, cert)| {
                let AlonzoCert::PoolRetirement(pool, epoch) = cert else {
                    return None;
                };

                Some(TxContentPoolRetiresInner {
                    pool_id: pool.to_string(),
                    cert_index: index as i32,
                    retiring_epoch: *epoch as i32,
                })
            })
            .collect::<Vec<_>>();

        let conway = match_certs!(tx, "conway", PoolRetirement)
            .filter_map(|(index, cert)| {
                let ConwayCert::PoolRetirement(pool, epoch) = cert else {
                    return None;
                };

                Some(TxContentPoolRetiresInner {
                    pool_id: pool.to_string(),
                    cert_index: index as i32,
                    retiring_epoch: *epoch as i32,
                })
            })
            .collect::<Vec<_>>();

        let items = alonzo.into_iter().chain(conway).collect();

        Ok(items)
    }
}

impl IntoModel<TxContentPoolCertsInnerRelaysInner> for alonzo::Relay {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentPoolCertsInnerRelaysInner, StatusCode> {
        let out = match self {
            alonzo::Relay::SingleHostAddr(port, ipv4, ipv6) => TxContentPoolCertsInnerRelaysInner {
                ipv4: ipv4.map(|ipv4| ipv4.to_string()),
                ipv6: ipv6.map(|ipv6| ipv6.to_string()),
                dns: None,
                dns_srv: None,
                port: port.unwrap_or_default() as i32,
            },
            alonzo::Relay::SingleHostName(port, dns) => TxContentPoolCertsInnerRelaysInner {
                ipv4: None,
                ipv6: None,
                dns: Some(dns.to_string()),
                dns_srv: None,
                port: port.unwrap_or_default() as i32,
            },
            alonzo::Relay::MultiHostName(dns) => TxContentPoolCertsInnerRelaysInner {
                ipv4: None,
                ipv6: None,
                dns: Some(dns.to_string()),
                dns_srv: None,
                port: Default::default(),
            },
        };

        Ok(out)
    }
}

struct PoolUpdateModelBuilder {
    operator: Hash<28>,
    vrf_keyhash: Hash<32>,
    pledge: u64,
    cost: u64,
    margin: alonzo::RationalNumber,
    reward_account: Bytes,
    pool_owners: Vec<Hash<28>>,
    relays: Vec<alonzo::Relay>,
    pool_metadata: Option<alonzo::PoolMetadata>,
    cert_index: usize,
    network: Network,
    current_epoch: i32,
}

impl PoolUpdateModelBuilder {
    fn new_from_alonzo(
        cert: AlonzoCert,
        cert_index: usize,
        network: Network,
        current_epoch: i32,
    ) -> Option<Self> {
        let AlonzoCert::PoolRegistration {
            operator,
            vrf_keyhash,
            pledge,
            cost,
            margin,
            reward_account,
            pool_owners,
            relays,
            pool_metadata,
        } = cert
        else {
            return None;
        };

        Some(Self {
            operator,
            vrf_keyhash,
            pledge,
            cost,
            margin,
            reward_account,
            pool_owners,
            relays,
            pool_metadata,
            cert_index,
            network,
            current_epoch,
        })
    }

    fn new_from_conway(
        cert: ConwayCert,
        cert_index: usize,
        network: Network,
        current_epoch: i32,
    ) -> Option<Self> {
        let ConwayCert::PoolRegistration {
            operator,
            vrf_keyhash,
            pledge,
            cost,
            margin,
            reward_account,
            pool_owners,
            relays,
            pool_metadata,
        } = cert
        else {
            return None;
        };

        Some(Self {
            operator,
            vrf_keyhash,
            pledge,
            cost,
            margin,
            reward_account,
            pool_owners: pool_owners.to_vec(),
            relays,
            pool_metadata,
            cert_index,
            network,
            current_epoch,
        })
    }

    fn new(
        cert: MultiEraCert,
        cert_index: usize,
        network: Network,
        current_epoch: i32,
    ) -> Option<Self> {
        match cert {
            MultiEraCert::AlonzoCompatible(cow) => {
                Self::new_from_alonzo((**cow).clone(), cert_index, network, current_epoch)
            }
            MultiEraCert::Conway(cow) => {
                Self::new_from_conway((**cow).clone(), cert_index, network, current_epoch)
            }
            _ => None,
        }
    }
}

impl IntoModel<TxContentPoolCertsInner> for PoolUpdateModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentPoolCertsInner, StatusCode> {
        Ok(TxContentPoolCertsInner {
            vrf_key: self.vrf_keyhash.to_string(),
            pledge: self.pledge.to_string(),
            margin_cost: rational_to_f64::<3>(&self.margin),
            fixed_cost: self.cost.to_string(),
            reward_account: self.reward_account.to_string(),
            owners: self
                .pool_owners
                .iter()
                .map(|owner| owner.to_string())
                .collect(),
            metadata: Some(Box::new(TxContentPoolCertsInnerMetadata {
                url: self.pool_metadata.as_ref().map(|x| x.url.clone()),
                hash: self.pool_metadata.as_ref().map(|x| x.hash.to_string()),
                ticker: None,
                name: None,
                description: None,
                homepage: None,
            })),
            relays: self
                .relays
                .iter()
                .map(|relay| relay.clone().into_model())
                .try_collect()?,
            cert_index: self.cert_index as i32,
            pool_id: self.operator.to_string(),
            active_epoch: self.current_epoch + 1,
        })
    }
}

impl IntoModel<Vec<TxContentPoolCertsInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentPoolCertsInner>, StatusCode> {
        let tx = self.tx()?;

        let network = self.network.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let epoch = self
            .chain
            .as_ref()
            .map(|c| dolos_cardano::slot_epoch(self.block.slot(), c))
            .map(|(a, _)| a)
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let items = tx
            .certs()
            .into_iter()
            .enumerate()
            .filter_map(|(cert_index, cert)| {
                PoolUpdateModelBuilder::new(cert, cert_index, network, epoch as i32)
            })
            .map(|builder| builder.into_model())
            .try_collect()?;

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

        let (epoch, epoch_slot) = self
            .chain
            .as_ref()
            .map(|c| dolos_cardano::slot_epoch(block.slot(), c))
            .map(|(a, b)| (Some(a), Some(b)))
            .unwrap_or_default();

        let block_time = self
            .chain
            .as_ref()
            .map(|c| dolos_cardano::slot_time(block.slot(), c))
            .map(|x| Some(x as i32))
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
            epoch: epoch.map(|x| x as i32),
            epoch_slot: epoch_slot.map(|x| x as i32),
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
