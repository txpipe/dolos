use axum::{http::StatusCode, Json};
use futures::future::join_all;
use itertools::Itertools;
use num_bigint::BigInt;
use num_rational::BigRational;
use num_traits::ToPrimitive;
use pallas::{
    codec::{minicbor, utils::Bytes},
    crypto::hash::{Hash, Hasher},
    ledger::{
        addresses::{Address, Network, ShelleyPaymentPart, StakeAddress, StakePayload},
        primitives::{
            alonzo::{self, Certificate as AlonzoCert},
            conway::{Certificate as ConwayCert, DRep, DatumOption, RedeemerTag, ScriptRef},
            Epoch, ExUnitPrices, ExUnits, PlutusData, StakeCredential,
        },
        traverse::{
            ComputeHash, MultiEraBlock, MultiEraCert, MultiEraHeader, MultiEraInput,
            MultiEraOutput, MultiEraRedeemer, MultiEraTx, MultiEraValue, OriginalHash,
        },
    },
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, ops::Deref, time::Duration};

use blockfrost_openapi::models::{
    address_utxo_content_inner::AddressUtxoContentInner,
    block_content::BlockContent,
    block_content_addresses_inner::BlockContentAddressesInner,
    block_content_addresses_inner_transactions_inner::BlockContentAddressesInnerTransactionsInner,
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
    tx_content_redeemers_inner::{Purpose, TxContentRedeemersInner},
    tx_content_stake_addr_inner::TxContentStakeAddrInner,
    tx_content_utxo::TxContentUtxo,
    tx_content_utxo_inputs_inner::TxContentUtxoInputsInner,
    tx_content_utxo_outputs_inner::TxContentUtxoOutputsInner,
    tx_content_withdrawals_inner::TxContentWithdrawalsInner,
};

use dolos_cardano::{pallas_extras, ChainSummary, PParamsSet, PoolHash};
use dolos_core::{Domain, EraCbor, TxHash, TxOrder, TxoIdx, TxoRef};

use crate::Facade;

macro_rules! try_into_or_500 {
    ($expr:expr) => {
        $expr.try_into().map_err(|err| {
            tracing::error!(error = ?err, expr = stringify!($expr), "numeric conversion failed");
            StatusCode::INTERNAL_SERVER_ERROR
        })?
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

const DREP_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("drep");
const POOL_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("pool");
const ASSET_HRP: bech32::Hrp = bech32::Hrp::parse_unchecked("asset");

#[inline]
pub fn bech32(hrp: bech32::Hrp, key: impl AsRef<[u8]>) -> Result<String, StatusCode> {
    bech32::encode::<bech32::Bech32>(hrp, key.as_ref())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub fn bech32_drep(drep: &DRep) -> Result<String, StatusCode> {
    let mut payload = [0; 29];

    let key_prefix: [u8; 1] = [0b00100010];
    let script_prefix: [u8; 1] = [0b00100011];

    match drep {
        DRep::Key(key) => {
            payload[..1].copy_from_slice(&key_prefix);
            payload[1..].copy_from_slice(key.as_slice());
        }
        DRep::Script(key) => {
            payload[..1].copy_from_slice(&script_prefix);
            payload[1..].copy_from_slice(key.as_slice());
        }
        _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
    };

    bech32(DREP_HRP, payload)
}

pub fn bech32_pool(key: impl AsRef<[u8]>) -> Result<String, StatusCode> {
    bech32(POOL_HRP, key)
}

pub fn asset_fingerprint(subject: &[u8]) -> Result<String, StatusCode> {
    let mut hasher = pallas::crypto::hash::Hasher::<160>::new();
    hasher.input(subject);
    let hash = hasher.finalize();
    bech32(ASSET_HRP, hash.as_ref())
}

pub fn stake_cred_to_address(cred: &StakeCredential, network: Network) -> StakeAddress {
    match cred {
        StakeCredential::AddrKeyhash(key) => StakeAddress::new(network, StakePayload::Stake(*key)),
        StakeCredential::ScriptHash(key) => StakeAddress::new(network, StakePayload::Script(*key)),
    }
}

pub fn vkey_to_stake_address(vkey: Hash<28>, network: Network) -> StakeAddress {
    StakeAddress::new(network, StakePayload::Stake(vkey))
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PoolOffchainMetadata {
    pub name: String,
    pub description: String,
    pub ticker: String,
    pub homepage: String,
}

pub async fn pool_offchain_metadata(url: &str) -> Option<PoolOffchainMetadata> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .user_agent("Dolos MiniBF")
        .build()
        .ok()?;

    let res = client.get(url).send().await.ok()?;

    if res.status() != StatusCode::OK {
        return None;
    }

    res.json().await.ok()
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

    #[allow(unused)]
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
    txo_ref: TxoRef,
    output: MultiEraOutput<'a>,
    is_collateral: bool,
    block: Option<MultiEraBlock<'a>>,
    tx_order: Option<TxOrder>,
    consumed_by_tx: Option<TxHash>,
}

impl<'a> UtxoOutputModelBuilder<'a> {
    pub fn txo_ref(&self) -> TxoRef {
        self.txo_ref.clone()
    }

    pub fn from_output(tx_hash: TxHash, tx_index: TxoIdx, output: MultiEraOutput<'a>) -> Self {
        Self {
            txo_ref: TxoRef(tx_hash, tx_index),
            output,
            is_collateral: false,
            block: None,
            tx_order: None,
            consumed_by_tx: None,
        }
    }

    pub fn from_collateral(
        tx_hash: TxHash,
        output_count: usize,
        collateral_idx: TxoIdx,
        output: MultiEraOutput<'a>,
    ) -> Self {
        Self {
            txo_ref: TxoRef(tx_hash, (output_count + collateral_idx as usize) as u32),
            output,
            is_collateral: true,
            block: None,
            tx_order: None,
            consumed_by_tx: None,
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

    pub fn with_consumed_by(self, tx: TxHash) -> Self {
        Self {
            consumed_by_tx: Some(tx),
            ..self
        }
    }
}

impl<'a> IntoModel<TxContentUtxoOutputsInner> for UtxoOutputModelBuilder<'a> {
    type SortKey = u64;

    fn into_model(self) -> Result<TxContentUtxoOutputsInner, StatusCode> {
        let out = TxContentUtxoOutputsInner {
            address: self.output.address().into_model()?,
            amount: self
                .output
                .value()
                .into_model()?
                .into_iter()
                .filter(|x| x.unit == "lovelace" || !self.is_collateral)
                .collect(),
            output_index: try_into_or_500!(self.txo_ref.1),
            consumed_by_tx: Some(self.consumed_by_tx.map(|x| x.to_string())),
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
            (Some(block), Some(txorder)) => Some((block.slot(), *txorder, self.txo_ref.1)),
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
            output_index: try_into_or_500!(self.txo_ref.1),
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
            tx_index: try_into_or_500!(self.txo_ref.1),
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
                amount: o
                    .value()
                    .into_model()?
                    .into_iter()
                    .filter(|x| x.unit == "lovelace" || !self.is_collateral)
                    .collect(),
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
    pparams: Option<PParamsSet>,
    network: Option<Network>,
    block: MultiEraBlock<'a>,
    order: TxOrder,
    deps: HashMap<TxHash, MultiEraTx<'a>>,
    consumed_deps: HashMap<TxoRef, TxHash>,
    pool_metadata: HashMap<PoolHash, PoolOffchainMetadata>,
}

impl<'a> TxModelBuilder<'a> {
    pub fn new(block: &'a [u8], order: TxOrder) -> Result<Self, StatusCode> {
        let block = MultiEraBlock::decode(block).map_err(|err| {
            tracing::error!(error = ?err, "failed to decode block in TxModelBuilder");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        Ok(Self {
            block,
            order,
            chain: None,
            pparams: None,
            network: None,
            deps: HashMap::new(),
            consumed_deps: HashMap::new(),
            pool_metadata: HashMap::new(),
        })
    }

    pub fn with_chain(self, chain: ChainSummary) -> Self {
        Self {
            chain: Some(chain),
            ..self
        }
    }

    pub fn with_pparams(self, pparams: PParamsSet) -> Self {
        Self {
            pparams: Some(pparams),
            ..self
        }
    }

    pub fn with_historical_pparams<D: Domain>(
        self,
        facade: &Facade<D>,
    ) -> Result<Self, StatusCode> {
        let epoch = self.tx_epoch()?;
        let chain = self.chain_or_500()?;

        let pparams = facade.get_historical_effective_pparams(epoch, chain)?;

        Ok(self.with_pparams(pparams))
    }

    pub fn with_network(self, network: Network) -> Self {
        Self {
            network: Some(network),
            ..self
        }
    }

    pub fn with_consumed_deps(self, consumed_deps: HashMap<TxoRef, TxHash>) -> Self {
        Self {
            consumed_deps,
            ..self
        }
    }

    pub async fn fetch_pool_metadata(&mut self) -> Result<(), StatusCode> {
        let pool_registrations = self
            .tx()?
            .certs()
            .iter()
            .filter_map(|cert| match cert {
                MultiEraCert::AlonzoCompatible(cow) => {
                    if let AlonzoCert::PoolRegistration {
                        operator,
                        ref pool_metadata,
                        ..
                    } = *(**cow).clone()
                    {
                        pool_metadata
                            .as_ref()
                            .map(|meta| (operator, meta.url.clone()))
                    } else {
                        None
                    }
                }
                MultiEraCert::Conway(cow) => {
                    if let ConwayCert::PoolRegistration {
                        operator,
                        ref pool_metadata,
                        ..
                    } = *(**cow).clone()
                    {
                        pool_metadata
                            .as_ref()
                            .map(|meta| (operator, meta.url.clone()))
                    } else {
                        None
                    }
                }
                _ => None,
            })
            .collect_vec();

        self.pool_metadata = join_all(pool_registrations.iter().map(
            |(pool_hash, url)| async move {
                pool_offchain_metadata(url)
                    .await
                    .map(|meta| (*pool_hash, meta))
            },
        ))
        .await
        .into_iter()
        .flatten()
        .collect();

        Ok(())
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
        self.chain.as_ref().ok_or_else(|| {
            tracing::error!("chain summary not set on TxModelBuilder");
            StatusCode::INTERNAL_SERVER_ERROR
        })
    }

    fn pparams_or_500(&self) -> Result<&PParamsSet, StatusCode> {
        self.pparams.as_ref().ok_or_else(|| {
            tracing::error!("pparams not set on TxModelBuilder");
            StatusCode::INTERNAL_SERVER_ERROR
        })
    }

    fn tx_epoch(&self) -> Result<Epoch, StatusCode> {
        let (epoch, _) = self.chain_or_500()?.slot_epoch(self.block.slot());
        Ok(epoch)
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

    pub fn required_consumed_deps(&self) -> Result<Vec<TxoRef>, StatusCode> {
        let tx = self.tx()?;

        let mut deps = vec![];

        for (i, _) in tx.produces() {
            deps.push(TxoRef(tx.hash(), i as u32));
        }

        Ok(deps)
    }

    pub fn deposit(&self) -> Result<u64, StatusCode> {
        let pparms = self.pparams_or_500()?;

        let key_deposit = pparms.key_deposit().unwrap_or_default();
        let pool_deposit = pparms.pool_deposit().unwrap_or_default();
        let drep_deposit = pparms.drep_deposit().unwrap_or_default();

        let out = self
            .tx()?
            .certs()
            .iter()
            .flat_map(|x| {
                if pallas_extras::cert_as_stake_registration(x).is_some() {
                    return Some(key_deposit);
                }

                if pallas_extras::cert_as_pool_registration(x).is_some() {
                    return Some(pool_deposit);
                }

                if let MultiEraCert::Conway(cert) = x {
                    if let ConwayCert::RegDRepCert(..) = cert.deref().deref() {
                        return Some(drep_deposit);
                    }
                }

                None
            })
            .sum();

        Ok(out)
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
            .map(|(i, o)| UtxoOutputModelBuilder::from_output(tx.hash(), i as u32, o))
            .map(|b| {
                let builder = if let Some(consumed_by) = self.consumed_deps.get(&b.txo_ref()) {
                    b.with_consumed_by(*consumed_by)
                } else {
                    b
                };
                builder.into_model()
            })
            .try_collect()?;

        let collateral_outputs: Vec<_> = tx
            .collateral_return()
            .into_iter()
            .enumerate()
            .map(|(i, o)| {
                UtxoOutputModelBuilder::from_collateral(tx.hash(), outputs.len(), i as u32, o)
            })
            .map(|b| {
                let builder = if let Some(consumed_by) = self.consumed_deps.get(&b.txo_ref()) {
                    b.with_consumed_by(*consumed_by)
                } else {
                    b
                };
                builder.into_model()
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

        let block_time = chain.slot_time(block.slot());

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
            deposit: self.deposit()?.to_string(),
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
            alonzo::Metadatum::Bytes(x) => format!("0x{}", hex::encode(x.as_slice())),
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
            alonzo::Metadatum::Int(x) => serde_json::Number::from_i128(x.into())
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::String(x.to_string())),
            alonzo::Metadatum::Text(x) => serde_json::Value::String(x.to_string()),
            alonzo::Metadatum::Bytes(x) => {
                let hex_str = format!("0x{}", hex::encode(x.as_slice()));

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
            alonzo::Metadatum::Bytes(x) => TxContentMetadataInnerJsonMetadata::String(format!(
                "0x{}",
                hex::encode(x.as_slice())
            )),
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
                let meta: alonzo::Metadata = vec![(label, metadatum.clone())].into_iter().collect();
                let encoded = hex::encode(minicbor::to_vec(meta).unwrap());
                Ok(TxContentMetadataCborInner {
                    label: label.to_string(),
                    metadata: Some(encoded.clone()),
                    cbor_metadata: Some(format!("\\x{encoded}")),
                })
            })
            .try_collect()
            .map_err(|_: StatusCode| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(items)
    }
}

impl TxModelBuilder<'_> {
    fn find_output_for_input(&self, input: &MultiEraInput<'_>) -> Option<MultiEraOutput<'_>> {
        let tx_hash = input.hash();
        let index = input.index() as usize;

        let source = self.deps.get(tx_hash)?;

        let outputs = source.outputs();
        let output = outputs.get(index)?;

        Some(output.clone())
    }

    fn find_redeemer_script(
        &self,
        redeemer: &MultiEraRedeemer<'_>,
    ) -> Result<Option<Hash<28>>, StatusCode> {
        let index = redeemer.index() as usize;
        let tx = self.tx()?;

        match redeemer.tag() {
            RedeemerTag::Spend => {
                let inputs = tx.inputs_sorted_set();
                let Some(input) = inputs.get(index) else {
                    return Ok(None);
                };

                let Some(output) = self.find_output_for_input(input) else {
                    return Ok(None);
                };

                let address = output
                    .address()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                match address {
                    Address::Shelley(x) => match x.payment() {
                        ShelleyPaymentPart::Script(hash) => Ok(Some(*hash)),
                        _ => Ok(None),
                    },
                    _ => Ok(None),
                }
            }
            RedeemerTag::Mint => {
                let mints = tx.mints();
                Ok(mints.get(index).map(|x| x.policy()).cloned())
            }
            _ => Ok(None),
        }
    }

    fn compute_fee(&self, units: &ExUnits, prices: &ExUnitPrices) -> Result<u64, StatusCode> {
        let ExUnitPrices {
            mem_price,
            step_price,
        } = prices;

        let unit_mem = BigRational::from_integer(BigInt::from(units.mem));
        let unit_steps = BigRational::from_integer(BigInt::from(units.steps));

        let mem_price = BigRational::new(
            BigInt::from(mem_price.numerator),
            BigInt::from(mem_price.denominator),
        );

        let step_price = BigRational::new(
            BigInt::from(step_price.numerator),
            BigInt::from(step_price.denominator),
        );

        let mem_fee = unit_mem * mem_price;
        let step_fee = unit_steps * step_price;

        let fee = mem_fee + step_fee;

        let fee: u64 = fee
            .ceil()
            .to_integer()
            .try_into()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(fee)
    }

    fn build_redeemer_inner(
        &self,
        redeemer: &MultiEraRedeemer<'_>,
        prices: &ExUnitPrices,
    ) -> Result<TxContentRedeemersInner, StatusCode> {
        let units = redeemer.ex_units();

        let fee = self.compute_fee(&units, prices)?;

        let out = TxContentRedeemersInner {
            purpose: match redeemer.tag() {
                RedeemerTag::Spend => Purpose::Spend,
                RedeemerTag::Mint => Purpose::Mint,
                RedeemerTag::Cert => Purpose::Cert,
                RedeemerTag::Reward => Purpose::Reward,
                // TODO: discuss with BF team if schema should be extended to include these
                _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
            },
            tx_index: redeemer.index() as i32,
            // TODO: we should change this in Pallas to ensure that we have a KeepRaw wrapping the
            // redeemer data
            redeemer_data_hash: redeemer.data().compute_hash().to_string(),
            fee: fee.to_string(),
            unit_mem: units.mem.to_string(),
            unit_steps: units.steps.to_string(),
            script_hash: self
                .find_redeemer_script(redeemer)?
                .map(|x| x.to_string())
                .unwrap_or_default(),
            datum_hash: redeemer.data().compute_hash().to_string(),
        };

        Ok(out)
    }
}

impl IntoModel<Vec<TxContentRedeemersInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentRedeemersInner>, StatusCode> {
        let tx = self.tx()?;
        let redeemers = tx.redeemers();

        let pparms = self.pparams_or_500()?;

        let prices = pparms
            .execution_costs()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let items = redeemers
            .into_iter()
            .map(|x| self.build_redeemer_inner(&x, &prices))
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
        active_epoch: active_epoch + 1,
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
            .map(|c| c.slot_epoch(self.block.slot()))
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
                            ConwayCert::StakeRegDeleg(cred, pool, _) => Some(
                                build_delegation_inner(index, cred, pool, network, active_epoch),
                            ),
                            ConwayCert::StakeVoteRegDeleg(cred, pool, _, _) => Some(
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
                    pool_id: bech32_pool(pool.as_slice()).ok()?,
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
                    pool_id: bech32_pool(pool.as_slice()).ok()?,
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
                ipv4: ipv4.map(|ipv4| {
                    if let Ok(slice) = <[u8; 4]>::try_from(ipv4.as_slice()) {
                        std::net::Ipv4Addr::from(slice).to_string()
                    } else {
                        Default::default()
                    }
                }),
                ipv6: ipv6.map(|ipv6| {
                    if let Ok(slice) = <[u8; 16]>::try_from(ipv6.as_slice()) {
                        std::net::Ipv6Addr::from(slice).to_string()
                    } else {
                        Default::default()
                    }
                }),
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
    offchain_pool_metadata: Option<PoolOffchainMetadata>,
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
            offchain_pool_metadata: None,
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
            offchain_pool_metadata: None,
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

    fn with_offchain(&mut self, offchain_pool_metadata: Option<PoolOffchainMetadata>) {
        self.offchain_pool_metadata = offchain_pool_metadata
    }
}

impl IntoModel<TxContentPoolCertsInner> for PoolUpdateModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentPoolCertsInner, StatusCode> {
        let reward_account =
            vkey_to_stake_address(self.reward_account.as_slice()[1..].into(), self.network)
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let owners: Vec<_> = self
            .pool_owners
            .iter()
            .map(|owner| vkey_to_stake_address(*owner, self.network))
            .map(|owner| {
                owner
                    .to_bech32()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
            })
            .try_collect()?;

        Ok(TxContentPoolCertsInner {
            vrf_key: self.vrf_keyhash.to_string(),
            pledge: self.pledge.to_string(),
            margin_cost: rational_to_f64::<3>(&self.margin),
            fixed_cost: self.cost.to_string(),
            reward_account,
            owners,
            metadata: Some(Box::new(TxContentPoolCertsInnerMetadata {
                url: self.pool_metadata.as_ref().map(|x| x.url.clone()),
                hash: self.pool_metadata.as_ref().map(|x| x.hash.to_string()),
                ticker: self
                    .offchain_pool_metadata
                    .as_ref()
                    .map(|x| x.ticker.clone()),
                name: self.offchain_pool_metadata.as_ref().map(|x| x.name.clone()),
                description: self
                    .offchain_pool_metadata
                    .as_ref()
                    .map(|x| x.description.clone()),
                homepage: self
                    .offchain_pool_metadata
                    .as_ref()
                    .map(|x| x.homepage.clone()),
            })),
            relays: self
                .relays
                .iter()
                .map(|relay| relay.clone().into_model())
                .try_collect()?,
            cert_index: self.cert_index as i32,
            pool_id: bech32_pool(self.operator.as_slice())?,
            active_epoch: self.current_epoch + 2,
        })
    }
}

impl IntoModel<Vec<TxContentPoolCertsInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentPoolCertsInner>, StatusCode> {
        let tx = self.tx()?;

        let network = self.network.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
        let chain = self
            .chain
            .as_ref()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let epoch = chain.slot_epoch(self.block.slot()).0;
        let items = tx
            .certs()
            .into_iter()
            .enumerate()
            .filter_map(|(cert_index, cert)| {
                let builder = PoolUpdateModelBuilder::new(cert, cert_index, network, epoch as i32);
                builder.map(|mut x| {
                    x.with_offchain(self.pool_metadata.get(&x.operator).cloned());
                    x
                })
            })
            .map(|builder| builder.into_model())
            .try_collect()?;

        Ok(items)
    }
}

struct StakeCertModelBuilder {
    stake_credential: StakeCredential,
    is_registration: bool,
    cert_index: usize,
    network: Network,
}

impl StakeCertModelBuilder {
    fn new(cert: MultiEraCert, cert_index: usize, network: Network) -> Option<Self> {
        match cert {
            MultiEraCert::AlonzoCompatible(cow) => match cow.deref().deref() {
                AlonzoCert::StakeRegistration(stake_credential) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                AlonzoCert::StakeDeregistration(stake_credential) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: false,
                    cert_index,
                    network,
                }),
                _ => None,
            },
            MultiEraCert::Conway(cow) => match cow.deref().deref() {
                ConwayCert::StakeRegistration(stake_credential) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                ConwayCert::StakeDeregistration(stake_credential) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: false,
                    cert_index,
                    network,
                }),
                ConwayCert::Reg(stake_credential, _) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                ConwayCert::StakeRegDeleg(stake_credential, _, _) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                ConwayCert::StakeVoteRegDeleg(stake_credential, _, _, _) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: true,
                    cert_index,
                    network,
                }),
                ConwayCert::UnReg(stake_credential, _) => Some(Self {
                    stake_credential: stake_credential.clone(),
                    is_registration: false,
                    cert_index,
                    network,
                }),
                _ => None,
            },
            _ => None,
        }
    }
}

impl IntoModel<TxContentStakeAddrInner> for StakeCertModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<TxContentStakeAddrInner, StatusCode> {
        let address = stake_cred_to_address(&self.stake_credential, self.network)
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let out = TxContentStakeAddrInner {
            address,
            registration: self.is_registration,
            cert_index: self.cert_index as i32,
        };

        Ok(out)
    }
}

impl IntoModel<Vec<TxContentStakeAddrInner>> for TxModelBuilder<'_> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<TxContentStakeAddrInner>, StatusCode> {
        let tx = self.tx()?;

        let network = self.network.ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let items = tx
            .certs()
            .into_iter()
            .enumerate()
            .filter_map(|(index, cert)| StakeCertModelBuilder::new(cert, index, network))
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

        let Some(use_bech32) = self
            .chain
            .map(|x| x.slot_epoch(self.block.slot()).0 > x.first_shelley_epoch() + 1)
        else {
            return Ok(None);
        };

        let Some(key) = header.issuer_vkey() else {
            return Ok(None);
        };
        let hash: Hash<28> = Hasher::<224>::hash(key);

        if use_bech32 {
            Ok(Some(bech32_pool(hash)?))
        } else {
            Ok(Some(format!(
                "ShelleyGenesis-{}",
                hex::encode(hash.as_slice().first_chunk::<8>().unwrap())
            )))
        }
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
            .map(|c| c.slot_epoch(block.slot()))
            .map(|(a, b)| (Some(a), Some(b)))
            .unwrap_or_default();

        let block_time = self
            .chain
            .as_ref()
            .map(|c| c.slot_time(block.slot()))
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
            size: block.body_size().unwrap_or_default() as i32,
            confirmations,
            slot_leader,
            block_vrf,
            op_cert,
            op_cert_counter,
            output: match output.as_str() {
                "0" => None,
                _ => Some(output),
            },
            fees: match fees.as_str() {
                "0" => None,
                _ => Some(fees),
            },
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

impl<'a> IntoModel<Vec<BlockContentAddressesInner>> for BlockModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<BlockContentAddressesInner>, StatusCode> {
        let block = &self.block;
        let addresses = block
            .txs()
            .iter()
            .flat_map(|tx| {
                tx.produces()
                    .iter()
                    .map(|(_, output)| BlockContentAddressesInner {
                        address: output.address().unwrap().to_string(),
                        transactions: vec![BlockContentAddressesInnerTransactionsInner {
                            tx_hash: tx.hash().to_string(),
                        }],
                    })
                    .collect::<Vec<_>>()
            })
            .sorted_by(|x, y| x.address.cmp(&y.address))
            .collect();

        Ok(addresses)
    }
}

pub struct PlutusDataWrapper(pub PlutusData);
impl PlutusDataWrapper {
    fn as_value(&self) -> Result<serde_json::Value, StatusCode> {
        match &self.0 {
            PlutusData::Constr(x) => {
                let values = x
                    .fields
                    .iter()
                    .map(|d| PlutusDataWrapper(d.clone()).as_value())
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                Ok(serde_json::Value::Object(serde_json::Map::from_iter([
                    (
                        "constructor".to_string(),
                        serde_json::Value::Number(x.constr_index().into()),
                    ),
                    ("fields".to_string(), serde_json::Value::Array(values)),
                ])))
            }

            PlutusData::Map(x) => {
                let mut map = serde_json::Map::new();
                for (k, v) in x.iter() {
                    let key_opt = PlutusDataWrapper(k.clone())
                        .as_value()?
                        .as_str()
                        .map(|s| s.to_owned());

                    if let Some(key) = key_opt {
                        map.insert(key, PlutusDataWrapper(v.clone()).as_value()?);
                    }
                }

                Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                    "map".to_string(),
                    serde_json::Value::Object(map),
                )])))
            }

            PlutusData::Array(x) => {
                let values = x
                    .iter()
                    .map(|d| PlutusDataWrapper(d.clone()).as_value())
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                    "list".to_string(),
                    serde_json::Value::Array(values),
                )])))
            }

            PlutusData::BigInt(x) => match x {
                pallas::ledger::primitives::BigInt::Int(int) => {
                    let i = Into::<i128>::into(*int);
                    Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                        "int".to_string(),
                        serde_json::Value::Number((i as i64).into()),
                    )])))
                }
                pallas::ledger::primitives::BigInt::BigUInt(bounded_bytes) => {
                    let bigint = num_bigint::BigUint::from_bytes_be(bounded_bytes.as_slice());
                    let number = serde_json::Number::from_f64(
                        bigint.to_f64().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?,
                    )
                    .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                    Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                        "int".to_string(),
                        serde_json::Value::Number(number),
                    )])))
                }
                pallas::ledger::primitives::BigInt::BigNInt(bounded_bytes) => {
                    let bigint = num_bigint::BigInt::from_signed_bytes_be(bounded_bytes.as_slice());
                    let number = serde_json::Number::from_f64(
                        bigint.to_f64().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?,
                    )
                    .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
                    Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                        "int".to_string(),
                        serde_json::Value::Number(number),
                    )])))
                }
            },

            PlutusData::BoundedBytes(x) => {
                Ok(serde_json::Value::Object(serde_json::Map::from_iter([(
                    "bytes".to_string(),
                    serde_json::Value::String(x.to_string()),
                )])))
            }
        }
    }
}
impl IntoModel<HashMap<String, serde_json::Value>> for PlutusDataWrapper {
    type SortKey = ();

    fn into_model(self) -> Result<HashMap<String, serde_json::Value>, StatusCode> {
        let value = self.as_value()?;
        serde_json::from_value(value).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
    }
}
