use std::{collections::HashMap, ops::Deref, time::Duration};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    asset::{Asset, OnchainMetadataStandard},
    asset_addresses_inner::AssetAddressesInner,
    asset_metadata::AssetMetadata as OffchainMetadata,
    asset_transactions_inner::AssetTransactionsInner,
};
use crc::{Crc, CRC_8_SMBUS};
use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, CardanoIndexExt},
    model::AssetState,
    ChainSummary,
};
use dolos_core::{BlockSlot, Domain, EraCbor, IndexStore as _, StateStore as _};
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    ledger::{
        primitives::{BigInt, Metadatum, PlutusData},
        traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx},
        validate::phase2::to_plutus_data::ToPlutusData,
    },
};
use serde::Deserialize;

use crate::{
    error::Error,
    mapping::{asset_fingerprint, IntoModel},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

struct OnchainMetadata {
    version: Option<OnchainMetadataStandard>,
    metadata: HashMap<String, serde_json::Value>,
    extra: Option<String>,
}
impl OnchainMetadata {
    fn from_plutus_data(plutus_data: PlutusData) -> Result<Option<Self>, StatusCode> {
        let value = CIP68Metadata(plutus_data.clone()).into_model()?;
        if !value.is_array() {
            return Ok(None);
        }

        let array = value.as_array().unwrap();
        let Some(metadata) = array.first() else {
            return Ok(None);
        };
        if metadata.is_null() || !metadata.is_object() {
            return Ok(None);
        }
        let metadata = metadata.as_object().unwrap().clone().into_iter().collect();

        let version = array
            .get(1)
            .and_then(|v| v.as_number())
            .and_then(|n| n.as_i64())
            .and_then(|n| match n {
                1 => Some(OnchainMetadataStandard::Cip68v1),
                2 => Some(OnchainMetadataStandard::Cip68v2),
                3 => Some(OnchainMetadataStandard::Cip68v3),
                _ => None,
            });

        let extra = if let PlutusData::Constr(constr) = plutus_data {
            constr.fields.get(2).map(encode_to_hex).transpose()?
        } else {
            None
        };

        Ok(Some(Self {
            metadata,
            version,
            extra,
        }))
    }

    fn from_metadatum(unit: &str, metadatum: Metadatum) -> Result<Option<Self>, StatusCode> {
        let value = CIP25Metadata(metadatum).into_model()?;

        let (metadata, version) = match value {
            serde_json::Value::Object(map) => {
                let policy_id = &unit[..56];
                let asset_name_raw = &unit[56..];

                let asset_name = hex::decode(asset_name_raw)
                    .ok()
                    .and_then(|v| String::from_utf8(v).ok())
                    .unwrap_or_else(|| asset_name_raw.to_string());

                let metadata = map
                    .get(policy_id)
                    .and_then(|policy_metadata| policy_metadata.get(&asset_name))
                    .and_then(|asset_metadata| asset_metadata.as_object())
                    .map(|obj| obj.clone().into_iter().collect())
                    .unwrap_or_default();

                let version = map.get("version").and_then(|v| match v {
                    serde_json::Value::Number(num) => num.as_i64(),
                    serde_json::Value::String(s) => s.parse::<f64>().ok().map(|f| f as i64),
                    _ => None,
                });

                (metadata, version)
            }
            _ => (HashMap::new(), None),
        };

        let version = Some(match version {
            Some(2) => OnchainMetadataStandard::Cip25v2,
            _ => OnchainMetadataStandard::Cip25v1,
        });

        let extra = None;

        Ok(Some(Self {
            metadata,
            version,
            extra,
        }))
    }
}

const CIP68_FIELDS: &[&str] = &[
    "name",
    "description",
    "image",
    "mediaType",
    "files",
    "ticker",
    "url",
    "logo",
    "decimals",
    "src",
];
const CRC8_ALGO: Crc<u8> = Crc::<u8>::new(&CRC_8_SMBUS);
#[derive(Debug, Clone)]
enum CIP68Label {
    ReferenceNft,
    Nft,
    Ft,
    Rft,
}
impl CIP68Label {
    fn from_u32(value: u32) -> Option<Self> {
        match value {
            100 => Some(Self::ReferenceNft),
            222 => Some(Self::Nft),
            333 => Some(Self::Ft),
            444 => Some(Self::Rft),
            _ => None,
        }
    }

    fn to_u32(&self) -> u32 {
        match self {
            CIP68Label::ReferenceNft => 100,
            CIP68Label::Nft => 222,
            CIP68Label::Ft => 333,
            CIP68Label::Rft => 444,
        }
    }

    fn to_label(&self) -> String {
        let number_hex = format!("{:04x}", self.to_u32());
        let bytes = hex::decode(&number_hex).unwrap();
        let checksum = format!("{:02x}", CRC8_ALGO.checksum(&bytes));
        format!("0{number_hex}{checksum}0")
    }
}

#[derive(Debug, Deserialize)]
pub struct TokenRegistryValue<T> {
    pub value: T,
}
#[derive(Debug, Deserialize)]
pub struct TokenRegistryMetadata {
    pub name: Option<TokenRegistryValue<String>>,
    pub description: Option<TokenRegistryValue<String>>,
    pub ticker: Option<TokenRegistryValue<String>>,
    pub url: Option<TokenRegistryValue<String>>,
    pub logo: Option<TokenRegistryValue<String>>,
    pub decimals: Option<TokenRegistryValue<i32>>,
}
impl From<TokenRegistryMetadata> for OffchainMetadata {
    fn from(token_registry_asset: TokenRegistryMetadata) -> Self {
        Self {
            name: token_registry_asset.name.as_ref().unwrap().value.clone(),
            description: token_registry_asset
                .description
                .as_ref()
                .unwrap()
                .value
                .clone(),
            ticker: token_registry_asset
                .ticker
                .as_ref()
                .map(|v| v.value.clone()),
            url: token_registry_asset.url.as_ref().map(|v| v.value.clone()),
            logo: token_registry_asset.logo.as_ref().map(|v| v.value.clone()),
            decimals: token_registry_asset.decimals.as_ref().map(|v| v.value),
        }
    }
}

struct CIP25Metadata(Metadatum);
impl IntoModel<serde_json::Value> for CIP25Metadata {
    type SortKey = ();

    fn into_model(self) -> Result<serde_json::Value, StatusCode> {
        Ok(match self.0 {
            Metadatum::Int(x) => serde_json::Number::from_i128(x.into())
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::String(x.to_string())),

            Metadatum::Text(x) => serde_json::Value::String(x),

            Metadatum::Bytes(x) => match String::from_utf8(x.to_vec().clone()) {
                Ok(s) => serde_json::Value::String(s),
                Err(_) => serde_json::Value::String(hex::encode(x.to_vec())),
            },

            Metadatum::Array(x) => {
                let values = x
                    .into_iter()
                    .map(|d| CIP25Metadata(d).into_model())
                    .collect::<Result<Vec<_>, _>>()?;
                serde_json::Value::Array(values)
            }

            Metadatum::Map(x) => {
                let mut map = serde_json::Map::new();
                for (k, v) in x.iter() {
                    if let Some(key) = CIP25Metadata(k.clone()).into_model()?.as_str() {
                        map.insert(key.to_string(), CIP25Metadata(v.clone()).into_model()?);
                    }
                }
                serde_json::Value::Object(map)
            }
        })
    }
}

fn encode_to_hex<T: minicbor::Encode<()>>(value: &T) -> Result<String, StatusCode> {
    let mut buf = Vec::new();
    minicbor::encode(value, &mut buf).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(hex::encode(buf))
}

struct CIP68Metadata(PlutusData);
impl IntoModel<serde_json::Value> for CIP68Metadata {
    type SortKey = ();

    fn into_model(self) -> Result<serde_json::Value, StatusCode> {
        Ok(match self.0 {
            PlutusData::Constr(x) => {
                let values = x
                    .fields
                    .iter()
                    .map(|d| CIP68Metadata(d.clone()).into_model())
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                serde_json::Value::Array(values)
            }

            PlutusData::Map(x) => {
                let mut map = serde_json::Map::new();
                for (k, v) in x.iter() {
                    let key_opt = CIP68Metadata(k.clone())
                        .into_model()?
                        .as_str()
                        .map(|s| s.to_owned());

                    if let Some(key) = key_opt {
                        if CIP68_FIELDS.contains(&key.as_str()) {
                            map.insert(key, CIP68Metadata(v.clone()).into_model()?);
                        } else {
                            map.insert(key, serde_json::Value::String(encode_to_hex(&v)?));
                        }
                    }
                }
                serde_json::Value::Object(map)
            }

            PlutusData::Array(x) => {
                let values = x
                    .iter()
                    .map(|d| CIP68Metadata(d.clone()).into_model())
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                serde_json::Value::Array(values)
            }

            PlutusData::BigInt(x) => match x {
                BigInt::Int(int) => match i64::try_from(*int.deref()) {
                    Ok(num) => serde_json::Value::Number(num.into()),
                    Err(_) => {
                        let hex_str = hex::encode(i128::from(*int.deref()).to_be_bytes());
                        serde_json::Value::String(hex_str)
                    }
                },
                BigInt::BigUInt(bounded_bytes) => {
                    serde_json::Value::String(hex::encode(bounded_bytes.as_slice()))
                }
                BigInt::BigNInt(bounded_bytes) => {
                    serde_json::Value::String(hex::encode(bounded_bytes.as_slice()))
                }
            },

            PlutusData::BoundedBytes(x) => match String::from_utf8(x.to_vec()) {
                Ok(s) => serde_json::Value::String(s),
                Err(_) => serde_json::Value::String(hex::encode(x.as_slice())),
            },
        })
    }
}

struct AssetModelBuilder {
    subject: Vec<u8>,
    unit: String,
    asset_state: dolos_cardano::model::AssetState,
    initial_tx: Option<EraCbor>,
    registry_url: Option<String>,
}

impl AssetModelBuilder {
    fn cip_68_reference_asset(&self) -> Option<String> {
        let policy_id = &self.unit[..56];
        let asset_name = &self.unit[56..];

        let label = asset_name.get(0..8)?;
        if !(label.starts_with('0') && label.ends_with('0')) {
            return None;
        }

        let number = u32::from_str_radix(&label[1..5], 16).ok()?;
        let asset_name_without_label_prefix = &asset_name[8..];

        CIP68Label::from_u32(number).and_then(|label| match label {
            CIP68Label::ReferenceNft => None,
            _ => Some(format!(
                "{}{}{}",
                policy_id,
                CIP68Label::ReferenceNft.to_label(),
                asset_name_without_label_prefix
            )),
        })
    }

    fn onchain_metadata(&self) -> Result<Option<OnchainMetadata>, StatusCode> {
        let Some(EraCbor(era, cbor)) = &self.initial_tx else {
            return Ok(None);
        };

        let era = pallas::ledger::traverse::Era::try_from(*era)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx =
            MultiEraTx::decode_for_era(era, cbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if let Some(ref_asset) = self.cip_68_reference_asset() {
            let ref_asset_output = tx
                .outputs()
                .iter()
                .find(|o| {
                    o.value().assets().iter().any(|multi_asset| {
                        multi_asset.assets().iter().any(|asset| {
                            let mut unit = multi_asset.policy().to_vec();
                            unit.extend(asset.name());
                            let unit = hex::encode(unit);
                            ref_asset == unit
                        })
                    })
                })
                .cloned();

            if let Some(ref_asset_output) = ref_asset_output {
                if let Some(datum_option) = ref_asset_output.datum() {
                    match datum_option {
                        pallas::ledger::primitives::conway::DatumOption::Hash(hash) => {
                            if let Some(cbor_wrap) = tx.find_plutus_data(&hash) {
                                let out =
                                    OnchainMetadata::from_plutus_data(cbor_wrap.to_plutus_data())?;
                                return Ok(out);
                            }
                        }
                        pallas::ledger::primitives::conway::DatumOption::Data(cbor_wrap) => {
                            let out =
                                OnchainMetadata::from_plutus_data(cbor_wrap.to_plutus_data())?;
                            return Ok(out);
                        }
                    };
                }
            }
        }

        let out = tx
            .metadata()
            .find(721)
            .map(|metadatum| OnchainMetadata::from_metadatum(&self.unit, metadatum.clone()))
            .transpose()?
            .flatten();

        Ok(out)
    }

    async fn offchain_metadata(&self, asset: &str) -> Result<Option<OffchainMetadata>, StatusCode> {
        // TODO: apply memory cache
        let Some(url) = &self.registry_url else {
            return Ok(None);
        };

        let url = format!("{url}/metadata/{asset}");

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("Dolos MiniBF")
            .build()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let res = client
            .get(&url)
            .send()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if res.status() != StatusCode::OK {
            return Ok(None);
        }

        let metadata: TokenRegistryMetadata = res
            .json()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if metadata.name.is_none() || metadata.description.is_none() {
            return Ok(None);
        }

        Ok(Some(metadata.into()))
    }

    async fn into_model(self) -> Result<Asset, StatusCode> {
        let policy = self.subject[..28].to_vec();
        let asset = self.subject[28..].to_vec();

        let metadata = self.onchain_metadata()?;

        let onchain_metadata_standard = Some(metadata.as_ref().and_then(|m| m.version));
        let onchain_metadata = metadata.as_ref().map(|m| m.metadata.clone());
        let onchain_metadata_extra = Some(metadata.as_ref().and_then(|m| m.extra.clone()));

        let metadata = self.offchain_metadata(&self.unit).await?.map(Box::new);

        let asset_name = hex::encode(asset);
        let asset_name = (!asset_name.is_empty()).then_some(asset_name);

        let out = Asset {
            asset: hex::encode(&self.subject),
            policy_id: hex::encode(policy),
            asset_name,
            fingerprint: asset_fingerprint(&self.subject)?,
            quantity: self.asset_state.quantity().to_string(),
            initial_mint_tx_hash: self
                .asset_state
                .initial_tx
                .map(|h| h.to_string())
                .unwrap_or_default(),
            mint_or_burn_count: self.asset_state.mint_tx_count as i32,
            onchain_metadata,
            onchain_metadata_standard,
            onchain_metadata_extra,
            metadata,
        };

        Ok(out)
    }
}

pub async fn by_subject<D>(
    Path(unit): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Asset>, StatusCode>
where
    Option<AssetState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let subject = hex::decode(&unit).map_err(|_| StatusCode::BAD_REQUEST)?;
    let entity_key = pallas::crypto::hash::Hasher::<256>::hash(subject.as_slice());

    let registry_url = domain.config.token_registry_url.clone();

    let asset_state = domain
        .read_cardano_entity::<AssetState>(entity_key.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let initial_tx = if let Some(initial_tx) = asset_state.initial_tx {
        domain
            .query()
            .tx_cbor(initial_tx.as_slice().to_vec())
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        None
    };

    let model = AssetModelBuilder {
        subject,
        unit,
        asset_state,
        initial_tx,
        registry_url,
    };

    Ok(Json(model.into_model().await?))
}

pub async fn by_subject_addresses<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetAddressesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let asset = hex::decode(&subject).map_err(|_| Error::InvalidAsset)?;
    let utxoset = domain
        .indexes()
        .utxos_by_asset(&asset)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .into_iter()
        .collect_vec();

    let utxos = domain
        .state()
        .get_utxos(utxoset)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut addresses: HashMap<String, ((BlockSlot, u32), u128)> = HashMap::new();
    for (txoref, eracbor) in utxos {
        let sort = (
            domain
                .indexes()
                .slot_by_tx_hash(txoref.0.as_slice())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?,
            txoref.1,
        );

        let utxo = MultiEraOutput::decode(eracbor.0.try_into().unwrap(), &eracbor.1)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let amount = utxo
            .value()
            .assets()
            .iter()
            .flat_map(|x| {
                let subject = x.policy().to_vec();
                x.assets()
                    .iter()
                    .find(|x| [subject.as_slice(), x.name()].concat() == asset.as_slice())
                    .map(|x| x.any_coin() as u128)
            })
            .sum();

        addresses
            .entry(
                utxo.address()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                    .to_string(),
            )
            .and_modify(|entry| {
                entry.0 = entry.0.min(sort);
                entry.1 += amount;
            })
            .or_insert((sort, amount));
    }

    let mut items = addresses
        .into_iter()
        .sorted_by_key(|(_, (sort, _))| *sort)
        .map(|(address, (_, amount))| AssetAddressesInner {
            address,
            quantity: amount.to_string(),
        })
        .collect_vec();

    if matches!(pagination.order, Order::Desc) {
        items.reverse();
    }

    let sorted = items
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .collect_vec();

    Ok(Json(sorted))
}

fn subject_matches(subject: &[u8], policy: &[u8], name: &[u8]) -> bool {
    [policy, name].concat() == subject
}

fn output_has_subject(subject: &[u8], output: &MultiEraOutput) -> bool {
    for pa in output.value().assets() {
        for asset in pa.assets() {
            if subject_matches(subject, pa.policy().as_slice(), asset.name()) {
                return true;
            }
        }
    }
    false
}

async fn tx_has_subject<D>(
    domain: &Facade<D>,
    subject: &[u8],
    tx: &MultiEraTx<'_>,
) -> Result<bool, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    for (_, output) in tx.produces() {
        if output_has_subject(subject, &output) {
            return Ok(true);
        }
    }

    for input in tx.consumes() {
        if let Some(EraCbor(era, cbor)) = domain
            .query()
            .tx_cbor(input.hash().as_slice().to_vec())
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            let parsed = MultiEraTx::decode_for_era(
                era.try_into()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                &cbor,
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            if let Some(output) = parsed.produces_at(input.index() as usize) {
                if output_has_subject(subject, &output) {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

async fn find_txs<D>(
    domain: &Facade<D>,
    subject: &[u8],
    chain: &ChainSummary,
    pagination: &Pagination,
    block: &[u8],
) -> Result<Vec<AssetTransactionsInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut matches = vec![];

    for (idx, tx) in block.txs().iter().enumerate() {
        if !pagination.should_skip(block.number(), idx)
            && tx_has_subject(domain, subject, tx).await?
        {
            let model = AssetTransactionsInner {
                tx_hash: hex::encode(tx.hash().as_slice()),
                tx_index: idx as i32,
                block_height: block.number() as i32,
                block_time: chain.slot_time(block.slot()) as i32,
            };

            matches.push(model);
        }
    }

    if matches!(pagination.order, Order::Desc) {
        matches = matches.into_iter().rev().collect();
    }

    Ok(matches)
}

pub async fn by_subject_transactions<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetTransactionsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit()?;

    let subject = hex::decode(&subject).map_err(|_| Error::InvalidAsset)?;
    let end_slot = domain.get_tip_slot()?;
    let blocks = domain
        .query()
        .blocks_by_asset(&subject, 0, end_slot)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut blocks = blocks;
    if matches!(pagination.order, Order::Desc) {
        blocks.reverse();
    }

    let mut matches = Vec::new();
    for (_slot, block) in blocks {
        let Some(block) = block else {
            continue;
        };

        let mut txs = find_txs(&domain, &subject, &chain, &pagination, &block)
            .await
            .map_err(Error::Code)?;
        matches.append(&mut txs);

        if matches.len() >= pagination.from() + pagination.count {
            break;
        }
    }

    let transactions = matches
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    Ok(Json(transactions))
}
