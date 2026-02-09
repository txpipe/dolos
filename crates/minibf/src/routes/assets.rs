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
use dolos_cardano::{
    cip25::{cip25_metadata_is_valid, Cip25MetadataVersion},
    cip68::{cip_68_reference_asset, encode_to_hex, parse_cip68_metadata_map, Cip68TokenStandard},
    indexes::{AsyncCardanoQueryExt, CardanoIndexExt, SlotOrder},
    model::AssetState,
    ChainSummary,
};
use dolos_core::{BlockSlot, Domain, EraCbor, IndexStore as _, StateStore as _};
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
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
    fn from_plutus_data(
        plutus_data: PlutusData,
        standard: Cip68TokenStandard,
    ) -> Result<Option<Self>, StatusCode> {
        let PlutusData::Constr(constr) = plutus_data else {
            return Ok(None);
        };

        if constr.fields.len() < 2 {
            return Ok(None);
        }

        let PlutusData::Map(map) = &constr.fields[0] else {
            return Ok(None);
        };

        let version = match &constr.fields[1] {
            PlutusData::BigInt(BigInt::Int(int)) => i64::try_from(*int.deref()).ok(),
            _ => None,
        };

        let Some(version) = version else {
            return Ok(None);
        };

        let metadata = parse_cip68_metadata_map(map.as_slice(), standard)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let version = match version {
            1 => Some(OnchainMetadataStandard::Cip68v1),
            2 => Some(OnchainMetadataStandard::Cip68v2),
            3 => Some(OnchainMetadataStandard::Cip68v3),
            _ => None,
        };

        let extra = constr
            .fields
            .get(2)
            .map(encode_to_hex)
            .transpose()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

        let version = match version {
            Some(2) => Cip25MetadataVersion::V2,
            _ => Cip25MetadataVersion::V1,
        };

        let version = if cip25_metadata_is_valid(&metadata, version) {
            Some(match version {
                Cip25MetadataVersion::V2 => OnchainMetadataStandard::Cip25v2,
                Cip25MetadataVersion::V1 => OnchainMetadataStandard::Cip25v1,
            })
        } else {
            None
        };

        let extra = None;

        Ok(Some(Self {
            metadata,
            version,
            extra,
        }))
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

async fn datum_from_hash<D>(
    domain: &Facade<D>,
    hash: Hash<32>,
) -> Result<Option<PlutusData>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let Some(bytes) = domain
        .query()
        .get_datum(&hash)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Ok(None);
    };

    let datum = minicbor::decode(&bytes).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Some(datum))
}

fn cip68_reference_from_unit(
    unit: &str,
) -> Result<Option<(String, Cip68TokenStandard)>, StatusCode> {
    if unit.len() < 56 {
        return Ok(None);
    }

    let policy_id = &unit[..56];
    let asset_name = &unit[56..];
    cip_68_reference_asset(policy_id, asset_name).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

fn decode_era_tx(era: u16, cbor: &[u8]) -> Result<MultiEraTx<'_>, StatusCode> {
    let era = pallas::ledger::traverse::Era::try_from(era)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    MultiEraTx::decode_for_era(era, cbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn metadata_from_datum_option<D>(
    domain: &Facade<D>,
    datum_option: &pallas::ledger::primitives::conway::DatumOption<'_>,
    standard: Cip68TokenStandard,
) -> Result<Option<OnchainMetadata>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    match datum_option {
        pallas::ledger::primitives::conway::DatumOption::Hash(hash) => {
            let Some(plutus_data) = datum_from_hash(domain, *hash).await? else {
                return Ok(None);
            };
            OnchainMetadata::from_plutus_data(plutus_data, standard)
        }
        pallas::ledger::primitives::conway::DatumOption::Data(cbor_wrap) => {
            OnchainMetadata::from_plutus_data(cbor_wrap.to_plutus_data(), standard)
        }
    }
}

async fn last_cip68_metadata_from_tx<D>(
    domain: &Facade<D>,
    tx: &MultiEraTx<'_>,
    ref_asset_bytes: &[u8],
    standard: Cip68TokenStandard,
) -> Result<Option<OnchainMetadata>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let mut last_metadata = None;

    for output in tx.outputs().iter() {
        if !output_has_subject(ref_asset_bytes, output) {
            continue;
        }

        if let Some(datum_option) = output.datum() {
            if let Some(out) = metadata_from_datum_option(domain, &datum_option, standard).await? {
                last_metadata = Some(out);
            }
        }
    }

    Ok(last_metadata)
}

struct AssetModelBuilder {
    subject: Vec<u8>,
    unit: String,
    asset_state: dolos_cardano::model::AssetState,
    initial_tx: Option<EraCbor>,
    registry_url: Option<String>,
}

impl AssetModelBuilder {
    async fn onchain_metadata<D>(
        &self,
        domain: &Facade<D>,
    ) -> Result<Option<OnchainMetadata>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
        Option<AssetState>: From<D::Entity>,
    {
        let cip68_reference = match cip68_reference_from_unit(&self.unit)? {
            Some((ref_asset, standard)) => {
                let bytes =
                    hex::decode(&ref_asset).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                Some((ref_asset, standard, bytes))
            }
            None => None,
        };

        if let Some((_, standard, ref_asset_bytes)) = &cip68_reference {
            let entity_key = pallas::crypto::hash::Hasher::<256>::hash(ref_asset_bytes.as_slice());
            let ref_state = domain.read_cardano_entity::<AssetState>(entity_key.as_slice())?;

            if let Some(metadata_tx) = ref_state.and_then(|state| state.metadata_tx) {
                if let Some(EraCbor(era, cbor)) = domain
                    .query()
                    .tx_cbor(metadata_tx.as_slice().to_vec())
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                {
                    let tx = decode_era_tx(era, &cbor)?;
                    if let Some(metadata) =
                        last_cip68_metadata_from_tx(domain, &tx, ref_asset_bytes, *standard).await?
                    {
                        return Ok(Some(metadata));
                    }
                }
            }
        }

        if let Some(EraCbor(era, cbor)) = &self.initial_tx {
            let tx = decode_era_tx(*era, cbor)?;

            if let Some((_, standard, ref_asset_bytes)) = &cip68_reference {
                if let Some(metadata) =
                    last_cip68_metadata_from_tx(domain, &tx, ref_asset_bytes, *standard).await?
                {
                    return Ok(Some(metadata));
                }
            }

            let out = tx
                .metadata()
                .find(721)
                .map(|metadatum| OnchainMetadata::from_metadatum(&self.unit, metadatum.clone()))
                .transpose()?
                .flatten();

            return Ok(out);
        }

        Ok(None)
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

    async fn into_model<D>(self, domain: &Facade<D>) -> Result<Asset, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
        Option<AssetState>: From<D::Entity>,
    {
        let policy = self.subject[..28].to_vec();
        let asset = self.subject[28..].to_vec();

        let metadata = self.onchain_metadata(domain).await?;

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
        .read_cardano_entity::<AssetState>(entity_key.as_slice())?
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

    Ok(Json(model.into_model(&domain).await?))
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

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let stream = domain.query().blocks_by_asset_stream(
        &subject,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut matches = Vec::new();
    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::asset::Asset;

    fn invalid_asset() -> &'static str {
        "not-hex-asset"
    }

    fn missing_asset() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    #[tokio::test]
    async fn assets_by_subject_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Asset = serde_json::from_slice(&bytes).expect("failed to parse asset");
    }

    #[tokio::test]
    async fn assets_by_subject_bad_request() {
        let app = TestApp::new();
        let path = format!("/assets/{}", invalid_asset());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn assets_by_subject_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
