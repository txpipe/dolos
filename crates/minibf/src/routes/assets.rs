use std::{
    collections::HashMap,
    ops::Deref,
    sync::{Arc, OnceLock},
    time::Duration,
};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    asset::{Asset, OnchainMetadataStandard},
    asset_addresses_inner::AssetAddressesInner,
    asset_metadata::AssetMetadata as OffchainMetadata,
    asset_policy_inner::AssetPolicyInner,
    asset_transactions_inner::AssetTransactionsInner,
    asset_utxo_content_inner::AssetUtxoContentInner,
};
use dolos_cardano::{
    cip25::{cip25_metadata_is_valid, Cip25MetadataVersion},
    cip68::{
        cip68_metadata_is_valid, cip_68_reference_asset, encode_to_hex, parse_cip68_metadata_map,
        Cip68TokenStandard,
    },
    indexes::{AsyncCardanoQueryExt, CardanoStateIndexExt, SlotOrder},
    model::AssetState,
    periods::first_mints,
    ChainSummary,
};
use dolos_core::{
    ArchiveStore as _, BlockBody, BlockSlot, Domain, DomainError, EraCbor, StateStore as _, TxoRef,
};
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    crypto::hash::Hash,
    ledger::{
        primitives::{BigInt, Metadatum, PlutusData},
        traverse::{MultiEraOutput, MultiEraTx},
        validate::phase2::to_plutus_data::ToPlutusData,
    },
};
use serde::Deserialize;

use crate::{
    error::Error,
    inputs::{match_block_txs, tx_touches_output, InputDeps, InputResolver},
    mapping::{asset_fingerprint, IntoModel},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

mod all;
mod history;
pub use all::all;
pub use history::by_subject_history;

struct OnchainMetadata {
    version: Option<OnchainMetadataStandard>,
    metadata: HashMap<String, serde_json::Value>,
    extra: Option<String>,
    /// This field holds the standard when the metadata comes from a CIP-68
    /// reference datum. Only this datum can declare decimal places.
    cip68: Option<Cip68TokenStandard>,
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

        // a datum with a version that the standard does not define describes
        // nothing. a datum that does not meet the scheme of its label also
        // describes nothing. in both cases the asset uses the CIP-25 metadata
        // from its minting transaction
        let version = match version {
            1 => OnchainMetadataStandard::Cip68v1,
            2 => OnchainMetadataStandard::Cip68v2,
            3 => OnchainMetadataStandard::Cip68v3,
            _ => return Ok(None),
        };

        let metadata = parse_cip68_metadata_map(map.as_slice(), standard)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if !cip68_metadata_is_valid(&metadata, standard) {
            return Ok(None);
        }

        let version = Some(version);

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
            cip68: Some(standard),
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
            cip68: None,
        }))
    }

    /// The decimal places that the asset declares on chain. Only the CIP-68
    /// fungible standard and rich fungible standard define this property.
    /// CIP-25 has no equivalent.
    fn decimals(&self) -> Option<i32> {
        match self.cip68 {
            Some(Cip68TokenStandard::Ft) | Some(Cip68TokenStandard::Rft) => self
                .metadata
                .get("decimals")
                .and_then(serde_json::Value::as_i64)
                .and_then(|value| i32::try_from(value).ok()),
            _ => None,
        }
    }

    /// Whether the asset has usable on-chain metadata. A CIP-25 label that has
    /// no entry for this asset parses into an empty map. An empty map is the
    /// same as no metadata.
    fn is_present(&self) -> bool {
        !self.metadata.is_empty()
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

        // the last matching output that has a datum wins. one transaction can
        // hold the reference token in more than one output. only the output
        // that locks the token has a datum. an output that has no datum makes
        // no claim about the metadata. as a result, it does not replace an
        // earlier datum.
        //
        // a datum has an unknown version, or it does not meet the scheme of its
        // label. in these cases the datum describes nothing, this code returns
        // `None`, and it replaces any earlier CIP-68 metadata. the caller then
        // uses the CIP-25 metadata of the minting transaction.
        if let Some(datum_option) = output.datum() {
            last_metadata = metadata_from_datum_option(domain, &datum_option, standard).await?;
        }
    }

    Ok(last_metadata)
}

/// The time limit for one token-registry request. The client applies this
/// limit. Each request also applies it, so the limit holds even if the client
/// falls back to the default and loses its own limit.
const TOKEN_REGISTRY_TIMEOUT: Duration = Duration::from_secs(10);

/// The HTTP client that every token-registry request shares. `reqwest::Client`
/// holds an internal connection pool, so one instance reuses DNS and TLS across
/// the many concurrent lookups that `/addresses/{address}/extended` starts.
///
/// `get_or_init` builds the client one time. Two concurrent calls do not
/// build two clients. The builder fails only when the TLS backend does not
/// start. In that case, this function uses the default client. As a result,
/// the caller gets a client and does not handle a `Result`. The default
/// client has no time limit, so each request applies `TOKEN_REGISTRY_TIMEOUT`
/// on its own.
fn token_registry_client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(TOKEN_REGISTRY_TIMEOUT)
            .user_agent("Dolos MiniBF")
            .build()
            .unwrap_or_default()
    })
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
            let asset_key = dolos_cardano::AssetKey::of(ref_asset_bytes);
            let ref_state = domain.read_cardano_entity::<AssetState>(&asset_key)?;

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

        let mut cip25_tx = self.initial_tx.clone();
        if let Some(metadata_tx) = self.asset_state.metadata_tx {
            if Some(metadata_tx) != self.asset_state.initial_tx {
                let metadata_cbor = domain
                    .query()
                    .tx_cbor(metadata_tx.as_slice().to_vec())
                    .await
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                if metadata_cbor.is_some() {
                    cip25_tx = metadata_cbor;
                }
            }
        }

        if let Some(EraCbor(era, cbor)) = &cip25_tx {
            let tx = decode_era_tx(*era, cbor)?;

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

        // The off-chain metadata adds to the response. It is not necessary.
        // If the token registry does not answer, or sends data that does not
        // parse, the code reports no metadata. The request does not fail.
        let Ok(res) = token_registry_client()
            .get(&url)
            .timeout(TOKEN_REGISTRY_TIMEOUT)
            .send()
            .await
        else {
            return Ok(None);
        };

        if res.status() != StatusCode::OK {
            return Ok(None);
        }

        let Ok(metadata) = res.json::<TokenRegistryMetadata>().await else {
            return Ok(None);
        };

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

/// Decodes an asset subject: a 56 hex char policy id plus up to 64 hex chars
/// of asset name. Blockfrost rejects any other shape with 400 before looking
/// the asset up.
pub(crate) fn decode_asset_subject(subject: &str) -> Result<Vec<u8>, Error> {
    if !(56..=120).contains(&subject.len()) {
        return Err(Error::InvalidAsset);
    }

    hex::decode(subject).map_err(|_| Error::InvalidAsset)
}

/// Decodes the subject and loads its state. Blockfrost returns 404 for a
/// valid but unknown asset.
fn resolve_asset_state<D>(domain: &Facade<D>, subject: &str) -> Result<(Vec<u8>, AssetState), Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let subject = decode_asset_subject(subject)?;

    let asset_key = dolos_cardano::AssetKey::of(&subject);
    let state = domain
        .read_cardano_entity::<AssetState>(&asset_key)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok((subject, state))
}

/// Loads the minting state and the minting transaction that holds the
/// metadata of an asset.
async fn load_asset_builder<D>(domain: &Facade<D>, unit: String) -> Result<AssetModelBuilder, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let (subject, asset_state) = resolve_asset_state(domain, &unit)?;

    let initial_tx = if let Some(initial_tx) = asset_state.initial_tx {
        domain
            .query()
            .tx_cbor(initial_tx.as_slice().to_vec())
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        None
    };

    Ok(AssetModelBuilder {
        subject,
        unit,
        asset_state,
        initial_tx,
        registry_url: domain.config.token_registry_url.clone(),
    })
}

/// The facts that an amount entry reports about its unit, in addition to the
/// quantity.
pub(crate) struct AssetAmountExtras {
    pub decimals: Option<i32>,
    pub has_nft_onchain_metadata: bool,
}

impl AssetAmountExtras {
    /// Lovelace is not a minted asset. It holds no metadata. Its six decimal
    /// places are a property of the currency itself.
    pub fn lovelace() -> Self {
        Self {
            decimals: Some(6),
            has_nft_onchain_metadata: false,
        }
    }

    /// The values that an asset reports when the code can derive nothing for
    /// it.
    fn bare() -> Self {
        Self {
            decimals: None,
            has_nft_onchain_metadata: false,
        }
    }

    /// Derives the metadata facts of a single unit. If the standard declares
    /// decimal places, they come from the CIP-68 reference datum. If it does
    /// not, they come from the off-chain token registry.
    pub async fn resolve<D>(domain: &Facade<D>, unit: &str) -> Result<Self, Error>
    where
        D: Domain + Clone + Send + Sync + 'static,
        Option<AssetState>: From<D::Entity>,
    {
        let builder = match load_asset_builder(domain, unit.to_string()).await {
            Ok(builder) => builder,
            // an asset that a live utxo holds always has minting state. a
            // missing state must not make the whole address request fail
            Err(Error::InvalidAsset) | Err(Error::Code(StatusCode::NOT_FOUND)) => {
                return Ok(Self::bare())
            }
            Err(err) => return Err(err),
        };

        let onchain = builder.onchain_metadata(domain).await?;

        // the registry request reaches an external service. the on-chain datum
        // already declares the decimal places of every CIP-68 fungible asset,
        // so a known value makes the request unnecessary
        let onchain_decimals = onchain.as_ref().and_then(OnchainMetadata::decimals);
        let registry = if onchain_decimals.is_some() {
            None
        } else {
            builder.offchain_metadata(unit).await?
        };

        Ok(Self {
            decimals: onchain_decimals.or_else(|| registry.and_then(|metadata| metadata.decimals)),
            has_nft_onchain_metadata: onchain.is_some_and(|x| x.is_present()),
        })
    }
}

pub async fn by_subject<D>(
    Path(unit): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Asset>, Error>
where
    Option<AssetState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let model = load_asset_builder(&domain, unit).await?;

    Ok(Json(model.into_model(&domain).await?))
}

/// UTxOs of an asset read, decoded and located per blocking task. Each one
/// also costs an archive index lookup, so one chunk holds its permit for a
/// few milliseconds.
const ASSET_ADDRESS_CHUNK: usize = 256;

/// Per address: the earliest chain position of a UTxO holding the asset, and
/// the quantity held.
type AssetHoldings = HashMap<String, ((BlockSlot, u32), u128)>;

pub async fn by_subject_addresses<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetAddressesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let (asset, _) = resolve_asset_state(&domain, &subject)?;

    let addresses = asset_holdings(&domain, asset, ASSET_ADDRESS_CHUNK).await?;

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

/// The holdings of every address with a UTxO that holds `asset`, `chunk`
/// UTxOs per blocking task.
async fn asset_holdings<D>(
    domain: &Facade<D>,
    asset: Vec<u8>,
    chunk: usize,
) -> Result<AssetHoldings, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let asset = Arc::new(asset);

    let utxoset = {
        let asset = asset.clone();
        domain
            .blocking(move |domain| {
                domain
                    .state()
                    .utxos_by_asset(&asset)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
            })
            .await?
    };

    let mut refs = utxoset.into_iter().collect_vec();
    refs.sort();

    let mut addresses = AssetHoldings::new();

    for refs in refs.chunks(chunk.max(1)) {
        let refs = refs.to_vec();
        let asset = asset.clone();

        let holdings = domain
            .blocking(move |domain| chunk_holdings(&domain, refs, &asset))
            .await?;

        for (address, (sort, amount)) in holdings {
            addresses
                .entry(address)
                .and_modify(|entry| {
                    entry.0 = entry.0.min(sort);
                    entry.1 += amount;
                })
                .or_insert((sort, amount));
        }
    }

    Ok(addresses)
}

fn chunk_holdings<D>(
    domain: &Facade<D>,
    refs: Vec<TxoRef>,
    asset: &[u8],
) -> Result<AssetHoldings, StatusCode>
where
    D: Domain,
{
    let utxos = domain
        .state()
        .get_utxos(refs)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut addresses = AssetHoldings::new();
    for (txoref, eracbor) in utxos {
        let sort = (
            domain
                .archive()
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
                    .find(|x| [subject.as_slice(), x.name()].concat() == asset)
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

    Ok(addresses)
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

fn tx_has_subject(
    resolver: &mut InputResolver<'_>,
    subject: &[u8],
    tx: &MultiEraTx<'_>,
) -> Result<bool, StatusCode> {
    tx_touches_output(resolver, tx, |output| {
        Ok(output_has_subject(subject, output))
    })
}

async fn find_txs<D>(
    domain: &Facade<D>,
    deps: &mut InputDeps,
    subject: &[u8],
    chain: &ChainSummary,
    pagination: &Pagination,
    block: BlockBody,
) -> Result<Vec<AssetTransactionsInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let subject = subject.to_vec();
    let matched = match_block_txs(domain, deps, pagination, block, move |resolver, tx| {
        tx_has_subject(resolver, &subject, tx)
    })
    .await?;

    let mut matches: Vec<_> = matched
        .into_iter()
        .map(|tx| AssetTransactionsInner {
            tx_hash: hex::encode(tx.hash.as_slice()),
            tx_index: tx.index as i32,
            block_height: tx.block_number as i32,
            block_time: chain.slot_time(tx.block_slot) as i32,
        })
        .collect();

    if matches!(pagination.order, Order::Desc) {
        matches = matches.into_iter().rev().collect();
    }

    Ok(matches)
}

/// `GET /assets/{subject}/utxos`
///
/// Live UTxOs that carry the asset, in chain order. `from` / `to` bound the
/// creation block height, inclusive. A valid but unknown asset is a 404; a
/// known asset that no live UTxO holds any more (fully burned) is an empty
/// page.
pub async fn by_subject_utxos<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let (asset, _) = resolve_asset_state(&domain, &subject)?;

    let refs = domain
        .state()
        .utxos_by_asset(&asset)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos = super::utxos::load_utxo_models_in_height_range(&domain, refs, pagination).await?;

    Ok(Json(utxos))
}

pub async fn by_subject_transactions<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetTransactionsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let (subject, _) = resolve_asset_state(&domain, &subject)?;

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
    let mut deps = InputDeps::default();
    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(block) = block else {
            continue;
        };

        let mut txs = find_txs(&domain, &mut deps, &subject, &chain, &pagination, block)
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

/// Alias of `/assets/{subject}/transactions` that returns only the tx hashes.
/// Same scan, same order/count/page pagination, thinner payload.
pub async fn by_subject_txs<D>(
    path: Path<String>,
    Query(mut params): Query<PaginationParameters>,
    state: State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    // Blockfrost does not define `from`/`to` for this variant and ignores any
    // supplied values, so the alias drops them before delegating.
    params.from = None;
    params.to = None;

    let Json(transactions) = by_subject_transactions(path, Query(params), state).await?;

    let hashes = transactions.into_iter().map(|x| x.tx_hash).collect();

    Ok(Json(hashes))
}

/// `GET /assets/policy/{policy}`: the assets a policy minted, ordered by
/// first mint, from the first-mint period logs.
pub async fn by_policy<D>(
    Path(policy_hex): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetPolicyInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let policy = hex::decode(&policy_hex).map_err(|_| Error::InvalidPolicy)?;
    let policy: Hash<28> = <[u8; 28]>::try_from(policy.as_slice())
        .map_err(|_| Error::InvalidPolicy)?
        .into();

    let chain = domain.get_chain_summary()?;
    let Some(mary_epoch) = chain.first_mary_epoch() else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let (tip_epoch, _) = chain.slot_epoch(domain.get_tip_slot()?);

    let order = match pagination.order {
        Order::Asc => SlotOrder::Asc,
        Order::Desc => SlotOrder::Desc,
    };
    let (skip, count) = (pagination.skip(), pagination.count);

    let subjects = domain
        .query()
        .run_blocking(move |domain| {
            first_mints::policy_page(
                &domain,
                &chain,
                mary_epoch..=tip_epoch,
                &policy,
                order,
                skip,
                count,
            )
            .map_err(DomainError::from)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Blockfrost returns 404 when the policy has no mint operations.
    let subjects = subjects.ok_or(StatusCode::NOT_FOUND)?;

    let mut items = Vec::with_capacity(subjects.len());

    for subject in subjects {
        let asset_key = dolos_cardano::AssetKey::of(&subject);
        let asset_state = domain
            .read_cardano_entity::<AssetState>(&asset_key)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        items.push(AssetPolicyInner {
            asset: hex::encode(&subject),
            quantity: asset_state.quantity().to_string(),
        });
    }

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        first_mint_chain, first_mint_policies, health_beside, TestApp, TestFault, SLOW_READ,
    };
    use blockfrost_openapi::models::asset::Asset;
    use dolos_cardano::model::PolicyMintChunk;
    use dolos_core::AsyncQueryFacade;
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use pallas::ledger::traverse::MultiEraBlock;

    /// The scan the period logs replaced: the policy's tagged blocks from
    /// genesis, each name kept at its first appearance in a mint.
    async fn scan_policy(domain: &ToyDomain, policy: &[u8]) -> Vec<Vec<u8>> {
        let end_slot = domain.state().read_cursor().unwrap().unwrap().slot();
        let stream = AsyncQueryFacade::new(domain.clone()).blocks_by_policy_stream(
            policy,
            0,
            end_slot,
            SlotOrder::Asc,
        );
        let mut stream = Box::pin(stream);

        let mut seen = std::collections::HashSet::new();
        let mut subjects = Vec::new();

        while let Some(res) = stream.next().await {
            let Some(block) = res.unwrap().1 else {
                continue;
            };
            let block = MultiEraBlock::decode(&block).unwrap();

            for tx in block.txs() {
                for policy_assets in tx.mints() {
                    if policy_assets.policy().as_slice() != policy {
                        continue;
                    }

                    for asset in policy_assets.assets() {
                        let subject = [policy, asset.name()].concat();

                        if seen.insert(subject.clone()) {
                            subjects.push(subject);
                        }
                    }
                }
            }
        }

        subjects
    }

    fn invalid_assets() -> Vec<String> {
        vec![
            // non-hex characters
            "not-hex-asset".to_string(),
            // valid hex, shorter than a policy id
            "abcd".to_string(),
            // valid hex, longer than a policy id plus the longest asset name
            "f".repeat(122),
            // valid length, non-hex characters
            "z".repeat(56),
        ]
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
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
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

    #[tokio::test]
    async fn assets_by_subject_addresses_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/addresses");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let addresses: Vec<AssetAddressesInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset addresses");
        assert!(!addresses.is_empty());
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/addresses");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/addresses", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/addresses");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/transactions");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let items: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset transactions");
        assert!(!items.is_empty());

        // every tx must sit exactly where the synthetic chain placed it
        for item in items {
            let (block_number, tx_index) = app.vectors().tx_position(&item.tx_hash);
            assert_eq!(item.block_height as u64, block_number);
            assert_eq!(item.tx_index as usize, tx_index);
        }
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_slot_constrained() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/assets/{asset}/transactions?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset transactions");
        assert!(!items.is_empty());
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_paginated() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path_page_1 = format!("/assets/{asset}/transactions?page=1&count=2");
        let path_page_2 = format!("/assets/{asset}/transactions?page=2&count=2");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse transactions page 1");
        let page_2: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse transactions page 2");

        assert_eq!(page_1.len(), 2);
        assert_eq!(page_2.len(), 2);

        let page_1_hashes: std::collections::HashSet<_> =
            page_1.into_iter().map(|x| x.tx_hash).collect();
        let page_2_hashes: std::collections::HashSet<_> =
            page_2.into_iter().map(|x| x.tx_hash).collect();
        assert!(page_1_hashes.is_disjoint(&page_2_hashes));
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_order_asc() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/transactions?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions asc");
        assert!(!asc.is_empty());
        let asc_pos: Vec<_> = asc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] < w[1]));
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_order_desc() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/transactions?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions desc");
        assert!(!desc.is_empty());
        let desc_pos: Vec<_> = desc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] > w[1]));
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/transactions");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    fn parse_utxos(bytes: &[u8]) -> Vec<AssetUtxoContentInner> {
        serde_json::from_slice(bytes).expect("failed to parse asset utxos")
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/utxos");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let items = parse_utxos(&bytes);
        assert!(!items.is_empty());

        for item in items {
            // every UTxO must carry the asset and sit where the chain put it
            assert!(item.amount.iter().any(|x| x.unit == asset));
            assert!(item.amount.iter().any(|x| x.unit == "lovelace"));
            let (block_number, _) = app.vectors().tx_position(&item.tx_hash);
            assert_eq!(item.block_height as u64, block_number);
            assert!(!item.block.is_empty());
            assert!(item.block_time > 0);
            assert_eq!(item.inline_datum, None);
            assert_eq!(item.inline_datum_json, None);
        }
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_paginated() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/assets/{asset}/utxos?page=1&count=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/assets/{asset}/utxos?page=2&count=1"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1 = parse_utxos(&bytes_1);
        let page_2 = parse_utxos(&bytes_2);
        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);

        let key = |x: &AssetUtxoContentInner| format!("{}#{}", x.tx_hash, x.output_index);
        assert_ne!(key(&page_1[0]), key(&page_2[0]));
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_order() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?order=asc"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let asc = parse_utxos(&bytes);

        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?order=desc"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let desc = parse_utxos(&bytes);

        assert!(asc.len() > 1);
        let position = |x: &AssetUtxoContentInner| {
            let (_, tx_index) = app.vectors().tx_position(&x.tx_hash);
            (x.block_height, tx_index, x.output_index)
        };
        let asc_pos: Vec<_> = asc.iter().map(position).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] < w[1]));

        let mut reversed = asc;
        reversed.reverse();
        assert_eq!(desc, reversed);
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_height_constrained() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/utxos")).await;
        assert_eq!(status, StatusCode::OK);
        let all = parse_utxos(&bytes);
        let heights: std::collections::BTreeSet<_> = all.iter().map(|x| x.block_height).collect();
        assert!(heights.len() > 1, "need UTxOs in more than one block");
        let pivot = *heights.iter().nth(1).expect("second height");

        // inclusive on both ends
        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?from={pivot}&to={pivot}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let within = parse_utxos(&bytes);
        assert!(!within.is_empty());
        assert!(within.iter().all(|x| x.block_height == pivot));

        // one-sided bounds
        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?from={pivot}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let from_pivot = parse_utxos(&bytes);
        assert!(from_pivot.iter().all(|x| x.block_height >= pivot));
        assert!(from_pivot.len() < all.len());

        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?to={pivot}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let to_pivot = parse_utxos(&bytes);
        assert!(to_pivot.iter().all(|x| x.block_height <= pivot));
        assert_eq!(from_pivot.len() + to_pivot.len(), all.len() + within.len());

        // a range past the tip is an empty page, not an error
        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?from=99999999"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(parse_utxos(&bytes).is_empty());
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/utxos");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }

        let asset = app.vectors().asset_unit.as_str();
        for query in ["from=abc", "from=999999999&to=1", "order=a", "count=0"] {
            let path = format!("/assets/{asset}/utxos?{query}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/utxos", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/transactions", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/transactions");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn assets_by_subject_txs_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/txs");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let hashes: Vec<String> =
            serde_json::from_slice(&bytes).expect("failed to parse asset txs");
        assert!(!hashes.is_empty());

        let known: std::collections::HashSet<&String> = app
            .vectors()
            .blocks
            .iter()
            .flat_map(|block| block.tx_hashes.iter())
            .collect();
        for hash in &hashes {
            assert!(known.contains(hash), "unknown tx hash {hash}");
        }
    }

    #[tokio::test]
    async fn assets_by_subject_txs_matches_transactions() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        for query in [
            "",
            "?order=desc",
            "?page=2&count=2",
            "?order=desc&page=1&count=3",
        ] {
            let (status, bytes) = app
                .get_bytes(&format!("/assets/{asset}/transactions{query}"))
                .await;
            assert_eq!(status, StatusCode::OK);
            let expected: Vec<String> =
                serde_json::from_slice::<Vec<AssetTransactionsInner>>(&bytes)
                    .expect("failed to parse asset transactions")
                    .into_iter()
                    .map(|x| x.tx_hash)
                    .collect();

            assert!(!expected.is_empty(), "no transactions for query {query:?}");

            let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/txs{query}")).await;
            assert_eq!(status, StatusCode::OK);
            let actual: Vec<String> =
                serde_json::from_slice(&bytes).expect("failed to parse asset txs");

            assert_eq!(actual, expected, "mismatch for query {query:?}");
        }
    }

    #[tokio::test]
    async fn assets_by_subject_txs_ignores_from_to() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");

        let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/txs")).await;
        assert_eq!(status, StatusCode::OK);
        let all: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse asset txs");

        // the range must actually constrain `/transactions`, otherwise the
        // comparison below proves nothing
        let (status, bytes) = app
            .get_bytes(&format!(
                "/assets/{asset}/transactions?from={}&to={}",
                block.block_number, block.block_number
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let constrained: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset transactions");
        assert!(constrained.len() < all.len());

        let (status, bytes) = app
            .get_bytes(&format!(
                "/assets/{asset}/txs?from={}&to={}",
                block.block_number, block.block_number
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let ranged: Vec<String> =
            serde_json::from_slice(&bytes).expect("failed to parse asset txs");
        assert_eq!(ranged, all);

        // a malformed range is ignored as well, never validated
        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/txs?from=not-a-number"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let ranged: Vec<String> =
            serde_json::from_slice(&bytes).expect("failed to parse asset txs");
        assert_eq!(ranged, all);
    }

    #[tokio::test]
    async fn assets_by_subject_txs_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/txs");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_txs_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/txs", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_txs_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/txs");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    fn unminted_policy() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    #[tokio::test]
    async fn assets_by_policy_happy_path() {
        let app = TestApp::new();
        let policy = app.vectors().policy_id.clone();
        let unit = app.vectors().asset_unit.clone();
        let path = format!("/assets/policy/{policy}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let assets: Vec<AssetPolicyInner> =
            serde_json::from_slice(&bytes).expect("failed to parse policy assets");
        assert!(assets.iter().any(|a| a.asset == unit));
    }

    #[tokio::test]
    async fn assets_by_policy_ascending_pagination_preserves_first_mint_order() {
        let asset_names = ["FIRST", "SECOND", "THIRD"];
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: asset_names.len(),
            txs_per_block: 1,
            asset_names_by_block: asset_names.iter().map(|x| (*x).to_string()).collect(),
            ..Default::default()
        });
        let policy = app.vectors().policy_id.as_str();

        for (index, asset_name) in asset_names.into_iter().enumerate() {
            let page = index + 1;
            let path = format!("/assets/policy/{policy}?order=asc&page={page}&count=1");
            let (status, bytes) = app.get_bytes(&path).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "unexpected status {status} with body: {}",
                String::from_utf8_lossy(&bytes)
            );

            let assets: Vec<AssetPolicyInner> =
                serde_json::from_slice(&bytes).expect("failed to parse policy assets");
            let expected = format!("{policy}{}", hex::encode(asset_name));
            assert_eq!(assets.len(), 1);
            assert_eq!(assets[0].asset, expected);
            assert_eq!(assets[0].quantity, "1");
        }
    }

    /// Every page in both orders of every policy, as the scan the logs
    /// replaced answers it, over a chain whose first epochs are sealed and
    /// whose last are staged.
    #[tokio::test]
    async fn assets_by_policy_matches_the_policy_scan() {
        use first_mint_policies::*;

        let mut domain = None;
        let app = TestApp::new_with_cfg_and_setup(first_mint_chain(), |inner, _| {
            domain = Some(inner.clone())
        });
        let domain = domain.expect("setup ran");

        let sealed = domain
            .archive()
            .iter_logs_typed::<PolicyMintChunk>(None)
            .unwrap()
            .count();
        assert!(sealed > 4, "{sealed} sealed rows");

        for policy in [ALPHA, BETA, GAMMA, SIBLING] {
            let all = scan_policy(&domain, &policy).await;
            assert!(!all.is_empty());

            for (order, name) in [(Order::Asc, "asc"), (Order::Desc, "desc")] {
                let ordered: Vec<String> = match order {
                    Order::Asc => all.iter().map(hex::encode).collect(),
                    Order::Desc => all.iter().rev().map(hex::encode).collect(),
                };

                for count in [1, 3, 100] {
                    for page in 1..=all.len().div_ceil(count) + 1 {
                        let skip = (page - 1) * count;
                        let expected: Vec<String> =
                            ordered.iter().skip(skip).take(count).cloned().collect();

                        let path = format!(
                            "/assets/policy/{}?order={name}&count={count}&page={page}",
                            hex::encode(policy)
                        );
                        let (status, bytes) = app.get_bytes(&path).await;
                        assert_eq!(status, StatusCode::OK, "{path}");

                        let got: Vec<AssetPolicyInner> = serde_json::from_slice(&bytes).unwrap();
                        let got: Vec<String> = got.into_iter().map(|x| x.asset).collect();

                        assert_eq!(got, expected, "{path}");
                    }
                }
            }
        }

        let path = format!("/assets/policy/{}", unminted_policy());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_policy_bad_request() {
        let app = TestApp::new();
        // This policy contains non-hexadecimal characters.
        assert_status(&app, "/assets/policy/not-hex", StatusCode::BAD_REQUEST).await;
        // This policy contains fewer than 28 bytes.
        assert_status(&app, "/assets/policy/abcd", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn assets_by_policy_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/policy/{}", unminted_policy());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_policy_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let policy = app.vectors().policy_id.clone();
        let path = format!("/assets/policy/{policy}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_asset_addresses_do_not_stall_health() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowUtxoReads(SLOW_READ)));
        let path = format!("/assets/{}/addresses", app.vectors().asset_unit);

        assert_eq!(health_beside(&app, &path).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn chunked_asset_holdings_match_unchunked() {
        let app = TestApp::new();
        let facade = app.facade();
        let asset = decode_asset_subject(&app.vectors().asset_unit).expect("asset subject");

        let utxos = facade.state().utxos_by_asset(&asset).expect("asset refs");
        assert!(utxos.len() > 2, "too few utxos to split into chunks");

        let whole = asset_holdings(&facade, asset.clone(), usize::MAX)
            .await
            .expect("holdings");

        for chunk in [1, 2] {
            let chunked = asset_holdings(&facade, asset.clone(), chunk)
                .await
                .expect("holdings");
            assert_eq!(chunked, whole);
        }
    }
}
