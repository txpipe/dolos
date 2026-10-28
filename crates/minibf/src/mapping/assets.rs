//! The Blockfrost model of an asset: its on-chain metadata (CIP-25, CIP-68),
//! the off-chain token-registry metadata and the per-unit facts an amount
//! entry reports.

use std::{collections::HashMap, ops::Deref, sync::OnceLock, time::Duration};

use axum::http::StatusCode;
use blockfrost_openapi::models::{
    asset::{Asset, OnchainMetadataStandard},
    asset_metadata::AssetMetadata as OffchainMetadata,
};
use dolos_cardano::{
    cip25::{cip25_metadata_is_valid, Cip25MetadataVersion},
    cip68::{
        cip68_metadata_is_valid, cip_68_reference_asset, encode_to_hex, parse_cip68_metadata_map,
        Cip68TokenStandard,
    },
    indexes::AsyncCardanoQueryExt,
    model::AssetState,
};
use dolos_core::{Domain, EraCbor};

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
    mapping::{asset_fingerprint, IntoModel},
    Facade,
};

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

pub struct AssetModelBuilder {
    pub subject: Vec<u8>,
    pub unit: String,
    pub asset_state: AssetState,
    pub initial_tx: Option<EraCbor>,
    pub registry_url: Option<String>,
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

    pub async fn into_model<D>(self, domain: &Facade<D>) -> Result<Asset, StatusCode>
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
pub(crate) fn resolve_asset_state<D>(
    domain: &Facade<D>,
    subject: &str,
) -> Result<(Vec<u8>, AssetState), Error>
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
pub(crate) async fn load_asset_builder<D>(
    domain: &Facade<D>,
    unit: String,
) -> Result<AssetModelBuilder, Error>
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

pub(crate) fn subject_matches(subject: &[u8], policy: &[u8], name: &[u8]) -> bool {
    [policy, name].concat() == subject
}

pub(crate) fn output_has_subject(subject: &[u8], output: &MultiEraOutput) -> bool {
    for pa in output.value().assets() {
        for asset in pa.assets() {
            if subject_matches(subject, pa.policy().as_slice(), asset.name()) {
                return true;
            }
        }
    }
    false
}
