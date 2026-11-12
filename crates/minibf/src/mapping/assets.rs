//! The Blockfrost model of an asset: its on-chain metadata (CIP-25, CIP-68),
//! the off-chain token-registry metadata and the per-unit facts an amount
//! entry reports.

use std::{collections::HashMap, ops::Deref, sync::OnceLock, time::Duration};

use axum::{http::StatusCode, Json};
use blockfrost_openapi::models::{
    asset::{Asset, OnchainMetadataStandard},
    asset_metadata::AssetMetadata as OffchainMetadata,
};
use dolos_cardano::{
    cip25::{cip25_metadata_is_valid, Cip25MetadataVersion},
    cip68::{
        cip68_metadata_is_valid, cip_68_reference_asset, encode_to_hex,
        parse_cip67_label_from_asset_name, parse_cip68_metadata_map, Cip68TokenStandard,
    },
    indexes::AsyncCardanoQueryExt,
    periods::assets::{self, AssetSummary, WalkBudget},
    ChainSummary,
};
use dolos_core::{BlockSlot, Domain, EraCbor};
use futures_util::{StreamExt as _, TryStreamExt as _};

use pallas::{
    codec::{minicbor, tree::fold_tree},
    crypto::hash::Hash,
    ledger::{
        primitives::{BigInt, Epoch, Metadatum, PlutusData},
        traverse::{MultiEraOutput, MultiEraTx},
        validate::phase2::to_plutus_data::ToPlutusData,
    },
};
use serde::Deserialize;

use crate::{
    error::Error,
    mapping::{
        asset_fingerprint,
        direct_json::{drop_value, ValueBody},
        sorted_json,
    },
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
        let value = cip25_metadata_json(&metadatum);

        let (metadata, version) = match value {
            serde_json::Value::Object(mut map) => {
                let policy_id = &unit[..56];
                let asset_name_raw = &unit[56..];

                let asset_name = hex::decode(asset_name_raw)
                    .ok()
                    .and_then(|v| String::from_utf8(v).ok())
                    .unwrap_or_else(|| asset_name_raw.to_string());

                let version = map.get("version").and_then(|v| match v {
                    serde_json::Value::Number(num) => num.as_i64(),
                    serde_json::Value::String(s) => s.parse::<f64>().ok().map(|f| f as i64),
                    _ => None,
                });

                // moves the asset's properties out instead of copying them
                let metadata = take_object(map.remove(policy_id))
                    .and_then(|mut policy_metadata| {
                        let asset_metadata = take_object(policy_metadata.remove(&asset_name));
                        drop_value(serde_json::Value::Object(policy_metadata));
                        asset_metadata
                    })
                    .map(|asset_metadata| asset_metadata.into_iter().collect())
                    .unwrap_or_default();

                // the metadata of the other policies and assets
                drop_value(serde_json::Value::Object(map));

                (metadata, version)
            }
            other => {
                drop_value(other);
                (HashMap::new(), None)
            }
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

    /// Moves out the properties, the standard and the extra datum.
    fn into_parts(
        mut self,
    ) -> (
        HashMap<String, serde_json::Value>,
        Option<OnchainMetadataStandard>,
        Option<String>,
    ) {
        (
            std::mem::take(&mut self.metadata),
            self.version,
            self.extra.take(),
        )
    }
}

/// Frees the properties with [`drop_value`], on every path that does not move
/// them into a response.
impl Drop for OnchainMetadata {
    fn drop(&mut self) {
        for (_, value) in self.metadata.drain() {
            drop_value(value);
        }
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

/// Returns the object in `value`. A value of another kind is freed with
/// [`drop_value`].
fn take_object(
    value: Option<serde_json::Value>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    match value? {
        serde_json::Value::Object(fields) => Some(fields),
        other => {
            drop_value(other);
            None
        }
    }
}

/// Returns the CIP-25 text of a byte string: UTF-8, or hex without a prefix.
fn cip25_bytes(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(text) => text.to_string(),
        Err(_) => hex::encode(bytes),
    }
}

/// Converts a CIP-25 metadatum to JSON: bytes become UTF-8 or bare hex, and
/// a map keeps only the entries whose key converts to a JSON string.
fn cip25_metadata_json(metadatum: &Metadatum) -> serde_json::Value {
    fold_tree(
        metadatum,
        |node, children: Vec<serde_json::Value>| match node {
            Metadatum::Int(x) => serde_json::Number::from_i128(i128::from(*x))
                .map(serde_json::Value::Number)
                .unwrap_or_else(|| serde_json::Value::String(x.to_string())),
            Metadatum::Text(x) => serde_json::Value::String(x.clone()),
            Metadatum::Bytes(x) => serde_json::Value::String(cip25_bytes(x)),
            Metadatum::Array(_) => serde_json::Value::Array(children),
            Metadatum::Map(_) => {
                let mut fields = serde_json::Map::new();
                let mut children = children.into_iter();

                // children alternate key and value
                while let (Some(key), Some(value)) = (children.next(), children.next()) {
                    match key {
                        serde_json::Value::String(key) => {
                            // a repeated key keeps its last value
                            if let Some(replaced) = fields.insert(key, value) {
                                drop_value(replaced);
                            }
                        }
                        other => {
                            drop_value(other);
                            drop_value(value);
                        }
                    }
                }

                serde_json::Value::Object(fields)
            }
        },
    )
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

/// Asks the token registry at `url` for the metadata of `asset`.
async fn token_registry_metadata(
    url: &str,
    asset: &str,
) -> Result<Option<OffchainMetadata>, StatusCode> {
    // TODO: apply memory cache
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

pub struct AssetModelBuilder {
    pub subject: Vec<u8>,
    pub unit: String,
    pub summary: AssetSummary,
    pub initial_tx: Option<EraCbor>,
    pub registry_url: Option<String>,
    chain: ChainSummary,
    tip: Epoch,
    /// The request's label-100 walks, or its unit's share of them in
    /// `/addresses/{address}/extended`, shared with the reference token's.
    budget: WalkBudget,
}

impl AssetModelBuilder {
    async fn onchain_metadata<D>(
        &self,
        domain: &Facade<D>,
    ) -> Result<Option<OnchainMetadata>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
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
            let reference = assets::asset_summary(
                &domain.inner,
                &self.chain,
                self.tip,
                ref_asset_bytes.clone(),
                &self.budget,
            )
            .await
            .map_err(log_500)?;

            if let Some(metadata_tx) = reference.and_then(|x| x.fold.metadata_tx) {
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
        if let Some(metadata_tx) = self.summary.fold.metadata_tx {
            if Some(metadata_tx) != self.summary.fold.initial_tx {
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
        match &self.registry_url {
            Some(url) => token_registry_metadata(url, asset).await,
            None => Ok(None),
        }
    }

    pub async fn into_model<D>(self, domain: &Facade<D>) -> Result<Asset, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        let policy = self.subject[..28].to_vec();
        let asset = self.subject[28..].to_vec();

        let onchain = self.onchain_metadata(domain).await?;
        let metadata = self.offchain_metadata(&self.unit).await?.map(Box::new);
        let fingerprint = asset_fingerprint(&self.subject)?;

        // moves the on-chain metadata instead of copying it. until here, an
        // early return frees it through `OnchainMetadata`
        let (onchain_metadata, onchain_metadata_standard, onchain_metadata_extra) = match onchain {
            Some(onchain) => {
                let (metadata, version, extra) = onchain.into_parts();
                (Some(metadata), Some(version), Some(extra))
            }
            None => (None, Some(None), Some(None)),
        };

        let asset_name = hex::encode(asset);
        let asset_name = (!asset_name.is_empty()).then_some(asset_name);

        let out = Asset {
            asset: hex::encode(&self.subject),
            policy_id: hex::encode(policy),
            asset_name,
            fingerprint,
            quantity: self.summary.fold.quantity().to_string(),
            initial_mint_tx_hash: self
                .summary
                .fold
                .initial_tx
                .map(|h| h.to_string())
                .unwrap_or_default(),
            mint_or_burn_count: self.summary.fold.mint_tx_count as i32,
            onchain_metadata,
            onchain_metadata_standard,
            onchain_metadata_extra,
            metadata,
        };

        Ok(out)
    }
}

/// Serializes an asset with sorted keys at every level. The on-chain metadata
/// moves into the sorted map without a copy.
pub(crate) fn asset_json(mut asset: Asset) -> Result<ValueBody, StatusCode> {
    // a `ValueBody` frees the metadata on an early return
    let onchain_metadata = asset
        .onchain_metadata
        .take()
        .map(|metadata| ValueBody(serde_json::Value::Object(metadata.into_iter().collect())));

    let Json(mut json) = sorted_json(asset)?;

    if let (Some(mut onchain_metadata), serde_json::Value::Object(fields)) =
        (onchain_metadata, &mut json)
    {
        fields.insert(
            "onchain_metadata".to_string(),
            std::mem::take(&mut onchain_metadata.0),
        );
    }

    Ok(ValueBody(json))
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

    /// Derives the on-chain metadata facts of a single unit. If the standard
    /// declares decimal places, they come from the CIP-68 reference datum. If
    /// it does not, [`OnchainExtras::with_registry`] takes them from the
    /// off-chain token registry. Its label-100 walks draw on `budget`.
    pub async fn onchain<D>(
        domain: &Facade<D>,
        unit: &str,
        budget: &WalkBudget,
    ) -> Result<OnchainExtras, Error>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        let builder = match load_asset_builder(domain, unit.to_string(), budget).await {
            Ok(builder) => builder,
            // an asset that a live utxo holds always has minting state. a
            // missing state must not make the whole address request fail
            Err(Error::InvalidAsset) | Err(Error::Code(StatusCode::NOT_FOUND)) => {
                return Ok(OnchainExtras::known(Self::bare()))
            }
            Err(err) => return Err(err),
        };

        let onchain = builder.onchain_metadata(domain).await?;
        let decimals = onchain.as_ref().and_then(OnchainMetadata::decimals);

        let extras = Self {
            decimals,
            has_nft_onchain_metadata: onchain.is_some_and(|x| x.is_present()),
        };

        // the registry request reaches an external service. the on-chain datum
        // already declares the decimal places of every CIP-68 fungible asset,
        // so a known value makes the request unnecessary
        let registry = match (decimals, builder.registry_url) {
            (None, Some(url)) => Some((url, builder.unit)),
            _ => None,
        };

        Ok(OnchainExtras { extras, registry })
    }
}

/// A unit's extras as the chain gives them, and the unit to ask the token
/// registry for its decimal places where the chain declares none. It keeps
/// nothing else of the unit's reads, so a request can hold every unit's at
/// once.
pub(crate) struct OnchainExtras {
    extras: AssetAmountExtras,
    /// The registry's URL and the unit to ask it for.
    registry: Option<(String, String)>,
}

impl OnchainExtras {
    pub fn known(extras: AssetAmountExtras) -> Self {
        Self {
            extras,
            registry: None,
        }
    }

    /// The extras, with the registry's decimal places where it is asked.
    pub async fn with_registry(self) -> Result<AssetAmountExtras, Error> {
        let Some((url, unit)) = self.registry else {
            return Ok(self.extras);
        };

        let registry = token_registry_metadata(&url, &unit).await?;

        Ok(AssetAmountExtras {
            decimals: self
                .extras
                .decimals
                .or_else(|| registry.and_then(|metadata| metadata.decimals)),
            ..self.extras
        })
    }
}

/// Whether resolving `unit` walks a CIP-68 reference token's blocks for its
/// metadata tx: the unit is one (label 100), or a user token whose metadata
/// one holds.
pub(crate) fn walks_reference_token(unit: &str) -> bool {
    let Ok(subject) = decode_asset_subject(unit) else {
        return false;
    };

    parse_cip67_label_from_asset_name(&subject[28..]) == Some(100)
        || matches!(cip68_reference_from_unit(unit), Ok(Some(_)))
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

fn log_500(error: impl std::fmt::Debug) -> StatusCode {
    tracing::error!(?error, "failed to read an asset");
    StatusCode::INTERNAL_SERVER_ERROR
}

/// Decodes the subject and finds the slot of its first mint. Blockfrost
/// returns 404 for a valid but unknown asset.
pub(crate) async fn resolve_asset<D>(
    domain: &Facade<D>,
    subject: &str,
) -> Result<(Vec<u8>, BlockSlot), Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let subject = decode_asset_subject(subject)?;

    let chain = domain.get_chain_summary()?;
    let (tip, _) = chain.slot_epoch(domain.get_tip_slot()?);

    let slot = domain
        .query()
        .run_blocking({
            let subject = subject.clone();
            move |domain| Ok(assets::first_mint_slot(&domain, &chain, tip, &subject)?)
        })
        .await
        .map_err(log_500)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok((subject, slot))
}

/// The supply of each listed subject, in order, read as many at once as one
/// request may run on the query limiter.
///
/// A subject with no first mint is left out with a warning. A rollback
/// between the list read and the supply read removes a staged first mint,
/// once; a sealed list that predates the archive's asset index names one on
/// every read, and its node needs `dolos data rebuild --rewrite-logs`.
pub(crate) async fn asset_quantities<D>(
    domain: &Facade<D>,
    subjects: Vec<Vec<u8>>,
) -> Result<Vec<(Vec<u8>, i128)>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let chain = domain.get_chain_summary()?;
    let (tip, _) = chain.slot_epoch(domain.get_tip_slot()?);

    let read: Vec<_> = futures_util::stream::iter(subjects)
        .map(|subject| {
            let chain = &chain;

            async move {
                let quantity = assets::asset_quantity(&domain.inner, chain, tip, subject.clone())
                    .await
                    .map_err(log_500)?;

                Ok::<_, Error>((subject, quantity))
            }
        })
        .buffered(domain.inner.query_limiter().fan_out())
        .try_collect()
        .await?;

    let mut out = Vec::with_capacity(read.len());

    for (subject, quantity) in read {
        let Some(quantity) = quantity else {
            tracing::warn!(
                asset = hex::encode(&subject),
                "an asset list names an asset with no first mint: a rollback removed it since \
                 the list was read, or, if this repeats for the asset, a sealed first-mint list \
                 predates the archive's asset index and `dolos data rebuild \
                 --rewrite-logs` indexes it"
            );

            continue;
        };

        out.push((subject, quantity));
    }

    Ok(out)
}

/// Loads the summary of an asset and the minting transaction that holds its
/// metadata. Its label-100 walks, and its reference token's, draw on
/// `budget`.
pub(crate) async fn load_asset_builder<D>(
    domain: &Facade<D>,
    unit: String,
    budget: &WalkBudget,
) -> Result<AssetModelBuilder, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let subject = decode_asset_subject(&unit)?;

    let chain = domain.get_chain_summary()?;
    let (tip, _) = chain.slot_epoch(domain.get_tip_slot()?);

    let summary = assets::asset_summary(&domain.inner, &chain, tip, subject.clone(), budget)
        .await
        .map_err(log_500)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let initial_tx = if let Some(initial_tx) = summary.fold.initial_tx {
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
        summary,
        initial_tx,
        registry_url: domain.config.token_registry_url.clone(),
        chain,
        tip,
        budget: budget.clone(),
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

#[cfg(test)]
mod tests {
    use pallas::{
        codec::utils::{KeyValuePairs, MaybeIndefArray},
        ledger::primitives::Constr,
    };

    use super::*;
    use crate::mapping::direct_json::WriteJson;

    const POLICY: &str = "f0ff48bbb7bbe9d59a40f1ce90e9e9d0ff5002ec48f232b49ca0fb9a";

    /// Property names in their on-chain order, which is not sorted.
    const KEYS: [&str; 8] = [
        "name",
        "description",
        "website",
        "image",
        "mediaType",
        "author",
        "collection",
        "artist",
    ];

    /// Serializes the asset that `metadata` describes as the route does.
    fn asset_bytes(metadata: OnchainMetadata) -> Vec<u8> {
        let (metadata, version, extra) = metadata.into_parts();
        let asset = Asset {
            asset: String::new(),
            policy_id: POLICY.to_string(),
            asset_name: None,
            fingerprint: String::new(),
            quantity: "1".to_string(),
            initial_mint_tx_hash: String::new(),
            mint_or_burn_count: 1,
            onchain_metadata: Some(metadata),
            onchain_metadata_standard: Some(version),
            onchain_metadata_extra: Some(extra),
            metadata: None,
        };

        asset_json(asset).unwrap().to_json_vec()
    }

    /// The asset body has the bytes that `serde_json` encodes for the sorted
    /// asset.
    #[test]
    fn asset_body_matches_serde() {
        let unit = format!("{POLICY}{}", hex::encode("james"));
        let text = |x: &str| Metadatum::Text(x.to_string());

        let asset = vec![(text("james"), crate::test_support::mixed_metadatum())];
        let policy = vec![(text(POLICY), Metadatum::Map(asset.into()))];
        let (metadata, version, _) =
            OnchainMetadata::from_metadatum(&unit, Metadatum::Map(policy.into()))
                .unwrap()
                .unwrap()
                .into_parts();

        let asset = Asset {
            policy_id: POLICY.to_string(),
            onchain_metadata: Some(metadata),
            onchain_metadata_standard: Some(version),
            ..Default::default()
        };

        assert_eq!(
            asset_json(asset.clone()).unwrap().to_json_vec(),
            serde_json::to_vec(&sorted_json(asset).unwrap().0).unwrap()
        );
    }

    fn assert_identical_and_sorted(mut next: impl FnMut() -> Vec<u8>) {
        let expected = next();

        // each pass builds a new `HashMap`, so each has its own seed
        for _ in 0..64 {
            assert_eq!(next(), expected);
        }

        let body: serde_json::Value = serde_json::from_slice(&expected).unwrap();
        let keys: Vec<_> = body["onchain_metadata"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        let body = std::str::from_utf8(&expected).unwrap();
        let positions: Vec<_> = keys
            .iter()
            .map(|key| body.find(&format!("\"{key}\":")).unwrap())
            .collect();

        assert_eq!(keys.len(), KEYS.len());
        // the parsed keys come sorted: the body must list them in that order
        assert!(positions.windows(2).all(|w| w[0] < w[1]), "{body}");
    }

    /// The CIP-25 JSON of a metadatum with every variant. CIP-25 drops keys
    /// that are not text or bytes, and writes bytes as UTF-8 or bare hex.
    #[test]
    fn cip25_metadata_json_keeps_its_output() {
        let json = cip25_metadata_json(&crate::test_support::mixed_metadatum());

        assert_eq!(
            serde_json::to_string(&json).unwrap(),
            r#"{"-18446744073709551616":"huge key","a":[1,"x","ok"],"b":"second","ff00":"ff00","name":"-18446744073709551616"}"#
        );
    }

    /// The CIP-25 label of one policy and one asset: `{policy: {asset:
    /// entry}}`, and an optional top-level `version`.
    fn cip25_label(asset: Metadatum, entry: Metadatum, version: Option<Metadatum>) -> Metadatum {
        let text = |x: &str| Metadatum::Text(x.to_string());

        let mut label = vec![(
            text(POLICY),
            Metadatum::Map(KeyValuePairs::Def(vec![(asset, entry)])),
        )];
        label.extend(version.map(|version| (text("version"), version)));

        Metadatum::Map(KeyValuePairs::Def(label))
    }

    /// The properties and standard that `from_metadatum` finds for `unit`.
    fn cip25_parts(
        unit: &str,
        label: Metadatum,
    ) -> (serde_json::Value, Option<OnchainMetadataStandard>) {
        let (metadata, version, _) = OnchainMetadata::from_metadatum(unit, label)
            .unwrap()
            .unwrap()
            .into_parts();

        (
            serde_json::Value::Object(metadata.into_iter().collect()),
            version,
        )
    }

    /// The `version` field selects CIP-25 v2 as an integer or as a string.
    /// Only v2 allows an array of strings for `image`.
    #[test]
    fn cip25_metadata_reads_its_version() {
        let text = |x: &str| Metadatum::Text(x.to_string());
        let unit = format!("{POLICY}{}", hex::encode("james"));
        let properties = || {
            Metadatum::Map(KeyValuePairs::Def(vec![
                (text("name"), text("James")),
                (
                    text("image"),
                    Metadatum::Array(vec![text("ipfs://"), text("a")]),
                ),
            ]))
        };
        let expected = serde_json::json!({"image": ["ipfs://", "a"], "name": "James"});

        for (version, standard) in [
            (
                Some(Metadatum::Int(2.into())),
                Some(OnchainMetadataStandard::Cip25v2),
            ),
            (Some(text("2")), Some(OnchainMetadataStandard::Cip25v2)),
            (Some(Metadatum::Int(1.into())), None),
            (None, None),
        ] {
            let label = cip25_label(text("james"), properties(), version);
            assert_eq!(cip25_parts(&unit, label), (expected.clone(), standard));
        }
    }

    /// An asset name that is not UTF-8 matches its key as hex without a
    /// prefix. A policy entry that is not an object gives no properties.
    #[test]
    fn cip25_metadata_finds_its_asset_entry() {
        let text = |x: &str| Metadatum::Text(x.to_string());
        let name = [0xff, 0x00];
        let unit = format!("{POLICY}{}", hex::encode(name));
        let properties = Metadatum::Map(KeyValuePairs::Def(vec![
            (text("name"), text("Raw")),
            (text("image"), text("ipfs://raw")),
        ]));

        let label = cip25_label(Metadatum::Bytes(name.to_vec().into()), properties, None);
        assert_eq!(
            cip25_parts(&unit, label),
            (
                serde_json::json!({"image": "ipfs://raw", "name": "Raw"}),
                Some(OnchainMetadataStandard::Cip25v1)
            )
        );

        let label = cip25_label(
            Metadatum::Bytes(name.to_vec().into()),
            text("not a map"),
            None,
        );
        assert_eq!(cip25_parts(&unit, label), (serde_json::json!({}), None));
    }

    /// The CIP-25 converter frees what it does not keep: an entry whose key is
    /// not a JSON string, and the value that a repeated key replaces. Each
    /// holds many nesting levels here.
    #[test]
    fn cip25_metadata_frees_the_parts_it_drops() {
        use crate::test_support::nested_metadatum;

        let text = |x: &str| Metadatum::Text(x.to_string());
        let metadatum = Metadatum::Map(KeyValuePairs::Def(vec![
            (nested_metadatum(100_000), text("structured key")),
            (Metadatum::Int(0.into()), nested_metadatum(100_000)),
            (text("a"), nested_metadatum(100_000)),
            (text("a"), text("last")),
        ]));

        let json = cip25_metadata_json(&metadatum);
        assert_eq!(json, serde_json::json!({"a": "last"}));

        // pallas frees the metadatum itself, which is outside this test
        std::mem::forget(metadatum);
    }

    /// On-chain metadata that does not reach a response frees its properties
    /// in full.
    #[test]
    fn onchain_metadata_frees_nested_properties() {
        use crate::test_support::{nested_value, VALUE_LEVELS};

        let metadata = OnchainMetadata {
            version: None,
            metadata: HashMap::from([("nested".to_string(), nested_value(VALUE_LEVELS))]),
            extra: None,
            cip68: None,
        };

        assert!(metadata.is_present());
    }

    #[test]
    fn cip25_metadata_serializes_to_the_same_sorted_bytes() {
        let text = |x: &str| Metadatum::Text(x.to_string());
        let unit = format!("{POLICY}{}", hex::encode("james"));

        let properties = KEYS.map(|key| (text(key), text(&format!("{key} value"))));
        let asset = KeyValuePairs::Def(vec![(
            text("james"),
            Metadatum::Map(properties.to_vec().into()),
        )]);
        let policy = KeyValuePairs::Def(vec![(text(POLICY), Metadatum::Map(asset))]);
        let metadatum = Metadatum::Map(policy);

        assert_identical_and_sorted(|| {
            let metadata = OnchainMetadata::from_metadatum(&unit, metadatum.clone())
                .unwrap()
                .unwrap();
            asset_bytes(metadata)
        });
    }

    #[test]
    fn cip68_metadata_serializes_to_the_same_sorted_bytes() {
        let text = |x: &str| PlutusData::BoundedBytes(x.as_bytes().to_vec().into());

        let properties = KEYS.map(|key| (text(key), text(&format!("{key} value"))));
        let datum = PlutusData::Constr(Constr {
            tag: 121,
            any_constructor: None,
            fields: MaybeIndefArray::Def(vec![
                PlutusData::Map(KeyValuePairs::Def(properties.into())),
                PlutusData::BigInt(BigInt::Int(1.into())),
            ]),
        });

        assert_identical_and_sorted(|| {
            let metadata =
                OnchainMetadata::from_plutus_data(datum.clone(), Cip68TokenStandard::Nft)
                    .unwrap()
                    .unwrap();
            asset_bytes(metadata)
        });
    }
}
