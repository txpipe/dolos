use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use dolos_cardano::{indexes::CardanoStateIndexExt, network_from_genesis, pallas_extras};
use dolos_core::async_query::BlockMetaResolver;
use dolos_core::{
    ArchiveStore as _, AsyncQueryFacade, Domain, EraCbor, StateStore as _, TxoRef, UtxoSet,
};
use pallas::codec::minicbor;
use pallas::crypto::hash::Hash;
use pallas::ledger::{
    addresses::{Address, StakeAddress},
    primitives::{conway::DatumOption, conway::ScriptRef, StakeCredential},
    traverse::{ComputeHash, Era, MultiEraOutput, MultiEraTx, MultiEraValue, OriginalHash},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

use crate::{bad_request, patterns, Facade};

/// UTxO refs read, decoded and filtered per blocking task. A heavy match
/// releases its permit between chunks, and a dropped one stops at the next
/// chunk.
const MATCH_CHUNK: usize = 1024;

pub async fn by_pattern<D: Domain>(
    State(facade): State<Facade<D>>,
    Path(pattern): Path<String>,
    Query(query): Query<MatchesQuery>,
) -> Response {
    let filters = match MatchesFilters::try_from_query(query, &facade).await {
        Ok(filters) => filters,
        Err(err) => return err.into_response(),
    };

    let parsed = match patterns::Pattern::parse(&pattern) {
        Ok(parsed) => parsed,
        Err(err) => return bad_request(err.to_string()),
    };

    let query = facade.query();
    let refs = match parsed {
        patterns::Pattern::Address(pattern) => {
            blocking(&query, move |domain| {
                refs_for_address_pattern(&domain, &pattern)
            })
            .await
        }
        patterns::Pattern::Asset(pattern) => {
            blocking(&query, move |domain| {
                refs_for_asset_pattern(&domain, &pattern)
            })
            .await
        }
        patterns::Pattern::OutputRef(pattern) => {
            refs_for_output_ref_pattern(&facade, &pattern).await
        }
        patterns::Pattern::Any => return bad_request("wildcard patterns are not supported"),
    };

    let (refs, filter) = match refs {
        Ok(result) => result,
        Err(err) => return err.into_response(),
    };

    let matches = match build_matches(&facade, refs, filter, filters.resolve_hashes).await {
        Ok(matches) => matches,
        Err(err) => return err.into_response(),
    };

    let mut matches = apply_filters(matches, &filters);
    sort_matches(&mut matches, filters.order);

    Json(matches).into_response()
}

#[derive(Debug)]
enum MatchError {
    BadRequest(String),
    Internal,
}

impl MatchError {
    fn into_response(self) -> Response {
        match self {
            MatchError::BadRequest(hint) => bad_request(hint),
            MatchError::Internal => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        }
    }
}

/// Run `f` on the blocking pool under the domain's query limiter, so a store
/// read never occupies an async worker.
async fn blocking<D, T, F>(query: &AsyncQueryFacade<D>, f: F) -> Result<T, MatchError>
where
    D: Domain,
    T: Send + 'static,
    F: FnOnce(D) -> Result<T, MatchError> + Send + 'static,
{
    query
        .run_blocking(move |domain| Ok(f(domain)))
        .await
        .map_err(|_| MatchError::Internal)?
}

#[derive(Serialize)]
struct MatchResponse {
    transaction_index: u32,
    transaction_id: String,
    output_index: u32,
    address: String,
    value: ValueResponse,
    datum_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    datum: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    datum_type: Option<String>,
    script_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    script: Option<serde_json::Value>,
    created_at: PointResponse,
    spent_at: Option<serde_json::Value>,
}

#[derive(Debug, PartialEq, Serialize)]
struct ValueResponse {
    coins: u64,
    assets: HashMap<String, u64>,
}

#[derive(Serialize)]
struct PointResponse {
    slot_no: u64,
    header_hash: String,
}

#[derive(Default, Debug, Deserialize)]
pub(crate) struct MatchesQuery {
    resolve_hashes: Option<String>,
    spent: Option<String>,
    unspent: Option<String>,
    order: Option<String>,
    created_after: Option<String>,
    created_before: Option<String>,
    spent_after: Option<String>,
    spent_before: Option<String>,
    policy_id: Option<String>,
    asset_name: Option<String>,
    transaction_id: Option<String>,
    output_index: Option<String>,
}

#[derive(Clone, Copy, Debug, Default)]
enum MatchOrder {
    #[default]
    MostRecentFirst,
    OldestFirst,
}

#[derive(Debug, Default)]
struct MatchesFilters {
    order: MatchOrder,
    created_after: Option<u64>,
    created_before: Option<u64>,
    policy_id: Option<String>,
    asset_name: Option<String>,
    transaction_id: Option<String>,
    output_index: Option<u32>,
    resolve_hashes: bool,
}

impl MatchesFilters {
    async fn try_from_query<D: Domain>(
        query: MatchesQuery,
        facade: &Facade<D>,
    ) -> Result<Self, MatchError> {
        let resolve_hashes = query.resolve_hashes.is_some();
        let _ = query.unspent;
        if query.spent.is_some() || query.spent_after.is_some() || query.spent_before.is_some() {
            return Err(MatchError::BadRequest(spent_hint()));
        }

        if query.asset_name.is_some() && query.policy_id.is_none() {
            return Err(MatchError::BadRequest(policy_asset_hint()));
        }

        if query.output_index.is_some() && query.transaction_id.is_none() {
            return Err(MatchError::BadRequest(tx_output_hint()));
        }

        let order = match query.order.as_deref() {
            None | Some("most_recent_first") => MatchOrder::MostRecentFirst,
            Some("oldest_first") => MatchOrder::OldestFirst,
            Some(_) => return Err(MatchError::BadRequest(order_hint())),
        };

        let created_after = match query.created_after.as_deref() {
            Some(value) => Some(parse_slot_or_point(value, facade).await?),
            None => None,
        };

        let created_before = match query.created_before.as_deref() {
            Some(value) => Some(parse_slot_or_point(value, facade).await?),
            None => None,
        };

        let policy_id = match query.policy_id.as_deref() {
            Some(value) => Some(parse_policy_id(value)?),
            None => None,
        };

        let asset_name = match query.asset_name.as_deref() {
            Some(value) => Some(parse_asset_name(value)?),
            None => None,
        };

        let transaction_id = match query.transaction_id.as_deref() {
            Some(value) => Some(parse_transaction_id(value)?),
            None => None,
        };

        let output_index = match query.output_index.as_deref() {
            Some(value) => Some(
                value
                    .parse::<u32>()
                    .map_err(|_| MatchError::BadRequest(tx_output_hint()))?,
            ),
            None => None,
        };

        Ok(Self {
            order,
            created_after,
            created_before,
            policy_id,
            asset_name,
            transaction_id,
            output_index,
            resolve_hashes,
        })
    }
}

#[derive(Clone)]
struct AssetFilter {
    policy: Vec<u8>,
    name: Option<Vec<u8>>,
}

enum OutputFilter {
    None,
    Address(patterns::AddressPattern),
    Asset(AssetFilter),
}

fn refs_for_address_pattern<D: Domain>(
    domain: &D,
    pattern: &patterns::AddressPattern,
) -> Result<(UtxoSet, OutputFilter), MatchError> {
    match pattern {
        patterns::AddressPattern::Full(bytes) => {
            let address = Address::from_bytes(bytes)
                .map_err(|_| MatchError::BadRequest("invalid address".to_string()))?;

            match address {
                Address::Stake(stake) => {
                    let stake_bytes = stake.to_vec();
                    let refs = domain
                        .state()
                        .utxos_by_stake(&stake_bytes)
                        .map_err(|_| MatchError::Internal)?;
                    let delegation = stake_credential_pattern(&stake);
                    let filter = patterns::AddressPattern::Credentials {
                        payment: patterns::CredentialPattern::Any,
                        delegation,
                    };
                    Ok((refs, OutputFilter::Address(filter)))
                }
                _ => {
                    let refs = domain
                        .state()
                        .utxos_by_address(bytes)
                        .map_err(|_| MatchError::Internal)?;
                    Ok((refs, OutputFilter::None))
                }
            }
        }
        patterns::AddressPattern::Credentials {
            payment,
            delegation,
        } => {
            let payment_key = credential_hash(payment);
            let stake_keys = stake_keys_for_credential(domain, delegation)?;

            if payment_key.is_none() && stake_keys.is_none() {
                return Err(MatchError::BadRequest(
                    "wildcard credential patterns are not supported yet".to_string(),
                ));
            }

            let mut refs = payment_key
                .as_ref()
                .map(|key| domain.state().utxos_by_payment(key))
                .transpose()
                .map_err(|_| MatchError::Internal)?
                .unwrap_or_default();

            if let Some(stake_keys) = stake_keys {
                let mut stake_refs = UtxoSet::new();
                for key in stake_keys {
                    let next = domain
                        .state()
                        .utxos_by_stake(&key)
                        .map_err(|_| MatchError::Internal)?;
                    stake_refs.extend(next);
                }

                if payment_key.is_some() {
                    refs = refs.intersection(&stake_refs).cloned().collect::<UtxoSet>();
                } else {
                    refs = stake_refs;
                }
            }

            Ok((refs, OutputFilter::Address(pattern.clone())))
        }
    }
}

fn refs_for_asset_pattern<D: Domain>(
    domain: &D,
    pattern: &patterns::AssetPattern,
) -> Result<(UtxoSet, OutputFilter), MatchError> {
    let filter = match pattern.name() {
        patterns::AssetNamePattern::Any => OutputFilter::Asset(AssetFilter {
            policy: pattern.policy().to_vec(),
            name: None,
        }),
        patterns::AssetNamePattern::Exact(name) => OutputFilter::Asset(AssetFilter {
            policy: pattern.policy().to_vec(),
            name: Some(name.clone()),
        }),
    };

    let refs = match pattern.name() {
        patterns::AssetNamePattern::Any => domain
            .state()
            .utxos_by_policy(pattern.policy())
            .map_err(|_| MatchError::Internal)?,
        patterns::AssetNamePattern::Exact(name) => {
            let mut subject = pattern.policy().to_vec();
            subject.extend_from_slice(name);
            domain
                .state()
                .utxos_by_asset(&subject)
                .map_err(|_| MatchError::Internal)?
        }
    };

    Ok((refs, filter))
}

async fn refs_for_output_ref_pattern<D: Domain>(
    facade: &Facade<D>,
    pattern: &patterns::OutputRefPattern,
) -> Result<(UtxoSet, OutputFilter), MatchError> {
    let tx_id = pattern.tx_id();
    let tx_hash = pallas::crypto::hash::Hash::<32>::from(tx_id);

    let refs = match pattern.index() {
        patterns::OutputIndexPattern::Exact(index) => {
            let mut refs = UtxoSet::new();
            refs.insert(TxoRef(tx_hash, *index));
            refs
        }
        patterns::OutputIndexPattern::Any => {
            let Some(EraCbor(era, cbor)) = facade
                .query()
                .tx_cbor(tx_id.to_vec())
                .await
                .map_err(|_| MatchError::Internal)?
            else {
                return Ok((UtxoSet::new(), OutputFilter::None));
            };

            let era = Era::try_from(era).map_err(|_| MatchError::Internal)?;
            let tx = MultiEraTx::decode_for_era(era, &cbor).map_err(|_| MatchError::Internal)?;
            let mut refs = UtxoSet::new();
            for (index, _) in tx.outputs().iter().enumerate() {
                refs.insert(TxoRef(tx_hash, index as u32));
            }
            refs
        }
    };

    Ok((refs, OutputFilter::None))
}

fn credential_hash(pattern: &patterns::CredentialPattern) -> Option<Vec<u8>> {
    match pattern {
        patterns::CredentialPattern::Any => None,
        patterns::CredentialPattern::KeyHash(bytes) => Some(bytes.clone()),
        patterns::CredentialPattern::ScriptHash(bytes) => Some(bytes.clone()),
        patterns::CredentialPattern::AnyHash(bytes) => Some(bytes.clone()),
    }
}

fn stake_credential_pattern(stake: &StakeAddress) -> patterns::CredentialPattern {
    match pallas_extras::stake_address_to_cred(stake) {
        StakeCredential::AddrKeyhash(hash) => patterns::CredentialPattern::KeyHash(hash.to_vec()),
        StakeCredential::ScriptHash(hash) => patterns::CredentialPattern::ScriptHash(hash.to_vec()),
    }
}

fn stake_keys_for_credential<D: Domain>(
    domain: &D,
    credential: &patterns::CredentialPattern,
) -> Result<Option<Vec<Vec<u8>>>, MatchError> {
    let network = network_from_genesis(&domain.genesis());

    let build_key = |cred: StakeCredential| -> Vec<u8> {
        pallas_extras::stake_credential_to_address(network, &cred).to_vec()
    };

    let keys = match credential {
        patterns::CredentialPattern::Any => return Ok(None),
        patterns::CredentialPattern::KeyHash(bytes) => vec![build_key(
            StakeCredential::AddrKeyhash(bytes.as_slice().into()),
        )],
        patterns::CredentialPattern::ScriptHash(bytes) => vec![build_key(
            StakeCredential::ScriptHash(bytes.as_slice().into()),
        )],
        patterns::CredentialPattern::AnyHash(bytes) => vec![
            build_key(StakeCredential::AddrKeyhash(bytes.as_slice().into())),
            build_key(StakeCredential::ScriptHash(bytes.as_slice().into())),
        ],
    };

    Ok(Some(keys))
}

/// An output that passed the pattern's filter, owned so it can leave the
/// blocking task that decoded it.
#[derive(Debug, PartialEq)]
struct Candidate {
    txo_ref: TxoRef,
    address: String,
    value: ValueResponse,
    datum_hash: Option<String>,
    datum_type: Option<String>,
    /// Present only when the match resolves hashes.
    datum: Option<DatumSource>,
    script_hash: Option<Hash<28>>,
}

#[derive(Debug, PartialEq)]
enum DatumSource {
    Absent,
    /// The inline datum's CBOR, hex-encoded.
    Inline(String),
    Hash(Hash<32>),
}

async fn build_matches<D: Domain>(
    facade: &Facade<D>,
    refs: UtxoSet,
    filter: OutputFilter,
    resolve_hashes: bool,
) -> Result<Vec<MatchResponse>, MatchError> {
    let candidates =
        select_candidates(&facade.query(), refs, filter, resolve_hashes, MATCH_CHUNK).await?;

    let mut block_meta = BlockMetaResolver::new(facade.query());
    let block_deps = block_meta
        .resolve_batch(candidates.iter().map(|candidate| candidate.txo_ref.0))
        .await
        .map_err(|_| MatchError::Internal)?;
    let mut out = Vec::new();

    for candidate in candidates {
        let tx_hash = candidate.txo_ref.0;
        let block_info = block_deps.get(&tx_hash).ok_or(MatchError::Internal)?;

        let (datum, script) =
            resolve_output_extras(facade, candidate.datum, candidate.script_hash).await?;

        out.push(MatchResponse {
            transaction_index: block_info
                .tx_index
                .try_into()
                .map_err(|_| MatchError::Internal)?,
            transaction_id: tx_hash.to_string(),
            output_index: candidate.txo_ref.1,
            address: candidate.address,
            value: candidate.value,
            datum_hash: candidate.datum_hash,
            datum,
            datum_type: candidate.datum_type,
            script_hash: candidate.script_hash.map(|hash| hash.to_string()),
            script,
            created_at: PointResponse {
                slot_no: block_info.slot,
                header_hash: block_info.hash.to_string(),
            },
            spent_at: None,
        });
    }

    Ok(out)
}

/// Read, decode and filter the outputs behind `refs`, `chunk` refs per
/// blocking task.
async fn select_candidates<D: Domain>(
    query: &AsyncQueryFacade<D>,
    refs: UtxoSet,
    filter: OutputFilter,
    resolve_hashes: bool,
    chunk: usize,
) -> Result<Vec<Candidate>, MatchError> {
    let mut refs: Vec<_> = refs.into_iter().collect();
    refs.sort();

    let filter = Arc::new(filter);
    let mut candidates = Vec::new();

    for refs in refs.chunks(chunk.max(1)) {
        let refs = refs.to_vec();
        let filter = filter.clone();
        let selected = blocking(query, move |domain| {
            select_chunk(&domain, refs, &filter, resolve_hashes)
        })
        .await?;
        candidates.extend(selected);
    }

    Ok(candidates)
}

fn select_chunk<D: Domain>(
    domain: &D,
    refs: Vec<TxoRef>,
    filter: &OutputFilter,
    resolve_hashes: bool,
) -> Result<Vec<Candidate>, MatchError> {
    let utxos = domain
        .state()
        .get_utxos(refs)
        .map_err(|_| MatchError::Internal)?;

    let mut selected = Vec::new();
    for (txo_ref, cbor) in utxos {
        let cbor: &dolos_core::EraCbor = cbor.as_ref();
        let output = MultiEraOutput::try_from(cbor).map_err(|_| MatchError::Internal)?;
        let address = output.address().map_err(|_| MatchError::Internal)?;

        if !matches_output_filter(&output, &address, filter) {
            continue;
        }

        let (datum_hash, datum_type) = output_datum_info(&output);
        let datum = if resolve_hashes {
            Some(output_datum_source(&output)?)
        } else {
            None
        };

        selected.push(Candidate {
            txo_ref,
            address: address.to_string(),
            value: map_value(output.value()),
            datum_hash,
            datum_type,
            datum,
            script_hash: output_script_hash(&output),
        });
    }

    Ok(selected)
}

fn output_has_asset(output: &MultiEraOutput<'_>, filter: &AssetFilter) -> bool {
    for policy_assets in output.value().assets() {
        if policy_assets.policy().as_slice() != filter.policy.as_slice() {
            continue;
        }

        let Some(name) = filter.name.as_ref() else {
            return true;
        };

        for asset in policy_assets.assets() {
            if asset.name() == name.as_slice() {
                return true;
            }
        }
    }

    false
}

fn matches_output_filter(
    output: &MultiEraOutput<'_>,
    address: &Address,
    filter: &OutputFilter,
) -> bool {
    match filter {
        OutputFilter::None => true,
        OutputFilter::Address(filter_pattern) => {
            let pattern = patterns::Pattern::Address(filter_pattern.clone());
            pattern.matches_address(address)
        }
        OutputFilter::Asset(asset_filter) => output_has_asset(output, asset_filter),
    }
}

fn output_datum_info(output: &MultiEraOutput<'_>) -> (Option<String>, Option<String>) {
    match output.datum() {
        None => (None, None),
        Some(DatumOption::Hash(hash)) => (Some(hash.to_string()), Some("hash".to_string())),
        Some(DatumOption::Data(data)) => (
            Some(data.original_hash().to_string()),
            Some("inline".to_string()),
        ),
    }
}

fn output_script_hash(output: &MultiEraOutput<'_>) -> Option<pallas::crypto::hash::Hash<28>> {
    output.script_ref().map(|script| match script {
        ScriptRef::NativeScript(x) => x.original_hash(),
        ScriptRef::PlutusV1Script(x) => x.compute_hash(),
        ScriptRef::PlutusV2Script(x) => x.compute_hash(),
        ScriptRef::PlutusV3Script(x) => x.compute_hash(),
    })
}

fn output_datum_source(output: &MultiEraOutput<'_>) -> Result<DatumSource, MatchError> {
    match output.datum() {
        None => Ok(DatumSource::Absent),
        Some(DatumOption::Data(data)) => minicbor::to_vec(&data.0)
            .map(hex::encode)
            .map(DatumSource::Inline)
            .map_err(|_| MatchError::Internal),
        Some(DatumOption::Hash(hash)) => Ok(DatumSource::Hash(hash)),
    }
}

async fn resolve_output_extras<D: Domain>(
    facade: &Facade<D>,
    datum: Option<DatumSource>,
    script_hash: Option<Hash<28>>,
) -> Result<(Option<serde_json::Value>, Option<serde_json::Value>), MatchError> {
    let Some(datum) = datum else {
        return Ok((None, None));
    };

    let datum = match datum {
        DatumSource::Absent => serde_json::Value::Null,
        DatumSource::Inline(cbor) => serde_json::Value::String(cbor),
        DatumSource::Hash(hash) => facade
            .resolve_datum(&hash)
            .await
            .map_err(|_| MatchError::Internal)?
            .map(|datum| serde_json::Value::String(datum.datum))
            .unwrap_or(serde_json::Value::Null),
    };

    let script = match script_hash {
        None => serde_json::Value::Null,
        Some(script_hash) => facade
            .resolve_script(&script_hash)
            .await
            .map_err(|_| MatchError::Internal)?
            .map(serde_json::to_value)
            .transpose()
            .map_err(|_| MatchError::Internal)?
            .unwrap_or(serde_json::Value::Null),
    };

    Ok((Some(datum), Some(script)))
}

fn map_value(value: MultiEraValue<'_>) -> ValueResponse {
    let mut assets: HashMap<String, u64> = HashMap::new();
    for policy in value.assets() {
        let policy_hex = hex::encode(policy.policy().as_slice());
        for asset in policy.assets() {
            let name_hex = hex::encode(asset.name());
            let unit = if name_hex.is_empty() {
                policy_hex.clone()
            } else {
                format!("{policy_hex}.{name_hex}")
            };
            let amount = asset.output_coin().unwrap_or_default();
            assets.insert(unit, amount);
        }
    }

    ValueResponse {
        coins: value.coin(),
        assets,
    }
}

fn apply_filters(mut matches: Vec<MatchResponse>, filters: &MatchesFilters) -> Vec<MatchResponse> {
    matches.retain(|item| {
        if let Some(min_slot) = filters.created_after {
            if item.created_at.slot_no < min_slot {
                return false;
            }
        }

        if let Some(max_slot) = filters.created_before {
            if item.created_at.slot_no > max_slot {
                return false;
            }
        }

        if let Some(tx_id) = filters.transaction_id.as_ref() {
            if &item.transaction_id != tx_id {
                return false;
            }
        }

        if let Some(output_index) = filters.output_index {
            if item.output_index != output_index {
                return false;
            }
        }

        if let Some(policy_id) = filters.policy_id.as_ref() {
            let policy_prefix = format!("{policy_id}.");
            let has_policy = item
                .value
                .assets
                .keys()
                .any(|key| key == policy_id || key.starts_with(&policy_prefix));
            if !has_policy {
                return false;
            }

            if let Some(asset_name) = filters.asset_name.as_ref() {
                let key = format!("{policy_id}.{asset_name}");
                if !item.value.assets.contains_key(&key) {
                    return false;
                }
            }
        }

        true
    });

    matches
}

fn sort_matches(matches: &mut [MatchResponse], order: MatchOrder) {
    match order {
        MatchOrder::MostRecentFirst => matches.sort_by_key(|item| {
            (
                std::cmp::Reverse(item.created_at.slot_no),
                std::cmp::Reverse(item.transaction_index),
                std::cmp::Reverse(item.output_index),
            )
        }),
        MatchOrder::OldestFirst => matches.sort_by_key(|item| {
            (
                item.created_at.slot_no,
                item.transaction_index,
                item.output_index,
            )
        }),
    }
}

async fn parse_slot_or_point<D: Domain>(
    value: &str,
    facade: &Facade<D>,
) -> Result<u64, MatchError> {
    if let Some((slot, hash)) = value.split_once('.') {
        let slot = slot
            .parse::<u64>()
            .map_err(|_| MatchError::BadRequest(slot_range_hint()))?;

        let bytes = hex::decode(hash).map_err(|_| MatchError::BadRequest(slot_range_hint()))?;
        if bytes.len() != 32 {
            return Err(MatchError::BadRequest(slot_range_hint()));
        }
        let found_slot = blocking(&facade.query(), move |domain| {
            domain
                .archive()
                .slot_by_block_hash(&bytes)
                .map_err(|_| MatchError::Internal)
        })
        .await?
        .ok_or_else(|| MatchError::BadRequest(slot_range_hint()))?;

        if found_slot != slot {
            return Err(MatchError::BadRequest(slot_range_hint()));
        }

        Ok(slot)
    } else {
        let slot = value
            .parse::<u64>()
            .map_err(|_| MatchError::BadRequest(slot_range_hint()))?;
        if slot < 1 {
            return Err(MatchError::BadRequest(slot_range_hint()));
        }
        Ok(slot)
    }
}

fn parse_policy_id(value: &str) -> Result<String, MatchError> {
    if value.len() != 56 {
        return Err(MatchError::BadRequest(policy_asset_hint()));
    }
    hex::decode(value)
        .map_err(|_| MatchError::BadRequest(policy_asset_hint()))
        .map(|_| value.to_lowercase())
}

fn parse_asset_name(value: &str) -> Result<String, MatchError> {
    let len = value.len();
    if !(2..=64).contains(&len) || !len.is_multiple_of(2) {
        return Err(MatchError::BadRequest(policy_asset_hint()));
    }
    hex::decode(value)
        .map_err(|_| MatchError::BadRequest(policy_asset_hint()))
        .map(|_| value.to_lowercase())
}

fn parse_transaction_id(value: &str) -> Result<String, MatchError> {
    if value.len() != 64 {
        return Err(MatchError::BadRequest(tx_output_hint()));
    }
    hex::decode(value)
        .map_err(|_| MatchError::BadRequest(tx_output_hint()))
        .map(|_| value.to_lowercase())
}

fn policy_asset_hint() -> String {
    "Invalid or incomplete filter query parameters! 'policy_id' and 'asset_name' query values must be encoded in base16. Be aware that you MUST specify a 'policy_id' if you specify an 'asset_name'. In case of doubts, check the documentation at: <https://cardanosolutions.github.io/kupo>!".to_string()
}

fn tx_output_hint() -> String {
    "Invalid or incomplete filter query parameters! 'transaction_id' query value must be encoded in base16 and 'output_index' must be an integer. Be aware that you MUST specify a 'transaction_id' if you specify an 'output_index'. In case of doubts, check the documentation at: <https://cardanosolutions.github.io/kupo>!".to_string()
}

fn order_hint() -> String {
    "Invalid sort direction provided as query parameter. You can specify either 'order=most_recent_first' or 'order=oldest_first'. Please refer to the API reference for details <https://cardanosolutions.github.io/kupo#operation/getAllMatches>.".to_string()
}

fn slot_range_hint() -> String {
    "Unprocessable slot range! Slot ranges can be specified in the form of lower and upper bound, in absolute slots. Either bound is optional and you can only provide each bound once. Anything else is an error. In case of doubts, check the documentation at: <https://cardanosolutions.github.io/kupo#operation/getAllMatches>!".to_string()
}

fn spent_hint() -> String {
    "Invalid or unsupported filter query parameters! 'spent', 'spent_after' and 'spent_before' are not supported. Only unspent results are available. In case of doubts, check the documentation at: <https://cardanosolutions.github.io/kupo>!".to_string()
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashMap,
        sync::Arc,
        time::{Duration, Instant},
    };

    use axum::http::StatusCode;
    use dolos_core::AsyncQueryFacade;
    use serde::Deserialize;
    use serde_json::{json, Value};

    use super::{refs_for_asset_pattern, select_candidates, Candidate};
    use crate::{
        patterns,
        test_support::{TestApp, TestFault},
    };

    #[derive(Debug, Deserialize)]
    struct MatchResponseTest {
        transaction_index: u32,
        transaction_id: String,
        output_index: u32,
        address: String,
        value: ValueResponseTest,
        datum_hash: Option<String>,
        datum_type: Option<String>,
        script_hash: Option<String>,
        created_at: PointResponseTest,
    }

    #[derive(Debug, Deserialize)]
    struct ValueResponseTest {
        coins: u64,
        assets: HashMap<String, u64>,
    }

    #[derive(Debug, Deserialize)]
    struct PointResponseTest {
        slot_no: u64,
        header_hash: String,
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
    async fn matches_address_happy_path() {
        let app = TestApp::new();
        let path = format!("/matches/{}", app.vectors().address);
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let items: Vec<MatchResponseTest> =
            serde_json::from_slice(&bytes).expect("failed to parse matches response");
        assert_eq!(items.len(), app.vectors().blocks.len());
        assert!(items
            .iter()
            .all(|item| item.address == app.vectors().address));
        assert!(items.iter().all(|item| item.transaction_index == 0));
        assert!(items.iter().all(|item| item.value.coins > 0));
        assert!(items
            .iter()
            .all(|item| item.datum_hash.as_deref() == Some(app.vectors().datum_hash.as_str())));
        assert!(items
            .iter()
            .all(|item| item.datum_type.as_deref() == Some("hash")));
        assert!(items
            .iter()
            .all(|item| item.script_hash.as_deref() == Some(app.vectors().script_hash.as_str())));

        let actual_slots: Vec<_> = items.iter().map(|item| item.created_at.slot_no).collect();
        let expected_slots: Vec<_> = app
            .vectors()
            .blocks
            .iter()
            .rev()
            .map(|block| block.slot)
            .collect();
        assert_eq!(actual_slots, expected_slots);
    }

    #[tokio::test]
    async fn matches_asset_pattern_happy_path() {
        let app = TestApp::new();
        let asset_pattern = format!(
            "{}.{}",
            app.vectors().policy_id,
            app.vectors().asset_name_hex
        );
        let asset_key = asset_pattern.clone();
        let path = format!("/matches/{asset_pattern}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let items: Vec<MatchResponseTest> =
            serde_json::from_slice(&bytes).expect("failed to parse asset matches");
        assert!(!items.is_empty());
        assert!(items
            .iter()
            .all(|item| item.value.assets.contains_key(&asset_key)));
    }

    #[tokio::test]
    async fn matches_output_ref_happy_path() {
        let app = TestApp::new();
        let path = format!("/matches/0@{}", app.vectors().tx_hash);
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let items: Vec<MatchResponseTest> =
            serde_json::from_slice(&bytes).expect("failed to parse output ref matches");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].transaction_id, app.vectors().tx_hash);
        assert_eq!(items[0].output_index, 0);
    }

    #[tokio::test]
    async fn matches_resolve_hashes_happy_path() {
        let app = TestApp::new();
        let path = format!("/matches/{}?resolve_hashes", app.vectors().address);
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let items: Vec<Value> =
            serde_json::from_slice(&bytes).expect("failed to parse resolved matches");
        assert_eq!(items.len(), app.vectors().blocks.len());

        for item in items {
            assert_eq!(
                item.get("datum_hash"),
                Some(&json!(app.vectors().datum_hash))
            );
            assert_eq!(item.get("datum_type"), Some(&json!("hash")));
            assert_eq!(
                item.get("datum"),
                Some(&json!(app.vectors().datum_cbor_hex))
            );
            assert_eq!(
                item.get("script_hash"),
                Some(&json!(app.vectors().script_hash))
            );
            assert_eq!(
                item.get("script"),
                Some(&json!({
                    "language": "native",
                    "script": app.vectors().script_cbor_hex,
                }))
            );
        }
    }

    #[tokio::test]
    async fn matches_order_oldest_first() {
        let app = TestApp::new();
        let path = format!("/matches/{}?order=oldest_first", app.vectors().address);
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let items: Vec<MatchResponseTest> =
            serde_json::from_slice(&bytes).expect("failed to parse ordered matches");
        let slots: Vec<_> = items.iter().map(|item| item.created_at.slot_no).collect();
        assert!(slots.windows(2).all(|window| window[0] <= window[1]));
    }

    #[tokio::test]
    async fn matches_created_bounds_with_point() {
        let app = TestApp::new();
        let block = app
            .vectors()
            .blocks
            .get(2)
            .expect("missing bounded block vector");
        let point = format!("{}.{}", block.slot, block.block_hash);
        let path = format!(
            "/matches/{}?created_after={point}&created_before={point}",
            app.vectors().address
        );
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let items: Vec<MatchResponseTest> =
            serde_json::from_slice(&bytes).expect("failed to parse bounded matches");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].created_at.slot_no, block.slot);
        assert_eq!(items[0].created_at.header_hash, block.block_hash);
    }

    #[tokio::test]
    async fn matches_policy_and_asset_filters() {
        let app = TestApp::new();
        let asset_key = format!(
            "{}.{}",
            app.vectors().policy_id,
            app.vectors().asset_name_hex
        );
        let path = format!(
            "/matches/{}?policy_id={}&asset_name={}",
            app.vectors().address,
            app.vectors().policy_id,
            app.vectors().asset_name_hex,
        );
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let items: Vec<MatchResponseTest> =
            serde_json::from_slice(&bytes).expect("failed to parse filtered matches");
        assert_eq!(items.len(), app.vectors().blocks.len());
        assert!(items
            .iter()
            .all(|item| item.value.assets.contains_key(&asset_key)));
    }

    #[tokio::test]
    async fn matches_transaction_id_and_output_index_filters() {
        let app = TestApp::new();
        let path = format!(
            "/matches/{}?transaction_id={}&output_index=0",
            app.vectors().address,
            app.vectors().tx_hash,
        );
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let items: Vec<MatchResponseTest> =
            serde_json::from_slice(&bytes).expect("failed to parse tx/output filtered matches");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].transaction_id, app.vectors().tx_hash);
        assert_eq!(items[0].output_index, 0);
    }

    #[tokio::test]
    async fn matches_wildcard_pattern_is_rejected() {
        let app = TestApp::new();
        assert_status(&app, "/matches/*", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn matches_metadata_tag_pattern_is_rejected() {
        let app = TestApp::new();
        assert_status(&app, "/matches/%7B42%7D", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn matches_spent_filters_are_rejected() {
        let app = TestApp::new();
        let path = format!("/matches/{}?spent_after=1", app.vectors().address);
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn matches_bad_order_is_rejected() {
        let app = TestApp::new();
        let path = format!("/matches/{}?order=sideways", app.vectors().address);
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn matches_asset_name_without_policy_id_is_rejected() {
        let app = TestApp::new();
        let path = format!(
            "/matches/{}?asset_name={}",
            app.vectors().address,
            app.vectors().asset_name_hex,
        );
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn matches_output_index_without_transaction_id_is_rejected() {
        let app = TestApp::new();
        let path = format!("/matches/{}?output_index=0", app.vectors().address);
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn matches_bad_created_point_is_rejected() {
        let app = TestApp::new();
        let path = format!(
            "/matches/{}?created_after=1.not-a-hash",
            app.vectors().address
        );
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn matches_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = format!("/matches/{}", app.vectors().address);
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    const SLOW_READ: Duration = Duration::from_secs(1);
    const HEALTH_BUDGET: Duration = Duration::from_millis(300);

    fn policy_match_path(app: &TestApp) -> String {
        format!("/matches/{}.*", app.vectors().policy_id)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_match_does_not_stall_health_on_one_thread() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowUtxoReads(SLOW_READ)));
        let path = policy_match_path(&app);

        let started = Instant::now();
        let matched = app.get_bytes(&path);
        let health = async {
            let (status, _) = app.get_bytes("/health").await;
            (status, started.elapsed())
        };
        let ((match_status, _), (health_status, health_took)) = tokio::join!(matched, health);

        assert_eq!(match_status, StatusCode::OK);
        assert_eq!(health_status, StatusCode::OK);
        assert!(
            health_took < HEALTH_BUDGET,
            "/health took {health_took:?} behind a slow match"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_matches_do_not_stall_health_on_two_workers() {
        let app = Arc::new(TestApp::new_with_fault(Some(TestFault::SlowUtxoReads(
            SLOW_READ,
        ))));
        let path = policy_match_path(&app);

        let matches: Vec<_> = (0..4)
            .map(|_| {
                let app = app.clone();
                let path = path.clone();
                tokio::spawn(async move { app.get_bytes(&path).await.0 })
            })
            .collect();

        // Let the matches reach their store reads before /health asks. The
        // clock starts first: workers stuck in a read drive no timers, so the
        // sleep itself stalls with them.
        let started = Instant::now();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let health = tokio::spawn({
            let app = app.clone();
            async move {
                let (status, _) = app.get_bytes("/health").await;
                (status, started.elapsed())
            }
        });

        let (health_status, health_took) = health.await.expect("health task");
        assert_eq!(health_status, StatusCode::OK);
        assert!(
            health_took < HEALTH_BUDGET,
            "/health took {health_took:?} behind four slow matches"
        );

        for matched in matches {
            assert_eq!(matched.await.expect("match task"), StatusCode::OK);
        }
    }

    async fn candidates(app: &TestApp, resolve_hashes: bool, chunk: usize) -> Vec<Candidate> {
        let pattern = match patterns::Pattern::parse(&format!("{}.*", app.vectors().policy_id)) {
            Ok(patterns::Pattern::Asset(pattern)) => pattern,
            other => panic!("unexpected policy pattern {other:?}"),
        };
        let (refs, filter) = refs_for_asset_pattern(app.domain(), &pattern).expect("policy refs");
        let query = AsyncQueryFacade::new(app.domain().clone());

        let mut candidates = select_candidates(&query, refs, filter, resolve_hashes, chunk)
            .await
            .expect("candidates");
        candidates.sort_by(|a, b| a.txo_ref.cmp(&b.txo_ref));
        candidates
    }

    #[tokio::test]
    async fn chunked_selection_matches_unchunked() {
        let app = TestApp::new();

        for resolve_hashes in [false, true] {
            let whole = candidates(&app, resolve_hashes, usize::MAX).await;
            let chunked = candidates(&app, resolve_hashes, 2).await;

            assert!(whole.len() > 2, "too few candidates to split into chunks");
            assert_eq!(chunked, whole);
        }
    }
}
