use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use dolos_cardano::{
    indexes::{
        AsyncCardanoQueryExt, CardanoIndexExt, ScriptData, ScriptLanguage as CardanoLanguage,
    },
    network_from_genesis, pallas_extras,
};
use dolos_core::{Domain, EraCbor, IndexStore as _, StateStore as _, TxoRef, UtxoSet};
use pallas::codec::minicbor;
use pallas::ledger::{
    addresses::{Address, StakeAddress},
    primitives::{conway::DatumOption, conway::ScriptRef, StakeCredential},
    traverse::{
        ComputeHash, Era, MultiEraBlock, MultiEraOutput, MultiEraTx, MultiEraValue, OriginalHash,
    },
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{bad_request, patterns, Facade};

pub async fn by_pattern<D: Domain>(
    State(facade): State<Facade<D>>,
    Path(pattern): Path<String>,
    Query(query): Query<MatchesQuery>,
) -> Response {
    let filters = match MatchesFilters::try_from_query(query, &facade) {
        Ok(filters) => filters,
        Err(err) => return err.into_response(),
    };

    let parsed = match patterns::Pattern::parse(&pattern) {
        Ok(parsed) => parsed,
        Err(err) => return bad_request(err.to_string()),
    };

    let (refs, filter) = match &parsed {
        patterns::Pattern::Address(pattern) => match refs_for_address_pattern(&facade, pattern) {
            Ok(result) => result,
            Err(err) => return err.into_response(),
        },
        patterns::Pattern::Asset(pattern) => match refs_for_asset_pattern(&facade, pattern) {
            Ok(result) => result,
            Err(err) => return err.into_response(),
        },
        patterns::Pattern::OutputRef(pattern) => {
            match refs_for_output_ref_pattern(&facade, pattern).await {
                Ok(result) => result,
                Err(err) => return err.into_response(),
            }
        }
        patterns::Pattern::Any => return bad_request("wildcard patterns are not supported"),
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

#[derive(Serialize)]
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
    fn try_from_query<D: Domain>(
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
            Some(value) => Some(parse_slot_or_point(value, facade)?),
            None => None,
        };

        let created_before = match query.created_before.as_deref() {
            Some(value) => Some(parse_slot_or_point(value, facade)?),
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

#[derive(Clone, Copy)]
struct BlockInfo {
    slot_no: u64,
    header_hash: pallas::crypto::hash::Hash<32>,
    tx_index: usize,
}

fn refs_for_address_pattern<D: Domain>(
    facade: &Facade<D>,
    pattern: &patterns::AddressPattern,
) -> Result<(UtxoSet, OutputFilter), MatchError> {
    match pattern {
        patterns::AddressPattern::Full(bytes) => {
            let address = Address::from_bytes(bytes)
                .map_err(|_| MatchError::BadRequest("invalid address".to_string()))?;

            match address {
                Address::Stake(stake) => {
                    let stake_bytes = stake.to_vec();
                    let refs = facade
                        .indexes()
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
                    let refs = facade
                        .indexes()
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
            let stake_keys = stake_keys_for_credential(facade, delegation)?;

            if payment_key.is_none() && stake_keys.is_none() {
                return Err(MatchError::BadRequest(
                    "wildcard credential patterns are not supported yet".to_string(),
                ));
            }

            let mut refs = payment_key
                .as_ref()
                .map(|key| facade.indexes().utxos_by_payment(key))
                .transpose()
                .map_err(|_| MatchError::Internal)?
                .unwrap_or_default();

            if let Some(stake_keys) = stake_keys {
                let mut stake_refs = UtxoSet::new();
                for key in stake_keys {
                    let next = facade
                        .indexes()
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
    facade: &Facade<D>,
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
        patterns::AssetNamePattern::Any => facade
            .indexes()
            .utxos_by_policy(pattern.policy())
            .map_err(|_| MatchError::Internal)?,
        patterns::AssetNamePattern::Exact(name) => {
            let mut subject = pattern.policy().to_vec();
            subject.extend_from_slice(name);
            facade
                .indexes()
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
    facade: &Facade<D>,
    credential: &patterns::CredentialPattern,
) -> Result<Option<Vec<Vec<u8>>>, MatchError> {
    let network = network_from_genesis(&facade.genesis());

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

async fn build_matches<D: Domain>(
    facade: &Facade<D>,
    refs: UtxoSet,
    filter: OutputFilter,
    resolve_hashes: bool,
) -> Result<Vec<MatchResponse>, MatchError> {
    let utxos = facade
        .state()
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| MatchError::Internal)?;

    let mut block_cache: HashMap<pallas::crypto::hash::Hash<32>, BlockInfo> = HashMap::new();
    let mut out = Vec::new();

    for (txo_ref, cbor) in utxos {
        let cbor: &dolos_core::EraCbor = cbor.as_ref();
        let output = MultiEraOutput::try_from(cbor).map_err(|_| MatchError::Internal)?;
        let address = output.address().map_err(|_| MatchError::Internal)?;

        if !matches_output_filter(&output, &address, &filter) {
            continue;
        }

        let tx_hash = txo_ref.0;
        let block_info = match block_cache.get(&tx_hash) {
            Some(info) => *info,
            None => {
                let Some((raw_block, tx_index)) = facade
                    .query()
                    .block_by_tx_hash(tx_hash.to_vec())
                    .await
                    .map_err(|_| MatchError::Internal)?
                else {
                    return Err(MatchError::Internal);
                };

                let block = MultiEraBlock::decode(&raw_block).map_err(|_| MatchError::Internal)?;
                let info = BlockInfo {
                    slot_no: block.slot(),
                    header_hash: block.header().hash(),
                    tx_index,
                };
                block_cache.insert(tx_hash, info);
                info
            }
        };

        let (datum_hash, datum_type) = output_datum_info(&output);
        let script_hash = output_script_hash(&output);
        let (datum, script) =
            resolve_output_extras(facade, &output, script_hash, resolve_hashes).await?;

        out.push(MatchResponse {
            transaction_index: block_info
                .tx_index
                .try_into()
                .map_err(|_| MatchError::Internal)?,
            transaction_id: tx_hash.to_string(),
            output_index: txo_ref.1,
            address: address.to_string(),
            value: map_value(output.value()),
            datum_hash,
            datum,
            datum_type,
            script_hash: script_hash.map(|hash: pallas::crypto::hash::Hash<28>| hash.to_string()),
            script,
            created_at: PointResponse {
                slot_no: block_info.slot_no,
                header_hash: block_info.header_hash.to_string(),
            },
            spent_at: None,
        });
    }

    Ok(out)
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

async fn resolve_output_extras<D: Domain>(
    facade: &Facade<D>,
    output: &MultiEraOutput<'_>,
    script_hash: Option<pallas::crypto::hash::Hash<28>>,
    resolve_hashes: bool,
) -> Result<(Option<serde_json::Value>, Option<serde_json::Value>), MatchError> {
    if resolve_hashes {
        let datum = resolve_datum(facade, output.datum()).await?;
        let script = resolve_script(facade, script_hash).await?;
        Ok((Some(datum), Some(script)))
    } else {
        Ok((None, None))
    }
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

async fn resolve_datum<D: Domain>(
    facade: &Facade<D>,
    datum: Option<DatumOption<'_>>,
) -> Result<serde_json::Value, MatchError> {
    let Some(datum) = datum else {
        return Ok(serde_json::Value::Null);
    };

    match datum {
        DatumOption::Data(data) => minicbor::to_vec(&data.0)
            .map(hex::encode)
            .map(serde_json::Value::String)
            .map_err(|_| MatchError::Internal),
        DatumOption::Hash(hash) => {
            let resolved = facade
                .query()
                .plutus_data(&hash)
                .await
                .map_err(|_| MatchError::Internal)?;
            Ok(resolved
                .map(minicbor::to_vec)
                .transpose()
                .map_err(|_| MatchError::Internal)?
                .map(hex::encode)
                .map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null))
        }
    }
}

async fn resolve_script<D: Domain>(
    facade: &Facade<D>,
    script_hash: Option<pallas::crypto::hash::Hash<28>>,
) -> Result<serde_json::Value, MatchError> {
    let Some(script_hash) = script_hash else {
        return Ok(serde_json::Value::Null);
    };

    let script = facade
        .query()
        .script_by_hash(&script_hash)
        .await
        .map_err(|_| MatchError::Internal)?;

    Ok(script
        .map(map_script_json)
        .unwrap_or(serde_json::Value::Null))
}

fn map_script_json(data: ScriptData) -> serde_json::Value {
    let language = match data.language {
        CardanoLanguage::Native => crate::types::ScriptLanguage::Native,
        CardanoLanguage::PlutusV1 => crate::types::ScriptLanguage::PlutusV1,
        CardanoLanguage::PlutusV2 => crate::types::ScriptLanguage::PlutusV2,
        CardanoLanguage::PlutusV3 => crate::types::ScriptLanguage::PlutusV3,
    };

    let script = crate::types::Script {
        language,
        script: hex::encode(data.script),
    };

    serde_json::to_value(script).unwrap_or(serde_json::Value::Null)
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

fn parse_slot_or_point<D: Domain>(value: &str, facade: &Facade<D>) -> Result<u64, MatchError> {
    if let Some((slot, hash)) = value.split_once('.') {
        let slot = slot
            .parse::<u64>()
            .map_err(|_| MatchError::BadRequest(slot_range_hint()))?;

        let bytes = hex::decode(hash).map_err(|_| MatchError::BadRequest(slot_range_hint()))?;
        if bytes.len() != 32 {
            return Err(MatchError::BadRequest(slot_range_hint()));
        }
        let found_slot = facade
            .indexes()
            .slot_by_block_hash(&bytes)
            .map_err(|_| MatchError::Internal)?
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
