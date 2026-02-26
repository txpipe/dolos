use std::collections::HashMap;

use pallas::crypto::hash::Hash;
use pallas::ledger::primitives::Metadatum;
use pallas::ledger::traverse::MultiEraTx;
use serde_json::Value as JsonValue;

const CIP25_REQUIRED_KEYS: &[&str] = &["name", "image"];
const CIP25_STRING_KEYS: &[&str] = &["name", "mediaType"];
const CIP25_STRING_OR_STRING_ARRAY_KEYS: &[&str] = &["image", "description"];
const CIP25_FILES_REQUIRED_KEYS: &[&str] = &["name", "mediaType", "src"];
const CIP25_FILES_STRING_KEYS: &[&str] = &["name", "mediaType"];
const CIP25_FILES_STRING_OR_STRING_ARRAY_KEYS: &[&str] = &["src"];
const CIP25_METADATA_LABEL: u64 = 721;

#[derive(Debug, Clone, Copy)]
pub enum Cip25MetadataVersion {
    V1,
    V2,
}

pub fn cip25_metadata_is_valid(
    metadata: &HashMap<String, JsonValue>,
    version: Cip25MetadataVersion,
) -> bool {
    if !cip25_required_keys_present(metadata) {
        return false;
    }

    let allow_string_array = matches!(version, Cip25MetadataVersion::V2);

    for (key, value) in metadata.iter() {
        if cip25_key_in_list(CIP25_STRING_KEYS, key) {
            if !cip25_is_string(value) {
                return false;
            }
        } else if cip25_key_in_list(CIP25_STRING_OR_STRING_ARRAY_KEYS, key) {
            if !cip25_is_string_or_string_array(value, allow_string_array) {
                return false;
            }
        } else if key == "files" && !cip25_files_are_valid(value, allow_string_array) {
            return false;
        }
    }

    true
}

pub fn cip25_metadata_for_tx(tx: &MultiEraTx) -> Option<Metadatum> {
    tx.metadata().find(CIP25_METADATA_LABEL).cloned()
}

pub fn cip25_metadata_has_asset(
    metadata: &Metadatum,
    policy: &Hash<28>,
    asset_name: &[u8],
) -> bool {
    let Metadatum::Map(policies) = metadata else {
        return false;
    };

    let policy_hex = hex::encode(policy.as_slice());
    let asset_hex = hex::encode(asset_name);
    let asset_utf8 = std::str::from_utf8(asset_name).ok();

    let policy_entry = policies
        .iter()
        .find(|(key, _)| key_matches_policy(key, policy.as_slice(), &policy_hex));

    let Some((_, policy_metadata)) = policy_entry else {
        return false;
    };

    let Metadatum::Map(assets) = policy_metadata else {
        return false;
    };

    assets
        .iter()
        .any(|(key, _)| key_matches_asset(key, asset_name, asset_utf8, &asset_hex))
}

fn key_matches_policy(key: &Metadatum, policy_bytes: &[u8], policy_hex: &str) -> bool {
    match key {
        Metadatum::Bytes(bytes) => bytes.as_slice() == policy_bytes,
        Metadatum::Text(text) => text.eq_ignore_ascii_case(policy_hex),
        _ => false,
    }
}

fn key_matches_asset(
    key: &Metadatum,
    asset_bytes: &[u8],
    asset_utf8: Option<&str>,
    asset_hex: &str,
) -> bool {
    match key {
        Metadatum::Bytes(bytes) => bytes.as_slice() == asset_bytes,
        Metadatum::Text(text) => {
            if let Some(asset_utf8) = asset_utf8 {
                if text == asset_utf8 {
                    return true;
                }
            }
            text.eq_ignore_ascii_case(asset_hex)
        }
        _ => false,
    }
}

fn cip25_required_keys_present(metadata: &HashMap<String, JsonValue>) -> bool {
    CIP25_REQUIRED_KEYS
        .iter()
        .all(|key| metadata.contains_key(*key))
}

fn cip25_key_in_list(list: &[&str], key: &str) -> bool {
    list.contains(&key)
}

fn cip25_is_string(value: &JsonValue) -> bool {
    matches!(value, JsonValue::String(_))
}

fn cip25_is_string_array(value: &JsonValue) -> bool {
    match value {
        JsonValue::Array(items) => items
            .iter()
            .all(|item| matches!(item, JsonValue::String(_))),
        _ => false,
    }
}

fn cip25_is_string_or_string_array(value: &JsonValue, allow_array: bool) -> bool {
    cip25_is_string(value) || (allow_array && cip25_is_string_array(value))
}

fn cip25_files_are_valid(value: &JsonValue, allow_array: bool) -> bool {
    let JsonValue::Array(items) = value else {
        return false;
    };

    for item in items.iter() {
        let JsonValue::Object(map) = item else {
            return false;
        };

        if !CIP25_FILES_REQUIRED_KEYS
            .iter()
            .all(|key| map.contains_key(*key))
        {
            return false;
        }

        for (key, value) in map.iter() {
            if cip25_key_in_list(CIP25_FILES_STRING_KEYS, key) {
                if !cip25_is_string(value) {
                    return false;
                }
            } else if cip25_key_in_list(CIP25_FILES_STRING_OR_STRING_ARRAY_KEYS, key)
                && !cip25_is_string_or_string_array(value, allow_array)
            {
                return false;
            }
        }
    }

    true
}
