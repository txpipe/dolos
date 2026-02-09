use std::collections::HashMap;

use serde_json::Value as JsonValue;

const CIP25_REQUIRED_KEYS: &[&str] = &["name", "image"];
const CIP25_STRING_KEYS: &[&str] = &["name", "mediaType"];
const CIP25_STRING_OR_STRING_ARRAY_KEYS: &[&str] = &["image", "description"];
const CIP25_FILES_REQUIRED_KEYS: &[&str] = &["name", "mediaType", "src"];
const CIP25_FILES_STRING_KEYS: &[&str] = &["name", "mediaType"];
const CIP25_FILES_STRING_OR_STRING_ARRAY_KEYS: &[&str] = &["src"];

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
