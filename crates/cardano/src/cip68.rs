use std::{collections::HashMap, ops::Deref};

use crc::{Crc, CRC_8_SMBUS};
use pallas::codec::minicbor;
use pallas::ledger::primitives::{BigInt, PlutusData};
use pallas::ledger::traverse::MultiEraTx;
use serde_json::Value as JsonValue;
use thiserror::Error;

const CRC8_ALGO: Crc<u8> = Crc::<u8>::new(&CRC_8_SMBUS);
const CIP25_METADATA_LABEL: u64 = 721;

pub fn has_cip25_metadata(tx: &MultiEraTx) -> bool {
    tx.metadata().find(CIP25_METADATA_LABEL).is_some()
}

#[derive(Debug, Error)]
pub enum Cip68Error {
    #[error("cbor encode error: {0}")]
    CborEncode(String),
}

#[derive(Debug, Clone)]
pub enum CIP68Label {
    ReferenceNft,
    Nft,
    Ft,
    Rft,
}

impl CIP68Label {
    pub fn from_u32(value: u32) -> Option<Self> {
        match value {
            100 => Some(Self::ReferenceNft),
            222 => Some(Self::Nft),
            333 => Some(Self::Ft),
            444 => Some(Self::Rft),
            _ => None,
        }
    }

    pub fn to_u32(&self) -> u32 {
        match self {
            CIP68Label::ReferenceNft => 100,
            CIP68Label::Nft => 222,
            CIP68Label::Ft => 333,
            CIP68Label::Rft => 444,
        }
    }

    pub fn to_label(&self) -> String {
        let number_hex = format!("{:04x}", self.to_u32());
        let bytes = hex::decode(&number_hex).unwrap();
        let checksum = format!("{:02x}", CRC8_ALGO.checksum(&bytes));
        format!("0{number_hex}{checksum}0")
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Cip68TokenStandard {
    Nft,
    Ft,
    Rft,
}

impl Cip68TokenStandard {
    pub fn from_label(label: CIP68Label) -> Option<Self> {
        match label {
            CIP68Label::Nft => Some(Self::Nft),
            CIP68Label::Ft => Some(Self::Ft),
            CIP68Label::Rft => Some(Self::Rft),
            CIP68Label::ReferenceNft => None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum PropertyKind {
    Bytestring,
    Number,
    Array,
    /// A string, or an array of strings. The `src` property of a files item
    /// uses this kind. The parser gives a single string for both forms. The
    /// validator accepts both forms.
    StringOrArray,
}

/// One key in a files item, its scheme, and whether the item must have it.
#[derive(Debug, Clone, Copy)]
pub struct ItemProperty {
    pub key: &'static str,
    pub scheme: PropertyScheme,
    pub required: bool,
}

#[derive(Debug, Clone, Copy)]
pub struct PropertyScheme {
    pub kind: PropertyKind,
    pub items: Option<&'static [ItemProperty]>,
}

const fn bytestring_scheme() -> PropertyScheme {
    PropertyScheme {
        kind: PropertyKind::Bytestring,
        items: None,
    }
}

const fn number_scheme() -> PropertyScheme {
    PropertyScheme {
        kind: PropertyKind::Number,
        items: None,
    }
}

const fn string_or_array_scheme() -> PropertyScheme {
    PropertyScheme {
        kind: PropertyKind::StringOrArray,
        items: None,
    }
}

const fn array_scheme(items: &'static [ItemProperty]) -> PropertyScheme {
    PropertyScheme {
        kind: PropertyKind::Array,
        items: Some(items),
    }
}

const fn item(key: &'static str, scheme: PropertyScheme, required: bool) -> ItemProperty {
    ItemProperty {
        key,
        scheme,
        required,
    }
}

// A files item must have `mediaType` and `src`. The `name` property is
// optional. The `src` property is a string, or an array of strings. Other
// properties are permitted.
const FILES_ITEM_SCHEMA: &[ItemProperty] = &[
    item("name", bytestring_scheme(), false),
    item("mediaType", bytestring_scheme(), true),
    item("src", string_or_array_scheme(), true),
];

pub fn property_scheme_for_key(standard: Cip68TokenStandard, key: &str) -> Option<PropertyScheme> {
    match standard {
        Cip68TokenStandard::Ft => match key {
            "name" => Some(bytestring_scheme()),
            "description" => Some(bytestring_scheme()),
            "ticker" => Some(bytestring_scheme()),
            "url" => Some(bytestring_scheme()),
            "logo" => Some(bytestring_scheme()),
            "decimals" => Some(number_scheme()),
            _ => None,
        },
        Cip68TokenStandard::Nft => match key {
            "name" => Some(bytestring_scheme()),
            "image" => Some(bytestring_scheme()),
            "mediaType" => Some(bytestring_scheme()),
            "description" => Some(bytestring_scheme()),
            "files" => Some(array_scheme(FILES_ITEM_SCHEMA)),
            _ => None,
        },
        Cip68TokenStandard::Rft => match key {
            "name" => Some(bytestring_scheme()),
            "image" => Some(bytestring_scheme()),
            "mediaType" => Some(bytestring_scheme()),
            "description" => Some(bytestring_scheme()),
            "decimals" => Some(number_scheme()),
            "files" => Some(array_scheme(FILES_ITEM_SCHEMA)),
            _ => None,
        },
    }
}

/// These are the properties that a standard requires. A datum that does not
/// have all of them describes no asset. As a result, it has no metadata.
fn required_properties(standard: Cip68TokenStandard) -> &'static [&'static str] {
    match standard {
        // the 222 standard and the 444 standard both show the asset
        Cip68TokenStandard::Nft | Cip68TokenStandard::Rft => &["name", "image"],
        Cip68TokenStandard::Ft => &["name", "description"],
    }
}

fn value_matches_kind(value: &JsonValue, scheme: PropertyScheme) -> bool {
    match scheme.kind {
        PropertyKind::Bytestring => value.is_string(),
        PropertyKind::Number => value.is_number(),
        PropertyKind::StringOrArray => {
            value.is_string()
                || value
                    .as_array()
                    .is_some_and(|items| items.iter().all(JsonValue::is_string))
        }
        PropertyKind::Array => match (value.as_array(), scheme.items) {
            (Some(items), Some(item_schema)) => items
                .iter()
                .all(|item| item_matches_schema(item, item_schema)),
            (Some(_), None) => true,
            (None, _) => false,
        },
    }
}

/// Whether one item of an array property meets its scheme. The item must be an
/// object. It must have every required key with a value of the correct kind.
/// A key that the scheme declares as optional must also have the correct kind
/// when it is present. Other keys are permitted.
fn item_matches_schema(item: &JsonValue, schema: &'static [ItemProperty]) -> bool {
    let Some(object) = item.as_object() else {
        return false;
    };

    schema
        .iter()
        .all(|property| match object.get(property.key) {
            Some(value) => value_matches_kind(value, property.scheme),
            None => !property.required,
        })
}

/// Whether a parsed datum meets the scheme of its standard. The datum must
/// have every required property. Each property that the scheme declares must
/// have the kind of value that the scheme gives it. A property that the
/// scheme does not declare can have any value.
pub fn cip68_metadata_is_valid(
    metadata: &HashMap<String, JsonValue>,
    standard: Cip68TokenStandard,
) -> bool {
    if !required_properties(standard)
        .iter()
        .all(|key| metadata.contains_key(*key))
    {
        return false;
    }

    metadata.iter().all(
        |(key, value)| match property_scheme_for_key(standard, key) {
            Some(scheme) => value_matches_kind(value, scheme),
            None => true,
        },
    )
}

pub fn cip_68_reference_asset(
    policy_id: &str,
    asset_name: &str,
) -> Result<Option<(String, Cip68TokenStandard)>, Cip68Error> {
    if asset_name.len() < 8 {
        return Ok(None);
    }

    let label = &asset_name[..8];
    let number = match parse_cip67_label_hex(label) {
        Some(value) => value,
        None => return Ok(None),
    };
    let asset_name_without_label_prefix = &asset_name[8..];

    let label = match CIP68Label::from_u32(number) {
        Some(value) => value,
        None => return Ok(None),
    };
    let standard = match Cip68TokenStandard::from_label(label) {
        Some(value) => value,
        None => return Ok(None),
    };
    let reference = format!(
        "{}{}{}",
        policy_id,
        CIP68Label::ReferenceNft.to_label(),
        asset_name_without_label_prefix
    );
    Ok(Some((reference, standard)))
}

pub fn encode_to_hex<T: minicbor::Encode<()>>(value: &T) -> Result<String, Cip68Error> {
    let mut buf = Vec::new();
    minicbor::encode(value, &mut buf).map_err(|err| Cip68Error::CborEncode(err.to_string()))?;
    Ok(hex::encode(buf))
}

pub fn parse_cip68_metadata_map(
    map: &[(PlutusData, PlutusData)],
    standard: Cip68TokenStandard,
) -> Result<HashMap<String, JsonValue>, Cip68Error> {
    let mut metadata = HashMap::new();
    for (key, value) in map.iter() {
        let key_str = convert_metadata_key(key)?;
        if let Some(schema) = property_scheme_for_key(standard, &key_str) {
            let parsed = convert_datum_value(value, schema);
            let entry = parsed.unwrap_or(JsonValue::String(encode_to_hex(value)?));
            metadata.insert(key_str, entry);
        } else {
            metadata.insert(key_str, JsonValue::String(encode_to_hex(value)?));
        }
    }

    Ok(metadata)
}

fn to_utf8_or_hex(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(value) => value.to_string(),
        Err(_) => hex::encode(bytes),
    }
}

fn map_schema_lookup(schema: &'static [ItemProperty], key: &str) -> Option<PropertyScheme> {
    schema
        .iter()
        .find(|property| property.key == key)
        .map(|property| property.scheme)
}

fn convert_bytestring_value(value: &PlutusData) -> Option<JsonValue> {
    match value {
        PlutusData::BoundedBytes(bytes) => {
            Some(JsonValue::String(to_utf8_or_hex(bytes.as_slice())))
        }
        PlutusData::Array(items) => {
            let mut buffer = Vec::new();
            for item in items.iter() {
                match item {
                    PlutusData::BoundedBytes(bytes) => buffer.extend_from_slice(bytes.as_slice()),
                    _ => return None,
                }
            }
            Some(JsonValue::String(to_utf8_or_hex(&buffer)))
        }
        _ => None,
    }
}

fn convert_number_value(value: &PlutusData) -> Option<JsonValue> {
    match value {
        PlutusData::BigInt(BigInt::Int(int)) => {
            let num = i64::try_from(*int.deref()).ok()?;
            Some(JsonValue::Number(num.into()))
        }
        _ => None,
    }
}

fn convert_map_value(value: &PlutusData, schema: &'static [ItemProperty]) -> Option<JsonValue> {
    let PlutusData::Map(map) = value else {
        return None;
    };

    let mut object = serde_json::Map::new();
    for (key, value) in map.iter() {
        let key_str = match key {
            PlutusData::BoundedBytes(bytes) => to_utf8_or_hex(bytes.as_slice()),
            PlutusData::BigInt(BigInt::Int(int)) => int.deref().to_string(),
            _ => return None,
        };
        // the scheme permits other properties. a key that the scheme does not
        // declare keeps its hex form.
        let converted = match map_schema_lookup(schema, &key_str) {
            Some(value_schema) => convert_datum_value(value, value_schema)?,
            None => JsonValue::String(encode_to_hex(value).ok()?),
        };
        object.insert(key_str, converted);
    }

    Some(JsonValue::Object(object))
}

fn convert_datum_value(value: &PlutusData, schema: PropertyScheme) -> Option<JsonValue> {
    match schema.kind {
        // the bytestring converter joins an array of byte parts into one
        // string. a version 3 `src` uses this form for a payload of more than
        // 64 bytes, so this converter gives a single string for it.
        PropertyKind::Bytestring | PropertyKind::StringOrArray => convert_bytestring_value(value),
        PropertyKind::Number => convert_number_value(value),
        PropertyKind::Array => {
            let PlutusData::Array(items) = value else {
                return None;
            };
            let items_schema = schema.items?;
            let mut converted = Vec::new();
            for item in items.iter() {
                let value = convert_map_value(item, items_schema)?;
                converted.push(value);
            }
            Some(JsonValue::Array(converted))
        }
    }
}

fn convert_metadata_key(key: &PlutusData) -> Result<String, Cip68Error> {
    Ok(match key {
        PlutusData::BoundedBytes(bytes) => to_utf8_or_hex(bytes.as_slice()),
        PlutusData::BigInt(BigInt::Int(int)) => int.deref().to_string(),
        PlutusData::BigInt(BigInt::BigUInt(bytes)) => hex::encode(bytes.as_slice()),
        PlutusData::BigInt(BigInt::BigNInt(bytes)) => hex::encode(bytes.as_slice()),
        _ => encode_to_hex(key)?,
    })
}

pub fn parse_cip67_label_hex(label_hex: &str) -> Option<u32> {
    if label_hex.len() != 8 || !(label_hex.starts_with('0') && label_hex.ends_with('0')) {
        return None;
    }

    let number_hex = &label_hex[1..5];
    let checksum_hex = &label_hex[5..7];
    let bytes = hex::decode(number_hex).ok()?;
    let checksum = format!("{:02x}", CRC8_ALGO.checksum(&bytes));
    if !checksum_hex.eq_ignore_ascii_case(&checksum) {
        return None;
    }

    u32::from_str_radix(number_hex, 16).ok()
}

pub fn parse_cip67_label_from_asset_name(asset_name: &[u8]) -> Option<u32> {
    if asset_name.len() < 4 {
        return None;
    }

    let label_hex = hex::encode(&asset_name[..4]);
    parse_cip67_label_hex(&label_hex)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn label_hex(number: u32) -> String {
        let number_hex = format!("{:04x}", number);
        let bytes = hex::decode(&number_hex).expect("valid hex");
        let checksum = format!("{:02x}", CRC8_ALGO.checksum(&bytes));
        format!("0{number_hex}{checksum}0")
    }

    #[test]
    fn parses_known_labels() {
        for value in [100u32, 222u32, 333u32, 444u32] {
            let hex = label_hex(value);
            assert_eq!(parse_cip67_label_hex(&hex), Some(value));
        }
    }

    #[test]
    fn rejects_invalid_checksum() {
        let mut hex = label_hex(100);
        hex.replace_range(6..7, "0");
        assert_eq!(parse_cip67_label_hex(&hex), None);
    }

    #[test]
    fn rejects_invalid_length() {
        assert_eq!(parse_cip67_label_hex("0000"), None);
    }

    #[test]
    fn parses_from_asset_name_bytes() {
        let hex = label_hex(222);
        let bytes = hex::decode(hex).expect("valid hex");
        assert_eq!(parse_cip67_label_from_asset_name(&bytes), Some(222));
    }

    fn metadata(pairs: &[(&str, JsonValue)]) -> HashMap<String, JsonValue> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.clone()))
            .collect()
    }

    #[test]
    fn accepts_metadata_that_meets_the_scheme() {
        let nft = metadata(&[
            ("name", JsonValue::String("token".into())),
            ("image", JsonValue::String("ipfs://x".into())),
        ]);
        assert!(cip68_metadata_is_valid(&nft, Cip68TokenStandard::Nft));

        let ft = metadata(&[
            ("name", JsonValue::String("token".into())),
            ("description", JsonValue::String("a token".into())),
            ("decimals", JsonValue::Number(6.into())),
        ]);
        assert!(cip68_metadata_is_valid(&ft, Cip68TokenStandard::Ft));
    }

    #[test]
    fn rejects_metadata_missing_a_required_property() {
        // fungible properties and no image: this datum does not meet a non-fungible
        // standard
        let value = metadata(&[
            ("name", JsonValue::String("token".into())),
            ("description", JsonValue::String("a token".into())),
        ]);

        assert!(!cip68_metadata_is_valid(&value, Cip68TokenStandard::Nft));
        assert!(!cip68_metadata_is_valid(&value, Cip68TokenStandard::Rft));
        assert!(cip68_metadata_is_valid(&value, Cip68TokenStandard::Ft));
    }

    #[test]
    fn rejects_declared_property_of_the_wrong_kind() {
        let value = metadata(&[
            ("name", JsonValue::String("token".into())),
            ("description", JsonValue::String("a token".into())),
            ("decimals", JsonValue::String("02".into())),
        ]);

        assert!(!cip68_metadata_is_valid(&value, Cip68TokenStandard::Ft));
    }

    #[test]
    fn ignores_properties_the_scheme_does_not_declare() {
        let value = metadata(&[
            ("name", JsonValue::String("token".into())),
            ("image", JsonValue::String("ipfs://x".into())),
            ("decimals", JsonValue::String("02".into())),
        ]);

        assert!(cip68_metadata_is_valid(&value, Cip68TokenStandard::Nft));
    }

    fn nft_with_files(files: JsonValue) -> HashMap<String, JsonValue> {
        metadata(&[
            ("name", JsonValue::String("token".into())),
            ("image", JsonValue::String("ipfs://x".into())),
            ("files", files),
        ])
    }

    #[test]
    fn rejects_files_item_without_the_required_keys() {
        // an empty files item has no `mediaType` and no `src`. the scheme
        // requires both, so this datum does not meet the scheme.
        let empty = nft_with_files(json!([{}]));
        assert!(!cip68_metadata_is_valid(&empty, Cip68TokenStandard::Nft));

        // a files item that has no `src`.
        let no_src = nft_with_files(json!([{ "mediaType": "image/png" }]));
        assert!(!cip68_metadata_is_valid(&no_src, Cip68TokenStandard::Nft));

        // a files item that has no `mediaType`.
        let no_media_type = nft_with_files(json!([{ "src": "ipfs://y" }]));
        assert!(!cip68_metadata_is_valid(
            &no_media_type,
            Cip68TokenStandard::Nft
        ));
    }

    #[test]
    fn accepts_a_files_item_that_meets_the_scheme() {
        // `name` is optional. `src` is a string.
        let string_src = nft_with_files(json!([{
            "mediaType": "image/png",
            "src": "ipfs://y",
        }]));
        assert!(cip68_metadata_is_valid(
            &string_src,
            Cip68TokenStandard::Nft
        ));

        // `src` is an array of strings. a version 3 payload uses this form.
        let array_src = nft_with_files(json!([{
            "mediaType": "image/png",
            "src": ["ipfs://part-one", "part-two"],
        }]));
        assert!(cip68_metadata_is_valid(&array_src, Cip68TokenStandard::Nft));

        // the scheme permits other keys in a files item.
        let extra_key = nft_with_files(json!([{
            "mediaType": "image/png",
            "src": "ipfs://y",
            "name": "picture",
            "note": "extra",
        }]));
        assert!(cip68_metadata_is_valid(&extra_key, Cip68TokenStandard::Nft));
    }

    #[test]
    fn rejects_files_item_of_the_wrong_kind() {
        // `src` must be a string, or an array of strings. a number does not
        // meet the scheme.
        let number_src = nft_with_files(json!([{
            "mediaType": "image/png",
            "src": 7,
        }]));
        assert!(!cip68_metadata_is_valid(
            &number_src,
            Cip68TokenStandard::Nft
        ));

        // a files item that is not an object.
        let not_object = nft_with_files(json!(["not-an-object"]));
        assert!(!cip68_metadata_is_valid(
            &not_object,
            Cip68TokenStandard::Nft
        ));
    }

    #[test]
    fn cip68_label_round_trip() {
        let label = CIP68Label::ReferenceNft;
        let hex = label.to_label();
        assert_eq!(parse_cip67_label_hex(&hex), Some(label.to_u32()));
    }
}
