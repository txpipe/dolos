use std::{collections::HashMap, ops::Deref};

use crc::{Crc, CRC_8_SMBUS};
use pallas::codec::minicbor;
use pallas::ledger::primitives::{BigInt, PlutusData};
use serde_json::Value as JsonValue;
use thiserror::Error;

const CRC8_ALGO: Crc<u8> = Crc::<u8>::new(&CRC_8_SMBUS);

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
}

#[derive(Debug, Clone, Copy)]
pub struct PropertyScheme {
    pub kind: PropertyKind,
    pub items: Option<&'static [(&'static str, PropertyScheme)]>,
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

const fn array_scheme(items: &'static [(&'static str, PropertyScheme)]) -> PropertyScheme {
    PropertyScheme {
        kind: PropertyKind::Array,
        items: Some(items),
    }
}

const FILES_ITEM_SCHEMA: &[(&str, PropertyScheme)] = &[
    ("name", bytestring_scheme()),
    ("mediaType", bytestring_scheme()),
    ("src", bytestring_scheme()),
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

fn map_schema_lookup(
    schema: &'static [(&'static str, PropertyScheme)],
    key: &str,
) -> Option<PropertyScheme> {
    schema
        .iter()
        .find(|(name, _)| *name == key)
        .map(|(_, scheme)| *scheme)
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

fn convert_map_value(
    value: &PlutusData,
    schema: &'static [(&'static str, PropertyScheme)],
) -> Option<JsonValue> {
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
        let value_schema = map_schema_lookup(schema, &key_str)?;
        let converted = convert_datum_value(value, value_schema)?;
        object.insert(key_str, converted);
    }

    Some(JsonValue::Object(object))
}

fn convert_datum_value(value: &PlutusData, schema: PropertyScheme) -> Option<JsonValue> {
    match schema.kind {
        PropertyKind::Bytestring => convert_bytestring_value(value),
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

    #[test]
    fn cip68_label_round_trip() {
        let label = CIP68Label::ReferenceNft;
        let hex = label.to_label();
        assert_eq!(parse_cip67_label_hex(&hex), Some(label.to_u32()));
    }
}
