use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct BadRequest {
    /// Some hint about what went wrong.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hint: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Datum {
    #[serde(rename = "datum")]
    pub datum: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Script {
    #[serde(rename = "language")]
    pub language: ScriptLanguage,
    #[serde(rename = "script")]
    pub script: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize)]
pub enum ScriptLanguage {
    #[serde(rename = "native")]
    Native,
    #[serde(rename = "plutus:v1")]
    PlutusV1,
    #[serde(rename = "plutus:v2")]
    PlutusV2,
    #[serde(rename = "plutus:v3")]
    PlutusV3,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Metadata {
    pub hash: String,
    pub raw: String,
    pub schema: HashMap<String, Metadatum>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Metadatum {
    Int(MetadatumInt),
    String(MetadatumString),
    Bytes(MetadatumBytes),
    List(MetadatumList),
    Map(MetadatumMap),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadatumInt {
    pub int: i32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadatumString {
    pub string: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadatumBytes {
    pub bytes: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadatumList {
    pub list: Vec<Metadatum>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadatumMap {
    pub map: Vec<MetadatumMapEntry>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct MetadatumMapEntry {
    pub k: Metadatum,
    pub v: Metadatum,
}
