use dolos_core::BlockSlot;
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

/// An overview of the server & connection status. Note that, when
/// `most_recent_checkpoint` and `most_recent_node_tip` are equal, the index is
/// fully synchronized.
#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct Health {
    pub connection_status: ConnectionStatus,
    pub most_recent_checkpoint: Option<BlockSlot>,
    pub most_recent_node_tip: Option<BlockSlot>,
    pub seconds_since_last_block: Option<i32>,
    pub network_synchronization: Option<f64>,
    pub configuration: HealthConfiguration,
    pub version: String,
}

/// Condition of the connection with the underlying node.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Default,
)]
pub enum ConnectionStatus {
    #[serde(rename = "connected")]
    #[default]
    Connected,
    #[serde(rename = "disconnected")]
    Disconnected,
}

#[derive(Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
pub struct HealthConfiguration {
    pub indexes: Indexes,
}

#[derive(
    Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Default,
)]
pub enum Indexes {
    #[serde(rename = "deferred")]
    Deferred,
    #[serde(rename = "installed")]
    #[default]
    Installed,
}

macro_rules! string_or_nan {
    ($value:expr) => {
        match $value {
            Some(inner) => inner.to_string(),
            None => "NaN".to_string(),
        }
    };
}

impl Health {
    pub fn to_prometheus(&self) -> String {
        let checkpoint = string_or_nan!(self.most_recent_checkpoint);
        let node_tip = string_or_nan!(self.most_recent_node_tip);
        let seconds_since_last_block = string_or_nan!(self.seconds_since_last_block);
        let network_synchronization = string_or_nan!(self.network_synchronization);

        format!(
            "# TYPE kupo_configuration_indexes gauge\n\
            kupo_configuration_indexes 1.0\n\n\
            # TYPE kupo_connection_status gauge\n\
            kupo_connection_status 1.0\n\n\
            # TYPE kupo_most_recent_checkpoint counter\n\
            kupo_most_recent_checkpoint {checkpoint}\n\n\
            # TYPE kupo_most_recent_node_tip counter\n\
            kupo_most_recent_node_tip {node_tip}\n\n\
            # TYPE kupo_network_synchronization gauge\n\
            kupo_network_synchronization {network_synchronization}\n\n\
            # TYPE kupo_seconds_since_last_block gauge\n\
            kupo_seconds_since_last_block {seconds_since_last_block}\n",
        )
    }
}
