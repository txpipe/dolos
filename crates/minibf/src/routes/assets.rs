use std::{collections::HashMap, ops::Deref};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset::{Asset, OnchainMetadataStandard};
use crc::{Crc, CRC_8_SMBUS};
use dolos_core::{ArchiveStore, Domain, EraCbor, State3Store as _};
use pallas::{
    codec::minicbor,
    ledger::{
        primitives::{BigInt, Metadatum, PlutusData},
        traverse::MultiEraTx,
        validate::phase2::to_plutus_data::ToPlutusData,
    },
};

use crate::{
    mapping::{asset_fingerprint, IntoModel},
    Facade,
};

struct OnchainMetadata {
    version: Option<OnchainMetadataStandard>,
    metadata: HashMap<String, serde_json::Value>,
    extra: Option<String>,
}
impl OnchainMetadata {
    fn from_plutus_data(plutus_data: PlutusData) -> Result<Option<Self>, StatusCode> {
        let value: serde_json::Value = plutus_data.into_model()?;

        if !value.is_array() {
            return Ok(None);
        }

        let array = value.as_array().unwrap();
        let Some(metadata) = array.first() else {
            return Ok(None);
        };
        if metadata.is_null() || !metadata.is_object() {
            return Ok(None);
        }
        let metadata = metadata.as_object().unwrap().clone().into_iter().collect();

        let version = array
            .get(1)
            .and_then(|v| v.as_number())
            .and_then(|n| n.as_i64())
            .and_then(|n| match n {
                1 => Some(OnchainMetadataStandard::Cip68v1),
                2 => Some(OnchainMetadataStandard::Cip68v2),
                3 => Some(OnchainMetadataStandard::Cip68v3),
                _ => None,
            });

        let extra = if let PlutusData::Constr(constr) = plutus_data {
            constr.fields.get(2).map(encode_to_hex).transpose()?
        } else {
            None
        };

        Ok(Some(Self {
            metadata,
            version,
            extra,
        }))
    }

    fn from_metadatum(unit: &str, metadatum: Metadatum) -> Result<Option<Self>, StatusCode> {
        let value = AssetMetadatum(metadatum).into_model()?;

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

        let version = Some(match version {
            Some(2) => OnchainMetadataStandard::Cip25v2,
            _ => OnchainMetadataStandard::Cip25v1,
        });

        let extra = None;

        Ok(Some(Self {
            metadata,
            version,
            extra,
        }))
    }
}

const CIP68_FIELDS: &[&str] = &[
    "name",
    "description",
    "image",
    "mediaType",
    "files",
    "ticker",
    "url",
    "logo",
    "decimals",
    "src",
];
const CRC8_ALGO: Crc<u8> = Crc::<u8>::new(&CRC_8_SMBUS);
#[derive(Debug, Clone)]
enum CIP68Label {
    ReferenceNft,
    Nft,
    Ft,
    Rft,
}
impl CIP68Label {
    fn from_u32(value: u32) -> Option<Self> {
        match value {
            100 => Some(Self::ReferenceNft),
            222 => Some(Self::Nft),
            333 => Some(Self::Ft),
            444 => Some(Self::Rft),
            _ => None,
        }
    }

    fn to_u32(&self) -> u32 {
        match self {
            CIP68Label::ReferenceNft => 100,
            CIP68Label::Nft => 222,
            CIP68Label::Ft => 333,
            CIP68Label::Rft => 444,
        }
    }

    fn to_label(&self) -> String {
        let number_hex = format!("{:04x}", self.to_u32());
        let bytes = hex::decode(&number_hex).unwrap();
        let checksum = format!("{:02x}", CRC8_ALGO.checksum(&bytes));
        format!("0{}{}0", number_hex, checksum)
    }
}

struct AssetMetadatum(Metadatum);
impl IntoModel<serde_json::Value> for AssetMetadatum {
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
                    .map(|d| AssetMetadatum(d).into_model())
                    .collect::<Result<Vec<_>, _>>()?;
                serde_json::Value::Array(values)
            }

            Metadatum::Map(x) => {
                let mut map = serde_json::Map::new();
                for (k, v) in x.iter() {
                    if let Some(key) = AssetMetadatum(k.clone()).into_model()?.as_str() {
                        map.insert(key.to_string(), AssetMetadatum(v.clone()).into_model()?);
                    }
                }
                serde_json::Value::Object(map)
            }
        })
    }
}

fn encode_to_hex<T: minicbor::Encode<()>>(value: &T) -> Result<String, StatusCode> {
    let mut buf = Vec::new();
    minicbor::encode(value, &mut buf).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(hex::encode(buf))
}

impl IntoModel<serde_json::Value> for &PlutusData {
    type SortKey = ();

    fn into_model(self) -> Result<serde_json::Value, StatusCode> {
        Ok(match self {
            PlutusData::Constr(x) => {
                let values = x
                    .fields
                    .iter()
                    .map(|d| d.clone().into_model())
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                serde_json::Value::Array(values)
            }

            PlutusData::Map(x) => {
                let mut map = serde_json::Map::new();
                for (k, v) in x.iter() {
                    let key_opt = k.into_model()?.as_str().map(|s| s.to_owned());

                    if let Some(key) = key_opt {
                        if CIP68_FIELDS.contains(&key.as_str()) {
                            map.insert(key, v.into_model()?);
                        } else {
                            map.insert(key, serde_json::Value::String(encode_to_hex(v)?));
                        }
                    }
                }
                serde_json::Value::Object(map)
            }

            PlutusData::Array(x) => {
                let values = x
                    .iter()
                    .map(|d| d.clone().into_model())
                    .collect::<Result<Vec<serde_json::Value>, _>>()?;

                serde_json::Value::Array(values)
            }

            PlutusData::BigInt(x) => match x {
                BigInt::Int(int) => match i64::try_from(*int.deref()) {
                    Ok(num) => serde_json::Value::Number(num.into()),
                    Err(_) => {
                        let hex_str = hex::encode(i128::from(*int.deref()).to_be_bytes());
                        serde_json::Value::String(hex_str)
                    }
                },
                BigInt::BigUInt(bounded_bytes) => {
                    serde_json::Value::String(hex::encode(bounded_bytes.as_slice()))
                }
                BigInt::BigNInt(bounded_bytes) => {
                    serde_json::Value::String(hex::encode(bounded_bytes.as_slice()))
                }
            },

            PlutusData::BoundedBytes(x) => match String::from_utf8(x.to_vec()) {
                Ok(s) => serde_json::Value::String(s),
                Err(_) => serde_json::Value::String(hex::encode(x.as_slice())),
            },
        })
    }
}

struct AssetModelBuilder {
    subject: Vec<u8>,
    unit: String,
    asset_state: dolos_cardano::model::AssetState,
    initial_tx: Option<EraCbor>,
}

impl AssetModelBuilder {
    fn cip_68_reference_asset(&self) -> Option<String> {
        let policy_id = &self.unit[..56];
        let asset_name = &self.unit[56..];

        let label = asset_name.get(0..8)?;
        if !(label.starts_with('0') && label.ends_with('0')) {
            return None;
        }

        let number = u32::from_str_radix(&label[1..5], 16).ok()?;
        let asset_name_without_label_prefix = &asset_name[8..];

        CIP68Label::from_u32(number).and_then(|label| match label {
            CIP68Label::ReferenceNft => None,
            _ => Some(format!(
                "{}{}{}",
                policy_id,
                CIP68Label::ReferenceNft.to_label(),
                asset_name_without_label_prefix
            )),
        })
    }

    fn initial_tx_metadata(&self) -> Result<Option<OnchainMetadata>, StatusCode> {
        let Some(EraCbor(era, cbor)) = &self.initial_tx else {
            return Ok(None);
        };

        let era = pallas::ledger::traverse::Era::try_from(*era)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx =
            MultiEraTx::decode_for_era(era, cbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if let Some(ref_asset) = self.cip_68_reference_asset() {
            let ref_asset_output = tx
                .outputs()
                .iter()
                .find(|o| {
                    o.value().assets().iter().any(|multi_asset| {
                        multi_asset.assets().iter().any(|asset| {
                            let mut unit = multi_asset.policy().to_vec();
                            unit.extend(asset.name());
                            let unit = hex::encode(unit);
                            ref_asset == unit
                        })
                    })
                })
                .cloned();

            if let Some(ref_asset_output) = ref_asset_output {
                if let Some(datum_option) = ref_asset_output.datum() {
                    match datum_option {
                        pallas::ledger::primitives::conway::DatumOption::Hash(hash) => {
                            if let Some(cbor_wrap) = tx.find_plutus_data(&hash) {
                                let out =
                                    OnchainMetadata::from_plutus_data(cbor_wrap.to_plutus_data())?;
                                return Ok(out);
                            }
                        }
                        pallas::ledger::primitives::conway::DatumOption::Data(cbor_wrap) => {
                            let out =
                                OnchainMetadata::from_plutus_data(cbor_wrap.to_plutus_data())?;
                            return Ok(out);
                        }
                    };
                }
            }
        }

        let out = tx
            .metadata()
            .find(721)
            .map(|metadatum| OnchainMetadata::from_metadatum(&self.unit, metadatum.clone()))
            .transpose()?
            .flatten();

        Ok(out)
    }
}

impl IntoModel<Asset> for AssetModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<Asset, StatusCode> {
        let policy = self.subject[..28].to_vec();
        let asset = self.subject[28..].to_vec();

        let metadata = self.initial_tx_metadata()?;

        let onchain_metadata_standard = Some(metadata.as_ref().and_then(|m| m.version));
        let onchain_metadata = metadata.as_ref().map(|m| m.metadata.clone());
        let onchain_metadata_extra = Some(metadata.as_ref().and_then(|m| m.extra.clone()));

        let asset_name = hex::encode(asset);
        let asset_name = (!asset_name.is_empty()).then_some(asset_name);

        let out = Asset {
            asset: hex::encode(&self.subject),
            policy_id: hex::encode(policy),
            asset_name,
            fingerprint: asset_fingerprint(&self.subject)?,
            quantity: self.asset_state.quantity().to_string(),
            initial_mint_tx_hash: self.asset_state.initial_tx.to_string(),
            mint_or_burn_count: self.asset_state.mint_tx_count as i32,
            onchain_metadata,
            onchain_metadata_standard,
            onchain_metadata_extra,
            metadata: None,
        };

        Ok(out)
    }
}

pub async fn by_subject<D: Domain>(
    Path(unit): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Asset>, StatusCode> {
    let subject = hex::decode(&unit).map_err(|_| StatusCode::BAD_REQUEST)?;

    let asset_state = domain
        .state3()
        .read_entity_typed::<dolos_cardano::model::AssetState>(&subject)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let initial_tx = domain
        .archive()
        .get_tx(asset_state.initial_tx.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = AssetModelBuilder {
        subject,
        unit,
        asset_state,
        initial_tx,
    };

    model.into_response()
}
