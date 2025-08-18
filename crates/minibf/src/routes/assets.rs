use std::{collections::HashMap, ops::Deref};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset::{Asset, OnchainMetadataStandard};
use crc::{Crc, CRC_8_SMBUS};
use dolos_core::{ArchiveStore, Domain, EraCbor, State3Store as _};
use pallas::ledger::{
    primitives::{BigInt, Metadatum, PlutusData},
    traverse::MultiEraTx,
    validate::phase2::to_plutus_data::ToPlutusData,
};
use serde_json::Value;

use crate::{
    mapping::{asset_fingerprint, IntoModel},
    Facade,
};

#[derive(Clone)]
enum OnchainMetadata {
    CIP25v1(Metadatum),
    CIP68v1(PlutusData),
}

impl IntoModel<OnchainMetadataStandard> for OnchainMetadata {
    type SortKey = ();

    fn into_model(self) -> Result<OnchainMetadataStandard, StatusCode> {
        let out = match self {
            OnchainMetadata::CIP25v1(_) => OnchainMetadataStandard::Cip25v1,
            OnchainMetadata::CIP68v1(_) => OnchainMetadataStandard::Cip68v1,
        };

        Ok(out)
    }
}

impl IntoModel<HashMap<String, serde_json::Value>> for OnchainMetadata {
    type SortKey = ();

    fn into_model(self) -> Result<HashMap<String, serde_json::Value>, StatusCode> {
        match self {
            OnchainMetadata::CIP25v1(metadatum) => {
                let Metadatum::Map(map) = metadatum else {
                    return Ok(HashMap::new());
                };

                let to_key = |k: &Metadatum| match k {
                    Metadatum::Int(int) => Ok(int.to_string()),
                    Metadatum::Text(text) => Ok(text.to_string()),
                    _ => Err(StatusCode::INTERNAL_SERVER_ERROR),
                };

                let to_value = |v: &Metadatum| {
                    serde_json::to_value(v).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
                };

                let to_entry = |(k, v): (&Metadatum, &Metadatum)| {
                    let k = to_key(k)?;
                    let v = to_value(v)?;
                    Result::<_, StatusCode>::Ok((k, v))
                };

                let out = map
                    .iter()
                    .map(|(k, v)| to_entry((k, v)))
                    .collect::<Result<_, _>>()?;

                Ok(out)
            }
            OnchainMetadata::CIP68v1(plutus_data) => {
                let v = plutus_metadata(&plutus_data);
                if v.is_null() || !v.is_object() {
                    return Ok(HashMap::new());
                }

                let out = v.as_object().unwrap().clone().into_iter().collect();

                return Ok(out);
            }
        }
    }
}

const CRC8_ALGO: Crc<u8> = Crc::<u8>::new(&CRC_8_SMBUS);
#[derive(Debug, Clone)]
enum CIP68Label {
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

    // TODO: verify why label checksum is required
    pub fn to_label(&self) -> String {
        let number_hex = format!("{:04x}", self.to_u32());
        let bytes = hex::decode(&number_hex).unwrap();
        let checksum = format!("{:02x}", CRC8_ALGO.checksum(&bytes));
        format!("0{}{}0", number_hex, checksum)
    }
}

struct AssetModelBuilder {
    subject: Vec<u8>,
    asset_state: dolos_cardano::model::AssetState,
    initial_tx: Option<EraCbor>,
}

impl AssetModelBuilder {
    fn initial_tx_metadata(&self) -> Result<Option<OnchainMetadata>, StatusCode> {
        let Some(EraCbor(era, cbor)) = &self.initial_tx else {
            return Ok(None);
        };

        let era = pallas::ledger::traverse::Era::try_from(*era)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx =
            MultiEraTx::decode_for_era(era, cbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // TODO: add unit hex string into the struct builder
        // TODO: check if the initial_tx will always contains the token 100 in the output
        if let Some(ref_asset) = cip_68_reference_asset(&hex::encode(&self.subject)) {
            let ref_asset_output = tx
                .outputs()
                .iter()
                .find(|o| {
                    o.value()
                        .assets()
                        .iter()
                        .find(|multi_asset| {
                            multi_asset
                                .assets()
                                .iter()
                                .find(|asset| {
                                    let mut unit = multi_asset.policy().to_vec();
                                    unit.extend(asset.name());
                                    let unit = hex::encode(unit);
                                    ref_asset.eq(&unit)
                                })
                                .is_some()
                        })
                        .is_some()
                })
                .cloned();

            if let Some(ref_asset_output) = ref_asset_output {
                if let Some(datum_option) = ref_asset_output.datum() {
                    match datum_option {
                        pallas::ledger::primitives::conway::DatumOption::Hash(_hash) => {
                            // TODO: what to do?
                        }
                        pallas::ledger::primitives::conway::DatumOption::Data(cbor_wrap) => {
                            let out = OnchainMetadata::CIP68v1(cbor_wrap.to_plutus_data());
                            return Ok(Some(out));
                        }
                    };
                }
            }
        }

        let metadata = tx.metadata();
        let out = metadata.find(721).cloned().map(OnchainMetadata::CIP25v1);

        Ok(out)
    }
}

impl IntoModel<Asset> for AssetModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<Asset, StatusCode> {
        let policy = self.subject[..28].to_vec();
        let asset = self.subject[28..].to_vec();

        let metadata = self.initial_tx_metadata()?;

        let standard = metadata.clone().map(|m| m.into_model()).transpose()?;

        let metadata = metadata.map(|m| m.into_model()).transpose()?;

        let out = Asset {
            asset: hex::encode(&self.subject),
            policy_id: hex::encode(policy),
            asset_name: Some(hex::encode(asset)),
            fingerprint: asset_fingerprint(&self.subject)?,
            quantity: self.asset_state.quantity().to_string(),
            initial_mint_tx_hash: self.asset_state.initial_tx.to_string(),
            mint_or_burn_count: self.asset_state.mint_tx_count as i32,
            onchain_metadata: metadata,
            onchain_metadata_standard: Some(standard),
            onchain_metadata_extra: None,
            metadata: None,
        };

        Ok(out)
    }
}

fn plutus_metadata(plutus_data: &PlutusData) -> serde_json::Value {
    match plutus_data {
        PlutusData::Constr(x) => x
            .fields
            .iter()
            .map(plutus_metadata)
            .collect::<Vec<serde_json::Value>>()
            .first()
            .cloned()
            .unwrap_or(serde_json::Value::Null),
        PlutusData::Map(x) => {
            let map = x
                .iter()
                .filter_map(|(k, v)| {
                    plutus_metadata(k)
                        .as_str()
                        .map(|key| (key.to_string(), plutus_metadata(v)))
                })
                .collect();

            Value::Object(map)
        }
        PlutusData::Array(x) => serde_json::Value::Array(x.iter().map(plutus_metadata).collect()),
        PlutusData::BigInt(x) => match x {
            BigInt::Int(int) => match i64::try_from(*int.deref()) {
                Ok(x) => serde_json::Value::Number(x.into()),
                Err(_) => {
                    serde_json::Value::String(hex::encode(i128::from(*int.deref()).to_be_bytes()))
                }
            },
            BigInt::BigUInt(bounded_bytes) => {
                serde_json::Value::String(hex::encode(bounded_bytes.as_slice()))
            }
            BigInt::BigNInt(bounded_bytes) => {
                serde_json::Value::String(hex::encode(bounded_bytes.as_slice()))
            }
        },
        PlutusData::BoundedBytes(x) => serde_json::Value::String(
            String::from_utf8(x.to_vec()).unwrap_or(hex::encode(x.as_slice())),
        ),
    }
}

fn cip_68_reference_asset(unit: &str) -> Option<String> {
    let policy_id = &unit[..56];
    let asset_name = &unit[56..];

    let label = &asset_name[0..8];

    if label.len() != 8 || !(label.starts_with('0') && label.ends_with('0')) {
        return None;
    }

    let Ok(number) = u32::from_str_radix(&label[1..5], 16) else {
        return None;
    };

    let asset_name_without_label_prefix = &asset_name[8..];

    match CIP68Label::from_u32(number) {
        Some(label) => match label {
            CIP68Label::ReferenceNft => None,
            _ => Some(format!(
                "{}{}{}",
                policy_id.to_string(),
                CIP68Label::ReferenceNft.to_label(),
                asset_name_without_label_prefix
            )),
        },
        None => None,
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
    // dbg!(&initial_tx);

    // TODO: check if the ref_asset is inside the output first
    // TODO: if cip_68_reference_asset is None, validate CIP25 metadata
    // if let Some(ref_unit) = cip_68_reference_asset(&unit) {
    //     let subject = hex::decode(&ref_unit).map_err(|_| StatusCode::BAD_REQUEST)?;
    //     let asset_state = domain
    //         .state3()
    //         .read_entity_typed::<dolos_cardano::model::AssetState>(&subject)
    //         .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    //
    //     if let Some(asset_state) = asset_state {
    //         // dbg!(asset_state.initial_tx);
    //         // TODO: return response when it is CIP68
    //     }
    // }

    // TODO: check CIP25

    // TODO: refactor asset model builder
    let model = AssetModelBuilder {
        subject,
        asset_state,
        initial_tx,
    };

    model.into_response()
}
