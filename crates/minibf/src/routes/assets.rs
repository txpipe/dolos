use std::collections::HashMap;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset::{Asset, OnchainMetadataStandard};
use crc::{Crc, CRC_8_SMBUS};
use dolos_core::{ArchiveStore, Domain, EraCbor, State3Store as _};
use pallas::ledger::{
    primitives::{Metadatum, PolicyId},
    traverse::MultiEraTx,
};

use crate::{
    mapping::{asset_fingerprint, IntoModel},
    Facade,
};

#[derive(Clone)]
enum OnchainMetadata {
    CIP25v1(Metadatum),
    CIP68v1(Metadatum),
}

impl OnchainMetadata {
    fn as_metadatum(&self) -> &Metadatum {
        match self {
            OnchainMetadata::CIP25v1(m) => m,
            OnchainMetadata::CIP68v1(m) => m,
        }
    }
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
        let Metadatum::Map(map) = self.as_metadatum() else {
            return Ok(HashMap::new());
        };

        let to_key = |k: &Metadatum| match k {
            Metadatum::Int(int) => Ok(int.to_string()),
            Metadatum::Text(text) => Ok(text.to_string()),
            _ => Err(StatusCode::INTERNAL_SERVER_ERROR),
        };

        let to_value =
            |v: &Metadatum| serde_json::to_value(v).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR);

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
}

struct AssetModelBuilder {
    subject: Vec<u8>,
    asset_state: dolos_cardano::model::AssetState,
    initial_tx: Option<EraCbor>,
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

impl AssetModelBuilder {
    fn initial_tx_metadata(&self) -> Result<Option<OnchainMetadata>, StatusCode> {
        let Some(EraCbor(era, cbor)) = &self.initial_tx else {
            return Ok(None);
        };

        let era = pallas::ledger::traverse::Era::try_from(*era)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let tx =
            MultiEraTx::decode_for_era(era, cbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

fn cip_68_reference_asset(unit: &str) -> Option<String> {
    let policy_id = &unit[..28];
    let asset_name = &unit[28..];

    let label = &asset_name[0..8];

    if label.len() != 8 || !(label.starts_with('0') && label.ends_with('0')) {
        return None;
    }

    // TODO: check if it's required to ignore label checksum
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

    // TODO: check if the ref_asset is inside the output first
    // TODO: if cip_68_reference_asset is None, validate CIP25 metadata
    if let Some(ref_unit) = cip_68_reference_asset(&unit) {
        let subject = hex::decode(&ref_unit).map_err(|_| StatusCode::BAD_REQUEST)?;
        let asset_state = domain
            .state3()
            .read_entity_typed::<dolos_cardano::model::AssetState>(&subject);
        dbg!(asset_state);
        // TODO: return response when it is CIP68
    }

    // TODO: check CIP25

    // TODO: refactor asset model builder
    let model = AssetModelBuilder {
        subject,
        asset_state,
        initial_tx,
    };

    model.into_response()
}
