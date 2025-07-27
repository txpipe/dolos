use std::collections::HashMap;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset::{Asset, OnchainMetadataStandard};
use dolos_core::{ArchiveStore, Domain, EraCbor, State3Store as _};
use pallas::ledger::{primitives::Metadatum, traverse::MultiEraTx};

use crate::{
    mapping::{asset_fingerprint, IntoModel},
    Facade,
};

#[derive(Clone)]
enum OnchainMetadata {
    CIP25v1(Metadatum),
    CIP25v2(Metadatum),
}

impl OnchainMetadata {
    fn as_metadatum(&self) -> &Metadatum {
        match self {
            OnchainMetadata::CIP25v1(m) => m,
            OnchainMetadata::CIP25v2(m) => m,
        }
    }
}

impl IntoModel<OnchainMetadataStandard> for OnchainMetadata {
    type SortKey = ();

    fn into_model(self) -> Result<OnchainMetadataStandard, StatusCode> {
        let out = match self {
            OnchainMetadata::CIP25v1(_) => OnchainMetadataStandard::Cip25v1,
            OnchainMetadata::CIP25v2(_) => OnchainMetadataStandard::Cip25v2,
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
            quantity: self.asset_state.quantity.to_string(),
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

pub async fn by_subject<D: Domain>(
    Path(subject): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Asset>, StatusCode> {
    let subject = hex::decode(subject).map_err(|_| StatusCode::BAD_REQUEST)?;

    let state = domain
        .state3()
        .read_entity_typed::<dolos_cardano::model::AssetState>(&subject)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let initial_tx = domain
        .archive()
        .get_tx(state.initial_tx.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = AssetModelBuilder {
        subject,
        asset_state: state,
        initial_tx: initial_tx,
    };

    model.into_response()
}
