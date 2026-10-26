use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    proposal_metadata::ProposalMetadata, proposal_metadata_v2::ProposalMetadataV2,
    DrepsInnerMetadataError,
};
use dolos_cardano::model::{ProposalKey, ProposalState};
use dolos_core::Domain;
use pallas::{crypto::hash::Hash, ledger::primitives::conway::Anchor};

use crate::{
    error::Error,
    mapping::{anchor_offchain_metadata, bech32_gov_action, parse_gov_action_id, AnchorMetadata},
    Facade,
};

use super::read_proposal_row;

/// This function builds the common fields for a proposal metadata response.
///
/// The anchor contains the metadata URL and hash from the proposal procedure.
/// The function returns metadata or an error, but not both.
async fn proposal_metadata_parts(
    tx: &Hash<32>,
    idx: u32,
    anchor: &Anchor,
    ipfs_gateways: &[String],
) -> Result<
    (
        String,
        String,
        Option<AnchorMetadata>,
        Option<DrepsInnerMetadataError>,
    ),
    StatusCode,
> {
    let id = bech32_gov_action(tx, idx)?;
    let hash = hex::encode(anchor.content_hash);

    let (metadata, error) =
        anchor_offchain_metadata(&anchor.url, anchor.content_hash.as_ref(), ipfs_gateways).await;

    Ok((id, hash, metadata, error))
}

/// The anchor of the proposal `tx` proposed at action index `idx`, from its
/// state row or its final row. Both metadata routes read it here. A proposal
/// without an anchor is a 404.
fn proposal_anchor<D: Domain>(
    domain: &Facade<D>,
    tx: Hash<32>,
    idx: u32,
) -> Result<Anchor, StatusCode> {
    read_proposal_row(&domain.inner, &ProposalKey::new(tx, idx))?
        .ok_or(StatusCode::NOT_FOUND)?
        .into_state()
        .anchor
        .ok_or(StatusCode::NOT_FOUND)
}

/// This endpoint returns proposal metadata by transaction hash and certificate
/// index.
///
/// An absent anchor causes a 404 response.
/// A failed metadata fetch causes a 404 response.
/// The `/{gov_action_id}/metadata` endpoint returns the anchor and an error
/// object.
pub async fn proposal_metadata<D: Domain>(
    Path((tx_hash, cert_index)): Path<(String, u32)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ProposalMetadata>, Error>
where
    Option<ProposalState>: From<D::Entity>,
{
    let tx: Hash<32> = tx_hash.parse().map_err(|_| StatusCode::BAD_REQUEST)?;

    let anchor = proposal_anchor(&domain, tx, cert_index)?;

    let gateways = domain.config.ipfs_gateways();
    let (id, hash, metadata, _error) =
        proposal_metadata_parts(&tx, cert_index, &anchor, &gateways).await?;

    let metadata = metadata.ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ProposalMetadata {
        id,
        tx_hash: hex::encode(tx),
        cert_index: cert_index.try_into().map_err(|_| StatusCode::BAD_REQUEST)?,
        url: anchor.url,
        hash,
        json_metadata: Some(metadata.json),
        bytes: metadata.bytes,
    }))
}

/// This endpoint returns proposal metadata by CIP-129 governance-action ID.
///
/// The endpoint returns the anchor for a failed metadata fetch.
/// It returns null metadata fields and an error object.
pub async fn proposal_metadata_by_gov_action<D: Domain>(
    Path(gov_action_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ProposalMetadataV2>, Error>
where
    Option<ProposalState>: From<D::Entity>,
{
    let (tx, idx) = parse_gov_action_id(&gov_action_id).map_err(|_| Error::InvalidGovActionId)?;

    let anchor = proposal_anchor(&domain, tx, idx)?;

    let gateways = domain.config.ipfs_gateways();
    let (id, hash, metadata, error) = proposal_metadata_parts(&tx, idx, &anchor, &gateways).await?;

    let (json_metadata, bytes) = match metadata {
        Some(AnchorMetadata { json, bytes }) => (Some(json), Some(bytes)),
        None => (None, None),
    };

    Ok(Json(ProposalMetadataV2 {
        id,
        tx_hash: hex::encode(tx),
        cert_index: idx.try_into().map_err(|_| StatusCode::BAD_REQUEST)?,
        url: anchor.url,
        hash,
        json_metadata,
        bytes,
        error: error.map(Box::new),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{routes::governance::testing::*, test_support::TestDomainBuilder};
    use dolos_core::config::MinibfConfig;
    use dolos_testing::{measured::MeasuredStores, toy_domain::MemoryStores};

    #[tokio::test]
    async fn governance_proposal_metadata_returns_404_when_fetch_fails() {
        let app = proposal_app();
        let tx = tx_hash_of_block(&app, 0);

        assert_status(
            &app,
            &format!("/governance/proposals/{tx}/0/metadata"),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_proposal_metadata_by_gov_action_returns_anchor_error() {
        let app = proposal_app();
        let tx = tx_hash_of_block(&app, 0);
        let tx_hash: Hash<32> = tx.parse().expect("Cannot parse the transaction hash.");
        let id = bech32_gov_action(&tx_hash, 0).expect("Cannot encode the governance action ID.");

        let (status, body) = app
            .get_bytes(&format!("/governance/proposals/{id}/metadata"))
            .await;

        assert_eq!(status, StatusCode::OK);

        let metadata: ProposalMetadataV2 =
            serde_json::from_slice(&body).expect("Cannot parse the proposal metadata.");

        assert_eq!(metadata.id, id);
        assert_eq!(metadata.tx_hash, tx);
        assert_eq!(metadata.cert_index, 0);
        assert_eq!(metadata.url, "https://example.invalid/proposal");
        assert_eq!(metadata.hash, hex::encode([6u8; 32]));
        assert!(metadata.json_metadata.is_none());
        assert!(metadata.bytes.is_none());
        assert_eq!(
            metadata.error.expect("The fetch error is absent.").code,
            blockfrost_openapi::models::dreps_inner_metadata_error::Code::ConnectionError
        );
    }

    #[tokio::test]
    async fn governance_proposal_metadata_by_gov_action_bad_request() {
        let app = proposal_app();

        // a malformed CIP-129 id is a 400, not a lookup that misses
        for id in ["not-bech32", &missing_drep()] {
            let path = format!("/governance/proposals/{id}/metadata");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    /// On a proposal moved to the final-row log, the by-id route serves the
    /// anchor from the final row, and the by-tx route reads the same log
    /// rows. The by-tx route answers 404 when the anchor fetch fails, and the
    /// log read scans every row whatever the key, so this shows only that it
    /// falls back to the log, not which row it uses.
    #[tokio::test]
    async fn both_metadata_routes_read_a_moved_proposal_from_its_final_row() {
        let (domain, vectors) =
            TestDomainBuilder::<MeasuredStores<MemoryStores>>::new_with_synthetic_in(
                resolved_votes_cfg(),
                |_| (),
            )
            .finish();
        enact_proposals(&domain, true);

        let facade = Facade {
            inner: domain.clone(),
            config: MinibfConfig::new("[::]:0".parse().expect("invalid listen address")),
        };
        let tx = vectors.blocks[0].tx_hashes[0].clone();
        let tx_hash: Hash<32> = tx.parse().expect("Cannot parse the transaction hash.");
        let id = bech32_gov_action(&tx_hash, 0).expect("Cannot encode the governance action ID.");
        let log_rows = || domain.archive().counters.snapshot().log_rows;

        let before = log_rows();
        let Json(by_id) = proposal_metadata_by_gov_action(Path(id), State(facade.clone()))
            .await
            .expect("The by-id route failed.");
        assert_eq!(by_id.url, "https://example.invalid/proposal");
        let read_by_id = log_rows() - before;
        assert!(read_by_id > 0);

        let before = log_rows();
        let by_tx = proposal_metadata(Path((tx, 0)), State(facade)).await;
        assert!(matches!(by_tx, Err(Error::Code(StatusCode::NOT_FOUND))));
        assert_eq!(log_rows() - before, read_by_id);
    }
}
