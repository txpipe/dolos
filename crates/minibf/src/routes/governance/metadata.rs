use axum::http::StatusCode;
use blockfrost_openapi::models::{
    dreps_inner_metadata_error::Code as MetadataError, DrepsInnerMetadata, DrepsInnerMetadataError,
};
use pallas::{crypto::hash::Hasher, ledger::primitives::conway::Anchor};
use std::time::Duration;

fn hash_mismatch_error(
    url: &str,
    expected_hash: &[u8],
    actual_hash: &[u8],
) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::HashMismatch,
        format!(
            "Hash mismatch when fetching metadata from {url}. Expected \"{}\" but got \"{}\".",
            hex::encode(expected_hash),
            hex::encode(actual_hash),
        ),
    )
}

fn http_response_error(url: &str, status: StatusCode) -> DrepsInnerMetadataError {
    let reason = status.canonical_reason().unwrap_or("Unknown");

    DrepsInnerMetadataError::new(
        MetadataError::HttpResponseError,
        format!(
            "Error Offchain Drep: HTTP Response error from {url} resulted in HTTP status code : {} \"{reason}\"",
            status.as_u16(),
        ),
    )
}

fn connection_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::ConnectionError,
        format!("Error Offchain Drep: Connection failure error when fetching metadata from {url}."),
    )
}

pub async fn fetch_drep_metadata(anchor: Option<Anchor>) -> Option<DrepsInnerMetadata> {
    let anchor = anchor?;

    let mut out = DrepsInnerMetadata {
        url: anchor.url.clone(),
        hash: hex::encode(anchor.content_hash),
        json_metadata: None,
        bytes: None,
        error: None,
    };

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::limited(3))
        .user_agent("dolos")
        .build()
    {
        Ok(client) => client,
        Err(_) => return Some(out),
    };

    let response = match client.get(&anchor.url).send().await {
        Ok(response) => response,
        Err(_) => {
            out.error = Some(Box::new(connection_error(&anchor.url)));
            return Some(out);
        }
    };

    if response.status() != StatusCode::OK {
        out.error = Some(Box::new(http_response_error(
            &anchor.url,
            response.status(),
        )));
        return Some(out);
    }

    let body = match response.bytes().await {
        Ok(body) => body,
        Err(_) => {
            out.error = Some(Box::new(connection_error(&anchor.url)));
            return Some(out);
        }
    };

    let actual_hash = Hasher::<256>::hash(body.as_ref());

    if actual_hash.as_ref() != anchor.content_hash.as_slice() {
        out.error = Some(Box::new(hash_mismatch_error(
            &anchor.url,
            anchor.content_hash.as_slice(),
            actual_hash.as_ref(),
        )));
        return Some(out);
    }

    out.json_metadata = serde_json::from_slice(body.as_ref()).ok();
    out.bytes = Some(format!("\\x{}", hex::encode(body.as_ref())));

    Some(out)
}
