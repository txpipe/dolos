use axum::http::StatusCode;
use blockfrost_openapi::models::{
    dreps_inner_metadata_error::Code as MetadataError, DrepsInnerMetadata, DrepsInnerMetadataError,
};
use pallas::{crypto::hash::Hasher, ledger::primitives::conway::Anchor};
use std::{sync::OnceLock, time::Duration};

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

fn http_client() -> Option<&'static reqwest::Client> {
    static CLIENT: OnceLock<Option<reqwest::Client>> = OnceLock::new();

    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::limited(3))
                .user_agent("dolos")
                .build()
                .ok()
        })
        .as_ref()
}

fn errored(
    mut out: DrepsInnerMetadata,
    error: DrepsInnerMetadataError,
) -> Option<DrepsInnerMetadata> {
    out.error = Some(Box::new(error));
    Some(out)
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

    let Some(client) = http_client() else {
        return Some(out);
    };

    let response = match client.get(&anchor.url).send().await {
        Ok(response) => response,
        Err(_) => return errored(out, connection_error(&anchor.url)),
    };

    if response.status() != StatusCode::OK {
        return errored(out, http_response_error(&anchor.url, response.status()));
    }

    let body = match response.bytes().await {
        Ok(body) => body,
        Err(_) => return errored(out, connection_error(&anchor.url)),
    };

    let actual_hash = Hasher::<256>::hash(body.as_ref());

    if actual_hash.as_ref() != anchor.content_hash.as_slice() {
        return errored(
            out,
            hash_mismatch_error(
                &anchor.url,
                anchor.content_hash.as_slice(),
                actual_hash.as_ref(),
            ),
        );
    }

    out.json_metadata = serde_json::from_slice(body.as_ref()).ok();
    out.bytes = Some(format!("\\x{}", hex::encode(body.as_ref())));

    Some(out)
}
