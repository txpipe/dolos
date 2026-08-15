use axum::http::StatusCode;
use blockfrost_openapi::models::{
    dreps_inner_metadata_error::Code as MetadataError, DrepsInnerMetadata, DrepsInnerMetadataError,
};
use pallas::{crypto::hash::Hasher, ledger::primitives::conway::Anchor};
use std::{sync::OnceLock, time::Duration};

const MAX_METADATA_BYTES: usize = 1024 * 1024;

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
            "Error Offchain DRep: HTTP response error from {url} resulted in HTTP status code: {} \"{reason}\"",
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

fn size_exceeded_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::SizeExceeded,
        format!(
            "Error Offchain Drep: Metadata from {url} exceeds the maximum allowed size of {MAX_METADATA_BYTES} bytes."
        ),
    )
}

fn blocked_url_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::ConnectionError,
        format!("Error Offchain Drep: Refused to fetch metadata from {url}, only http and https URLs are allowed."),
    )
}

fn is_fetchable(url: &str) -> bool {
    reqwest::Url::parse(url).is_ok_and(|x| matches!(x.scheme(), "http" | "https"))
}

fn http_client() -> Option<&'static reqwest::Client> {
    static CLIENT: OnceLock<Option<reqwest::Client>> = OnceLock::new();

    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .redirect(reqwest::redirect::Policy::limited(3))
                .user_agent("Dolos MiniBF")
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
        return errored(out, connection_error(&anchor.url));
    };

    if !is_fetchable(&anchor.url) {
        return errored(out, blocked_url_error(&anchor.url));
    }

    let mut response = match client.get(&anchor.url).send().await {
        Ok(response) => response,
        Err(_) => return errored(out, connection_error(&anchor.url)),
    };

    if response.status() != StatusCode::OK {
        return errored(out, http_response_error(&anchor.url, response.status()));
    }

    if response
        .content_length()
        .is_some_and(|len| len > MAX_METADATA_BYTES as u64)
    {
        return errored(out, size_exceeded_error(&anchor.url));
    }

    let mut body = Vec::new();

    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if body.len() + chunk.len() > MAX_METADATA_BYTES {
                    return errored(out, size_exceeded_error(&anchor.url));
                }

                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(_) => return errored(out, connection_error(&anchor.url)),
        }
    }

    let actual_hash = Hasher::<256>::hash(&body);

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

    out.json_metadata = serde_json::from_slice(&body).ok();
    out.bytes = Some(format!("\\x{}", hex::encode(&body)));

    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_http_schemes() {
        assert!(!is_fetchable("file:///etc/passwd"));
        assert!(!is_fetchable("ftp://example.com/x"));
        assert!(!is_fetchable("not a url"));
    }

    #[test]
    fn accepts_http_urls() {
        assert!(is_fetchable("https://example.com/meta.json"));
        assert!(is_fetchable("http://example.com/meta.json"));
    }
}
