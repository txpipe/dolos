use axum::http::StatusCode;
use blockfrost_openapi::models::{
    dreps_inner_metadata_error::Code as MetadataError, DrepsInnerMetadata, DrepsInnerMetadataError,
};
use pallas::{crypto::hash::Hasher, ledger::primitives::conway::Anchor};
use std::{net::IpAddr, sync::OnceLock, time::Duration};

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
        format!("Error Offchain Drep: Refused to fetch metadata from {url}, only public http and https URLs are allowed."),
    )
}

fn decode_error(url: &str) -> DrepsInnerMetadataError {
    DrepsInnerMetadataError::new(
        MetadataError::DecodeError,
        format!(
            "Error Offchain Drep: Failed to decode metadata from {url}, payload is not valid JSON."
        ),
    )
}

/// The anchor URL is attacker-controlled on-chain data; an address in one of
/// these ranges would let a DRep aim the node's own network position
/// (cloud metadata services, localhost daemons, LAN hosts).
fn ip_is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            let cgnat = ip.octets()[0] == 100 && (ip.octets()[1] & 0b1100_0000) == 0b0100_0000;

            !(ip.is_loopback()
                || ip.is_private()
                || ip.is_link_local()
                || ip.is_unspecified()
                || ip.is_broadcast()
                || ip.is_documentation()
                || cgnat)
        }
        IpAddr::V6(ip) => {
            let unique_local = (ip.segments()[0] & 0xfe00) == 0xfc00;
            let link_local = (ip.segments()[0] & 0xffc0) == 0xfe80;

            !(ip.is_loopback() || ip.is_unspecified() || unique_local || link_local)
        }
    }
}

fn is_fetchable(url: &str) -> bool {
    let Ok(parsed) = reqwest::Url::parse(url) else {
        return false;
    };

    if !matches!(parsed.scheme(), "http" | "https") {
        return false;
    }

    let Some(host) = parsed.host_str() else {
        return false;
    };

    // IPv6 hosts keep their brackets in `host_str`
    let host = host.trim_start_matches('[').trim_end_matches(']');

    match host.parse::<IpAddr>() {
        Ok(ip) => ip_is_public(ip),
        Err(_) => !host.eq_ignore_ascii_case("localhost"),
    }
}

fn http_client() -> Option<&'static reqwest::Client> {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

    if let Some(client) = CLIENT.get() {
        return Some(client);
    }

    // built outside `get_or_init` so a failed build is retried on the next
    // call instead of pinning every future fetch to a connection error
    let built = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        // every redirect hop gets the same public-URL gate as the anchor
        .redirect(reqwest::redirect::Policy::custom(|attempt| {
            if attempt.previous().len() > 3 {
                attempt.error("too many redirects")
            } else if !is_fetchable(attempt.url().as_str()) {
                attempt.error("redirect to a non-public URL")
            } else {
                attempt.follow()
            }
        }))
        .user_agent("Dolos MiniBF")
        .build()
        .ok()?;

    Some(CLIENT.get_or_init(|| built))
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

    if !response.status().is_success() {
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

    match serde_json::from_slice(&body) {
        Ok(json) => {
            out.json_metadata = Some(json);
            out.bytes = Some(format!("\\x{}", hex::encode(&body)));
        }
        Err(_) => {
            // the spec keeps `json_metadata` and `bytes` null on failed
            // validation and reports the failure through `error`
            return errored(out, decode_error(&anchor.url));
        }
    }

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
    fn rejects_non_public_hosts() {
        assert!(!is_fetchable("http://127.0.0.1:8080/meta.json"));
        assert!(!is_fetchable("http://localhost:3000/meta.json"));
        assert!(!is_fetchable("http://169.254.169.254/latest/meta-data"));
        assert!(!is_fetchable("http://10.1.2.3/meta.json"));
        assert!(!is_fetchable("http://172.16.0.1/meta.json"));
        assert!(!is_fetchable("http://192.168.1.1/meta.json"));
        assert!(!is_fetchable("http://100.64.0.1/meta.json"));
        assert!(!is_fetchable("http://0.0.0.0/meta.json"));
        assert!(!is_fetchable("https://[::1]/meta.json"));
        assert!(!is_fetchable("https://[fe80::1]/meta.json"));
        assert!(!is_fetchable("https://[fd00::1]/meta.json"));
    }

    #[test]
    fn accepts_http_urls() {
        assert!(is_fetchable("https://example.com/meta.json"));
        assert!(is_fetchable("http://example.com/meta.json"));
        assert!(is_fetchable("https://93.184.216.34/meta.json"));
        assert!(is_fetchable("http://100.128.0.1/meta.json"));
    }
}
