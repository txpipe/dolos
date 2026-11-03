//! The `/scripts` routes: the script and datum hash parsing the endpoints
//! share.

use pallas::crypto::hash::Hash;
use reqwest::StatusCode;

mod all;
mod by_datum_hash;
mod by_datum_hash_cbor;
mod by_hash;
mod by_hash_cbor;
mod by_hash_json;
mod by_hash_redeemers;
mod by_hash_utxos;

pub use all::all;
pub use by_datum_hash::by_datum_hash;
pub use by_datum_hash_cbor::by_datum_hash_cbor;
pub use by_hash::by_hash;
pub use by_hash_cbor::by_hash_cbor;
pub use by_hash_json::by_hash_json;
pub use by_hash_redeemers::by_hash_redeemers;
pub use by_hash_utxos::by_hash_utxos;

fn parse_script_hash(script_hash: &str) -> Result<Hash<28>, StatusCode> {
    if script_hash.len() != 56 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Hash::<28>::from(
        hex::decode(script_hash)
            .map_err(|_| StatusCode::NOT_FOUND)?
            .as_slice(),
    ))
}

fn parse_datum_hash(datum_hash: &str) -> Result<Hash<32>, StatusCode> {
    if datum_hash.len() != 64 {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Hash::<32>::from(
        hex::decode(datum_hash)
            .map_err(|_| StatusCode::NOT_FOUND)?
            .as_slice(),
    ))
}

#[cfg(test)]
pub(super) mod testing {
    use super::*;
    use crate::test_support::TestApp;

    pub fn fixture_app() -> TestApp {
        TestApp::new()
    }

    pub fn invalid_script_hash() -> &'static str {
        "not-a-script-hash"
    }

    pub fn missing_script_hash() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    pub fn invalid_datum_hash() -> &'static str {
        "not-a-datum-hash"
    }

    pub fn missing_datum_hash() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
}
